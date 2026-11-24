using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace SimdKafka;

internal sealed record Topic(string Name, Guid Id, string Directory, PartitionLog[] Partitions) : IDisposable
{
    public void Dispose()
    {
        foreach (var partition in Partitions) partition.Dispose();
    }
}

internal sealed record CommittedOffset(string Topic, int Partition, long Offset, int Epoch, string? Metadata,
    Guid? TopicId = null, long CommitIndex = 0);
internal sealed record CatalogState(string ClusterId, long LastProducerId,
    Dictionary<string, int> Topics, Dictionary<string, CommittedOffset[]> Offsets,
    Dictionary<string, Guid>? TopicIds = null, string[]? RetiredDirectories = null, KafkaConfigurations? Configurations = null,
    ImmutableArray<KafkaAcl>? Acls = null, ScramDirectory? Scram = null, ClientQuotas? Quotas = null, TokenDirectory? Tokens = null);

internal sealed class Catalog : IDisposable
{
    private readonly object _gate = new();
    private readonly BrokerOptions _options;
    private readonly FileStream _lease;
    private readonly string _path;
    private readonly FlushScheduler? _flushes;
    private readonly RemoteTierRoot? _remoteTier;
    private readonly ConcurrentDictionary<string, Topic> _topics = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Guid, string> _namesById = new();
    private readonly List<Task> _reclaiming = [];
    private CatalogState _state;
    private int _partitionCount;
    private bool _faulted;

    internal Catalog(BrokerOptions options)
    {
        _options = options;
        DurableFileSystem.EnsureDirectory(options.Data, ownerOnly: options.DynamicScram || options.DelegationTokenSecretFile is not null);
        _path = Path.Combine(options.Data, "catalog.json");
        _lease = new FileStream(Path.Combine(options.Data, "broker.lock"), FileMode.OpenOrCreate,
            FileAccess.ReadWrite, FileShare.None);
        _flushes = options.Flush ? new FlushScheduler() : null;
        try
        {
            if (File.Exists(Path.Combine(options.Data, "control", "identity")))
                throw new InvalidDataException("The data directory belongs to a clustered broker");
            if (File.Exists(_path))
                _state = Load(_path);
            else
            {
                if (HasPartitionLogs(options.Data))
                    throw new InvalidDataException("Catalog is missing but partition logs remain");
                string clusterId = Convert.ToBase64String(Guid.NewGuid().ToByteArray()).TrimEnd('=')
                    .Replace('+', '-').Replace('/', '_');
                _state = new(clusterId, 0, new(StringComparer.Ordinal), new(StringComparer.Ordinal));
                Save(_state);
            }
            if (_state.Acls is { IsEmpty: false } && options.AclSuperUsers is not { Length: > 0 })
                throw new InvalidDataException("Persisted ACLs require an ACL superuser");
            if (options.TierDirectory is { } tier)
                _remoteTier = RemoteTierRoot.Connect(tier, options.Data, _state.ClusterId);
            else if (File.Exists(Path.Combine(options.Data, "remote-tier.identity")))
                throw new InvalidDataException("This data directory requires --tiered-storage-dir");
            foreach (var (name, count) in _state.Topics)
            {
                if (!ValidName(name) || count is < 1 or > BrokerOptions.MaxPartitionsPerTopic ||
                    _partitionCount + count > BrokerOptions.MaxPartitions)
                    throw new InvalidDataException($"Invalid catalog topic: {name}");
                Guid? storedId = _state.TopicIds?.GetValueOrDefault(name);
                Register(OpenTopic(name, count, storedId is { } id && id != Guid.Empty ? id : null, existing: true));
                _partitionCount += count;
            }
            foreach (string directory in _state.RetiredDirectories ?? [])
            {
                if (!IsDiskName(directory) || _topics.Values.Any(topic => Path.GetFileName(topic.Directory) == directory))
                    throw new InvalidDataException("Invalid retired topic directory");
            }
            foreach (string directory in _state.RetiredDirectories ?? [])
                ScheduleReclaim(directory, Task.CompletedTask);
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    internal string ClusterId => Volatile.Read(ref _state).ClusterId;
    internal long LastProducerId => Volatile.Read(ref _state).LastProducerId;
    internal KafkaConfigurations Configurations => Volatile.Read(ref _state).Configurations ?? KafkaConfigurations.Empty;
    internal ImmutableArray<KafkaAcl> Acls => Volatile.Read(ref _state).Acls ?? [];
    internal ScramDirectory Scram => Volatile.Read(ref _state).Scram ?? ScramDirectory.Empty;
    internal TokenDirectory Tokens => Volatile.Read(ref _state).Tokens ?? TokenDirectory.Empty;
    internal ClientQuotas Quotas => Volatile.Read(ref _state).Quotas ?? ClientQuotas.Empty;
    internal Topic? Get(string name) => _topics.GetValueOrDefault(name);
    internal Topic? Get(Guid id) => _namesById.TryGetValue(id, out string? name) &&
        Get(name) is { } topic && topic.Id == id ? topic : null;
    internal Topic? Get(ReadOnlySpan<byte> name)
    {
        if (name.Length is < 1 or > 249) return null;
        Span<char> chars = stackalloc char[name.Length];
        Encoding.ASCII.GetChars(name, chars);
        var lookup = _topics.GetAlternateLookup<ReadOnlySpan<char>>();
        return lookup.TryGetValue(chars, out var topic) ? topic : null;
    }
    internal Topic[] Topics() => _topics.Values.ToArray();

    internal (Error Error, Topic? Topic) Resolve(string name, bool autoCreate)
    {
        if (!ValidName(name)) return (Error.InvalidTopic, null);
        var topic = Get(name);
        if (topic is not null) return (Error.None, topic);
        if (!autoCreate || !_options.AutoCreate) return (Error.UnknownTopicOrPartition, null);
        var error = Create(name, _options.DefaultPartitions, false);
        return error is Error.None or Error.TopicAlreadyExists ? (Error.None, Get(name)) : (error, null);
    }

    internal Error Create(string name, int count, bool validateOnly) => Create(name, count, validateOnly, out _);

    internal Error Create(string name, int count, bool validateOnly, out Guid topicId, int? messageLimit = null,
        RemoteTopicPolicy? tier = null, bool? logAppendTime = null, TimestampLimits timestampLimits = default)
    {
        topicId = Guid.Empty;
        if (!ValidName(name)) return Error.InvalidTopic;
        if (tier is not null && !Tiered) return Error.InvalidConfig;
        if (count is < 1 or > BrokerOptions.MaxPartitionsPerTopic) return Error.InvalidPartitions;
        lock (_gate)
        {
            if (_faulted) return Error.KafkaStorageError;
            if (_topics.ContainsKey(name)) return Error.TopicAlreadyExists;
            if (_partitionCount + count > BrokerOptions.MaxPartitions) return Error.InvalidPartitions;
            if (validateOnly) return Error.None;
            Topic? topic = null;
            try
            {
                var id = Guid.NewGuid();
                topic = OpenTopic(name, count, id, existing: false);
                var topics = new Dictionary<string, int>(_state.Topics, StringComparer.Ordinal) { [name] = count };
                var ids = new Dictionary<string, Guid>(_state.TopicIds ?? [], StringComparer.Ordinal) { [name] = id };
                var configs = messageLimit.HasValue ? Configurations.WithTopicLimit(id, messageLimit) : _state.Configurations;
                if (tier is not null) configs = (configs ?? KafkaConfigurations.Empty).WithTopicTier(id, tier);
                if (logAppendTime.HasValue || !timestampLimits.IsEmpty)
                    configs = (configs ?? KafkaConfigurations.Empty).WithTopicTimestamp(id, logAppendTime, timestampLimits);
                Save(_state with { Topics = topics, TopicIds = ids, Configurations = configs });
                Register(topic);
                _partitionCount += count;
                topicId = id;
                return Error.None;
            }
            catch (Exception e) when (DurableFileSystem.IsStorageError(e))
            {
                topic?.Dispose();
                return Error.KafkaStorageError;
            }
        }
    }

    internal Error Expand(string name, int count, bool validateOnly, int? assignmentCount = null)
    {
        lock (_gate)
        {
            if (_faulted) return Error.KafkaStorageError;
            var topic = Get(name);
            if (topic is null) return Error.UnknownTopicOrPartition;
            int current = topic.Partitions.Length;
            if (count <= current || count > BrokerOptions.MaxPartitionsPerTopic ||
                _partitionCount + count - current > BrokerOptions.MaxPartitions)
                return Error.InvalidPartitions;
            if (assignmentCount.HasValue && assignmentCount != count - current)
                return Error.InvalidReplicaAssignment;
            if (validateOnly) return Error.None;

            var logs = new PartitionLog[count];
            topic.Partitions.CopyTo(logs, 0);
            int opened = current;
            try
            {
                for (; opened < count; opened++)
                    logs[opened] = OpenPartition(topic.Directory, topic.Id, opened, existing: false);
                DurableFileSystem.FlushDirectory(topic.Directory);
                var topics = new Dictionary<string, int>(_state.Topics, StringComparer.Ordinal) { [name] = count };
                Save(_state with { Topics = topics });
                _topics[name] = topic with { Partitions = logs };
                _partitionCount += count - current;
                return Error.None;
            }
            catch (Exception e) when (DurableFileSystem.IsStorageError(e))
            {
                for (int p = current; p < opened; p++) logs[p].Dispose();
                return Error.KafkaStorageError;
            }
        }
    }

    internal (Error Error, string? Name, Guid Id) Delete(string? name, Guid id)
    {
        lock (_gate)
        {
            if (_faulted) return (Error.KafkaStorageError, name, id);
            var topic = id == Guid.Empty ? Get(name!) : Get(id);
            if (topic is null)
                return (id == Guid.Empty ? Error.UnknownTopicOrPartition : Error.UnknownTopicId, name, id);

            var topics = new Dictionary<string, int>(_state.Topics, StringComparer.Ordinal);
            topics.Remove(topic.Name);
            var ids = new Dictionary<string, Guid>(_state.TopicIds ?? [], StringComparer.Ordinal);
            ids.Remove(topic.Name);
            var offsets = new Dictionary<string, CommittedOffset[]>(StringComparer.Ordinal);
            foreach (var (group, committed) in _state.Offsets)
            {
                var kept = committed.Where(offset => offset.Topic != topic.Name).ToArray();
                if (kept.Length > 0) offsets.Add(group, kept);
            }
            string directory = Path.GetFileName(topic.Directory);
            string[] retired = [.._state.RetiredDirectories ?? [], directory];
            try
            {
                Save(_state with { Topics = topics, TopicIds = ids, Offsets = offsets, RetiredDirectories = retired,
                    Configurations = _state.Configurations?.WithTopicLimit(topic.Id, null).WithTopicTier(topic.Id, null)
                        .WithTopicTimestamp(topic.Id, null) });
            }
            catch (Exception e) when (DurableFileSystem.IsStorageError(e))
            {
                return (Error.KafkaStorageError, topic.Name, topic.Id);
            }
            _topics.TryRemove(topic.Name, out _);
            _namesById.TryRemove(topic.Id, out _);
            _partitionCount -= topic.Partitions.Length;
            ScheduleReclaim(directory, Task.WhenAll(topic.Partitions.Select(static log => log.Retire())));
            return (Error.None, topic.Name, topic.Id);
        }
    }

    private void ScheduleReclaim(string directory, Task closed)
    {
        _reclaiming.RemoveAll(static job => job.IsCompletedSuccessfully);
        _reclaiming.Add(Task.Run(async () =>
        {
            await closed;
            try
            {
                try { Directory.Delete(Path.Combine(_options.Data, directory), recursive: true); }
                catch (DirectoryNotFoundException) { }
                DurableFileSystem.FlushDirectory(_options.Data);
                lock (_gate)
                {
                    if (!_faulted)
                        Save(_state with { RetiredDirectories = _state.RetiredDirectories!
                            .Where(retired => retired != directory).ToArray() });
                }
            }
            catch (Exception e) when (DurableFileSystem.IsStorageError(e))
            {
                Console.Error.WriteLine($"SimdKafka cannot reclaim retired topic: {e.Message}");
            }
        }));
    }

    internal (Error Error, long Id) AllocateProducerId()
    {
        lock (_gate)
        {
            if (_faulted) return (Error.KafkaStorageError, -1);
            long id = checked(_state.LastProducerId + 1);
            try
            {
                Save(_state with { LastProducerId = id });
                return (Error.None, id);
            }
            catch (Exception e) when (DurableFileSystem.IsStorageError(e))
            {
                return (Error.KafkaStorageError, -1);
            }
        }
    }

    internal CommittedOffset[] GetOffsets(string group) => Volatile.Read(ref _state).Offsets.GetValueOrDefault(group) ?? [];

    internal Error AlterConfiguration(ConfigUpdate update)
    {
        lock (_gate)
        {
            if (_faulted) return Error.KafkaStorageError;
            Guid topic = Guid.Empty;
            int broker = KafkaConfigurations.DefaultBroker;
            var current = Configurations;
            if (update.Type == KafkaConfigurations.Topic)
            {
                if (Get(update.Name) is not { } existing) return Error.UnknownTopicOrPartition;
                topic = existing.Id;
                if (Tiered && current.TopicTiers.GetValueOrDefault(topic)?.Enabled is null && TopicTier(existing) is { Enabled: true } archived)
                    current = current.WithTopicTier(topic, archived);
            }
            else if (update.Type != KafkaConfigurations.Broker || !KafkaConfigurations.TryBrokerId(update.Name, out broker) ||
                broker is not (KafkaConfigurations.DefaultBroker or BrokerOptions.NodeId))
                return Error.InvalidRequest;
            var (error, next) = current.Apply(update, topic, broker, Tiered);
            if (ReferenceEquals(current, next)) return error;
            try { Save(_state with { Configurations = next }); }
            catch (Exception e) when (DurableFileSystem.IsStorageError(e)) { return Error.KafkaStorageError; }
            return Error.None;
        }
    }

    internal Error AlterScram(string user, ScramChange change)
    {
        lock (_gate)
        {
            if (_faulted) return Error.KafkaStorageError;
            var current = Scram;
            var (error, next) = current.Apply(user, change);
            if (ReferenceEquals(current, next)) return error;
            try { Save(_state with { Scram = next }); }
            catch (Exception e) when (DurableFileSystem.IsStorageError(e)) { return Error.KafkaStorageError; }
            return Error.None;
        }
    }

    internal MetadataResult ChangeToken(TokenChange change)
    {
        lock (_gate)
        {
            if (_faulted) return new(Error.KafkaStorageError);
            var current = Tokens;
            var (result, next) = current.Apply(change);
            if (ReferenceEquals(current, next)) return result;
            try { Save(_state with { Tokens = next }); }
            catch (Exception e) when (DurableFileSystem.IsStorageError(e)) { return new(Error.KafkaStorageError); }
            return result;
        }
    }

    internal Error AlterQuota(QuotaUpdate update)
    {
        lock (_gate)
        {
            if (_faulted) return Error.KafkaStorageError;
            var current = Quotas;
            var (error, next) = current.Apply(update);
            if (ReferenceEquals(current, next)) return error;
            try { Save(_state with { Quotas = next }); }
            catch (Exception e) when (DurableFileSystem.IsStorageError(e)) { return Error.KafkaStorageError; }
            return Error.None;
        }
    }

    internal MetadataResult MutateAcl(AclMutation mutation)
    {
        lock (_gate)
        {
            if (_faulted) return new(Error.KafkaStorageError);
            var current = Acls;
            var (result, next) = mutation.Apply(current);
            if (current == next) return result;
            try { Save(_state with { Acls = next }); }
            catch (Exception e) when (DurableFileSystem.IsStorageError(e)) { return new(Error.KafkaStorageError); }
            return result;
        }
    }
    internal string[] OffsetGroups() => Volatile.Read(ref _state).Offsets.Keys.ToArray();
    internal bool HasOffsets(string group) => Volatile.Read(ref _state).Offsets.ContainsKey(group);

    internal Error DeleteOffsets(string group, HashSet<(string Topic, int Partition)>? selected)
    {
        lock (_gate)
        {
            if (_faulted) return Error.KafkaStorageError;
            if (!_state.Offsets.TryGetValue(group, out var existing)) return Error.GroupIdNotFound;
            var next = selected is null ? [] : existing.Where(offset => !selected.Contains((offset.Topic, offset.Partition))).ToArray();
            if (existing.Length == next.Length) return Error.None;
            var offsets = new Dictionary<string, CommittedOffset[]>(_state.Offsets, StringComparer.Ordinal);
            if (next.Length == 0) offsets.Remove(group);
            else offsets[group] = next;
            try
            {
                Save(_state with { Offsets = offsets });
                return Error.None;
            }
            catch (Exception e) when (DurableFileSystem.IsStorageError(e))
            {
                return Error.KafkaStorageError;
            }
        }
    }


    internal Error Commit(string group, List<CommittedOffset> changes)
    {
        if (changes.Count == 0) return Error.None;
        lock (_gate)
        {
            if (_faulted) return Error.KafkaStorageError;
            foreach (var change in changes)
                if (Get(change.Topic) is not { } topic || (uint)change.Partition >= topic.Partitions.Length ||
                    (change.TopicId.HasValue && change.TopicId != topic.Id))
                    return Error.UnknownTopicOrPartition;
            var offsets = new Dictionary<string, CommittedOffset[]>(_state.Offsets, StringComparer.Ordinal);
            var updated = new Dictionary<(string, int), CommittedOffset>();
            foreach (var old in offsets.GetValueOrDefault(group) ?? []) updated[(old.Topic, old.Partition)] = old;
            foreach (var change in changes) updated[(change.Topic, change.Partition)] = change;
            offsets[group] = updated.Values.ToArray();
            try
            {
                Save(_state with { Offsets = offsets });
                return Error.None;
            }
            catch (Exception e) when (DurableFileSystem.IsStorageError(e))
            {
                return Error.KafkaStorageError;
            }
        }
    }

    private static CatalogState Load(string path)
    {
        using var document = JsonDocument.Parse(File.ReadAllBytes(path));
        JsonElement content = document.RootElement;
        if (content.ValueKind != JsonValueKind.Object)
            throw new InvalidDataException("Invalid catalog");
        if (content.TryGetProperty("Sha256", out var checksum))
        {
            if (!content.TryGetProperty("State", out content) ||
                checksum.ValueKind != JsonValueKind.String ||
                checksum.GetString() != Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(content.GetRawText()))))
                throw new InvalidDataException("Catalog checksum mismatch");
        }
        var state = content.Deserialize<CatalogState>();
        if (state is null || string.IsNullOrEmpty(state.ClusterId) || state.LastProducerId < 0 ||
            state.Topics is null || state.Offsets is null)
            throw new InvalidDataException("Invalid catalog");
        return state;
    }

    private static bool HasPartitionLogs(string directory)
    {
        foreach (string folder in Directory.EnumerateDirectories(directory))
        {
            var name = Path.GetFileName(folder);
            if (IsDiskName(name) &&
                Directory.EnumerateFiles(folder, "*.log").Any())
                return true;
        }
        return false;
    }

    private void Save(CatalogState state)
    {
        string temporary = _path + ".tmp";
        var content = JsonSerializer.SerializeToUtf8Bytes(state);
        try
        {
            var fileOptions = new FileStreamOptions { Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.None };
            if (!OperatingSystem.IsWindows() && (state.Scram is not null || state.Tokens is not null))
                fileOptions.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
            using (var file = new FileStream(temporary, fileOptions))
            {
                if (!OperatingSystem.IsWindows() && (state.Scram is not null || state.Tokens is not null))
                    File.SetUnixFileMode(file.SafeFileHandle, UnixFileMode.UserRead | UnixFileMode.UserWrite);
                using var writer = new Utf8JsonWriter(file);
                writer.WriteStartObject();
                writer.WriteString("Sha256", Convert.ToHexString(SHA256.HashData(content)));
                writer.WritePropertyName("State");
                writer.WriteRawValue(content, skipInputValidation: true);
                writer.WriteEndObject();
                writer.Flush();
                file.Flush(flushToDisk: true);
            }
            DurableFileSystem.Replace(temporary, _path);
            Volatile.Write(ref _state, state);
        }
        catch (Exception e) when (DurableFileSystem.IsStorageError(e))
        {
            // The on-disk snapshot may have advanced if the failure occurred after rename.
            _faulted = true;
            throw;
        }
    }

    private void Register(Topic topic)
    {
        if (!_namesById.TryAdd(topic.Id, topic.Name))
        {
            topic.Dispose();
            throw new InvalidDataException($"Duplicate topic ID: {topic.Id}");
        }
        _topics[topic.Name] = topic;
    }

    internal bool Tiered => _remoteTier is not null;

    internal RemoteTopicPolicy? TopicTier(Topic topic)
    {
        var policy = Configurations.TopicTiers.GetValueOrDefault(topic.Id);
        return policy?.Enabled is null && topic.Partitions.Any(static replica => replica.ArchivedOffset != 0) ?
            (policy ?? new()) with { Enabled = true } : policy;
    }

    internal async Task RunTierClockAsync(CancellationToken stop)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        long nextError = 0;
        try
        {
            while (await timer.WaitForNextTickAsync(stop))
            {
                long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                foreach (var topic in _topics.Values)
                {
                    stop.ThrowIfCancellationRequested();
                    var policy = TopicTier(topic);
                    if (policy is not { Enabled: true }) continue;
                    int segment = policy.SegmentBytes ?? _options.TierSegmentBytes;
                    long retainBytes = policy.LocalBytes ?? _options.LocalRetentionBytes;
                    long retainMs = policy.LocalMs ?? _options.LocalRetentionMs;
                    long oldestLocalTimestamp = retainMs < 0 ? -1 : now - retainMs;
                    foreach (var replica in topic.Partitions)
                    {
                        try { replica.ArchiveNext(retainBytes, segment, oldestLocalTimestamp); }
                        catch (Exception e) when (DurableFileSystem.IsStorageError(e) || e is InvalidDataException)
                        {
                            long tick = Environment.TickCount64;
                            if (tick < nextError) continue;
                            nextError = tick + 30_000;
                            Console.Error.WriteLine($"SimdKafka remote tier: {e.Message}");
                        }
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
    }

    private Topic OpenTopic(string name, int count, Guid? storedId, bool existing)
    {
        var diskKey = storedId.HasValue ? storedId.Value.ToByteArray() : Encoding.ASCII.GetBytes(name);
        string directory = Path.Combine(_options.Data, Convert.ToHexString(SHA256.HashData(diskKey)));
        var id = storedId ?? new Guid(SHA256.HashData(Encoding.UTF8.GetBytes(_state.ClusterId + "\0" + name))
            .AsSpan(0, 16), bigEndian: true);
        if (!existing) DurableFileSystem.EnsureDirectory(directory);
        var partitions = new PartitionLog[count];
        int i = 0;
        try
        {
            for (; i < count; i++)
            {
                partitions[i] = OpenPartition(directory, id, i, existing);
            }
            if (!existing) DurableFileSystem.FlushDirectory(directory);
            return new Topic(name, id, directory, partitions);
        }
        catch
        {
            for (int opened = 0; opened < i; opened++) partitions[opened].Dispose();
            throw;
        }
    }

    private PartitionLog OpenPartition(string directory, Guid id, int index, bool existing)
    {
        string path = Path.Combine(directory, $"{index}.log");
        if (existing && !File.Exists(path)) throw new InvalidDataException($"Missing partition log: {path}");
        return _remoteTier is null ? new PartitionLog(path, _flushes) :
            new PartitionLog(path, _flushes, _remoteTier, Path.Combine(id.ToString("N"), index.ToString(System.Globalization.CultureInfo.InvariantCulture)));
    }

    private static readonly System.Buffers.SearchValues<char> HexChars =
        System.Buffers.SearchValues.Create("0123456789ABCDEF");
    private static bool IsDiskName(string name) => name.Length == 64 && !name.AsSpan().ContainsAnyExcept(HexChars);
    private static readonly System.Buffers.SearchValues<char> TopicChars =
        System.Buffers.SearchValues.Create("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-");

    internal static bool ValidName(string name) => name.Length is >= 1 and <= 249 &&
        name is not "." and not ".." && !name.AsSpan().ContainsAnyExcept(TopicChars);

    public void Dispose()
    {
        _flushes?.Dispose();
        foreach (var topic in _topics.Values) topic.Dispose();
        Task.WaitAll([.._reclaiming]);
        _lease.Dispose();
    }
}
