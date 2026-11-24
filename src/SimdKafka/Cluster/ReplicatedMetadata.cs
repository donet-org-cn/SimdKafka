using System.Buffers;
using System.Collections.Immutable;
using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using DotNext.IO;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;
using DotNext.Net.Security;

namespace SimdKafka;

internal sealed record BrokerRegistration(int Id, string Host, int Port, string? Rack, IPEndPoint Control,
    int FirstReplicaPort, int ReplicaPortCount, IPEndPoint? Internal = null);
internal sealed record ReplicaMigration(Guid Id, ImmutableArray<int> Target, ImmutableArray<int> Participants);
internal sealed record PartitionAssignment(Guid Id, int Slot, ImmutableArray<int> Replicas, ImmutableArray<int> InitialReplicas,
    int LeaderId = -1, int LeaderEpoch = -1, ReplicaMigration? Migration = null);
internal sealed record ReplicatedTopic(Guid Id, string Name, ImmutableArray<PartitionAssignment> Partitions);
internal sealed record ClusterMetadataImage(long LastProducerId, int NextReplicaSlot,
    ImmutableDictionary<string, ReplicatedTopic> Topics,
    ImmutableDictionary<string, ImmutableArray<CommittedOffset>> Offsets)
{
    public ImmutableDictionary<string, ClusterTransaction> Transactions { get; init; } =
        ImmutableDictionary.Create<string, ClusterTransaction>(StringComparer.Ordinal);
    public ImmutableDictionary<string, ConsumerGroupState> ConsumerGroups { get; init; } =
        ImmutableDictionary.Create<string, ConsumerGroupState>(StringComparer.Ordinal);
    public long TopicVersion { get; init; }
    public KafkaConfigurations Configurations { get; init; } = KafkaConfigurations.Empty;
    public ImmutableArray<KafkaAcl> Acls { get; init; } = [];
    public ScramDirectory Scram { get; init; } = ScramDirectory.Empty;
    public ClientQuotas Quotas { get; init; } = ClientQuotas.Empty;
    public TokenDirectory Tokens { get; init; } = TokenDirectory.Empty;
    public ImmutableDictionary<string, ShareGroupState> ShareGroups { get; init; } =
        ImmutableDictionary.Create<string, ShareGroupState>(StringComparer.Ordinal);

    internal ImmutableDictionary<long, string> TransactionNames { get; init; } =
        ImmutableDictionary<long, string>.Empty;
    internal ImmutableHashSet<string> ActiveTransactions { get; init; } =
        ImmutableHashSet.Create<string>(StringComparer.Ordinal);

    internal ImmutableDictionary<Guid, string> TopicNames { get; init; } =
        Topics.ToImmutableDictionary(static pair => pair.Value.Id, static pair => pair.Key);

    internal ReplicatedTopic? TopicById(Guid id) =>
        TopicNames.TryGetValue(id, out var name) ? Topics.GetValueOrDefault(name) : null;
}

internal enum MetadataOperation : byte
{
    CreateTopic = 1, ExpandTopic, DeleteTopic, AllocateProducer, CommitOffsets, DeleteOffsets,
    ReportLeader, BeginReassignment, CancelReassignment, CompleteReassignment, DeleteGroup, Transaction, ConsumerGroup, AlterConfig, Acl,
    AlterScram, AlterQuota, ShareGroup, DelegationToken
}

internal sealed record MetadataCommand(MetadataOperation Operation, string Name = "", Guid TopicId = default,
    int Count = 0, int ReplicaCount = 0, ImmutableArray<Guid> PartitionIds = default,
    ImmutableArray<CommittedOffset> Offsets = default, int Partition = -1, Guid AssignmentId = default,
    Guid MigrationId = default, ImmutableArray<int> Target = default, int BrokerId = -1, int Epoch = -1,
    bool AllowReplicaCountChange = true, ImmutableArray<ImmutableArray<int>> ReplicaAssignments = default,
    TransactionCommand? Transaction = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] ConsumerGroupCommand? ConsumerGroup = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] ConfigUpdate? ConfigUpdate = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] int? TopicMessageLimit = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] bool? TopicLogAppendTime = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)] TimestampLimits TopicTimestampLimits = default,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] AclMutation? Acl = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] ScramChange? Scram = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] QuotaUpdate? Quota = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] ShareCommand? Share = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] TokenChange? Token = null);

internal readonly record struct MetadataResult(Error Error, Guid TopicId = default, long ProducerId = -1,
    short ProducerEpoch = -1, Guid TransactionId = default, ConsumerHeartbeat? Consumer = null,
    long OngoingProducerId = -1, short OngoingProducerEpoch = -1, KafkaAcl[]? RemovedAcls = null, ShareOutcome? Share = null,
    long TokenExpiry = -1);

internal sealed partial class ReplicatedMetadata : SimpleStateMachine
{
    internal const int MaxCommandBytes = 8 * 1024 * 1024;
    private const int SnapshotHashBytes = 32;
    private readonly ImmutableArray<BrokerRegistration> _members;
    private readonly int _slotCount;
    private readonly bool _reuseSlots;
    private readonly int _snapshotInterval;
    private long _snapshotIndex;
    private RaftJournal _journal = null!;
    private ClusterMetadataImage _image = new(0, 0, ImmutableDictionary.Create<string, ReplicatedTopic>(StringComparer.Ordinal),
        ImmutableDictionary.Create<string, ImmutableArray<CommittedOffset>>(StringComparer.Ordinal));

    private sealed class CommandContext(MetadataCommand command)
    {
        internal MetadataCommand Command { get; } = command;
        internal MetadataResult? Result;
    }

    private sealed record MetadataSnapshot(long Index, ClusterMetadataImage Image);

    private sealed class DurableSnapshots(IStateMachine state, string directory) : IStateMachine
    {
        public int Version => 12;
        private readonly object _gate = new();
        private long _syncedIndex;

        public ISnapshot? Snapshot
        {
            get
            {
                var snapshot = state.Snapshot;
                if (snapshot is null || snapshot.Index <= Volatile.Read(ref _syncedIndex)) return snapshot;
                lock (_gate)
                {
                    if (snapshot.Index > _syncedIndex)
                    {
                        // A snapshot must survive directory recovery before the WAL can discard earlier entries.
                        DurableFileSystem.FlushDirectory(directory);
                        Volatile.Write(ref _syncedIndex, snapshot.Index);
                    }
                }
                return snapshot;
            }
        }

        public ValueTask<long> ApplyAsync(LogEntry entry, CancellationToken stop) => state.ApplyAsync(entry, stop);

        public ValueTask ReclaimGarbageAsync(long watermark, CancellationToken stop)
        {
            _ = Snapshot;
            return state.ReclaimGarbageAsync(watermark, stop);
        }
    }

    private ReplicatedMetadata(string snapshots, ImmutableArray<BrokerRegistration> members, int snapshotInterval, bool reuseSlots)
        : base(new DirectoryInfo(snapshots))
    {
        _members = members;
        _slotCount = members.Min(static member => member.ReplicaPortCount);
        _reuseSlots = reuseSlots;
        _snapshotInterval = snapshotInterval;
    }

    internal ClusterMetadataImage Image => Volatile.Read(ref _image);
    internal LogChanges TransactionChanges { get; } = new();
    internal Action? WakeTransactions { private get; set; }
    internal bool IsLeader => _journal.IsLeader;
    internal long Term => _journal.Term;
    internal CancellationToken LeadershipToken => _journal.LeadershipToken;
    internal int LeaderId => _members.FirstOrDefault(member => member.Control.Equals(_journal.Leader))?.Id ?? -1;
    internal QuorumProgress CurrentQuorumProgress
    {
        get
        {
            long term = _journal.Term;
            int leader = LeaderId;
            long watermark = _journal.CommitIndex + 1;
            long end = Math.Max(watermark, _journal.LastIndex + 1);
            return term == _journal.Term ? new(leader, checked((int)term), watermark, end) : QuorumProgress.Unknown;
        }
    }
    internal bool HasReplicaSlots(int additions)
    {
        var image = Image;
        int assigned = image.Topics.Values.Sum(static topic => topic.Partitions.Length);
        return CanAssign(image, assigned, additions);
    }

    private bool CanAssign(ClusterMetadataImage image, int assigned, int additions) =>
        (_reuseSlots ? assigned : image.NextReplicaSlot) <= _slotCount - additions &&
        assigned <= BrokerOptions.MaxPartitions - additions;

    internal static async Task<ReplicatedMetadata> OpenAsync(string path, Guid clusterId,
        int localId, IReadOnlyCollection<BrokerRegistration> members, CancellationToken stop, SslOptions? tls = null,
        int snapshotInterval = 1024, bool reuseSlots = false)
    {
        var layout = members.OrderBy(static member => member.Id).ToImmutableArray();
        var local = layout.FirstOrDefault(member => member.Id == localId);
        if (local is null || layout.Any(static member => member.Id < 0 || string.IsNullOrWhiteSpace(member.Host) ||
            member.Host.Length > 253 || member.Port is < 1 or > 65535 || member.ReplicaPortCount < 1 ||
            member.FirstReplicaPort < 1 || member.FirstReplicaPort > 65536 - member.ReplicaPortCount ||
            member.Control.Port >= member.FirstReplicaPort && member.Control.Port - member.FirstReplicaPort < member.ReplicaPortCount) ||
            layout.DistinctBy(static member => member.Id).Count() != layout.Length ||
            layout.DistinctBy(static member => (member.Host, member.Port)).Count() != layout.Length)
            throw new ArgumentException("Invalid broker ID, address, or replica port range");
        if (snapshotInterval < 1) throw new ArgumentOutOfRangeException(nameof(snapshotInterval));
        string snapshots = Path.Combine(path, "snapshots");
        DurableFileSystem.EnsureDirectory(snapshots);
        var metadata = new ReplicatedMetadata(snapshots, layout, snapshotInterval, reuseSlots);
        try
        {
            await metadata.RestoreAsync(stop);
            metadata._journal = await RaftJournal.OpenAsync(path, clusterId, local.Control,
                layout.Select(static member => member.Control).ToArray(), new DurableSnapshots(metadata, snapshots), stop, tls,
                reuseSlots: reuseSlots, validateLayout: () => metadata.VerifyReplicaLayout(path));
            metadata._transactionWriter = metadata.ReplicateTransactionsAsync(metadata._transactionStop.Token);
            return metadata;
        }
        catch { await metadata.DisposeAsync(); throw; }
    }

    private void VerifyReplicaLayout(string path)
    {
        // Historical CreateTopics commands replay their placement from these inputs.
        byte[] fingerprint = SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(new
        {
            SlotCount = _slotCount,
            Brokers = _members.Select(static member => new { member.Id, member.Rack, member.FirstReplicaPort }).ToArray()
        }));
        string manifest = Path.Combine(path, "replica-layout.sha256");
        if (File.Exists(manifest))
        {
            if (!CryptographicOperations.FixedTimeEquals(File.ReadAllBytes(manifest), fingerprint))
                throw new InvalidDataException("Configured replica layout differs from the controller journal layout");
            return;
        }
        string temporary = manifest + ".tmp";
        using (var file = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            file.Write(fingerprint);
            file.Flush(flushToDisk: true);
        }
        DurableFileSystem.Replace(temporary, manifest);
    }

    internal async ValueTask<MetadataResult> ExecuteAsync(MetadataCommand command, CancellationToken stop)
    {
        command = Normalize(command);
        var bytes = JsonSerializer.SerializeToUtf8Bytes(command);
        if (bytes.Length > MaxCommandBytes) return new(Error.MessageTooLarge);
        if (command.Operation == MetadataOperation.Transaction)
            return await QueueTransactionAsync(command, bytes, stop);
        var context = new CommandContext(command);
        try { await _journal.ReplicateAsync(bytes, context, stop); }
        catch (NotLeaderException) { return new(Error.NotController); }
        return context.Result ?? throw new InvalidOperationException("Committed metadata command was not applied locally");
    }

    internal ValueTask ReadBarrierAsync(CancellationToken stop) => _journal.ReadBarrierAsync(stop);
    internal Error ValidateConfiguration(MetadataCommand command) => ApplyConfig(Image, command).Item1.Error;

    internal static MetadataCommand Normalize(MetadataCommand command) => command with
    {
        PartitionIds = command.PartitionIds.IsDefault ? [] : command.PartitionIds,
        Offsets = command.Offsets.IsDefault ? [] : command.Offsets,
        Target = command.Target.IsDefault ? [] : command.Target,
        ReplicaAssignments = command.ReplicaAssignments.IsDefault ? [] : command.ReplicaAssignments
    };

    protected override async ValueTask PersistAsync(IAsyncBinaryWriter writer, CancellationToken stop)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(new MetadataSnapshot(_snapshotIndex, Image));
        await writer.WriteAsync(SHA256.HashData(payload), null, stop);
        await writer.WriteAsync(payload, null, stop);
    }

    protected override async ValueTask RestoreAsync(FileInfo snapshotFile, CancellationToken stop)
    {
        await using var file = snapshotFile.OpenRead();
        if (file.Length <= SnapshotHashBytes)
            throw new InvalidDataException("Invalid controller snapshot length");
        byte[] hash = new byte[SnapshotHashBytes];
        await file.ReadExactlyAsync(hash, stop);
        byte[] computed = await SHA256.HashDataAsync(file, stop);
        if (!CryptographicOperations.FixedTimeEquals(hash, computed))
            throw new InvalidDataException("Controller snapshot failed SHA-256 verification");
        file.Position = SnapshotHashBytes;
        var restored = await JsonSerializer.DeserializeAsync<MetadataSnapshot>(file, cancellationToken: stop) ??
            throw new InvalidDataException("Invalid controller snapshot");
        int separator = snapshotFile.Name.IndexOf('-');
        if (restored.Index <= 0 || restored.Image is null || restored.Image.NextReplicaSlot < 0 ||
            restored.Image.NextReplicaSlot > _slotCount || (_reuseSlots && restored.Image.NextReplicaSlot == _slotCount) ||
            separator < 1 ||
            !long.TryParse(snapshotFile.Name.AsSpan(0, separator), NumberStyles.None, CultureInfo.InvariantCulture, out long index) ||
            index != restored.Index)
            throw new InvalidDataException("Invalid controller snapshot metadata");
        if (_reuseSlots)
        {
            var slots = new HashSet<int>();
            if (restored.Image.Topics.Values.SelectMany(static topic => topic.Partitions)
                .Any(assignment => (uint)assignment.Slot >= _slotCount || !slots.Add(assignment.Slot)))
                throw new InvalidDataException("Controller snapshot contains invalid or overlapping replica slots");
        }
        _snapshotIndex = restored.Index;
        Volatile.Write(ref _image, restored.Image with
        {
            TransactionNames = restored.Image.Transactions.ToImmutableDictionary(static pair => pair.Value.ProducerId,
                static pair => pair.Key),
            ActiveTransactions = restored.Image.Transactions.Where(static pair => pair.Value.Active)
                .Select(static pair => pair.Key).ToImmutableHashSet(StringComparer.Ordinal)
        });
    }

    protected override ValueTask<bool> ApplyAsync(LogEntry entry, CancellationToken token)
    {
        if (entry.Length == 0 || entry.IsConfiguration) return ValueTask.FromResult(false);
        if (!entry.TryGetPayload(out var payload) || payload.Length > MaxTransactionEntryBytes)
            throw new InvalidDataException("Invalid replicated metadata command length");
        long index = checked(entry.Index * MaxTransactionCommands);
        if (entry.Context is PendingTransaction[] transactions)
        {
            foreach (var transaction in transactions)
                transaction.Result = Apply(transaction.Command, index++);
        }
        else if (IsTransactionGroup(payload)) ApplyTransactionGroup(payload, index);
        else
        {
            var context = entry.Context as CommandContext;
            var command = context?.Command ?? DecodeMetadata(payload);
            var result = Apply(command, index);
            if (context is not null) context.Result = result;
        }
        bool snapshot = entry.Index - _snapshotIndex >= _snapshotInterval;
        if (snapshot) _snapshotIndex = entry.Index;
        return ValueTask.FromResult(snapshot);
    }

    private MetadataResult Apply(MetadataCommand command, long index)
    {
        var image = _image;
        var (result, next) = command.Operation switch
        {
            MetadataOperation.CreateTopic or MetadataOperation.ExpandTopic => AssignPartitions(image, command),
            MetadataOperation.DeleteTopic => DeleteTopic(image, command),
            MetadataOperation.AllocateProducer => AllocateProducer(image),
            MetadataOperation.CommitOffsets => CommitOffsets(image, command, index),
            MetadataOperation.DeleteOffsets or MetadataOperation.DeleteGroup => DeleteOffsets(image, command),
            MetadataOperation.ReportLeader or MetadataOperation.BeginReassignment or MetadataOperation.CancelReassignment or
                MetadataOperation.CompleteReassignment => UpdateAssignment(image, command),
            MetadataOperation.Transaction => ApplyTransaction(image, command, index),
            MetadataOperation.ConsumerGroup => ApplyConsumerGroup(image, command.Name, command.ConsumerGroup ??
                throw new InvalidDataException("Missing consumer group command")),
            MetadataOperation.AlterConfig => ApplyConfig(image, command),
            MetadataOperation.Acl => ApplyAcl(image, command.Acl ?? throw new InvalidDataException("Missing ACL command")),
            MetadataOperation.AlterScram => ApplyScram(image, command.Name, command.Scram ?? throw new InvalidDataException("Missing SCRAM command")),
            MetadataOperation.AlterQuota => ApplyQuota(image, command.Quota ?? throw new InvalidDataException("Missing client quota command")),
            MetadataOperation.ShareGroup => ShareGroups.Apply(image, command.Name, command.Share ?? throw new InvalidDataException("Missing share group command")),
            MetadataOperation.DelegationToken => ApplyToken(image, command.Token ?? throw new InvalidDataException("Missing delegation token command")),
            _ => throw new InvalidDataException("Unknown replicated metadata operation")
        };
        if (!ReferenceEquals(image, next))
        {
            Volatile.Write(ref _image, next);
            if (command.Operation == MetadataOperation.Transaction)
            {
                TransactionChanges.Signal();
                if (next.Transactions.GetValueOrDefault(command.Name)?.Preparing == true &&
                    image.Transactions.GetValueOrDefault(command.Name)?.Preparing != true) WakeTransactions?.Invoke();
            }
        }
        return result;
    }

    private static (MetadataResult, ClusterMetadataImage) ApplyAcl(ClusterMetadataImage image, AclMutation change)
    {
        var (result, next) = change.Apply(image.Acls);
        return (result, next == image.Acls ? image : image with { Acls = next, TopicVersion = checked(image.TopicVersion + 1) });
    }

    private static (MetadataResult, ClusterMetadataImage) ApplyToken(ClusterMetadataImage image, TokenChange change)
    {
        var (result, next) = image.Tokens.Apply(change);
        return (result, ReferenceEquals(next, image.Tokens) ? image : image with { Tokens = next });
    }

    private static (MetadataResult, ClusterMetadataImage) ApplyScram(ClusterMetadataImage image, string user, ScramChange change)
    {
        var (error, next) = image.Scram.Apply(user, change);
        return (new(error), ReferenceEquals(next, image.Scram) ? image : image with { Scram = next });
    }

    private static (MetadataResult, ClusterMetadataImage) ApplyQuota(ClusterMetadataImage image, QuotaUpdate update)
    {
        var (error, next) = image.Quotas.Apply(update);
        return (new(error), ReferenceEquals(next, image.Quotas) ? image : image with { Quotas = next });
    }

    private (MetadataResult, ClusterMetadataImage) ApplyConfig(ClusterMetadataImage image, MetadataCommand command)
    {
        var update = command.ConfigUpdate ?? throw new InvalidDataException("Missing configuration update");
        int broker = KafkaConfigurations.DefaultBroker;
        if (update.Type == KafkaConfigurations.Topic)
        {
            if (!image.Topics.TryGetValue(update.Name, out var topic) || topic.Id != command.TopicId)
                return (new(Error.UnknownTopicOrPartition), image);
        }
        else if (update.Type != KafkaConfigurations.Broker || !KafkaConfigurations.TryBrokerId(update.Name, out broker) ||
            broker != KafkaConfigurations.DefaultBroker && !_members.Any(member => member.Id == broker))
            return (new(Error.InvalidRequest), image);
        var (error, next) = image.Configurations.Apply(update, command.TopicId, broker);
        return (new(error), ReferenceEquals(next, image.Configurations) ? image : image with { Configurations = next });
    }

    private (MetadataResult Result, ClusterMetadataImage Image) AssignPartitions(ClusterMetadataImage image, MetadataCommand command)
    {
        if (!Catalog.ValidName(command.Name) || command.TopicId == Guid.Empty) return (new(Error.InvalidTopic), image);
        var existing = image.Topics.GetValueOrDefault(command.Name);
        bool creating = command.Operation == MetadataOperation.CreateTopic;
        if (creating && command.TopicMessageLimit is < 0 or > BrokerOptions.MaxBatchBytes)
            return (new(Error.InvalidConfig), image);
        if (creating && existing is not null) return (new(Error.TopicAlreadyExists, existing.Id), image);
        if (!creating && existing?.Id != command.TopicId) return (new(Error.UnknownTopicOrPartition), image);
        int count = existing?.Partitions.Length ?? 0;
        if (command.Count <= count || command.Count > BrokerOptions.MaxPartitionsPerTopic ||
            command.PartitionIds.IsDefault || command.PartitionIds.Length != command.Count - count)
            return (new(Error.InvalidPartitions), image);
        int replicas = existing?.Partitions[0].Replicas.Length ?? command.ReplicaCount;
        if (replicas < 1 || replicas > _members.Length) return (new(Error.InvalidReplicationFactor), image);
        bool explicitAssignment = !command.ReplicaAssignments.IsDefaultOrEmpty;
        if (explicitAssignment && (command.ReplicaAssignments.Length != command.Count - count ||
            !ValidReplicaAssignments(command.ReplicaAssignments, replicas, _members)))
            return (new(Error.InvalidReplicaAssignment), image);

        var allAssignments = image.Topics.Values.SelectMany(static topic => topic.Partitions).ToArray();
        var used = new HashSet<Guid>(allAssignments.Select(static assignment => assignment.Id));
        if (image.TopicNames.TryGetValue(command.TopicId, out var previousName) && previousName != command.Name ||
            command.PartitionIds.Any(id => id == Guid.Empty || !used.Add(id))) return (new(Error.InvalidRequest), image);
        int additions = command.Count - count;
        if (!CanAssign(image, allAssignments.Length, additions))
            return (new(Error.InvalidPartitions), image);
        var partitions = ImmutableArray.CreateBuilder<PartitionAssignment>(command.Count);
        if (existing is not null) partitions.AddRange(existing.Partitions);
        // Reuse requires Raft peer connections to present the group's unique TLS server name.
        var occupied = _reuseSlots ? allAssignments.Select(static assignment => assignment.Slot).ToHashSet() : null;
        int slot = image.NextReplicaSlot;
        for (int p = 0; p < command.PartitionIds.Length; p++)
        {
            if (occupied is not null)
            {
                while (!occupied.Add(slot)) slot = (slot + 1) % _slotCount;
            }
            var assigned = explicitAssignment ? command.ReplicaAssignments[p] : PlaceReplicas(slot, replicas);
            partitions.Add(new(command.PartitionIds[p], slot, assigned, assigned));
            slot = _reuseSlots ? (slot + 1) % _slotCount : slot + 1;
        }
        var topic = new ReplicatedTopic(command.TopicId, command.Name, partitions.MoveToImmutable());
        var configs = image.Configurations;
        if (creating && command.TopicMessageLimit.HasValue) configs = configs.WithTopicLimit(topic.Id, command.TopicMessageLimit);
        if (creating && (command.TopicLogAppendTime.HasValue || !command.TopicTimestampLimits.IsEmpty))
            configs = configs.WithTopicTimestamp(topic.Id, command.TopicLogAppendTime, command.TopicTimestampLimits);
        return (new(Error.None, topic.Id), image with
        {
            NextReplicaSlot = slot,
            TopicVersion = checked(image.TopicVersion + 1),
            Topics = image.Topics.SetItem(topic.Name, topic),
            TopicNames = image.TopicNames.SetItem(topic.Id, topic.Name),
            Configurations = configs
        });
    }

    internal static bool ValidReplicaAssignments(ImmutableArray<ImmutableArray<int>> assignments, int count,
        IReadOnlyCollection<BrokerRegistration> members) => assignments.All(assigned =>
        !assigned.IsDefaultOrEmpty && assigned.Length == count && assigned.Distinct().Count() == count &&
        assigned.All(id => members.Any(member => member.Id == id)));

    private ImmutableArray<int> PlaceReplicas(int slot, int replicas)
    {
        int first = slot % _members.Length;
        var members = ImmutableArray.CreateBuilder<int>(replicas);
        var leader = _members[first];
        members.Add(leader.Id);
        if (replicas == 1) return members.MoveToImmutable();
        var racks = new HashSet<string>(StringComparer.Ordinal);
        if (leader.Rack is not null) racks.Add(leader.Rack);
        for (int i = 1; i < _members.Length && members.Count < replicas; i++)
        {
            var member = _members[(first + i) % _members.Length];
            if (member.Rack is not null && racks.Add(member.Rack)) members.Add(member.Id);
        }
        for (int i = 1; members.Count < replicas; i++)
        {
            int id = _members[(first + i) % _members.Length].Id;
            if (!members.Contains(id)) members.Add(id);
        }
        return members.MoveToImmutable();
    }

    private (MetadataResult, ClusterMetadataImage) UpdateAssignment(ClusterMetadataImage image, MetadataCommand command)
    {
        if (!image.Topics.TryGetValue(command.Name, out var topic) || topic.Id != command.TopicId ||
            (uint)command.Partition >= topic.Partitions.Length) return (new(Error.UnknownTopicOrPartition), image);
        var current = topic.Partitions[command.Partition];
        if (current.Id != command.AssignmentId) return (new(Error.UnknownTopicOrPartition), image);
        PartitionAssignment next;
        switch (command.Operation)
        {
            case MetadataOperation.ReportLeader:
                var participants = current.Migration?.Participants ?? current.Replicas;
                if (!participants.Contains(command.BrokerId) || command.Epoch < 0) return (new(Error.InvalidRequest), image);
                if (command.Epoch <= current.LeaderEpoch) return (new(Error.None), image);
                next = current with { LeaderId = command.BrokerId, LeaderEpoch = command.Epoch };
                break;
            case MetadataOperation.BeginReassignment:
                if (command.MigrationId == Guid.Empty || command.Target.IsDefaultOrEmpty ||
                    command.Target.Distinct().Count() != command.Target.Length ||
                    command.Target.Any(id => !_members.Any(member => member.Id == id)))
                    return (new(Error.InvalidReplicaAssignment), image);
                if (!command.AllowReplicaCountChange && command.Target.Length != current.Replicas.Length)
                    return (new(Error.InvalidReplicationFactor), image);
                if (current.Migration is { } active)
                    return (new(active.Target.SequenceEqual(command.Target) ? Error.None : Error.ReassignmentInProgress), image);
                if (current.Replicas.SequenceEqual(command.Target)) return (new(Error.None), image);
                next = current with { Migration = new(command.MigrationId, command.Target,
                    current.Replicas.Union(command.Target).ToImmutableArray()) };
                break;
            case MetadataOperation.CancelReassignment:
                if (current.Migration is not { } pending) return (new(Error.NoReassignmentInProgress), image);
                next = current with { Migration = new(command.MigrationId, current.Replicas, pending.Participants) };
                break;
            case MetadataOperation.CompleteReassignment:
                if (current.Migration is not { } finished || finished.Id != command.MigrationId)
                    return (new(Error.NoReassignmentInProgress), image);
                if (!finished.Target.Contains(command.BrokerId) || command.Epoch < current.LeaderEpoch)
                    return (new(Error.NotLeaderOrFollower), image);
                next = current with { Replicas = finished.Target, Migration = null, LeaderId = command.BrokerId, LeaderEpoch = command.Epoch };
                break;
            default:
                throw new InvalidOperationException("Expected a partition metadata operation");
        }
        var updated = topic with { Partitions = topic.Partitions.SetItem(command.Partition, next) };
        return (new(Error.None, topic.Id), image with { Topics = image.Topics.SetItem(topic.Name, updated) });
    }

    private static (MetadataResult, ClusterMetadataImage) DeleteTopic(ClusterMetadataImage image, MetadataCommand command)
    {
        if (!image.Topics.TryGetValue(command.Name, out var topic) || topic.Id != command.TopicId)
            return (new(Error.UnknownTopicOrPartition), image);
        var offsets = image.Offsets.ToBuilder();
        foreach (var (group, values) in image.Offsets)
        {
            var remaining = values.Where(offset => offset.TopicId != topic.Id).ToImmutableArray();
            if (remaining.Length == 0) offsets.Remove(group);
            else if (remaining.Length != values.Length) offsets[group] = remaining;
        }
        string sharePrefix = $"{topic.Id:N}:";
        var shares = image.ShareGroups.ToBuilder();
        foreach (var (group, state) in image.ShareGroups)
        {
            var partitions = state.Partitions.RemoveRange(state.Partitions.Keys.Where(key => key.StartsWith(sharePrefix, StringComparison.Ordinal)));
            if (!ReferenceEquals(partitions, state.Partitions)) shares[group] = state with { Partitions = partitions };
        }
        return (new(Error.None, topic.Id), image with
        {
            TopicVersion = checked(image.TopicVersion + 1),
            Topics = image.Topics.Remove(topic.Name),
            TopicNames = image.TopicNames.Remove(topic.Id),
            Configurations = image.Configurations.WithTopicLimit(topic.Id, null).WithTopicTimestamp(topic.Id, null),
            Offsets = offsets.ToImmutable(),
            ShareGroups = shares.ToImmutable()
        });
    }

    private static (MetadataResult, ClusterMetadataImage) AllocateProducer(ClusterMetadataImage image)
    {
        long id = checked(image.LastProducerId + 1);
        return (new(Error.None, ProducerId: id), image with { LastProducerId = id });
    }

    private static (MetadataResult, ClusterMetadataImage) CommitOffsets(ClusterMetadataImage image, MetadataCommand command, long index)
    {
        if (string.IsNullOrEmpty(command.Name)) return (new(Error.InvalidGroupId), image);
        if (image.ShareGroups.ContainsKey(command.Name)) return (new(Error.InconsistentGroupProtocol), image);
        if (command.Offsets.IsDefaultOrEmpty) return (new(Error.None), image);
        foreach (var offset in command.Offsets)
        {
            if (!image.Topics.TryGetValue(offset.Topic, out var topic) || topic.Id != offset.TopicId ||
                (uint)offset.Partition >= topic.Partitions.Length) return (new(Error.UnknownTopicOrPartition), image);
            if (offset.Offset < 0) return (new(Error.InvalidRequest), image);
        }
        return (new(Error.None), MergeCommittedOffsets(image, command.Name, command.Offsets, index));
    }

    private static ClusterMetadataImage MergeCommittedOffsets(ClusterMetadataImage image, string group,
        IEnumerable<CommittedOffset> additions, long? index = null)
    {
        var offsets = new Dictionary<(string, int), CommittedOffset>();
        if (image.Offsets.TryGetValue(group, out var existing))
            foreach (var offset in existing) offsets[(offset.Topic, offset.Partition)] = offset;
        foreach (var offset in additions)
        {
            var pending = index is long committed ? offset with { CommitIndex = committed } : offset;
            var key = (offset.Topic, offset.Partition);
            if (!offsets.TryGetValue(key, out var previous) || pending.CommitIndex >= previous.CommitIndex)
                offsets[key] = pending;
        }
        return offsets.Count == 0 ? image : image with { Offsets = image.Offsets.SetItem(group, offsets.Values.ToImmutableArray()) };
    }

    private static (MetadataResult, ClusterMetadataImage) DeleteOffsets(ClusterMetadataImage image, MetadataCommand command)
    {
        if (string.IsNullOrEmpty(command.Name)) return (new(Error.InvalidGroupId), image);
        var consumer = image.ConsumerGroups.GetValueOrDefault(command.Name);
        if (consumer is not null)
        {
            if (command.Operation == MetadataOperation.DeleteGroup && !consumer.Members.IsEmpty)
                return (new(Error.NonEmptyGroup), image);
            if (command.Operation == MetadataOperation.DeleteOffsets && consumer.Members.Values.Any(member =>
            {
                var pattern = member.Pattern is { } source ? ConsumerGroupPattern.Compile(source) : null;
                return command.Offsets.Any(offset => member.SubscribedTo(offset.Topic, pattern));
            }))
                return (new(Error.GroupSubscribedToTopic), image);
        }
        if (image.ActiveTransactions.Any(name => image.Transactions[name].Groups.Contains(command.Name)))
            return (new(Error.UnstableOffsetCommit), image);
        if (command.Operation == MetadataOperation.DeleteGroup && consumer is not null)
            return (new(Error.None), image with
            {
                ConsumerGroups = image.ConsumerGroups.Remove(command.Name),
                Offsets = image.Offsets.Remove(command.Name)
            });
        if (!image.Offsets.TryGetValue(command.Name, out var existing)) return (new(Error.GroupIdNotFound), image);
        if (command.Operation == MetadataOperation.DeleteGroup)
            return (new(Error.None), image with { Offsets = image.Offsets.Remove(command.Name) });
        if (command.Offsets.IsDefaultOrEmpty) return (new(Error.None), image);
        var selected = command.Offsets.Select(static offset => (offset.TopicId, offset.Partition)).ToHashSet();
        var remaining = existing.Where(offset => !selected.Contains((offset.TopicId, offset.Partition))).ToImmutableArray();
        var offsets = remaining.IsEmpty ? image.Offsets.Remove(command.Name) : image.Offsets.SetItem(command.Name, remaining);
        return (new(Error.None), image with { Offsets = offsets });
    }

    public override async ValueTask DisposeAsync()
    {
        _pendingTransactions.Writer.TryComplete();
        await _transactionStop.CancelAsync();
        try { await _transactionWriter; }
        finally
        {
            _transactionStop.Dispose();
            try { if (_journal is not null) await _journal.DisposeAsync(); }
            finally { await base.DisposeAsync(); }
        }
    }
}
