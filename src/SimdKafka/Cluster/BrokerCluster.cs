using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Security;

namespace SimdKafka;

internal sealed partial class BrokerCluster : IAsyncDisposable
{
    private readonly BrokerOptions _options;
    private readonly SslOptions? _tls;
    private readonly bool _reuseSlots;
    private readonly ControllerRpc _rpc;
    private readonly FileStream _lease;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentDictionary<Guid, ReplicatedPartition> _localPartitions = new();
    private readonly ConcurrentDictionary<Guid, long> _replicaStorageFailures = new();
    private readonly ConcurrentDictionary<(Guid Partition, int Broker), (ReplicaProgress Sample, long Deadline)> _progress = new();
    private readonly ConcurrentDictionary<(Guid Partition, int Broker), byte> _probes = new();
    private readonly Task _reconcile;
    private readonly Task _reclaim;
    private readonly Task _transactions;
    private readonly Task _transactionIdCleanup;

    internal BrokerRegistration Local { get; }
    internal BrokerRegistration[] Members { get; }
    internal string ClusterId { get; }
    internal ReplicatedMetadata Metadata { get; }
    internal LogChanges Changes { get; } = new();
    internal ReplicatedPartition? FindReplica(Guid assignmentId) => _localPartitions.GetValueOrDefault(assignmentId);
    internal bool ReplicaStorageFailed(Guid assignmentId) => _replicaStorageFailures.ContainsKey(assignmentId);

    private BrokerCluster(BrokerOptions options, Guid clusterId, BrokerRegistration local, BrokerRegistration[] members,
        ReplicatedMetadata metadata, SslOptions? tls, ControllerRpc rpc, FileStream lease, bool reuseSlots)
    {
        _options = options;
        _tls = tls;
        _reuseSlots = reuseSlots;
        _rpc = rpc;
        _lease = lease;
        Local = local;
        Members = members;
        Metadata = metadata;
        ClusterId = Convert.ToBase64String(clusterId.ToByteArray(bigEndian: true)).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        rpc.Probe = assignment => FindReplica(assignment) is { } replica && replica.Leader is not null ?
            new ReplicaProgress(replica.LeaderEpoch, replica.HighWatermark, replica.LogStartOffset) : new ReplicaProgress(-1, -1);
        rpc.MarkTransaction = ApplyTransactionMarkerAsync;
        Metadata.WakeTransactions = () => _transactionWork.Writer.TryWrite(0);
        _reconcile = ReconcileAsync(_stop.Token);
        _reclaim = ReclaimDeletedTopicsAsync(_stop.Token);
        _transactions = RecoverTransactionsAsync(_stop.Token);
        _transactionIdCleanup = ExpireTransactionIdsAsync(_stop.Token);
    }

    internal static async Task<BrokerCluster> OpenAsync(BrokerOptions options, string configuration, int nodeId,
        CancellationToken stop, SslOptions? tls = null)
    {
        using var file = File.OpenRead(configuration);
        if (file.Length > 1024 * 1024) throw new InvalidDataException("Cluster configuration exceeds 1 MiB");
        var config = await JsonSerializer.DeserializeAsync<ClusterConfiguration>(file, JsonSerializerOptions.Web, stop) ??
            throw new InvalidDataException("Missing cluster configuration");
        if (config.RecycleReplicaPorts && tls is null)
            throw new ArgumentException("Replica slot reuse requires mutual cluster TLS");
        if (config.ClientAclRequired != (options.AclSuperUsers is { Length: > 0 }))
            throw new ArgumentException("clientAclRequired and --acl-super-user must be enabled together on every broker");
        if (config.ClientAclRequired && tls is null)
            throw new ArgumentException("Cluster ACLs require mutual TLS for internal traffic");
        if (config.DynamicScram != options.DynamicScram)
            throw new ArgumentException("dynamicScram and --sasl-dynamic-scram must be enabled together on every broker");
        if (config.DelegationTokens != (options.DelegationTokenSecretFile is not null) || config.DelegationTokens && tls is null)
            throw new ArgumentException("delegationTokens requires --delegation-token-secret-file and mutual cluster TLS on every broker");
        if (config.Brokers is not { Length: > 0 } || config.ClusterId == Guid.Empty)
            throw new InvalidDataException("Cluster configuration requires a cluster ID and brokers");
        var brokers = config.Brokers.Select(static broker => new BrokerRegistration(broker.Id, broker.Host, broker.Port,
            broker.Rack, IPEndPoint.Parse(broker.Control), broker.FirstReplicaPort, broker.ReplicaPortCount,
            IPEndPoint.Parse(broker.Internal))).ToArray();
        var local = brokers.FirstOrDefault(member => member.Id == nodeId);
        if (local is null || local.Port != options.Port || !string.Equals(local.Host, options.Host, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("Local Kafka advertised address must match the cluster broker registration");
        if (!options.Flush) throw new ArgumentException("A clustered broker requires durable replication");
        ValidateListeners(brokers, tls);
        DurableFileSystem.EnsureDirectory(options.Data, ownerOnly: options.DynamicScram || config.DelegationTokens);
        var lease = new FileStream(Path.Combine(options.Data, "broker.lock"), FileMode.OpenOrCreate,
            FileAccess.ReadWrite, FileShare.None);
        ReplicatedMetadata? metadata = null;
        try
        {
            if (File.Exists(Path.Combine(options.Data, "catalog.json")))
                throw new ArgumentException("A clustered broker must use a directory separate from its standalone catalog");
            string controlPath = Path.Combine(options.Data, "control");
            if (options.DynamicScram || config.DelegationTokens) DurableFileSystem.EnsureDirectory(controlPath, ownerOnly: true);
            metadata = await ReplicatedMetadata.OpenAsync(controlPath, config.ClusterId,
                nodeId, brokers, stop, tls, reuseSlots: config.RecycleReplicaPorts);
            if (!config.ClientAclRequired && !metadata.Image.Acls.IsEmpty)
                throw new InvalidDataException("Persisted cluster ACLs require clientAclRequired and an ACL superuser");
            var rpc = new ControllerRpc(local.Internal!, config.ClusterId, metadata, tls, config.DelegationTokens);
            return new BrokerCluster(options, config.ClusterId, local, brokers, metadata, tls, rpc, lease, config.RecycleReplicaPorts);
        }
        catch
        {
            try { if (metadata is not null) await metadata.DisposeAsync(); }
            finally { lease.Dispose(); }
            throw;
        }
    }

    private static void ValidateListeners(IEnumerable<BrokerRegistration> brokers, SslOptions? tls)
    {
        var listeners = new Dictionary<IPAddress, List<(int First, int Last)>>();
        foreach (var broker in brokers)
        {
            if (broker.Internal is not { Port: > 0 } rpc || broker.Control.Port == 0 ||
                broker.ReplicaPortCount <= 0 || broker.FirstReplicaPort < 1 ||
                broker.FirstReplicaPort > 65536 - broker.ReplicaPortCount)
                throw new ArgumentException("Invalid cluster listener port range");
            if (tls is null && (!IPAddress.IsLoopback(rpc.Address) || !IPAddress.IsLoopback(broker.Control.Address)))
                throw new ArgumentException("Cluster networking outside loopback requires mutual TLS");
            Add(broker.Control.Address, broker.Control.Port, broker.Control.Port);
            Add(rpc.Address, rpc.Port, rpc.Port);
            Add(broker.Control.Address, broker.FirstReplicaPort, broker.FirstReplicaPort + broker.ReplicaPortCount - 1);
            if (IPAddress.TryParse(broker.Host, out var advertised)) Add(advertised, broker.Port, broker.Port);
        }

        void Add(IPAddress address, int first, int last)
        {
            if (!listeners.TryGetValue(address, out var existing)) listeners.Add(address, existing = []);
            if (existing.Any(range => range.First <= last && first <= range.Last))
                throw new ArgumentException("Kafka, controller, internal and replica listeners must not overlap");
            existing.Add((first, last));
        }
    }

    internal ValueTask<MetadataResult> ExecuteMetadataAsync(MetadataCommand command, CancellationToken stop)
    {
        if (Metadata.IsLeader) return Metadata.ExecuteAsync(command, stop);
        int leader = Metadata.LeaderId;
        var remote = Members.FirstOrDefault(member => member.Id == leader);
        return remote?.Internal is { } address ? _rpc.SendAsync(address, command, stop) :
            ValueTask.FromResult(new MetadataResult(Error.NotController));
    }

    internal async ValueTask<(QuorumProgress Leader, QuorumProgress[] Voters)> ReadQuorumProgressAsync(CancellationToken stop)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(stop);
        deadline.CancelAfter(TimeSpan.FromMilliseconds(1500));
        var voters = await Task.WhenAll(Members.Select(Sample));
        var local = Metadata.CurrentQuorumProgress;
        int leaderIndex = Array.FindIndex(Members, member => member.Id == local.Leader);
        if (leaderIndex < 0) return (QuorumProgress.Unknown, voters);
        var leader = Members[leaderIndex].Id == Local.Id ? local : voters[leaderIndex];
        return (leader.Leader == local.Leader && leader.Epoch == local.Epoch ? leader : QuorumProgress.Unknown, voters);

        async Task<QuorumProgress> Sample(BrokerRegistration broker)
        {
            if (broker.Id == Local.Id) return Metadata.CurrentQuorumProgress;
            try { return await _rpc.ProbeQuorumAsync(broker.Internal!, deadline.Token); }
            catch (OperationCanceledException) when (!stop.IsCancellationRequested) { return QuorumProgress.Unknown; }
        }
    }

    internal async Task ServeKafkaAsync(CancellationToken stop)
    {
        using var shutdown = CancellationTokenSource.CreateLinkedTokenSource(stop);
        var service = new Broker(_options, new ClusterRouter(_options, this)).RunAsync(shutdown.Token);
        try { await await Task.WhenAny(service, _reconcile, _transactions, _transactionIdCleanup); }
        finally
        {
            await shutdown.CancelAsync();
            await service;
        }
    }

    internal (int LeaderId, int Epoch) FindLeader(PartitionAssignment assignment)
    {
        var replica = FindReplica(assignment.Id);
        if (replica?.Leader is not IPEndPoint address) return (assignment.LeaderId, assignment.LeaderEpoch);
        foreach (var member in Members)
        {
            if (member.Control.Address.Equals(address.Address) && member.FirstReplicaPort + assignment.Slot == address.Port)
                return (member.Id, replica.LeaderEpoch);
        }
        return (assignment.LeaderId, assignment.LeaderEpoch);
    }

    internal int PreferredReplica(PartitionAssignment assignment, ReplicatedPartition replica, string rack, long offset)
    {
        if (rack.Length == 0 || string.Equals(Local.Rack, rack, StringComparison.Ordinal) || !replica.IsLeader) return -1;
        foreach (var member in Members)
        {
            if (!string.Equals(member.Rack, rack, StringComparison.Ordinal) || !assignment.Replicas.Contains(member.Id)) continue;
            var key = (assignment.Id, member.Id);
            if (_progress.TryGetValue(key, out var state) && state.Deadline > Environment.TickCount64)
            {
                if (state.Sample.Epoch == replica.LeaderEpoch && state.Sample.HighWatermark >= offset &&
                    (replica.LogStartOffset == 0 || state.Sample.LogStartOffset >= replica.LogStartOffset)) return member.Id;
            }
            else _probes.TryAdd(key, 0);
        }
        return -1;
    }

    private async Task ReconcileAsync(CancellationToken stop)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        var current = Metadata.Image.Topics;
        var jobs = new Dictionary<Guid, Task>();
        var completed = new List<Guid>();
        bool retry = true;
        try
        {
            do
            {
                foreach (var (id, job) in jobs)
                    if (job.IsCompleted)
                    {
                        await job;
                        completed.Add(id);
                    }
                foreach (Guid id in completed) jobs.Remove(id);
                completed.Clear();

                var topics = Metadata.Image.Topics;
                if (retry || !ReferenceEquals(current, topics))
                {
                    current = topics;
                    retry = await ReconcileLocalReplicasAsync(topics, jobs, stop);
                }
                foreach (var topic in topics.Values)
                    for (int p = 0; p < topic.Partitions.Length; p++)
                    {
                        var assignment = topic.Partitions[p];
                        if (!jobs.ContainsKey(assignment.Id) && FindReplica(assignment.Id) is { IsLeader: true } replica &&
                            (assignment.LeaderEpoch < replica.LeaderEpoch || assignment.Migration is not null))
                            jobs.Add(assignment.Id, ReconcileAssignmentAsync(topic, p, assignment, replica, stop));
                    }

                var probes = new List<Task>();
                foreach (var key in _probes.Keys)
                    if (_probes.TryRemove(key, out _)) probes.Add(RefreshProgressAsync(key, stop));
                if (probes.Count != 0) await Task.WhenAll(probes);
            } while (await timer.WaitForNextTickAsync(stop));
        }
        finally { await Task.WhenAll(jobs.Values); }
    }

    private async Task<bool> ReconcileLocalReplicasAsync(ImmutableDictionary<string, ReplicatedTopic> topics,
        Dictionary<Guid, Task> jobs, CancellationToken stop)
    {
        bool retry = false;
        var desired = topics.Values.SelectMany(static topic => topic.Partitions)
            .Where(assignment => (assignment.Migration?.Participants ?? assignment.Replicas).Contains(Local.Id))
            .ToDictionary(static assignment => assignment.Id);
        foreach (var id in _replicaStorageFailures.Keys)
            if (!desired.ContainsKey(id)) _replicaStorageFailures.TryRemove(id, out _);
        foreach (var id in _localPartitions.Keys)
        {
            if (desired.ContainsKey(id) || !_localPartitions.TryGetValue(id, out var retired)) continue;
            if (jobs.ContainsKey(id)) { retry = true; continue; }
            foreach (var member in Members) _progress.TryRemove((id, member.Id), out _);
            await retired.DisposeAsync();
            _localPartitions.TryRemove(id, out _);
        }

        foreach (var (id, assignment) in desired)
        {
            if (_localPartitions.ContainsKey(id)) continue;
            if (_replicaStorageFailures.TryGetValue(id, out long retryAt) && retryAt > Environment.TickCount64)
            {
                retry = true;
                continue;
            }
            var local = ReplicaAddress(Local, assignment.Slot);
            var voters = Members.Where(member => assignment.InitialReplicas.Contains(member.Id))
                .Select(member => ReplicaAddress(member, assignment.Slot)).ToArray();
            try
            {
                var replica = await ReplicatedPartition.OpenAsync(Path.Combine(_options.Data, "replicas", id.ToString("N")),
                    id, local, voters, stop, _tls, Changes.Signal, joinExisting: !assignment.InitialReplicas.Contains(Local.Id),
                    preferredLeader: assignment.Replicas[0] == Local.Id, reuseSlots: _reuseSlots);
                _localPartitions[id] = replica;
                _replicaStorageFailures.TryRemove(id, out _);
            }
            catch (SocketException) when (!stop.IsCancellationRequested) { retry = true; }
            catch (Exception error) when (error is InvalidDataException || DurableFileSystem.IsStorageError(error))
            {
                if (!_replicaStorageFailures.ContainsKey(id))
                    Console.Error.WriteLine($"SimdKafka replica {id:N} unavailable: {error.Message}");
                _replicaStorageFailures[id] = Environment.TickCount64 + 5_000;
                retry = true;
            }
        }
        return retry;
    }

    private async Task ReclaimDeletedTopicsAsync(CancellationToken stop)
    {
        var replicas = new DirectoryInfo(Path.Combine(_options.Data, "replicas"));
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(3));
        while (await timer.WaitForNextTickAsync(stop))
        {
            if (!Directory.Exists(replicas.FullName)) continue;
            try
            {
                var current = LivePartitions();
                var orphaned = new List<(Guid Id, DirectoryInfo Directory)>();
                foreach (var candidate in replicas.EnumerateDirectories())
                    if ((candidate.Attributes & FileAttributes.ReparsePoint) == 0 &&
                        Guid.TryParseExact(candidate.Name, "N", out var id) && !current.Contains(id) &&
                        !_localPartitions.ContainsKey(id)) orphaned.Add((id, candidate));
                if (orphaned.Count == 0) continue;

                using var deadline = CancellationTokenSource.CreateLinkedTokenSource(stop);
                deadline.CancelAfter(TimeSpan.FromSeconds(2));
                // The local controller may still be catching up after restart. Confirm the latest image before unlinking data.
                await Metadata.ReadBarrierAsync(deadline.Token);
                current = LivePartitions();
                foreach (var (id, candidate) in orphaned)
                {
                    if (current.Contains(id) || _localPartitions.ContainsKey(id) || !Directory.Exists(candidate.FullName)) continue;
                    candidate.Delete(recursive: true);
                    DurableFileSystem.FlushDirectory(replicas.FullName);
                }
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException or QuorumUnreachableException or
                NotLeaderException || e is OperationCanceledException && !stop.IsCancellationRequested) { }
        }

        HashSet<Guid> LivePartitions() => Metadata.Image.Topics.Values.SelectMany(static topic => topic.Partitions)
            .Select(static partition => partition.Id).ToHashSet();
    }

    private async Task RefreshProgressAsync((Guid Partition, int Broker) key, CancellationToken stop)
    {
        if (FindReplica(key.Partition) is not { IsLeader: true } replica) return;
        var address = Members.FirstOrDefault(member => member.Id == key.Broker)?.Internal;
        if (address is null) return;
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stop);
        timeout.CancelAfter(TimeSpan.FromMilliseconds(500));
        ReplicaProgress sample;
        try { sample = await _rpc.ProbeAsync(address, key.Partition, timeout.Token, includeLogStart: replica.LogStartOffset > 0); }
        catch (OperationCanceledException) { sample = new(-1, -1); }
        if (FindReplica(key.Partition)?.IsLeader == true) _progress[key] = (sample, Environment.TickCount64 + 1000);
    }

    internal async ValueTask<(Error Error, long Offset)> DeleteRecordsAsync(ReplicatedTopic topic, int partition,
        ReplicatedPartition replica, long requested, CancellationToken stop)
    {
        var assignment = topic.Partitions[partition];
        if (assignment.Migration is not null) return (Error.ReassignmentInProgress, -1);
        if (!replica.IsLeader) return (Error.NotLeaderOrFollower, -1);
        if (requested < -1 || requested > replica.HighWatermark) return (Error.OffsetOutOfRange, -1);
        int epoch = replica.LeaderEpoch;
        var targets = Members.Where(member => member.Id != Local.Id && assignment.Replicas.Contains(member.Id)).ToArray();
        // A replica that cannot report this state may run a previous binary that does not understand the WAL command.
        foreach (var target in targets)
        {
            var sample = await ProbeLogStartAsync(target, assignment.Id, stop);
            if (sample.Epoch != epoch || sample.LogStartOffset < 0) return (Error.RequestTimedOut, -1);
        }
        if (!replica.IsLeader || replica.LeaderEpoch != epoch) return (Error.NotLeaderOrFollower, -1);
        var deleted = await replica.AdvanceLogStartAsync(requested, stop);
        if (deleted.Error != Error.None) return deleted;

        while (replica.IsLeader && replica.LeaderEpoch == epoch)
        {
            var known = Metadata.Image.Topics.GetValueOrDefault(topic.Name);
            if (known?.Id != topic.Id) return (Error.UnknownTopicOrPartition, -1);
            var actual = known.Partitions[partition];
            if (actual.Migration is not null || !actual.Replicas.SequenceEqual(assignment.Replicas))
                return (Error.ReassignmentInProgress, -1);
            long watermark = replica.LogStartOffset;
            foreach (var target in targets)
            {
                var sample = await ProbeLogStartAsync(target, assignment.Id, stop);
                if (sample.Epoch != epoch || sample.LogStartOffset < deleted.Offset)
                {
                    watermark = -1;
                    break;
                }
                watermark = Math.Min(watermark, sample.LogStartOffset);
            }
            if (watermark >= 0 && replica.IsLeader && replica.LeaderEpoch == epoch) return (Error.None, watermark);
            await Task.Delay(50, stop);
        }
        return (Error.NotLeaderOrFollower, -1);
    }

    private async ValueTask<ReplicaProgress> ProbeLogStartAsync(BrokerRegistration target, Guid assignment, CancellationToken stop)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stop);
        timeout.CancelAfter(TimeSpan.FromMilliseconds(400));
        try { return await _rpc.ProbeAsync(target.Internal!, assignment, timeout.Token, includeLogStart: true); }
        catch (OperationCanceledException) when (!stop.IsCancellationRequested) { return new(-1, -1); }
    }

    internal async ValueTask<(int Leader, ReplicaProgress Progress)> ShareOffsetProgressAsync(PartitionAssignment assignment, CancellationToken stop)
    {
        var (leader, epoch) = FindLeader(assignment);
        if (leader == Local.Id && FindReplica(assignment.Id) is { IsLeader: true } local)
            return (leader, new(local.LeaderEpoch, local.HighWatermark, local.LogStartOffset));
        var remote = Members.FirstOrDefault(member => member.Id == leader);
        if (remote is null || leader == Local.Id) return (-1, new(-1, -1));
        var progress = await ProbeLogStartAsync(remote, assignment.Id, stop);
        return progress.Epoch == epoch ? (leader, progress) : (-1, new(-1, -1));
    }

    private async Task ReconcileAssignmentAsync(ReplicatedTopic topic, int index, PartitionAssignment assignment,
        ReplicatedPartition replica, CancellationToken stop)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stop);
        timeout.CancelAfter(TimeSpan.FromSeconds(assignment.Migration is null ? 3 : 30));
        try
        {
            int epoch = replica.LeaderEpoch;
            if (assignment.LeaderEpoch < epoch)
            {
                await replica.ReadBarrierAsync(timeout.Token);
                if (!replica.IsLeader || replica.LeaderEpoch != epoch) return;
                await ExecuteMetadataAsync(new MetadataCommand(MetadataOperation.ReportLeader, topic.Name, topic.Id,
                    Partition: index, AssignmentId: assignment.Id, BrokerId: Local.Id, Epoch: epoch), timeout.Token);
            }
            if (assignment.Migration is not { } migration) return;
            var known = Metadata.Image.Topics.GetValueOrDefault(topic.Name);
            if (known?.Id != topic.Id || (uint)index >= known.Partitions.Length ||
                known.Partitions[index].Migration?.Id != migration.Id || !replica.IsLeader) return;

            var voters = await replica.VotersAsync(timeout.Token);
            var desired = Members.Where(member => migration.Target.Contains(member.Id))
                .Select(member => ReplicaAddress(member, assignment.Slot)).ToArray();
            var add = desired.FirstOrDefault(address => !voters.Contains(address));
            if (add is not null)
            {
                await replica.AddVoterAsync(add, timeout.Token);
                return;
            }
            var local = ReplicaAddress(Local, assignment.Slot);
            var remove = voters.OfType<IPEndPoint>().Where(address => !desired.Contains(address))
                .OrderBy(address => address.Equals(local)).FirstOrDefault();
            if (remove is not null)
            {
                await replica.RemoveVoterAsync(remove, timeout.Token);
                return;
            }
            await ExecuteMetadataAsync(new MetadataCommand(MetadataOperation.CompleteReassignment, topic.Name, topic.Id,
                Partition: index, AssignmentId: assignment.Id, MigrationId: migration.Id, BrokerId: Local.Id,
                Epoch: replica.LeaderEpoch), timeout.Token);
        }
        catch (Exception e) when (e is NotLeaderException or QuorumUnreachableException or OperationCanceledException) { }
    }

    private static IPEndPoint ReplicaAddress(BrokerRegistration broker, int slot) =>
        new(broker.Control.Address, broker.FirstReplicaPort + slot);

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        try { await Task.WhenAll(_reconcile, _reclaim, _transactions, _transactionIdCleanup); }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        finally
        {
            try
            {
                await Task.WhenAll(_localPartitions.Values.Select(static partition => partition.DisposeAsync().AsTask()));
            }
            finally
            {
                try { await _rpc.DisposeAsync(); }
                finally
                {
                    try { await Metadata.DisposeAsync(); }
                    finally
                    {
                        _lease.Dispose();
                        _stop.Dispose();
                    }
                }
            }
        }
    }

    private sealed record ClusterConfiguration(Guid ClusterId, BrokerConfiguration[] Brokers, bool RecycleReplicaPorts = false,
        bool ClientAclRequired = false, bool DynamicScram = false, bool DelegationTokens = false);
    private sealed record BrokerConfiguration(int Id, string Host, int Port, string? Rack, string Control, string Internal,
        int FirstReplicaPort, int ReplicaPortCount = 1024);
}
