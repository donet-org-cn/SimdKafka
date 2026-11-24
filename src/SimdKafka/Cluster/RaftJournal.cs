using System.Buffers.Binary;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using DotNext.Buffers;
using DotNext.Net;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.Membership;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;
using DotNext.Net.Security;

namespace SimdKafka;

internal sealed class RaftJournal : IAsyncDisposable
{
    private readonly RaftCluster _node;
    private readonly WriteAheadLog _wal;
    private readonly VoterStorage _membership;
    private readonly FileStream _lease;

    private RaftJournal(RaftCluster node, WriteAheadLog wal, VoterStorage membership, FileStream lease)
    {
        _node = node;
        _wal = wal;
        _membership = membership;
        _lease = lease;
    }

    internal bool IsLeader => !_node.LeadershipToken.IsCancellationRequested;
    internal EndPoint? Leader => _node.Leader?.EndPoint;
    internal long Term => ((IRaftCluster)_node).Term;
    internal long LastIndex => _wal.LastEntryIndex;
    internal long CommitIndex => _wal.LastCommittedEntryIndex;
    internal CancellationToken LeadershipToken => _node.LeadershipToken;

    internal static async Task<RaftJournal> OpenAsync(string path, Guid groupId, IPEndPoint local,
        IReadOnlyCollection<IPEndPoint> voters, IStateMachine machine, CancellationToken stop, SslOptions? tls = null,
        bool joinExisting = false, int transmissionBlockSize = 4 * 1024, int chunkSize = 4 * 1024 * 1024,
        bool? preferredLeader = null, bool reuseSlots = false, Action? validateLayout = null)
    {
        if (groupId == Guid.Empty || voters.Count == 0 || (!joinExisting && !voters.Contains(local)) ||
            voters.Distinct().Count() != voters.Count || voters.Append(local).Any(static node => node.Port == 0 ||
                node.Address.Equals(IPAddress.Any) || node.Address.Equals(IPAddress.IPv6Any) || node.Address.IsIPv6Multicast))
            throw new ArgumentException("A Raft group requires a group ID and distinct voter addresses including the local address");
        if (tls is null && voters.Append(local).Any(static node => !IPAddress.IsLoopback(node.Address)))
            throw new ArgumentException("Raft communication outside loopback requires TLS");

        path = Path.GetFullPath(path);
        DurableFileSystem.EnsureDirectory(path);
        var lease = new FileStream(Path.Combine(path, "journal.lock"), FileMode.OpenOrCreate,
            FileAccess.ReadWrite, FileShare.None);
        RaftCluster? node = null;
        VoterStorage? membership = null;
        WriteAheadLog? wal = null;
        try
        {
            VerifyIdentity(path, groupId, local, voters, reuseSlots);
            validateLayout?.Invoke();
            if (reuseSlots) tls = ClusterTls.BindToGroup(tls ?? throw new ArgumentException("Replica slot reuse requires TLS"), groupId);
            string configurationPath = Path.Combine(path, "voters");
            membership = new VoterStorage(configurationPath);
            if (!File.Exists(configurationPath))
            {
                if (Directory.Exists(Path.Combine(path, "wal")))
                    throw new InvalidDataException("Raft voter configuration is missing");
                using var initial = new InitialVoters();
                var builder = initial.CreateInitialConfigurationBuilder();
                builder.UnionWith(voters.OrderBy(static voter => voter.ToString(), StringComparer.Ordinal));
                builder.Build();
                IClusterConfigurationStorage seed = initial;
                var (configuration, version) = await seed.LoadConfigurationAsync(stop);
                await ((IClusterConfigurationStorage)membership).SaveConfigurationAsync(configuration, version, stop);
                DurableFileSystem.FlushDirectory(path);
            }
            string walPath = Path.Combine(path, "wal");
            // Chunk geometry is fixed per journal. Older journals used the library's 4 KiB chunks.
            int storedChunkSize = LoadChunkSize(path, walPath, chunkSize);
            wal = new WriteAheadLog(new WriteAheadLog.Options
            {
                Location = walPath,
                ChunkSize = storedChunkSize,
                MemoryManagement = WriteAheadLog.MemoryManagementStrategy.SharedMemory,
                HashAlgorithm = WriteAheadLog.IntegrityHashAlgorithm.XxHash3
            }, machine);
            node = new RaftCluster(new RaftCluster.TcpConfiguration(local)
            {
                ConfigurationStorage = membership,
                ColdStart = false,
                LowerElectionTimeout = preferredLeader switch { false => 2800, true => 1800, null => 800 },
                UpperElectionTimeout = preferredLeader switch { false => 4400, true => 2500, null => 1600 },
                HeartbeatThreshold = preferredLeader.HasValue ? 0.15 : 0.25,
                ConnectTimeout = TimeSpan.FromMilliseconds(preferredLeader.HasValue ? 500 : 200),
                RequestTimeout = TimeSpan.FromMilliseconds(preferredLeader.HasValue ? 1800 : 600),
                TransmissionBlockSize = transmissionBlockSize,
                SslOptions = tls,
            }) { AuditTrail = wal };
            await node.StartAsync(stop);
            return new RaftJournal(node, wal, membership, lease);
        }
        catch
        {
            try
            {
                if (node is not null) await node.DisposeAsync();
            }
            finally { await CloseStoreAsync(wal, membership, lease); }
            throw;
        }
    }

    private static int LoadChunkSize(string path, string walPath, int requested)
    {
        const int legacy = 4 * 1024;
        string manifest = Path.Combine(path, "chunk-size");
        if (File.Exists(manifest))
        {
            byte[] recorded = File.ReadAllBytes(manifest);
            if (recorded.Length == sizeof(int))
            {
                int size = BinaryPrimitives.ReadInt32LittleEndian(recorded);
                if (size is >= legacy and <= 64 * 1024 * 1024 && (size & (size - 1)) == 0) return size;
            }
            throw new InvalidDataException("Invalid journal chunk size manifest");
        }
        if (Directory.Exists(walPath))
        {
            string data = Path.Combine(walPath, "data");
            if (Directory.Exists(data) && Directory.EnumerateFiles(data).FirstOrDefault() is { } first &&
                new FileInfo(first).Length != legacy)
                throw new InvalidDataException("Journal chunk size manifest is missing");
            return legacy;
        }

        string temporary = manifest + ".tmp";
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, requested);
        using (var file = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            file.Write(bytes);
            file.Flush(flushToDisk: true);
        }
        DurableFileSystem.Replace(temporary, manifest);
        return requested;
    }

    private static void VerifyIdentity(string path, Guid groupId, IPEndPoint local, IEnumerable<IPEndPoint> voters, bool reuseSlots)
    {
        string identityPath = Path.Combine(path, "identity");
        string addresses = string.Join('\n', voters.Select(static voter => voter.ToString()).Order(StringComparer.Ordinal));
        string namespaceSuffix = reuseSlots ? "\nreplica-slot-reuse-v1" : string.Empty;
        byte[] identity = SHA256.HashData(Encoding.UTF8.GetBytes($"{groupId:D}\n{local}\n{addresses}{namespaceSuffix}"));
        if (File.Exists(identityPath))
        {
            if (!File.ReadAllBytes(identityPath).AsSpan().SequenceEqual(identity))
                throw new InvalidDataException("Raft journal belongs to a different group, node, bootstrap voter set, or slot reuse mode");
            return;
        }
        if (File.Exists(Path.Combine(path, "voters")) || Directory.Exists(Path.Combine(path, "wal")))
            throw new InvalidDataException("Raft journal identity is missing");

        string temporary = identityPath + ".tmp";
        using (var file = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            file.Write(identity);
            file.Flush(flushToDisk: true);
        }
        DurableFileSystem.Replace(temporary, identityPath);
    }

    internal ValueTask ReplicateAsync(ReadOnlyMemory<byte> command, object? context, CancellationToken stop) =>
        ((IRaftCluster)_node).ReplicateAsync(command, context, stop);

    internal ValueTask ReplicateAsync<T>(T entry, CancellationToken stop) where T : IRaftLogEntry =>
        _node.ReplicateAsync(entry, stop);

    internal ValueTask ReadBarrierAsync(CancellationToken stop) =>
        ((IRaftCluster)_node).ApplyReadBarrierAsync(ReadBarrierType.Strong, stop);

    internal ValueTask<WriteAheadLog.LogEntryReader> ReadAsync(long first, long last, CancellationToken stop) =>
        _wal.ReadAsync(first, last, stop);

    internal async ValueTask<IReadOnlySet<EndPoint>> VotersAsync(CancellationToken stop) =>
        (await ((IClusterConfigurationStorage<EndPoint>)_membership).LoadConfigurationAsync(stop)).Members.ToHashSet();

    internal Task<bool> AddVoterAsync(IPEndPoint address, CancellationToken stop) => _node.AddMemberAsync(address, stop);
    internal Task<bool> RemoveVoterAsync(IPEndPoint address, CancellationToken stop) => _node.RemoveMemberAsync(address, stop);

    public async ValueTask DisposeAsync()
    {
        try
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            try { await _node.StopAsync(stop.Token); }
            finally { await _node.DisposeAsync(); }
        }
        finally { await CloseStoreAsync(_wal, _membership, _lease); }
    }

    private static async ValueTask CloseStoreAsync(WriteAheadLog? wal, VoterStorage? membership, FileStream lease)
    {
        try { if (wal is not null) await wal.DisposeAsync(); }
        finally
        {
            membership?.Dispose();
            lease.Dispose();
        }
    }

    private sealed class InitialVoters : InMemoryClusterConfigurationStorage<EndPoint>
    {
        protected override void Encode(EndPoint address, ref BufferWriterSlim<byte> writer) => writer.WriteEndPoint(address);
        protected override EndPoint Decode(ref SequenceReader reader) => reader.ReadEndPoint();
    }

    private sealed class VoterStorage(string path) : PersistentClusterConfigurationStorage<EndPoint>(path)
    {
        protected override void Encode(EndPoint address, ref BufferWriterSlim<byte> writer) => writer.WriteEndPoint(address);
        protected override EndPoint Decode(ref SequenceReader reader) => reader.ReadEndPoint();
    }
}
