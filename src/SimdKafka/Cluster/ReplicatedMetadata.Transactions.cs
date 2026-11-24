using System.Collections.Immutable;

namespace SimdKafka;

internal enum TransactionOperation : byte { Init, Enlist, StageOffsets, Prepare, Expire, Complete, ExpireId }
internal enum TransactionPhase : byte { Empty, Ongoing, PreparingCommit, PreparingAbort, Committed, Aborted }

internal sealed record TransactionCommand(TransactionOperation Operation, long ProducerId, short Epoch, long Now,
    Guid TransactionId = default, int TimeoutMs = 0, bool Commit = false, string? Group = null,
    TransactionPartition[]? Partitions = null, bool Modern = false, bool EnableTwoPhase = false, bool KeepPrepared = false);

internal sealed record TransactionPartition(string Topic, int Partition, Guid AssignmentId);
internal sealed record TransactionCompletion(Guid Id, long ProducerId, short Epoch, bool Commit, long NextProducerId, short NextEpoch);
internal sealed record TransactionRecovery(long ProducerId, short Epoch, int NextTimeoutMs);

internal sealed record ClusterTransaction(long ProducerId, short Epoch, long PreviousProducerId, short PreviousEpoch,
    int TimeoutMs, Guid Id, long Started, TransactionPhase Phase, ImmutableArray<TransactionPartition> Partitions,
    ImmutableHashSet<string> Groups, ImmutableDictionary<string, ImmutableArray<CommittedOffset>> StagedOffsets,
    bool FenceProducer = false, bool RotateOnCompletion = false, TransactionCompletion? Completion = null,
    TransactionRecovery? Recovery = null, long LastUpdated = 0)
{
    internal bool Preparing => Phase is TransactionPhase.PreparingCommit or TransactionPhase.PreparingAbort;
    internal bool Active => Phase == TransactionPhase.Ongoing || Preparing;
    internal bool Commit => Phase is TransactionPhase.PreparingCommit or TransactionPhase.Committed;
    internal bool IsExpired(long now) =>
        Phase == TransactionPhase.Ongoing && Recovery is null && TimeoutMs != int.MaxValue && now - Started >= TimeoutMs;
    internal bool IsIdExpired(long now) => !Active && LastUpdated > 0 &&
        now - LastUpdated >= ReplicatedMetadata.TransactionalIdExpirationMs;

    internal ClusterTransaction Clear(TransactionPhase phase) => this with
    {
        Phase = phase, Partitions = [], Groups = Groups.Clear(), StagedOffsets = StagedOffsets.Clear()
    };

    internal Error ValidateProducer(long id, short epoch) =>
        id != ProducerId ? Error.InvalidProducerIdMapping : epoch != Epoch ? Error.ProducerFenced : Error.None;
}

internal sealed partial class ReplicatedMetadata
{
    internal const int MaxTransactionTimeoutMs = 900_000;
    internal const int MaxTransactionPartitions = 4_096;
    internal const int TransactionalIdExpirationMs = 604_800_000;
    internal const int TransactionalIdCleanupIntervalMs = 3_600_000;

    internal static bool TryTransactionPartition(ClusterMetadataImage image, TransactionPartition key,
        out PartitionAssignment assignment)
    {
        if (image.Topics.TryGetValue(key.Topic, out var topic) && (uint)key.Partition < topic.Partitions.Length)
        {
            assignment = topic.Partitions[key.Partition];
            if (assignment.Id == key.AssignmentId) return true;
        }
        assignment = null!;
        return false;
    }

    internal ValueTask<MetadataResult> ExecuteTransactionAsync(string name, TransactionCommand transaction,
        CancellationToken stop, ImmutableArray<CommittedOffset> offsets = default) =>
        ExecuteAsync(new(MetadataOperation.Transaction, name, Offsets: offsets, Transaction: transaction), stop);

    private static (MetadataResult, ClusterMetadataImage) ApplyTransaction(ClusterMetadataImage image,
        MetadataCommand command, long index)
    {
        var tx = command.Transaction ?? throw new InvalidDataException("Missing transaction command");
        string name = command.Name;
        if (string.IsNullOrEmpty(name)) return (new(Error.InvalidRequest), image);
        var current = image.Transactions.GetValueOrDefault(name);
        if (tx.Operation == TransactionOperation.Init) return InitTransaction(image, name, tx, current);
        if (tx.Operation == TransactionOperation.ExpireId)
        {
            if (current is null || current.ProducerId != tx.ProducerId || current.Epoch != tx.Epoch || !current.IsIdExpired(tx.Now))
                return (new(Error.None), image);
            return (new(Error.None), image with
            {
                Transactions = image.Transactions.Remove(name),
                TransactionNames = image.TransactionNames.Remove(current.ProducerId)
            });
        }
        if (current is null) return (new(Error.InvalidProducerIdMapping), image);

        if (tx is { Operation: TransactionOperation.Prepare, Modern: true } && current.Completion is { } receipt &&
            receipt.ProducerId == tx.ProducerId && receipt.Epoch == tx.Epoch)
            return (new(receipt.Commit == tx.Commit ? Error.None : Error.InvalidTxnState, TransactionId: receipt.Id), image);

        bool matchingRecovery = current.Recovery is { } recovery && tx.ProducerId == recovery.ProducerId && tx.Epoch == recovery.Epoch;
        Error error = current.Recovery is not null && tx.Operation == TransactionOperation.Prepare ?
            !matchingRecovery ? Error.ProducerFenced : tx.Modern ? Error.None : Error.UnsupportedVersion :
            current.ValidateProducer(tx.ProducerId, tx.Epoch);
        if (error != Error.None) return (new(error), image);
        if (tx.Group is { } shareGroupId && image.ShareGroups.ContainsKey(shareGroupId)) return (new(Error.InconsistentGroupProtocol), image);
        if (current.Recovery is not null && tx.Operation is TransactionOperation.Enlist or TransactionOperation.StageOffsets)
            return (new(Error.ProducerFenced), image);
        if (current.FenceProducer && tx.Operation is TransactionOperation.Enlist or TransactionOperation.StageOffsets or
            TransactionOperation.Prepare) return (new(Error.ProducerFenced), image);

        switch (tx.Operation)
        {
            case TransactionOperation.Enlist:
                return EnlistTransaction(image, name, tx, current);
            case TransactionOperation.StageOffsets:
                if (current.Preparing) return (new(Error.ConcurrentTransactions), image);
                if (tx.Modern && tx.Group is { Length: > 0 } && tx.TransactionId != Guid.Empty)
                {
                    current = BeginTransaction(current, tx);
                    if (current.Groups.Count > 64) return (new(Error.InvalidRequest), image);
                }
                if (current.Phase != TransactionPhase.Ongoing || tx.Group is null || !current.Groups.Contains(tx.Group))
                    return (new(Error.InvalidTxnState), image);
                var staged = current.StagedOffsets.GetValueOrDefault(tx.Group, []);
                var merged = staged.ToDictionary(static offset => (offset.Topic, offset.Partition));
                foreach (var offset in command.Offsets)
                {
                    if (!image.Topics.TryGetValue(offset.Topic, out var topic) || topic.Id != offset.TopicId ||
                        (uint)offset.Partition >= topic.Partitions.Length) return (new(Error.UnknownTopicOrPartition), image);
                    if (offset.Offset < 0) return (new(Error.InvalidRequest), image);
                    merged[(offset.Topic, offset.Partition)] = offset with { CommitIndex = index };
                }
                if (current.StagedOffsets.Values.Sum(static offsets => offsets.Length) - staged.Length + merged.Count >
                    MaxTransactionPartitions) return (new(Error.InvalidRequest), image);
                return SaveTransaction(image, name, current with
                {
                    StagedOffsets = current.StagedOffsets.SetItem(tx.Group, [.. merged.Values])
                }, tx.Now);
            case TransactionOperation.Prepare:
                if (tx.Modern && !current.Active)
                    current = BeginTransaction(current, tx);
                if (current.Phase == TransactionPhase.Empty) return (new(Error.InvalidTxnState), image);
                if (current.Phase != TransactionPhase.Ongoing)
                    return (new(current.Commit == tx.Commit ? Error.None : Error.InvalidTxnState, TransactionId: current.Id), image);
                return SaveTransaction(image, name, current with
                {
                    Phase = tx.Commit ? TransactionPhase.PreparingCommit : TransactionPhase.PreparingAbort,
                    RotateOnCompletion = current.RotateOnCompletion || tx.Modern
                }, tx.Now);
            case TransactionOperation.Expire:
                if (current.Id != tx.TransactionId || !current.IsExpired(tx.Now)) return (new(Error.None), image);
                return SaveTransaction(image, name, current with { Phase = TransactionPhase.PreparingAbort, FenceProducer = true }, tx.Now);
            case TransactionOperation.Complete:
                if (!current.Preparing || current.Id != tx.TransactionId || current.Commit != tx.Commit)
                    return (new(Error.InvalidTxnState), image);
                if (tx.Commit)
                {
                    foreach (var (group, offsets) in current.StagedOffsets)
                        image = MergeCommittedOffsets(image, group, offsets.Where(offset =>
                            image.Topics.TryGetValue(offset.Topic, out var topic) && topic.Id == offset.TopicId));
                }
                var completed = current.Clear(tx.Commit ? TransactionPhase.Committed : TransactionPhase.Aborted);
                if (current.RotateOnCompletion && !current.FenceProducer)
                {
                    long decidingId = current.Recovery?.ProducerId ?? current.ProducerId;
                    short decidingEpoch = current.Recovery?.Epoch ?? current.Epoch;
                    var (id, epoch) = NextProducer(image, decidingId, decidingEpoch);
                    completed = completed with
                    {
                        ProducerId = id, Epoch = epoch,
                        TimeoutMs = current.Recovery?.NextTimeoutMs ?? current.TimeoutMs,
                        Completion = new(current.Id, decidingId, decidingEpoch, tx.Commit, id, epoch),
                        Recovery = null
                    };
                    if (id != current.ProducerId) image = image with
                    {
                        LastProducerId = Math.Max(image.LastProducerId, id),
                        TransactionNames = image.TransactionNames.Remove(current.ProducerId).SetItem(id, name)
                    };
                }
                return SaveTransaction(image, name, completed, tx.Now);
            default:
                throw new InvalidDataException("Unknown transaction operation");
        }
    }

    private static (MetadataResult, ClusterMetadataImage) InitTransaction(ClusterMetadataImage image, string name,
        TransactionCommand command, ClusterTransaction? current)
    {
        int timeout = command.EnableTwoPhase ? int.MaxValue : command.TimeoutMs;
        if (!command.EnableTwoPhase && timeout is <= 0 or > MaxTransactionTimeoutMs)
            return (new(Error.InvalidTransactionTimeout), image);
        long latestId = current?.Recovery?.ProducerId ?? current?.ProducerId ?? -1;
        short latestEpoch = current?.Recovery?.Epoch ?? current?.Epoch ?? -1;
        if (command.ProducerId >= 0 && current is not null)
        {
            if (current.Recovery is null && command.ProducerId == current.PreviousProducerId && command.Epoch == current.PreviousEpoch)
                return (current.Active || current.FenceProducer ? new(Error.ProducerFenced) :
                    new(Error.None, ProducerId: current.ProducerId, ProducerEpoch: current.Epoch), image);
            if (command.ProducerId != latestId || command.Epoch != latestEpoch)
                return (new(Error.ProducerFenced), image);
        }
        var (id, epoch) = NextProducer(image, latestId, latestEpoch);
        if (current?.Active == true)
        {
            if (current.Phase == TransactionPhase.Ongoing)
            {
                if (command.KeepPrepared)
                {
                    var recovered = current with { Recovery = new(id, epoch, timeout), LastUpdated = Math.Max(current.LastUpdated, command.Now) };
                    return (new(Error.None, ProducerId: id, ProducerEpoch: epoch, TransactionId: current.Id,
                        OngoingProducerId: current.ProducerId, OngoingProducerEpoch: current.Epoch),
                        image with { Transactions = image.Transactions.SetItem(name, recovered),
                            LastProducerId = Math.Max(image.LastProducerId, id) });
                }
                image = SaveTransaction(image, name, current with
                { Phase = TransactionPhase.PreparingAbort, FenceProducer = true }, command.Now).Item2;
            }
            return (new(Error.ConcurrentTransactions), image);
        }
        var transaction = new ClusterTransaction(id, epoch, latestId, latestEpoch, timeout, Guid.Empty,
            0, TransactionPhase.Empty, [], ImmutableHashSet.Create<string>(StringComparer.Ordinal),
            ImmutableDictionary.Create<string, ImmutableArray<CommittedOffset>>(StringComparer.Ordinal),
            LastUpdated: Math.Max(current?.LastUpdated ?? 0, command.Now));
        return (new(Error.None, ProducerId: id, ProducerEpoch: epoch), image with
        {
            LastProducerId = Math.Max(image.LastProducerId, id),
            Transactions = image.Transactions.SetItem(name, transaction),
            TransactionNames = (current is not null && current.ProducerId != id ? image.TransactionNames.Remove(current.ProducerId) :
                image.TransactionNames).SetItem(id, name)
        });
    }

    private static (long ProducerId, short Epoch) NextProducer(ClusterMetadataImage image, long id, short epoch) =>
        id < 0 || epoch >= short.MaxValue - 1 ? (checked(image.LastProducerId + 1), (short)0) : (id, checked((short)(epoch + 1)));

    private static (MetadataResult, ClusterMetadataImage) EnlistTransaction(ClusterMetadataImage image, string name,
        TransactionCommand command, ClusterTransaction current)
    {
        if (current.Preparing) return (new(Error.ConcurrentTransactions), image);
        if (command.Group is "" || command.TransactionId == Guid.Empty) return (new(Error.InvalidRequest), image);
        var partitions = current.Partitions;
        if (command.Partitions is { Length: > 0 } additions)
        {
            var enlisted = partitions.ToHashSet();
            foreach (var partition in additions)
            {
                if (!TryTransactionPartition(image, partition, out _)) return (new(Error.UnknownTopicOrPartition), image);
                enlisted.Add(partition);
            }
            if (enlisted.Count > MaxTransactionPartitions) return (new(Error.InvalidRequest), image);
            partitions = [.. enlisted];
        }
        var ongoing = BeginTransaction(current, command);
        return ongoing.Groups.Count > 64 ? (new(Error.InvalidRequest), image) :
            SaveTransaction(image, name, ongoing with { Partitions = partitions }, command.Now);
    }

    private static ClusterTransaction BeginTransaction(ClusterTransaction current, TransactionCommand command)
    {
        bool ongoing = current.Phase == TransactionPhase.Ongoing;
        return current with
        {
            Id = ongoing ? current.Id : command.TransactionId,
            Started = ongoing ? current.Started : command.Now,
            Phase = TransactionPhase.Ongoing,
            RotateOnCompletion = command.Modern || ongoing && current.RotateOnCompletion,
            Groups = command.Group is { } group ? current.Groups.Add(group) : current.Groups
        };
    }

    private static (MetadataResult, ClusterMetadataImage) SaveTransaction(ClusterMetadataImage image, string name,
        ClusterTransaction transaction, long now) =>
        (new(Error.None, ProducerId: transaction.ProducerId, ProducerEpoch: transaction.Epoch, TransactionId: transaction.Id),
            image with
            {
                Transactions = image.Transactions.SetItem(name, transaction with { LastUpdated = Math.Max(transaction.LastUpdated, now) }),
                ActiveTransactions = transaction.Active ? image.ActiveTransactions.Add(name) : image.ActiveTransactions.Remove(name)
            });
}
