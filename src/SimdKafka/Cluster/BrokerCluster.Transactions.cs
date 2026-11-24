using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Threading.Channels;
using DotNext.Net.Cluster.Consensus.Raft;

namespace SimdKafka;

internal readonly record struct TransactionMarker(Guid AssignmentId, Guid TransactionId, long ProducerId,
    short Epoch, bool Commit, int CoordinatorEpoch, long Timestamp, bool Seal = false)
{
    internal const int Size = 55;

    internal byte[] Encode()
    {
        byte[] bytes = new byte[Size];
        var output = bytes.AsSpan();
        AssignmentId.TryWriteBytes(output);
        TransactionId.TryWriteBytes(output[16..]);
        BinaryPrimitives.WriteInt64BigEndian(output[32..], ProducerId);
        BinaryPrimitives.WriteInt16BigEndian(output[40..], Epoch);
        output[42] = Seal ? (byte)2 : Commit ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteInt32BigEndian(output[43..], CoordinatorEpoch);
        BinaryPrimitives.WriteInt64BigEndian(output[47..], Timestamp);
        return bytes;
    }

    internal static TransactionMarker Decode(ReadOnlySpan<byte> bytes)
    {
        if (bytes[42] > 2) throw new InvalidDataException("Invalid transaction marker kind");
        return new(new Guid(bytes[..16]), new Guid(bytes[16..32]), BinaryPrimitives.ReadInt64BigEndian(bytes[32..]),
            BinaryPrimitives.ReadInt16BigEndian(bytes[40..]), bytes[42] == 1,
            BinaryPrimitives.ReadInt32BigEndian(bytes[43..]), BinaryPrimitives.ReadInt64BigEndian(bytes[47..]), bytes[42] == 2);
    }
}

internal sealed partial class BrokerCluster
{
    private readonly ConcurrentDictionary<string, (Guid Id, ConcurrentDictionary<Guid, byte> Marked)> _transactionProgress =
        new(StringComparer.Ordinal);
    private readonly Channel<byte> _transactionWork = Channel.CreateBounded<byte>(
        new BoundedChannelOptions(1) { SingleReader = true, FullMode = BoundedChannelFullMode.DropWrite });

    internal static long TransactionTime() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private async Task ExpireTransactionIdsAsync(CancellationToken stop)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(ReplicatedMetadata.TransactionalIdCleanupIntervalMs));
        try
        {
            while (await timer.WaitForNextTickAsync(stop))
            {
                var active = Metadata.LeadershipToken;
                if (active.IsCancellationRequested) continue;
                long now = TransactionTime();
                foreach (var (name, transaction) in Metadata.Image.Transactions)
                {
                    if (active.IsCancellationRequested || stop.IsCancellationRequested) break;
                    if (!transaction.IsIdExpired(now)) continue;
                    using var deadline = CancellationTokenSource.CreateLinkedTokenSource(active, stop);
                    deadline.CancelAfter(TimeSpan.FromSeconds(3));
                    try
                    {
                        await Metadata.ExecuteTransactionAsync(name, new(TransactionOperation.ExpireId,
                            transaction.ProducerId, transaction.Epoch, now), deadline.Token);
                    }
                    catch (Exception e) when (e is OperationCanceledException or NotLeaderException or QuorumUnreachableException or IOException) { }
                }
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
    }

    private async Task RecoverTransactionsAsync(CancellationToken stop)
    {
        const int maxRecovering = 8;
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(250));
        var tick = timer.WaitForNextTickAsync(stop).AsTask();
        var request = _transactionWork.Reader.WaitToReadAsync(stop).AsTask();
        var attempts = new Dictionary<string, (Guid Id, TransactionPhase Phase, long RetryAt, Task Work)>(StringComparer.Ordinal);
        try
        {
            while (!stop.IsCancellationRequested)
            {
                await Task.WhenAny(tick, request);
                if (tick.IsCompleted)
                {
                    await tick;
                    tick = timer.WaitForNextTickAsync(stop).AsTask();
                }
                if (request.IsCompleted)
                {
                    await request;
                    _transactionWork.Reader.TryRead(out _);
                    request = _transactionWork.Reader.WaitToReadAsync(stop).AsTask();
                }
                var active = Metadata.LeadershipToken;
                var image = Metadata.Image;
                long now = TransactionTime(), interval = Environment.TickCount64;
                int running = 0;
                foreach (var (name, attempt) in attempts)
                {
                    if (!attempt.Work.IsCompleted) { running++; continue; }
                    await attempt.Work;
                    if (interval >= attempt.RetryAt || !image.Transactions.TryGetValue(name, out var state) ||
                        state.Id != attempt.Id || state.Phase != attempt.Phase) attempts.Remove(name);
                }
                foreach (var (name, progress) in _transactionProgress)
                    if (!image.Transactions.TryGetValue(name, out var state) || !state.Preparing || state.Id != progress.Id)
                        _transactionProgress.TryRemove(name, out _);
                if (active.IsCancellationRequested) continue;
                foreach (string name in image.ActiveTransactions)
                {
                    if (running == maxRecovering) break;
                    var transaction = image.Transactions[name];
                    if (attempts.ContainsKey(name) || !transaction.Preparing && !transaction.IsExpired(now))
                        continue;
                    var work = RecoverTransactionAttemptAsync(name, transaction, active, stop);
                    attempts.Add(name, (transaction.Id, transaction.Phase, interval + 100, work));
                    running++;
                    _ = work.ContinueWith(static (_, state) => ((ChannelWriter<byte>)state!).TryWrite(0),
                        _transactionWork.Writer, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously,
                        TaskScheduler.Default);
                }
            }
        }
        finally { await Task.WhenAll(attempts.Values.Select(static attempt => attempt.Work)); }
    }

    private async Task RecoverTransactionAttemptAsync(string name, ClusterTransaction transaction, CancellationToken leadership,
        CancellationToken stop)
    {
        using var attempt = CancellationTokenSource.CreateLinkedTokenSource(leadership, stop);
        attempt.CancelAfter(TimeSpan.FromSeconds(2));
        try { await RecoverTransactionAsync(name, transaction, attempt.Token); }
        catch (Exception e) when (e is OperationCanceledException or NotLeaderException or QuorumUnreachableException or IOException) { }
    }

    private async ValueTask RecoverTransactionAsync(string name, ClusterTransaction transaction, CancellationToken stop)
    {
        long now = TransactionTime();
        if (!transaction.Preparing)
        {
            await Metadata.ExecuteTransactionAsync(name, new(TransactionOperation.Expire, transaction.ProducerId,
                transaction.Epoch, now, transaction.Id), stop);
            return;
        }

        var image = Metadata.Image;
        var progress = _transactionProgress.AddOrUpdate(name, _ => (transaction.Id, new()),
            (_, previous) => previous.Id == transaction.Id ? previous : (transaction.Id, new()));
        int retry = 0;
        await Parallel.ForEachAsync(transaction.Partitions,
            new ParallelOptions { MaxDegreeOfParallelism = 4, CancellationToken = stop }, async (partition, token) =>
            {
                if (progress.Marked.ContainsKey(partition.AssignmentId)) return;
                Error error = await ReplicateTransactionMarkerAsync(image, partition, transaction, now, token);
                if (error == Error.None) progress.Marked.TryAdd(partition.AssignmentId, 0);
                else Interlocked.Exchange(ref retry, 1);
            });
        if (retry == 0)
            await Metadata.ExecuteTransactionAsync(name, new(TransactionOperation.Complete, transaction.ProducerId,
                transaction.Epoch, now, transaction.Id, Commit: transaction.Commit), stop);
    }

    internal async ValueTask<Error> SealRecoveredTransactionAsync(string name, MetadataResult result, CancellationToken stop)
    {
        var leadership = Metadata.LeadershipToken;
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(leadership, stop);
        deadline.CancelAfter(TimeSpan.FromSeconds(5));
        var image = Metadata.Image;
        if (leadership.IsCancellationRequested) return Error.NotCoordinator;
        if (!MatchesRecovery(image.Transactions.GetValueOrDefault(name), result)) return Error.ProducerFenced;
        var state = image.Transactions[name];
        int error = 0;
        long now = TransactionTime();
        try
        {
            await Parallel.ForEachAsync(state.Partitions,
                new ParallelOptions { MaxDegreeOfParallelism = 4, CancellationToken = deadline.Token }, async (partition, token) =>
                {
                    Error status = await ReplicateTransactionMarkerAsync(image, partition, state, now, token, seal: true);
                    if (status != Error.None) Interlocked.CompareExchange(ref error, (int)status, 0);
                });
        }
        catch (OperationCanceledException) when (!stop.IsCancellationRequested)
        {
            return leadership.IsCancellationRequested ? Error.NotCoordinator : Error.ConcurrentTransactions;
        }
        if (leadership.IsCancellationRequested) return Error.NotCoordinator;
        if (!MatchesRecovery(Metadata.Image.Transactions.GetValueOrDefault(name), result)) return Error.ProducerFenced;
        return error == 0 ? Error.None : error == (int)Error.KafkaStorageError ? Error.KafkaStorageError : Error.ConcurrentTransactions;
    }

    private static bool MatchesRecovery(ClusterTransaction? transaction, MetadataResult result) =>
        transaction is { Phase: TransactionPhase.Ongoing, Recovery: { } recovery } &&
        transaction.Id == result.TransactionId && recovery.ProducerId == result.ProducerId && recovery.Epoch == result.ProducerEpoch;

    private async ValueTask<Error> ReplicateTransactionMarkerAsync(ClusterMetadataImage image, TransactionPartition partition,
        ClusterTransaction transaction, long now, CancellationToken stop, bool seal = false)
    {
        if (!ReplicatedMetadata.TryTransactionPartition(image, partition, out var assignment)) return Error.None;
        var target = Members.FirstOrDefault(member => member.Id == FindLeader(assignment).LeaderId);
        if (target is null) return Error.LeaderNotAvailable;
        var marker = new TransactionMarker(assignment.Id, transaction.Id, transaction.ProducerId, transaction.Epoch,
            transaction.Commit, (int)Math.Min(Metadata.Term, int.MaxValue), now, seal);
        return target.Id == Local.Id ? await ApplyTransactionMarkerAsync(marker, stop) :
            await _rpc.SendMarkerAsync(target.Internal!, marker, stop);
    }

    private async ValueTask<Error> ApplyTransactionMarkerAsync(TransactionMarker marker, CancellationToken stop)
    {
        var replica = FindReplica(marker.AssignmentId);
        if (replica?.IsLeader != true) return Error.NotLeaderOrFollower;
        // An applied preparation is already committed. Its decision is immutable for this transaction GUID.
        if (!HasMarkerDecision(Metadata.Image, marker))
        {
            var changed = Metadata.TransactionChanges.Capture();
            if (!HasMarkerDecision(Metadata.Image, marker))
                await WaitForTransactionApplyAsync(changed, stop);
            if (!HasMarkerDecision(Metadata.Image, marker))
            {
                try { await Metadata.ReadBarrierAsync(stop); }
                catch (NotLeaderException) { return Error.CoordinatorNotAvailable; }
                catch (QuorumUnreachableException) { return Error.CoordinatorNotAvailable; }
                if (!HasMarkerDecision(Metadata.Image, marker)) return Error.InvalidTxnState;
            }
        }
        return await replica.WriteTransactionMarkerAsync(marker, stop);
    }

    private static bool HasMarkerDecision(ClusterMetadataImage image, TransactionMarker marker) =>
        image.TransactionNames.TryGetValue(marker.ProducerId, out var name) &&
        image.Transactions.TryGetValue(name, out var state) && state.Id == marker.TransactionId &&
        state.ValidateProducer(marker.ProducerId, marker.Epoch) == Error.None &&
        (marker.Seal ? state.Active && state.Recovery is not null : state.Preparing && state.Commit == marker.Commit) &&
        state.Partitions.Any(partition => partition.AssignmentId == marker.AssignmentId);

    internal static async ValueTask WaitForTransactionApplyAsync(Task change, CancellationToken stop)
    {
        try { await change.WaitAsync(TimeSpan.FromMilliseconds(12), stop); }
        catch (TimeoutException) { }
    }
}
