using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Text;
using DotNext.Net.Cluster.Consensus.Raft;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    private readonly record struct TransactionTopicResult(string Name, Guid Id, (int Index, Error Error)[] Partitions);

    private static Error ProducerFencingError(Error error, short version) =>
        error == Error.ProducerFenced && version < 2 ? Error.InvalidProducerEpoch : error;

    private async ValueTask<MetadataResult> ExecuteTransactionAsync(string name, TransactionCommand command,
        CancellationToken stop, ImmutableArray<CommittedOffset> offsets = default)
    {
        if (name.Length == 0) return new(Error.InvalidRequest);
        var leadership = cluster.Metadata.LeadershipToken;
        if (leadership.IsCancellationRequested) return new(Error.NotCoordinator);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stop, leadership);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        try
        {
            var result = await cluster.Metadata.ExecuteTransactionAsync(name, command, timeout.Token, offsets);
            return result.Error == Error.NotController ? new(Error.NotCoordinator) : result;
        }
        catch (OperationCanceledException) when (!stop.IsCancellationRequested)
        {
            return new(leadership.IsCancellationRequested ? Error.NotCoordinator : Error.RequestTimedOut);
        }
        catch (QuorumUnreachableException) { return new(Error.CoordinatorNotAvailable); }
    }

    private async ValueTask<AppendResult> ProduceTransactionAsync(string name, ReplicatedTopic topic, int partition,
        ReplicatedPartition replica, Memory<byte> batch, bool modern, bool logAppendTime, TimestampBounds? timestampBounds, CancellationToken stop)
    {
        if (!replica.IsLeader) return new(Error.NotLeaderOrFollower, -1);
        if (batch.Length < RecordBatch.HeaderSize) return new(Error.CorruptMessage, -1);
        if ((batch.Span[22] & 0x10) == 0) return new(Error.InvalidTxnState, -1);
        long producer = BinaryPrimitives.ReadInt64BigEndian(batch.Span[43..]);
        short epoch = BinaryPrimitives.ReadInt16BigEndian(batch.Span[51..]);
        Guid assignment = topic.Partitions[partition].Id;
        var image = cluster.Metadata.Image;
        var state = image.Transactions.GetValueOrDefault(name);
        if (!HasTransactionEnrollment(state, producer, epoch, assignment))
        {
            // The enrollment can be committed on the coordinator just ahead of local Raft apply.
            var changed = cluster.Metadata.TransactionChanges.Capture();
            image = cluster.Metadata.Image;
            state = image.Transactions.GetValueOrDefault(name);
            if (!HasTransactionEnrollment(state, producer, epoch, assignment) && state?.ProducerId == producer &&
                state.Epoch == epoch && !cluster.Metadata.IsLeader)
            {
                await BrokerCluster.WaitForTransactionApplyAsync(changed, stop);
                image = cluster.Metadata.Image;
                state = image.Transactions.GetValueOrDefault(name);
            }
        }
        if (HasTransactionEnrollment(state, producer, epoch, assignment) && !replica.HasTransactionMarker(state!.Id))
        {
            var appended = await replica.AppendAsync(batch, image.LastProducerId, stop, state.Id, logAppendTime, timestampBounds);
            // The WAL checks the GUID again at apply, including when the marker wins a race after this read.
            if (appended.Error != Error.InvalidTxnState || !replica.HasTransactionMarker(state.Id)) return appended;
        }
        await cluster.Metadata.ReadBarrierAsync(stop);
        image = cluster.Metadata.Image;
        if (!image.Transactions.TryGetValue(name, out state)) return new(Error.InvalidProducerIdMapping, -1);
        if (state.Recovery is not null) return new(Error.InvalidProducerEpoch, -1);
        Error error = state.ValidateProducer(producer, epoch);
        if (error != Error.None) return new(error == Error.ProducerFenced ? Error.InvalidProducerEpoch : error, -1);
        if (state.Phase != TransactionPhase.Ongoing || !state.Partitions.Any(item => item.AssignmentId == assignment))
        {
            if (!modern) return new(Error.InvalidTxnState, -1);
            var enlist = new TransactionCommand(TransactionOperation.Enlist, producer, epoch, BrokerCluster.TransactionTime(),
                Guid.NewGuid(), Partitions: [new(topic.Name, partition, assignment)], Modern: true);
            var result = await cluster.ExecuteMetadataAsync(new(MetadataOperation.Transaction, name, Transaction: enlist), stop);
            if (result.Error != Error.None)
                return new(result.Error switch
                {
                    Error.ProducerFenced => Error.InvalidProducerEpoch,
                    Error.NotController => Error.CoordinatorNotAvailable,
                    _ => result.Error
                }, -1);
            await cluster.Metadata.ReadBarrierAsync(stop);
            image = cluster.Metadata.Image;
            if (!image.Transactions.TryGetValue(name, out state) || state.Recovery is not null ||
                state.ValidateProducer(producer, epoch) != Error.None ||
                state.Phase != TransactionPhase.Ongoing || !state.Partitions.Any(item => item.AssignmentId == assignment))
                return new(Error.InvalidTxnState, -1);
        }
        return await replica.AppendAsync(batch, image.LastProducerId, stop, state.Id, logAppendTime, timestampBounds);
    }

    private static bool HasTransactionEnrollment(ClusterTransaction? state, long producer, short epoch, Guid assignment) =>
        state is { Phase: TransactionPhase.Ongoing, Recovery: null } && state.ValidateProducer(producer, epoch) == Error.None &&
        state.Partitions.Any(partition => partition.AssignmentId == assignment);

    private async ValueTask<bool> AddTransactionPartitionsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        short version = request.Version;
        bool compact = version >= 3;
        var r = new WireReader(body.Span);
        string name = r.Text(compact);
        long producer = r.I64();
        short epoch = r.I16();
        var topics = new TransactionTopicResult[r.ArrayLength(compact)];
        var enlisted = new List<TransactionPartition>();
        var image = cluster.Metadata.Image;
        bool transactionAllowed = request.AllowsTransaction(name, AclOperation.Write);
        Error result = Error.None;
        for (int t = 0; t < topics.Length; t++)
        {
            string topicName = r.Text(compact);
            var topic = image.Topics.GetValueOrDefault(topicName);
            bool topicAllowed = request.AllowsTopic(topicName, AclOperation.Write);
            var partitions = new (int Index, Error Error)[r.ArrayLength(compact)];
            for (int p = 0; p < partitions.Length; p++)
            {
                int index = r.I32();
                if (!transactionAllowed || !topicAllowed || topic is null || (uint)index >= topic.Partitions.Length)
                {
                    partitions[p] = (index, !transactionAllowed ? Error.TransactionalIdAuthorizationFailed :
                        !topicAllowed ? Error.TopicAuthorizationFailed : Error.UnknownTopicOrPartition);
                    result = Error.OperationNotAttempted;
                }
                else
                {
                    partitions[p] = (index, Error.None);
                    if (enlisted.Count <= ReplicatedMetadata.MaxTransactionPartitions)
                        enlisted.Add(new(topicName, index, topic.Partitions[index].Id));
                }
            }
            r.EndStruct(compact);
            topics[t] = new(topicName, Guid.Empty, partitions);
        }
        r.EndStruct(compact);
        r.End();
        if (result == Error.None)
        {
            result = enlisted.Count > ReplicatedMetadata.MaxTransactionPartitions ? Error.InvalidRequest :
                (await ExecuteTransactionAsync(name, new(TransactionOperation.Enlist, producer, epoch,
                    BrokerCluster.TransactionTime(), Guid.NewGuid(), Partitions: [.. enlisted]), stop)).Error;
        }
        WriteTransactionTopics(w, compact, topics, ProducerFencingError(result, version));
        return true;
    }

    private async ValueTask<bool> AddTransactionOffsetsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        short version = request.Version;
        bool compact = version >= 3;
        var r = new WireReader(body.Span);
        string name = r.Text(compact);
        long producer = r.I64();
        short epoch = r.I16();
        string group = r.Text(compact);
        r.EndStruct(compact);
        r.End();
        Error error = !request.AllowsTransaction(name, AclOperation.Write) ? Error.TransactionalIdAuthorizationFailed :
            !request.AllowsGroup(group, AclOperation.Read) ? Error.GroupAuthorizationFailed :
            group.Length == 0 ? Error.InvalidGroupId :
            (await ExecuteTransactionAsync(name, new(TransactionOperation.Enlist, producer, epoch,
                BrokerCluster.TransactionTime(), Guid.NewGuid(), Group: group), stop)).Error;
        WriteTransactionResult(w, compact, ProducerFencingError(error, version));
        return true;
    }

    private async ValueTask<bool> EndTransactionAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        short version = request.Version;
        bool compact = version >= 3;
        var r = new WireReader(body.Span);
        string name = r.Text(compact);
        long producer = r.I64();
        short epoch = r.I16();
        bool commit = r.Bool();
        r.EndStruct(compact);
        r.End();
        var result = !request.AllowsTransaction(name, AclOperation.Write) ? new MetadataResult(Error.TransactionalIdAuthorizationFailed) :
            await ExecuteTransactionAsync(name, new(TransactionOperation.Prepare, producer, epoch,
                BrokerCluster.TransactionTime(), version >= 5 ? Guid.NewGuid() : Guid.Empty, Commit: commit, Modern: version >= 5), stop);
        Error error = result.Error;
        TransactionCompletion? completion = null;
        if (error == Error.None)
        {
            var active = cluster.Metadata.LeadershipToken;
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(stop, active);
            deadline.CancelAfter(TimeSpan.FromSeconds(5));
            try
            {
                while (true)
                {
                    Task changed = cluster.Metadata.TransactionChanges.Capture();
                    if (!cluster.Metadata.Image.Transactions.TryGetValue(name, out var tx)) break;
                    if (tx.Completion is { } receipt && receipt.Id == result.TransactionId) completion = receipt;
                    if (tx.Id != result.TransactionId || !tx.Preparing) break;
                    await changed.WaitAsync(deadline.Token);
                }
            }
            catch (OperationCanceledException) when (!stop.IsCancellationRequested)
            { error = active.IsCancellationRequested ? Error.NotCoordinator : Error.ConcurrentTransactions; }
        }
        if (version >= 5)
        {
            if (error == Error.None && completion is null) error = Error.ProducerFenced;
            w.I32(0);
            w.Write(error);
            w.I64(error == Error.None ? completion!.NextProducerId : -1);
            w.I16(error == Error.None ? completion!.NextEpoch : (short)-1);
            w.UVar(0);
        }
        else WriteTransactionResult(w, compact, ProducerFencingError(error, version));
        return true;
    }

    private async ValueTask<bool> CommitTransactionOffsetsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        short version = request.Version;
        bool compact = version >= 3;
        bool byId = version >= 6;
        var r = new WireReader(body.Span);
        string name = r.Text(compact);
        string group = r.Text(compact);
        Error access = !request.AllowsTransaction(name, AclOperation.Write) ? Error.TransactionalIdAuthorizationFailed :
            !request.AllowsGroup(group, AclOperation.Read) ? Error.GroupAuthorizationFailed : Error.None;
        long producer = r.I64();
        short epoch = r.I16();
        int generation = compact ? r.I32() : -1;
        string member = compact ? r.Text(true) : "";
        string? instance = compact ? r.NullableText(true) : null;
        var topics = new TransactionTopicResult[r.ArrayLength(compact)];
        var staged = ImmutableArray.CreateBuilder<CommittedOffset>();
        var image = cluster.Metadata.Image;
        for (int t = 0; t < topics.Length; t++)
        {
            var id = byId ? r.Uuid() : Guid.Empty;
            string topicName = byId ? "" : r.Text(compact);
            var topic = byId ? image.TopicById(id) : image.Topics.GetValueOrDefault(topicName);
            bool allowed = byId && topic is null || request.AllowsTopic(topic?.Name ?? topicName, AclOperation.Read);
            var partitions = new (int Index, Error Error)[r.ArrayLength(compact)];
            for (int p = 0; p < partitions.Length; p++)
            {
                int index = r.I32();
                long offset = r.I64();
                int leaderEpoch = version >= 2 ? r.I32() : -1;
                string? metadata = r.NullableText(compact);
                r.EndStruct(compact);
                Error error = access != Error.None ? access : !allowed ? Error.TopicAuthorizationFailed :
                    topic is null && byId ? Error.UnknownTopicId :
                    topic is null || (uint)index >= topic.Partitions.Length ? Error.UnknownTopicOrPartition :
                    offset < 0 ? Error.InvalidRequest : metadata is not null && Encoding.UTF8.GetByteCount(metadata) > 4096 ?
                    Error.OffsetMetadataTooLarge : Error.None;
                if (error == Error.None && staged.Count <= ReplicatedMetadata.MaxTransactionPartitions)
                    staged.Add(new(topic!.Name, index, offset, leaderEpoch, metadata, topic.Id));
                partitions[p] = (index, error);
            }
            r.EndStruct(compact);
            topics[t] = new(topicName, id, partitions);
        }
        r.EndStruct(compact);
        r.End();

        Error result;
        await _groupWrites.WaitAsync(stop);
        try
        {
            var leadership = GroupLeadership();
            result = access != Error.None ? access :
                leadership.IsCancellationRequested ? Error.NotCoordinator : await EnsureConsumerTermAsync(leadership, stop);
            if (result == Error.None) result = ValidateGroupCommit(group, member, generation, instance);
            if (result == Error.None && staged.Count > 0)
                result = staged.Count > ReplicatedMetadata.MaxTransactionPartitions ? Error.InvalidRequest :
                    (await ExecuteTransactionAsync(name, new(TransactionOperation.StageOffsets, producer, epoch,
                        BrokerCluster.TransactionTime(), version >= 5 ? Guid.NewGuid() : Guid.Empty,
                        Group: group, Modern: version >= 5), stop, staged.ToImmutable())).Error;
        }
        finally { _groupWrites.Release(); }
        WriteTransactionTopics(w, compact, topics, ProducerFencingError(result, version), byId);
        return true;
    }

    private static void WriteTransactionResult(WireWriter w, bool compact, Error error)
    {
        w.I32(0);
        w.Write(error);
        w.EndStruct(compact);
    }

    private static void WriteTransactionTopics(WireWriter w, bool compact, TransactionTopicResult[] topics, Error result,
        bool byId = false)
    {
        w.I32(0);
        w.ArrayLength(topics.Length, compact);
        foreach (var (topic, id, partitions) in topics)
        {
            if (byId) w.Uuid(id);
            else w.Text(topic, compact);
            w.ArrayLength(partitions.Length, compact);
            foreach (var (index, error) in partitions)
            {
                w.I32(index);
                w.Write(error == Error.None ? result : error);
                w.EndStruct(compact);
            }
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
    }
}
