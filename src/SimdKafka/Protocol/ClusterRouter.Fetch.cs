using System.Buffers;

namespace SimdKafka;

internal readonly record struct FetchPartition(int Id, int Epoch, long Offset, int Max);
internal readonly record struct FetchTopic(string? Name, Guid Id, FetchPartition[] Partitions);

internal sealed partial class ClusterRouter
{
    private async ValueTask<bool> FetchAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request, CancellationToken stop)
    {
        short version = request.Version;
        var head = Router.ParseFetchHeader(body.Span, version);
        bool compact = version >= 12;
        var (topics, forgotten, rack) = ReadFetchTopics(body.Span[head.BodyOffset..], version);
        int sessionId = 0;
        if (request.FetchSession is { } session)
        {
            Error error = session.Resolve(head.SessionId, head.SessionEpoch, version >= 13, head.Valid,
                forgotten, ref topics, out sessionId);
            if (error != Error.None)
            {
                FetchHeader(w, version, error, sessionId: 0);
                w.ArrayLength(0, compact);
                w.EndStruct(compact);
                return true;
            }
        }
        long deadline = Environment.TickCount64 + head.WaitMs;
        if (head.Valid && head.MinBytes > 0)
        {
            long remaining = head.WaitMs;
            while (remaining > 0)
            {
                Task changed = cluster.Changes.Capture();
                if (FetchReady(topics, head.MaxBytes, head.MinBytes, head.ReadCommitted, request)) break;
                try { await changed.WaitAsync(TimeSpan.FromMilliseconds(remaining), stop); }
                catch (TimeoutException) { break; }
                remaining = deadline - Environment.TickCount64;
            }
        }
        int throttlePosition = w.Length;
        if (sessionId != 0 && head.SessionEpoch > 0)
            await WriteIncrementalFetchAsync(w, version, topics, rack, head.MaxBytes, request.FetchSession!, sessionId,
                head.ReadCommitted, request, stop);
        else await WriteFetchAsync(w, version, topics, rack, head.MaxBytes, head.Valid,
            sessionId == 0 ? null : request.FetchSession, sessionId, head.ReadCommitted, request, stop);
        if (request.Quota is { } quota) w.PatchI32(throttlePosition, quota.Charge(w.Length));
        return true;
    }

    private static (FetchTopic[] Topics, FetchTopic[] Forgotten, string Rack) ReadFetchTopics(ReadOnlySpan<byte> body, short version)
    {
        bool compact = version >= 12, byId = version >= 13;
        var r = new WireReader(body);
        int count = r.ArrayLength(compact);
        var topics = count == 0 ? [] : new FetchTopic[count];
        for (int t = 0; t < topics.Length; t++)
        {
            Guid id = byId ? r.Uuid() : Guid.Empty;
            string? name = byId ? null : r.Text(compact);
            var partitions = new FetchPartition[r.ArrayLength(compact)];
            for (int p = 0; p < partitions.Length; p++)
            {
                int partition = r.I32();
                int epoch = version >= 9 ? r.I32() : -1;
                long offset = r.I64();
                if (compact) r.I32();
                if (version >= 5) r.I64();
                partitions[p] = new(partition, epoch, offset, r.I32());
                r.EndStruct(compact);
            }
            r.EndStruct(compact);
            topics[t] = new FetchTopic(name, id, partitions);
        }
        FetchTopic[] forgotten = [];
        if (version >= 7)
        {
            count = r.ArrayLength(compact);
            if (count > 0) forgotten = new FetchTopic[count];
            for (int i = 0; i < forgotten.Length; i++)
            {
                Guid id = byId ? r.Uuid() : Guid.Empty;
                string? name = byId ? null : r.Text(compact);
                var partitions = new FetchPartition[r.ArrayLength(compact)];
                for (int p = 0; p < partitions.Length; p++) partitions[p] = new(r.I32(), -1, 0, 0);
                forgotten[i] = new FetchTopic(name, id, partitions);
                r.EndStruct(compact);
            }
        }
        string rack = version >= 11 ? r.Text(compact) : "";
        r.EndStruct(compact);
        r.End();
        return (topics, forgotten, rack);
    }

    private bool FetchReady(FetchTopic[] topics, int budget, int minimum, bool readCommitted, Request request)
    {
        int bytes = 0;
        var image = cluster.Metadata.Image;
        foreach (var (name, id, partitions) in topics)
        {
            var topic = FetchTopicFrom(image, name, id);
            if (!CanFetch(topic, name, request)) return true;
            foreach (var partition in partitions)
            {
                var (_, selection) = SelectFetchPartition(topic, name, partition, budget, bytes, valid: true,
                    readCommitted, reportAborted: false);
                if (selection.Error != Error.None) return true;
                bytes += selection.Size;
                if (bytes >= minimum) return true;
            }
        }
        return false;
    }

    private readonly record struct FetchPlan(int Partition, ReplicatedPartition? Replica, ReplicatedPartition.Selection Selection,
        FetchResponseState State);

    private async ValueTask WriteFetchAsync(WireWriter w, short version, FetchTopic[] topics,
        string rack, int budget, bool valid, FetchSession? session, int sessionId, bool readCommitted, Request access, CancellationToken stop)
    {
        bool compact = version >= 12;
        FetchHeader(w, version, Error.None, sessionId);
        w.ArrayLength(topics.Length, compact);
        int bytes = 0;
        var image = cluster.Metadata.Image;
        foreach (var requested in topics)
        {
            var (name, id, partitions) = requested;
            var topic = FetchTopicFrom(image, name, id);
            bool allowed = CanFetch(topic, name, access);
            if (name is null) w.Uuid(id);
            else w.Text(name, compact);
            w.ArrayLength(partitions.Length, compact);
            foreach (var req in partitions)
            {
                var plan = PrepareFetch(topic, name, req, rack, budget, bytes, valid, readCommitted, allowed);
                var state = await WriteSelectedFetchAsync(w, version, plan, stop);
                if (state.Error == Error.None) bytes += plan.Selection.Size;
                session?.RecordResponse(requested, req.Id, state);
            }
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
    }

    private async ValueTask WriteIncrementalFetchAsync(WireWriter w, short version, FetchTopic[] topics, string rack,
        int budget, FetchSession session, int sessionId, bool readCommitted, Request access, CancellationToken stop)
    {
        var plans = ArrayPool<FetchPlan>.Shared.Rent(topics.Sum(static topic => topic.Partitions.Length));
        var topicCounts = ArrayPool<int>.Shared.Rent(topics.Length);
        int selected = 0;
        try
        {
            int bytes = 0, topicCount = 0;
            var image = cluster.Metadata.Image;
            for (int t = 0; t < topics.Length; t++)
            {
                var requested = topics[t];
                var topic = FetchTopicFrom(image, requested.Name, requested.Id);
                bool allowed = CanFetch(topic, requested.Name, access);
                int start = selected;
                foreach (var req in requested.Partitions)
                {
                    var plan = PrepareFetch(topic, requested.Name, req, rack, budget, bytes, valid: true, readCommitted, allowed);
                    if (plan.Selection.Size == 0 && !session.RequiresUpdate(requested, req.Id, plan.State)) continue;
                    plans[selected++] = plan;
                    bytes += plan.Selection.Size;
                }
                topicCounts[t] = selected - start;
                if (topicCounts[t] != 0) topicCount++;
            }

            bool compact = version >= 12;
            FetchHeader(w, version, Error.None, sessionId);
            w.ArrayLength(topicCount, compact);
            int index = 0;
            for (int t = 0; t < topics.Length; t++)
            {
                int count = topicCounts[t];
                if (count == 0) continue;
                var topic = topics[t];
                if (topic.Name is null) w.Uuid(topic.Id);
                else w.Text(topic.Name, compact);
                w.ArrayLength(count, compact);
                for (int p = 0; p < count; p++)
                {
                    var plan = plans[index++];
                    var state = await WriteSelectedFetchAsync(w, version, plan, stop);
                    session.RecordResponse(topic, plan.Partition, state);
                }
                w.EndStruct(compact);
            }
            w.EndStruct(compact);
        }
        finally
        {
            plans.AsSpan(0, selected).Clear();
            ArrayPool<FetchPlan>.Shared.Return(plans);
            ArrayPool<int>.Shared.Return(topicCounts);
        }
    }

    private FetchPlan PrepareFetch(ReplicatedTopic? topic, string? name, FetchPartition requested, string rack,
        int budget, int written, bool valid, bool readCommitted, bool allowed)
    {
        if (!allowed)
        {
            const Error error = Error.TopicAuthorizationFailed;
            return new(requested.Id, null, new(error, -1, 0, 0, 0, 0, 0), new(error, -1, -1, -1, -1));
        }
        var (replica, selection) = SelectFetchPartition(topic, name, requested, budget, written, valid, readCommitted);
        int preferred = selection.Error == Error.None ?
            cluster.PreferredReplica(topic!.Partitions[requested.Id], replica!, rack, requested.Offset) : -1;
        return new(requested.Id, replica, selection,
            new(selection.Error, selection.HighWatermark, selection.LogStartOffset, preferred, selection.LastStableOffset));
    }

    private static bool CanFetch(ReplicatedTopic? topic, string? name, Request request) =>
        (topic?.Name ?? name) is not { } resolved || request.AllowsTopic(resolved, AclOperation.Read);

    private static async ValueTask<FetchResponseState> WriteSelectedFetchAsync(WireWriter w, short version, FetchPlan plan,
        CancellationToken stop)
    {
        var state = plan.State;
        w.I32(plan.Partition);
        int start = w.Length;
        WriteFetchPartition(w, version, state, plan.Selection.Size, plan.Selection.Aborted);
        if (plan.Selection.Size > 0)
        {
            Error error = Error.None;
            try
            {
                if (!await plan.Replica!.CopyAsync(plan.Selection, w.ReserveMemory(plan.Selection.Size), stop, zstdAllowed: version >= 10))
                    error = Error.UnsupportedCompressionType;
            }
            catch (Exception e) when (e is IOException or InvalidDataException) { error = Error.KafkaStorageError; }
            catch (ObjectDisposedException) { error = Error.UnknownTopicOrPartition; }
            if (error != Error.None)
            {
                w.Truncate(start);
                state = new(error, -1, -1, -1);
                WriteFetchPartition(w, version, state, 0, null);
            }
        }
        w.EndStruct(version >= 12);
        return state;
    }

    private static ReplicatedTopic? FetchTopicFrom(ClusterMetadataImage image, string? name, Guid id) => name is null ?
        image.TopicById(id) : image.Topics.GetValueOrDefault(name);

    private (ReplicatedPartition? Replica, ReplicatedPartition.Selection Selection) SelectFetchPartition(ReplicatedTopic? topic,
        string? name, FetchPartition req, int budget, int written, bool valid, bool readCommitted, bool reportAborted = true)
    {
        var replica = GetReplica(topic, req.Id);
        Error error = !valid || req.Max < 0 ? Error.InvalidFetchSize : topic is null && name is null ? Error.UnknownTopicId :
            topic is null || (uint)req.Id >= topic.Partitions.Length ? Error.UnknownTopicOrPartition :
            replica is null ? MissingLocalReplica(topic, req.Id) : EpochError(replica, req.Epoch);
        if (error != Error.None) return (replica, new(error, -1, 0, 0, 0, 0, 0));
        var selection = replica!.Select(req.Offset, Math.Min(req.Max, budget - written), written == 0, readCommitted, reportAborted);
        if (selection.Error == Error.OffsetOutOfRange && req.Offset > selection.HighWatermark && !replica.IsLeader)
            selection = selection with { Error = Error.OffsetNotAvailable };
        return (replica, selection);
    }

    private static void FetchHeader(WireWriter w, short version, Error error, int sessionId)
    {
        w.I32(0);
        if (version < 7) return;
        w.Write(error);
        w.I32(sessionId);
    }

    private static void WriteFetchPartition(WireWriter w, short version, FetchResponseState state, int size,
        ReplicatedPartition.AbortedTransaction[]? aborted)
    {
        bool compact = version >= 12;
        w.Write(state.Error);
        w.I64(state.HighWatermark);
        w.I64(state.LastStableOffset);
        if (version >= 5) w.I64(state.LogStartOffset);
        w.ArrayLength(aborted?.Length ?? -1, compact);
        if (aborted is not null)
        {
            foreach (var transaction in aborted)
            {
                w.I64(transaction.ProducerId);
                w.I64(transaction.FirstOffset);
                w.EndStruct(compact);
            }
        }
        if (version >= 11) w.I32(state.PreferredReplica);
        w.ArrayLength(size, compact);
    }

    private static Error EpochError(ReplicatedPartition? replica, int requested)
    {
        if (replica?.Leader is null) return Error.LeaderNotAvailable;
        if (requested < 0) return Error.None;
        int current = replica.LeaderEpoch;
        return requested == current ? Error.None : requested > current ? Error.UnknownLeaderEpoch : Error.FencedLeaderEpoch;
    }

    private void OffsetForLeaderEpoch(ref WireReader r, WireWriter w, Request request)
    {
        bool compact = request.Flexible;
        int requester = request.Version >= 3 ? r.I32() : -1;
        int topicCount = r.ArrayLength(compact);
        w.I32(0);
        w.ArrayLength(topicCount, compact);
        var image = cluster.Metadata.Image;
        for (int t = 0; t < topicCount; t++)
        {
            string name = r.Text(compact);
            var topic = image.Topics.GetValueOrDefault(name);
            bool authorized = request.AllowsTopic(name, AclOperation.Describe) || request.AllowsCluster(AclOperation.ClusterAction);
            w.Text(name, compact);
            int partitionCount = r.ArrayLength(compact);
            w.ArrayLength(partitionCount, compact);
            for (int p = 0; p < partitionCount; p++)
            {
                int partition = r.I32(), current = r.I32(), requested = r.I32();
                r.EndStruct(compact);
                var replica = GetReplica(topic, partition);
                int epoch = replica?.LeaderEpoch ?? -1;
                Error error = requester >= 0 || requester < -2 || current < -1 || requested < -1 ? Error.InvalidRequest :
                    !authorized ? Error.TopicAuthorizationFailed :
                    topic is null || (uint)partition >= topic.Partitions.Length ? Error.UnknownTopicOrPartition :
                    replica?.IsLeader != true ? Error.NotLeaderOrFollower :
                    current >= 0 && current != epoch ? (current > epoch ? Error.UnknownLeaderEpoch : Error.FencedLeaderEpoch) : Error.None;
                var (foundEpoch, offset) = error == Error.None ? replica!.EndOffsetForEpoch(requested, epoch) : (-1, -1L);
                if (error == Error.None && (!replica!.IsLeader || epoch != replica.LeaderEpoch)) error = Error.NotLeaderOrFollower;
                w.Write(error);
                w.I32(partition);
                w.I32(error == Error.None ? foundEpoch : -1);
                w.I64(error == Error.None ? offset : -1);
                w.EndStruct(compact);
            }
            r.EndStruct(compact);
            w.EndStruct(compact);
        }
        r.EndStruct(compact);
        r.End();
        w.EndStruct(compact);
    }

    private async ValueTask<bool> ListOffsetsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request, CancellationToken stop)
    {
        short version = request.Version;
        bool compact = version >= 6;
        var r = new WireReader(body.Span);
        int requester = r.I32();
        sbyte isolation = version >= 2 ? r.I8() : (sbyte)0;
        var topics = new (string Name, (int Id, int Epoch, long Timestamp)[] Partitions)[r.ArrayLength(compact)];
        for (int i = 0; i < topics.Length; i++)
        {
            string name = r.Text(compact);
            var partitions = new (int Id, int Epoch, long Timestamp)[r.ArrayLength(compact)];
            for (int p = 0; p < partitions.Length; p++)
            {
                partitions[p] = (r.I32(), version >= 4 ? r.I32() : -1, r.I64());
                r.EndStruct(compact);
            }
            topics[i] = (name, partitions);
            r.EndStruct(compact);
        }
        int timeout = version >= 10 ? r.I32() : 0;
        r.EndStruct(compact);
        r.End();

        if (version >= 2) w.I32(0);
        w.ArrayLength(topics.Length, compact);
        foreach (var (name, partitions) in topics)
        {
            w.Text(name, compact);
            w.ArrayLength(partitions.Length, compact);
            var topic = cluster.Metadata.Image.Topics.GetValueOrDefault(name);
            bool authorized = request.AllowsTopic(name, AclOperation.Describe);
            foreach (var (partition, epoch, timestamp) in partitions)
            {
                var replica = GetReplica(topic, partition);
                Error error = requester is not (-1 or -2) || isolation is not (0 or 1) || timeout < 0 ||
                    !Router.ValidListOffsetTimestamp(timestamp, version) ? Error.InvalidRequest :
                    !authorized ? Error.TopicAuthorizationFailed :
                    topic is null || (uint)partition >= topic.Partitions.Length ? Error.UnknownTopicOrPartition :
                    replica is null ? MissingLocalReplica(topic, partition) : !replica.IsLeader ? Error.NotLeaderOrFollower : EpochError(replica, epoch);
                var result = error == Error.None && timestamp is not (-5 or -6) ?
                    await replica!.OffsetForAsync(timestamp, stop, isolation == 1) : (error, -1L, -1L);
                w.I32(partition);
                w.Write(result.Item1);
                w.I64(result.Item2);
                w.I64(result.Item3);
                if (version >= 4) w.I32(result.Item3 < 0 ? -1 : replica!.LeaderEpoch);
                w.EndStruct(compact);
            }
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
        return true;
    }
}
