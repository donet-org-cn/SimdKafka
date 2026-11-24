using System.Text;

namespace SimdKafka;

internal sealed partial class Router
{
    private async ValueTask<bool> ProduceAsync(Memory<byte> body, WireWriter w, Request request)
    {
        short version = request.Version;
        bool compact = version >= 9;
        var (acks, transactional, timeout, topics, cursor) = ValidateProduce(body.Span, version);
        w.ArrayLength(topics, compact);
        for (int t = 0; t < topics; t++)
        {
            var (topic, partitions, consumed, allowed) = ReadTopic(body.Span[cursor..], w, request);
            var settings = catalog.Configurations;
            bool logAppendTime = topic is not null && settings.UsesLogAppendTime(topic.Id);
            var timestampBounds = topic is null ? null : settings.ProduceTimestampBounds(topic.Id, logAppendTime);
            cursor += consumed;
            for (int p = 0; p < partitions; p++)
            {
                var (index, start, length, total) = ReadProducePartition(body.Span[cursor..], compact);
                start += cursor;
                cursor += total;
                var log = GetPartition(topic, index);
                AppendResult result;
                if (acks is not (-1 or 0 or 1) || timeout < 0) result = new(Error.InvalidRequest, -1);
                else if (!allowed) result = new(Error.TopicAuthorizationFailed, -1);
                else if (transactional is not null) result = new(Error.InvalidTxnState, -1);
                else if (log is null) result = new(topic is null && version >= 13 ? Error.UnknownTopicId : Error.UnknownTopicOrPartition, -1);
                else if (length <= 0) result = new(Error.CorruptMessage, -1);
                else if (length > settings.TopicLimit(topic!.Id, BrokerOptions.NodeId))
                    result = new(Error.MessageTooLarge, -1);
                else if (version < 7 && length >= RecordBatch.HeaderSize && (body.Span[start + 22] & 7) == 4)
                    result = new(Error.UnsupportedCompressionType, -1);
                else
                {
                    result = await log.AppendAsync(body.Slice(start, length), catalog.LastProducerId, logAppendTime, timestampBounds);
                    if (result.Error == Error.None) _changes.Signal();
                }
                WriteProducePartition(w, version, index, acks, result, log?.LogStartOffset ?? -1);
            }
            if (compact) cursor += ReadTags(body.Span[cursor..]);
            w.EndStruct(compact);
        }
        w.I32(request.Quota?.Charge(request.BodyOffset + body.Length) ?? 0);
        w.EndStruct(compact);
        return acks != 0;
    }

    internal static (short Acks, string? Transactional, int Timeout, int Topics, int Position)
        ValidateProduce(ReadOnlySpan<byte> body, short version)
    {
        var r = new WireReader(body);
        bool compact = version >= 9;
        var transactional = r.NullableText(compact);
        short acks = r.I16();
        int timeout = r.I32();
        int topics = r.ArrayLength(compact);
        int position = body.Length - r.Remaining;
        for (int t = 0; t < topics; t++)
        {
            if (version >= 13) r.Take(16);
            else r.StringBytes(compact);
            int partitions = r.ArrayLength(compact);
            for (int p = 0; p < partitions; p++)
            {
                ReadProducePartition(ref r, compact, out _, out _);
                r.EndStruct(compact);
            }
            r.EndStruct(compact);
        }
        r.EndStruct(compact);
        r.End();
        return (acks, transactional, timeout, topics, position);
    }

    private (Topic? Topic, int Partitions, int Consumed, bool Allowed) ReadTopic(ReadOnlySpan<byte> body, WireWriter w, Request request)
    {
        short version = request.Version;
        bool compact = version >= 9;
        var r = new WireReader(body);
        Topic? topic;
        bool allowed;
        if (version >= 13)
        {
            Guid id = r.Uuid();
            w.Uuid(id);
            topic = catalog.Get(id);
            allowed = topic is null || request.AllowsTopic(topic.Name, AclOperation.Write);
        }
        else
        {
            var name = r.StringBytes(compact);
            w.StringBytes(name, compact);
            topic = catalog.Get(name);
            allowed = request.Access is null || request.AllowsTopic(topic?.Name ?? Encoding.UTF8.GetString(name), AclOperation.Write);
        }
        int partitions = r.ArrayLength(compact);
        w.ArrayLength(partitions, compact);
        return (topic, partitions, body.Length - r.Remaining, allowed);
    }

    private static void ReadProducePartition(ref WireReader r, bool compact, out int index, out int length)
    {
        index = r.I32();
        length = compact ? checked((int)r.UVar()) - 1 : r.I32();
        if (length != -1) r.Take(length);
    }

    internal static (int Index, int Start, int Length, int Total) ReadProducePartition(ReadOnlySpan<byte> body, bool compact)
    {
        var r = new WireReader(body);
        ReadProducePartition(ref r, compact, out int index, out int length);
        int start = body.Length - r.Remaining - Math.Max(0, length);
        r.EndStruct(compact);
        return (index, start, length, body.Length - r.Remaining);
    }

    internal static int ReadTags(ReadOnlySpan<byte> body)
    {
        var r = new WireReader(body);
        r.Tags();
        return body.Length - r.Remaining;
    }

    internal static void WriteProducePartition(WireWriter w, short version, int index, short acks, AppendResult result,
        long logStart = 0)
    {
        // The receive buffer stays leased until all writes have finished, even on disconnect.
        if (acks == 0 && result.Error != Error.None) throw new WireException("Unacknowledged produce was rejected");
        bool compact = version >= 9;
        w.I32(index);
        w.Write(result.Error);
        w.I64(result.Offset);
        w.I64(result.LogAppendTime);
        if (version >= 5) w.I64(result.Error == Error.None ? logStart : -1);
        if (version >= 8)
        {
            w.ArrayLength(0, compact);
            w.Text(null, compact);
        }
        w.EndStruct(compact);
    }

    private async ValueTask<bool> FetchAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request, CancellationToken stop)
    {
        short v = request.Version;
        var head = ParseFetchHeader(body.Span, v);
        if (head.SessionId != 0)
        {
            bool compact = v >= 12;
            w.I32(0);
            w.Write(Error.FetchSessionIdNotFound);
            w.I32(0);
            w.ArrayLength(0, compact);
            w.EndStruct(compact);
            return true;
        }
        int start = w.Length;
        long deadline = Environment.TickCount64 + head.WaitMs;
        while (true)
        {
            Task? changed = head.WaitMs > 0 && head.MinBytes > 0 ? _changes.Capture() : null;
            var (bytes, error) = Fetch(body.Span[head.BodyOffset..], w, request, head.MaxBytes, head.Valid);
            long remaining = deadline - Environment.TickCount64;
            if (error || bytes >= head.MinBytes || remaining <= 0 || changed is null)
            {
                if (request.Quota is { } quota) w.PatchI32(start, quota.Charge(w.Length));
                return true;
            }
            try
            {
                await changed.WaitAsync(TimeSpan.FromMilliseconds(remaining), stop);
            }
            catch (TimeoutException)
            {
                // Read once more for appends at the deadline.
            }
            w.Truncate(start);
        }
    }

    internal static (int WaitMs, int MinBytes, int MaxBytes, int BodyOffset, bool Valid, int SessionId, int SessionEpoch, bool ReadCommitted) ParseFetchHeader(
        ReadOnlySpan<byte> body, short version)
    {
        var r = new WireReader(body);
        int replica = r.I32();
        int wait = r.I32();
        int min = r.I32();
        int max = r.I32();
        sbyte isolation = r.I8();
        int session = version >= 7 ? r.I32() : 0;
        int epoch = version >= 7 ? r.I32() : -1;
        bool valid = replica == -1 && wait >= 0 && min >= 0 && max >= 0 && isolation is 0 or 1;
        return (Math.Clamp(wait, 0, 30_000), Math.Clamp(min, 0, BrokerOptions.MaxFetchBytes),
            Math.Clamp(max, 0, BrokerOptions.MaxFetchBytes), body.Length - r.Remaining, valid, session, epoch, isolation == 1);
    }

    private (int Bytes, bool Error) Fetch(ReadOnlySpan<byte> body, WireWriter w, Request request, int max, bool valid)
    {
        short v = request.Version;
        var r = new WireReader(body);
        bool compact = v >= 12;
        bool byId = v >= 13;
        w.I32(0);
        if (v >= 7)
        {
            w.Write(Error.None);
            w.I32(0);
        }
        int count = r.ArrayLength(compact);
        w.ArrayLength(count, compact);
        int total = 0;
        bool errors = !valid;
        for (int t = 0; t < count; t++)
        {
            Topic? topic;
            bool allowed;
            if (byId)
            {
                var id = r.Uuid();
                w.Uuid(id);
                topic = catalog.Get(id);
                allowed = topic is null || request.AllowsTopic(topic.Name, AclOperation.Read);
            }
            else
            {
                var name = r.StringBytes(compact);
                w.StringBytes(name, compact);
                topic = catalog.Get(name);
                allowed = request.Access is null || request.AllowsTopic(topic?.Name ?? Encoding.UTF8.GetString(name), AclOperation.Read);
            }
            int partitions = r.ArrayLength(compact);
            w.ArrayLength(partitions, compact);
            for (int p = 0; p < partitions; p++)
            {
                int index = r.I32();
                int epoch = v >= 9 ? r.I32() : -1;
                long offset = r.I64();
                if (compact) r.I32(); // Last fetched epoch. There are no earlier leader epochs on this broker.
                if (v >= 5) r.I64(); // replica log start offset
                int partitionMax = r.I32();
                r.EndStruct(compact);
                var log = GetPartition(topic, index);
                var missing = byId && topic is null ? Error.UnknownTopicId : Error.UnknownTopicOrPartition;
                var (error, highWatermark, position, size, logStart) = !valid || partitionMax < 0 ?
                    (Error.InvalidFetchSize, -1L, 0L, 0, -1L) : !allowed ? (Error.TopicAuthorizationFailed, -1L, 0L, 0, -1L) : log is null ?
                    (missing, -1L, 0L, 0, -1L) : epoch > 0 ?
                    (Error.UnknownLeaderEpoch, -1L, 0L, 0, -1L) :
                    log.Select(offset, Math.Min(partitionMax, max - total), total == 0);

                w.I32(index);
                int dataStart = w.Length;
                Header(error, highWatermark, size, logStart);
                if (size != 0)
                {
                    try
                    {
                        if (log!.ReadVerified(w.Reserve(size), position, offset, highWatermark, zstdAllowed: v >= 10))
                            total += size;
                        else
                        {
                            w.Truncate(dataStart);
                            error = Error.UnsupportedCompressionType;
                            Header(error, highWatermark, 0, logStart);
                        }
                    }
                    catch (IOException)
                    {
                        w.Truncate(dataStart);
                        error = Error.KafkaStorageError;
                        Header(error, -1, 0, -1);
                    }
                    catch (TopicRetiredException)
                    {
                        w.Truncate(dataStart);
                        error = byId ? Error.UnknownTopicId : Error.UnknownTopicOrPartition;
                        Header(error, -1, 0, -1);
                    }
                }
                w.EndStruct(compact);
                errors |= error != Error.None;
            }
            r.EndStruct(compact);
            w.EndStruct(compact);
        }
        if (v >= 7)
        {
            int forgotten = r.ArrayLength(compact);
            for (int i = 0; i < forgotten; i++)
            {
                if (byId) r.Take(16);
                else r.StringBytes(compact);
                r.Take(r.ArrayLength(compact) * sizeof(int));
                r.EndStruct(compact);
            }
        }
        if (v >= 11) r.StringBytes(compact);
        r.EndStruct(compact);
        r.End();
        w.EndStruct(compact);
        return (total, errors);

        void Header(Error error, long end, int size, long start)
        {
            w.Write(error);
            w.I64(end);
            w.I64(end); // With no transactions, LSO equals high watermark.
            if (v >= 5) w.I64(start);
            w.ArrayLength(-1, compact); // nullable aborted_transactions
            if (v >= 11) w.I32(-1); // preferred read replica
            w.ArrayLength(size, compact);
        }
    }

    private void OffsetForLeaderEpoch(ref WireReader r, WireWriter w, Request request)
    {
        bool compact = request.Flexible;
        int requester = request.Version >= 3 ? r.I32() : -1;
        int topics = r.ArrayLength(compact);
        w.I32(0);
        w.ArrayLength(topics, compact);
        for (int t = 0; t < topics; t++)
        {
            string name = r.Text(compact);
            var topic = catalog.Get(name);
            bool allowed = request.AllowsTopic(name, AclOperation.Describe) || request.AllowsCluster(AclOperation.ClusterAction);
            w.Text(name, compact);
            int partitions = r.ArrayLength(compact);
            w.ArrayLength(partitions, compact);
            for (int p = 0; p < partitions; p++)
            {
                int partition = r.I32(), current = r.I32(), epoch = r.I32();
                r.EndStruct(compact);
                var log = GetPartition(topic, partition);
                Error error = requester is not (-1 or -2) || current < -1 || epoch < -1 ? Error.InvalidRequest :
                    !allowed ? Error.TopicAuthorizationFailed : log is null ? Error.UnknownTopicOrPartition :
                    current > 0 ? Error.UnknownLeaderEpoch : Error.None;
                long offset = -1;
                if (error == Error.None && epoch == 0)
                    (error, _, offset) = log!.OffsetFor(-1);
                w.Write(error);
                w.I32(partition);
                w.I32(offset >= 0 ? 0 : -1);
                w.I64(offset);
                w.EndStruct(compact);
            }
            r.EndStruct(compact);
            w.EndStruct(compact);
        }
        r.EndStruct(compact); r.End();
        w.EndStruct(compact);
    }

    private void ListOffsets(ref WireReader r, WireWriter w, Request request)
    {
        short v = request.Version;
        bool compact = request.Flexible;
        int timeout = v >= 10 ? ReadListOffsetsTimeout(r) : 0;
        long deadline = timeout > 0 ? Environment.TickCount64 + timeout : long.MaxValue;
        int replica = r.I32();
        sbyte isolation = v >= 2 ? r.I8() : (sbyte)0;
        if (v >= 2) w.I32(0);
        int count = r.ArrayLength(compact);
        w.ArrayLength(count, compact);
        for (int t = 0; t < count; t++)
        {
            var name = r.StringBytes(compact);
            var topic = catalog.Get(name);
            bool allowed = request.Access is null || request.AllowsTopic(topic?.Name ?? Encoding.UTF8.GetString(name), AclOperation.Describe);
            w.StringBytes(name, compact);
            int partitions = r.ArrayLength(compact);
            w.ArrayLength(partitions, compact);
            for (int p = 0; p < partitions; p++)
            {
                int index = r.I32();
                int epoch = v >= 4 ? r.I32() : -1;
                long time = r.I64();
                int maxOffsets = v == 0 ? r.I32() : 1;
                r.EndStruct(compact);
                var log = GetPartition(topic, index);
                var (error, timestamp, offset) = replica is not (-1 or -2) || isolation is not (0 or 1) || maxOffsets < 1 ||
                    !ValidListOffsetTimestamp(time, v) || timeout < 0
                    ? (Error.InvalidRequest, -1L, -1L)
                    : !allowed ? (Error.TopicAuthorizationFailed, -1L, -1L)
                    : log is null ? (Error.UnknownTopicOrPartition, -1L, -1L)
                    : epoch > 0 ? (Error.UnknownLeaderEpoch, -1L, -1L)
                    : time == -6 ? catalog.TopicTier(topic!)?.Enabled == true ? log.OffsetFor(-4) : (Error.None, -1L, -1L)
                    : LookupOffset(log, time, deadline);
                w.I32(index);
                w.Write(error);
                if (v == 0)
                {
                    w.I32(offset < 0 ? 0 : 1);
                    if (offset >= 0) w.I64(offset);
                }
                else
                {
                    w.I64(timestamp);
                    w.I64(offset);
                    if (v >= 4) w.I32(offset < 0 ? -1 : 0);
                }
                w.EndStruct(compact);
            }
            r.EndStruct(compact);
            w.EndStruct(compact);
        }
        if (v >= 10) r.I32();
        r.EndStruct(compact);
        w.EndStruct(compact);
        r.End();
    }

    private static int ReadListOffsetsTimeout(WireReader r)
    {
        r.I32(); r.I8();
        int count = r.ArrayLength(true);
        for (int i = 0; i < count; i++)
        {
            r.StringBytes(true);
            int partitions = r.ArrayLength(true);
            for (int p = 0; p < partitions; p++)
            {
                r.Take(sizeof(int) * 2 + sizeof(long));
                r.Tags();
            }
            r.Tags();
        }
        return r.I32();
    }

    internal static bool ValidListOffsetTimestamp(long timestamp, short version) => timestamp >= (version switch
    {
        >= 11 => -6,
        >= 9 => -5,
        8 => -4,
        7 => -3,
        _ => -2
    });

    private static (Error, long, long) LookupOffset(PartitionLog log, long timestamp, long deadline)
    {
        bool timed = deadline != long.MaxValue && log.ArchivedOffset > log.LogStartOffset && (timestamp >= 0 || timestamp == -3);
        if (timed && Environment.TickCount64 >= deadline) return (Error.RequestTimedOut, -1, -1);
        var result = log.OffsetFor(timestamp);
        return timed && result.Error == Error.None && Environment.TickCount64 >= deadline ?
            (Error.RequestTimedOut, -1, -1) : result;
    }
}

internal sealed class LogChanges
{
    private readonly object _gate = new();
    private TaskCompletionSource? _next;

    internal Task Capture()
    {
        lock (_gate) return (_next ??= new(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
    }

    internal void Signal()
    {
        TaskCompletionSource? wake;
        lock (_gate)
        {
            wake = _next;
            _next = null;
        }
        wake?.SetResult();
    }
}
