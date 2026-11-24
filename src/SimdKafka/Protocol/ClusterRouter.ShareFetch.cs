using System.Buffers;
using System.Buffers.Binary;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    private readonly ShareSessionCache _shareSessions = new();

    private sealed record ShareRequest(string Group, string Member, int SessionEpoch, int Wait, int MinBytes, int MaxBytes,
        int MaxRecords, bool AcknowledgeOnly, bool Valid, Dictionary<ConsumerPartition, ShareAck[]> Topics,
        ConsumerPartition[] Forgotten);

    private async ValueTask<bool> ShareFetchAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request, CancellationToken stop)
    {
        var data = ReadShareRequest(body.Span, request);
        if (!data.Valid) { EmptyShareResponse(w, request, Error.InvalidRequest); return true; }
        if (!request.AllowsGroup(data.Group, AclOperation.Read))
        { EmptyShareResponse(w, request, Error.GroupAuthorizationFailed); return true; }
        Error status = await RefreshConfigurationsAsync(stop);
        if (status != Error.None) { EmptyShareResponse(w, request, status); return true; }
        var image = cluster.Metadata.Image;
        long now = BrokerCluster.TransactionTime();
        string principal = SharePrincipal(request);
        var group = image.ShareGroups.GetValueOrDefault(data.Group);
        if (group?.Active(data.Member, principal, now) is not { } member)
        { EmptyShareResponse(w, request, Error.UnknownMemberId); return true; }
        var sessionError = _shareSessions.Begin(data.Group, member.Cycle, data.SessionEpoch, data.AcknowledgeOnly,
            [.. data.Topics.Keys], data.Forgotten, out var handle, out var partitions);
        if (sessionError != Error.None) { EmptyShareResponse(w, request, sessionError); return true; }
        try
        {
            Task changed = cluster.Changes.Capture();
            if (data.Wait > 0 && data.MinBytes > 0 && data.SessionEpoch >= 0 && !data.AcknowledgeOnly &&
                data.Topics.Values.All(static acks => acks.Length == 0) && !ShareFetchReady(image, group, partitions, request, now))
            {
                try { await changed.WaitAsync(TimeSpan.FromMilliseconds(data.Wait), stop); }
                catch (TimeoutException) { }
            }
            int throttle = w.Length;
            await WriteShareDataAsync(w, request, data, partitions, principal, stop);
            if (request.Quota is { } quota) w.PatchI32(throttle, quota.Charge(w.Length));
            return true;
        }
        finally { _shareSessions.End(data.Group, member.Cycle, handle!, data.SessionEpoch == -1); }
    }

    private static ShareRequest ReadShareRequest(ReadOnlySpan<byte> body, Request request)
    {
        var r = new WireReader(body);
        string? group = r.NullableText(true), member = r.NullableText(true);
        int epoch = r.I32();
        bool acknowledge = request.Api == Api.ShareAcknowledge;
        int wait = 0, minBytes = 0, maxBytes = 0, maxRecords = 0;
        sbyte mode = 0;
        if (!acknowledge)
        {
            wait = r.I32(); minBytes = r.I32(); maxBytes = r.I32(); maxRecords = r.I32();
            r.I32(); // Client batch-size hint. Acquisition is bounded by records and encoded bytes.
            if (request.Version >= 2) mode = r.I8();
        }
        bool renew = request.Version >= 2 && r.Bool();
        int topics = r.ArrayLength(true);
        var requested = new Dictionary<ConsumerPartition, ShareAck[]>();
        bool valid = group is { Length: > 0 and <= 512 } && member is { Length: > 0 and <= 512 } &&
            wait >= 0 && minBytes >= 0 && maxBytes >= 0 && maxRecords >= 0 && mode is 0 or 1;
        int operations = 0;
        for (int t = 0; t < topics; t++)
        {
            Guid topic = r.Uuid();
            int partitions = r.ArrayLength(true);
            for (int p = 0; p < partitions; p++)
            {
                int partition = r.I32();
                int count = r.ArrayLength(true);
                if (count > ShareGroups.MaxInFlight || requested.Count >= BrokerOptions.MaxPartitions)
                    throw new WireException("Share request exceeds partition or acknowledgment limit");
                var acks = new ShareAck[count];
                for (int i = 0; i < count; i++)
                {
                    long first = r.I64(), last = r.I64();
                    int types = r.ArrayLength(true);
                    operations += types;
                    if (operations > 32_768) throw new WireException("Too many share acknowledgments");
                    var codes = new sbyte[types];
                    for (int j = 0; j < types; j++)
                    {
                        codes[j] = r.I8();
                        if (codes[j] == 4 && !renew) valid = false;
                    }
                    acks[i] = new(first, last, codes); r.Tags();
                }
                valid &= requested.TryAdd(new(topic, partition), acks);
                r.Tags();
            }
            r.Tags();
        }
        var forgotten = new HashSet<ConsumerPartition>();
        if (!acknowledge)
        {
            int count = r.ArrayLength(true);
            for (int i = 0; i < count; i++)
            {
                Guid topic = r.Uuid();
                int partitions = r.ArrayLength(true);
                for (int p = 0; p < partitions; p++)
                {
                    forgotten.Add(new(topic, r.I32()));
                    if (forgotten.Count > BrokerOptions.MaxPartitions) throw new WireException("Too many forgotten share partitions");
                }
                r.Tags();
            }
        }
        r.Tags(); r.End();
        return new(group ?? "", member ?? "", epoch, Math.Min(wait, 30_000), minBytes,
            Math.Min(maxBytes, BrokerOptions.MaxFetchBytes), Math.Min(maxRecords, ShareGroups.MaxInFlight), acknowledge,
            valid, requested, [.. forgotten]);
    }

    private bool ShareFetchReady(ClusterMetadataImage image, ShareGroupState group, ConsumerPartition[] partitions, Request request, long now)
    {
        foreach (var (id, partition) in partitions)
        {
            var topic = image.TopicById(id);
            if (topic is null || !request.AllowsTopic(topic.Name, AclOperation.Read) || GetReplica(topic, partition) is not { IsLeader: true } replica)
                return true;
            var state = group.Partitions.GetValueOrDefault(ShareGroupState.PartitionKey(id, partition));
            if (state is null || state.FirstCandidate(replica.HighWatermark, now) < replica.HighWatermark) return true;
        }
        return false;
    }

    private async ValueTask WriteShareDataAsync(WireWriter w, Request request, ShareRequest data, ConsumerPartition[] partitions,
        string principal, CancellationToken stop)
    {
        WriteShareDataHeader(w, request, Error.None);
        var byTopic = partitions.GroupBy(static partition => partition.TopicId).ToArray();
        w.ArrayLength(byTopic.Length, true);
        int bytes = data.MaxBytes, records = data.MaxRecords;
        bool emitted = false;
        var leaders = new HashSet<int>();
        foreach (var parts in byTopic)
        {
            w.Uuid(parts.Key);
            w.ArrayLength(parts.Count(), true);
            foreach (var key in parts)
            {
                var image = cluster.Metadata.Image;
                var topic = image.TopicById(key.TopicId);
                var assignment = topic is not null && (uint)key.Partition < topic.Partitions.Length ? topic.Partitions[key.Partition] : null;
                var (leader, epoch) = assignment is null ? (-1, -1) : cluster.FindLeader(assignment);
                var replica = GetReplica(topic, key.Partition);
                Error status = topic is null ? Error.UnknownTopicId : !request.AllowsTopic(topic.Name, AclOperation.Read) ?
                    Error.TopicAuthorizationFailed : assignment is null ? Error.UnknownTopicOrPartition :
                    replica?.IsLeader != true ? Error.NotLeaderOrFollower : Error.None;
                var ack = data.Topics.GetValueOrDefault(key) ?? [];
                Error acknowledged = ack.Length == 0 ? Error.None : status;
                ShareRange[] acquired = [];
                byte[]? buffer = null;
                int size = 0;
                try
                {
                    if (status == Error.None)
                    {
                        if (ack.Length != 0)
                        {
                            var applied = await ExecuteShareAsync(data.Group, Work(ShareAction.Acknowledge, replica!) with { Acks = ack }, stop);
                            acknowledged = SharePartitionError(applied.Error);
                            if (acknowledged == Error.None) cluster.Changes.Signal();
                        }
                        if (!data.AcknowledgeOnly && data.SessionEpoch != -1 && bytes > 0 && records > 0)
                        {
                            var state = cluster.Metadata.Image.ShareGroups.GetValueOrDefault(data.Group)?.Partitions
                                .GetValueOrDefault(ShareGroupState.PartitionKey(key.TopicId, key.Partition));
                            if (state is null || replica!.LogStartOffset > state.Start)
                            {
                                var initialized = await ExecuteShareAsync(data.Group, Work(ShareAction.Acquire, replica!), stop);
                                status = SharePartitionError(initialized.Error);
                                state = cluster.Metadata.Image.ShareGroups.GetValueOrDefault(data.Group)?.Partitions
                                    .GetValueOrDefault(ShareGroupState.PartitionKey(key.TopicId, key.Partition));
                            }
                            if (status == Error.None && state is not null)
                            {
                                long high = replica!.HighWatermark;
                                long current = Math.Max(replica.LogStartOffset, state.FirstCandidate(high, BrokerCluster.TransactionTime()));
                                if (current < high && current - state.Start < ShareGroups.MaxInFlight)
                                {
                                    var selection = replica.Select(current, bytes, allowOversize: !emitted, reportAborted: false);
                                    status = selection.Error;
                                    if (selection.Size != 0)
                                    {
                                        buffer = ArrayPool<byte>.Shared.Rent(selection.Size);
                                        await replica.CopyAsync(selection, buffer.AsMemory(0, selection.Size), stop);
                                        var ranges = ReadShareRanges(buffer.AsSpan(0, selection.Size), current, state.Start + ShareGroups.MaxInFlight);
                                        var result = await ExecuteShareAsync(data.Group, Work(ShareAction.Acquire, replica) with
                                        { Records = ranges, Limit = records, LogEnd = selection.HighWatermark }, stop);
                                        status = SharePartitionError(result.Error);
                                        acquired = result.Share?.Ranges ?? [];
                                        if (acquired.Length > 0 && status == Error.None)
                                        {
                                            size = selection.Size;
                                            bytes -= size;
                                            records -= acquired.Sum(static item => (int)(item.Last - item.First + 1));
                                            emitted = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                catch (IOException) { status = Error.KafkaStorageError; }
                catch (ObjectDisposedException) { status = Error.NotLeaderOrFollower; }
                finally
                {
                    if (status == Error.NotLeaderOrFollower && leader >= 0) leaders.Add(leader);
                    w.I32(key.Partition);
                    WriteShareError(w, data.AcknowledgeOnly && status == Error.None ? acknowledged : status);
                    if (!data.AcknowledgeOnly) WriteShareError(w, acknowledged);
                    w.I32(leader); w.I32(epoch); w.UVar(0);
                    if (!data.AcknowledgeOnly)
                    {
                        w.Bytes(buffer.AsSpan(0, size), true);
                        w.ArrayLength(acquired.Length, true);
                        foreach (var range in acquired)
                        { w.I64(range.First); w.I64(range.Last); w.I16(range.Count); w.UVar(0); }
                    }
                    w.UVar(0);
                    if (buffer is not null) ArrayPool<byte>.Shared.Return(buffer);
                }

                ShareCommand Work(ShareAction action, ReplicatedPartition local) =>
                    new(action, data.Member, principal, Now: BrokerCluster.TransactionTime(), TopicId: key.TopicId,
                        Partition: key.Partition, LeaderId: cluster.Local.Id, LeaderEpoch: local.LeaderEpoch,
                        LogStart: local.LogStartOffset, LogEnd: local.HighWatermark);
            }
            w.UVar(0);
        }
        var endpoints = cluster.Members.Where(member => leaders.Contains(member.Id)).ToArray();
        w.ArrayLength(endpoints.Length, true);
        foreach (var member in endpoints)
        { w.I32(member.Id); w.Text(member.Host, true); w.I32(member.Port); w.Text(member.Rack, true); w.UVar(0); }
        w.UVar(0);
    }

    private static ShareRange[] ReadShareRanges(ReadOnlySpan<byte> bytes, long current, long exclusive)
    {
        var ranges = new List<ShareRange>();
        while (!bytes.IsEmpty)
        {
            int size = RecordBatch.SizeFromPrefix(bytes);
            long start = BinaryPrimitives.ReadInt64BigEndian(bytes);
            if (start >= exclusive) break;
            int lastDelta = BinaryPrimitives.ReadInt32BigEndian(bytes[23..]);
            long end = Math.Min(start + lastDelta, exclusive - 1);
            if (end >= current) ranges.Add(new(Math.Max(start, current), end, (bytes[22] & 32) == 0 ? (short)0 : (short)-1));
            bytes = bytes[size..];
        }
        return [.. ranges];
    }

    private static Error SharePartitionError(Error error) => error == Error.NotController ? Error.NotLeaderOrFollower : error;

    private static void EmptyShareResponse(WireWriter w, Request request, Error error)
    {
        WriteShareDataHeader(w, request, error);
        w.ArrayLength(0, true); w.ArrayLength(0, true); w.UVar(0);
    }

    private static void WriteShareDataHeader(WireWriter w, Request request, Error error)
    {
        w.I32(0);
        WriteShareError(w, error);
        if (request.Api == Api.ShareFetch || request.Version >= 2) w.I32(ShareGroups.LockMs);
    }
}
