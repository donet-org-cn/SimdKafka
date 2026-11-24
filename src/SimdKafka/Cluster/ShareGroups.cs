using System.Collections.Immutable;

namespace SimdKafka;

internal enum ShareAction : byte { Heartbeat, Acquire, Acknowledge, Reset, DeleteOffsets, DeleteGroup }
internal sealed record ShareCommand(ShareAction Action, string Member = "", string Principal = "", int Epoch = 0,
    long Now = 0, Guid Cycle = default, string[]? Topics = null, string? Rack = null, string ClientId = "", string ClientHost = "",
    Guid TopicId = default, int Partition = -1, int LeaderId = -1, int LeaderEpoch = -1, long LogStart = 0, long LogEnd = 0,
    ShareRange[]? Records = null, ShareAck[]? Acks = null, int Limit = 0, long Offset = -1);
internal readonly record struct ShareRange(long First, long Last, short Count = 0);
internal sealed record ShareAck(long First, long Last, sbyte[] Types);
internal sealed record ShareOutcome(int Epoch = 0, ShareRange[]? Ranges = null);
internal sealed record ShareMember(Guid Cycle, string Principal, int Epoch, long Expires, string? Rack, string ClientId,
    string ClientHost, ImmutableArray<string> Topics);
internal sealed record ShareDelivery(Guid Owner, long Expires, short Count, bool Terminal = false);
internal sealed record SharePartitionState(long Start, ImmutableDictionary<long, ShareDelivery> Deliveries)
{
    internal static SharePartitionState StartingAt(long offset) => new(offset, ImmutableDictionary<long, ShareDelivery>.Empty);

    internal long FirstCandidate(long end, long now)
    {
        long offset = Start;
        while (offset < end && Deliveries.TryGetValue(offset, out var delivery) &&
            (delivery.Terminal || delivery.Owner != Guid.Empty && delivery.Expires > now)) offset++;
        return offset;
    }
}

internal sealed record ShareGroupState(int Epoch, ImmutableDictionary<string, ShareMember> Members,
    ImmutableDictionary<string, SharePartitionState> Partitions)
{
    internal static ShareGroupState Empty { get; } = new(0, ImmutableDictionary.Create<string, ShareMember>(StringComparer.Ordinal),
        ImmutableDictionary.Create<string, SharePartitionState>(StringComparer.Ordinal));

    internal static string PartitionKey(Guid topic, int partition) => $"{topic:N}:{partition}";

    internal ShareMember? Active(string member, string principal, long now) =>
        Members.GetValueOrDefault(member) is { } current && current.Expires > now && current.Principal == principal ? current : null;
}

internal static class ShareGroups
{
    internal const int HeartbeatMs = 5_000;
    internal const int SessionMs = 45_000;
    internal const int LockMs = 30_000;
    internal const int MaxDeliveries = 5;
    internal const int MaxInFlight = 2_048;
    private const int MaxGroups = 1_024;
    private const int MaxMembers = 64;

    internal static (MetadataResult Result, ClusterMetadataImage Next) Apply(ClusterMetadataImage image, string name, ShareCommand command)
    {
        if (name.Length is 0 or > 512) return (new(Error.InvalidGroupId), image);
        var existing = image.ShareGroups.GetValueOrDefault(name);
        var group = existing ?? ShareGroupState.Empty;
        if (image.ConsumerGroups.ContainsKey(name) || image.Offsets.ContainsKey(name) || existing is null &&
            image.Transactions.Values.Any(transaction => transaction.StagedOffsets.ContainsKey(name) || transaction.Groups.Contains(name)))
            return (new(Error.InconsistentGroupProtocol), image);
        if (command.Action == ShareAction.Heartbeat) return Heartbeat();
        if (command.Action is ShareAction.Reset or ShareAction.DeleteOffsets or ShareAction.DeleteGroup) return Admin();
        if (group.Active(command.Member, command.Principal, command.Now) is not { } member)
            return (new(Error.UnknownMemberId), image);
        var topic = image.TopicById(command.TopicId);
        if (topic is null || (uint)command.Partition >= (uint)topic.Partitions.Length)
            return (new(Error.UnknownTopicOrPartition), image);
        if (command.Action == ShareAction.Acquire && !member.Topics.Contains(topic.Name))
            return (new(Error.InvalidRequest), image);
        var assignment = topic.Partitions[command.Partition];
        if (assignment.LeaderId != command.LeaderId || assignment.LeaderEpoch != command.LeaderEpoch)
            return (new(Error.NotLeaderOrFollower), image);
        if (command.LogStart < 0 || command.LogEnd < command.LogStart) return (new(Error.InvalidRequest), image);
        string key = ShareGroupState.PartitionKey(topic.Id, command.Partition);
        var previous = group.Partitions.GetValueOrDefault(key);
        var state = previous ?? SharePartitionState.StartingAt(command.LogEnd);
        if (command.Action == ShareAction.Acknowledge && (previous is null || command.Acks is null))
            return (new(Error.InvalidRecordState), image);
        long start = Math.Max(state.Start, command.LogStart);
        var deliveries = state.Deliveries.ToBuilder();
        foreach (var (offset, delivery) in state.Deliveries)
        {
            if (offset < start) { deliveries.Remove(offset); continue; }
            if (!delivery.Terminal && delivery.Expires <= command.Now && delivery.Count >= MaxDeliveries)
                deliveries[offset] = delivery with { Terminal = true, Owner = default };
        }
        ShareRange[]? acquired = null;
        if (command.Action == ShareAction.Acknowledge)
        {
            if (!Acknowledge(deliveries, command.Acks!, member.Cycle, command.Now)) return (new(Error.InvalidRecordState), image);
        }
        else if (command.Action == ShareAction.Acquire)
        {
            if (command.Limit is < 0 or > MaxInFlight) return (new(Error.InvalidRequest), image);
            if (previous is not null && command.Records is { Length: > 0 } records)
                acquired = Acquire(deliveries, records, member.Cycle, command, start);
        }
        else return (new(Error.InvalidRequest), image);
        while (deliveries.TryGetValue(start, out var next) && next.Terminal) deliveries.Remove(start++);
        var updated = new SharePartitionState(start, deliveries.ToImmutable());
        return (new(Error.None, Share: new(Ranges: acquired)), updated == previous ? image :
            image with { ShareGroups = image.ShareGroups.SetItem(name, group with { Partitions = group.Partitions.SetItem(key, updated) }) });

        (MetadataResult, ClusterMetadataImage) Heartbeat()
        {
            if (command.Member.Length is 0 or > 512 || command.Epoch < -1 || command.Now < 0 ||
                command.Topics is { } topics && (topics.Length > BrokerOptions.MaxPartitions || topics.Any(static item => !Catalog.ValidName(item))))
                return (new(Error.InvalidRequest), image);
            var live = group.Members.RemoveRange(group.Members.Where(entry => entry.Value.Expires <= command.Now).Select(static entry => entry.Key));
            live.TryGetValue(command.Member, out var old);
            if (old is not null && old.Principal != command.Principal) return (new(Error.UnknownMemberId), image);
            if (command.Epoch == -1)
                return old is null ? (new(Error.UnknownMemberId), image) :
                    Save(ReleaseLocks(group with { Members = live.Remove(command.Member), Epoch = checked(group.Epoch + 1) }), new(-1));
            if (command.Epoch == 0 && command.Topics is null) return (new(Error.InvalidRequest), image);
            if (command.Epoch != 0 && old is null) return (new(Error.UnknownMemberId), image);
            if (command.Epoch != 0 && command.Epoch != old?.Epoch) return (new(Error.FencedMemberEpoch), image);
            if (old is null && command.Cycle == Guid.Empty) return (new(Error.InvalidRequest), image);
            if (existing is null && image.ShareGroups.Count >= MaxGroups || old is null && live.Count >= MaxMembers)
                return (new(Error.GroupMaxSizeReached), image);
            var names = command.Topics is null ? old!.Topics : command.Topics.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToImmutableArray();
            bool changed = old is null || !old.Topics.AsSpan().SequenceEqual(names.AsSpan()) || group.Members.Count != live.Count;
            int epoch = changed ? checked(group.Epoch + 1) : group.Epoch;
            var next = new ShareMember(old?.Cycle ?? command.Cycle, command.Principal, old is null || changed ? epoch : old.Epoch,
                command.Now + SessionMs, command.Rack ?? old?.Rack, command.ClientId, command.ClientHost, names);
            var updated = new ShareGroupState(epoch, live.SetItem(command.Member, next), group.Partitions);
            return Save(group.Members.Count == live.Count ? updated : ReleaseLocks(updated), new(next.Epoch));
        }

        (MetadataResult, ClusterMetadataImage) Admin()
        {
            if (group.Members.Values.Any(member => member.Expires > command.Now)) return (new(Error.NonEmptyGroup), image);
            if (command.Action == ShareAction.DeleteGroup) return existing is null ? (new(Error.GroupIdNotFound), image) :
                (new(Error.None), image with { ShareGroups = image.ShareGroups.Remove(name) });
            var topic = image.TopicById(command.TopicId);
            if (topic is null)
                return (new(Error.UnknownTopicOrPartition), image);
            if (command.Action == ShareAction.DeleteOffsets)
            {
                if (existing is null) return (new(Error.GroupIdNotFound), image);
                string prefix = $"{command.TopicId:N}:";
                return Save(group with { Partitions = group.Partitions.RemoveRange(group.Partitions.Keys.Where(key =>
                    key.StartsWith(prefix, StringComparison.Ordinal))) }, new());
            }
            if ((uint)command.Partition >= (uint)topic.Partitions.Length) return (new(Error.UnknownTopicOrPartition), image);
            string key = ShareGroupState.PartitionKey(command.TopicId, command.Partition);
            if (command.Offset < command.LogStart || command.Offset > command.LogEnd) return (new(Error.OffsetOutOfRange), image);
            var placement = topic.Partitions[command.Partition];
            if (placement.LeaderId != command.LeaderId || placement.LeaderEpoch != command.LeaderEpoch)
                return (new(Error.NotLeaderOrFollower), image);
            if (existing is null && image.ShareGroups.Count >= MaxGroups) return (new(Error.GroupMaxSizeReached), image);
            return Save(group with { Partitions = group.Partitions.SetItem(key, SharePartitionState.StartingAt(command.Offset)) }, new());
        }

        (MetadataResult, ClusterMetadataImage) Save(ShareGroupState changed, ShareOutcome outcome) =>
            (new(Error.None, Share: outcome), image with { ShareGroups = image.ShareGroups.SetItem(name, changed) });
    }

    private static ShareGroupState ReleaseLocks(ShareGroupState group)
    {
        var owners = group.Members.Values.Select(static member => member.Cycle).ToHashSet();
        var partitions = group.Partitions.ToBuilder();
        foreach (var (key, partition) in group.Partitions)
        {
            var records = partition.Deliveries.ToBuilder();
            foreach (var (offset, record) in partition.Deliveries)
                if (!record.Terminal && record.Owner != Guid.Empty && !owners.Contains(record.Owner))
                    records[offset] = new(default, 0, record.Count, record.Count >= MaxDeliveries);
            long start = partition.Start;
            while (records.TryGetValue(start, out var next) && next.Terminal) records.Remove(start++);
            partitions[key] = new(start, records.ToImmutable());
        }
        return group with { Partitions = partitions.ToImmutable() };
    }

    private static ShareRange[] Acquire(ImmutableDictionary<long, ShareDelivery>.Builder deliveries, ShareRange[] records,
        Guid owner, ShareCommand command, long start)
    {
        var result = new List<ShareRange>();
        int remaining = command.Limit;
        long ceiling = Math.Min(command.LogEnd, start + MaxInFlight);
        foreach (var segment in records)
        {
            long last = Math.Min(segment.Last, ceiling - 1);
            for (long offset = Math.Max(segment.First, start); offset <= last && remaining > 0; offset++)
            {
                var prior = deliveries.GetValueOrDefault(offset);
                if (prior is { Terminal: true } || prior is { Owner: var previousOwner, Expires: var expires } &&
                    previousOwner != Guid.Empty && expires > command.Now) continue;
                short count = (short)((prior?.Count ?? 0) + 1);
                if (segment.Count < 0 || count > MaxDeliveries)
                {
                    deliveries[offset] = new(default, 0, 0, Terminal: true);
                    continue;
                }
                deliveries[offset] = new(owner, command.Now + LockMs, count);
                if (result.Count > 0 && result[^1].Last + 1 == offset && result[^1].Count == count)
                    result[^1] = result[^1] with { Last = offset };
                else result.Add(new(offset, offset, count));
                remaining--;
            }
        }
        return [.. result];
    }

    private static bool Acknowledge(ImmutableDictionary<long, ShareDelivery>.Builder deliveries, ShareAck[] batches,
        Guid owner, long now)
    {
        int operations = 0;
        foreach (var batch in batches)
        {
            if (batch.First < 0 || batch.Last < batch.First || batch.Last - batch.First >= MaxInFlight) return false;
            int length = (int)(batch.Last - batch.First) + 1;
            if (batch.Types.Length != 1 && batch.Types.Length != length || operations + length > MaxInFlight) return false;
            operations += length;
            for (int i = 0; i < length; i++)
            {
                sbyte action = batch.Types.Length == 1 ? batch.Types[0] : batch.Types[i];
                if (action == 0) continue;
                long offset = batch.First + i;
                if (action is < 1 or > 4 || !deliveries.TryGetValue(offset, out var delivery) ||
                    delivery.Terminal || delivery.Owner != owner || delivery.Expires <= now) return false;
                deliveries[offset] = action == 4 ? delivery with { Expires = now + LockMs } :
                    new(default, 0, delivery.Count, action != 2 || delivery.Count >= MaxDeliveries);
            }
        }
        return true;
    }
}
