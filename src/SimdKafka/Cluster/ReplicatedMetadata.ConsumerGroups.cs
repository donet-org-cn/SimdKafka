using System.Collections.Immutable;
using System.Text.RegularExpressions;

namespace SimdKafka;

internal enum ConsumerGroupOperation : byte { Heartbeat, Expire, ConvertToClassic }
internal readonly record struct ConsumerPartition(Guid TopicId, int Partition);
internal sealed record ConsumerGroupCommand(ConsumerGroupOperation Operation, string Member = "", int Epoch = 0,
    string? Instance = null, string? Rack = null, int RebalanceMs = -1, string[]? Topics = null, string? Assignor = null,
    ConsumerPartition[]? Owned = null, long Now = 0, string ClientId = "", string ClientHost = "", string? Pattern = null,
    AclSubject? Subject = null);
internal readonly record struct ConsumerHeartbeat(string Member, int Epoch, ImmutableArray<ConsumerPartition> Assignment);
internal sealed record ConsumerMember(string Id, string? Instance, string? Rack, string ClientId, string ClientHost,
    int Epoch, int PreviousEpoch, int RebalanceMs, ImmutableArray<string> Topics, string? Assignor,
    ImmutableArray<ConsumerPartition> Assigned, ImmutableArray<ConsumerPartition> Revoking,
    ImmutableArray<ConsumerPartition> Target, long RevokingSince = 0, bool Suspended = false, string? Pattern = null,
    AclSubject? Subject = null)
{
    internal bool SubscribedTo(string topic, Regex? pattern) => Topics.Contains(topic) || pattern?.IsMatch(topic) == true;
}
internal sealed record ConsumerGroupState(int Epoch, long TopicVersion, string Assignor,
    ImmutableDictionary<string, ConsumerMember> Members)
{
    internal static ConsumerGroupState Empty { get; } = new(0, -1, "uniform",
        ImmutableDictionary.Create<string, ConsumerMember>(StringComparer.Ordinal));

    internal string State => Members.IsEmpty ? "Empty" :
        Members.Values.All(member => member.Epoch == Epoch && member.Revoking.IsEmpty &&
            member.Assigned.AsSpan().SequenceEqual(member.Target.AsSpan())) ? "Stable" : "Reconciling";
}

internal sealed partial class ReplicatedMetadata
{
    internal const int ConsumerHeartbeatMs = 5_000;
    internal const int ConsumerSessionMs = 45_000;
    private const int MaxConsumerGroupMembers = 64;

    internal static Error ValidateConsumerMember(ConsumerGroupState? group, string member, int epoch, string? instance,
        bool requireInstance = true)
    {
        if (group is null) return Error.GroupIdNotFound;
        if (epoch == -1 && member.Length == 0 && instance is null && group.Members.IsEmpty) return Error.None;
        if (!group.Members.TryGetValue(member, out var current) || current.Suspended) return Error.UnknownMemberId;
        if (requireInstance && current.Instance != instance) return Error.FencedInstanceId;
        return current.Epoch == epoch ? Error.None : Error.StaleMemberEpoch;
    }

    internal static (MetadataResult Result, ClusterMetadataImage Next) ApplyConsumerGroup(ClusterMetadataImage image, string name,
        ConsumerGroupCommand command)
    {
        if (name.Length == 0) return (new(Error.InvalidGroupId), image);
        if (image.ShareGroups.ContainsKey(name)) return (new(Error.InconsistentGroupProtocol), image);
        var original = image.ConsumerGroups.GetValueOrDefault(name);
        var group = original ?? ConsumerGroupState.Empty;
        group.Members.TryGetValue(command.Member, out var member);
        if (command.Operation == ConsumerGroupOperation.ConvertToClassic)
            return !group.Members.IsEmpty ? (new(Error.InconsistentGroupProtocol), image) :
                (new(Error.None), original is null ? image : image with { ConsumerGroups = image.ConsumerGroups.Remove(name) });

        if (command.Operation == ConsumerGroupOperation.Expire)
        {
            if (member is null || member.Epoch != command.Epoch) return (new(Error.None), image);
            return Save(Replan(group with { Members = group.Members.Remove(command.Member) }, image, name, changedMembership: true), null);
        }

        if (command.Member.Length == 0 || command.Instance is "" || command.Rack is "" || command.Epoch < -2 ||
            command.RebalanceMs is < -1 or 0 or > 900_000) return (new(Error.InvalidRequest), image);
        if (command.Assignor is not (null or "uniform" or "range")) return (new(Error.UnsupportedAssignor), image);
        if (command.Topics is { } names && (names.Length > BrokerOptions.MaxPartitions || names.Any(static topic => !Catalog.ValidName(topic))))
            return (new(Error.InvalidRequest), image);
        if (command.Owned is { Length: > BrokerOptions.MaxPartitions }) return (new(Error.InvalidRequest), image);

        if (command.Epoch < 0)
        {
            if (member is null) return (new(Error.UnknownMemberId), image);
            if (member.Instance != command.Instance) return (new(Error.FencedInstanceId), image);
            if (command.Epoch == -2)
            {
                if (member.Instance is null) return (new(Error.InvalidRequest), image);
                return Save(group with { Members = group.Members.SetItem(member.Id, member with { Suspended = true }) },
                    new(command.Member, command.Epoch, default));
            }
            return Save(Replan(group with { Members = group.Members.Remove(member.Id) }, image, name, changedMembership: true),
                new(command.Member, command.Epoch, default));
        }

        bool joining = command.Epoch == 0;
        if (joining && (command.RebalanceMs <= 0 || command.Topics is null && string.IsNullOrEmpty(command.Pattern) ||
            command.Owned is not { Length: 0 }))
            return (new(Error.InvalidRequest), image);
        if (member is not null && member.Instance != command.Instance)
            return (new(Error.FencedInstanceId), image);
        if (!joining)
        {
            if (member is null || member.Suspended) return (new(Error.UnknownMemberId), image);
            if (command.Epoch != member.Epoch)
            {
                if (command.Epoch == member.PreviousEpoch && command.Owned is not null &&
                    member.Subject == command.Subject && (command.Subject is null || group.TopicVersion == image.TopicVersion) &&
                    command.Owned.All(member.Assigned.Contains)) return Save(group, new(member.Id, member.Epoch, member.Assigned));
                return (new(Error.FencedMemberEpoch), image);
            }
            if (command.Owned is not null && command.Owned.Any(p => !member.Assigned.Contains(p) && !member.Revoking.Contains(p)))
                return (new(Error.FencedMemberEpoch), image);
        }

        bool membershipChanged = joining && (member is null || member.Suspended);
        if (membershipChanged && command.Instance is not null)
        {
            var previous = group.Members.Values.FirstOrDefault(item => item.Instance == command.Instance);
            if (previous is not null)
            {
                if (!previous.Suspended) return (new(Error.UnreleasedInstanceId), image);
                group = group with { Members = group.Members.Remove(previous.Id) };
                member = previous with { Id = command.Member, Epoch = 0, PreviousEpoch = -1, Suspended = false, Revoking = [] };
            }
        }
        if (member is null && group.Members.Count >= MaxConsumerGroupMembers)
            return (new(Error.GroupMaxSizeReached), image);
        member ??= new(command.Member, command.Instance, command.Rack, command.ClientId, command.ClientHost, 0, -1,
            command.RebalanceMs, [], command.Assignor, [], [], []);
        var subscriptions = command.Topics is null ? member.Topics :
            command.Topics.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToImmutableArray();
        string? pattern = command.Pattern is null ? member.Pattern : command.Pattern.Length == 0 ? null : command.Pattern;
        bool patternChanged = member.Pattern != pattern;
        if (patternChanged && pattern is not null && !ConsumerGroupPattern.TryCompile(pattern, out _))
            return (new(Error.InvalidRegularExpression), image);
        bool subscriptionChanged = patternChanged || !subscriptions.AsSpan().SequenceEqual(member.Topics.AsSpan());
        var updated = member;
        bool subjectChanged = command.Subject != member.Subject;
        if (subscriptionChanged || subjectChanged || command.RebalanceMs > 0 && command.RebalanceMs != member.RebalanceMs ||
            command.Rack is not null && command.Rack != member.Rack ||
            command.Assignor is not null && command.Assignor != member.Assignor ||
            command.ClientId != member.ClientId || command.ClientHost != member.ClientHost)
        {
            updated = member with
            {
                Topics = subscriptionChanged ? subscriptions : member.Topics,
                Pattern = pattern,
                Subject = command.Subject,
                RebalanceMs = command.RebalanceMs > 0 ? command.RebalanceMs : member.RebalanceMs,
                Rack = command.Rack ?? member.Rack,
                Assignor = command.Assignor ?? member.Assignor,
                ClientId = command.ClientId,
                ClientHost = command.ClientHost
            };
        }
        if (updated != member || membershipChanged)
            group = group with { Members = group.Members.SetItem(updated.Id, updated) };
        bool reassign = membershipChanged || subscriptionChanged || subjectChanged || updated.Assignor != member.Assignor;
        if (reassign || group.TopicVersion != image.TopicVersion) group = Replan(group, image, name, reassign);
        var current = group.Members[updated.Id];
        var assigned = Reconcile(group, current, joining ? [] : command.Owned, command.Now);
        if (assigned != current) group = group with { Members = group.Members.SetItem(current.Id, assigned) };
        bool includeAssignment = joining || command.Owned is not null || assigned.Epoch != member.Epoch ||
            !assigned.Assigned.AsSpan().SequenceEqual(member.Assigned.AsSpan());
        return Save(group, new(assigned.Id, assigned.Epoch, includeAssignment ? assigned.Assigned : default));

        (MetadataResult, ClusterMetadataImage) Save(ConsumerGroupState replacement, ConsumerHeartbeat? heartbeat) =>
            (new(Error.None, Consumer: heartbeat), ReferenceEquals(replacement, original) ?
                image : image with { ConsumerGroups = image.ConsumerGroups.SetItem(name, replacement) });
    }

    private static ConsumerGroupState Replan(ConsumerGroupState group, ClusterMetadataImage image, string groupName,
        bool changedMembership)
    {
        var members = group.Members.Values.OrderBy(static m => m.Instance ?? m.Id, StringComparer.Ordinal)
            .ThenBy(static m => m.Id, StringComparer.Ordinal).ToArray();
        string assignor = members.Where(static m => m.Assignor is not null).GroupBy(static m => m.Assignor!)
            .OrderByDescending(static votes => votes.Count()).ThenBy(static votes => votes.Key, StringComparer.Ordinal)
            .FirstOrDefault()?.Key ?? "uniform";
        var targets = members.ToDictionary(static member => member.Id, static _ => new List<ConsumerPartition>(), StringComparer.Ordinal);
        var owners = members.SelectMany(static member => member.Target.Select(partition => (partition, member.Id)))
            .ToDictionary(static target => target.partition, static target => target.Id);
        var patterns = members.Select(static member => member.Pattern).OfType<string>().Distinct(StringComparer.Ordinal)
            .ToDictionary(static pattern => pattern, static pattern => ConsumerGroupPattern.Compile(pattern), StringComparer.Ordinal);
        var permissions = members.Where(static member => member.Subject is not null).ToDictionary(static member => member.Id,
            member => member.Subject?.Authorize(image.Acls), StringComparer.Ordinal);
        var topics = image.Topics.Values.Select(topic => (Topic: topic, Eligible: members.Where(member =>
                MemberCanUseTopic(member, topic.Name)).ToArray()))
            .Where(static item => item.Eligible.Length > 0)
            .OrderBy(static item => item.Eligible.Length).ThenBy(static item => item.Topic.Name, StringComparer.Ordinal);
        foreach (var (topic, eligible) in topics)
        {
            if (assignor == "range")
            {
                int partition = 0;
                for (int member = 0; member < eligible.Length; member++)
                {
                    int end = partition + topic.Partitions.Length / eligible.Length + (member < topic.Partitions.Length % eligible.Length ? 1 : 0);
                    while (partition < end) targets[eligible[member].Id].Add(new(topic.Id, partition++));
                }
                continue;
            }
            for (int partition = 0; partition < topic.Partitions.Length; partition++)
            {
                var key = new ConsumerPartition(topic.Id, partition);
                string? previous = owners.GetValueOrDefault(key);
                var selected = eligible[0].Id;
                foreach (var candidate in eligible.AsSpan(1))
                    if (targets[candidate.Id].Count < targets[selected].Count ||
                        targets[candidate.Id].Count == targets[selected].Count && candidate.Id == previous)
                        selected = candidate.Id;
                targets[selected].Add(key);
            }
        }
        var result = group.Members.ToBuilder();
        foreach (var member in members)
        {
            var target = Ordered(targets[member.Id]);
            if (target.AsSpan().SequenceEqual(member.Target.AsSpan())) continue;
            result[member.Id] = member with { Target = target };
            changedMembership = true;
        }
        return new(changedMembership ? checked(group.Epoch + 1) : group.Epoch, image.TopicVersion, assignor, result.ToImmutable());

        bool MemberCanUseTopic(ConsumerMember member, string topic) =>
            member.SubscribedTo(topic, member.Pattern is { } pattern ? patterns[pattern] : null) &&
            (member.Subject is null || permissions[member.Id] is { } access && access.Allows(AclResource.Group, groupName, AclOperation.Read) &&
                access.Allows(AclResource.Topic, topic, AclOperation.Describe));
    }

    private static ConsumerMember Reconcile(ConsumerGroupState group, ConsumerMember member, ConsumerPartition[]? owned, long now)
    {
        if (member.Epoch == group.Epoch && member.Revoking.IsEmpty &&
            member.Assigned.AsSpan().SequenceEqual(member.Target.AsSpan())) return member;
        var revoke = Ordered(member.Revoking.Concat(member.Assigned.Except(member.Target))
            .Where(partition => owned is null || owned.Contains(partition)));
        var assigned = Ordered(member.Assigned.Intersect(member.Target));
        int epoch = member.Epoch;
        if (revoke.IsEmpty)
        {
            var held = group.Members.Values.Where(other => other.Id != member.Id)
                .SelectMany(static other => other.Assigned.Concat(other.Revoking)).ToHashSet();
            assigned = Ordered(member.Target.Where(partition => !held.Contains(partition)));
            epoch = group.Epoch;
        }
        return member with
        {
            Epoch = epoch,
            PreviousEpoch = epoch == member.Epoch ? member.PreviousEpoch : member.Epoch,
            Assigned = assigned.AsSpan().SequenceEqual(member.Assigned.AsSpan()) ? member.Assigned : assigned,
            Revoking = revoke.AsSpan().SequenceEqual(member.Revoking.AsSpan()) ? member.Revoking : revoke,
            RevokingSince = revoke.IsEmpty ? 0 : member.RevokingSince == 0 ? now : member.RevokingSince
        };
    }

    private static ImmutableArray<ConsumerPartition> Ordered(IEnumerable<ConsumerPartition> items) =>
        items.OrderBy(static item => item.TopicId).ThenBy(static item => item.Partition).ToImmutableArray();
}
