namespace SimdKafka;

internal sealed record GroupProtocol(string Name, byte[] Metadata);
internal sealed record JoinRequest(string Group, string Member, string Type, int SessionMs, int RebalanceMs,
    bool RequireMemberId, GroupProtocol[] Protocols, string ClientId = "", string ClientHost = "", string? Instance = null);
internal sealed record JoinResult(Error Error, string Member = "", int Generation = -1, string Protocol = "",
    string Leader = "", (string Id, string? Instance, byte[] Metadata)[]? Members = null);
internal sealed record SyncRequest(string Group, string Member, int Generation, Dictionary<string, byte[]> Assignments, string? Instance = null);
internal sealed record GroupMemberSnapshot(string Id, string ClientId, string ClientHost, byte[] Metadata, byte[] Assignment, string? Instance);
internal sealed record GroupSnapshot(string Id, string State, string Type, string Protocol, GroupMemberSnapshot[] Members,
    string GroupType = "classic");

internal sealed class GroupCoordinator
{
    private const int MaxMembers = 64;
    private const int MaxGroups = 1024;
    private readonly object _gate = new();
    private readonly Dictionary<string, Group> _groups = new(StringComparer.Ordinal);
    private readonly HashSet<string> _deleting = new(StringComparer.Ordinal);

    private enum Phase { Empty, Preparing, Completing, Stable }

    private sealed class Group
    {
        internal readonly Dictionary<string, Member> Members = new(StringComparer.Ordinal);
        internal readonly Dictionary<string, string> Instances = new(StringComparer.Ordinal);
        internal readonly Dictionary<string, long> Pending = new(StringComparer.Ordinal);
        internal Phase Phase;
        internal int Generation;
        internal long ReadyAt;
        internal long Deadline;
        internal string Leader = "";
        internal string Protocol = "";
    }

    private sealed class Member(string id, JoinRequest request, long now)
    {
        internal string Id { get; set; } = id;
        internal string? Instance { get; } = request.Instance;
        internal string ClientId = request.ClientId;
        internal string ClientHost = request.ClientHost;
        internal GroupProtocol[] Protocols = request.Protocols;
        internal int Session = request.SessionMs;
        internal int Rebalance = request.RebalanceMs;
        internal long Expires = now + request.SessionMs;
        internal TaskCompletionSource<JoinResult>? Joining;
        internal TaskCompletionSource<(Error, byte[])>? Syncing;
        internal byte[] Assignment = [];
    }

    internal Task<JoinResult> Join(JoinRequest req)
    {
        if (req.Group.Length == 0) return Task.FromResult(new JoinResult(Error.InvalidGroupId));
        if (req.Instance is { } instance && !Catalog.ValidName(instance)) return Task.FromResult(new JoinResult(Error.InvalidRequest));
        if (req.SessionMs is < 1000 or > 300_000 || req.RebalanceMs is < 1 or > 1_800_000)
            return Task.FromResult(new JoinResult(Error.InvalidSessionTimeout));
        if (req.Type != "consumer" || req.Protocols.Length == 0)
            return Task.FromResult(new JoinResult(Error.InconsistentGroupProtocol));
        lock (_gate)
        {
            if (_deleting.Contains(req.Group)) return Task.FromResult(new JoinResult(Error.RebalanceInProgress));
            if (!_groups.TryGetValue(req.Group, out var group))
            {
                if (req.Member.Length != 0) return Task.FromResult(new JoinResult(Error.UnknownMemberId));
                if (_groups.Count >= MaxGroups) return Task.FromResult(new JoinResult(Error.GroupMaxSizeReached));
                _groups[req.Group] = group = new();
            }
            long now = Environment.TickCount64;
            string id = req.Member;
            string? previous = req.Instance is null ? null : group.Instances.GetValueOrDefault(req.Instance);
            if (id.Length != 0 && previous is not null && previous != id)
                return Task.FromResult(new JoinResult(Error.FencedInstanceId));
            bool replace = id.Length == 0 && previous is not null;
            if (id.Length == 0)
            {
                if (!replace && group.Members.Count + group.Pending.Count >= MaxMembers)
                    return Task.FromResult(new JoinResult(Error.GroupMaxSizeReached));
                id = $"simdkafka-{Guid.NewGuid():N}";
                if (req.RequireMemberId && req.Instance is null)
                {
                    group.Pending.Add(id, now + req.SessionMs);
                    return Task.FromResult(new JoinResult(Error.MemberIdRequired, id));
                }
            }
            else if (group.Members.TryGetValue(id, out var registered))
            {
                if (registered.Instance != req.Instance) return Task.FromResult(new JoinResult(Error.FencedInstanceId));
            }
            else if (req.Instance is not null || !group.Pending.Remove(id))
            {
                return Task.FromResult(new JoinResult(Error.UnknownMemberId));
            }

            bool common = req.Protocols.Any(protocol => group.Members.Values.All(member =>
                member.Id == (previous ?? id) || Supports(member, protocol.Name)));
            if (!common) return Task.FromResult(new JoinResult(Error.InconsistentGroupProtocol, id));

            if (replace)
            {
                var prior = group.Members[previous!];
                group.Members.Remove(previous!);
                prior.Joining?.TrySetResult(new(Error.FencedInstanceId, previous!));
                prior.Syncing?.TrySetResult((Error.FencedInstanceId, []));
                prior.Joining = null;
                prior.Syncing = null;
                prior.Id = id;
                group.Members.Add(id, prior);
                group.Instances[req.Instance!] = id;
                if (group.Leader == previous) group.Leader = id;
                if (group.Phase == Phase.Stable && SameProtocols(prior.Protocols, req.Protocols))
                {
                    Refresh(prior, req, now);
                    // An identical static member restart retains its assignment and existing generation.
                    return Task.FromResult(Result(group, prior));
                }
            }
            if (!group.Members.TryGetValue(id, out var joining))
            {
                joining = new Member(id, req, now);
                group.Members.Add(id, joining);
                if (req.Instance is not null) group.Instances.Add(req.Instance, id);
            }
            else
            {
                if (!replace && group.Phase == Phase.Completing && SameProtocols(joining.Protocols, req.Protocols))
                    return Task.FromResult(Result(group, joining));
                Refresh(joining, req, now);
            }
            Prepare(group, now);
            joining.Joining ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
            Task<JoinResult> response = joining.Joining.Task;
            Advance(group, now);
            return response;
        }
    }

    private static void Refresh(Member member, JoinRequest request, long now)
    {
        member.Protocols = request.Protocols;
        member.ClientId = request.ClientId;
        member.ClientHost = request.ClientHost;
        member.Session = request.SessionMs;
        member.Rebalance = request.RebalanceMs;
        member.Expires = now + member.Session;
    }

    internal Task<(Error Error, byte[] Assignment)> Sync(SyncRequest req)
    {
        lock (_gate)
        {
            var error = Validate(req.Group, req.Member, req.Generation, req.Instance, out var group, out var member);
            if (error != Error.None || group!.Phase == Phase.Preparing)
                return Task.FromResult((error != Error.None ? error : Error.RebalanceInProgress, Array.Empty<byte>()));
            member!.Expires = Environment.TickCount64 + member.Session;
            if (group.Phase == Phase.Stable) return Task.FromResult((Error.None, member.Assignment));
            if (member.Id == group.Leader)
            {
                if (req.Assignments.Keys.Any(id => !group.Members.ContainsKey(id)))
                    return Task.FromResult((Error.UnknownMemberId, Array.Empty<byte>()));
                foreach (var candidate in group.Members.Values)
                {
                    candidate.Assignment = req.Assignments.GetValueOrDefault(candidate.Id) ?? [];
                    if (candidate.Instance is null || candidate.Syncing is not null || candidate == member)
                        candidate.Expires = Environment.TickCount64 + candidate.Session;
                    candidate.Syncing?.TrySetResult((Error.None, candidate.Assignment));
                    candidate.Syncing = null;
                }
                group.Phase = Phase.Stable;
                return Task.FromResult((Error.None, member.Assignment));
            }
            member.Syncing ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
            return member.Syncing.Task;
        }
    }

    internal Error Heartbeat(string id, string memberId, int generation, string? instance = null)
    {
        lock (_gate)
        {
            var error = Validate(id, memberId, generation, instance, out var group, out var member);
            if (error != Error.None) return error;
            member!.Expires = Environment.TickCount64 + member.Session;
            return group!.Phase == Phase.Stable ? Error.None : Error.RebalanceInProgress;
        }
    }

    internal Error Leave(string id, string memberId, string? instance = null)
    {
        lock (_gate)
        {
            if (!_groups.TryGetValue(id, out var group)) return Error.UnknownMemberId;
            if (instance is not null)
            {
                if (!group.Instances.TryGetValue(instance, out string? current)) return Error.UnknownMemberId;
                if (memberId.Length > 0 && memberId != current) return Error.FencedInstanceId;
                memberId = current;
            }
            else if (group.Members.GetValueOrDefault(memberId)?.Instance is not null) return Error.FencedInstanceId;
            if (group.Pending.Remove(memberId)) return Error.None;
            if (!Remove(group, memberId)) return Error.UnknownMemberId;
            Prepare(group, Environment.TickCount64);
            return Error.None;
        }
    }

    internal Error Commit(string id, string memberId, int generation, List<CommittedOffset> offsets, Catalog catalog, string? instance = null)
    {
        lock (_gate)
        {
            var error = ValidateCommitLocked(id, memberId, generation, instance);
            return error == Error.None ? catalog.Commit(id, offsets) : error;
        }
    }

    internal Error ValidateCommit(string id, string memberId, int generation, string? instance = null)
    {
        lock (_gate) return ValidateCommitLocked(id, memberId, generation, instance);
    }

    private Error ValidateCommitLocked(string id, string memberId, int generation, string? instance)
    {
        if (id.Length == 0) return Error.InvalidGroupId;
        if (generation < 0)
            return generation != -1 || memberId.Length != 0 || instance is not null ||
                (_groups.TryGetValue(id, out var active) && active.Members.Count > 0) ? Error.UnknownMemberId : Error.None;
        var error = Validate(id, memberId, generation, instance, out var group, out _);
        return error != Error.None ? error : group!.Phase == Phase.Stable ? Error.None : Error.RebalanceInProgress;
    }

    internal void Reset()
    {
        lock (_gate)
        {
            foreach (var group in _groups.Values)
                foreach (var member in group.Members.Values)
                {
                    member.Joining?.TrySetResult(new(Error.NotCoordinator));
                    member.Syncing?.TrySetResult((Error.NotCoordinator, []));
                }
            _groups.Clear();
        }
    }

    internal GroupSnapshot[] List(Catalog catalog)
    {
        lock (_gate) return List(catalog.OffsetGroups());
    }

    internal GroupSnapshot[] List(IEnumerable<string> offsetGroups)
    {
        lock (_gate)
        {
            var ids = _groups.Keys.Union(offsetGroups, StringComparer.Ordinal);
            return ids.Select(id => DescribeLocked(id, _groups.GetValueOrDefault(id), includeMembers: false)).ToArray();
        }
    }

    internal GroupSnapshot? Describe(string id, Catalog catalog)
    {
        lock (_gate) return Describe(id, catalog.HasOffsets(id));
    }

    internal GroupSnapshot? Describe(string id, bool hasOffsets)
    {
        lock (_gate)
        {
            var group = _groups.GetValueOrDefault(id);
            return group is not null || hasOffsets ? DescribeLocked(id, group, includeMembers: true) : null;
        }
    }

    internal bool HasMembersOrPending(string id)
    {
        lock (_gate)
            return _groups.TryGetValue(id, out var group) && (group.Members.Count > 0 || group.Pending.Count > 0);
    }

    internal Error BeginDelete(string id, bool offsetsOnly, out bool hasGroup)
    {
        hasGroup = false;
        if (id.Length == 0) return Error.InvalidGroupId;
        lock (_gate)
        {
            var group = _groups.GetValueOrDefault(id);
            hasGroup = group is not null;
            if (group is not null && (group.Members.Count > 0 || group.Pending.Count > 0))
                return offsetsOnly ? Error.GroupSubscribedToTopic : Error.NonEmptyGroup;
            return _deleting.Add(id) ? Error.None : Error.RebalanceInProgress;
        }
    }

    internal void EndDelete(string id, bool deleted)
    {
        lock (_gate)
        {
            if (deleted) _groups.Remove(id);
            _deleting.Remove(id);
        }
    }

    internal Error Delete(string id, Catalog catalog, HashSet<(string Topic, int Partition)>? selected = null)
    {
        if (string.IsNullOrEmpty(id)) return Error.InvalidGroupId;
        lock (_gate)
        {
            var group = _groups.GetValueOrDefault(id);
            if (group is not null && (group.Members.Count != 0 || group.Pending.Count != 0))
                return selected is null ? Error.NonEmptyGroup : Error.GroupSubscribedToTopic;

            var result = catalog.DeleteOffsets(id, selected);
            if (selected is null && (result == Error.None || (result == Error.GroupIdNotFound && group is not null)))
            {
                _groups.Remove(id);
                return Error.None;
            }
            return result;
        }
    }

    private static GroupSnapshot DescribeLocked(string id, Group? group, bool includeMembers)
    {
        string state = group?.Phase switch
        {
            Phase.Preparing => "PreparingRebalance",
            Phase.Completing => "CompletingRebalance",
            Phase.Stable => "Stable",
            _ => "Empty"
        };
        if (group is null || group.Members.Count == 0) return new(id, state, "", "", []);
        GroupMemberSnapshot[] members = includeMembers ? group.Members.Values.Select(member =>
            new GroupMemberSnapshot(member.Id, member.ClientId, member.ClientHost,
                member.Protocols.FirstOrDefault(protocol => protocol.Name == group.Protocol)?.Metadata ?? [],
                member.Assignment, member.Instance)).ToArray() : [];
        return new(id, state, "consumer", group.Protocol, members);
    }

    private Error Validate(string id, string memberId, int generation, string? instance, out Group? group, out Member? member)
    {
        member = null;
        if (!_groups.TryGetValue(id, out group)) return Error.UnknownMemberId;
        if (instance is not null && group.Instances.TryGetValue(instance, out var mapped) && mapped != memberId)
            return Error.FencedInstanceId;
        if (!group.Members.TryGetValue(memberId, out member)) return Error.UnknownMemberId;
        if (member.Instance != instance) return Error.FencedInstanceId;
        return generation == group.Generation ? Error.None : Error.IllegalGeneration;
    }

    private static bool Supports(Member member, string protocol) =>
        Array.Exists(member.Protocols, item => item.Name == protocol);

    private static bool SameProtocols(GroupProtocol[] a, GroupProtocol[] b)
    {
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++)
            if (a[i].Name != b[i].Name || !a[i].Metadata.AsSpan().SequenceEqual(b[i].Metadata)) return false;
        return true;
    }

    private static bool Remove(Group group, string id)
    {
        if (!group.Members.Remove(id, out var member)) return false;
        if (member.Instance is not null) group.Instances.Remove(member.Instance);
        member.Joining?.TrySetResult(new(Error.UnknownMemberId, id));
        member.Syncing?.TrySetResult((Error.UnknownMemberId, []));
        return true;
    }

    private static void Prepare(Group group, long now)
    {
        if (group.Members.Count == 0)
        {
            group.Phase = Phase.Empty;
            return;
        }
        if (group.Phase == Phase.Preparing) return;
        group.ReadyAt = now + (group.Phase == Phase.Empty ? 150 : 50);
        group.Deadline = now + Math.Max(150, group.Members.Values.Max(static member => member.Rebalance));
        group.Phase = Phase.Preparing;
        foreach (var member in group.Members.Values)
        {
            member.Syncing?.TrySetResult((Error.RebalanceInProgress, []));
            member.Syncing = null;
        }
    }

    private static JoinResult Result(Group group, Member member)
    {
        var members = member.Id == group.Leader
            ? group.Members.Values.Select(m => (m.Id, m.Instance, m.Protocols.First(p => p.Name == group.Protocol).Metadata)).ToArray()
            : null;
        return new(Error.None, member.Id, group.Generation, group.Protocol, group.Leader, members);
    }

    private static void Advance(Group group, long now)
    {
        if (group.Phase == Phase.Preparing && now >= group.ReadyAt)
        {
            if (now < group.Deadline && group.Members.Values.Any(static m => m.Instance is null && m.Joining is null)) return;
            foreach (var member in group.Members.Values)
                if (member.Joining is null && member.Instance is null) Remove(group, member.Id);
            if (group.Members.Count == 0)
            {
                group.Phase = Phase.Empty;
                return;
            }
            if (group.Members.GetValueOrDefault(group.Leader)?.Joining is null)
            {
                var active = group.Members.Values.FirstOrDefault(static m => m.Joining is not null);
                if (active is null) return;
                group.Leader = active.Id;
            }
            var common = group.Members[group.Leader].Protocols.Where(p => group.Members.Values.All(m => Supports(m, p.Name)))
                .Select(p => p.Name).ToHashSet(StringComparer.Ordinal);
            group.Protocol = group.Members.Values.Select(m => m.Protocols.First(p => common.Contains(p.Name)).Name)
                .GroupBy(static p => p).MaxBy(static vote => vote.Count())!.Key;
            group.Generation++;
            group.Phase = Phase.Completing;
            group.Deadline = now + group.Members.Values.Max(static member => member.Rebalance);
            foreach (var member in group.Members.Values)
            {
                if (member.Instance is null || member.Joining is not null) member.Expires = now + member.Session;
                member.Joining?.TrySetResult(Result(group, member));
                member.Joining = null;
            }
        }
        else if (group.Phase == Phase.Completing && now >= group.Deadline)
        {
            Prepare(group, now);
        }
    }

    internal async Task RunClockAsync(CancellationToken stop)
    {
        using var clock = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        try
        {
            while (await clock.WaitForNextTickAsync(stop))
            {
                lock (_gate)
                {
                    long now = Environment.TickCount64;
                    foreach (var (id, group) in _groups)
                    {
                        foreach (var pending in group.Pending)
                            if (pending.Value <= now) group.Pending.Remove(pending.Key);
                        bool removed = false;
                        foreach (var member in group.Members.Values)
                            if (member.Joining is null && member.Syncing is null && member.Expires <= now)
                                removed |= Remove(group, member.Id);
                        if (removed) Prepare(group, now);
                        Advance(group, now);
                        if (group.Members.Count == 0 && group.Pending.Count == 0) _groups.Remove(id);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
    }
}
