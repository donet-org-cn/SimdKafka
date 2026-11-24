using System.Collections.Immutable;
using DotNext.Net.Cluster.Consensus.Raft;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    // Accessed under _groupWrites. Only the metadata leader maintains session timers.
    private readonly Dictionary<(string Group, string Member), long> _consumerLeases = [];
    private long _consumerTerm = -1;

    private async ValueTask<bool> ConsumerHeartbeatAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        var r = new WireReader(body.Span);
        string group = r.Text(true), member = r.Text(true);
        if (!request.AllowsGroup(group, AclOperation.Read))
            return WriteConsumerHeartbeat(w, new(Error.GroupAuthorizationFailed));
        int epoch = r.I32();
        string? instance = r.NullableText(true), rack = r.NullableText(true);
        int rebalance = r.I32();
        int count = r.ArrayLength(true, nullable: true);
        string[]? topics = count < 0 ? null : new string[count];
        for (int i = 0; i < count; i++) topics![i] = r.Text(true);
        string? pattern = request.Version >= 1 ? r.NullableText(true) : null;
        string? assignor = r.NullableText(true);
        count = r.ArrayLength(true, nullable: true);
        var owned = count < 0 ? null : new List<ConsumerPartition>();
        for (int i = 0; i < count; i++)
        {
            Guid topic = r.Uuid();
            int partitions = r.ArrayLength(true);
            for (int p = 0; p < partitions; p++) owned!.Add(new(topic, r.I32()));
            r.Tags();
        }
        r.Tags();
        r.End();

        if (request.Version == 0 && epoch == 0 && member.Length == 0)
            member = Guid.NewGuid().ToString("D");
        var command = new ConsumerGroupCommand(ConsumerGroupOperation.Heartbeat, member, epoch, instance, rack, rebalance,
            topics, assignor, owned?.ToArray(), BrokerCluster.TransactionTime(), request.ClientId, request.ClientHost, pattern,
            request.Access?.Subject);
        MetadataResult result;
        await _groupWrites.WaitAsync(stop);
        try
        {
            var leadership = GroupLeadership();
            if (leadership.IsCancellationRequested) result = new(Error.NotCoordinator);
            else if (_groups.HasMembersOrPending(group)) result = new(Error.InconsistentGroupProtocol);
            else
            {
                Error ready = await EnsureConsumerTermAsync(leadership, stop);
                result = new(ready);
                if (ready == Error.None)
                {
                    var image = cluster.Metadata.Image;
                    var preview = ReplicatedMetadata.ApplyConsumerGroup(image, group, command);
                    result = ReferenceEquals(preview.Next, image) ? preview.Result :
                        await ExecuteConsumerGroupAsync(group, command, leadership, stop);
                    if (leadership.IsCancellationRequested) result = new(Error.NotCoordinator);
                    if (result.Error == Error.None)
                    {
                        var key = (group, member);
                        if (epoch == -1) _consumerLeases.Remove(key);
                        else _consumerLeases[key] = Environment.TickCount64 + ReplicatedMetadata.ConsumerSessionMs;
                    }
                }
            }
        }
        finally { _groupWrites.Release(); }
        return WriteConsumerHeartbeat(w, result);
    }

    private bool WriteConsumerHeartbeat(WireWriter w, MetadataResult result)
    {
        w.I32(0);
        w.Write(result.Error);
        w.Text(result.Error == Error.InvalidRegularExpression ? "Invalid subscription regular expression" : null, true);
        w.Text(result.Consumer?.Member, true);
        w.I32(result.Consumer?.Epoch ?? -1);
        w.I32(result.Error == Error.None ? ReplicatedMetadata.ConsumerHeartbeatMs : 0);
        if (result.Consumer is { } response && !response.Assignment.IsDefault)
        {
            w.I8(1);
            WriteConsumerAssignment(w, response.Assignment, cluster.Metadata.Image, includeName: false);
        }
        else w.I8(-1);
        w.UVar(0);
        return true;
    }

    private async ValueTask<MetadataResult> ExecuteConsumerGroupAsync(string group, ConsumerGroupCommand command,
        CancellationToken leadership, CancellationToken stop, int timeoutMs = 10_000)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(stop, leadership);
        deadline.CancelAfter(timeoutMs);
        try
        {
            var result = await cluster.Metadata.ExecuteAsync(new(MetadataOperation.ConsumerGroup, group, ConsumerGroup: command),
                deadline.Token);
            return result.Error == Error.NotController || leadership.IsCancellationRequested ? new(Error.NotCoordinator) : result;
        }
        catch (OperationCanceledException) when (!stop.IsCancellationRequested)
        { return new(leadership.IsCancellationRequested ? Error.NotCoordinator : Error.RequestTimedOut); }
        catch (QuorumUnreachableException) { return new(Error.CoordinatorNotAvailable); }
        catch (NotLeaderException) { return new(Error.NotCoordinator); }
    }

    private async ValueTask<Error> EnsureConsumerTermAsync(CancellationToken leadership, CancellationToken stop)
    {
        long term = cluster.Metadata.Term;
        if (_consumerTerm == term) return Error.None;
        Error ready = await CoordinatorReadAsync(stop);
        if (leadership.IsCancellationRequested) return Error.NotCoordinator;
        if (ready != Error.None) return ready;
        _consumerLeases.Clear();
        _consumerTerm = term;
        return Error.None;
    }

    private async Task RunConsumerGroupClockAsync(CancellationToken stop)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
        try
        {
            while (await timer.WaitForNextTickAsync(stop))
            {
                if (cluster.Metadata.Image.ConsumerGroups.IsEmpty || !cluster.Metadata.IsLeader) continue;
                await _groupWrites.WaitAsync(stop);
                try
                {
                    var leadership = GroupLeadership();
                    if (leadership.IsCancellationRequested || await EnsureConsumerTermAsync(leadership, stop) != Error.None)
                        continue;
                    var groups = cluster.Metadata.Image.ConsumerGroups;
                    foreach (var (key, _) in _consumerLeases)
                        if (groups.GetValueOrDefault(key.Group)?.Members.ContainsKey(key.Member) != true)
                            _consumerLeases.Remove(key);
                    long tick = Environment.TickCount64, now = BrokerCluster.TransactionTime();
                    int expired = 0;
                    foreach (var (group, state) in groups)
                    {
                        foreach (var member in state.Members.Values)
                        {
                            if (expired >= 4 || leadership.IsCancellationRequested) break;
                            var key = (group, member.Id);
                            if (!_consumerLeases.TryGetValue(key, out long until))
                                _consumerLeases[key] = until = tick + ReplicatedMetadata.ConsumerSessionMs;
                            bool revokeTimeout = member.RevokingSince > 0 && now - member.RevokingSince >= member.RebalanceMs;
                            if (tick < until && !revokeTimeout) continue;
                            var command = new ConsumerGroupCommand(ConsumerGroupOperation.Expire, member.Id, member.Epoch);
                            if ((await ExecuteConsumerGroupAsync(group, command, leadership, stop, timeoutMs: 1000)).Error == Error.None)
                                _consumerLeases.Remove(key);
                            expired++;
                        }
                        if (expired >= 4 || leadership.IsCancellationRequested) break;
                    }
                }
                finally { _groupWrites.Release(); }
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
    }

    private async ValueTask<bool> DescribeConsumerGroupsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        var r = new WireReader(body.Span);
        var names = Router.TextArray(ref r, compact: true);
        bool authorized = r.Bool();
        r.Tags();
        r.End();
        Error coordinator = await CoordinatorReadAsync(stop);
        var image = cluster.Metadata.Image;
        w.I32(0);
        w.ArrayLength(names.Length, true);
        foreach (var name in names)
        {
            Error error = name.Length == 0 ? Error.InvalidGroupId : !request.AllowsGroup(name, AclOperation.Describe) ?
                Error.GroupAuthorizationFailed : coordinator;
            var group = error == Error.None ? image.ConsumerGroups.GetValueOrDefault(name) : null;
            if (error == Error.None && group is null) error = Error.GroupIdNotFound;
            w.Write(error);
            w.Text(null, true);
            w.Text(name, true);
            w.Text(group?.State ?? "", true);
            w.I32(group?.Epoch ?? -1);
            w.I32(group?.Epoch ?? -1);
            w.Text(group?.Assignor ?? "", true);
            w.ArrayLength(group?.Members.Count ?? 0, true);
            if (group is not null)
                foreach (var member in group.Members.Values)
                {
                    w.Text(member.Id, true);
                    w.Text(member.Instance, true);
                    w.Text(member.Rack, true);
                    w.I32(member.Suspended ? -2 : member.Epoch);
                    w.Text(member.ClientId, true);
                    w.Text(member.ClientHost, true);
                    w.ArrayLength(member.Topics.Length, true);
                    foreach (string topic in member.Topics) w.Text(topic, true);
                    w.Text(member.Pattern, true);
                    WriteConsumerAssignment(w, member.Assigned, image, includeName: true);
                    WriteConsumerAssignment(w, member.Target, image, includeName: true);
                    if (request.Version >= 1) w.I8(1);
                    w.UVar(0);
                }
            w.I32(authorized && error == Error.None ? request.Access?.Operations(AclResource.Group, name) ??
                ((1 << 3) | (1 << 6) | (1 << 8)) : int.MinValue);
            w.UVar(0);
        }
        w.UVar(0);
        return true;
    }

    private static void WriteConsumerAssignment(WireWriter w, ImmutableArray<ConsumerPartition> assignment,
        ClusterMetadataImage image, bool includeName)
    {
        var topics = assignment.GroupBy(static item => item.TopicId).ToArray();
        w.ArrayLength(topics.Length, true);
        foreach (var topic in topics)
        {
            w.Uuid(topic.Key);
            if (includeName) w.Text(image.TopicById(topic.Key)?.Name ?? "", true);
            w.ArrayLength(topic.Count(), true);
            foreach (var partition in topic) w.I32(partition.Partition);
            w.UVar(0);
        }
        w.UVar(0);
    }

    private Error ValidateGroupCommit(string group, string member, int epoch, string? instance) =>
        cluster.Metadata.Image.ConsumerGroups.TryGetValue(group, out var consumer) ?
            ReplicatedMetadata.ValidateConsumerMember(consumer, member, epoch, instance) :
            _groups.ValidateCommit(group, member, epoch, instance);
}
