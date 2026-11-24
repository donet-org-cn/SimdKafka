using System.Collections.Immutable;
using DotNext.Net.Cluster.Consensus.Raft;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    private async ValueTask<bool> ShareHeartbeatAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request, CancellationToken stop)
    {
        var r = new WireReader(body.Span);
        string group = r.Text(true), member = r.Text(true);
        int epoch = r.I32();
        string? rack = r.NullableText(true);
        int count = r.ArrayLength(true, nullable: true);
        if (count > BrokerOptions.MaxPartitions) throw new WireException("Too many share subscriptions");
        string[]? names = count < 0 ? null : new string[count];
        for (int i = 0; i < count; i++) names![i] = r.Text(true);
        r.Tags(); r.End();

        MetadataResult result;
        if (!request.AllowsGroup(group, AclOperation.Read)) result = new(Error.GroupAuthorizationFailed);
        else
        {
            await _groupWrites.WaitAsync(stop);
            try
            {
                var leadership = GroupLeadership();
                if (leadership.IsCancellationRequested) result = new(Error.NotCoordinator);
                else if (_groups.HasMembersOrPending(group)) result = new(Error.InconsistentGroupProtocol);
                else
                {
                    var command = new ShareCommand(ShareAction.Heartbeat, member, SharePrincipal(request), epoch,
                        BrokerCluster.TransactionTime(), Guid.NewGuid(), names, rack, request.ClientId, request.ClientHost);
                    result = await ExecuteShareAsync(group, command, stop, allowForward: false);
                    if (result.Error == Error.NotController || leadership.IsCancellationRequested) result = new(Error.NotCoordinator);
                }
            }
            finally { _groupWrites.Release(); }
        }

        w.I32(0);
        WriteShareError(w, result.Error);
        w.Text(result.Error == Error.None ? member : null, true);
        w.I32(result.Share?.Epoch ?? -1);
        w.I32(result.Error == Error.None ? ShareGroups.HeartbeatMs : 0);
        var image = cluster.Metadata.Image;
        var entry = image.ShareGroups.GetValueOrDefault(group)?.Members.GetValueOrDefault(member);
        if (result.Error != Error.None || epoch == -1 || entry is null) w.I8(-1);
        else
        {
            w.I8(1);
            WriteShareAssignment(w, image, entry.Topics, includeNames: false, request);
        }
        w.UVar(0);
        return true;
    }

    private async ValueTask<bool> DescribeShareGroupsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        var r = new WireReader(body.Span);
        var names = Router.TextArray(ref r, compact: true);
        bool authorized = r.Bool();
        r.Tags(); r.End();
        Error coordinator = await CoordinatorReadAsync(stop);
        var image = cluster.Metadata.Image;
        long now = BrokerCluster.TransactionTime();
        w.I32(0);
        w.ArrayLength(names.Length, true);
        foreach (string name in names)
        {
            Error error = !request.AllowsGroup(name, AclOperation.Describe) ? Error.GroupAuthorizationFailed : coordinator;
            var group = error == Error.None ? image.ShareGroups.GetValueOrDefault(name) : null;
            if (error == Error.None && group is null) error = Error.GroupIdNotFound;
            var members = group?.Members.Where(pair => pair.Value.Expires > now).ToArray() ?? [];
            WriteShareError(w, error);
            w.Text(name, true); w.Text(group is null ? "" : members.Length == 0 ? "Empty" : "Stable", true);
            w.I32(group?.Epoch ?? -1); w.I32(group?.Epoch ?? -1); w.Text(group is null ? "" : "simple", true);
            w.ArrayLength(members.Length, true);
            foreach (var (id, member) in members)
            {
                w.Text(id, true); w.Text(member.Rack, true); w.I32(member.Epoch); w.Text(member.ClientId, true); w.Text(member.ClientHost, true);
                w.ArrayLength(member.Topics.Length, true);
                foreach (string topic in member.Topics) w.Text(topic, true);
                WriteShareAssignment(w, image, member.Topics, includeNames: true, request);
                w.UVar(0);
            }
            w.I32(authorized && error == Error.None ? request.Access?.Operations(AclResource.Group, name) ??
                ((1 << 3) | (1 << 6) | (1 << 8)) : int.MinValue);
            w.UVar(0);
        }
        w.UVar(0);
        return true;
    }

    private async ValueTask<MetadataResult> ExecuteShareAsync(string group, ShareCommand command, CancellationToken stop,
        bool allowForward = true)
    {
        using var deadline = Deadline(10_000, stop);
        try
        {
            var operation = new MetadataCommand(MetadataOperation.ShareGroup, group, Share: command);
            return allowForward ? await cluster.ExecuteMetadataAsync(operation, deadline.Token) :
                await cluster.Metadata.ExecuteAsync(operation, deadline.Token);
        }
        catch (OperationCanceledException) when (!stop.IsCancellationRequested) { return new(Error.RequestTimedOut); }
        catch (QuorumUnreachableException) { return new(Error.CoordinatorNotAvailable); }
        catch (NotLeaderException) { return new(Error.NotCoordinator); }
    }

    private static string SharePrincipal(Request request) => request.AuthenticatedPrincipal ?? request.Access?.Subject.Principal ?? "";

    private static void WriteShareAssignment(WireWriter w, ClusterMetadataImage image, ImmutableArray<string> names,
        bool includeNames, Request request)
    {
        var topics = names.Where(name => request.AllowsTopic(name, AclOperation.Describe))
            .Select(name => image.Topics.GetValueOrDefault(name)).OfType<ReplicatedTopic>().ToArray();
        w.ArrayLength(topics.Length, true);
        foreach (var topic in topics)
        {
            w.Uuid(topic.Id);
            if (includeNames) w.Text(topic.Name, true);
            w.ArrayLength(topic.Partitions.Length, true);
            for (int partition = 0; partition < topic.Partitions.Length; partition++) w.I32(partition);
            w.UVar(0);
        }
        w.UVar(0);
    }

    private static void WriteShareError(WireWriter w, Error error)
    {
        w.Write(error);
        w.Text(error == Error.None ? null : error.ToString(), true);
    }
}
