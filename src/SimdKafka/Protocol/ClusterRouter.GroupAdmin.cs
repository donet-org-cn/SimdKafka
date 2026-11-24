using System.Collections.Immutable;
using DotNext.Net.Cluster.Consensus.Raft;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    private void ListGroups(ref WireReader r, WireWriter w, Request request)
    {
        bool coordinator = !GroupLeadership().IsCancellationRequested;
        var image = cluster.Metadata.Image;
        long now = BrokerCluster.TransactionTime();
        var groups = coordinator ? _groups.List(image.Offsets.Keys).Where(group =>
                !image.ConsumerGroups.ContainsKey(group.Id) && !image.ShareGroups.ContainsKey(group.Id))
            .Concat(image.ConsumerGroups.Select(static pair => new GroupSnapshot(pair.Key, pair.Value.State, "consumer",
                pair.Value.Assignor, [], GroupType: "consumer")))
            .Concat(image.ShareGroups.Select(pair => new GroupSnapshot(pair.Key,
                pair.Value.Members.Values.Any(member => member.Expires > now) ? "Stable" : "Empty", "share", "simple", [], GroupType: "share")))
            .ToArray() : [];
        Router.WriteGroupList(ref r, w, request, groups,
            cluster.Metadata.LeaderId >= 0 ? Error.None : Error.CoordinatorNotAvailable);
    }

    private async ValueTask<bool> DescribeGroupsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        Error coordinator = await CoordinatorReadAsync(stop);
        var image = cluster.Metadata.Image;
        var r = new WireReader(body.Span);
        Router.WriteGroupDescriptions(ref r, w, request, id =>
            image.ConsumerGroups.ContainsKey(id) || image.ShareGroups.ContainsKey(id) ? null :
                _groups.Describe(id, image.Offsets.ContainsKey(id)), coordinator);
        return true;
    }

    private async ValueTask<bool> DeleteGroupsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        bool compact = request.Flexible;
        var r = new WireReader(body.Span);
        var groups = Router.TextArray(ref r, compact);
        r.EndStruct(compact);
        r.End();
        w.I32(0);
        w.ArrayLength(groups.Length, compact);
        foreach (string id in groups)
        {
            var error = request.AllowsGroup(id, AclOperation.Delete) ? await DeleteGroupDataAsync(id, false, [], stop) :
                Error.GroupAuthorizationFailed;
            w.Text(id, compact);
            w.Write(error);
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
        return true;
    }

    private async ValueTask<bool> DeleteOffsetsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request, CancellationToken stop)
    {
        var r = new WireReader(body.Span);
        string group = r.String();
        var topics = new (string Name, (int Index, bool Exists)[] Partitions, bool Allowed)[r.Count()];
        var selected = ImmutableArray.CreateBuilder<CommittedOffset>();
        var image = cluster.Metadata.Image;
        for (int t = 0; t < topics.Length; t++)
        {
            string name = r.String();
            bool allowed = request.AllowsTopic(name, AclOperation.Read);
            var topic = image.Topics.GetValueOrDefault(name);
            var partitions = new (int Index, bool Exists)[r.Count()];
            for (int p = 0; p < partitions.Length; p++)
            {
                int index = r.I32();
                bool exists = topic is not null && (uint)index < topic.Partitions.Length;
                if (exists && allowed) selected.Add(new(name, index, 0, -1, null, topic!.Id));
                partitions[p] = (index, exists);
            }
            topics[t] = (name, partitions, allowed);
        }
        r.End();

        Error error = request.AllowsGroup(group, AclOperation.Delete) ?
            await DeleteGroupDataAsync(group, true, selected.ToImmutable(), stop) : Error.GroupAuthorizationFailed;
        bool perPartition = error is Error.None or Error.GroupSubscribedToTopic;
        w.Write(perPartition ? Error.None : error);
        w.I32(0);
        w.I32(perPartition ? topics.Length : 0);
        if (perPartition)
        {
            foreach (var (name, partitions, allowed) in topics)
            {
                w.String(name);
                w.I32(partitions.Length);
                foreach (var (index, exists) in partitions)
                {
                    w.I32(index);
                    w.Write(!allowed ? Error.TopicAuthorizationFailed : exists ? error : Error.UnknownTopicOrPartition);
                }
            }
        }
        return true;
    }

    private async ValueTask<Error> DeleteGroupDataAsync(string id, bool offsetsOnly,
        ImmutableArray<CommittedOffset> selected, CancellationToken stop)
    {
        await _groupWrites.WaitAsync(stop);
        try
        {
            var leadership = GroupLeadership();
            if (leadership.IsCancellationRequested) return Error.NotCoordinator;
            if (cluster.Metadata.Image.ShareGroups.ContainsKey(id))
            {
                if (offsetsOnly) return Error.InconsistentGroupProtocol;
                return (await ExecuteShareAsync(id, new(ShareAction.DeleteGroup, Now: BrokerCluster.TransactionTime()), stop,
                    allowForward: false)).Error;
            }
            Error error = _groups.BeginDelete(id, offsetsOnly, out bool hasGroup);
            if (error != Error.None) return error;
            bool deleted = false;
            try
            {
                using var deadline = CancellationTokenSource.CreateLinkedTokenSource(leadership, stop);
                deadline.CancelAfter(TimeSpan.FromSeconds(10));
                var operation = offsetsOnly ? MetadataOperation.DeleteOffsets : MetadataOperation.DeleteGroup;
                error = (await cluster.Metadata.ExecuteAsync(new(operation, id, Offsets: selected), deadline.Token)).Error;
                if (leadership.IsCancellationRequested || error == Error.NotController) return Error.NotCoordinator;
                if (error == Error.GroupIdNotFound && hasGroup) error = Error.None;
                deleted = !offsetsOnly && error == Error.None;
                return error;
            }
            catch (OperationCanceledException) when (!stop.IsCancellationRequested)
            { return leadership.IsCancellationRequested ? Error.NotCoordinator : Error.RequestTimedOut; }
            catch (QuorumUnreachableException) { return Error.CoordinatorNotAvailable; }
            finally { _groups.EndDelete(id, deleted); }
        }
        finally { _groupWrites.Release(); }
    }
}
