using System.Collections.Immutable;
using DotNext.Net.Cluster.Consensus.Raft;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    private async ValueTask<bool> CreatePartitionsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        var r = new WireReader(body.Span);
        bool compact = request.Flexible;
        var topics = new (string Name, int Count, ImmutableArray<ImmutableArray<int>> Assignments, bool Explicit)[r.ArrayLength(compact)];
        for (int t = 0; t < topics.Length; t++)
        {
            string name = r.Text(compact);
            int count = r.I32();
            int assignments = r.ArrayLength(compact, nullable: true);
            var placements = ImmutableArray.CreateBuilder<ImmutableArray<int>>(Math.Max(0, assignments));
            for (int a = 0; a < assignments; a++)
            {
                int brokerCount = r.ArrayLength(compact);
                var brokers = ImmutableArray.CreateBuilder<int>(brokerCount);
                for (int b = 0; b < brokerCount; b++) brokers.Add(r.I32());
                placements.Add(brokers.MoveToImmutable());
                r.EndStruct(compact);
            }
            r.EndStruct(compact);
            topics[t] = (name, count, placements.MoveToImmutable(), assignments >= 0);
        }
        int timeout = r.I32();
        bool dryRun = r.Bool();
        r.EndStruct(compact); r.End();

        w.I32(0);
        w.ArrayLength(topics.Length, compact);
        using var deadline = Deadline(timeout, stop);
        foreach (var (name, count, placements, hasAssignments) in topics)
        {
            var topic = cluster.Metadata.Image.Topics.GetValueOrDefault(name);
            Error error = !request.AllowsTopic(name, AclOperation.Alter) ? Error.TopicAuthorizationFailed :
                !cluster.Metadata.IsLeader ? Error.NotController : timeout < 0 ? Error.InvalidRequest :
                topic is null ? Error.UnknownTopicOrPartition :
                count <= topic.Partitions.Length || count > BrokerOptions.MaxPartitionsPerTopic ? Error.InvalidPartitions : Error.None;
            int additions = error == Error.None ? count - topic!.Partitions.Length : 0;
            if (error == Error.None && !cluster.Metadata.HasReplicaSlots(additions)) error = Error.InvalidPartitions;
            if (error == Error.None && hasAssignments && (placements.Length != additions ||
                !ReplicatedMetadata.ValidReplicaAssignments(placements, topic!.Partitions[0].Replicas.Length, cluster.Members)))
                error = Error.InvalidReplicaAssignment;
            if (error == Error.None && !dryRun)
            {
                var ids = Enumerable.Range(0, additions).Select(static _ => Guid.NewGuid()).ToImmutableArray();
                error = await ExecuteTopicCommand(new(MetadataOperation.ExpandTopic, name, topic!.Id, count, PartitionIds: ids,
                    ReplicaAssignments: placements),
                    deadline.Token, stop);
                if (error == Error.None)
                {
                    try { error = await WaitForTopicLeadersAsync(name, topic.Id, deadline.Token); }
                    catch (OperationCanceledException) when (!stop.IsCancellationRequested) { error = Error.RequestTimedOut; }
                }
            }
            w.Text(name, compact);
            w.Write(error);
            w.Text(error == Error.None ? null : error.ToString(), compact);
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
        return true;
    }

    private async ValueTask<Error> WaitForTopicLeadersAsync(string name, Guid id, CancellationToken stop)
    {
        while (true)
        {
            var current = cluster.Metadata.Image.Topics.GetValueOrDefault(name);
            if (current?.Id != id) return Error.UnknownTopicOrPartition;
            if (current.Partitions.All(static partition => partition.LeaderId >= 0)) return Error.None;
            await Task.Delay(30, stop);
        }
    }

    private async ValueTask<bool> DeleteTopicsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        var r = new WireReader(body.Span);
        short version = request.Version;
        bool compact = request.Flexible;
        var topics = new (string? Name, Guid Id)[r.ArrayLength(compact)];
        for (int t = 0; t < topics.Length; t++)
        {
            topics[t] = version >= 6 ? (r.NullableText(compact), r.Uuid()) : (r.Text(compact), Guid.Empty);
            if (version >= 6) r.EndStruct(compact);
        }
        int timeout = r.I32();
        r.EndStruct(compact); r.End();
        if (version >= 1) w.I32(0);
        w.ArrayLength(topics.Length, compact);
        using var deadline = Deadline(timeout, stop);
        foreach (var (name, id) in topics)
        {
            var image = cluster.Metadata.Image;
            var topic = name is not null ? image.Topics.GetValueOrDefault(name) : image.TopicById(id);
            string? resolved = topic?.Name ?? name;
            bool allowed = resolved is null || request.AllowsTopic(resolved, AclOperation.Delete);
            Error error = !allowed ? Error.TopicAuthorizationFailed :
                !cluster.Metadata.IsLeader ? Error.NotController : timeout < 0 || (name is null) == (id == Guid.Empty) ?
                Error.InvalidRequest : topic is null ? (name is null ? Error.UnknownTopicId : Error.UnknownTopicOrPartition) : Error.None;
            if (error == Error.None)
                error = await ExecuteTopicCommand(new(MetadataOperation.DeleteTopic, topic!.Name, topic.Id), deadline.Token, stop);
            w.Text(allowed ? topic?.Name ?? name : name, compact);
            if (version >= 6) w.Uuid(allowed ? topic?.Id ?? id : id);
            w.Write(error);
            if (version >= 5) w.Text(error == Error.None ? null : error.ToString(), compact);
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
        return true;
    }

    private async ValueTask<Error> ExecuteTopicCommand(MetadataCommand command, CancellationToken deadline, CancellationToken stop)
    {
        try { return (await cluster.Metadata.ExecuteAsync(command, deadline)).Error; }
        catch (OperationCanceledException) when (!stop.IsCancellationRequested) { return Error.RequestTimedOut; }
        catch (QuorumUnreachableException) { return Error.NotController; }
    }

    private void DescribeTopicPartitions(ref WireReader r, WireWriter w, Request request)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        int count = r.ArrayLength(true);
        for (int t = 0; t < count; t++) { names.Add(r.Text(true)); r.Tags(); }
        int limit = r.I32();
        string? cursor = null;
        int index = 0;
        if (r.I8() >= 0)
        {
            cursor = r.Text(true);
            index = r.I32();
            r.Tags();
        }
        r.Tags(); r.End();
        if (limit <= 0 || index < 0) throw new WireException("Invalid partition page");

        var image = cluster.Metadata.Image;
        if (count == 0) names.UnionWith(image.Topics.Keys.Where(name => request.AllowsTopic(name, AclOperation.Describe)));
        var page = new List<(string Name, ReplicatedTopic? Topic, int Start, int Size, Error Error)>();
        (string Name, int Index)? next = null;
        foreach (string name in names.Order(StringComparer.Ordinal))
        {
            int compare = cursor is null ? 1 : string.CompareOrdinal(name, cursor);
            if (compare < 0) continue;
            bool allowed = request.AllowsTopic(name, AclOperation.Describe);
            var topic = allowed ? image.Topics.GetValueOrDefault(name) : null;
            int start = compare == 0 ? index : 0;
            int remaining = Math.Max(0, (topic?.Partitions.Length ?? 0) - start);
            int size = Math.Min(remaining, limit);
            if (size != 0 || topic is null) page.Add((name, topic, start, size,
                !allowed ? Error.TopicAuthorizationFailed : topic is null ? Error.UnknownTopicOrPartition : Error.None));
            limit -= size;
            if (size < remaining) { next = (name, start + size); break; }
        }

        w.I32(0);
        w.ArrayLength(page.Count, true);
        foreach (var (name, topic, start, size, error) in page)
        {
            w.Write(error);
            w.Text(name, true);
            w.Uuid(topic?.Id ?? Guid.Empty);
            w.Bool(false);
            w.ArrayLength(size, true);
            for (int p = start; p < start + size; p++)
            {
                var assignment = topic!.Partitions[p];
                var (leader, epoch) = cluster.FindLeader(assignment);
                w.Write(leader < 0 ? Error.LeaderNotAvailable : Error.None);
                w.I32(p); w.I32(leader); w.I32(epoch);
                WriteNodes(w, assignment.Replicas);
                WriteNodes(w, leader >= 0 ? [leader] : []);
                w.ArrayLength(0, true); w.ArrayLength(0, true); w.ArrayLength(0, true);
                w.UVar(0);
            }
            w.I32(error == Error.None ? request.Access?.Operations(AclResource.Topic, name) ?? int.MinValue : int.MinValue);
            w.UVar(0);
        }
        if (next is { } more)
        {
            w.I8(1); w.Text(more.Name, true); w.I32(more.Index); w.UVar(0);
        }
        else w.I8(-1);
        w.UVar(0);
    }
}
