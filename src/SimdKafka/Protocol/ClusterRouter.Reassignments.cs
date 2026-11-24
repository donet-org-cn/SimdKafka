using System.Collections.Immutable;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    private async ValueTask<bool> AlterReassignmentsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        short version = request.Version;
        var r = new WireReader(body.Span);
        int timeout = r.I32();
        bool allowCountChange = version == 0 || r.Bool();
        var topics = new (string Name, (int Index, int[]? Replicas)[] Partitions)[r.ArrayLength(true)];
        for (int t = 0; t < topics.Length; t++)
        {
            string name = r.Text(true);
            var partitions = new (int Index, int[]? Replicas)[r.ArrayLength(true)];
            for (int p = 0; p < partitions.Length; p++)
            {
                int index = r.I32();
                int count = r.ArrayLength(true, nullable: true);
                int[]? targets = count < 0 ? null : new int[count];
                for (int n = 0; n < count; n++) targets![n] = r.I32();
                partitions[p] = (index, targets);
                r.Tags();
            }
            r.Tags();
            topics[t] = (name, partitions);
        }
        r.Tags(); r.End();

        Error top = !request.AllowsCluster(AclOperation.Alter) ? Error.ClusterAuthorizationFailed :
            timeout < 0 ? Error.InvalidRequest : !cluster.Metadata.IsLeader ? Error.NotController : Error.None;
        w.I32(0);
        if (version >= 1) w.Bool(allowCountChange);
        w.Write(top);
        w.Text(top == Error.None ? null : top.ToString(), true);
        w.ArrayLength(top == Error.None ? topics.Length : 0, true);
        if (top == Error.None)
        {
            using var pending = Deadline(timeout, stop);
            foreach (var (name, partitions) in topics)
            {
                var topic = cluster.Metadata.Image.Topics.GetValueOrDefault(name);
                w.Text(name, true);
                w.ArrayLength(partitions.Length, true);
                foreach (var (partition, target) in partitions)
                {
                    Error error = Error.UnknownTopicOrPartition;
                    if (topic is not null && (uint)partition < topic.Partitions.Length)
                    {
                        var command = new MetadataCommand(target is null ? MetadataOperation.CancelReassignment :
                            MetadataOperation.BeginReassignment, name, topic.Id, Partition: partition,
                            AssignmentId: topic.Partitions[partition].Id, MigrationId: Guid.NewGuid(),
                            Target: target?.ToImmutableArray() ?? [], AllowReplicaCountChange: allowCountChange);
                        try { error = (await cluster.Metadata.ExecuteAsync(command, pending.Token)).Error; }
                        catch (OperationCanceledException) when (!stop.IsCancellationRequested) { error = Error.RequestTimedOut; }
                    }
                    w.I32(partition);
                    w.Write(error);
                    w.Text(error == Error.None ? null : error.ToString(), true);
                    w.UVar(0);
                }
                w.UVar(0);
            }
        }
        w.UVar(0);
        return true;
    }

    private void ListReassignments(ref WireReader r, WireWriter w, Request header)
    {
        int timeout = r.I32();
        int count = r.ArrayLength(true, nullable: true);
        var request = new Dictionary<string, HashSet<int>>(StringComparer.Ordinal);
        for (int t = 0; t < count; t++)
        {
            string name = r.Text(true);
            int partitions = r.ArrayLength(true);
            var keys = new HashSet<int>();
            for (int p = 0; p < partitions; p++) keys.Add(r.I32());
            request[name] = keys;
            r.Tags();
        }
        r.Tags(); r.End();
        var error = !header.AllowsCluster(AclOperation.Describe) ? Error.ClusterAuthorizationFailed :
            timeout < 0 ? Error.InvalidRequest : !cluster.Metadata.IsLeader ? Error.NotController : Error.None;
        w.I32(0);
        w.Write(error);
        w.Text(error == Error.None ? null : error.ToString(), true);
        var result = new List<(string Name, (int Index, PartitionAssignment Assignment)[] Partitions)>();
        if (error == Error.None)
        {
            foreach (var topic in cluster.Metadata.Image.Topics.Values)
            {
                var filter = request.GetValueOrDefault(topic.Name);
                if (count >= 0 && filter is null) continue;
                var partitions = topic.Partitions.Select((assignment, index) => (index, assignment))
                    .Where(entry => entry.assignment.Migration is not null && (count < 0 || filter!.Contains(entry.index))).ToArray();
                if (partitions.Length > 0) result.Add((topic.Name, partitions));
            }
        }
        w.ArrayLength(result.Count, true);
        foreach (var (name, partitions) in result)
        {
            w.Text(name, true);
            w.ArrayLength(partitions.Length, true);
            foreach (var (index, assignment) in partitions)
            {
                var target = assignment.Migration!.Target;
                w.I32(index);
                WriteNodes(w, assignment.Replicas.Union(target));
                WriteNodes(w, target.Except(assignment.Replicas));
                WriteNodes(w, assignment.Replicas.Except(target));
                w.UVar(0);
            }
            w.UVar(0);
        }
        w.UVar(0);
    }

    private static void WriteNodes(WireWriter w, IEnumerable<int> brokers)
    {
        var ids = brokers.ToArray();
        w.ArrayLength(ids.Length, true);
        foreach (int id in ids) w.I32(id);
    }
}
