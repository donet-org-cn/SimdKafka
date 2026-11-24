namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    private void DescribeLogDirs(ref WireReader r, WireWriter w, Request request)
    {
        var selection = Router.ReadLogDirSelection(ref r, request.Flexible);
        if (!request.AllowsCluster(AclOperation.Describe))
        {
            Router.WriteLogDirsUnauthorized(w, request);
            return;
        }
        var reported = new List<(string Name, List<(int Index, long Size, long Lag)> Partitions)>();
        foreach (var topic in cluster.Metadata.Image.Topics.Values)
        {
            if (selection is not null && !selection.ContainsKey(topic.Name)) continue;
            var selected = selection?.GetValueOrDefault(topic.Name);
            var partitions = new List<(int Index, long Size, long Lag)>();
            for (int i = 0; i < topic.Partitions.Length; i++)
            {
                if (selected is not null && !selected.Contains(i)) continue;
                if (cluster.FindReplica(topic.Partitions[i].Id) is { } replica)
                    partitions.Add((i, replica.EncodedLogBytes, replica.IsLeader ? 0 : -1));
            }
            if (partitions.Count != 0) reported.Add((topic.Name, partitions));
        }

        Router.WriteLogDirs(w, request, options.Data, Path.Combine(options.Data, "replicas"), reported);
    }
}
