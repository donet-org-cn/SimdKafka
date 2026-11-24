namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    private const string MetadataTopic = "__cluster_metadata";

    private async ValueTask<bool> DescribeQuorumAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request, CancellationToken stop)
    {
        bool modern = request.Version >= 2;
        var r = new WireReader(body.Span);
        int count = r.ArrayLength(true);
        if (count > 32) throw new WireException("Too many quorum topics");
        var topics = new (string Name, int[] Partitions)[count];
        bool requestedMetadata = false;
        for (int i = 0; i < count; i++)
        {
            string name = r.Text(true);
            int length = r.ArrayLength(true);
            if (length > 32) throw new WireException("Too many quorum partitions");
            var partitions = new int[length];
            for (int j = 0; j < length; j++)
            {
                partitions[j] = r.I32();
                requestedMetadata |= name == MetadataTopic && partitions[j] == 0;
                r.Tags();
            }
            r.Tags();
            topics[i] = (name, partitions);
        }
        r.Tags(); r.End();

        bool allowed = request.AllowsCluster(AclOperation.Describe);
        var (leader, voters) = allowed && requestedMetadata ?
            await cluster.ReadQuorumProgressAsync(stop) : (QuorumProgress.Unknown, []);
        var members = cluster.Members;
        w.Write(allowed ? Error.None : Error.ClusterAuthorizationFailed);
        if (modern) w.Text(null, true);
        w.ArrayLength(allowed ? topics.Length : 0, true);
        if (allowed)
            foreach (var (name, partitions) in topics)
            {
                w.Text(name, true);
                w.ArrayLength(partitions.Length, true);
                foreach (int partition in partitions)
                {
                    bool valid = name == MetadataTopic && partition == 0;
                    w.I32(partition);
                    w.Write(!valid ? Error.UnknownTopicOrPartition : leader.Leader < 0 ? Error.LeaderNotAvailable : Error.None);
                    if (modern) w.Text(null, true);
                    w.I32(valid ? leader.Leader : -1);
                    w.I32(valid ? leader.Epoch : -1);
                    w.I64(valid ? leader.HighWatermark : -1);
                    w.ArrayLength(valid ? voters.Length : 0, true);
                    if (valid)
                        for (int i = 0; i < voters.Length; i++)
                        {
                            w.I32(members[i].Id);
                            if (modern) w.Uuid(Guid.Empty);
                            w.I64(members[i].Id == leader.Leader ? leader.LogEnd : voters[i].LogEnd);
                            if (request.Version >= 1) { w.I64(-1); w.I64(-1); }
                            w.UVar(0);
                        }
                    w.ArrayLength(0, true); // This quorum has no observer nodes.
                    w.UVar(0);
                }
                w.UVar(0);
            }
        if (modern)
        {
            bool includeListeners = allowed && requestedMetadata;
            w.ArrayLength(includeListeners ? members.Length : 0, true);
            if (includeListeners)
                foreach (var member in members)
                {
                    w.I32(member.Id);
                    w.ArrayLength(1, true);
                    w.Text("CLIENT", true); w.Text(member.Host, true); w.I16(unchecked((short)member.Port)); w.UVar(0);
                    w.UVar(0);
                }
        }
        w.UVar(0);
        return true;
    }
}
