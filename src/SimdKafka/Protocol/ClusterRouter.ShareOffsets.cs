namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    private sealed record ShareOffsetTopic(string Name, int[] Partitions);

    private async ValueTask<bool> DescribeShareOffsetsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        var r = new WireReader(body.Span);
        int count = r.ArrayLength(true);
        var groups = new (string Name, ShareOffsetTopic[]? Topics)[count];
        for (int i = 0; i < count; i++)
        {
            string group = r.Text(true);
            int entries = r.ArrayLength(true, nullable: true);
            ShareOffsetTopic[]? topics = entries < 0 ? null : new ShareOffsetTopic[entries];
            for (int t = 0; t < entries; t++)
            {
                string name = r.Text(true);
                var partitions = new int[r.ArrayLength(true)];
                for (int p = 0; p < partitions.Length; p++) partitions[p] = r.I32();
                topics![t] = new(name, partitions); r.Tags();
            }
            r.Tags(); groups[i] = (group, topics);
        }
        r.Tags(); r.End();
        Error coordinator = await CoordinatorReadAsync(stop);
        var image = cluster.Metadata.Image;
        w.I32(0); w.ArrayLength(groups.Length, true);
        foreach (var (name, selection) in groups)
        {
            Error error = request.AllowsGroup(name, AclOperation.Describe) ? coordinator : Error.GroupAuthorizationFailed;
            var group = error == Error.None ? image.ShareGroups.GetValueOrDefault(name) : null;
            if (group is null && error == Error.None) error = Error.GroupIdNotFound;
            var topics = group is null ? [] : selection ?? image.Topics.Values
                .Where(topic => request.AllowsTopic(topic.Name, AclOperation.Describe))
                .Select(topic => new ShareOffsetTopic(topic.Name, Enumerable.Range(0, topic.Partitions.Length)
                    .Where(p => group.Partitions.ContainsKey(ShareGroupState.PartitionKey(topic.Id, p))).ToArray()))
                .Where(static topic => topic.Partitions.Length != 0).ToArray();
            w.Text(name, true); w.ArrayLength(topics.Length, true);
            foreach (var selected in topics)
            {
                var topic = image.Topics.GetValueOrDefault(selected.Name);
                bool allowed = request.AllowsTopic(selected.Name, AclOperation.Describe);
                w.Text(selected.Name, true); w.Uuid(allowed ? topic?.Id ?? Guid.Empty : Guid.Empty);
                var indices = selected.Partitions.Length == 0 && topic is not null ?
                    Enumerable.Range(0, topic.Partitions.Length).ToArray() : selected.Partitions;
                w.ArrayLength(indices.Length, true);
                foreach (int partition in indices)
                {
                    var assignment = topic is not null && (uint)partition < topic.Partitions.Length ? topic.Partitions[partition] : null;
                    Error partitionError = !allowed ? Error.TopicAuthorizationFailed : assignment is null ? Error.UnknownTopicOrPartition : Error.None;
                    long offset = partitionError == Error.None ? group!.Partitions.GetValueOrDefault(ShareGroupState.PartitionKey(topic!.Id, partition))?.Start ?? -1 : -1;
                    w.I32(partition); w.I64(offset); w.I32(partitionError == Error.None ? assignment!.LeaderEpoch : -1);
                    if (request.Version >= 1)
                    {
                        long lag = -1;
                        if (offset >= 0)
                        {
                            var (_, progress) = await cluster.ShareOffsetProgressAsync(assignment!, stop);
                            if (progress.HighWatermark >= 0) lag = Math.Max(0, progress.HighWatermark - offset);
                        }
                        w.I64(lag);
                    }
                    WriteShareError(w, partitionError); w.UVar(0);
                }
                w.UVar(0);
            }
            WriteShareError(w, error); w.UVar(0);
        }
        w.UVar(0);
        return true;
    }

    private async ValueTask<bool> AlterShareOffsetsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        var r = new WireReader(body.Span);
        string group = r.Text(true);
        bool delete = request.Api == Api.DeleteShareGroupOffsets;
        var topics = new (string Name, (int Id, long Offset)[] Partitions)[r.ArrayLength(true)];
        for (int i = 0; i < topics.Length; i++)
        {
            string name = r.Text(true);
            var partitions = delete ? [] : new (int Id, long Offset)[r.ArrayLength(true)];
            for (int p = 0; p < partitions.Length; p++) { partitions[p] = (r.I32(), r.I64()); r.Tags(); }
            r.Tags(); topics[i] = (name, partitions);
        }
        r.Tags(); r.End();

        Error status = !request.AllowsGroup(group, delete ? AclOperation.Delete : AclOperation.Read) ?
            Error.GroupAuthorizationFailed : await CoordinatorReadAsync(stop);
        if (status == Error.None && cluster.Metadata.Image.ShareGroups.GetValueOrDefault(group) is { } active &&
            active.Members.Values.Any(member => member.Expires > BrokerCluster.TransactionTime())) status = Error.NonEmptyGroup;
        w.I32(0); WriteShareError(w, status);
        w.ArrayLength(status == Error.None ? topics.Length : 0, true);
        if (status == Error.None)
        {
            foreach (var (name, partitions) in topics)
            {
                var topic = cluster.Metadata.Image.Topics.GetValueOrDefault(name);
                bool allowed = request.AllowsTopic(name, AclOperation.Read);
                w.Text(name, true); w.Uuid(allowed ? topic?.Id ?? Guid.Empty : Guid.Empty);
                Error topicError = !allowed ? Error.TopicAuthorizationFailed : topic is null ? Error.UnknownTopicOrPartition : Error.None;
                if (delete)
                {
                    if (topicError == Error.None)
                        topicError = await ExecuteShareOffsetChangeAsync(group, new(ShareAction.DeleteOffsets, Now: BrokerCluster.TransactionTime(),
                            TopicId: topic!.Id), stop);
                    WriteShareError(w, topicError);
                }
                else
                {
                    w.ArrayLength(partitions.Length, true);
                    foreach (var (partition, offset) in partitions)
                    {
                        Error error = topicError;
                        if (error == Error.None)
                        {
                            if ((uint)partition >= topic!.Partitions.Length) error = Error.UnknownTopicOrPartition;
                            else
                            {
                                var (leader, progress) = await cluster.ShareOffsetProgressAsync(topic.Partitions[partition], stop);
                                error = progress.LogStartOffset < 0 ? Error.LeaderNotAvailable :
                                    await ExecuteShareOffsetChangeAsync(group, new(ShareAction.Reset, Now: BrokerCluster.TransactionTime(),
                                        TopicId: topic.Id, Partition: partition, LeaderId: leader, LeaderEpoch: progress.Epoch,
                                        LogStart: progress.LogStartOffset, LogEnd: progress.HighWatermark, Offset: offset),
                                        stop);
                            }
                        }
                        w.I32(partition); WriteShareError(w, error); w.UVar(0);
                    }
                }
                w.UVar(0);
            }
        }
        w.UVar(0);
        return true;
    }

    private async ValueTask<Error> ExecuteShareOffsetChangeAsync(string group, ShareCommand command, CancellationToken stop)
    {
        await _groupWrites.WaitAsync(stop);
        try
        {
            if (GroupLeadership().IsCancellationRequested) return Error.NotCoordinator;
            if (_groups.HasMembersOrPending(group)) return Error.InconsistentGroupProtocol;
            return (await ExecuteShareAsync(group, command, stop, allowForward: false)).Error;
        }
        finally { _groupWrites.Release(); }
    }
}
