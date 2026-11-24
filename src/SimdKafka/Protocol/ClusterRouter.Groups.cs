using System.Collections.Immutable;
using System.Text;
using DotNext.Net.Cluster.Consensus.Raft;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    private readonly GroupCoordinator _groups = new();
    private readonly object _groupLeadershipGate = new();
    private readonly SemaphoreSlim _groupWrites = new(1, 1);
    private long _groupTerm = -1;

    internal override Task RunGroupClockAsync(CancellationToken stop) =>
        Task.WhenAll(_groups.RunClockAsync(stop), RunConsumerGroupClockAsync(stop));

    private CancellationToken GroupLeadership()
    {
        lock (_groupLeadershipGate)
        {
            var leadership = cluster.Metadata.LeadershipToken;
            long term = cluster.Metadata.Term;
            if (!leadership.IsCancellationRequested && _groupTerm != term)
            {
                _groups.Reset();
                _groupTerm = term;
            }
            return leadership;
        }
    }

    private void FindCoordinator(ref WireReader r, WireWriter w, Request request)
    {
        short v = request.Version;
        bool compact = v >= 3;
        string key = v <= 3 ? r.Text(compact) : "";
        sbyte kind = v >= 1 ? r.I8() : (sbyte)0;
        int leader = cluster.Metadata.LeaderId;
        var broker = cluster.Members.FirstOrDefault(member => member.Id == leader);
        if (v >= 1) w.I32(0);
        if (v >= 4)
        {
            int count = r.ArrayLength(true);
            w.ArrayLength(count, true);
            for (int i = 0; i < count; i++)
            {
                key = r.Text(true);
                var error = CoordinatorError(key);
                w.Text(key, true);
                Address(error);
                w.Write(error); w.Text(null, true); w.UVar(0);
            }
        }
        else
        {
            var error = CoordinatorError(key);
            w.Write(error);
            if (v >= 1) w.Text(null, compact);
            Address(error);
        }
        r.EndStruct(compact); r.End(); w.EndStruct(compact);

        Error CoordinatorError(string id) => kind is not (0 or 1 or 2) ? Error.UnsupportedVersion :
            kind is 0 or 2 && !request.AllowsGroup(id, AclOperation.Describe) ? Error.GroupAuthorizationFailed :
            kind == 1 && !request.AllowsTransaction(id, AclOperation.Describe) ? Error.TransactionalIdAuthorizationFailed :
            id.Length == 0 ? kind == 0 ? Error.InvalidGroupId : Error.InvalidRequest :
            broker is null ? Error.CoordinatorNotAvailable : Error.None;

        void Address(Error error)
        {
            w.I32(error == Error.None ? broker!.Id : -1);
            w.Text(error == Error.None ? broker!.Host : "", compact);
            w.I32(error == Error.None ? broker!.Port : -1);
        }
    }

    private async ValueTask<bool> JoinGroupAsync(JoinRequest request, WireWriter w, Request header, CancellationToken stop)
    {
        if (!header.AllowsGroup(request.Group, AclOperation.Read))
        {
            Router.WriteJoin(w, header.Version, new(Error.GroupAuthorizationFailed));
            return true;
        }
        var active = GroupLeadership();
        var result = new JoinResult(Error.NotCoordinator);
        if (!active.IsCancellationRequested)
        {
            using var token = CancellationTokenSource.CreateLinkedTokenSource(active, stop);
            try
            {
                Task<JoinResult>? join = null;
                await _groupWrites.WaitAsync(token.Token);
                try
                {
                    token.Token.ThrowIfCancellationRequested();
                    result = new(await EnsureConsumerTermAsync(active, stop));
                    if (result.Error == Error.None && cluster.Metadata.Image.ConsumerGroups.ContainsKey(request.Group))
                    {
                        var command = new ConsumerGroupCommand(ConsumerGroupOperation.ConvertToClassic);
                        result = new((await ExecuteConsumerGroupAsync(request.Group, command, active, stop)).Error);
                    }
                    if (result.Error == Error.None && cluster.Metadata.Image.ShareGroups.ContainsKey(request.Group))
                        result = new(Error.InconsistentGroupProtocol);
                    if (result.Error == Error.None) join = _groups.Join(request);
                }
                finally { _groupWrites.Release(); }
                if (join is not null) result = await join.WaitAsync(token.Token);
            }
            catch (OperationCanceledException) when (active.IsCancellationRequested && !stop.IsCancellationRequested) { }
        }
        Router.WriteJoin(w, header.Version, result);
        return true;
    }

    private async ValueTask<bool> SyncGroupAsync(SyncRequest request, WireWriter w, Request header, CancellationToken stop)
    {
        if (!header.AllowsGroup(request.Group, AclOperation.Read))
        {
            Router.WriteSync(w, header.Version, Error.GroupAuthorizationFailed, []);
            return true;
        }
        var active = GroupLeadership();
        (Error Error, byte[] Assignment) response = (Error.NotCoordinator, []);
        if (!active.IsCancellationRequested)
        {
            using var token = CancellationTokenSource.CreateLinkedTokenSource(active, stop);
            try { response = await _groups.Sync(request).WaitAsync(token.Token); }
            catch (OperationCanceledException) when (active.IsCancellationRequested && !stop.IsCancellationRequested) { }
        }
        Router.WriteSync(w, header.Version, response.Error, response.Assignment);
        return true;
    }

    private async ValueTask<bool> CommitOffsetsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request, CancellationToken stop)
    {
        short version = request.Version;
        bool compact = version >= 8;
        bool byId = version >= 10;
        var r = new WireReader(body.Span);
        string group = r.Text(compact);
        bool groupAllowed = request.AllowsGroup(group, AclOperation.Read);
        int generation = version >= 1 ? r.I32() : -1;
        string member = version >= 1 ? r.Text(compact) : "";
        string? instance = version >= 7 ? r.NullableText(compact) : null;
        if (version is >= 2 and <= 4) r.I64();
        var changes = ImmutableArray.CreateBuilder<CommittedOffset>();
        var topics = new (string Name, Guid Id, (int Index, Error Error)[] Partitions)[r.ArrayLength(compact)];
        var image = cluster.Metadata.Image;
        for (int t = 0; t < topics.Length; t++)
        {
            Guid id = byId ? r.Uuid() : Guid.Empty;
            string name = byId ? "" : r.Text(compact);
            var topic = byId ? image.TopicById(id) : image.Topics.GetValueOrDefault(name);
            bool topicAllowed = byId && topic is null || request.AllowsTopic(topic?.Name ?? name, AclOperation.Read);
            var partitions = new (int Index, Error Error)[r.ArrayLength(compact)];
            for (int p = 0; p < partitions.Length; p++)
            {
                int index = r.I32();
                long offset = r.I64();
                int epoch = version >= 6 ? r.I32() : -1;
                if (version == 1) r.I64();
                string? metadata = r.NullableText(compact);
                r.EndStruct(compact);
                Error error = !groupAllowed ? Error.GroupAuthorizationFailed : !topicAllowed ? Error.TopicAuthorizationFailed :
                    topic is null && byId ? Error.UnknownTopicId :
                    topic is null || (uint)index >= topic.Partitions.Length ? Error.UnknownTopicOrPartition :
                    offset < 0 ? Error.InvalidRequest : metadata is not null && Encoding.UTF8.GetByteCount(metadata) > 4096 ?
                    Error.OffsetMetadataTooLarge : Error.None;
                if (error == Error.None) changes.Add(new(topic!.Name, index, offset, epoch, metadata, topic.Id));
                partitions[p] = (index, error);
            }
            r.EndStruct(compact);
            topics[t] = (name, id, partitions);
        }
        r.EndStruct(compact);
        r.End();

        Error result;
        await _groupWrites.WaitAsync(stop);
        try
        {
            var active = GroupLeadership();
            result = !groupAllowed ? Error.GroupAuthorizationFailed :
                active.IsCancellationRequested ? Error.NotCoordinator : await EnsureConsumerTermAsync(active, stop);
            if (result == Error.None) result = ValidateGroupCommit(group, member, generation, instance);
            if (result == Error.None && changes.Count > 0)
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stop, active);
                timeout.CancelAfter(TimeSpan.FromSeconds(10));
                try
                {
                    result = (await cluster.Metadata.ExecuteAsync(new(MetadataOperation.CommitOffsets, group,
                        Offsets: changes.ToImmutable()), timeout.Token)).Error;
                    if (result == Error.NotController) result = Error.NotCoordinator;
                }
                catch (OperationCanceledException) when (!stop.IsCancellationRequested)
                { result = active.IsCancellationRequested ? Error.NotCoordinator : Error.RequestTimedOut; }
                catch (QuorumUnreachableException) { result = Error.CoordinatorNotAvailable; }
            }
        }
        finally { _groupWrites.Release(); }

        if (version >= 3) w.I32(0);
        w.ArrayLength(topics.Length, compact);
        foreach (var (name, id, partitions) in topics)
        {
            if (byId) w.Uuid(id);
            else w.Text(name, compact);
            w.ArrayLength(partitions.Length, compact);
            foreach (var (index, error) in partitions)
            {
                w.I32(index); w.Write(error == Error.None ? result : error); w.EndStruct(compact);
            }
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
        return true;
    }

    private async ValueTask<bool> FetchOffsetsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        Error coordinator = await CoordinatorReadAsync(stop);
        var r = new WireReader(body.Span);
        FetchOffsets(ref r, w, request, coordinator);
        return true;
    }

    private async ValueTask<Error> CoordinatorReadAsync(CancellationToken stop)
    {
        var active = GroupLeadership();
        Error coordinator = active.IsCancellationRequested ? Error.NotCoordinator : Error.None;
        if (coordinator == Error.None)
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(stop, active);
            deadline.CancelAfter(TimeSpan.FromSeconds(10));
            try { await cluster.Metadata.ReadBarrierAsync(deadline.Token); }
            catch (OperationCanceledException) when (!stop.IsCancellationRequested) { coordinator = Error.NotCoordinator; }
            catch (QuorumUnreachableException) { coordinator = Error.CoordinatorNotAvailable; }
            catch (NotLeaderException) { coordinator = Error.NotCoordinator; }
        }
        return active.IsCancellationRequested ? Error.NotCoordinator : coordinator;
    }

    private void FetchOffsets(ref WireReader r, WireWriter w, Request request, Error coordinator)
    {
        short version = request.Version;
        bool compact = version >= 6, multiple = version >= 8;
        var image = cluster.Metadata.Image;
        bool requireStable = version >= 7 && !image.ActiveTransactions.IsEmpty && ReadOffsetStability(r, version);
        if (version >= 3) w.I32(0);
        int groups = multiple ? r.ArrayLength(compact: true) : 1;
        if (multiple) w.ArrayLength(groups, compact: true);
        for (int i = 0; i < groups; i++)
        {
            string group = r.Text(compact);
            Error groupError = request.AllowsGroup(group, AclOperation.Describe) ? coordinator : Error.GroupAuthorizationFailed;
            if (version >= 9)
            {
                string? member = r.NullableText(true);
                int epoch = r.I32();
                if (member is not null && groupError == Error.None)
                    groupError = ReplicatedMetadata.ValidateConsumerMember(image.ConsumerGroups.GetValueOrDefault(group),
                        member, epoch, null, requireInstance: false);
            }
            if (multiple) w.Text(group, compact: true);
            WriteGroupOffsets(ref r, w, version, compact, groupError, image, group, requireStable, request);
            if (multiple) { r.Tags(); w.UVar(0); }
        }
        if (version >= 7) r.Bool();
        r.EndStruct(compact);
        r.End();
        w.EndStruct(compact);
    }

    private static bool ReadOffsetStability(WireReader reader, short version)
    {
        bool multiple = version >= 8;
        int groups = multiple ? reader.ArrayLength(true) : 1;
        for (int group = 0; group < groups; group++)
        {
            reader.StringBytes(compact: true);
            if (version >= 9) { reader.NullableText(true); reader.I32(); }
            int topics = reader.ArrayLength(true, nullable: true);
            for (int t = 0; t < topics; t++)
            {
                if (version >= 10) reader.Take(16);
                else reader.StringBytes(compact: true);
                reader.Take(checked(reader.ArrayLength(true) * sizeof(int)));
                reader.Tags();
            }
            if (multiple) reader.Tags();
        }
        return reader.Bool();
    }

    private static void WriteGroupOffsets(ref WireReader r, WireWriter w, short version, bool compact,
        Error coordinator, ClusterMetadataImage image, string group, bool requireStable, Request request)
    {
        bool byId = version >= 10;
        int count = r.ArrayLength(compact, nullable: version >= 2);
        var offsets = image.Offsets.GetValueOrDefault(group);
        if (offsets.IsDefault) offsets = [];
        Error groupError = group.Length == 0 ? Error.InvalidGroupId : coordinator;
        Dictionary<(string, int), CommittedOffset>? unstable = null;
        if (requireStable && groupError == Error.None)
            foreach (string transaction in image.ActiveTransactions)
            {
                if (!image.Transactions[transaction].StagedOffsets.TryGetValue(group, out var staged)) continue;
                foreach (var offset in staged)
                {
                    if (!image.Topics.TryGetValue(offset.Topic, out var topic) || topic.Id != offset.TopicId) continue;
                    var key = (offset.Topic, offset.Partition);
                    unstable ??= [];
                    if (!unstable.TryGetValue(key, out var existing) || offset.CommitIndex > existing.CommitIndex)
                        unstable[key] = offset;
                }
            }
        if (count < 0)
        {
            var entries = unstable is null ? offsets.AsEnumerable() :
                offsets.Concat(unstable.Values).DistinctBy(static offset => (offset.Topic, offset.Partition));
            var groups = groupError == Error.None ? entries.Where(offset => request.AllowsTopic(offset.Topic, AclOperation.Describe))
                .GroupBy(static offset => offset.Topic).ToArray() : [];
            w.ArrayLength(groups.Length, compact);
            foreach (var topic in groups)
            {
                if (byId) w.Uuid(image.Topics.GetValueOrDefault(topic.Key)?.Id ?? topic.First().TopicId ?? Guid.Empty);
                else w.Text(topic.Key, compact);
                w.ArrayLength(topic.Count(), compact);
                foreach (var offset in topic) WriteOffset(offset.Topic, offset.Partition, offset, Error.None);
                w.EndStruct(compact);
            }
        }
        else
        {
            var byPartition = offsets.ToDictionary(static offset => (offset.Topic, offset.Partition));
            w.ArrayLength(count, compact);
            for (int t = 0; t < count; t++)
            {
                Guid id = byId ? r.Uuid() : Guid.Empty;
                string name = byId ? image.TopicById(id)?.Name ?? "" : r.Text(compact);
                var topic = image.Topics.GetValueOrDefault(name);
                bool topicAllowed = byId && topic is null || request.AllowsTopic(name, AclOperation.Describe);
                int partitions = r.ArrayLength(compact);
                if (byId) w.Uuid(id);
                else w.Text(name, compact);
                w.ArrayLength(partitions, compact);
                for (int p = 0; p < partitions; p++)
                {
                    int index = r.I32();
                    Error error = groupError != Error.None ? groupError : !topicAllowed ? Error.TopicAuthorizationFailed :
                        topic is null && byId ? Error.UnknownTopicId :
                        topic is null || (uint)index >= topic.Partitions.Length ?
                        Error.UnknownTopicOrPartition : Error.None;
                    WriteOffset(name, index, error == Error.None ? byPartition.GetValueOrDefault((name, index)) : null, error);
                }
                r.EndStruct(compact);
                w.EndStruct(compact);
            }
        }
        if (version >= 2) w.Write(groupError);

        void WriteOffset(string topic, int index, CommittedOffset? offset, Error error)
        {
            if (error == Error.None && unstable?.TryGetValue((topic, index), out var staged) == true &&
                staged.CommitIndex >= (offset?.CommitIndex ?? 0))
            {
                error = Error.UnstableOffsetCommit;
                offset = null;
            }
            Router.WriteOffset(w, version, index, offset, error, compact);
        }
    }
}
