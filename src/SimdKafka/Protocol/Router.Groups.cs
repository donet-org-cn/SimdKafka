namespace SimdKafka;

internal sealed partial class Router
{
    internal static JoinRequest ReadJoin(ref WireReader r, Request request)
    {
        short v = request.Version;
        string group = r.String();
        int session = r.I32();
        int rebalance = v >= 1 ? r.I32() : session;
        string member = r.String();
        string? instance = v >= 5 ? r.NullableString() : null;
        string type = r.String();
        int count = r.Count();
        if (count > 8) throw new WireException("Too many group protocols");
        var protocols = new GroupProtocol[count];
        var names = new HashSet<string>(StringComparer.Ordinal);
        for (int i = 0; i < count; i++)
        {
            string name = r.String();
            var bytes = r.Bytes();
            if (!names.Add(name) || bytes.Length > 32 * 1024)
                throw new WireException("Invalid group protocol metadata");
            protocols[i] = new GroupProtocol(name, bytes.ToArray());
        }
        r.End();
        return new(group, member, type, session, rebalance, v >= 4, protocols, request.ClientId, request.ClientHost, instance);
    }

    private async ValueTask<bool> EncodeJoinAsync(JoinRequest request, WireWriter w, Request header, CancellationToken stop)
    {
        var result = header.AllowsGroup(request.Group, AclOperation.Read) ?
            await _groups.Join(request).WaitAsync(stop) : new JoinResult(Error.GroupAuthorizationFailed);
        WriteJoin(w, header.Version, result);
        return true;
    }

    internal static void WriteJoin(WireWriter w, short v, JoinResult result)
    {
        if (v >= 2) w.I32(0);
        w.Write(result.Error);
        w.I32(result.Generation);
        w.String(result.Protocol);
        w.String(result.Leader);
        w.String(result.Member);
        w.I32(result.Members?.Length ?? 0);
        foreach (var (id, instance, metadata) in result.Members ?? [])
        {
            w.String(id);
            if (v >= 5) w.String(instance);
            w.Bytes(metadata);
        }
    }

    internal static SyncRequest ReadSync(ref WireReader r, short version)
    {
        string id = r.String();
        int generation = r.I32();
        string member = r.String();
        string? instance = version >= 3 ? r.NullableString() : null;
        int count = r.Count();
        if (count > 64) throw new WireException("Too many group assignments");
        var assignments = new Dictionary<string, byte[]>(count, StringComparer.Ordinal);
        for (int i = 0; i < count; i++)
        {
            string target = r.String();
            var bytes = r.Bytes();
            if (bytes.Length > 32 * 1024 || !assignments.TryAdd(target, bytes.ToArray()))
                throw new WireException("Invalid member assignment");
        }
        r.End();
        return new(id, member, generation, assignments, instance);
    }

    private async ValueTask<bool> EncodeSyncAsync(SyncRequest request, WireWriter w, Request header, CancellationToken stop)
    {
        var (error, assignment) = header.AllowsGroup(request.Group, AclOperation.Read) ?
            await _groups.Sync(request).WaitAsync(stop) : (Error.GroupAuthorizationFailed, Array.Empty<byte>());
        WriteSync(w, header.Version, error, assignment);
        return true;
    }

    internal static void WriteSync(WireWriter w, short v, Error error, byte[] assignment)
    {
        if (v >= 1) w.I32(0);
        w.Write(error);
        w.Bytes(assignment);
    }

    internal static void WriteHeartbeat(ref WireReader r, WireWriter w, Request request, GroupCoordinator groups, bool coordinator)
    {
        short v = request.Version;
        string id = r.String();
        int generation = r.I32();
        string member = r.String();
        string? instance = v >= 3 ? r.NullableString() : null;
        r.End();
        if (v >= 1) w.I32(0);
        w.Write(!request.AllowsGroup(id, AclOperation.Read) ? Error.GroupAuthorizationFailed :
            coordinator ? groups.Heartbeat(id, member, generation, instance) : Error.NotCoordinator);
    }

    internal static void WriteLeaveGroup(ref WireReader r, WireWriter w, Request request, GroupCoordinator groups, bool coordinator)
    {
        short v = request.Version;
        string id = r.String();
        bool allowed = request.AllowsGroup(id, AclOperation.Read);
        if (v >= 3)
        {
            int count = r.Count();
            if (count > 64) throw new WireException("Too many group members to remove");
            var identities = new (string Member, string? Instance)[count];
            for (int i = 0; i < count; i++) identities[i] = (r.String(), r.NullableString());
            r.End();
            Error result = !allowed ? Error.GroupAuthorizationFailed :
                !coordinator ? Error.NotCoordinator : id.Length == 0 ? Error.InvalidGroupId : Error.None;
            w.I32(0);
            w.Write(result);
            w.I32(result == Error.None ? count : 0);
            if (result == Error.None)
                foreach (var (memberId, instance) in identities)
                {
                    w.String(memberId);
                    w.String(instance);
                    w.Write(groups.Leave(id, memberId, instance));
                }
            return;
        }
        string member = r.String();
        r.End();
        if (v >= 1) w.I32(0);
        w.Write(!allowed ? Error.GroupAuthorizationFailed : coordinator ? groups.Leave(id, member) : Error.NotCoordinator);
    }

    private void OffsetCommit(ref WireReader r, WireWriter w, Request request)
    {
        short v = request.Version;
        string group = r.String();
        bool groupAllowed = request.AllowsGroup(group, AclOperation.Read);
        int generation = v >= 1 ? r.I32() : -1;
        string member = v >= 1 ? r.String() : "";
        string? instance = v >= 7 ? r.NullableString() : null;
        if (v is >= 2 and <= 4) r.I64();
        var count = r.Count();
        var topics = new (string Name, (int Index, Error Error)[] Partitions)[count];
        var changes = new List<CommittedOffset>();
        for (int t = 0; t < count; t++)
        {
            string name = r.String();
            var topic = catalog.Get(name);
            bool topicAllowed = request.AllowsTopic(name, AclOperation.Read);
            int partitions = r.Count();
            var results = new (int Index, Error Error)[partitions];
            topics[t] = (name, results);
            for (int p = 0; p < partitions; p++)
            {
                int index = r.I32();
                long offset = r.I64();
                int epoch = v >= 6 ? r.I32() : -1;
                if (v == 1) r.I64();
                string? metadata = r.NullableString();
                var error = !groupAllowed ? Error.GroupAuthorizationFailed : !topicAllowed ? Error.TopicAuthorizationFailed :
                    GetPartition(topic, index) is null ? Error.UnknownTopicOrPartition :
                    offset < 0 ? Error.InvalidRequest : Error.None;
                if (error == Error.None) changes.Add(new(name, index, offset, epoch, metadata, topic!.Id));
                results[p] = (index, error);
            }
        }
        r.End();
        Error groupError = groupAllowed ? _groups.Commit(group, member, generation, changes, catalog, instance) : Error.GroupAuthorizationFailed;
        if (v >= 3) w.I32(0);
        w.I32(count);
        foreach (var (name, partitions) in topics)
        {
            w.String(name);
            w.I32(partitions.Length);
            foreach (var (index, error) in partitions)
            {
                w.I32(index);
                w.Write(error != Error.None ? error : groupError);
            }
        }
    }

    private void OffsetFetch(ref WireReader r, WireWriter w, Request request)
    {
        short v = request.Version;
        string group = r.String();
        bool allowed = request.AllowsGroup(group, AclOperation.Describe);
        int count = r.Count(nullable: v >= 2);
        var offsets = allowed ? catalog.GetOffsets(group) : [];
        if (v >= 3) w.I32(0);
        if (count < 0)
        {
            var topics = offsets.Where(offset => request.AllowsTopic(offset.Topic, AclOperation.Describe))
                .GroupBy(static offset => offset.Topic).ToArray();
            w.I32(topics.Length);
            foreach (var topic in topics)
            {
                w.String(topic.Key);
                w.I32(topic.Count());
                foreach (var offset in topic)
                    WriteOffset(w, v, offset.Partition, offset, Error.None);
            }
        }
        else
        {
            var byPartition = offsets.ToDictionary(static offset => (offset.Topic, offset.Partition));
            w.I32(count);
            for (int t = 0; t < count; t++)
            {
                string name = r.String();
                var topic = catalog.Get(name);
                bool topicAllowed = request.AllowsTopic(name, AclOperation.Describe);
                w.String(name);
                int partitions = r.Count();
                w.I32(partitions);
                for (int p = 0; p < partitions; p++)
                {
                    int index = r.I32();
                    var error = !allowed ? Error.GroupAuthorizationFailed : !topicAllowed ? Error.TopicAuthorizationFailed :
                        GetPartition(topic, index) is null ? Error.UnknownTopicOrPartition : Error.None;
                    WriteOffset(w, v, index, error == Error.None ? byPartition.GetValueOrDefault((name, index)) : null, error);
                }
            }
        }
        r.End();
        if (v >= 2) w.Write(!allowed ? Error.GroupAuthorizationFailed : group.Length == 0 ? Error.InvalidGroupId : Error.None);
    }

    internal static void WriteOffset(WireWriter w, short v, int index, CommittedOffset? offset, Error error, bool compact = false)
    {
        w.I32(index);
        w.I64(offset?.Offset ?? -1);
        if (v >= 5) w.I32(offset?.Epoch ?? -1);
        w.Text(offset?.Metadata ?? "", compact);
        w.Write(error);
        w.EndStruct(compact);
    }
}
