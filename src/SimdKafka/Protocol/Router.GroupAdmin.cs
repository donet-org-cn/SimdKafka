namespace SimdKafka;

internal sealed partial class Router
{
    private void ListGroups(ref WireReader r, WireWriter w, Request request)
        => WriteGroupList(ref r, w, request, _groups.List(catalog), Error.None);

    internal static void WriteGroupList(ref WireReader r, WireWriter w, Request request,
        GroupSnapshot[] snapshot, Error error)
    {
        bool compact = request.Flexible;
        short version = request.Version;
        var states = version >= 4 ? TextArray(ref r, compact) : [];
        var types = version >= 5 ? TextArray(ref r, compact) : [];
        r.EndStruct(compact);
        r.End();

        var groups = snapshot.Where(group =>
            (request.AllowsCluster(AclOperation.Describe) || request.AllowsGroup(group.Id, AclOperation.Describe)) &&
            (states.Length == 0 || states.Contains(group.State, StringComparer.OrdinalIgnoreCase)) &&
            (types.Length == 0 || types.Contains(group.GroupType, StringComparer.OrdinalIgnoreCase))).ToArray();
        if (version >= 1) w.I32(0);
        w.Write(error);
        w.ArrayLength(groups.Length, compact);
        foreach (var group in groups)
        {
            w.Text(group.Id, compact);
            w.Text(group.Type, compact);
            if (version >= 4) w.Text(group.State, compact);
            if (version >= 5) w.Text(group.GroupType, compact);
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
    }

    private void DescribeGroups(ref WireReader r, WireWriter w, Request request)
        => WriteGroupDescriptions(ref r, w, request, id => _groups.Describe(id, catalog), Error.None);

    internal static void WriteGroupDescriptions(ref WireReader r, WireWriter w, Request request,
        Func<string, GroupSnapshot?> describe, Error coordinator)
    {
        bool compact = request.Flexible;
        short version = request.Version;
        var ids = TextArray(ref r, compact);
        bool authorized = version >= 3 && r.Bool();
        r.EndStruct(compact);
        r.End();
        if (version >= 1) w.I32(0);
        w.ArrayLength(ids.Length, compact);
        foreach (string id in ids)
        {
            bool allowed = request.AllowsGroup(id, AclOperation.Describe);
            var group = allowed && coordinator == Error.None ? describe(id) : null;
            var error = !allowed ? Error.GroupAuthorizationFailed : id.Length == 0 ? Error.InvalidGroupId :
                coordinator != Error.None ? coordinator : group is null ? Error.GroupIdNotFound : Error.None;
            w.Write(error);
            if (version >= 6) w.Text(error == Error.None ? null : error.ToString(), compact);
            w.Text(id, compact);
            w.Text(group?.State ?? "Dead", compact);
            w.Text(group?.Type ?? "", compact);
            w.Text(group?.Protocol ?? "", compact);
            var members = group?.Members ?? [];
            w.ArrayLength(members.Length, compact);
            foreach (var member in members)
            {
                w.Text(member.Id, compact);
                if (version >= 4) w.Text(member.Instance, compact);
                w.Text(member.ClientId, compact);
                w.Text(member.ClientHost, compact);
                w.Bytes(member.Metadata, compact);
                w.Bytes(member.Assignment, compact);
                w.EndStruct(compact);
            }
            if (version >= 3)
                w.I32(authorized && error == Error.None ?
                    request.Access?.Operations(AclResource.Group, id) ?? ((1 << 3) | (1 << 6) | (1 << 8)) : int.MinValue);
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
    }

    private void DeleteGroups(ref WireReader r, WireWriter w, Request request)
    {
        bool compact = request.Flexible;
        var ids = TextArray(ref r, compact);
        r.EndStruct(compact);
        r.End();
        w.I32(0);
        w.ArrayLength(ids.Length, compact);
        foreach (var id in ids)
        {
            w.Text(id, compact);
            w.Write(request.AllowsGroup(id, AclOperation.Delete) ? _groups.Delete(id, catalog) : Error.GroupAuthorizationFailed);
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
    }

    private void OffsetDelete(ref WireReader r, WireWriter w, Request request)
    {
        string group = r.String();
        int count = r.Count();
        var topics = new (string Name, int[] Partitions, bool Allowed)[count];
        var selected = new HashSet<(string Topic, int Partition)>();
        for (int i = 0; i < count; i++)
        {
            string name = r.String();
            bool allowed = request.AllowsTopic(name, AclOperation.Read);
            int[] keys = new int[r.Count()];
            for (int p = 0; p < keys.Length; p++)
            {
                keys[p] = r.I32();
                if (allowed && GetPartition(catalog.Get(name), keys[p]) is not null) selected.Add((name, keys[p]));
            }
            topics[i] = (name, keys, allowed);
        }
        r.End();
        var result = request.AllowsGroup(group, AclOperation.Delete) ? _groups.Delete(group, catalog, selected) : Error.GroupAuthorizationFailed;
        bool topicErrors = result is Error.None or Error.GroupSubscribedToTopic;
        w.Write(topicErrors ? Error.None : result);
        w.I32(0);
        w.I32(topicErrors ? count : 0);
        if (!topicErrors) return;
        foreach (var topic in topics)
        {
            w.String(topic.Name);
            w.I32(topic.Partitions.Length);
            foreach (int index in topic.Partitions)
            {
                w.I32(index);
                w.Write(!topic.Allowed ? Error.TopicAuthorizationFailed :
                    selected.Contains((topic.Name, index)) ? result : Error.UnknownTopicOrPartition);
            }
        }
    }

    internal static string[] TextArray(ref WireReader r, bool compact)
    {
        var result = new string[r.ArrayLength(compact)];
        for (int i = 0; i < result.Length; i++) result[i] = r.Text(compact);
        return result;
    }
}
