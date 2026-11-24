using System.Collections.Immutable;

namespace SimdKafka;

internal readonly record struct AclReadResult(Error Error, ImmutableArray<KafkaAcl> Acls);

internal sealed partial class Router
{
    internal static async ValueTask<bool> HandleAclsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        Func<ValueTask<AclReadResult>> read, Func<AclMutation, ValueTask<MetadataResult>> apply)
    {
        bool compact = request.Flexible;
        var r = new WireReader(body.Span);
        var operation = request.Api == Api.DescribeAcls ? AclOperation.Describe : AclOperation.Alter;
        Error access = request.Access is null ? Error.SecurityDisabled :
            request.AllowsCluster(operation) ? Error.None : Error.ClusterAuthorizationFailed;
        if (request.Api == Api.DescribeAcls)
        {
            var filter = ReadAclFilter(ref r, request);
            r.End();
            var result = access != Error.None ? new AclReadResult(access, []) :
                !filter.IsValid ? new AclReadResult(Error.InvalidRequest, []) : await read();
            w.I32(0);
            WriteAclError(w, result.Error, compact);
            var resources = result.Acls.Where(filter.Matches).GroupBy(static acl => (acl.Resource, acl.Name, acl.Pattern)).ToArray();
            w.ArrayLength(resources.Length, compact);
            foreach (var resource in resources)
            {
                WriteAclResource(w, request, resource.Key.Resource, resource.Key.Name, resource.Key.Pattern);
                w.ArrayLength(resource.Count(), compact);
                foreach (var acl in resource)
                {
                    WriteAclEntry(w, compact, acl);
                    w.EndStruct(compact);
                }
                w.EndStruct(compact);
            }
            w.EndStruct(compact);
            return true;
        }

        int count = r.ArrayLength(compact);
        if (count > 1_024) throw new WireException("Too many ACL changes");
        var mutations = new AclMutation[count];
        bool delete = request.Api == Api.DeleteAcls;
        for (int i = 0; i < count; i++)
        {
            if (delete) mutations[i] = new(Delete: ReadAclFilter(ref r, request));
            else
            {
                var resource = (AclResource)r.I8();
                string name = r.Text(compact);
                var pattern = request.Version >= 1 ? (AclPattern)r.I8() : AclPattern.Literal;
                var acl = new KafkaAcl(resource, name, pattern, r.Text(compact), r.Text(compact),
                    (AclOperation)r.I8(), (AclPermission)r.I8());
                r.EndStruct(compact);
                mutations[i] = new(Create: acl);
            }
        }
        r.EndStruct(compact); r.End();
        w.I32(0);
        w.ArrayLength(count, compact);
        foreach (var mutation in mutations)
        {
            var result = access != Error.None ? new MetadataResult(access) :
                mutation.Create is { IsValid: false } || mutation.Delete is { IsValid: false } ?
                    new MetadataResult(Error.InvalidRequest) : await apply(mutation);
            WriteAclError(w, result.Error, compact);
            if (delete)
            {
                var removed = result.RemovedAcls ?? [];
                w.ArrayLength(removed.Length, compact);
                foreach (var acl in removed)
                {
                    WriteAclError(w, Error.None, compact);
                    WriteAclResource(w, request, acl.Resource, acl.Name, acl.Pattern);
                    WriteAclEntry(w, compact, acl);
                    w.EndStruct(compact);
                }
            }
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
        return true;
    }

    private static KafkaAclFilter ReadAclFilter(ref WireReader r, Request request)
    {
        bool compact = request.Flexible;
        var resource = (AclResource)r.I8();
        string? name = r.NullableText(compact);
        var pattern = request.Version >= 1 ? (AclPattern)r.I8() : AclPattern.Literal;
        var filter = new KafkaAclFilter(resource, name, pattern, r.NullableText(compact), r.NullableText(compact),
            (AclOperation)r.I8(), (AclPermission)r.I8());
        r.EndStruct(compact);
        return filter;
    }

    private static void WriteAclError(WireWriter w, Error error, bool compact)
    {
        w.Write(error);
        w.Text(error == Error.None ? null : error.ToString(), compact);
    }

    private static void WriteAclResource(WireWriter w, Request request, AclResource resource, string name, AclPattern pattern)
    {
        w.I8((sbyte)resource);
        w.Text(name, request.Flexible);
        if (request.Version >= 1) w.I8((sbyte)pattern);
    }

    private static void WriteAclEntry(WireWriter w, bool compact, KafkaAcl acl)
    {
        w.Text(acl.Principal, compact);
        w.Text(acl.Host, compact);
        w.I8((sbyte)acl.Operation);
        w.I8((sbyte)acl.Permission);
    }
}
