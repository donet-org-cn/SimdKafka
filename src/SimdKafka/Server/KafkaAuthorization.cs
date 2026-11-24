using System.Collections.Immutable;
using System.Net;

namespace SimdKafka;

internal enum AclResource : sbyte { Any = 1, Topic, Group, Cluster, TransactionalId, DelegationToken, User }
internal enum AclPattern : sbyte { Any = 1, Match, Literal, Prefixed }
internal enum AclOperation : sbyte
{
    Any = 1, All, Read, Write, Create, Delete, Alter, Describe, ClusterAction, DescribeConfigs, AlterConfigs,
    IdempotentWrite, CreateTokens, DescribeTokens, TwoPhaseCommit
}
internal enum AclPermission : sbyte { Any = 1, Deny, Allow }

internal sealed record KafkaAcl(AclResource Resource, string Name, AclPattern Pattern, string Principal,
    string Host, AclOperation Operation, AclPermission Permission)
{
    internal const string ClusterName = "kafka-cluster";

    internal bool IsValid => Resource is >= AclResource.Topic and <= AclResource.User &&
        Name.Length is > 0 and <= 512 && !Name.Any(char.IsControl) &&
        Pattern is AclPattern.Literal or AclPattern.Prefixed &&
        (Resource != AclResource.Cluster || Name == ClusterName && Pattern == AclPattern.Literal) &&
        ValidPrincipal(Principal) && (Host == "*" || IPAddress.TryParse(Host, out _)) &&
        Permission is AclPermission.Allow or AclPermission.Deny && KafkaAuthorization.Supported(Resource, Operation);

    internal static bool ValidPrincipal(string principal) =>
        principal.StartsWith("User:", StringComparison.Ordinal) && principal.Length is > 5 and <= 2_048 &&
        !principal.Any(char.IsControl);

    internal bool Matches(string name) => Pattern == AclPattern.Prefixed ?
        name.StartsWith(Name, StringComparison.Ordinal) : Name == "*" || Name == name;

    internal int MaximumEncodedSize => 256 + 6 * (Name.Length + Principal.Length + Host.Length);
}

internal sealed record KafkaAclFilter(AclResource Resource, string? Name, AclPattern Pattern, string? Principal,
    string? Host, AclOperation Operation, AclPermission Permission)
{
    internal bool IsValid => Resource is >= AclResource.Any and <= AclResource.User &&
        Pattern is >= AclPattern.Any and <= AclPattern.Prefixed && Operation is >= AclOperation.Any and <= AclOperation.TwoPhaseCommit &&
        Permission is >= AclPermission.Any and <= AclPermission.Allow;

    internal bool Matches(KafkaAcl acl) =>
        (Resource == AclResource.Any || Resource == acl.Resource) &&
        (Pattern is AclPattern.Any or AclPattern.Match || Pattern == acl.Pattern) &&
        (Name is null || (Pattern == AclPattern.Match ? acl.Matches(Name) : Name == acl.Name)) &&
        (Principal is null || Principal == acl.Principal) && (Host is null || Host == acl.Host) &&
        (Operation == AclOperation.Any || Operation == acl.Operation) &&
        (Permission == AclPermission.Any || Permission == acl.Permission);
}

internal sealed record AclMutation(KafkaAcl? Create = null, KafkaAclFilter? Delete = null)
{
    internal (MetadataResult Result, ImmutableArray<KafkaAcl> Next) Apply(ImmutableArray<KafkaAcl> current)
    {
        if (Create is { } acl && Delete is null && acl.IsValid)
        {
            if (current.Contains(acl)) return (new(Error.None), current);
            if (current.Length >= 4_096 || current.Sum(static item => item.MaximumEncodedSize) + acl.MaximumEncodedSize > 4_194_304)
                return (new(Error.InvalidRequest), current);
            return (new(Error.None), current.Add(acl));
        }
        if (Create is null && Delete is { IsValid: true } filter)
        {
            var removed = current.Where(filter.Matches).ToArray();
            return (new(Error.None, RemovedAcls: removed), removed.Length == 0 ? current : current.RemoveRange(removed));
        }
        return (new(Error.InvalidRequest), current);
    }
}

internal sealed record AclSubject(string Principal, string Host, bool SuperUser)
{
    internal KafkaAuthorization Authorize(ImmutableArray<KafkaAcl> acls) => new(Principal, Host, acls, SuperUser);
}

internal sealed class KafkaAuthorization(string principal, string host, ImmutableArray<KafkaAcl> acls, bool superUser)
{
    internal bool SuperUser => superUser;
    internal AclSubject Subject => new(principal, host, superUser);

    internal bool Allows(AclResource resource, string name, AclOperation operation)
    {
        if (superUser) return true;
        bool allowed = false;
        foreach (var acl in acls)
        {
            if (acl.Resource != resource || acl.Principal != principal && acl.Principal != "User:*" ||
                acl.Host != "*" && acl.Host != host || !acl.Matches(name)) continue;
            if (acl.Permission == AclPermission.Deny && (acl.Operation == AclOperation.All || acl.Operation == operation))
                return false;
            if (acl.Permission == AclPermission.Allow && MatchesOperation(acl.Operation, operation)) allowed = true;
        }
        return allowed;
    }

    internal bool AllowsIdempotence(IEnumerable<string> topics)
    {
        if (Allows(AclResource.Cluster, KafkaAcl.ClusterName, AclOperation.IdempotentWrite)) return true;
        if (topics.Any(topic => Allows(AclResource.Topic, topic, AclOperation.Write))) return true;
        return acls.Any(acl => acl.Resource == AclResource.Topic && acl.Permission == AclPermission.Allow &&
            acl.Name != "*" && acl.Pattern == AclPattern.Literal && Allows(AclResource.Topic, acl.Name, AclOperation.Write));
    }

    internal int Operations(AclResource resource, string name)
    {
        int bits = 0;
        for (var operation = AclOperation.Read; operation <= AclOperation.TwoPhaseCommit; operation++)
            if (Supported(resource, operation) && Allows(resource, name, operation)) bits |= 1 << (int)operation;
        return bits;
    }

    internal static bool Supported(AclResource resource, AclOperation operation) => operation == AclOperation.All ||
        resource switch
        {
            AclResource.Topic => operation is AclOperation.Read or AclOperation.Write or AclOperation.Create or AclOperation.Delete or
                AclOperation.Alter or AclOperation.Describe or AclOperation.DescribeConfigs or AclOperation.AlterConfigs,
            AclResource.Group => operation is AclOperation.Read or AclOperation.Delete or AclOperation.Describe,
            AclResource.Cluster => operation is AclOperation.Create or AclOperation.Alter or AclOperation.Describe or
                AclOperation.ClusterAction or AclOperation.DescribeConfigs or AclOperation.AlterConfigs or AclOperation.IdempotentWrite,
            AclResource.TransactionalId => operation is AclOperation.Write or AclOperation.Describe or AclOperation.TwoPhaseCommit,
            AclResource.DelegationToken => operation == AclOperation.Describe,
            AclResource.User => operation is AclOperation.CreateTokens or AclOperation.DescribeTokens,
            _ => false
        };

    private static bool MatchesOperation(AclOperation grant, AclOperation requested) =>
        grant == AclOperation.All || grant == requested ||
        requested == AclOperation.Describe && grant is AclOperation.Read or AclOperation.Write or AclOperation.Delete or AclOperation.Alter ||
        requested == AclOperation.DescribeConfigs && grant == AclOperation.AlterConfigs;
}
