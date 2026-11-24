using System.Collections.Immutable;
using DotNext.Net.Cluster.Consensus.Raft;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    internal override async ValueTask<ImmutableArray<KafkaAcl>> ReadAclsAsync(CancellationToken stop) =>
        (await ReadAclSnapshotAsync(stop)).Acls;

    private async ValueTask<AclReadResult> ReadAclSnapshotAsync(CancellationToken stop)
    {
        Error status = await RefreshConfigurationsAsync(stop);
        return status == Error.None ? new(status, cluster.Metadata.Image.Acls) : new(status, []);
    }

    private async ValueTask<MetadataResult> MutateAclAsync(AclMutation mutation, CancellationToken stop)
    {
        using var deadline = Deadline(10_000, stop);
        try { return await cluster.ExecuteMetadataAsync(new(MetadataOperation.Acl, Acl: mutation), deadline.Token); }
        catch (OperationCanceledException) when (!stop.IsCancellationRequested) { return new(Error.RequestTimedOut); }
        catch (QuorumUnreachableException) { return new(Error.RequestTimedOut); }
    }
}
