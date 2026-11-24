using DotNext.Net.Cluster.Consensus.Raft;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    internal override TokenDirectory CurrentTokens => cluster.Metadata.Image.Tokens;

    internal override async ValueTask<(Error Error, TokenDirectory? Directory)> ReadTokensAsync(CancellationToken stop)
    {
        Error status = await RefreshConfigurationsAsync(stop);
        return (status, status == Error.None ? CurrentTokens : null);
    }

    private async ValueTask<MetadataResult> ChangeTokenAsync(TokenChange change, CancellationToken stop)
    {
        using var deadline = Deadline(10_000, stop);
        try
        {
            return await cluster.ExecuteMetadataAsync(new(MetadataOperation.DelegationToken, Token: change), deadline.Token);
        }
        catch (OperationCanceledException) when (!stop.IsCancellationRequested) { return new(Error.RequestTimedOut); }
        catch (QuorumUnreachableException) { return new(Error.RequestTimedOut); }
    }
}
