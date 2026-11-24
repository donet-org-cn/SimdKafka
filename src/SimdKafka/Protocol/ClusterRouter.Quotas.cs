using DotNext.Net.Cluster.Consensus.Raft;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    internal override ClientQuotas CurrentQuotas => cluster.Metadata.Image.Quotas;

    private async ValueTask<QuotaReadResult> ReadQuotasAsync(CancellationToken stop)
    {
        Error status = await RefreshConfigurationsAsync(stop);
        return new(status, status == Error.None ? CurrentQuotas : ClientQuotas.Empty);
    }

    private async ValueTask<Error> AlterQuotaAsync(QuotaUpdate update, CancellationToken stop)
    {
        using var deadline = Deadline(10_000, stop);
        try
        {
            if (update.ValidateOnly)
            {
                var current = await ReadQuotasAsync(deadline.Token);
                return current.Error == Error.None ? current.Directory.Apply(update).Error : current.Error;
            }
            var command = new MetadataCommand(MetadataOperation.AlterQuota, Quota: update);
            return (await cluster.ExecuteMetadataAsync(command, deadline.Token)).Error;
        }
        catch (OperationCanceledException) when (!stop.IsCancellationRequested) { return Error.RequestTimedOut; }
        catch (QuorumUnreachableException) { return Error.RequestTimedOut; }
    }
}
