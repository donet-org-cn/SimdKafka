using DotNext.Net.Cluster.Consensus.Raft;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    internal override async ValueTask<ScramReadResult> ReadScramAsync(CancellationToken stop)
    {
        Error status = await RefreshConfigurationsAsync(stop);
        return new(status, status == Error.None ? cluster.Metadata.Image.Scram : null);
    }

    private async ValueTask<Error> AlterScramAsync(string user, ScramChange change, CancellationToken stop)
    {
        using var deadline = Deadline(10_000, stop);
        try
        {
            var command = new MetadataCommand(MetadataOperation.AlterScram, user, Scram: change);
            return (await cluster.ExecuteMetadataAsync(command, deadline.Token)).Error;
        }
        catch (OperationCanceledException) when (!stop.IsCancellationRequested) { return Error.RequestTimedOut; }
        catch (QuorumUnreachableException) { return Error.RequestTimedOut; }
    }
}
