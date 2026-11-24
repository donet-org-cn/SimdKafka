using DotNext.Net.Cluster.Consensus.Raft;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    private async ValueTask<bool> ListConfigurationResourcesAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        Error status = request.AllowsCluster(AclOperation.DescribeConfigs) ? await RefreshConfigurationsAsync(stop) : Error.ClusterAuthorizationFailed;
        var image = cluster.Metadata.Image;
        var r = new WireReader(body.Span);
        Router.WriteConfigResources(ref r, w, request, status, image.Configurations,
            image.Topics.Values.Select(static topic => (topic.Name, topic.Id)));
        return true;
    }

    private async ValueTask<bool> DescribeConfigurationsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        Error refreshed = await RefreshConfigurationsAsync(stop);
        var image = cluster.Metadata.Image;
        var r = new WireReader(body.Span);
        Router.WriteConfigs(ref r, w, request, (type, name) => refreshed != Error.None ? (refreshed, []) : type switch
        {
            KafkaConfigurations.Topic when image.Topics.TryGetValue(name, out var topic) => (Error.None,
                Router.TopicConfiguration(image.Configurations, topic.Id, cluster.Local.Id, standalone: false)),
            KafkaConfigurations.Topic => (Error.UnknownTopicOrPartition, []),
            KafkaConfigurations.Broker when KafkaConfigurations.TryBrokerId(name, out int id) &&
                (id == KafkaConfigurations.DefaultBroker || cluster.Members.Any(member => member.Id == id)) =>
                (Error.None, Router.BrokerConfig(options, cluster.Members.Length, image.Configurations, id)),
            _ => (Error.InvalidRequest, [])
        });
        return true;
    }

    private async ValueTask<Error> AlterConfigurationAsync(ConfigUpdate update, CancellationToken stop)
    {
        Error refreshed = await RefreshConfigurationsAsync(stop);
        if (refreshed != Error.None) return refreshed;
        Guid id = update.Type == KafkaConfigurations.Topic ?
            cluster.Metadata.Image.Topics.GetValueOrDefault(update.Name)?.Id ?? Guid.Empty : Guid.Empty;
        var command = new MetadataCommand(MetadataOperation.AlterConfig, TopicId: id, ConfigUpdate: update);
        if (update.ValidateOnly) return cluster.Metadata.ValidateConfiguration(command);
        using var deadline = Deadline(10_000, stop);
        try { return (await cluster.ExecuteMetadataAsync(command, deadline.Token)).Error; }
        catch (OperationCanceledException) when (!stop.IsCancellationRequested) { return Error.RequestTimedOut; }
        catch (QuorumUnreachableException) { return Error.RequestTimedOut; }
    }

    private async ValueTask<Error> RefreshConfigurationsAsync(CancellationToken stop)
    {
        using var deadline = Deadline(4_000, stop);
        try { await cluster.Metadata.ReadBarrierAsync(deadline.Token); return Error.None; }
        catch (NotLeaderException) { return Error.NotController; }
        catch (OperationCanceledException) when (!stop.IsCancellationRequested) { return Error.RequestTimedOut; }
        catch (QuorumUnreachableException) { return Error.RequestTimedOut; }
    }
}
