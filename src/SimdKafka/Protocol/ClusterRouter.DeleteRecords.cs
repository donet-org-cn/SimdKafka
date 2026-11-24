using DotNext.Net.Cluster.Consensus.Raft;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    private async ValueTask<bool> DeleteRecordsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request, CancellationToken stop)
    {
        bool compact = request.Flexible;
        var (timeout, topics) = Router.ReadDeleteRecords(body, compact);

        using var deadline = Deadline(timeout, stop);
        var image = cluster.Metadata.Image;
        w.I32(0);
        w.ArrayLength(topics.Length, compact);
        foreach (var (name, partitions) in topics)
        {
            var topic = image.Topics.GetValueOrDefault(name);
            bool allowed = request.AllowsTopic(name, AclOperation.Delete);
            w.Text(name, compact);
            w.ArrayLength(partitions.Length, compact);
            foreach (var (partition, offset) in partitions)
            {
                var replica = GetReplica(topic, partition);
                Error error = !allowed ? Error.TopicAuthorizationFailed : timeout < 0 ? Error.InvalidRequest :
                    topic is null || (uint)partition >= topic.Partitions.Length ? Error.UnknownTopicOrPartition :
                    replica is null ? MissingLocalReplica(topic, partition) : Error.None;
                long watermark = -1;
                if (error == Error.None)
                {
                    try { (error, watermark) = await cluster.DeleteRecordsAsync(topic!, partition, replica!, offset, deadline.Token); }
                    catch (OperationCanceledException) when (!stop.IsCancellationRequested) { error = Error.RequestTimedOut; }
                    catch (QuorumUnreachableException) { error = Error.RequestTimedOut; }
                    catch (NotLeaderException) { error = Error.NotLeaderOrFollower; }
                    catch (ObjectDisposedException) { error = Error.UnknownTopicOrPartition; }
                    catch (IOException) { error = Error.KafkaStorageError; }
                }
                w.I32(partition);
                w.I64(watermark);
                w.Write(error);
                w.EndStruct(compact);
            }
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
        return true;
    }
}
