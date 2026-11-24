namespace SimdKafka;

internal sealed partial class Router
{
    internal static (int Timeout, (string Name, (int Id, long Offset)[] Partitions)[] Topics) ReadDeleteRecords(
        ReadOnlyMemory<byte> body, bool compact)
    {
        var r = new WireReader(body.Span);
        var topics = new (string Name, (int Id, long Offset)[] Partitions)[r.ArrayLength(compact)];
        for (int i = 0; i < topics.Length; i++)
        {
            string name = r.Text(compact);
            var partitions = new (int Id, long Offset)[r.ArrayLength(compact)];
            for (int p = 0; p < partitions.Length; p++)
            {
                partitions[p] = (r.I32(), r.I64());
                r.EndStruct(compact);
            }
            r.EndStruct(compact);
            topics[i] = (name, partitions);
        }
        int timeout = r.I32();
        r.EndStruct(compact);
        r.End();
        return (timeout, topics);
    }

    private void DeleteRecords(ReadOnlyMemory<byte> body, WireWriter w, Request request)
    {
        bool compact = request.Flexible;
        var (timeout, topics) = ReadDeleteRecords(body, compact);
        w.I32(0);
        w.ArrayLength(topics.Length, compact);
        foreach (var (name, partitions) in topics)
        {
            var topic = catalog.Get(name);
            bool allowed = request.AllowsTopic(name, AclOperation.Delete);
            w.Text(name, compact);
            w.ArrayLength(partitions.Length, compact);
            foreach (var (partition, offset) in partitions)
            {
                var log = GetPartition(topic, partition);
                var (error, watermark) = !allowed ? (Error.TopicAuthorizationFailed, -1L) :
                    timeout < 0 ? (Error.InvalidRequest, -1L) : log is null ? (Error.UnknownTopicOrPartition, -1L) :
                    log.AdvanceLogStart(offset);
                if (error == Error.None) _changes.Signal();
                w.I32(partition);
                w.I64(watermark);
                w.Write(error);
                w.EndStruct(compact);
            }
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
    }
}
