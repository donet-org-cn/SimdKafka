namespace SimdKafka;

internal readonly record struct FetchResponseState(Error Error, long HighWatermark, long LogStartOffset, int PreferredReplica,
    long LastStableOffset = -1);

internal sealed class FetchSession
{
    private const int MaxPartitions = 256;
    private static int _lastId;
    private int _id, _nextEpoch;
    private bool _byId;
    private readonly Dictionary<PartitionKey, Location> _locations = [];
    private readonly Dictionary<PartitionKey, FetchResponseState> _lastResponses = [];
    private FetchTopic[] _topics = [];

    private readonly record struct PartitionKey(string? Name, Guid TopicId, int Partition);
    private readonly record struct Location(FetchPartition[] Partitions, int Index);

    internal Error Resolve(int id, int epoch, bool byId, bool valid, FetchTopic[] forgotten, ref FetchTopic[] updated,
        out int sessionId)
    {
        sessionId = 0;
        if (epoch < -1 || id < 0) return Error.InvalidFetchSessionEpoch;
        if (epoch <= 0)
        {
            Close();
            if (epoch == -1 || !valid || !Fits(updated))
                return Error.None;

            _topics = updated;
            if (!IndexTopics()) { Close(); return Error.InvalidRequest; }
            _byId = byId;
            int newId;
            do newId = Interlocked.Increment(ref _lastId) & int.MaxValue; while (newId == 0);
            sessionId = _id = newId;
            _nextEpoch = 1;
            return Error.None;
        }
        if (_id == 0 || id != _id) return Error.FetchSessionIdNotFound;
        if (epoch != _nextEpoch) return Error.InvalidFetchSessionEpoch;
        if (_byId != byId) return Error.FetchSessionTopicIdError;
        if (!valid) return Error.InvalidFetchSize;
        if (!Fits(updated) || !Fits(forgotten)) { Close(); return Error.FetchSessionIdNotFound; }

        bool rebuild = forgotten.Length != 0;
        foreach (var topic in updated)
            foreach (var partition in topic.Partitions)
                if (!_locations.ContainsKey(new(topic.Name, topic.Id, partition.Id))) rebuild = true;

        if (rebuild)
        {
            var partitions = _locations.ToDictionary(static pair => pair.Key,
                static pair => pair.Value.Partitions[pair.Value.Index]);
            foreach (var topic in forgotten)
                foreach (var partition in topic.Partitions)
                {
                    var key = new PartitionKey(topic.Name, topic.Id, partition.Id);
                    partitions.Remove(key);
                    _lastResponses.Remove(key);
                }
            foreach (var topic in updated)
                foreach (var partition in topic.Partitions)
                    partitions[new(topic.Name, topic.Id, partition.Id)] = partition;
            if (partitions.Count > MaxPartitions) { Close(); return Error.FetchSessionIdNotFound; }
            _topics = partitions.GroupBy(static pair => (pair.Key.Name, pair.Key.TopicId))
                .Select(static group => new FetchTopic(group.Key.Name, group.Key.TopicId,
                    group.Select(static pair => pair.Value).ToArray())).ToArray();
            IndexTopics();
        }
        else
        {
            foreach (var topic in updated)
                foreach (var partition in topic.Partitions)
                {
                    var location = _locations[new(topic.Name, topic.Id, partition.Id)];
                    location.Partitions[location.Index] = partition;
                }
        }
        updated = _topics;
        sessionId = _id;
        _nextEpoch = epoch == int.MaxValue ? 1 : epoch + 1;
        return Error.None;
    }

    internal bool RequiresUpdate(FetchTopic topic, int partition, FetchResponseState state) =>
        !_lastResponses.TryGetValue(new(topic.Name, topic.Id, partition), out var previous) || previous != state;

    internal void RecordResponse(FetchTopic topic, int partition, FetchResponseState state) =>
        _lastResponses[new(topic.Name, topic.Id, partition)] = state;

    private static bool Fits(FetchTopic[] topics) =>
        topics.Length <= MaxPartitions && topics.Sum(static topic => (long)topic.Partitions.Length) <= MaxPartitions;

    private bool IndexTopics()
    {
        _locations.Clear();
        foreach (var topic in _topics)
            for (int p = 0; p < topic.Partitions.Length; p++)
                if (!_locations.TryAdd(new(topic.Name, topic.Id, topic.Partitions[p].Id), new(topic.Partitions, p)))
                    return false;
        return true;
    }

    private void Close()
    {
        _id = _nextEpoch = 0;
        _locations.Clear();
        _lastResponses.Clear();
        _topics = [];
    }
}
