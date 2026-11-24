namespace SimdKafka;

internal sealed class ShareSessionCache
{
    internal sealed class Session
    {
        internal int NextEpoch = 1;
        internal bool Busy;
        internal long Deadline;
        internal readonly HashSet<ConsumerPartition> Partitions = [];
    }

    private readonly Dictionary<(string Group, Guid Cycle), Session> _sessions = [];
    private const int Capacity = 4096;

    internal Error Begin(string group, Guid cycle, int epoch, bool acknowledge, ConsumerPartition[] added,
        ConsumerPartition[] forgotten, out Session? handle, out ConsumerPartition[] partitions)
    {
        handle = null; partitions = [];
        if (epoch < -1 || epoch == 0 && acknowledge) return Error.InvalidShareSessionEpoch;
        var key = (group, cycle);
        long now = Environment.TickCount64;
        lock (_sessions)
        {
            var current = _sessions.GetValueOrDefault(key);
            if (current?.Busy == true) return Error.InvalidShareSessionEpoch;
            if (epoch == 0)
            {
                if (_sessions.Count >= Capacity && current is null)
                {
                    foreach (var item in _sessions.Where(pair => !pair.Value.Busy && pair.Value.Deadline <= now).Select(static pair => pair.Key).ToArray())
                        _sessions.Remove(item);
                    if (_sessions.Count >= Capacity) return Error.ShareSessionLimitReached;
                }
                current = new();
            }
            else
            {
                if (current is null || current.Deadline <= now) return Error.ShareSessionNotFound;
                if (epoch != -1 && epoch != current.NextEpoch) return Error.InvalidShareSessionEpoch;
            }
            if (!acknowledge && epoch != -1)
            {
                int modified = current.Partitions.Count(partition => !forgotten.Contains(partition)) +
                    added.Count(partition => !current.Partitions.Contains(partition) || forgotten.Contains(partition));
                if (modified > BrokerOptions.MaxPartitions) return Error.InvalidRequest;
                current.Partitions.ExceptWith(forgotten);
                current.Partitions.UnionWith(added);
            }
            current.NextEpoch = epoch is -1 or int.MaxValue ? 1 : epoch + 1;
            current.Deadline = now + 120_000;
            current.Busy = true;
            _sessions[key] = current;
            handle = current;
            partitions = acknowledge || epoch == -1 ? added : [.. current.Partitions];
            return Error.None;
        }
    }

    internal void End(string group, Guid cycle, Session handle, bool close)
    {
        lock (_sessions)
        {
            if (close) _sessions.Remove((group, cycle));
            else handle.Busy = false;
        }
    }
}
