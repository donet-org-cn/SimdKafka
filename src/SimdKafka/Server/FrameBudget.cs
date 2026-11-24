namespace SimdKafka;

internal sealed class FrameBudget(int capacity)
{
    private int _available = capacity;
    private int _waiters;
    private readonly LogChanges _released = new();

    internal ValueTask AcquireAsync(int bytes, CancellationToken stop) => TryAcquire(bytes) ?
        ValueTask.CompletedTask : WaitAsync(bytes, stop);

    internal void Release(int bytes)
    {
        Interlocked.Add(ref _available, bytes);
        if (Volatile.Read(ref _waiters) != 0) _released.Signal();
    }

    private bool TryAcquire(int bytes)
    {
        int available;
        do
        {
            available = Volatile.Read(ref _available);
            if (available < bytes) return false;
        } while (Interlocked.CompareExchange(ref _available, available - bytes, available) != available);
        return true;
    }

    private async ValueTask WaitAsync(int bytes, CancellationToken stop)
    {
        Interlocked.Increment(ref _waiters);
        try
        {
            while (true)
            {
                var changed = _released.Capture();
                stop.ThrowIfCancellationRequested();
                if (TryAcquire(bytes)) return;
                await changed.WaitAsync(stop);
            }
        }
        finally { Interlocked.Decrement(ref _waiters); }
    }
}
