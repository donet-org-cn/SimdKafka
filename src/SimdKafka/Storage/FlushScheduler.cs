using System.Threading.Channels;

namespace SimdKafka;

internal sealed class FlushScheduler : IDisposable
{
    private readonly Channel<PartitionLog> _ready = Channel.CreateUnbounded<PartitionLog>();
    private readonly Task[] _workers;

    internal FlushScheduler()
    {
        _workers = new Task[Math.Clamp(Environment.ProcessorCount / 2, 1, 8)];
        for (int i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Factory.StartNew(Work, CancellationToken.None,
                TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    internal void Schedule(PartitionLog log)
    {
        if (!_ready.Writer.TryWrite(log)) throw new ObjectDisposedException(nameof(FlushScheduler));
    }

    private void Work()
    {
        var reader = _ready.Reader;
        var group = new List<PendingAppend>(PartitionLog.MaxFlushBatches);
        while (reader.WaitToReadAsync().AsTask().GetAwaiter().GetResult())
            while (reader.TryRead(out var log))
                log.FlushPending(group);
    }

    public void Dispose()
    {
        _ready.Writer.TryComplete();
        Task.WaitAll(_workers);
    }
}

internal sealed class PendingAppend(Memory<byte> data, RecordBatch batch, long allocatedProducerId, bool logAppendTime)
    : TaskCompletionSource<AppendResult>(TaskCreationOptions.RunContinuationsAsynchronously)
{
    internal Memory<byte> Data { get; } = data;
    internal RecordBatch Batch { get; set; } = batch;
    internal long AllocatedProducerId { get; } = allocatedProducerId;
    internal bool LogAppendTime { get; } = logAppendTime;
    internal AppendResult Result;
    internal bool Written;
}

internal readonly record struct AppendResult(Error Error, long Offset, long LogAppendTime = -1);
