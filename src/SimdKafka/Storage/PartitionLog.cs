using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Microsoft.Win32.SafeHandles;

namespace SimdKafka;

internal sealed class TopicRetiredException : Exception;

internal sealed class PartitionLog : IDisposable
{
    private readonly record struct Entry(long Offset, long Position, int Size, RecordBatch Batch)
    {
        internal long EndOffset => Offset + Batch.Count;
    }

    private sealed class Producer
    {
        internal short Epoch;
        internal int Next;
        internal readonly Entry[] Recent = new Entry[5];
        private int _slot;

        internal void Add(Entry entry)
        {
            if (Epoch != entry.Batch.Epoch)
            {
                Array.Clear(Recent);
                _slot = 0;
                Epoch = entry.Batch.Epoch;
            }
            Next = ProducerSequence.Next(entry.Batch);
            Recent[_slot] = entry;
            _slot = (_slot + 1) % Recent.Length;
        }
    }

    private readonly SafeFileHandle _file;
    private readonly string _logStartPath;
    private readonly RemoteLogTier? _tier;
    private readonly List<Entry> _index = [];
    private readonly List<long> _timestamps = [];
    private readonly Dictionary<long, Producer> _producers = [];
    internal const int MaxFlushBatches = 64;
    private const int MaxFlushBytes = 16 * 1024 * 1024;
    private readonly FlushScheduler? _scheduler;
    private readonly Queue<PendingAppend> _pending = new();
    private readonly object _pageCacheWriter = new();
    private long _offset, _position, _logStartOffset;
    private bool _scheduled;
    private volatile bool _faulted;
    private const int Retired = int.MinValue;
    private int _uses = 1;
    private readonly TaskCompletionSource _closed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal PartitionLog(string path, FlushScheduler? scheduler, RemoteTierRoot? tier = null, string? remotePartition = null)
    {
        _scheduler = scheduler;
        _logStartPath = path + ".start";
        string manifest = path + ".tier.json";
        if (tier is null && File.Exists(manifest)) throw new InvalidDataException("Archived log requires --tiered-storage-dir");
        if (tier is not null) _tier = new(tier, remotePartition!, manifest);
        _file = File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read,
            FileOptions.RandomAccess);
        try
        {
            _tier?.RecoverEviction(_file);
            Recover();
            RecoverLogStart();
            // Catalog publication must follow durable creation of every partition file.
            if (_position == 0) RandomAccess.FlushToDisk(_file);
        }
        catch
        {
            _tier?.Dispose();
            _file.Dispose();
            throw;
        }
    }

    internal long LogStartOffset => Interlocked.Read(ref _logStartOffset);
    internal long EncodedLogBytes { get { lock (_index) return _position - (_tier?.EvictedBytes ?? 0); } }
    internal long ArchivedOffset => _tier?.ArchivedOffset ?? 0;

    internal bool ArchiveNext(long retainBytes, int segmentBytes, long oldestLocalTimestamp)
    {
        if (_tier is null || !TryUse()) return false;
        try
        {
            Entry first, last;
            lock (_index)
            {
                if (_faulted) return false;
                int index = FindFirstBatch(_tier.ArchivedOffset);
                if (index == _index.Count) return false;
                first = _index[index];
                long cutoff = retainBytes < 0 ? -1 : _position - retainBytes;
                last = default;
                bool eligibleByTime = false;
                for (; index < _index.Count; index++)
                {
                    var entry = _index[index];
                    long end = entry.Position + entry.Size;
                    bool aged = entry.Batch.MaxTimestamp >= 0 && entry.Batch.MaxTimestamp <= oldestLocalTimestamp;
                    if (!aged && end > cutoff) break;
                    eligibleByTime |= aged;
                    last = entry;
                    if (end - first.Position >= segmentBytes) break;
                }
                if (last.Size == 0 || !eligibleByTime && last.Position + last.Size - first.Position < segmentBytes) return false;
            }
            _tier.Publish(_file, first.Offset, last.EndOffset, first.Position, (int)(last.Position + last.Size - first.Position));
            return true;
        }
        finally { Release(); }
    }

    internal (Error Error, long Offset) AdvanceLogStart(long requested)
    {
        if (!TryUse()) return (Error.UnknownTopicOrPartition, -1);
        try
        {
            lock (_index)
            {
                if (_faulted) return (Error.KafkaStorageError, -1);
                long offset = requested == -1 ? _offset : requested;
                if (offset < 0 || offset > _offset) return (Error.OffsetOutOfRange, -1);
                if (offset <= _logStartOffset) return (Error.None, _logStartOffset);
                try
                {
                    if (_scheduler is null) DurableFileSystem.FlushData(_file);
                    Span<byte> record = stackalloc byte[sizeof(long) + SHA256.HashSizeInBytes];
                    BinaryPrimitives.WriteInt64BigEndian(record, offset);
                    SHA256.HashData(record[..sizeof(long)], record[sizeof(long)..]);
                    string temporary = _logStartPath + ".tmp";
                    using (var file = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None))
                    {
                        file.Write(record);
                        file.Flush(flushToDisk: true);
                    }
                    DurableFileSystem.Replace(temporary, _logStartPath);
                    Interlocked.Exchange(ref _logStartOffset, offset);
                    return (Error.None, offset);
                }
                catch (Exception e) when (DurableFileSystem.IsStorageError(e))
                {
                    _faulted = true;
                    ReportFault("partition log start write fault", e);
                    return (Error.KafkaStorageError, -1);
                }
            }
        }
        finally { Release(); }
    }

    private void RecoverLogStart()
    {
        FileStream source;
        try { source = File.OpenRead(_logStartPath); }
        catch (FileNotFoundException) { return; }
        Span<byte> record = stackalloc byte[sizeof(long) + SHA256.HashSizeInBytes];
        using var file = source;
        if (file.Length != record.Length) throw new InvalidDataException("Invalid partition log start record size");
        file.ReadExactly(record);
        Span<byte> checksum = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(record[..sizeof(long)], checksum);
        long offset = BinaryPrimitives.ReadInt64BigEndian(record);
        if (offset < 0 || offset > _offset || !checksum.SequenceEqual(record[sizeof(long)..]))
            throw new InvalidDataException("Invalid partition log start record");
        _logStartOffset = offset;
    }

    internal ValueTask<AppendResult> AppendAsync(Memory<byte> bytes, long allocatedProducerId, bool logAppendTime = false,
        TimestampBounds? timestampBounds = null)
    {
        if (!TryUse()) return ValueTask.FromResult(new AppendResult(Error.UnknownTopicOrPartition, -1));
        bool queued = false;
        try { return AppendWhileOpen(bytes, allocatedProducerId, logAppendTime, timestampBounds, out queued); }
        finally { if (!queued) Release(); }
    }

    private ValueTask<AppendResult> AppendWhileOpen(Memory<byte> bytes, long allocatedProducerId, bool logAppendTime,
        TimestampBounds? timestampBounds, out bool queued)
    {
        queued = false;
        if (bytes.Length > BrokerOptions.MaxBatchBytes) return ValueTask.FromResult(new AppendResult(Error.MessageTooLarge, -1));
        var error = RecordBatch.Parse(bytes, out var batch, timestampBounds: timestampBounds);
        if (error != Error.None) return ValueTask.FromResult(new AppendResult(error, -1));

        if (_scheduler is null)
        {
            lock (_pageCacheWriter)
            {
                if (_faulted) return ValueTask.FromResult(new AppendResult(Error.KafkaStorageError, -1));
                try
                {
                    long offset = _offset, position = _position;
                    var result = WriteBatch(bytes.Span, batch, allocatedProducerId, logAppendTime, ref offset, ref position, out var entry);
                    if (entry.Size != 0)
                    {
                        lock (_index)
                        {
                            if (_faulted) throw new IOException("Partition faulted before publication");
                            Index(entry);
                        }
                    }
                    return ValueTask.FromResult(result);
                }
                catch (Exception e) when (DurableFileSystem.IsStorageError(e))
                {
                    FailWrite(e);
                    return ValueTask.FromResult(new AppendResult(Error.KafkaStorageError, -1));
                }
            }
        }

        lock (_pending)
        {
            if (_faulted) return ValueTask.FromResult(new AppendResult(Error.KafkaStorageError, -1));
            var request = new PendingAppend(bytes, batch, allocatedProducerId, logAppendTime);
            _pending.Enqueue(request);
            queued = true;
            if (!_scheduled)
            {
                _scheduled = true;
                _scheduler.Schedule(this);
            }
            return new ValueTask<AppendResult>(request.Task);
        }
    }

    internal void FlushPending(List<PendingAppend> group)
    {
        lock (_pending)
        {
            int bytes = 0;
            while (group.Count < MaxFlushBatches && _pending.TryPeek(out var next) &&
                bytes + next.Data.Length <= MaxFlushBytes)
            {
                group.Add(_pending.Dequeue());
                bytes += next.Data.Length;
            }
        }

        FlushGroup(group);
        foreach (var request in group)
        {
            request.SetResult(request.Result);
            Release();
        }
        group.Clear();

        lock (_pending)
        {
            if (_pending.Count != 0) _scheduler!.Schedule(this);
            else _scheduled = false;
        }
    }

    private void FlushGroup(List<PendingAppend> group)
    {
        if (!_faulted)
        {
            try
            {
                long offset = _offset, position = _position;
                foreach (var request in group)
                {
                    request.Result = WriteBatch(request.Data.Span, request.Batch, request.AllocatedProducerId, request.LogAppendTime,
                        ref offset, ref position, out var entry);
                    request.Written = entry.Size != 0;
                    if (request.Written) request.Batch = entry.Batch;
                }
                if (position == _position) return;

                // Neither offset queries nor Fetch can observe any of these bytes until the flush succeeds.
                DurableFileSystem.FlushData(_file);
                lock (_index)
                {
                    if (_faulted) throw new IOException("Partition faulted before publication");
                    foreach (var request in group)
                    {
                        if (!request.Written) continue;
                        Index(new Entry(request.Result.Offset, _position, request.Data.Length, request.Batch));
                    }
                }
                return;
            }
            catch (Exception e)
            {
                // Each queued request must complete even if the storage API throws an unexpected exception.
                FailWrite(e);
            }
        }
        foreach (var request in group) request.Result = new(Error.KafkaStorageError, -1);
    }

    private AppendResult WriteBatch(Span<byte> bytes, RecordBatch batch, long allocatedProducerId, bool logAppendTime,
        ref long offset, ref long position, out Entry entry)
    {
        entry = default;
        if (batch.ProducerId >= 0)
        {
            if (batch.ProducerId == 0 || batch.ProducerId > allocatedProducerId)
                return new(Error.UnknownProducerId, -1);
            _producers.TryGetValue(batch.ProducerId, out var producer);
            if (producer is not null && batch.Epoch == producer.Epoch && batch.Sequence != producer.Next)
            {
                foreach (var recent in producer.Recent)
                    if (recent.Batch.Sequence == batch.Sequence && recent.Batch.Count == batch.Count)
                        return new(Error.None, recent.Offset, recent.Batch.AppendTimestamp);
            }
            var error = ProducerSequence.Validate(batch, producer?.Epoch ?? -1, producer?.Next ?? 0, offset == 0);
            if (error != Error.None) return new(error, -1);
        }

        long originalMax = batch.MaxTimestamp;
        uint originalCrc = 0;
        if (logAppendTime) batch = batch.StampAppendTime(bytes, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), out originalCrc);
        entry = new Entry(offset, position, bytes.Length, batch);
        BinaryPrimitives.WriteInt64BigEndian(bytes, offset);
        BinaryPrimitives.WriteInt32BigEndian(bytes[12..], 0);
        try { RandomAccess.Write(_file, bytes, position); }
        finally { if (logAppendTime) RecordBatch.RestoreCreateTime(bytes, originalMax, originalCrc); }
        offset = entry.EndOffset;
        position += bytes.Length;
        TrackProducer(entry);
        return new(Error.None, entry.Offset, batch.AppendTimestamp);
    }

    private void FailWrite(Exception error)
    {
        _faulted = true;
        ReportFault("partition storage fault", error);
        try
        {
            RandomAccess.SetLength(_file, _position);
            RandomAccess.FlushToDisk(_file);
        }
        catch (Exception rollback)
        {
            ReportFault("cannot roll back the uncommitted tail", rollback);
        }
    }

    private static void ReportFault(string action, Exception error)
    {
        try { Console.Error.WriteLine($"SimdKafka {action}: {error.Message}"); }
        catch (Exception logging) when (DurableFileSystem.IsStorageError(logging))
        {
            // A full filesystem can also prevent writing the broker log.
        }
    }

    internal (Error Error, long HighWatermark, long Position, int Size, long LogStartOffset) Select(
        long offset, int maxBytes, bool allowOversizeFirstBatch)
    {
        lock (_index)
        {
            if (Volatile.Read(ref _uses) < 0) return (Error.UnknownTopicOrPartition, -1, 0, 0, -1);
            if (_faulted) return (Error.KafkaStorageError, -1, 0, 0, -1);
            if (offset < _logStartOffset || offset > _offset) return (Error.OffsetOutOfRange, _offset, 0, 0, _logStartOffset);
            if (offset == _offset) return (Error.None, _offset, 0, 0, _logStartOffset);
            int lo = FindFirstBatch(offset);
            var first = _index[lo];
            if (first.Size > maxBytes)
                return (Error.None, _offset, first.Position, allowOversizeFirstBatch ? first.Size : 0, _logStartOffset);

            long budgetEnd = first.Position + maxBytes;
            lo++;
            int hi = _index.Count;
            while (lo < hi)
            {
                int mid = lo + (hi - lo) / 2;
                var candidate = _index[mid];
                if (candidate.Position + candidate.Size <= budgetEnd) lo = mid + 1;
                else hi = mid;
            }
            var last = _index[lo - 1];
            return (Error.None, _offset, first.Position, (int)(last.Position + last.Size - first.Position), _logStartOffset);
        }
    }

    private int FindFirstBatch(long offset)
    {
        int lo = 0, hi = _index.Count;
        while (lo < hi)
        {
            int mid = lo + (hi - lo) / 2;
            if (_index[mid].EndOffset <= offset) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    internal (Error Error, long Timestamp, long Offset) OffsetFor(long timestamp)
    {
        if (timestamp == -3) return MaximumTimestamp();
        int first;
        long start;
        lock (_index)
        {
            if (Volatile.Read(ref _uses) < 0) return (Error.UnknownTopicOrPartition, -1, -1);
            if (_faulted) return (Error.KafkaStorageError, -1, -1);
            if (timestamp == -2) return (Error.None, -1, _logStartOffset);
            if (timestamp == -4) return (Error.None, -1, Math.Max(_logStartOffset, _tier?.ArchivedOffset ?? 0));
            if (timestamp == -5)
            {
                long latest = _tier?.LatestArchivedOffset ?? -1;
                return (Error.None, -1, latest >= _logStartOffset ? latest : -1);
            }
            if (timestamp == -1) return (Error.None, -1, _offset);
            if (timestamp < 0) return (Error.InvalidRequest, -1, -1);

            start = _logStartOffset;
            first = start == 0 ? 0 : FindFirstBatch(start);
        }
        while (true)
        {
            Entry entry;
            lock (_index)
            {
                int index = TimestampIndex.FirstAtLeast(CollectionsMarshal.AsSpan(_timestamps)[first..], timestamp);
                if (index < 0) return (Error.None, -1, -1);
                first += index;
                entry = _index[first++];
            }
            var found = ScanTimestamp(entry, start, timestamp);
            if (found.Error != Error.None || found.Offset >= 0) return found;
        }
    }

    private (Error Error, long Timestamp, long Offset) MaximumTimestamp()
    {
        Entry partial = default, maximum = default;
        long start, maximumTime = long.MinValue;
        lock (_index)
        {
            if (Volatile.Read(ref _uses) < 0) return (Error.UnknownTopicOrPartition, -1, -1);
            if (_faulted) return (Error.KafkaStorageError, -1, -1);
            start = _logStartOffset;
            int first = start == 0 ? 0 : FindFirstBatch(start);
            if (first < _index.Count && _index[first].Offset < start) partial = _index[first++];
            for (int i = first; i < _index.Count; i++)
            {
                if (maximum.Size != 0 && _timestamps[i] <= maximumTime) continue;
                maximum = _index[i];
                maximumTime = _timestamps[i];
            }
        }
        var retained = partial.Size == 0 ? (Error: Error.None, Timestamp: -1L, Offset: -1L) :
            ScanTimestamp(partial, start, null, findMaximum: true);
        if (retained.Error != Error.None || maximum.Size == 0 || retained.Offset >= 0 && retained.Timestamp >= maximumTime) return retained;
        return ScanTimestamp(maximum, start, maximumTime);
    }

    private (Error Error, long Timestamp, long Offset) ScanTimestamp(Entry entry, long start, long? timestamp, bool findMaximum = false)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(entry.Size);
        try
        {
            ReadVerified(buffer.AsSpan(0, entry.Size), entry.Position, entry.Offset, entry.EndOffset);
            var record = RecordBatch.FindRecord(buffer.AsMemory(0, entry.Size), entry.Batch, timestamp,
                (int)Math.Max(0, start - entry.Offset), findMaximum);
            return (Error.None, record.Timestamp, record.Delta < 0 ? -1 : entry.Offset + record.Delta);
        }
        catch (IOException)
        {
            return (Error.KafkaStorageError, -1, -1);
        }
        catch (TopicRetiredException)
        {
            return (Error.UnknownTopicOrPartition, -1, -1);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    internal bool ReadVerified(Span<byte> bytes, long position, long requestedOffset, long highWatermark,
        bool zstdAllowed = true)
    {
        if (!TryUse()) throw new TopicRetiredException();
        try
        {
            ReadExactly(bytes, position);
            long next = -1;
            while (!bytes.IsEmpty)
            {
                if (bytes.Length < RecordBatch.HeaderSize) throw new IOException("Incomplete committed batch header");
                int size = RecordBatch.SizeFromPrefix(bytes);
                if (size < 0 || size > bytes.Length) throw new IOException("Invalid committed batch size");
                var batch = bytes[..size];
                long first = BinaryPrimitives.ReadInt64BigEndian(batch);
                int count = BinaryPrimitives.ReadInt32BigEndian(batch[57..]);
                if (first < 0 || count <= 0 || first > highWatermark - count ||
                    (next < 0 ? requestedOffset < first || requestedOffset - first >= count : next != first) ||
                    BinaryPrimitives.ReadInt32BigEndian(batch[12..]) != 0 ||
                    batch[16] != 2 || BinaryPrimitives.ReadUInt32BigEndian(batch[17..]) != RecordBatch.Crc32C(batch[21..]))
                    throw new IOException("Committed batch failed integrity validation");
                if (!zstdAllowed && (batch[22] & 7) == 4) return false;
                next = first + count;
                bytes = bytes[size..];
            }
            return true;
        }
        catch (IOException e)
        {
            lock (_index) _faulted = true;
            ReportFault("partition read fault", e);
            throw;
        }
        finally { Release(); }
    }

    private void ReadExactly(Span<byte> bytes, long position)
    {
        if (_tier is not null) { _tier.ReadExactly(_file, bytes, position); return; }
        while (!bytes.IsEmpty)
        {
            int read = RandomAccess.Read(_file, bytes, position);
            if (read == 0) throw new EndOfStreamException();
            bytes = bytes[read..];
            position += read;
        }
    }

    private void Recover()
    {
        long length = RandomAccess.GetLength(_file);
        Span<byte> prefix = stackalloc byte[12];
        byte[]? scratch = null;
        try
        {
            while (length - _position >= prefix.Length)
            {
                ReadExactly(prefix, _position);
                int size = RecordBatch.SizeFromPrefix(prefix);
                if (size < 0 || BinaryPrimitives.ReadInt64BigEndian(prefix) != _offset)
                    throw new InvalidDataException($"Invalid batch prefix at position {_position}");
                if (size > length - _position) break;
                if (scratch is null || scratch.Length < size)
                {
                    if (scratch is not null) ArrayPool<byte>.Shared.Return(scratch);
                    scratch = ArrayPool<byte>.Shared.Rent(size);
                }
                var bytes = scratch.AsSpan(0, size);
                ReadExactly(bytes, _position);
                if (BinaryPrimitives.ReadInt32BigEndian(bytes[12..]) != 0 ||
                    RecordBatch.Parse(scratch.AsMemory(0, size), out var batch, fromStorage: true) != Error.None)
                    throw new InvalidDataException($"Invalid batch at position {_position}");
                var entry = new Entry(_offset, _position, size, batch);
                Index(entry);
                TrackProducer(entry);
            }
            if (_position != length)
            {
                RandomAccess.SetLength(_file, _position);
                RandomAccess.FlushToDisk(_file);
            }
        }
        finally
        {
            if (scratch is not null) ArrayPool<byte>.Shared.Return(scratch);
        }
    }

    private void Index(Entry entry)
    {
        _index.Add(entry);
        _timestamps.Add(entry.Batch.MaxTimestamp);
        _position += entry.Size;
        _offset = entry.EndOffset;
    }

    private void TrackProducer(Entry entry)
    {
        if (entry.Batch.ProducerId < 0) return;
        if (!_producers.TryGetValue(entry.Batch.ProducerId, out var producer))
        {
            producer = new Producer();
            _producers.Add(entry.Batch.ProducerId, producer);
        }
        producer.Add(entry);
    }

    private bool TryUse()
    {
        int state = Volatile.Read(ref _uses);
        while (state >= 0)
        {
            int observed = Interlocked.CompareExchange(ref _uses, state + 1, state);
            if (observed == state) return true;
            state = observed;
        }
        return false;
    }

    private void Release()
    {
        if (Interlocked.Decrement(ref _uses) != Retired) return;
        _tier?.Dispose();
        _file.Dispose();
        _closed.TrySetResult();
    }

    internal Task Retire()
    {
        if (Interlocked.Or(ref _uses, Retired) >= 0) Release();
        return _closed.Task;
    }

    public void Dispose() => Retire().GetAwaiter().GetResult();
}
