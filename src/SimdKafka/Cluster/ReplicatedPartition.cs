using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading.Channels;
using DotNext.IO;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;
using DotNext.Net.Security;

namespace SimdKafka;

internal sealed partial class ReplicatedPartition : IStateMachine, IAsyncDisposable
{
    private const int IndexBlockBits = 9;
    private const int IndexBlockSize = 1 << IndexBlockBits;
    private readonly object _indexGate = new();
    private IndexedBatch[][] _index = [];
    private int _batchCount;
    private readonly Guid _id;
    private readonly List<long> _timestamps = [];
    private readonly Dictionary<long, ProducerState> _producers = [];
    private readonly Action? _changed;
    private readonly CancellationTokenSource _stop = new();
    private readonly Channel<PendingBatch> _pending = Channel.CreateUnbounded<PendingBatch>(new UnboundedChannelOptions
    {
        SingleReader = true
    });
    private RaftJournal _journal = null!;
    private Task _writer = Task.CompletedTask;
    private long _logStartOffset;
    private long _endOffset;
    private long _encodedLogBytes;
    private long _largestTimestamp = long.MinValue;

    private ReplicatedPartition(Guid id, Action? changed)
    {
        _id = id;
        _changed = changed;
    }

    private const int MaxGroupBytes = 16 * 1024 * 1024;
    // A negative batch length makes the marker unambiguous in the previous WAL format.
    private static readonly byte[] SequenceRulesHeader = [0x53, 0x49, 0x4d, 0x44, 0x4b, 0x50, 0x53, 0x31, 255, 255, 255, 255];
    private static readonly byte[] TransactionGroupHeader = [0x53, 0x49, 0x4d, 0x44, 0x4b, 0x54, 0x47, 0x31, 255, 255, 255, 253];
    private const int TransactionIdBytes = 16;
    private const ulong LogStartCommand = 0x53494d444b4c5331; // SIMDKLS1
    private const int LogStartCommandBytes = 16;

    private readonly record struct IndexedBatch(long Index, long EndOffset, long EncodedEnd, int Position, int Count, int Size, int Epoch,
        bool Control = false)
    {
        internal long BaseOffset => EndOffset - Count;
    }

    private readonly record struct Digest(ulong A, ulong B, ulong C, ulong D)
    {
        internal static Digest Calculate(ReadOnlySpan<byte> bytes)
        {
            Span<byte> hash = stackalloc byte[32];
            SHA256.HashData(bytes[16..], hash);
            return new(BinaryPrimitives.ReadUInt64LittleEndian(hash), BinaryPrimitives.ReadUInt64LittleEndian(hash[8..]),
                BinaryPrimitives.ReadUInt64LittleEndian(hash[16..]), BinaryPrimitives.ReadUInt64LittleEndian(hash[24..]));
        }
    }

    private readonly record struct RecentBatch(int Sequence, int Count, Digest Digest, long Offset, long LogAppendTime);

    private sealed class ProducerState
    {
        internal short Epoch = -1;
        internal int Next;
        internal long LastTimestamp;
        internal int CoordinatorEpoch = -1;
        private readonly RecentBatch[] _recent = new RecentBatch[5];
        private int _count, _slot;

        internal AppendResult? Find(RecordBatch batch, Digest digest, bool kafkaRules)
        {
            for (int i = 0; i < _count; i++)
                if (_recent[i].Sequence == batch.Sequence &&
                    (kafkaRules ? _recent[i].Count == batch.Count : _recent[i].Digest == digest))
                    return new(Error.None, _recent[i].Offset, _recent[i].LogAppendTime);
            return null;
        }

        internal void Add(RecordBatch batch, Digest digest, long offset)
        {
            if (Epoch != batch.Epoch)
            {
                Epoch = batch.Epoch;
                _count = _slot = 0;
            }
            Next = ProducerSequence.Next(batch);
            LastTimestamp = batch.MaxTimestamp;
            _recent[_slot] = new(batch.Sequence, batch.Count, digest, offset, batch.AppendTimestamp);
            _slot = (_slot + 1) % _recent.Length;
            _count = Math.Min(_count + 1, _recent.Length);
        }
    }

    private sealed class PendingBatch(Memory<byte> bytes, RecordBatch batch, CancellationToken deadline, Guid transactionId, bool logAppendTime)
        : TaskCompletionSource<AppendResult>(TaskCreationOptions.RunContinuationsAsynchronously)
    {
        private long _originalTimestamp;
        private uint _originalCrc;
        internal RecordBatch Batch { get; private set; } = batch;
        internal bool LogAppendTime { get; } = logAppendTime;
        internal int Length => bytes.Length;
        internal CancellationToken Deadline { get; } = deadline;
        internal Guid TransactionId { get; } = transactionId;
        internal AppendResult? Result;
        internal void Stamp(long timestamp)
        {
            _originalTimestamp = Batch.MaxTimestamp;
            Batch = Batch.StampAppendTime(bytes.Span, timestamp, out _originalCrc);
        }

        internal void Restore()
        {
            if (Batch.LogAppendTime) RecordBatch.RestoreCreateTime(bytes.Span, _originalTimestamp, _originalCrc);
        }

        internal ValueTask WriteToAsync<TWriter>(TWriter writer, CancellationToken stop) where TWriter : IAsyncBinaryWriter =>
            writer.WriteAsync(bytes, null, stop);
    }

    private sealed class BatchGroup(long term, PendingBatch[] requests, int length, bool transactions) : IRaftLogEntry, IInputLogEntry
    {
        public long Term => term;
        public int? CommandId => null;
        public long? Length => length;
        public bool IsSnapshot => false;
        public bool IsReusable => true;
        public bool IsConfiguration => false;
        public object? Context { get; init; } = requests;

        public async ValueTask WriteToAsync<TWriter>(TWriter writer, CancellationToken token) where TWriter : IAsyncBinaryWriter
        {
            await writer.WriteAsync(transactions ? TransactionGroupHeader : SequenceRulesHeader, null, token);
            byte[]? identity = transactions ? new byte[TransactionIdBytes] : null;
            foreach (var request in requests)
            {
                if (identity is not null)
                {
                    request.TransactionId.TryWriteBytes(identity);
                    await writer.WriteAsync(identity, null, token);
                }
                await request.WriteToAsync(writer, token);
            }
        }
    }

    internal bool IsLeader => _journal.IsLeader;
    internal EndPoint? Leader => _journal.Leader;
    internal int LeaderEpoch => checked((int)_journal.Term);
    internal long HighWatermark => Interlocked.Read(ref _endOffset);
    internal long LogStartOffset => Interlocked.Read(ref _logStartOffset);
    internal long EncodedLogBytes => Interlocked.Read(ref _encodedLogBytes);

    internal (long Id, short Epoch, int LastSequence, long LastTimestamp, int CoordinatorEpoch, long FirstOffset)[] DescribeProducers()
    {
        lock (_indexGate)
            return _producers.Select(producer => (producer.Key, producer.Value.Epoch, (producer.Value.Next - 1) & int.MaxValue,
                producer.Value.LastTimestamp, producer.Value.CoordinatorEpoch,
                _activeProducers.TryGetValue(producer.Key, out var id) ? _openTransactions[id].FirstOffset : -1)).ToArray();
    }
    internal ValueTask ReadBarrierAsync(CancellationToken stop) => _journal.ReadBarrierAsync(stop);
    internal ValueTask<IReadOnlySet<EndPoint>> VotersAsync(CancellationToken stop) => _journal.VotersAsync(stop);
    internal Task<bool> AddVoterAsync(IPEndPoint address, CancellationToken stop) => _journal.AddVoterAsync(address, stop);
    internal Task<bool> RemoveVoterAsync(IPEndPoint address, CancellationToken stop) => _journal.RemoveVoterAsync(address, stop);

    internal readonly record struct Selection(Error Error, long HighWatermark, int Size, int First, int Last,
        long FirstIndex, long LastIndex, long LogStartOffset = -1, long LastStableOffset = -1, AbortedTransaction[]? Aborted = null);

    internal async ValueTask<(Error Error, long Offset)> AdvanceLogStartAsync(long requested, CancellationToken stop)
    {
        if (!IsLeader) return (Error.NotLeaderOrFollower, -1);
        long end;
        lock (_indexGate) end = LastStableOffset;
        long offset = requested == -1 ? end : requested;
        if (offset < 0 || offset > end) return (Error.OffsetOutOfRange, -1);
        if (offset > LogStartOffset)
        {
            var command = new byte[LogStartCommandBytes];
            BinaryPrimitives.WriteUInt64BigEndian(command, LogStartCommand);
            BinaryPrimitives.WriteInt64BigEndian(command.AsSpan(8), offset);
            await _journal.ReplicateAsync(command, null, stop);
        }
        return (Error.None, LogStartOffset);
    }

    internal static async Task<ReplicatedPartition> OpenAsync(string path, Guid groupId, IPEndPoint local,
        IReadOnlyCollection<IPEndPoint> voters, CancellationToken stop, SslOptions? tls = null, Action? changed = null,
        bool joinExisting = false, bool? preferredLeader = null, bool reuseSlots = false)
    {
        var partition = new ReplicatedPartition(groupId, changed);
        try
        {
            // Small Raft blocks add request/response exchanges within a single Kafka batch.
            partition._journal = await RaftJournal.OpenAsync(path, groupId, local, voters, partition, stop, tls,
                joinExisting, transmissionBlockSize: 64 * 1024, chunkSize: 32 * 1024 * 1024,
                preferredLeader: preferredLeader, reuseSlots: reuseSlots);
            partition._writer = partition.WriteGroupsAsync(partition._stop.Token);
            return partition;
        }
        catch { partition._stop.Dispose(); throw; }
    }

    internal ValueTask<AppendResult> AppendAsync(Memory<byte> bytes, long allocatedProducerId,
        CancellationToken stop, Guid transactionId = default, bool logAppendTime = false, TimestampBounds? timestampBounds = null)
    {
        if (!IsLeader) return ValueTask.FromResult(new AppendResult(Error.NotLeaderOrFollower, -1));
        if (bytes.Length > BrokerOptions.MaxBatchBytes) return ValueTask.FromResult(new AppendResult(Error.MessageTooLarge, -1));
        var error = RecordBatch.Parse(bytes, out var batch, transactionId != Guid.Empty, timestampBounds: timestampBounds);
        if (error != Error.None) return ValueTask.FromResult(new AppendResult(error, -1));
        if (batch.Transactional != (transactionId != Guid.Empty))
            return ValueTask.FromResult(new AppendResult(Error.InvalidTxnState, -1));
        if (batch.ProducerId == 0 || batch.ProducerId > allocatedProducerId)
            return ValueTask.FromResult(new AppendResult(Error.UnknownProducerId, -1));
        if (stop.IsCancellationRequested) return ValueTask.FromResult(new AppendResult(Error.RequestTimedOut, -1));

        var append = new PendingBatch(bytes, batch, stop, transactionId, logAppendTime);
        return _pending.Writer.TryWrite(append) ? new ValueTask<AppendResult>(append.Task) :
            ValueTask.FromResult(new AppendResult(Error.NotLeaderOrFollower, -1));
    }

    private async Task WriteGroupsAsync(CancellationToken stop)
    {
        var reader = _pending.Reader;
        var group = new List<PendingBatch>(PartitionLog.MaxFlushBatches);
        try
        {
            while (await reader.WaitToReadAsync(stop))
            {
                int size = SequenceRulesHeader.Length;
                bool transactions = false;
                while (group.Count < PartitionLog.MaxFlushBatches && reader.TryPeek(out var next) &&
                    next.Length + TransactionIdBytes <= MaxGroupBytes - size)
                {
                    if (!reader.TryRead(out next)) break;
                    if (next.Deadline.IsCancellationRequested)
                        next.SetResult(new(Error.RequestTimedOut, -1));
                    else
                    {
                        group.Add(next);
                        size += next.Length + TransactionIdBytes;
                        transactions |= next.TransactionId != Guid.Empty;
                    }
                }
                if (group.Count == 0) continue;
                Error error = Error.KafkaStorageError;
                using var replication = CancellationTokenSource.CreateLinkedTokenSource(stop);
                replication.CancelAfter(TimeSpan.FromSeconds(30));
                long started = Stopwatch.GetTimestamp();
                long term = _journal.Term;
                try
                {
                    long timestamp = -1;
                    foreach (var append in group)
                    {
                        if (!append.LogAppendTime) continue;
                        if (timestamp < 0) timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        append.Stamp(timestamp);
                    }
                    if (!transactions) size -= group.Count * TransactionIdBytes;
                    await _journal.ReplicateAsync(new BatchGroup(term, group.ToArray(), size, transactions), replication.Token);
                    error = Error.None;
                }
                catch (NotLeaderException)
                {
                    error = Error.NotLeaderOrFollower;
                    Console.Error.WriteLine($"SimdKafka replica {_id:N} lost term {term} during {group.Count} batches after " +
                        $"{Stopwatch.GetElapsedTime(started).TotalMilliseconds:F1} ms");
                }
                catch (OperationCanceledException) { error = Error.RequestTimedOut; }
                catch (QuorumUnreachableException) { error = Error.RequestTimedOut; }
                catch (IOException) { error = Error.KafkaStorageError; }
                finally
                {
                    // The networking buffer stays leased until the WAL has stopped reading it.
                    foreach (var append in group)
                    {
                        append.Restore();
                        append.SetResult(error != Error.None ? new(error, -1) : append.Result ?? new(Error.KafkaStorageError, -1));
                    }
                    group.Clear();
                }
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        finally
        {
            _pending.Writer.TryComplete();
            while (reader.TryRead(out var request)) request.SetResult(new(Error.NotLeaderOrFollower, -1));
        }
    }

    internal async ValueTask<(Error Error, long HighWatermark, int Bytes)> ReadAsync(long offset,
        Memory<byte> destination, CancellationToken stop)
    {
        await _journal.ReadBarrierAsync(stop);
        var selection = Select(offset, destination.Length, allowOversize: false);
        await CopyAsync(selection, destination, stop);
        return (selection.Error, selection.HighWatermark, selection.Size);
    }

    internal Selection Select(long offset, int maxBytes, bool allowOversize, bool readCommitted = false, bool reportAborted = true)
    {
        int first, last;
        long highWatermark, firstIndex, lastIndex;
        lock (_indexGate)
        {
            highWatermark = _endOffset;
            long stable = LastStableOffset;
            long end = readCommitted ? stable : highWatermark;
            long start = _logStartOffset;
            if (offset < start || offset > highWatermark) return new(Error.OffsetOutOfRange, highWatermark, 0, 0, 0, 0, 0, start, stable);
            if (offset >= end) return new(Error.None, highWatermark, 0, 0, 0, 0, 0, start, stable);
            first = FindFirstBatch(offset);
            var firstBatch = BatchAt(first);
            int budget = allowOversize ? Math.Max(maxBytes, firstBatch.Size) : maxBytes;
            if (budget < firstBatch.Size) return new(Error.None, highWatermark, 0, 0, 0, 0, 0, start, stable);
            long firstByte = firstBatch.EncodedEnd - firstBatch.Size;
            int lo = first + 1, hi = end == highWatermark ? _batchCount : FindFirstBatch(end);
            while (lo < hi)
            {
                int mid = lo + (hi - lo) / 2;
                if (BatchAt(mid).EncodedEnd - firstByte <= budget) lo = mid + 1;
                else hi = mid;
            }
            last = lo;
            var lastBatch = BatchAt(last - 1);
            firstIndex = firstBatch.Index;
            lastIndex = lastBatch.Index;
            var aborted = readCommitted && reportAborted ? AbortedTransactions(firstBatch.BaseOffset, lastBatch.EndOffset) : null;
            return new(Error.None, highWatermark, (int)(lastBatch.EncodedEnd - firstByte), first, last, firstIndex, lastIndex, start, stable, aborted);
        }
    }

    private int FindFirstBatch(long offset)
    {
        int lo = 0, hi = _batchCount;
        while (lo < hi)
        {
            int mid = lo + (hi - lo) / 2;
            if (BatchAt(mid).EndOffset <= offset) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private IndexedBatch BatchAt(int index) => _index[index >> IndexBlockBits][index & (IndexBlockSize - 1)];

    internal async ValueTask<bool> CopyAsync(Selection selection, Memory<byte> destination, CancellationToken stop,
        bool zstdAllowed = true)
    {
        if (selection.Size == 0) return true;
        // The captured directory keeps all published blocks alive while this reader awaits WAL I/O.
        var index = Volatile.Read(ref _index);
        using var entries = await _journal.ReadAsync(selection.FirstIndex, selection.LastIndex, stop);
        int written = 0;
        for (int i = selection.First; i < selection.Last; i++)
        {
            var batch = index[i >> IndexBlockBits][i & (IndexBlockSize - 1)];
            var entry = entries[checked((int)(batch.Index - selection.FirstIndex))];
            if (entry.Index != batch.Index || !entry.TryGetPayload(out var data) || data.Length < batch.Position + batch.Size)
                throw new InvalidDataException("Raft partition index does not match its journal entry");
            var output = destination.Span.Slice(written, batch.Size);
            data.Slice(batch.Position, batch.Size).CopyTo(output);
            if (BinaryPrimitives.ReadUInt32BigEndian(output[17..]) != RecordBatch.Crc32C(output[21..]))
                throw new InvalidDataException("Replicated batch failed CRC32C verification");
            if (!zstdAllowed && (output[22] & 7) == 4) return false;
            BinaryPrimitives.WriteInt64BigEndian(output, batch.BaseOffset);
            BinaryPrimitives.WriteInt32BigEndian(output[12..], batch.Epoch);
            written += batch.Size;
        }
        return true;
    }

    internal async ValueTask<(Error Error, long Timestamp, long Offset)> OffsetForAsync(long timestamp, CancellationToken stop,
        bool readCommitted = false)
    {
        int first, last;
        long start;
        bool findMaximum = timestamp == -3;
        lock (_indexGate)
        {
            if (timestamp is -2 or -4) return (Error.None, -1, _logStartOffset);
            long end = readCommitted ? LastStableOffset : _endOffset;
            if (timestamp == -1) return (Error.None, -1, end);
            start = _logStartOffset;
            first = FindFirstBatch(start);
            last = end == _endOffset ? _batchCount : FindFirstBatch(end);
            if (findMaximum && start == 0 && end == _endOffset)
            {
                timestamp = _largestTimestamp;
                findMaximum = false;
            }
        }
        if (findMaximum) return await MaximumTimestampAsync(first, last, start, stop);
        while (first < last)
        {
            IndexedBatch entry;
            lock (_indexGate)
            {
                int found = TimestampIndex.FirstAtLeast(CollectionsMarshal.AsSpan(_timestamps)[first..last], timestamp);
                if (found < 0) break;
                first += found;
                entry = BatchAt(first++);
                if (entry.Control) continue;
            }
            var foundRecord = await ReadTimestampAsync(entry, timestamp, start, findMaximum: false, stop);
            if (foundRecord.Error != Error.None || foundRecord.Offset >= 0) return foundRecord;
        }
        return (Error.None, -1, -1);
    }

    private async ValueTask<(Error Error, long Timestamp, long Offset)> MaximumTimestampAsync(
        int first, int last, long start, CancellationToken stop)
    {
        IndexedBatch partial = default, maximum = default;
        long maxTime = long.MinValue;
        lock (_indexGate)
        {
            if (first < last && BatchAt(first).BaseOffset < start) partial = BatchAt(first++);
            for (int i = first; i < last; i++)
            {
                if (BatchAt(i).Control) continue;
                if (maximum.Count != 0 && _timestamps[i] <= maxTime) continue;
                maxTime = _timestamps[i];
                maximum = BatchAt(i);
            }
        }
        var earliest = partial.Count == 0 || partial.Control ? (Error.None, -1L, -1L) :
            await ReadTimestampAsync(partial, null, start, findMaximum: true, stop);
        if (earliest.Item1 != Error.None || maximum.Count == 0 ||
            earliest.Item3 >= 0 && earliest.Item2 >= maxTime) return earliest;
        return await ReadTimestampAsync(maximum, maxTime, start, findMaximum: false, stop);
    }

    private async ValueTask<(Error Error, long Timestamp, long Offset)> ReadTimestampAsync(
        IndexedBatch entry, long? timestamp, long start, bool findMaximum, CancellationToken stop)
    {
        using var entries = await _journal.ReadAsync(entry.Index, entry.Index, stop);
        if (!entries[0].TryGetPayload(out var payload)) return (Error.KafkaStorageError, -1, -1);
        payload = payload.Slice(entry.Position, entry.Size);
        var bytes = payload.IsSingleSegment ? payload.First : payload.ToArray();
        if (RecordBatch.Parse(bytes, out var batch, allowTransactional: true, fromStorage: true) != Error.None) return (Error.KafkaStorageError, -1, -1);
        var (delta, time) = RecordBatch.FindRecord(bytes, batch, timestamp,
            (int)Math.Max(0, start - entry.BaseOffset), findMaximum);
        return (Error.None, time, delta < 0 ? -1 : entry.BaseOffset + delta);
    }

    internal (int Epoch, long Offset) EndOffsetForEpoch(int requested, int current)
    {
        lock (_indexGate)
        {
            if (requested < 0 || requested > current) return (-1, -1);
            if (requested == current) return (current, _endOffset);
            int first = 0, last = _batchCount;
            while (first < last)
            {
                int middle = first + (last - first) / 2;
                if (BatchAt(middle).Epoch <= requested) first = middle + 1;
                else last = middle;
            }
            if (first == 0) return (requested, _logStartOffset);
            var found = BatchAt(first - 1);
            return found.EndOffset < _logStartOffset ? (requested, _logStartOffset) : (found.Epoch, found.EndOffset);
        }
    }

    int IStateMachine.Version => 5;
    ISnapshot? ISnapshotManager.Snapshot => null;
    ValueTask ISnapshotManager.ReclaimGarbageAsync(long watermark, CancellationToken token) => ValueTask.CompletedTask;

    ValueTask<long> IStateMachine.ApplyAsync(LogEntry entry, CancellationToken token)
    {
        if (entry.Length == 0) return ValueTask.FromResult(entry.Index);
        if (entry.IsConfiguration) return ValueTask.FromResult(entry.Index);
        if (entry.Length == LogStartCommandBytes)
        {
            Span<byte> command = stackalloc byte[LogStartCommandBytes];
            if (!entry.TryGetPayload(out var trim)) throw new InvalidDataException("Missing replicated log-start command");
            trim.CopyTo(command);
            long offset = BinaryPrimitives.ReadInt64BigEndian(command[8..]);
            lock (_indexGate)
            {
                if (BinaryPrimitives.ReadUInt64BigEndian(command) != LogStartCommand || offset < 0 || offset > _endOffset)
                    throw new InvalidDataException("Invalid replicated log-start command");
                if (offset > _logStartOffset) Interlocked.Exchange(ref _logStartOffset, Math.Min(offset, LastStableOffset));
            }
            _changed?.Invoke();
            return ValueTask.FromResult(entry.Index);
        }
        if (!entry.TryGetPayload(out var payload) || payload.Length is < 12 or > MaxGroupBytes)
            throw new InvalidDataException("Invalid replicated record batch length");

        var contexts = entry.Context as PendingBatch[];
        int position = 0, processed = 0;
        Span<byte> prefix = stackalloc byte[12];
        payload.Slice(0, prefix.Length).CopyTo(prefix);
        if (prefix.SequenceEqual(TransactionEntryHeader))
        {
            ApplyTransactionEntry(entry, payload);
            _changed?.Invoke();
            return ValueTask.FromResult(entry.Index);
        }
        if (payload.Length < RecordBatch.HeaderSize) throw new InvalidDataException("Truncated replicated record batch");
        bool transactions = prefix.SequenceEqual(TransactionGroupHeader);
        bool kafkaRules = transactions || prefix.SequenceEqual(SequenceRulesHeader);
        if (kafkaRules)
        {
            position = prefix.Length;
            payload = payload.Slice(position);
            if (payload.Length < RecordBatch.HeaderSize) throw new InvalidDataException("Empty versioned batch group");
        }
        Span<byte> identity = stackalloc byte[TransactionIdBytes];
        while (payload.Length >= prefix.Length)
        {
            Guid transactionId = default;
            if (transactions)
            {
                if (payload.Length < TransactionIdBytes + RecordBatch.HeaderSize)
                    throw new InvalidDataException("Truncated transactional batch group");
                payload.Slice(0, TransactionIdBytes).CopyTo(identity);
                transactionId = new Guid(identity);
                payload = payload.Slice(TransactionIdBytes);
                position += TransactionIdBytes;
            }
            payload.Slice(0, prefix.Length).CopyTo(prefix);
            int size = RecordBatch.SizeFromPrefix(prefix);
            if (size <= 0 || size > payload.Length) throw new InvalidDataException("Invalid batch bounds in replicated log");
            var context = contexts?[processed++];
            ApplyBatch(entry, payload.Slice(0, size), position, context, kafkaRules, transactionId);
            payload = payload.Slice(size);
            position += size;
        }
        if (!payload.IsEmpty || contexts is not null && contexts.Length != processed)
            throw new InvalidDataException("Invalid replicated batch group");
        _changed?.Invoke();
        return ValueTask.FromResult(entry.Index);
    }

    private void ApplyBatch(LogEntry entry, ReadOnlySequence<byte> payload, int position, PendingBatch? context, bool kafkaRules,
        Guid transactionId = default)
    {
        byte[]? rented = null;
        try
        {
            var bytes = payload.First;
            if (!payload.IsSingleSegment)
            {
                rented = ArrayPool<byte>.Shared.Rent((int)payload.Length);
                bytes = rented.AsMemory(0, (int)payload.Length);
                payload.CopyTo(rented.AsSpan(0, bytes.Length));
            }
            RecordBatch batch;
            if (context is not null) batch = context.Batch;
            else if (RecordBatch.Parse(bytes, out batch, transactionId != Guid.Empty, fromStorage: true) != Error.None)
                throw new InvalidDataException("Invalid replicated record batch");
            if (batch.Transactional != (transactionId != Guid.Empty))
                throw new InvalidDataException("Replicated batch was stored in the wrong entry type");

            var result = Apply(entry, position, batch, bytes.Span, kafkaRules, transactionId);
            if (context is not null) context.Result = result;
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private AppendResult Apply(LogEntry entry, int position, RecordBatch batch, ReadOnlySpan<byte> bytes, bool kafkaRules,
        Guid transactionId)
    {
        var digest = batch.ProducerId >= 0 && !kafkaRules ? Digest.Calculate(bytes) : default;
        lock (_indexGate)
        {
            long offset = _endOffset;
            if (batch.Transactional && _sealedTransactions.Contains(transactionId)) return new(Error.InvalidProducerEpoch, -1);
            if (batch.Transactional && _finishedTransactions.Contains(transactionId)) return new(Error.InvalidTxnState, -1);
            if (_activeProducers.TryGetValue(batch.ProducerId, out var active) && active != transactionId)
                return new(Error.InvalidTxnState, -1);
            if (batch.ProducerId >= 0)
            {
                _producers.TryGetValue(batch.ProducerId, out var producer);
                if (producer is not null && (!kafkaRules || batch.Epoch == producer.Epoch) && batch.Sequence != producer.Next)
                {
                    if (producer.Find(batch, digest, kafkaRules) is { } previous) return previous;
                }
                var error = kafkaRules ? ProducerSequence.Validate(batch, producer?.Epoch ?? -1, producer?.Next ?? 0, offset == 0) :
                    batch.Sequence == (producer?.Next ?? 0) ? Error.None : Error.OutOfOrderSequence;
                if (error != Error.None) return new(error, -1);
                if (producer is null) _producers.Add(batch.ProducerId, producer = new());
                producer.Add(batch, digest, offset);
            }
            if (batch.Transactional && _openTransactions.TryAdd(transactionId, new(batch.ProducerId, batch.Epoch, offset)))
            {
                _unstableOffsets.Add(offset);
                _activeProducers.Add(batch.ProducerId, transactionId);
            }
            IndexBatch(entry, position, batch.Count, batch.Size, batch.MaxTimestamp);
            return new(Error.None, offset, batch.AppendTimestamp);
        }
    }

    private void IndexBatch(LogEntry entry, int position, int count, int size, long timestamp, bool control = false)
    {
        long end = checked(_endOffset + count);
        long encodedEnd = checked(_encodedLogBytes + size);
        AddBatch(new(entry.Index, end, encodedEnd, position, count, size, checked((int)entry.Term), control));
        _timestamps.Add(timestamp);
        _largestTimestamp = Math.Max(_largestTimestamp, timestamp);
        Interlocked.Exchange(ref _encodedLogBytes, encodedEnd);
        Interlocked.Exchange(ref _endOffset, end);
    }

    private void AddBatch(IndexedBatch batch)
    {
        int block = _batchCount >> IndexBlockBits;
        int position = _batchCount & (IndexBlockSize - 1);
        if (block >= _index.Length)
        {
            var grown = new IndexedBatch[Math.Max(1, _index.Length * 2)][];
            _index.CopyTo(grown, 0);
            Volatile.Write(ref _index, grown);
        }
        if (position == 0) _index[block] = new IndexedBatch[IndexBlockSize];
        _index[block][position] = batch;
        _batchCount++;
    }

    public async ValueTask DisposeAsync()
    {
        _pending.Writer.TryComplete();
        await _stop.CancelAsync();
        try { await _writer; }
        finally
        {
            try { await _journal.DisposeAsync(); }
            finally { _stop.Dispose(); }
        }
    }
}
