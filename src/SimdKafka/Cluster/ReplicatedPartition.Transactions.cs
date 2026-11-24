using System.Buffers;
using System.Buffers.Binary;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;

namespace SimdKafka;

internal sealed partial class ReplicatedPartition
{
    private static readonly byte[] TransactionEntryHeader = [0x53, 0x49, 0x4d, 0x44, 0x4b, 0x54, 0x58, 0x31, 255, 255, 255, 254];
    private const int TransactionPrefixBytes = 29;
    private const int MarkerBytes = RecordBatch.HeaderSize + 17;
    private const byte TransactionDataEntry = 1, TransactionMarkerEntry = 2, TransactionSealEntry = 3;
    private readonly Dictionary<Guid, OpenTransaction> _openTransactions = [];
    private readonly Dictionary<long, Guid> _activeProducers = [];
    private readonly HashSet<Guid> _finishedTransactions = [];
    private readonly HashSet<Guid> _sealedTransactions = [];
    private readonly SortedSet<long> _unstableOffsets = [];
    private readonly List<AbortedRange> _abortedTransactions = [];
    private long LastStableOffset => _unstableOffsets.Count == 0 ? _endOffset : _unstableOffsets.Min;

    internal bool HasTransactionMarker(Guid transactionId)
    {
        lock (_indexGate) return _finishedTransactions.Contains(transactionId);
    }

    internal readonly record struct AbortedTransaction(long ProducerId, long FirstOffset);
    private readonly record struct OpenTransaction(long ProducerId, short Epoch, long FirstOffset);
    private readonly record struct AbortedRange(AbortedTransaction Transaction, long Marker, long StableEnd);

    internal async ValueTask<Error> WriteTransactionMarkerAsync(TransactionMarker marker, CancellationToken stop)
    {
        if (!IsLeader) return Error.NotLeaderOrFollower;
        lock (_indexGate)
            if (_finishedTransactions.Contains(marker.TransactionId) || marker.Seal && _sealedTransactions.Contains(marker.TransactionId))
                return Error.None;
        byte[] command = new byte[TransactionPrefixBytes + (marker.Seal ? 0 : MarkerBytes)];
        WritePrefix(command, marker.Seal ? TransactionSealEntry : TransactionMarkerEntry, marker.TransactionId);
        if (!marker.Seal) WriteControlBatch(command.AsSpan(TransactionPrefixBytes), marker);
        try
        {
            await _journal.ReplicateAsync(command, null, stop);
            return Error.None;
        }
        catch (NotLeaderException) { return Error.NotLeaderOrFollower; }
        catch (QuorumUnreachableException) { return Error.RequestTimedOut; }
        catch (IOException) { return Error.KafkaStorageError; }
    }

    private static void WritePrefix(Span<byte> buffer, byte type, Guid id)
    {
        TransactionEntryHeader.CopyTo(buffer);
        buffer[12] = type;
        id.TryWriteBytes(buffer[13..]);
    }

    private static void WriteControlBatch(Span<byte> batch, TransactionMarker marker)
    {
        BinaryPrimitives.WriteInt32BigEndian(batch[8..], batch.Length - 12);
        batch[16] = 2;
        BinaryPrimitives.WriteInt16BigEndian(batch[21..], 0x30);
        BinaryPrimitives.WriteInt64BigEndian(batch[27..], marker.Timestamp);
        BinaryPrimitives.WriteInt64BigEndian(batch[35..], marker.Timestamp);
        BinaryPrimitives.WriteInt64BigEndian(batch[43..], marker.ProducerId);
        BinaryPrimitives.WriteInt16BigEndian(batch[51..], marker.Epoch);
        BinaryPrimitives.WriteInt32BigEndian(batch[53..], -1);
        BinaryPrimitives.WriteInt32BigEndian(batch[57..], 1);
        var record = batch[RecordBatch.HeaderSize..];
        record[0] = 32; // Zigzag-encoded 16-byte body.
        record[4] = 8; // Four-byte versioned control key.
        record[8] = marker.Commit ? (byte)1 : (byte)0;
        record[9] = 12; // Six-byte versioned marker body.
        BinaryPrimitives.WriteInt32BigEndian(record[12..], marker.CoordinatorEpoch);
        BinaryPrimitives.WriteUInt32BigEndian(batch[17..], RecordBatch.Crc32C(batch[21..]));
    }

    private void ApplyTransactionEntry(LogEntry entry, ReadOnlySequence<byte> payload)
    {
        if (payload.Length < TransactionPrefixBytes)
            throw new InvalidDataException("Truncated transaction record");
        Span<byte> header = stackalloc byte[TransactionPrefixBytes];
        payload.Slice(0, header.Length).CopyTo(header);
        var id = new Guid(header[13..]);
        if (id == Guid.Empty) throw new InvalidDataException("Missing replicated transaction identity");
        var data = payload.Slice(header.Length);
        if (header[12] == TransactionSealEntry && data.IsEmpty)
        {
            lock (_indexGate)
                if (!_finishedTransactions.Contains(id)) _sealedTransactions.Add(id);
            return;
        }
        if (header[12] == TransactionDataEntry)
        {
            if (data.Length < RecordBatch.HeaderSize) throw new InvalidDataException("Truncated transactional record batch");
            ApplyBatch(entry, data, header.Length, entry.Context as PendingBatch, kafkaRules: true, id);
            return;
        }
        if (header[12] != TransactionMarkerEntry || data.Length != MarkerBytes)
            throw new InvalidDataException("Invalid replicated transaction marker");
        Span<byte> bytes = stackalloc byte[MarkerBytes];
        data.CopyTo(bytes);
        if (RecordBatch.SizeFromPrefix(bytes) != bytes.Length || bytes[16] != 2 ||
            BinaryPrimitives.ReadUInt32BigEndian(bytes[17..]) != RecordBatch.Crc32C(bytes[21..]) ||
            BinaryPrimitives.ReadInt16BigEndian(bytes[21..]) != 0x30 || bytes[RecordBatch.HeaderSize + 8] > 1)
            throw new InvalidDataException("Corrupt replicated transaction marker");
        long producerId = BinaryPrimitives.ReadInt64BigEndian(bytes[43..]);
        short epoch = BinaryPrimitives.ReadInt16BigEndian(bytes[51..]);
        bool commit = bytes[RecordBatch.HeaderSize + 8] == 1;
        lock (_indexGate)
        {
            if (!_finishedTransactions.Add(id)) return;
            _sealedTransactions.Remove(id);
            long markerOffset = _endOffset;
            IndexBatch(entry, header.Length, 1, bytes.Length, long.MinValue, control: true);
            if (!_openTransactions.Remove(id, out var open)) return;
            if (open.ProducerId != producerId || open.Epoch != epoch)
                throw new InvalidDataException("Transaction marker does not match the producer");
            _unstableOffsets.Remove(open.FirstOffset);
            _activeProducers.Remove(producerId);
            if (_producers.TryGetValue(producerId, out var producer))
                producer.CoordinatorEpoch = BinaryPrimitives.ReadInt32BigEndian(bytes[(RecordBatch.HeaderSize + 12)..]);
            if (!commit) _abortedTransactions.Add(new(new(producerId, open.FirstOffset), markerOffset, LastStableOffset));
        }
    }

    private AbortedTransaction[]? AbortedTransactions(long first, long end)
    {
        int lo = 0, hi = _abortedTransactions.Count;
        while (lo < hi)
        {
            int middle = lo + (hi - lo) / 2;
            if (_abortedTransactions[middle].Marker < first) lo = middle + 1;
            else hi = middle;
        }
        List<AbortedTransaction>? result = null;
        for (int i = lo; i < _abortedTransactions.Count; i++)
        {
            var range = _abortedTransactions[i];
            if (range.Transaction.FirstOffset < end) (result ??= []).Add(range.Transaction);
            if (range.StableEnd >= end) break;
        }
        result?.Sort(static (a, b) => a.FirstOffset.CompareTo(b.FirstOffset));
        return result?.ToArray();
    }
}
