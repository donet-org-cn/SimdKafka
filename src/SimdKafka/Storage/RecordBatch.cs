using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace SimdKafka;

internal readonly record struct RecordBatch(int Size, int Count, long FirstTimestamp, long MaxTimestamp,
    long ProducerId, short Epoch, int Sequence, int Codec, bool Transactional = false, bool LogAppendTime = false)
{
    internal const int HeaderSize = 61;
    private const byte AppendTimeAttribute = 8;
    private sealed class InvalidRecordTimestampException : Exception;
    internal long AppendTimestamp => LogAppendTime ? MaxTimestamp : -1;

    internal static int SizeFromPrefix(ReadOnlySpan<byte> bytes)
    {
        int length = BinaryPrimitives.ReadInt32BigEndian(bytes[8..]);
        return length is >= HeaderSize - 12 and <= BrokerOptions.MaxBatchBytes - 12 ? length + 12 : -1;
    }

    internal static Error Parse(ReadOnlyMemory<byte> encoded, out RecordBatch batch, bool allowTransactional = false,
        bool fromStorage = false, TimestampBounds? timestampBounds = null)
    {
        var bytes = encoded.Span;
        batch = default;
        if (bytes.Length < HeaderSize || SizeFromPrefix(bytes) != bytes.Length || bytes[16] != 2)
            return Error.CorruptMessage;
        if (BinaryPrimitives.ReadUInt32BigEndian(bytes[17..]) != Crc32C(bytes[21..]))
            return Error.CorruptMessage;

        short attributes = BinaryPrimitives.ReadInt16BigEndian(bytes[21..]);
        int codec = attributes & 7;
        if (codec > 4) return Error.UnsupportedCompressionType;
        bool transactional = (attributes & 0x10) != 0;
        bool appendTime = (attributes & AppendTimeAttribute) != 0;
        if (appendTime && !fromStorage) return Error.InvalidTimestamp;
        int allowed = 7 | AppendTimeAttribute | (allowTransactional ? 0x10 : 0);
        if ((attributes & ~allowed) != 0) return Error.InvalidTxnState;

        int delta = BinaryPrimitives.ReadInt32BigEndian(bytes[23..]);
        long first = BinaryPrimitives.ReadInt64BigEndian(bytes[27..]);
        long max = BinaryPrimitives.ReadInt64BigEndian(bytes[35..]);
        long producer = BinaryPrimitives.ReadInt64BigEndian(bytes[43..]);
        short epoch = BinaryPrimitives.ReadInt16BigEndian(bytes[51..]);
        int sequence = BinaryPrimitives.ReadInt32BigEndian(bytes[53..]);
        int count = BinaryPrimitives.ReadInt32BigEndian(bytes[57..]);
        if (count <= 0 || delta != count - 1 || count > InflatedRecords.MaxSize / 7 || appendTime && max < 0 ||
            producer < -1 || (producer == -1 && (epoch != -1 || sequence != -1 || transactional)) ||
            (producer >= 0 && (epoch < 0 || sequence < 0)))
            return Error.CorruptMessage;

        batch = new(bytes.Length, count, first, max, producer, epoch, sequence, codec, transactional, appendTime);
        try
        {
            FindRecord(encoded, batch, null, earliest: timestampBounds?.Earliest ?? long.MinValue);
            return timestampBounds is { } bounds && max > bounds.Latest ? Error.InvalidTimestamp : Error.None;
        }
        catch (InvalidRecordTimestampException)
        {
            return Error.InvalidTimestamp;
        }
        catch (ExpansionLimitException)
        {
            return Error.MessageTooLarge;
        }
        catch (Exception e) when (e is WireException or OverflowException)
        {
            return Error.CorruptMessage;
        }
    }

    internal RecordBatch StampAppendTime(Span<byte> bytes, long timestamp, out uint originalCrc)
    {
        originalCrc = BinaryPrimitives.ReadUInt32BigEndian(bytes[17..]);
        bytes[22] |= AppendTimeAttribute;
        BinaryPrimitives.WriteInt64BigEndian(bytes[35..], timestamp);
        BinaryPrimitives.WriteUInt32BigEndian(bytes[17..], Crc32C(bytes[21..]));
        return this with { MaxTimestamp = timestamp, LogAppendTime = true };
    }

    internal static void RestoreCreateTime(Span<byte> bytes, long timestamp, uint crc)
    {
        bytes[22] &= unchecked((byte)~AppendTimeAttribute);
        BinaryPrimitives.WriteInt64BigEndian(bytes[35..], timestamp);
        BinaryPrimitives.WriteUInt32BigEndian(bytes[17..], crc);
    }

    internal static (int Delta, long Timestamp) FindRecord(ReadOnlyMemory<byte> encoded, RecordBatch batch, long? target,
        int firstDelta = 0, bool findMaximum = false, long earliest = long.MinValue) =>
        batch.Codec == 0 ? ScanRecords(encoded.Span[HeaderSize..], batch, target, firstDelta, findMaximum, earliest) :
        RecordCompression.Scan(encoded[HeaderSize..], batch, target, firstDelta, findMaximum, earliest);

    internal static (int Delta, long Timestamp) ScanRecords(ReadOnlySpan<byte> bytes, RecordBatch batch, long? target,
        int firstDelta = 0, bool findMaximum = false, long earliest = long.MinValue)
    {
        if (batch.Count > bytes.Length / 7) throw new WireException("Invalid record count");
        var records = new WireReader(bytes);
        long max = long.MinValue;
        long retainedMax = long.MinValue;
        int retainedMaxDelta = -1;
        for (int i = 0; i < batch.Count; i++)
        {
            var record = new WireReader(records.Take(SignedInt(ref records)));
            record.I8();
            long timestampDelta = SignedLong(ref record);
            long timestamp = batch.LogAppendTime ? batch.MaxTimestamp : checked(batch.FirstTimestamp + timestampDelta);
            if (timestamp < earliest && timestamp != -1) throw new InvalidRecordTimestampException();
            if (SignedInt(ref record) != i) throw new WireException("Invalid record offset delta");
            if (i >= firstDelta)
            {
                if (target is long time && timestamp >= time) return (i, timestamp);
                if (findMaximum && (retainedMaxDelta < 0 || timestamp > retainedMax))
                {
                    retainedMax = timestamp;
                    retainedMaxDelta = i;
                }
            }
            SkipRecordBytes(ref record, nullable: true);
            SkipRecordBytes(ref record, nullable: true);
            int headers = SignedInt(ref record);
            if (headers < 0 || headers > record.Remaining / 2) throw new WireException("Invalid record headers");
            for (int h = 0; h < headers; h++)
            {
                SkipRecordBytes(ref record, nullable: false);
                SkipRecordBytes(ref record, nullable: true);
            }
            record.End();
            max = Math.Max(max, timestamp);
        }
        records.End();
        if (max != batch.MaxTimestamp) throw new WireException("Invalid batch max timestamp");
        return retainedMaxDelta < 0 ? (-1, -1) : (retainedMaxDelta, retainedMax);
    }

    private static void SkipRecordBytes(ref WireReader r, bool nullable)
    {
        int length = SignedInt(ref r);
        if (!nullable || length != -1) r.Take(length);
    }

    private static int SignedInt(ref WireReader r)
    {
        uint value = r.UVar();
        return (int)(value >> 1) ^ -(int)(value & 1);
    }

    private static long SignedLong(ref WireReader r)
    {
        ulong value = 0;
        for (int shift = 0; shift <= 63; shift += 7)
        {
            ulong b = (byte)r.I8();
            if (shift == 63 && b > 1) throw new WireException("Signed varlong overflow");
            value |= (b & 127) << shift;
            if (b < 128) return (long)(value >> 1) ^ -(long)(value & 1);
        }
        throw new WireException("Invalid signed varlong");
    }

    internal static uint Crc32C(ReadOnlySpan<byte> bytes)
    {
        uint crc = uint.MaxValue;
        if (Sse42.X64.IsSupported || System.Runtime.Intrinsics.Arm.Crc32.Arm64.IsSupported)
            crc = CrcLanes.Consume(ref bytes, crc);
        return Crc32CTail(bytes, crc);
    }

    internal static uint Crc32CTail(ReadOnlySpan<byte> bytes, uint crc = uint.MaxValue)
    {
        while (bytes.Length >= sizeof(ulong))
        {
            crc = BitOperations.Crc32C(crc, BinaryPrimitives.ReadUInt64LittleEndian(bytes));
            bytes = bytes[sizeof(ulong)..];
        }
        foreach (byte b in bytes) crc = BitOperations.Crc32C(crc, b);
        return ~crc;
    }

    private static class CrcLanes
    {
        private const int LaneBytes = 256;
        private const uint Polynomial = 0x82f63b78;
        private static readonly uint[] ShiftTable = CreateShiftTable();

        internal static uint Consume(ref ReadOnlySpan<byte> bytes, uint crc)
        {
            while (bytes.Length >= 3 * LaneBytes)
            {
                uint first = crc, second = 0, third = 0;
                ref byte p = ref MemoryMarshal.GetReference(bytes);
                for (int i = 0; i < LaneBytes; i += sizeof(ulong))
                {
                    first = BitOperations.Crc32C(first, Load(ref p, i));
                    second = BitOperations.Crc32C(second, Load(ref p, i + LaneBytes));
                    third = BitOperations.Crc32C(third, Load(ref p, i + 2 * LaneBytes));
                }
                // CRC is linear over GF(2). Advance by the length of the next lane before merging it.
                crc = Shift(Shift(first) ^ second) ^ third;
                bytes = bytes[(3 * LaneBytes)..];
            }
            return crc;
        }

        private static ulong Load(ref byte start, int offset)
        {
            ulong value = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref start, offset));
            return BitConverter.IsLittleEndian ? value : BinaryPrimitives.ReverseEndianness(value);
        }

        private static uint Shift(uint crc) => ShiftTable[crc & 255] ^ ShiftTable[256 + ((crc >> 8) & 255)] ^
            ShiftTable[512 + ((crc >> 16) & 255)] ^ ShiftTable[768 + (crc >> 24)];

        private static uint[] CreateShiftTable()
        {
            var table = new uint[1024];
            for (int bit = 0; bit < 32; bit++)
            {
                uint shifted = 1u << bit;
                for (int i = 0; i < LaneBytes * 8; i++)
                    shifted = (shifted >> 1) ^ ((shifted & 1) != 0 ? Polynomial : 0);
                int start = bit / 8 * 256;
                int width = 1 << (bit % 8);
                for (int value = 0; value < width; value++)
                    table[start + width + value] = table[start + value] ^ shifted;
            }
            return table;
        }
    }
}

internal static class TimestampIndex
{
    internal static int FirstAtLeast(ReadOnlySpan<long> values, long target)
    {
        int i = 0;
        if (Vector256.IsHardwareAccelerated && values.Length >= Vector256<long>.Count)
        {
            var threshold = Vector256.Create(target);
            ref long start = ref MemoryMarshal.GetReference(values);
            for (; i <= values.Length - Vector256<long>.Count; i += Vector256<long>.Count)
            {
                var timestamps = Vector256.LoadUnsafe(ref start, (nuint)i);
                uint mask = Vector256.GreaterThanOrEqual(timestamps, threshold).ExtractMostSignificantBits();
                if (mask != 0) return i + BitOperations.TrailingZeroCount(mask);
            }
        }
        for (; i < values.Length; i++)
            if (values[i] >= target) return i;
        return -1;
    }
}
