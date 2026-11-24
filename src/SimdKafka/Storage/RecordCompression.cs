using System.Buffers;
using System.Buffers.Binary;
using System.IO.Compression;
using System.IO.Hashing;
using System.Runtime.InteropServices;
using K4os.Compression.LZ4.Streams;
using Snappier;
using ZstdSharp;
using ZstdSharp.Unsafe;

namespace SimdKafka;

internal sealed class ExpansionLimitException : Exception;

internal sealed class InflatedRecords : IDisposable
{
    internal const int MaxSize = 32 * 1024 * 1024;
    private byte[] _bytes;
    internal int Length { get; private set; }
    internal ReadOnlySpan<byte> Span => _bytes.AsSpan(0, Length);

    internal InflatedRecords(int initialSize) => _bytes = ArrayPool<byte>.Shared.Rent(initialSize);

    internal Span<byte> Writable(int size)
    {
        if (size < 0 || size > MaxSize - Length) throw new ExpansionLimitException();
        if (Length + size > _bytes.Length)
        {
            byte[] next = ArrayPool<byte>.Shared.Rent(Math.Min(MaxSize, Math.Max(Length + size, _bytes.Length * 2)));
            Span.CopyTo(next);
            ArrayPool<byte>.Shared.Return(_bytes);
            _bytes = next;
        }
        return _bytes.AsSpan(Length, size);
    }

    internal void Advance(int count) => Length += count;
    public void Dispose() => ArrayPool<byte>.Shared.Return(_bytes);
}

internal static class RecordCompression
{
    private static readonly SemaphoreSlim DecodeSlots = new(Math.Clamp(Environment.ProcessorCount / 2, 1, 4));
    private static ReadOnlySpan<byte> XerialMagic => [0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0];

    internal static (int Delta, long Timestamp) Scan(ReadOnlyMemory<byte> source, RecordBatch batch, long? target,
        int firstDelta, bool findMaximum, long earliest)
    {
        DecodeSlots.Wait();
        try
        {
            using var output = new InflatedRecords(Math.Clamp(source.Length * 2, 4096, 256 * 1024));
            if (batch.Codec == 2) SnappyRecords(source.Span, output);
            else StreamRecords(source, batch.Codec, output);
            return RecordBatch.ScanRecords(output.Span, batch, target, firstDelta, findMaximum, earliest);
        }
        catch (Exception error) when (error is IOException or InvalidDataException or ArgumentException or
            ZstdException or InvalidOperationException or NotSupportedException)
        {
            throw new WireException("Malformed compressed record batch");
        }
        finally
        {
            DecodeSlots.Release();
        }
    }

    private static void StreamRecords(ReadOnlyMemory<byte> bytes, int codec, InflatedRecords output)
    {
        if (!MemoryMarshal.TryGetArray(bytes, out var segment))
            segment = new ArraySegment<byte>(bytes.ToArray());
        using var source = new MemoryStream(segment.Array!, segment.Offset, segment.Count, writable: false);
        using Stream decoder = codec switch
        {
            1 => new GZipStream(source, CompressionMode.Decompress, leaveOpen: true),
            3 => LZ4Stream.Decode(source, leaveOpen: true),
            4 => ZstdDecoder(source),
            _ => throw new WireException("Unsupported codec")
        };
        while (output.Length < InflatedRecords.MaxSize)
        {
            int read = decoder.Read(output.Writable(Math.Min(64 * 1024, InflatedRecords.MaxSize - output.Length)));
            if (read == 0) break;
            output.Advance(read);
        }
        if (output.Length == InflatedRecords.MaxSize && decoder.ReadByte() != -1)
            throw new ExpansionLimitException();
        if (source.Position != source.Length) throw new WireException("Trailing compressed bytes");
        if (codec == 1 && (bytes.Length < 18 ||
            BinaryPrimitives.ReadUInt32LittleEndian(bytes.Span[^4..]) != (uint)output.Length ||
            BinaryPrimitives.ReadUInt32LittleEndian(bytes.Span[^8..]) != Crc32.HashToUInt32(output.Span)))
            throw new WireException("Invalid gzip trailer");
    }

    private static DecompressionStream ZstdDecoder(Stream source)
    {
        var decoder = new DecompressionStream(source, leaveOpen: true);
        try
        {
            decoder.SetParameter(ZSTD_dParameter.ZSTD_d_windowLogMax, 25);
            return decoder;
        }
        catch
        {
            decoder.Dispose();
            throw;
        }
    }

    private static void SnappyRecords(ReadOnlySpan<byte> bytes, InflatedRecords output)
    {
        if (!bytes.StartsWith(XerialMagic))
        {
            SnappyBlock(bytes, output);
            return;
        }
        var reader = new WireReader(bytes[8..]);
        if (reader.I32() != 1 || reader.I32() != 1)
            throw new WireException("Unsupported Xerial Snappy stream version");
        while (reader.Remaining != 0)
        {
            int size = reader.I32();
            if (size <= 0) throw new WireException("Invalid Snappy block length");
            SnappyBlock(reader.Take(size), output);
        }
    }

    private static void SnappyBlock(ReadOnlySpan<byte> bytes, InflatedRecords output)
    {
        int size = Snappy.GetUncompressedLength(bytes);
        if (size <= 0 || Snappy.Decompress(bytes, output.Writable(size)) != size)
            throw new WireException("Invalid Snappy block");
        output.Advance(size);
    }
}
