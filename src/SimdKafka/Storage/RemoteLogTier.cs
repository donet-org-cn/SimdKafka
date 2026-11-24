using System.Buffers;
using System.Collections.Immutable;
using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Win32.SafeHandles;

namespace SimdKafka;

internal sealed record RemoteTierRoot(string Directory, string Identity)
{
    internal static RemoteTierRoot Connect(string root, string data, string cluster)
    {
        if (!System.IO.Directory.Exists(root)) throw new DirectoryNotFoundException("Remote log tier root is unavailable");
        string directory = Path.Combine(root, Convert.ToHexString(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(cluster))));
        string local = Path.Combine(data, "remote-tier.identity"), remote = Path.Combine(directory, "identity");
        if (File.Exists(local))
        {
            var existing = new RemoteTierRoot(directory, File.ReadAllText(local));
            existing.Check();
            return existing;
        }
        DurableFileSystem.EnsureDirectory(directory);
        string identity = File.Exists(remote) ? File.ReadAllText(remote) : Guid.NewGuid().ToString("N");
        if (!Guid.TryParseExact(identity, "N", out _)) throw new InvalidDataException("Invalid remote log tier identity");
        if (!File.Exists(remote)) WriteIdentity(remote, identity);
        WriteIdentity(local, identity);
        return new(directory, identity);
    }

    private static void WriteIdentity(string path, string identity)
    {
        string temporary = path + ".tmp";
        using (var file = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            file.Write(System.Text.Encoding.ASCII.GetBytes(identity));
            file.Flush(flushToDisk: true);
        }
        DurableFileSystem.Replace(temporary, path);
    }

    internal void Check()
    {
        if (!Guid.TryParseExact(Identity, "N", out _) || File.ReadAllText(Path.Combine(Directory, "identity")) != Identity)
            throw new InvalidDataException("Mounted remote log tier does not match its local identity");
    }
}

internal sealed class RemoteLogTier : IDisposable
{
    internal sealed record Segment(long FirstOffset, long EndOffset, long Position, int Length, string Sha256);
    private sealed record Manifest(ImmutableArray<Segment> Segments, string Sha256);

    private readonly string _root, _manifestPath;
    private readonly RemoteTierRoot _mount;
    private readonly object _gate = new();
    private ImmutableArray<Segment> _segments;
    private SafeFileHandle? _remoteReader;
    private long _readerPosition = -1;
    private long _evictedBytes;
    private bool _evictionFailed;

    internal RemoteLogTier(RemoteTierRoot mount, string partition, string manifestPath)
    {
        _mount = mount;
        _root = Path.Combine(mount.Directory, partition);
        _manifestPath = manifestPath;
        _segments = LoadManifest();
    }

    internal long ArchivedOffset { get { lock (_gate) return _segments.IsEmpty ? 0 : _segments[^1].EndOffset; } }
    internal long LatestArchivedOffset { get { lock (_gate) return _segments.IsEmpty ? -1 : _segments[^1].EndOffset - 1; } }
    internal long EvictedBytes => Interlocked.Read(ref _evictedBytes);

    private string SegmentPath(long position) => Path.Combine(_root, position.ToString("D20", CultureInfo.InvariantCulture) + ".log");

    private ImmutableArray<Segment> LoadManifest()
    {
        FileStream file;
        try { file = File.OpenRead(_manifestPath); }
        catch (FileNotFoundException) { return []; }
        using (file)
        {
            var manifest = JsonSerializer.Deserialize<Manifest>(file) ?? throw new InvalidDataException("Missing remote log manifest");
            if (manifest.Segments.IsDefault || !string.Equals(HashManifest(manifest.Segments), manifest.Sha256, StringComparison.Ordinal))
                throw new InvalidDataException("Remote log manifest checksum mismatch");
            long position = 0, offset = 0;
            foreach (var segment in manifest.Segments)
            {
                if (segment.Position != position || segment.FirstOffset != offset || segment.EndOffset <= offset || segment.Length <= 0)
                    throw new InvalidDataException("Remote log manifest is not contiguous");
                using var remote = File.OpenRead(SegmentPath(position));
                if (remote.Length != segment.Length || !string.Equals(Convert.ToHexString(SHA256.HashData(remote)), segment.Sha256,
                    StringComparison.Ordinal)) throw new InvalidDataException("Remote log segment checksum mismatch");
                position += segment.Length;
                offset = segment.EndOffset;
            }
            return manifest.Segments;
        }
    }

    private static string HashManifest(ImmutableArray<Segment> segments) =>
        Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(segments)));

    internal void Publish(SafeFileHandle local, long firstOffset, long endOffset, long position, int length)
    {
        _mount.Check();
        DurableFileSystem.EnsureDirectory(_root);
        string destination = SegmentPath(position), temporary = destination + ".tmp";
        byte[] scratch = ArrayPool<byte>.Shared.Rent(128 * 1024);
        string digest;
        try
        {
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            using (var output = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                int written = 0;
                while (written < length)
                {
                    var buffer = scratch.AsSpan(0, Math.Min(scratch.Length, length - written));
                    ReadAll(local, buffer, position + written);
                    hash.AppendData(buffer);
                    output.Write(buffer);
                    written += buffer.Length;
                }
                output.Flush(flushToDisk: true);
            }
            digest = Convert.ToHexString(hash.GetHashAndReset());
        }
        finally { ArrayPool<byte>.Shared.Return(scratch); }
        DurableFileSystem.Replace(temporary, destination);
        _mount.Check();
        using (var remote = File.OpenRead(destination))
            if (remote.Length != length || !string.Equals(Convert.ToHexString(SHA256.HashData(remote)), digest, StringComparison.Ordinal))
                throw new InvalidDataException("Published remote log segment failed verification");

        lock (_gate)
        {
            var next = _segments.Add(new(firstOffset, endOffset, position, length, digest));
            string draft = _manifestPath + ".tmp";
            using (var output = new FileStream(draft, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                JsonSerializer.Serialize(output, new Manifest(next, HashManifest(next)));
                output.Flush(flushToDisk: true);
            }
            DurableFileSystem.Replace(draft, _manifestPath);
            _segments = next;
            Evict(local, position, length);
        }
    }

    internal void RecoverEviction(SafeFileHandle local)
    {
        lock (_gate)
        {
            long end = _segments.IsEmpty ? 0 : _segments[^1].Position + _segments[^1].Length;
            if (end > RandomAccess.GetLength(local)) throw new InvalidDataException("Remote log manifest extends past local logical size");
            if (end != 0) Evict(local, 0, end);
        }
    }

    private void Evict(SafeFileHandle local, long position, long length)
    {
        if (_evictionFailed) return;
        if (DurableFileSystem.TryPunchHole(local, position, length))
        {
            Interlocked.Exchange(ref _evictedBytes, position + length);
            return;
        }
        _evictionFailed = true;
        Console.Error.WriteLine("SimdKafka remote tier: local filesystem could not release archived bytes");
    }

    internal void ReadExactly(SafeFileHandle local, Span<byte> bytes, long position)
    {
        lock (_gate)
        {
            while (!bytes.IsEmpty)
            {
                int lo = 0, hi = _segments.Length;
                while (lo < hi)
                {
                    int mid = lo + (hi - lo) / 2;
                    var range = _segments[mid];
                    if (position >= range.Position + range.Length) lo = mid + 1;
                    else hi = mid;
                }
                if (lo == _segments.Length) { ReadAll(local, bytes, position); return; }
                var segment = _segments[lo];
                if (_readerPosition != segment.Position)
                {
                    _mount.Check();
                    _remoteReader?.Dispose();
                    _readerPosition = -1;
                    _remoteReader = File.OpenHandle(SegmentPath(segment.Position), FileMode.Open, FileAccess.Read, FileShare.Read,
                        FileOptions.RandomAccess);
                    _readerPosition = segment.Position;
                }
                long relative = position - segment.Position;
                int chunk = (int)Math.Min(bytes.Length, segment.Length - relative);
                ReadAll(_remoteReader!, bytes[..chunk], relative);
                position += chunk;
                bytes = bytes[chunk..];
            }
        }
    }

    private static void ReadAll(SafeFileHandle file, Span<byte> bytes, long position)
    {
        while (!bytes.IsEmpty)
        {
            int read = RandomAccess.Read(file, bytes, position);
            if (read == 0) throw new EndOfStreamException();
            position += read;
            bytes = bytes[read..];
        }
    }

    public void Dispose() { lock (_gate) _remoteReader?.Dispose(); }
}
