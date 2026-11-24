using System.Buffers.Binary;
using System.Buffers.Text;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO.Compression;
using System.Net;
using System.Net.Security;
using System.Security.Authentication;
using System.Net.Sockets;
using System.Runtime.Intrinsics;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using SimdKafka;

Check(RecordBatch.Crc32C("123456789"u8) == 0xe3069283, "CRC32C check vector");
var crcInput = new byte[64 * 1024 + 13];
new Random(29).NextBytes(crcInput);
for (int size = 0; size <= 2048; size++)
    Check(RecordBatch.Crc32C(crcInput.AsSpan(3, size)) == ReferenceCrc32C(crcInput.AsSpan(3, size)),
        "CRC32C matches the bitwise reference across lane boundaries");
Check(RecordBatch.Crc32C(crcInput) == ReferenceCrc32C(crcInput), "CRC32C long input matches the bitwise reference");
ExerciseCompression();
await CheckFrameBudget();
CheckShareExpiry();
long[] timestamps = [-9, -4, 7, 10, 0, 12, 7, long.MaxValue, 13];
for (int i = 0; i <= timestamps.Length; i++)
    foreach (long target in new[] { long.MinValue, -7, 0, 10, 12, long.MaxValue })
        Check(TimestampIndex.FirstAtLeast(timestamps.AsSpan(0, i), target) ==
            Array.FindIndex(timestamps[..i], value => value >= target), "SIMD index matches scalar");

if (args.Contains("--benchmark"))
{
    const int runs = 6000;
    long[] values = new long[128 * 1024];
    values[^1] = 1;
    for (int i = 0; i < 200; i++) { FindScalar(values, 1); TimestampIndex.FirstAtLeast(values, 1); }
    var watch = Stopwatch.StartNew();
    int scalar = 0;
    for (int i = 0; i < runs; i++) scalar ^= FindScalar(values, (i & 1) + 1);
    double scalarMs = watch.Elapsed.TotalMilliseconds;
    watch.Restart();
    int vector = 0;
    for (int i = 0; i < runs; i++) vector ^= TimestampIndex.FirstAtLeast(values, (i & 1) + 1);
    Check(vector == scalar, "benchmark paths agree");
    Console.WriteLine($"Timestamp index, Vector256={Vector256.IsHardwareAccelerated}, scalar {scalarMs:F1} ms, vector {watch.Elapsed.TotalMilliseconds:F1} ms");

    const int crcRuns = 16_000;
    for (int i = 0; i < 2_000; i++) { RecordBatch.Crc32CTail(crcInput); RecordBatch.Crc32C(crcInput); }
    watch.Restart();
    uint single = 0;
    for (int i = 0; i < crcRuns; i++) single += RecordBatch.Crc32CTail(crcInput.AsSpan(i & 3));
    double singleMs = watch.Elapsed.TotalMilliseconds;
    watch.Restart();
    uint parallel = 0;
    for (int i = 0; i < crcRuns; i++) parallel += RecordBatch.Crc32C(crcInput.AsSpan(i & 3));
    Check(single == parallel, "CRC32C benchmark paths agree");
    Console.WriteLine($"CRC32C across 1000 MiB, single lane {singleMs:F1} ms, three lanes {watch.Elapsed.TotalMilliseconds:F1} ms");
}

static uint ReferenceCrc32C(ReadOnlySpan<byte> bytes)
{
    uint crc = uint.MaxValue;
    foreach (byte value in bytes)
    {
        crc ^= value;
        for (int bit = 0; bit < 8; bit++) crc = (crc >> 1) ^ ((crc & 1) != 0 ? 0x82f63b78u : 0u);
    }
    return ~crc;
}

var reserve = new TcpListener(IPAddress.Loopback, 0);
reserve.Start();
var endpoint = (IPEndPoint)reserve.LocalEndpoint;
reserve.Stop();
string dir = Path.Combine(Path.GetTempPath(), $"simdkafka-smoke-{Guid.NewGuid():N}");
var options = new BrokerOptions(endpoint, "127.0.0.1", endpoint.Port, dir, 1, true, true);
string clusterId = "";
long producerId = -1;
byte[]? latest = null;
try
{
    if (args.Contains("--security-only"))
    {
        await ScramSmoke();
        await OAuthTokenSmoke();
        await ClusterSecuritySmoke();
        return;
    }
    await RunBroker(async catalog =>
    {
        using var peer = await Connect();
        using var reader = await Connect();
        clusterId = catalog.ClusterId;
        var versions = new WireReader(await Send(peer, Api.ApiVersions, 4, w =>
        {
            w.UVar(5);
            "test"u8.CopyTo(w.Reserve(4));
            w.UVar(2);
            w.I8((sbyte)'1');
            w.UVar(0);
        }, fragmented: true));
        Check(versions.I16() == 0, "flexible ApiVersions");
        int apiCount = checked((int)versions.UVar()) - 1;
        var advertised = new Dictionary<Api, short>();
        for (int i = 0; i < apiCount; i++)
        {
            var api = (Api)versions.I16();
            versions.I16();
            advertised.Add(api, versions.I16());
            versions.Tags();
        }
        Check(advertised[Api.Fetch] == 13 && advertised[Api.FindCoordinator] == 6 &&
            advertised.ContainsKey(Api.DescribeTopicPartitions) &&
            !advertised.ContainsKey(Api.SaslAuthenticate), "feature and security negotiation");
        var versionError = new WireReader(await Send(peer, Api.ApiVersions, 42, static _ => { }));
        Check(versionError.I16() == 35 && versionError.I32() == 0, "ApiVersions version 0 fallback");
        Check(await ApiVersionsV5(peer, null, -1) == Error.None &&
            await ApiVersionsV5(peer, clusterId, 1) == Error.None &&
            await ApiVersionsV5(peer, clusterId, 2) == Error.RebootstrapRequired &&
            await ApiVersionsV5(peer, "other-cluster", 1) == Error.RebootstrapRequired &&
            await ApiVersionsV5(peer, null, 1) == Error.InvalidRequest, "ApiVersions v5 detects misrouted connections");
        await CheckMetadataV13(peer, 1, clusterId);

        var m = new WireReader(await Send(peer, Api.Metadata, 8, w =>
        {
            w.I32(1); w.String("events"); w.Bool(true); w.Bool(false); w.Bool(false);
        }));
        Check(m.I32() == 0 && m.I32() == 1 && m.I32() == 1 && m.String() == options.Host &&
              m.I32() == options.Port && m.NullableString() is null && m.String() == clusterId &&
              m.I32() == 1 && m.I32() == 1 && m.I16() == 0 && m.String() == "events", "metadata and advertised broker");
        Check(await ProducerIdV5(peer, 1, -1) == (Error.InvalidRequest, -1L, (short)-1),
            "InitProducerId rejects a partially specified producer identity");
        var id = await ProducerIdV5(peer, -1, -1);
        producerId = id.Id;
        Check(id.Error == Error.None && producerId > 0 && id.Epoch == 0, "InitProducerId v5 allocates a fresh producer ID");

        var first = Batch(producerId, 0, 1234, "first"u8);
        var waiting = Send(reader, Api.Fetch, 5, w => FetchRequest(w, 0, 2000));
        await Task.Delay(100);
        Check(!waiting.IsCompleted, "Fetch waits for MinBytes");
        Check(Appended(await Send(peer, Api.Produce, 8, w => ProduceRequest(w, first)), 0), "first append");
        Check(Fetched(await waiting, 0, 1, first), "append wakes Fetch");
        await CheckFlexibleApis(peer, catalog.Get("events")!.Id, first);
        Check(Appended(await Send(peer, Api.Produce, 8, w => ProduceRequest(w, first)), 0), "idempotent retry");
        var conflict = Batch(producerId, 0, 1234, "other"u8);
        Check(Appended(await Send(peer, Api.Produce, 8, w => ProduceRequest(w, conflict)), 0) &&
            Fetched(await Send(peer, Api.Fetch, 5, w => FetchRequest(w, 0, 0)), 0, 1, first),
            "Kafka recognizes an existing sequence range and keeps the acknowledged original bytes");
        latest = Batch(producerId, 1, 5678, "latest"u8);
        var bad = (byte[])latest.Clone();
        bad[^1] ^= 1;
        Check(Appended(await Send(peer, Api.Produce, 8, w => ProduceRequest(w, bad)), -1, Error.CorruptMessage), "bad CRC rejected");
        await Send(peer, Api.Produce, 8, w => ProduceRequest(w, latest, acks: 0), expectResponse: false);
        Check(Fetched(await Send(peer, Api.Fetch, 5, w => FetchRequest(w, 1, 0)), 1, 2, latest), "acks=0 sends no response");
        var log = catalog.Get("events")!.Partitions[0];
        Check(log.Select(0, first.Length, true).Size == first.Length &&
              log.Select(0, 1, true).Size == first.Length && log.Select(0, 1, false).Size == 0 &&
              log.Select(0, first.Length + latest.Length, true).Size == first.Length + latest.Length,
            "fetch byte budgets preserve batch boundaries");

        var list = new WireReader(await Send(peer, Api.ListOffsets, 5, w =>
        {
            w.I32(-1); w.I8(0); w.I32(1); w.String("events"); w.I32(1); w.I32(0); w.I32(0); w.I64(2000);
        }));
        Check(list.I32() == 0 && list.Count() == 1 && list.String() == "events" &&
              list.Count() == 1 && list.I32() == 0 && list.I16() == 0 && list.I64() == 5678 &&
              list.I64() == 1 && list.I32() == 0, "timestamp lookup");
        var commit = new WireReader(await Send(peer, Api.OffsetCommit, 6, w =>
        {
            w.String("manual"); w.I32(-1); w.String(""); w.I32(1); w.String("events"); w.I32(1);
            w.I32(0); w.I64(2); w.I32(0); w.String("persisted");
        }));
        Check(commit.I32() == 0 && commit.Count() == 1 && commit.String() == "events" &&
              commit.Count() == 1 && commit.I32() == 0 && commit.I16() == 0, "offset commit");

        await TwoMembers(catalog);
    });

    string log = Directory.GetFiles(dir, "*.log", SearchOption.AllDirectories).Single();
    long goodLength = new FileInfo(log).Length;
    await using (var torn = new FileStream(log, FileMode.Append))
        await torn.WriteAsync(new byte[] { 0, 0, 0, 0, 0 });

    await RunBroker(async catalog =>
    {
        Check(new FileInfo(log).Length == goodLength, "torn tail truncated");
        Check(catalog.ClusterId == clusterId && catalog.LastProducerId == producerId, "durable catalog");
        using var peer = await Connect();
        Check(Fetched(await Send(peer, Api.Fetch, 5, w => FetchRequest(w, 1, 0)), 1, 2, latest!), "durable log");
        Check(Appended(await Send(peer, Api.Produce, 8, w => ProduceRequest(w, latest!)), 1), "durable idempotent state");
        var f = new WireReader(await Send(peer, Api.OffsetFetch, 5, w =>
        {
            w.String("manual"); w.I32(1); w.String("events"); w.I32(1); w.I32(0);
        }));
        Check(f.I32() == 0 && f.Count() == 1 && f.String() == "events" && f.Count() == 1 && f.I32() == 0 &&
              f.I64() == 2 && f.I32() == 0 && f.String() == "persisted" && f.I16() == 0 && f.I16() == 0,
            "durable offset");
        f.End();
        var bumped = Batch(producerId, 0, 7890, "new-epoch"u8, epoch: 1);
        var gap = Batch(producerId, 1, 7890, "not-first"u8, epoch: 1);
        Check(Appended(await Send(peer, Api.Produce, 8, w => ProduceRequest(w, gap)), -1, Error.OutOfOrderSequence) &&
            Appended(await Send(peer, Api.Produce, 8, w => ProduceRequest(w, bumped)), 2) &&
            Appended(await Send(peer, Api.Produce, 8, w => ProduceRequest(w, latest!)), -1, Error.InvalidProducerEpoch),
            "new epoch requires sequence zero and fences older batches");
    });
    await RunBroker(async _ =>
    {
        using var peer = await Connect();
        var bumped = Batch(producerId, 0, 7890, "new-epoch"u8, epoch: 1);
        Check(Appended(await Send(peer, Api.Produce, 8, w => ProduceRequest(w, latest!)), -1, Error.InvalidProducerEpoch) &&
            Appended(await Send(peer, Api.Produce, 8, w => ProduceRequest(w, bumped)), 2) &&
            Fetched(await Send(peer, Api.Fetch, 5, w => FetchRequest(w, 2, 0)), 2, 3, bumped),
            "producer epoch and duplicate offset survive standalone restart");
    });
    string snapshot = Path.Combine(dir, "catalog.json");
    string saved = File.ReadAllText(snapshot);
    File.WriteAllText(snapshot, saved.Replace("persisted", "corrupted", StringComparison.Ordinal));
    try
    {
        using var catalog = new Catalog(options);
        throw new InvalidOperationException("Corrupt catalog was accepted");
    }
    catch (InvalidDataException e)
    {
        Check(e.Message == "Catalog checksum mismatch", "checksum detected a valid JSON mutation");
    }
    using (var snapshotJson = JsonDocument.Parse(saved))
        File.WriteAllText(snapshot, snapshotJson.RootElement.GetProperty("State").GetRawText());
    using (var legacy = new Catalog(options))
    {
        Check(legacy.ClusterId == clusterId && legacy.GetOffsets("manual").Single().Offset == 2,
            "legacy catalog opens with existing offsets");
        Check(legacy.AllocateProducerId() == (Error.None, producerId + 1), "legacy catalog upgrades on change");
    }
    using (var upgraded = new Catalog(options))
        Check(upgraded.LastProducerId == producerId + 1, "upgraded catalog persists");
    File.Move(snapshot, snapshot + ".saved");
    try
    {
        using var catalog = new Catalog(options);
        throw new InvalidOperationException("Reinitialized cluster with existing partition logs");
    }
    catch (InvalidDataException e)
    {
        Check(e.Message == "Catalog is missing but partition logs remain", "missing catalog fails closed");
    }
    await TopicLifecycle();
    await TieredLogSmoke();
    await LogAppendTimeSmoke();
    await ScramSmoke();
    await OAuthTokenSmoke();
    await ClusterSmoke.RunAsync(Path.Combine(dir, "raft"), Batch(1, 0, 100, "replicated"u8),
        Batch(1, 1, 200, "successor"u8), Batch(1, 0, 100, "conflict"u8), Batch(-1, -1, 300, "concurrent"u8),
        Batch(1, 0, 400, "new-epoch"u8, epoch: 2), Batch(1, 1, 400, "invalid-next-epoch"u8, epoch: 2));
    await ClusterSmoke.RunMetadataAsync(Path.Combine(dir, "controller"));
    await ClusterBrokerSmoke();
    await ClusterSecuritySmoke();
    Console.WriteLine("SimdKafka smoke passed");
}
finally
{
    if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
}

static void CheckShareExpiry()
{
    Guid topic = Guid.NewGuid(), cycleA = Guid.NewGuid(), cycleB = Guid.NewGuid();
    var image = new ClusterMetadataImage(0, 0,
        ImmutableDictionary<string, ReplicatedTopic>.Empty.Add("lease-test", new(topic, "lease-test",
            [new(Guid.NewGuid(), 0, [1], [1], LeaderId: 1, LeaderEpoch: 5)])),
        ImmutableDictionary<string, ImmutableArray<CommittedOffset>>.Empty);
    const string group = "expiring-share";
    MetadataResult Apply(ShareCommand command)
    {
        var applied = ShareGroups.Apply(image, group, command);
        image = applied.Next;
        return applied.Result;
    }
    var reset = new ShareCommand(ShareAction.Reset, TopicId: topic, Partition: 0, LeaderId: 1, LeaderEpoch: 5,
        LogEnd: 1, Offset: 0);
    Check(Apply(reset).Error == Error.None &&
        Apply(new(ShareAction.Heartbeat, "a", "User:a", 0, 1, cycleA, ["lease-test"])).Error == Error.None &&
        Apply(new(ShareAction.Heartbeat, "b", "User:b", 0, 1, cycleB, ["lease-test"])).Error == Error.None,
        "initialize an expiring share delivery");
    var claim = new ShareCommand(ShareAction.Acquire, "a", "User:a", Now: 2, TopicId: topic, Partition: 0, LeaderId: 1,
        LeaderEpoch: 5, LogEnd: 1, Records: [new(0, 0)], Limit: 1);
    Check(Apply(claim).Share?.Ranges is [{ First: 0, Last: 0, Count: 1 }] &&
        Apply(claim with { Member = "b", Principal = "User:b", Now = 3 }).Share?.Ranges is [], "live share lock excludes peers");
    Check(Apply(claim with { Member = "b", Principal = "User:b", Now = ShareGroups.LockMs + 3 }).Share?.Ranges
        is [{ First: 0, Last: 0, Count: 2 }], "expired share lock is redelivered with the same offset");
    Check(Apply(claim with { Action = ShareAction.Acknowledge, Now = ShareGroups.LockMs + 4, Acks = [new(0, 0, [1])] }).Error ==
        Error.InvalidRecordState, "a stale share owner cannot acknowledge a reallocated record");
}

static async Task CheckFrameBudget()
{
    var budget = new FrameBudget(16);
    await budget.AcquireAsync(12, CancellationToken.None);
    using var stop = new CancellationTokenSource();
    Task blocked = budget.AcquireAsync(8, stop.Token).AsTask();
    Check(!blocked.IsCompleted, "oversized frames wait for shared capacity");
    await stop.CancelAsync();
    try
    {
        await blocked;
        throw new InvalidOperationException("canceled frame acquired capacity");
    }
    catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
    Task next = budget.AcquireAsync(16, CancellationToken.None).AsTask();
    budget.Release(12);
    await next.WaitAsync(TimeSpan.FromSeconds(2));
    budget.Release(16);
}

async Task RunBroker(Func<Catalog, Task> exercise)
{
    using var catalog = new Catalog(options);
    using var stop = new CancellationTokenSource();
    var broker = new Broker(options, catalog).RunAsync(stop.Token);
    try { await exercise(catalog); }
    finally { await stop.CancelAsync(); await broker; }
}

async Task TieredLogSmoke()
{
    string root = Path.Combine(dir, "tiered"), remote = Path.Combine(root, "remote");
    Directory.CreateDirectory(remote);
    var address = ClusterSmoke.ReserveAddresses(1).Single();
    var tiered = BrokerOptions.Parse(["--listen", address.ToString(), "--data", Path.Combine(root, "local"),
        "--tiered-storage-dir", remote, "--tier-segment-bytes", "4096", "--local-retention-bytes", "4096"]);
    byte[] first = Batch(-1, -1, 100, new byte[2500]);
    Guid topicId;
    using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(25));
    using (var catalog = new Catalog(tiered))
    {
        Check(catalog.Create("events", 1, false, out topicId, tier: new(true)) == Error.None, "create tiered topic");
        using var stop = new CancellationTokenSource();
        Task serve = new Broker(tiered, catalog).RunAsync(stop.Token);
        try
        {
            using var client = new TcpClient();
            await client.ConnectAsync(address, deadline.Token);
            for (int i = 0; i < 8; i++)
                Check(Appended(await Send(client, Api.Produce, 8, w => ProduceRequest(w, first)), i), "durable append before archive");
            var log = catalog.Get("events")!.Partitions[0];
            while (log.ArchivedOffset < 2) await Task.Delay(25, deadline.Token);
            Check(Fetched(await Send(client, Api.Fetch, 5, w => FetchRequest(w, 0, 0)), 0, 8, first), "Kafka Fetch reads archived data");
            long boundary = log.ArchivedOffset;
            var selection = log.Select(boundary - 1, 8192, true);
            Check(selection.Size >= first.Length * 2 && log.ReadVerified(new byte[selection.Size], selection.Position,
                boundary - 1, 8), "one fetch can cross from archived storage into a local tail");
            Check(log.OffsetFor(-2).Offset == 0 && log.OffsetFor(-4).Offset == log.ArchivedOffset &&
                log.OffsetFor(-5).Offset == log.ArchivedOffset - 1, "earliest, local earliest and tiered offsets remain distinct");
            Check(catalog.Create("aged", 1, false, out _, tier: new(true, LocalBytes: -1)) == Error.None,
                "create a low-volume tiered topic without byte retention");
            var aged = catalog.Get("aged")!.Partitions[0];
            byte[] old = Batch(-1, -1, 100, "aged"u8.ToArray());
            byte[] fresh = Batch(-1, -1, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 60_000, "fresh"u8.ToArray());
            Check(await aged.AppendAsync(old, 0) == new AppendResult(Error.None, 0) &&
                await aged.AppendAsync(fresh, 0) == new AppendResult(Error.None, 1), "append two small batches to the timed tier");
            Check(!aged.ArchiveNext(-1, 4096, -1) && aged.ArchivedOffset == 0, "infinite byte and time retention keep both batches local");
            var changed = new WireReader(await Send(client, Api.IncrementalAlterConfigs, 1, w =>
            {
                w.ArrayLength(1, true); w.I8(KafkaConfigurations.Topic); w.Text("aged", true);
                w.ArrayLength(1, true); w.Text(KafkaConfigurations.LocalMs, true); w.I8(0); w.Text("30000", true);
                w.UVar(0); w.UVar(0); w.Bool(false); w.UVar(0);
            }));
            Check(changed.I32() == 0 && changed.ArrayLength(true) == 1 && changed.I16() == 0, "change local time retention in Kafka config");
            while (aged.ArchivedOffset == 0) await Task.Delay(25, deadline.Token);
            Check(aged.ArchivedOffset == 1 && aged.OffsetFor(-2).Offset == 0 && aged.OffsetFor(-4).Offset == 1,
                "idle partitions archive old batches below the size threshold and retain fresh records");
            var pendingUpload = new WireReader(await Send(client, Api.ListOffsets, 11, w =>
            {
                w.I32(-1); w.I8(0); w.ArrayLength(1, true); w.Text("aged", true); w.ArrayLength(1, true);
                w.I32(0); w.I32(0); w.I64(-6); w.UVar(0); w.UVar(0); w.I32(5_000); w.UVar(0);
            }));
            Check(pendingUpload.I32() == 0 && pendingUpload.ArrayLength(true) == 1 && pendingUpload.Text(true) == "aged" &&
                pendingUpload.ArrayLength(true) == 1 && pendingUpload.I32() == 0 && pendingUpload.I16() == 0 &&
                pendingUpload.I64() == -1 && pendingUpload.I64() == 1, "ListOffsets v11 locates the first unarchived offset");
            var mixed = aged.Select(0, 8192, true);
            Check(mixed.Size == old.Length + fresh.Length && aged.ReadVerified(new byte[mixed.Size], mixed.Position, 0, 2),
                "timed archive and newer local batch are both readable");
        }
        finally { await stop.CancelAsync(); await serve; }
    }
    using (var reopened = new Catalog(tiered))
    {
        var topic = reopened.Get("events")!;
        Check(topic.Id == topicId && topic.Partitions[0].ArchivedOffset > 0 && topic.Partitions[0].OffsetFor(100).Offset == 0,
            "restart rebuilds indices from checked remote bytes and keeps the original topic ID");
        var aged = reopened.Get("aged")!;
        Check(reopened.TopicTier(aged)?.LocalMs == 30_000 && aged.Partitions[0].ArchivedOffset == 1,
            "timed tier retention and its archive boundary survive restart");
        Check(await topic.Partitions[0].AppendAsync(first, 0) == new AppendResult(Error.None, 8),
            "restart appends to the local tail without resetting offsets");
    }
    string archived = Directory.EnumerateFiles(remote, "*.log", SearchOption.AllDirectories).First();
    byte[] corrupted = await File.ReadAllBytesAsync(archived, deadline.Token);
    corrupted[^1] ^= 1;
    await File.WriteAllBytesAsync(archived, corrupted, deadline.Token);
    try
    {
        using var invalid = new Catalog(tiered);
        throw new InvalidOperationException("corrupt archived data was opened");
    }
    catch (InvalidDataException e) when (e.Message == "Remote log segment checksum mismatch") { }
    Console.WriteLine("Kafka standalone tier passed byte and time retention, mixed-tier reads, restart and remote corruption rejection");
}

async Task<TcpClient> Connect()
{
    var client = new TcpClient { NoDelay = true };
    await client.ConnectAsync(endpoint);
    return client;
}

async Task LogAppendTimeSmoke()
{
    var address = ClusterSmoke.ReserveAddresses(1).Single();
    var settings = options with { Listen = address, Port = address.Port, Data = Path.Combine(dir, "append-time") };
    byte[] pair = BatchWithOlderSecondRecord();
    using var compressed = new MemoryStream();
    using (var gzip = new GZipStream(compressed, CompressionLevel.Fastest, leaveOpen: true))
        gzip.Write(pair.AsSpan(RecordBatch.HeaderSize));
    var batch = GzipRecordBatch(pair, compressed.ToArray());
    long timestamp = -1;

    await Run(async (catalog, client) =>
    {
        var created = new WireReader(await SendFrame(client, Api.CreateTopics, 7, w =>
        {
            w.ArrayLength(1, true); w.Text("events", true); w.I32(1); w.I16(1);
            w.ArrayLength(0, true); w.ArrayLength(1, true); w.Text(KafkaConfigurations.TopicTimestampType, true);
            w.Text(KafkaConfigurations.LogAppendTime, true); w.UVar(0); w.UVar(0); w.I32(10000); w.Bool(false); w.UVar(0);
        }));
        Check(created.I32() == 0 && created.ArrayLength(true) == 1 && created.Text(true) == "events" &&
            created.Uuid() != Guid.Empty && created.I16() == 0, "Kafka creates a LogAppendTime topic");
        var allocated = catalog.AllocateProducerId();
        Check(allocated.Error == Error.None, "allocate an idempotent broker append-time producer");
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43), allocated.Id);
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53), 0);
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17), RecordBatch.Crc32C(batch.AsSpan(21)));
        long before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        timestamp = await Append(client, batch, 0);
        Check(timestamp >= before && timestamp <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() &&
            await Append(client, batch, 0) == timestamp, "Produce acks and retries preserve the broker timestamp");
        await Read(client, watermark: 2);
        byte[] forged = batch.ToArray();
        Check(RecordBatch.Parse(batch, out var original) == Error.None, "valid producer batch");
        original.StampAppendTime(forged, 123_456, out _);
        Check(Appended(await SendFrame(client, Api.Produce, 8, w => ProduceRequest(w, forged)), -1, Error.InvalidTimestamp),
            "producer cannot set the LogAppendTime wire flag");
        Check(await ChangeConfig(client, KafkaConfigurations.BrokerTimestampType, KafkaConfigurations.LogAppendTime,
            type: KafkaConfigurations.Broker, name: "") == Error.None, "set the cluster-wide timestamp default");
        Check(await ChangeConfig(client, KafkaConfigurations.TopicTimestampType, KafkaConfigurations.CreateTime) == Error.None &&
            await Append(client, Batch(-1, -1, 500, "older-create-time"u8), 2) == -1,
            "topic override uses CreateTime for subsequent batches");
        Check(await ChangeConfig(client, KafkaConfigurations.TimestampBefore, "1000", validate: true) == Error.None &&
            !catalog.Configurations.TopicTimestampLimits.ContainsKey(catalog.Get("events")!.Id), "validate-only does not alter timestamp bounds");
        Check(await ChangeConfig(client, KafkaConfigurations.TimestampBefore, "1000") == Error.None &&
            await ChangeConfig(client, KafkaConfigurations.TimestampAfter, "1000") == Error.None,
            "configure per-topic timestamp bounds through IncrementalAlterConfigs");
        var old = await SendFrame(client, Api.Produce, 8, w => ProduceRequest(w, batch));
        var future = await SendFrame(client, Api.Produce, 8, w =>
            ProduceRequest(w, Batch(-1, -1, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 10_000, "future"u8)));
        Check(Appended(old, -1, Error.InvalidTimestamp) && Appended(future, -1, Error.InvalidTimestamp),
            "compressed old timestamps and future timestamps are rejected without moving offsets");
        Check(await ChangeConfig(client, KafkaConfigurations.TopicTimestampType, null) == Error.None &&
            catalog.Configurations.UsesLogAppendTime(catalog.Get("events")!.Id),
            "deleting the topic setting restores the broker default");
    });
    await Run(async (catalog, client) =>
    {
        Check(catalog.Configurations.UsesLogAppendTime(catalog.Get("events")!.Id) &&
            catalog.Configurations.TopicTimestampLimits.GetValueOrDefault(catalog.Get("events")!.Id) == new TimestampLimits(1000, 1000) &&
            await Append(client, batch, 0) == timestamp, "restart preserves configuration and original idempotent acknowledgment");
        await Read(client, watermark: 3);
    });
    Console.WriteLine("Kafka LogAppendTime passed gzip byte preservation, broker timestamp, lookup, mode changes and restart");

    async Task Run(Func<Catalog, Stream, Task> exercise)
    {
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var catalog = new Catalog(settings);
        Task server = new Broker(settings, catalog).RunAsync(stop.Token);
        try
        {
            using var client = new TcpClient();
            await client.ConnectAsync(address, stop.Token);
            await exercise(catalog, client.GetStream());
        }
        finally { await stop.CancelAsync(); await server; }
    }

    static async Task<long> Append(Stream client, byte[] data, long offset)
    {
        var r = new WireReader(await SendFrame(client, Api.Produce, 8, w => ProduceRequest(w, data)));
        Check(r.Count() == 1 && r.String() == "events" && r.Count() == 1 && r.I32() == 0 &&
            r.I16() == 0 && r.I64() == offset, "valid append for timestamp test");
        return r.I64();
    }

    async Task Read(Stream client, long watermark)
    {
        var reader = new WireReader(await SendFrame(client, Api.Fetch, 5, w => FetchRequest(w, 0, 0)));
        Check(reader.I32() == 0 && reader.Count() == 1 && reader.String() == "events" && reader.Count() == 1 &&
            reader.I32() == 0 && reader.I16() == 0 && reader.I64() == watermark && reader.I64() == watermark &&
            reader.I64() == 0 && reader.Count(true) == -1, "timestamp Fetch header");
        byte[] fetched = reader.Bytes()[..batch.Length].ToArray();
        Check(RecordBatch.Parse(fetched, out var stored, fromStorage: true) == Error.None && stored.LogAppendTime &&
            stored.MaxTimestamp == timestamp && stored.FirstTimestamp == 700 &&
            batch.AsSpan(RecordBatch.HeaderSize).SequenceEqual(fetched.AsSpan(RecordBatch.HeaderSize)) &&
            RecordBatch.FindRecord(fetched, stored, timestamp, firstDelta: 1) == (1, timestamp),
            "both records use the broker timestamp and the compressed payload remains unchanged");
        var offset = new WireReader(await SendFrame(client, Api.ListOffsets, 5, w =>
        { w.I32(-1); w.I8(0); w.I32(1); w.String("events"); w.I32(1); w.I32(0); w.I32(-1); w.I64(timestamp); }));
        Check(offset.I32() == 0 && offset.Count() == 1 && offset.String() == "events" && offset.Count() == 1 &&
            offset.I32() == 0 && offset.I16() == 0 && offset.I64() == timestamp && offset.I64() == 0,
            "ListOffsets uses the broker timestamp across restarts");
    }
}

async Task ScramSmoke()
{
    var root = Path.Combine(dir, "scram");
    Directory.CreateDirectory(root);
    var (ca, certificates) = CreateClusterCertificates(root);
    using var authority = X509CertificateLoader.LoadCertificateFromFile(ca);
    var endpoint = ClusterSmoke.ReserveAddresses(1).Single();
    byte[] salt = RandomNumberGenerator.GetBytes(24);
    byte[] password = "smoke-password"u8.ToArray();
    var keys = new Dictionary<string, object>(StringComparer.Ordinal);
    foreach (string mechanism in new[] { ScramCredentials.Sha256, ScramCredentials.Sha512 })
    {
        bool sha256 = mechanism == ScramCredentials.Sha256;
        byte[] salted = Rfc2898DeriveBytes.Pbkdf2(password, salt, 4096, sha256 ? HashAlgorithmName.SHA256 : HashAlgorithmName.SHA512,
            sha256 ? 32 : 64);
        byte[] client = Hmac(sha256, salted, "Client Key"u8);
        keys.Add(mechanism, new { iterations = 4096, salt,
            storedKey = sha256 ? SHA256.HashData(client) : SHA512.HashData(client), serverKey = Hmac(sha256, salted, "Server Key"u8) });
    }
    var scram = Path.Combine(root, "scram.json");
    await File.WriteAllBytesAsync(scram, JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object> { ["smoke,user"] = keys }));
    var plain = Path.Combine(root, "plain.json");
    await File.WriteAllTextAsync(plain, "{\"smoke\":\"plain-smoke\",\"reader\":\"reader-smoke\"}");
    var secured = BrokerOptions.Parse(["--listen", endpoint.ToString(), "--tls-pfx", certificates[0], "--sasl-scram-users", scram,
        "--sasl-plain-users", plain, "--sasl-dynamic-scram", "--acl-super-user", "User:smoke", "--acl-super-user", "User:smoke,user",
        "--data", Path.Combine(root, "data")]);
    using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(50));
    using (var catalog = new Catalog(secured))
    {
        Task serve = new Broker(secured, catalog).RunAsync(stop.Token);
        try
        {
            await Exchange(ScramCredentials.Sha256, tampered: false);
            await Exchange(ScramCredentials.Sha512, tampered: false);
            await Exchange(ScramCredentials.Sha256, tampered: true);
            using var client = new TcpClient();
            using var tls = await OpenTls(client);
            await Plain(tls, "\0smoke\0plain-smoke");
            using var reader = new TcpClient();
            using var restricted = await OpenTls(reader);
            await Plain(restricted, "\0reader\0reader-smoke");
            using var legacyClient = new TcpClient();
            using var legacy = await OpenTls(legacyClient);
            await Select(legacy, "PLAIN", 0);
            using (var token = new WireWriter(256))
            {
                token.Bytes("\0reader\0reader-smoke"u8);
                await legacy.WriteAsync(token.Memory, stop.Token);
            }
            byte[] legacyResponse = new byte[4];
            await legacy.ReadExactlyAsync(legacyResponse, stop.Token);
            Check(BinaryPrimitives.ReadInt32BigEndian(legacyResponse) == 0, "SaslHandshake v0 returns a raw length-prefixed token");
            var legacyMetadata = new WireReader(await SendFrame(legacy, Api.Metadata, 0, w => w.I32(0)));
            Check(legacyMetadata.Count() == 1, "raw SASL authentication allows Kafka requests");
            Check(catalog.Create("events", 1, false) == Error.None, "seed ACL test topic");
            await CheckAclAndConfig(tls, restricted);
            await CheckStandaloneLogStart(tls, restricted);
            const string original = "original-smoke-secret", rotated = "rotated-smoke-secret";
            Check(await ChangeScram(restricted, "dynamic-user", 1, original) == Error.ClusterAuthorizationFailed &&
                await ChangeScram(tls, "smoke,user", 1, original) == Error.UnacceptableCredential &&
                await ChangeScram(tls, "dynamic-user", 1, original, duplicate: true) == Error.DuplicateResource &&
                (await DescribeScram(tls, "dynamic-user")).Error == Error.ResourceNotFound &&
                await ChangeScram(tls, "dynamic-user", 1, original) == Error.None &&
                await ChangeScram(tls, "dynamic-user", 2, rotated) == Error.None,
                "authorized SCRAM upserts respect duplicate, missing and file credential boundaries");
            Check((await DescribeScram(tls, "dynamic-user")) is (Error.None, 2) &&
                await Login(original, ScramCredentials.Sha256) == Error.None &&
                await Login(rotated, ScramCredentials.Sha512) == Error.None, "both configured dynamic mechanisms authenticate");
            Check(await Login(original, ScramCredentials.Sha256, async () =>
                Check(await ChangeScram(tls, "dynamic-user", 1, rotated) == Error.None, "rotate while a SCRAM challenge is in flight")) ==
                Error.SaslAuthenticationFailed && await Login(original, ScramCredentials.Sha256) == Error.SaslAuthenticationFailed &&
                await Login(rotated, ScramCredentials.Sha256) == Error.None,
                "SCRAM rejects old passwords and unfinished proofs after rotation");
            Check(await ChangeScram(tls, "dynamic-user", 1, null) == Error.None &&
                await ChangeScram(tls, "dynamic-user", 1, null) == Error.ResourceNotFound &&
                await Login(rotated, ScramCredentials.Sha256) == Error.SaslAuthenticationFailed &&
                await Login(rotated, ScramCredentials.Sha512) == Error.None, "SCRAM deletion applies to the selected mechanism");
            await CheckClientQuotas(tls, restricted);
            var ip = new QuotaComponent("ip", "::ffff:127.0.0.1");
            Check(await ChangeQuota(tls, [ip], [new(ClientQuotas.ConnectionRate, 0)]) == Error.None &&
                (await ReadQuotas(tls, new("ip", "127.0.0.1"))) is (Error.None, 1), "IP quota normalizes mapped IPv4 addresses");
            using (var blocked = new TcpClient())
            using (var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(4)))
            {
                try
                {
                    await blocked.ConnectAsync(endpoint, deadline.Token);
                    Check(await blocked.GetStream().ReadAsync(new byte[1], deadline.Token) == 0,
                        "a zero IP quota closes a new connection before TLS");
                }
                catch (IOException e) when (e.InnerException is SocketException { SocketErrorCode: SocketError.ConnectionReset }) { }
            }
            Check(await ChangeQuota(tls, [ip], [new(ClientQuotas.ConnectionRate, null)]) == Error.None &&
                await Login(rotated, ScramCredentials.Sha512) == Error.None,
                "existing connections retain admin access and removing the IP quota admits new connections");
            var meter = new ClientQuotaLimiter.Meter();
            Check(meter.AdmitConnection(1) == 0 && meter.AdmitConnection(1) > 0, "positive IP quota admits and defers at its configured rate");
            Console.WriteLine("Kafka TLS passed SCRAM, ACL enforcement, client quotas and standalone log truncation");
        }
        finally { await stop.CancelAsync(); await serve; }
    }
    string startPath;
    using (var reopened = new Catalog(secured))
    {
        Check(reopened.Acls.Length == 1 && reopened.Acls[0].Permission == AclPermission.Deny &&
            reopened.Configurations.TopicLimit(reopened.Get("events")!.Id, BrokerOptions.NodeId) == 64,
            "ACLs, revocations and topic config persist across catalog restart");
        Check(reopened.Scram.Find("dynamic-user", 1) is null && reopened.Scram.Find("dynamic-user", 2) is not null,
            "SCRAM credential and deletion persist across catalog restart");
        var topic = reopened.Get("events")!;
        var log = topic.Partitions[0];
        startPath = Path.Combine(topic.Directory, "0.log.start");
        Check(reopened.Quotas.Resolve("smoke", "smoke", ClientQuotas.ProducerBytes).Rate == 1024,
            "client quota survives catalog restart");
        Check(log.OffsetFor(-2).Offset == 4 && log.Select(3, 4096, true).Error == Error.OffsetOutOfRange &&
            log.AdvanceLogStart(-1) == (Error.None, 6L) &&
            await log.AppendAsync(Batch(-1, -1, 900, "appended-after-restart"u8), 0) == new AppendResult(Error.None, 6),
            "standalone log start survives restart and appends preserve monotonic offsets");
    }
    byte[] marker = await File.ReadAllBytesAsync(startPath);
    marker[^1] ^= 1;
    await File.WriteAllBytesAsync(startPath, marker);
    try
    {
        using var corrupted = new Catalog(secured);
        throw new InvalidOperationException("broker opened a corrupt log start marker");
    }
    catch (InvalidDataException e) when (e.Message == "Invalid partition log start record") { }

    async Task<Error> Login(string secret, string mechanism, Func<Task>? beforeProof = null)
    {
        using var peer = new TcpClient();
        using var tls = await OpenTls(peer);
        return await ScramLogin(tls, "dynamic-user", mechanism, Encoding.UTF8.GetBytes(secret), beforeProof: beforeProof);
    }

    static async Task Plain(SslStream tls, string value)
    {
        await Select(tls, "PLAIN");
        var allowed = new WireReader(await SendFrame(tls, Api.SaslAuthenticate, 2, w =>
        { w.Bytes(Encoding.UTF8.GetBytes(value), true); w.UVar(0); }));
        Check(allowed.I16() == 0 && allowed.NullableText(true) is null && allowed.ArrayLength(true) == 0 && allowed.I64() == 0,
            "flexible SaslAuthenticate v2 over TLS");
        allowed.Tags(); allowed.End();
    }

    async Task<SslStream> OpenTls(TcpClient peer)
    {
        await peer.ConnectAsync(endpoint, stop.Token);
        var tls = new SslStream(peer.GetStream());
        var chain = new X509ChainPolicy { TrustMode = X509ChainTrustMode.CustomRootTrust, RevocationMode = X509RevocationMode.NoCheck };
        chain.CustomTrustStore.Add(authority);
        try
        {
            await tls.AuthenticateAsClientAsync(new SslClientAuthenticationOptions { TargetHost = "simdkafka-raft", CertificateChainPolicy = chain }, stop.Token);
            return tls;
        }
        catch { tls.Dispose(); throw; }
    }

    static async Task Select(SslStream tls, string mechanism, short version = 1)
    {
        var selected = new WireReader(await SendFrame(tls, Api.SaslHandshake, version, w => w.String(mechanism)));
        Check(selected.I16() == 0 && selected.Count() == 3 && selected.String() == "PLAIN" &&
            selected.String() == ScramCredentials.Sha256 && selected.String() == ScramCredentials.Sha512, "Kafka SASL mechanisms");
    }

    async Task Exchange(string mechanism, bool tampered)
    {
        using var client = new TcpClient();
        using var tls = await OpenTls(client);
        Error error = await ScramLogin(tls, "smoke,user", mechanism, password, tampered: tampered, expectedSalt: salt);
        Check(error == (tampered ? Error.SaslAuthenticationFailed : Error.None), "file-based Kafka SCRAM proof validation");
        if (tampered) return;
        var metadata = new WireReader(await SendFrame(tls, Api.Metadata, 0, w => w.I32(0)));
        Check(metadata.Count() == 1, "Kafka metadata is accessible after SCRAM");
    }
}

static async Task<Error> ScramLogin(Stream tls, string user, string mechanism, byte[] password, bool tampered = false,
    byte[]? expectedSalt = null, Func<Task>? beforeProof = null, bool delegation = false)
{
        var selected = new WireReader(await SendFrame(tls, Api.SaslHandshake, 1, w => w.String(mechanism)));
        Check(selected.I16() == 0, "select Kafka SCRAM mechanism");
        string first = "n=" + user.Replace("=", "=3D", StringComparison.Ordinal).Replace(",", "=2C", StringComparison.Ordinal) + ",r=smokeNonce";
        if (delegation) first += ",tokenauth=true";
        var challenge = new WireReader(await SendFrame(tls, Api.SaslAuthenticate, 1, w => w.Bytes(Encoding.UTF8.GetBytes("n,," + first))));
        Check(challenge.I16() == 0 && challenge.NullableString() is null, "Kafka SCRAM challenge accepted");
        string serverFirst = Encoding.UTF8.GetString(challenge.Bytes());
        string[] parts = serverFirst.Split(',');
        Check(parts.Length == 3 && parts[0].StartsWith("r=smokeNonce", StringComparison.Ordinal) &&
            parts[0].Length > "r=smokeNonce".Length && parts[1].StartsWith("s=", StringComparison.Ordinal) &&
            parts[2].StartsWith("i=", StringComparison.Ordinal),
            "SCRAM challenge produces a fresh nonce");
        byte[] salt = Convert.FromBase64String(parts[1][2..]);
        if (expectedSalt is not null) Check(salt.SequenceEqual(expectedSalt), "bootstrap SCRAM uses the configured salt");
        bool sha256 = mechanism == ScramCredentials.Sha256;
        byte[] salted = Rfc2898DeriveBytes.Pbkdf2(password, salt, int.Parse(parts[2][2..]), sha256 ? HashAlgorithmName.SHA256 : HashAlgorithmName.SHA512,
            sha256 ? 32 : 64);
        byte[] clientKey = Hmac(sha256, salted, "Client Key"u8);
        byte[] stored = sha256 ? SHA256.HashData(clientKey) : SHA512.HashData(clientKey);
        string final = "c=biws," + parts[0];
        byte[] message = Encoding.UTF8.GetBytes(string.Concat(first, ",", serverFirst, ",", final));
        byte[] proof = Hmac(sha256, stored, message);
        for (int i = 0; i < proof.Length; i++) proof[i] ^= clientKey[i];
        if (tampered) proof[0] ^= 1;
        if (beforeProof is not null) await beforeProof();
        var result = new WireReader(await SendFrame(tls, Api.SaslAuthenticate, 1, w =>
            w.Bytes(Encoding.UTF8.GetBytes(final + ",p=" + Convert.ToBase64String(proof)))));
        Error error = (Error)result.I16();
        if (error == Error.None)
            Check(result.NullableString() is null && Encoding.UTF8.GetString(result.Bytes()) ==
                "v=" + Convert.ToBase64String(Hmac(sha256, Hmac(sha256, salted, "Server Key"u8), message)), "SCRAM server signature");
        return error;
}

static byte[] Hmac(bool sha256, byte[] key, ReadOnlySpan<byte> message) =>
    sha256 ? HMACSHA256.HashData(key, message) : HMACSHA512.HashData(key, message);

async Task OAuthTokenSmoke()
{
    var root = Path.Combine(dir, "oauth-token");
    Directory.CreateDirectory(root);
    var (ca, certificates) = CreateClusterCertificates(root);
    using var authority = X509CertificateLoader.LoadCertificateFromFile(ca);
    using var signer = RSA.Create(2048);
    using var wrongKey = RSA.Create(2048);
    var key = signer.ExportParameters(false);
    string config = Path.Combine(root, "oauth.json"), jwks = Path.Combine(root, "jwks.json"), secret = Path.Combine(root, "token.key");
    await File.WriteAllTextAsync(config, """{"issuer":"urn:simdkafka","audience":"smoke","jwksFile":"jwks.json"}""");
    await File.WriteAllBytesAsync(jwks, JsonSerializer.SerializeToUtf8Bytes(new { keys = new[] { new
    {
        kty = "RSA", alg = "RS256", use = "sig", kid = "smoke", n = Base64Url.EncodeToString(key.Modulus!), e = Base64Url.EncodeToString(key.Exponent!)
    } } }));
    await File.WriteAllTextAsync(secret, Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)) + "\n");
    var ep = ClusterSmoke.ReserveAddresses(1).Single();
    var settings = BrokerOptions.Parse(["--listen", ep.ToString(), "--tls-pfx", certificates[0], "--sasl-oauth-config", config,
        "--delegation-token-secret-file", secret, "--acl-super-user", "User:operator", "--data", Path.Combine(root, "data")]);
    using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(45));
    (string Id, byte[] Hmac) issued = ("", []);
    await Run(async catalog =>
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        foreach (var jwt in new[]
        {
            Jwt(wrongKey, "smoke", now + 120), Jwt(signer, "elsewhere", now + 120), Jwt(signer, "smoke", now - 1),
            Jwt(signer, "smoke", now + 120, issuer: "urn:untrusted"), Jwt(signer, "smoke", now + 120, notBefore: now + 20)
        })
        {
            using var invalid = await Open();
            Check(await Login(invalid, jwt) == Error.SaslAuthenticationFailed, "OAuth rejects untrusted signature, audience, expiry, issuer and not-before");
        }
        using var admin = await Open();
        Check(await Login(admin, Jwt(signer, "smoke", now + 120)) == Error.None, "OAuth bearer RSA JWT authenticates");
        Check(await Login(admin, Jwt(signer, "smoke", now + 180)) == Error.None, "OAuth renews the same identity on the existing Kafka connection");
        using (var identitySwitch = await Open())
        {
            Check(await Login(identitySwitch, Jwt(signer, "smoke", now + 120)) == Error.None &&
                await Login(identitySwitch, Jwt(signer, "smoke", now + 180, subject: "other")) == Error.SaslAuthenticationFailed,
                "OAuth reauthentication cannot switch principals");
        }
        Check(catalog.Create("oauth-events", 1, false) == Error.None, "create OAuth test topic");
        var invalidOwner = new WireReader(await SendFrame(admin, Api.CreateDelegationToken, 3, w =>
        {
            w.Text("User:operator", true); w.Text("other", true); w.ArrayLength(0, true); w.I64(60_000); w.UVar(0);
        }));
        Check(invalidOwner.I16() == (short)Error.InvalidPrincipalType, "token owner type and name are validated separately");
        issued = await IssueToken(admin, "operator", renewer: "renewer");
        using var borrower = await Open();
        Check(await ScramLogin(borrower, issued.Id, ScramCredentials.Sha512, Encoding.UTF8.GetBytes(Convert.ToBase64String(issued.Hmac)),
            delegation: true) == Error.None, "delegation authenticates using SCRAM-SHA-512 and token HMAC");
        var described = new WireReader(await SendFrame(borrower, Api.DescribeDelegationToken, 3, w => { w.ArrayLength(-1, true); w.UVar(0); }));
        Check(described.I16() == (short)Error.DelegationTokenRequestNotAllowed, "a delegated connection cannot manage other tokens");
        Check(await ChangeToken(admin, Api.RenewDelegationToken, issued.Hmac, 30_000) == Error.None, "renew an active token");
    });
    await Run(async catalog =>
    {
        Check(catalog.Tokens.Entries.ContainsKey(issued.Id), "single broker loads the token after restart");
        using var admin = await Open();
        Check(await Login(admin, Jwt(signer, "smoke", DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 120)) == Error.None, "broker accepts OAuth after restart");
        byte[] password = Encoding.UTF8.GetBytes(Convert.ToBase64String(issued.Hmac));
        using var borrower = await Open();
        Check(await ScramLogin(borrower, issued.Id, ScramCredentials.Sha256, password, delegation: true) == Error.None,
            "the same token can authenticate with SCRAM-SHA-256 after restart");
        using var inFlight = await Open();
        Check(await ScramLogin(inFlight, issued.Id, ScramCredentials.Sha256, password, delegation: true, beforeProof: async () =>
        {
            Check(await ChangeToken(admin, Api.ExpireDelegationToken, issued.Hmac, -1) == Error.None, "revoke delegated credentials");
        }) == Error.SaslAuthenticationFailed, "revocation invalidates an in-flight SCRAM proof");
        try
        {
            await SendFrame(borrower, Api.Metadata, 0, w => w.I32(0));
            throw new InvalidOperationException("revoked token keeps access on an existing connection");
        }
        catch (IOException) { }
    });
    Console.WriteLine("Kafka OAuth and delegation tokens passed signature and claim checks, SCRAM, restart and revocation");

    string Jwt(RSA key, string audience, long expires, string issuer = "urn:simdkafka", long notBefore = 0,
        string subject = "operator")
    {
        string header = Base64Url.EncodeToString("""{"alg":"RS256","kid":"smoke"}"""u8);
        string claims = Base64Url.EncodeToString(JsonSerializer.SerializeToUtf8Bytes(new { iss = issuer, aud = new[] { audience },
            sub = subject, exp = expires, nbf = notBefore }));
        string prefix = header + "." + claims;
        return prefix + "." + Base64Url.EncodeToString(key.SignData(Encoding.ASCII.GetBytes(prefix),
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));
    }

    async Task Run(Func<Catalog, Task> exercise)
    {
        using var turn = CancellationTokenSource.CreateLinkedTokenSource(stop.Token);
        using var catalog = new Catalog(settings);
        Task server = new Broker(settings, catalog).RunAsync(turn.Token);
        try { await exercise(catalog); }
        finally { await turn.CancelAsync(); await server; }
    }

    async Task<SslStream> Open()
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        try { await socket.ConnectAsync(ep, stop.Token); }
        catch { socket.Dispose(); throw; }
        var tls = new SslStream(new NetworkStream(socket, ownsSocket: true));
        try
        {
            var policy = new X509ChainPolicy { TrustMode = X509ChainTrustMode.CustomRootTrust, RevocationMode = X509RevocationMode.NoCheck };
            policy.CustomTrustStore.Add(authority);
            await tls.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            { TargetHost = "127.0.0.1", CertificateChainPolicy = policy }, stop.Token);
            return tls;
        }
        catch { tls.Dispose(); throw; }
    }

    static async Task<Error> Login(Stream stream, string jwt)
    {
        var selected = new WireReader(await SendFrame(stream, Api.SaslHandshake, 1, w => w.String("OAUTHBEARER")));
        Check(selected.I16() == 0, "select Kafka OAUTHBEARER");
        var result = new WireReader(await SendFrame(stream, Api.SaslAuthenticate, 2, w =>
        {
            w.Bytes(Encoding.UTF8.GetBytes("n,,\u0001auth=Bearer " + jwt + "\u0001\u0001"), true);
            w.UVar(0);
        }));
        var error = (Error)result.I16();
        if (error == Error.None)
            Check(result.NullableText(true) is null && result.ArrayLength(true) == 0 && result.I64() > 0,
                "OAuth framed auth advertises token-based session lifetime");
        return error;
    }
}

static async Task<(string Id, byte[] Hmac)> IssueToken(Stream peer, string owner, string? renewer = null)
{
    var r = new WireReader(await SendFrame(peer, Api.CreateDelegationToken, 3, w =>
    {
        w.Text("User", true); w.Text(owner, true); w.ArrayLength(renewer is null ? 0 : 1, true);
        if (renewer is not null) { w.Text("User", true); w.Text(renewer, true); w.UVar(0); }
        w.I64(300_000); w.UVar(0);
    }));
    Check(r.I16() == 0 && r.Text(true) == "User" && r.Text(true) == owner, "create Kafka delegation token for expected owner");
    Check(r.Text(true) == "User" && r.Text(true).Length > 0, "creation returns the authenticated requester");
    long issued = r.I64(), expiry = r.I64(), maximum = r.I64();
    string id = r.Text(true);
    byte[] hmac = r.Take(r.ArrayLength(true)).ToArray();
    Check(issued > 0 && expiry == maximum && maximum - issued == 300_000 && hmac.Length == 64, "Kafka delegation token lifetime and HMAC");
    return (id, hmac);
}

static async Task<Error> ChangeToken(Stream peer, Api api, byte[] hmac, long period)
{
    var r = new WireReader(await SendFrame(peer, api, 2, w => { w.Bytes(hmac, true); w.I64(period); w.UVar(0); }));
    return (Error)r.I16();
}

static async Task<Error> ChangeScram(Stream peer, string user, sbyte mechanism, string? secret, bool duplicate = false)
{
    var r = new WireReader(await SendFrame(peer, Api.AlterUserScramCredentials, 0, w =>
    {
        w.ArrayLength(secret is null ? 1 : 0, true);
        if (secret is null) { w.Text(user, true); w.I8(mechanism); w.UVar(0); }
        else
        {
            byte[] salt = RandomNumberGenerator.GetBytes(24);
            bool sha256 = mechanism == 1;
            byte[] salted = Rfc2898DeriveBytes.Pbkdf2(secret, salt, 8192, sha256 ? HashAlgorithmName.SHA256 : HashAlgorithmName.SHA512,
                sha256 ? 32 : 64);
            w.ArrayLength(duplicate ? 2 : 1, true);
            Write();
            if (duplicate) Write();
            void Write()
            {
                w.Text(user, true); w.I8(mechanism); w.I32(8192); w.Bytes(salt, true); w.Bytes(salted, true); w.UVar(0);
            }
        }
        if (secret is null) w.ArrayLength(0, true);
        w.UVar(0);
    }));
    Check(r.I32() == 0 && r.ArrayLength(true) == 1 && r.Text(true) == user, "SCRAM alteration response");
    return (Error)r.I16();
}

static async Task<(Error Error, int Mechanisms)> DescribeScram(Stream peer, string user)
{
    var r = new WireReader(await SendFrame(peer, Api.DescribeUserScramCredentials, 0, w =>
    { w.ArrayLength(1, true); w.Text(user, true); w.UVar(0); w.UVar(0); }));
    Check(r.I32() == 0, "SCRAM description throttle");
    Error top = (Error)r.I16();
    if (top != Error.None) return (top, 0);
    Check(r.NullableText(true) is null && r.ArrayLength(true) == 1 && r.Text(true) == user, "SCRAM description response");
    Error row = (Error)r.I16();
    r.NullableText(true);
    return (row, r.ArrayLength(true));
}

static async Task CheckStandaloneLogStart(Stream admin, Stream restricted)
{
    byte[] pair = BatchWithOlderSecondRecord();
    Check(await ChangeMessageLimit(admin, null) == Error.None &&
        Appended(await SendFrame(admin, Api.Produce, 8, w => ProduceRequest(w, pair)), 3), "append a multi-record batch to trim midway");
    Check(await TrimRecords(restricted, 4) == (Error.TopicAuthorizationFailed, -1L) &&
        await TrimRecords(admin, 6) == (Error.OffsetOutOfRange, -1L) &&
        await TrimRecords(admin, 4) == (Error.None, 4L) && await TrimRecords(admin, 3) == (Error.None, 4L),
        "standalone DeleteRecords enforces access, bounds and monotonic progress");
    Check(await Position(-2) == (4L, -1L) && await Position(650) == (-1L, -1L) && await Position(600) == (4L, 600L) &&
        await Position(-3, 8) == (4L, 600L) && await Position(-4, 8) == (4L, -1L),
        "standalone timestamp lookup excludes trimmed records inside a retained batch");
    Check(await QueryEpoch(0, 0) == (Error.None, 0, 5L) &&
        await QueryEpoch(0, 1) == (Error.None, -1, -1L) && await QueryEpoch(1, 0) == (Error.UnknownLeaderEpoch, -1, -1L),
        "standalone OffsetForLeaderEpoch reports the retained epoch and rejects newer current epochs");
    var dirs = new WireReader(await SendFrame(admin, Api.DescribeLogDirs, 4, w => { w.ArrayLength(-1, true); w.UVar(0); }));
    Check(dirs.I32() == 0 && dirs.I16() == 0 && dirs.ArrayLength(true) == 1 && dirs.I16() == 0 &&
        Path.IsPathFullyQualified(dirs.Text(true)) && dirs.ArrayLength(true) == 1 && dirs.Text(true) == "events" &&
        dirs.ArrayLength(true) == 1 && dirs.I32() == 0 && dirs.I64() > pair.Length && dirs.I64() == 0 && !dirs.Bool(),
        "standalone DescribeLogDirs returns the actual committed byte size including retained log storage");
    var deniedDirs = new WireReader(await SendFrame(restricted, Api.DescribeLogDirs, 4,
        w => { w.ArrayLength(-1, true); w.UVar(0); }));
    Check(deniedDirs.I32() == 0 && deniedDirs.I16() == (short)Error.ClusterAuthorizationFailed && deniedDirs.ArrayLength(true) == 0,
        "DescribeLogDirs enforces cluster access without exposing paths");
    var reply = new WireReader(await SendFrame(admin, Api.Fetch, 5, w => FetchRequest(w, 3, 0)));
    Check(reply.I32() == 0 && reply.Count() == 1 && reply.String() == "events" && reply.Count() == 1 && reply.I32() == 0 &&
        reply.I16() == (short)Error.OffsetOutOfRange && reply.I64() == 5 && reply.I64() == 5 && reply.I64() == 4,
        "standalone Fetch returns the durable start and excludes old offsets");
    Check(await ChangeMessageLimit(admin, "64") == Error.None, "retain config for restart assertions");

    async Task<(long Offset, long Timestamp)> Position(long time, short version = 1)
    {
        bool compact = version >= 6;
        var r = new WireReader(await SendFrame(admin, Api.ListOffsets, version, w =>
        {
            w.I32(-1);
            if (version >= 2) w.I8(0);
            w.ArrayLength(1, compact); w.Text("events", compact); w.ArrayLength(1, compact); w.I32(0);
            if (version >= 4) w.I32(-1);
            w.I64(time); w.EndStruct(compact); w.EndStruct(compact); w.EndStruct(compact);
        }));
        if (version >= 2) Check(r.I32() == 0, "ListOffsets throttle");
        Check(r.ArrayLength(compact) == 1 && r.Text(compact) == "events" && r.ArrayLength(compact) == 1 && r.I32() == 0 && r.I16() == 0,
            "standalone ListOffsets response shape");
        long timestamp = r.I64();
        return (r.I64(), timestamp);
    }

    async Task<(Error Error, int Epoch, long Offset)> QueryEpoch(int current, int requested)
    {
        var r = new WireReader(await SendFrame(admin, Api.OffsetForLeaderEpoch, 4, w =>
        {
            w.I32(-1); w.ArrayLength(1, true); w.Text("events", true); w.ArrayLength(1, true);
            w.I32(0); w.I32(current); w.I32(requested); w.UVar(0); w.UVar(0); w.UVar(0);
        }));
        Check(r.I32() == 0 && r.ArrayLength(true) == 1 && r.Text(true) == "events" && r.ArrayLength(true) == 1,
            "standalone OffsetForLeaderEpoch response shape");
        Error error = (Error)r.I16();
        Check(r.I32() == 0, "epoch response partition index");
        return (error, r.I32(), r.I64());
    }
}

static async Task CheckClientQuotas(Stream admin, Stream restricted)
{
    var user = new QuotaComponent("user", "smoke");
    var client = new QuotaComponent("client-id", "smoke");
    var rule = new QuotaChange(ClientQuotas.ProducerBytes, 1024);
    Check(await ChangeQuota(restricted, [user], [rule]) == Error.ClusterAuthorizationFailed &&
        await ChangeQuota(admin, [user], [rule], validate: true) == Error.None &&
        (await ReadQuotas(admin, user)) is (Error.None, 0), "quota authorization and validate-only");
    Check(await ChangeQuota(admin, [user], [rule, new(ClientQuotas.ConsumerBytes, 1024)]) == Error.None &&
        await ChangeQuota(admin, [user, client], [new(ClientQuotas.ProducerBytes, 8192)]) == Error.None &&
        (await ReadQuotas(admin, user)) is (Error.None, 1), "strict quota filters exclude combined entities");
    Check(await ChangeMessageLimit(admin, "4096") == Error.None, "allow record larger than quota burst");
    var data = Batch(-1, -1, 800, new byte[1120]);
    Check(await ChangeQuota(admin, [client, user], [new(ClientQuotas.ProducerBytes, null)]) == Error.None,
        "quota operations match combined entities regardless of component order");
    var produced = new WireReader(await SendFrame(admin, Api.Produce, 8, w => ProduceRequest(w, data)));
    Check(produced.Count() == 1 && produced.String() == "events" && produced.Count() == 1 && produced.I32() == 0 &&
        produced.I16() == 0 && produced.I64() == 5, "quota throttling still acknowledges a durable write");
    produced.I64(); produced.I64(); produced.Count(); produced.NullableString();
    Check(produced.I32() > 0, "fallback producer quota advertises a throttle time");
    var fetched = new WireReader(await SendFrame(admin, Api.Fetch, 5, w => FetchRequest(w, 5, 0)));
    Check(fetched.I32() > 0 && fetched.Count() == 1 && fetched.String() == "events" && fetched.Count() == 1 &&
        fetched.I32() == 0 && fetched.I16() == 0, "consumer quota advertises throttling on a successful read");
    Check(await ChangeMessageLimit(admin, "64") == Error.None, "restore independently configured max batch size");
}

static async Task<Error> ChangeQuota(Stream peer, QuotaComponent[] entity, QuotaChange[] changes, bool validate = false)
{
    var r = new WireReader(await SendFrame(peer, Api.AlterClientQuotas, 1, w =>
    {
        w.ArrayLength(1, true); w.ArrayLength(entity.Length, true);
        foreach (var item in entity) { w.Text(item.Type, true); w.Text(item.Name, true); w.UVar(0); }
        w.ArrayLength(changes.Length, true);
        foreach (var item in changes)
        {
            w.Text(item.Key, true); w.I64(BitConverter.DoubleToInt64Bits(item.Value ?? 0));
            w.Bool(item.Value is null); w.UVar(0);
        }
        w.UVar(0); w.Bool(validate); w.UVar(0);
    }));
    Check(r.I32() == 0 && r.ArrayLength(true) == 1, "AlterClientQuotas response shape");
    return (Error)r.I16();
}

static async Task<(Error Error, int Entries)> ReadQuotas(Stream peer, QuotaComponent filter)
{
    var r = new WireReader(await SendFrame(peer, Api.DescribeClientQuotas, 1, w =>
    {
        w.ArrayLength(1, true); w.Text(filter.Type, true); w.I8(filter.Name is null ? (sbyte)1 : (sbyte)0);
        w.Text(filter.Name, true); w.UVar(0); w.Bool(true); w.UVar(0);
    }));
    Check(r.I32() == 0, "DescribeClientQuotas response shape");
    Error error = (Error)r.I16();
    r.NullableText(true);
    return (error, r.ArrayLength(true, nullable: true));
}

static async Task<(Error Error, long Watermark)> TrimRecords(Stream peer, long offset)
{
    var result = new WireReader(await SendFrame(peer, Api.DeleteRecords, 2, w =>
    {
        w.ArrayLength(1, true); w.Text("events", true); w.ArrayLength(1, true);
        w.I32(0); w.I64(offset); w.UVar(0); w.UVar(0); w.I32(5000); w.UVar(0);
    }));
    Check(result.I32() == 0 && result.ArrayLength(true) == 1 && result.Text(true) == "events" &&
        result.ArrayLength(true) == 1 && result.I32() == 0, "DeleteRecords v2 response shape");
    long watermark = result.I64();
    return ((Error)result.I16(), watermark);
}

static byte[] BatchWithOlderSecondRecord()
{
    byte[] first = Batch(-1, -1, 700, "retired-record"u8);
    using var pair = new WireWriter(1024);
    first.CopyTo(pair.Reserve(first.Length));
    pair.PatchI32(23, 1);
    pair.PatchI32(57, 2);
    var value = "retained-record"u8;
    pair.UVar((uint)(7 + value.Length) * 2);
    pair.I8(0); pair.UVar(199); pair.UVar(2); pair.UVar(1);
    pair.UVar((uint)value.Length * 2); value.CopyTo(pair.Reserve(value.Length)); pair.UVar(0);
    pair.PatchI32(8, pair.Length - 12);
    pair.PatchI32(17, unchecked((int)RecordBatch.Crc32C(pair.Memory.Span[21..])));
    return pair.Memory.ToArray();
}

static async Task CheckAclAndConfig(Stream admin, Stream restricted)
{
    byte[] data = Batch(-1, -1, 100, "authorized-record"u8);
    Check(Appended(await SendFrame(admin, Api.Produce, 8, w => ProduceRequest(w, data)), 0), "authorized produce");
    Check(await ReadError(restricted) == Error.TopicAuthorizationFailed, "default-deny fetch");
    Check(await GrantRead(restricted, "User:reader", "events", AclPattern.Literal, AclPermission.Allow) == Error.ClusterAuthorizationFailed,
        "an ordinary principal cannot grant an ACL");
    Check(await GrantRead(admin, "User:reader", "ev", AclPattern.Prefixed, AclPermission.Allow) == Error.None &&
        Fetched(await SendFrame(restricted, Api.Fetch, 5, w => FetchRequest(w, 0, 0)), 0, 1, data),
        "literal IP and prefixed ACL allow an existing connection to read");
    Check(Appended(await SendFrame(restricted, Api.Produce, 8, w => ProduceRequest(w, data)), -1, Error.TopicAuthorizationFailed),
        "topic READ does not grant WRITE");
    Check(await GrantRead(admin, "User:reader", "events", AclPattern.Literal, AclPermission.Deny) == Error.None &&
        await ReadError(restricted) == Error.TopicAuthorizationFailed, "literal DENY overrides prefixed ALLOW");

    var described = new WireReader(await SendFrame(admin, Api.DescribeAcls, 3, w => Filter(w, AclPermission.Any)));
    Check(described.I32() == 0 && described.I16() == 0 && described.NullableText(true) is null &&
        described.ArrayLength(true) == 2, "ACL MATCH filter returns both literal and prefixed entries");
    var removed = new WireReader(await SendFrame(admin, Api.DeleteAcls, 3, w =>
    { w.ArrayLength(1, true); Filter(w, AclPermission.Allow); w.UVar(0); }));
    Check(removed.I32() == 0 && removed.ArrayLength(true) == 1 && removed.I16() == 0 && removed.NullableText(true) is null &&
        removed.ArrayLength(true) == 1, "ACL deletion returns the removed binding");

    Check(await ChangeMessageLimit(restricted, "64") == Error.TopicAuthorizationFailed &&
        await ChangeMessageLimit(admin, "64", validate: true) == Error.None &&
        Appended(await SendFrame(admin, Api.Produce, 8, w => ProduceRequest(w, data)), 1),
        "config authorization and validation without mutation");
    Check(await ChangeMessageLimit(admin, "64") == Error.None &&
        Appended(await SendFrame(admin, Api.Produce, 8, w => ProduceRequest(w, data)), -1, Error.MessageTooLarge) &&
        Fetched(await SendFrame(admin, Api.Fetch, 5, w => FetchRequest(w, 1, 0)), 1, 2, data),
        "new batch limit is enforced and old data remains readable");
    Check(await ChangeMessageLimit(admin, "8388609") == Error.InvalidConfig &&
        Appended(await SendFrame(admin, Api.Produce, 8, w => ProduceRequest(w, data)), -1, Error.MessageTooLarge),
        "invalid config leaves the active limit intact");
    Check(await ChangeMessageLimit(admin, null) == Error.None &&
        Appended(await SendFrame(admin, Api.Produce, 8, w => ProduceRequest(w, data)), 2) &&
        await ChangeMessageLimit(admin, "64") == Error.None, "deleting the override restores inherited limit");
    Check(await ChangeMessageLimit(admin, "4096", type: KafkaConfigurations.Broker, name: "") == Error.None,
        "persist a broker-default limit");
    var resources = new WireReader(await SendFrame(admin, Api.ListConfigResources, 1, w =>
    { w.ArrayLength(0, true); w.UVar(0); }));
    Check(resources.I32() == 0 && resources.I16() == 0 && resources.ArrayLength(true) == 2 &&
        resources.Text(true) == "events" && resources.I8() == KafkaConfigurations.Topic, "list explicitly configured topic");
    resources.Tags();
    Check(resources.Text(true) == "" && resources.I8() == KafkaConfigurations.Broker, "list explicitly configured default broker");
    resources.Tags(); resources.Tags(); resources.End();
    var unauthorized = new WireReader(await SendFrame(restricted, Api.ListConfigResources, 1, w =>
    { w.ArrayLength(0, true); w.UVar(0); }));
    Check(unauthorized.I32() == 0 && unauthorized.I16() == (short)Error.ClusterAuthorizationFailed &&
        unauthorized.ArrayLength(true) == 0, "configuration inventory requires cluster authorization");

    static async Task<Error> ReadError(Stream peer)
    {
        var r = new WireReader(await SendFrame(peer, Api.Fetch, 5, w => FetchRequest(w, 0, 0)));
        Check(r.I32() == 0 && r.Count() == 1 && r.String() == "events" && r.Count() == 1 && r.I32() == 0,
            "fetch authorization response structure");
        return (Error)r.I16();
    }

    static void Filter(WireWriter w, AclPermission permission)
    {
        w.I8((sbyte)AclResource.Topic); w.Text("events", true); w.I8((sbyte)AclPattern.Match);
        w.Text("User:reader", true); w.Text(null, true); w.I8((sbyte)AclOperation.Read); w.I8((sbyte)permission); w.UVar(0);
    }
}

static async Task<Error> GrantRead(Stream peer, string principal, string name, AclPattern pattern, AclPermission permission,
    AclResource resource = AclResource.Topic)
{
    var r = new WireReader(await SendFrame(peer, Api.CreateAcls, 3, w =>
    {
        w.ArrayLength(1, true); w.I8((sbyte)resource); w.Text(name, true); w.I8((sbyte)pattern);
        w.Text(principal, true); w.Text("127.0.0.1", true); w.I8((sbyte)AclOperation.Read); w.I8((sbyte)permission);
        w.UVar(0); w.UVar(0);
    }));
    Check(r.I32() == 0 && r.ArrayLength(true) == 1, "CreateAcls response structure");
    return (Error)r.I16();
}

static Task<Error> ChangeMessageLimit(Stream peer, string? value, bool validate = false,
    sbyte type = KafkaConfigurations.Topic, string name = "events") =>
    ChangeConfig(peer, type == KafkaConfigurations.Topic ? KafkaConfigurations.TopicMessageBytes : KafkaConfigurations.BrokerMessageBytes,
        value, validate, type, name);

static async Task<Error> ChangeConfig(Stream peer, string key, string? value, bool validate = false,
    sbyte type = KafkaConfigurations.Topic, string name = "events")
{
    var r = new WireReader(await SendFrame(peer, Api.IncrementalAlterConfigs, 1, w =>
    {
        w.ArrayLength(1, true); w.I8(type); w.Text(name, true); w.ArrayLength(1, true);
        w.Text(key, true);
        w.I8(value is null ? (sbyte)1 : (sbyte)0); w.Text(value, true);
        w.UVar(0); w.UVar(0); w.Bool(validate); w.UVar(0);
    }));
    Check(r.I32() == 0 && r.ArrayLength(true) == 1, "IncrementalAlterConfigs response structure");
    return (Error)r.I16();
}

async Task ClusterSecuritySmoke()
{
    var root = Path.Combine(dir, "secure-cluster");
    Directory.CreateDirectory(root);
    var addresses = ClusterSmoke.ReserveAddresses(12);
    var (ca, certificates) = CreateClusterCertificates(root);
    string config = Path.Combine(root, "cluster.json");
    string plain = Path.Combine(root, "plain.json");
    string tokenSecret = Path.Combine(root, "tokens.key");
    const string dynamicName = "cluster-user", secret = "cluster-scram-smoke";
    const string adminToken = "\0admin\0admin-smoke", readerToken = "\0reader\0reader-smoke";
    const string adminName = "User:admin", readerName = "User:reader";
    await File.WriteAllTextAsync(plain, "{\"admin\":\"admin-smoke\",\"reader\":\"reader-smoke\"}");
    await File.WriteAllTextAsync(tokenSecret, Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)));
    await File.WriteAllBytesAsync(config, JsonSerializer.SerializeToUtf8Bytes(new
    {
        clusterId = Guid.NewGuid(), clientAclRequired = true, dynamicScram = true, delegationTokens = true,
        brokers = Enumerable.Range(0, 3).Select(i => new
        {
            id = i + 1, host = "127.0.0.1", port = addresses[i].Port,
            control = addresses[i + 3].ToString(), firstReplicaPort = addresses[i + 6].Port, replicaPortCount = 1,
            @internal = addresses[i + 9].ToString()
        })
    }));
    using var authority = X509CertificateLoader.LoadCertificateFromFile(ca);
    using var adminIdentity = X509CertificateLoader.LoadPkcs12FromFile(certificates[0], null);
    using var readerIdentity = X509CertificateLoader.LoadPkcs12FromFile(certificates[1], null);
    using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(75));
    var nodes = new List<(BrokerCluster Cluster, ClusterTls Tls, CancellationTokenSource Stop, Task Serving)>();
    (string Id, byte[] Hmac) issued;
    try
    {
        await StartNodes();
        var (leader, follower) = await WaitForController();
        using (var controllerAdmin = await Open(leader, adminIdentity, adminToken))
        using (var admin = await Open(follower, adminIdentity, adminToken))
        using (var reader = await Open(leader, readerIdentity, readerToken))
        {
            var created = new WireReader(await SendFrame(controllerAdmin, Api.CreateTopics, 7, w =>
            {
                w.ArrayLength(1, true); w.Text("events", true); w.I32(1); w.I16(3); w.ArrayLength(0, true);
                w.ArrayLength(0, true); w.UVar(0); w.I32(10000); w.Bool(false); w.UVar(0);
            }));
            Check(created.I32() == 0 && created.ArrayLength(true) == 1 && created.Text(true) == "events" &&
                created.Uuid() != Guid.Empty && created.I16() == 0, "create topic through the controller over mutual TLS");
            var deniedQuorum = new WireReader(await ReadQuorum(reader, 2));
            Check(deniedQuorum.I16() == (short)Error.ClusterAuthorizationFailed, "only cluster Describe users can read quorum state");
            for (short version = 0; version <= 2; version++)
            {
                while (true)
                {
                    var quorum = new WireReader(await ReadQuorum(admin, version));
                    Check(quorum.I16() == 0, "read controller quorum from a follower");
                    if (version == 2) Check(quorum.NullableText(true) is null, "no top-level quorum error message");
                    Check(quorum.ArrayLength(true) == 1 && quorum.Text(true) == "__cluster_metadata" &&
                        quorum.ArrayLength(true) == 1 && quorum.I32() == 0, "valid metadata quorum partition");
                    var status = (Error)quorum.I16();
                    if (status == Error.LeaderNotAvailable)
                    {
                        await Task.Delay(50, deadline.Token);
                        continue;
                    }
                    Check(status == Error.None, "controller quorum partition succeeded");
                    if (version == 2) Check(quorum.NullableText(true) is null, "no quorum partition error message");
                    int observedLeader = quorum.I32();
                    Check(observedLeader is >= 1 and <= 3 && quorum.I32() > 0 && quorum.I64() > 0 &&
                        quorum.ArrayLength(true) == 3, "quorum leader, epoch, commit watermark and voter count");
                    break;
                }
            }
            Check(await MetadataError(reader) == Error.TopicAuthorizationFailed &&
                await GrantRead(admin, readerName, "events", AclPattern.Literal, AclPermission.Allow) == Error.None &&
                await MetadataError(reader) == Error.None, "forwarded ACL grants access on the controller without reconnecting");
            Check(await ChangeMessageLimit(admin, "128") == Error.None &&
                await GrantRead(admin, readerName, "events", AclPattern.Literal, AclPermission.Deny) == Error.None &&
                await MetadataError(reader) == Error.None, "READ deny does not remove an independent DESCRIBE authorization");
            Check(await ChangeScram(admin, dynamicName, 1, secret) == Error.None &&
                (await DescribeScram(controllerAdmin, dynamicName)) is (Error.None, 1), "SCRAM changes forward to the controller");
            Check(await ChangeQuota(admin, [new("user", dynamicName)], [new(ClientQuotas.ProducerBytes, 2048)]) == Error.None &&
                (await ReadQuotas(controllerAdmin, new("user", dynamicName))) is (Error.None, 1), "quota changes forward to the controller");
            issued = await IssueToken(admin, "reader");
            Check(await ChangeToken(controllerAdmin, Api.RenewDelegationToken, issued.Hmac, 60_000) == Error.DelegationTokenOwnerMismatch &&
                await ChangeToken(reader, Api.RenewDelegationToken, issued.Hmac, 180_000) == Error.None,
                "delegation owner can renew the token created on a follower");
            var shareTopic = nodes[leader].Cluster.Metadata.Image.Topics["events"];
            var placement = shareTopic.Partitions[0];
            int dataLeader = placement.LeaderId - 1;
            using var shareBroker = await Open(dataLeader, adminIdentity, adminToken);
            await CheckShareWire(controllerAdmin, shareBroker, shareTopic.Id);
        }
        await StopNodes();
        await StartNodes();
        var coordinator = await WaitForController();
        while (nodes.Any(node => !node.Cluster.Metadata.Image.Topics.TryGetValue("events", out var topic) ||
            node.Cluster.Metadata.Image.Configurations.TopicLimit(topic.Id, 1) != 128 || node.Cluster.Metadata.Image.Acls.Length != 2 ||
            node.Cluster.Metadata.Image.ShareGroups.GetValueOrDefault("secure-share")?.Partitions.Values.Single().Start != 2))
            await Task.Delay(30, deadline.Token);
        Check(nodes.All(node => node.Cluster.Metadata.Image.ShareGroups.GetValueOrDefault("secure-share")?.Partitions.Values
            .Single().Start == 2), "all brokers restore the share cursor after full restart");
        using var reopened = await Open(coordinator.Follower, readerIdentity, readerToken);
        Check(await MetadataError(reopened) == Error.None, "authenticated follower restores replicated ACLs and config after restart");
        using var dynamicPeer = await Open(coordinator.Follower, readerIdentity);
        Check(await ScramLogin(dynamicPeer, dynamicName, ScramCredentials.Sha256, Encoding.UTF8.GetBytes(secret)) == Error.None,
            "a follower can authenticate the replicated SCRAM user after full restart");
        using var adminAgain = await Open(coordinator.Follower, adminIdentity, adminToken);
        using var delegated = await Open(coordinator.Leader, readerIdentity);
        byte[] delegatedPassword = Encoding.UTF8.GetBytes(Convert.ToBase64String(issued.Hmac));
        Check(await ScramLogin(delegated, issued.Id, ScramCredentials.Sha256, delegatedPassword, delegation: true) == Error.None &&
            await MetadataError(delegated) == Error.None, "delegation survives a cluster restart and uses the owner's ACLs");
        Check(await ChangeToken(reopened, Api.ExpireDelegationToken, issued.Hmac, -1) == Error.None,
            "delegation revocation forwards from a follower to the controller");
        using var revoked = await Open(coordinator.Leader, readerIdentity);
        Check(await ScramLogin(revoked, issued.Id, ScramCredentials.Sha256, delegatedPassword, delegation: true) == Error.SaslAuthenticationFailed,
            "the controller rejects a revoked delegation token");
        Check((await ReadQuotas(adminAgain, new("user", dynamicName))) is (Error.None, 1), "client quota survives full cluster restart");
        Check(await ChangeScram(adminAgain, dynamicName, 1, null) == Error.None, "delete a replicated SCRAM credential through a follower");
        using var deletedPeer = await Open(coordinator.Leader, readerIdentity);
        Check(await ScramLogin(deletedPeer, dynamicName, ScramCredentials.Sha256, Encoding.UTF8.GetBytes(secret)) == Error.SaslAuthenticationFailed,
            "SCRAM deletion reaches other brokers before new logins");
        var heartbeat = new WireReader(await SendFrame(reopened, Api.ConsumerGroupHeartbeat, 1,
            w =>
            {
                w.Text("restricted-group", true); w.Text("member", true); w.I32(0); w.Text(null, true); w.Text(null, true);
                w.I32(10000); w.ArrayLength(1, true); w.Text("events", true); w.Text(null, true); w.Text(null, true);
                w.ArrayLength(0, true); w.UVar(0);
            }));
        Check(heartbeat.I32() == 0 && heartbeat.I16() == (short)Error.GroupAuthorizationFailed,
            "modern group dispatch rejects unauthorized users");
        Console.WriteLine("Kafka cluster ACLs, config, dynamic SCRAM and delegation passed controller forwarding, mutual TLS and full restart");
    }
    finally { await StopNodes(); }

    async Task StartNodes()
    {
        for (int i = 0; i < 3; i++)
        {
            var settings = BrokerOptions.Parse(["--listen", addresses[i].ToString(), "--data", Path.Combine(root, $"data-{i}"),
                "--cluster-config", config, "--node-id", (i + 1).ToString(), "--cluster-pfx", certificates[i], "--cluster-ca", ca,
                "--tls-pfx", certificates[i], "--tls-client-ca", ca, "--acl-super-user", adminName,
                "--sasl-plain-users", plain, "--sasl-dynamic-scram", "--delegation-token-secret-file", tokenSecret]);
            var security = new ClusterTls(certificates[i], ca, passwordPath: null);
            try
            {
                var node = await BrokerCluster.OpenAsync(settings, config, i + 1, deadline.Token, security.Options);
                var stop = new CancellationTokenSource();
                nodes.Add((node, security, stop, node.ServeKafkaAsync(stop.Token)));
            }
            catch { security.Dispose(); throw; }
        }
    }

    async Task StopNodes()
    {
        foreach (var node in nodes) await node.Stop.CancelAsync();
        foreach (var node in nodes)
        {
            try { await node.Serving; }
            finally { await node.Cluster.DisposeAsync(); node.Tls.Dispose(); node.Stop.Dispose(); }
        }
        nodes.Clear();
    }

    async Task<(int Leader, int Follower)> WaitForController()
    {
        while (true)
        {
            int leader = nodes.FindIndex(static node => node.Cluster.Metadata.IsLeader);
            int follower = (leader + 1) % nodes.Count;
            if (leader >= 0 && nodes[follower].Cluster.Metadata.LeaderId == leader + 1) return (leader, follower);
            await Task.Delay(30, deadline.Token);
        }
    }

    async Task<SslStream> Open(int index, X509Certificate2 identity, string? plain = null)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        try { await socket.ConnectAsync(addresses[index], deadline.Token); }
        catch { socket.Dispose(); throw; }
        var tls = new SslStream(new NetworkStream(socket, ownsSocket: true));
        try
        {
            var chain = new X509ChainPolicy { TrustMode = X509ChainTrustMode.CustomRootTrust, RevocationMode = X509RevocationMode.NoCheck };
            chain.CustomTrustStore.Add(authority);
            await tls.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            { TargetHost = "127.0.0.1", ClientCertificates = [identity], CertificateChainPolicy = chain }, deadline.Token);
            if (plain is not null)
            {
                var selected = new WireReader(await SendFrame(tls, Api.SaslHandshake, 1, w => w.String("PLAIN")));
                Check(selected.I16() == 0, "select cluster PLAIN bootstrap");
                var authenticated = new WireReader(await SendFrame(tls, Api.SaslAuthenticate, 1, w => w.Bytes(Encoding.UTF8.GetBytes(plain))));
                Check(authenticated.I16() == 0, "authenticate cluster PLAIN bootstrap");
            }
            return tls;
        }
        catch { tls.Dispose(); throw; }
    }

    static Task<byte[]> ReadQuorum(Stream stream, short version) => SendFrame(stream, Api.DescribeQuorum, version, w =>
    {
        w.ArrayLength(1, true); w.Text("__cluster_metadata", true); w.ArrayLength(1, true); w.I32(0);
        w.UVar(0); w.UVar(0); w.UVar(0);
    });

    static async Task<Error> MetadataError(Stream peer)
    {
        var r = new WireReader(await SendFrame(peer, Api.Metadata, 0, w => { w.I32(1); w.String("events"); }));
        int brokers = r.Count();
        for (int i = 0; i < brokers; i++) { r.I32(); r.String(); r.I32(); }
        Check(brokers == 3 && r.Count() == 1, "secure metadata returns a single requested topic");
        return (Error)r.I16();
    }
}

static async Task CheckShareWire(Stream coordinator, Stream broker, Guid topic)
{
    const string group = "secure-share";
    string a = Guid.NewGuid().ToString(), b = Guid.NewGuid().ToString();
    var reset = new WireReader(await SendFrame(coordinator, Api.AlterShareGroupOffsets, 0, w =>
    {
        w.Text(group, true); w.ArrayLength(1, true); w.Text("events", true); w.ArrayLength(1, true);
        w.I32(0); w.I64(0); w.UVar(0); w.UVar(0); w.UVar(0);
    }));
    Check(reset.I32() == 0 && reset.I16() == 0 && reset.NullableText(true) is null && reset.ArrayLength(true) == 1 &&
        reset.Text(true) == "events" && reset.Uuid() == topic && reset.ArrayLength(true) == 1 && reset.I32() == 0 && reset.I16() == 0,
        "share start offset is initialized on an empty group");
    int aEpoch = await Heartbeat(a, 0), bEpoch = await Heartbeat(b, 0);
    byte[] data = Batch(-1, -1, 100, "share-record"u8);
    for (int i = 0; i < 2; i++)
        Check(Appended(await SendFrame(broker, Api.Produce, 8, w => ProduceRequest(w, data)), i), "append a share record through Kafka");
    Check(await Fetch(a, 0) == (0L, (short)1) && await Fetch(b, 0) == (1L, (short)1),
        "two members hold separate records of one share partition concurrently");
    Check(await Acknowledge(a, 1, 0, 4) == Error.None && await Acknowledge(b, 1, 0, 1) == Error.InvalidRecordState &&
        await Acknowledge(b, 2, 1, 1) == Error.None && await Acknowledge(a, 2, 0, 2) == Error.None,
        "renewal, ownership fencing, acceptance and release are persisted");
    Check(await Fetch(b, 3) == (0L, (short)2) && await Acknowledge(b, 4, 0, 1) == Error.None,
        "release is redelivered with the original offset and a higher delivery count");
    Check(await Heartbeat(a, -1) == -1 && await Heartbeat(b, -1) == -1 && aEpoch > 0 && bEpoch > 0,
        "share members leave without rewinding the cursor");
    Console.WriteLine("Kafka Share Groups passed record ownership, renew, release, accept and multiple members on three brokers");

    async Task<int> Heartbeat(string member, int epoch)
    {
        var r = new WireReader(await SendFrame(coordinator, Api.ShareGroupHeartbeat, 1, w =>
        {
            w.Text(group, true); w.Text(member, true); w.I32(epoch); w.Text(null, true);
            w.ArrayLength(epoch == 0 ? 1 : -1, true);
            if (epoch == 0) w.Text("events", true);
            w.UVar(0);
        }));
        Check(r.I32() == 0 && r.I16() == 0 && r.NullableText(true) is null && r.Text(true) == member, "share member heartbeat");
        return r.I32();
    }

    async Task<(long Offset, short Attempts)> Fetch(string member, int epoch)
    {
        var r = new WireReader(await SendFrame(broker, Api.ShareFetch, 2, w =>
        {
            w.Text(group, true); w.Text(member, true); w.I32(epoch);
            w.I32(0); w.I32(0); w.I32(4096); w.I32(1); w.I32(1); w.I8(1); w.Bool(false);
            w.ArrayLength(epoch == 0 ? 1 : 0, true);
            if (epoch == 0)
            {
                w.Uuid(topic); w.ArrayLength(1, true); w.I32(0); w.ArrayLength(0, true); w.UVar(0); w.UVar(0);
            }
            w.ArrayLength(0, true); w.UVar(0);
        }));
        Check(r.I32() == 0 && r.I16() == 0 && r.NullableText(true) is null && r.I32() == ShareGroups.LockMs &&
            r.ArrayLength(true) == 1 && r.Uuid() == topic && r.ArrayLength(true) == 1 && r.I32() == 0 &&
            r.I16() == 0 && r.NullableText(true) is null && r.I16() == 0 && r.NullableText(true) is null, "ShareFetch response envelope");
        r.I32(); r.I32(); r.Tags();
        Check(r.Take(r.ArrayLength(true)).Length > 0 && r.ArrayLength(true) == 1, "ShareFetch contains acquired records");
        long offset = r.I64();
        Check(r.I64() == offset, "record-limit mode acquires exactly one record");
        return (offset, r.I16());
    }

    async Task<Error> Acknowledge(string member, int epoch, long offset, sbyte type)
    {
        var r = new WireReader(await SendFrame(broker, Api.ShareAcknowledge, 2, w =>
        {
            w.Text(group, true); w.Text(member, true); w.I32(epoch); w.Bool(type == 4);
            w.ArrayLength(1, true); w.Uuid(topic); w.ArrayLength(1, true); w.I32(0);
            w.ArrayLength(1, true); w.I64(offset); w.I64(offset); w.ArrayLength(1, true); w.I8(type);
            w.UVar(0); w.UVar(0); w.UVar(0); w.UVar(0);
        }));
        Check(r.I32() == 0 && r.I16() == 0 && r.NullableText(true) is null && r.I32() == ShareGroups.LockMs &&
            r.ArrayLength(true) == 1 && r.Uuid() == topic && r.ArrayLength(true) == 1 && r.I32() == 0, "ShareAcknowledge response envelope");
        return (Error)r.I16();
    }
}

async Task ClusterBrokerSmoke()
{
    var addresses = ClusterSmoke.ReserveAddresses(12);
    string config = Path.Combine(dir, "cluster.json");
    Guid clusterId = Guid.NewGuid();
    await File.WriteAllBytesAsync(config, JsonSerializer.SerializeToUtf8Bytes(new
    {
        clusterId,
        recycleReplicaPorts = true,
        brokers = Enumerable.Range(0, 3).Select(i => new
        {
            id = i + 1, host = "127.0.0.1", port = addresses[i].Port, rack = $"rack-{i + 1}",
            control = addresses[i + 3].ToString(), firstReplicaPort = addresses[i + 6].Port, replicaPortCount = 1,
            @internal = addresses[i + 9].ToString()
        })
    }));
    using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(90));
    var (authority, certificates) = CreateClusterCertificates(dir);
    var clusters = new BrokerCluster?[3];
    var brokers = new (CancellationTokenSource? Stop, Task? Running, ClusterTls? Tls)[3];
    var data = Batch(1, 0, 100, "broker-failover"u8);
    var next = Batch(1, 1, 200, "next-leader"u8);
    try
    {
        for (int i = 0; i < clusters.Length; i++) await Start(i);
        int control = await WaitFor(() => Array.FindIndex(clusters, static cluster => cluster?.Metadata.IsLeader == true));
        int ingress = (control + 1) % clusters.Length;
        await WaitFor(() => clusters[ingress]!.Metadata.LeaderId == control + 1 ? ingress : -1);
        using (var controller = await ConnectTo(control))
        {
            var validated = new WireReader(await Send(controller, Api.CreateTopics, 7, w =>
            {
                w.ArrayLength(1, true); w.Text("validate-topic", true); w.I32(-1); w.I16(-1);
                w.ArrayLength(1, true); w.I32(0); w.ArrayLength(2, true); w.I32(2); w.I32(1); w.UVar(0);
                w.ArrayLength(0, true); w.UVar(0); w.I32(10000); w.Bool(true); w.UVar(0);
            }));
            Check(validated.I32() == 0 && validated.ArrayLength(true) == 1 && validated.Text(true) == "validate-topic" &&
                validated.Uuid() == Guid.Empty && validated.I16() == 0 && validated.NullableText(true) is null &&
                validated.I32() == 1 && validated.I16() == 2 && !clusters[control]!.Metadata.Image.Topics.ContainsKey("validate-topic"),
                "Kafka CreateTopics v7 validates explicit replica placement without creating a topic");
        }
        using (var anyBroker = await ConnectTo(ingress))
        {
            await CheckMetadataV13(anyBroker, 3, clusters[ingress]!.ClusterId);
            await Send(anyBroker, Api.Metadata, 8, w =>
            {
                w.I32(1); w.String("events"); w.Bool(false); w.Bool(false); w.Bool(false);
            });
            Check(!clusters[control]!.Metadata.Image.Topics.ContainsKey("events"), "Kafka Metadata honors disabled auto creation");
            await Send(anyBroker, Api.Metadata, 8, w =>
            {
                w.I32(1); w.String("events"); w.Bool(true); w.Bool(false); w.Bool(false);
            });
            Check(clusters[ingress]!.Metadata.Image.Topics["events"].Partitions.Single().Replicas.Length == 3,
                "Kafka Metadata forwards auto creation and returns after the topic is visible");
            var id = new WireReader(await Send(anyBroker, Api.InitProducerId, 1, w => { w.String(null); w.I32(-1); }));
            Check(id.I32() == 0 && id.I16() == 0 && id.I64() == 1 && id.I16() == 0, "cluster producer ID allocated");
        }

        int leader = await WaitFor(PartitionLeader);
        int initialEpoch = ReplicaEpoch(leader);
        int follower = (leader + 1) % clusters.Length;
        using (var writer = await ConnectTo(leader))
        using (var reader = await ConnectTo(follower))
        {
            var metadata = new WireReader(await Send(reader, Api.Metadata, 8, w =>
            {
                w.I32(1); w.String("events"); w.Bool(false); w.Bool(false); w.Bool(false);
            }));
            Check(metadata.I32() == 0 && metadata.Count() == 3, "Kafka Metadata advertises all brokers");
            Check(Appended(await Send(reader, Api.Produce, 8, w => ProduceRequest(w, data)), -1, Error.NotLeaderOrFollower),
                "Kafka follower refuses leader writes");
            Check(Appended(await Send(writer, Api.Produce, 8, w => ProduceRequest(w, data)), 0), "Kafka quorum append acknowledged");
            Check(Fetched(await Send(reader, Api.Fetch, 5, w => FetchRequest(w, 0, 3000)), 0, 1, data, expectedEpoch: null),
                "Kafka fetch reads committed data from another broker");
            long since = Stopwatch.GetTimestamp();
            byte[] waited = await Send(reader, Api.Fetch, 5, w => FetchRequest(w, 0, 100, 4096));
            Check(Stopwatch.GetElapsedTime(since) >= TimeSpan.FromMilliseconds(70) &&
                Fetched(waited, 0, 1, data, expectedEpoch: null), "cluster Fetch waits for minBytes and returns available data on timeout");
        }
        await Close(leader);
        int successor = await WaitFor(PartitionLeader);
        using (var client = await ConnectTo(successor))
        {
            Check(Appended(await Send(client, Api.Produce, 8, w => ProduceRequest(w, data)), 0),
                "Kafka retried batch retains its offset after failover");
            Check(Appended(await Send(client, Api.Produce, 8, w => ProduceRequest(w, next)), 1),
                "Kafka writes continue at the next offset on the new leader");
            int newEpoch = ReplicaEpoch(successor);
            Check(newEpoch > initialEpoch && await OffsetForEpoch(client, newEpoch, initialEpoch) == (Error.None, initialEpoch, 1L) &&
                await OffsetForEpoch(client, newEpoch, newEpoch) == (Error.None, newEpoch, 2L) &&
                await OffsetForEpoch(client, initialEpoch, initialEpoch) == (Error.FencedLeaderEpoch, -1, -1L),
                "Kafka OffsetForLeaderEpoch preserves the old term's boundary and fences stale requests");
            var timestamp = new WireReader(await Send(client, Api.ListOffsets, 8, w =>
            {
                w.I32(-1); w.I8(0); w.ArrayLength(1, true); w.Text("events", true); w.ArrayLength(1, true);
                w.I32(0); w.I32(newEpoch); w.I64(-3); w.UVar(0); w.UVar(0); w.UVar(0);
            }));
            Check(timestamp.I32() == 0 && timestamp.ArrayLength(true) == 1 && timestamp.Text(true) == "events" &&
                timestamp.ArrayLength(true) == 1 && timestamp.I32() == 0 && timestamp.I16() == 0 &&
                timestamp.I64() == 200 && timestamp.I64() == 1 && timestamp.I32() == newEpoch,
                "Kafka ListOffsets v8 returns the maximum timestamp and its exact offset");
        }
        await Start(leader);
        await WaitFor(() => clusters[leader]!.Metadata.Image.Topics.TryGetValue("events", out var topic) &&
            clusters[leader]!.FindReplica(topic.Partitions[0].Id)?.Leader is not null ? leader : -1);
        using var restarted = await ConnectTo(leader);
        Check(Fetched(await Send(restarted, Api.Fetch, 5, w => FetchRequest(w, 1, 3000)), 1, 2, next, expectedEpoch: null),
            "restarted Kafka broker reads the acknowledged batch from its replica");
        int remove = await WaitFor(PartitionLeader);
        int candidate = (remove + 1) % clusters.Length;
        using (var reader = await ConnectTo(remove))
        {
            int preferred;
            do
            {
                var fetch = new WireReader(await Send(reader, Api.Fetch, 11, w => FetchFromRack(w, $"rack-{candidate + 1}")));
                Check(fetch.I32() == 0 && fetch.I16() == 0 && fetch.I32() == 0 && fetch.Count() == 1 &&
                    fetch.String() == "events" && fetch.Count() == 1 && fetch.I32() == 0 && fetch.I16() == 0,
                    "rack-aware fetch response");
                fetch.I64(); fetch.I64(); fetch.I64();
                Check(fetch.Count(nullable: true) == -1, "no aborted transactions");
                preferred = fetch.I32();
                if (preferred < 0) await Task.Delay(50, deadline.Token);
            } while (preferred < 0);
            Check(preferred == candidate + 1, "fetch advertises the observed follower in the client rack");
        }

        int[] remaining = [.. Enumerable.Range(1, 3).Where(id => id != remove + 1)];
        await Reassign(remaining);
        await WaitFor(() => clusters[remove]!.Metadata.Image.Topics["events"].Partitions[0].Replicas.SequenceEqual(remaining) &&
            !ReplicaPresent(remove) ? remove : -1);
        int migratedLeader = await WaitFor(PartitionLeader);
        using (var client = await ConnectTo(migratedLeader))
        {
            var more = Batch(1, 2, 300, "after-reassignment"u8);
            Check(Appended(await Send(client, Api.Produce, 8, w => ProduceRequest(w, more)), 2),
                "replacement replica set continues writing");
            await Reassign([1, 2, 3]);
            await WaitFor(() => ReplicaPresent(remove) && clusters[remove]!.Metadata.Image.Topics["events"]
                .Partitions[0].Replicas.Length == 3 ? remove : -1);
            using var restored = await ConnectTo(remove);
            Check(Fetched(await Send(restored, Api.Fetch, 5, w => FetchRequest(w, 2, 3000)), 2, 3, more, expectedEpoch: null),
                "added replica catches up before reassignment completes");
        }
        await CheckGroupFailover();
        int staticCoordinator = await WaitFor(() => Array.FindIndex(clusters, static cluster => cluster?.Metadata.IsLeader == true));
        using (var staticClient = await ConnectTo(staticCoordinator)) await CheckStaticMembership(staticClient);
        var deletedTopic = clusters.First(static cluster => cluster is not null)!.Metadata.Image.Topics["events"];
        Guid deletedPartition = deletedTopic.Partitions[0].Id;
        int healthyLeader = await WaitFor(PartitionLeader);
        int damaged = (healthyLeader + 1) % clusters.Length;
        await WaitFor(() => ReplicaPresent(damaged) ? damaged : -1);
        await Close(damaged);
        string chunkManifest = Path.Combine(dir, $"cluster-{damaged}", "replicas", deletedPartition.ToString("N"), "chunk-size");
        byte[] geometry = await File.ReadAllBytesAsync(chunkManifest, deadline.Token);
        await File.WriteAllBytesAsync(chunkManifest, new byte[4], deadline.Token);
        await Start(damaged);
        await WaitFor(() => clusters[damaged]!.ReplicaStorageFailed(deletedPartition) ? damaged : -1);
        using (var brokenReplica = await ConnectTo(damaged))
        {
            var reply = new WireReader(await Send(brokenReplica, Api.Fetch, 5, w => FetchRequest(w, 0, 0)));
            Check(reply.I32() == 0 && reply.Count() == 1 && reply.String() == "events" && reply.Count() == 1 &&
                reply.I32() == 0 && (Error)reply.I16() == Error.KafkaStorageError,
                "damaged local replica responds with a storage error instead of serving data");
        }
        int writeLeader = await WaitFor(PartitionLeader);
        var healthyBatch = Batch(1, 3, 400, "quorum-with-damaged-replica"u8);
        using (var healthy = await ConnectTo(writeLeader))
        {
            Check(Appended(await Send(healthy, Api.Produce, 8, w => ProduceRequest(w, healthyBatch)), 3),
                "the surviving quorum continues writing despite a damaged replica");
            var dirs = new WireReader(await Send(healthy, Api.DescribeLogDirs, 4, w => { w.ArrayLength(-1, true); w.UVar(0); }));
            Check(dirs.I32() == 0 && dirs.I16() == 0 && dirs.ArrayLength(true) == 1 && dirs.I16() == 0 &&
                dirs.Text(true) == Path.Combine(dir, $"cluster-{writeLeader}", "replicas") && dirs.ArrayLength(true) == 1 &&
                dirs.Text(true) == "events" && dirs.ArrayLength(true) == 1 && dirs.I32() == 0 &&
                dirs.I64() >= healthyBatch.Length && dirs.I64() == 0 && !dirs.Bool(), "Kafka DescribeLogDirs reports committed bytes");
            var producers = new WireReader(await Send(healthy, Api.DescribeProducers, 0, w =>
            { w.ArrayLength(1, true); w.Text("events", true); w.ArrayLength(1, true); w.I32(0); w.UVar(0); w.UVar(0); }));
            Check(producers.I32() == 0 && producers.ArrayLength(true) == 1 && producers.Text(true) == "events" &&
                producers.ArrayLength(true) == 1 && producers.I32() == 0 && producers.I16() == 0 &&
                producers.NullableText(true) is null && producers.ArrayLength(true) == 1 && producers.I64() == 1 &&
                producers.I32() == 0 && producers.I32() == 3 && producers.I64() == 400,
                "Kafka DescribeProducers exposes the recovered producer sequence and timestamp");
        }
        await Close(damaged);
        await File.WriteAllBytesAsync(chunkManifest, geometry, deadline.Token);
        await Start(damaged);
        await WaitFor(() => clusters[damaged]!.FindReplica(deletedPartition) is { Leader: not null, HighWatermark: 4 } ? damaged : -1);
        using (var rejoined = await ConnectTo(damaged))
            Check(Fetched(await Send(rejoined, Api.Fetch, 5, w => FetchRequest(w, 3, 3000)), 3, 4, healthyBatch, expectedEpoch: null),
                "a repaired replica catches up to the acknowledged offset");
        await CheckLogStart();
        int activeController = await WaitFor(() => Array.FindIndex(clusters, static cluster => cluster?.Metadata.IsLeader == true));
        using (var admin = await ConnectTo(activeController))
        {
            var removed = new WireReader(await Send(admin, Api.DeleteTopics, 3, w =>
            { w.I32(1); w.String("events"); w.I32(10000); }));
            Check(removed.I32() == 0 && removed.Count() == 1 && removed.String() == "events" && removed.I16() == 0,
                "Kafka DeleteTopics persists removal through the controller quorum");
            await Send(admin, Api.Metadata, 8, w =>
            { w.I32(1); w.String("events"); w.Bool(true); w.Bool(false); w.Bool(false); });
        }
        int replacementLeader = await WaitFor(PartitionLeader);
        var freshTopic = clusters[replacementLeader]!.Metadata.Image.Topics["events"];
        var replacement = freshTopic.Partitions[0];
        Check(replacement.Id != deletedPartition && replacement.Slot == 0, "new Raft group reuses the one available slot");
        using (var writer = await ConnectTo(replacementLeader))
        {
            var fresh = Batch(1, 0, 500, "reused-port-new-log"u8);
            Check(await ProduceFlexible(writer, 13, deletedTopic.Id, fresh) == (Error.UnknownTopicId, -1L),
                "Produce v13 rejects a retired topic ID even after reuse of its name and slot");
            Check(await ProduceFlexible(writer, 13, freshTopic.Id, fresh) == (Error.None, 0L) &&
                await ProduceFlexible(writer, 9, freshTopic.Id, fresh) == (Error.None, 0L) &&
                Fetched(await Send(writer, Api.Fetch, 5, w => FetchRequest(w, 0, 3000)), 0, 1, fresh, expectedEpoch: null),
                "recreated topic has independent records, offsets and producer deduplication");
            await CheckFetchSessions(writer, fresh);
            var newProducer = await ProducerIdV5(writer, 1, 0);
            Check(newProducer is (Error.None, > 1, 0), "InitProducerId v5 renews an exhausted non-transactional identity");
        }
        using (var oldWire = new TcpClient())
        {
            await oldWire.ConnectAsync(addresses[replacementLeader + 6], deadline.Token);
            using var oldPeer = new SslStream(oldWire.GetStream());
            var oldOptions = ClusterTls.BindToGroup(brokers[replacementLeader].Tls!.Options, deletedPartition);
            bool rejected = false;
            try { await oldPeer.AuthenticateAsClientAsync(oldOptions.ClientOptions!, deadline.Token); }
            catch (Exception error) when (error is AuthenticationException or IOException) { rejected = true; }
            Check(rejected, "retired Raft group is rejected by the reused TLS listener");
        }
        await WaitFor(() => Enumerable.Range(0, clusters.Length).All(i => !Directory.Exists(Path.Combine(dir,
            $"cluster-{i}", "replicas", deletedPartition.ToString("N")))) ? 0 : -1);
        int txLeader = await WaitFor(PartitionLeader);
        int txController = await WaitFor(() => Array.FindIndex(clusters, static node => node?.Metadata.IsLeader == true));
        const string externalTransaction = "external-2pc";
        byte[] expected;
        (long Id, short Epoch) original, recovered;
        using (var writer = await ConnectTo(txLeader))
        using (var coordinator = await ConnectTo(txController))
        {
            expected = await ExerciseTransactions(writer, coordinator, freshTopic.Id);
            await ExerciseConsumerGroup(coordinator, freshTopic.Id);
            var started = await InitExternalTransaction(coordinator, externalTransaction, enable: true, keep: false);
            original = started.Producer;
            Check(started.Error == Error.None && started.Ongoing == (-1L, (short)-1) &&
                await AppendModernTransaction(writer, externalTransaction, original, 0, "prepared-external"u8.ToArray()) == (Error.None, 10L),
                "2PC v6 opens a transaction even when the requested ordinary timeout is outside the valid range");
            Check(!clusters[txController]!.Metadata.Image.Transactions[externalTransaction].IsExpired(long.MaxValue),
                "externally coordinated transaction is not aborted by its age");
            var activeState = clusters[txController]!.Metadata.Image.Transactions[externalTransaction];
            await clusters[txController]!.Metadata.ExecuteTransactionAsync(externalTransaction,
                new(TransactionOperation.ExpireId, activeState.ProducerId, activeState.Epoch,
                    activeState.LastUpdated + ReplicatedMetadata.TransactionalIdExpirationMs), deadline.Token);
            Check(clusters[txController]!.Metadata.Image.Transactions.ContainsKey(externalTransaction),
                "inactive identity cleanup cannot remove an ongoing 2PC transaction");
            var claim = await InitExternalTransaction(coordinator, externalTransaction, enable: false, keep: true);
            recovered = claim.Producer;
            Check(claim.Error == Error.None && claim.Ongoing == original && recovered == (original.Id, (short)(original.Epoch + 1)),
                "recovery returns a newer producer identity alongside the ongoing one");
            Check(await AppendModernTransaction(writer, externalTransaction, original, 1, "late-old-producer"u8.ToArray()) ==
                (Error.InvalidProducerEpoch, -1L) &&
                (await EndModernTransaction(coordinator, externalTransaction, original, commit: true)).Error == Error.ProducerFenced,
                "a recovered transaction rejects data and decisions from the old producer");
            var transaction = clusters[txController]!.Metadata.Image.Transactions[externalTransaction];
            byte[] delayedBatch = Batch(original.Id, 1, 700, "already-validated"u8, original.Epoch);
            delayedBatch[22] = 16;
            BinaryPrimitives.WriteUInt32BigEndian(delayedBatch.AsSpan(17), RecordBatch.Crc32C(delayedBatch.AsSpan(21)));
            var delayed = await clusters[txLeader]!.FindReplica(replacement.Id)!.AppendAsync(delayedBatch,
                original.Id, deadline.Token, transaction.Id);
            Check(delayed.Error == Error.InvalidProducerEpoch && delayed.Offset == -1,
                "a batch validated before recovery is still fenced when applying the partition WAL");
        }
        var layout = clusters[0]!.Members;
        for (int i = 0; i < clusters.Length; i++) await Close(i);
        var wrongLayout = layout.Select(member => member with { ReplicaPortCount = member.ReplicaPortCount + 1 }).ToArray();
        using (var tls = new ClusterTls(certificates[0], authority, passwordPath: null))
        {
            bool rejected = false;
            try
            {
                await using var wrong = await ReplicatedMetadata.OpenAsync(Path.Combine(dir, "cluster-0", "control"),
                    clusterId, 1, wrongLayout, deadline.Token, tls.Options, reuseSlots: true);
            }
            catch (InvalidDataException error) when (error.Message.Contains("replica layout", StringComparison.Ordinal))
            { rejected = true; }
            Check(rejected, "replica slot range changes are rejected before the controller replays old placement commands");
        }
        for (int i = 0; i < clusters.Length; i++) await Start(i);
        int groupController = await WaitFor(() => Array.FindIndex(clusters, static node => node?.Metadata.IsLeader == true));
        using (var coordinator = await ConnectTo(groupController))
        {
            await CheckMetadataV13(coordinator, 3, clusters[groupController]!.ClusterId, freshTopic.Id, "events");
            var heartbeat = await ConsumerHeartbeat(coordinator, "modern-group", "alpha", 2, freshTopic.Id, [0]);
            Check(heartbeat.Error == Error.None && heartbeat.Epoch == 2 && heartbeat.Partitions.SequenceEqual([0]),
                "modern consumer member epoch and partition ownership survive a controller restart");
            Check(await ConsumerOffset(coordinator, "modern-group", "alpha", 2, freshTopic.Id) == (Error.None, 9L),
                "Topic ID offset fetch and member fencing survive a controller restart");
            var regex = await ConsumerHeartbeat(coordinator, "regex-group", "regex-client", 2, freshTopic.Id, [0], version: 1);
            Check(regex.Error == Error.None && regex.Epoch == 2 && regex.Partitions.SequenceEqual([0]),
                "regex membership and assignment survive a controller restart");
        }
        using (var writer = await ConnectTo(await WaitFor(PartitionLeader)))
        using (var coordinator = await ConnectTo(groupController))
        {
            byte[] recoveredTransactions = await ReadTransactions(writer, committed: true);
            var (watermark, stable, aborted, batches) = DecodeTransactions(recoveredTransactions);
            var (_, _, expectedAborted, expectedBatches) = DecodeTransactions(expected);
            Check(aborted.SequenceEqual(expectedAborted) && batches.Select(static batch => (batch.Offset, batch.Attributes, batch.Data))
                .SequenceEqual(expectedBatches.Select(static batch => (batch.Offset, batch.Attributes, batch.Data))),
                "transaction batches, commit and abort markers, and the aborted index survive full restart");
            Check(watermark == 11 && stable == 10 &&
                await AppendModernTransaction(writer, externalTransaction, original, 1, "late-after-restart"u8.ToArray()) ==
                    (Error.InvalidProducerEpoch, -1L), "recovery fences the old identity and holds the LSO after restart");
            var nextExternal = await EndModernTransaction(coordinator, externalTransaction, recovered, commit: true);
            Check(nextExternal == (Error.None, recovered.Id, (short)(recovered.Epoch + 1)) &&
                await EndModernTransaction(coordinator, externalTransaction, recovered, commit: true) == nextExternal &&
                (await EndModernTransaction(coordinator, externalTransaction, recovered, commit: false)).Error == Error.InvalidTxnState,
                "recovered EndTxn is durably idempotent and rejects a conflicting outcome");
            var committed = DecodeTransactions(await ReadTransactions(writer, committed: true));
            Check(committed.Watermark == 12 && committed.Stable == 12 && committed.Aborted.SequenceEqual(aborted) &&
                committed.Batches[^2] is (10L, 16, var preparedData) &&
                preparedData.Contains(Convert.ToHexString("prepared-external"u8), StringComparison.Ordinal) &&
                committed.Batches[^1] == (11L, 48, "1"), "the original data and one commit marker become visible");
            var metadata = clusters[groupController]!.Metadata;
            var idle = metadata.Image.Transactions[externalTransaction];
            var cutoff = idle.LastUpdated + ReplicatedMetadata.TransactionalIdExpirationMs;
            var expire = new TransactionCommand(TransactionOperation.ExpireId, idle.ProducerId, idle.Epoch, cutoff);
            await metadata.ExecuteTransactionAsync(externalTransaction, expire with { Now = cutoff - 1 }, deadline.Token);
            Check(metadata.Image.Transactions.ContainsKey(externalTransaction), "identity is retained before the expiry threshold");
            await metadata.ExecuteTransactionAsync(externalTransaction, expire, deadline.Token);
            Check(!metadata.Image.Transactions.ContainsKey(externalTransaction) &&
                !metadata.Image.TransactionNames.ContainsKey(idle.ProducerId), "idle transaction identity and reverse index expire together");
            var renewed = await InitExternalTransaction(coordinator, externalTransaction, enable: true, keep: false);
            Check(renewed.Error == Error.None && renewed.Producer.Id > idle.ProducerId &&
                DecodeTransactions(await ReadTransactions(writer, committed: true)).Batches.SequenceEqual(committed.Batches),
                "expiry never reuses producer IDs or removes acknowledged records");
        }
        Console.WriteLine("Kafka TCP passed quorum Produce, replica reads, failover, reassignment, group recovery, log-start recovery and fenced slot reuse");
    }
    finally
    {
        for (int i = 0; i < clusters.Length; i++) await Close(i);
    }

    async Task Start(int index)
    {
        var address = addresses[index];
        var local = BrokerOptions.Parse(["--listen", address.ToString(), "--data", Path.Combine(dir, $"cluster-{index}"),
            "--cluster-config", config, "--node-id", (index + 1).ToString(), "--cluster-pfx", certificates[index],
            "--cluster-ca", authority]);
        var tls = new ClusterTls(certificates[index], authority, passwordPath: null);
        try
        {
            var cluster = await BrokerCluster.OpenAsync(local, config, index + 1, deadline.Token, tls.Options);
            clusters[index] = cluster;
            var stop = new CancellationTokenSource();
            brokers[index] = (stop, cluster.ServeKafkaAsync(stop.Token), tls);
        }
        catch { tls.Dispose(); throw; }
    }

    async Task Close(int index)
    {
        var (stop, listening, tls) = brokers[index];
        brokers[index] = default;
        var cluster = clusters[index];
        clusters[index] = null;
        try
        {
            if (stop is not null) await stop.CancelAsync();
            if (listening is not null) await listening;
        }
        finally
        {
            stop?.Dispose();
            try { if (cluster is not null) await cluster.DisposeAsync(); }
            finally { tls?.Dispose(); }
        }
    }

    async Task<TcpClient> ConnectTo(int index)
    {
        var client = new TcpClient { NoDelay = true };
        try { await client.ConnectAsync(addresses[index], deadline.Token); return client; }
        catch { client.Dispose(); throw; }
    }

    int PartitionLeader() => Array.FindIndex(clusters, static cluster => cluster is not null &&
        cluster.Metadata.Image.Topics.TryGetValue("events", out var topic) &&
        cluster.FindReplica(topic.Partitions[0].Id)?.IsLeader == true);

    int ReplicaEpoch(int index)
    {
        var broker = clusters[index]!;
        return broker.FindReplica(broker.Metadata.Image.Topics["events"].Partitions[0].Id)!.LeaderEpoch;
    }

    static async Task<(Error Error, long Offset)> ProduceFlexible(TcpClient peer, short version, Guid id, byte[] records)
    {
        var response = new WireReader(await Send(peer, Api.Produce, version, w =>
        {
            w.UVar(0); w.I16(-1); w.I32(5000); w.ArrayLength(1, true);
            if (version >= 13) w.Uuid(id);
            else w.Text("events", true);
            w.ArrayLength(1, true); w.I32(0); w.Bytes(records, compact: true);
            w.UVar(1); w.UVar(42); w.UVar(1); w.I8(7);
            w.UVar(0); w.UVar(0);
        }));
        Check(response.ArrayLength(true) == 1 &&
            (version >= 13 ? response.Uuid() == id : response.Text(true) == "events") &&
            response.ArrayLength(true) == 1 && response.I32() == 0, "flexible Produce response shape");
        var error = (Error)response.I16();
        long offset = response.I64();
        Check(response.I64() == -1 && response.I64() == (error == Error.None ? 0 : -1) &&
            response.ArrayLength(true) == 0 && response.NullableText(true) is null, "flexible Produce partition details");
        response.Tags(); response.Tags(); response.I32(); response.Tags(); response.End();
        return (error, offset);
    }

    static async Task CheckFetchSessions(TcpClient peer, byte[] records)
    {
        var initial = await Fetch(0, 0, offset: 0);
        Check(initial.Error == Error.None && initial.Session > 0 && initial.Records.Length == records.Length &&
            initial.Records.AsSpan(16).SequenceEqual(records.AsSpan(16)), "full incremental Fetch setup carries the batch");
        int id = initial.Session;
        var invalid = await Fetch(id, 2);
        Check(invalid.Error == Error.InvalidFetchSessionEpoch && invalid.Session == 0, "Fetch session rejects skipped epoch");
        var resumed = await Fetch(id, 1);
        Check(resumed.Error == Error.None && resumed.Session == id && resumed.Records.Length == records.Length,
            "incremental Fetch retains unchanged request partitions");
        var moved = await Fetch(id, 2, offset: 1);
        Check(moved.Error == Error.None && moved.Session == id && moved.Topics == 0,
            "incremental Fetch applies offset updates and omits unchanged empty partitions");
        byte[] more = Batch(-1, -1, 601, "advanced-watermark"u8);
        Check(Appended(await Send(peer, Api.Produce, 8, w => ProduceRequest(w, more)), 1),
            "a new batch advances the session partition watermark");
        var watermark = await Fetch(id, 3, offset: 2, expectedWatermark: 2);
        Check(watermark.Error == Error.None && watermark.Session == id && watermark.Topics == 1 &&
            watermark.Records.Length == 0, "incremental Fetch emits a changed watermark even without records");
        var forgotten = await Fetch(id, 4, forget: true);
        Check(forgotten.Error == Error.None && forgotten.Session == id && forgotten.Topics == 0,
            "incremental Fetch removes forgotten partitions");
        var closed = await Fetch(id, -1, offset: 0, expectedWatermark: 2);
        Check(closed.Error == Error.None && closed.Session == 0 && closed.Records.Length == records.Length + more.Length &&
            (await Fetch(id, 5)).Error == Error.FetchSessionIdNotFound, "closing a Fetch session permits a full fetch");

        async Task<(Error Error, int Session, int Topics, byte[] Records)> Fetch(int session, int epoch, long? offset = null,
            bool forget = false, long expectedWatermark = 1)
        {
            var response = new WireReader(await Send(peer, Api.Fetch, 7, w =>
            {
                w.I32(-1); w.I32(0); w.I32(0); w.I32(4096); w.I8(0); w.I32(session); w.I32(epoch);
                w.I32(offset is null ? 0 : 1);
                if (offset is long at)
                {
                    w.String("events"); w.I32(1); w.I32(0); w.I64(at); w.I64(-1); w.I32(4096);
                }
                w.I32(forget ? 1 : 0);
                if (forget) { w.String("events"); w.I32(1); w.I32(0); }
            }));
            Check(response.I32() == 0, "Fetch session throttle");
            var error = (Error)response.I16();
            int sessionId = response.I32(), topics = response.Count();
            if (topics == 0) return (error, sessionId, topics, []);
            Check(topics == 1 && response.String() == "events" && response.Count() == 1 && response.I32() == 0 &&
                response.I16() == 0 && response.I64() == expectedWatermark && response.I64() == expectedWatermark && response.I64() == 0 &&
                response.Count(nullable: true) == -1, "incremental Fetch partition response");
            return (error, sessionId, topics, response.Bytes().ToArray());
        }
    }

    static async Task<(Error, int, long)> OffsetForEpoch(TcpClient peer, int current, int requested)
    {
        var response = new WireReader(await Send(peer, Api.OffsetForLeaderEpoch, 4, w =>
        {
            w.I32(-1); w.ArrayLength(1, true); w.Text("events", true); w.ArrayLength(1, true);
            w.I32(0); w.I32(current); w.I32(requested); w.UVar(0); w.UVar(0); w.UVar(0);
        }));
        Check(response.I32() == 0 && response.ArrayLength(true) == 1 && response.Text(true) == "events" &&
            response.ArrayLength(true) == 1, "Kafka OffsetForLeaderEpoch response shape");
        var error = (Error)response.I16();
        Check(response.I32() == 0, "Kafka OffsetForLeaderEpoch partition ID");
        return (error, response.I32(), response.I64());
    }

    bool ReplicaPresent(int node) => clusters[node]!.Metadata.Image.Topics.TryGetValue("events", out var topic) &&
        clusters[node]!.FindReplica(topic.Partitions[0].Id) is not null;

    async Task Reassign(int[] members)
    {
        int controller = await WaitFor(() => Array.FindIndex(clusters, static cluster => cluster?.Metadata.IsLeader == true));
        using var admin = await ConnectTo(controller);
        var result = new WireReader(await Send(admin, Api.AlterPartitionReassignments, 1, w =>
        {
            w.I32(10000); w.Bool(true); w.UVar(2); w.Text("events", true); w.UVar(2); w.I32(0);
            w.ArrayLength(members.Length, true);
            foreach (int id in members) w.I32(id);
            w.UVar(0); w.UVar(0); w.UVar(0);
        }));
        Check(result.I32() == 0 && result.Bool() && result.I16() == 0 && result.NullableText(true) is null &&
            result.ArrayLength(true) == 1 && result.Text(true) == "events" && result.ArrayLength(true) == 1 &&
            result.I32() == 0 && result.I16() == 0, "Kafka accepts the replica reassignment request");
    }

    async Task CheckLogStart()
    {
        int node = await WaitFor(PartitionLeader);
        using var peer = await ConnectTo(node);
        byte[] pair = BatchWithOlderSecondRecord();
        Check(Appended(await Send(peer, Api.Produce, 8, w => ProduceRequest(w, pair)), 4),
            "multi-record batch appended at the next offset");
        Check(await Trim(peer, 7) == (Error.OffsetOutOfRange, -1L) &&
            await Trim(peer, 5) == (Error.None, 5L) && await Trim(peer, 3) == (Error.None, 5L),
            "DeleteRecords fences out-of-range, commits the new boundary and never moves it backwards");
        Check(await Position(peer, -2) == (5L, -1L) && await Position(peer, 650) == (-1L, -1L) &&
            await Position(peer, -3) == (5L, 600L), "offset queries exclude retired records inside a retained batch");
        for (int i = 0; i < clusters.Length; i++)
        {
            using var reader = await ConnectTo(i);
            var reply = new WireReader(await Send(reader, Api.Fetch, 5, w => FetchRequest(w, 4, 0)));
            Check(reply.I32() == 0 && reply.Count() == 1 && reply.String() == "events" && reply.Count() == 1 &&
                reply.I32() == 0 && reply.I16() == (short)Error.OffsetOutOfRange && reply.I64() == 6 &&
                reply.I64() == 6 && reply.I64() == 5, "all assigned replicas enforce the confirmed log start");
        }
        for (int i = 0; i < clusters.Length; i++) await Close(i);
        for (int i = 0; i < clusters.Length; i++) await Start(i);
        int leader = await WaitFor(PartitionLeader);
        using var restarted = await ConnectTo(leader);
        Check(await Position(restarted, -2) == (5L, -1L), "confirmed log start survives full cluster restart");
        await WaitFor(() => clusters.All(cluster => cluster?.Metadata.Image.Topics.TryGetValue("events", out var topic) == true &&
            cluster.FindReplica(topic.Partitions[0].Id) is { Leader: not null, LogStartOffset: 5 }) ? 0 : -1);
        Check(await Trim(restarted, -1) == (Error.None, 6L) && await Position(restarted, -3) == (-1L, -1L),
            "DeleteRecords -1 advances to the current high watermark");
        var record = Batch(1, 4, 800, "after-deletion"u8);
        Check(Appended(await Send(restarted, Api.Produce, 8, w => ProduceRequest(w, record)), 6) &&
            Fetched(await Send(restarted, Api.Fetch, 5, w => FetchRequest(w, 6, 3000)), 6, 7, record,
                expectedEpoch: null, expectedStart: 6),
            "writes preserve increasing offsets after deleting all previously committed records");

        static Task<(Error Error, long Watermark)> Trim(TcpClient client, long offset) => TrimRecords(client.GetStream(), offset);

        static async Task<(long Offset, long Timestamp)> Position(TcpClient client, long timestamp)
        {
            var result = new WireReader(await Send(client, Api.ListOffsets, 8, w =>
            {
                w.I32(-1); w.I8(0); w.ArrayLength(1, true); w.Text("events", true); w.ArrayLength(1, true);
                w.I32(0); w.I32(-1); w.I64(timestamp); w.UVar(0); w.UVar(0); w.UVar(0);
            }));
            Check(result.I32() == 0 && result.ArrayLength(true) == 1 && result.Text(true) == "events" &&
                result.ArrayLength(true) == 1 && result.I32() == 0 && result.I16() == 0, "ListOffsets log-start test response");
            long time = result.I64();
            return (result.I64(), time);
        }
    }

    static async Task CheckStaticMembership(TcpClient peer)
    {
        const string group = "static-smoke", instance = "client-instance";
        var initial = await Join("");
        Check(initial.Error == Error.None && initial.Generation > 0, "new static member joins without the dynamic ID handshake");
        var synced = new WireReader(await Send(peer, Api.SyncGroup, 3, w =>
        {
            w.String(group); w.I32(initial.Generation); w.String(initial.Member); w.String(instance); w.I32(1);
            w.String(initial.Member); w.Bytes([3, 5, 7]);
        }));
        Check(synced.I32() == 0 && synced.I16() == 0 && synced.Bytes().SequenceEqual(new byte[] { 3, 5, 7 }),
            "static member received its assignment");

        var restarted = await Join("");
        Check(restarted.Error == Error.None && restarted.Generation == initial.Generation && restarted.Member != initial.Member,
            "static restart retains its generation with a new member ID");
        Check((await Join(initial.Member)).Error == Error.FencedInstanceId, "old static member cannot rejoin using its previous ID");
        Check(await Heartbeat(initial.Member) == Error.FencedInstanceId && await Heartbeat(restarted.Member) == Error.None,
            "heartbeat fences the old instance and refreshes the restarted member");
        Check(await Commit(initial.Member) == Error.FencedInstanceId && await Commit(restarted.Member) == Error.None,
            "offset commits require the current static member identity");
        var assignment = new WireReader(await Send(peer, Api.SyncGroup, 3, w =>
        {
            w.String(group); w.I32(restarted.Generation); w.String(restarted.Member); w.String(instance); w.I32(0);
        }));
        Check(assignment.I32() == 0 && assignment.I16() == 0 && assignment.Bytes().SequenceEqual(new byte[] { 3, 5, 7 }),
            "static member recovers the prior assignment");
        var left = new WireReader(await Send(peer, Api.LeaveGroup, 3, w =>
        {
            w.String(group); w.I32(2); w.String(initial.Member); w.String(instance); w.String(""); w.String(instance);
        }));
        Check(left.I32() == 0 && left.I16() == 0 && left.Count() == 2 && left.String() == initial.Member &&
            left.String() == instance && left.I16() == (short)Error.FencedInstanceId && left.String() == "" &&
            left.String() == instance && left.I16() == 0 && await Heartbeat(restarted.Member) == Error.UnknownMemberId,
            "LeaveGroup v3 fences stale IDs and allows removal by instance");

        async Task<(Error Error, int Generation, string Member)> Join(string member)
        {
            var reply = new WireReader(await Send(peer, Api.JoinGroup, 5, w =>
            {
                w.String(group); w.I32(10_000); w.I32(10_000); w.String(member); w.String(instance);
                w.String("consumer"); w.I32(1); w.String("range"); w.Bytes([0]);
            }));
            Check(reply.I32() == 0, "static JoinGroup throttle");
            var error = (Error)reply.I16();
            int generation = reply.I32();
            reply.String(); reply.String();
            string assigned = reply.String();
            if (error == Error.None)
                Check(reply.Count() == 1 && reply.String() == assigned && reply.NullableString() == instance,
                    "static JoinGroup result contains the member and instance");
            return (error, generation, assigned);
        }

        async Task<Error> Heartbeat(string member)
        {
            var reply = new WireReader(await Send(peer, Api.Heartbeat, 3, w =>
            { w.String(group); w.I32(initial.Generation); w.String(member); w.String(instance); }));
            Check(reply.I32() == 0, "static Heartbeat throttle");
            return (Error)reply.I16();
        }

        async Task<Error> Commit(string member)
        {
            var reply = new WireReader(await Send(peer, Api.OffsetCommit, 7, w =>
            {
                w.String(group); w.I32(initial.Generation); w.String(member); w.String(instance);
                w.I32(1); w.String("events"); w.I32(1); w.I32(0); w.I64(1); w.I32(-1); w.String(null);
            }));
            Check(reply.I32() == 0 && reply.Count() == 1 && reply.String() == "events" &&
                reply.Count() == 1 && reply.I32() == 0, "static OffsetCommit response shape");
            return (Error)reply.I16();
        }
    }

    async Task CheckGroupFailover()
    {
        int controller = await WaitFor(() => Array.FindIndex(clusters, static cluster => cluster?.Metadata.IsLeader == true));
        using (var bootstrap = await ConnectTo((controller + 1) % clusters.Length))
        {
            var coordinator = new WireReader(await Send(bootstrap, Api.FindCoordinator, 2, w =>
            { w.String("cluster-group"); w.I8(0); }));
            Check(coordinator.I32() == 0 && coordinator.I16() == 0 && coordinator.NullableString() is null &&
                coordinator.I32() == controller + 1 && coordinator.String() == "127.0.0.1" && coordinator.I32() == addresses[controller].Port,
                "a non-coordinator advertises the Kafka group coordinator");
        }

        string member;
        int generation;
        using (var peer = await ConnectTo(controller))
        {
            var join = new WireReader(await Send(peer, Api.JoinGroup, 3, w =>
            {
                w.String("cluster-group"); w.I32(10000); w.I32(10000); w.String(""); w.String("consumer");
                w.I32(1); w.String("range"); w.Bytes([0]);
            }));
            Check(join.I32() == 0 && join.I16() == 0, "classic group join on controller");
            generation = join.I32();
            Check(join.String() == "range", "classic group protocol selected");
            string groupLeader = join.String();
            member = join.String();
            Check(groupLeader == member, "classic group elected a client leader");

            var sync = new WireReader(await Send(peer, Api.SyncGroup, 2, w =>
            {
                w.String("cluster-group"); w.I32(generation); w.String(member);
                w.I32(1); w.String(member); w.Bytes([1]);
            }));
            Check(sync.I32() == 0 && sync.I16() == 0 && sync.Bytes().SequenceEqual(new byte[] { 1 }), "classic group assignment delivered");
            Check(await Commit(peer, generation, member, 3) == Error.None, "group offset acknowledged by the controller quorum");
            Check(await DeleteGroup(peer) == Error.NonEmptyGroup, "Kafka DeleteGroups protects active groups");
            var description = new WireReader(await Send(peer, Api.DescribeGroups, 0, w =>
            { w.I32(1); w.String("cluster-group"); }));
            Check(description.Count() == 1 && description.I16() == 0 && description.String() == "cluster-group" &&
                description.String() == "Stable", "Kafka DescribeGroups returns the live coordinator state");
        }

        await Close(controller);
        int successor = await WaitFor(() => Array.FindIndex(clusters, static cluster => cluster?.Metadata.IsLeader == true));
        using (var peer = await ConnectTo(successor))
        {
            var offsets = new WireReader(await Send(peer, Api.OffsetFetch, 5, w =>
            { w.String("cluster-group"); w.I32(1); w.String("events"); w.I32(1); w.I32(0); }));
            Check(offsets.I32() == 0 && offsets.Count() == 1 && offsets.String() == "events" && offsets.Count() == 1 &&
                offsets.I32() == 0 && offsets.I64() == 3 && offsets.I32() == -1 && offsets.String() == "recovered" &&
                offsets.I16() == 0 && offsets.I16() == 0, "new group coordinator recovers committed metadata");
            var multiple = new WireReader(await Send(peer, Api.OffsetFetch, 8, w =>
            {
                w.ArrayLength(2, true);
                w.Text("cluster-group", true); w.ArrayLength(-1, true); w.UVar(0);
                w.Text("empty-group", true); w.ArrayLength(-1, true); w.UVar(0);
                w.Bool(true); w.UVar(0);
            }));
            Check(multiple.I32() == 0 && multiple.ArrayLength(true) == 2 && multiple.Text(true) == "cluster-group" &&
                multiple.ArrayLength(true) == 1 && multiple.Text(true) == "events" && multiple.ArrayLength(true) == 1 &&
                multiple.I32() == 0 && multiple.I64() == 3 && multiple.I32() == -1 && multiple.Text(true) == "recovered" &&
                multiple.I16() == 0, "Kafka OffsetFetch v8 returns committed offsets by group");
            multiple.Tags(); multiple.Tags();
            Check(multiple.I16() == 0, "first batched group succeeded");
            multiple.Tags();
            Check(multiple.Text(true) == "empty-group" && multiple.ArrayLength(true) == 0 && multiple.I16() == 0,
                "Kafka OffsetFetch v8 returns an empty offset set for a group with no commits");
            multiple.Tags(); multiple.Tags(); multiple.End();
            Check(await Commit(peer, generation, member, 1) == Error.UnknownMemberId, "old group member cannot overwrite the recovered offset");
            var groups = new WireReader(await Send(peer, Api.ListGroups, 0, _ => { }));
            Check(groups.I16() == 0 && groups.Count() == 1 && groups.String() == "cluster-group", "Kafka ListGroups discovers restored offsets");
            var invalidOffset = new WireReader(await Send(peer, Api.OffsetDelete, 0, w =>
            { w.String("cluster-group"); w.I32(1); w.String("events"); w.I32(1); w.I32(99); }));
            Check(invalidOffset.I16() == 0 && invalidOffset.I32() == 0 && invalidOffset.Count() == 1 &&
                invalidOffset.String() == "events" && invalidOffset.Count() == 1 && invalidOffset.I32() == 99 &&
                (Error)invalidOffset.I16() == Error.UnknownTopicOrPartition &&
                clusters[successor]!.Metadata.Image.Offsets["cluster-group"].Single().Offset == 3,
                "Kafka OffsetDelete with unknown partitions preserves existing offsets");
            var deletedOffset = new WireReader(await Send(peer, Api.OffsetDelete, 0, w =>
            { w.String("cluster-group"); w.I32(1); w.String("events"); w.I32(1); w.I32(0); }));
            Check(deletedOffset.I16() == 0 && !clusters[successor]!.Metadata.Image.Offsets.ContainsKey("cluster-group"),
                "Kafka OffsetDelete removes only requested offsets");
            var committed = new WireReader(await Send(peer, Api.OffsetCommit, 9, w =>
            {
                w.Text("cluster-group", true); w.I32(-1); w.Text("", true); w.Text(null, true);
                w.ArrayLength(1, true); w.Text("events", true); w.ArrayLength(1, true);
                w.I32(0); w.I64(4); w.I32(-1); w.Text("manual", true); w.UVar(0); w.UVar(0); w.UVar(0);
            }));
            Check(committed.I32() == 0 && committed.ArrayLength(true) == 1 && committed.Text(true) == "events" &&
                committed.ArrayLength(true) == 1 && committed.I32() == 0 && committed.I16() == 0,
                "Kafka OffsetCommit v9 stores the updated offset");
            Check(await DeleteGroup(peer) == Error.None &&
                !clusters[successor]!.Metadata.Image.Offsets.ContainsKey("cluster-group"), "Kafka DeleteGroups removes restored metadata");
        }
        await Start(controller);

        static async Task<Error> DeleteGroup(TcpClient peer)
        {
            var response = new WireReader(await Send(peer, Api.DeleteGroups, 0, w => { w.I32(1); w.String("cluster-group"); }));
            Check(response.I32() == 0 && response.Count() == 1 && response.String() == "cluster-group", "Kafka DeleteGroups response shape");
            return (Error)response.I16();
        }

        static async Task<Error> Commit(TcpClient peer, int generation, string member, long offset)
        {
            var reply = new WireReader(await Send(peer, Api.OffsetCommit, 6, w =>
            {
                w.String("cluster-group"); w.I32(generation); w.String(member);
                w.I32(1); w.String("events"); w.I32(1); w.I32(0); w.I64(offset); w.I32(-1); w.String("recovered");
            }));
            Check(reply.I32() == 0 && reply.Count() == 1 && reply.String() == "events" && reply.Count() == 1 && reply.I32() == 0,
                "cluster offset commit response shape");
            return (Error)reply.I16();
        }
    }

    static void FetchFromRack(WireWriter w, string rack)
    {
        w.I32(-1); w.I32(0); w.I32(1); w.I32(4096); w.I8(0); w.I32(0); w.I32(-1);
        w.I32(1); w.String("events"); w.I32(1); w.I32(0); w.I32(-1); w.I64(0); w.I64(-1); w.I32(4096);
        w.I32(0); w.String(rack);
    }

    async Task<int> WaitFor(Func<int> select)
    {
        int index;
        while ((index = select()) < 0) await Task.Delay(25, deadline.Token);
        return index;
    }
}

static async Task ExerciseConsumerGroup(TcpClient coordinator, Guid topicId)
{
    const string group = "modern-group";
    var first = await ConsumerHeartbeat(coordinator, group, "zulu", 0, topicId, []);
    var second = await ConsumerHeartbeat(coordinator, group, "alpha", 0, topicId, []);
    Check(first.Error == Error.None && first.Epoch == 1 && first.Partitions.SequenceEqual([0]) &&
        second.Error == Error.None && second.Epoch == 2 && second.Partitions.Length == 0,
        "KIP-848 target ownership waits for the current member to revoke");
    var revoking = await ConsumerHeartbeat(coordinator, group, "zulu", 1, topicId, [0]);
    var waiting = await ConsumerHeartbeat(coordinator, group, "alpha", 2, topicId, []);
    Check(revoking.Error == Error.None && revoking.Epoch == 1 && revoking.Partitions.Length == 0 &&
        waiting.Error == Error.None && waiting.Partitions.Length == 0, "revocation retains the old member epoch and blocks the target");
    var released = await ConsumerHeartbeat(coordinator, group, "zulu", 1, topicId, []);
    var claimed = await ConsumerHeartbeat(coordinator, group, "alpha", 2, topicId, []);
    Check(released.Error == Error.None && released.Epoch == 2 && claimed.Error == Error.None && claimed.Partitions.SequenceEqual([0]),
        "a revoked partition transfers after the old member acknowledges");
    Check((await ConsumerHeartbeat(coordinator, group, "alpha", 1, topicId, null)).Error == Error.FencedMemberEpoch,
        "stale member epoch is fenced");

    var commit = new WireReader(await Send(coordinator, Api.OffsetCommit, 10, w =>
    {
        w.Text(group, true); w.I32(2); w.Text("alpha", true); w.Text(null, true);
        w.ArrayLength(1, true); w.Uuid(topicId); w.ArrayLength(1, true);
        w.I32(0); w.I64(9); w.I32(-1); w.Text("consumer", true); w.UVar(0); w.UVar(0); w.UVar(0);
    }));
    Check(commit.I32() == 0 && commit.ArrayLength(true) == 1 && commit.Uuid() == topicId &&
        commit.ArrayLength(true) == 1 && commit.I32() == 0 && (Error)commit.I16() == Error.None &&
        await ConsumerOffset(coordinator, group, "alpha", 2, topicId) == (Error.None, 9L) &&
        await ConsumerOffset(coordinator, group, "alpha", 1, topicId) == (Error.StaleMemberEpoch, -1L),
        "topic-ID offset commit and fetch with consumer member epochs");

    var describe = new WireReader(await Send(coordinator, Api.ConsumerGroupDescribe, 1, w =>
    { w.ArrayLength(1, true); w.Text(group, true); w.Bool(false); w.UVar(0); }));
    Check(describe.I32() == 0 && describe.ArrayLength(true) == 1 && (Error)describe.I16() == Error.None &&
        describe.NullableText(true) is null && describe.Text(true) == group && describe.Text(true) == "Stable" &&
        describe.I32() == 2 && describe.I32() == 2 && describe.Text(true) == "range" && describe.ArrayLength(true) == 2,
        "ConsumerGroupDescribe reports the reconciled group and member count");

    const string staticGroup = "modern-static";
    Check((await ConsumerHeartbeat(coordinator, staticGroup, "before", 0, topicId, [], "device")).Error == Error.None &&
        (await ConsumerHeartbeat(coordinator, staticGroup, "after", 0, topicId, [], "device")).Error == Error.UnreleasedInstanceId,
        "active static member cannot be replaced without leaving");
    Check((await ConsumerHeartbeat(coordinator, staticGroup, "before", -2, topicId, null, "device")).Error == Error.None,
        "static member temporarily leaves without releasing its assignment");
    var resumed = await ConsumerHeartbeat(coordinator, staticGroup, "after", 0, topicId, [], "device");
    Check(resumed.Error == Error.None && resumed.Partitions.SequenceEqual([0]) &&
        (await ConsumerHeartbeat(coordinator, staticGroup, "before", 1, topicId, [0], "device")).Error == Error.UnknownMemberId,
        "rejoining static instance retains its assignment and replaces the old member");

    const string regexGroup = "regex-group", regexMember = "regex-client";
    Check((await ConsumerHeartbeat(coordinator, regexGroup, regexMember, 0, topicId, [], version: 1,
        pattern: @"(?=events)")).Error == Error.InvalidRegularExpression, "unsupported regex is rejected by v1");
    var unmatched = await ConsumerHeartbeat(coordinator, regexGroup, regexMember, 0, topicId, [], version: 1, pattern: "vent");
    Check(unmatched.Error == Error.None && unmatched.Epoch == 1 && unmatched.Partitions.Length == 0,
        "subscription regex requires a full topic-name match");
    var matched = await ConsumerHeartbeat(coordinator, regexGroup, regexMember, 1, topicId, [], version: 1,
        pattern: @"(?i)Ev(?:en)[[:alpha:]]\x{73}");
    Check(matched.Error == Error.None && matched.Epoch == 2 && matched.Partitions.SequenceEqual([0]),
        "changing a regex subscription reassigns a matching topic");
    var offsetDelete = new WireReader(await Send(coordinator, Api.OffsetDelete, 0, w =>
    { w.String(regexGroup); w.I32(1); w.String("events"); w.I32(1); w.I32(0); }));
    Check(offsetDelete.I16() == 0 && offsetDelete.I32() == 0 && offsetDelete.Count() == 1 &&
        offsetDelete.String() == "events" && offsetDelete.Count() == 1 && offsetDelete.I32() == 0 &&
        offsetDelete.I16() == (short)Error.GroupSubscribedToTopic,
        "offset delete protects topics matched by a consumer subscription regex");
}

static async Task<(Error Error, int Epoch, int[] Partitions)> ConsumerHeartbeat(TcpClient coordinator, string group,
    string member, int epoch, Guid topicId, int[]? owned, string? instance = null, short version = 0, string? pattern = null)
{
    var response = new WireReader(await Send(coordinator, Api.ConsumerGroupHeartbeat, version, w =>
    {
        w.Text(group, true); w.Text(member, true); w.I32(epoch);
        w.Text(instance, true); w.Text(null, true); w.I32(epoch == 0 ? 20_000 : -1);
        bool namedJoin = epoch == 0 && pattern is null;
        w.ArrayLength(namedJoin ? 1 : -1, true);
        if (namedJoin) w.Text("events", true);
        if (version >= 1) w.Text(pattern, true);
        w.Text(epoch == 0 ? "range" : null, true);
        w.ArrayLength(owned is null ? -1 : owned.Length > 0 ? 1 : 0, true);
        if (owned is { Length: > 0 })
        {
            w.Uuid(topicId); w.ArrayLength(owned.Length, true);
            foreach (int partition in owned) w.I32(partition);
            w.UVar(0);
        }
        w.UVar(0);
    }));
    Check(response.I32() == 0, "consumer heartbeat throttle");
    var error = (Error)response.I16();
    response.NullableText(true);
    string? returned = response.NullableText(true);
    int current = response.I32(), interval = response.I32();
    var assigned = new List<int>();
    if (response.I8() >= 0)
    {
        int topics = response.ArrayLength(true);
        for (int t = 0; t < topics; t++)
        {
            Check(response.Uuid() == topicId, "consumer assignment names the topic UUID");
            int partitions = response.ArrayLength(true);
            for (int p = 0; p < partitions; p++) assigned.Add(response.I32());
            response.Tags();
        }
        response.Tags();
    }
    response.Tags();
    response.End();
    Check(error != Error.None || returned == member && interval == ReplicatedMetadata.ConsumerHeartbeatMs,
        "consumer group heartbeat returns the member and heartbeat interval");
    return (error, current, [.. assigned]);
}

static async Task<(Error, long)> ConsumerOffset(TcpClient coordinator, string group, string member, int epoch, Guid topicId)
{
    var result = new WireReader(await Send(coordinator, Api.OffsetFetch, 10, w =>
    {
        w.ArrayLength(1, true); w.Text(group, true); w.Text(member, true); w.I32(epoch);
        w.ArrayLength(1, true); w.Uuid(topicId); w.ArrayLength(1, true); w.I32(0); w.UVar(0); w.UVar(0);
        w.Bool(true); w.UVar(0);
    }));
    Check(result.I32() == 0 && result.ArrayLength(true) == 1 && result.Text(true) == group &&
        result.ArrayLength(true) == 1 && result.Uuid() == topicId && result.ArrayLength(true) == 1 && result.I32() == 0,
        "member offset fetch v10 response envelope");
    long offset = result.I64();
    result.I32(); result.NullableText(true);
    return ((Error)result.I16(), offset);
}

static async Task<byte[]> ExerciseTransactions(TcpClient writer, TcpClient coordinator, Guid topicId)
{
    const string abortName = "will-abort", commitName = "will-commit";
    var a = await Enlist(abortName);
    var b = await Enlist(commitName);
    await Write(abortName, a, 2, "replayed-abort"u8.ToArray());
    await Write(commitName, b, 3, "replayed-commit"u8.ToArray());
    await Stage(abortName, a, 42);
    Check(await GetOffset(requireStable: false) == (Error.None, -1L) &&
        await GetOffset(requireStable: true) == (Error.UnstableOffsetCommit, -1L),
        "offset fetch distinguishes an unstable transaction offset from no committed offset");
    await Stage(commitName, b, 43);
    await Finish(commitName, b, commit: true);
    var (watermark, stable, _, batches) = DecodeTransactions(await ReadTransactions(writer, committed: true));
    Check(watermark == 5 && stable == 2 && batches.Length == 0,
        "an earlier open transaction prevents a later committed transaction from crossing the LSO");
    var (_, _, _, dirty) = DecodeTransactions(await ReadTransactions(writer, committed: false));
    Check(dirty.Length == 3, "read_uncommitted sees an open transaction, committed data and its marker");
    await Finish(abortName, a, commit: false);
    Check(await GetOffset(requireStable: true) == (Error.None, 43L), "aborted offsets do not overwrite committed offsets");
    var result = await ReadTransactions(writer, committed: true);
    var after = DecodeTransactions(result);
    Check(after.Watermark == 6 && after.Stable == 6 && after.Aborted.SequenceEqual([(a.Id, 2L)]) &&
        after.Batches.Select(static batch => (batch.Offset, batch.Attributes)).SequenceEqual([(2L, 16), (3L, 16), (4L, 48), (5L, 48)]) &&
        after.Batches[2].Data == "1" && after.Batches[3].Data == "0",
        "Kafka Fetch returns the aborted offset, correctly checksummed control records, and the new LSO");
    const string modern = "current-transaction";
    var initial = await Init(modern);
    await Stage(modern, initial, 44, modern: true);
    byte[] payload = "epoch-scoped"u8.ToArray();
    Check(await AppendModernTransaction(writer, modern, initial, 0, payload) == (Error.None, 6L),
        "Produce v12 implicitly enlists the partition");
    var next = await EndModernTransaction(coordinator, modern, initial, commit: true);
    Check(next == (Error.None, initial.Id, (short)(initial.Epoch + 1)) &&
        await EndModernTransaction(coordinator, modern, initial, commit: true) == next &&
        await GetOffset(requireStable: true) == (Error.None, 44L),
        "EndTxn v5 returns a durable epoch bump and the same outcome on retry");
    Check(await AppendModernTransaction(writer, modern, initial, 0, payload) == (Error.InvalidProducerEpoch, -1L) &&
        await AppendModernTransaction(writer, modern, (next.Id, next.Epoch), 0, payload) == (Error.None, 8L),
        "late data with the previous transaction epoch cannot enter a new transaction");
    Check((await EndModernTransaction(coordinator, modern, (next.Id, next.Epoch), commit: false)).Error == Error.None,
        "EndTxn v5 aborts the next epoch");
    return await ReadTransactions(writer, committed: true);

    async Task<(long Id, short Epoch)> Init(string name)
    {
        var init = new WireReader(await Send(coordinator, Api.InitProducerId, 5, w =>
        { w.Text(name, true); w.I32(30_000); w.I64(-1); w.I16(-1); w.UVar(0); }));
        Check(init.I32() == 0 && init.I16() == 0, "transaction producer initialization");
        return (init.I64(), init.I16());
    }

    async Task<(long Id, short Epoch)> Enlist(string name)
    {
        var identity = await Init(name);
        var add = new WireReader(await Send(coordinator, Api.AddPartitionsToTxn, 3, w =>
        {
            w.Text(name, true); w.I64(identity.Item1); w.I16(identity.Item2);
            w.ArrayLength(1, true); w.Text("events", true); w.ArrayLength(1, true); w.I32(0); w.UVar(0); w.UVar(0);
        }));
        Check(add.I32() == 0 && add.ArrayLength(true) == 1 && add.Text(true) == "events" &&
            add.ArrayLength(true) == 1 && add.I32() == 0 && add.I16() == 0, "transaction partition enrollment");
        return identity;
    }

    async Task Write(string name, (long Id, short Epoch) identity, long offset, byte[] value)
    {
        byte[] batch = Batch(identity.Id, 0, 600, value, identity.Epoch);
        batch[22] = 16;
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17), RecordBatch.Crc32C(batch.AsSpan(21)));
        var appended = await Send(writer, Api.Produce, 8, w =>
        {
            w.String(name); w.I16(-1); w.I32(10_000);
            w.I32(1); w.String("events"); w.I32(1); w.I32(0); w.Bytes(batch);
        });
        Check(Appended(appended, offset), "transactional quorum append");
    }

    async Task Finish(string name, (long Id, short Epoch) identity, bool commit)
    {
        var ended = new WireReader(await Send(coordinator, Api.EndTxn, 4, w =>
        { w.Text(name, true); w.I64(identity.Id); w.I16(identity.Epoch); w.Bool(commit); w.UVar(0); }));
        Check(ended.I32() == 0 && ended.I16() == 0, "transaction marker completed on its partition leader");
    }

    async Task Stage(string name, (long Id, short Epoch) identity, long offset, bool modern = false)
    {
        if (!modern)
        {
            var enlisted = new WireReader(await Send(coordinator, Api.AddOffsetsToTxn, 3, w =>
            { w.Text(name, true); w.I64(identity.Id); w.I16(identity.Epoch); w.Text("tx-group", true); w.UVar(0); }));
            Check(enlisted.I32() == 0 && enlisted.I16() == 0, "transaction group enrollment");
        }
        var response = new WireReader(await Send(coordinator, Api.TxnOffsetCommit, modern ? (short)6 : (short)3, w =>
        {
            w.Text(name, true); w.Text("tx-group", true); w.I64(identity.Id); w.I16(identity.Epoch);
            w.I32(-1); w.Text("", true); w.Text(null, true);
            w.ArrayLength(1, true);
            if (modern) w.Uuid(topicId);
            else w.Text("events", true);
            w.ArrayLength(1, true);
            w.I32(0); w.I64(offset); w.I32(-1); w.Text(null, true); w.UVar(0); w.UVar(0); w.UVar(0);
        }));
        Check(response.I32() == 0 && response.ArrayLength(true) == 1 &&
            (modern ? response.Uuid() == topicId : response.Text(true) == "events") &&
            response.ArrayLength(true) == 1 && response.I32() == 0 && response.I16() == 0, "offset staged in a transaction");
    }

    async Task<(Error, long)> GetOffset(bool requireStable)
    {
        var response = new WireReader(await Send(coordinator, Api.OffsetFetch, 7, w =>
        {
            w.Text("tx-group", true); w.ArrayLength(1, true); w.Text("events", true); w.ArrayLength(1, true); w.I32(0); w.UVar(0);
            w.Bool(requireStable); w.UVar(0);
        }));
        Check(response.I32() == 0 && response.ArrayLength(true) == 1 && response.Text(true) == "events" &&
            response.ArrayLength(true) == 1 && response.I32() == 0, "stable offset response envelope");
        long offset = response.I64();
        response.I32();
        response.NullableText(true);
        return ((Error)response.I16(), offset);
    }
}

static async Task<(Error Error, (long Id, short Epoch) Producer, (long Id, short Epoch) Ongoing)>
    InitExternalTransaction(TcpClient coordinator, string name, bool enable, bool keep)
{
    var response = new WireReader(await Send(coordinator, Api.InitProducerId, 6, w =>
    {
        w.Text(name, true); w.I32(enable ? -1 : 30_000); w.I64(-1); w.I16(-1);
        w.Bool(enable); w.Bool(keep); w.UVar(0);
    }));
    Check(response.I32() == 0, "InitProducerId v6 throttle");
    var result = ((Error)response.I16(), (response.I64(), response.I16()), (response.I64(), response.I16()));
    response.Tags();
    response.End();
    return result;
}

static async Task<(Error Error, long Offset)> AppendModernTransaction(TcpClient writer, string name,
    (long Id, short Epoch) identity, int sequence, byte[] value)
{
    byte[] batch = Batch(identity.Id, sequence, 700, value, identity.Epoch);
    batch[22] = 16;
    BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17), RecordBatch.Crc32C(batch.AsSpan(21)));
    var reply = new WireReader(await Send(writer, Api.Produce, 12, w =>
    {
        w.Text(name, true); w.I16(-1); w.I32(10_000);
        w.ArrayLength(1, true); w.Text("events", true); w.ArrayLength(1, true); w.I32(0);
        w.ArrayLength(batch.Length, true); batch.CopyTo(w.Reserve(batch.Length));
        w.UVar(0); w.UVar(0); w.UVar(0);
    }));
    Check(reply.ArrayLength(true) == 1 && reply.Text(true) == "events" && reply.ArrayLength(true) == 1 && reply.I32() == 0,
        "transaction Produce v12 response envelope");
    return ((Error)reply.I16(), reply.I64());
}

static async Task<(Error Error, long Id, short Epoch)> EndModernTransaction(TcpClient coordinator, string name,
    (long Id, short Epoch) identity, bool commit)
{
    var response = new WireReader(await Send(coordinator, Api.EndTxn, 5, w =>
    { w.Text(name, true); w.I64(identity.Id); w.I16(identity.Epoch); w.Bool(commit); w.UVar(0); }));
    Check(response.I32() == 0, "EndTxn v5 throttle");
    return ((Error)response.I16(), response.I64(), response.I16());
}

static Task<byte[]> ReadTransactions(TcpClient writer, bool committed) => Send(writer, Api.Fetch, 5, w =>
{
    w.I32(-1); w.I32(0); w.I32(0); w.I32(4096); w.I8(committed ? (sbyte)1 : (sbyte)0);
    w.I32(1); w.String("events"); w.I32(1); w.I32(0); w.I64(2); w.I64(-1); w.I32(4096);
});

static (long Watermark, long Stable, (long Producer, long First)[] Aborted, (long Offset, int Attributes, string Data)[] Batches)
    DecodeTransactions(byte[] response)
{
    var fetch = new WireReader(response);
    Check(fetch.I32() == 0 && fetch.Count() == 1 && fetch.String() == "events" &&
        fetch.Count() == 1 && fetch.I32() == 0 && fetch.I16() == 0, "transaction Fetch response envelope");
    long watermark = fetch.I64(), stable = fetch.I64();
    fetch.I64();
    var aborted = new (long, long)[Math.Max(fetch.Count(nullable: true), 0)];
    for (int i = 0; i < aborted.Length; i++) aborted[i] = (fetch.I64(), fetch.I64());
    var records = fetch.Bytes();
    var batches = new List<(long, int, string)>();
    while (!records.IsEmpty)
    {
        int size = RecordBatch.SizeFromPrefix(records);
        var record = records[..size];
        Check(RecordBatch.Crc32C(record[21..]) == BinaryPrimitives.ReadUInt32BigEndian(record[17..]), "transaction record CRC32C");
        int flags = record[22];
        string data = (flags & 32) != 0 ? record[69].ToString() : Convert.ToHexString(record[21..]);
        batches.Add((BinaryPrimitives.ReadInt64BigEndian(record), flags, data));
        records = records[size..];
    }
    return (watermark, stable, aborted, [.. batches]);
}

static (string Authority, string[] Certificates) CreateClusterCertificates(string directory)
{
    using var rootKey = RSA.Create(2048);
    var root = new CertificateRequest("CN=SimdKafka smoke CA", rootKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
    root.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, critical: true));
    root.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign, critical: true));
    DateTimeOffset now = DateTimeOffset.UtcNow;
    using var authority = root.CreateSelfSigned(now.AddMinutes(-5), now.AddHours(1));
    string ca = Path.Combine(directory, "cluster-ca.pem");
    File.WriteAllText(ca, authority.ExportCertificatePem());
    var files = new string[3];
    for (int index = 0; index < files.Length; index++)
    {
        using var key = RSA.Create(2048);
        var leaf = new CertificateRequest($"CN=SimdKafka node {index + 1}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        leaf.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection
        {
            new("1.3.6.1.5.5.7.3.1"), new("1.3.6.1.5.5.7.3.2")
        }, critical: true));
        var names = new SubjectAlternativeNameBuilder();
        names.AddDnsName("simdkafka-raft");
        names.AddIpAddress(IPAddress.Loopback);
        leaf.CertificateExtensions.Add(names.Build());
        using var signed = leaf.Create(authority, now.AddMinutes(-3), now.AddMinutes(50), RandomNumberGenerator.GetBytes(12));
        using var certificate = signed.CopyWithPrivateKey(key);
        files[index] = Path.Combine(directory, $"cluster-{index}.pfx");
        File.WriteAllBytes(files[index], certificate.Export(X509ContentType.Pfx));
    }
    return (ca, files);
}

async Task TopicLifecycle()
{
    var storage = options with { Data = Path.Combine(dir, "topic-lifecycle") };
    Guid previousId, currentId;
    string previousDirectory;
    var clear = Batch(-1, -1, 100, "durable-compression"u8);
    var encoded = EncodeGzip(clear, clear.AsSpan(RecordBatch.HeaderSize));
    using (var catalog = new Catalog(storage))
    {
        Check(catalog.Create("generation-test", 1, false) == Error.None, "create lifecycle topic");
        var original = catalog.Get("generation-test")!;
        previousId = original.Id;
        previousDirectory = original.Directory;
        var admitted = Enumerable.Range(0, 32).Select(i =>
            original.Partitions[0].AppendAsync(Batch(-1, -1, i + 1, "before-delete"u8), 0).AsTask()).ToArray();
        Check(catalog.Delete(null, previousId).Error == Error.None, "delete by UUID while appends are pending");
        Check(catalog.Create(original.Name, 1, false) == Error.None, "recreate same topic name");
        var fresh = catalog.Get(original.Name)!;
        currentId = fresh.Id;
        Check(fresh.Id != original.Id && fresh.Directory != original.Directory && catalog.Get(previousId) is null,
            "topic generations have distinct IDs and files");
        Check((await original.Partitions[0].AppendAsync(Batch(-1, -1, 50, "retired"u8), 0)).Error ==
            Error.UnknownTopicOrPartition, "retired partition rejects new appends");
        var drained = await Task.WhenAll(admitted).WaitAsync(TimeSpan.FromSeconds(10));
        Check(drained.Select(x => x.Offset).Order().SequenceEqual(Enumerable.Range(0, 32).Select(x => (long)x)) &&
            drained.All(x => x.Error == Error.None), "accepted appends drain in the retired generation");
        Check(fresh.Partitions[0].Select(0, 65536, true).HighWatermark == 0, "recreated topic starts empty");
        Check(catalog.Commit("old-commit", [new(original.Name, 0, 10, 0, null, previousId)]) ==
            Error.UnknownTopicOrPartition, "stale commits cannot target a recreated topic");
        Check(catalog.Expand(original.Name, 3, validateOnly: true) == Error.None &&
            catalog.Get(currentId)!.Partitions.Length == 1, "validate-only expansion");
        Check(catalog.Expand(original.Name, 3, validateOnly: false) == Error.None, "durable partition expansion");
        Check(await catalog.Get(currentId)!.Partitions[2].AppendAsync(encoded, 0) == new AppendResult(Error.None, 0),
            "new partition persists compressed batches");
    }
    Check(!Directory.Exists(previousDirectory), "retired topic files reclaimed");
    using var reopened = new Catalog(storage);
    var expanded = reopened.Get(currentId)!;
    Check(reopened.Get(previousId) is null && expanded.Partitions.Length == 3 &&
        expanded.Partitions[0].OffsetFor(-1).Offset == 0 && expanded.Partitions[1].OffsetFor(-1).Offset == 0,
        "restart preserves identity and expansion without restoring deleted data");
    var log = expanded.Partitions[2];
    var selection = log.Select(0, 65536, true);
    var recovered = new byte[selection.Size];
    Check(selection.HighWatermark == 1 && log.ReadVerified(recovered, selection.Position, 0, 1) &&
        recovered.AsSpan().SequenceEqual(encoded) && log.OffsetFor(100) == (Error.None, 100L, 0L),
        "compressed bytes and timestamp index survive restart");
}

static void ExerciseCompression()
{
    var batch = Batch(-1, -1, 100, "compress-me"u8);
    var encoded = EncodeGzip(batch, batch.AsSpan(RecordBatch.HeaderSize));
    Check(RecordBatch.Parse(encoded, out var parsed) == Error.None &&
        RecordBatch.FindRecord(encoded, parsed, 100) == (0, 100L), "gzip validation and timestamp scan");
    encoded[^1] ^= 1;
    BinaryPrimitives.WriteUInt32BigEndian(encoded.AsSpan(17), RecordBatch.Crc32C(encoded.AsSpan(21)));
    Check(RecordBatch.Parse(encoded, out _) == Error.CorruptMessage, "invalid gzip with valid Kafka CRC is rejected");
    using var large = new MemoryStream();
    using (var gzip = new GZipStream(large, CompressionLevel.Fastest, leaveOpen: true))
    {
        var zeroes = new byte[64 * 1024];
        for (int size = 0; size < InflatedRecords.MaxSize; size += zeroes.Length)
            gzip.Write(zeroes);
        gzip.WriteByte(1);
    }
    Check(RecordBatch.Parse(GzipRecordBatch(batch, large.ToArray()), out _) == Error.MessageTooLarge, "inflation limit");
}

static byte[] EncodeGzip(byte[] batch, ReadOnlySpan<byte> payload)
{
    using var memory = new MemoryStream();
    using (var gzip = new GZipStream(memory, CompressionLevel.Fastest, leaveOpen: true)) gzip.Write(payload);
    return GzipRecordBatch(batch, memory.ToArray());
}

static byte[] GzipRecordBatch(byte[] batch, byte[] payload)
{
    byte[] result = [..batch.AsSpan(0, RecordBatch.HeaderSize), ..payload];
    BinaryPrimitives.WriteInt32BigEndian(result.AsSpan(8), result.Length - 12);
    BinaryPrimitives.WriteInt16BigEndian(result.AsSpan(21), 1);
    BinaryPrimitives.WriteUInt32BigEndian(result.AsSpan(17), RecordBatch.Crc32C(result.AsSpan(21)));
    return result;
}

static async Task TwoMembers(Catalog catalog)
{
    var group = new GroupCoordinator();
    using var stop = new CancellationTokenSource();
    var clock = group.RunClockAsync(stop.Token);
    try
    {
        var request = new JoinRequest("two", "", "consumer", 3000, 3000, true, [new("range", [0])]);
        var firstId = await group.Join(request);
        var secondId = await group.Join(request);
        Check(firstId.Error == Error.MemberIdRequired && secondId.Error == Error.MemberIdRequired, "member ID handshake");
        var joined = await Task.WhenAll(group.Join(request with { Member = firstId.Member }),
            group.Join(request with { Member = secondId.Member })).WaitAsync(TimeSpan.FromSeconds(5));
        Check(joined.All(j => j.Error == Error.None && j.Generation == joined[0].Generation), "shared generation");
        var leader = joined.Single(j => j.Members is not null);
        var follower = joined.Single(j => j.Member != leader.Member);
        var wait = group.Sync(new("two", follower.Member, follower.Generation, []));
        Check(!wait.IsCompleted, "follower awaits assignment");
        var synced = await group.Sync(new("two", leader.Member, leader.Generation,
            new() { [leader.Member] = [1], [follower.Member] = [2] }));
        Check(synced.Assignment.SequenceEqual(new byte[] { 1 }) &&
              (await wait).Assignment.SequenceEqual(new byte[] { 2 }), "group routes assignments");
        Check(group.Leave("two", follower.Member) == Error.None &&
              group.Heartbeat("two", leader.Member, leader.Generation) == Error.RebalanceInProgress, "leave triggers rebalance");
        var rejoined = await group.Join(request with { Member = leader.Member }).WaitAsync(TimeSpan.FromSeconds(5));
        Check(rejoined.Generation > leader.Generation && rejoined.Members?.Length == 1 &&
              group.Commit("two", leader.Member, leader.Generation, [], catalog) == Error.IllegalGeneration,
              "rebalance fences stale commit");
    }
    finally { await stop.CancelAsync(); await clock; }
}

static async Task CheckFlexibleApis(TcpClient peer, Guid topicId, byte[] batch)
{
    var topic = new WireReader(await Send(peer, Api.CreateTopics, 7, w =>
    {
        w.UVar(2); w.Text("dry-run", true); w.I32(2); w.I16(1); w.UVar(1); w.UVar(1); w.UVar(0);
        w.I32(1000); w.Bool(true); w.UVar(0);
    }));
    Check(topic.I32() == 0 && topic.ArrayLength(true) == 1 && topic.Text(true) == "dry-run" &&
        topic.Uuid() == Guid.Empty && topic.I16() == 0 && topic.NullableText(true) is null && topic.I32() == 2 &&
        topic.I16() == 1 && topic.ArrayLength(true) > 0 && topic.Text(true) == "max.message.bytes" &&
        topic.NullableText(true) == "8388608" && !topic.Bool() && topic.I8() == 5 && !topic.Bool(),
        "successful topic creation includes readable configuration");

    var partitions = new WireReader(await Send(peer, Api.DescribeTopicPartitions, 0, w =>
    {
        w.UVar(2); w.Text("events", true); w.UVar(0); w.I32(1); w.I8(-1); w.UVar(0);
    }));
    Check(partitions.I32() == 0 && partitions.ArrayLength(true) == 1 && partitions.I16() == 0 &&
        partitions.Text(true) == "events" && partitions.Uuid() == topicId && !partitions.Bool() &&
        partitions.ArrayLength(true) == 1 && partitions.I16() == 0 && partitions.I32() == 0 &&
        partitions.I32() == BrokerOptions.NodeId && partitions.I32() == 0 &&
        partitions.ArrayLength(true) == 1 && partitions.I32() == BrokerOptions.NodeId &&
        partitions.ArrayLength(true) == 1 && partitions.I32() == BrokerOptions.NodeId &&
        partitions.ArrayLength(true) == 0 && partitions.ArrayLength(true) == 0 && partitions.ArrayLength(true) == 0,
        "partition description includes empty replica lists for a single broker");
    partitions.Tags(); partitions.I32(); partitions.Tags();
    Check(partitions.I8() == -1, "single partition requires no next page");
    partitions.Tags(); partitions.End();

    var coordinator = new WireReader(await Send(peer, Api.FindCoordinator, 6, w =>
    {
        w.I8(0); w.UVar(3); w.Text("group-a", true); w.Text("", true); w.UVar(0);
    }));
    Check(coordinator.I32() == 0 && coordinator.ArrayLength(true) == 2 && coordinator.Text(true) == "group-a" &&
        coordinator.I32() == BrokerOptions.NodeId && coordinator.Text(true) == "127.0.0.1" && coordinator.I32() > 0 &&
        coordinator.I16() == 0 && coordinator.NullableText(true) is null, "batched coordinator lookup");
    coordinator.Tags();
    Check(coordinator.Text(true) == "" && coordinator.I32() == -1 && coordinator.Text(true) == "" &&
        coordinator.I32() == -1 && (Error)coordinator.I16() == Error.InvalidGroupId && coordinator.NullableText(true) is null,
        "batched coordinator per-key error");
    coordinator.Tags(); coordinator.Tags(); coordinator.End();

    await FetchById(topicId, Error.None);
    await FetchById(Guid.NewGuid(), Error.UnknownTopicId);

    async Task FetchById(Guid id, Error error)
    {
        var fetch = new WireReader(await Send(peer, Api.Fetch, 13, w =>
        {
            w.I32(-1); w.I32(0); w.I32(1); w.I32(4096); w.I8(0); w.I32(0); w.I32(-1);
            w.UVar(2); w.Uuid(id); w.UVar(2);
            w.I32(0); w.I32(0); w.I64(0); w.I32(-1); w.I64(-1); w.I32(4096); w.UVar(0);
            w.UVar(0); w.UVar(1); w.Text("", true); w.UVar(0);
        }));
        long watermark = error == Error.None ? 1 : -1;
        Check(fetch.I32() == 0 && fetch.I16() == 0 && fetch.I32() == 0 && fetch.ArrayLength(true) == 1 &&
            fetch.Uuid() == id && fetch.ArrayLength(true) == 1 && fetch.I32() == 0 && (Error)fetch.I16() == error &&
            fetch.I64() == watermark && fetch.I64() == watermark && fetch.I64() == (watermark < 0 ? -1 : 0) &&
            fetch.ArrayLength(true, nullable: true) == -1 && fetch.I32() == -1, "topic UUID fetch metadata");
        int length = checked((int)fetch.UVar() - 1);
        var records = fetch.Take(length);
        Check(error == Error.None ? records[21..].SequenceEqual(batch.AsSpan(21)) : records.IsEmpty,
            "topic UUID fetch record bytes");
        fetch.Tags(); fetch.Tags(); fetch.Tags(); fetch.End();
    }
}

static async Task CheckMetadataV13(TcpClient peer, int expectedBrokers, string clusterId, Guid topicId = default, string? topic = null)
{
    var response = new WireReader(await Send(peer, Api.Metadata, 13, w =>
    {
        w.ArrayLength(topic is null ? 0 : 1, true);
        if (topic is not null)
        {
            w.Uuid(topicId); // The Java client sends the topic UUID with the empty default name.
            w.Text("", true); w.UVar(0);
        }
        w.Bool(false); w.Bool(false); w.UVar(0);
    }));
    Check(response.I32() == 0 && response.ArrayLength(true) == expectedBrokers, "Metadata v13 includes the broker list");
    for (int i = 0; i < expectedBrokers; i++)
    {
        response.I32(); response.Text(true); response.I32(); response.NullableText(true); response.Tags();
    }
    Check(response.NullableText(true) == clusterId && response.I32() >= 0, "Metadata v13 identifies the cluster");
    if (topic is not null)
    {
        Check(response.ArrayLength(true) == 1 && response.I16() == 0 && response.Text(true) == topic &&
            response.Uuid() == topicId, "Metadata v13 resolves a UUID even when the name field is an empty string");
        return;
    }
    Check(response.ArrayLength(true) == 0 && response.I16() == 0, "Metadata v13 includes the top-level error field");
    response.Tags();
    response.End();
}

static async Task<Error> ApiVersionsV5(TcpClient peer, string? cluster, int brokerId)
{
    var result = new WireReader(await Send(peer, Api.ApiVersions, 5, w =>
    {
        w.Text("smoke", true); w.Text("1", true); w.Text(cluster, true); w.I32(brokerId); w.UVar(0);
    }));
    var error = (Error)result.I16();
    int count = result.ArrayLength(true);
    Check(error == Error.None ? count > 0 : count == 0, "ApiVersions v5 response advertises versions only on success");
    return error;
}

static Task<byte[]> Send(TcpClient peer, Api api, short version, Action<WireWriter> body,
    bool fragmented = false, bool expectResponse = true) => SendFrame(peer.GetStream(), api, version, body, fragmented, expectResponse);

static async Task<byte[]> SendFrame(Stream stream, Api api, short version, Action<WireWriter> body,
    bool fragmented = false, bool expectResponse = true)
{
    using var request = new WireWriter(BrokerOptions.MaxFrameBytes);
    using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(8));
    const int correlationId = 0x12345678;
    request.I32(0);
    request.I16((short)api); request.I16(version); request.I32(correlationId);
    request.String("smoke");
    bool compact = new Request(api, version, correlationId, 0).Flexible;
    if (api == Api.ApiVersions && version is >= 3 and <= Request.MaxApiVersions)
    {
        request.UVar(1); request.UVar(7); request.UVar(1); request.I8(0);
    }
    else if (compact && api != Api.ApiVersions) request.UVar(0);
    body(request);
    request.PatchI32(0, request.Length - 4);
    if (fragmented)
        for (int i = 0; i < request.Length; i++) await stream.WriteAsync(request.Memory.Slice(i, 1), stop.Token);
    else
        await stream.WriteAsync(request.Memory, stop.Token);
    if (!expectResponse) return [];
    byte[] header = new byte[8];
    await stream.ReadExactlyAsync(header, stop.Token);
    int length = BinaryPrimitives.ReadInt32BigEndian(header);
    Check(length is >= 4 and <= BrokerOptions.MaxFrameBytes &&
          BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(4)) == correlationId, "response framing");
    byte[] response = new byte[length - 4];
    await stream.ReadExactlyAsync(response, stop.Token);
    if (compact && api != Api.ApiVersions)
    {
        Check(response[0] == 0, "flexible response header");
        return response[1..];
    }
    return response;
}

static async Task<(Error Error, long Id, short Epoch)> ProducerIdV5(TcpClient peer, long id, short epoch)
{
    var result = new WireReader(await Send(peer, Api.InitProducerId, 5, w =>
    { w.Text(null, true); w.I32(-1); w.I64(id); w.I16(epoch); w.UVar(0); }));
    Check(result.I32() == 0, "InitProducerId v5 throttle");
    var response = ((Error)result.I16(), result.I64(), result.I16());
    result.Tags();
    result.End();
    return response;
}

static byte[] Batch(long producer, int sequence, long timestamp, ReadOnlySpan<byte> value, short epoch = 0)
{
    using var batch = new WireWriter(8192);
    batch.I64(0); batch.I32(0); batch.I32(-1); batch.I8(2); batch.I32(0);
    batch.I16(0); batch.I32(0); batch.I64(timestamp); batch.I64(timestamp);
    batch.I64(producer); batch.I16(producer >= 0 ? epoch : (short)-1); batch.I32(sequence); batch.I32(1);
    int recordSize = 6 + value.Length;
    for (uint length = (uint)value.Length * 2; length >= 128; length >>= 7) recordSize++;
    batch.UVar((uint)recordSize * 2);
    batch.I8(0); batch.UVar(0); batch.UVar(0); batch.UVar(1);
    batch.UVar((uint)value.Length * 2); value.CopyTo(batch.Reserve(value.Length)); batch.UVar(0);
    batch.PatchI32(8, batch.Length - 12);
    batch.PatchI32(17, unchecked((int)RecordBatch.Crc32C(batch.Memory.Span[21..])));
    return batch.Memory.ToArray();
}

static void ProduceRequest(WireWriter w, byte[] batch, short acks = -1)
{
    w.String(null); w.I16(acks); w.I32(10000);
    w.I32(1); w.String("events"); w.I32(1); w.I32(0); w.Bytes(batch);
}

static bool Appended(byte[] bytes, long offset, Error error = Error.None)
{
    var r = new WireReader(bytes);
    return r.Count() == 1 && r.String() == "events" && r.Count() == 1 && r.I32() == 0 &&
           r.I16() == (short)error && r.I64() == offset;
}

static void FetchRequest(WireWriter w, long offset, int wait, int minimum = 1)
{
    w.I32(-1); w.I32(wait); w.I32(minimum); w.I32(4096); w.I8(0);
    w.I32(1); w.String("events"); w.I32(1); w.I32(0); w.I64(offset); w.I64(-1); w.I32(4096);
}

static bool Fetched(byte[] bytes, long offset, long highWatermark, byte[] original, int? expectedEpoch = 0, long expectedStart = 0)
{
    var r = new WireReader(bytes);
    if (r.I32() != 0 || r.Count() != 1 || r.String() != "events" || r.Count() != 1 || r.I32() != 0 ||
        r.I16() != 0 || r.I64() != highWatermark || r.I64() != highWatermark || r.I64() != expectedStart || r.Count(true) != -1)
        return false;
    var batch = r.Bytes();
    return batch.Length == original.Length && BinaryPrimitives.ReadInt64BigEndian(batch) == offset &&
           (expectedEpoch is null || BinaryPrimitives.ReadInt32BigEndian(batch[12..]) == expectedEpoch) &&
           batch[16..].SequenceEqual(original.AsSpan(16));
}

static int FindScalar(ReadOnlySpan<long> values, long target)
{
    for (int i = 0; i < values.Length; i++)
        if (values[i] >= target) return i;
    return -1;
}

static void Check(bool valid, string detail)
{
    if (!valid) throw new InvalidOperationException(detail);
}
