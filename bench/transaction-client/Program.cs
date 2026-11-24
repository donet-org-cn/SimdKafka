using Confluent.Kafka;
using Confluent.Kafka.Admin;

if (args.Length != 2)
    throw new ArgumentException("Usage: TransactionClient bootstrap-servers run-id");

string bootstrap = args[0], run = args[1];
string input = $"tx-input-{run}", output = $"tx-output-{run}";
var source = new TopicPartition(input, 0);
var sinks = new[] { new TopicPartition(output, 0), new TopicPartition(output, 1) };
using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
await admin.CreateTopicsAsync([
    new TopicSpecification { Name = input, NumPartitions = 1, ReplicationFactor = 3 },
    new TopicSpecification { Name = output, NumPartitions = 2, ReplicationFactor = 3 }
]);

using (var seed = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap, Acks = Acks.All }).Build())
{
    for (int i = 0; i < 4; i++) await seed.ProduceAsync(source, new() { Value = $"input-{i}" }, deadline.Token);
}

using var consumer = CreateConsumer($"tx-group-{run}", IsolationLevel.ReadCommitted, eof: false);
consumer.Subscribe(input);
using var producer = CreateProducer();
producer.InitTransactions(TimeSpan.FromSeconds(35));
await Process(producer, "commit", commit: true, first: true);
await Process(producer, "abort", commit: false);
Check(consumer.Committed([source], TimeSpan.FromSeconds(10)).Single().Offset == 1, "aborted offset leaked");

producer.BeginTransaction();
await Write(producer, "fenced");
using (var replacement = CreateProducer())
{
    replacement.InitTransactions(TimeSpan.FromSeconds(35));
    try
    {
        producer.CommitTransaction();
        throw new InvalidOperationException("Old producer was able to commit");
    }
    catch (KafkaException e) when (e.Error.Code is ErrorCode.ProducerFenced or ErrorCode.InvalidProducerEpoch or ErrorCode.Local_Fenced)
    {
        Console.WriteLine($"Old producer fenced with {e.Error.Code}");
    }
    await Process(replacement, "replace", commit: true);
}
consumer.Close();
Verify(IsolationLevel.ReadCommitted, ["commit", "replace"]);
Verify(IsolationLevel.ReadUncommitted, ["commit", "abort", "fenced", "replace"]);
Console.WriteLine($"Confluent.Kafka {Library.VersionString} passed commit, abort, consumer offsets and fencing for {run}");

IProducer<string, string> CreateProducer() =>
    new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = bootstrap, TransactionalId = $"tx-producer-{run}", TransactionTimeoutMs = 30_000,
        CompressionType = CompressionType.Zstd
    }).Build();

IConsumer<string, string> CreateConsumer(string group, IsolationLevel isolation, bool eof) =>
    new ConsumerBuilder<string, string>(new ConsumerConfig
    {
        BootstrapServers = bootstrap, GroupId = group, IsolationLevel = isolation, EnablePartitionEof = eof,
        EnableAutoCommit = false, AutoOffsetReset = AutoOffsetReset.Earliest,
        FetchWaitMaxMs = 100, FetchErrorBackoffMs = 100
    }).Build();

async Task Write(IProducer<string, string> writer, string value)
{
    await Task.WhenAll(sinks.Select(partition => writer.ProduceAsync(partition,
        new() { Key = run, Value = value }, deadline.Token)));
}

async Task Process(IProducer<string, string> writer, string value, bool commit, bool first = false)
{
    var received = consumer.Consume(deadline.Token);
    writer.BeginTransaction();
    await Write(writer, value);
    if (first)
    {
        Verify(IsolationLevel.ReadCommitted, []);
        Verify(IsolationLevel.ReadUncommitted, ["commit"]);
    }
    writer.SendOffsetsToTransaction([new(received.TopicPartition, received.Offset + 1)], consumer.ConsumerGroupMetadata,
        TimeSpan.FromSeconds(20));
    if (commit) writer.CommitTransaction();
    else writer.AbortTransaction();
    if (commit)
        Check(consumer.Committed([source], TimeSpan.FromSeconds(10)).Single().Offset == received.Offset + 1,
            "committed offset mismatch");
}

void Verify(IsolationLevel isolation, string[] values)
{
    using var reader = CreateConsumer($"tx-verify-{run}", isolation, eof: true);
    reader.Assign(sinks.Select(partition => new TopicPartitionOffset(partition, Offset.Beginning)));
    var seen = sinks.ToDictionary(partition => partition, _ => new List<string>());
    var eof = new HashSet<TopicPartition>();
    while (eof.Count < sinks.Length)
    {
        var record = reader.Consume(deadline.Token);
        if (record.IsPartitionEOF) eof.Add(record.TopicPartition);
        else seen[record.TopicPartition].Add(record.Message.Value);
    }
    foreach (var (partition, actual) in seen)
        Check(actual.SequenceEqual(values), $"{isolation} {partition}: [{string.Join(", ", actual)}]");
}

static void Check(bool condition, string detail)
{
    if (!condition) throw new InvalidOperationException(detail);
}
