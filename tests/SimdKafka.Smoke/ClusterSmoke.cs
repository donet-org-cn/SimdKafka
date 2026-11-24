using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;
using SimdKafka;

internal static class ClusterSmoke
{
    internal static async Task RunAsync(string directory, byte[] firstBatch, byte[] nextBatch, byte[] conflictingBatch,
        byte[] concurrentBatch, byte[] bumped, byte[] invalidBump)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(35));
        var stop = deadline.Token;
        var addresses = ReserveAddresses();
        var group = Guid.NewGuid();
        var replicas = new ReplicatedPartition?[addresses.Length];
        try
        {
            for (int i = 0; i < addresses.Length; i++) await Start(i);
            int first = await Leader();
            int follower = (first + 1) % addresses.Length;
            Check(await Append(follower, firstBatch, stop) == new AppendResult(Error.NotLeaderOrFollower, -1),
                "Raft follower rejected append");
            var appended = await replicas[first]!.AppendAsync(firstBatch, allocatedProducerId: 1, stop, logAppendTime: true);
            Check(appended.Error == Error.None && appended.Offset == 0 && appended.LogAppendTime > 0, "quorum append assigns broker time");
            Check(RecordBatch.Parse(firstBatch, out var decoded) == Error.None && !decoded.LogAppendTime,
                "replication restores the shared incoming batch header");
            var storedFirst = firstBatch.ToArray();
            decoded.StampAppendTime(storedFirst, appended.LogAppendTime, out _);
            Check(await Append(first, conflictingBatch, stop) == appended,
                "Kafka deduplicates the same sequence range");
            await Read(storedFirst);

            await Close(first);
            int successor = await Leader();
            Check(await Append(successor, firstBatch, stop) == appended &&
                await replicas[successor]!.OffsetForAsync(appended.LogAppendTime, stop) == (Error.None, appended.LogAppendTime, 0L),
                "new leader preserves the timestamp and offset on an idempotent retry");
            Check(await Append(successor, nextBatch, stop) == new AppendResult(Error.None, 1),
                "successor preserves the next offset");
            await Read(storedFirst, nextBatch);

            await Start(first);
            await Read(storedFirst, nextBatch);

            await Close(first);
            await Close(successor);
            int isolated = Array.FindIndex(replicas, static replica => replica is not null);
            using (var minority = CancellationTokenSource.CreateLinkedTokenSource(stop))
            {
                minority.CancelAfter(TimeSpan.FromSeconds(2));
                try
                {
                    Check((await Append(isolated, nextBatch, minority.Token)).Error == Error.NotLeaderOrFollower,
                        "a minority Raft replica cannot acknowledge writes");
                }
                catch (OperationCanceledException) when (minority.IsCancellationRequested && !stop.IsCancellationRequested) { }
            }
            await Close(isolated);

            for (int i = 0; i < addresses.Length; i++) await Start(i);
            int rebuilt = await Leader();
            await Read(storedFirst, nextBatch);
            Check(await Append(rebuilt, firstBatch, stop) == appended, "full cluster restart preserves the original append timestamp");
            const int concurrentBatches = 600;
            var acknowledgements = await Task.WhenAll(Enumerable.Range(0, concurrentBatches)
                .Select(_ => Append(rebuilt, concurrentBatch, stop).AsTask()));
            Check(acknowledgements.All(static result => result.Error == Error.None) &&
                acknowledgements.Select(static result => result.Offset).Order()
                    .SequenceEqual(Enumerable.Range(2, concurrentBatches).Select(static i => (long)i)),
                "group replication preserves a distinct offset for each concurrent append");
            byte[][] expected = [storedFirst, nextBatch, ..Enumerable.Repeat(concurrentBatch, concurrentBatches)];
            await Read(expected);
            var selection = replicas[rebuilt]!.Select(510, concurrentBatch.Length * 4 - 1, allowOversize: false);
            byte[] crossing = new byte[selection.Size];
            Check(selection.Size == concurrentBatch.Length * 3 &&
                await replicas[rebuilt]!.CopyAsync(selection, crossing, stop) &&
                BinaryPrimitives.ReadInt64BigEndian(crossing) == 510 &&
                BinaryPrimitives.ReadInt64BigEndian(crossing.AsSpan(concurrentBatch.Length * 2)) == 512,
                "read budgets stop on a complete batch across an index block boundary");
            Check(await Append(rebuilt, invalidBump, stop) == new AppendResult(Error.OutOfOrderSequence, -1) &&
                await Append(rebuilt, bumped, stop) == new AppendResult(Error.None, expected.Length) &&
                await Append(rebuilt, nextBatch, stop) == new AppendResult(Error.InvalidProducerEpoch, -1),
                "Raft commits a higher producer epoch and fences a stale batch");
            expected = [.. expected, bumped];
            await Close(rebuilt);
            int afterBump = await Leader();
            Check(await Append(afterBump, bumped, stop) == new AppendResult(Error.None, expected.Length - 1) &&
                await Append(afterBump, nextBatch, stop) == new AppendResult(Error.InvalidProducerEpoch, -1),
                "new leader retains the producer epoch and original offset");
            await Start(rebuilt);
            for (int i = 0; i < addresses.Length; i++) await Close(i);
            for (int i = 0; i < addresses.Length; i++) await Start(i);
            int recovered = await Leader();
            await Read(expected);
            Check(await Append(recovered, nextBatch, stop) == new AppendResult(Error.InvalidProducerEpoch, -1) &&
                await Append(recovered, bumped, stop) == new AppendResult(Error.None, expected.Length - 1),
                "full cluster restart preserves the producer epoch");
            Console.WriteLine("Raft partition passed election, quorum writes, LogAppendTime, idempotence, follower catch-up and full restart");
        }
        finally
        {
            for (int i = 0; i < replicas.Length; i++) await Close(i);
        }

        async Task Start(int index)
        {
            replicas[index] = await ReplicatedPartition.OpenAsync(Path.Combine(directory, $"node-{index}"), group,
                addresses[index], addresses, stop);
        }

        async ValueTask Close(int index)
        {
            var replica = replicas[index];
            replicas[index] = null;
            if (replica is not null) await replica.DisposeAsync();
        }

        async Task<int> Leader()
        {
            while (true)
            {
                var candidate = Array.FindIndex(replicas, static replica => replica?.IsLeader == true);
                if (candidate >= 0) return candidate;
                await Task.Delay(25, stop);
            }
        }

        ValueTask<AppendResult> Append(int index, byte[] batch, CancellationToken token) =>
            replicas[index]!.AppendAsync(batch, allocatedProducerId: 1, token);

        async Task Read(params byte[][] expected)
        {
            byte[] data = new byte[expected.Sum(static batch => batch.Length)];
            foreach (var replica in replicas)
            {
                if (replica is null) continue;
                while (replica.Leader is null) await Task.Delay(25, stop);
                var response = await replica.ReadAsync(0, data, stop);
                Check(response == (Error.None, (long)expected.Length, expected.Sum(static batch => batch.Length)),
                    "replica returned the committed high watermark and byte count");
                int position = 0;
                for (int offset = 0; offset < expected.Length; offset++)
                {
                    var batch = expected[offset];
                    var fetched = data.AsSpan(position, batch.Length);
                    Check(BinaryPrimitives.ReadInt64BigEndian(fetched) == offset &&
                        batch.AsSpan(16).SequenceEqual(fetched[16..]), "replica returned the committed offset and payload");
                    position += batch.Length;
                }
            }
        }
    }

    internal static IPEndPoint[] ReserveAddresses(int count = 3)
    {
        var reservations = new TcpListener[count];
        try
        {
            for (int i = 0; i < reservations.Length; i++)
            {
                reservations[i] = new TcpListener(IPAddress.Loopback, 0);
                reservations[i].Start();
            }
            return reservations.Select(static listener => (IPEndPoint)listener.LocalEndpoint).ToArray();
        }
        finally
        {
            foreach (var reservation in reservations) reservation?.Stop();
        }
    }

    internal static async Task RunMetadataAsync(string directory)
    {
        using var time = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        var stop = time.Token;
        var controls = ReserveAddresses();
        var clients = ReserveAddresses();
        var registration = controls.Select((control, i) => new BrokerRegistration(i + 1, "127.0.0.1", clients[i].Port,
            $"rack-{i + 1}", control, 14000 + i * 2000, 1024)).ToArray();
        // This test does not bind replica ports. Choose a range away from the ephemeral controller listeners.
        var groupId = Guid.NewGuid();
        var controllers = new ReplicatedMetadata?[registration.Length];
        try
        {
            for (int i = 0; i < controllers.Length; i++) await Start(i);
            int leader = await Leader();
            Guid topicId = Guid.NewGuid(), partitionId = Guid.NewGuid();
            var create = new MetadataCommand(MetadataOperation.CreateTopic, "cluster-events", topicId, 1, 3, [partitionId]);
            Check((await controllers[(leader + 1) % controllers.Length]!.ExecuteAsync(create, stop)).Error == Error.NotController,
                "metadata writes require the controller");
            Check(await controllers[leader]!.ExecuteAsync(create, stop) == new MetadataResult(Error.None, topicId),
                "metadata creates a replicated topic");
            var producer = await controllers[leader]!.ExecuteAsync(new(MetadataOperation.AllocateProducer), stop);
            Check(producer == new MetadataResult(Error.None, ProducerId: 1), "producer ID assigned by the quorum");
            var offset = new CommittedOffset(create.Name, 0, 7, 1, "stable", topicId);
            var commit = new MetadataCommand(MetadataOperation.CommitOffsets, "group", Offsets: [offset]);
            Check((await controllers[leader]!.ExecuteAsync(commit, stop)).Error == Error.None, "offset commit replicated");
            Check(((ISnapshotManager)controllers[leader]!).Snapshot is not null, "controller persisted a metadata snapshot");
            await Stop(leader);

            int successor = await Leader();
            var target = controllers[successor]!;
            await target.ReadBarrierAsync(stop);
            var recovered = target.Image.Topics[create.Name];
            Check(recovered.Id == topicId && recovered.Partitions[0].Id == partitionId &&
                recovered.Partitions[0].Replicas.SequenceEqual([1, 2, 3]) &&
                target.Image.Offsets["group"].Single() is { CommitIndex: > 0 } durableOffset &&
                (durableOffset with { CommitIndex = 0 }) == offset,
                "elected controller recovers topic IDs, replica assignment and offsets");
            Check(await target.ExecuteAsync(new(MetadataOperation.AllocateProducer), stop) == new MetadataResult(Error.None, ProducerId: 2),
                "producer IDs do not repeat after failover");
            Guid secondPartition = Guid.NewGuid();
            Check((await target.ExecuteAsync(new(MetadataOperation.ExpandTopic, create.Name, topicId, 2,
                PartitionIds: [secondPartition], ReplicaAssignments: [[3, 2, 1]]), stop)).Error == Error.None, "metadata expansion replicated");
            var expanded = target.Image.Topics[create.Name];
            Check(expanded.Partitions[0] == recovered.Partitions[0] && expanded.Partitions[1].Id == secondPartition &&
                expanded.Partitions[1].Replicas.SequenceEqual([3, 2, 1]),
                "expansion preserves the first partition assignment and identity");
            Check((await target.ExecuteAsync(new(MetadataOperation.DeleteTopic, create.Name, topicId), stop)).Error == Error.None,
                "durable metadata deletion");
            Guid newTopic = Guid.NewGuid(), newPartition = Guid.NewGuid();
            Check((await target.ExecuteAsync(create with { TopicId = newTopic, PartitionIds = [newPartition] }, stop)).Error == Error.None &&
                (await target.ExecuteAsync(commit, stop)).Error == Error.UnknownTopicOrPartition,
                "a stale offset commit cannot write to the recreated topic");
            await Start(leader);
            var restored = controllers[leader]!;
            while (restored.LeaderId < 0) await Task.Delay(25, stop);
            await restored.ReadBarrierAsync(stop);
            Check(restored.Image.Topics[create.Name].Id == newTopic && restored.Image.Topics[create.Name].Partitions[0].Slot == 2 &&
                !restored.Image.Offsets.ContainsKey("group"), "old controller catches up without reusing the deleted partition slot");
            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            const string transactionName = "snapshot-transaction";
            var initialized = await target.ExecuteTransactionAsync(transactionName, new(TransactionOperation.Init, -1, -1, now,
                TimeoutMs: 60_000), stop);
            long pid = initialized.ProducerId;
            short epoch = initialized.ProducerEpoch;
            var instance = Guid.NewGuid();
            var transactionOffset = offset with { TopicId = newTopic, Offset = 10 };
            Check(initialized.Error == Error.None &&
                (await target.ExecuteTransactionAsync(transactionName, new(TransactionOperation.Enlist, pid, epoch, now,
                    instance, Group: "group"), stop)).Error == Error.None &&
                (await target.ExecuteTransactionAsync(transactionName, new(TransactionOperation.StageOffsets, pid, epoch, now,
                    Group: "group"), stop, [transactionOffset])).Error == Error.None &&
                (await target.ExecuteAsync(commit with { Offsets = [transactionOffset with { Offset = 11 }] }, stop)).Error == Error.None &&
                (await target.ExecuteTransactionAsync(transactionName, new(TransactionOperation.Prepare, pid, epoch, now,
                    Commit: true), stop)).Error == Error.None, "prepared decision and ordered offsets persisted");
            var simultaneous = await Task.WhenAll(Enumerable.Range(0, 40).Select(i => target.ExecuteTransactionAsync(
                $"snapshot-concurrent-{i}", new(TransactionOperation.Init, -1, -1, now, TimeoutMs: 60_000), stop).AsTask()));
            Check(simultaneous.All(static item => item.Error == Error.None && item.ProducerEpoch == 0) &&
                simultaneous.Select(static item => item.ProducerId).Distinct().Count() == simultaneous.Length,
                "concurrent transaction initialization allocates distinct producer IDs");
            for (int i = 0; i < controllers.Length; i++) await Stop(i);
            for (int i = 0; i < controllers.Length; i++) await Start(i);
            var election = controllers[await Leader()]!;
            await election.ReadBarrierAsync(stop);
            var transaction = election.Image.Transactions[transactionName];
            Check(transaction.Id == instance && transaction.Commit && transaction.Preparing &&
                election.Image.ActiveTransactions.Contains(transactionName) &&
                election.Image.TransactionNames[pid] == transactionName && transaction.StagedOffsets["group"].Single().Offset == 10 &&
                simultaneous.All(item => election.Image.TransactionNames.ContainsKey(item.ProducerId)),
                "controller reconstructs the prepared decision, staged offsets and transaction indexes");
            Check((await election.ExecuteTransactionAsync(transactionName, new(TransactionOperation.Complete, pid, epoch, now,
                instance, Commit: true), stop)).Error == Error.None &&
                election.Image.Offsets["group"].Single().Offset == 11, "a late transaction marker cannot replace a newer group offset");
            Console.WriteLine("Raft metadata passed failover, replica assignments, partition expansion and offset fencing");
        }
        finally
        {
            for (int i = 0; i < controllers.Length; i++) await Stop(i);
        }

        async Task Start(int index) => controllers[index] = await ReplicatedMetadata.OpenAsync(
            Path.Combine(directory, $"node-{index}"), groupId, registration[index].Id, registration, stop, snapshotInterval: 2);

        async ValueTask Stop(int index)
        {
            var controller = controllers[index];
            controllers[index] = null;
            if (controller is not null) await controller.DisposeAsync();
        }

        async Task<int> Leader()
        {
            while (true)
            {
                int i = Array.FindIndex(controllers, static controller => controller?.IsLeader == true);
                if (i >= 0) return i;
                await Task.Delay(25, stop);
            }
        }
    }

    private static void Check(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }
}
