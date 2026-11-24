using System.Buffers;
using System.Buffers.Binary;
using System.Text.Json;
using System.Threading.Channels;
using DotNext.IO;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;

namespace SimdKafka;

internal sealed partial class ReplicatedMetadata
{
    private const int MaxTransactionCommands = 32;
    private const int MaxTransactionEntryBytes = MaxCommandBytes + 8;
    private const uint TransactionGroupMagic = 0x534B5458; // SKTX
    private static readonly byte[] TransactionGroupPrefix = [0x53, 0x4B, 0x54, 0x58];
    private readonly Channel<PendingTransaction> _pendingTransactions = Channel.CreateUnbounded<PendingTransaction>(
        new UnboundedChannelOptions { SingleReader = true });
    private readonly CancellationTokenSource _transactionStop = new();
    private Task _transactionWriter = Task.CompletedTask;

    private sealed class PendingTransaction(MetadataCommand command, byte[] encoded, long term, CancellationToken deadline)
        : TaskCompletionSource<MetadataResult>(TaskCreationOptions.RunContinuationsAsynchronously)
    {
        internal MetadataCommand Command { get; } = command;
        internal byte[] Encoded { get; } = encoded;
        internal long Term { get; } = term;
        internal CancellationToken Deadline { get; } = deadline;
        internal MetadataResult? Result;
    }

    private sealed class TransactionWriteGroup(long term, PendingTransaction[] commands, int size) : IRaftLogEntry, IInputLogEntry
    {
        public long Term => term;
        public int? CommandId => null;
        public long? Length => size;
        public bool IsSnapshot => false;
        public bool IsReusable => true;
        public bool IsConfiguration => false;
        public object? Context { get; init; } = commands;

        public async ValueTask WriteToAsync<TWriter>(TWriter writer, CancellationToken stop) where TWriter : IAsyncBinaryWriter
        {
            if (commands.Length == 1)
            {
                await writer.WriteAsync(commands[0].Encoded, null, stop);
                return;
            }
            await writer.WriteAsync(TransactionGroupPrefix, null, stop);
            byte[] length = new byte[sizeof(int)];
            foreach (var command in commands)
            {
                BinaryPrimitives.WriteInt32BigEndian(length, command.Encoded.Length);
                await writer.WriteAsync(length, null, stop);
                await writer.WriteAsync(command.Encoded, null, stop);
            }
        }
    }

    private ValueTask<MetadataResult> QueueTransactionAsync(MetadataCommand command, byte[] bytes, CancellationToken stop)
    {
        if (!IsLeader) return ValueTask.FromResult(new MetadataResult(Error.NotController));
        if (stop.IsCancellationRequested) return ValueTask.FromResult(new MetadataResult(Error.RequestTimedOut));
        var request = new PendingTransaction(command, bytes, Term, stop);
        return _pendingTransactions.Writer.TryWrite(request) ? new ValueTask<MetadataResult>(request.Task) :
            ValueTask.FromResult(new MetadataResult(Error.NotController));
    }

    private async Task ReplicateTransactionsAsync(CancellationToken stop)
    {
        var reader = _pendingTransactions.Reader;
        var group = new List<PendingTransaction>(MaxTransactionCommands);
        try
        {
            while (await reader.WaitToReadAsync(stop))
            {
                long term = Term;
                int size = TransactionGroupPrefix.Length;
                while (group.Count < MaxTransactionCommands && reader.TryPeek(out var candidate) &&
                    candidate.Encoded.Length + sizeof(int) <= MaxTransactionEntryBytes - size)
                {
                    if (!reader.TryRead(out candidate)) break;
                    if (candidate.Term != term) candidate.SetResult(new(Error.NotController));
                    else if (candidate.Deadline.IsCancellationRequested) candidate.SetResult(new(Error.RequestTimedOut));
                    else
                    {
                        group.Add(candidate);
                        size += candidate.Encoded.Length + sizeof(int);
                    }
                }
                if (group.Count == 0) continue;
                Error error = Error.None;
                using var replication = CancellationTokenSource.CreateLinkedTokenSource(stop, LeadershipToken);
                replication.CancelAfter(TimeSpan.FromSeconds(10));
                try
                {
                    if (group.Count == 1) size = group[0].Encoded.Length;
                    await _journal.ReplicateAsync(new TransactionWriteGroup(term, group.ToArray(), size),
                        replication.Token);
                }
                catch (NotLeaderException) { error = Error.NotController; }
                catch (OperationCanceledException) { error = IsLeader ? Error.RequestTimedOut : Error.NotController; }
                catch (QuorumUnreachableException) { error = Error.CoordinatorNotAvailable; }
                catch (IOException) { error = Error.KafkaStorageError; }
                finally
                {
                    foreach (var request in group)
                        request.SetResult(error == Error.None ? request.Result ?? new(Error.KafkaStorageError) : new(error));
                    group.Clear();
                }
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        finally
        {
            _pendingTransactions.Writer.TryComplete();
            while (reader.TryRead(out var request)) request.SetResult(new(Error.NotController));
        }
    }

    private static bool IsTransactionGroup(ReadOnlySequence<byte> data)
    {
        if (data.Length < sizeof(uint)) return false;
        var reader = new SequenceReader<byte>(data);
        return reader.TryReadBigEndian(out int magic) && (uint)magic == TransactionGroupMagic;
    }

    private void ApplyTransactionGroup(ReadOnlySequence<byte> data, long index)
    {
        var reader = new SequenceReader<byte>(data.Slice(TransactionGroupPrefix.Length));
        int count = 0;
        while (!reader.End)
        {
            if (++count > MaxTransactionCommands || !reader.TryReadBigEndian(out int size) ||
                size <= 0 || size > reader.Remaining) throw new InvalidDataException("Invalid transaction command group");
            var command = DecodeMetadata(data.Slice(TransactionGroupPrefix.Length + reader.Consumed, size));
            if (command.Operation != MetadataOperation.Transaction)
                throw new InvalidDataException("Transaction command group contains another metadata operation");
            Apply(command, index++);
            reader.Advance(size);
        }
        if (count < 2) throw new InvalidDataException("Truncated transaction command group");
    }

    private static MetadataCommand DecodeMetadata(ReadOnlySequence<byte> payload) =>
        JsonSerializer.Deserialize<MetadataCommand>(payload.IsSingleSegment ? payload.FirstSpan : payload.ToArray()) ??
        throw new InvalidDataException("Invalid replicated metadata command");
}
