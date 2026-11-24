using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text.Json;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Security;

namespace SimdKafka;

internal readonly record struct ReplicaProgress(int Epoch, long HighWatermark, long LogStartOffset = -1);
internal readonly record struct QuorumProgress(int Leader, int Epoch, long HighWatermark, long LogEnd)
{
    internal static QuorumProgress Unknown => new(-1, -1, -1, -1);
}

internal sealed class ControllerRpc : IAsyncDisposable
{
    private const int HeaderSize = 21;
    private const int ResultSize = 26;
    private const int ProgressSize = 12;
    private const int FullProgressSize = 20;
    private const int QuorumProgressSize = 24;
    private const byte MetadataRequest = 1, ReplicaStatus = 2, ReplicaLogStart = 3, TransactionMarkerRequest = 4,
        AclRequest = 5, ShareRequest = 6, TokenRequest = 7, QuorumStatus = 8;
    private readonly bool _tokensEnabled;
    private readonly TcpListener _listener;
    private readonly SslOptions? _tls;
    private readonly Guid _clusterId;
    private readonly ReplicatedMetadata _controller;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly Task _server;
    internal Func<Guid, ReplicaProgress>? Probe { private get; set; }
    internal Func<TransactionMarker, CancellationToken, ValueTask<Error>>? MarkTransaction { private get; set; }

    internal ControllerRpc(IPEndPoint local, Guid clusterId, ReplicatedMetadata controller, SslOptions? tls, bool tokensEnabled)
    {
        _tokensEnabled = tls is not null && tokensEnabled;
        if (tls is null && !IPAddress.IsLoopback(local.Address))
            throw new ArgumentException("The broker internal listener requires mutual TLS outside loopback");
        _clusterId = clusterId;
        _controller = controller;
        _tls = tls;
        _listener = new TcpListener(local);
        _listener.Start(64);
        _server = RunAsync(_shutdown.Token);
    }

    internal async ValueTask<MetadataResult> SendAsync(IPEndPoint address, MetadataCommand command, CancellationToken stop)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(ReplicatedMetadata.Normalize(command));
        if (payload.Length > ReplicatedMetadata.MaxCommandBytes) return new(Error.MessageTooLarge);
        byte kind = command.Operation switch
        {
            MetadataOperation.Acl => AclRequest,
            MetadataOperation.ShareGroup => ShareRequest,
            MetadataOperation.DelegationToken => TokenRequest,
            _ => MetadataRequest
        };
        var response = await ExchangeAsync(address, kind, payload, kind == MetadataRequest ? ResultSize : -1, stop);
        if (kind != MetadataRequest) return response is null ? new(Error.NotController) : JsonSerializer.Deserialize<MetadataResult>(response);
        return response is null ? new(Error.NotController) :
            new((Error)BinaryPrimitives.ReadInt16BigEndian(response), new Guid(response.AsSpan(2, 16), bigEndian: true),
                BinaryPrimitives.ReadInt64BigEndian(response.AsSpan(18)));
    }

    internal async ValueTask<ReplicaProgress> ProbeAsync(IPEndPoint address, Guid assignment, CancellationToken stop,
        bool includeLogStart = false)
    {
        byte[]? data = await ExchangeAsync(address, includeLogStart ? ReplicaLogStart : ReplicaStatus,
            assignment.ToByteArray(bigEndian: true), includeLogStart ? FullProgressSize : ProgressSize, stop);
        return data is null ? new(-1, -1) :
            new(BinaryPrimitives.ReadInt32BigEndian(data), BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(4)),
                includeLogStart ? BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(12)) : -1);
    }

    internal async Task<QuorumProgress> ProbeQuorumAsync(IPEndPoint address, CancellationToken stop)
    {
        byte[]? data = await ExchangeAsync(address, QuorumStatus, [], QuorumProgressSize, stop);
        return data is null ? QuorumProgress.Unknown : new(BinaryPrimitives.ReadInt32BigEndian(data),
            BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(4)), BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(8)),
            BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(16)));
    }

    internal async ValueTask<Error> SendMarkerAsync(IPEndPoint address, TransactionMarker marker, CancellationToken stop)
    {
        var response = await ExchangeAsync(address, TransactionMarkerRequest, marker.Encode(), sizeof(short), stop);
        return response is null ? Error.NotLeaderOrFollower : (Error)BinaryPrimitives.ReadInt16BigEndian(response);
    }

    private async ValueTask<byte[]?> ExchangeAsync(IPEndPoint address, byte operation, byte[] payload, int resultSize,
        CancellationToken stop)
    {
        try
        {
            using var client = new TcpClient(address.AddressFamily) { NoDelay = true };
            await client.ConnectAsync(address, stop);
            var stream = client.GetStream();
            using var encrypted = _tls is null ? null : new SslStream(stream);
            if (encrypted is not null) await encrypted.AuthenticateAsClientAsync(_tls!.ClientOptions, stop);
            Stream transport = (Stream?)encrypted ?? stream;
            byte[] header = new byte[HeaderSize];
            BinaryPrimitives.WriteInt32BigEndian(header, payload.Length);
            _clusterId.TryWriteBytes(header.AsSpan(4), bigEndian: true, out _);
            header[20] = operation;
            await transport.WriteAsync(header, stop);
            await transport.WriteAsync(payload, stop);
            if (resultSize < 0)
            {
                await transport.ReadExactlyAsync(header.AsMemory(0, sizeof(int)), stop);
                resultSize = BinaryPrimitives.ReadInt32BigEndian(header);
                if (resultSize is <= 0 or > ReplicatedMetadata.MaxCommandBytes) return null;
            }
            byte[] response = new byte[resultSize];
            await transport.ReadExactlyAsync(response, stop);
            return response;
        }
        catch (Exception e) when (e is IOException or SocketException or AuthenticationException)
        {
            return null;
        }
    }

    private async Task RunAsync(CancellationToken stop)
    {
        var clients = new HashSet<Task>();
        try
        {
            while (true)
            {
                var client = await _listener.AcceptTcpClientAsync(stop);
                clients.RemoveWhere(static task => task.IsCompletedSuccessfully);
                if (clients.Count >= 64) client.Dispose();
                else clients.Add(ServeAsync(client, stop));
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        finally { await Task.WhenAll(clients); }
    }

    private async Task ServeAsync(TcpClient client, CancellationToken stop)
    {
        using (client)
        using (var timeout = CancellationTokenSource.CreateLinkedTokenSource(stop))
        {
            timeout.CancelAfter(TimeSpan.FromSeconds(30));
            var token = timeout.Token;
            try
            {
                client.NoDelay = true;
                var stream = client.GetStream();
                using var encrypted = _tls is null ? null : new SslStream(stream);
                if (encrypted is not null)
                {
                    await encrypted.AuthenticateAsServerAsync(_tls!.ServerOptions, token);
                    if (encrypted.RemoteCertificate is null) return;
                }
                Stream transport = (Stream?)encrypted ?? stream;
                byte[] header = new byte[HeaderSize];
                await transport.ReadExactlyAsync(header, token);
                int length = BinaryPrimitives.ReadInt32BigEndian(header);
                if (new Guid(header.AsSpan(4, 16), bigEndian: true) != _clusterId)
                    return;
                if (header[20] == QuorumStatus && length == 0)
                {
                    var progress = _controller.CurrentQuorumProgress;
                    byte[] sample = new byte[QuorumProgressSize];
                    BinaryPrimitives.WriteInt32BigEndian(sample, progress.Leader);
                    BinaryPrimitives.WriteInt32BigEndian(sample.AsSpan(4), progress.Epoch);
                    BinaryPrimitives.WriteInt64BigEndian(sample.AsSpan(8), progress.HighWatermark);
                    BinaryPrimitives.WriteInt64BigEndian(sample.AsSpan(16), progress.LogEnd);
                    await transport.WriteAsync(sample, token);
                    return;
                }
                if (header[20] is ReplicaStatus or ReplicaLogStart && length == 16)
                {
                    byte[] request = new byte[16];
                    await transport.ReadExactlyAsync(request, token);
                    var status = Probe?.Invoke(new Guid(request, bigEndian: true)) ?? new ReplicaProgress(-1, -1);
                    bool includeLogStart = header[20] == ReplicaLogStart;
                    byte[] update = new byte[includeLogStart ? FullProgressSize : ProgressSize];
                    BinaryPrimitives.WriteInt32BigEndian(update, status.Epoch);
                    BinaryPrimitives.WriteInt64BigEndian(update.AsSpan(4), status.HighWatermark);
                    if (includeLogStart) BinaryPrimitives.WriteInt64BigEndian(update.AsSpan(12), status.LogStartOffset);
                    await transport.WriteAsync(update, token);
                    return;
                }
                if (header[20] == TransactionMarkerRequest && length == TransactionMarker.Size && MarkTransaction is { } mark)
                {
                    byte[] request = new byte[length];
                    await transport.ReadExactlyAsync(request, token);
                    Error error = await mark(TransactionMarker.Decode(request), token);
                    byte[] markerResult = new byte[sizeof(short)];
                    BinaryPrimitives.WriteInt16BigEndian(markerResult, (short)error);
                    await transport.WriteAsync(markerResult, token);
                    return;
                }
                byte kind = header[20];
                if (kind is not (MetadataRequest or AclRequest or ShareRequest or TokenRequest) ||
                    kind == TokenRequest && !_tokensEnabled || length is <= 0 or > ReplicatedMetadata.MaxCommandBytes) return;
                byte[] payload = ArrayPool<byte>.Shared.Rent(length);
                MetadataResult result;
                try
                {
                    await transport.ReadExactlyAsync(payload.AsMemory(0, length), token);
                    var command = JsonSerializer.Deserialize<MetadataCommand>(payload.AsSpan(0, length));
                    if (command is null || command.Name is null || !Enum.IsDefined(command.Operation) ||
                        (command.Operation == MetadataOperation.Acl) != (kind == AclRequest) ||
                        (command.Operation == MetadataOperation.ShareGroup) != (kind == ShareRequest) ||
                        (command.Operation == MetadataOperation.DelegationToken) != (kind == TokenRequest) ||
                        command.Operation == MetadataOperation.ConsumerGroup) return;
                    if (command.Operation == MetadataOperation.Transaction && command.Transaction is not
                        { Operation: TransactionOperation.Enlist, Modern: true, Partitions.Length: 1, Group: null }) return;
                    result = await _controller.ExecuteAsync(command, token);
                }
                finally { ArrayPool<byte>.Shared.Return(payload, clearArray: true); }
                if (kind != MetadataRequest)
                {
                    byte[] body = JsonSerializer.SerializeToUtf8Bytes(result);
                    if (body.Length > ReplicatedMetadata.MaxCommandBytes)
                        body = JsonSerializer.SerializeToUtf8Bytes(new MetadataResult(Error.MessageTooLarge));
                    BinaryPrimitives.WriteInt32BigEndian(header, body.Length);
                    await transport.WriteAsync(header.AsMemory(0, sizeof(int)), token);
                    await transport.WriteAsync(body, token);
                    return;
                }
                byte[] response = new byte[ResultSize];
                BinaryPrimitives.WriteInt16BigEndian(response, (short)result.Error);
                result.TopicId.TryWriteBytes(response.AsSpan(2, 16), bigEndian: true, out _);
                BinaryPrimitives.WriteInt64BigEndian(response.AsSpan(18), result.ProducerId);
                await transport.WriteAsync(response, token);
            }
            catch (Exception e) when (e is IOException or SocketException or AuthenticationException or OperationCanceledException or
                JsonException or ArgumentException or QuorumUnreachableException or NotLeaderException) { }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _shutdown.CancelAsync();
        try { await _server; }
        finally
        {
            _listener.Stop();
            _shutdown.Dispose();
        }
    }
}
