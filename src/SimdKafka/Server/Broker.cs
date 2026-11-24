using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Numerics;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace SimdKafka;

internal sealed class Broker(BrokerOptions options, RequestRouter router)
{
    private const int RetainedFrameBytes = 256 * 1024;
    internal Broker(BrokerOptions options, Catalog catalog) : this(options, new Router(options, catalog)) { }
    private readonly PlainCredentials? _credentials = options.SaslUsers is null ? null : new(options.SaslUsers);
    private readonly ScramCredentials? _scram = options.ScramUsers is not null || options.DynamicScram || options.DelegationTokenSecretFile is not null ?
        new(options.ScramUsers, options.DynamicScram, tokenAuth: options.DelegationTokenSecretFile is not null) : null;
    private readonly OAuthBearerCredentials? _oauth = options.OAuthConfig is null ? null : new(options.OAuthConfig);
    private readonly DelegationTokenCredentials? _tokens = options.DelegationTokenSecretFile is null ? null : new(options.DelegationTokenSecretFile);
    private readonly FrameBudget _largeFrames = new(128 * 1024 * 1024);
    private readonly HashSet<string>? _superUsers = options.AclSuperUsers?.ToHashSet(StringComparer.Ordinal);
    private readonly ClientQuotaLimiter _quotas = new();

    internal async Task RunAsync(CancellationToken stop)
    {
        using var certificate = options.TlsCertificate is null ? null : X509CertificateLoader.LoadPkcs12FromFile(
            options.TlsCertificate, options.TlsPasswordFile is null ? null :
                File.ReadAllText(options.TlsPasswordFile).TrimEnd('\r', '\n'),
            OperatingSystem.IsWindows() ? X509KeyStorageFlags.DefaultKeySet : X509KeyStorageFlags.EphemeralKeySet);
        if (certificate is not null && !certificate.HasPrivateKey)
            throw new ArgumentException("The TLS certificate requires a private key");
        if (certificate is null && options.SaslEnabled)
            throw new ArgumentException("SASL requires TLS");
        using var clientCa = options.ClientCa is null ? null : X509CertificateLoader.LoadCertificateFromFile(options.ClientCa);
        using var listener = new Socket(options.Listen.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        listener.Bind(options.Listen);
        listener.Listen(BrokerOptions.MaxConnections);
        using var shutdown = CancellationTokenSource.CreateLinkedTokenSource(stop);
        var clients = new HashSet<Task>();
        Task groupClock = router.RunGroupClockAsync(shutdown.Token);
        string protocol = certificate is null ? "PLAINTEXT" : clientCa is null ? "TLS" : "mTLS";
        Console.WriteLine($"SimdKafka listening on {listener.LocalEndPoint} ({protocol}) | advertised {options.Host}:{options.Port} | data {options.Data}");
        try
        {
            while (!stop.IsCancellationRequested)
            {
                Socket socket = await listener.AcceptAsync(stop);
                clients.RemoveWhere(static task => task.IsCompletedSuccessfully);
                if (clients.Count >= BrokerOptions.MaxConnections)
                {
                    socket.Dispose();
                    continue;
                }
                clients.Add(ServeWithConnectionQuotaAsync(socket, certificate, clientCa, shutdown.Token));
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        finally
        {
            await shutdown.CancelAsync();
            await Task.WhenAll(clients);
            await groupClock;
        }
    }

    private async Task ServeWithConnectionQuotaAsync(Socket socket, X509Certificate2? certificate, X509Certificate2? clientCa,
        CancellationToken stop)
    {
        int delay = ConnectionQuotaDelay(socket);
        if (delay == 0)
        {
            await ServeAsync(socket, certificate, clientCa, stop);
            return;
        }
        try
        {
            if (delay > 0)
            {
                await Task.Delay(delay, stop);
                if (ConnectionQuotaDelay(socket) == 0) await ServeAsync(socket, certificate, clientCa, stop);
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        finally { socket.Dispose(); }
    }

    private int ConnectionQuotaDelay(Socket socket)
    {
        var current = router.CurrentQuotas;
        if (current.Entries.IsEmpty || socket.RemoteEndPoint is not IPEndPoint address) return 0;
        var (scope, rate) = current.ResolveConnection(address.Address);
        if (!rate.HasValue) return 0;
        if (rate == 0) return -1;
        return _quotas.Get(scope, ClientQuotaLimiter.Kind.Connect).AdmitConnection(rate.Value);
    }

    private async Task ServeAsync(Socket socket, X509Certificate2? certificate, X509Certificate2? clientCa,
        CancellationToken stop)
    {
        using (socket)
        using (var io = CancellationTokenSource.CreateLinkedTokenSource(stop))
        using (var output = new WireWriter(BrokerOptions.MaxFrameBytes + 4))
        using (var tls = certificate is null ? null : new SslStream(new NetworkStream(socket, ownsSocket: false)))
        {
            byte[] input = ArrayPool<byte>.Shared.Rent(4096);
            int reserved = 0;
            FetchSession? session = null;
            RequestQuota? quotaSession = null;
            var authentication = options.SaslEnabled ? new SaslConnection(_credentials, _scram, _oauth, _tokens) : null;
            string principal = "User:ANONYMOUS", host = "", tlsUser = "ANONYMOUS";
            try
            {
                socket.NoDelay = true;
                if (tls is not null)
                {
                    io.CancelAfter(TimeSpan.FromSeconds(10));
                    await AuthenticateAsync(tls, certificate!, clientCa, io.Token);
                    if (clientCa is not null && tls.RemoteCertificate is null)
                        throw new AuthenticationException("Client certificate required");
                    if (authentication is null && tls.RemoteCertificate is { } peer)
                    {
                        using var identity = X509CertificateLoader.LoadCertificate(peer.GetRawCertData());
                        tlsUser = identity.Subject;
                        principal = "User:" + tlsUser;
                    }
                }
                if (_superUsers is not null && socket.RemoteEndPoint is IPEndPoint remote)
                    host = remote.Address.IsIPv4MappedToIPv6 ? remote.Address.MapToIPv4().ToString() : remote.Address.ToString();
                while (!stop.IsCancellationRequested)
                {
                    io.CancelAfter(TimeSpan.FromMinutes(2));
                    if (!await ReceiveExactlyAsync(socket, tls, input.AsMemory(0, 4), io.Token)) return;
                    int length = BinaryPrimitives.ReadInt32BigEndian(input);
                    bool rawSasl = authentication is { Authenticated: false, UsesRawTokens: true };
                    int limit = authentication is { Authenticated: false } ? 16 * 1024 : BrokerOptions.MaxFrameBytes;
                    if (length < (rawSasl ? 1 : 8) || length > limit) return;
                    if (length > RetainedFrameBytes)
                    {
                        int bytes = (int)BitOperations.RoundUpToPowerOf2((uint)length);
                        await _largeFrames.AcquireAsync(bytes, io.Token);
                        reserved = bytes;
                    }
                    if (length > input.Length)
                    {
                        var bigger = ArrayPool<byte>.Shared.Rent(length);
                        ArrayPool<byte>.Shared.Return(input);
                        input = bigger;
                    }
                    if (!await ReceiveExactlyAsync(socket, tls, input.AsMemory(0, length), io.Token)) return;
                    io.CancelAfter(Timeout.InfiniteTimeSpan);
                    var frame = input.AsMemory(0, length);
                    if (rawSasl)
                    {
                        output.Reset();
                        try
                        {
                            bool tokenAuth = authentication!.NeedsTokens(frame.Span);
                            var current = !tokenAuth && options.DynamicScram && authentication.UsesScram ?
                                (await router.ReadScramAsync(stop)).Directory : null;
                            var directory = tokenAuth ? (await router.ReadTokensAsync(stop)).Directory : null;
                            if (!authentication.AcceptRaw(frame.Span, output, current, directory)) return;
                        }
                        finally { CryptographicOperations.ZeroMemory(frame.Span); }
                        io.CancelAfter(TimeSpan.FromMinutes(2));
                        await tls!.WriteAsync(output.Memory, io.Token);
                        continue;
                    }
                    var quotas = router.CurrentQuotas;
                    Request request = Request.Parse(frame.Span, readQuotaClient: !quotas.Entries.IsEmpty);
                    if (request.Api is Api.JoinGroup or Api.ConsumerGroupHeartbeat or Api.ShareGroupHeartbeat && socket.RemoteEndPoint is IPEndPoint address)
                        request = request with { ClientHost = address.Address.ToString() };
                    if (request.Api == Api.Fetch && request.Version >= 7)
                        request = request with { FetchSession = session ??= new() };
                    output.Reset();
                    output.I32(0);
                    output.I32(request.CorrelationId);
                    output.EndStruct(request.Api != Api.ApiVersions && request.Flexible);
                    bool send = true, keep = true;
                    if (authentication is { OAuthExpiry: > 0 } oauthSession &&
                        oauthSession.OAuthExpiry <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) return;
                    bool reauthenticate = request.Api == Api.SaslHandshake && authentication is { CanReauthenticate: true };
                    if (authentication is not null && (!authentication.Authenticated || reauthenticate) && request.Api != Api.ApiVersions)
                    {
                        try
                        {
                            bool tokenAuth = request.Api == Api.SaslAuthenticate && authentication.NeedsTokens(frame.Span[request.BodyOffset..]);
                            var current = !tokenAuth && request.Api == Api.SaslAuthenticate && options.DynamicScram && authentication.UsesScram ?
                                (await router.ReadScramAsync(stop)).Directory : null;
                            var directory = tokenAuth ? (await router.ReadTokensAsync(stop)).Directory : null;
                            keep = authentication.Accept(request, frame.Span[request.BodyOffset..], output, current, directory);
                        }
                        finally
                        {
                            if (request.Api == Api.SaslAuthenticate)
                                CryptographicOperations.ZeroMemory(frame.Span[request.BodyOffset..]);
                        }
                    }
                    else
                    {
                        string? tokenId = authentication?.TokenId;
                        if (tokenId is not null &&
                            router.CurrentTokens.Entries.GetValueOrDefault(tokenId)?.Active(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) != true)
                            return;
                        if (_superUsers is not null && request.Api != Api.ApiVersions)
                        {
                            if (authentication?.User is { } user) principal = "User:" + user;
                            bool superUser = _superUsers.Contains(principal);
                            var acls = superUser ? ImmutableArray<KafkaAcl>.Empty : await router.ReadAclsAsync(stop);
                            request = request with { Access = new(principal, host, acls, superUser) };
                        }
                        if (request.Api is Api.DescribeUserScramCredentials or Api.AlterUserScramCredentials)
                            request = request with { BootstrapScram = _scram?.Bootstrap };
                        if (request.Api is Api.ShareGroupHeartbeat or Api.ShareFetch or Api.ShareAcknowledge)
                            request = request with { AuthenticatedPrincipal = authentication?.User is { } authenticated ? "User:" + authenticated : principal };
                        bool tokenApi = Router.IsTokenApi(request.Api);
                        if (tokenApi)
                            request = request with
                            {
                                AuthenticatedPrincipal = authentication?.User is { } identity ? "User:" + identity : principal,
                                TokenAuthenticated = tokenId is not null,
                                TokenCredentials = _tokens
                            };
                        if (!quotas.Entries.IsEmpty && request.Api is Api.Produce or Api.Fetch or Api.ShareFetch)
                        {
                            string key = request.Api == Api.Produce ? ClientQuotas.ProducerBytes : ClientQuotas.ConsumerBytes;
                            var (scope, rate) = quotas.Resolve(authentication?.User ?? tlsUser, request.ClientId, key);
                            if (rate > 0)
                            {
                                quotaSession ??= new();
                                var kind = request.Api == Api.Produce ? ClientQuotaLimiter.Kind.Produce : ClientQuotaLimiter.Kind.Fetch;
                                quotaSession.Reset(_quotas.Get(scope, kind), rate);
                                request = request with { Quota = quotaSession };
                            }
                        }
                        try { send = await router.DispatchAsync(request, frame[request.BodyOffset..], output, stop); }
                        finally
                        {
                            if (request.Api is Api.AlterUserScramCredentials or Api.RenewDelegationToken or Api.ExpireDelegationToken)
                                CryptographicOperations.ZeroMemory(frame.Span[request.BodyOffset..]);
                        }
                    }
                    if (send)
                    {
                        if (request.Quota is { Delay: > 0 } legacyQuota && request.DelaysQuotaResponse)
                            await Task.Delay(legacyQuota.Delay, stop);
                        output.PatchI32(0, output.Length - 4);
                        io.CancelAfter(TimeSpan.FromMinutes(2));
                        var unsent = output.Memory;
                        if (tls is not null) await tls.WriteAsync(unsent, io.Token);
                        else while (!unsent.IsEmpty)
                        {
                            int sent = await socket.SendAsync(unsent, SocketFlags.None, io.Token);
                            if (sent == 0) return;
                            unsent = unsent[sent..];
                        }
                    }
                    if (!keep) return;
                    if (input.Length > RetainedFrameBytes)
                    {
                        var small = ArrayPool<byte>.Shared.Rent(4096);
                        ArrayPool<byte>.Shared.Return(input);
                        input = small;
                    }
                    if (reserved != 0)
                    {
                        _largeFrames.Release(reserved);
                        reserved = 0;
                    }
                    output.Reset();
                    if (request.Quota is { Delay: > 0 } quota && (!send || !request.DelaysQuotaResponse))
                        await Task.Delay(quota.Delay, stop);
                }
            }
            catch (Exception e) when (e is SocketException or OperationCanceledException or WireException or
                OverflowException or IOException or AuthenticationException)
            {
                // A malformed frame or disconnected client affects only its TCP session.
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"SimdKafka connection: {e.Message}");
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(input);
                if (reserved != 0) _largeFrames.Release(reserved);
            }
        }
    }

    private static Task AuthenticateAsync(SslStream stream, X509Certificate2 certificate, X509Certificate2? ca,
        CancellationToken stop)
    {
        var parameters = new SslServerAuthenticationOptions
        {
            ServerCertificate = certificate,
            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
            ClientCertificateRequired = ca is not null
        };
        if (ca is not null)
        {
            var policy = new X509ChainPolicy
            {
                TrustMode = X509ChainTrustMode.CustomRootTrust,
                RevocationMode = X509RevocationMode.NoCheck
            };
            policy.CustomTrustStore.Add(ca);
            policy.ApplicationPolicy.Add(new Oid("1.3.6.1.5.5.7.3.2"));
            parameters.CertificateChainPolicy = policy;
        }
        return stream.AuthenticateAsServerAsync(parameters, stop);
    }

    private static async ValueTask<bool> ReceiveExactlyAsync(Socket socket, SslStream? tls, Memory<byte> buffer,
        CancellationToken stop)
    {
        while (!buffer.IsEmpty)
        {
            int read = tls is null ? await socket.ReceiveAsync(buffer, SocketFlags.None, stop) :
                await tls.ReadAsync(buffer, stop);
            if (read == 0) return false;
            buffer = buffer[read..];
        }
        return true;
    }
}

internal readonly record struct Request(Api Api, short Version, int CorrelationId, int BodyOffset,
    string ClientId = "", string ClientHost = "", FetchSession? FetchSession = null, KafkaAuthorization? Access = null,
    ScramDirectory? BootstrapScram = null, RequestQuota? Quota = null, string? AuthenticatedPrincipal = null,
    DelegationTokenCredentials? TokenCredentials = null, bool TokenAuthenticated = false)
{
    internal const short MaxApiVersions = 5;
    internal bool Flexible => IsFlexible(Api, Version);
    internal bool DelaysQuotaResponse => Api == Api.Produce ? Version < 6 : Api == Api.Fetch && Version < 8;
    internal bool Allows(AclResource resource, string name, AclOperation operation) =>
        Access is null || Access.Allows(resource, name, operation);
    internal bool AllowsCluster(AclOperation operation) => Allows(AclResource.Cluster, KafkaAcl.ClusterName, operation);
    internal bool AllowsTopic(string name, AclOperation operation) => Allows(AclResource.Topic, name, operation);
    internal bool AllowsGroup(string name, AclOperation operation) => Allows(AclResource.Group, name, operation);
    internal bool AllowsTransaction(string name, AclOperation operation) => Allows(AclResource.TransactionalId, name, operation);

    internal void RequireCoveredRoute()
    {
        if (Access is null or { SuperUser: true }) return;
        if (Api is Api.ApiVersions or Api.DescribeAcls or Api.CreateAcls or Api.DeleteAcls or
            Api.Metadata or Api.Produce or Api.Fetch or Api.ListOffsets or Api.OffsetForLeaderEpoch or
            Api.InitProducerId or Api.DescribeConfigs or Api.AlterConfigs or Api.IncrementalAlterConfigs or Api.ListConfigResources or
            Api.FindCoordinator or Api.JoinGroup or Api.SyncGroup or Api.Heartbeat or Api.LeaveGroup or
            Api.OffsetCommit or Api.OffsetFetch or Api.DescribeGroups or Api.ListGroups or Api.DeleteGroups or Api.OffsetDelete or
            Api.CreateTopics or Api.DeleteTopics or Api.CreatePartitions or Api.DescribeTopicPartitions or Api.DescribeCluster or
            Api.DescribeQuorum or
            Api.DeleteRecords or Api.DescribeLogDirs or Api.DescribeProducers or Api.AlterPartitionReassignments or
            Api.ListPartitionReassignments or Api.AddPartitionsToTxn or Api.AddOffsetsToTxn or Api.EndTxn or Api.TxnOffsetCommit or
            Api.DescribeTransactions or Api.ListTransactions or Api.ConsumerGroupHeartbeat or Api.ConsumerGroupDescribe or
            Api.ShareGroupHeartbeat or Api.ShareGroupDescribe or Api.ShareFetch or Api.ShareAcknowledge or
            Api.DescribeShareGroupOffsets or Api.AlterShareGroupOffsets or Api.DeleteShareGroupOffsets or
            Api.CreateDelegationToken or Api.RenewDelegationToken or Api.ExpireDelegationToken or Api.DescribeDelegationToken or
            Api.DescribeUserScramCredentials or Api.AlterUserScramCredentials or Api.DescribeClientQuotas or Api.AlterClientQuotas)
            return;
        throw new WireException("This operation requires an ACL superuser");
    }

    private static bool IsFlexible(Api api, short version) => api switch
    {
        Api.ApiVersions => version >= 3,
        Api.Produce => version >= 9,
        Api.Fetch => version >= 12,
        Api.ListOffsets => version >= 6,
        Api.Metadata => version >= 9,
        Api.OffsetCommit => version >= 8,
        Api.OffsetFetch => version >= 6,
        Api.FindCoordinator => version >= 3,
        Api.DescribeGroups => version >= 5,
        Api.ListGroups => version >= 3,
        Api.CreateTopics => version >= 5,
        Api.InitProducerId => version >= 2,
        Api.AddPartitionsToTxn or Api.AddOffsetsToTxn or Api.EndTxn or Api.TxnOffsetCommit => version >= 3,
        Api.SaslAuthenticate => version >= 2,
        Api.CreateDelegationToken or Api.RenewDelegationToken or Api.ExpireDelegationToken or Api.DescribeDelegationToken => version >= 2,
        Api.DescribeClientQuotas or Api.AlterClientQuotas => version >= 1,
        Api.OffsetForLeaderEpoch => version >= 4,
        Api.DeleteTopics => version >= 4,
        Api.DeleteRecords => version >= 2,
        Api.DescribeConfigs => version >= 4,
        Api.DescribeAcls or Api.CreateAcls or Api.DeleteAcls => version >= 2,
        Api.AlterConfigs => version >= 2,
        Api.IncrementalAlterConfigs => version >= 1,
        Api.DescribeLogDirs => version >= 2,
        Api.CreatePartitions or Api.DeleteGroups => version >= 2,
        Api.DescribeQuorum or Api.DescribeCluster or Api.DescribeProducers or Api.DescribeTransactions or Api.ListTransactions or Api.DescribeTopicPartitions or
            Api.ListConfigResources or Api.DescribeUserScramCredentials or Api.AlterUserScramCredentials or
            Api.AlterPartitionReassignments or Api.ListPartitionReassignments or Api.ConsumerGroupHeartbeat or Api.ConsumerGroupDescribe or
            Api.ShareGroupHeartbeat or Api.ShareGroupDescribe or Api.ShareFetch or Api.ShareAcknowledge or
            Api.DescribeShareGroupOffsets or Api.AlterShareGroupOffsets or Api.DeleteShareGroupOffsets => true,
        _ => false
    };

    internal static Request Parse(ReadOnlySpan<byte> frame, bool readQuotaClient = false)
    {
        var r = new WireReader(frame);
        var api = (Api)r.I16();
        short version = r.I16();
        int id = r.I32();
        if (api == Api.ApiVersions && version is < 0 or > MaxApiVersions)
            return new(api, version, id, frame.Length);
        int clientIdLength = r.I16();
        var client = clientIdLength == -1 ? default : r.Take(clientIdLength);
        if (IsFlexible(api, version)) r.Tags();
        string clientId = api is Api.JoinGroup or Api.ConsumerGroupHeartbeat or Api.ShareGroupHeartbeat ||
            readQuotaClient && api is Api.Produce or Api.Fetch or Api.ShareFetch ?
            System.Text.Encoding.UTF8.GetString(client) : "";
        return new(api, version, id, frame.Length - r.Remaining, clientId);
    }
}
