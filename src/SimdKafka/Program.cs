using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using SimdKafka;

try
{
    if (args.Contains("--help"))
    {
        Console.WriteLine("SimdKafka --listen 127.0.0.1:9092 --advertise-host localhost --advertise-port 9092 --data data --partitions 1 [--no-auto-create] [--unsafe-no-fsync] [--tls-pfx FILE [--tls-password-file FILE] [--tls-client-ca FILE] [--sasl-plain-users FILE] [--sasl-scram-users FILE]]");
        Console.WriteLine("Permissions: --acl-super-user User:NAME (repeat for multiple administrators; requires SASL or mutual TLS)");
        Console.WriteLine("Dynamic SCRAM: --sasl-dynamic-scram (requires TLS and ACLs; cluster JSON: dynamicScram=true)");
        Console.WriteLine("OAuth bearer: --sasl-oauth-config FILE (requires TLS)");
        Console.WriteLine("Delegation tokens: --delegation-token-secret-file FILE (requires TLS and a bootstrap SASL mechanism)");
        Console.WriteLine("Standalone remote log tier: --tiered-storage-dir DIR [--tier-segment-bytes BYTES] [--local-retention-bytes BYTES] [--local-retention-ms MS]");
        Console.WriteLine("Cluster mode: --cluster-config FILE --node-id ID [--cluster-pfx FILE --cluster-ca FILE [--cluster-password-file FILE]]");
        return 0;
    }
    var options = BrokerOptions.Parse(args);
    using var stop = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        stop.Cancel();
    };
    using var sigterm = OperatingSystem.IsWindows() ? null :
        PosixSignalRegistration.Create(PosixSignal.SIGTERM, signal =>
        {
            signal.Cancel = true;
            stop.Cancel();
        });
    if (options.ClusterConfig is { } config)
    {
        using var tls = options.ClusterCertificate is { } certificate ?
            new ClusterTls(certificate, options.ClusterCa!, options.ClusterPasswordFile) : null;
        await using var cluster = await BrokerCluster.OpenAsync(options, config, options.BrokerId!.Value, stop.Token, tls?.Options);
        await cluster.ServeKafkaAsync(stop.Token);
    }
    else
    {
        using var catalog = new Catalog(options);
        await new Broker(options, catalog).RunAsync(stop.Token);
    }
    return 0;
}
catch (Exception e) when (e is ArgumentException or IOException or InvalidDataException or System.Net.Sockets.SocketException or
    UnauthorizedAccessException or CryptographicException or System.Text.Json.JsonException or FormatException or OverflowException)
{
    Console.Error.WriteLine($"SimdKafka: {e.Message}");
    return 1;
}

namespace SimdKafka
{
    internal sealed record BrokerOptions(IPEndPoint Listen, string Host, int Port, string Data,
        int DefaultPartitions, bool AutoCreate, bool Flush,
        string? TlsCertificate = null, string? TlsPasswordFile = null, string? ClientCa = null, string? SaslUsers = null,
        string? ClusterConfig = null, int? BrokerId = null, string? ClusterCertificate = null, string? ClusterCa = null,
        string? ClusterPasswordFile = null, string? ScramUsers = null, string[]? AclSuperUsers = null, bool DynamicScram = false,
        string? TierDirectory = null, int TierSegmentBytes = 16 * 1024 * 1024, long LocalRetentionBytes = 64 * 1024 * 1024,
        string? OAuthConfig = null, string? DelegationTokenSecretFile = null, long LocalRetentionMs = -1)
    {
        internal const int NodeId = 1;
        internal const int MaxFrameBytes = 32 * 1024 * 1024;
        internal const int MaxFetchBytes = 16 * 1024 * 1024;
        internal const int MaxBatchBytes = 8 * 1024 * 1024;
        internal const int MaxPartitions = 1024;
        internal const int MaxPartitionsPerTopic = 128;
        internal const int MaxConnections = 256;
        internal bool SaslEnabled => SaslUsers is not null || ScramUsers is not null || DynamicScram || OAuthConfig is not null;

        internal static BrokerOptions Parse(string[] args)
        {
            IPEndPoint listen = new(IPAddress.Loopback, 9092);
            string? host = null;
            int? port = null;
            string? certificate = null, passwordFile = null, clientCa = null, saslUsers = null, scramUsers = null;
            string? oauthConfig = null, tokenSecret = null;
            string? clusterConfig = null, clusterCertificate = null, clusterCa = null, clusterPassword = null;
            int? brokerId = null;
            string data = "data";
            string? tierDirectory = null;
            int tierSegment = 16 * 1024 * 1024;
            long localRetention = 64 * 1024 * 1024;
            long localRetentionMs = -1;
            int partitions = 1;
            bool auto = true, flush = true, dynamicScram = false;
            var superUsers = new HashSet<string>(StringComparer.Ordinal);
            for (int i = 0; i < args.Length; i++)
            {
                string arg = args[i];
                string Next() => ++i < args.Length ? args[i] : throw new ArgumentException($"Missing value for {arg}");
                switch (arg)
                {
                    case "--listen": listen = IPEndPoint.Parse(Next()); break;
                    case "--advertise-host": host = Next(); break;
                    case "--advertise-port": port = int.Parse(Next()); break;
                    case "--data": data = Next(); break;
                    case "--tiered-storage-dir": tierDirectory = Next(); break;
                    case "--tier-segment-bytes": tierSegment = int.Parse(Next()); break;
                    case "--local-retention-bytes": localRetention = long.Parse(Next()); break;
                    case "--local-retention-ms": localRetentionMs = long.Parse(Next()); break;
                    case "--partitions": partitions = int.Parse(Next()); break;
                    case "--no-auto-create": auto = false; break;
                    case "--unsafe-no-fsync": flush = false; break;
                    case "--tls-pfx": certificate = Next(); break;
                    case "--tls-password-file": passwordFile = Next(); break;
                    case "--tls-client-ca": clientCa = Next(); break;
                    case "--sasl-plain-users": saslUsers = Next(); break;
                    case "--sasl-scram-users": scramUsers = Next(); break;
                    case "--sasl-dynamic-scram": dynamicScram = true; break;
                    case "--sasl-oauth-config": oauthConfig = Next(); break;
                    case "--delegation-token-secret-file": tokenSecret = Next(); break;
                    case "--acl-super-user":
                        string principal = Next();
                        if (!KafkaAcl.ValidPrincipal(principal) || principal == "User:*" || principal == "User:ANONYMOUS")
                            throw new ArgumentException("An ACL superuser must be an exact authenticated User principal");
                        superUsers.Add(principal);
                        break;
                    case "--cluster-config": clusterConfig = Next(); break;
                    case "--node-id": brokerId = int.Parse(Next()); break;
                    case "--cluster-pfx": clusterCertificate = Next(); break;
                    case "--cluster-ca": clusterCa = Next(); break;
                    case "--cluster-password-file": clusterPassword = Next(); break;
                    default: throw new ArgumentException($"Unknown option {arg}");
                }
            }
            if (host is null)
            {
                if (listen.Address.Equals(IPAddress.Any) || listen.Address.Equals(IPAddress.IPv6Any))
                    throw new ArgumentException("A wildcard listener requires --advertise-host");
                host = listen.Address.ToString();
            }
            if (listen.Port == 0 || port is < 1 or > 65535 || string.IsNullOrWhiteSpace(host) || host.Length > 253 ||
                partitions is < 1 or > MaxPartitionsPerTopic)
                throw new ArgumentException("Invalid listener, advertised address or partition count");
            bool sasl = saslUsers is not null || scramUsers is not null || dynamicScram || oauthConfig is not null;
            if (certificate is null && (passwordFile is not null || clientCa is not null || sasl || tokenSecret is not null))
                throw new ArgumentException("TLS password, client CA and SASL options require --tls-pfx");
            if (tokenSecret is not null && (!sasl || !flush || clusterConfig is not null && clusterCertificate is null))
                throw new ArgumentException("Delegation tokens require SASL, durable storage and internal TLS in cluster mode");
            if (superUsers.Count != 0 && clientCa is null && !sasl)
                throw new ArgumentException("ACLs require SASL or mutual TLS on the client listener");
            if (dynamicScram && superUsers.Count == 0)
                throw new ArgumentException("Dynamic SCRAM requires an ACL superuser");
            data = Path.GetFullPath(data);
            if (tierDirectory is not null)
            {
                tierDirectory = Path.GetFullPath(tierDirectory);
                if (clusterConfig is not null || !flush) throw new ArgumentException("Remote tier requires a durable standalone broker");
                if (tierSegment is < 4096 or > 256 * 1024 * 1024 || localRetention < -1 || localRetentionMs < -1 ||
                    ContainsPath(data, tierDirectory) || ContainsPath(tierDirectory, data))
                    throw new ArgumentException("Invalid remote tier paths or byte limits");
            }
            if ((clusterConfig is null) != (brokerId is null) || brokerId is < 0 ||
                (clusterConfig is null && (clusterCertificate is not null || clusterCa is not null || clusterPassword is not null)) ||
                (clusterCertificate is null) != (clusterCa is null) || (clusterCertificate is null && clusterPassword is not null))
                throw new ArgumentException("Cluster mode requires --cluster-config and --node-id. Internal TLS requires a PFX and CA");
            return new(listen, host, port ?? listen.Port, data, partitions, auto, flush,
                certificate, passwordFile, clientCa, saslUsers, clusterConfig, brokerId, clusterCertificate, clusterCa, clusterPassword, scramUsers,
                superUsers.Count == 0 ? null : [..superUsers], dynamicScram, tierDirectory, tierSegment, localRetention, oauthConfig, tokenSecret,
                localRetentionMs);
        }

        private static bool ContainsPath(string root, string candidate)
        {
            string relative = Path.GetRelativePath(root, candidate);
            return !Path.IsPathRooted(relative) && relative != ".." &&
                !relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal);
        }
    }
}
