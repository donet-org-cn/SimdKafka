using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using DotNext.Net.Security;

namespace SimdKafka;

internal sealed class ClusterTls : IDisposable
{
    private readonly X509Certificate2 _certificate;
    private readonly X509Certificate2 _authority;
    internal SslOptions Options { get; }
    internal const string PeerName = "simdkafka-raft";

    internal ClusterTls(string certificatePath, string authorityPath, string? passwordPath)
    {
        string? password = passwordPath is null ? null : File.ReadAllText(passwordPath).TrimEnd('\r', '\n');
        _authority = X509CertificateLoader.LoadCertificateFromFile(authorityPath);
        try
        {
            _certificate = X509CertificateLoader.LoadPkcs12FromFile(certificatePath, password,
                OperatingSystem.IsWindows() ? X509KeyStorageFlags.DefaultKeySet : X509KeyStorageFlags.EphemeralKeySet);
            if (!_certificate.HasPrivateKey)
            {
                _certificate.Dispose();
                throw new ArgumentException("The cluster TLS certificate requires a private key");
            }
        }
        catch
        {
            _authority.Dispose();
            throw;
        }

        const SslProtocols protocol = SslProtocols.Tls12 | SslProtocols.Tls13;
        Options = new SslOptions
        {
            ServerOptions = new SslServerAuthenticationOptions
            {
                ServerCertificate = _certificate,
                ClientCertificateRequired = true,
                EnabledSslProtocols = protocol,
                CertificateChainPolicy = ChainPolicy("1.3.6.1.5.5.7.3.2")
            },
            ClientOptions = new SslClientAuthenticationOptions
            {
                TargetHost = PeerName,
                ClientCertificates = new X509CertificateCollection { _certificate },
                EnabledSslProtocols = protocol,
                CertificateChainPolicy = ChainPolicy("1.3.6.1.5.5.7.3.1")
            }
        };
    }

    internal static SslOptions BindToGroup(SslOptions options, Guid groupId)
    {
        string groupName = $"{groupId:N}.{PeerName}";
        var server = options.ServerOptions ?? throw new ArgumentException("Replica slot reuse requires server TLS options");
        var client = options.ClientOptions ?? throw new ArgumentException("Replica slot reuse requires client TLS options");
        var certificate = server.ServerCertificate ?? throw new ArgumentException("Replica slot reuse requires a server certificate");
        return new SslOptions
        {
            ServerOptions = new SslServerAuthenticationOptions
            {
                ServerCertificateSelectionCallback = (_, name) => string.Equals(name, groupName, StringComparison.Ordinal)
                    ? certificate : throw new AuthenticationException("Invalid Raft group in TLS server name"),
                AllowTlsResume = false,
                ClientCertificateRequired = true,
                EnabledSslProtocols = server.EnabledSslProtocols,
                CertificateChainPolicy = server.CertificateChainPolicy
            },
            ClientOptions = new SslClientAuthenticationOptions
            {
                TargetHost = groupName,
                AllowTlsResume = false,
                ClientCertificates = client.ClientCertificates,
                EnabledSslProtocols = client.EnabledSslProtocols,
                CertificateChainPolicy = client.CertificateChainPolicy,
                RemoteCertificateValidationCallback = static (_, leaf, _, errors) =>
                    (errors & ~SslPolicyErrors.RemoteCertificateNameMismatch) == SslPolicyErrors.None &&
                    leaf is X509Certificate2 x509 && x509.MatchesHostname(PeerName, allowWildcards: false, allowCommonName: false)
            }
        };
    }

    private X509ChainPolicy ChainPolicy(string usage)
    {
        var policy = new X509ChainPolicy
        {
            TrustMode = X509ChainTrustMode.CustomRootTrust,
            RevocationMode = X509RevocationMode.NoCheck,
            DisableCertificateDownloads = true
        };
        policy.CustomTrustStore.Add(_authority);
        policy.ApplicationPolicy.Add(new Oid(usage));
        return policy;
    }

    public void Dispose()
    {
        _certificate.Dispose();
        _authority.Dispose();
    }
}
