using System.Buffers.Text;
using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;

namespace SimdKafka;

internal sealed record DelegationToken(string Id, string Owner, string Requester, string[] Renewers,
    long Issued, long Expiry, long Maximum, string Fingerprint)
{
    internal bool Active(long now) => now < Expiry && now < Maximum;
    internal bool ManagedBy(string principal) => Owner == principal || Array.IndexOf(Renewers, principal) >= 0;

    internal static bool ValidPrincipal(string principal) => KafkaAcl.ValidPrincipal(principal) && ScramDirectory.ValidName(principal[5..]) &&
        principal is not ("User:*" or "User:ANONYMOUS");
}

internal enum TokenAction : byte { Create, Renew, Expire }
internal sealed record TokenChange(TokenAction Action, string Id, string Principal, long Now, long Period = 0,
    DelegationToken? Creation = null);

internal sealed record TokenDirectory
{
    internal const long DefaultExpiry = 24 * 60 * 60 * 1000;
    internal const long DefaultMaximum = 7 * DefaultExpiry;
    internal const int MaxRenewers = 16;
    internal static TokenDirectory Empty { get; } = new();
    public ImmutableDictionary<string, DelegationToken> Entries { get; init; } =
        ImmutableDictionary.Create<string, DelegationToken>(StringComparer.Ordinal);

    internal (MetadataResult Result, TokenDirectory Next) Apply(TokenChange change)
    {
        if (change.Now is < 1 or > 253402300799000 || !DelegationToken.ValidPrincipal(change.Principal))
            return (new(Error.InvalidRequest), this);
        if (change.Action == TokenAction.Create)
        {
            if (change.Creation is not { } token || token.Id != change.Id || token.Id.Length != 22 ||
                token.Fingerprint.Length != 64 || token.Issued != change.Now || token.Requester != change.Principal ||
                !DelegationToken.ValidPrincipal(token.Owner) || token.Renewers.Length > MaxRenewers ||
                token.Renewers.Any(static principal => !DelegationToken.ValidPrincipal(principal)) ||
                token.Maximum <= change.Now || token.Maximum > change.Now + DefaultMaximum ||
                token.Expiry != Math.Min(change.Now + DefaultExpiry, token.Maximum) || Entries.ContainsKey(token.Id))
                return (new(Error.InvalidRequest), this);
            var active = Entries.RemoveRange(Entries.Values.Where(t => !t.Active(change.Now)).Select(t => t.Id));
            if (active.Count >= 1024) return (new(Error.InvalidRequest), this);
            return (new(Error.None, TokenExpiry: token.Expiry), this with { Entries = active.Add(token.Id, token) });
        }
        if (change.Action is not (TokenAction.Renew or TokenAction.Expire)) return (new(Error.InvalidRequest), this);
        if (!Entries.TryGetValue(change.Id, out var existing)) return (new(Error.DelegationTokenNotFound), this);
        if (!existing.ManagedBy(change.Principal)) return (new(Error.DelegationTokenOwnerMismatch), this);
        if (change.Action == TokenAction.Expire && change.Period < 0)
            return (new(Error.None, TokenExpiry: change.Now), this with { Entries = Entries.Remove(change.Id) });
        if (!existing.Active(change.Now)) return (new(Error.DelegationTokenExpired), this);

        long period = change.Action == TokenAction.Renew ?
            change.Period <= 0 ? DefaultExpiry : Math.Min(change.Period, DefaultExpiry) : change.Period;
        long expiry = change.Now + Math.Min(period, existing.Maximum - change.Now);
        return (new(Error.None, TokenExpiry: expiry), this with { Entries = Entries.SetItem(change.Id, existing with { Expiry = expiry }) });
    }
}

internal sealed class DelegationTokenCredentials
{
    internal const int HmacSize = 64;
    private readonly byte[] _key;

    internal DelegationTokenCredentials(string path)
    {
        using var file = File.OpenRead(path);
        if (file.Length > 1024) throw new InvalidDataException("Delegation token secret file exceeds 1 KiB");
        Span<byte> encoded = stackalloc byte[(int)file.Length];
        try
        {
            file.ReadExactly(encoded);
            var input = encoded.TrimEnd((byte)'\n').TrimEnd((byte)'\r');
            if (Base64.DecodeFromUtf8InPlace(input, out int length) != System.Buffers.OperationStatus.Done ||
                length is < 32 or > 128) throw new InvalidDataException("Delegation token secret must contain 32–128 Base64-encoded bytes");
            _key = input[..length].ToArray();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(encoded);
        }
    }

    internal void Hmac(string id, Span<byte> destination)
    {
        Span<byte> encoded = stackalloc byte[22];
        int length = Encoding.UTF8.GetBytes(id, encoded);
        HMACSHA512.HashData(_key, encoded[..length], destination);
    }

    internal DelegationToken Issue(string owner, string requester, string[] renewers, long now, long requestedLifetime)
    {
        Span<byte> entropy = stackalloc byte[16];
        RandomNumberGenerator.Fill(entropy);
        string id = Base64Url.EncodeToString(entropy);
        Span<byte> hmac = stackalloc byte[HmacSize];
        try
        {
            Hmac(id, hmac);
            long maximum = now + (requestedLifetime > 0 ? Math.Min(requestedLifetime, TokenDirectory.DefaultMaximum) : TokenDirectory.DefaultMaximum);
            return new(id, owner, requester, renewers, now, Math.Min(maximum, now + TokenDirectory.DefaultExpiry), maximum,
                Convert.ToHexString(SHA256.HashData(hmac)));
        }
        finally { CryptographicOperations.ZeroMemory(hmac); }
    }

    internal bool TryHmac(DelegationToken token, Span<byte> hmac)
    {
        Hmac(token.Id, hmac);
        return string.Equals(token.Fingerprint, Convert.ToHexString(SHA256.HashData(hmac)), StringComparison.Ordinal);
    }

    internal ScramCredentials.Verifier? Verifier(DelegationToken token, string mechanism)
    {
        Span<byte> hmac = stackalloc byte[HmacSize];
        Span<byte> password = stackalloc byte[Base64.GetMaxEncodedToUtf8Length(HmacSize)];
        Span<byte> salt = stackalloc byte[16];
        sbyte id = ScramCredentials.MechanismId(mechanism);
        Span<byte> salted = stackalloc byte[ScramDirectory.KeySize(id)];
        try
        {
            if (!TryHmac(token, hmac)) return null;
            Base64.EncodeToUtf8(hmac, password, out _, out int length);
            RandomNumberGenerator.Fill(salt);
            var hash = id == 1 ? HashAlgorithmName.SHA256 : HashAlgorithmName.SHA512;
            Rfc2898DeriveBytes.Pbkdf2(password[..length], salt, salted, 4096, hash);
            return ScramDirectory.Derive(id, 4096, salt, salted);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(hmac);
            CryptographicOperations.ZeroMemory(password);
            CryptographicOperations.ZeroMemory(salted);
        }
    }
}
