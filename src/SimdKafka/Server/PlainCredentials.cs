using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace SimdKafka;

internal sealed class PlainCredentials
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private readonly byte[] _key = RandomNumberGenerator.GetBytes(32);
    private readonly byte[] _unknown = RandomNumberGenerator.GetBytes(32);
    private readonly Dictionary<string, byte[]> _users = new(StringComparer.Ordinal);

    internal PlainCredentials(string path)
    {
        using var file = File.OpenRead(path);
        if (file.Length > 1024 * 1024) throw new InvalidDataException("SASL credentials file exceeds 1 MiB");
        using var doc = JsonDocument.Parse(file);
        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            throw new InvalidDataException("SASL credentials must be a JSON object");
        foreach (var user in doc.RootElement.EnumerateObject())
        {
            if (user.Value.ValueKind != JsonValueKind.String)
                throw new InvalidDataException("SASL credentials require string passwords");
            string password = user.Value.GetString()!;
            if (user.Name.Length == 0 || user.Name.Contains('\0') || password.Length == 0 ||
                password.Contains('\0') || Encoding.UTF8.GetByteCount(user.Name) > 512 ||
                Encoding.UTF8.GetByteCount(password) > 3072 || _users.ContainsKey(user.Name))
                throw new InvalidDataException("Invalid or duplicate SASL credential");
            byte[] secret = Encoding.UTF8.GetBytes(password);
            try { _users.Add(user.Name, HMACSHA256.HashData(_key, secret)); }
            finally { CryptographicOperations.ZeroMemory(secret); }
        }
        if (_users.Count == 0) throw new InvalidDataException("SASL credentials file is empty");
    }

    internal bool Verify(ReadOnlySpan<byte> token, out string? authenticatedUser)
    {
        authenticatedUser = null;
        if (token.Length is < 3 or > 4096) return false;
        int first = token.IndexOf((byte)0);
        if (first < 0) return false;
        var login = token[(first + 1)..];
        int next = login.IndexOf((byte)0);
        if (next is < 1 or > 512) return false;
        var password = login[(next + 1)..];
        if (password.IsEmpty || password.IndexOf((byte)0) >= 0 ||
            (first != 0 && !token[..first].SequenceEqual(login[..next])))
            return false;
        try
        {
            string user = StrictUtf8.GetString(login[..next]);
            bool known = _users.TryGetValue(user, out var expected);
            Span<byte> actual = stackalloc byte[32];
            HMACSHA256.HashData(_key, password, actual);
            bool equal = CryptographicOperations.FixedTimeEquals(actual, expected ?? _unknown);
            if (!equal || !known) return false;
            authenticatedUser = user;
            return true;
        }
        catch (DecoderFallbackException)
        {
            return false;
        }
    }
}

internal sealed class SaslConnection(PlainCredentials? plain, ScramCredentials? scram, OAuthBearerCredentials? oauth,
    DelegationTokenCredentials? tokens)
{
    private bool _plainSelected, _oauthSelected;
    private ScramExchange? _exchange;
    private string? _reauthenticatedUser;
    internal bool Authenticated { get; private set; }
    internal bool UsesRawTokens { get; private set; }
    internal bool UsesScram => _exchange is not null;
    internal string? User { get; private set; }
    internal string? TokenId => Authenticated ? _exchange?.TokenId : null;
    internal long OAuthExpiry { get; private set; }
    internal bool CanReauthenticate => Authenticated && _oauthSelected && !UsesRawTokens;
    internal bool NeedsTokens(ReadOnlySpan<byte> body) => tokens is not null && _exchange is not null &&
        (_exchange.TokenId is not null || body.IndexOf(",tokenauth=true"u8) >= 0);

    internal bool Accept(Request request, ReadOnlySpan<byte> body, WireWriter output, ScramDirectory? current, TokenDirectory? directory)
    {
        var r = new WireReader(body);
        if (request.Api == Api.SaslHandshake && request.Version is 0 or 1)
        {
            string mechanism = r.String();
            r.End();
            if (Authenticated)
            {
                if (!CanReauthenticate || mechanism != OAuthBearerCredentials.Mechanism || request.Version == 0)
                    throw new WireException("OAuth reauthentication must use the original mechanism and framed SASL");
                _reauthenticatedUser = User;
                Authenticated = false;
            }
            else if (_reauthenticatedUser is not null)
                throw new WireException("OAuth reauthentication already in progress");
            _plainSelected = plain is not null && mechanism == "PLAIN";
            _oauthSelected = oauth is not null && mechanism == OAuthBearerCredentials.Mechanism;
            _exchange = scram is not null && scram.Mechanisms.Contains(mechanism, StringComparer.Ordinal) ?
                new(scram, mechanism, tokens) : null;
            bool selected = _plainSelected || _oauthSelected || _exchange is not null;
            UsesRawTokens = selected && request.Version == 0;
            output.Write(selected ? Error.None : Error.UnsupportedSaslMechanism);
            output.I32((plain is null ? 0 : 1) + (oauth is null ? 0 : 1) + (scram?.Mechanisms.Length ?? 0));
            if (plain is not null) output.String("PLAIN");
            if (oauth is not null) output.String(OAuthBearerCredentials.Mechanism);
            if (scram is not null)
                foreach (string supported in scram.Mechanisms) output.String(supported);
            return true;
        }
        if (request.Api == Api.SaslAuthenticate && request.Version is >= 0 and <= 2)
        {
            bool compact = request.Flexible;
            var token = compact ? r.Take(r.ArrayLength(true)) : r.Bytes();
            r.EndStruct(compact);
            r.End();
            bool accepted = Authenticate(token, current, directory, out var response);
            output.Write(accepted ? Error.None : Error.SaslAuthenticationFailed);
            output.Text(accepted ? null : "Authentication failed", compact);
            output.Bytes(response, compact);
            if (request.Version >= 1)
                output.I64(accepted && _oauthSelected ?
                    Math.Clamp(OAuthExpiry - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 1, int.MaxValue) : 0);
            output.EndStruct(compact);
            return accepted;
        }
        throw new WireException("SASL authentication required");
    }

    internal bool AcceptRaw(ReadOnlySpan<byte> token, WireWriter output, ScramDirectory? current, TokenDirectory? directory)
    {
        if (!Authenticate(token, current, directory, out var response)) return false;
        output.Bytes(response);
        return true;
    }

    private bool Authenticate(ReadOnlySpan<byte> token, ScramDirectory? current, TokenDirectory? directory, out byte[] response)
    {
        response = [];
        if (_plainSelected)
        {
            if (!plain!.Verify(token, out string? user)) return false;
            User = user;
            Authenticated = true;
            return true;
        }
        if (_oauthSelected)
        {
            if (!oauth!.Verify(token, out string? user, out long expiresAt) ||
                _reauthenticatedUser is not null && _reauthenticatedUser != user) return false;
            User = user;
            OAuthExpiry = expiresAt;
            _reauthenticatedUser = null;
            Authenticated = true;
            return true;
        }
        if (_exchange is null || !_exchange.Next(token, current, directory, out response, out bool complete)) return false;
        Authenticated = complete;
        if (complete) User = _exchange.User;
        return true;
    }
}
