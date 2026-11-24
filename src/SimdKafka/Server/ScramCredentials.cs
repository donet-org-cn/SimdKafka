using System.Globalization;
using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace SimdKafka;

internal sealed class ScramCredentials
{
    internal const string Sha256 = "SCRAM-SHA-256";
    internal const string Sha512 = "SCRAM-SHA-512";
    private readonly byte[] _dummyKey = RandomNumberGenerator.GetBytes(64);
    private readonly byte[] _dummyServer = RandomNumberGenerator.GetBytes(64);
    private readonly byte[] _dummySalt = RandomNumberGenerator.GetBytes(32);

    internal sealed record Verifier(int Iterations, byte[] Salt, byte[] StoredKey, byte[] ServerKey)
    {
        internal bool SameAs(Verifier other) => ReferenceEquals(this, other) || Iterations == other.Iterations &&
            Salt.AsSpan().SequenceEqual(other.Salt) && CryptographicOperations.FixedTimeEquals(StoredKey, other.StoredKey) &&
            CryptographicOperations.FixedTimeEquals(ServerKey, other.ServerKey);
    }

    internal ScramCredentials(string? path, bool dynamic, bool tokenAuth = false)
    {
        Mechanisms = dynamic || tokenAuth ? [Sha256, Sha512] : [];
        if (path is null) return;
        using var file = File.OpenRead(path);
        if (file.Length > 1024 * 1024) throw new InvalidDataException("SCRAM credentials file exceeds 1 MiB");
        using var doc = JsonDocument.Parse(file);
        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            throw new InvalidDataException("SCRAM credentials must be an object indexed by username");
        var users = ImmutableDictionary.CreateBuilder<string, ScramUser>(StringComparer.Ordinal);
        var mechanisms = new HashSet<string>(StringComparer.Ordinal);
        foreach (var user in doc.RootElement.EnumerateObject())
        {
            if (!ScramDirectory.ValidName(user.Name) || users.ContainsKey(user.Name) || user.Value.ValueKind != JsonValueKind.Object)
                throw new InvalidDataException("Invalid SCRAM username or credential object");
            var credentials = new ScramUser();
            foreach (var entry in user.Value.EnumerateObject())
            {
                if (entry.Name is not (Sha256 or Sha512) || entry.Value.ValueKind != JsonValueKind.Object ||
                    !entry.Value.TryGetProperty("iterations", out var count) || count.ValueKind != JsonValueKind.Number ||
                    !count.TryGetInt32(out int iterations) || iterations is < 4096 or > 1_000_000)
                    throw new InvalidDataException("Invalid SCRAM mechanism or iteration count");
                sbyte mechanism = MechanismId(entry.Name);
                int width = ScramDirectory.KeySize(mechanism);
                var salt = Decode(entry.Value, "salt", 16, 64);
                var stored = Decode(entry.Value, "storedKey", width, width);
                var server = Decode(entry.Value, "serverKey", width, width);
                if (credentials.Find(mechanism) is not null)
                    throw new InvalidDataException("Duplicate SCRAM verifier");
                credentials = credentials.Set(mechanism, new(iterations, salt, stored, server));
                mechanisms.Add(entry.Name);
            }
            users.Add(user.Name, credentials);
        }
        if (mechanisms.Count == 0) throw new InvalidDataException("SCRAM credentials file is empty");
        Bootstrap = new() { Users = users.ToImmutable() };
        if (!dynamic && !tokenAuth) Mechanisms = mechanisms.Order(StringComparer.Ordinal).ToArray();
    }

    internal string[] Mechanisms { get; }
    internal ScramDirectory Bootstrap { get; } = ScramDirectory.Empty;
    internal static sbyte MechanismId(string name) => name switch { Sha256 => 1, Sha512 => 2, _ => 0 };
    internal Verifier? Find(string user, string mechanism, ScramDirectory? dynamic)
    {
        sbyte id = MechanismId(mechanism);
        return Bootstrap.Find(user, id) ?? dynamic?.Find(user, id);
    }

    private static byte[] Decode(JsonElement entry, string key, int minimum, int maximum)
    {
        if (!entry.TryGetProperty(key, out var field) || field.ValueKind != JsonValueKind.String ||
            !field.TryGetBytesFromBase64(out var value) || value.Length < minimum || value.Length > maximum)
            throw new InvalidDataException($"Invalid SCRAM {key}");
        return value;
    }

    internal Verifier FindOrDummy(string user, string mechanism, ScramDirectory? dynamic, out bool known, bool dummyOnly = false)
    {
        var verifier = dummyOnly ? null : Find(user, mechanism, dynamic);
        known = verifier is not null;
        if (verifier is not null) return verifier;
        byte[] salt = HMACSHA256.HashData(_dummySalt, Encoding.UTF8.GetBytes(user));
        return new(4096, salt[..16], _dummyKey, _dummyServer);
    }
}

internal sealed class ScramExchange(ScramCredentials credentials, string mechanism, DelegationTokenCredentials? tokens)
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private ScramCredentials.Verifier? _verifier;
    private bool _known;
    private string _clientFirst = "", _serverFirst = "", _channelBinding = "", _nonce = "";
    internal string? User { get; private set; }
    internal string? TokenId { get; private set; }

    internal bool Next(ReadOnlySpan<byte> token, ScramDirectory? dynamic, TokenDirectory? directory, out byte[] response, out bool authenticated)
    {
        response = [];
        authenticated = false;
        if (token.Length is < 1 or > 4096) return false;
        string text;
        try { text = StrictUtf8.GetString(token); }
        catch (DecoderFallbackException) { return false; }
        if (text.Any(char.IsControl)) return false;
        if (_verifier is null) return Challenge(text, dynamic, directory, out response);
        if (TokenId is { } id)
        {
            var current = directory?.Entries.GetValueOrDefault(id);
            if (current is null || current.Owner != "User:" + User || !current.Active(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()))
                return false;
        }
        else if (_known)
        {
            var current = credentials.Find(User!, mechanism, dynamic);
            if (current is null || !_verifier.SameAs(current)) return false;
        }
        authenticated = Verify(text, _verifier, out response);
        return authenticated;
    }

    private bool Challenge(string first, ScramDirectory? dynamic, TokenDirectory? directory, out byte[] response)
    {
        response = [];
        if (!(first.StartsWith("n,,", StringComparison.Ordinal) || first.StartsWith("y,,", StringComparison.Ordinal)))
            return false;
        string bare = first[3..];
        string[] fields = bare.Split(',');
        if (fields.Length < 2 || !fields[0].StartsWith("n=", StringComparison.Ordinal) ||
            !fields[1].StartsWith("r=", StringComparison.Ordinal) || !DecodeName(fields[0].AsSpan(2), out string user))
            return false;
        string clientNonce = fields[1][2..];
        if (clientNonce.Length is 0 or > 1024 || clientNonce.Any(static ch => ch is < '!' or > '~')) return false;

        User = user;
        var extensions = fields.AsSpan(2);
        bool tokenAuth = extensions.Contains("tokenauth=true");
        foreach (string extension in extensions)
            if (extension.StartsWith("tokenauth=", StringComparison.Ordinal) &&
                extension != (tokenAuth ? "tokenauth=true" : "tokenauth=false") ||
                extension.StartsWith("m=", StringComparison.Ordinal)) return false;
        if (tokenAuth)
        {
            TokenId = user;
            var token = directory?.Entries.GetValueOrDefault(user);
            if (token is not null && token.Active(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()))
                _verifier = tokens?.Verifier(token, mechanism);
            _known = _verifier is not null;
            if (_known) User = token!.Owner[5..];
            else _verifier = credentials.FindOrDummy(user, mechanism, null, out _, dummyOnly: true);
        }
        else _verifier = credentials.FindOrDummy(user, mechanism, dynamic, out _known);
        _channelBinding = first[0] == 'n' ? "c=biws" : "c=eSws";
        _clientFirst = bare;
        _nonce = clientNonce + Convert.ToBase64String(RandomNumberGenerator.GetBytes(24));
        _serverFirst = string.Concat("r=", _nonce, ",s=", Convert.ToBase64String(_verifier!.Salt),
            ",i=", _verifier.Iterations.ToString(CultureInfo.InvariantCulture));
        response = Encoding.UTF8.GetBytes(_serverFirst);
        return true;
    }

    private bool Verify(string final, ScramCredentials.Verifier verifier, out byte[] response)
    {
        response = [];
        int proofStart = final.LastIndexOf(",p=", StringComparison.Ordinal);
        if (proofStart < 0) return false;
        string withoutProof = final[..proofStart];
        if (!withoutProof.Equals(string.Concat(_channelBinding, ",r=", _nonce), StringComparison.Ordinal)) return false;

        bool sha256 = mechanism == ScramCredentials.Sha256;
        int width = sha256 ? 32 : 64;
        Span<byte> proof = stackalloc byte[width];
        Span<byte> signature = stackalloc byte[width];
        Span<byte> stored = stackalloc byte[width];
        try
        {
            if (!Convert.TryFromBase64Chars(final.AsSpan(proofStart + 3), proof, out int written) || written != width) return false;
            byte[] message = Encoding.UTF8.GetBytes(string.Concat(_clientFirst, ",", _serverFirst, ",", withoutProof));
            var key = verifier.StoredKey.AsSpan(0, width);
            if (sha256) HMACSHA256.HashData(key, message, signature);
            else HMACSHA512.HashData(key, message, signature);
            for (int i = 0; i < width; i++) proof[i] ^= signature[i];
            if (sha256) SHA256.HashData(proof, stored);
            else SHA512.HashData(proof, stored);
            bool valid = CryptographicOperations.FixedTimeEquals(stored, key);
            if (!valid || !_known) return false;

            var server = verifier.ServerKey.AsSpan(0, width);
            if (sha256) HMACSHA256.HashData(server, message, signature);
            else HMACSHA512.HashData(server, message, signature);
            response = Encoding.UTF8.GetBytes("v=" + Convert.ToBase64String(signature));
            return true;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(proof);
            CryptographicOperations.ZeroMemory(signature);
            CryptographicOperations.ZeroMemory(stored);
        }
    }

    private static bool DecodeName(ReadOnlySpan<char> input, out string name)
    {
        var output = new StringBuilder(input.Length);
        for (int i = 0; i < input.Length; i++)
        {
            char value = input[i];
            if (value == '=')
            {
                if (i + 2 >= input.Length) { name = ""; return false; }
                var escape = input.Slice(i + 1, 2);
                if (escape.SequenceEqual("2C")) value = ',';
                else if (escape.SequenceEqual("3D")) value = '=';
                else { name = ""; return false; }
                i += 2;
            }
            output.Append(value);
        }
        name = output.ToString();
        return Encoding.UTF8.GetByteCount(name) is > 0 and <= 512;
    }
}
