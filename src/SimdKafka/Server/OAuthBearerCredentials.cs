using System.Buffers.Text;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace SimdKafka;

internal sealed class OAuthBearerCredentials
{
    internal const string Mechanism = "OAUTHBEARER";
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private readonly string _issuer;
    private readonly string _audience;
    private readonly int _skew;
    private readonly Dictionary<string, RSAParameters> _keys = new(StringComparer.Ordinal);

    internal OAuthBearerCredentials(string path)
    {
        using var config = ReadFile(path);
        if (!UniqueProperties(config.RootElement)) throw new InvalidDataException("Invalid OAuth configuration");
        _issuer = Required(config.RootElement, "issuer");
        _audience = Required(config.RootElement, "audience");
        if (config.RootElement.TryGetProperty("clockSkewSeconds", out var skew) &&
            (skew.ValueKind != JsonValueKind.Number || !skew.TryGetInt32(out _skew) || _skew is < 0 or > 120))
            throw new InvalidDataException("OAuth clockSkewSeconds must be between 0 and 120");

        path = Path.GetFullPath(Required(config.RootElement, "jwksFile"), Path.GetDirectoryName(Path.GetFullPath(path))!);
        using var jwks = ReadFile(path);
        if (!UniqueProperties(jwks.RootElement) || !jwks.RootElement.TryGetProperty("keys", out var keys) ||
            keys.ValueKind != JsonValueKind.Array || keys.GetArrayLength() > 128)
            throw new InvalidDataException("Invalid OAuth JWKS");
        foreach (var key in keys.EnumerateArray())
        {
            if (!UniqueProperties(key)) throw new InvalidDataException("Invalid OAuth JWK");
            if (Get(key, "kty") != "RSA" || key.TryGetProperty("alg", out _) && Get(key, "alg") != "RS256" ||
                key.TryGetProperty("use", out _) && Get(key, "use") != "sig") continue;
            if (key.TryGetProperty("key_ops", out var operations) &&
                (operations.ValueKind != JsonValueKind.Array || !operations.EnumerateArray().Any(static op =>
                    op.ValueKind == JsonValueKind.String && op.GetString() == "verify"))) continue;
            byte[] modulus = Base64Url.DecodeFromChars(Required(key, "n"));
            byte[] exponent = Base64Url.DecodeFromChars(Required(key, "e"));
            var parameters = new RSAParameters { Modulus = modulus, Exponent = exponent };
            if (modulus.Length is < 256 or > 512 || modulus[0] == 0 || modulus.Length == 256 && modulus[0] < 128 ||
                exponent.Length is < 1 or > 4 || !_keys.TryAdd(Required(key, "kid"), parameters))
                throw new InvalidDataException("Invalid or duplicate OAuth RSA key");
            using var rsa = RSA.Create(parameters);
            if (rsa.KeySize < 2048) throw new InvalidDataException("OAuth RSA keys must have at least 2048 bits");
        }
        if (_keys.Count == 0) throw new InvalidDataException("OAuth JWKS contains no RS256 verification keys");
    }

    internal bool Verify(ReadOnlySpan<byte> frame, out string? user, out long expiresAt)
    {
        user = null;
        expiresAt = 0;
        if (frame.Length is < 20 or > 15 * 1024 || !frame.StartsWith("n,"u8) ||
            !frame.EndsWith("\u0001\u0001"u8)) return false;
        int separator = frame.IndexOf((byte)1);
        if (separator < 3) return false;
        var gs2 = frame[2..separator];
        if (!gs2.SequenceEqual(","u8) && (!gs2.StartsWith("a="u8) || !gs2.EndsWith(","u8))) return false;
        var fields = frame[(separator + 1)..^1];
        ReadOnlySpan<byte> token = default;
        while (!fields.IsEmpty)
        {
            int end = fields.IndexOf((byte)1);
            if (end <= 0) return false;
            var field = fields[..end];
            fields = fields[(end + 1)..];
            if (field.StartsWith("auth="u8))
            {
                if (!token.IsEmpty || !field.StartsWith("auth=Bearer "u8) || field.Length == 12) return false;
                token = field[12..];
            }
            else
            {
                int equal = field.IndexOf((byte)'=');
                if (equal <= 0) return false;
                foreach (byte ch in field[..equal])
                    if (ch is not (>= (byte)'a' and <= (byte)'z') and not (>= (byte)'A' and <= (byte)'Z')) return false;
                foreach (byte ch in field[(equal + 1)..])
                    if (ch is < 0x20 or > 0x7e) return false;
            }
        }
        if (token.IsEmpty) return false;
        int first = token.IndexOf((byte)'.'), last = token.LastIndexOf((byte)'.');
        if (first <= 0 || last <= first + 1 || last == token.Length - 1) return false;
        try
        {
            using var header = JsonDocument.Parse(Base64Url.DecodeFromUtf8(token[..first]));
            var jwtHeader = header.RootElement;
            if (!UniqueProperties(jwtHeader) || Get(jwtHeader, "alg") != "RS256" ||
                jwtHeader.TryGetProperty("crit", out _) || jwtHeader.TryGetProperty("b64", out _) ||
                Get(jwtHeader, "kid") is not { } kid || !_keys.TryGetValue(kid, out var key)) return false;
            Span<byte> signature = stackalloc byte[512];
            if (!Base64Url.TryDecodeFromUtf8(token[(last + 1)..], signature, out int written) ||
                written != key.Modulus!.Length) return false;
            using var rsa = RSA.Create(key);
            if (!rsa.VerifyData(token[..last], signature[..written], HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1))
                return false;

            using var claims = JsonDocument.Parse(Base64Url.DecodeFromUtf8(token[(first + 1)..last]));
            var jwt = claims.RootElement;
            long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            if (!UniqueProperties(jwt) || Get(jwt, "iss") != _issuer || !MatchesAudience(jwt) ||
                !jwt.TryGetProperty("exp", out var expiry) || !Timestamp(expiry, out long exp) || exp <= now ||
                jwt.TryGetProperty("nbf", out var notBefore) && (!Timestamp(notBefore, out long nbf) || nbf > now + _skew) ||
                Get(jwt, "sub") is not { } subject || !ScramDirectory.ValidName(subject) ||
                subject is "*" or "ANONYMOUS" || subject.Any(char.IsControl)) return false;
            if (!gs2.SequenceEqual(","u8) &&
                StrictUtf8.GetString(gs2[2..^1]) != subject.Replace("=", "=3D").Replace(",", "=2C")) return false;

            user = subject;
            expiresAt = exp * 1000;
            return true;
        }
        catch (Exception e) when (e is FormatException or JsonException or CryptographicException or DecoderFallbackException)
        {
            return false;
        }
    }

    private bool MatchesAudience(JsonElement jwt) =>
        jwt.TryGetProperty("aud", out var aud) && (aud.ValueKind == JsonValueKind.String && aud.GetString() == _audience ||
            aud.ValueKind == JsonValueKind.Array && aud.EnumerateArray().Any(a =>
                a.ValueKind == JsonValueKind.String && a.GetString() == _audience));

    private static bool Timestamp(JsonElement value, out long seconds)
    {
        seconds = 0;
        return value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out seconds) && seconds is >= -62135596800 and <= 253402300799;
    }

    private static string? Get(JsonElement value, string name) =>
        value.TryGetProperty(name, out var field) && field.ValueKind == JsonValueKind.String ? field.GetString() : null;

    private static string Required(JsonElement value, string name) => Get(value, name) is { Length: > 0 } field ?
        field : throw new InvalidDataException($"Missing OAuth configuration field {name}");

    private static bool UniqueProperties(JsonElement value)
    {
        if (value.ValueKind != JsonValueKind.Object) return false;
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var field in value.EnumerateObject())
            if (!names.Add(field.Name)) return false;
        return true;
    }

    private static JsonDocument ReadFile(string path)
    {
        using var file = File.OpenRead(path);
        if (file.Length > 1024 * 1024) throw new InvalidDataException("OAuth configuration or JWKS exceeds 1 MiB");
        return JsonDocument.Parse(file, new() { MaxDepth = 16 });
    }
}
