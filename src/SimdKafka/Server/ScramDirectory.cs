using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;

namespace SimdKafka;

internal sealed record ScramUser(ScramCredentials.Verifier? Sha256 = null, ScramCredentials.Verifier? Sha512 = null)
{
    internal bool IsEmpty => Sha256 is null && Sha512 is null;
    internal ScramCredentials.Verifier? Find(sbyte mechanism) => mechanism switch { 1 => Sha256, 2 => Sha512, _ => null };
    internal ScramUser Set(sbyte mechanism, ScramCredentials.Verifier? verifier) =>
        mechanism == 1 ? this with { Sha256 = verifier } : this with { Sha512 = verifier };
}

internal sealed record ScramChange(sbyte Mechanism, ScramCredentials.Verifier? Credential);

internal sealed record ScramDirectory
{
    internal static ScramDirectory Empty { get; } = new();
    public ImmutableDictionary<string, ScramUser> Users { get; init; } =
        ImmutableDictionary.Create<string, ScramUser>(StringComparer.Ordinal);

    internal static bool ValidName(string name) => Encoding.UTF8.GetByteCount(name) is > 0 and <= 512 && !name.Any(char.IsControl);
    internal static int KeySize(sbyte mechanism) => mechanism switch { 1 => 32, 2 => 64, _ => 0 };
    internal ScramCredentials.Verifier? Find(string name, sbyte mechanism) => Users.GetValueOrDefault(name)?.Find(mechanism);

    internal (Error Error, ScramDirectory Next) Apply(string name, ScramChange change)
    {
        int width = KeySize(change.Mechanism);
        if (!ValidName(name)) return (Error.UnacceptableCredential, this);
        if (width == 0) return (Error.UnsupportedSaslMechanism, this);
        var user = Users.GetValueOrDefault(name);
        if (change.Credential is { } verifier)
        {
            if (verifier.Iterations is < 4096 or > 16384 || verifier.Salt is not { Length: > 0 and <= 1024 } ||
                verifier.StoredKey?.Length != width || verifier.ServerKey?.Length != width)
                return (Error.UnacceptableCredential, this);
            if (user is null && Users.Count >= 4096) return (Error.UnacceptableCredential, this);
            if (user?.Find(change.Mechanism)?.SameAs(verifier) == true) return (Error.None, this);
            return (Error.None, this with { Users = Users.SetItem(name, (user ?? new()).Set(change.Mechanism, verifier)) });
        }
        if (user?.Find(change.Mechanism) is null) return (Error.ResourceNotFound, this);
        var replacement = user.Set(change.Mechanism, null);
        return (Error.None, this with { Users = replacement.IsEmpty ? Users.Remove(name) : Users.SetItem(name, replacement) });
    }

    internal static ScramCredentials.Verifier Derive(sbyte mechanism, int iterations, ReadOnlySpan<byte> salt, ReadOnlySpan<byte> saltedPassword)
    {
        Span<byte> clientKey = stackalloc byte[KeySize(mechanism)];
        try
        {
            if (mechanism == 1)
            {
                HMACSHA256.HashData(saltedPassword, "Client Key"u8, clientKey);
                return new(iterations, salt.ToArray(), SHA256.HashData(clientKey), HMACSHA256.HashData(saltedPassword, "Server Key"u8));
            }
            HMACSHA512.HashData(saltedPassword, "Client Key"u8, clientKey);
            return new(iterations, salt.ToArray(), SHA512.HashData(clientKey), HMACSHA512.HashData(saltedPassword, "Server Key"u8));
        }
        finally { CryptographicOperations.ZeroMemory(clientKey); }
    }
}
