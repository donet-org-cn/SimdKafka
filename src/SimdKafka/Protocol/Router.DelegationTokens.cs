using System.Security.Cryptography;

namespace SimdKafka;

internal sealed partial class Router
{
    private static readonly (Api Key, short Min, short Max)[] DelegationTokenVersions =
    [
        (Api.CreateDelegationToken, 1, 3), (Api.RenewDelegationToken, 1, 2),
        (Api.ExpireDelegationToken, 1, 2), (Api.DescribeDelegationToken, 1, 3)
    ];

    internal static bool IsTokenApi(Api api) => api is >= Api.CreateDelegationToken and <= Api.DescribeDelegationToken;
    internal static bool SupportsTokenApi(Api api, short version)
    {
        foreach (var (key, min, max) in DelegationTokenVersions)
            if (key == api) return version >= min && version <= max;
        return false;
    }

    internal static async ValueTask<bool> HandleTokenAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        Func<ValueTask<(Error Error, TokenDirectory? Directory)>> read, Func<TokenChange, ValueTask<MetadataResult>> change)
    {
        bool compact = request.Flexible;
        string actor = request.AuthenticatedPrincipal ?? "";
        var credentials = request.TokenCredentials;
        Error admission = credentials is null ? Error.DelegationTokenAuthDisabled :
            request.TokenAuthenticated || !DelegationToken.ValidPrincipal(actor) ? Error.DelegationTokenRequestNotAllowed : Error.None;
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var r = new WireReader(body.Span);
        switch (request.Api)
        {
            case Api.CreateDelegationToken:
                string? type = request.Version >= 3 ? r.NullableText(compact) : null;
                string? name = request.Version >= 3 ? r.NullableText(compact) : null;
                bool explicitOwner = !string.IsNullOrEmpty(name);
                string owner = explicitOwner ? type + ":" + name : actor;
                string[] renewers = ReadTokenPrincipals(ref r, compact, nullable: false) ?? [];
                long lifetime = r.I64();
                r.EndStruct(compact); r.End();
                Error error = admission;
                if (error == Error.None && (explicitOwner && type != "User" || !DelegationToken.ValidPrincipal(owner) ||
                    renewers.Any(static principal => !DelegationToken.ValidPrincipal(principal))))
                    error = Error.InvalidPrincipalType;
                if (error == Error.None && owner != actor && request.Access?.Allows(AclResource.User, owner, AclOperation.CreateTokens) != true)
                    error = Error.DelegationTokenAuthorizationFailed;
                DelegationToken? issued = null;
                if (error == Error.None)
                {
                    issued = credentials!.Issue(owner, actor, renewers, now, lifetime);
                    error = (await change(new(TokenAction.Create, issued.Id, actor, now, Creation: issued))).Error;
                }
                w.Write(error);
                WriteTokenPrincipal(w, owner, compact);
                if (request.Version >= 3) WriteTokenPrincipal(w, actor, compact);
                WriteTokenDetails(w, error == Error.None ? issued : null, credentials, compact);
                break;

            case Api.RenewDelegationToken or Api.ExpireDelegationToken:
                var hmac = compact ? r.Take(r.ArrayLength(true)) : r.Bytes();
                string? fingerprint = hmac.Length == DelegationTokenCredentials.HmacSize ? Convert.ToHexString(SHA256.HashData(hmac)) : null;
                long period = r.I64();
                r.EndStruct(compact); r.End();
                var result = new MetadataResult(admission);
                if (admission == Error.None)
                {
                    var (status, directory) = await read();
                    var token = fingerprint is null ? null : directory?.Entries.Values.FirstOrDefault(t => t.Fingerprint == fingerprint);
                    result = status != Error.None ? new(status) : token is null || !MatchesSecret(token, credentials!) ?
                        new(Error.DelegationTokenNotFound) :
                        await change(new(request.Api == Api.RenewDelegationToken ? TokenAction.Renew : TokenAction.Expire,
                            token.Id, actor, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), period));
                }
                w.Write(result.Error);
                w.I64(result.TokenExpiry);
                break;

            case Api.DescribeDelegationToken:
                string[]? owners = ReadTokenPrincipals(ref r, compact, nullable: true);
                r.EndStruct(compact); r.End();
                var (describeError, snapshot) = admission == Error.None && owners is not { Length: 0 } ?
                    await read() : (admission, null);
                var visible = snapshot is null ? [] : snapshot.Entries.Values.Where(token => token.Active(now) &&
                    (owners is null || owners.Contains(token.Owner, StringComparer.Ordinal)) &&
                    (token.ManagedBy(actor) || token.Requester == actor ||
                        request.Access?.Allows(AclResource.User, token.Owner, AclOperation.DescribeTokens) == true ||
                        request.Access?.Allows(AclResource.DelegationToken, token.Id, AclOperation.Describe) == true) &&
                    MatchesSecret(token, credentials!)).OrderBy(static token => token.Id, StringComparer.Ordinal).ToArray();
                w.Write(describeError);
                w.ArrayLength(visible.Length, compact);
                foreach (var token in visible)
                {
                    WriteTokenPrincipal(w, token.Owner, compact);
                    if (request.Version >= 3) WriteTokenPrincipal(w, token.Requester, compact);
                    WriteTokenDetails(w, token, credentials, compact);
                    w.ArrayLength(token.Renewers.Length, compact);
                    foreach (string renewer in token.Renewers)
                    {
                        WriteTokenPrincipal(w, renewer, compact);
                        w.EndStruct(compact);
                    }
                    w.EndStruct(compact);
                }
                break;
        }
        w.I32(0);
        w.EndStruct(compact);
        return true;
    }

    private static string[]? ReadTokenPrincipals(ref WireReader r, bool compact, bool nullable)
    {
        int count = r.ArrayLength(compact, nullable);
        if (count < 0) return null;
        if (count > (nullable ? 1024 : TokenDirectory.MaxRenewers)) throw new WireException("Too many token principals");
        var principals = new string[count];
        for (int i = 0; i < count; i++)
        {
            string type = r.Text(compact), name = r.Text(compact);
            principals[i] = type == "User" ? "User:" + name : "";
            r.EndStruct(compact);
        }
        return principals;
    }

    private static void WriteTokenPrincipal(WireWriter w, string principal, bool compact)
    {
        int separator = principal.IndexOf(':');
        w.Text(separator < 0 ? "" : principal[..separator], compact);
        w.Text(separator < 0 ? "" : principal[(separator + 1)..], compact);
    }

    private static bool MatchesSecret(DelegationToken token, DelegationTokenCredentials credentials)
    {
        Span<byte> hmac = stackalloc byte[DelegationTokenCredentials.HmacSize];
        try { return credentials.TryHmac(token, hmac); }
        finally { CryptographicOperations.ZeroMemory(hmac); }
    }

    private static void WriteTokenDetails(WireWriter w, DelegationToken? token, DelegationTokenCredentials? credentials, bool compact)
    {
        w.I64(token?.Issued ?? -1);
        w.I64(token?.Expiry ?? -1);
        w.I64(token?.Maximum ?? -1);
        w.Text(token?.Id ?? "", compact);
        if (token is null) { w.Bytes([], compact); return; }
        Span<byte> hmac = stackalloc byte[DelegationTokenCredentials.HmacSize];
        try
        {
            credentials!.Hmac(token.Id, hmac);
            w.Sensitive = true;
            w.Bytes(hmac, compact);
        }
        finally { CryptographicOperations.ZeroMemory(hmac); }
    }
}
