namespace SimdKafka;

internal readonly record struct ScramReadResult(Error Error, ScramDirectory? Directory);

internal sealed partial class Router
{
    internal static async ValueTask<bool> DescribeScramAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        bool enabled, Func<ValueTask<ScramReadResult>> read)
    {
        var r = new WireReader(body.Span);
        int count = r.ArrayLength(true, nullable: true);
        if (count > 4096) throw new WireException("Too many SCRAM users");
        var names = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int i = 0; i < count; i++)
        {
            string name = r.Text(true);
            names[name] = names.GetValueOrDefault(name) + 1;
            r.Tags();
        }
        r.Tags(); r.End();

        Error access = ScramAccess(request, enabled, AclOperation.Describe);
        var result = access == Error.None ? await read() : new ScramReadResult(access, null);
        w.I32(0);
        WriteScramError(w, result.Error);
        var bootstrap = request.BootstrapScram ?? ScramDirectory.Empty;
        if (result.Directory is not { } current) w.ArrayLength(0, true);
        else if (names.Count == 0)
        {
            var all = current.Users.Keys.Union(bootstrap.Users.Where(static user => !user.Value.IsEmpty).Select(static user => user.Key),
                StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
            w.ArrayLength(all.Length, true);
            foreach (var name in all) DescribeUser(name, current, Error.None);
        }
        else
        {
            w.ArrayLength(names.Count, true);
            foreach (var (name, occurrences) in names)
                DescribeUser(name, current, occurrences == 1 ? Error.None : Error.DuplicateResource);
        }
        w.UVar(0);
        return true;

        void DescribeUser(string name, ScramDirectory directory, Error error)
        {
            var first = bootstrap.Find(name, 1) ?? directory.Find(name, 1);
            var second = bootstrap.Find(name, 2) ?? directory.Find(name, 2);
            if (error == Error.None && first is null && second is null) error = Error.ResourceNotFound;
            w.Text(name, true);
            WriteScramError(w, error);
            w.ArrayLength(error == Error.None ? (first is null ? 0 : 1) + (second is null ? 0 : 1) : 0, true);
            if (error == Error.None)
            {
                if (first is not null) { w.I8(1); w.I32(first.Iterations); w.UVar(0); }
                if (second is not null) { w.I8(2); w.I32(second.Iterations); w.UVar(0); }
            }
            w.UVar(0);
        }
    }

    internal static async ValueTask<bool> AlterScramAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        bool enabled, Func<string, ScramChange, ValueTask<Error>> apply)
    {
        var r = new WireReader(body.Span);
        var changes = new Dictionary<string, (Error Error, ScramChange? Change)>(StringComparer.Ordinal);
        Error access = ScramAccess(request, enabled, AclOperation.Alter);
        int removals = r.ArrayLength(true);
        if (removals > 4096) throw new WireException("Too many SCRAM changes");
        for (int i = 0; i < removals; i++)
        {
            string name = r.Text(true);
            sbyte mechanism = r.I8();
            var error = CheckUser(name, mechanism);
            changes[name] = (error, error == Error.None ? new(mechanism, null) : null);
            r.Tags();
        }
        int upserts = r.ArrayLength(true);
        if (upserts > 4096 - removals) throw new WireException("Too many SCRAM changes");
        for (int i = 0; i < upserts; i++)
        {
            string name = r.Text(true);
            sbyte mechanism = r.I8();
            int iterations = r.I32();
            var salt = r.Take(r.ArrayLength(true));
            var salted = r.Take(r.ArrayLength(true));
            Error error = CheckUser(name, mechanism);
            if (error == Error.None && (iterations is < 4096 or > 16384 || salt.Length is 0 or > 1024 ||
                salted.Length != ScramDirectory.KeySize(mechanism))) error = Error.UnacceptableCredential;
            changes[name] = (error, error == Error.None ?
                new(mechanism, ScramDirectory.Derive(mechanism, iterations, salt, salted)) : null);
            r.Tags();
        }
        r.Tags(); r.End();

        w.I32(0);
        w.ArrayLength(changes.Count, true);
        foreach (var (name, change) in changes)
        {
            Error error = change.Change is { } operation ? await apply(name, operation) : change.Error;
            w.Text(name, true);
            WriteScramError(w, error);
            w.UVar(0);
        }
        w.UVar(0);
        return true;

        Error CheckUser(string user, sbyte mechanism)
        {
            if (access != Error.None) return access;
            if (changes.ContainsKey(user)) return Error.DuplicateResource;
            if (!ScramDirectory.ValidName(user)) return Error.UnacceptableCredential;
            if (ScramDirectory.KeySize(mechanism) == 0) return Error.UnsupportedSaslMechanism;
            return request.BootstrapScram?.Find(user, mechanism) is null ? Error.None : Error.UnacceptableCredential;
        }
    }

    private static Error ScramAccess(Request request, bool enabled, AclOperation operation) =>
        !enabled || request.Access is null ? Error.SecurityDisabled :
        request.AllowsCluster(operation) ? Error.None : Error.ClusterAuthorizationFailed;

    private static void WriteScramError(WireWriter w, Error error)
    {
        w.Write(error);
        w.Text(error == Error.None ? null : error.ToString(), true);
    }
}
