using System.Net;

namespace SimdKafka;

internal readonly record struct QuotaReadResult(Error Error, ClientQuotas Directory);

internal sealed partial class Router
{
    internal static async ValueTask<bool> DescribeQuotasAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        Func<ValueTask<QuotaReadResult>> read)
    {
        bool compact = request.Flexible;
        var r = new WireReader(body.Span);
        int count = r.ArrayLength(compact);
        if (count > 32) throw new WireException("Too many quota filter components");
        var filters = new QuotaFilter[count];
        for (int i = 0; i < count; i++)
        {
            filters[i] = new(r.Text(compact), r.I8(), r.NullableText(compact));
            r.EndStruct(compact);
        }
        bool strict = r.Bool();
        r.EndStruct(compact); r.End();
        Error error = !request.AllowsCluster(AclOperation.DescribeConfigs) ? Error.ClusterAuthorizationFailed :
            !ValidQuotaFilters(filters) ? Error.InvalidRequest : Error.None;
        var result = error == Error.None ? await read() : new QuotaReadResult(error, ClientQuotas.Empty);
        w.I32(0);
        WriteQuotaError(w, result.Error, compact);
        if (result.Error != Error.None) w.ArrayLength(-1, compact);
        else
        {
            var matches = result.Directory.Entries.Where(entry => (!strict || entry.Entity.Length == filters.Length) &&
                filters.All(filter => entry.Entity.Any(component => MatchQuotaFilter(component, filter)))).ToArray();
            w.ArrayLength(matches.Length, compact);
            foreach (var entry in matches)
            {
                WriteQuotaEntity(w, entry.Entity, compact);
                w.ArrayLength(entry.Values.Count, compact);
                foreach (var (key, value) in entry.Values)
                {
                    w.Text(key, compact); w.I64(BitConverter.DoubleToInt64Bits(value)); w.EndStruct(compact);
                }
                w.EndStruct(compact);
            }
        }
        w.EndStruct(compact);
        return true;
    }

    internal static async ValueTask<bool> AlterQuotasAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        Func<QuotaUpdate, ValueTask<Error>> apply)
    {
        bool compact = request.Flexible;
        var r = new WireReader(body.Span);
        int count = r.ArrayLength(compact);
        if (count > 4096) throw new WireException("Too many quota entries");
        var entries = new (QuotaUpdate Update, QuotaEntity Key, bool Valid)[count];
        var occurrences = new Dictionary<QuotaEntity, int>();
        for (int i = 0; i < entries.Length; i++)
        {
            int components = r.ArrayLength(compact);
            if (components > 32) throw new WireException("Too many quota entity components");
            var entity = new QuotaComponent[components];
            for (int j = 0; j < components; j++)
            {
                entity[j] = new(r.Text(compact), r.NullableText(compact));
                r.EndStruct(compact);
            }
            bool validEntity = QuotaEntity.TryRead(entity, out var key);
            if (validEntity) occurrences[key] = occurrences.GetValueOrDefault(key) + 1;
            int operations = r.ArrayLength(compact);
            if (operations > 32) throw new WireException("Too many quota operations");
            var changes = new QuotaChange[operations];
            bool validValues = true;
            for (int j = 0; j < operations; j++)
            {
                string name = r.Text(compact);
                double value = BitConverter.Int64BitsToDouble(r.I64());
                bool remove = r.Bool();
                validValues &= remove || double.IsFinite(value);
                changes[j] = new(name, remove ? null : value);
                r.EndStruct(compact);
            }
            entries[i] = (new(entity, changes, false), key, validEntity && validValues);
            r.EndStruct(compact);
        }
        bool validateOnly = r.Bool();
        r.EndStruct(compact); r.End();
        bool allowed = request.AllowsCluster(AclOperation.AlterConfigs);
        w.I32(0);
        w.ArrayLength(count, compact);
        foreach (var (entry, key, valid) in entries)
        {
            Error error = !allowed ? Error.ClusterAuthorizationFailed : !valid || occurrences[key] != 1 ? Error.InvalidRequest :
                await apply(entry with { ValidateOnly = validateOnly });
            WriteQuotaError(w, error, compact);
            WriteQuotaEntity(w, entry.Entity, compact);
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
        return true;
    }

    private static bool ValidQuotaFilters(QuotaFilter[] filters) =>
        filters.All(static filter => filter.Type is "user" or "client-id" or "ip" &&
            (filter.Match == 0 ? filter.Name is not null : filter.Match is 1 or 2 && filter.Name is null)) &&
        filters.DistinctBy(static filter => filter.Type, StringComparer.Ordinal).Count() == filters.Length &&
        (filters.Length <= 1 || !filters.Any(static filter => filter.Type == "ip"));

    private static bool MatchQuotaFilter(QuotaComponent component, QuotaFilter filter)
    {
        if (component.Type != filter.Type) return false;
        if (filter.Match == 2 || component.Name == filter.Name) return true;
        return component.Type == "ip" && IPAddress.TryParse(component.Name, out var configured) &&
            IPAddress.TryParse(filter.Name, out var requested) && QuotaEntity.FormatIp(configured) == QuotaEntity.FormatIp(requested);
    }

    private static void WriteQuotaError(WireWriter w, Error error, bool compact)
    { w.Write(error); w.Text(error == Error.None ? null : error.ToString(), compact); }

    private static void WriteQuotaEntity(WireWriter w, QuotaComponent[] entity, bool compact)
    {
        w.ArrayLength(entity.Length, compact);
        foreach (var component in entity)
        { w.Text(component.Type, compact); w.Text(component.Name, compact); w.EndStruct(compact); }
    }
}
