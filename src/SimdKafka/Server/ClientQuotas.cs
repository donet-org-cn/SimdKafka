using System.Collections.Immutable;
using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace SimdKafka;

internal readonly record struct QuotaComponent(string Type, string? Name);
internal readonly record struct QuotaFilter(string Type, sbyte Match, string? Name);
internal sealed record QuotaChange(string Key, double? Value);
internal sealed record QuotaUpdate(QuotaComponent[] Entity, QuotaChange[] Changes, bool ValidateOnly);
internal sealed record QuotaEntry(QuotaComponent[] Entity, ImmutableDictionary<string, double> Values);

internal readonly record struct QuotaEntity(bool WithUser, string? User, bool WithClient, string? Client,
    bool WithIp = false, string? Ip = null)
{
    internal static bool TryRead(QuotaComponent[] components, out QuotaEntity entity)
    {
        bool withUser = false, withClient = false, withIp = false;
        string? user = null, client = null, ip = null;
        foreach (var component in components)
        {
            if (component.Name is { } name && (name.Length is 0 or > 1024 || name.Any(char.IsControl)))
            { entity = default; return false; }
            switch (component.Type)
            {
                case "user" when !withUser: withUser = true; user = component.Name; break;
                case "client-id" when !withClient: withClient = true; client = component.Name; break;
                case "ip" when !withIp:
                    withIp = true;
                    if (component.Name is { } ipName)
                    {
                        if (!IPAddress.TryParse(ipName, out var address)) { entity = default; return false; }
                        ip = FormatIp(address);
                    }
                    break;
                default: entity = default; return false;
            }
        }
        entity = new(withUser, user, withClient, client, withIp, ip);
        return withIp ? !withUser && !withClient : withUser || withClient;
    }

    internal static string FormatIp(IPAddress address) =>
        (address.IsIPv4MappedToIPv6 ? address.MapToIPv4() : address).ToString();
}

internal sealed record ClientQuotas
{
    internal const string ProducerBytes = "producer_byte_rate", ConsumerBytes = "consumer_byte_rate";
    internal const string ConnectionRate = "connection_creation_rate";
    internal static ClientQuotas Empty { get; } = new();
    public ImmutableArray<QuotaEntry> Entries { get; init; } = [];
    private ImmutableDictionary<QuotaEntity, ImmutableDictionary<string, double>>? _index;

    private ImmutableDictionary<QuotaEntity, ImmutableDictionary<string, double>> Index =>
        LazyInitializer.EnsureInitialized(ref _index, BuildIndex);

    private ImmutableDictionary<QuotaEntity, ImmutableDictionary<string, double>> BuildIndex()
    {
        var builder = ImmutableDictionary.CreateBuilder<QuotaEntity, ImmutableDictionary<string, double>>();
        foreach (var row in Entries)
            if (QuotaEntity.TryRead(row.Entity, out var key)) builder.Add(key, row.Values);
        return builder.ToImmutable();
    }

    internal (Error Error, ClientQuotas Next) Apply(QuotaUpdate update)
    {
        if (!QuotaEntity.TryRead(update.Entity, out var key)) return (Error.InvalidRequest, this);
        var values = Index.GetValueOrDefault(key) ?? ImmutableDictionary<string, double>.Empty;
        var changed = new HashSet<string>(StringComparer.Ordinal);
        foreach (var change in update.Changes)
        {
            bool supported = key.WithIp ? change.Key == ConnectionRate : change.Key is ProducerBytes or ConsumerBytes;
            double minimum = key.WithIp ? 0 : 1;
            double maximum = key.WithIp ? int.MaxValue : long.MaxValue;
            if (!supported || !changed.Add(change.Key) || change.Value is double rate &&
                (!double.IsFinite(rate) || rate < minimum || rate > maximum || Math.Truncate(rate) != rate))
                return (Error.InvalidRequest, this);
            values = change.Value is double value ? values.SetItem(change.Key, value) : values.Remove(change.Key);
        }
        int index = -1;
        for (int i = 0; i < Entries.Length; i++)
            if (QuotaEntity.TryRead(Entries[i].Entity, out var existing) && existing == key) { index = i; break; }
        if (index < 0 && !values.IsEmpty && Entries.Length >= 4096) return (Error.InvalidRequest, this);
        if (update.ValidateOnly || index < 0 && values.IsEmpty || index >= 0 && ReferenceEquals(Entries[index].Values, values))
            return (Error.None, this);
        var row = new QuotaEntry(update.Entity, values);
        return (Error.None, new() { Entries = index < 0 ? Entries.Add(row) : values.IsEmpty ? Entries.RemoveAt(index) : Entries.SetItem(index, row) });
    }

    internal (QuotaEntity Scope, double? Rate) ResolveConnection(IPAddress address)
    {
        var scope = new QuotaEntity(false, null, false, null, true, QuotaEntity.FormatIp(address));
        var rules = Index;
        var values = rules.GetValueOrDefault(scope) ?? rules.GetValueOrDefault(scope with { Ip = null });
        return (scope, values is not null && values.TryGetValue(ConnectionRate, out double rate) ? rate : null);
    }

    internal (QuotaEntity Scope, double Rate) Resolve(string user, string client, string key)
    {
        var byUser = new QuotaEntity(true, user, false, null);
        var byClient = new QuotaEntity(false, null, true, client);
        var byBoth = new QuotaEntity(true, user, true, client);
        var records = Index;
        double Rate(QuotaEntity entity) => records.GetValueOrDefault(entity)?.GetValueOrDefault(key) ?? 0;
        double rate = Rate(byBoth);
        if (rate == 0) rate = Rate(byBoth with { Client = null });
        if (rate != 0) return (byBoth, rate);
        rate = Rate(byUser);
        if (rate != 0) return (byUser, rate);
        rate = Rate(byBoth with { User = null });
        if (rate == 0) rate = Rate(byBoth with { User = null, Client = null });
        if (rate != 0) return (byBoth, rate);
        rate = Rate(byUser with { User = null });
        if (rate != 0) return (byUser, rate);
        rate = Rate(byClient);
        return (byClient, rate == 0 ? Rate(byClient with { Client = null }) : rate);
    }
}

internal sealed class ClientQuotaLimiter
{
    private const int MaxThrottleMs = 30_000;
    internal enum Kind : byte { Produce, Fetch, Connect }
    private readonly record struct MeterKey(QuotaEntity Scope, Kind Kind, bool HashedUser, bool HashedClient)
    {
        internal static MeterKey Create(QuotaEntity scope, Kind kind)
        {
            bool user = scope.User is { Length: > 512 }, client = scope.Client is { Length: > 512 };
            if (user) scope = scope with { User = Hash(scope.User!) };
            if (client) scope = scope with { Client = Hash(scope.Client!) };
            return new(scope, kind, user, client);
        }

        private static string Hash(string value) => Convert.ToHexString(SHA256.HashData(MemoryMarshal.AsBytes(value.AsSpan())));
    }

    private readonly Dictionary<MeterKey, Meter> _meters = new();
    private readonly Meter[] _overflow = [new(), new(), new()];
    private long _nextCleanup;

    internal sealed class Meter
    {
        private double _next;
        private double _rate;
        internal long LastUsed;

        internal int Charge(int bytes, double rate)
        {
            long now = Environment.TickCount64;
            Volatile.Write(ref LastUsed, now);
            lock (this)
            {
                if (rate != _rate) { _rate = rate; _next = now; }
                _next = Math.Min(Math.Max(now, _next) + bytes * (1000d / rate), now + MaxThrottleMs + 1000);
                return (int)Math.Ceiling(Math.Clamp(_next - now - 1000, 0, MaxThrottleMs));
            }
        }

        internal int AdmitConnection(double rate)
        {
            long now = Environment.TickCount64;
            Volatile.Write(ref LastUsed, now);
            lock (this)
            {
                if (rate != _rate) { _rate = rate; _next = now; }
                double proposed = Math.Max(now, _next) + 1000d / rate;
                if (proposed > now + 1000) return (int)Math.Ceiling(proposed - now - 1000);
                _next = proposed;
                return 0;
            }
        }
    }

    internal Meter Get(QuotaEntity scope, Kind kind)
    {
        MeterKey key = MeterKey.Create(scope, kind);
        long now = Environment.TickCount64;
        lock (_meters)
        {
            if (now >= _nextCleanup)
            {
                foreach (var expired in _meters.Where(pair => now - Volatile.Read(ref pair.Value.LastUsed) >= 60_000)
                    .Select(static pair => pair.Key).ToArray()) _meters.Remove(expired);
                _nextCleanup = now + 60_000;
            }
            if (_meters.TryGetValue(key, out var meter)) return meter;
            if (_meters.Count >= 16_384) return _overflow[(int)kind];
            meter = new() { LastUsed = now };
            _meters.Add(key, meter);
            return meter;
        }
    }
}

internal sealed class RequestQuota
{
    private ClientQuotaLimiter.Meter? _meter;
    private double _rate;
    internal int Delay { get; private set; }

    internal void Reset(ClientQuotaLimiter.Meter meter, double rate)
    {
        _meter = meter;
        _rate = rate;
        Delay = 0;
    }

    internal int Charge(int bytes) => Delay = _meter!.Charge(bytes, _rate);
}
