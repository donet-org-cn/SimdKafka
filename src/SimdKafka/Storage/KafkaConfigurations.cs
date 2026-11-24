using System.Collections.Immutable;
using System.Globalization;

namespace SimdKafka;

internal sealed record ConfigChange(string Name, sbyte Operation, string? Value);
internal sealed record ConfigUpdate(sbyte Type, string Name, ConfigChange[] Changes, bool Replace, bool ValidateOnly);
internal readonly record struct TimestampLimits(long? Before = null, long? After = null)
{
    internal const long DefaultBefore = long.MaxValue, DefaultAfter = 3_600_000;
    internal bool IsEmpty => Before is null && After is null;

    internal TimestampLimits Inherit(TimestampLimits fallback) => new(Before ?? fallback.Before, After ?? fallback.After);

    internal TimestampBounds At(long now)
    {
        long before = Before ?? DefaultBefore, after = After ?? DefaultAfter;
        return new(before == long.MaxValue ? long.MinValue : now - before, now + Math.Min(after, long.MaxValue - now));
    }
}

internal readonly record struct TimestampBounds(long Earliest, long Latest);
internal sealed record RemoteTopicPolicy(bool? Enabled = null, int? SegmentBytes = null, long? LocalBytes = null, long? LocalMs = null)
{
    internal bool IsEmpty => Enabled is null && SegmentBytes is null && LocalBytes is null && LocalMs is null;
}

internal sealed record KafkaConfigurations
{
    internal const sbyte Topic = 2, Broker = 4;
    internal const string TopicMessageBytes = "max.message.bytes", BrokerMessageBytes = "message.max.bytes";
    internal const string TopicTimestampType = "message.timestamp.type", BrokerTimestampType = "log.message.timestamp.type";
    internal const string TimestampBefore = "message.timestamp.before.max.ms", TimestampAfter = "message.timestamp.after.max.ms";
    internal const string BrokerTimestampBefore = "log." + TimestampBefore, BrokerTimestampAfter = "log." + TimestampAfter;
    internal const string CreateTime = "CreateTime", LogAppendTime = "LogAppendTime";
    internal const string RemoteEnabled = "remote.storage.enable", SegmentBytes = "segment.bytes", LocalBytes = "local.retention.bytes";
    internal const string LocalMs = "local.retention.ms";
    internal const int DefaultBroker = -1;
    internal static KafkaConfigurations Empty { get; } = new();
    public ImmutableDictionary<Guid, int> TopicLimits { get; init; } = ImmutableDictionary<Guid, int>.Empty;
    public ImmutableDictionary<int, int> BrokerLimits { get; init; } = ImmutableDictionary<int, int>.Empty;
    public ImmutableDictionary<Guid, RemoteTopicPolicy> TopicTiers { get; init; } = ImmutableDictionary<Guid, RemoteTopicPolicy>.Empty;
    public ImmutableDictionary<Guid, bool> TopicAppendTime { get; init; } = ImmutableDictionary<Guid, bool>.Empty;
    public bool? DefaultAppendTime { get; init; }
    public ImmutableDictionary<Guid, TimestampLimits> TopicTimestampLimits { get; init; } = ImmutableDictionary<Guid, TimestampLimits>.Empty;
    public TimestampLimits DefaultTimestampLimits { get; init; }

    internal int DefaultLimit => BrokerLimits.GetValueOrDefault(DefaultBroker, BrokerOptions.MaxBatchBytes);
    internal int BrokerLimit(int id) => BrokerLimits.GetValueOrDefault(id, DefaultLimit);
    internal int TopicLimit(Guid id, int broker) => TopicLimits.GetValueOrDefault(id, BrokerLimit(broker));
    internal bool UsesLogAppendTime(Guid topic) => TopicAppendTime.GetValueOrDefault(topic, DefaultAppendTime == true);
    internal TimestampBounds? ProduceTimestampBounds(Guid topic, bool logAppendTime) => logAppendTime ? null :
        TopicTimestampLimits.GetValueOrDefault(topic).Inherit(DefaultTimestampLimits).At(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

    internal KafkaConfigurations WithTopicTimestamp(Guid topic, bool? logAppendTime, TimestampLimits limits = default) => this with
    {
        TopicAppendTime = logAppendTime is bool value ? TopicAppendTime.SetItem(topic, value) : TopicAppendTime.Remove(topic),
        TopicTimestampLimits = limits.IsEmpty ? TopicTimestampLimits.Remove(topic) : TopicTimestampLimits.SetItem(topic, limits)
    };

    internal KafkaConfigurations WithTopicLimit(Guid topic, int? limit) => this with
    {
        TopicLimits = limit is int value ? TopicLimits.SetItem(topic, value) : TopicLimits.Remove(topic)
    };

    internal KafkaConfigurations WithTopicTier(Guid topic, RemoteTopicPolicy? policy) => this with
    {
        TopicTiers = policy is { IsEmpty: false } ? TopicTiers.SetItem(topic, policy) : TopicTiers.Remove(topic)
    };

    internal static (Error Error, int? Limit, RemoteTopicPolicy? Tier, bool? AppendTime, TimestampLimits TimestampLimits) ParseTopic(
        ReadOnlySpan<ConfigChange> changes, bool tierSupported,
        int? currentLimit = null, RemoteTopicPolicy? currentTier = null, bool? currentAppendTime = null,
        TimestampLimits timestampLimits = default)
    {
        if (changes.IsEmpty) return (Error.None, currentLimit, currentTier, currentAppendTime, timestampLimits);
        var names = new HashSet<string>(StringComparer.Ordinal);
        var tier = currentTier ?? new();
        var invalid = (Error.InvalidConfig, (int?)null, (RemoteTopicPolicy?)null, (bool?)null, default(TimestampLimits));
        foreach (var change in changes)
        {
            if (!names.Add(change.Name)) return (Error.InvalidRequest, null, null, null, default);
            if (change.Operation != 0 && (change.Operation != 1 || change.Value is not null))
                return invalid;
            bool remove = change.Operation == 1;
            switch (change.Name)
            {
                case TopicMessageBytes:
                    if (!TryLimit(change, out currentLimit)) return invalid;
                    break;
                case TopicTimestampType:
                    if (!TryAppendTime(change, out currentAppendTime)) return invalid;
                    break;
                case TimestampBefore or TimestampAfter:
                    if (!TryTimestampLimit(change, ref timestampLimits)) return invalid;
                    break;
                case RemoteEnabled when tierSupported:
                    if (remove) tier = tier with { Enabled = null };
                    else if (bool.TryParse(change.Value, out bool enabled)) tier = tier with { Enabled = enabled };
                    else return invalid;
                    break;
                case SegmentBytes when tierSupported:
                    if (remove) tier = tier with { SegmentBytes = null };
                    else if (int.TryParse(change.Value, NumberStyles.None, CultureInfo.InvariantCulture, out int segment) &&
                        segment is >= 4096 and <= 256 * 1024 * 1024) tier = tier with { SegmentBytes = segment };
                    else return invalid;
                    break;
                case LocalBytes or LocalMs when tierSupported:
                    long? threshold = null;
                    if (!remove)
                    {
                        if (!long.TryParse(change.Value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long value) || value < -2)
                            return invalid;
                        threshold = value;
                    }
                    tier = change.Name == LocalBytes ? tier with { LocalBytes = threshold } : tier with { LocalMs = threshold };
                    break;
                default: return invalid;
            }
        }
        return (Error.None, currentLimit, tier.IsEmpty ? null : tier, currentAppendTime, timestampLimits);
    }

    private static bool TryLimit(ConfigChange change, out int? result)
    {
        result = null;
        if (change.Operation == 1) return change.Value is null;
        if (change.Operation != 0 || !int.TryParse(change.Value, NumberStyles.None, CultureInfo.InvariantCulture, out int limit) ||
            limit > BrokerOptions.MaxBatchBytes) return false;
        result = limit;
        return true;
    }

    private static bool TryAppendTime(ConfigChange change, out bool? result)
    {
        result = null;
        if (change.Operation == 1) return change.Value is null;
        if (change.Operation != 0 || change.Value is not (CreateTime or LogAppendTime)) return false;
        result = change.Value == LogAppendTime;
        return true;
    }

    private static bool TryTimestampLimit(ConfigChange change, ref TimestampLimits limits)
    {
        long? limit = null;
        if (change.Operation != 1)
        {
            if (change.Operation != 0 ||
                !long.TryParse(change.Value, NumberStyles.None, CultureInfo.InvariantCulture, out long value)) return false;
            limit = value;
        }
        else if (change.Value is not null) return false;
        limits = change.Name is TimestampBefore or BrokerTimestampBefore ? limits with { Before = limit } : limits with { After = limit };
        return true;
    }

    internal static bool TryBrokerId(string name, out int id)
    {
        if (name.Length == 0) { id = DefaultBroker; return true; }
        return int.TryParse(name, NumberStyles.None, CultureInfo.InvariantCulture, out id) &&
            id.ToString(CultureInfo.InvariantCulture) == name;
    }

    internal (Error Error, KafkaConfigurations Next) Apply(ConfigUpdate update, Guid topic, int broker, bool tierSupported = false)
    {
        if (update.Type is not (Topic or Broker)) return (Error.InvalidRequest, this);
        if (!update.Replace && update.Changes.Length == 0) return (Error.None, this);
        if (update.Type == Topic)
        {
            var before = TopicTiers.GetValueOrDefault(topic);
            var parsed = ParseTopic(update.Changes, tierSupported, !update.Replace && TopicLimits.TryGetValue(topic, out int beforeLimit) ?
                beforeLimit : null, update.Replace ? null : before,
                !update.Replace && TopicAppendTime.TryGetValue(topic, out bool beforeTime) ? beforeTime : null,
                update.Replace ? default : TopicTimestampLimits.GetValueOrDefault(topic));
            if (parsed.Error != Error.None) return (parsed.Error, this);
            if (before?.Enabled == true && parsed.Tier?.Enabled != true) return (Error.InvalidConfig, this);
            return (Error.None, update.ValidateOnly ? this :
                WithTopicLimit(topic, parsed.Limit).WithTopicTier(topic, parsed.Tier)
                    .WithTopicTimestamp(topic, parsed.AppendTime, parsed.TimestampLimits));
        }

        int? limit = !update.Replace && BrokerLimits.TryGetValue(broker, out int existingLimit) ? existingLimit : null;
        bool? appendTime = update.Replace && broker == DefaultBroker ? null : DefaultAppendTime;
        var timestampLimits = update.Replace && broker == DefaultBroker ? default : DefaultTimestampLimits;
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var change in update.Changes)
        {
            if (!names.Add(change.Name)) return (Error.InvalidRequest, this);
            bool valid = change.Name switch
            {
                BrokerMessageBytes => TryLimit(change, out limit),
                BrokerTimestampType when broker == DefaultBroker => TryAppendTime(change, out appendTime),
                BrokerTimestampBefore or BrokerTimestampAfter when broker == DefaultBroker => TryTimestampLimit(change, ref timestampLimits),
                _ => false
            };
            if (!valid) return (Error.InvalidConfig, this);
        }
        return (Error.None, update.ValidateOnly ? this : this with
        {
            BrokerLimits = limit is int value ? BrokerLimits.SetItem(broker, value) : BrokerLimits.Remove(broker),
            DefaultAppendTime = appendTime,
            DefaultTimestampLimits = timestampLimits
        });
    }
}
