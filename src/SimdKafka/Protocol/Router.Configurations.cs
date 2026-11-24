using System.Globalization;

namespace SimdKafka;

internal sealed partial class Router
{
    internal static ConfigChange[] ReadConfigChanges(ref WireReader r, bool compact, bool incremental)
    {
        int count = r.ArrayLength(compact);
        if (count > 1_024) throw new WireException("Too many configuration entries");
        var changes = new ConfigChange[count];
        for (int i = 0; i < changes.Length; i++)
        {
            string name = r.Text(compact);
            sbyte operation = incremental ? r.I8() : (sbyte)0;
            string? value = r.NullableText(compact);
            changes[i] = new(name, !incremental && value is null ? (sbyte)1 : operation, value);
            r.EndStruct(compact);
        }
        return changes;
    }

    internal static async ValueTask<bool> AlterConfigurationsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        Func<ConfigUpdate, ValueTask<Error>> apply)
    {
        bool compact = request.Flexible, replace = request.Api == Api.AlterConfigs;
        var r = new WireReader(body.Span);
        int count = r.ArrayLength(compact);
        if (count > 1_024) throw new WireException("Too many configuration resources");
        var resources = new ConfigUpdate[count];
        var counts = new Dictionary<(sbyte, string), int>();
        for (int i = 0; i < resources.Length; i++)
        {
            sbyte type = r.I8();
            string name = r.Text(compact);
            resources[i] = new(type, name, ReadConfigChanges(ref r, compact, incremental: !replace), replace, false);
            counts[(type, name)] = counts.GetValueOrDefault((type, name)) + 1;
            r.EndStruct(compact);
        }
        bool validateOnly = r.Bool();
        r.EndStruct(compact); r.End();
        w.I32(0);
        w.ArrayLength(resources.Length, compact);
        foreach (var resource in resources)
        {
            Error error = ConfigurationAccess(request, resource.Type, resource.Name, AclOperation.AlterConfigs);
            if (error == Error.None)
                error = counts[(resource.Type, resource.Name)] == 1 ?
                    await apply(resource with { ValidateOnly = validateOnly }) : Error.InvalidRequest;
            w.Write(error);
            w.Text(error == Error.None ? null : error.ToString(), compact);
            w.I8(resource.Type);
            w.Text(resource.Name, compact);
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
        return true;
    }

    private static Error ConfigurationAccess(Request request, sbyte type, string name, AclOperation operation) =>
        type == KafkaConfigurations.Topic ? request.AllowsTopic(name, operation) ? Error.None : Error.TopicAuthorizationFailed :
        request.AllowsCluster(operation) ? Error.None : Error.ClusterAuthorizationFailed;

    private static string FormatSize(int number) => number.ToString(CultureInfo.InvariantCulture);

    internal static void WriteConfigResources(ref WireReader r, WireWriter w, Request request, Error status,
        KafkaConfigurations settings, IEnumerable<(string Name, Guid Id)> topics)
    {
        int count = r.ArrayLength(true);
        bool includeTopics = count == 0, includeBrokers = count == 0;
        for (int i = 0; i < count; i++)
        {
            sbyte type = r.I8();
            includeTopics |= type == KafkaConfigurations.Topic;
            includeBrokers |= type == KafkaConfigurations.Broker;
        }
        r.Tags(); r.End();
        if (!request.AllowsCluster(AclOperation.DescribeConfigs)) status = Error.ClusterAuthorizationFailed;
        w.I32(0);
        w.Write(status);
        var topicNames = status == Error.None && includeTopics &&
            (!settings.TopicLimits.IsEmpty || !settings.TopicTiers.IsEmpty || !settings.TopicAppendTime.IsEmpty ||
                !settings.TopicTimestampLimits.IsEmpty) ?
            topics.Where(topic => settings.TopicLimits.ContainsKey(topic.Id) || settings.TopicTiers.ContainsKey(topic.Id) ||
                    settings.TopicAppendTime.ContainsKey(topic.Id) || settings.TopicTimestampLimits.ContainsKey(topic.Id))
                .Select(static topic => topic.Name)
                .Order(StringComparer.Ordinal).ToArray() : [];
        bool brokers = status == Error.None && includeBrokers;
        bool extraDefault = brokers && (settings.DefaultAppendTime.HasValue || !settings.DefaultTimestampLimits.IsEmpty) &&
            !settings.BrokerLimits.ContainsKey(KafkaConfigurations.DefaultBroker);
        w.ArrayLength(topicNames.Length + (brokers ? settings.BrokerLimits.Count : 0) + (extraDefault ? 1 : 0), true);
        foreach (string name in topicNames) WriteResource(name, KafkaConfigurations.Topic);
        if (extraDefault) WriteResource("", KafkaConfigurations.Broker);
        if (brokers)
            foreach (int broker in settings.BrokerLimits.Keys.Order())
                WriteResource(broker == KafkaConfigurations.DefaultBroker ? "" : FormatSize(broker), KafkaConfigurations.Broker);
        w.UVar(0);

        void WriteResource(string name, sbyte type)
        {
            w.Text(name, true); w.I8(type); w.UVar(0);
        }
    }

    internal static ReportedConfig[] TopicConfiguration(KafkaConfigurations settings, Guid topic, int broker,
        bool standalone, int? creatingLimit = null, BrokerOptions? tierOptions = null, RemoteTopicPolicy? creatingTier = null,
        bool? creatingLogAppendTime = null, TimestampLimits creatingTimestampLimits = default)
    {
        var inherited = BrokerLimitConfig(settings, broker);
        var topicLimit = creatingLimit ??
            (settings.TopicLimits.TryGetValue(topic, out int configured) ? configured : null);
        var limit = new ReportedConfig(KafkaConfigurations.TopicMessageBytes,
            topicLimit is int explicitLimit ? FormatSize(explicitLimit) : inherited.Value, 3, "Maximum encoded record batch bytes.",
            false, topicLimit.HasValue ? (sbyte)1 : inherited.Source,
            topicLimit is int configuredLimit ?
                [new(KafkaConfigurations.TopicMessageBytes, FormatSize(configuredLimit), 1), .. inherited.Synonyms!] :
                inherited.Synonyms);
        var defaultTime = BrokerTimestampConfig(settings, broker);
        bool? topicTime = creatingLogAppendTime ?? (settings.TopicAppendTime.TryGetValue(topic, out bool configuredTime) ? configuredTime : null);
        var timestamp = new ReportedConfig(KafkaConfigurations.TopicTimestampType,
            topicTime is bool explicitTime ? FormatTimestampType(explicitTime) : defaultTime.Value, 2,
            "Use producer creation time or broker append time.", false, topicTime.HasValue ? (sbyte)1 : defaultTime.Source,
            topicTime is bool localTime ?
                [new(KafkaConfigurations.TopicTimestampType, FormatTimestampType(localTime), 1), .. defaultTime.Synonyms!] : defaultTime.Synonyms);
        var timeLimits = creatingTimestampLimits.Inherit(settings.TopicTimestampLimits.GetValueOrDefault(topic));
        var before = TopicTimestampLimit(settings, broker, timeLimits.Before, before: true);
        var after = TopicTimestampLimit(settings, broker, timeLimits.After, before: false);
        if (!standalone) return [limit, timestamp, before, after, .. LogConfig];
        var basis = new ReportedConfig("min.insync.replicas", "1", 3, "Single-replica broker.");
        if (tierOptions?.TierDirectory is null) return [limit, timestamp, before, after, .. LogConfig, basis];
        var tier = creatingTier ?? settings.TopicTiers.GetValueOrDefault(topic);
        return [limit, timestamp, before, after, .. LogConfig, basis,
            new(KafkaConfigurations.RemoteEnabled, tier?.Enabled == true ? "true" : "false", 1, "Enable remote log segments on this topic.",
                false, tier?.Enabled is null ? (sbyte)5 : (sbyte)1),
            new(KafkaConfigurations.SegmentBytes, FormatSize(tier?.SegmentBytes ?? tierOptions.TierSegmentBytes), 3,
                "Minimum size of remote log segments.", false, tier?.SegmentBytes is null ? (sbyte)4 : (sbyte)1),
            new(KafkaConfigurations.LocalBytes, (tier?.LocalBytes ?? tierOptions.LocalRetentionBytes).ToString(CultureInfo.InvariantCulture),
                5, "Byte threshold for the local tail.", false, tier?.LocalBytes is null ? (sbyte)4 : (sbyte)1),
            new(KafkaConfigurations.LocalMs, (tier?.LocalMs ?? tierOptions.LocalRetentionMs).ToString(CultureInfo.InvariantCulture),
                5, "Maximum local age of archived records.", false, tier?.LocalMs is null ? (sbyte)4 : (sbyte)1)];
    }

    private static ReportedConfig BrokerTimestampConfig(KafkaConfigurations settings, int broker)
    {
        const string key = KafkaConfigurations.BrokerTimestampType;
        string value = FormatTimestampType(settings.DefaultAppendTime == true);
        ConfigSynonym fallback = new(key, KafkaConfigurations.CreateTime, 5);
        ConfigSynonym[] synonyms = settings.DefaultAppendTime.HasValue ? [new(key, value, 3), fallback] : [fallback];
        return new(key, value, 2, "Default topic timestamp type.", broker != KafkaConfigurations.DefaultBroker,
            synonyms[0].Source, synonyms);
    }

    private static string FormatTimestampType(bool logAppendTime) => logAppendTime ?
        KafkaConfigurations.LogAppendTime : KafkaConfigurations.CreateTime;

    private static ReportedConfig BrokerTimestampLimit(KafkaConfigurations settings, int broker, bool before)
    {
        string key = before ? KafkaConfigurations.BrokerTimestampBefore : KafkaConfigurations.BrokerTimestampAfter;
        long? value = before ? settings.DefaultTimestampLimits.Before : settings.DefaultTimestampLimits.After;
        var fallback = new ConfigSynonym(key, (before ? TimestampLimits.DefaultBefore : TimestampLimits.DefaultAfter)
            .ToString(CultureInfo.InvariantCulture), 5);
        ConfigSynonym[] synonyms = value.HasValue ? [new(key, value.Value.ToString(CultureInfo.InvariantCulture), 3), fallback] : [fallback];
        return new(key, synonyms[0].Value, 5, "Default allowable CreateTime difference in milliseconds.",
            broker != KafkaConfigurations.DefaultBroker, synonyms[0].Source, synonyms);
    }

    private static ReportedConfig TopicTimestampLimit(KafkaConfigurations settings, int broker, long? value, bool before)
    {
        var fallback = BrokerTimestampLimit(settings, broker, before);
        string key = before ? KafkaConfigurations.TimestampBefore : KafkaConfigurations.TimestampAfter;
        if (value is null) return fallback with { Name = key, ReadOnly = false };
        string configured = value.Value.ToString(CultureInfo.InvariantCulture);
        return new(key, configured, 5, fallback.Doc, false, 1, [new(key, configured, 1), .. fallback.Synonyms!]);
    }

    private static ReportedConfig BrokerLimitConfig(KafkaConfigurations settings, int broker)
    {
        const string key = KafkaConfigurations.BrokerMessageBytes;
        var synonyms = new List<ConfigSynonym>(3);
        if (broker != KafkaConfigurations.DefaultBroker && settings.BrokerLimits.TryGetValue(broker, out int local))
            synonyms.Add(new(key, FormatSize(local), 2));
        if (settings.BrokerLimits.TryGetValue(KafkaConfigurations.DefaultBroker, out int fallback))
            synonyms.Add(new(key, FormatSize(fallback), 3));
        synonyms.Add(new(key, FormatSize(BrokerOptions.MaxBatchBytes), 5));
        return new(key, synonyms[0].Value, 3, "Inherited maximum encoded record batch bytes.", false,
            synonyms[0].Source, [.. synonyms]);
    }

    internal static void WriteCreatedTopicConfigs(WireWriter w, bool compact, ReportedConfig[] configs)
    {
        w.ArrayLength(configs.Length, compact);
        foreach (var config in configs)
        {
            w.Text(config.Name, compact);
            w.Text(config.Value, compact);
            w.Bool(config.ReadOnly);
            w.I8(config.Source);
            w.Bool(false);
            w.EndStruct(compact);
        }
    }
}
