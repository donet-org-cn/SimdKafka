namespace SimdKafka;

internal sealed partial class Router
{
    private void CreateTopics(ref WireReader r, WireWriter w, Request request)
    {
        bool compact = request.Flexible;
        short version = request.Version;
        var topics = new (string Name, int Partitions, Error Error, int? MessageLimit, RemoteTopicPolicy? Tier,
            bool? LogAppendTime, TimestampLimits TimestampLimits)
            [r.ArrayLength(compact)];
        var names = new HashSet<string>(StringComparer.Ordinal);
        for (int i = 0; i < topics.Length; i++)
        {
            string name = r.Text(compact);
            int partitions = r.I32();
            short replicas = r.I16();
            int assignments = r.ArrayLength(compact);
            bool validAssignment = true;
            var indexes = new HashSet<int>();
            for (int a = 0; a < assignments; a++)
            {
                int partition = r.I32();
                validAssignment &= (uint)partition < assignments && indexes.Add(partition);
                validAssignment &= IsSingleBroker(ref r, compact);
                r.EndStruct(compact);
            }
            var config = KafkaConfigurations.ParseTopic(ReadConfigChanges(ref r, compact, incremental: false), catalog.Tiered);
            r.EndStruct(compact);

            Error error = Error.None;
            if (!names.Add(name)) error = Error.InvalidRequest;
            else if (assignments != 0)
            {
                if (!validAssignment || partitions != -1 || replicas != -1)
                    error = Error.InvalidReplicaAssignment;
                else
                    partitions = assignments;
            }
            else if (replicas != 1 && !(version >= 4 && replicas == -1))
                error = Error.InvalidReplicationFactor;
            if (error == Error.None) error = config.Error;
            if (version >= 4 && partitions == -1) partitions = options.DefaultPartitions;
            topics[i] = (name, partitions, error, config.Limit, config.Tier, config.AppendTime, config.TimestampLimits);
        }
        r.I32(); // timeout
        bool dryRun = version >= 1 && r.Bool();
        r.EndStruct(compact);
        r.End();

        if (version >= 2) w.I32(0);
        w.ArrayLength(topics.Length, compact);
        foreach (var (name, partitions, error, limit, tier, logAppendTime, timestampLimits) in topics)
        {
            Guid id = Guid.Empty;
            var result = !request.AllowsCluster(AclOperation.Create) && !request.AllowsTopic(name, AclOperation.Create) ?
                Error.TopicAuthorizationFailed : error == Error.None ?
                    catalog.Create(name, partitions, dryRun, out id, limit, tier, logAppendTime, timestampLimits) : error;
            w.Text(name, compact);
            if (version >= 7) w.Uuid(id);
            w.Write(result);
            if (version >= 1) w.Text(result == Error.None ? null : result.ToString(), compact);
            if (version >= 5)
            {
                w.I32(result == Error.None ? partitions : -1);
                w.I16(result == Error.None ? (short)1 : (short)-1);
                if (result == Error.None)
                    WriteCreatedTopicConfigs(w, compact, TopicConfiguration(catalog.Configurations, id,
                        BrokerOptions.NodeId, standalone: true, creatingLimit: limit, tierOptions: options, creatingTier: tier,
                        creatingLogAppendTime: logAppendTime, creatingTimestampLimits: timestampLimits));
                else w.ArrayLength(-1, compact);
            }
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
    }

    private void DeleteTopics(ref WireReader r, WireWriter w, Request request)
    {
        bool compact = request.Flexible;
        short version = request.Version;
        var topics = new (string? Name, Guid Id)[r.ArrayLength(compact)];
        for (int i = 0; i < topics.Length; i++)
        {
            if (version >= 6)
            {
                topics[i] = (r.NullableText(compact), r.Uuid());
                r.EndStruct(compact);
            }
            else
                topics[i] = (r.Text(compact), Guid.Empty);
        }
        r.I32(); // timeout
        r.EndStruct(compact);
        r.End();

        if (version >= 1) w.I32(0);
        w.ArrayLength(topics.Length, compact);
        foreach (var (name, id) in topics)
        {
            string? resolved = name ?? catalog.Get(id)?.Name;
            var result = resolved is not null && !request.AllowsTopic(resolved, AclOperation.Delete) ?
                (Error.TopicAuthorizationFailed, name, id) : (name is null) == (id == Guid.Empty) ?
                (Error.InvalidRequest, name, id) : catalog.Delete(name, id);
            if (result.Item1 == Error.None) _changes.Signal();
            w.Text(result.Item2, compact);
            if (version >= 6) w.Uuid(result.Item3);
            w.Write(result.Item1);
            if (version >= 5) w.Text(result.Item1 == Error.None ? null : result.Item1.ToString(), compact);
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
    }

    private void CreatePartitions(ref WireReader r, WireWriter w, Request request)
    {
        bool compact = request.Flexible;
        var topics = new (string Name, int Partitions, int? Assignments, Error Error)[r.ArrayLength(compact)];
        var names = new HashSet<string>(StringComparer.Ordinal);
        for (int i = 0; i < topics.Length; i++)
        {
            string name = r.Text(compact);
            int partitions = r.I32();
            int assignments = r.ArrayLength(compact, nullable: true);
            bool valid = true;
            for (int n = 0; n < assignments; n++)
            {
                valid &= IsSingleBroker(ref r, compact);
                r.EndStruct(compact);
            }
            r.EndStruct(compact);
            Error error = !names.Add(name) ? Error.InvalidRequest :
                valid ? Error.None : Error.InvalidReplicaAssignment;
            topics[i] = (name, partitions, assignments == -1 ? null : assignments, error);
        }
        r.I32(); // timeout
        bool dryRun = r.Bool();
        r.EndStruct(compact);
        r.End();

        w.I32(0);
        w.ArrayLength(topics.Length, compact);
        foreach (var (name, partitions, assignments, error) in topics)
        {
            var result = !request.AllowsTopic(name, AclOperation.Alter) ? Error.TopicAuthorizationFailed :
                error == Error.None ? catalog.Expand(name, partitions, dryRun, assignments) : error;
            if (result == Error.None && !dryRun) _changes.Signal();
            w.Text(name, compact);
            w.Write(result);
            w.Text(result == Error.None ? null : result.ToString(), compact);
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
    }

    private static bool IsSingleBroker(ref WireReader r, bool compact)
    {
        int replicas = r.ArrayLength(compact);
        if (replicas == 1) return r.I32() == BrokerOptions.NodeId;
        r.Take(replicas * sizeof(int));
        return false;
    }

    private void DescribeCluster(ref WireReader r, WireWriter w, Request request)
    {
        short version = request.Version;
        bool authorized = r.Bool();
        sbyte endpoint = version >= 1 ? r.I8() : (sbyte)1;
        if (version >= 2) r.Bool(); // Include fenced brokers.
        r.Tags();
        r.End();

        Error error = !request.AllowsCluster(AclOperation.Describe) ? Error.ClusterAuthorizationFailed :
            endpoint != 1 ? Error.UnsupportedEndpointType : Error.None;
        bool supported = error == Error.None;
        w.I32(0);
        w.Write(error);
        w.Text(supported ? null : error.ToString(), compact: true);
        if (version >= 1) w.I8(1);
        w.Text(error == Error.ClusterAuthorizationFailed ? "" : catalog.ClusterId, compact: true);
        w.I32(supported ? BrokerOptions.NodeId : -1);
        w.ArrayLength(supported ? 1 : 0, compact: true);
        if (supported)
        {
            w.I32(BrokerOptions.NodeId);
            w.Text(options.Host, compact: true);
            w.I32(options.Port);
            w.Text(null, compact: true); // rack
            if (version >= 2) w.Bool(false);
            w.UVar(0);
        }
        w.I32(authorized && supported ? request.Access?.Operations(AclResource.Cluster, KafkaAcl.ClusterName) ??
            ClusterOperations : int.MinValue);
        w.UVar(0);
    }

    private void DescribeTopicPartitions(ref WireReader r, WireWriter w, Request request)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        int count = r.ArrayLength(compact: true);
        for (int i = 0; i < count; i++)
        {
            names.Add(r.Text(compact: true));
            r.Tags();
        }
        int limit = r.I32();
        if (limit <= 0) throw new WireException("Invalid partition limit");
        string? cursor = null;
        int index = 0;
        if (r.I8() >= 0)
        {
            cursor = r.Text(compact: true);
            index = r.I32();
            if (index < 0) throw new WireException("Invalid partition cursor");
            r.Tags();
        }
        r.Tags();
        r.End();
        if (count == 0) names.UnionWith(catalog.Topics().Where(topic => request.AllowsTopic(topic.Name, AclOperation.Describe))
            .Select(static topic => topic.Name));

        var page = new List<(string Name, Topic? Topic, int Start, int Size, Error Error)>();
        (string Name, int Index)? continuation = null;
        foreach (string name in names.Order(StringComparer.Ordinal))
        {
            int compare = cursor is null ? 1 : string.CompareOrdinal(name, cursor);
            if (compare < 0) continue;
            bool allowed = request.AllowsTopic(name, AclOperation.Describe);
            var topic = allowed ? catalog.Get(name) : null;
            int start = compare == 0 ? index : 0;
            int available = Math.Max(0, (topic?.Partitions.Length ?? 0) - start);
            int size = Math.Min(available, limit);
            if (topic is null || size > 0) page.Add((name, topic, start, size,
                !allowed ? Error.TopicAuthorizationFailed : topic is null ? Error.UnknownTopicOrPartition : Error.None));
            limit -= size;
            if (size < available)
            {
                continuation = (name, start + size);
                break;
            }
        }

        w.I32(0);
        w.ArrayLength(page.Count, compact: true);
        foreach (var (name, topic, start, size, error) in page)
        {
            w.Write(error);
            w.Text(name, compact: true);
            w.Uuid(topic?.Id ?? Guid.Empty);
            w.Bool(false);
            w.ArrayLength(size, compact: true);
            for (int p = start; p < start + size; p++)
            {
                w.Write(Error.None);
                w.I32(p);
                w.I32(BrokerOptions.NodeId);
                w.I32(0);
                WriteLeaderNodes(w, compact: true);
                WriteLeaderNodes(w, compact: true);
                w.ArrayLength(0, compact: true); // eligible leaders
                w.ArrayLength(0, compact: true); // last-known eligible leaders
                w.ArrayLength(0, compact: true);  // offline replicas
                w.UVar(0);
            }
            w.I32(topic is null ? int.MinValue : request.Access?.Operations(AclResource.Topic, name) ?? TopicOperations);
            w.UVar(0);
        }
        if (continuation is { } more)
        {
            w.I8(1);
            w.Text(more.Name, compact: true);
            w.I32(more.Index);
            w.UVar(0);
        }
        else w.I8(-1);
        w.UVar(0);
    }

    private void DescribeConfigs(ref WireReader r, WireWriter w, Request request)
        => WriteConfigs(ref r, w, request, (type, name) => type switch
        {
            2 when catalog.Get(name) is { } topic => (Error.None,
                TopicConfiguration(catalog.Configurations, topic.Id, BrokerOptions.NodeId, standalone: true, tierOptions: options,
                    creatingTier: catalog.TopicTier(topic))),
            2 => (Error.UnknownTopicOrPartition, []),
            4 when name.Length == 0 || name == BrokerOptions.NodeId.ToString() => (Error.None, BrokerConfig(options, 1,
                catalog.Configurations, name.Length == 0 ? KafkaConfigurations.DefaultBroker : BrokerOptions.NodeId)),
            _ => (Error.InvalidRequest, [])
        });

    internal static void WriteConfigs(ref WireReader r, WireWriter w, Request request,
        Func<sbyte, string, (Error Error, ReportedConfig[] Configs)> lookup)
    {
        bool compact = request.Flexible;
        short version = request.Version;
        var resources = new (sbyte Type, string Name, HashSet<string>? Keys)[r.ArrayLength(compact)];
        for (int i = 0; i < resources.Length; i++)
        {
            sbyte type = r.I8();
            string name = r.Text(compact);
            int count = r.ArrayLength(compact, nullable: true);
            HashSet<string>? keys = count < 0 ? null : new(StringComparer.Ordinal);
            for (int k = 0; k < count; k++) keys!.Add(r.Text(compact));
            r.EndStruct(compact);
            resources[i] = (type, name, keys);
        }
        bool synonyms = version >= 1 && r.Bool();
        bool documentation = version >= 3 && r.Bool();
        r.EndStruct(compact);
        r.End();

        w.I32(0);
        w.ArrayLength(resources.Length, compact);
        foreach (var (type, name, keys) in resources)
        {
            Error access = ConfigurationAccess(request, type, name, AclOperation.DescribeConfigs);
            var (error, configs) = access == Error.None ? lookup(type, name) : (access, []);
            if (keys is not null) configs = configs.Where(config => keys.Contains(config.Name)).ToArray();
            w.Write(error);
            w.Text(error == Error.None ? null : error.ToString(), compact);
            w.I8(type);
            w.Text(name, compact);
            w.ArrayLength(configs.Length, compact);
            foreach (var config in configs)
            {
                w.Text(config.Name, compact);
                w.Text(config.Value, compact);
                w.Bool(config.ReadOnly);
                if (version == 0) w.Bool(config.Source == 5);
                else w.I8(config.Source);
                w.Bool(false); // not sensitive
                if (version >= 1)
                {
                    var alternatives = synonyms ? config.Synonyms ?? [new(config.Name, config.Value, config.Source)] : [];
                    w.ArrayLength(alternatives.Length, compact);
                    foreach (var alternative in alternatives)
                    {
                        w.Text(alternative.Name, compact); w.Text(alternative.Value, compact); w.I8(alternative.Source);
                        w.EndStruct(compact);
                    }
                }
                if (version >= 3)
                {
                    w.I8(config.Type);
                    w.Text(documentation ? config.Doc : null, compact);
                }
                w.EndStruct(compact);
            }
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
    }

    internal readonly record struct ConfigSynonym(string Name, string Value, sbyte Source);
    internal readonly record struct ReportedConfig(string Name, string Value, sbyte Type, string Doc,
        bool ReadOnly = true, sbyte Source = 5, ConfigSynonym[]? Synonyms = null);
    internal static readonly ReportedConfig[] LogConfig =
    [
        new("compression.type", "producer", 2, "Logs preserve the producer's codec."),
        new("retention.ms", "-1", 5, "Time-based retention is disabled."),
        new("retention.bytes", "-1", 5, "Size-based retention is disabled.")
    ];

    internal static ReportedConfig[] BrokerConfig(BrokerOptions options, int defaultReplicaCount, KafkaConfigurations settings, int broker) =>
    [
        new("num.partitions", options.DefaultPartitions.ToString(), 3, "Default partition count.", Source: options.DefaultPartitions == 1 ? (sbyte)5 : (sbyte)4),
        new("default.replication.factor", defaultReplicaCount.ToString(), 3, "Default topic replication factor."),
        BrokerLimitConfig(settings, broker),
        BrokerTimestampConfig(settings, broker),
        BrokerTimestampLimit(settings, broker, before: true),
        BrokerTimestampLimit(settings, broker, before: false),
        new("remote.log.storage.system.enable", options.TierDirectory is null ? "false" : "true", 1,
            "Whether the broker has a remote log provider.", Source: options.TierDirectory is null ? (sbyte)5 : (sbyte)4),
        new("auto.create.topics.enable", options.AutoCreate ? "true" : "false", 1, "Create a topic when requested by metadata.",
            Source: options.AutoCreate ? (sbyte)5 : (sbyte)4)
    ];
}
