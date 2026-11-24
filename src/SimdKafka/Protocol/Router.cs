using System.Collections.Immutable;

namespace SimdKafka;

internal abstract class RequestRouter
{
    internal abstract ClientQuotas CurrentQuotas { get; }
    internal virtual Task RunGroupClockAsync(CancellationToken stop) => Task.CompletedTask;
    internal virtual ValueTask<ImmutableArray<KafkaAcl>> ReadAclsAsync(CancellationToken stop) => ValueTask.FromResult(ImmutableArray<KafkaAcl>.Empty);
    internal abstract ValueTask<ScramReadResult> ReadScramAsync(CancellationToken stop);
    internal abstract TokenDirectory CurrentTokens { get; }
    internal abstract ValueTask<(Error Error, TokenDirectory? Directory)> ReadTokensAsync(CancellationToken stop);
    internal abstract ValueTask<bool> DispatchAsync(Request request, Memory<byte> body, WireWriter writer, CancellationToken stop);
}

internal sealed partial class Router(BrokerOptions options, Catalog catalog) : RequestRouter
{
    private static readonly (Api Key, short Min, short Max)[] Versions =
    [
        (Api.Produce, 3, 13), (Api.Fetch, 4, 13), (Api.ListOffsets, 0, 11), (Api.Metadata, 0, 13),
        (Api.OffsetCommit, 0, 7), (Api.OffsetFetch, 0, 5), (Api.FindCoordinator, 0, 6),
        (Api.JoinGroup, 0, 5), (Api.Heartbeat, 0, 3), (Api.LeaveGroup, 0, 3), (Api.SyncGroup, 0, 3),
        (Api.DescribeGroups, 0, 6), (Api.ListGroups, 0, 5), (Api.ApiVersions, 0, Request.MaxApiVersions),
        (Api.CreateTopics, 0, 7), (Api.DeleteTopics, 0, 6), (Api.DeleteRecords, 0, 2), (Api.InitProducerId, 0, 5),
        (Api.DescribeConfigs, 0, 4), (Api.AlterConfigs, 0, 2), (Api.IncrementalAlterConfigs, 0, 1),
        (Api.DescribeAcls, 0, 3), (Api.CreateAcls, 0, 3), (Api.DeleteAcls, 0, 3),
        (Api.CreatePartitions, 0, 3), (Api.DeleteGroups, 0, 3),
        (Api.OffsetDelete, 0, 0), (Api.DescribeCluster, 0, 2), (Api.ListConfigResources, 1, 1), (Api.DescribeTopicPartitions, 0, 0),
        (Api.DescribeUserScramCredentials, 0, 0), (Api.AlterUserScramCredentials, 0, 0),
        (Api.DescribeClientQuotas, 0, 1), (Api.AlterClientQuotas, 0, 1), (Api.OffsetForLeaderEpoch, 2, 4),
        (Api.DescribeLogDirs, 1, 4)
    ];

    private readonly GroupCoordinator _groups = new();
    private readonly LogChanges _changes = new();

    internal override ClientQuotas CurrentQuotas => catalog.Quotas;
    internal override TokenDirectory CurrentTokens => catalog.Tokens;
    internal override ValueTask<(Error Error, TokenDirectory? Directory)> ReadTokensAsync(CancellationToken stop) =>
        ValueTask.FromResult<(Error, TokenDirectory?)>((Error.None, catalog.Tokens));
    internal override Task RunGroupClockAsync(CancellationToken stop) => catalog.Tiered ?
        Task.WhenAll(_groups.RunClockAsync(stop), catalog.RunTierClockAsync(stop)) : _groups.RunClockAsync(stop);
    internal override ValueTask<ImmutableArray<KafkaAcl>> ReadAclsAsync(CancellationToken stop) => ValueTask.FromResult(catalog.Acls);
    internal override ValueTask<ScramReadResult> ReadScramAsync(CancellationToken stop) => ValueTask.FromResult(new ScramReadResult(Error.None, catalog.Scram));

    internal override ValueTask<bool> DispatchAsync(Request request, Memory<byte> body, WireWriter w, CancellationToken stop)
    {
        short v = request.Version;
        request.RequireCoveredRoute();
        if (IsTokenApi(request.Api) && SupportsTokenApi(request.Api, v))
            return HandleTokenAsync(body, w, request, () => ReadTokensAsync(stop), change => ValueTask.FromResult(catalog.ChangeToken(change)));
        if (!Supports(request.Api, v))
        {
            if (request.Api != Api.ApiVersions) throw new WireException("Unsupported Kafka API or version");
            w.Write(Error.UnsupportedVersion);
            w.I32(0); // Unsupported ApiVersions requests fall back to the version 0 response.
            return ValueTask.FromResult(true);
        }
        var r = new WireReader(body.Span);
        switch (request.Api)
        {
            case Api.ApiVersions: WriteVersions(ref r, w, v, Versions, options.SaslEnabled, catalog.ClusterId, BrokerOptions.NodeId,
                delegationTokens: options.DelegationTokenSecretFile is not null); break;
            case Api.Metadata: Metadata(ref r, w, request); break;
            case Api.Produce: return ProduceAsync(body, w, request);
            case Api.Fetch: return FetchAsync(body, w, request, stop);
            case Api.ListOffsets: ListOffsets(ref r, w, request); break;
            case Api.OffsetForLeaderEpoch: OffsetForLeaderEpoch(ref r, w, request); break;
            case Api.CreateTopics: CreateTopics(ref r, w, request); break;
            case Api.DeleteTopics: DeleteTopics(ref r, w, request); break;
            case Api.DeleteRecords: DeleteRecords(body, w, request); break;
            case Api.CreatePartitions: CreatePartitions(ref r, w, request); break;
            case Api.DescribeCluster: DescribeCluster(ref r, w, request); break;
            case Api.DescribeLogDirs: DescribeLogDirs(ref r, w, request); break;
            case Api.DescribeTopicPartitions: DescribeTopicPartitions(ref r, w, request); break;
            case Api.DescribeConfigs: DescribeConfigs(ref r, w, request); break;
            case Api.DescribeAcls or Api.CreateAcls or Api.DeleteAcls:
                return HandleAclsAsync(body, w, request, () => ValueTask.FromResult(new AclReadResult(Error.None, catalog.Acls)),
                    mutation => ValueTask.FromResult(catalog.MutateAcl(mutation)));
            case Api.AlterConfigs or Api.IncrementalAlterConfigs:
                return AlterConfigurationsAsync(body, w, request, update => ValueTask.FromResult(catalog.AlterConfiguration(update)));
            case Api.DescribeUserScramCredentials: return DescribeScramAsync(body, w, request, options.DynamicScram, () => ReadScramAsync(stop));
            case Api.AlterUserScramCredentials:
                return AlterScramAsync(body, w, request, options.DynamicScram, (name, change) => ValueTask.FromResult(catalog.AlterScram(name, change)));
            case Api.DescribeClientQuotas:
                return DescribeQuotasAsync(body, w, request, () => ValueTask.FromResult(new QuotaReadResult(Error.None, CurrentQuotas)));
            case Api.AlterClientQuotas:
                return AlterQuotasAsync(body, w, request, update => ValueTask.FromResult(catalog.AlterQuota(update)));
            case Api.InitProducerId:
                var init = ReadInitProducerId(ref r, v);
                var producerError = init.Error != Error.None ? init.Error : init.TransactionalId is not null ? Error.InvalidTxnState :
                    request.Access is { } access && !access.AllowsIdempotence(catalog.Topics().Select(static topic => topic.Name)) ?
                        Error.ClusterAuthorizationFailed : Error.None;
                var (allocationError, id) = producerError == Error.None ? catalog.AllocateProducerId() : (producerError, -1L);
                WriteInitProducerId(w, v, allocationError, id);
                break;
            case Api.FindCoordinator: FindCoordinator(ref r, w, request); break;
            case Api.JoinGroup: return EncodeJoinAsync(ReadJoin(ref r, request), w, request, stop);
            case Api.SyncGroup: return EncodeSyncAsync(ReadSync(ref r, v), w, request, stop);
            case Api.Heartbeat: WriteHeartbeat(ref r, w, request, _groups, coordinator: true); break;
            case Api.LeaveGroup: WriteLeaveGroup(ref r, w, request, _groups, coordinator: true); break;
            case Api.OffsetCommit: OffsetCommit(ref r, w, request); break;
            case Api.OffsetFetch: OffsetFetch(ref r, w, request); break;
            case Api.ListConfigResources:
                WriteConfigResources(ref r, w, request, Error.None, catalog.Configurations,
                    catalog.Topics().Select(static topic => (topic.Name, topic.Id))); break;
            case Api.ListGroups: ListGroups(ref r, w, request); break;
            case Api.DescribeGroups: DescribeGroups(ref r, w, request); break;
            case Api.DeleteGroups: DeleteGroups(ref r, w, request); break;
            case Api.OffsetDelete: OffsetDelete(ref r, w, request); break;
        }
        return ValueTask.FromResult(true);
    }

    private static bool Supports(Api api, short version)
    {
        foreach (var (key, min, max) in Versions)
            if (key == api) return version >= min && version <= max;
        return false;
    }

    internal static void WriteVersions(ref WireReader r, WireWriter w, short v,
        ReadOnlySpan<(Api Key, short Min, short Max)> versions, bool sasl, string clusterId, int brokerId,
        bool transactionV2 = false, bool delegationTokens = false)
    {
        if (v is < 0 or > Request.MaxApiVersions)
        {
            w.Write(Error.UnsupportedVersion);
            w.I32(0);
            return;
        }
        bool flexible = v >= 3;
        Error error = Error.None;
        if (flexible)
        {
            r.CompactString();
            r.CompactString();
            if (v >= 5)
            {
                string? expectedCluster = r.NullableText(true);
                int expectedBroker = r.I32();
                if ((expectedCluster is null) != (expectedBroker == -1)) error = Error.InvalidRequest;
                else if (expectedCluster is not null &&
                    (expectedBroker != brokerId || !string.Equals(expectedCluster, clusterId, StringComparison.Ordinal)))
                    error = Error.RebootstrapRequired;
            }
            r.Tags();
        }
        r.End();
        w.Write(error);
        if (error != Error.None) { versions = []; sasl = false; delegationTokens = false; }
        int count = versions.Length + (sasl ? 2 : 0) + (delegationTokens ? DelegationTokenVersions.Length : 0);
        if (flexible) w.UVar((uint)count + 1);
        else w.I32(count);
        foreach (var (key, min, max) in versions)
        {
            w.I16((short)key);
            w.I16(min);
            w.I16(max);
            if (flexible) w.UVar(0);
        }
        if (delegationTokens)
            foreach (var (key, min, max) in DelegationTokenVersions)
            {
                w.I16((short)key);
                w.I16(min);
                w.I16(max);
                w.EndStruct(flexible);
            }
        if (sasl)
        {
            w.I16((short)Api.SaslHandshake); w.I16(0); w.I16(1);
            if (flexible) w.UVar(0);
            w.I16((short)Api.SaslAuthenticate); w.I16(0); w.I16(2);
            if (flexible) w.UVar(0);
        }
        if (v >= 1) w.I32(0);
        if (flexible)
        {
            if (error == Error.None && transactionV2) WriteTransactionFeature(w, v);
            else w.UVar(0);
        }
    }

    private static void WriteTransactionFeature(WireWriter w, short version)
    {
        const string name = "transaction.version";
        uint size = 1 + 1 + (uint)name.Length + 2 + 2 + 1;
        w.UVar(version >= 4 ? 3u : 2u);
        if (version >= 4)
        {
            w.UVar(0); w.UVar(size);
            w.ArrayLength(1, compact: true); w.Text(name, compact: true);
            w.I16(0); w.I16(2); w.UVar(0);
        }
        w.UVar(1); w.UVar(sizeof(long)); w.I64(0);
        w.UVar(2); w.UVar(size);
        w.ArrayLength(1, compact: true); w.Text(name, compact: true);
        w.I16(2); w.I16(2); w.UVar(0);
    }

    private void Metadata(ref WireReader r, WireWriter w, Request request)
    {
        short v = request.Version;
        bool compact = request.Flexible;
        int count = r.ArrayLength(compact, nullable: v >= 1);
        var requested = new (string? Name, Guid Id)[Math.Max(count, 0)];
        for (int i = 0; i < count; i++)
        {
            Guid id = v >= 10 ? r.Uuid() : Guid.Empty;
            string? name = v >= 10 ? r.NullableText(compact) : r.Text(compact);
            requested[i] = (name, id);
            r.EndStruct(compact);
        }
        bool autoCreate = v < 4 || r.Bool();
        bool clusterOps = v is >= 8 and <= 10 && r.Bool();
        bool topicOps = v >= 8 && r.Bool();
        r.EndStruct(compact);
        r.End();

        if (v >= 3) w.I32(0);
        w.ArrayLength(1, compact);
        w.I32(BrokerOptions.NodeId);
        w.Text(options.Host, compact);
        w.I32(options.Port);
        if (v >= 1) w.Text(null, compact);
        w.EndStruct(compact);
        if (v >= 2) w.Text(catalog.ClusterId, compact);
        if (v >= 1) w.I32(BrokerOptions.NodeId);
        if (count == -1 || (v == 0 && count == 0))
        {
            var all = catalog.Topics();
            if (request.Access is { SuperUser: false }) all = all.Where(topic => request.AllowsTopic(topic.Name, AclOperation.Describe)).ToArray();
            w.ArrayLength(all.Length, compact);
            foreach (var topic in all)
                WriteTopic(w, v, compact, topic.Name, topic.Id, Error.None, topic, topicOps, request.Access);
        }
        else
        {
            w.ArrayLength(count, compact);
            foreach (var (name, id) in requested)
            {
                Topic? topic;
                Error error;
                bool byId = v >= 12 && id != Guid.Empty;
                if (byId)
                {
                    topic = catalog.Get(id);
                    error = topic is null ? Error.UnknownTopicId : Error.None;
                    if (topic is not null && !request.AllowsTopic(topic.Name, AclOperation.Describe))
                        (error, topic) = (Error.TopicAuthorizationFailed, null);
                }
                else if (id == Guid.Empty && name is not null)
                {
                    if (!request.AllowsTopic(name, AclOperation.Describe))
                        (error, topic) = (Error.TopicAuthorizationFailed, null);
                    else if (autoCreate && options.AutoCreate && Catalog.ValidName(name) && catalog.Get(name) is null &&
                        !request.AllowsCluster(AclOperation.Create) && !request.AllowsTopic(name, AclOperation.Create))
                        (error, topic) = (Error.TopicAuthorizationFailed, null);
                    else (error, topic) = catalog.Resolve(name, autoCreate);
                }
                else
                    (error, topic) = (Error.InvalidRequest, null);
                WriteTopic(w, v, compact, topic?.Name ?? (byId ? null : name), topic?.Id ?? id, error, topic, topicOps, request.Access);
            }
        }
        if (v is >= 8 and <= 10) w.I32(clusterOps ?
            request.Access?.Operations(AclResource.Cluster, KafkaAcl.ClusterName) ?? ClusterOperations : int.MinValue);
        if (v >= 13) w.Write(Error.None);
        w.EndStruct(compact);
    }

    private const int ClusterOperations = (1 << 5) | (1 << 8) | (1 << 12);
    private const int TopicOperations = (1 << 3) | (1 << 4) | (1 << 6) | (1 << 7) | (1 << 8) | (1 << 10);

    private static void WriteTopic(WireWriter w, short v, bool compact, string? name, Guid id,
        Error error, Topic? topic, bool operations, KafkaAuthorization? access)
    {
        w.Write(error);
        w.Text(v >= 12 ? name : name ?? "", compact);
        if (v >= 10) w.Uuid(id);
        if (v >= 1) w.Bool(false);
        int count = topic?.Partitions.Length ?? 0;
        w.ArrayLength(count, compact);
        for (int p = 0; p < count; p++)
        {
            w.Write(Error.None);
            w.I32(p);
            w.I32(BrokerOptions.NodeId);
            if (v >= 7) w.I32(0);
            WriteLeaderNodes(w, compact);
            WriteLeaderNodes(w, compact);
            if (v >= 5) w.ArrayLength(0, compact);
            w.EndStruct(compact);
        }
        if (v >= 8) w.I32(operations && error == Error.None ? access?.Operations(AclResource.Topic, name!) ?? TopicOperations : int.MinValue);
        w.EndStruct(compact);
    }

    private static void WriteLeaderNodes(WireWriter w, bool compact)
    {
        w.ArrayLength(1, compact);
        w.I32(BrokerOptions.NodeId);
    }

    private void FindCoordinator(ref WireReader r, WireWriter w, Request request)
    {
        short v = request.Version;
        bool compact = v >= 3;
        string key = v <= 3 ? r.Text(compact) : "";
        sbyte kind = v >= 1 ? r.I8() : (sbyte)0;
        if (v >= 1) w.I32(0);
        if (v >= 4)
        {
            int count = r.ArrayLength(compact: true);
            w.ArrayLength(count, compact: true);
            for (int i = 0; i < count; i++)
            {
                key = r.Text(compact: true);
                var error = CoordinatorError(kind, key, request);
                w.Text(key, compact: true);
                WriteCoordinatorAddress(w, error, compact: true);
                w.Write(error);
                w.Text(null, compact: true);
                w.EndStruct(compact: true);
            }
        }
        else
        {
            var error = CoordinatorError(kind, key, request);
            w.Write(error);
            if (v >= 1) w.Text(null, compact);
            WriteCoordinatorAddress(w, error, compact);
        }
        r.EndStruct(compact);
        r.End();
        w.EndStruct(compact);
    }

    private static Error CoordinatorError(sbyte kind, string key, Request request) =>
        kind != 0 ? Error.UnsupportedVersion : !request.AllowsGroup(key, AclOperation.Describe) ? Error.GroupAuthorizationFailed :
        key.Length == 0 ? Error.InvalidGroupId : Error.None;

    private void WriteCoordinatorAddress(WireWriter w, Error error, bool compact)
    {
        w.I32(error == Error.None ? BrokerOptions.NodeId : -1);
        w.Text(error == Error.None ? options.Host : "", compact);
        w.I32(error == Error.None ? options.Port : -1);
    }

    internal readonly record struct ProducerInit(string? TransactionalId, int Timeout, long ProducerId, short Epoch, Error Error,
        bool EnableTwoPhase, bool KeepPrepared);

    internal static ProducerInit ReadInitProducerId(ref WireReader r, short version)
    {
        bool compact = version >= 2;
        var transactionalId = r.NullableText(compact);
        int timeout = r.I32();
        long clientProducerId = version >= 3 ? r.I64() : -1;
        short clientEpoch = version >= 3 ? r.I16() : (short)-1;
        bool enableTwoPhase = version >= 6 && r.Bool();
        bool keepPrepared = version >= 6 && r.Bool();
        r.EndStruct(compact);
        r.End();
        var error = clientProducerId < -1 || clientEpoch < -1 || (clientProducerId == -1) != (clientEpoch == -1) ||
            transactionalId is "" ? Error.InvalidRequest : Error.None;
        return new(transactionalId, timeout, clientProducerId, clientEpoch, error, enableTwoPhase, keepPrepared);
    }

    internal static void WriteInitProducerId(WireWriter w, short version, Error error, long id, short epoch = 0,
        long ongoingId = -1, short ongoingEpoch = -1)
    {
        w.I32(0);
        w.Write(error);
        w.I64(id);
        w.I16(error == Error.None ? epoch : (short)-1);
        if (version >= 6)
        {
            w.I64(error == Error.None ? ongoingId : -1);
            w.I16(error == Error.None ? ongoingEpoch : (short)-1);
        }
        w.EndStruct(version >= 2);
    }

    private static PartitionLog? GetPartition(Topic? topic, int p) =>
        topic is not null && (uint)p < (uint)topic.Partitions.Length ? topic.Partitions[p] : null;
}
