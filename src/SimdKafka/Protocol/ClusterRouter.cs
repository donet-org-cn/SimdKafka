using System.Buffers.Binary;
using System.Collections.Immutable;
using DotNext.Net.Cluster.Consensus.Raft;

namespace SimdKafka;

internal sealed partial class ClusterRouter(BrokerOptions options, BrokerCluster cluster) : RequestRouter
{
    private static readonly (Api Key, short Min, short Max)[] Versions =
    [
        (Api.Produce, 3, 13), (Api.Fetch, 4, 13), (Api.ListOffsets, 1, 11), (Api.Metadata, 0, 13),
        (Api.OffsetCommit, 0, 10), (Api.OffsetFetch, 0, 10), (Api.FindCoordinator, 0, 6), (Api.JoinGroup, 0, 5),
        (Api.Heartbeat, 0, 3), (Api.LeaveGroup, 0, 3), (Api.SyncGroup, 0, 3),
        (Api.DescribeGroups, 0, 6), (Api.ListGroups, 0, 5), (Api.DeleteGroups, 0, 3), (Api.OffsetDelete, 0, 0),
        (Api.ApiVersions, 0, Request.MaxApiVersions), (Api.CreateTopics, 0, 7), (Api.DeleteTopics, 0, 6),
        (Api.DeleteRecords, 0, 2),
        (Api.InitProducerId, 0, 6), (Api.OffsetForLeaderEpoch, 2, 4), (Api.AddPartitionsToTxn, 0, 3),
        (Api.AddOffsetsToTxn, 0, 4), (Api.EndTxn, 0, 5), (Api.TxnOffsetCommit, 0, 6), (Api.DescribeConfigs, 0, 4),
        (Api.AlterConfigs, 0, 2), (Api.IncrementalAlterConfigs, 0, 1),
        (Api.DescribeAcls, 0, 3), (Api.CreateAcls, 0, 3), (Api.DeleteAcls, 0, 3),
        (Api.DescribeLogDirs, 1, 4), (Api.CreatePartitions, 0, 3),
        (Api.AlterPartitionReassignments, 0, 1), (Api.ListPartitionReassignments, 0, 0), (Api.DescribeCluster, 0, 2),
        (Api.DescribeTopicPartitions, 0, 0), (Api.DescribeProducers, 0, 0),
        (Api.DescribeTransactions, 0, 0), (Api.ListTransactions, 0, 2),
        (Api.ConsumerGroupHeartbeat, 0, 1), (Api.ConsumerGroupDescribe, 0, 1), (Api.ListConfigResources, 1, 1),
        (Api.DescribeUserScramCredentials, 0, 0), (Api.AlterUserScramCredentials, 0, 0),
        (Api.DescribeClientQuotas, 0, 1), (Api.AlterClientQuotas, 0, 1),
        (Api.DescribeQuorum, 0, 2),
        (Api.ShareGroupHeartbeat, 1, 1), (Api.ShareGroupDescribe, 1, 1),
        (Api.ShareFetch, 1, 2), (Api.ShareAcknowledge, 1, 2), (Api.DescribeShareGroupOffsets, 0, 1),
        (Api.AlterShareGroupOffsets, 0, 0), (Api.DeleteShareGroupOffsets, 0, 0)
    ];

    internal override ValueTask<bool> DispatchAsync(Request request, Memory<byte> body, WireWriter w, CancellationToken stop)
    {
        short v = request.Version;
        request.RequireCoveredRoute();
        var r = new WireReader(body.Span);
        if (request.Api == Api.ApiVersions)
        {
            Router.WriteVersions(ref r, w, v, Versions, options.SaslEnabled, cluster.ClusterId, cluster.Local.Id, transactionV2: true,
                delegationTokens: options.DelegationTokenSecretFile is not null);
            return ValueTask.FromResult(true);
        }
        if (Router.IsTokenApi(request.Api) && Router.SupportsTokenApi(request.Api, v))
            return Router.HandleTokenAsync(body, w, request, () => ReadTokensAsync(stop), change => ChangeTokenAsync(change, stop));
        foreach (var (key, min, max) in Versions)
        {
            if (key != request.Api || v < min || v > max) continue;
            switch (key)
            {
                case Api.Metadata: return MetadataAsync(body, w, request, stop);
                case Api.Produce: return ProduceAsync(body, w, request, stop);
                case Api.Fetch: return FetchAsync(body, w, request, stop);
                case Api.ListOffsets: return ListOffsetsAsync(body, w, request, stop);
                case Api.OffsetForLeaderEpoch: OffsetForLeaderEpoch(ref r, w, request); break;
                case Api.CreateTopics: return CreateTopicsAsync(body, w, request, stop);
                case Api.CreatePartitions: return CreatePartitionsAsync(body, w, request, stop);
                case Api.DeleteTopics: return DeleteTopicsAsync(body, w, request, stop);
                case Api.DeleteRecords: return DeleteRecordsAsync(body, w, request, stop);
                case Api.DescribeTopicPartitions: DescribeTopicPartitions(ref r, w, request); break;
                case Api.DescribeLogDirs: DescribeLogDirs(ref r, w, request); break;
                case Api.DescribeProducers: DescribeProducers(ref r, w, request); break;
                case Api.DescribeTransactions: return DescribeTransactionsAsync(body, w, request, stop);
                case Api.ListTransactions: return ListTransactionsAsync(body, w, request, stop);
                case Api.FindCoordinator: FindCoordinator(ref r, w, request); break;
                case Api.ConsumerGroupHeartbeat: return ConsumerHeartbeatAsync(body, w, request, stop);
                case Api.ConsumerGroupDescribe: return DescribeConsumerGroupsAsync(body, w, request, stop);
                case Api.ShareGroupHeartbeat: return ShareHeartbeatAsync(body, w, request, stop);
                case Api.ShareGroupDescribe: return DescribeShareGroupsAsync(body, w, request, stop);
                case Api.ShareFetch or Api.ShareAcknowledge: return ShareFetchAsync(body, w, request, stop);
                case Api.DescribeShareGroupOffsets: return DescribeShareOffsetsAsync(body, w, request, stop);
                case Api.AlterShareGroupOffsets or Api.DeleteShareGroupOffsets: return AlterShareOffsetsAsync(body, w, request, stop);
                case Api.JoinGroup: return JoinGroupAsync(Router.ReadJoin(ref r, request), w, request, stop);
                case Api.SyncGroup: return SyncGroupAsync(Router.ReadSync(ref r, v), w, request, stop);
                case Api.Heartbeat: Router.WriteHeartbeat(ref r, w, request, _groups, !GroupLeadership().IsCancellationRequested); break;
                case Api.LeaveGroup: Router.WriteLeaveGroup(ref r, w, request, _groups, !GroupLeadership().IsCancellationRequested); break;
                case Api.OffsetCommit: return CommitOffsetsAsync(body, w, request, stop);
                case Api.OffsetFetch: return FetchOffsetsAsync(body, w, request, stop);
                case Api.DescribeGroups: return DescribeGroupsAsync(body, w, request, stop);
                case Api.ListGroups: ListGroups(ref r, w, request); break;
                case Api.DeleteGroups: return DeleteGroupsAsync(body, w, request, stop);
                case Api.OffsetDelete: return DeleteOffsetsAsync(body, w, request, stop);
                case Api.AlterPartitionReassignments: return AlterReassignmentsAsync(body, w, request, stop);
                case Api.ListPartitionReassignments: ListReassignments(ref r, w, request); break;
                case Api.InitProducerId:
                    return InitProducerIdAsync(Router.ReadInitProducerId(ref r, v), w, request, stop);
                case Api.AddPartitionsToTxn: return AddTransactionPartitionsAsync(body, w, request, stop);
                case Api.AddOffsetsToTxn: return AddTransactionOffsetsAsync(body, w, request, stop);
                case Api.EndTxn: return EndTransactionAsync(body, w, request, stop);
                case Api.TxnOffsetCommit: return CommitTransactionOffsetsAsync(body, w, request, stop);
                case Api.DescribeCluster: DescribeCluster(ref r, w, request); break;
                case Api.DescribeQuorum: return DescribeQuorumAsync(body, w, request, stop);
                case Api.DescribeConfigs:
                    return DescribeConfigurationsAsync(body, w, request, stop);
                case Api.ListConfigResources:
                    return ListConfigurationResourcesAsync(body, w, request, stop);
                case Api.AlterConfigs or Api.IncrementalAlterConfigs:
                    return Router.AlterConfigurationsAsync(body, w, request, update => AlterConfigurationAsync(update, stop));
                case Api.DescribeAcls or Api.CreateAcls or Api.DeleteAcls:
                    return Router.HandleAclsAsync(body, w, request, () => ReadAclSnapshotAsync(stop), mutation => MutateAclAsync(mutation, stop));
                case Api.DescribeUserScramCredentials:
                    return Router.DescribeScramAsync(body, w, request, options.DynamicScram, () => ReadScramAsync(stop));
                case Api.AlterUserScramCredentials:
                    return Router.AlterScramAsync(body, w, request, options.DynamicScram, (name, change) => AlterScramAsync(name, change, stop));
                case Api.DescribeClientQuotas:
                    return Router.DescribeQuotasAsync(body, w, request, () => ReadQuotasAsync(stop));
                case Api.AlterClientQuotas:
                    return Router.AlterQuotasAsync(body, w, request, update => AlterQuotaAsync(update, stop));
            }
            return ValueTask.FromResult(true);
        }
        throw new WireException("Unsupported Kafka API or version in cluster mode");
    }

    private async ValueTask<bool> MetadataAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request, CancellationToken stop)
    {
        var r = new WireReader(body.Span);
        short v = request.Version;
        bool compact = request.Flexible;
        int count = r.ArrayLength(compact, nullable: v >= 1);
        var requested = new (string? Name, Guid Id)[Math.Max(count, 0)];
        for (int i = 0; i < count; i++)
        {
            Guid id = v >= 10 ? r.Uuid() : Guid.Empty;
            requested[i] = (v >= 10 ? r.NullableText(compact) : r.Text(compact), id);
            r.EndStruct(compact);
        }
        bool autoCreate = v < 4 || r.Bool();
        bool clusterOps = v is >= 8 and <= 10 && r.Bool();
        bool topicOps = v >= 8 && r.Bool();
        r.EndStruct(compact);
        r.End();

        var image = cluster.Metadata.Image;
        var topics = image.Topics;
        if (requested.Any(item =>
        {
            ReplicatedTopic? known;
            if (v >= 12 && item.Id != Guid.Empty) known = image.TopicById(item.Id);
            else if (item.Id == Guid.Empty && item.Name is { } name && Catalog.ValidName(name))
                known = topics.GetValueOrDefault(name);
            else return false;
            return known is null || known.Partitions.Any(static partition => partition.LeaderId < 0);
        }))
        {
            using var refresh = Deadline(1200, stop);
            try
            {
                await cluster.Metadata.ReadBarrierAsync(refresh.Token);
                image = cluster.Metadata.Image;
                topics = image.Topics;
            }
            catch (OperationCanceledException) when (!stop.IsCancellationRequested) { }
            catch (QuorumUnreachableException) { }
            catch (NotLeaderException) { }
        }
        if (autoCreate && options.AutoCreate && requested.Length != 0)
        {
            using var timeout = Deadline(10_000, stop);
            try
            {
                foreach (var (name, id) in requested)
                {
                    if (name is null || id != Guid.Empty || !Catalog.ValidName(name) || topics.ContainsKey(name)) continue;
                    if (!request.AllowsTopic(name, AclOperation.Describe) ||
                        !request.AllowsCluster(AclOperation.Create) && !request.AllowsTopic(name, AclOperation.Create)) continue;
                    var partitionIds = Enumerable.Range(0, options.DefaultPartitions).Select(static _ => Guid.NewGuid()).ToImmutableArray();
                    var created = await cluster.ExecuteMetadataAsync(new(MetadataOperation.CreateTopic, name, Guid.NewGuid(),
                        options.DefaultPartitions, cluster.Members.Length, partitionIds), timeout.Token);
                    if (created.Error is Error.None or Error.TopicAlreadyExists)
                    {
                        await cluster.Metadata.ReadBarrierAsync(timeout.Token);
                        image = cluster.Metadata.Image;
                        topics = image.Topics;
                    }
                }
            }
            catch (OperationCanceledException) when (!stop.IsCancellationRequested) { }
            catch (QuorumUnreachableException) { }
        }
        if (v >= 3) w.I32(0);
        WriteMembers(w, compact, rack: v >= 1);
        if (v >= 2) w.Text(cluster.ClusterId, compact);
        if (v >= 1) w.I32(cluster.Metadata.LeaderId);
        if (count < 0 || v == 0 && count == 0)
        {
            var visible = request.Access is null or { SuperUser: true } ? null :
                topics.Values.Where(topic => request.AllowsTopic(topic.Name, AclOperation.Describe)).ToArray();
            w.ArrayLength(visible?.Length ?? topics.Count, compact);
            foreach (var topic in visible ?? topics.Values) WriteTopic(w, v, compact, topic.Name, topic.Id, topic, Error.None, topicOps, request.Access);
        }
        else
        {
            w.ArrayLength(count, compact);
            foreach (var (name, id) in requested)
            {
                bool byId = v >= 12 && id != Guid.Empty;
                bool byName = id == Guid.Empty && name is not null;
                var topic = byId ? image.TopicById(id) : byName ? topics.GetValueOrDefault(name!) : null;
                Error error = topic is not null ? Error.None : byId ? Error.UnknownTopicId :
                    byName ? Catalog.ValidName(name!) ? Error.UnknownTopicOrPartition : Error.InvalidTopic : Error.InvalidRequest;
                string? known = topic?.Name ?? (byName ? name : null);
                if (known is not null && (!request.AllowsTopic(known, AclOperation.Describe) ||
                    topic is null && autoCreate && options.AutoCreate && Catalog.ValidName(known) &&
                    !request.AllowsCluster(AclOperation.Create) && !request.AllowsTopic(known, AclOperation.Create)))
                    (error, topic) = (Error.TopicAuthorizationFailed, null);
                WriteTopic(w, v, compact, topic?.Name ?? (byId ? null : name), topic?.Id ?? id, topic, error, topicOps, request.Access);
            }
        }
        if (v is >= 8 and <= 10) w.I32(clusterOps && request.Access is { } access ?
            access.Operations(AclResource.Cluster, KafkaAcl.ClusterName) : int.MinValue);
        if (v >= 13) w.Write(Error.None);
        w.EndStruct(compact);
        return true;
    }

    private void WriteTopic(WireWriter w, short version, bool compact, string? name, Guid id, ReplicatedTopic? topic,
        Error error, bool operations, KafkaAuthorization? access)
    {
        w.Write(error);
        w.Text(version >= 12 ? name : name ?? "", compact);
        if (version >= 10) w.Uuid(id);
        if (version >= 1) w.Bool(false);
        var partitions = topic?.Partitions ?? [];
        w.ArrayLength(partitions.Length, compact);
        for (int p = 0; p < partitions.Length; p++)
        {
            var (leader, epoch) = cluster.FindLeader(partitions[p]);
            w.Write(leader >= 0 ? Error.None : Error.LeaderNotAvailable);
            w.I32(p);
            w.I32(leader);
            if (version >= 7) w.I32(epoch);
            w.ArrayLength(partitions[p].Replicas.Length, compact);
            foreach (var replica in partitions[p].Replicas) w.I32(replica);
            // Until a leader's replication progress can be observed remotely, advertise only the proven in-sync node.
            w.ArrayLength(leader >= 0 ? 1 : 0, compact);
            if (leader >= 0) w.I32(leader);
            if (version >= 5) w.ArrayLength(0, compact);
            w.EndStruct(compact);
        }
        if (version >= 8) w.I32(operations && error == Error.None && access is not null ?
            access.Operations(AclResource.Topic, name!) : int.MinValue);
        w.EndStruct(compact);
    }

    private void WriteMembers(WireWriter w, bool compact, bool rack, bool fenced = false)
    {
        w.ArrayLength(cluster.Members.Length, compact);
        foreach (var broker in cluster.Members)
        {
            w.I32(broker.Id);
            w.Text(broker.Host, compact);
            w.I32(broker.Port);
            if (rack) w.Text(broker.Rack, compact);
            if (fenced) w.Bool(false);
            w.EndStruct(compact);
        }
    }

    private void DescribeCluster(ref WireReader r, WireWriter w, Request request)
    {
        short version = request.Version;
        bool authorized = r.Bool();
        sbyte endpoint = version >= 1 ? r.I8() : (sbyte)1;
        if (version >= 2) r.Bool();
        r.Tags(); r.End();
        w.I32(0);
        var error = !request.AllowsCluster(AclOperation.Describe) ? Error.ClusterAuthorizationFailed :
            endpoint == 1 ? Error.None : Error.UnsupportedEndpointType;
        w.Write(error);
        w.Text(error == Error.None ? null : error.ToString(), true);
        if (version >= 1) w.I8(1);
        w.Text(error == Error.ClusterAuthorizationFailed ? "" : cluster.ClusterId, true);
        w.I32(error == Error.None ? cluster.Metadata.LeaderId : -1);
        if (error == Error.None) WriteMembers(w, compact: true, rack: true, fenced: version >= 2);
        else w.ArrayLength(0, true);
        w.I32(authorized && error == Error.None && request.Access is { } access ?
            access.Operations(AclResource.Cluster, KafkaAcl.ClusterName) : int.MinValue);
        w.UVar(0);
    }

    private async ValueTask<bool> CreateTopicsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request, CancellationToken stop)
    {
        short version = request.Version;
        bool compact = request.Flexible;
        var r = new WireReader(body.Span);
        var topics = new (string Name, int Count, int Replicas, Error Error,
            ImmutableArray<ImmutableArray<int>> Assignments, int? MessageLimit, bool? LogAppendTime, TimestampLimits TimestampLimits)
            [r.ArrayLength(compact)];
        var names = new HashSet<string>(StringComparer.Ordinal);
        for (int i = 0; i < topics.Length; i++)
        {
            string name = r.Text(compact);
            int count = r.I32();
            int replicas = r.I16();
            int assignments = r.ArrayLength(compact);
            var specified = new ImmutableArray<int>[assignments];
            bool validAssignment = true;
            for (int a = 0; a < assignments; a++)
            {
                int index = r.I32();
                int brokerCount = r.ArrayLength(compact);
                var brokers = ImmutableArray.CreateBuilder<int>(brokerCount);
                for (int b = 0; b < brokerCount; b++) brokers.Add(r.I32());
                if ((uint)index >= specified.Length || !specified[index].IsDefault) validAssignment = false;
                else specified[index] = brokers.MoveToImmutable();
                r.EndStruct(compact);
            }
            var config = KafkaConfigurations.ParseTopic(Router.ReadConfigChanges(ref r, compact, incremental: false),
                tierSupported: false);
            r.EndStruct(compact);
            var placements = specified.ToImmutableArray();
            Error error = !names.Add(name) ? Error.InvalidRequest : config.Error;
            if (assignments != 0 && error == Error.None)
            {
                int assignedReplicas = specified[0].IsDefault ? 0 : specified[0].Length;
                if (count != -1 || replicas != -1 || !validAssignment ||
                    !ReplicatedMetadata.ValidReplicaAssignments(placements, assignedReplicas, cluster.Members))
                    error = Error.InvalidReplicaAssignment;
                else { count = assignments; replicas = assignedReplicas; }
            }
            topics[i] = (name, version >= 4 && count == -1 ? options.DefaultPartitions : count,
                version >= 4 && replicas == -1 ? cluster.Members.Length : replicas, error, placements,
                config.Limit, config.AppendTime, config.TimestampLimits);
        }
        int timeout = r.I32();
        bool dryRun = version >= 1 && r.Bool();
        r.EndStruct(compact);
        r.End();

        if (version >= 2) w.I32(0);
        w.ArrayLength(topics.Length, compact);
        using var requestStop = Deadline(timeout, stop);
        foreach (var (name, count, replicas, invalid, placements, limit, logAppendTime, timestampLimits) in topics)
        {
            Error error = invalid;
            Guid id = Guid.Empty;
            if (!request.AllowsCluster(AclOperation.Create) && !request.AllowsTopic(name, AclOperation.Create))
                error = Error.TopicAuthorizationFailed;
            else if (!cluster.Metadata.IsLeader) error = Error.NotController;
            else if (error == Error.None)
            {
                if (timeout < 0) error = Error.InvalidRequest;
                else if (!Catalog.ValidName(name)) error = Error.InvalidTopic;
                else if (replicas <= 0 || replicas > cluster.Members.Length) error = Error.InvalidReplicationFactor;
                else if (count is < 1 or > BrokerOptions.MaxPartitionsPerTopic) error = Error.InvalidPartitions;
                else if (cluster.Metadata.Image.Topics.ContainsKey(name)) error = Error.TopicAlreadyExists;
                else if (!cluster.Metadata.HasReplicaSlots(count)) error = Error.InvalidPartitions;
                else if (!dryRun)
                {
                    var ids = Enumerable.Range(0, count).Select(static _ => Guid.NewGuid()).ToImmutableArray();
                    try
                    {
                        var result = await cluster.Metadata.ExecuteAsync(new MetadataCommand(MetadataOperation.CreateTopic,
                            name, Guid.NewGuid(), count, replicas, ids, ReplicaAssignments: placements,
                            TopicMessageLimit: limit, TopicLogAppendTime: logAppendTime, TopicTimestampLimits: timestampLimits),
                            requestStop.Token);
                        error = result.Error;
                        if (error == Error.None)
                        {
                            error = await WaitForTopicLeadersAsync(name, result.TopicId, requestStop.Token);
                            if (error == Error.None) id = result.TopicId;
                        }
                    }
                    catch (OperationCanceledException) when (!stop.IsCancellationRequested) { error = Error.RequestTimedOut; }
                    catch (QuorumUnreachableException) { error = Error.NotController; }
                }
            }
            w.Text(name, compact);
            if (version >= 7) w.Uuid(id);
            w.Write(error);
            if (version >= 1) w.Text(error == Error.None ? null : error.ToString(), compact);
            if (version >= 5)
            {
                w.I32(error == Error.None ? count : -1);
                w.I16(error == Error.None ? (short)replicas : (short)-1);
                if (error == Error.None)
                    Router.WriteCreatedTopicConfigs(w, compact, Router.TopicConfiguration(cluster.Metadata.Image.Configurations,
                        id, cluster.Local.Id, standalone: false, creatingLimit: limit, creatingLogAppendTime: logAppendTime,
                        creatingTimestampLimits: timestampLimits));
                else w.ArrayLength(-1, compact);
            }
            w.EndStruct(compact);
        }
        w.EndStruct(compact);
        return true;
    }

    private async ValueTask<bool> InitProducerIdAsync(Router.ProducerInit request, WireWriter w, Request header, CancellationToken stop)
    {
        short version = header.Version;
        MetadataResult result;
        if (request.Error != Error.None) result = new(request.Error);
        else if (request.TransactionalId is { } tx && (!header.AllowsTransaction(tx, AclOperation.Write) ||
            request.EnableTwoPhase && !header.AllowsTransaction(tx, AclOperation.TwoPhaseCommit)))
            result = new(Error.TransactionalIdAuthorizationFailed);
        else if (request.TransactionalId is null && header.Access is { } access &&
            !access.AllowsIdempotence(cluster.Metadata.Image.Topics.Keys))
            result = new(Error.ClusterAuthorizationFailed);
        else if (request.TransactionalId is { } name)
        {
            result = await ExecuteTransactionAsync(name, new(TransactionOperation.Init, request.ProducerId, request.Epoch,
                BrokerCluster.TransactionTime(), TimeoutMs: request.Timeout, EnableTwoPhase: request.EnableTwoPhase,
                KeepPrepared: request.KeepPrepared), stop);
            if (result.Error == Error.None && result.OngoingProducerId >= 0)
            {
                Error fenced = await cluster.SealRecoveredTransactionAsync(name, result, stop);
                if (fenced != Error.None) result = new(fenced);
            }
        }
        else
        {
            using var requestStop = Deadline(10_000, stop);
            try { result = await cluster.ExecuteMetadataAsync(new(MetadataOperation.AllocateProducer), requestStop.Token); }
            catch (OperationCanceledException) when (!stop.IsCancellationRequested) { result = new(Error.RequestTimedOut); }
            catch (QuorumUnreachableException) { result = new(Error.CoordinatorNotAvailable); }
        }
        Error error = result.Error == Error.ProducerFenced && version < 4 ? Error.InvalidProducerEpoch : result.Error;
        Router.WriteInitProducerId(w, version, error,
            result.Error == Error.None ? result.ProducerId : -1, Math.Max(result.ProducerEpoch, (short)0),
            result.OngoingProducerId, result.OngoingProducerEpoch);
        return true;
    }

    private async ValueTask<bool> ProduceAsync(Memory<byte> body, WireWriter w, Request request, CancellationToken stop)
    {
        short v = request.Version;
        bool compact = v >= 9;
        var (acks, transactional, timeout, topics, cursor) = Router.ValidateProduce(body.Span, v);
        using var requestStop = Deadline(timeout, stop);
        w.ArrayLength(topics, compact);
        for (int t = 0; t < topics; t++)
        {
            var (topic, partitions, consumed, allowed) = ReadProduceTopic(body.Span[cursor..], w, request);
            var settings = cluster.Metadata.Image.Configurations;
            bool logAppendTime = topic is not null && settings.UsesLogAppendTime(topic.Id);
            var timestampBounds = topic is null ? null : settings.ProduceTimestampBounds(topic.Id, logAppendTime);
            cursor += consumed;
            for (int p = 0; p < partitions; p++)
            {
                var (index, start, length, total) = Router.ReadProducePartition(body.Span[cursor..], compact);
                start += cursor;
                cursor += total;
                var replica = GetReplica(topic, index);
                AppendResult result;
                if (acks is not (-1 or 0 or 1) || timeout < 0) result = new(Error.InvalidRequest, -1);
                else if (transactional is not null && !request.AllowsTransaction(transactional, AclOperation.Write))
                    result = new(Error.TransactionalIdAuthorizationFailed, -1);
                else if (!allowed) result = new(Error.TopicAuthorizationFailed, -1);
                else if (transactional is not null && acks != -1) result = new(Error.InvalidRequest, -1);
                else if (topic is null) result = new(v >= 13 ? Error.UnknownTopicId : Error.UnknownTopicOrPartition, -1);
                else if ((uint)index >= topic.Partitions.Length) result = new(Error.UnknownTopicOrPartition, -1);
                else if (replica is null) result = new(MissingLocalReplica(topic, index), -1);
                else if (length <= 0) result = new(Error.CorruptMessage, -1);
                else if (length > settings.TopicLimit(topic.Id, cluster.Local.Id))
                    result = new(Error.MessageTooLarge, -1);
                else if (v < 7 && length >= RecordBatch.HeaderSize && (body.Span[start + 22] & 7) == 4)
                    result = new(Error.UnsupportedCompressionType, -1);
                else
                {
                    try
                    {
                        if (transactional is not null)
                        {
                            result = await ProduceTransactionAsync(transactional, topic, index, replica,
                                body.Slice(start, length), modern: v >= 12, logAppendTime, timestampBounds, requestStop.Token);
                        }
                        else
                        {
                            long allocated = cluster.Metadata.Image.LastProducerId;
                            if (length >= RecordBatch.HeaderSize && BinaryPrimitives.ReadInt64BigEndian(body.Span[(start + 43)..]) > allocated)
                            {
                                await cluster.Metadata.ReadBarrierAsync(requestStop.Token);
                                allocated = cluster.Metadata.Image.LastProducerId;
                            }
                            result = await replica.AppendAsync(body.Slice(start, length), allocated, requestStop.Token,
                                logAppendTime: logAppendTime, timestampBounds: timestampBounds);
                        }
                    }
                    catch (OperationCanceledException) when (!stop.IsCancellationRequested)
                    { result = new(Error.RequestTimedOut, -1); }
                    catch (QuorumUnreachableException) { result = new(Error.LeaderNotAvailable, -1); }
                    catch (NotLeaderException) { result = new(Error.LeaderNotAvailable, -1); }
                    catch (ObjectDisposedException) { result = new(Error.UnknownTopicOrPartition, -1); }
                }
                Router.WriteProducePartition(w, v, index, acks, result, replica?.LogStartOffset ?? 0);
            }
            if (compact) cursor += Router.ReadTags(body.Span[cursor..]);
            w.EndStruct(compact);
        }
        w.I32(request.Quota?.Charge(request.BodyOffset + body.Length) ?? 0);
        w.EndStruct(compact);
        return acks != 0;
    }

    private (ReplicatedTopic? Topic, int Partitions, int Size, bool Allowed) ReadProduceTopic(ReadOnlySpan<byte> body, WireWriter w, Request request)
    {
        short version = request.Version;
        var r = new WireReader(body);
        bool compact = version >= 9;
        var image = cluster.Metadata.Image;
        ReplicatedTopic? topic;
        string? name;
        if (version >= 13)
        {
            Guid id = r.Uuid();
            w.Uuid(id);
            topic = image.TopicById(id);
            name = topic?.Name;
        }
        else
        {
            name = r.Text(compact);
            w.Text(name, compact);
            topic = image.Topics.GetValueOrDefault(name);
        }
        int count = r.ArrayLength(compact);
        w.ArrayLength(count, compact);
        return (topic, count, body.Length - r.Remaining, name is null || request.AllowsTopic(name, AclOperation.Write));
    }

    private ReplicatedPartition? GetReplica(ReplicatedTopic? topic, int partition) =>
        topic is not null && (uint)partition < topic.Partitions.Length ? cluster.FindReplica(topic.Partitions[partition].Id) : null;

    private Error MissingLocalReplica(ReplicatedTopic topic, int partition) =>
        cluster.ReplicaStorageFailed(topic.Partitions[partition].Id) ? Error.KafkaStorageError : Error.LeaderNotAvailable;

    private void DescribeProducers(ref WireReader r, WireWriter w, Request request)
    {
        var image = cluster.Metadata.Image;
        int count = r.ArrayLength(true);
        w.I32(0);
        w.ArrayLength(count, true);
        for (int i = 0; i < count; i++)
        {
            string topicName = r.Text(true);
            var topic = image.Topics.GetValueOrDefault(topicName);
            bool allowed = request.AllowsTopic(topicName, AclOperation.Read);
            int partitions = r.ArrayLength(true);
            w.Text(topicName, true);
            w.ArrayLength(partitions, true);
            for (int p = 0; p < partitions; p++)
            {
                int partition = r.I32();
                var replica = GetReplica(topic, partition);
                Error error = !allowed ? Error.TopicAuthorizationFailed :
                    topic is null || (uint)partition >= topic.Partitions.Length ? Error.UnknownTopicOrPartition :
                    replica is null ? MissingLocalReplica(topic, partition) : Error.None;
                var producers = error == Error.None ? replica!.DescribeProducers() : [];
                w.I32(partition);
                w.Write(error);
                w.Text(null, true);
                w.ArrayLength(producers.Length, true);
                foreach (var (id, epoch, sequence, stamp, coordinatorEpoch, first) in producers)
                {
                    w.I64(id);
                    w.I32(epoch);
                    w.I32(sequence);
                    w.I64(stamp);
                    w.I32(coordinatorEpoch);
                    w.I64(first);
                    w.UVar(0);
                }
                w.UVar(0);
            }
            r.Tags();
            w.UVar(0);
        }
        r.Tags();
        r.End();
        w.UVar(0);
    }

    private static CancellationTokenSource Deadline(int timeout, CancellationToken stop)
    {
        var deadline = CancellationTokenSource.CreateLinkedTokenSource(stop);
        deadline.CancelAfter(Math.Clamp(timeout, 1, 120_000));
        return deadline;
    }
}
