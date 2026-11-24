using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace SimdKafka;

internal sealed class WireException(string message) : Exception(message);

internal ref struct WireReader(ReadOnlySpan<byte> data)
{
    private ReadOnlySpan<byte> _data = data;
    internal int Remaining => _data.Length;
    internal ReadOnlySpan<byte> Take(int size)
    {
        if ((uint)size > (uint)_data.Length)
            throw new WireException("Truncated field or invalid length");
        var result = _data[..size];
        _data = _data[size..];
        return result;
    }

    internal sbyte I8() => (sbyte)Take(1)[0];
    internal short I16() => BinaryPrimitives.ReadInt16BigEndian(Take(2));
    internal int I32() => BinaryPrimitives.ReadInt32BigEndian(Take(4));
    internal long I64() => BinaryPrimitives.ReadInt64BigEndian(Take(8));
    internal bool Bool() => I8() != 0;
    internal uint UVar()
    {
        uint value = 0;
        for (int shift = 0; shift <= 28; shift += 7)
        {
            uint octet = Take(1)[0];
            if (shift == 28 && octet > 15)
                throw new WireException("Unsigned varint overflow");
            value |= (octet & 127) << shift;
            if (octet < 128) return value;
        }
        throw new WireException("Invalid unsigned varint");
    }

    internal int Count(bool nullable = false)
    {
        int count = I32();
        if (count == -1 && nullable) return -1;
        if ((uint)count > 100_000 || count > Remaining)
            throw new WireException("Invalid array count");
        return count;
    }

    internal int ArrayLength(bool compact, bool nullable = false)
    {
        if (!compact) return Count(nullable);
        uint encoded = UVar();
        if (encoded == 0 && nullable) return -1;
        if (encoded is 0 or > 100_001 || encoded - 1 > Remaining)
            throw new WireException("Invalid compact array count");
        return (int)encoded - 1;
    }

    internal string? NullableString()
    {
        int size = I16();
        return size == -1 ? null : Encoding.UTF8.GetString(Take(size));
    }

    internal string String() => NullableString() ?? throw new WireException("Null string");
    internal string Text(bool compact) =>
        NullableText(compact) ?? throw new WireException("Null string");

    internal string? NullableText(bool compact)
    {
        if (!compact) return NullableString();
        uint size = UVar();
        return size == 0 ? null : Encoding.UTF8.GetString(Take(checked((int)(size - 1))));
    }

    internal Guid Uuid() => new(Take(16), bigEndian: true);
    internal ReadOnlySpan<byte> StringBytes(bool compact = false)
    {
        if (!compact) return Take(I16());
        uint size = UVar();
        if (size == 0) throw new WireException("Null compact string");
        return Take(checked((int)(size - 1)));
    }
    internal void SkipString(bool nullable = true)
    {
        int size = I16();
        if (size != -1 || !nullable) Take(size);
    }
    internal ReadOnlySpan<byte> Bytes()
    {
        int size = I32();
        return size == -1 ? default : Take(size);
    }

    internal void CompactString() => StringBytes(compact: true);

    internal void Tags()
    {
        uint count = UVar();
        if (count > Remaining / 2) throw new WireException("Invalid tagged fields");
        uint previous = 0;
        for (uint i = 0; i < count; i++)
        {
            uint tag = UVar();
            if (i > 0 && tag <= previous) throw new WireException("Unordered tagged fields");
            previous = tag;
            Take(checked((int)UVar()));
        }
    }

    internal void EndStruct(bool compact)
    {
        if (compact) Tags();
    }

    internal void End()
    {
        if (Remaining != 0) throw new WireException("Unexpected request bytes");
    }
}

internal sealed class WireWriter(int limit) : IDisposable
{
    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(4096);
    internal int Length { get; private set; }
    internal bool Sensitive { get; set; }
    internal ReadOnlyMemory<byte> Memory => _buffer.AsMemory(0, Length);
    internal Span<byte> Reserve(int size)
    {
        if ((uint)size > (uint)(limit - Length))
            throw new WireException("Response exceeds frame limit");
        int end = Length + size;
        if (end > _buffer.Length)
        {
            var next = ArrayPool<byte>.Shared.Rent(end);
            _buffer.AsSpan(0, Length).CopyTo(next);
            if (Sensitive) System.Security.Cryptography.CryptographicOperations.ZeroMemory(_buffer.AsSpan(0, Length));
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = next;
        }
        var result = _buffer.AsSpan(Length, size);
        Length = end;
        return result;
    }

    internal void Reset()
    {
        if (Sensitive) System.Security.Cryptography.CryptographicOperations.ZeroMemory(_buffer.AsSpan(0, Length));
        Sensitive = false;
        Length = 0;
        if (_buffer.Length <= 256 * 1024) return;
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = ArrayPool<byte>.Shared.Rent(4096);
    }

    internal void Truncate(int length) => Length = length;

    internal Memory<byte> ReserveMemory(int size)
    {
        int start = Length;
        Reserve(size);
        return _buffer.AsMemory(start, size);
    }

    internal void PatchI32(int position, int value) =>
        BinaryPrimitives.WriteInt32BigEndian(_buffer.AsSpan(position, 4), value);
    internal void I8(sbyte n) => Reserve(1)[0] = (byte)n;
    internal void I16(short n) => BinaryPrimitives.WriteInt16BigEndian(Reserve(2), n);
    internal void I32(int n) => BinaryPrimitives.WriteInt32BigEndian(Reserve(4), n);
    internal void I64(long n) => BinaryPrimitives.WriteInt64BigEndian(Reserve(8), n);
    internal void Bool(bool value) => I8(value ? (sbyte)1 : (sbyte)0);

    internal void UVar(uint value)
    {
        while (value > 127)
        {
            I8((sbyte)(value | 128));
            value >>= 7;
        }
        I8((sbyte)value);
    }

    internal void String(string? value)
    {
        if (value is null)
        {
            I16(-1);
            return;
        }
        int size = Encoding.UTF8.GetByteCount(value);
        if (size > short.MaxValue) throw new WireException("String exceeds INT16 length");
        I16((short)size);
        Encoding.UTF8.GetBytes(value, Reserve(size));
    }

    internal void Text(string? value, bool compact)
    {
        if (!compact)
        {
            String(value);
            return;
        }
        if (value is null)
        {
            UVar(0);
            return;
        }
        int size = Encoding.UTF8.GetByteCount(value);
        UVar((uint)size + 1);
        Encoding.UTF8.GetBytes(value, Reserve(size));
    }

    internal void ArrayLength(int count, bool compact)
    {
        if (compact) UVar((uint)(count + 1));
        else I32(count);
    }

    internal void EndStruct(bool compact)
    {
        if (compact) UVar(0);
    }

    internal void Uuid(Guid value) => value.TryWriteBytes(Reserve(16), bigEndian: true, out _);

    internal void StringBytes(ReadOnlySpan<byte> value, bool compact = false)
    {
        if (compact) UVar((uint)value.Length + 1);
        else I16(checked((short)value.Length));
        value.CopyTo(Reserve(value.Length));
    }

    internal void Bytes(ReadOnlySpan<byte> value)
    {
        I32(value.Length);
        value.CopyTo(Reserve(value.Length));
    }

    internal void Bytes(ReadOnlySpan<byte> value, bool compact)
    {
        ArrayLength(value.Length, compact);
        value.CopyTo(Reserve(value.Length));
    }

    public void Dispose()
    {
        if (Sensitive) System.Security.Cryptography.CryptographicOperations.ZeroMemory(_buffer.AsSpan(0, Length));
        ArrayPool<byte>.Shared.Return(_buffer);
    }
}

internal enum Api : short
{
    Produce = 0, Fetch = 1, ListOffsets = 2, Metadata = 3,
    OffsetCommit = 8, OffsetFetch = 9, FindCoordinator = 10, JoinGroup = 11,
    Heartbeat = 12, LeaveGroup = 13, SyncGroup = 14, DescribeGroups = 15, ListGroups = 16,
    SaslHandshake = 17, ApiVersions = 18, CreateTopics = 19, DeleteTopics = 20, DeleteRecords = 21,
    InitProducerId = 22, OffsetForLeaderEpoch = 23, AddPartitionsToTxn = 24, AddOffsetsToTxn = 25,
    EndTxn = 26, TxnOffsetCommit = 28, DescribeAcls = 29, CreateAcls = 30, DeleteAcls = 31,
    DescribeConfigs = 32, AlterConfigs = 33, DescribeLogDirs = 35,
    SaslAuthenticate = 36, CreatePartitions = 37,
    CreateDelegationToken = 38, RenewDelegationToken = 39, ExpireDelegationToken = 40, DescribeDelegationToken = 41,
    DeleteGroups = 42, IncrementalAlterConfigs = 44, AlterPartitionReassignments = 45, ListPartitionReassignments = 46,
    OffsetDelete = 47, DescribeClientQuotas = 48, AlterClientQuotas = 49, DescribeUserScramCredentials = 50, AlterUserScramCredentials = 51,
    DescribeQuorum = 55, DescribeCluster = 60, DescribeProducers = 61, DescribeTransactions = 65,
    ListTransactions = 66, ConsumerGroupHeartbeat = 68, ConsumerGroupDescribe = 69, ListConfigResources = 74,
    DescribeTopicPartitions = 75, ShareGroupHeartbeat = 76, ShareGroupDescribe = 77, ShareFetch = 78, ShareAcknowledge = 79,
    DescribeShareGroupOffsets = 90, AlterShareGroupOffsets = 91, DeleteShareGroupOffsets = 92
}

internal enum Error : short
{
    InvalidRequest = 42, None = 0, OffsetOutOfRange = 1, CorruptMessage = 2,
    UnknownTopicOrPartition = 3, InvalidFetchSize = 4, LeaderNotAvailable = 5, NotLeaderOrFollower = 6,
    RequestTimedOut = 7, MessageTooLarge = 10, OffsetMetadataTooLarge = 12,
    CoordinatorNotAvailable = 15, NotCoordinator = 16,
    InvalidTopic = 17, IllegalGeneration = 22, InconsistentGroupProtocol = 23,
    InvalidGroupId = 24, UnknownMemberId = 25, InvalidSessionTimeout = 26,
    RebalanceInProgress = 27, TopicAuthorizationFailed = 29, GroupAuthorizationFailed = 30, ClusterAuthorizationFailed = 31,
    InvalidTimestamp = 32,
    UnsupportedVersion = 35, TopicAlreadyExists = 36,
    InvalidPartitions = 37, InvalidReplicationFactor = 38, InvalidReplicaAssignment = 39,
    InvalidConfig = 40, NotController = 41, OutOfOrderSequence = 45, InvalidProducerEpoch = 47,
    InvalidTxnState = 48, InvalidProducerIdMapping = 49, InvalidTransactionTimeout = 50,
    ConcurrentTransactions = 51, TransactionalIdAuthorizationFailed = 53, SecurityDisabled = 54,
    OperationNotAttempted = 55, UnsupportedSaslMechanism = 33, KafkaStorageError = 56,
    SaslAuthenticationFailed = 58, UnknownProducerId = 59, FetchSessionIdNotFound = 70, InvalidFetchSessionEpoch = 71,
    ReassignmentInProgress = 60, NonEmptyGroup = 68, GroupIdNotFound = 69, FencedLeaderEpoch = 74, UnknownLeaderEpoch = 75,
    DelegationTokenAuthDisabled = 61, DelegationTokenNotFound = 62, DelegationTokenOwnerMismatch = 63,
    DelegationTokenRequestNotAllowed = 64, DelegationTokenAuthorizationFailed = 65, DelegationTokenExpired = 66, InvalidPrincipalType = 67,
    UnsupportedCompressionType = 76, OffsetNotAvailable = 78,
    MemberIdRequired = 79, GroupMaxSizeReached = 81, FencedInstanceId = 82, NoReassignmentInProgress = 85, GroupSubscribedToTopic = 86, UnknownTopicId = 100,
    UnstableOffsetCommit = 88, ProducerFenced = 90, ResourceNotFound = 91, DuplicateResource = 92, UnacceptableCredential = 93,
    TransactionalIdNotFound = 105, FetchSessionTopicIdError = 106,
    FencedMemberEpoch = 110, UnreleasedInstanceId = 111, UnsupportedAssignor = 112, StaleMemberEpoch = 113,
    UnsupportedEndpointType = 115, InvalidRecordState = 121, ShareSessionNotFound = 122, InvalidShareSessionEpoch = 123,
    InvalidRegularExpression = 128, RebootstrapRequired = 129, ShareSessionLimitReached = 133
}

internal static class ErrorEncoding
{
    internal static void Write(this WireWriter writer, Error error) => writer.I16((short)error);
}
