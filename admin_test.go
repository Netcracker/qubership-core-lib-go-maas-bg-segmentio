package blue_green_segmentio

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	bgKafka "github.com/netcracker/qubership-core-lib-go-bg-kafka/v3"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func TestAdminManyBrokersLastBrokerAvailable(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	testConn := &kafka.Conn{}
	testUnavailableErr := fmt.Errorf("unavailable")
	adapter := adminAdapter{addr: &testAddr{
		network: "tcp,tcp,tcp",
		address: "test.kafka-1:9092,test.kafka-2:9092,test.kafka-3:9092",
	}, dialer: &testDialer{results: map[string]*connAndErr{
		"tcp@test.kafka-1:9092": {conn: nil, err: testUnavailableErr},
		"tcp@test.kafka-2:9092": {conn: nil, err: testUnavailableErr},
		"tcp@test.kafka-3:9092": {conn: testConn, err: nil},
	}}}
	conn, err := adapter.connectToAnyBroker(ctx)
	assertions.NoError(err)
	assertions.True(reflect.DeepEqual(testConn, conn))
}

func TestAdminManyBrokersLastNoBrokerAvailable(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	testUnavailableErr := fmt.Errorf("unavailable")
	adapter := adminAdapter{addr: &testAddr{
		network: "tcp,tcp,tcp",
		address: "test.kafka-1:9092,test.kafka-2:9092,test.kafka-3:9092",
	}, dialer: &testDialer{results: map[string]*connAndErr{
		"tcp@test.kafka-1:9092": {conn: nil, err: testUnavailableErr},
		"tcp@test.kafka-2:9092": {conn: nil, err: testUnavailableErr},
		"tcp@test.kafka-3:9092": {conn: nil, err: testUnavailableErr},
	}}}
	conn, err := adapter.connectToAnyBroker(ctx)
	assertions.Equal(testUnavailableErr, err)
	assertions.Nil(conn)
}

type testAddr struct {
	network string
	address string
}

func (add *testAddr) Network() string {
	return add.network
}
func (add *testAddr) String() string {
	return add.address
}

type testDialer struct {
	results map[string]*connAndErr
}

type connAndErr struct {
	conn *kafka.Conn
	err  error
}

func (t *testDialer) Dial(network string, address string) (*kafka.Conn, error) {
	cAndE := t.results[fmt.Sprintf("%s@%s", network, address)]
	if cAndE == nil {
		return nil, fmt.Errorf("unknown network/address")
	}
	return cAndE.conn, cAndE.err
}

// fakeClient implements the client interface, capturing the last ListOffsetsRequest and
// returning a canned response, keyed by topic then partition.
type fakeClient struct {
	lastListOffsetsRequest *kafka.ListOffsetsRequest
	listOffsetsResponse    map[string][]kafka.PartitionOffsets
}

func (f *fakeClient) ListGroups(ctx context.Context, req *kafka.ListGroupsRequest) (*kafka.ListGroupsResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeClient) CreateTopics(ctx context.Context, req *kafka.CreateTopicsRequest) (*kafka.CreateTopicsResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeClient) OffsetFetch(ctx context.Context, req *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeClient) OffsetCommit(ctx context.Context, req *kafka.OffsetCommitRequest) (*kafka.OffsetCommitResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeClient) ListOffsets(ctx context.Context, req *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error) {
	f.lastListOffsetsRequest = req
	return &kafka.ListOffsetsResponse{Topics: f.listOffsetsResponse}, nil
}

func TestEndOffsets_QueriesAllPartitions(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	topic := "test-topic"
	fc := &fakeClient{
		listOffsetsResponse: map[string][]kafka.PartitionOffsets{
			topic: {
				{Partition: 0, LastOffset: 100},
				{Partition: 1, LastOffset: 200},
				{Partition: 2, LastOffset: 300},
			},
		},
	}
	adapter := &adminAdapter{client: fc}

	topicPartitions := []bgKafka.TopicPartition{
		{Topic: topic, Partition: 0},
		{Topic: topic, Partition: 1},
		{Topic: topic, Partition: 2},
	}
	result, err := adapter.EndOffsets(ctx, topicPartitions)
	assertions.NoError(err)

	// The request sent to the broker must contain an entry for every partition, not just
	// the last one processed (regression test for the map-overwrite bug in offsetsForTimes).
	assertions.Len(fc.lastListOffsetsRequest.Topics[topic], 3)

	assertions.Equal(map[bgKafka.TopicPartition]int64{
		{Topic: topic, Partition: 0}: 100,
		{Topic: topic, Partition: 1}: 200,
		{Topic: topic, Partition: 2}: 300,
	}, result)
}

func TestBeginningOffsets_QueriesAllPartitions(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	topic := "test-topic"
	fc := &fakeClient{
		listOffsetsResponse: map[string][]kafka.PartitionOffsets{
			topic: {
				{Partition: 0, FirstOffset: 10},
				{Partition: 1, FirstOffset: 20},
			},
		},
	}
	adapter := &adminAdapter{client: fc}

	topicPartitions := []bgKafka.TopicPartition{
		{Topic: topic, Partition: 0},
		{Topic: topic, Partition: 1},
	}
	result, err := adapter.BeginningOffsets(ctx, topicPartitions)
	assertions.NoError(err)

	assertions.Len(fc.lastListOffsetsRequest.Topics[topic], 2)
	assertions.Equal(map[bgKafka.TopicPartition]int64{
		{Topic: topic, Partition: 0}: 10,
		{Topic: topic, Partition: 1}: 20,
	}, result)
}

func TestOffsetsForTimes_QueriesAllPartitionsAndReturnsResultsForAll(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	topic := "test-topic"
	now := time.Now()
	fc := &fakeClient{
		listOffsetsResponse: map[string][]kafka.PartitionOffsets{
			topic: {
				{Partition: 0, Offsets: map[int64]time.Time{150: now}},
				{Partition: 1, Offsets: map[int64]time.Time{250: now}},
			},
		},
	}
	adapter := &adminAdapter{client: fc}

	query := map[bgKafka.TopicPartition]time.Time{
		{Topic: topic, Partition: 0}: now,
		{Topic: topic, Partition: 1}: now,
	}
	result, err := adapter.OffsetsForTimes(ctx, query)
	assertions.NoError(err)

	assertions.Len(fc.lastListOffsetsRequest.Topics[topic], 2)
	assertions.Len(result, 2)
	assertions.Equal(int64(150), result[bgKafka.TopicPartition{Topic: topic, Partition: 0}].Offset)
	assertions.Equal(int64(250), result[bgKafka.TopicPartition{Topic: topic, Partition: 1}].Offset)
}

func TestOffsetsForTimes_MapsPartitionWithNoRecordAtOrAfterTimestampToNil(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	topic := "test-topic"
	now := time.Now()
	fc := &fakeClient{
		listOffsetsResponse: map[string][]kafka.PartitionOffsets{
			topic: {
				// partition 0 has a record at/after the requested timestamp
				{Partition: 0, Offsets: map[int64]time.Time{150: now}},
				// partition 1's entire backlog is older than the requested timestamp:
				// the broker reports no matching record, so Offsets is empty.
				{Partition: 1, Offsets: map[int64]time.Time{}},
			},
		},
	}
	adapter := &adminAdapter{client: fc}

	query := map[bgKafka.TopicPartition]time.Time{
		{Topic: topic, Partition: 0}: now,
		{Topic: topic, Partition: 1}: now,
	}
	result, err := adapter.OffsetsForTimes(ctx, query)
	assertions.NoError(err)

	assertions.Equal(int64(150), result[bgKafka.TopicPartition{Topic: topic, Partition: 0}].Offset)
	// Per NativeAdminAdapter's contract: an entry must be present, mapped to nil rather than
	// omitted or a fabricated Offset: 0.
	assertions.Contains(result, bgKafka.TopicPartition{Topic: topic, Partition: 1})
	assertions.Nil(result[bgKafka.TopicPartition{Topic: topic, Partition: 1}])
}

func TestOffsetsForTimes_ReturnsErrorOnPartitionBrokerError(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	topic := "test-topic"
	now := time.Now()
	fc := &fakeClient{
		listOffsetsResponse: map[string][]kafka.PartitionOffsets{
			topic: {
				{Partition: 0, Offsets: map[int64]time.Time{150: now}},
				// kafka-go reports a per-partition broker error (e.g. leader not available
				// during a rebalance) alongside a sentinel Offset: -1.
				{Partition: 1, LastOffset: -1, FirstOffset: -1, Error: kafka.LeaderNotAvailable},
			},
		},
	}
	adapter := &adminAdapter{client: fc}

	query := map[bgKafka.TopicPartition]time.Time{
		{Topic: topic, Partition: 0}: now,
		{Topic: topic, Partition: 1}: now,
	}
	_, err := adapter.OffsetsForTimes(ctx, query)

	// Must surface the real broker error rather than silently dropping the partition or
	// trusting the -1 sentinel as a resolved offset.
	assertions.ErrorIs(err, kafka.LeaderNotAvailable)
}

func TestEndOffsets_ReturnsErrorOnPartitionBrokerError(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	topic := "test-topic"
	fc := &fakeClient{
		listOffsetsResponse: map[string][]kafka.PartitionOffsets{
			topic: {
				{Partition: 0, LastOffset: 100},
				// broker error: LastOffset comes back as -1, must not be trusted as real.
				{Partition: 1, LastOffset: -1, Error: kafka.LeaderNotAvailable},
			},
		},
	}
	adapter := &adminAdapter{client: fc}

	topicPartitions := []bgKafka.TopicPartition{
		{Topic: topic, Partition: 0},
		{Topic: topic, Partition: 1},
	}
	_, err := adapter.EndOffsets(ctx, topicPartitions)

	assertions.ErrorIs(err, kafka.LeaderNotAvailable)
}
