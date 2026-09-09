//go:build failover

package blue_green_segmentio

import (
	"context"
	"strconv"
	"testing"
	"time"

	bgKafka "github.com/netcracker/qubership-core-lib-go-bg-kafka/v3"
	bg "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	kafkaModel "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	maasKafkaGo "github.com/netcracker/qubership-core-lib-go-maas-segmentio/v3"
	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders/xversion"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/ctxmanager"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

const (
	failoverTopic   = "failover-coordinator"
	failoverGroup   = "failover-group"
	failoverVersion = "v1"
	// offsetsTopic holds the group offsets. It has one partition, so the broker
	// leading it coordinates every group.
	offsetsTopic = "__consumer_offsets"
	// recordsBeforeLoss and recordsAfterLoss are produced either side of the stop.
	recordsBeforeLoss = 5
	recordsAfterLoss  = 5
	// recoveryAllowance is how long the consumer is given to reach the new
	// coordinator.
	recoveryAllowance = 90 * time.Second
	// pollStep bounds one poll attempt during a recovery.
	pollStep = 5 * time.Second
)

// The group coordinator is lost while a consumer is reading. Everything produced
// has to arrive, and committing has to work again, without recreating the
// consumer.
func TestFailover_BgConsumerSurvivesCoordinatorLoss(t *testing.T) {
	ctx := context.Background()
	assertions := require.New(t)

	configloader.InitWithSourcesArray([]*configloader.PropertySource{configloader.EnvPropertySource()})
	ctxmanager.Register(baseproviders.Get())
	useDockerHostFromEnv()

	cluster, err := newKafkaCluster(ctx, "7.4.0", brokers, replicationFactor)
	assertions.NoError(err)
	t.Cleanup(func() { cluster.stop(context.Background()) })

	address := kafkaModel.TopicAddress{
		TopicName:       failoverTopic,
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"PLAINTEXT": cluster.brokers()},
	}
	createFailoverTopic(t, ctx, cluster, address)

	writer, err := maasKafkaGo.NewWriter(address)
	assertions.NoError(err)
	t.Cleanup(func() { writer.Close() })

	states := newStates("2024-01-01T10:00:00Z",
		bg.NamespaceVersion{State: bg.StateActive, Version: bg.NewVersionMust(failoverVersion)})
	publisher, err := bg.NewInMemoryPublisher(states.Origin)
	assertions.NoError(err)
	consumer, err := NewBgConsumer(ctx, address, failoverGroup,
		bgKafka.WithBlueGreenStatePublisher(publisher),
		bgKafka.WithConsistencyMode(bgKafka.GuaranteeConsumption))
	assertions.NoError(err)

	produceFailoverRecords(t, ctx, writer, 0, recordsBeforeLoss)
	seen := map[string]bool{}
	for i := 0; i < recordsBeforeLoss; i++ {
		record := pollRecord(t, ctx, consumer, recoveryAllowance)
		assertions.NoError(consumer.Commit(ctx, record.Marker))
		seen[string(record.Message.Key())] = true
	}
	assertions.Len(seen, recordsBeforeLoss, "the records produced before the loss must all arrive")

	// the offsets topic is created by the first commit, so the coordinator is
	// only known at this point
	coordinator, err := cluster.partitionLeader(ctx, offsetsTopic, 0)
	assertions.NoError(err)
	t.Logf("stopping broker %d, which coordinates group %s", coordinator, failoverGroup)
	assertions.NoError(cluster.stopBroker(ctx, coordinator))
	assertions.NoError(cluster.awaitLeaders(ctx, failoverTopic, 1))

	produceFailoverRecords(t, ctx, writer, recordsBeforeLoss, recordsBeforeLoss+recordsAfterLoss)

	// delivery is at least once: an uncommitted offset is read again, so the set
	// of keys is the measure here rather than the count of records
	expected := recordsBeforeLoss + recordsAfterLoss
	start := time.Now()
	delivered := 0
	for len(seen) < expected && time.Since(start) < recoveryAllowance {
		record := pollRecord(t, ctx, consumer, recoveryAllowance)
		commitWithRecovery(t, ctx, consumer, record.Marker, recoveryAllowance)
		seen[string(record.Message.Key())] = true
		delivered++
	}
	t.Logf("the consumer delivered %d records, %d of them new, in %s",
		delivered, len(seen)-recordsBeforeLoss, time.Since(start).Round(time.Millisecond))
	assertions.Len(seen, expected, "losing the coordinator must not lose a record")

	assertions.NoError(cluster.startBroker(ctx, coordinator))
	assertions.NoError(cluster.awaitLeaders(ctx, failoverTopic, 1))
}

// createFailoverTopic creates the topic through the library and waits until every
// broker reports a leader for it.
func createFailoverTopic(t *testing.T, ctx context.Context, cluster *kafkaTestCluster, address kafkaModel.TopicAddress) {
	t.Helper()
	assertions := require.New(t)

	client, err := maasKafkaGo.NewClient(address)
	assertions.NoError(err)
	response, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{{
		Topic:             address.TopicName,
		NumPartitions:     address.NumPartitions,
		ReplicationFactor: replicationFactor,
	}}})
	assertions.NoError(err)
	assertions.Nil(response.Errors[address.TopicName])
	assertions.NoError(cluster.awaitLeaders(ctx, address.TopicName, address.NumPartitions))
}

// produceFailoverRecords writes keys from..to with the version header the
// consumer filters on. Without it a record is declined by the filter.
func produceFailoverRecords(t *testing.T, ctx context.Context, writer *kafka.Writer, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		key := strconv.Itoa(i)
		require.NoError(t, writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(key),
			Value: []byte(key),
			Headers: []kafka.Header{
				{Key: xversion.X_VERSION_HEADER_NAME, Value: []byte(failoverVersion)},
			},
		}), "producing key %s", key)
	}
}

// commitWithRecovery commits one marker, retrying while the group has no
// coordinator.
func commitWithRecovery(t *testing.T, ctx context.Context, consumer *bgKafka.BgConsumer,
	marker *bgKafka.CommitMarker, allowance time.Duration) {
	t.Helper()
	start := time.Now()
	var lastErr error
	for time.Since(start) < allowance {
		if err := consumer.Commit(ctx, marker); err == nil {
			return
		} else {
			lastErr = err
		}
	}
	t.Fatalf("the offset was not committed within %s, last error: %v", allowance, lastErr)
}

// pollRecord returns the next record carrying a message, retrying while the
// group has no coordinator. A record with a nil message was declined by the
// version filter.
func pollRecord(t *testing.T, ctx context.Context, consumer *bgKafka.BgConsumer, allowance time.Duration) *bgKafka.Record {
	t.Helper()
	deadline := time.Now().Add(allowance)
	var lastErr error
	for time.Now().Before(deadline) {
		record, err := consumer.Poll(ctx, pollStep)
		if err != nil {
			lastErr = err
			continue
		}
		if record != nil && record.Message != nil {
			return record
		}
	}
	t.Fatalf("no record arrived within %s, last error: %v", allowance, lastErr)
	return nil
}
