package blue_green_segmentio

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	bgKafka "github.com/netcracker/qubership-core-lib-go-bg-kafka/v3"
	bg "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	kafkaModel "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	maasKafkaGo "github.com/netcracker/qubership-core-lib-go-maas-segmentio/v3"
	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/ctxmanager"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

// Verifies the offset corrector end-to-end against a real broker, in a non-BG namespace
// (plain consumer group):
//
//  1. an existing group's committed offsets survive consumer reinitialization untouched, even
//     when its unconsumed records are older than the default Rewind(5m) install window — the
//     incident scenario in which offsets used to be silently rewritten on every start;
//  2. after partitions are added to the topic, offsets are installed for the new partitions
//     only; the existing partitions' committed offsets again stay untouched.
//
// Reinitialization is triggered by republishing the BG state: BgConsumer.Poll then closes the
// current reader and runs the exact corrector path a process restart runs (fresh group index,
// freshly parsed group id).
func TestBgConsumerCommittedOffsetsSurviveRestart(t *testing.T) {
	ctx := context.Background()
	assertions := require.New(t)

	initialLogLevel := os.Getenv("LOG_LEVEL")
	defer os.Setenv("LOG_LEVEL", initialLogLevel)
	os.Setenv("LOG_LEVEL", "debug")

	configloader.InitWithSourcesArray([]*configloader.PropertySource{configloader.EnvPropertySource()})
	ctxmanager.Register(baseproviders.Get())
	useDockerHostFromEnv()
	kafkaCluster, err := newKafkaCluster(ctx, "7.4.0", 1, 1)
	assertions.NoError(err)
	defer kafkaCluster.stop(ctx)
	t.Logf("kafka cluster started")

	servers := kafkaCluster.brokers()

	topic := "restart-topic"
	topics := []string{topic}
	groupId := "restart-group"
	initialPartitions := 2

	topicAddress = kafkaModel.TopicAddress{
		TopicName:       topic,
		NumPartitions:   initialPartitions,
		BoostrapServers: map[string][]string{"PLAINTEXT": servers},
	}
	kafkaClient, err := maasKafkaGo.NewClient(topicAddress)
	assertions.NoError(err)
	topicResp, err := kafkaClient.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{{
		Topic:             topic,
		NumPartitions:     initialPartitions,
		ReplicationFactor: 1,
	}}})
	assertions.NoError(err)
	assertions.Nil(topicResp.Errors[topic])
	waitForPartitions(assertions, topic, initialPartitions)

	// no sibling namespace in the state, so the consumer group is plain
	statePublisher, err := bg.NewInMemoryPublisher(newStates("2024-01-01T10:00:00Z",
		bg.NamespaceVersion{State: bg.StateActive, Version: bg.NewVersionMust("v1")}).Origin)
	assertions.NoError(err)
	consumer, err := NewBgConsumer(ctx, topicAddress, groupId, bgKafka.WithBlueGreenStatePublisher(statePublisher))
	assertions.NoError(err)
	consumers := &ConsumerKey{Key: originNs, Consumers: map[string]*bgKafka.BgConsumer{originNs + "/pod-1": consumer}}

	restartCounter := 0
	restartConsumer := func() {
		restartCounter++
		statePublisher.SetState(newStates(fmt.Sprintf("2024-01-01T10:%02d:00Z", restartCounter),
			bg.NamespaceVersion{State: bg.StateActive, Version: bg.NewVersionMust("v1")}).Origin)
	}

	msgCounter := &atomic.Int32{}

	t.Logf("bootstrap the group: consume and commit an initial record on every partition")
	sent, err := sendRecords(t, "producing initial records", "", records(topics, initialPartitions, msgCounter))
	assertions.NoError(err)
	assertions.True(parallelConsume(ctx, t, pollTimeout,
		map[string][]string{originNs: recordsKafkaAsString(sent)},
		map[string]map[TopicPartition]int64{originNs: {
			{Topic: topic, Partition: 0}: 1,
			{Topic: topic, Partition: 1}: 1,
		}},
		consumers))

	t.Logf("produce records timestamped before the Rewind(5m) window, then restart the consumer")
	sent, err = sendRecords(t, "producing records older than the rewind window", "",
		agedBeforeRewindWindow(records(topics, initialPartitions, msgCounter)))
	assertions.NoError(err)
	restartConsumer()

	// a corrector that rewrites an existing group's offsets on restart would move the
	// committed offsets off the old records; every one of them must be consumed instead
	assertions.True(parallelConsume(ctx, t, pollTimeout,
		map[string][]string{originNs: recordsKafkaAsString(sent)},
		map[string]map[TopicPartition]int64{originNs: {
			{Topic: topic, Partition: 0}: 2,
			{Topic: topic, Partition: 1}: 2,
		}},
		consumers))

	expandedPartitions := 4
	t.Logf("add partitions %d -> %d, produce to all of them, then restart the consumer", initialPartitions, expandedPartitions)
	createResp, err := kafkaClient.CreatePartitions(ctx, &kafka.CreatePartitionsRequest{
		Topics: []kafka.TopicPartitionsConfig{{Name: topic, Count: int32(expandedPartitions)}},
	})
	assertions.NoError(err)
	assertions.Nil(createResp.Errors[topic])
	waitForPartitions(assertions, topic, expandedPartitions)
	topicAddress.NumPartitions = expandedPartitions

	_, err = sendRecords(t, "producing to the new partitions, before the group knows them", "",
		agedBeforeRewindWindow(recordsForPartitions(topic, []int{2, 3}, msgCounter)))
	assertions.NoError(err)

	sent, err = sendRecords(t, "producing to pre-expansion partitions", "",
		agedBeforeRewindWindow(records(topics, initialPartitions, msgCounter)))
	assertions.NoError(err)
	restartConsumer()

	assertions.True(parallelConsume(ctx, t, pollTimeout,
		map[string][]string{originNs: recordsKafkaAsString(sent)},
		map[string]map[TopicPartition]int64{originNs: {
			{Topic: topic, Partition: 0}: 3,
			{Topic: topic, Partition: 1}: 3,
		}},
		consumers))

	committed := committedOffsets(ctx, assertions, kafkaClient, groupId, topic, expandedPartitions)
	assertions.Equal(int64(1), committed[2], "partition 2 must start at its end offset")
	assertions.Equal(int64(1), committed[3], "partition 3 must start at its end offset")
}

// committedOffsets reads the group's committed offset per partition straight from the broker.
func committedOffsets(ctx context.Context, assertions *require.Assertions, client *kafka.Client,
	groupId, topic string, partitions int) map[int]int64 {
	indexes := make([]int, partitions)
	for i := range indexes {
		indexes[i] = i
	}
	response, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: groupId,
		Topics:  map[string][]int{topic: indexes},
	})
	assertions.NoError(err)
	assertions.NoError(response.Error)

	result := map[int]int64{}
	for _, p := range response.Topics[topic] {
		assertions.NoError(p.Error)
		result[p.Partition] = p.CommittedOffset
	}
	return result
}

// agedBeforeRewindWindow backdates records past the default Rewind(5m) install window.
func agedBeforeRewindWindow(msgs []kafka.Message) []kafka.Message {
	for i := range msgs {
		msgs[i].Time = time.Now().Add(-10 * time.Minute)
	}
	return msgs
}

func waitForPartitions(assertions *require.Assertions, topic string, expected int) {
	dialer, servers, err := maasKafkaGo.NewDialerAndServers(topicAddress)
	assertions.NoError(err)
	for _, server := range servers {
		conn, err := dialer.Dial("tcp", server)
		assertions.NoError(err)
		for {
			prts, err := conn.ReadPartitions(topic)
			if err == nil && len(prts) == expected {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		conn.Close()
	}
}

func recordsForPartitions(topic string, partitions []int, msgCounter *atomic.Int32) (result []kafka.Message) {
	for _, p := range partitions {
		msgCounter.Add(1)
		result = append(result, kafka.Message{
			Topic: topic, Partition: p,
			Key:   []byte(buildKey(msgCounter.Load())),
			Value: []byte(buildValue(topic, msgCounter.Load())),
		})
	}
	return
}
