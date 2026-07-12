package blue_green_segmentio

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	bgKafka "github.com/netcracker/qubership-core-lib-go-bg-kafka/v3"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	segmentio "github.com/netcracker/qubership-core-lib-go-maas-segmentio/v3"
	"github.com/segmentio/kafka-go"
)

type adminAdapter struct {
	addr    net.Addr
	client  client
	dialer  dialer
	brokers []string
	topic   string
}

type dialer interface {
	Dial(network string, address string) (*kafka.Conn, error)
}

type client interface {
	ListGroups(ctx context.Context, req *kafka.ListGroupsRequest) (*kafka.ListGroupsResponse, error)
	CreateTopics(ctx context.Context, req *kafka.CreateTopicsRequest) (*kafka.CreateTopicsResponse, error)
	OffsetFetch(ctx context.Context, req *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error)
	OffsetCommit(ctx context.Context, req *kafka.OffsetCommitRequest) (*kafka.OffsetCommitResponse, error)
	ListOffsets(ctx context.Context, req *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error)
}

func getAdminSupplier(topicAddr model.TopicAddress) (bgKafka.NativeAdminAdapter, error) {
	client, err := segmentio.NewClient(topicAddr)
	if err != nil {
		return nil, err
	}
	d, brokers, err := segmentio.NewDialerAndServers(topicAddr)
	if err != nil {
		return nil, err
	}
	return &adminAdapter{addr: client.Addr, client: client, dialer: d, brokers: brokers, topic: topicAddr.TopicName}, nil
}

func (d *adminAdapter) ListConsumerGroups(ctx context.Context) (result []bgKafka.ConsumerGroup, err error) {
	listGroupsResponse, err := d.client.ListGroups(ctx, &kafka.ListGroupsRequest{})
	if err != nil {
		return
	}
	for _, gr := range listGroupsResponse.Groups {
		result = append(result, bgKafka.ConsumerGroup{GroupId: gr.GroupID})
	}
	return
}

func (d *adminAdapter) ListConsumerGroupOffsets(ctx context.Context, groupId string) (map[bgKafka.TopicPartition]bgKafka.OffsetAndMetadata, error) {
	partitions, err := d.PartitionsFor(ctx, d.topic)
	if err != nil {
		return nil, err
	}
	var topicPartitions map[string][]int
	for _, p := range partitions {
		if topicPartitions == nil {
			topicPartitions = map[string][]int{}
		}
		parts := topicPartitions[p.Topic]
		parts = append(parts, p.Partition)
		topicPartitions[p.Topic] = parts
	}
	offsetFetchResponse, err := d.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: groupId,
		Topics:  topicPartitions,
	})
	if err != nil {
		return nil, err
	}
	result := map[bgKafka.TopicPartition]bgKafka.OffsetAndMetadata{}
	for t, ofps := range offsetFetchResponse.Topics {
		for _, ofp := range ofps {
			result[bgKafka.TopicPartition{
				Partition: ofp.Partition,
				Topic:     t,
			}] = bgKafka.OffsetAndMetadata{Offset: ofp.CommittedOffset}
		}
	}
	return result, err
}

func (d *adminAdapter) AlterConsumerGroupOffsets(ctx context.Context, groupId bgKafka.GroupId, proposedOffsets map[bgKafka.TopicPartition]bgKafka.OffsetAndMetadata) error {
	topicOffsets := map[string][]kafka.OffsetCommit{}
	var topics []string
	for tp, om := range proposedOffsets {
		offsetCommitsForTopic, ok := topicOffsets[tp.Topic]
		if !ok {
			offsetCommitsForTopic = []kafka.OffsetCommit{}
		}
		offsetCommitsForTopic = append(offsetCommitsForTopic, kafka.OffsetCommit{
			Partition: tp.Partition,
			Offset:    om.Offset,
		})
		topicOffsets[tp.Topic] = offsetCommitsForTopic
		topics = append(topics, tp.Topic)
	}
	// need to join consumer group to avoid NotCoordinatorForGroup(16) error from Kafka
	consumerGroupConfig := kafka.ConsumerGroupConfig{
		ID:      groupId.String(),
		Brokers: d.brokers,
		Dialer:  d.dialer.(*kafka.Dialer),
		Topics:  topics,
	}
	return getConsumerGroupGenerationWithRetry(ctx, consumerGroupConfig, func(ctx context.Context, generation *kafka.Generation) error {
		response, err := d.client.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
			GroupID:      groupId.String(),
			Topics:       topicOffsets,
			GenerationID: int(generation.ID),
			MemberID:     generation.MemberID,
		})
		if err != nil {
			return err
		}
		var errs []error
		for _, tErr := range response.Topics {
			for _, pErr := range tErr {
				if pErr.Error != nil {
					errs = append(errs, pErr.Error)
				}
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("errors: %v", errs)
		} else {
			return nil
		}
	})
}

func getConsumerGroupGenerationWithRetry(ctx context.Context, config kafka.ConsumerGroupConfig,
	commitOperation func(ctx context.Context, generation *kafka.Generation) error) error {
	var err error
	var consumerGroup *kafka.ConsumerGroup
	warnLogFunc := func(ctx context.Context, id string, err error) {
		logger.DebugC(ctx, "Failed to get next generation for consumer group: '%s', err: %s", config.ID, err.Error())
	}
	sleepDuration := 100 * time.Millisecond
	attempts := 100
	attempt := 0
	for attempt < attempts {
		attempt++
		consumerGroup, err = kafka.NewConsumerGroup(config)
		if err != nil {
			if errors.Is(err, ctx.Err()) {
				return err
			} else {
				warnLogFunc(ctx, config.ID, err)
				time.Sleep(sleepDuration)
				continue
			}
		}
		err = getConsumerGroupGenerationAndCommit(ctx, consumerGroup, commitOperation)
		if err != nil {
			if errors.Is(err, ctx.Err()) {
				return err
			} else {
				warnLogFunc(ctx, config.ID, err)
				time.Sleep(sleepDuration)
				continue
			}
		} else {
			return nil
		}
	}
	return err
}

func getConsumerGroupGenerationAndCommit(ctx context.Context, consumerGroup *kafka.ConsumerGroup,
	commitOperation func(ctx context.Context, generation *kafka.Generation) error) error {
	generation, err := consumerGroup.Next(ctx)
	defer consumerGroup.Close()
	if err != nil {
		return err
	} else {
		return commitOperation(ctx, generation)
	}
}

func (d *adminAdapter) PartitionsFor(ctx context.Context, topics ...string) (partitionsInfo []bgKafka.PartitionInfo, err error) {
	if len(topics) == 0 {
		err = errors.New("topics cannot be empty")
	}
	conn, err := d.connectToAnyBroker(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions(topics...)
	if err != nil {
		return
	}
	for _, p := range partitions {
		partitionsInfo = append(partitionsInfo, bgKafka.PartitionInfo{
			Topic:     p.Topic,
			Partition: p.ID,
		})
	}
	return
}

func (d *adminAdapter) connectToAnyBroker(ctx context.Context) (conn *kafka.Conn, err error) {
	networks := strings.Split(d.addr.Network(), ",")
	addresses := strings.Split(d.addr.String(), ",")
	if len(networks) != len(addresses) {
		return nil, fmt.Errorf("invalid client Addr: Network() must return the same entries as String(), but got: '%v' and '%v'",
			networks, addresses)
	}
	brokers := map[string]string{}
	for i, network := range networks {
		brokers[network] = addresses[i]
	}
	for network, broker := range brokers {
		// connect to the first available broker
		if conn, err = d.dialer.Dial(network, broker); err == nil {
			return conn, nil
		}
	}
	return
}

func (d *adminAdapter) BeginningOffsets(ctx context.Context, topicPartitions []bgKafka.TopicPartition) (map[bgKafka.TopicPartition]int64, error) {
	return d.absoluteOffsets(ctx, topicPartitions,
		func(partition int) kafka.OffsetRequest {
			return kafka.FirstOffsetOf(partition)
		},
		func(offsets kafka.PartitionOffsets) *bgKafka.OffsetAndTimestamp {
			return &bgKafka.OffsetAndTimestamp{Offset: offsets.FirstOffset, Timestamp: time.Time{}.UnixMilli()}
		})
}

func (d *adminAdapter) EndOffsets(ctx context.Context, topicPartitions []bgKafka.TopicPartition) (map[bgKafka.TopicPartition]int64, error) {
	return d.absoluteOffsets(ctx, topicPartitions,
		func(partition int) kafka.OffsetRequest {
			return kafka.LastOffsetOf(partition)
		},
		func(offsets kafka.PartitionOffsets) *bgKafka.OffsetAndTimestamp {
			return &bgKafka.OffsetAndTimestamp{Offset: offsets.LastOffset, Timestamp: time.Time{}.UnixMilli()}
		})
}

func (d *adminAdapter) absoluteOffsets(ctx context.Context, topicPartitions []bgKafka.TopicPartition,
	reqFunc func(partition int) kafka.OffsetRequest,
	offsetFunc func(offsets kafka.PartitionOffsets) *bgKafka.OffsetAndTimestamp) (map[bgKafka.TopicPartition]int64, error) {
	offsetRequests := make(map[bgKafka.TopicPartition]kafka.OffsetRequest)
	for _, tp := range topicPartitions {
		offsetRequests[tp] = reqFunc(tp.Partition)
	}
	offsetForTimes, err := d.offsetsForTimes(ctx, offsetRequests, offsetFunc)
	result := map[bgKafka.TopicPartition]int64{}
	for tp, ot := range offsetForTimes {
		if ot != nil {
			result[tp] = ot.Offset
		}
	}
	return result, err
}

func (d *adminAdapter) OffsetsForTimes(ctx context.Context, m map[bgKafka.TopicPartition]time.Time) (
	map[bgKafka.TopicPartition]*bgKafka.OffsetAndTimestamp, error) {
	offsetRequests := make(map[bgKafka.TopicPartition]kafka.OffsetRequest)
	for tp, t := range m {
		offsetRequests[tp] = kafka.TimeOffsetOf(tp.Partition, t)
	}
	return d.offsetsForTimes(ctx, offsetRequests, func(offsets kafka.PartitionOffsets) *bgKafka.OffsetAndTimestamp {
		var result *bgKafka.OffsetAndTimestamp
		var earliest time.Time
		// todo return the earliest by time offset?
		for offs, t := range offsets.Offsets {
			if result == nil || t.Before(earliest) {
				earliest = t
				result = &bgKafka.OffsetAndTimestamp{Offset: offs, Timestamp: t.UnixMilli()}
			}
		}
		// nil means no matching record, per NativeAdminAdapter's contract.
		return result
	})
}

func (d *adminAdapter) offsetsForTimes(ctx context.Context, m map[bgKafka.TopicPartition]kafka.OffsetRequest,
	offsetFunc func(offsets kafka.PartitionOffsets) *bgKafka.OffsetAndTimestamp) (
	result map[bgKafka.TopicPartition]*bgKafka.OffsetAndTimestamp, err error) {
	topicsMap := map[string][]kafka.OffsetRequest{}
	for tp, offsetRequest := range m {
		topicsMap[tp.Topic] = append(topicsMap[tp.Topic], offsetRequest)
	}
	offsetsResponse, err := d.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: topicsMap,
	})
	if err != nil {
		return
	}
	result = map[bgKafka.TopicPartition]*bgKafka.OffsetAndTimestamp{}
	var errs []error
	for t, pos := range offsetsResponse.Topics {
		for _, po := range pos {
			topicPartition := bgKafka.TopicPartition{
				Partition: po.Partition,
				Topic:     t,
			}
			if po.Error != nil {
				// don't trust the sentinel Offset: -1 kafka-go attaches on a per-partition error
				logger.WarnC(ctx, "ListOffsets reported an error for partition %+v: %s", topicPartition, po.Error)
				errs = append(errs, fmt.Errorf("partition %+v: %w", topicPartition, po.Error))
				continue
			}
			result[topicPartition] = offsetFunc(po)
		}
	}
	if len(errs) > 0 {
		err = errors.Join(errs...)
	}
	return
}
