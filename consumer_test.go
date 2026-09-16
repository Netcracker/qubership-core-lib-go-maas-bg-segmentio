package blue_green_segmentio

import (
	"context"
	"testing"

	blue_green_kafka "github.com/netcracker/qubership-core-lib-go-bg-kafka/v3"
	bgKafkaTest "github.com/netcracker/qubership-core-lib-go-bg-kafka/v3/test"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	segmentio "github.com/netcracker/qubership-core-lib-go-maas-segmentio/v3"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func TestBGConsumer(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()

	brokers, err := bgKafkaTest.StartContainers(t)
	assertions.NoError(err)

	topic := "orders"
	topicAddress := model.TopicAddress{
		Classifier:      classifier.New("test"),
		TopicName:       topic,
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"PLAINTEXT": brokers},
	}

	bgKafkaTest.RunBGConsumerTest(t, topic,
		func(options []blue_green_kafka.Option) (*blue_green_kafka.BgConsumer, error) {
			return NewBgConsumer(ctx, topicAddress, "test-prep-go", options...)
		},
		func() (bgKafkaTest.Writer, error) {
			w, err := segmentio.NewWriter(topicAddress)
			if err != nil {
				return nil, err
			}
			return &writer{writer: w}, nil
		},
		func(replicationFactor int) error {
			admin, err := getAdminSupplier(topicAddress)
			client := admin.(*adminAdapter).client
			_, err = client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
				Topics: []kafka.TopicConfig{{
					Topic:             topicAddress.TopicName,
					NumPartitions:     topicAddress.NumPartitions,
					ReplicationFactor: replicationFactor,
				}},
				ValidateOnly: false,
			})
			return err
		})
}

// A failed read must not hand back a message wrapper. It would wrap a zero
// message, and a caller reading it before the error gets an empty record with an
// offset of 0.
func TestConsumerAdapter_ReadMessageReturnsNoMessageOnError(t *testing.T) {
	assertions := require.New(t)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"127.0.0.1:1"},
		Topic:   "orders",
		GroupID: "group",
	})
	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	message, err := (&consumerAdapter{reader: reader}).ReadMessage(ctx)

	assertions.Error(err)
	assertions.Nil(message)
}

type writer struct {
	writer *kafka.Writer
}

func (w *writer) Write(ctx context.Context, msg blue_green_kafka.Message) error {
	var headers []kafka.Header
	for _, h := range msg.Headers() {
		headers = append(headers, kafka.Header{Key: h.Key, Value: h.Value})
	}
	return w.writer.WriteMessages(ctx, kafka.Message{
		Topic:     msg.Topic(),
		Partition: msg.Partition(),
		Offset:    msg.Offset(),
		Key:       msg.Key(),
		Value:     msg.Value(),
		Headers:   headers,
	})
}
