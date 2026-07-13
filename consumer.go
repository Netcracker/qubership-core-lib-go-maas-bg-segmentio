package blue_green_segmentio

import (
	"context"

	bgKafka "github.com/netcracker/qubership-core-lib-go-bg-kafka/v3"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	segmentio "github.com/netcracker/qubership-core-lib-go-maas-segmentio/v3"
	"github.com/netcracker/qubership-core-lib-go/v3/logging"
	"github.com/segmentio/kafka-go"
)

var logger logging.Logger

func init() {
	logger = logging.GetLogger("bg-kafka-segmentio")
}

func NewBgConsumer(ctx context.Context, topicAddr model.TopicAddress, groupId string, options ...bgKafka.Option) (*bgKafka.BgConsumer, error) {
	return bgKafka.NewConsumer(ctx, topicAddr.TopicName, groupId,
		func() (bgKafka.NativeAdminAdapter, error) {
			return getAdminSupplier(topicAddr)
		},
		func(groupId string) (bgKafka.Consumer, error) {
			return getConsumer(topicAddr, groupId)
		},
		options...)
}

func getConsumer(topicAddr model.TopicAddress, groupId string) (bgKafka.Consumer, error) {
	readerConf, err := segmentio.NewReaderConfig(topicAddr, groupId)
	if err != nil {
		return nil, err
	}
	reader := kafka.NewReader(*readerConf)
	return &consumerAdapter{reader: reader}, nil
}

type consumerAdapter struct {
	reader *kafka.Reader
}

func (s *consumerAdapter) Offset() int64 {
	return s.reader.Offset()
}

// ReadMessage never redelivers within a session: FetchMessage's read position only moves
// forward. Callers must retry a failed message in place, not by calling ReadMessage again.
func (s *consumerAdapter) ReadMessage(ctx context.Context) (bgKafka.Message, error) {
	nativeMsg, err := s.reader.FetchMessage(ctx)
	return fromKafkaMessage(&nativeMsg), err
}

func fromKafkaMessage(nativeMsg *kafka.Message) bgKafka.Message {
	return &msgAdapter{nativeMessage: nativeMsg}
}

type msgAdapter struct {
	nativeMessage *kafka.Message
}

func (m *msgAdapter) Topic() string {
	return m.nativeMessage.Topic
}

func (m *msgAdapter) HighWaterMark() int64 {
	return m.nativeMessage.HighWaterMark
}

func (m *msgAdapter) Offset() int64 {
	return m.nativeMessage.Offset
}

func (m *msgAdapter) Headers() (headers []bgKafka.Header) {
	for _, h := range m.nativeMessage.Headers {
		headers = append(headers, bgKafka.Header{
			Key:   h.Key,
			Value: h.Value,
		})
	}
	return headers
}

func (m *msgAdapter) Key() []byte {
	return m.nativeMessage.Key
}

func (m *msgAdapter) Value() []byte {
	return m.nativeMessage.Value
}

func (m *msgAdapter) Partition() int {
	return m.nativeMessage.Partition
}

func (m *msgAdapter) NativeMsg() any {
	return m.nativeMessage
}

func (s *consumerAdapter) Commit(ctx context.Context, marker *bgKafka.CommitMarker) error {
	return s.reader.CommitMessages(ctx, kafka.Message{
		Topic:     marker.TopicPartition.Topic,
		Partition: marker.TopicPartition.Partition,
		Offset:    marker.OffsetAndMeta.Offset,
	})
}

func (s *consumerAdapter) Close() error {
	return s.reader.Close()
}
