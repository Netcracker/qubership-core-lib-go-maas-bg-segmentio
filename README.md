[![Go build](https://github.com/Netcracker/qubership-core-lib-go-maas-bg-segmentio/actions/workflows/go-build.yml/badge.svg)](https://github.com/Netcracker/qubership-core-lib-go-maas-bg-segmentio/actions/workflows/go-build.yml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?metric=coverage&project=Netcracker_qubership-core-lib-go-maas-bg-segmentio)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-bg-segmentio)
[![duplicated_lines_density](https://sonarcloud.io/api/project_badges/measure?metric=duplicated_lines_density&project=Netcracker_qubership-core-lib-go-maas-bg-segmentio)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-bg-segmentio)
[![vulnerabilities](https://sonarcloud.io/api/project_badges/measure?metric=vulnerabilities&project=Netcracker_qubership-core-lib-go-maas-bg-segmentio)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-bg-segmentio)
[![bugs](https://sonarcloud.io/api/project_badges/measure?metric=bugs&project=Netcracker_qubership-core-lib-go-maas-bg-segmentio)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-bg-segmentio)
[![code_smells](https://sonarcloud.io/api/project_badges/measure?metric=code_smells&project=Netcracker_qubership-core-lib-go-maas-bg-segmentio)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-bg-segmentio)

# Blue-Green for segmentio Kafka client 

This library provides adaptation for Blue Green Consumer from [blue-green-kafka](https://github.com/Netcracker/qubership-core-lib-go-bg-kafka)
for [segmentio/kafka-go](https://github.com/segmentio/kafka-go) native go client.

<!-- TOC -->
* [Blue-Green for segmentio Kafka client](#blue-green-for-segmentio-kafka-client-)
  * [Usage examples](#usage-examples)
    * [Simple consumer creation and polling](#simple-consumer-creation-and-polling)
    * [Metrics](#metrics)
  * [Migration to Blue Green 2](#migration-to-blue-green-2)
<!-- TOC -->

## Usage examples

### Simple consumer creation and polling
~~~ go 
package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/netcracker/qubership-core-lib-go/v3/logging"
	bgKafka "github.com/netcracker/qubership-core-lib-go-maas-bg-kafka/v3"
	bgSegmentio "github.com/netcracker/qubership-core-lib-go-maas-bg-segmentio/v3"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"time"
)
var logger logging.Logger

func init()  {
	logger = logging.GetLogger("bg-segmentio-consumer")
}

// vars below provided by skipped code
var topicAddress model.TopicAddress
const readTimeout = time.Minute

func main() {
	ctx := context.Background()
	consumer, err := bgSegmentio.NewBgConsumer(ctx, topicAddress, "group-id")
	if err != nil {
		panic(err)
	}
	for {
		record, err := consumer.Poll(ctx, readTimeout)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// all messages from Kafka are processed and readTimeout occurred, try again
				continue
			} else {
				logger.ErrorC(ctx, "Failed to read message from Kafka: %s", err.Error())
				return
			}
		}
		if record.Message == nil {
			// message can be nil, if message at particular offset was filtered out by the filter
			// because current bg version should ignore this particular message
			logger.DebugC(ctx, "Commit skipped message at offset: %d", record.Marker.OffsetAndMeta.Offset)
			cErr := consumer.Commit(ctx, record.Marker)
			if cErr != nil {
				logger.ErrorC(ctx, "Failed to commit message at offset: %d. Cause: %s", record.Marker.OffsetAndMeta.Offset, cErr.Error())
				return
			}
		} else {
			logger.InfoC(ctx, "Received message: %+v", record.Message)
			cErr := processMsg(ctx, consumer, record.Message, record.Marker)
			if cErr != nil {
				logger.ErrorC(ctx, "Failed to process message at offset: %d. Cause: %s", record.Marker.OffsetAndMeta.Offset, cErr.Error())
				return
			}
		}
	}
}

type myType struct {
	Id   string `json:"id"`
	Data string `json:"data"`
}

func processMsg(ctx context.Context, consumer *bgKafka.BgConsumer, message bgKafka.Message, marker *bgKafka.CommitMarker) error {
	logger.DebugC(ctx, "Received message: %+v", message)
	var msg myType
	err := json.Unmarshal(message.Value(), &msg)
	if err != nil {
		return err
	}
	logger.DebugC(ctx, "Data: %s", msg.Data)
	return consumer.Commit(ctx, marker)
}
~~~

### Metrics

Blue-green kafka consumer collects own metrics set.
Metrics are updating on messages poll and offset commit (interaction with message broker).

Values of each metric are split by partitions and contains data only from partitions assigned to this consumer.

Metrics usage example:

```go
// Use Stats() method to receive metrics snapshot.
metrics := consumer.Stats()

// Call GetByPartitions() method for required metric to get map[int]in64, where key is a partition number
lags := metrics.Lag.GetByPartitions()
for partition, lag := range lags {
    logger.Info("Lag on partition %d: %d", partition, lag)	
}

// Be aware that metric for each partition is updating on poll or commit. 
// It means that before that after consumer init metrics may be empty 
_, commitOffsetExists := metrics.CommitOffset.GetByPartitions()[0]
if !commitOffsetExists {
    logger.Info("No offest has been commited for partition 0 yet")
}   
```

You can find full list of supported metrics and its description
in [metrics.go](https://github.com/Netcracker/qubership-core-lib-go-bg-kafka/blob/main/metrics.go#L43)

## Migration to Blue Green 2
See details [here](docs/migration.md)
