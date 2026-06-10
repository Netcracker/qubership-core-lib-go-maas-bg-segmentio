package blue_green_segmentio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bgKafka "github.com/netcracker/qubership-core-lib-go-bg-kafka/v3"
	bg "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	kafkaModel "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	maasKafkaGo "github.com/netcracker/qubership-core-lib-go-maas-segmentio/v3"
	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders/xrequestid"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders/xversion"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/ctxmanager"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	originNs          = "origin"
	peerNs            = "peer"
	brokers           = 3
	pods              = 1
	partitions        = pods * 2
	replicationFactor = 2
	pollTimeout       = 30 * time.Second
	noRecordsTimeout  = 5 * time.Second

	topicAddress kafkaModel.TopicAddress
	publicPort   = "9093"
)

func TestNewBgConsumer(t *testing.T) {
	ctx := context.Background()
	assertions := require.New(t)

	initialLogLevel := os.Getenv("LOG_LEVEL")
	defer os.Setenv("LOG_LEVEL", initialLogLevel)
	os.Setenv("LOG_LEVEL", "debug")

	configloader.InitWithSourcesArray([]*configloader.PropertySource{configloader.EnvPropertySource()})
	ctxmanager.Register(baseproviders.Get())
	setTestDocker(t)
	kafkaCluster, err := NewKafkaCluster(ctx, "7.4.0", brokers, replicationFactor)
	assertions.NoError(err)
	defer kafkaCluster.Stop(ctx)
	t.Logf("kafka cluster started")

	servers, err := kafkaCluster.Brokers(ctx)
	assertions.NoError(err)
	fmt.Printf("servers=%v", servers)

	topic := "topic-1"
	topics := []string{topic}
	groupId := "test-group"

	topicAddress = kafkaModel.TopicAddress{
		TopicName:       topic,
		NumPartitions:   partitions,
		BoostrapServers: map[string][]string{"PLAINTEXT": servers},
	}

	kafkaClient, err := maasKafkaGo.NewClient(topicAddress)
	assertions.NoError(err)
	topicConfigs := []kafka.TopicConfig{{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	}}
	// create topics
	topicResp, err := kafkaClient.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: topicConfigs})
	assertions.NoError(err)
	assertions.Nil(topicResp.Errors[topic])

	// WA to wait for topics to be registered everywhere
	diler, servers, err := maasKafkaGo.NewDialerAndServers(topicAddress)
	assertions.NoError(err)
	for _, srvr := range servers {
		conn, err := diler.Dial("tcp", srvr)
		assertions.NoError(err)
		check := true
		for check {
			prts, err := conn.ReadPartitions(topic)
			check = err != nil || len(prts) != len(topics)*partitions
		}
		conn.Close()
	}
	podNameFunc := func(ns string, podId int) string {
		return fmt.Sprintf("%s/pod-%d", ns, podId+1)
	}
	states := newStates("2024-01-01T10:00:00Z", bg.NamespaceVersion{State: bg.StateActive, Version: bg.NewVersionMust("v1")})
	statePublisherOrigin, _ := bg.NewInMemoryPublisher(states.Origin)
	originConsumers := map[string]*bgKafka.BgConsumer{}
	for podId := 0; podId < pods; podId++ {
		consumer, err := NewBgConsumer(ctx, topicAddress, groupId,
			bgKafka.WithBlueGreenStatePublisher(statePublisherOrigin), bgKafka.WithConsistencyMode(bgKafka.GuaranteeConsumption))
		if err != nil {
			panic(err)
		}
		originConsumers[podNameFunc(originNs, podId)] = consumer
	}

	assertions.True(parallelConsume(ctx, t, noRecordsTimeout,
		map[string][]string{originNs: {}},
		map[string]map[TopicPartition]int64{originNs: {}},
		&ConsumerKey{Key: originNs, Consumers: originConsumers}))

	msgCounter := &atomic.Int32{}
	sentRecords, err := sendRecords(t, "producing records for standalone active", "", records(topics, partitions, msgCounter))
	assertions.NoError(err)
	expectedLatestOffset := len(sentRecords) / len(topics) / partitions

	t.Logf("consume and commit all records by standalone active consumer")

	assertions.True(parallelConsume(ctx, t, pollTimeout,
		map[string][]string{originNs: recordsKafkaAsString(sentRecords)},
		map[string]map[TopicPartition]int64{originNs: offsetsMap(topics, expectedLatestOffset)},
		&ConsumerKey{Key: originNs, Consumers: originConsumers}))

	t.Logf("BG-Operation:init")

	states = newStates("2024-01-01T10:01:00Z", bg.NamespaceVersion{State: bg.StateActive, Version: bg.NewVersionMust("v1")}, bg.NamespaceVersion{State: bg.StateIdle})
	updatePublishersVersions(states, statePublisherOrigin)

	assertions.True(parallelConsume(ctx, t, noRecordsTimeout,
		map[string][]string{originNs: {}},
		map[string]map[TopicPartition]int64{originNs: {}},
		&ConsumerKey{Key: originNs, Consumers: originConsumers}))

	sentRecords, err = sendRecords(t, "producing records: active+idle", "", records(topics, partitions, msgCounter))
	assertions.NoError(err)
	expectedLatestOffset += len(sentRecords) / len(topics) / partitions

	t.Logf("consume and commit all records by active consumer")

	assertions.True(parallelConsume(ctx, t, pollTimeout,
		map[string][]string{originNs: recordsKafkaAsString(sentRecords)},
		map[string]map[TopicPartition]int64{originNs: offsetsMap(topics, expectedLatestOffset)},
		&ConsumerKey{Key: originNs, Consumers: originConsumers}))

	statePublisherPeer, _ := bg.NewInMemoryPublisher(*states.Peer)

	peerConsumers := map[string]*bgKafka.BgConsumer{}
	for podId := 0; podId < pods; podId++ {
		consumer, err := NewBgConsumer(ctx, topicAddress, groupId,
			bgKafka.WithBlueGreenStatePublisher(statePublisherPeer), bgKafka.WithConsistencyMode(bgKafka.GuaranteeConsumption))
		if err != nil {
			panic(err)
		}
		peerConsumers[podNameFunc(peerNs, podId)] = consumer
	}
	bgCycles := 1
	timeCounter := 2
	verCounter := 1

	for i := 1; i <= bgCycles; i++ {
		// warmup#1
		ver1Active := fmt.Sprintf("v%d", verCounter)
		verCounter++
		ver1Candidate := fmt.Sprintf("v%d", verCounter)
		// promote#1
		// rollback#1
		// promote#2
		// commit#1
		ver2Active := ver1Candidate
		ver2Legacy := ver1Active
		// warmup#2
		ver3Active := ver2Active
		verCounter++
		ver3Candidate := fmt.Sprintf("v%d", verCounter)
		// promote#1
		ver4Active := ver3Candidate
		ver4Legacy := ver3Active

		t.Logf("BG-Operation:warmup #%d", i)
		timeCounter++
		states = newStates(fmt.Sprintf("2024-01-01T10:%02d:00Z", timeCounter),
			bg.NamespaceVersion{State: bg.StateActive, Version: bg.NewVersionMust(ver1Active)},
			bg.NamespaceVersion{State: bg.StateCandidate, Version: bg.NewVersionMust(ver1Candidate)})
		updatePublishersVersions(states, statePublisherOrigin, statePublisherPeer)

		assertions.True(parallelConsume(ctx, t, noRecordsTimeout,
			map[string][]string{
				originNs: {},
				peerNs:   {},
			},
			map[string]map[TopicPartition]int64{
				originNs: {},
				peerNs:   {},
			},
			&ConsumerKey{Key: originNs, Consumers: originConsumers},
			&ConsumerKey{Key: peerNs, Consumers: peerConsumers},
		))

		t.Logf("consume and commit all records by active, candidate consumers")

		sentRecordsV1Active, err := sendRecords(t, "producing records for active", ver1Active, records(topics, partitions, msgCounter))
		assertions.NoError(err)
		expectedLatestOffset += len(sentRecordsV1Active) / len(topics) / partitions

		sentRecordsV1Candidate, err := sendRecords(t, "producing records for candidate", ver1Candidate, records(topics, partitions, msgCounter))
		assertions.NoError(err)
		expectedLatestOffset += len(sentRecordsV1Candidate) / len(topics) / partitions

		assertions.True(parallelConsume(ctx, t, pollTimeout,
			map[string][]string{
				originNs: recordsKafkaAsString(sentRecordsV1Active),
				peerNs:   recordsKafkaAsString(sentRecordsV1Candidate),
			},
			map[string]map[TopicPartition]int64{
				originNs: offsetsMap(topics, expectedLatestOffset),
				peerNs:   offsetsMap(topics, expectedLatestOffset),
			},
			&ConsumerKey{Key: originNs, Consumers: originConsumers},
			&ConsumerKey{Key: peerNs, Consumers: peerConsumers},
		))

		// ----------------------------------------

		t.Logf("BG-Operation:promote #%d", i)
		timeCounter++
		states = newStates(fmt.Sprintf("2024-01-01T10:%02d:00Z", timeCounter),
			bg.NamespaceVersion{State: bg.StateLegacy, Version: bg.NewVersionMust(ver2Legacy)},
			bg.NamespaceVersion{State: bg.StateActive, Version: bg.NewVersionMust(ver2Active)})
		updatePublishersVersions(states, statePublisherOrigin, statePublisherPeer)

		t.Logf("consume and commit all records by active, legacy consumers")

		assertions.True(parallelConsume(ctx, t, noRecordsTimeout,
			map[string][]string{originNs: {}, peerNs: {}},
			map[string]map[TopicPartition]int64{originNs: {}, peerNs: {}},
			&ConsumerKey{Key: originNs, Consumers: originConsumers},
			&ConsumerKey{Key: peerNs, Consumers: peerConsumers}))

		sentRecordsV2Active, err := sendRecords(t, "producing records for active", ver2Active, records(topics, partitions, msgCounter))
		assertions.NoError(err)
		expectedLatestOffset += len(sentRecordsV2Active) / len(topics) / partitions

		sentRecordsV2Legacy, err := sendRecords(t, "producing records for legacy", ver2Legacy, records(topics, partitions, msgCounter))
		assertions.NoError(err)
		expectedLatestOffset += len(sentRecordsV2Legacy) / len(topics) / partitions

		assertions.True(parallelConsume(ctx, t, pollTimeout,
			map[string][]string{originNs: recordsKafkaAsString(sentRecordsV2Legacy), peerNs: recordsKafkaAsString(sentRecordsV2Active)},
			map[string]map[TopicPartition]int64{originNs: offsetsMap(topics, expectedLatestOffset), peerNs: offsetsMap(topics, expectedLatestOffset)},
			&ConsumerKey{Key: originNs, Consumers: originConsumers},
			&ConsumerKey{Key: peerNs, Consumers: peerConsumers}))

		// ----------------------------------------

		t.Logf("BG-Operation:rollback #%d", i)
		timeCounter++
		states = newStates(fmt.Sprintf("2024-01-01T10:%02d:00Z", timeCounter),
			bg.NamespaceVersion{State: bg.StateActive, Version: bg.NewVersionMust(ver1Active)},
			bg.NamespaceVersion{State: bg.StateCandidate, Version: bg.NewVersionMust(ver1Candidate)})
		updatePublishersVersions(states, statePublisherOrigin, statePublisherPeer)

		t.Logf("consume and commit all records by active, candidate consumers")

		assertions.True(parallelConsume(ctx, t, noRecordsTimeout,
			map[string][]string{originNs: {}, peerNs: {}},
			map[string]map[TopicPartition]int64{originNs: {}, peerNs: {}},
			&ConsumerKey{Key: originNs, Consumers: originConsumers},
			&ConsumerKey{Key: peerNs, Consumers: peerConsumers}))

		sentRecordsV1Active, err = sendRecords(t, "producing records for active", ver1Active, records(topics, partitions, msgCounter))
		assertions.NoError(err)
		expectedLatestOffset += len(sentRecordsV1Active) / len(topics) / partitions

		sentRecordsV1Candidate, err = sendRecords(t, "producing records for candidate", ver1Candidate, records(topics, partitions, msgCounter))
		assertions.NoError(err)
		expectedLatestOffset += len(sentRecordsV1Candidate) / len(topics) / partitions

		assertions.True(parallelConsume(ctx, t, pollTimeout,
			map[string][]string{originNs: recordsKafkaAsString(sentRecordsV1Active), peerNs: recordsKafkaAsString(sentRecordsV1Candidate)},
			map[string]map[TopicPartition]int64{originNs: offsetsMap(topics, expectedLatestOffset), peerNs: offsetsMap(topics, expectedLatestOffset)},
			&ConsumerKey{Key: originNs, Consumers: originConsumers},
			&ConsumerKey{Key: peerNs, Consumers: peerConsumers}))

		// ----------------------------------------

		t.Logf("BG-Operation:promote #%d", i+1)
		timeCounter++
		states = newStates(fmt.Sprintf("2024-01-01T10:%02d:00Z", timeCounter),
			bg.NamespaceVersion{State: bg.StateLegacy, Version: bg.NewVersionMust(ver2Legacy)},
			bg.NamespaceVersion{State: bg.StateActive, Version: bg.NewVersionMust(ver2Active)})
		updatePublishersVersions(states, statePublisherOrigin, statePublisherPeer)

		t.Logf("consume and commit all records by active, legacy consumers")

		assertions.True(parallelConsume(ctx, t, noRecordsTimeout,
			map[string][]string{originNs: {}, peerNs: {}},
			map[string]map[TopicPartition]int64{originNs: {}, peerNs: {}},
			&ConsumerKey{Key: originNs, Consumers: originConsumers},
			&ConsumerKey{Key: peerNs, Consumers: peerConsumers}))

		sentRecordsV2Active, err = sendRecords(t, "producing records for active", ver2Active, records(topics, partitions, msgCounter))
		assertions.NoError(err)
		expectedLatestOffset += len(sentRecordsV2Active) / len(topics) / partitions

		sentRecordsV2Legacy, err = sendRecords(t, "producing records for legacy", ver2Legacy, records(topics, partitions, msgCounter))
		assertions.NoError(err)
		expectedLatestOffset += len(sentRecordsV2Legacy) / len(topics) / partitions

		assertions.True(parallelConsume(ctx, t, pollTimeout,
			map[string][]string{originNs: recordsKafkaAsString(sentRecordsV2Legacy), peerNs: recordsKafkaAsString(sentRecordsV2Active)},
			map[string]map[TopicPartition]int64{originNs: offsetsMap(topics, expectedLatestOffset), peerNs: offsetsMap(topics, expectedLatestOffset)},
			&ConsumerKey{Key: originNs, Consumers: originConsumers},
			&ConsumerKey{Key: peerNs, Consumers: peerConsumers}))

		// ----------------------------------------

		t.Logf("BG-Operation:commit #%d", i)
		timeCounter++
		states = newStates(fmt.Sprintf("2024-01-01T10:%02d:00Z", timeCounter),
			bg.NamespaceVersion{State: bg.StateIdle},
			bg.NamespaceVersion{State: bg.StateActive, Version: bg.NewVersionMust(ver2Active)})
		updatePublishersVersions(states, statePublisherOrigin, statePublisherPeer)

		t.Logf("consume and commit all records by active consumer")

		assertions.True(parallelConsume(ctx, t, noRecordsTimeout,
			map[string][]string{peerNs: {}},
			map[string]map[TopicPartition]int64{peerNs: {}},
			&ConsumerKey{Key: peerNs, Consumers: peerConsumers}))

		sentRecords, err = sendRecords(t, "producing records for active", "", records(topics, partitions, msgCounter))
		assertions.NoError(err)
		expectedLatestOffset += len(sentRecords) / len(topics) / partitions

		assertions.True(parallelConsume(ctx, t, pollTimeout,
			map[string][]string{peerNs: recordsKafkaAsString(sentRecords)},
			map[string]map[TopicPartition]int64{peerNs: offsetsMap(topics, expectedLatestOffset)},
			&ConsumerKey{Key: peerNs, Consumers: peerConsumers}))

		// ----------------------------------------
		// ----------------------------------------

		t.Logf("BG-Operation:warmup #%d", i+1)
		timeCounter++
		states = newStates(fmt.Sprintf("2024-01-01T10:%02d:00Z", timeCounter),
			bg.NamespaceVersion{State: bg.StateLegacy, Version: bg.NewVersionMust(ver3Candidate)},
			bg.NamespaceVersion{State: bg.StateActive, Version: bg.NewVersionMust(ver3Active)})
		updatePublishersVersions(states, statePublisherOrigin, statePublisherPeer)

		t.Logf("consume and commit all records by active, candidate consumers")

		assertions.True(parallelConsume(ctx, t, noRecordsTimeout,
			map[string][]string{originNs: {}, peerNs: {}},
			map[string]map[TopicPartition]int64{originNs: {}, peerNs: {}},
			&ConsumerKey{Key: originNs, Consumers: originConsumers},
			&ConsumerKey{Key: peerNs, Consumers: peerConsumers}))

		sentRecordsV3Active, err := sendRecords(t, "producing records for active", ver3Active, records(topics, partitions, msgCounter))
		assertions.NoError(err)
		expectedLatestOffset += len(sentRecordsV3Active) / len(topics) / partitions

		sentRecordsV3Candidate, err := sendRecords(t, "producing records for legacy", ver3Candidate, records(topics, partitions, msgCounter))
		assertions.NoError(err)
		expectedLatestOffset += len(sentRecordsV3Candidate) / len(topics) / partitions

		assertions.True(parallelConsume(ctx, t, pollTimeout,
			map[string][]string{originNs: recordsKafkaAsString(sentRecordsV3Candidate), peerNs: recordsKafkaAsString(sentRecordsV3Active)},
			map[string]map[TopicPartition]int64{originNs: offsetsMap(topics, expectedLatestOffset), peerNs: offsetsMap(topics, expectedLatestOffset)},
			&ConsumerKey{Key: originNs, Consumers: originConsumers},
			&ConsumerKey{Key: peerNs, Consumers: peerConsumers}))

		// ----------------------------------------

		t.Logf("BG-Operation:promote #%d", i+2)
		timeCounter++
		states = newStates(fmt.Sprintf("2024-01-01T10:%02d:00Z", timeCounter),
			bg.NamespaceVersion{State: bg.StateActive, Version: bg.NewVersionMust(ver4Active)},
			bg.NamespaceVersion{State: bg.StateLegacy, Version: bg.NewVersionMust(ver4Legacy)})
		updatePublishersVersions(states, statePublisherOrigin, statePublisherPeer)

		t.Logf("consume and commit all records by active, legacy consumers")

		assertions.True(parallelConsume(ctx, t, noRecordsTimeout,
			map[string][]string{originNs: {}, peerNs: {}},
			map[string]map[TopicPartition]int64{originNs: {}, peerNs: {}},
			&ConsumerKey{Key: originNs, Consumers: originConsumers},
			&ConsumerKey{Key: peerNs, Consumers: peerConsumers}))

		sentRecordsV4Active, err := sendRecords(t, "producing records for active", ver4Active, records(topics, partitions, msgCounter))
		assertions.NoError(err)
		expectedLatestOffset += len(sentRecordsV4Active) / len(topics) / partitions

		sentRecordsV4Legacy, err := sendRecords(t, "producing records for legacy", ver4Legacy, records(topics, partitions, msgCounter))
		assertions.NoError(err)
		expectedLatestOffset += len(sentRecordsV4Legacy) / len(topics) / partitions

		assertions.True(parallelConsume(ctx, t, pollTimeout,
			map[string][]string{originNs: recordsKafkaAsString(sentRecordsV4Active), peerNs: recordsKafkaAsString(sentRecordsV4Legacy)},
			map[string]map[TopicPartition]int64{originNs: offsetsMap(topics, expectedLatestOffset), peerNs: offsetsMap(topics, expectedLatestOffset)},
			&ConsumerKey{Key: originNs, Consumers: originConsumers},
			&ConsumerKey{Key: peerNs, Consumers: peerConsumers}))

		// ----------------------------------------
		t.Logf("BG-Operation:commit #%d", i+1)
		timeCounter++
		states = newStates(fmt.Sprintf("2024-01-01T10:%02d:00Z", timeCounter),
			bg.NamespaceVersion{State: bg.StateActive, Version: bg.NewVersionMust(ver4Active)},
			bg.NamespaceVersion{State: bg.StateIdle})
		updatePublishersVersions(states, statePublisherOrigin, statePublisherPeer)

		t.Logf("consume and commit all records by active consumer")

		assertions.True(parallelConsume(ctx, t, noRecordsTimeout,
			map[string][]string{originNs: {}},
			map[string]map[TopicPartition]int64{originNs: {}},
			&ConsumerKey{Key: originNs, Consumers: originConsumers}))

		sentRecords, err = sendRecords(t, "producing records for active", "", records(topics, partitions, msgCounter))
		assertions.NoError(err)
		expectedLatestOffset += len(sentRecords) / len(topics) / partitions

		assertions.True(parallelConsume(ctx, t, pollTimeout,
			map[string][]string{originNs: recordsKafkaAsString(sentRecords)},
			map[string]map[TopicPartition]int64{originNs: offsetsMap(topics, expectedLatestOffset)},
			&ConsumerKey{Key: originNs, Consumers: originConsumers}))
	}
}

type KafkaContainerKraftCluster struct {
	Version           string
	BrokersNum        int
	ReplicationFactor int
	BrokerInstances   []testcontainers.Container
}

func setTestDocker(t *testing.T) {
	if testDockerUrl := os.Getenv("TEST_DOCKER_URL"); testDockerUrl != "" {
		err := os.Setenv("DOCKER_HOST", testDockerUrl)
		if err != nil {
			t.Fatal(err.Error())
		}
		t.Logf("set DOCKER_HOST to value from 'TEST_DOCKER_URL' as '%s'.", testDockerUrl)
	} else {
		t.Logf("TEST_DOCKER_URL is empty")
	}
}

func NewKafkaCluster(ctx context.Context, version string, brokersNum int, replicationFactor int) (*KafkaContainerKraftCluster, error) {
	if brokersNum < 0 {
		panic("brokersNum '" + strconv.Itoa(brokersNum) + "' must be greater than 0")
	}
	if replicationFactor < 0 || replicationFactor > brokersNum {
		panic("replicationFactor '" + strconv.Itoa(replicationFactor) + "' must be less than brokersNum and greater than 0")
	}

	controllerQuorumVoters := ""
	for i := 0; i < brokersNum; i++ {
		if controllerQuorumVoters != "" {
			controllerQuorumVoters += ","
		}
		voter := fmt.Sprintf("%d@broker-%d:9094", i, i)
		controllerQuorumVoters += voter
	}
	clusterId := "4L6g3nShT-eMCtK--X86sw"
	var brokers []testcontainers.Container
	wg := &sync.WaitGroup{}
	wg.Add(brokersNum)

	resultChan := make(chan any, brokersNum)
	nw, err := network.New(ctx)
	if err != nil {
		return nil, err
	}
	for i := 0; i < brokersNum; i++ {
		id := i
		go func(ctx context.Context, resultChan chan any) {
			kafkaContainer, err := startKafkaContainer(ctx, version, clusterId, controllerQuorumVoters, id, replicationFactor, nw)
			if err != nil {
				resultChan <- err
			} else {
				resultChan <- kafkaContainer
			}
		}(ctx, resultChan)
	}
	timer := time.NewTimer(1 * time.Minute)
	for len(brokers) != brokersNum {
		select {
		case <-timer.C:
			return nil, errors.New("timed out waiting for all brokers to start up")
		case brokerContainerOrErr := <-resultChan:
			switch r := brokerContainerOrErr.(type) {
			case testcontainers.Container:
				brokers = append(brokers, r)
			case error:
				return nil, r
			}
		}
	}
	timer.Stop()
	retry := true
	for retry {
		_, reader, err := brokers[0].Exec(ctx, []string{"sh", "-c", "kafka-metadata-shell --snapshot /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log ls /brokers | wc -l"})
		if err != nil {
			return nil, err
		}
		if b, err := io.ReadAll(reader); err != nil {
			return nil, err
		} else {
			stdout := strings.ReplaceAll(string(b), "\n", "")
			retry = !strings.Contains(stdout, fmt.Sprintf("%d", brokersNum))
		}
	}
	return &KafkaContainerKraftCluster{
		Version:           version,
		BrokersNum:        brokersNum,
		ReplicationFactor: replicationFactor,
		BrokerInstances:   brokers,
	}, nil
}

func startKafkaContainer(ctx context.Context, version string, clusterId, voters string, brokerId int, replicationFactor int, nw *testcontainers.DockerNetwork) (testcontainers.Container, error) {
	starterScript := "/usr/sbin/testcontainers_start.sh"
	starterScriptContent := `#!/bin/bash
export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%s:%d,BROKER://%s:9092
/etc/confluent/docker/run
`
	name := fmt.Sprintf("broker-%d", brokerId)
	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:" + version,
		ExposedPorts: []string{string(publicPort)},
		//Name:           name,
		Networks:       []string{nw.Name},
		NetworkAliases: map[string][]string{nw.Name: {name}},
		Env: map[string]string{
			"CLUSTER_ID":                                     clusterId,
			"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094",
			"KAFKA_REST_BOOTSTRAP_SERVERS":                   "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 voters,
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
			"KAFKA_BROKER_ID":                                fmt.Sprintf("%d", brokerId),
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         fmt.Sprintf("%d", replicationFactor),
			"KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS":             "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": fmt.Sprintf("%d", replicationFactor),
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_LOG_FLUSH_INTERVAL_MESSAGES":              fmt.Sprintf("%d", math.MaxInt),
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_NODE_ID":                                  fmt.Sprintf("%d", brokerId),
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
		},
		Entrypoint: []string{"sh"},
		// this CMD will wait for the starter script to be copied into the container and then execute it
		Cmd: []string{"-c", "while [ ! -f " + starterScript + " ]; do sleep 0.1; done; bash " + starterScript},
		LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
			{
				PostStarts: []testcontainers.ContainerHook{
					// 1. copy the starter script into the container
					func(ctx context.Context, c testcontainers.Container) error {
						host, err := c.Host(ctx)
						if err != nil {
							return err
						}
						port, err := c.MappedPort(ctx, publicPort)
						if err != nil {
							return err
						}
						scriptContent := fmt.Sprintf(starterScriptContent, host, int(port.Num()), host)
						return c.CopyToContainer(ctx, []byte(scriptContent), starterScript, 0o755)
					},
					// 2. wait for the Kafka server to be ready
					func(ctx context.Context, c testcontainers.Container) error {
						return wait.ForLog(".*Transitioning from RECOVERY to RUNNING.*").AsRegexp().WaitUntilReady(ctx, c)
					},
				},
			},
		},
	}
	genericContainerReq := testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true}
	return testcontainers.GenericContainer(ctx, genericContainerReq)
}

func (cluster *KafkaContainerKraftCluster) Brokers(ctx context.Context) ([]string, error) {
	var brokers []string
	for _, broker := range cluster.BrokerInstances {
		host, err := broker.Host(ctx)
		if err != nil {
			return nil, err
		}
		port, err := broker.MappedPort(ctx, publicPort)
		if err != nil {
			return nil, err
		}
		instanceBrokers := []string{fmt.Sprintf("%s:%d", host, int(port.Num()))}
		brokers = append(brokers, instanceBrokers...)
	}
	return brokers, nil
}

func (cluster *KafkaContainerKraftCluster) Stop(ctx context.Context) {
	for _, broker := range cluster.BrokerInstances {
		broker.Terminate(ctx)
	}
}

type States struct {
	Origin bg.BlueGreenState
	Peer   *bg.BlueGreenState
}

func newStates(updateTimeStr string, origin bg.NamespaceVersion, peer ...bg.NamespaceVersion) States {
	var sibling *bg.NamespaceVersion
	var peerBgState *bg.BlueGreenState
	current := bg.NamespaceVersion{
		Namespace: originNs,
		Version:   origin.Version,
		State:     origin.State,
	}
	updateTime, err := time.Parse("2006-01-02T15:04:05Z", updateTimeStr)
	if len(peer) > 0 {
		sibling = &bg.NamespaceVersion{
			Namespace: peerNs,
			Version:   peer[0].Version,
			State:     peer[0].State,
		}
		peerBgState = &bg.BlueGreenState{
			Current:    *sibling,
			Sibling:    &current,
			UpdateTime: updateTime,
		}
	}
	if err != nil {
		panic(err)
	}
	originBgState := bg.BlueGreenState{
		Current:    current,
		Sibling:    sibling,
		UpdateTime: updateTime,
	}
	return States{
		Origin: originBgState,
		Peer:   peerBgState,
	}
}

func sendRecords(t *testing.T, logMessage string, version string, messages []kafka.Message) ([]kafka.Message, error) {
	t.Logf("%s, version: %s", logMessage, version)
	writers := map[string]*kafka.Writer{}
	for _, m := range messages {
		if w := writers[m.Topic]; w == nil {
			w = createKafkaWriter(topicAddress)
			w.Topic = ""
			writers[m.Topic] = w
		}
	}
	var sentMsgs []kafka.Message
	for _, msg := range messages {
		if version != "" {
			msg.Headers = append(msg.Headers, kafka.Header{Key: xversion.X_VERSION_HEADER_NAME, Value: []byte(version)})
		}
		var hdrs []string
		for _, h := range msg.Headers {
			hdrs = append(hdrs, fmt.Sprintf("%s:%s", h.Key, string(h.Value)))
		}
		t.Logf("producing: ProducerRecord(topic=%s, partition=%d, key=%s, value=%s, headers=[%s])",
			msg.Topic, msg.Partition, string(msg.Key), string(msg.Value), strings.Join(hdrs, ","))
		err := writers[msg.Topic].WriteMessages(context.Background(), msg)
		if err != nil {
			return nil, err
		} else {
			sentMsgs = append(sentMsgs, msg)
		}
	}
	return sentMsgs, nil
}

type MsgBalancer struct {
}

// Balance java allows to set partition at the message, so need to implement the same behaviour in go
func (b *MsgBalancer) Balance(msg kafka.Message, partitions ...int) int {
	partition := msg.Partition
	if partition == -1 {
		partition = 0
	}
	return partition
}

func createKafkaWriter(topicAddress kafkaModel.TopicAddress) *kafka.Writer {
	w, err := maasKafkaGo.NewWriter(topicAddress, maasKafkaGo.WriterOptions{
		AlterTransport: func(transport *kafka.Transport) (*kafka.Transport, error) {
			transport.ClientID = "test-kafka-producer"
			return transport, nil
		}})
	if err != nil {
		panic(err)
	}
	w.Balancer = &MsgBalancer{}
	return w
}

func consumeAndCommit(ctx context.Context, timeout time.Duration, key string, consumer *bgKafka.BgConsumer, result *consumeResult) error {
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	check := true
	defer func() {
		cancelFunc()
		check = false
	}()
	errChan := make(chan error, 1)
	readyChan := make(chan struct{}, 1)
	go func(ctx context.Context) {
		for {
			record, err := consumer.Poll(ctx, timeout)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					result.saveRecords(key)
					if record != nil {
						panic("non nil record with err")
					}
					errChan <- err
					return
				} else {
					errChan <- err
					return
				}
			}
			if record != nil {
				err = consumer.Commit(ctx, record.Marker)
				if err != nil {
					errChan <- err
					return
				} else {
					result.saveRecords(key, *record)
				}
			}
		}
	}(cancelCtx)
	go func(check bool) {
		ready := false
		for check && !ready {
			ready = waitWG(100*time.Millisecond, result.wg)
		}
		if ready {
			readyChan <- struct{}{}
		}
	}(check)
	select {
	case err := <-errChan:
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil
		} else {
			return err
		}
	case <-readyChan:
		return nil
	}
}

type ConsumerKey struct {
	Key       string
	Consumers map[string]*bgKafka.BgConsumer // podId to consumer map
}

type TopicPartition struct {
	Topic     string
	Partition int
}

type consumeResult struct {
	expectedRecords map[string][]string
	expectedOffsets map[string]map[TopicPartition]int64
	receivedRecords map[string][]string
	receivedOffsets map[string]map[TopicPartition]int64
	wg              *sync.WaitGroup
	ready           bool
	mutex           *sync.Mutex
}

func NewConsumeResult(expectedRecords map[string][]string, expectedOffsets map[string]map[TopicPartition]int64) *consumeResult {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	for k, v := range expectedRecords {
		sort.Strings(v)
		expectedRecords[k] = v
	}
	return &consumeResult{
		expectedRecords: expectedRecords,
		expectedOffsets: expectedOffsets,
		receivedRecords: map[string][]string{},
		receivedOffsets: map[string]map[TopicPartition]int64{},
		wg:              wg,
		ready:           false,
		mutex:           &sync.Mutex{},
	}
}

func parallelConsume(ctx context.Context, t *testing.T, timeout time.Duration,
	expectedRecords map[string][]string,
	expectedOffsets map[string]map[TopicPartition]int64,
	consumers ...*ConsumerKey) bool {

	result := NewConsumeResult(expectedRecords, expectedOffsets)
	attempts := 1
	attempt := 0
	ready := false
	for attempt < attempts {
		attempt++
		t.Logf("parallel consume, attempt #%d", attempt)
		consumeTimeout := time.Duration(timeout.Nanoseconds() / int64(attempts))
		wg := &sync.WaitGroup{}
		for _, consumer := range consumers {
			for podId, c := range consumer.Consumers {
				wg.Add(1)
				cnsmr := c
				podId := podId
				podCtx, err := ctxmanager.SetContextObject(ctx, xrequestid.X_REQUEST_ID_COTEXT_NAME,
					xrequestid.NewXRequestIdContextObject(podId))
				if err != nil {
					panic(err)
				}
				go func(podCtx context.Context, consumer *ConsumerKey) {
					err := consumeAndCommit(podCtx, consumeTimeout, consumer.Key, cnsmr, result)
					if err != nil {
						panic(err)
					}
					wg.Done()
				}(podCtx, consumer)
			}
		}
		// wait until all consumer's poll method finished until we try next attempt
		allDone := waitWG(consumeTimeout, wg)
		for !allDone {
			allDone = waitWG(100*time.Millisecond, wg)
			ready = waitWG(100*time.Millisecond, result.wg)
		}
		if ready {
			return ready
		}
	}
	ready = waitWG(100*time.Millisecond, result.wg)
	if !ready {
		t.Logf(`
Failed to receive expected records.
expectedRecordsMap:%v
== ? (%v)
receivedRecordsMap:%v
-
expectedOffsets:%v
== ? (%v)
receivedOffsets:%v
`,
			result.expectedRecords, reflect.DeepEqual(result.expectedRecords, result.receivedRecords), result.receivedRecords,
			result.expectedOffsets, reflect.DeepEqual(result.expectedOffsets, result.receivedOffsets), result.receivedOffsets)
	}
	return ready
}

func (cr *consumeResult) saveRecords(key string, records ...bgKafka.Record) {
	cr.mutex.Lock()
	defer cr.mutex.Unlock()
	resultRecords := cr.receivedRecords[key]
	resultOffsets := cr.receivedOffsets[key]
	if resultRecords == nil {
		resultRecords = []string{}
	}
	if resultOffsets == nil {
		resultOffsets = map[TopicPartition]int64{}
	}
	var recordsAsStrings []string
	for _, r := range records {
		if r.Message != nil {
			messageAsString := recordBgKafkaAsString(r.Message)
			// if records already contains this message - ignore it (current behaviour of concurrent consumer group modification assumes that some pods may re-read the same messages)
			skip := false
			for _, existing := range resultRecords {
				if messageAsString == existing {
					skip = true
					break
				}
			}
			if !skip {
				recordsAsStrings = append(recordsAsStrings, messageAsString)
			}
		}
		if r.Marker != nil {
			tp := r.Marker.TopicPartition
			resultOffsets[TopicPartition{Topic: tp.Topic, Partition: tp.Partition}] = r.Marker.OffsetAndMeta.Offset + 1
		}
	}
	resultRecords = append(resultRecords, recordsAsStrings...)
	sort.Strings(resultRecords)
	cr.receivedRecords[key] = resultRecords
	cr.receivedOffsets[key] = resultOffsets

	if reflect.DeepEqual(cr.expectedRecords, cr.receivedRecords) && reflect.DeepEqual(cr.expectedOffsets, cr.receivedOffsets) {
		if !cr.ready {
			cr.ready = true
			cr.wg.Done()
		}
	}
}

func records(topics []string, partitions int, msgCounter *atomic.Int32) (result []kafka.Message) {
	for _, topic := range topics {
		for p := 0; p < partitions; p++ {
			msgCounter.Add(1)
			result = append(result, kafka.Message{
				Topic: topic, Partition: p,
				Key:   []byte(buildKey(msgCounter.Load())),
				Value: []byte(buildValue(topic, msgCounter.Load())),
			})
		}
	}
	return
}

func offsetsMap(topics []string, expectedLatestOffset int) map[TopicPartition]int64 {
	result := map[TopicPartition]int64{}
	for _, topic := range topics {
		for p := 0; p < partitions; p++ {
			result[TopicPartition{Topic: topic, Partition: p}] = int64(expectedLatestOffset)
		}
	}
	return result
}

func recordsKafkaAsString(msgs []kafka.Message) (result []string) {
	for _, msg := range msgs {
		result = append(result, recordKafkaAsString(msg))
	}
	return
}

func recordKafkaAsString(msg kafka.Message) string {
	return recordAsString(msg.Topic, msg.Partition, string(msg.Key), string(msg.Value))
}

func recordBgKafkaAsString(msg bgKafka.Message) string {
	return recordAsString(msg.Topic(), msg.Partition(), string(msg.Key()), string(msg.Value()))
}

func recordAsString(topic string, partition int, key string, value string) string {
	return fmt.Sprintf("%s/%d/%s/%s", topic, partition, key, value)
}

func buildValue(topic string, msgId int32) string {
	return fmt.Sprintf("%s-%04d", topic, msgId)
}

func buildKey(msgId int32) string {
	return fmt.Sprintf("%04d", msgId)
}

func updatePublishersVersions(states States, origin *bg.InMemoryBlueGreenStatePublisher, peer ...*bg.InMemoryBlueGreenStatePublisher) {
	origin.SetState(states.Origin)
	if len(peer) > 0 {
		peer[0].SetState(*states.Peer)
	}
}

func waitWG(timeout time.Duration, groups ...*sync.WaitGroup) bool {
	finalWg := &sync.WaitGroup{}
	finalWg.Add(len(groups))
	c := make(chan struct{})
	for _, wg := range groups {
		go func(wg *sync.WaitGroup) {
			wg.Wait()
			finalWg.Done()
		}(wg)
	}
	go func() {
		finalWg.Wait()
		c <- struct{}{}
	}()

	timer := time.NewTimer(timeout)
	select {
	case <-c:
		timer.Stop()
		return true
	case <-timer.C:
		return false
	}
}
