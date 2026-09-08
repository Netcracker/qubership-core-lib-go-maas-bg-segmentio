package blue_green_segmentio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// brokerPort is the listener clients connect to from outside Docker.
	brokerPort = "9093"
	// clusterId is fixed: every broker of one cluster must report the same one.
	clusterId = "4L6g3nShT-eMCtK--X86sw"
	// readyLog is what a broker prints once it is serving.
	readyLog = ".*Transitioning from RECOVERY to RUNNING.*"
	// startupTimeout bounds bringing the whole cluster up.
	startupTimeout = time.Minute
)

// kafkaTestCluster is a Kafka cluster in KRaft mode, running in Docker.
type kafkaTestCluster struct {
	instances []testcontainers.Container
}

// useDockerHostFromEnv points Docker at TEST_DOCKER_URL when it is set.
func useDockerHostFromEnv() string {
	url := os.Getenv("TEST_DOCKER_URL")
	if url == "" {
		return ""
	}
	if err := os.Setenv("DOCKER_HOST", url); err != nil {
		return ""
	}
	return url
}

// loopbackV4 replaces "localhost" with the IPv4 address it may or may not
// resolve to. The published port is bound to IPv4, so a client that resolves
// localhost to ::1 is refused even though the broker is running.
func loopbackV4(host string) string {
	if host == "localhost" {
		return "127.0.0.1"
	}
	return host
}

// newKafkaCluster starts brokersNum brokers of the given Confluent image version.
func newKafkaCluster(ctx context.Context, version string, brokersNum, replicationFactor int) (*kafkaTestCluster, error) {
	if brokersNum <= 0 {
		return nil, fmt.Errorf("brokersNum %d must be greater than 0", brokersNum)
	}
	if replicationFactor <= 0 || replicationFactor > brokersNum {
		return nil, fmt.Errorf("replicationFactor %d must be between 1 and brokersNum %d", replicationFactor, brokersNum)
	}

	voters := make([]string, brokersNum)
	for i := range voters {
		voters[i] = fmt.Sprintf("%d@broker-%d:9094", i, i)
	}

	nw, err := network.New(ctx)
	if err != nil {
		return nil, err
	}

	instances, err := startBrokers(ctx, version, strings.Join(voters, ","), replicationFactor, brokersNum, nw)
	if err != nil {
		return nil, err
	}
	if err := awaitQuorum(ctx, instances[0], brokersNum); err != nil {
		return nil, err
	}
	return &kafkaTestCluster{instances: instances}, nil
}

// brokers returns the seed list of every broker.
func (cluster *kafkaTestCluster) brokers(ctx context.Context) ([]string, error) {
	addresses := make([]string, len(cluster.instances))
	for i, instance := range cluster.instances {
		host, err := instance.Host(ctx)
		if err != nil {
			return nil, err
		}
		port, err := instance.MappedPort(ctx, brokerPort)
		if err != nil {
			return nil, err
		}
		addresses[i] = fmt.Sprintf("%s:%d", loopbackV4(host), port.Num())
	}
	return addresses, nil
}

// stop terminates every broker.
func (cluster *kafkaTestCluster) stop(ctx context.Context) {
	for _, broker := range cluster.instances {
		_ = broker.Terminate(ctx)
	}
}

// startBrokers brings the brokers up in parallel: each waits for the quorum, so
// starting them one by one would wait out the whole cluster per broker.
func startBrokers(ctx context.Context, version, voters string, replicationFactor, brokersNum int,
	nw *testcontainers.DockerNetwork) ([]testcontainers.Container, error) {
	results := make(chan any, brokersNum)
	for id := 0; id < brokersNum; id++ {
		go func(id int) {
			instance, err := startBrokerContainer(ctx, version, voters, id, replicationFactor, nw)
			if err != nil {
				results <- err
				return
			}
			results <- instance
		}(id)
	}

	var started []testcontainers.Container
	timer := time.NewTimer(startupTimeout)
	defer timer.Stop()
	for len(started) != brokersNum {
		select {
		case <-timer.C:
			return nil, errors.New("timed out waiting for all brokers to start up")
		case result := <-results:
			switch r := result.(type) {
			case testcontainers.Container:
				started = append(started, r)
			case error:
				return nil, r
			}
		}
	}
	return started, nil
}

// awaitQuorum waits until every broker has registered in the cluster metadata.
func awaitQuorum(ctx context.Context, broker testcontainers.Container, brokersNum int) error {
	command := []string{"sh", "-c",
		"kafka-metadata-shell --snapshot /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log ls /brokers | wc -l"}
	for {
		_, reader, err := broker.Exec(ctx, command)
		if err != nil {
			return err
		}
		out, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		if strings.Contains(strings.ReplaceAll(string(out), "\n", ""), fmt.Sprintf("%d", brokersNum)) {
			return nil
		}
	}
}

func startBrokerContainer(ctx context.Context, version, voters string, brokerId, replicationFactor int,
	nw *testcontainers.DockerNetwork) (testcontainers.Container, error) {
	starterScript := "/usr/sbin/testcontainers_start.sh"
	// PLAINTEXT is advertised to the tests, which reach the broker through the
	// Docker host. BROKER is the inter-broker listener and has to be advertised
	// as the network alias: inside a container the host address points back at
	// itself, replication never leaves the broker, and no follower joins the ISR.
	starterScriptContent := `#!/bin/bash
export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%s:%d,BROKER://%s:9092
/etc/confluent/docker/run
`
	name := fmt.Sprintf("broker-%d", brokerId)
	request := testcontainers.ContainerRequest{
		Image:          "confluentinc/cp-kafka:" + version,
		ExposedPorts:   []string{brokerPort},
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
		// wait for the starter script to be copied in, then run it
		Cmd: []string{"-c", "while [ ! -f " + starterScript + " ]; do sleep 0.1; done; bash " + starterScript},
		LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
			{
				PostStarts: []testcontainers.ContainerHook{
					func(ctx context.Context, c testcontainers.Container) error {
						host, err := c.Host(ctx)
						if err != nil {
							return err
						}
						port, err := c.MappedPort(ctx, brokerPort)
						if err != nil {
							return err
						}
						script := fmt.Sprintf(starterScriptContent, loopbackV4(host), port.Num(), name)
						return c.CopyToContainer(ctx, []byte(script), starterScript, 0o755)
					},
					func(ctx context.Context, c testcontainers.Container) error {
						return wait.ForLog(readyLog).AsRegexp().WaitUntilReady(ctx, c)
					},
				},
			},
		},
	}
	return testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{ContainerRequest: request, Started: true})
}
