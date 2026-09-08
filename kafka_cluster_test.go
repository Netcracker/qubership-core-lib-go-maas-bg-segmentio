package blue_green_segmentio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	mobynet "github.com/moby/moby/api/types/network"
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
	// shutdownTimeout is how long a broker is given to stop before it is killed.
	shutdownTimeout = 10 * time.Second
)

// kafkaTestCluster is a Kafka cluster in KRaft mode, running in Docker.
//
// Each broker is published on a host port fixed at creation, so a broker that is
// stopped and started again comes back at the address it had before, the way a
// real broker does. That port is picked on the machine running the tests, so a
// remote Docker daemon is not supported.
type kafkaTestCluster struct {
	instances []testcontainers.Container
	host      string
	hostPorts []int
	// boots counts how many times each broker has started, so waiting for the
	// ready line after a restart does not match the line from an earlier boot
	boots []int
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

// newKafkaCluster starts brokersNum brokers of the given Confluent image version.
func newKafkaCluster(ctx context.Context, version string, brokersNum, replicationFactor int) (*kafkaTestCluster, error) {
	if brokersNum <= 0 {
		return nil, fmt.Errorf("brokersNum %d must be greater than 0", brokersNum)
	}
	if replicationFactor <= 0 || replicationFactor > brokersNum {
		return nil, fmt.Errorf("replicationFactor %d must be between 1 and brokersNum %d", replicationFactor, brokersNum)
	}

	hostPorts := make([]int, brokersNum)
	voters := make([]string, brokersNum)
	for i := range hostPorts {
		port, err := freePort()
		if err != nil {
			return nil, fmt.Errorf("failed to reserve a host port for broker %d: %w", i, err)
		}
		hostPorts[i] = port
		voters[i] = fmt.Sprintf("%d@broker-%d:9094", i, i)
	}

	nw, err := network.New(ctx)
	if err != nil {
		return nil, err
	}
	instances, err := startBrokers(ctx, version, strings.Join(voters, ","), replicationFactor, hostPorts, nw)
	if err != nil {
		return nil, err
	}
	if err := awaitQuorum(ctx, instances[0], brokersNum); err != nil {
		return nil, err
	}
	host, err := instances[0].Host(ctx)
	if err != nil {
		return nil, err
	}

	boots := make([]int, brokersNum)
	for i := range boots {
		boots[i] = 1
	}
	return &kafkaTestCluster{instances: instances, host: host, hostPorts: hostPorts, boots: boots}, nil
}

// brokers returns the seed list of every broker.
func (cluster *kafkaTestCluster) brokers() []string {
	addresses := make([]string, len(cluster.instances))
	for i := range addresses {
		addresses[i] = cluster.brokerAddress(i)
	}
	return addresses
}

// brokerAddress returns the address of one broker. It does not change when the
// broker is stopped and started again.
func (cluster *kafkaTestCluster) brokerAddress(broker int) string {
	return fmt.Sprintf("%s:%d", cluster.host, cluster.hostPorts[broker])
}

// stopBroker shuts one broker down, leaving its data behind. A partition it led
// has to elect a new leader among the replicas, which is the point of the test.
func (cluster *kafkaTestCluster) stopBroker(ctx context.Context, broker int) error {
	timeout := shutdownTimeout
	if err := cluster.instances[broker].Stop(ctx, &timeout); err != nil {
		return fmt.Errorf("failed to stop broker %d: %w", broker, err)
	}
	return nil
}

// startBroker brings a stopped broker back at the same address and with the data
// it had, and returns once it is serving again.
func (cluster *kafkaTestCluster) startBroker(ctx context.Context, broker int) error {
	instance := cluster.instances[broker]
	if err := instance.Start(ctx); err != nil {
		return fmt.Errorf("failed to start broker %d: %w", broker, err)
	}
	cluster.boots[broker]++
	// the ready line from the previous boot is still in the log, so match the one
	// this boot prints
	if err := wait.ForLog(readyLog).AsRegexp().
		WithOccurrence(cluster.boots[broker]).
		WaitUntilReady(ctx, instance); err != nil {
		return fmt.Errorf("broker %d did not come back: %w", broker, err)
	}
	return nil
}

// stop terminates every broker.
func (cluster *kafkaTestCluster) stop(ctx context.Context) {
	for _, broker := range cluster.instances {
		_ = broker.Terminate(ctx)
	}
}

// startBrokers brings the brokers up in parallel: each waits for the quorum, so
// starting them one by one would wait out the whole cluster per broker.
func startBrokers(ctx context.Context, version, voters string, replicationFactor int,
	hostPorts []int, nw *testcontainers.DockerNetwork) ([]testcontainers.Container, error) {
	results := make(chan any, len(hostPorts))
	for id := range hostPorts {
		go func(id int) {
			instance, err := startBrokerContainer(ctx, version, voters, id, replicationFactor, hostPorts[id], nw)
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
	for len(started) != len(hostPorts) {
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

func startBrokerContainer(ctx context.Context, version, voters string, brokerId, replicationFactor, hostPort int,
	nw *testcontainers.DockerNetwork) (testcontainers.Container, error) {
	starterScript := "/usr/sbin/testcontainers_start.sh"
	// the script is written once and survives a restart, which is why the
	// advertised address has to be fixed rather than the mapped port of the moment
	starterScriptContent := `#!/bin/bash
export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%s:%d,BROKER://%s:9092
/etc/confluent/docker/run
`
	name := fmt.Sprintf("broker-%d", brokerId)
	request := testcontainers.ContainerRequest{
		Image:          "confluentinc/cp-kafka:" + version,
		ExposedPorts:   []string{brokerPort + "/tcp"},
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
		// bind the listener to the reserved port instead of letting Docker pick one,
		// so the address survives a restart
		HostConfigModifier: func(hostConfig *container.HostConfig) {
			hostConfig.PortBindings = mobynet.PortMap{
				mobynet.MustParsePort(brokerPort + "/tcp"): []mobynet.PortBinding{
					{HostIP: netip.IPv4Unspecified(), HostPort: strconv.Itoa(hostPort)},
				},
			}
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
						script := fmt.Sprintf(starterScriptContent, host, hostPort, host)
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

// freePort reserves a port by taking it and letting it go, which is as close to
// a reservation as the OS offers.
func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}
