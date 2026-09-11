package blue_green_segmentio

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	mobynet "github.com/moby/moby/api/types/network"
	"github.com/segmentio/kafka-go"
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
	// starterScript sets the advertised addresses before handing over to the image.
	starterScript = "/usr/sbin/testcontainers_start.sh"
)

// kafkaTestCluster is a Kafka cluster in KRaft mode, running in Docker. Broker
// ports are fixed at creation, so a restarted broker keeps its address. They are
// picked on the machine running the tests, so a remote daemon is not supported.
type kafkaTestCluster struct {
	instances []testcontainers.Container
	host      string
	hostPorts []int
	// boots counts the starts of each broker, so a wait for the ready line after
	// a restart does not match the line from an earlier boot
	boots []int
	// running tells which brokers are up
	running []bool
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
// The nodes carry both roles, so stopping one of three keeps the quorum.
func newKafkaCluster(ctx context.Context, version string, brokersNum, replicationFactor int) (*kafkaTestCluster, error) {
	if brokersNum <= 0 {
		return nil, fmt.Errorf("brokersNum %d must be greater than 0", brokersNum)
	}
	if replicationFactor <= 0 || replicationFactor > brokersNum {
		return nil, fmt.Errorf("replicationFactor %d must be between 1 and brokersNum %d", replicationFactor, brokersNum)
	}

	hostPorts, err := reservePorts(brokersNum)
	if err != nil {
		return nil, err
	}
	host, err := dockerHost(ctx)
	if err != nil {
		return nil, err
	}
	voters := make([]string, brokersNum)
	for i := range voters {
		voters[i] = fmt.Sprintf("%d@broker-%d:9094", i, i)
	}

	nw, err := network.New(ctx)
	if err != nil {
		return nil, err
	}

	instances, err := startBrokers(ctx, version, strings.Join(voters, ","), replicationFactor, host, hostPorts, nw)
	if err != nil {
		return nil, err
	}

	boots := make([]int, brokersNum)
	running := make([]bool, brokersNum)
	for i := range boots {
		boots[i] = 1
		running[i] = true
	}
	cluster := &kafkaTestCluster{
		instances: instances, host: host, hostPorts: hostPorts, boots: boots, running: running,
	}
	if err := cluster.awaitBrokersRegistered(ctx, brokersNum); err != nil {
		return nil, err
	}
	return cluster, nil
}

// dockerHost is the address the tests reach a published port on.
func dockerHost(ctx context.Context) (string, error) {
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		return "", err
	}
	defer provider.Close()

	host, err := provider.DaemonHost(ctx)
	if err != nil {
		return "", err
	}
	return loopbackV4(host), nil
}

// brokers returns the seed list of every broker.
func (cluster *kafkaTestCluster) brokers() []string {
	addresses := make([]string, len(cluster.instances))
	for i := range addresses {
		addresses[i] = cluster.brokerAddress(i)
	}
	return addresses
}

// brokerAddress returns the address of one broker, which a restart does not
// change.
func (cluster *kafkaTestCluster) brokerAddress(broker int) string {
	return fmt.Sprintf("%s:%d", cluster.host, cluster.hostPorts[broker])
}

// stopBroker shuts one broker down, leaving its data behind.
func (cluster *kafkaTestCluster) stopBroker(ctx context.Context, broker int) error {
	timeout := shutdownTimeout
	if err := cluster.instances[broker].Stop(ctx, &timeout); err != nil {
		return fmt.Errorf("failed to stop broker %d: %w", broker, err)
	}
	cluster.running[broker] = false
	return nil
}

// startBroker brings a stopped broker back and returns once it is serving again.
func (cluster *kafkaTestCluster) startBroker(ctx context.Context, broker int) error {
	instance := cluster.instances[broker]
	if err := instance.Start(ctx); err != nil {
		return fmt.Errorf("failed to start broker %d: %w", broker, err)
	}
	cluster.boots[broker]++
	// the ready line from the previous boot is still in the log
	if err := wait.ForLog(readyLog).AsRegexp().
		WithOccurrence(cluster.boots[broker]).
		WaitUntilReady(ctx, instance); err != nil {
		return fmt.Errorf("broker %d did not come back: %w", broker, err)
	}
	cluster.running[broker] = true
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
	host string, hostPorts []int, nw *testcontainers.DockerNetwork) ([]testcontainers.Container, error) {
	type startResult struct {
		id       int
		instance testcontainers.Container
		err      error
	}
	results := make(chan startResult, len(hostPorts))
	for id := range hostPorts {
		go func(id int) {
			instance, err := startBrokerContainer(ctx, version, voters, id, replicationFactor, host, hostPorts[id], nw)
			results <- startResult{id: id, instance: instance, err: err}
		}(id)
	}

	// indexed by broker id, not by the order the brokers come up in
	started := make([]testcontainers.Container, len(hostPorts))
	timer := time.NewTimer(startupTimeout)
	defer timer.Stop()
	for range hostPorts {
		select {
		case <-timer.C:
			return nil, errors.New("timed out waiting for all brokers to start up")
		case result := <-results:
			if result.err != nil {
				return nil, result.err
			}
			started[result.id] = result.instance
		}
	}
	return started, nil
}

// awaitBrokersRegistered waits until the cluster reports every broker. A serving
// broker is not necessarily registered yet, and a topic created before that
// lands on fewer replicas than it asks for.
func (cluster *kafkaTestCluster) awaitBrokersRegistered(ctx context.Context, brokersNum int) error {
	deadline := time.Now().Add(startupTimeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		conn, err := cluster.dialAny(ctx)
		if err == nil {
			registered, err := conn.Brokers()
			conn.Close()
			if err == nil && len(registered) == brokersNum {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("the cluster did not report %d brokers within %s", brokersNum, startupTimeout)
}

func startBrokerContainer(ctx context.Context, version, voters string, brokerId, replicationFactor int,
	host string, hostPort int, nw *testcontainers.DockerNetwork) (testcontainers.Container, error) {
	name := fmt.Sprintf("broker-%d", brokerId)
	// PLAINTEXT is advertised to the tests, which reach the broker through the
	// Docker host. BROKER carries replication and has to be advertised as the
	// network alias: the host address points a container back at itself.
	//
	// exec, so the JVM is PID 1 and a stop reaches it.
	script := fmt.Sprintf(`#!/bin/bash
export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%s:%d,BROKER://%s:9092
exec /etc/confluent/docker/run
`, host, hostPort, name)

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
			"KAFKA_BROKER_ID":                                strconv.Itoa(brokerId),
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         strconv.Itoa(replicationFactor),
			"KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS":             "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": strconv.Itoa(replicationFactor),
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_LOG_FLUSH_INTERVAL_MESSAGES":              strconv.Itoa(math.MaxInt),
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_NODE_ID":                                  strconv.Itoa(brokerId),
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			// how long a lost broker stays registered, and how long a replica
			// that stopped fetching counts as in sync
			"KAFKA_BROKER_SESSION_TIMEOUT_MS":    "6000",
			"KAFKA_BROKER_HEARTBEAT_INTERVAL_MS": "1000",
			"KAFKA_REPLICA_LAG_TIME_MAX_MS":      "5000",
		},
		// bind the listener to the reserved port, so the address survives a restart
		HostConfigModifier: func(hostConfig *container.HostConfig) {
			hostConfig.PortBindings = mobynet.PortMap{
				mobynet.MustParsePort(brokerPort + "/tcp"): []mobynet.PortBinding{
					{HostIP: netip.IPv4Unspecified(), HostPort: strconv.Itoa(hostPort)},
				},
			}
		},
		// written once, at creation, and survives a restart
		Files: []testcontainers.ContainerFile{{
			Reader:            strings.NewReader(script),
			ContainerFilePath: starterScript,
			FileMode:          0o755,
		}},
		Entrypoint: []string{"sh"},
		Cmd:        []string{"-c", "exec bash " + starterScript},
		WaitingFor: wait.ForLog(readyLog).AsRegexp(),
	}
	return testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{ContainerRequest: request, Started: true})
}

// reservePorts picks count distinct ports, holding every listener open until all
// of them are chosen: released one at a time, a port can be handed out twice.
func reservePorts(count int) ([]int, error) {
	listeners := make([]net.Listener, 0, count)
	defer func() {
		for _, listener := range listeners {
			listener.Close()
		}
	}()

	ports := make([]int, count)
	for i := range ports {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("failed to reserve a host port for broker %d: %w", i, err)
		}
		listeners = append(listeners, listener)
		ports[i] = listener.Addr().(*net.TCPAddr).Port
	}
	return ports, nil
}

// dialAny connects to whichever broker is up.
func (cluster *kafkaTestCluster) dialAny(ctx context.Context) (*kafka.Conn, error) {
	var err error
	for broker := range cluster.instances {
		if !cluster.running[broker] {
			continue
		}
		var conn *kafka.Conn
		if conn, err = kafka.DialContext(ctx, "tcp", cluster.brokerAddress(broker)); err == nil {
			return conn, nil
		}
	}
	return nil, fmt.Errorf("no broker answered: %w", err)
}

// awaitLeaders waits until every running broker reports every partition with a
// leader and a full in-sync set. One broker knowing the topic is not enough: a
// client picks a broker of its own, and one that has not caught up answers
// UNKNOWN_TOPIC_OR_PARTITION.
func (cluster *kafkaTestCluster) awaitLeaders(ctx context.Context, topic string, partitions int) error {
	deadline := time.Now().Add(startupTimeout)
	for time.Now().Before(deadline) {
		if cluster.everyBrokerKnows(ctx, topic, partitions) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("not every broker reports a leader and a full in-sync set for the %d partitions of %s, last seen:\n%s",
		partitions, topic, cluster.describe(ctx, topic))
}

func (cluster *kafkaTestCluster) everyBrokerKnows(ctx context.Context, topic string, partitions int) bool {
	for broker := range cluster.instances {
		if !cluster.running[broker] {
			continue
		}
		conn, err := kafka.DialContext(ctx, "tcp", cluster.brokerAddress(broker))
		if err != nil {
			return false
		}
		found, err := conn.ReadPartitions(topic)
		conn.Close()
		if err != nil || len(found) != partitions {
			return false
		}
		for _, partition := range found {
			if partition.Leader.ID < 0 || len(partition.Isr) < cluster.liveReplicas(partition) {
				return false
			}
		}
	}
	return true
}

// liveReplicas counts the replicas of one partition whose broker is still up.
// Brokers outside the replica set do not count, however many are running.
func (cluster *kafkaTestCluster) liveReplicas(partition kafka.Partition) int {
	live := 0
	for _, replica := range partition.Replicas {
		if replica.ID >= 0 && replica.ID < len(cluster.running) && cluster.running[replica.ID] {
			live++
		}
	}
	return live
}

// partitionLeader reports the broker leading one partition. Broker ids match the
// cluster indexes, because KAFKA_BROKER_ID is the index.
func (cluster *kafkaTestCluster) partitionLeader(ctx context.Context, topic string, partition int) (int, error) {
	conn, err := cluster.dialAny(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return -1, err
	}
	for _, found := range partitions {
		if found.ID == partition && found.Leader.ID >= 0 {
			return found.Leader.ID, nil
		}
	}
	return -1, fmt.Errorf("partition %d of %s has no leader", partition, topic)
}

// describe reports leader, replicas and in-sync replicas per partition, as each
// running broker sees them.
func (cluster *kafkaTestCluster) describe(ctx context.Context, topic string) string {
	var report strings.Builder
	for broker := range cluster.instances {
		if !cluster.running[broker] {
			fmt.Fprintf(&report, "broker %d: down\n", broker)
			continue
		}
		conn, err := kafka.DialContext(ctx, "tcp", cluster.brokerAddress(broker))
		if err != nil {
			fmt.Fprintf(&report, "broker %d: not reachable: %v\n", broker, err)
			continue
		}
		found, err := conn.ReadPartitions(topic)
		conn.Close()
		if err != nil {
			fmt.Fprintf(&report, "broker %d: %v\n", broker, err)
			continue
		}
		for _, partition := range found {
			fmt.Fprintf(&report, "broker %d sees partition %d: leader %d, replicas %v, isr %v\n",
				broker, partition.ID, partition.Leader.ID, brokerIds(partition.Replicas), brokerIds(partition.Isr))
		}
	}
	return report.String()
}

func brokerIds(brokers []kafka.Broker) []int {
	ids := make([]int, len(brokers))
	for i, broker := range brokers {
		ids[i] = broker.ID
	}
	return ids
}

// loopbackV4 replaces "localhost" with its IPv4 address: the published port is
// bound to IPv4, and a client that resolves localhost to ::1 is refused.
func loopbackV4(host string) string {
	if host == "localhost" {
		return "127.0.0.1"
	}
	return host
}
