//go:build integration

package state

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestMain sets testcontainers configuration that must be in place before any
// container is started. Mirrors the kafka package's TestMain for the same
// reasons (RYUK disabled for Podman compatibility).
func TestMain(m *testing.M) {
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	os.Exit(m.Run())
}

// dockerAvailable probes for a running Docker daemon without importing the
// Docker SDK.
func dockerAvailable() bool {
	cmd := exec.Command("docker", "info")
	return cmd.Run() == nil
}

// TestChangelogProducer_Flush is a full integration test that:
//  1. Spins up a single-broker Kafka cluster via testcontainers.
//  2. Creates a 1-partition compacted topic.
//  3. Constructs a ChangelogProducer and flushes 3 mutations (2 puts, 1 delete/tombstone).
//  4. Consumes partition 0 directly and asserts 3 records with the correct key/value.
//  5. Asserts the tombstone record has a nil value.
func TestChangelogProducer_Flush(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-changelog-cluster"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
		}),
	)
	if err != nil {
		t.Skipf("failed to start Kafka container (Docker may be unavailable or slow): %v", err)
	}
	t.Cleanup(func() { _ = kc.Terminate(ctx) })

	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get broker addresses: %v", err)
	}

	const topic = "state-changelog-test"
	createChangelogTopic(t, ctx, brokers, topic)

	// Build the producer.
	producer, err := NewChangelogProducer(brokers, topic)
	if err != nil {
		t.Fatalf("NewChangelogProducer: %v", err)
	}
	defer producer.Close()

	// Flush 3 mutations pinned to partition 0:
	//   mut0: Put  key="k1" value="v1"
	//   mut1: Put  key="k2" value="v2"
	//   mut2: Del  key="k3" value=nil  (tombstone: IsDelete=true)
	muts := []Mutation{
		{Key: []byte("k1"), Value: []byte("v1"), IsDelete: false},
		{Key: []byte("k2"), Value: []byte("v2"), IsDelete: false},
		{Key: []byte("k3"), Value: nil, IsDelete: true},
	}
	if err := producer.Flush(ctx, 0, muts); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Consume partition 0 from the start and collect exactly 3 records.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	var got []*kgo.Record
	deadline := time.Now().Add(30 * time.Second)
	for len(got) < 3 && time.Now().Before(deadline) {
		fs := consumer.PollFetches(ctx)
		if fs.IsClientClosed() {
			break
		}
		fs.EachRecord(func(r *kgo.Record) {
			got = append(got, r)
		})
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 records, got %d", len(got))
	}

	// Verify record 0: key=k1, value=v1, partition=0
	assertRecord(t, got[0], "k1", "v1", 0)
	// Verify record 1: key=k2, value=v2, partition=0
	assertRecord(t, got[1], "k2", "v2", 0)
	// Verify record 2 (tombstone): key=k3, value=nil, partition=0
	assertTombstone(t, got[2], "k3", 0)
}

// assertRecord checks that a consumed kgo.Record matches the expected
// key, value string, and partition.
func assertRecord(t *testing.T, r *kgo.Record, key, value string, partition int32) {
	t.Helper()
	if string(r.Key) != key {
		t.Errorf("record key: got %q, want %q", string(r.Key), key)
	}
	if string(r.Value) != value {
		t.Errorf("record value: got %q, want %q", string(r.Value), value)
	}
	if r.Partition != partition {
		t.Errorf("record partition: got %d, want %d", r.Partition, partition)
	}
}

// assertTombstone checks that a consumed kgo.Record is a tombstone: nil value.
func assertTombstone(t *testing.T, r *kgo.Record, key string, partition int32) {
	t.Helper()
	if string(r.Key) != key {
		t.Errorf("tombstone key: got %q, want %q", string(r.Key), key)
	}
	if r.Value != nil {
		t.Errorf("tombstone value: got %q, want nil", string(r.Value))
	}
	if r.Partition != partition {
		t.Errorf("tombstone partition: got %d, want %d", r.Partition, partition)
	}
}

// createChangelogTopic creates a single-partition compacted topic for tests.
func createChangelogTopic(t *testing.T, ctx context.Context, brokers []string, topic string) {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("createChangelogTopic: new kgo client: %v", err)
	}
	defer cl.Close()

	compact := "compact"
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = topic
	rt.NumPartitions = 1
	rt.ReplicationFactor = 1
	cfg := kmsg.NewCreateTopicsRequestTopicConfig()
	cfg.Name = "cleanup.policy"
	cfg.Value = &compact
	rt.Configs = append(rt.Configs, cfg)
	req.Topics = append(req.Topics, rt)

	resp, err := cl.Request(ctx, req)
	if err != nil {
		t.Fatalf("createChangelogTopic: request: %v", err)
	}
	ctr := resp.(*kmsg.CreateTopicsResponse)
	for _, topicResp := range ctr.Topics {
		kerErr := kerr.ErrorForCode(topicResp.ErrorCode)
		if kerErr != nil {
			t.Fatalf("createChangelogTopic: topic %q: %v", topicResp.Topic, kerErr)
		}
	}
}
