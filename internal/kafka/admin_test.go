//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// TestEnsureTopics verifies that EnsureTopics:
//  1. Creates a topic with cleanup.policy=compact.
//  2. Reads back the config via DescribeConfigs and asserts cleanup.policy=compact.
//  3. Is idempotent: a second call with the same spec returns no error.
//  4. Returns an error if the existing topic has a different partition count.
func TestEnsureTopics(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-admin-cluster"),
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

	const topicName = "changelog-test-compact"
	spec := TopicSpec{
		Name:              topicName,
		Partitions:        4,
		ReplicationFactor: 1,
		Configs:           map[string]string{"cleanup.policy": "compact"},
	}

	// First call: should create the topic.
	if err := EnsureTopics(ctx, brokers, []TopicSpec{spec}); err != nil {
		t.Fatalf("EnsureTopics (create): %v", err)
	}

	// Verify cleanup.policy via DescribeConfigs.
	policy, err := describeTopicConfig(ctx, brokers, topicName, "cleanup.policy")
	if err != nil {
		t.Fatalf("describeTopicConfig: %v", err)
	}
	if policy != "compact" {
		t.Errorf("cleanup.policy: got %q, want %q", policy, "compact")
	}

	// Second call: idempotent — no error for an already-existing topic.
	if err := EnsureTopics(ctx, brokers, []TopicSpec{spec}); err != nil {
		t.Fatalf("EnsureTopics (idempotent second call): %v", err)
	}

	// Partition-count mismatch: should return an error.
	mismatch := TopicSpec{
		Name:              topicName,
		Partitions:        8, // different from 4
		ReplicationFactor: 1,
		Configs:           map[string]string{"cleanup.policy": "compact"},
	}
	if err := EnsureTopics(ctx, brokers, []TopicSpec{mismatch}); err == nil {
		t.Fatal("EnsureTopics (partition mismatch): expected error, got nil")
	}
}
