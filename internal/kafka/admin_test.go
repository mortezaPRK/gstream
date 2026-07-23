//go:build integration

package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// TestValidateCoPartitioned_LessThanTwo verifies the <2 topics → nil short-circuit.
// This is a plain unit test with no container dependency.
func TestValidateCoPartitioned_LessThanTwo(t *testing.T) {
	if err := ValidateCoPartitioned(context.Background(), []string{"broker:9092"}, nil); err != nil {
		t.Errorf("nil topics: want nil, got %v", err)
	}
	if err := ValidateCoPartitioned(context.Background(), []string{"broker:9092"}, []string{"only-one"}); err != nil {
		t.Errorf("one topic: want nil, got %v", err)
	}
}

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

// TestValidateCoPartitioned_MissingTopic verifies GATE-1: a nonexistent topic causes
// ValidateCoPartitioned to return a non-nil error that names the missing topic.
func TestValidateCoPartitioned_MissingTopic(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-missingtopic-cluster"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
		}),
	)
	if err != nil {
		t.Skipf("failed to start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = kc.Terminate(ctx) })

	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("get brokers: %v", err)
	}

	// Create exactly one topic.
	if err := EnsureTopics(ctx, brokers, []TopicSpec{
		{Name: "exists-topic", Partitions: 4, ReplicationFactor: 1},
	}); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}

	const ghost = "definitely-does-not-exist-abc123"
	err = ValidateCoPartitioned(ctx, brokers, []string{"exists-topic", ghost})
	if err == nil {
		t.Fatal("want non-nil error for missing topic, got nil")
	}
	if !strings.Contains(err.Error(), ghost) {
		t.Errorf("error %q does not mention missing topic %q", err.Error(), ghost)
	}
	t.Logf("GATE-1 error (verbatim): %v", err)
}

// TestValidateCoPartitioned tests the co-partition validator against a live broker.
func TestValidateCoPartitioned(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-copartition-cluster"),
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

	// Create two topics with the SAME partition count.
	if err := EnsureTopics(ctx, brokers, []TopicSpec{
		{Name: "copart-a", Partitions: 4, ReplicationFactor: 1},
		{Name: "copart-b", Partitions: 4, ReplicationFactor: 1},
	}); err != nil {
		t.Fatalf("EnsureTopics (same): %v", err)
	}

	// Create a third topic with a DIFFERENT partition count.
	if err := EnsureTopics(ctx, brokers, []TopicSpec{
		{Name: "copart-c", Partitions: 8, ReplicationFactor: 1},
	}); err != nil {
		t.Fatalf("EnsureTopics (different): %v", err)
	}

	t.Run("equal partition counts returns nil", func(t *testing.T) {
		if err := ValidateCoPartitioned(ctx, brokers, []string{"copart-a", "copart-b"}); err != nil {
			t.Errorf("want nil, got: %v", err)
		}
	})

	t.Run("unequal partition counts returns descriptive error", func(t *testing.T) {
		err := ValidateCoPartitioned(ctx, brokers, []string{"copart-a", "copart-c"})
		if err == nil {
			t.Fatal("want error, got nil")
		}
		msg := err.Error()
		for _, want := range []string{"copart-a", "copart-c", "4", "8"} {
			if !strings.Contains(msg, want) {
				t.Errorf("error message %q missing %q", msg, want)
			}
		}
	})
}
