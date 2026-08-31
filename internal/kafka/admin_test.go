//go:build integration

package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/twmb/franz-go/pkg/kgo"
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
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	brokers := integrationBrokers(t)

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
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	brokers := integrationBrokers(t)

	// Create exactly one topic.
	if err := EnsureTopics(ctx, brokers, []TopicSpec{
		{Name: "exists-topic", Partitions: 4, ReplicationFactor: 1},
	}); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}

	const ghost = "definitely-does-not-exist-abc123"
	err := ValidateCoPartitioned(ctx, brokers, []string{"exists-topic", ghost})
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
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	brokers := integrationBrokers(t)

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

// TestEnsureGlobalTopics_EmptyBindings verifies the short-circuit: no
// GlobalTableBindings → nil return without any broker contact.
func TestEnsureGlobalTopics_EmptyBindings(t *testing.T) {
	cfg := gstream.Config{
		ApplicationID: "test-app",
		Brokers:       []string{"broker:9092"},
	}
	bt := &gstream.BuiltTopology{
		GlobalTableBindings: map[string]gstream.GlobalTableBinding{},
	}
	if err := EnsureGlobalTopics(context.Background(), cfg, bt); err != nil {
		t.Errorf("empty bindings: want nil, got %v", err)
	}
}

// TestFetchPartitionCount verifies FetchPartitionCount against a live broker:
//  1. A topic created with 3 partitions returns 3.
//  2. A non-existent topic returns a non-nil error.
func TestFetchPartitionCount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	brokers := integrationBrokers(t)

	const topicName = "fetch-count-test"
	if err := EnsureTopics(ctx, brokers, []TopicSpec{
		{Name: topicName, Partitions: 3, ReplicationFactor: 1},
	}); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}

	t.Run("existing topic returns correct count", func(t *testing.T) {
		count, err := FetchPartitionCount(ctx, brokers, topicName)
		if err != nil {
			t.Fatalf("FetchPartitionCount: %v", err)
		}
		if count != 3 {
			t.Errorf("partition count: got %d, want 3", count)
		}
	})

	t.Run("missing topic returns error", func(t *testing.T) {
		_, err := FetchPartitionCount(ctx, brokers, "definitely-does-not-exist-xyz")
		if err == nil {
			t.Fatal("want non-nil error for missing topic, got nil")
		}
		t.Logf("missing topic error (verbatim): %v", err)
	})
}

// TestEnsureGlobalTopics verifies that EnsureGlobalTopics:
//  1. Creates a global topic with cleanup.policy=compact.
//  2. Is idempotent: a second call returns no error.
//  3. Does not error if the topic pre-exists with different config.
func TestEnsureGlobalTopics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	brokers := integrationBrokers(t)

	cfg := gstream.Config{
		ApplicationID: "test-app",
		Brokers:       brokers,
	}
	bt := &gstream.BuiltTopology{
		GlobalTableBindings: map[string]gstream.GlobalTableBinding{
			"users": {StoreName: "users", Topic: "gt-users"},
		},
	}

	// First call: should create the topic.
	if err := EnsureGlobalTopics(ctx, cfg, bt); err != nil {
		t.Fatalf("EnsureGlobalTopics (create): %v", err)
	}

	// Verify cleanup.policy=compact.
	policy, err := describeTopicConfig(ctx, brokers, "gt-users", "cleanup.policy")
	if err != nil {
		t.Fatalf("describeTopicConfig: %v", err)
	}
	if policy != "compact" {
		t.Errorf("cleanup.policy: got %q, want %q", policy, "compact")
	}

	// Second call: idempotent — no error.
	if err := EnsureGlobalTopics(ctx, cfg, bt); err != nil {
		t.Fatalf("EnsureGlobalTopics (idempotent second call): %v", err)
	}

	// Pre-existing topic with different config should still return nil.
	// Create a topic with cleanup.policy=delete (simulating externally-managed topic).
	const externalTopic = "gt-external"
	if err := EnsureTopics(ctx, brokers, []TopicSpec{
		{Name: externalTopic, Partitions: 6, ReplicationFactor: 1,
			Configs: map[string]string{"cleanup.policy": "delete"}},
	}); err != nil {
		t.Fatalf("pre-create external topic: %v", err)
	}

	btExternal := &gstream.BuiltTopology{
		GlobalTableBindings: map[string]gstream.GlobalTableBinding{
			"ext": {StoreName: "ext", Topic: externalTopic},
		},
	}
	if err := EnsureGlobalTopics(ctx, cfg, btExternal); err != nil {
		t.Fatalf("EnsureGlobalTopics on pre-existing topic with different config: %v", err)
	}
}

// TestEnsureRepartitionTopics_EmptyBindings verifies the short-circuit: no
// RepartitionBindings → nil return without any broker contact.
func TestEnsureRepartitionTopics_EmptyBindings(t *testing.T) {
	cfg := gstream.Config{
		ApplicationID: "test-app",
		Brokers:       []string{"broker:9092"},
	}
	bt := &gstream.BuiltTopology{
		RepartitionBindings: map[string]gstream.RepartitionBinding{},
	}
	if err := EnsureRepartitionTopics(context.Background(), cfg, bt); err != nil {
		t.Errorf("empty bindings: want nil, got %v", err)
	}
}

// TestEnsureRepartitionTopics verifies that EnsureRepartitionTopics:
//  1. Creates repartition topic with the correct partition count.
//  2. Sets cleanup.policy=delete (NOT compact).
//  3. Is idempotent: a second call returns no error.
func TestEnsureRepartitionTopics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	brokers := integrationBrokers(t)

	const appID = "myapp"
	cfg := gstream.Config{
		ApplicationID: appID,
		Brokers:       brokers,
	}
	bt := &gstream.BuiltTopology{
		RepartitionBindings: map[string]gstream.RepartitionBinding{
			"rp": {Name: "rp", Partitions: 4},
		},
	}

	// First call: should create the topic.
	if err := EnsureRepartitionTopics(ctx, cfg, bt); err != nil {
		t.Fatalf("EnsureRepartitionTopics (create): %v", err)
	}

	const fullTopic = appID + "-rp-repartition"

	// Verify partition count via metadata.
	specs := []TopicSpec{{Name: fullTopic}}
	cl, clErr := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if clErr != nil {
		t.Fatalf("create test client: %v", clErr)
	}
	defer cl.Close()
	meta, metaErr := fetchTopicMetadata(ctx, cl, specs)
	if metaErr != nil {
		t.Fatalf("fetchTopicMetadata: %v", metaErr)
	}
	info, ok := meta[fullTopic]
	if !ok {
		t.Fatalf("topic %q not found in metadata", fullTopic)
	}
	if info.partitions != 4 {
		t.Errorf("partition count: got %d, want 4", info.partitions)
	}

	// Verify cleanup.policy=delete.
	policy, descErr := describeTopicConfig(ctx, brokers, fullTopic, "cleanup.policy")
	if descErr != nil {
		t.Fatalf("describeTopicConfig: %v", descErr)
	}
	if policy != "delete" {
		t.Errorf("cleanup.policy: got %q, want %q", policy, "delete")
	}

	// Second call: idempotent — no error.
	if err := EnsureRepartitionTopics(ctx, cfg, bt); err != nil {
		t.Fatalf("EnsureRepartitionTopics (idempotent second call): %v", err)
	}
}

func TestPrepareTopologyCreatesOnlyInternalTopics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	brokers := integrationBrokers(t)
	if err := EnsureTopics(ctx, brokers, []TopicSpec{
		{Name: "input", Partitions: 2, ReplicationFactor: 1},
		{Name: "output", Partitions: 2, ReplicationFactor: 1},
	}); err != nil {
		t.Fatalf("create caller-managed topics: %v", err)
	}

	builder := gstream.NewStreamBuilder()
	gstream.Stream[string, string](
		builder, "input", "source", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{},
	).
		SelectKey(func(_ string, value string) string { return value }).
		GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		Count("counts").
		To("output", gstream.JSONSerde[string]{}, gstream.JSONSerde[int64]{})
	topology := builder.Build()
	cfg := gstream.Config{ApplicationID: "planner", Brokers: brokers}
	if err := PrepareTopology(ctx, cfg, topology); err != nil {
		t.Fatalf("PrepareTopology: %v", err)
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("create metadata client: %v", err)
	}
	defer client.Close()
	metadata, err := fetchTopicMetadata(ctx, client, []TopicSpec{
		{Name: "planner-counts-changelog"},
	})
	if err != nil {
		t.Fatalf("fetch internal metadata: %v", err)
	}
	if metadata["planner-counts-changelog"].partitions != 2 {
		t.Fatalf("changelog partitions = %d, want 2", metadata["planner-counts-changelog"].partitions)
	}
	if len(topology.RepartitionBindings) != 1 {
		t.Fatalf("automatic repartition bindings = %d, want 1", len(topology.RepartitionBindings))
	}
	for _, binding := range topology.RepartitionBindings {
		name := cfg.ApplicationID + "-" + binding.Name + "-repartition"
		repartitionMetadata, err := fetchTopicMetadata(ctx, client, []TopicSpec{{Name: name}})
		if err != nil {
			t.Fatalf("fetch repartition metadata: %v", err)
		}
		if repartitionMetadata[name].partitions != 2 {
			t.Fatalf("repartition partitions = %d, want 2", repartitionMetadata[name].partitions)
		}
	}

	missingBuilder := gstream.NewStreamBuilder()
	gstream.Stream[string, string](
		missingBuilder, "missing-input", "source",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{},
	).To("output", "sink", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	err = PrepareTopology(ctx, cfg, missingBuilder.Build())
	if err == nil || !strings.Contains(err.Error(), "missing-input") {
		t.Fatalf("missing caller topic error = %v, want named failure", err)
	}
	if _, err := FetchPartitionCount(ctx, brokers, "missing-input"); err == nil {
		t.Fatal("PrepareTopology auto-created caller-managed source topic")
	}
}
