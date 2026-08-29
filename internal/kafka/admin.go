package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TopicSpec describes the desired state of a Kafka topic.
type TopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	// Configs holds topic-level config key/value pairs, e.g.
	// {"cleanup.policy": "compact"} for a changelog topic.
	Configs map[string]string
}

// EnsureTopics idempotently creates Kafka topics described by specs. For each
// spec it:
//  1. Checks whether the topic already exists via a Metadata request.
//  2. Creates any missing topics via a single CreateTopics request with the
//     supplied Configs (e.g. cleanup.policy=compact).
//  3. For topics that already exist, validates that the partition count matches
//     the spec; a mismatch is returned as an error.
//
// "Topic already exists" from the broker is treated as success (idempotent).
// If topic creation is denied (ACL), the function returns a descriptive error
// listing the affected topics and their expected configs.
//
// All kgo/kmsg types are internal; the exported API uses only stdlib + TopicSpec.
func EnsureTopics(ctx context.Context, brokers []string, specs []TopicSpec) error {
	if len(specs) == 0 {
		return nil
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("kafka.EnsureTopics: create client: %w", err)
	}
	defer cl.Close()

	// Step 1: fetch metadata for all requested topic names to learn which
	// already exist and how many partitions they have.
	existing, err := fetchTopicMetadata(ctx, cl, specs)
	if err != nil {
		return fmt.Errorf("kafka.EnsureTopics: metadata: %w", err)
	}

	// Validate existing topics: partition count must match the spec.
	for _, spec := range specs {
		info, ok := existing[spec.Name]
		if !ok {
			// Will be created below.
			continue
		}
		if info.partitions != spec.Partitions {
			return fmt.Errorf(
				"kafka.EnsureTopics: topic %q exists with %d partitions, spec requires %d",
				spec.Name, info.partitions, spec.Partitions,
			)
		}
	}

	// Step 2: create any topics that do not yet exist.
	var missing []TopicSpec
	for _, spec := range specs {
		if _, ok := existing[spec.Name]; !ok {
			missing = append(missing, spec)
		}
	}
	if len(missing) == 0 {
		return nil
	}

	return createTopicSpecs(ctx, cl, missing)
}

// EnsureRepartitionTopics idempotently creates the repartition topics declared
// in bt.RepartitionBindings. For each binding the full topic name is formed as
// <cfg.ApplicationID>-<rb.Name>-repartition and the topic is created with:
//   - Partitions = rb.Partitions (the declared co-partition count; callers set
//     this to match the co-grouped topics — use ValidateCoPartitioned if you
//     want to assert alignment at startup).
//   - cleanup.policy=delete (repartition topics are transient, NOT compacted).
//
// Empty RepartitionBindings → nil. Topic-already-exists is not an error
// (idempotent, same behaviour as EnsureTopics).
func EnsureRepartitionTopics(ctx context.Context, cfg gstream.Config, bt *gstream.BuiltTopology) error {
	if len(bt.RepartitionBindings) == 0 {
		return nil
	}

	specs := make([]TopicSpec, 0, len(bt.RepartitionBindings))
	for _, rb := range bt.RepartitionBindings {
		fullTopic := cfg.ApplicationID + "-" + rb.Name + "-repartition"
		specs = append(specs, TopicSpec{
			Name:              fullTopic,
			Partitions:        rb.Partitions,
			ReplicationFactor: 1,
			Configs:           map[string]string{"cleanup.policy": "delete"},
		})
	}
	return EnsureTopics(ctx, cfg.Brokers, specs)
}

// topicMeta holds the summary information returned by a Metadata response.
type topicMeta struct {
	partitions int32
}

// fetchTopicMetadata queries the broker for metadata on the named topics and
// returns a map of topic name → topicMeta for topics that exist.
// Topics that do not exist are simply absent from the map (not an error).
func fetchTopicMetadata(ctx context.Context, cl *kgo.Client, specs []TopicSpec) (map[string]topicMeta, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	for _, spec := range specs {
		t := kmsg.NewMetadataRequestTopic()
		t.Topic = &spec.Name
		req.Topics = append(req.Topics, t)
	}

	resp, err := cl.Request(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("metadata request: %w", err)
	}
	mr := resp.(*kmsg.MetadataResponse)

	result := make(map[string]topicMeta, len(mr.Topics))
	for _, t := range mr.Topics {
		if t.Topic == nil {
			continue
		}
		topicErr := kerr.ErrorForCode(t.ErrorCode)
		if topicErr != nil {
			if errors.Is(topicErr, kerr.UnknownTopicOrPartition) {
				// Topic does not exist; skip — it will be created.
				continue
			}
			return nil, fmt.Errorf("metadata for topic %q: %w", *t.Topic, topicErr)
		}
		result[*t.Topic] = topicMeta{partitions: int32(len(t.Partitions))}
	}
	return result, nil
}

// createTopicSpecs issues a single CreateTopics request for the supplied specs.
// TopicAlreadyExists is treated as success. Authorization failures are surfaced
// as a descriptive error listing the affected topics and their expected configs.
func createTopicSpecs(ctx context.Context, cl *kgo.Client, specs []TopicSpec) error {
	req := kmsg.NewPtrCreateTopicsRequest()
	for _, spec := range specs {
		t := kmsg.NewCreateTopicsRequestTopic()
		t.Topic = spec.Name
		t.NumPartitions = spec.Partitions
		t.ReplicationFactor = spec.ReplicationFactor
		for k, v := range spec.Configs {
			cfg := kmsg.NewCreateTopicsRequestTopicConfig()
			cfg.Name = k
			cfg.Value = &v
			t.Configs = append(t.Configs, cfg)
		}
		req.Topics = append(req.Topics, t)
	}

	resp, err := cl.Request(ctx, req)
	if err != nil {
		return fmt.Errorf("create topics request: %w", err)
	}
	ctr := resp.(*kmsg.CreateTopicsResponse)

	var authDenied []string
	for _, t := range ctr.Topics {
		topicErr := kerr.ErrorForCode(t.ErrorCode)
		if topicErr == nil {
			continue
		}
		if errors.Is(topicErr, kerr.TopicAlreadyExists) {
			// Idempotent: already exists is not an error.
			continue
		}
		if errors.Is(topicErr, kerr.TopicAuthorizationFailed) ||
			errors.Is(topicErr, kerr.ClusterAuthorizationFailed) {
			// Collect all ACL-denied topics for a single descriptive error.
			authDenied = append(authDenied, describeSpec(t.Topic, specs))
			continue
		}
		return fmt.Errorf("create topic %q: %w", t.Topic, topicErr)
	}

	if len(authDenied) > 0 {
		return fmt.Errorf(
			"kafka.EnsureTopics: not authorized to create topics — expected configurations:\n%s",
			strings.Join(authDenied, "\n"),
		)
	}
	return nil
}

// FetchPartitionCount returns the number of partitions for the named topic. It
// opens a short-lived kgo client, queries broker metadata, and closes the
// client before returning.
//
// Returns an error if the topic does not exist or the metadata request fails.
// Called by C3 (GlobalConsumer) to determine how many partitions to assign for
// full-topic consumption.
func FetchPartitionCount(ctx context.Context, brokers []string, topic string) (int32, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return 0, fmt.Errorf("kafka.FetchPartitionCount: create client: %w", err)
	}
	defer cl.Close()

	meta, err := fetchTopicMetadata(ctx, cl, []TopicSpec{{Name: topic}})
	if err != nil {
		return 0, fmt.Errorf("kafka.FetchPartitionCount: %w", err)
	}
	info, ok := meta[topic]
	if !ok {
		return 0, fmt.Errorf("kafka.FetchPartitionCount: topic %q does not exist", topic)
	}
	return info.partitions, nil
}

// EnsureGlobalTopics idempotently creates the global table topics declared in
// bt.GlobalTableBindings. For each binding, the topic name is binding.Topic
// DIRECTLY (the caller supplies the real topic name — unlike repartition topics
// which are derived from the application ID).
//
// New topics are created with:
//   - Partitions = 1 (global tables are usually pre-existing / externally managed;
//     the actual partition count is discovered at runtime via FetchPartitionCount).
//   - cleanup.policy=compact (global tables are compacted, opposite of repartition
//     topics which use delete).
//
// Idempotency and tolerance: if a topic already exists — regardless of its
// partition count or current config — it is silently skipped. Global topics are
// often owned and managed outside gstream; we must not fail if they were created
// with a different partition count or config.
//
// ValidateCoPartitioned is NOT needed for global tables: a global table is fully
// replicated across all instances (each instance reads all partitions), so there
// is no co-partition alignment requirement with stream topics.
//
// Empty GlobalTableBindings → nil.
func EnsureGlobalTopics(ctx context.Context, cfg gstream.Config, bt *gstream.BuiltTopology) error {
	if len(bt.GlobalTableBindings) == 0 {
		return nil
	}

	specs := make([]TopicSpec, 0, len(bt.GlobalTableBindings))
	for _, binding := range bt.GlobalTableBindings {
		specs = append(specs, TopicSpec{
			Name:              binding.Topic,
			Partitions:        1, // sane default; global topics are usually pre-existing
			ReplicationFactor: 1,
			Configs:           map[string]string{"cleanup.policy": "compact"},
		})
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		return fmt.Errorf("kafka.EnsureGlobalTopics: create client: %w", err)
	}
	defer cl.Close()

	// Fetch metadata to determine which topics already exist. We intentionally do
	// NOT validate partition counts here — global topics are often externally
	// managed and may have any partition count. Existing topics are skipped entirely.
	existing, err := fetchTopicMetadata(ctx, cl, specs)
	if err != nil {
		return fmt.Errorf("kafka.EnsureGlobalTopics: metadata: %w", err)
	}

	var missing []TopicSpec
	for _, spec := range specs {
		if _, ok := existing[spec.Name]; !ok {
			missing = append(missing, spec)
		}
	}
	if len(missing) == 0 {
		return nil
	}

	return createTopicSpecs(ctx, cl, missing)
}

// describeSpec returns a human-readable summary of the TopicSpec with the given
// name, used in authorization-failure error messages.
func describeSpec(name string, specs []TopicSpec) string {
	for _, s := range specs {
		if s.Name == name {
			return fmt.Sprintf("  topic=%q partitions=%d replicationFactor=%d configs=%v",
				s.Name, s.Partitions, s.ReplicationFactor, s.Configs)
		}
	}
	return fmt.Sprintf("  topic=%q (spec not found)", name)
}

// ValidateCoPartitioned returns an error if the given topics do not all have the
// same partition count. Co-partitioning is required for a stream-table join so
// that key K lands on the same partition index in both topics.
//
// Fewer than 2 topics → nil (nothing to compare).
// Missing or zero-partition topic → error naming it.
// Partition count mismatch → error naming both topics and both counts.
func ValidateCoPartitioned(ctx context.Context, brokers []string, topics []string) error {
	if len(topics) < 2 {
		return nil
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("kafka.ValidateCoPartitioned: create client: %w", err)
	}
	defer cl.Close()

	// fetchTopicMetadata only uses TopicSpec.Name; other fields are irrelevant here.
	specs := make([]TopicSpec, len(topics))
	for i, t := range topics {
		specs[i] = TopicSpec{Name: t}
	}

	meta, err := fetchTopicMetadata(ctx, cl, specs)
	if err != nil {
		return fmt.Errorf("kafka.ValidateCoPartitioned: metadata: %w", err)
	}

	for _, t := range topics {
		info, ok := meta[t]
		if !ok || info.partitions == 0 {
			return fmt.Errorf("kafka.ValidateCoPartitioned: topic %q not found or has 0 partitions", t)
		}
	}

	first := topics[0]
	for _, t := range topics[1:] {
		na := meta[first].partitions
		nb := meta[t].partitions
		if na != nb {
			return fmt.Errorf(
				"co-partitioning violation: topic %q has %d partitions but %q has %d; join topics must have equal partition counts",
				first, na, t, nb,
			)
		}
	}
	return nil
}

// describeTopicConfig fetches a single config entry for a topic via a
// DescribeConfigs request. It returns the string value, or an error if the
// topic or config key is unknown. This is unexported and used only by tests.
func describeTopicConfig(ctx context.Context, brokers []string, topic, configKey string) (string, error) { //nolint:unused // used by integration-tagged tests
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return "", fmt.Errorf("describeTopicConfig: create client: %w", err)
	}
	defer cl.Close()

	req := kmsg.NewPtrDescribeConfigsRequest()
	res := kmsg.NewDescribeConfigsRequestResource()
	res.ResourceType = kmsg.ConfigResourceTypeTopic
	res.ResourceName = topic
	res.ConfigNames = []string{configKey}
	req.Resources = append(req.Resources, res)

	resp, err := cl.Request(ctx, req)
	if err != nil {
		return "", fmt.Errorf("describeTopicConfig: request: %w", err)
	}
	dcr := resp.(*kmsg.DescribeConfigsResponse)

	for _, r := range dcr.Resources {
		if r.ResourceName != topic {
			continue
		}
		if kerErr := kerr.ErrorForCode(r.ErrorCode); kerErr != nil {
			return "", fmt.Errorf("describeTopicConfig: topic %q: %w", topic, kerErr)
		}
		for _, c := range r.Configs {
			if c.Name == configKey {
				if c.Value == nil {
					return "", fmt.Errorf("describeTopicConfig: config %q for topic %q is nil", configKey, topic)
				}
				return *c.Value, nil
			}
		}
	}
	return "", fmt.Errorf("describeTopicConfig: config key %q not found for topic %q", configKey, topic)
}
