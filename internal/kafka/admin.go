package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
		name := spec.Name // copy to take address of loop variable
		t.Topic = &name
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
// as a descriptive error listing the affected topics and their expected configs
// (§14).
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
			val := v // copy to take address of loop variable
			cfg.Value = &val
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

// describeTopicConfig fetches a single config entry for a topic via a
// DescribeConfigs request. It returns the string value, or an error if the
// topic or config key is unknown. This is unexported and used only by tests.
func describeTopicConfig(ctx context.Context, brokers []string, topic, configKey string) (string, error) {
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
