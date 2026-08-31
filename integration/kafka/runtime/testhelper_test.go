//go:build integration

package runtime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// createTopics creates the named Kafka topics on the given broker addresses
// using an explicit admin CreateTopics request. This is the safest way to
// ensure topics exist before a test pipeline starts: it does not rely on
// auto.create.topics.enable and does not inject spurious records.
//
// Each topic is created with 1 partition and replication-factor=1 (suitable
// for a single-broker test cluster). TopicAlreadyExists is treated as success
// to tolerate retries.
func createTopics(t *testing.T, ctx context.Context, brokers []string, topics ...string) {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("createTopics: new kgo client: %v", err)
	}
	defer cl.Close()

	req := kmsg.NewPtrCreateTopicsRequest()
	for _, topic := range topics {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic = topic
		rt.NumPartitions = 1
		rt.ReplicationFactor = 1
		req.Topics = append(req.Topics, rt)
	}

	resp, err := cl.Request(ctx, req)
	if err != nil {
		t.Fatalf("createTopics: request: %v", err)
	}
	ctr := resp.(*kmsg.CreateTopicsResponse)
	for _, topic := range ctr.Topics {
		kerErr := kerr.ErrorForCode(topic.ErrorCode)
		if kerErr != nil && !errors.Is(kerErr, kerr.TopicAlreadyExists) {
			t.Fatalf("createTopics: topic %q: %v", topic.Topic, kerErr)
		}
	}
}
