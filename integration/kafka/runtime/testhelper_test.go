//go:build integration

package runtime_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"testing"

	gstream "mortz.dev/go/gstream"
	"mortz.dev/go/gstream/internal/runtime"
	state "mortz.dev/go/gstream/stores/pebble"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type JSONSerde[T any] struct{}

func (JSONSerde[T]) Serialize(value T) ([]byte, error) { return json.Marshal(value) }

func (JSONSerde[T]) Deserialize(data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}

type BytesSerde struct{}

func (BytesSerde) Serialize(value []byte) ([]byte, error)   { return value, nil }
func (BytesSerde) Deserialize(value []byte) ([]byte, error) { return value, nil }

func newTestAdapter(bt *gstream.BuiltTopology, cfg gstream.Config, logger gstream.Logger) (*runtime.Adapter, error) {
	if cfg.StoreProvider == nil {
		cfg.StoreProvider = state.NewProvider()
	}
	return runtime.NewAdapter(bt, cfg, logger)
}

func TestNewTestAdapter(t *testing.T) {
	builder := gstream.NewStreamBuilder()
	gstream.Stream[string, string](
		builder, "input", "source", JSONSerde[string]{}, JSONSerde[string]{},
	).To("output", "sink", JSONSerde[string]{}, JSONSerde[string]{})
	cfg, err := gstream.Configure(
		gstream.WithName("adapter-helper"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := newTestAdapter(builder.Build(), cfg, nil)
	if err != nil || adapter == nil {
		t.Fatalf("newTestAdapter() = %v, %v", adapter, err)
	}
}

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
