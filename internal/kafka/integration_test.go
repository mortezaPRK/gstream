//go:build integration

package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// dockerAvailable probes for a running Docker daemon without importing the
// Docker SDK, so this helper stays lightweight.
func dockerAvailable() bool {
	cmd := exec.Command("docker", "info")
	return cmd.Run() == nil
}

// TestRoundTrip_ALO is a full produce→consume→process→commit integration test.
// It is gated by the "integration" build tag AND skipped at runtime when Docker
// is unavailable, so default `go test ./internal/kafka/` never requires Docker.
func TestRoundTrip_ALO(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Spin up a Kafka broker via testcontainers (kafka module — NOT redpanda).
	// WithClusterID is required for cp-kafka:7.4.0: that image's startup script
	// calls `dub ensure CLUSTER_ID` and exits 1 if the env var is absent.
	// KAFKA_AUTO_CREATE_TOPICS_ENABLE=true so that produce-before-consume works
	// without an explicit admin CreateTopics call.
	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-cluster"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
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

	const (
		srcTopic  = "integration-src"
		sinkTopic = "integration-sink"
		appID     = "gstream-integration-test"
	)

	// Explicitly create both topics before the pipeline starts. This is safer
	// than relying on auto.create.topics.enable because it guarantees the
	// topics exist when gstream's kgo client issues its first metadata request.
	createTopics(t, ctx, brokers, srcTopic, sinkTopic)

	// Produce a seed record (topic now exists, so no auto-create flag needed).
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("failed to create admin client: %v", err)
	}
	// Produce a seed record before starting our consumer.
	seedRecord := &kgo.Record{Topic: srcTopic, Key: []byte("key1"), Value: []byte("hello")}
	if res := admin.ProduceSync(ctx, seedRecord); res.FirstErr() != nil {
		t.Fatalf("seed produce failed: %v", res.FirstErr())
	}
	admin.Close()

	cfg, err := gstream.Configure(
		gstream.WithName(appID),
		gstream.WithBrokers(brokers...),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	received := make(chan InRecord, 10)
	processFunc := ProcessFunc(func(_ context.Context, in InRecord) ([]OutRecord, error) {
		received <- in
		return []OutRecord{{Topic: sinkTopic, Key: in.Key, Value: in.Value}}, nil
	})

	client, err := New(cfg, []string{srcTopic}, slog.Default())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer client.Close()

	// Create the sink consumer before starting the run, positioned at offset 0
	// so it will catch the output record as soon as the pipeline produces it.
	sinkClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(sinkTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("failed to create sink consumer: %v", err)
	}
	defer sinkClient.Close()

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- client.Run(runCtx, processFunc) }()

	// Wait for the seed record to arrive.
	select {
	case r := <-received:
		if string(r.Value) != "hello" {
			t.Errorf("expected value=hello, got %q", string(r.Value))
		}
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for record")
	}

	// Deterministic wait: poll the sink topic until the output record lands.
	// This confirms ProduceSync+CommitRecords completed for this batch, making it
	// safe to cancel. A 25 s backstop ensures a real failure still terminates.
	readyCtx, readyCancel := context.WithTimeout(ctx, 25*time.Second)
	defer readyCancel()
	var sinkRecords []*kgo.Record
	for len(sinkRecords) == 0 {
		fs := sinkClient.PollFetches(readyCtx)
		if fs.IsClientClosed() {
			break
		}
		if err := readyCtx.Err(); err != nil {
			t.Fatalf("timed out waiting for output record in sink topic")
		}
		fs.EachRecord(func(r *kgo.Record) {
			sinkRecords = append(sinkRecords, r)
		})
	}

	// NOTE: canceling the run context here may interrupt an in-flight offset
	// commit, producing a benign "failed to commit offsets ... context canceled"
	// WARN. Harmless under ALO — uncommitted records are redelivered on the next
	// run; assertions below account for this.
	runCancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}

	// Assert on the records collected above.
	if len(sinkRecords) == 0 {
		t.Fatal("expected at least one record in sink topic")
	}
	for _, r := range sinkRecords {
		fmt.Printf("sink record: key=%s value=%s\n", r.Key, r.Value)
	}
}
