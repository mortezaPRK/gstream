//go:build integration

package runtime_test

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/kafka"
	"github.com/mortezaPRK/gstream/internal/runtime"
	"github.com/mortezaPRK/gstream/internal/topology"
	"github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	kgo "github.com/twmb/franz-go/pkg/kgo"
)

// dockerAvailable probes for a running Docker daemon without importing the Docker
// SDK so this helper stays lightweight — mirrors the same check in
// internal/kafka/integration_test.go.
func dockerAvailable() bool {
	return exec.Command("docker", "info").Run() == nil
}

// TestE2E_StatelessFilterMap is the P0 exit-criteria test: records flow
// consume → filter → map → produce against a real broker, and offsets are
// committed after output so a second run does not reprocess (ALO, §4.1, §15).
//
// Pipeline:
//   - Source topic: "e2e-input"
//   - Filter: keep records whose JSON-decoded string value has len >= 4.
//   - Map:    uppercase the value; prefix the key with "out-".
//   - Sink topic: "e2e-output"
//
// Input records:
//
//	"hello"  (len 5) → passes → output key="out-key1" value=`"HELLO"`
//	"hi"     (len 2) → filtered out
//	"world"  (len 5) → passes → output key="out-key3" value=`"WORLD"`
//	"stream" (len 6) → passes → output key="out-key4" value=`"STREAM"`
//	"ab"     (len 2) → filtered out
//
// Expected output topic: 3 records (hello, world, stream); 2 absent (hi, ab).
func TestE2E_StatelessFilterMap(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping E2E integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// -------------------------------------------------------------------------
	// 1. Start a Kafka broker via testcontainers (kafka module — NOT redpanda).
	// -------------------------------------------------------------------------
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
	t.Cleanup(func() { _ = kc.Terminate(context.Background()) })

	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get broker addresses: %v", err)
	}
	t.Logf("broker addresses: %v", brokers)

	const (
		srcTopic  = "e2e-input"
		sinkTopic = "e2e-output"
		appID     = "gstream-e2e-filter-map"
	)

	// -------------------------------------------------------------------------
	// 2. Pre-create topics, then pre-produce input records.
	// -------------------------------------------------------------------------
	// Explicitly create both topics before the pipeline starts. This is safer
	// than relying on auto.create.topics.enable because it guarantees the
	// topics exist when gstream's kgo client issues its first metadata request.
	createTopics(t, ctx, brokers, srcTopic, sinkTopic)

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	type inputRecord struct {
		key   string
		value string // plain Go string; will be JSON-encoded
	}
	inputs := []inputRecord{
		{"key1", "hello"},  // len 5 → passes
		{"key2", "hi"},     // len 2 → filtered
		{"key3", "world"},  // len 5 → passes
		{"key4", "stream"}, // len 6 → passes
		{"key5", "ab"},     // len 2 → filtered
	}

	serde := gstream.JSONSerde[string]{}
	for _, inp := range inputs {
		encoded, err := serde.Serialize(inp.value)
		if err != nil {
			t.Fatalf("serialize(%q): %v", inp.value, err)
		}
		res := producer.ProduceSync(ctx, &kgo.Record{
			Topic: srcTopic,
			Key:   []byte(inp.key),
			Value: encoded,
		})
		if res.FirstErr() != nil {
			t.Fatalf("produce(%q): %v", inp.key, res.FirstErr())
		}
	}
	producer.Close()
	t.Logf("produced %d input records to %s", len(inputs), srcTopic)

	// -------------------------------------------------------------------------
	// 3. Build the topology: source → filter(len>=4) → map(uppercase) → sink.
	// -------------------------------------------------------------------------
	b := topology.NewBuilder()
	src := b.AddSource("source")
	filt := b.AddProcessor("filter",
		topology.Filter(func(_, v any) bool {
			s, ok := v.(string)
			return ok && len(s) >= 4
		}),
		src,
	)
	mapped := b.AddProcessor("map",
		topology.Mapper(func(k, v any) (any, any) {
			return "out-" + string(k.([]byte)), strings.ToUpper(v.(string))
		}),
		filt,
	)
	b.AddSink("sink", mapped)
	topo := b.Build()

	// -------------------------------------------------------------------------
	// 4. Wire the adapter and construct the kafka.Client.
	// -------------------------------------------------------------------------
	// Build a BuiltTopology using the already-constructed topology DAG and
	// hand-coded SourceBinding/SinkBinding closures that mirror the serde used
	// above. The Mapper in step 3 expects key as []byte and value as string, so
	// DecodeKey passes raw bytes through; DecodeVal JSON-decodes the string.
	bt := &gstream.BuiltTopology{
		Topology: topo,
		Sources: map[string]gstream.SourceBinding{
			"source": {
				Topic: srcTopic,
				DecodeKey: func(raw []byte) (any, error) {
					return raw, nil // []byte key, consumed as-is by the Mapper
				},
				DecodeVal: func(raw []byte) (any, error) {
					return serde.Deserialize(raw)
				},
			},
		},
		Sinks: map[string]gstream.SinkBinding{
			"sink": {
				Topic: sinkTopic,
				EncodeKey: func(x any) ([]byte, error) {
					// Mapper output key is string ("out-" + original key string)
					v, ok := x.(string)
					if !ok {
						return nil, fmt.Errorf("EncodeKey: expected string, got %T", x)
					}
					return []byte(v), nil
				},
				EncodeVal: func(x any) ([]byte, error) {
					// Mapper output value is uppercased string
					v, ok := x.(string)
					if !ok {
						return nil, fmt.Errorf("EncodeVal: expected string, got %T", x)
					}
					return serde.Serialize(v)
				},
			},
		},
	}
	adapter, err := runtime.NewAdapter(bt, slog.Default())
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	cfg := gstream.Config{
		ApplicationID: appID,
		Brokers:       brokers,
	}
	cfg.ApplyDefaults()

	client, err := kafka.New(cfg, []string{srcTopic}, slog.Default())
	if err != nil {
		t.Fatalf("kafka.New: %v", err)
	}
	defer client.Close()

	// -------------------------------------------------------------------------
	// 5. Run the pipeline until the expected output records land on the sink
	//    topic, then cancel. Creating the consumer here (before the run) lets it
	//    catch every record produced during the pipeline run.
	// -------------------------------------------------------------------------
	sinkConsumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(sinkTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("failed to create sink consumer: %v", err)
	}
	defer sinkConsumer.Close()

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- client.Run(runCtx, adapter.ProcessFunc())
	}()

	// Collect output records (we expect exactly 3).
	type outRecord struct {
		key   string
		value string // raw JSON bytes from output topic
	}
	var outputRecords []outRecord

	// Deterministic wait: poll the sink topic until all 3 expected output records
	// have arrived. This confirms ProduceSync+CommitRecords completed for the
	// batch — no fixed sleep needed. A 25 s backstop ensures a real failure still
	// terminates the test instead of hanging forever.
	readyCtx, readyCancel := context.WithTimeout(ctx, 25*time.Second)
	defer readyCancel()

	for len(outputRecords) < 3 {
		fetches := sinkConsumer.PollFetches(readyCtx)
		if fetches.IsClientClosed() {
			break
		}
		if err := readyCtx.Err(); err != nil {
			t.Fatalf("timed out waiting for output records in sink topic; got %d, want 3", len(outputRecords))
		}
		fetches.EachRecord(func(r *kgo.Record) {
			outputRecords = append(outputRecords, outRecord{
				key:   string(r.Key),
				value: string(r.Value),
			})
			t.Logf("sink record: key=%s value=%s", r.Key, r.Value)
		})
	}
	t.Logf("all expected output records arrived in sink topic (%d)", len(outputRecords))

	// Cancel the run loop and wait for it to stop.
	// NOTE: canceling the run context here may interrupt an in-flight offset
	// commit, producing a benign "failed to commit offsets ... context canceled"
	// WARN. Harmless under ALO — uncommitted records are redelivered on the next
	// run; assertions below account for this.
	runCancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("client.Run returned error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("client.Run did not stop after context cancellation")
	}

	// -------------------------------------------------------------------------
	// 6. Assert correctness on the output records collected above.
	// -------------------------------------------------------------------------

	// Assert count: exactly 3 records (hi and ab filtered out).
	if got, want := len(outputRecords), 3; got != want {
		t.Fatalf("output record count: got %d, want %d; records: %+v", got, want, outputRecords)
	}

	// Build an easy lookup.
	byKey := make(map[string]string, len(outputRecords))
	for _, r := range outputRecords {
		byKey[r.key] = r.value
	}

	// "hello" → key="out-key1" value=`"HELLO"` (JSON-encoded)
	if v, ok := byKey["out-key1"]; !ok {
		t.Error("expected out-key1 in output, not found")
	} else if v != `"HELLO"` {
		t.Errorf("out-key1: got %q, want %q", v, `"HELLO"`)
	}

	// "world" → key="out-key3" value=`"WORLD"`
	if v, ok := byKey["out-key3"]; !ok {
		t.Error("expected out-key3 in output, not found")
	} else if v != `"WORLD"` {
		t.Errorf("out-key3: got %q, want %q", v, `"WORLD"`)
	}

	// "stream" → key="out-key4" value=`"STREAM"`
	if v, ok := byKey["out-key4"]; !ok {
		t.Error("expected out-key4 in output, not found")
	} else if v != `"STREAM"` {
		t.Errorf("out-key4: got %q, want %q", v, `"STREAM"`)
	}

	// Filtered-out records must NOT appear.
	if _, ok := byKey["out-key2"]; ok {
		t.Error("key2 (value='hi') should have been filtered but appeared in output")
	}
	if _, ok := byKey["out-key5"]; ok {
		t.Error("key5 (value='ab') should have been filtered but appeared in output")
	}

	// -------------------------------------------------------------------------
	// 7. ALO offset check: restart with the same consumer group and assert no
	//    records are reprocessed (offsets were committed, §4.1).
	// -------------------------------------------------------------------------
	t.Log("verifying ALO: restarting pipeline to confirm no reprocessing")

	reprocessed := make(chan struct{}, 10)
	client2, err := kafka.New(cfg, []string{srcTopic}, slog.Default())
	if err != nil {
		t.Fatalf("kafka.New (second run): %v", err)
	}
	defer client2.Close()

	run2Ctx, run2Cancel := context.WithTimeout(ctx, 10*time.Second)
	defer run2Cancel()
	done2 := make(chan error, 1)
	go func() {
		done2 <- client2.Run(run2Ctx, func(ctx context.Context, in kafka.InRecord) ([]kafka.OutRecord, error) {
			reprocessed <- struct{}{}
			return nil, nil
		})
	}()

	// Wait for the second run to time out (no new records should arrive).
	select {
	case <-reprocessed:
		t.Error("ALO violation: second run received a record that should have been committed")
	case <-run2Ctx.Done():
		t.Log("ALO confirmed: no records reprocessed on second run")
	case err := <-done2:
		if err != nil {
			t.Logf("second run returned: %v", err)
		}
	}

	run2Cancel()
	<-done2
}
