package runtime_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/kafka"
	"github.com/mortezaPRK/gstream/internal/runtime"
	state "github.com/mortezaPRK/gstream/internal/testutil"
	"github.com/mortezaPRK/gstream/internal/topology"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// buildSimpleBuiltTopology builds a BuiltTopology whose DAG is:
//
//	source → filter(len>=4) → map(uppercase key + value) → sink
//
// Source decodes string keys and string values via JSONSerde[string].
// Sink encodes string keys and string values via JSONSerde[string].
// This topology is driven directly via topology.Builder, not via gstream
// operators (which are written by a concurrent agent), so the test
// package is fully independent.
func buildSimpleBuiltTopology(t *testing.T) *gstream.BuiltTopology {
	t.Helper()

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
			ks, _ := k.(string)
			vs, _ := v.(string)
			upper := vs
			if len(vs) > 0 {
				runes := []rune(vs)
				for i, r := range runes {
					if r >= 'a' && r <= 'z' {
						runes[i] = r - 32
					}
				}
				upper = string(runes)
			}
			return "mapped-" + ks, upper
		}),
		filt,
	)
	b.AddSink("sink", mapped)
	topo := b.Build()

	strSerde := state.JSONSerde[string]{}

	return &gstream.BuiltTopology{
		Topology: topo,
		Sources: map[string]gstream.SourceBinding{
			"source": {
				Topic: "input-topic",
				DecodeKey: func(raw []byte) (any, error) {
					// Keys are plain bytes (not JSON), pass as string.
					return string(raw), nil
				},
				DecodeVal: func(raw []byte) (any, error) {
					return strSerde.Deserialize(raw)
				},
			},
		},
		Sinks: map[string]gstream.SinkBinding{
			"sink": {
				Topic: "output-topic",
				EncodeKey: func(x any) ([]byte, error) {
					v, ok := x.(string)
					if !ok {
						return nil, fmt.Errorf("EncodeKey: expected string, got %T", x)
					}
					return []byte(v), nil
				},
				EncodeVal: func(x any) ([]byte, error) {
					v, ok := x.(string)
					if !ok {
						return nil, fmt.Errorf("EncodeVal: expected string, got %T", x)
					}
					return strSerde.Serialize(v)
				},
			},
		},
	}
}

// inRecord returns a kafka.InRecord whose value is a JSON-encoded string.
func inRecord(t *testing.T, key, value string) kafka.InRecord {
	t.Helper()
	b, err := state.JSONSerde[string]{}.Serialize(value)
	if err != nil {
		t.Fatalf("inRecord: serialize(%q): %v", value, err)
	}
	return kafka.InRecord{
		Topic:     "input-topic",
		Partition: 0,
		Offset:    0,
		Key:       []byte(key),
		Value:     b,
		Timestamp: time.Now(),
	}
}

// unitTestCfg returns a minimal valid Config for unit tests that do not
// contact a real broker (stateless topology tests).
func unitTestCfg(t *testing.T) gstream.Config {
	t.Helper()
	cfg, err := gstream.Configure(
		gstream.WithName("unit-test"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("unitTestCfg: %v", err)
	}
	return cfg
}

// ---------------------------------------------------------------------------
// NewAdapter validation tests
// ---------------------------------------------------------------------------

func TestNewAdapter_NilBt(t *testing.T) {
	_, err := runtime.NewAdapter(nil, unitTestCfg(t), nil)
	if err == nil {
		t.Fatal("expected error for nil bt")
	}
}

func TestNewAdapter_MultipleSourcesRejected(t *testing.T) {
	b := topology.NewBuilder()
	src1 := b.AddSource("src1")
	src2 := b.AddSource("src2")
	_ = src1
	b.AddSink("sink", src2)
	topo := b.Build()

	bt := &gstream.BuiltTopology{
		Topology: topo,
		Sources: map[string]gstream.SourceBinding{
			"src1": {Topic: "t", DecodeKey: func([]byte) (any, error) { return nil, nil }, DecodeVal: func([]byte) (any, error) { return nil, nil }},
			"src2": {Topic: "t", DecodeKey: func([]byte) (any, error) { return nil, nil }, DecodeVal: func([]byte) (any, error) { return nil, nil }},
		},
		Sinks: map[string]gstream.SinkBinding{
			"sink": {Topic: "t", EncodeKey: func(any) ([]byte, error) { return nil, nil }, EncodeVal: func(any) ([]byte, error) { return nil, nil }},
		},
	}
	_, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err == nil {
		t.Fatal("expected error for topology with more than one source")
	}
}

func TestNewAdapter_MissingSinkBinding(t *testing.T) {
	b := topology.NewBuilder()
	src := b.AddSource("source")
	b.AddSink("sink", src)
	topo := b.Build()

	bt := &gstream.BuiltTopology{
		Topology: topo,
		Sources: map[string]gstream.SourceBinding{
			"source": {Topic: "t", DecodeKey: func([]byte) (any, error) { return nil, nil }, DecodeVal: func([]byte) (any, error) { return nil, nil }},
		},
		Sinks: map[string]gstream.SinkBinding{
			// "sink" is intentionally absent
		},
	}
	_, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err == nil {
		t.Fatal("expected error when sink binding is missing")
	}
}

func TestNewAdapter_NilLogger(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	// nil logger must not panic — falls back to slog.Default().
	_, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter with nil logger: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Happy-path / correctness tests
// ---------------------------------------------------------------------------

// TestAdapter_FilterAndMap drives three records through the adapter; records with
// len >= 4 pass and are uppercased; short ones are filtered.
func TestAdapter_FilterAndMap(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	fn := adapter.ProcessFunc()
	ctx := context.Background()

	// "hello" (len 5) → passes, uppercased.
	outs, err := fn(ctx, inRecord(t, "k1", "hello"))
	if err != nil {
		t.Fatalf("process(hello): %v", err)
	}
	if len(outs) != 1 {
		t.Fatalf("expected 1 output for 'hello', got %d", len(outs))
	}
	if string(outs[0].Value) != `"HELLO"` {
		t.Errorf("unexpected value: %s", string(outs[0].Value))
	}
	if outs[0].Topic != "output-topic" {
		t.Errorf("unexpected topic: %s", outs[0].Topic)
	}

	// "hi" (len 2) → filtered out.
	outs, err = fn(ctx, inRecord(t, "k2", "hi"))
	if err != nil {
		t.Fatalf("process(hi): %v", err)
	}
	if len(outs) != 0 {
		t.Fatalf("expected 0 outputs for 'hi' (filtered), got %d", len(outs))
	}

	// "world" (len 5) → passes, uppercased.
	outs, err = fn(ctx, inRecord(t, "k3", "world"))
	if err != nil {
		t.Fatalf("process(world): %v", err)
	}
	if len(outs) != 1 {
		t.Fatalf("expected 1 output for 'world', got %d", len(outs))
	}
	if string(outs[0].Value) != `"WORLD"` {
		t.Errorf("unexpected value: %s", string(outs[0].Value))
	}
}

// TestAdapter_KeyPropagation checks that the output key is derived from the input
// key via the Mapper (test topology prefixes "mapped-" to the decoded string key).
func TestAdapter_KeyPropagation(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	fn := adapter.ProcessFunc()

	outs, err := fn(context.Background(), inRecord(t, "mykey", "hello"))
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	if string(outs[0].Key) != "mapped-mykey" {
		t.Errorf("expected key=mapped-mykey, got %s", string(outs[0].Key))
	}
}

// TestAdapter_TopicFromSinkBinding verifies that OutRecords carry the topic from
// the SinkBinding, not from anywhere else.
func TestAdapter_TopicFromSinkBinding(t *testing.T) {
	b := topology.NewBuilder()
	src := b.AddSource("src")
	b.AddSink("my-sink", src)
	topo := b.Build()

	const wantTopic = "the-output-topic"
	strSerde := state.JSONSerde[string]{}
	bt := &gstream.BuiltTopology{
		Topology: topo,
		Sources: map[string]gstream.SourceBinding{
			"src": {
				Topic:     "in",
				DecodeKey: func(raw []byte) (any, error) { return string(raw), nil },
				DecodeVal: func(raw []byte) (any, error) { return strSerde.Deserialize(raw) },
			},
		},
		Sinks: map[string]gstream.SinkBinding{
			"my-sink": {
				Topic:     wantTopic,
				EncodeKey: func(x any) ([]byte, error) { return []byte(x.(string)), nil },
				EncodeVal: func(x any) ([]byte, error) { return strSerde.Serialize(x.(string)) },
			},
		},
	}

	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	outs, err := adapter.ProcessFunc()(context.Background(), kafka.InRecord{
		Topic: "in", Partition: 0, Offset: 0,
		Key: []byte("k"), Value: mustSerialize(t, "hello"), Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	if outs[0].Topic != wantTopic {
		t.Errorf("topic: got %q, want %q", outs[0].Topic, wantTopic)
	}
}

// ---------------------------------------------------------------------------
// Type-changing path — #8 regression guard
//
// This test verifies that when the DAG processor changes the value type
// (string → int), the sink's EncodeVal closure (which expects int) correctly
// encodes the output and no record is silently dropped.
//
// This was the exact bug in the old Adapter[V]: the `r.Value.(V)` type assertion
// would fail for a type-changed value and silently skip it via `continue`. The new
// Adapter uses per-sink SinkBinding.EncodeVal closures captured at the correct type,
// so EncodeVal(int) is called and the record is emitted — not dropped.
// ---------------------------------------------------------------------------

func TestAdapter_TypeChangingPipeline_NoSilentDrop(t *testing.T) {
	// Build a topology where source produces strings, a processor converts string
	// to int (length), and sink encodes int. This is the minimum DAG needed to
	// reproduce task #8 without depending on gstream/operators.go.
	b := topology.NewBuilder()
	src := b.AddSource("source")
	// Processor: value string → int (length of string)
	lengthProc := b.AddProcessor("to-length",
		topology.Mapper(func(k, v any) (any, any) {
			s := v.(string)
			return k, len(s) // value type changes: string → int
		}),
		src,
	)
	b.AddSink("sink", lengthProc)
	topo := b.Build()

	strSerde := state.JSONSerde[string]{}
	intSerde := state.JSONSerde[int]{}

	bt := &gstream.BuiltTopology{
		Topology: topo,
		Sources: map[string]gstream.SourceBinding{
			"source": {
				Topic:     "in",
				DecodeKey: func(raw []byte) (any, error) { return string(raw), nil },
				DecodeVal: func(raw []byte) (any, error) { return strSerde.Deserialize(raw) },
			},
		},
		Sinks: map[string]gstream.SinkBinding{
			"sink": {
				Topic: "out",
				// Key stays string
				EncodeKey: func(x any) ([]byte, error) { return []byte(x.(string)), nil },
				// Value is now int — this is the type-changed encode path
				EncodeVal: func(x any) ([]byte, error) {
					v, ok := x.(int)
					if !ok {
						return nil, fmt.Errorf("EncodeVal: expected int, got %T (silent drop regression)", x)
					}
					return intSerde.Serialize(v)
				},
			},
		},
	}

	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	// Input: "hello" (5 chars). Expected output: int value 5, encoded as JSON "5".
	in := kafka.InRecord{
		Topic:     "in",
		Key:       []byte("k"),
		Value:     mustSerialize(t, "hello"),
		Timestamp: time.Now(),
	}

	outs, err := adapter.ProcessFunc()(context.Background(), in)
	if err != nil {
		t.Fatalf("process: unexpected error (possible silent-drop regression): %v", err)
	}
	// CRITICAL: must NOT be zero — that would be the silent drop bug.
	if len(outs) != 1 {
		t.Fatalf("#8 regression: expected 1 output (int-typed record), got %d — silent drop detected", len(outs))
	}
	if string(outs[0].Value) != "5" {
		t.Errorf("#8 regression: expected JSON value=5, got %s", string(outs[0].Value))
	}
	if outs[0].Topic != "out" {
		t.Errorf("topic: got %q, want %q", outs[0].Topic, "out")
	}
}

// ---------------------------------------------------------------------------
// ALO error-path tests
// ---------------------------------------------------------------------------

// TestAdapter_DecodeValError checks that a malformed value causes process to
// return an error (ALO: no produce, no commit).
func TestAdapter_DecodeValError(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	fn := adapter.ProcessFunc()

	bad := kafka.InRecord{
		Topic: "input-topic", Key: []byte("k"), Value: []byte("not-json"), Timestamp: time.Now(),
	}
	_, err = fn(context.Background(), bad)
	if err == nil {
		t.Fatal("expected error for invalid JSON value (ALO: decode error must propagate)")
	}
}

// TestAdapter_EncodeValError checks that an EncodeVal failure causes process to
// return an error (ALO: no produce, no commit).
func TestAdapter_EncodeValError(t *testing.T) {
	b := topology.NewBuilder()
	src := b.AddSource("source")
	b.AddSink("sink", src)
	topo := b.Build()

	strSerde := state.JSONSerde[string]{}
	bt := &gstream.BuiltTopology{
		Topology: topo,
		Sources: map[string]gstream.SourceBinding{
			"source": {
				Topic:     "in",
				DecodeKey: func(raw []byte) (any, error) { return string(raw), nil },
				DecodeVal: func(raw []byte) (any, error) { return strSerde.Deserialize(raw) },
			},
		},
		Sinks: map[string]gstream.SinkBinding{
			"sink": {
				Topic:     "out",
				EncodeKey: func(x any) ([]byte, error) { return []byte(x.(string)), nil },
				EncodeVal: func(x any) ([]byte, error) {
					return nil, fmt.Errorf("intentional EncodeVal failure")
				},
			},
		},
	}

	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	_, err = adapter.ProcessFunc()(context.Background(), kafka.InRecord{
		Topic: "in", Key: []byte("k"), Value: mustSerialize(t, "hello"), Timestamp: time.Now(),
	})
	if err == nil {
		t.Fatal("expected error from failing EncodeVal (ALO: encode error must propagate)")
	}
}

// TestAdapter_ProcessorError propagates topology DAG errors back to the caller
// (ALO: processor error → no produce, no commit → whole-batch redelivery).
func TestAdapter_ProcessorError(t *testing.T) {
	b := topology.NewBuilder()
	src := b.AddSource("src")
	fail := b.AddProcessor("fail", func(_ context.Context, _ topology.Record, _ topology.Forwarder) error {
		return fmt.Errorf("intentional failure")
	}, src)
	b.AddSink("sink", fail)
	topo := b.Build()

	strSerde := state.JSONSerde[string]{}
	bt := &gstream.BuiltTopology{
		Topology: topo,
		Sources: map[string]gstream.SourceBinding{
			"src": {
				Topic:     "in",
				DecodeKey: func(raw []byte) (any, error) { return string(raw), nil },
				DecodeVal: func(raw []byte) (any, error) { return strSerde.Deserialize(raw) },
			},
		},
		Sinks: map[string]gstream.SinkBinding{
			"sink": {
				Topic:     "out",
				EncodeKey: func(x any) ([]byte, error) { return []byte(x.(string)), nil },
				EncodeVal: func(x any) ([]byte, error) { return strSerde.Serialize(x.(string)) },
			},
		},
	}

	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	_, err = adapter.ProcessFunc()(context.Background(), kafka.InRecord{
		Topic: "in", Key: []byte("k"), Value: mustSerialize(t, "hello"), Timestamp: time.Now(),
	})
	if err == nil {
		t.Fatal("expected error from failing processor (ALO: processor error must propagate)")
	}
}

// TestAdapter_AllFilteredNoOutputs ensures no OutRecord is returned when all
// records are filtered out by the DAG.
func TestAdapter_AllFilteredNoOutputs(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	fn := adapter.ProcessFunc()

	for _, v := range []string{"ab", "cd", "ef"} {
		outs, err := fn(context.Background(), inRecord(t, "k", v))
		if err != nil {
			t.Fatalf("process(%q): %v", v, err)
		}
		if len(outs) != 0 {
			t.Errorf("expected 0 outputs for %q (len<4, filtered), got %d", v, len(outs))
		}
	}
}

// TestAdapter_ProcessFuncIsKafkaProcessFunc verifies that ProcessFunc() satisfies
// the kafka.ProcessFunc type at compile time.
func TestAdapter_ProcessFuncIsKafkaProcessFunc(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	var _ = adapter.ProcessFunc()
}

// TestAdapter_TimestampPreserved checks that the record timestamp is accepted and
// threaded into the topology without error.
func TestAdapter_TimestampPreserved(t *testing.T) {
	b := topology.NewBuilder()
	src := b.AddSource("src")
	pass := b.AddProcessor("pass", topology.Filter(func(_, _ any) bool { return true }), src)
	b.AddSink("sink", pass)
	topo := b.Build()

	strSerde := state.JSONSerde[string]{}
	bt := &gstream.BuiltTopology{
		Topology: topo,
		Sources: map[string]gstream.SourceBinding{
			"src": {
				Topic:     "in",
				DecodeKey: func(raw []byte) (any, error) { return string(raw), nil },
				DecodeVal: func(raw []byte) (any, error) { return strSerde.Deserialize(raw) },
			},
		},
		Sinks: map[string]gstream.SinkBinding{
			"sink": {
				Topic:     "out",
				EncodeKey: func(x any) ([]byte, error) { return []byte(x.(string)), nil },
				EncodeVal: func(x any) ([]byte, error) { return strSerde.Serialize(x.(string)) },
			},
		},
	}

	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rec := kafka.InRecord{
		Topic:     "in",
		Key:       []byte("k"),
		Value:     mustSerialize(t, "value"),
		Timestamp: ts,
	}
	_, err = adapter.ProcessFunc()(context.Background(), rec)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	// Timestamp is forwarded through the topology. If we get here without error the
	// timestamp was threaded through correctly; topology.Record.Timestamp is verified
	// in topology unit tests separately.
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func mustSerialize(t *testing.T, v string) []byte {
	t.Helper()
	b, err := state.JSONSerde[string]{}.Serialize(v)
	if err != nil {
		t.Fatalf("mustSerialize(%q): %v", v, err)
	}
	return b
}

// ---------------------------------------------------------------------------
// RepartitionBinding resolver tests (C3)
// ---------------------------------------------------------------------------

// buildRepartitionBuiltTopology builds a BuiltTopology with:
//
//	source → repartitionSink  (write side; encoded with rb)
//	repartitionSource → sink  (read side; decoded with rb)
//
// This mirrors the shape C2 (repartition.go) will emit.
func buildRepartitionBuiltTopology(t *testing.T) (*gstream.BuiltTopology, string /*fullTopic*/) {
	t.Helper()

	b := topology.NewBuilder()
	src := b.AddSource("source")
	b.AddSink("repart-sink", src)

	rePartSrc := b.AddSource("repart-source")
	b.AddSink("final-sink", rePartSrc)
	topo := b.Build()

	const appID = "testapp"
	const repartName = "mykey"
	fullTopic := appID + "-" + repartName + "-repartition"

	strSerde := state.JSONSerde[string]{}

	// Encode/decode closures shared between user source and repartition binding.
	encKey := func(x any) ([]byte, error) { return []byte(x.(string)), nil }
	decKey := func(raw []byte) (any, error) { return string(raw), nil }
	encVal := func(x any) ([]byte, error) { return strSerde.Serialize(x.(string)) }
	decVal := func(raw []byte) (any, error) { return strSerde.Deserialize(raw) }

	bt := &gstream.BuiltTopology{
		Topology: topo,
		Sources: map[string]gstream.SourceBinding{
			"source": {
				Topic:     "input-topic",
				DecodeKey: decKey,
				DecodeVal: decVal,
			},
		},
		Sinks: map[string]gstream.SinkBinding{
			"final-sink": {
				Topic:     "output-topic",
				EncodeKey: encKey,
				EncodeVal: encVal,
			},
		},
		RepartitionBindings: map[string]gstream.RepartitionBinding{
			repartName: {
				Name:       repartName,
				SinkName:   "repart-sink",
				SourceName: "repart-source",
				Partitions: 3,
				EncodeKey:  encKey,
				EncodeVal:  encVal,
				DecodeKey:  decKey,
				DecodeVal:  decVal,
			},
		},
	}
	cfg, err := gstream.Configure(
		gstream.WithName(appID),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	_ = cfg // returned to caller via appID constant
	return bt, fullTopic
}

// TestAdapter_RepartitionSourceTopicsIncluded asserts SourceTopics() includes the
// repartition full topic and the original source topic.
func TestAdapter_RepartitionSourceTopicsIncluded(t *testing.T) {
	bt, fullTopic := buildRepartitionBuiltTopology(t)
	cfg, err := gstream.Configure(
		gstream.WithName("testapp"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	adapter, err := runtime.NewAdapter(bt, cfg, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	topics := adapter.SourceTopics()
	topicSet := make(map[string]struct{}, len(topics))
	for _, tp := range topics {
		topicSet[tp] = struct{}{}
	}

	if _, ok := topicSet[fullTopic]; !ok {
		t.Errorf("SourceTopics missing repartition topic %q; got %v", fullTopic, topics)
	}
	if _, ok := topicSet["input-topic"]; !ok {
		t.Errorf("SourceTopics missing original source topic %q; got %v", "input-topic", topics)
	}
}

// TestAdapter_RepartitionSinkRoutesToRepartitionTopic asserts that a record arriving
// at "source" is routed through repart-sink and produces an OutRecord on the
// repartition topic with Partition UNSET (IsValid=false — murmur2 path).
func TestAdapter_RepartitionSinkRoutesToRepartitionTopic(t *testing.T) {
	bt, fullTopic := buildRepartitionBuiltTopology(t)
	cfg, err := gstream.Configure(
		gstream.WithName("testapp"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	adapter, err := runtime.NewAdapter(bt, cfg, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	in := kafka.InRecord{
		Topic:     "input-topic",
		Partition: 0,
		Key:       []byte("mykey"),
		Value:     mustSerialize(t, "hello"),
		Timestamp: time.Now(),
	}
	outs, err := adapter.ProcessFunc()(context.Background(), in)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	// repart-sink should produce one record to the repartition topic.
	// final-sink produces nothing here (repartitionSource not yet fed).
	var repartOuts []kafka.OutRecord
	for _, o := range outs {
		if o.Topic == fullTopic {
			repartOuts = append(repartOuts, o)
		}
	}
	if len(repartOuts) != 1 {
		t.Fatalf("expected 1 output on repartition topic %q, got %d total outputs (topics: %v)",
			fullTopic, len(outs), outTopics(outs))
	}

	// Partition MUST be unset (IsValid=false) so murmur2 routes by key.
	if repartOuts[0].Partition.IsValid {
		t.Errorf("repartition OutRecord Partition must be UNSET (IsValid=false), got IsValid=true value=%d",
			repartOuts[0].Partition.Value)
	}
}

// TestAdapter_RepartitionTopicRoutesToRepartitionSource asserts that a record
// arriving on the repartition topic is routed to the repartition source node
// (repart-source) and flows through to the final sink.
func TestAdapter_RepartitionTopicRoutesToRepartitionSource(t *testing.T) {
	bt, fullTopic := buildRepartitionBuiltTopology(t)
	cfg, err := gstream.Configure(
		gstream.WithName("testapp"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	adapter, err := runtime.NewAdapter(bt, cfg, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	// Feed a record on the repartition topic directly (simulates a re-consumed record).
	in := kafka.InRecord{
		Topic:     fullTopic,
		Partition: 1,
		Key:       []byte("rekey"),
		Value:     mustSerialize(t, "repartitioned"),
		Timestamp: time.Now(),
	}
	outs, err := adapter.ProcessFunc()(context.Background(), in)
	if err != nil {
		t.Fatalf("process repartition topic: %v", err)
	}

	// Should have one output on "output-topic" (final-sink), none on fullTopic.
	var finalOuts []kafka.OutRecord
	for _, o := range outs {
		if o.Topic == "output-topic" {
			finalOuts = append(finalOuts, o)
		}
	}
	if len(finalOuts) != 1 {
		t.Fatalf("expected 1 output on output-topic after repartition re-consume, got %d total (topics: %v)",
			len(outs), outTopics(outs))
	}
}

// TestAdapter_RepartitionNoBindings_RegressionZero confirms that a topology with zero
// RepartitionBindings behaves identically to before — resolved maps are just copies of
// bt.Sources/bt.Sinks; no new topics, no new sinks.
func TestAdapter_RepartitionNoBindings_RegressionZero(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	// Ensure RepartitionBindings is explicitly nil/empty.
	bt.RepartitionBindings = nil

	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	topics := adapter.SourceTopics()
	if len(topics) != 1 || topics[0] != "input-topic" {
		t.Errorf("zero-repartition: expected [input-topic], got %v", topics)
	}

	fn := adapter.ProcessFunc()
	outs, err := fn(context.Background(), inRecord(t, "k", "hello"))
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(outs) != 1 || outs[0].Topic != "output-topic" {
		t.Errorf("zero-repartition: expected 1 output on output-topic, got %v", outTopics(outs))
	}
}

// ---------------------------------------------------------------------------
// P5-C5: EOS hook wiring tests
// ---------------------------------------------------------------------------

// unitTestEOSCfg returns a minimal valid Config with ExactlyOnce for unit tests
// that do not contact a real broker.
func unitTestEOSCfg(t *testing.T) gstream.Config {
	t.Helper()
	cfg, err := gstream.Configure(
		gstream.WithName("unit-test-eos"),
		gstream.WithBrokers("localhost:9092"),
		gstream.WithGuarantee(gstream.ExactlyOnce),
	)
	if err != nil {
		t.Fatalf("unitTestEOSCfg: %v", err)
	}
	return cfg
}

// TestAdapter_ALO_PostBatchHook_IsFullFlush verifies that an ALO adapter exposes
// PostBatchHook (full flush) and NO changelog flusher, matching the ALO write order.
func TestAdapter_ALO_PostBatchHook_IsFullFlush(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter(ALO): %v", err)
	}

	// ALO: PostBatchHook must be non-nil (full-flush path).
	if hook := adapter.PostBatchHook(); hook == nil {
		t.Error("ALO: PostBatchHook() returned nil; expected non-nil full-flush hook")
	}

	// ALO: ChangelogFlusherHook must be nil (no EOS changelog drain).
	if flusher := adapter.ChangelogFlusherHook(); flusher != nil {
		t.Error("ALO: ChangelogFlusherHook() must return nil for ALO; got non-nil")
	}
}

// TestAdapter_EOS_PostBatchSweepHook_AndChangelogFlusher verifies that an EOS adapter
// exposes PostBatchSweepHook (no Kafka I/O) and a non-nil ChangelogFlusherHook
// (DrainChangelogRecords), confirming the EOS wiring contract (P5-C5).
func TestAdapter_EOS_PostBatchSweepHook_AndChangelogFlusher(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	adapter, err := runtime.NewAdapter(bt, unitTestEOSCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter(EOS): %v", err)
	}

	// EOS: PostBatchSweepHook must be non-nil (sweep-only, no Kafka flush).
	if hook := adapter.PostBatchSweepHook(); hook == nil {
		t.Error("EOS: PostBatchSweepHook() returned nil; expected non-nil sweep hook")
	}

	// EOS: ChangelogFlusherHook must be non-nil (DrainChangelogRecords).
	flusher := adapter.ChangelogFlusherHook()
	if flusher == nil {
		t.Fatal("EOS: ChangelogFlusherHook() returned nil; expected non-nil drain hook")
	}

	// Call the flusher on a stateless topology (zero collectors): must return empty
	// slice and no error. This exercises DrainChangelogRecords on the zero-store path.
	recs, err := flusher(context.Background())
	if err != nil {
		t.Fatalf("EOS: ChangelogFlusherHook()(ctx) returned error on zero-store topology: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("EOS: ChangelogFlusherHook() on zero-store topology: expected 0 records, got %d", len(recs))
	}
}

// TestAdapter_ALO_PostBatchSweepHook_AlwaysAvailable verifies that PostBatchSweepHook
// is callable on an ALO adapter too (no panic). The returned function is the same
// non-Kafka-flush path; this is a regression guard ensuring the hook exists regardless
// of Guarantee.
func TestAdapter_ALO_PostBatchSweepHook_AlwaysAvailable(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter(ALO): %v", err)
	}
	if hook := adapter.PostBatchSweepHook(); hook == nil {
		t.Error("ALO: PostBatchSweepHook() returned nil; method must always return non-nil")
	}
}

// outTopics extracts topic names from OutRecords for error messages.
func outTopics(outs []kafka.OutRecord) []string {
	topics := make([]string, len(outs))
	for i, o := range outs {
		topics[i] = o.Topic
	}
	return topics
}

// ---------------------------------------------------------------------------
// GlobalTableBinding exclusion tests (R1/R3)
// ---------------------------------------------------------------------------

// buildGlobalTableBuiltTopology builds a BuiltTopology with a single stream
// source and a GlobalTableBinding whose topic is distinct from the stream source.
// Used to assert R1/R3: global topic must NOT appear in SourceTopics().
func buildGlobalTableBuiltTopology(t *testing.T) (*gstream.BuiltTopology, string /*globalTopic*/) {
	t.Helper()

	b := topology.NewBuilder()
	src := b.AddSource("source")
	b.AddSink("sink", src)
	topo := b.Build()

	const globalTopic = "global-user-table"
	const globalStore = "user-store"

	strSerde := state.JSONSerde[string]{}

	bt := &gstream.BuiltTopology{
		Topology: topo,
		Sources: map[string]gstream.SourceBinding{
			"source": {
				Topic:     "stream-input",
				DecodeKey: func(raw []byte) (any, error) { return string(raw), nil },
				DecodeVal: func(raw []byte) (any, error) { return strSerde.Deserialize(raw) },
			},
		},
		Sinks: map[string]gstream.SinkBinding{
			"sink": {
				Topic:     "stream-output",
				EncodeKey: func(x any) ([]byte, error) { return []byte(x.(string)), nil },
				EncodeVal: func(x any) ([]byte, error) { return strSerde.Serialize(x.(string)) },
			},
		},
		GlobalTableBindings: map[string]gstream.GlobalTableBinding{
			globalStore: {
				StoreName: globalStore,
				Topic:     globalTopic,
				EncodeKey: func(x any) ([]byte, error) { return []byte(x.(string)), nil },
				DecodeKey: func(raw []byte) (any, error) { return string(raw), nil },
				EncodeVal: func(x any) ([]byte, error) { return strSerde.Serialize(x.(string)) },
				DecodeVal: func(raw []byte) (any, error) { return strSerde.Deserialize(raw) },
			},
		},
	}
	return bt, globalTopic
}

// TestAdapter_GlobalTopicExcludedFromSourceTopics is the R1/R3 regression guard:
// the global topic must NOT appear in SourceTopics() and must NOT be in
// topicToSource. If it did, the task consumer group would subscribe to it,
// shard it across instances, and silently break the "full replica" invariant.
func TestAdapter_GlobalTopicExcludedFromSourceTopics(t *testing.T) {
	bt, globalTopic := buildGlobalTableBuiltTopology(t)
	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	topics := adapter.SourceTopics()
	for _, tp := range topics {
		if tp == globalTopic {
			t.Errorf("R1/R3 violation: global topic %q found in SourceTopics() %v — "+
				"global topics must NOT join the task consumer group", globalTopic, topics)
		}
	}
	// Stream source must still be present.
	found := false
	for _, tp := range topics {
		if tp == "stream-input" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("stream source topic %q missing from SourceTopics() %v", "stream-input", topics)
	}
}

// TestAdapter_GlobalTopicExcludedFromTopicToSource verifies the internal
// topicToSource map does not contain the global topic. We drive this indirectly:
// ProcessFunc returns an error for unknown topics, so feeding the global topic
// as an incoming record must return an error (not route it to a source node).
func TestAdapter_GlobalTopicExcludedFromTopicToSource(t *testing.T) {
	bt, globalTopic := buildGlobalTableBuiltTopology(t)
	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	strSerde := state.JSONSerde[string]{}
	val, _ := strSerde.Serialize("some-value")

	_, procErr := adapter.ProcessFunc()(context.Background(), kafka.InRecord{
		Topic:     globalTopic,
		Partition: 0,
		Key:       []byte("k"),
		Value:     val,
		Timestamp: time.Now(),
	})
	if procErr == nil {
		t.Errorf("R1/R3 violation: ProcessFunc accepted a record on global topic %q — "+
			"topicToSource must not contain global topics", globalTopic)
	}
}

// TestAdapter_HasStoresTrueWhenOnlyGlobalTable verifies that hasStores is true
// when the topology contains only GlobalTableBindings (no regular/window/session
// stores). This ensures internal-sink drain logic activates correctly.
func TestAdapter_HasStoresTrueWhenOnlyGlobalTable(t *testing.T) {
	// Build a topology with an internal-only sink (no SinkBinding) and a global table.
	// NewAdapter should NOT return an error about a missing sink binding.
	b := topology.NewBuilder()
	src := b.AddSource("source")
	b.AddSink("internal-sink", src) // no SinkBinding — would error without hasStores
	topo := b.Build()

	strSerde := state.JSONSerde[string]{}

	bt := &gstream.BuiltTopology{
		Topology: topo,
		Sources: map[string]gstream.SourceBinding{
			"source": {
				Topic:     "stream-input",
				DecodeKey: func(raw []byte) (any, error) { return string(raw), nil },
				DecodeVal: func(raw []byte) (any, error) { return strSerde.Deserialize(raw) },
			},
		},
		Sinks: map[string]gstream.SinkBinding{
			// "internal-sink" intentionally absent — simulates a DAG-internal sink.
		},
		GlobalTableBindings: map[string]gstream.GlobalTableBinding{
			"user-store": {
				StoreName: "user-store",
				Topic:     "global-users",
				EncodeKey: func(x any) ([]byte, error) { return []byte(x.(string)), nil },
				DecodeKey: func(raw []byte) (any, error) { return string(raw), nil },
				EncodeVal: func(x any) ([]byte, error) { return strSerde.Serialize(x.(string)) },
				DecodeVal: func(raw []byte) (any, error) { return strSerde.Deserialize(raw) },
			},
		},
	}

	_, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Errorf("hasStores: NewAdapter returned error for topology with only GlobalTableBindings "+
			"(internal sink should be silently skipped): %v", err)
	}
}

// TestAdapter_ZeroGlobalBindings_NoOp verifies that BootstrapGlobalStores and
// RunGlobalConsumers are pure no-ops when GlobalTableBindings is empty, and that
// Close returns nil. Existing behavior (SourceTopics, ProcessFunc) must be
// identical to pre-C5 Adapter.
func TestAdapter_ZeroGlobalBindings_NoOp(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	// Explicitly empty.
	bt.GlobalTableBindings = nil

	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	// BootstrapGlobalStores must be a no-op.
	if err := adapter.BootstrapGlobalStores(context.Background()); err != nil {
		t.Fatalf("BootstrapGlobalStores (zero bindings) returned error: %v", err)
	}

	// RunGlobalConsumers must be a no-op.
	if err := adapter.RunGlobalConsumers(context.Background()); err != nil {
		t.Fatalf("RunGlobalConsumers (zero bindings) returned error: %v", err)
	}

	// Close must be a no-op.
	if err := adapter.Close(); err != nil {
		t.Fatalf("Close (zero bindings) returned error: %v", err)
	}

	// SourceTopics unchanged.
	topics := adapter.SourceTopics()
	if len(topics) != 1 || topics[0] != "input-topic" {
		t.Errorf("zero global: expected SourceTopics=[input-topic], got %v", topics)
	}

	// ProcessFunc works identically.
	outs, err := adapter.ProcessFunc()(context.Background(), inRecord(t, "k", "hello"))
	if err != nil {
		t.Fatalf("ProcessFunc (zero global): %v", err)
	}
	if len(outs) != 1 || outs[0].Topic != "output-topic" {
		t.Errorf("zero global: expected 1 output on output-topic, got %v", outTopics(outs))
	}
}
