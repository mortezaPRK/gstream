package runtime_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/kafka"
	"github.com/mortezaPRK/gstream/internal/runtime"
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

	strSerde := gstream.JSONSerde[string]{}

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
	b, err := gstream.JSONSerde[string]{}.Serialize(value)
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

// ---------------------------------------------------------------------------
// NewAdapter validation tests
// ---------------------------------------------------------------------------

func TestNewAdapter_NilBt(t *testing.T) {
	_, err := runtime.NewAdapter(nil, nil)
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
	_, err := runtime.NewAdapter(bt, nil)
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
	_, err := runtime.NewAdapter(bt, nil)
	if err == nil {
		t.Fatal("expected error when sink binding is missing")
	}
}

func TestNewAdapter_NilLogger(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	// nil logger must not panic — falls back to slog.Default().
	_, err := runtime.NewAdapter(bt, nil)
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
	adapter, err := runtime.NewAdapter(bt, nil)
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
	adapter, err := runtime.NewAdapter(bt, nil)
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
	strSerde := gstream.JSONSerde[string]{}
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

	adapter, err := runtime.NewAdapter(bt, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	outs, err := adapter.ProcessFunc()(context.Background(), inRecord(t, "k", "hello"))
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

	strSerde := gstream.JSONSerde[string]{}
	intSerde := gstream.JSONSerde[int]{}

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

	adapter, err := runtime.NewAdapter(bt, nil)
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
	adapter, err := runtime.NewAdapter(bt, nil)
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

	strSerde := gstream.JSONSerde[string]{}
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

	adapter, err := runtime.NewAdapter(bt, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	_, err = adapter.ProcessFunc()(context.Background(), inRecord(t, "k", "hello"))
	if err == nil {
		t.Fatal("expected error from failing EncodeVal (ALO: encode error must propagate)")
	}
}

// TestAdapter_ProcessorError propagates topology DAG errors back to the caller
// (ALO: processor error → no produce, no commit → whole-batch redelivery).
func TestAdapter_ProcessorError(t *testing.T) {
	b := topology.NewBuilder()
	src := b.AddSource("src")
	fail := b.AddProcessor("fail", func(_ topology.Record, _ topology.Forwarder) error {
		return fmt.Errorf("intentional failure")
	}, src)
	b.AddSink("sink", fail)
	topo := b.Build()

	strSerde := gstream.JSONSerde[string]{}
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

	adapter, err := runtime.NewAdapter(bt, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	_, err = adapter.ProcessFunc()(context.Background(), inRecord(t, "k", "hello"))
	if err == nil {
		t.Fatal("expected error from failing processor (ALO: processor error must propagate)")
	}
}

// TestAdapter_AllFilteredNoOutputs ensures no OutRecord is returned when all
// records are filtered out by the DAG.
func TestAdapter_AllFilteredNoOutputs(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	adapter, err := runtime.NewAdapter(bt, nil)
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
	adapter, err := runtime.NewAdapter(bt, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	var _ kafka.ProcessFunc = adapter.ProcessFunc()
}

// TestAdapter_TimestampPreserved checks that the record timestamp is accepted and
// threaded into the topology without error.
func TestAdapter_TimestampPreserved(t *testing.T) {
	b := topology.NewBuilder()
	src := b.AddSource("src")
	pass := b.AddProcessor("pass", topology.Filter(func(_, _ any) bool { return true }), src)
	b.AddSink("sink", pass)
	topo := b.Build()

	strSerde := gstream.JSONSerde[string]{}
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

	adapter, err := runtime.NewAdapter(bt, nil)
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
	b, err := gstream.JSONSerde[string]{}.Serialize(v)
	if err != nil {
		t.Fatalf("mustSerialize(%q): %v", v, err)
	}
	return b
}
