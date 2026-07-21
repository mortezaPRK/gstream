package runtime_test

import (
	"context"
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/kafka"
	"github.com/mortezaPRK/gstream/internal/runtime"
	"github.com/mortezaPRK/gstream/internal/state"
	"github.com/mortezaPRK/gstream/internal/topology"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// buildStatefulTopology builds a BuiltTopology with a single count store
// so we can exercise the stateful adapter path in unit tests (without a real
// Kafka broker — topology processing only, no actual changelog flush).
func buildStatefulTopology(t *testing.T) *gstream.BuiltTopology {
	t.Helper()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](
		b,
		"input-topic",
		"source",
		gstream.JSONSerde[string]{},
		gstream.JSONSerde[string]{},
	)
	// Count produces a ktable-out internal sink; the public sinks map is empty.
	src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		Count("word-count")
	return b.Build()
}

// ---------------------------------------------------------------------------
// NewAdapter — stateful validation
// ---------------------------------------------------------------------------

// TestNewAdapterWithConfig_NilBt verifies that nil bt returns an error.
func TestNewAdapterWithConfig_NilBt(t *testing.T) {
	_, err := runtime.NewAdapterWithConfig(nil, gstream.Config{}, nil)
	if err == nil {
		t.Fatal("expected error for nil bt")
	}
}

// TestNewAdapterWithConfig_StatefulReturnsAdapter verifies that a stateful
// topology (StoreBindings non-empty) constructs successfully.
func TestNewAdapterWithConfig_StatefulReturnsAdapter(t *testing.T) {
	bt := buildStatefulTopology(t)
	if len(bt.StoreBindings) == 0 {
		t.Fatal("buildStatefulTopology returned no StoreBindings")
	}
	// cfg has no brokers — fine for unit test (no I/O happens at construction time).
	cfg := gstream.Config{
		ApplicationID: "test-stateful",
		Brokers:       []string{"localhost:9092"},
	}
	cfg.ApplyDefaults()
	adapter, err := runtime.NewAdapterWithConfig(bt, cfg, nil)
	if err != nil {
		t.Fatalf("NewAdapterWithConfig stateful: %v", err)
	}
	if adapter == nil {
		t.Fatal("expected non-nil adapter")
	}
}

// TestAdapter_LifecycleCallbacksNilForStateless verifies that stateless
// topologies return nil callbacks.
func TestAdapter_LifecycleCallbacksNilForStateless(t *testing.T) {
	bt := buildSimpleBuiltTopology(t) // from adapter_test.go helper
	adapter, err := runtime.NewAdapter(bt, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	onAssigned, onRevoked := adapter.LifecycleCallbacks()
	if onAssigned != nil || onRevoked != nil {
		t.Error("expected nil lifecycle callbacks for stateless topology")
	}
	if adapter.PostBatchHook() != nil {
		t.Error("expected nil PostBatchHook for stateless topology")
	}
}

// TestAdapter_LifecycleCallbacksNonNilForStateful verifies that stateful
// topologies return non-nil callbacks.
func TestAdapter_LifecycleCallbacksNonNilForStateful(t *testing.T) {
	bt := buildStatefulTopology(t)
	cfg := gstream.Config{ApplicationID: "test-app", Brokers: []string{"localhost:9092"}}
	cfg.ApplyDefaults()
	adapter, err := runtime.NewAdapterWithConfig(bt, cfg, nil)
	if err != nil {
		t.Fatalf("NewAdapterWithConfig: %v", err)
	}
	onAssigned, onRevoked := adapter.LifecycleCallbacks()
	if onAssigned == nil {
		t.Error("expected non-nil onAssigned for stateful topology")
	}
	if onRevoked == nil {
		t.Error("expected non-nil onRevoked for stateful topology")
	}
	if adapter.PostBatchHook() == nil {
		t.Error("expected non-nil PostBatchHook for stateful topology")
	}
}

// ---------------------------------------------------------------------------
// Stateless path regression guard
// ---------------------------------------------------------------------------

// TestStatelessAdapterUnchanged verifies that the stateless Adapter (no
// StoreBindings) still behaves exactly as before: it processes records through
// the TestDriver and returns encoded outputs.
func TestStatelessAdapterUnchanged(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	adapter, err := runtime.NewAdapter(bt, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	fn := adapter.ProcessFunc()

	outs, err := fn(context.Background(), inRecord(t, "k", "hello"))
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	if string(outs[0].Value) != `"HELLO"` {
		t.Errorf("expected HELLO, got %s", outs[0].Value)
	}
}

// ---------------------------------------------------------------------------
// kafka.Client — WithLifecycle / WithPostBatch option smoke tests
// ---------------------------------------------------------------------------

// TestClientOption_WithLifecycle_NoError verifies that New() with WithLifecycle
// succeeds and the resulting client is non-nil.
func TestClientOption_WithLifecycle_NoError(t *testing.T) {
	cfg := gstream.Config{
		ApplicationID: "test-lifecycle",
		Brokers:       []string{"localhost:9092"},
	}
	cfg.ApplyDefaults()

	called := false
	cl, err := kafka.New(cfg, []string{"topic"}, nil,
		kafka.WithLifecycle(
			func(ctx context.Context, assigned map[string][]int32) error {
				called = true
				return nil
			},
			func(ctx context.Context, revoked map[string][]int32) {
				// no-op
			},
		),
	)
	if err != nil {
		t.Fatalf("kafka.New with WithLifecycle: %v", err)
	}
	if cl == nil {
		t.Fatal("expected non-nil client")
	}
	_ = called // callbacks are invoked by kgo on rebalance, not at construction
	cl.Close()
}

// TestClientOption_WithPostBatch_NoError verifies that New() with WithPostBatch
// succeeds.
func TestClientOption_WithPostBatch_NoError(t *testing.T) {
	cfg := gstream.Config{
		ApplicationID: "test-postbatch",
		Brokers:       []string{"localhost:9092"},
	}
	cfg.ApplyDefaults()

	cl, err := kafka.New(cfg, []string{"topic"}, nil,
		kafka.WithPostBatch(func(ctx context.Context) error {
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("kafka.New with WithPostBatch: %v", err)
	}
	if cl == nil {
		t.Fatal("expected non-nil client")
	}
	cl.Close()
}

// TestClientOption_NoOptsPreservesExistingBehaviour verifies that New() with
// no options (the old call signature) continues to work.
func TestClientOption_NoOptsPreservesExistingBehaviour(t *testing.T) {
	cfg := gstream.Config{
		ApplicationID: "test-noopts",
		Brokers:       []string{"localhost:9092"},
	}
	cfg.ApplyDefaults()

	cl, err := kafka.New(cfg, []string{"topic"}, nil)
	if err != nil {
		t.Fatalf("kafka.New no opts: %v", err)
	}
	if cl == nil {
		t.Fatal("expected non-nil client")
	}
	cl.Close()
}

// ---------------------------------------------------------------------------
// TaskManager.PostBatch — unit test without a real Pebble DB
// ---------------------------------------------------------------------------

// TestTaskManager_PostBatch_EmptyIsNoError verifies that PostBatch on a
// TaskManager with no tasks is a no-op.
func TestTaskManager_PostBatch_EmptyIsNoError(t *testing.T) {
	bt := buildStatefulTopology(t)
	cfg := gstream.Config{ApplicationID: "test-app", Brokers: []string{"localhost:9092"}}
	cfg.ApplyDefaults()
	adapter, err := runtime.NewAdapterWithConfig(bt, cfg, nil)
	if err != nil {
		t.Fatalf("NewAdapterWithConfig: %v", err)
	}
	hook := adapter.PostBatchHook()
	if hook == nil {
		t.Fatal("expected non-nil PostBatchHook")
	}
	// No tasks assigned → PostBatch should be a no-op.
	if err := hook(context.Background()); err != nil {
		t.Fatalf("PostBatch on empty TaskManager: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Adapter.ProcessFunc — stateful path error when no task is assigned
// ---------------------------------------------------------------------------

// TestStatefulAdapter_NoTask_ReturnsError verifies that processing a record for
// a partition that has no assigned task returns a descriptive error (ALO: batch
// aborted, no commit).
func TestStatefulAdapter_NoTask_ReturnsError(t *testing.T) {
	bt := buildStatefulTopology(t)
	cfg := gstream.Config{ApplicationID: "test-app", Brokers: []string{"localhost:9092"}}
	cfg.ApplyDefaults()
	adapter, err := runtime.NewAdapterWithConfig(bt, cfg, nil)
	if err != nil {
		t.Fatalf("NewAdapterWithConfig: %v", err)
	}

	fn := adapter.ProcessFunc()
	_, err = fn(context.Background(), kafka.InRecord{
		Topic:     "input-topic",
		Partition: 5, // no task for partition 5
		Key:       []byte(`"key"`),
		Value:     mustSerialize(t, "val"),
		Timestamp: time.Now(),
	})
	if err == nil {
		t.Fatal("expected error when no task is assigned for partition")
	}
}

// ---------------------------------------------------------------------------
// topology.Executor — per-partition independence (regression guard)
// ---------------------------------------------------------------------------

// TestExecutor_PerPartitionIndependence verifies that two Executor instances
// sharing the same *Topology but different stores maps do not interfere.
func TestExecutor_PerPartitionIndependence(t *testing.T) {
	b := topology.NewBuilder()
	src := b.AddSource("src")
	b.AddSink("sink", src)
	topo := b.Build()

	exec1 := topology.NewExecutor(topo, nil)
	exec2 := topology.NewExecutor(topo, nil)

	rec := topology.Record{Key: "k", Value: "v", Timestamp: 0}
	if err := exec1.Process("src", rec); err != nil {
		t.Fatalf("exec1.Process: %v", err)
	}
	// exec2 should have zero buffered records (independent buffers).
	recs, err := exec2.DrainSink("sink")
	if err != nil {
		t.Fatalf("exec2.DrainSink: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("expected 0 records in exec2 after processing via exec1, got %d", len(recs))
	}
	// exec1 should have exactly 1.
	recs, err = exec1.DrainSink("sink")
	if err != nil {
		t.Fatalf("exec1.DrainSink: %v", err)
	}
	if len(recs) != 1 {
		t.Errorf("expected 1 record in exec1 after processing, got %d", len(recs))
	}
}

// ---------------------------------------------------------------------------
// Byte-store stores-map construction — P2-S7fix regression guard
// ---------------------------------------------------------------------------

// TestBuildByteStoreAndExecuteCount verifies the EXACT construction path used by
// TaskManager.openTask (OnAssigned): a stores map built with
// NewKeyValueStoreWithChangelog[[]byte,[]byte] using gstream.BytesSerde{} is wired
// into a topology.Executor, and a Count processor runs end-to-end without error.
//
// This test is the runtime-level regression guard for the type-erasure boundary bug
// fixed in P2-S7fix. Before the fix, the runtime supplied an erasedStore that
// implemented kvStoreI[any,any]; the Aggregate processor asserted kvStoreI[K,A]
// (e.g. string,int64), which always failed because Go generics do not allow
// covariant interface satisfaction.
//
// After the fix (Option A):
//  1. grouped.go Aggregate asserts kvBytesStore instead of kvStoreI[K,A].
//  2. The runtime supplies *state.KeyValueStore[[]byte,[]byte] which satisfies kvBytesStore.
//  3. The processor encodes/decodes using the captured serdes.
//
// No broker is needed: we use OpenMemDB + topology.Executor directly.
func TestBuildByteStoreAndExecuteCount(t *testing.T) {
	t.Parallel()

	// Build a stateful topology with a Count processor.
	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](
		b, "input-topic", "source",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{},
	)
	src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		Count("word-count")
	bt := b.Build()

	// Construct the stores map the same way TaskManager.openTask does.
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	stores := make(map[string]any, len(bt.StoreBindings))
	for storeName := range bt.StoreBindings {
		// Exact replica of the openTask construction path.
		byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
			storeName,
			db,
			gstream.BytesSerde{},
			gstream.BytesSerde{},
			nil, // no changelog capture needed for this unit test
		)
		stores[storeName] = byteStore
	}

	exec := topology.NewExecutor(bt.Topology, stores)

	// Feed three records.
	for _, key := range []string{"foo", "bar", "foo"} {
		if err := exec.Process("source", topology.Record{Key: key, Value: key, Timestamp: 1}); err != nil {
			t.Fatalf("Process key=%q: unexpected error (P2-S7fix regression): %v", key, err)
		}
	}

	// Read back from the byte store and verify counts.
	byteStore := stores["word-count"].(*state.KeyValueStore[[]byte, []byte])
	keySerde := gstream.JSONSerde[string]{}
	intSerde := gstream.JSONSerde[int64]{}

	checkCount := func(key string, want int64) {
		t.Helper()
		kb, err := keySerde.Serialize(key)
		if err != nil {
			t.Fatalf("serialize key %q: %v", key, err)
		}
		vb, found, err := byteStore.Get(kb)
		if err != nil {
			t.Fatalf("store.Get(%q): %v", key, err)
		}
		if !found {
			t.Fatalf("store.Get(%q): key not found", key)
		}
		count, err := intSerde.Deserialize(vb)
		if err != nil {
			t.Fatalf("deserialize count for %q: %v", key, err)
		}
		if count != want {
			t.Errorf("count[%q]: got %d, want %d", key, count, want)
		}
	}

	checkCount("foo", 2) // processed twice
	checkCount("bar", 1) // processed once
}
