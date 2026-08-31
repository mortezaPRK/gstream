package runtime_test

import (
	"context"
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
		state.JSONSerde[string]{},
		state.JSONSerde[string]{},
	)
	// Count produces a ktable-out internal sink; the public sinks map is empty.
	src.GroupByKey(state.JSONSerde[string]{}, state.JSONSerde[string]{}).
		Count("word-count", state.JSONSerde[int64]{})
	return b.Build()
}

// ---------------------------------------------------------------------------
// NewAdapter — nil bt returns error
// ---------------------------------------------------------------------------

// TestNewAdapter_NilBt_WithConfig verifies that nil bt returns an error
// (mirrors TestNewAdapter_NilBt in adapter_test.go, here with a full config).
func TestNewAdapter_NilBt_WithConfig(t *testing.T) {
	_, err := runtime.NewAdapter(nil, unitTestCfg(t), nil)
	if err == nil {
		t.Fatal("expected error for nil bt")
	}
}

// TestNewAdapter_StatefulReturnsAdapter verifies that a stateful topology
// (StoreBindings non-empty) constructs successfully via the unified NewAdapter.
func TestNewAdapter_StatefulReturnsAdapter(t *testing.T) {
	bt := buildStatefulTopology(t)
	if len(bt.StoreBindings) == 0 {
		t.Fatal("buildStatefulTopology returned no StoreBindings")
	}
	cfg, err := gstream.Configure(
		gstream.WithName("test-stateful"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	adapter, err := runtime.NewAdapter(bt, cfg, nil)
	if err != nil {
		t.Fatalf("NewAdapter stateful: %v", err)
	}
	if adapter == nil {
		t.Fatal("expected non-nil adapter")
	}
}

// TestAdapter_LifecycleCallbacksNonNilForStateless verifies that stateless
// topologies return NON-nil callbacks (unified path — TaskManager always wired).
func TestAdapter_LifecycleCallbacksNonNilForStateless(t *testing.T) {
	bt := buildSimpleBuiltTopology(t) // from adapter_test.go helper
	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	onAssigned, onRevoked := adapter.LifecycleCallbacks()
	// Unified path: callbacks are always non-nil (TaskManager is always created).
	if onAssigned == nil {
		t.Error("expected non-nil onAssigned for zero-store topology (unified path)")
	}
	if onRevoked == nil {
		t.Error("expected non-nil onRevoked for zero-store topology (unified path)")
	}
	if adapter.PostBatchHook() == nil {
		t.Error("expected non-nil PostBatchHook for zero-store topology (unified path)")
	}
}

// TestAdapter_LifecycleCallbacksNonNilForStateful verifies that stateful
// topologies return non-nil callbacks.
func TestAdapter_LifecycleCallbacksNonNilForStateful(t *testing.T) {
	bt := buildStatefulTopology(t)
	cfg, err := gstream.Configure(
		gstream.WithName("test-app"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	adapter, err := runtime.NewAdapter(bt, cfg, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
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
// Stateless path regression guard (via zero-store TaskManager)
// ---------------------------------------------------------------------------

// TestStatelessAdapterUnchanged verifies that the stateless Adapter (no
// StoreBindings) still behaves exactly as before: it processes records through
// the zero-store TaskManager/Executor and returns encoded outputs.
func TestStatelessAdapterUnchanged(t *testing.T) {
	bt := buildSimpleBuiltTopology(t)
	adapter, err := runtime.NewAdapter(bt, unitTestCfg(t), nil)
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
	cfg, err := gstream.Configure(
		gstream.WithName("test-lifecycle"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

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
	cfg, err := gstream.Configure(
		gstream.WithName("test-postbatch"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

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
	cfg, err := gstream.Configure(
		gstream.WithName("test-noopts"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

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
	cfg, err := gstream.Configure(
		gstream.WithName("test-app"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	adapter, err := runtime.NewAdapter(bt, cfg, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
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
	cfg, err := gstream.Configure(
		gstream.WithName("test-app"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	adapter, err := runtime.NewAdapter(bt, cfg, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
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
	if err := exec1.Process(context.Background(), "src", rec); err != nil {
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
// NewKeyValueStoreWithChangelog[[]byte,[]byte] using state.BytesSerde{} is wired
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
		state.JSONSerde[string]{}, state.JSONSerde[string]{},
	)
	src.GroupByKey(state.JSONSerde[string]{}, state.JSONSerde[string]{}).
		Count("word-count", state.JSONSerde[int64]{})
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
			state.BytesSerde{},
			state.BytesSerde{},
			nil, // no changelog capture needed for this unit test
		)
		stores[storeName] = byteStore
	}

	exec := topology.NewExecutor(bt.Topology, stores)

	// Feed three records.
	for _, key := range []string{"foo", "bar", "foo"} {
		if err := exec.Process(context.Background(), "source", topology.Record{Key: key, Value: key, Timestamp: 1}); err != nil {
			t.Fatalf("Process key=%q: unexpected error (P2-S7fix regression): %v", key, err)
		}
	}

	// Read back from the byte store and verify counts.
	byteStore := stores["word-count"].(*state.KeyValueStore[[]byte, []byte])
	keySerde := state.JSONSerde[string]{}
	intSerde := state.JSONSerde[int64]{}

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

// ---------------------------------------------------------------------------
// TaskManager global-store injection (C4)
// ---------------------------------------------------------------------------

// buildZeroStoreBuiltTopology returns a BuiltTopology with no state stores so
// openTask uses the zero-store path (no Pebble DB, no broker required).
func buildZeroStoreBuiltTopology(t *testing.T) *gstream.BuiltTopology {
	t.Helper()
	b := topology.NewBuilder()
	src := b.AddSource("source")
	b.AddSink("sink", src)
	topo := b.Build()
	strSerde := state.JSONSerde[string]{}
	return &gstream.BuiltTopology{
		Topology: topo,
		Sources: map[string]gstream.SourceBinding{
			"source": {
				Topic:     "input-topic",
				DecodeKey: func(raw []byte) (any, error) { return string(raw), nil },
				DecodeVal: func(raw []byte) (any, error) { return strSerde.Deserialize(raw) },
			},
		},
		Sinks: map[string]gstream.SinkBinding{
			"sink": {
				Topic:     "output-topic",
				EncodeKey: func(x any) ([]byte, error) { return []byte(x.(string)), nil },
				EncodeVal: func(x any) ([]byte, error) { return strSerde.Serialize(x.(string)) },
			},
		},
		StoreBindings:        map[string]gstream.StoreBinding{},
		WindowStoreBindings:  map[string]gstream.WindowStoreBinding{},
		SessionStoreBindings: map[string]gstream.SessionStoreBinding{},
		GlobalTableBindings:  map[string]gstream.GlobalTableBinding{},
	}
}

// TestRegisterGlobalStore_InjectedIntoTask verifies that a store registered via
// RegisterGlobalStore appears in the per-partition stores map after openTask runs.
// Uses the zero-store (stateless) topology path so no broker is needed.
func TestRegisterGlobalStore_InjectedIntoTask(t *testing.T) {
	t.Parallel()

	bt := buildZeroStoreBuiltTopology(t)
	cfg, err := gstream.Configure(
		gstream.WithName("test-gstore"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	tm := runtime.NewTaskManager(bt, cfg, nil)

	// A fake store value — any pointer works; we assert pointer equality.
	fakeStore := &struct{ name string }{name: "fake-global"}

	tm.RegisterGlobalStore("gstore", fakeStore)

	// Trigger lazy task creation via Executor (zero-store path).
	exec := tm.Executor(0)
	if exec == nil {
		t.Fatal("expected non-nil executor for partition 0")
	}

	// Verify the injected store is the EXACT same instance.
	stores := runtime.TaskManagerStoresForPartition(tm, 0)
	if stores == nil {
		t.Fatal("expected non-nil stores map for partition 0")
	}
	got, ok := stores["gstore"]
	if !ok {
		t.Fatal("stores map does not contain 'gstore' key")
	}
	if got != fakeStore {
		t.Errorf("injected store pointer mismatch: got %p, want %p", got, fakeStore)
	}
}

// TestRegisterGlobalStore_ZeroGlobalStores_NoChange verifies that with no global
// stores registered, openTask behaves exactly as before (regression guard).
func TestRegisterGlobalStore_ZeroGlobalStores_NoChange(t *testing.T) {
	t.Parallel()

	bt := buildZeroStoreBuiltTopology(t)
	cfg, err := gstream.Configure(
		gstream.WithName("test-no-gstore"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	tm := runtime.NewTaskManager(bt, cfg, nil)
	// No RegisterGlobalStore call.

	exec := tm.Executor(0)
	if exec == nil {
		t.Fatal("expected non-nil executor for partition 0")
	}

	stores := runtime.TaskManagerStoresForPartition(tm, 0)
	if stores == nil {
		t.Fatal("expected non-nil stores map")
	}
	if len(stores) != 0 {
		t.Errorf("expected empty stores map with no global stores, got %d entries", len(stores))
	}
}

// TestAllChangelogTopics_ExcludesGlobalStores verifies that allChangelogTopics
// does NOT include global store names — global tables have no changelog and must
// NOT be checkpointed or restored through the changelog path.
func TestAllChangelogTopics_ExcludesGlobalStores(t *testing.T) {
	t.Parallel()

	// Build a BuiltTopology with both a regular store AND a global table binding.
	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](
		b, "input-topic", "source",
		state.JSONSerde[string]{}, state.JSONSerde[string]{},
	)
	src.GroupByKey(state.JSONSerde[string]{}, state.JSONSerde[string]{}).
		Count("regular-store", state.JSONSerde[int64]{})
	bt := b.Build()

	// Manually inject a GlobalTableBinding into the built topology so the test
	// covers a topology that has both kinds.
	bt.GlobalTableBindings["global-store"] = gstream.GlobalTableBinding{
		StoreName: "global-store",
		Topic:     "global-topic",
	}

	cfg, err := gstream.Configure(
		gstream.WithName("test-changelog"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	tm := runtime.NewTaskManager(bt, cfg, nil)
	topics := runtime.TaskManagerAllChangelogTopics(tm)

	// The regular store must appear.
	if _, ok := topics["regular-store"]; !ok {
		t.Error("allChangelogTopics missing regular store 'regular-store'")
	}

	// The global store must NOT appear.
	if _, ok := topics["global-store"]; ok {
		t.Error("allChangelogTopics incorrectly includes global store 'global-store'")
	}
}

// TestCloseTask_DoesNotCloseGlobalStore verifies that OnRevoked on a partition
// that had a global store injected does not panic and leaves the store usable.
// Uses the zero-store path so no broker is needed.
func TestCloseTask_DoesNotCloseGlobalStore(t *testing.T) {
	t.Parallel()

	bt := buildZeroStoreBuiltTopology(t)
	cfg, err := gstream.Configure(
		gstream.WithName("test-close-gstore"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	tm := runtime.NewTaskManager(bt, cfg, nil)

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	sharedStore := state.NewKeyValueStore[[]byte, []byte](
		"global-store", db, state.BytesSerde{}, state.BytesSerde{},
	)
	tm.RegisterGlobalStore("global-store", sharedStore)

	// Trigger task creation via Executor.
	exec := tm.Executor(0)
	if exec == nil {
		t.Fatal("expected non-nil executor")
	}

	// Verify the store is in the stores map.
	stores := runtime.TaskManagerStoresForPartition(tm, 0)
	if _, ok := stores["global-store"]; !ok {
		t.Fatal("global store not injected before revoke")
	}

	// Revoke partition 0. Must NOT close the shared store.
	tm.OnRevoked(context.Background(), map[string][]int32{"input-topic": {0}})

	// After revoke, the task is gone but sharedStore must still be usable (not closed).
	if err := sharedStore.Put([]byte("k"), []byte("v")); err != nil {
		t.Errorf("global store closed by closeTask (Put after revoke failed): %v", err)
	}
}

// ---------------------------------------------------------------------------
// PostBatchSweep — no Kafka I/O, sweep-only path (C3)
// ---------------------------------------------------------------------------

// TestPostBatchSweep_EmptyIsNoError verifies that PostBatchSweep on a
// TaskManager with no assigned tasks is a no-op (mirrors PostBatch).
func TestPostBatchSweep_EmptyIsNoError(t *testing.T) {
	bt := buildStatefulTopology(t)
	cfg, err := gstream.Configure(
		gstream.WithName("test-pbs"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	tm := runtime.NewTaskManager(bt, cfg, nil)
	if err := tm.PostBatchSweep(context.Background()); err != nil {
		t.Fatalf("PostBatchSweep on empty TaskManager: %v", err)
	}
}

// TestPostBatchSweep_DoesNotFlush verifies that PostBatchSweep on a zero-store
// task (created lazily via Executor) does not attempt any Kafka I/O —
// the task has db==nil so the body is skipped entirely, meaning no
// producer.Flush is called even if producers existed.
func TestPostBatchSweep_DoesNotFlush(t *testing.T) {
	bt := buildZeroStoreBuiltTopology(t)
	cfg, err := gstream.Configure(
		gstream.WithName("test-pbs-noflush"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	tm := runtime.NewTaskManager(bt, cfg, nil)
	// Create a lazy zero-store task (db==nil).
	exec := tm.Executor(0)
	if exec == nil {
		t.Fatal("expected non-nil executor")
	}
	// PostBatchSweep must not panic or error — the db==nil guard skips I/O.
	if err := tm.PostBatchSweep(context.Background()); err != nil {
		t.Fatalf("PostBatchSweep with zero-store task: %v", err)
	}
}

// ---------------------------------------------------------------------------
// DrainChangelogRecords — encoded OutRecords (C3)
// ---------------------------------------------------------------------------

// TestDrainChangelogRecords_EmptyTaskManagerReturnsNil verifies that
// DrainChangelogRecords returns nil (no error) when there are no live tasks.
func TestDrainChangelogRecords_EmptyTaskManagerReturnsNil(t *testing.T) {
	bt := buildStatefulTopology(t)
	cfg, err := gstream.Configure(
		gstream.WithName("test-dcr"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	tm := runtime.NewTaskManager(bt, cfg, nil)
	recs, err := tm.DrainChangelogRecords()
	if err != nil {
		t.Fatalf("DrainChangelogRecords on empty TaskManager: %v", err)
	}
	if recs != nil {
		t.Errorf("want nil OutRecords, got %d", len(recs))
	}
}

// TestDrainChangelogRecords_EncodesAndPinsPartition verifies that
// DrainChangelogRecords encodes pending mutations via ChangelogProducer.Encode
// and returns OutRecords with pinned partition and correct topic/key/value.
//
// This test manually constructs a task with a real Pebble MemDB so the
// collector can be populated without a broker. It calls store.Put to append a
// Put mutation to the collector, then calls DrainChangelogRecords and checks:
//   - len(recs) == 1
//   - recs[0].Topic matches the changelog topic
//   - recs[0].Partition is pinned (IsValid=true, Value=partition)
//   - recs[0].Value is not nil (Put → non-tombstone)
func TestDrainChangelogRecords_EncodesAndPinsPartition(t *testing.T) {
	t.Parallel()

	// Build a stateful topology so bt.StoreBindings is populated.
	bt := buildStatefulTopology(t)
	cfg, err := gstream.Configure(
		gstream.WithName("drain-test"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	tm := runtime.NewTaskManager(bt, cfg, nil)

	// Inject a pre-built task with real MemDB + collector + stub producer
	// via the test helper. The stub producer allows Encode without a broker.
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	const partition = int32(2)
	const storeName = "word-count"
	const changelogTopic = "drain-test-word-count-changelog"

	collector := &state.MutationCollector{}
	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		storeName, db, state.BytesSerde{}, state.BytesSerde{}, collector,
	)

	// Write a value so the collector captures a Put mutation.
	if err := byteStore.Put([]byte(`"key"`), []byte(`42`)); err != nil {
		t.Fatalf("store.Put: %v", err)
	}

	// Verify a mutation was collected before drain.
	if muts := runtime.TaskManagerDrainCollectorPreview(collector); len(muts) == 0 {
		t.Fatal("expected at least one pending mutation after store.Put")
	}
	// Re-append so DrainChangelogRecords sees them (preview drained them).
	if err := byteStore.Put([]byte(`"key"`), []byte(`42`)); err != nil {
		t.Fatalf("store.Put (re-add): %v", err)
	}

	// Register the fabricated task into the TaskManager via the test helper.
	runtime.TaskManagerInjectTask(tm, partition, storeName, changelogTopic, collector, db)

	recs, err := tm.DrainChangelogRecords()
	if err != nil {
		t.Fatalf("DrainChangelogRecords: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("DrainChangelogRecords: expected at least 1 OutRecord")
	}
	for i, r := range recs {
		if r.Topic != changelogTopic {
			t.Errorf("[%d] Topic: got %q, want %q", i, r.Topic, changelogTopic)
		}
		if !r.Partition.IsValid {
			t.Errorf("[%d] Partition.IsValid: want true (pinned)", i)
		}
		if r.Partition.Value != partition {
			t.Errorf("[%d] Partition.Value: got %d, want %d", i, r.Partition.Value, partition)
		}
		// Put → non-nil value
		if r.Value == nil {
			t.Errorf("[%d] Value: want non-nil for Put mutation", i)
		}
	}
}

// TestDrainChangelogRecords_DeleteProducesTombstone verifies that a Delete
// mutation produces an OutRecord with nil Value (Kafka tombstone).
func TestDrainChangelogRecords_DeleteProducesTombstone(t *testing.T) {
	t.Parallel()

	bt := buildStatefulTopology(t)
	cfg, err := gstream.Configure(
		gstream.WithName("drain-tombstone"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	tm := runtime.NewTaskManager(bt, cfg, nil)

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	const partition = int32(1)
	const storeName = "word-count"
	const changelogTopic = "drain-tombstone-word-count-changelog"

	collector := &state.MutationCollector{}
	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		storeName, db, state.BytesSerde{}, state.BytesSerde{}, collector,
	)

	// Put then Delete so only the Delete remains in the collector.
	_ = byteStore.Put([]byte("k"), []byte("v"))
	// Drain the Put so only the Delete gets encoded.
	collector.Drain()
	_ = byteStore.Delete([]byte("k"))

	runtime.TaskManagerInjectTask(tm, partition, storeName, changelogTopic, collector, db)

	recs, err := tm.DrainChangelogRecords()
	if err != nil {
		t.Fatalf("DrainChangelogRecords: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("want 1 OutRecord for Delete, got %d", len(recs))
	}
	if recs[0].Value != nil {
		t.Errorf("Delete OutRecord Value: want nil (tombstone), got %v", recs[0].Value)
	}
}
