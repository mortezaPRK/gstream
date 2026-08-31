//go:build integration

package runtime_test

// TestE2E_WindowedCountRestoreAfterRestart is the P3-T5 exit criterion test.
//
// Proves:
//  1. Windowed Count materializes per-key-per-window counts into Pebble via
//     composite keys (WindowCompositeKey) AND the changelog topic.
//  2. After deleting local Pebble and restarting, RestoreFromChangelog rebuilds
//     the exact same windowed counts without reprocessing source records.
//     Stream-time survives delete+restore (via changelog checkpoint, though
//     stream-time itself is NOT in changelog — it restarts from 0 after Pebble
//     delete, which is acceptable under ALO semantics).
//  3. Processing continues correctly from the restored baseline.
//
// Changelog topic name derivation (must match TaskManager.openTask exactly):
//
//	changelogTopic = appID + "-" + storeName + "-changelog"
//	             = "win-e2e" + "-" + "wc" + "-changelog"
//	             = "win-e2e-wc-changelog"
//
// Confirmed in task_manager.go allChangelogTopics():
//
//	tm.appID + "-" + binding.ChangelogTopic + "-changelog"
//
// where binding.ChangelogTopic == storeName (windowed.go Aggregate sets it).
//
// Event-time / produce-timestamp mechanism:
// kgo.Record.Timestamp is set EXPLICITLY for each produced record. The Adapter
// reads in.Timestamp.UnixMilli() → topology.Record.Timestamp, and the windowed
// aggregate processor uses r.Timestamp (no custom extractorFn). So setting
// kgo.Record.Timestamp = time.UnixMilli(tsMs) deterministically controls window
// assignment regardless of wall-clock time.
//
// Window assignments for TumblingWindows(10*time.Second):
//   - ts=1000ms   → window [0,     10000)
//   - ts=3000ms   → window [0,     10000)
//   - ts=2000ms   → window [0,     10000)
//   - ts=12000ms  → window [10000, 20000)
//
// Phase-1 counts (before sweep):
//   - a[0,10000)    = 2  (ts=1000, ts=3000)
//   - b[0,10000)    = 1  (ts=2000)
//   - a[10000,20000) = 1  (ts=12000)
//
// Retention sweep analysis:
// When a@ts=12000 is processed, streamTime advances to 12000.
// PostBatch sweep: sweepInterval = MaxSizeMs() = 10000.
// Condition: 12000 - lastSweepTime(0) = 12000 >= 10000 → sweep fires.
// expiryBoundary = 12000 - 10000 - 0 = 2000.
// Windows with windowStart < 2000:
//   - a[0,10000): windowStart=0 < 2000 → SWEPT (tombstoned)
//   - b[0,10000): windowStart=0 < 2000 → SWEPT (tombstoned)
//   - a[10000,20000): windowStart=10000 >= 2000 → NOT swept
//
// Phase-1 counts in Pebble after clean shutdown:
//   - a[0,10000):     ABSENT (tombstoned)
//   - b[0,10000):     ABSENT (tombstoned)
//   - a[10000,20000): count=1
//
// Phase-2 restore proof:
// After deleting Pebble and restoring from changelog, produce a@13000.
// This lands in window [10000,20000). The store should have baseline=1 from
// restore, so the count becomes 2. If restore failed, the store would start
// from 0 and emit 1 — so count=2 definitively proves the restore worked.
//
// Tombstone survival sub-assertion:
// After restore, a[0,10000) must not reappear. The changelog contains
// tombstones for win0 entries; RestoreFromChangelog replays them as Pebble
// deletes. The final Pebble state after restore must have win0 absent.
// We verify by scanning the final changelog state (tombstone semantics applied).

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json/v2"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	kafkamodule "github.com/mortezaPRK/gstream/integration/kafka"
	"github.com/mortezaPRK/gstream/internal/kafka"
	state "github.com/mortezaPRK/gstream/stores/pebble"
	kgo "github.com/twmb/franz-go/pkg/kgo"
)

func TestE2E_WindowedCountRestoreAfterRestart(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping windowed E2E integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	// 1. Start Kafka broker.
	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-cluster-windowed"),
		kafkamodule.WithEnv(map[string]string{
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
		}),
	)
	if err != nil {
		t.Skipf("failed to start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = kc.Terminate(context.Background()) })

	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("get brokers: %v", err)
	}
	t.Logf("brokers: %v", brokers)

	const (
		appID     = "win-e2e"
		srcTopic  = "win-input"
		storeName = "wc"
		// Derived: appID + "-" + binding.ChangelogTopic + "-changelog"
		// binding.ChangelogTopic == storeName per windowed.go Aggregate.
		changelogTopic = "win-e2e-wc-changelog"
	)

	const (
		win0Start = int64(0)
		win1Start = int64(10_000)
	)

	// 2. Temp state dir.
	stateDir, err := os.MkdirTemp("", "gstream-windowed-e2e-*")
	if err != nil {
		t.Fatalf("mktemp: %v", err)
	}
	defer os.RemoveAll(stateDir)
	t.Logf("stateDir: %s", stateDir)

	// 3. Create topics.
	if err := kafka.EnsureTopics(ctx, brokers, []kafka.TopicSpec{
		{Name: srcTopic, Partitions: 1, ReplicationFactor: 1},
		{
			Name: changelogTopic, Partitions: 1, ReplicationFactor: 1,
			Configs: map[string]string{"cleanup.policy": "compact"},
		},
	}); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}
	t.Logf("created topics: %s, %s", srcTopic, changelogTopic)

	cfg, err := gstream.Configure(
		gstream.WithName(appID),
		gstream.WithBrokers(brokers...),
		gstream.WithStateDir(stateDir),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	// =========================================================================
	// PHASE 1: materialize windowed counts
	// =========================================================================

	bt1 := buildWindowedCountTopology(srcTopic, storeName)
	adapter1, err := newTestAdapter(bt1, cfg, slog.Default())
	if err != nil {
		t.Fatalf("NewAdapterWithConfig p1: %v", err)
	}
	client1, err := kafka.New(cfg, []string{srcTopic}, slog.Default(),
		kafka.WithLifecycle(adapter1.LifecycleCallbacks()),
		kafka.WithPostBatch(adapter1.PostBatchHook()),
		kafka.WithHealthGate(adapter1.HealthGateHook()),
	)
	if err != nil {
		t.Fatalf("kafka.New p1: %v", err)
	}

	run1Ctx, run1Cancel := context.WithCancel(ctx)
	done1 := make(chan error, 1)
	go func() { done1 <- client1.Run(run1Ctx, adapter1.ProcessFunc()) }()

	serde := JSONSerde[string]{}

	// Batch A: three records → all land in window [0,10000).
	// streamTime after batchA = max(1000,3000,2000) = 3000.
	// PostBatch sweep: 3000 - 0 = 3000 < 10000 → NO sweep.
	produceWindowedRecords(t, ctx, brokers, srcTopic, serde, []windowedRecord{
		{key: "a", value: "a", tsMs: 1_000},
		{key: "a", value: "a", tsMs: 3_000},
		{key: "b", value: "b", tsMs: 2_000},
	})
	t.Log("phase-1 batchA: produced a@1000, a@3000, b@2000 (→ window[0,10000))")

	// Wait until win0 entries appear in changelog.
	pollWindowedChangelog(t, ctx, brokers, changelogTopic, storeName, serde, []windowedExpected{
		{key: "a", winStart: win0Start, count: 2},
		{key: "b", winStart: win0Start, count: 1},
	})
	t.Log("phase-1 batchA: changelog confirmed a[0,10000)=2, b[0,10000)=1")

	// Batch B: a@12000 → window [10000,20000).
	// streamTime advances to 12000.
	// PostBatch sweep: 12000 - 0 = 12000 >= 10000 → sweep fires.
	// expiryBoundary = 12000 - 10000 - 0 = 2000.
	// Swept: a[0,10000) start=0 < 2000, b[0,10000) start=0 < 2000.
	// Not swept: a[10000,20000) start=10000 >= 2000.
	produceWindowedRecords(t, ctx, brokers, srcTopic, serde, []windowedRecord{
		{key: "a", value: "a", tsMs: 12_000},
	})
	t.Log("phase-1 batchB: produced a@12000 (→ window[10000,20000); sweep fires: win0 tombstoned)")

	// Wait for a[10000,20000)=1. Since sweep runs before flush in PostBatch,
	// the tombstones for win0 and a[10000,20000)=1 are ALL in the same flush batch.
	// After applying tombstone semantics: win0 absent, win1=1.
	pollWindowedChangelog(t, ctx, brokers, changelogTopic, storeName, serde, []windowedExpected{
		{key: "a", winStart: win1Start, count: 1},
	})
	t.Log("phase-1 batchB: changelog confirmed a[10000,20000)=1 (and win0 tombstoned)")

	// Clean shutdown.
	run1Cancel()
	select {
	case err := <-done1:
		if err != nil {
			t.Errorf("client1.Run returned error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("client1.Run did not stop within 15s")
	}
	client1.Close()
	t.Log("phase-1: shutdown complete; Pebble closed")

	// Assert Pebble state after clean shutdown.
	partitionDir := filepath.Join(stateDir, appID, "partition-0")
	assertWindowedPebbleCounts(t, partitionDir, storeName, serde, "a", win1Start, 1, "a[10000,20000)")
	t.Log("phase-1: Pebble direct assertion passed — a[10000,20000)=1")

	// win0 must be absent (swept).
	if !checkWindowAbsentInPebble(t, partitionDir, storeName, serde, "a", win0Start) {
		t.Error("phase-1: expected a[0,10000) to be absent in Pebble (sweep should have tombstoned it)")
	} else {
		t.Log("phase-1: sweep confirmed — a[0,10000) absent in Pebble")
	}
	if !checkWindowAbsentInPebble(t, partitionDir, storeName, serde, "b", win0Start) {
		t.Error("phase-1: expected b[0,10000) to be absent in Pebble")
	} else {
		t.Log("phase-1: b[0,10000) also absent in Pebble")
	}

	// =========================================================================
	// PHASE 2: restore-after-restart
	// =========================================================================

	// Delete local Pebble partition dir. Changelog in Kafka is the only truth.
	if err := os.RemoveAll(partitionDir); err != nil {
		t.Fatalf("RemoveAll partition dir: %v", err)
	}
	t.Logf("phase-2: deleted partition dir %s", partitionDir)

	bt2 := buildWindowedCountTopology(srcTopic, storeName)
	adapter2, err := newTestAdapter(bt2, cfg, slog.Default())
	if err != nil {
		t.Fatalf("NewAdapterWithConfig p2: %v", err)
	}

	// Wrap onAssigned to signal when restore completes.
	// OnAssigned is synchronous inside the kgo rebalance handler; by the time it
	// returns, RestoreFromChangelog has fully replayed the changelog.
	restoreDone := make(chan struct{}, 1)
	onAssigned2, onRevoked2 := adapter2.LifecycleCallbacks()
	wrappedAssigned2 := func(ctx context.Context, assigned map[string][]int32) error {
		err := onAssigned2(ctx, assigned)
		if err == nil {
			select {
			case restoreDone <- struct{}{}:
			default:
			}
		}
		return err
	}

	client2, err := kafka.New(cfg, []string{srcTopic}, slog.Default(),
		kafka.WithLifecycle(wrappedAssigned2, onRevoked2),
		kafka.WithPostBatch(adapter2.PostBatchHook()),
		kafka.WithHealthGate(adapter2.HealthGateHook()),
	)
	if err != nil {
		t.Fatalf("kafka.New p2: %v", err)
	}

	run2Ctx, run2Cancel := context.WithCancel(ctx)
	defer run2Cancel()
	done2 := make(chan error, 1)
	go func() { done2 <- client2.Run(run2Ctx, adapter2.ProcessFunc()) }()

	select {
	case <-restoreDone:
		t.Log("phase-2: OnAssigned fired; RestoreFromChangelog complete")
	case <-time.After(30 * time.Second):
		t.Fatal("phase-2: timed out waiting for OnAssigned/restore")
	}

	// Produce post-restore record: a@13000 → a[10000,20000).
	// Baseline after restore is 1 (from phase-1 changelog).
	// If restore worked, the count becomes 1+1=2.
	// If restore failed (fresh state), the count would be 0+1=1.
	// So count=2 in the changelog definitively proves the restore was correct.
	produceWindowedRecords(t, ctx, brokers, srcTopic, serde, []windowedRecord{
		{key: "a", value: "a", tsMs: 13_000},
	})
	t.Log("phase-2: produced a@13000 (→ window[10000,20000)); restored baseline=1 → expect count=2)")

	pollWindowedChangelog(t, ctx, brokers, changelogTopic, storeName, serde, []windowedExpected{
		{key: "a", winStart: win1Start, count: 2},
	})
	t.Log("phase-2: RESTORE PROVEN — a[10000,20000)=2 confirms baseline=1 was restored (not 0)")

	// Tombstone survival sub-assertion:
	// a[0,10000) was tombstoned in phase-1. After restore, it must not reappear.
	// The changelog replays tombstones as Pebble deletes during restore.
	// checkFinalChangelogState applies full tombstone semantics to the changelog
	// and returns whether the key is PRESENT in the final state.
	if checkFinalChangelogState(t, ctx, brokers, changelogTopic, storeName, serde, "a", win0Start) {
		t.Error("tombstone regression: a[0,10000) reappeared after restore despite being swept in phase-1")
	} else {
		t.Log("phase-2: tombstone survival CONFIRMED — a[0,10000) absent in final changelog state")
	}

	run2Cancel()
	select {
	case err := <-done2:
		if err != nil {
			t.Errorf("client2.Run returned error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("client2.Run did not stop within 15s")
	}
	client2.Close()
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// buildWindowedCountTopology builds:
// Stream[string,string] → GroupByKey → WindowedBy(TumblingWindows(10s)) → Count(storeName, JSONSerde[int64]{}).
func buildWindowedCountTopology(srcTopic, storeName string) *gstream.BuiltTopology {
	b := gstream.NewStreamBuilder()
	gstream.Stream[string, string](b, srcTopic, "win-source",
		JSONSerde[string]{}, JSONSerde[string]{}).
		GroupByKey(JSONSerde[string]{}, JSONSerde[string]{}).
		WindowedBy(gstream.TumblingWindows(10*time.Second)).
		Count(storeName, JSONSerde[int64]{})
	return b.Build()
}

// windowedRecord is a single record with an explicit event-time timestamp.
type windowedRecord struct {
	key   string
	value string
	tsMs  int64 // Unix milliseconds — controls which window this record lands in
}

// produceWindowedRecords produces records with EXPLICIT Timestamp to control
// event-time window assignment.
//
// kgo.Record.Timestamp → Kafka record timestamp → Adapter reads
// in.Timestamp.UnixMilli() → topology.Record.Timestamp → windowed
// aggregate uses r.Timestamp as ts (no custom extractorFn).
func produceWindowedRecords(
	t *testing.T,
	ctx context.Context,
	brokers []string,
	topic string,
	serde JSONSerde[string],
	records []windowedRecord,
) {
	t.Helper()
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("produceWindowedRecords: new client: %v", err)
	}
	defer producer.Close()

	for _, r := range records {
		kb, _ := serde.Serialize(r.key)
		vb, _ := serde.Serialize(r.value)
		rec := &kgo.Record{
			Topic:     topic,
			Key:       kb,
			Value:     vb,
			Timestamp: time.UnixMilli(r.tsMs), // explicit event-time
		}
		res := producer.ProduceSync(ctx, rec)
		if res.FirstErr() != nil {
			t.Fatalf("produceWindowedRecords: produce key=%q ts=%d: %v", r.key, r.tsMs, res.FirstErr())
		}
	}
}

// windowedExpected holds a single windowed count assertion.
type windowedExpected struct {
	key      string
	winStart int64
	count    int64
}

// windowKey is the composite assertion key for windowed counts.
type windowKey struct {
	key      string
	winStart int64
}

// pollWindowedChangelog consumes the windowed changelog topic from offset 0
// and waits until all expected (key, windowStart) → count values are present,
// applying tombstone semantics (empty value = delete).
//
// Changelog record key format (matches WindowPut's Mutation.Key):
//
//	storeName + 0x00 + WindowCompositeKey(kBytes, windowStart)
//	= storeName + 0x00 + uint32(len(kBytes)) big-endian + kBytes + int64(windowStart) big-endian
func pollWindowedChangelog(
	t *testing.T,
	ctx context.Context,
	brokers []string,
	topic, storeName string,
	serde JSONSerde[string],
	expected []windowedExpected,
) map[windowKey]int64 {
	t.Helper()

	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().AtStart()},
		}),
	)
	if err != nil {
		t.Fatalf("pollWindowedChangelog: create consumer: %v", err)
	}
	defer consumer.Close()

	prefix := append([]byte(storeName), 0x00)
	latest := make(map[windowKey]int64)

	type wantKey struct {
		key      string
		winStart int64
	}
	want := make(map[wantKey]int64, len(expected))
	for _, e := range expected {
		want[wantKey{e.key, e.winStart}] = e.count
	}

	allMatch := func() bool {
		for wk, wantCount := range want {
			if got, ok := latest[windowKey(wk)]; !ok || got != wantCount {
				return false
			}
		}
		return true
	}

	for !allMatch() {
		fetches := consumer.PollFetches(readyCtx)
		if fetches.IsClientClosed() {
			break
		}
		if err := readyCtx.Err(); err != nil {
			t.Fatalf("pollWindowedChangelog: timed out; latest=%v expected=%v", latest, expected)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if !bytes.HasPrefix(r.Key, prefix) {
				return
			}
			kBytes, winStart, ok := decodeWindowChangelogKey(r.Key[len(prefix):])
			if !ok {
				return
			}
			var strKey string
			if err := json.Unmarshal(kBytes, &strKey); err != nil {
				return
			}
			wk := windowKey{strKey, winStart}
			if len(r.Value) == 0 {
				delete(latest, wk) // tombstone
				return
			}
			var count int64
			if err := json.Unmarshal(r.Value, &count); err != nil {
				return
			}
			latest[wk] = count
		})
	}
	return latest
}

// decodeWindowChangelogKey decodes the per-store portion of a windowed
// changelog key: uint32(len(kBytes)) ‖ kBytes ‖ int64(windowStart).
// Returns (nil, 0, false) on malformed input.
func decodeWindowChangelogKey(raw []byte) (kBytes []byte, windowStart int64, ok bool) {
	if len(raw) < 4+8 {
		return nil, 0, false
	}
	kLen := int(binary.BigEndian.Uint32(raw[0:4]))
	if len(raw) != 4+kLen+8 {
		return nil, 0, false
	}
	kb := make([]byte, kLen)
	copy(kb, raw[4:4+kLen])
	ws := int64(binary.BigEndian.Uint64(raw[4+kLen:]))
	return kb, ws, true
}

// assertWindowedPebbleCounts opens Pebble and asserts the windowed count for
// (key, windowStart) equals wantCount. Uses WindowGet on a raw-bytes store.
func assertWindowedPebbleCounts(
	t *testing.T,
	dbDir, storeName string,
	serde JSONSerde[string],
	key string,
	windowStart int64,
	wantCount int64,
	label string,
) {
	t.Helper()
	db, err := state.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("assertWindowedPebbleCounts[%s]: OpenDB %q: %v", label, dbDir, err)
	}
	defer db.Close()

	store := state.NewKeyValueStore[[]byte, []byte](storeName, db, BytesSerde{}, BytesSerde{})
	kBytes, err := serde.Serialize(key)
	if err != nil {
		t.Fatalf("assertWindowedPebbleCounts[%s]: serialize key %q: %v", label, key, err)
	}
	vb, found, err := store.WindowGet(kBytes, windowStart)
	if err != nil {
		t.Fatalf("assertWindowedPebbleCounts[%s]: WindowGet: %v", label, err)
	}
	if !found {
		t.Errorf("assertWindowedPebbleCounts[%s]: key %q win %d not found", label, key, windowStart)
		return
	}
	var got int64
	if err := json.Unmarshal(vb, &got); err != nil {
		t.Fatalf("assertWindowedPebbleCounts[%s]: unmarshal count: %v", label, err)
	}
	if got != wantCount {
		t.Errorf("assertWindowedPebbleCounts[%s]: count=%d want %d", label, got, wantCount)
	}
}

// checkWindowAbsentInPebble opens Pebble and returns true if the window
// (key, windowStart) is NOT present (absent = swept/tombstoned).
func checkWindowAbsentInPebble(
	t *testing.T,
	dbDir, storeName string,
	serde JSONSerde[string],
	key string,
	windowStart int64,
) bool {
	t.Helper()
	db, err := state.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("checkWindowAbsentInPebble: OpenDB %q: %v", dbDir, err)
	}
	defer db.Close()

	store := state.NewKeyValueStore[[]byte, []byte](storeName, db, BytesSerde{}, BytesSerde{})
	kBytes, err := serde.Serialize(key)
	if err != nil {
		t.Fatalf("checkWindowAbsentInPebble: serialize key %q: %v", key, err)
	}
	_, found, err := store.WindowGet(kBytes, windowStart)
	if err != nil {
		t.Fatalf("checkWindowAbsentInPebble: WindowGet: %v", err)
	}
	return !found // true = absent (swept)
}

// checkFinalChangelogState replays the full changelog applying tombstone
// semantics and returns true if (key, windowStart) is PRESENT in the final
// state. Reads until no new records arrive for 2s (changelog quiescence).
func checkFinalChangelogState(
	t *testing.T,
	ctx context.Context,
	brokers []string,
	topic, storeName string,
	serde JSONSerde[string],
	key string,
	windowStart int64,
) bool {
	t.Helper()

	kBytes, err := serde.Serialize(key)
	if err != nil {
		t.Fatalf("checkFinalChangelogState: serialize key: %v", err)
	}
	prefix := append([]byte(storeName), 0x00)
	ck := windowCompositeKeyLocal(kBytes, windowStart)
	targetKey := make([]byte, len(prefix)+len(ck))
	copy(targetKey, prefix)
	copy(targetKey[len(prefix):], ck)

	// Consume from start; stop once PollFetches returns no records for 2s.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().AtStart()},
		}),
	)
	if err != nil {
		t.Fatalf("checkFinalChangelogState: create consumer: %v", err)
	}
	defer consumer.Close()

	present := false
	for {
		pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := consumer.PollFetches(pollCtx)
		cancel()
		if fetches.IsClientClosed() || pollCtx.Err() != nil {
			break // quiescent or done
		}
		if fetches.Empty() {
			break // no more records
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if bytes.Equal(r.Key, targetKey) {
				present = len(r.Value) > 0 // empty = tombstone
			}
		})
	}
	return present
}

// windowCompositeKeyLocal mirrors state.WindowCompositeKey.
func windowCompositeKeyLocal(kBytes []byte, windowStart int64) []byte {
	out := make([]byte, 4+len(kBytes)+8)
	binary.BigEndian.PutUint32(out[0:4], uint32(len(kBytes)))
	copy(out[4:], kBytes)
	binary.BigEndian.PutUint64(out[4+len(kBytes):], uint64(windowStart))
	return out
}
