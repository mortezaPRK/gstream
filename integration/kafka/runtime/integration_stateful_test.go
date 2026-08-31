//go:build integration

package runtime_test

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	kafkamodule "github.com/mortezaPRK/gstream/integration/kafka"
	"github.com/mortezaPRK/gstream/internal/kafka"
	"github.com/mortezaPRK/gstream/internal/runtime"
	state "github.com/mortezaPRK/gstream/store/pebble"
	kgo "github.com/twmb/franz-go/pkg/kgo"
)

// TestE2E_StatefulCountRestoreAfterRestart is the P2-S8 exit criterion test.
//
// It proves:
//  1. Count materializes per-key counts into Pebble AND the changelog topic.
//  2. After deleting local Pebble and restarting, RestoreFromChangelog rebuilds
//     the exact same counts without reprocessing any source records.
//  3. Processing continues correctly from the restored baseline (no double-count).
//
// Changelog topic name derivation (must match TaskManager.openTask exactly):
//
//	changelogTopic = AppID + "-" + binding.ChangelogTopic + "-changelog"
//	             = "stateful-e2e" + "-" + "counts" + "-changelog"
//	             = "stateful-e2e-counts-changelog"
//
// where binding.ChangelogTopic == storeName ("counts") per grouped.go Aggregate.
//
// Phase 1: produce 9 records → a=5, b=3, c=1.
//
//	Deterministic wait on changelog. Assert Pebble after clean shutdown.
//
// Phase 2: delete Pebble partition dir. Restart with same AppID/stateDir.
//
//	OnAssigned fires → RestoreFromChangelog replays changelog → state rebuilt.
//	Produce ["a","c"] → a=6, c=2. b stays 3 (ALO: phase-1 offsets committed).
func TestE2E_StatefulCountRestoreAfterRestart(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping stateful E2E integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// --- 1. Start Kafka broker ---
	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-cluster-stateful"),
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
		appID     = "stateful-e2e"
		srcTopic  = "sc-input"
		storeName = "counts"
		// TaskManager derives: appID + "-" + binding.ChangelogTopic + "-changelog"
		// binding.ChangelogTopic = storeName (set in grouped.go Aggregate)
		changelogTopic = "stateful-e2e-counts-changelog"
	)

	// --- 2. Temp state dir ---
	stateDir, err := os.MkdirTemp("", "gstream-stateful-e2e-*")
	if err != nil {
		t.Fatalf("mktemp: %v", err)
	}
	defer os.RemoveAll(stateDir)
	t.Logf("stateDir: %s", stateDir)

	// --- 3. Create topics ---
	// Source: 1 partition (deterministic; single partition = no key-routing ambiguity).
	// Changelog: 1 partition, cleanup.policy=compact (required for state restore).
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
	// PHASE 1: materialize
	// =========================================================================

	// 4. Build topology: Stream[string,string] → GroupByKey → Count("counts").
	bt1 := buildCountTopology(srcTopic, storeName)

	// 5. Adapter + client.
	adapter1, err := runtime.NewAdapter(bt1, cfg, slog.Default())
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

	// 6. Produce 9 records → expected: a=5, b=3, c=1.
	produceStringKeys(t, ctx, brokers, srcTopic,
		[]string{"a", "b", "a", "c", "a", "b", "a", "a", "b"})
	t.Log("phase-1: produced 9 records")

	// 7. Deterministic wait: poll changelog until a=5, b=3, c=1.
	// No fixed sleep. Bound by 30 s timeout inside pollChangelog.
	latest1 := pollChangelog(t, ctx, brokers, changelogTopic, storeName,
		map[string]int64{"a": 5, "b": 3, "c": 1})
	t.Logf("phase-1: changelog confirmed counts: %v", latest1)

	// 8. Clean shutdown.
	// Cancel run context → Run exits. Then Close() triggers kgo cooperative revoke
	// → OnPartitionsRevoked → TaskManager.OnRevoked → closeTask
	// (flush pending mutations + write final checkpoint + close Pebble).
	// NOTE: canceling the run context here may interrupt an in-flight offset
	// commit, producing a benign "failed to commit offsets ... context canceled"
	// WARN. Harmless under ALO — uncommitted records are redelivered on the next
	// run; assertions below account for this.
	run1Cancel()
	select {
	case err := <-done1:
		if err != nil {
			t.Errorf("client1.Run returned error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("client1.Run did not stop within 15 s after context cancel")
	}
	client1.Close() // ← OnPartitionsRevoked fires here; Pebble closed after this returns
	t.Log("phase-1: shutdown complete; Pebble closed")

	// 9. Assert Pebble state directly (DB is closed, safe to open).
	partitionDir := filepath.Join(stateDir, appID, "partition-0")
	assertPebbleStoreCounts(t, partitionDir, storeName, map[string]int64{"a": 5, "b": 3, "c": 1})
	t.Log("phase-1: Pebble direct assertion passed")

	// =========================================================================
	// PHASE 2: restore-after-restart
	// =========================================================================

	// 10. Delete local Pebble partition dir.
	// The changelog topic in Kafka survives → only restore path available.
	if err := os.RemoveAll(partitionDir); err != nil {
		t.Fatalf("RemoveAll partition dir: %v", err)
	}
	t.Logf("phase-2: deleted partition dir %s", partitionDir)

	// 11. New adapter + client — same AppID, same stateDir, same brokers.
	bt2 := buildCountTopology(srcTopic, storeName)
	adapter2, err := runtime.NewAdapter(bt2, cfg, slog.Default())
	if err != nil {
		t.Fatalf("NewAdapterWithConfig p2: %v", err)
	}

	// Wrap onAssigned to detect when restore is complete.
	// OnAssigned is synchronous inside kgo rebalance (cooperative sticky): by the
	// time it returns, RestoreFromChangelog has fully replayed the changelog.
	restoreDone := make(chan struct{}, 1)
	onAssigned2, onRevoked2 := adapter2.LifecycleCallbacks()
	wrappedAssigned := func(ctx context.Context, assigned map[string][]int32) error {
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
		kafka.WithLifecycle(wrappedAssigned, onRevoked2),
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

	// 12. Wait for restore to complete.
	select {
	case <-restoreDone:
		t.Log("phase-2: OnAssigned fired; RestoreFromChangelog complete")
	case <-time.After(30 * time.Second):
		t.Fatal("phase-2: timed out waiting for OnAssigned/restore")
	}

	// 13. Produce 2 more records from a different producer.
	// Counts should continue from restored baseline: a: 5+1=6, c: 1+1=2.
	// b is not touched → stays at 3.
	produceStringKeys(t, ctx, brokers, srcTopic, []string{"a", "c"})
	t.Log("phase-2: produced a, c")

	// 14. Deterministic wait: changelog shows a=6, c=2, b=3.
	// Including b=3 in expected proves ALO: phase-1 source offsets were committed,
	// so the original 9 records are NOT reprocessed on restart.
	latest2 := pollChangelog(t, ctx, brokers, changelogTopic, storeName,
		map[string]int64{"a": 6, "b": 3, "c": 2})
	t.Logf("phase-2: changelog confirmed counts: %v", latest2)

	// 15. ALO assertion: b was not reprocessed (stays 3, not 6).
	if got := latest2["b"]; got != 3 {
		t.Errorf("ALO violation: b count = %d, want 3; phase-1 offsets must have been committed", got)
	}
	t.Log("ALO confirmed: b=3; phase-1 records were not reprocessed")

	run2Cancel()
	select {
	case err := <-done2:
		if err != nil {
			t.Errorf("client2.Run returned error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("client2.Run did not stop within 15 s after context cancel")
	}
	client2.Close()
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// buildCountTopology builds Stream[string,string] → GroupByKey → Count(storeName).
func buildCountTopology(srcTopic, storeName string) *gstream.BuiltTopology {
	b := gstream.NewStreamBuilder()
	gstream.Stream[string, string](b, srcTopic, "source",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		Count(storeName)
	return b.Build()
}

// produceStringKeys produces records to topic. Both key and value are
// JSON-encoded strings (matching JSONSerde[string] used in the topology).
func produceStringKeys(t *testing.T, ctx context.Context, brokers []string, topic string, keys []string) {
	t.Helper()
	serde := gstream.JSONSerde[string]{}
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("produceStringKeys: new client: %v", err)
	}
	defer producer.Close()
	for _, k := range keys {
		kb, _ := serde.Serialize(k)
		vb, _ := serde.Serialize(k)
		res := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: kb, Value: vb})
		if res.FirstErr() != nil {
			t.Fatalf("produceStringKeys: produce %q: %v", k, res.FirstErr())
		}
	}
}

// pollChangelog consumes the changelog topic from offset 0 and waits until
// every key in expected has reached its expected int64 value.
//
// Changelog key format: storeName + 0x00 + JSON-encoded string key
// (the Pebble-prefixed key written by KeyValueStore.Put and captured by MutationCollector).
// Value: JSON-encoded int64 count. Empty value = tombstone → delete.
//
// Returns the full latest-value map after all expected conditions are met.
// Calls t.Fatalf on timeout (30 s per call).
func pollChangelog(
	t *testing.T,
	ctx context.Context,
	brokers []string,
	topic, storeName string,
	expected map[string]int64,
) map[string]int64 {
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
		t.Fatalf("pollChangelog: create consumer: %v", err)
	}
	defer consumer.Close()

	// Pebble store prefix: storeName + 0x00 separator (see state/keyvalue.go).
	prefix := append([]byte(storeName), 0x00)
	latest := make(map[string]int64)

	allMatch := func() bool {
		for k, want := range expected {
			if got, ok := latest[k]; !ok || got != want {
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
			t.Fatalf("pollChangelog: timed out waiting for %v; latest: %v", expected, latest)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if !bytes.HasPrefix(r.Key, prefix) {
				return
			}
			encodedKey := r.Key[len(prefix):]
			var strKey string
			if err := json.Unmarshal(encodedKey, &strKey); err != nil {
				return // skip malformed
			}
			if len(r.Value) == 0 {
				// Kafka tombstone → key deleted.
				delete(latest, strKey)
				return
			}
			var count int64
			if err := json.Unmarshal(r.Value, &count); err != nil {
				return
			}
			latest[strKey] = count
		})
	}

	return latest
}

// assertPebbleStoreCounts opens the Pebble DB at dbDir and asserts the count
// for each key in expected. Uses the same raw-bytes store construction as
// TaskManager.openTask (NewKeyValueStore[[]byte,[]byte] with BytesSerde).
func assertPebbleStoreCounts(t *testing.T, dbDir, storeName string, expected map[string]int64) {
	t.Helper()

	db, err := state.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("assertPebbleStoreCounts: OpenDB %q: %v", dbDir, err)
	}
	defer db.Close()

	store := state.NewKeyValueStore[[]byte, []byte](
		storeName, db, gstream.BytesSerde{}, gstream.BytesSerde{},
	)
	keySerde := gstream.JSONSerde[string]{}

	for k, want := range expected {
		kb, err := keySerde.Serialize(k)
		if err != nil {
			t.Fatalf("assertPebbleStoreCounts: serialize key %q: %v", k, err)
		}
		vb, found, err := store.Get(kb)
		if err != nil {
			t.Fatalf("assertPebbleStoreCounts: store.Get(%q): %v", k, err)
		}
		if !found {
			t.Errorf("assertPebbleStoreCounts: key %q not found in store %q", k, storeName)
			continue
		}
		var got int64
		if err := json.Unmarshal(vb, &got); err != nil {
			t.Fatalf("assertPebbleStoreCounts: unmarshal count for key %q: %v", k, err)
		}
		if got != want {
			t.Errorf("assertPebbleStoreCounts: count[%q] = %d, want %d", k, got, want)
		}
	}
}
