//go:build integration

package runtime_test

// TestE2E_SessionCountRestoreAfterRestart is the P3b exit criterion test.
//
// Proves:
//  1. SessionWindowedBy + Count materializes sessions into Pebble via composite
//     keys (WindowCompositeKey) AND the changelog topic.
//  2. The merge processor correctly bridges two separate sessions: records
//     a@1000,a@3000 form session[1000,3000]; records a@14000,a@16000 form
//     session[14000,16000] (gap=14000-3000=11000ms > gapMs=10000ms → separate);
//     bridge a@8000 matches BOTH existing sessions (sEnd+gap=13000>=8000 for
//     session1; sStart-gap=4000<=8000 for session2) → merged [1000,16000]=5.
//     WindowDelete is called for both merged-away sessions, generating tombstones.
//  3. After deleting local Pebble and restarting, RestoreFromChangelog replays the
//     changelog (including tombstones and the merged Put) to exactly reconstruct
//     state: [1000,16000]=5, session[14000,16000] absent, b[50000,50000]=1.
//  4. Processing continues from the restored baseline (not from zero): a@17000
//     merges into [1000,17000]=6; count=6 proves restored baseline=5.
//  5. Tombstone survival: the Delete for sessionStart=14000 must survive and not
//     be overwritten by a spurious re-Put. If it reappears after restore, that
//     is the merge-tombstone-replay-order bug — test fails loudly.
//
// Changelog topic name derivation (must match TaskManager.openTask exactly):
//
//	changelogTopic = appID + "-" + storeName + "-changelog"
//	             = "session-e2e" + "-" + "sessions" + "-changelog"
//	             = "session-e2e-sessions-changelog"
//
// Key format in changelog (per WindowPut's Mutation.Key):
//
//	storeName + 0x00 + WindowCompositeKey(kBytes, sessionStart)
//
// Value format (per EncodeSessionValue):
//
//	int64(sessionEnd) big-endian (8 bytes) ‖ accumulatorBytes (JSON int64 count)
//
// WithGrace(30s) prevents the session sweeper from expiring test sessions before
// the bridge record is processed (expiryBoundary = streamTime - gapMs - graceMs;
// with graceMs=30000 the boundary stays negative throughout the test).
//
// Session construction and bridge analysis:
//
//	gap = 10s = 10000ms
//	a@1000 → create session [1000,1000] count=1
//	a@3000 → match [1000,1000]: sEnd+gap=11000>=3000 ✓ → merge [1000,3000] count=2
//	a@14000 → [1000,3000]: sEnd+gap=13000<14000 ✗ → new session [14000,14000] count=1
//	a@16000 → match [14000,14000] → merge [14000,16000] count=2
//	bridge a@8000 (after both sessions exist):
//	  [1000,3000]: sEnd+gap=3000+10000=13000>=8000 ✓, sStart-gap=1000-10000=-9000<=8000 ✓ → match
//	  [14000,16000]: sEnd+gap=16000+10000=26000>=8000 ✓, sStart-gap=14000-10000=4000<=8000 ✓ → match
//	  → mergedStart=min(1000,14000,8000)=1000, mergedEnd=max(3000,16000,8000)=16000
//	  → WindowDelete([a,sStart=1000]), WindowDelete([a,sStart=14000])
//	  → WindowPut([a,sStart=1000], sEnd=16000, count=5)
//	b@50000 → new session [50000,50000] count=1
//
// Phase-2 restore proof: a@17000 post-restore:
//
//	[1000,16000]: sEnd+gap=26000>=17000 ✓ → merge [1000,17000] count=6
//	count=6 proves restored baseline=5 (if restore failed, count would be 1)

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/kafka"
	"github.com/mortezaPRK/gstream/internal/runtime"
	"github.com/mortezaPRK/gstream/internal/state"
	"github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	kgo "github.com/twmb/franz-go/pkg/kgo"
)

func TestE2E_SessionCountRestoreAfterRestart(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping session E2E integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// 1. Start Kafka broker.
	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-cluster-session"),
		testcontainers.WithEnv(map[string]string{
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
		appID     = "session-e2e"
		srcTopic  = "session-input"
		storeName = "sessions"
		// Derived: appID + "-" + binding.ChangelogTopic + "-changelog"
		// binding.ChangelogTopic == storeName per session.go Aggregate.
		changelogTopic = "session-e2e-sessions-changelog"
	)

	// Session boundaries for assertions.
	const (
		sessionAStart1   = int64(1_000)  // merged session start after bridge
		sessionAEnd1     = int64(16_000) // merged session end after bridge
		sessionAStart2   = int64(14_000) // merged-away session (must be tombstoned)
		sessionBStart    = int64(50_000)
		phaseACount      = int64(5) // count for merged session a[1000,16000]
		phaseBCount      = int64(1) // count for b[50000,50000]
		postRestoreCount = int64(6) // count for a[1000,17000] after restore + a@17000
		postRestoreTsMs  = int64(17_000)
	)

	// 2. Temp state dir.
	stateDir, err := os.MkdirTemp("", "gstream-session-e2e-*")
	if err != nil {
		t.Fatalf("mktemp: %v", err)
	}
	defer os.RemoveAll(stateDir)
	t.Logf("stateDir: %s", stateDir)

	// 3. Create topics.
	if err := kafka.EnsureTopics(ctx, brokers, []kafka.TopicSpec{
		{Name: srcTopic, Partitions: 1, ReplicationFactor: 1},
		{
			Name:              changelogTopic,
			Partitions:        1,
			ReplicationFactor: 1,
			Configs:           map[string]string{"cleanup.policy": "compact"},
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
	// PHASE 1: materialize sessions and prove merge
	// =========================================================================

	bt1 := buildSessionCountTopology(srcTopic, storeName)
	adapter1, err := runtime.NewAdapter(bt1, cfg, slog.Default())
	if err != nil {
		t.Fatalf("NewAdapter p1: %v", err)
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

	// Batch A: create session a[1000,3000]=2.
	// gap=10000ms; 3000-1000=2000ms < gap → both records merge.
	produceWindowedRecords(t, ctx, brokers, srcTopic, gstream.JSONSerde[string]{}, []windowedRecord{
		{key: "a", value: "x", tsMs: 1_000},
		{key: "a", value: "x", tsMs: 3_000},
	})
	t.Log("phase-1 batchA: produced a@1000, a@3000 (→ session[1000,3000] count=2)")

	// Wait until a[sStart=1000] appears in changelog with count=2.
	s1 := pollSessionChangelog(t, ctx, brokers, changelogTopic, storeName,
		[]sessionExpected{
			{key: "a", sStart: sessionAStart1, count: 2},
		},
	)
	t.Logf("phase-1 batchA: confirmed a[sStart=1000] count=2 sEnd=%d", s1[sessionStateKey{"a", sessionAStart1}].sEnd)

	// Batch B: create session a[14000,16000]=2.
	// 14000-3000=11000ms > gap=10000ms → second session does NOT merge with first.
	produceWindowedRecords(t, ctx, brokers, srcTopic, gstream.JSONSerde[string]{}, []windowedRecord{
		{key: "a", value: "x", tsMs: 14_000},
		{key: "a", value: "x", tsMs: 16_000},
	})
	t.Log("phase-1 batchB: produced a@14000, a@16000 (→ session[14000,16000] count=2; gap=11000>10000 → separate)")

	// Wait until a[sStart=14000] appears in changelog with count=2.
	s2 := pollSessionChangelog(t, ctx, brokers, changelogTopic, storeName,
		[]sessionExpected{
			{key: "a", sStart: sessionAStart2, count: 2},
		},
	)
	t.Logf("phase-1 batchB: confirmed a[sStart=14000] count=2 sEnd=%d", s2[sessionStateKey{"a", sessionAStart2}].sEnd)

	// Batch C: bridge record a@8000.
	// Both sessions match:
	//   session[1000,3000]:   3000+10000=13000 >= 8000 ✓;  1000-10000=-9000 <= 8000 ✓
	//   session[14000,16000]: 16000+10000=26000 >= 8000 ✓; 14000-10000=4000 <= 8000 ✓
	// → merge [1000,16000] count=5; tombstones for sStart=1000 and sStart=14000.
	produceWindowedRecords(t, ctx, brokers, srcTopic, gstream.JSONSerde[string]{}, []windowedRecord{
		{key: "a", value: "x", tsMs: 8_000},
	})
	t.Log("phase-1 batchC: produced bridge a@8000 (merges [1000,3000] and [14000,16000] → [1000,16000] count=5)")

	// Wait until a[sStart=1000] has count=5 AND a[sStart=14000] is absent (tombstoned).
	// The merge writes: Delete[sStart=1000], Delete[sStart=14000], Put[sStart=1000, sEnd=16000, count=5]
	// all in one PostBatch flush, so when count=5 arrives, tombstones have already been applied.
	s3 := pollSessionChangelog(t, ctx, brokers, changelogTopic, storeName,
		[]sessionExpected{
			{key: "a", sStart: sessionAStart1, count: phaseACount},
		},
	)
	mergedEntry := s3[sessionStateKey{"a", sessionAStart1}]
	t.Logf("phase-1 batchC: confirmed merged a[sStart=1000, sEnd=%d] count=%d",
		mergedEntry.sEnd, mergedEntry.count)
	if mergedEntry.sEnd != sessionAEnd1 {
		t.Errorf("phase-1: merged session sEnd=%d, want %d", mergedEntry.sEnd, sessionAEnd1)
	}
	// Verify tombstone: sStart=14000 must be absent.
	if _, present := s3[sessionStateKey{"a", sessionAStart2}]; present {
		t.Error("TOMBSTONE BUG: a[sStart=14000] still present in changelog state after merge; expected tombstone")
	} else {
		t.Log("phase-1 batchC: tombstone confirmed — a[sStart=14000] absent after merge")
	}
	t.Log("phase-1 batchC: MERGE PROVEN — bridge correctly joined two separate sessions")

	// Batch D: key "b" single session.
	produceWindowedRecords(t, ctx, brokers, srcTopic, gstream.JSONSerde[string]{}, []windowedRecord{
		{key: "b", value: "y", tsMs: 50_000},
	})
	t.Log("phase-1 batchD: produced b@50000 (→ session[50000,50000] count=1)")

	pollSessionChangelog(t, ctx, brokers, changelogTopic, storeName,
		[]sessionExpected{
			{key: "b", sStart: sessionBStart, count: phaseBCount},
		},
	)
	t.Log("phase-1 batchD: confirmed b[sStart=50000] count=1")

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

	// Assert Pebble state directly after clean shutdown.
	partitionDir := filepath.Join(stateDir, appID, "partition-0")
	assertSessionPebble(t, partitionDir, storeName, "a", []sessionPebbleExpected{
		{sStart: sessionAStart1, sEnd: sessionAEnd1, count: phaseACount, label: "a[1000,16000]"},
	})
	t.Log("phase-1: Pebble direct assertion — a[1000,16000]=5 ✓")

	if checkSessionPresentInPebble(t, partitionDir, storeName, "a", sessionAStart2) {
		t.Error("phase-1: a[sStart=14000] unexpectedly present in Pebble (tombstone should have deleted it)")
	} else {
		t.Log("phase-1: a[sStart=14000] absent in Pebble ✓ (tombstone applied)")
	}

	assertSessionPebble(t, partitionDir, storeName, "b", []sessionPebbleExpected{
		{sStart: sessionBStart, sEnd: sessionBStart, count: phaseBCount, label: "b[50000,50000]"},
	})
	t.Log("phase-1: Pebble direct assertion — b[50000,50000]=1 ✓")

	// =========================================================================
	// PHASE 2: restore-after-restart
	// =========================================================================

	// Delete local Pebble partition dir. Changelog in Kafka is the only truth.
	if err := os.RemoveAll(partitionDir); err != nil {
		t.Fatalf("RemoveAll partition dir: %v", err)
	}
	t.Logf("phase-2: deleted partition dir %s", partitionDir)

	bt2 := buildSessionCountTopology(srcTopic, storeName)
	adapter2, err := runtime.NewAdapter(bt2, cfg, slog.Default())
	if err != nil {
		t.Fatalf("NewAdapter p2: %v", err)
	}

	// Wrap onAssigned to signal when RestoreFromChangelog completes.
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

	// Produce post-restore record: a@17000.
	// [1000,16000]: sEnd+gap=26000>=17000 ✓, sStart-gap=-9000<=17000 ✓ → merge [1000,17000]=6.
	// If restore was correct: count = 5+1 = 6.
	// If restore failed (fresh state): count = 0+1 = 1.
	// count=6 definitively proves restored baseline=5.
	produceWindowedRecords(t, ctx, brokers, srcTopic, gstream.JSONSerde[string]{}, []windowedRecord{
		{key: "a", value: "x", tsMs: postRestoreTsMs},
	})
	t.Logf("phase-2: produced a@%d (→ extends session; restored baseline=5 → expect count=%d)",
		postRestoreTsMs, postRestoreCount)

	s4 := pollSessionChangelog(t, ctx, brokers, changelogTopic, storeName,
		[]sessionExpected{
			{key: "a", sStart: sessionAStart1, count: postRestoreCount},
		},
	)
	extendedEntry := s4[sessionStateKey{"a", sessionAStart1}]
	t.Logf("phase-2: RESTORE PROVEN — a[sStart=1000, sEnd=%d] count=%d (baseline=5 restored, not 0)",
		extendedEntry.sEnd, extendedEntry.count)

	// Final tombstone survival check: sStart=14000 must still be absent.
	if _, present := s4[sessionStateKey{"a", sessionAStart2}]; present {
		t.Error("CRITICAL tombstone regression: a[sStart=14000] reappeared in changelog after phase-2 processing")
	} else {
		t.Log("phase-2: final tombstone check — a[sStart=14000] remains absent ✓")
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
	t.Log("phase-2: shutdown complete")

	// Post-shutdown Pebble assertions: DB is now closed by task cleanup; safe to open.
	assertSessionPebble(t, partitionDir, storeName, "a", []sessionPebbleExpected{
		// After a@17000, merged session extends to sEnd=17000.
		{sStart: sessionAStart1, sEnd: postRestoreTsMs, count: postRestoreCount, label: "post-restore a[1000,17000]"},
	})
	t.Log("phase-2: post-shutdown Pebble assertion — a[1000,17000]=6 ✓")

	if checkSessionPresentInPebble(t, partitionDir, storeName, "a", sessionAStart2) {
		t.Error("CRITICAL tombstone regression: a[sStart=14000] present in Pebble after phase-2; tombstone replay broken")
	} else {
		t.Log("phase-2: post-shutdown Pebble tombstone confirmed — a[sStart=14000] absent ✓")
	}

	t.Log("P3b VERIFIED — session windows correct, merge+restore proven")
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// buildSessionCountTopology builds:
//
//	Stream[string,string] → GroupByKey → SessionWindowedBy(10s, grace=30s) → Count(storeName)
//
// grace=30s prevents the sweeper from expiring sessions before the bridge record
// is processed (expiryBoundary = streamTime - gapMs - graceMs stays negative).
func buildSessionCountTopology(srcTopic, storeName string) *gstream.BuiltTopology {
	b := gstream.NewStreamBuilder()
	gstream.Stream[string, string](b, srcTopic, "session-source",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		SessionWindowedBy(gstream.SessionWindow(10 * time.Second)).
		WithGrace(30 * time.Second).
		Count(storeName)
	return b.Build()
}

// sessionStateKey is the compound key for tracking sessions in the changelog
// consumer's latest-state map.
type sessionStateKey struct {
	key    string
	sStart int64
}

// sessionStateEntry holds the decoded state of one session.
type sessionStateEntry struct {
	sEnd  int64
	count int64
}

// sessionExpected declares one expected session state in the changelog.
type sessionExpected struct {
	key    string
	sStart int64
	count  int64
}

// pollSessionChangelog consumes the session changelog from offset 0, applies
// tombstone semantics, and waits until all expected (key, sStart) → count
// conditions are satisfied. Returns the full latest-state map.
//
// Changelog record key format:
//
//	storeName + 0x00 + WindowCompositeKey(kBytes, sessionStart)
//
// Value format (EncodeSessionValue):
//
//	int64(sessionEnd) big-endian (8 bytes) ‖ JSON-encoded accumulator
//
// Empty value = Kafka tombstone → key deleted from latest map.
func pollSessionChangelog(
	t *testing.T,
	ctx context.Context,
	brokers []string,
	topic, storeName string,
	expected []sessionExpected,
) map[sessionStateKey]sessionStateEntry {
	t.Helper()

	readyCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().AtStart()},
		}),
	)
	if err != nil {
		t.Fatalf("pollSessionChangelog: create consumer: %v", err)
	}
	defer consumer.Close()

	prefix := append([]byte(storeName), 0x00)
	latest := make(map[sessionStateKey]sessionStateEntry)

	want := make(map[sessionStateKey]int64, len(expected))
	for _, e := range expected {
		want[sessionStateKey{e.key, e.sStart}] = e.count
	}

	allMatch := func() bool {
		for sk, wantCount := range want {
			entry, ok := latest[sk]
			if !ok || entry.count != wantCount {
				return false
			}
		}
		return true
	}

	serde := gstream.JSONSerde[string]{}

	for !allMatch() {
		fetches := consumer.PollFetches(readyCtx)
		if fetches.IsClientClosed() {
			break
		}
		if err := readyCtx.Err(); err != nil {
			// Build a human-readable debug snapshot.
			var gotStr string
			for sk, e := range latest {
				gotStr += " " + sk.key + "@" + itoa(sk.sStart) + "=" + itoa(e.count)
			}
			t.Fatalf("pollSessionChangelog: timed out; latest:%s; expected:%v", gotStr, expected)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if !bytes.HasPrefix(r.Key, prefix) {
				return
			}
			composite := r.Key[len(prefix):]
			kBytes, sStart, ok := decodeWindowChangelogKey(composite)
			if !ok {
				return
			}
			strKey, err := serde.Deserialize(kBytes)
			if err != nil {
				return
			}
			sk := sessionStateKey{strKey, sStart}
			if len(r.Value) == 0 {
				delete(latest, sk) // tombstone
				return
			}
			// Decode session value: int64(sEnd, 8 bytes BE) ‖ accBytes.
			sEnd, accBytes, decErr := gstream.DecodeSessionValue(r.Value)
			if decErr != nil {
				return
			}
			var count int64
			if err := json.Unmarshal(accBytes, &count); err != nil {
				return
			}
			latest[sk] = sessionStateEntry{sEnd: sEnd, count: count}
		})

	}
	return latest
}

// sessionPebbleExpected is a single Pebble session assertion.
type sessionPebbleExpected struct {
	sStart int64
	sEnd   int64
	count  int64
	label  string
}

// assertSessionPebble opens Pebble at dbDir and verifies all expected sessions
// for key using RangeForKey. Reports t.Error for each mismatch.
func assertSessionPebble(
	t *testing.T,
	dbDir, storeName, key string,
	expected []sessionPebbleExpected,
) {
	t.Helper()
	db, err := state.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("assertSessionPebble: OpenDB %q: %v", dbDir, err)
	}
	defer db.Close()

	store := state.NewKeyValueStore[[]byte, []byte](storeName, db, gstream.BytesSerde{}, gstream.BytesSerde{})
	kBytes, err := gstream.JSONSerde[string]{}.Serialize(key)
	if err != nil {
		t.Fatalf("assertSessionPebble: serialize key %q: %v", key, err)
	}

	// Collect actual sessions for this key.
	type actual struct{ sStart, sEnd, count int64 }
	var actuals []actual
	if err := store.RangeForKey(kBytes, func(sStart int64, val []byte) bool {
		sEnd, accBytes, decErr := gstream.DecodeSessionValue(val)
		if decErr != nil {
			t.Errorf("assertSessionPebble: DecodeSessionValue sStart=%d: %v", sStart, decErr)
			return false
		}
		var count int64
		if err := json.Unmarshal(accBytes, &count); err != nil {
			t.Errorf("assertSessionPebble: unmarshal count sStart=%d: %v", sStart, err)
			return false
		}
		actuals = append(actuals, actual{sStart, sEnd, count})
		return true
	}); err != nil {
		t.Fatalf("assertSessionPebble: RangeForKey key=%q: %v", key, err)
	}

	// Index by sStart for O(1) lookup.
	byStart := make(map[int64]actual, len(actuals))
	for _, a := range actuals {
		byStart[a.sStart] = a
	}

	for _, e := range expected {
		a, found := byStart[e.sStart]
		if !found {
			t.Errorf("assertSessionPebble[%s]: key=%q sStart=%d not found in Pebble (found %d sessions)",
				e.label, key, e.sStart, len(actuals))
			continue
		}
		if a.count != e.count {
			t.Errorf("assertSessionPebble[%s]: count=%d want=%d", e.label, a.count, e.count)
		}
		if a.sEnd != e.sEnd {
			t.Errorf("assertSessionPebble[%s]: sEnd=%d want=%d", e.label, a.sEnd, e.sEnd)
		}
	}
}

// checkSessionPresentInPebble opens Pebble and returns true if any session for
// (key, sStart) exists. Used to detect spurious re-appearance of tombstoned sessions.
func checkSessionPresentInPebble(t *testing.T, dbDir, storeName, key string, sStart int64) bool {
	t.Helper()
	db, err := state.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("checkSessionPresentInPebble: OpenDB %q: %v", dbDir, err)
	}
	defer db.Close()

	store := state.NewKeyValueStore[[]byte, []byte](storeName, db, gstream.BytesSerde{}, gstream.BytesSerde{})
	kBytes, err := gstream.JSONSerde[string]{}.Serialize(key)
	if err != nil {
		t.Fatalf("checkSessionPresentInPebble: serialize key %q: %v", key, err)
	}

	found := false
	_ = store.RangeForKey(kBytes, func(foundStart int64, _ []byte) bool {
		if foundStart == sStart {
			found = true
			return false // stop early
		}
		return true
	})
	return found
}

// itoa is a minimal int64-to-string helper for debug messages, avoiding fmt import.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := make([]byte, 20)
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
