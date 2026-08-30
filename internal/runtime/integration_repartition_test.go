//go:build integration

package runtime_test

// TestE2E_RepartitionDSL is the P4b-F1-C5 exit criterion test.
//
// It proves the full DSL→adapter→admin→broker round-trip:
// a key-changing SelectKey followed by .Repartition() followed by
// .GroupByKey().Count() produces correct per-new-key results, and the
// ALO crash case yields at-most-2x, never 4x.
//
// Topology:
//
//	Stream[string,string](input, "src") → SelectKey(rekey) →
//	Repartition("rp", 2) → GroupByKey → Count("cnt")
//
// Repartition topic: <appID>-rp-repartition (2 partitions, cleanup.policy=delete)
// Changelog topic:   <appID>-cnt-changelog  (2 partitions, cleanup.policy=compact)
//
// # Case 1 (CORRECTNESS)
//
// 6 input records: 4 map to newKey="A", 2 to newKey="D".
// SelectKey fn: key starting with 'a' or 'b' → "A", else → "D".
// Deterministic wait: pollRepartitionChangelog blocks until totals reach
// A=4 D=2 in the changelog. Assert no cross-partition contamination.
//
// # Case 2 (ALO-4x CRASH)
//
// Same 6 input records. Run1 is cancelled before offsets are guaranteed to
// commit. Run2 restarts on the same appID+stateDir and drains until stable.
//
// Invariant: count[key] ∈ [input_count, 2×input_count].
//   - Lower bound proves no data loss (at-least-once delivery).
//   - Upper bound proves repartition offsets are committed atomically with
//     source offsets: if they were not, a crash after producing repartition
//     records AND before committing either set of offsets would replay both
//     the source→repartition path and the repartition→count path, yielding
//     (restored 1x) + (re-consumed repartition 1x) + (re-produced repartition 1x)
//     = 3x, and with 2 such epochs 4x would be theoretically reachable.
//     ≤2x confirms the single-atomic-commit design.
//
// Deterministic sync: no bare time.Sleep. All waits are changelog-poll loops
// with bounded timeouts that call t.Fatalf on expiry.

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"log/slog"
	"os"
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/kafka"
	"github.com/mortezaPRK/gstream/internal/runtime"
	"github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	kgo "github.com/twmb/franz-go/pkg/kgo"
)

func TestE2E_RepartitionDSL(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping repartition DSL E2E integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// --- 1. Start Kafka broker ---
	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-cluster-repart-dsl"),
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

	const storeName = "cnt"

	// Input: 4 records map to newKey="A", 2 to newKey="D".
	// SelectKey fn: key[0]=='a' or key[0]=='b' → "A", else → "D".
	type repartInput = struct{ key, value string }
	inputs := []repartInput{
		{"alpha-1", "v1"}, {"alpha-2", "v2"},
		{"bravo-1", "v3"}, {"bravo-2", "v4"},
		{"charlie-1", "v5"}, {"charlie-2", "v6"},
	}
	wantCounts := map[string]int64{"A": 4, "D": 2}

	// ==========================================================================
	// CASE 1: CORRECTNESS
	// ==========================================================================
	t.Run("Correctness", func(t *testing.T) {
		const (
			appID          = "repart-e2e-c1"
			inputTopic     = "rp-input-c1"
			changelogTopic = "repart-e2e-c1-cnt-changelog"
			repartTopic    = "repart-e2e-c1-rp-repartition"
		)

		stateDir, err := os.MkdirTemp("", "gstream-repart-c1-*")
		if err != nil {
			t.Fatalf("mktemp: %v", err)
		}
		defer os.RemoveAll(stateDir)

		cfg, err := gstream.Configure(
			gstream.WithName(appID),
			gstream.WithBrokers(brokers...),
			gstream.WithStateDir(stateDir),
		)
		if err != nil {
			t.Fatalf("Configure: %v", err)
		}

		bt := buildRepartCountTopology(inputTopic, storeName)

		// Create input + changelog topics; EnsureRepartitionTopics creates repartition topic.
		if err := kafka.EnsureTopics(ctx, brokers, []kafka.TopicSpec{
			{Name: inputTopic, Partitions: 2, ReplicationFactor: 1},
			{Name: changelogTopic, Partitions: 2, ReplicationFactor: 1,
				Configs: map[string]string{"cleanup.policy": "compact"}},
		}); err != nil {
			t.Fatalf("EnsureTopics: %v", err)
		}
		if err := kafka.EnsureRepartitionTopics(ctx, cfg, bt); err != nil {
			t.Fatalf("EnsureRepartitionTopics: %v", err)
		}
		t.Logf("topics created: input=%s (2p), changelog=%s (2p compact), repartition=%s (2p delete)",
			inputTopic, changelogTopic, repartTopic)

		adapter, err := runtime.NewAdapter(bt, cfg, slog.Default())
		if err != nil {
			t.Fatalf("NewAdapter: %v", err)
		}

		// CRITICAL: pass adapter.SourceTopics() so client subscribes to both input
		// and repartition topics. Matches the pattern from join E2E.
		srcTopics := adapter.SourceTopics()
		t.Logf("source topics: %v", srcTopics)

		// Verify SourceTopics includes both the input and repartition topics.
		hasInput, hasRepart := false, false
		for _, tp := range srcTopics {
			if tp == inputTopic {
				hasInput = true
			}
			if tp == repartTopic {
				hasRepart = true
			}
		}
		if !hasInput || !hasRepart {
			t.Fatalf("SourceTopics missing required topics: hasInput=%v hasRepart=%v; got %v",
				hasInput, hasRepart, srcTopics)
		}
		t.Logf("SourceTopics confirmed: input=%q and repartition=%q both subscribed", inputTopic, repartTopic)

		client, err := kafka.New(cfg, srcTopics, slog.Default(),
			kafka.WithLifecycle(adapter.LifecycleCallbacks()),
			kafka.WithPostBatch(adapter.PostBatchHook()),
			kafka.WithHealthGate(adapter.HealthGateHook()),
		)
		if err != nil {
			t.Fatalf("kafka.New: %v", err)
		}

		runCtx, runCancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- client.Run(runCtx, adapter.ProcessFunc()) }()

		// Produce 6 input records.
		repartProduce(t, ctx, brokers, inputTopic, inputs)
		t.Logf("correctness: produced %d input records to %s", len(inputs), inputTopic)

		// Deterministic wait: poll changelog (both partitions) until A=4, D=2.
		// No bare sleep; pollRepartitionChangelog t.Fatalf on 30s timeout.
		totals, keyPartitions := pollRepartitionChangelog(t, ctx, brokers, changelogTopic, storeName, wantCounts)
		t.Logf("correctness: changelog counts confirmed: %v", totals)
		t.Logf("correctness: per-key changelog partitions: %v", keyPartitions)

		// Assert exact counts.
		for _, nk := range []string{"A", "D"} {
			if got, want := totals[nk], wantCounts[nk]; got != want {
				t.Errorf("newKey %q: count=%d, want %d", nk, got, want)
			}
		}

		// Assert no cross-partition contamination: A and D must land on different
		// changelog partitions, confirming murmur2 co-location.
		pA, okA := keyPartitions["A"]
		pD, okD := keyPartitions["D"]
		if okA && okD {
			if pA == pD {
				t.Errorf("CONTAMINATION: newKey A and D both on changelog partition %d — murmur2 co-location broken", pA)
			} else {
				t.Logf("correctness: no contamination — A on p%d, D on p%d", pA, pD)
			}
		}
		t.Logf("correctness PASSED: A=%d/4, D=%d/2", totals["A"], totals["D"])

		runCancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("client.Run returned error: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("client.Run did not stop within 15s after cancel")
		}
		client.Close()
	})

	// ==========================================================================
	// CASE 2: ALO-4x CRASH
	//
	// Strategy (mirrors stateful E2E): wait until A=4, D=2 fully land in the
	// changelog (state materialized + changelog written), THEN cancel run1.
	// The cancel races with the final offset commit: sometimes it hits before the
	// commit (forcing redelivery of the in-flight batch), sometimes after.
	// Both outcomes are valid under ALO:
	//
	//   COMMIT SUCCEEDED before cancel:
	//     run2 sees no new records; final counts stay at 1x. [possible]
	//
	//   COMMIT FAILED (context cancelled races with CommitRecords):
	//     run2 redelivers the in-flight batch (input+repartition records from the
	//     last uncommitted batch). Because the Kafka consumer subscribes to BOTH
	//     input and repartition topics under a SINGLE group, both sets of offsets
	//     are committed in ONE CommitRecords call. A partial crash cannot commit
	//     repartition offsets without also committing source offsets (that would be
	//     the 4x bug). Therefore, redelivery can add AT MOST 1 extra pass → ≤2x.
	//
	// Invariant asserted: count[key] ∈ [1x, 2x].
	//   Lower bound: ≥1x proves no loss (state was fully materialized in changelog).
	//   Upper bound: ≤2x proves repartition offsets are NOT committed separately from
	//     source offsets. If they were, the redelivered source batch would re-produce
	//     new repartition records AND the original uncommitted repartition records
	//     would also replay → 3x or more per crash epoch.
	// ==========================================================================
	t.Run("ALO_Crash", func(t *testing.T) {
		const (
			appID          = "repart-e2e-c2"
			inputTopic     = "rp-input-c2"
			changelogTopic = "repart-e2e-c2-cnt-changelog"
		)

		stateDir, err := os.MkdirTemp("", "gstream-repart-c2-*")
		if err != nil {
			t.Fatalf("mktemp: %v", err)
		}
		defer os.RemoveAll(stateDir)

		cfg, err := gstream.Configure(
			gstream.WithName(appID),
			gstream.WithBrokers(brokers...),
			gstream.WithStateDir(stateDir),
		)
		if err != nil {
			t.Fatalf("Configure: %v", err)
		}

		bt1 := buildRepartCountTopology(inputTopic, storeName)

		if err := kafka.EnsureTopics(ctx, brokers, []kafka.TopicSpec{
			{Name: inputTopic, Partitions: 2, ReplicationFactor: 1},
			{Name: changelogTopic, Partitions: 2, ReplicationFactor: 1,
				Configs: map[string]string{"cleanup.policy": "compact"}},
		}); err != nil {
			t.Fatalf("EnsureTopics: %v", err)
		}
		if err := kafka.EnsureRepartitionTopics(ctx, cfg, bt1); err != nil {
			t.Fatalf("EnsureRepartitionTopics: %v", err)
		}

		// --- Run 1: produce records, wait for FULL materialization, then cancel ---
		// Cancel races with the final offset commit — the interesting ALO window.
		// Mirrors stateful E2E: cancel AFTER changelog confirms correct state so
		// that the crash scenario is "commit race" not "incomplete state write".

		adapter1, err := runtime.NewAdapter(bt1, cfg, slog.Default())
		if err != nil {
			t.Fatalf("NewAdapter run1: %v", err)
		}
		srcTopics1 := adapter1.SourceTopics()

		client1, err := kafka.New(cfg, srcTopics1, slog.Default(),
			kafka.WithLifecycle(adapter1.LifecycleCallbacks()),
			kafka.WithPostBatch(adapter1.PostBatchHook()),
			kafka.WithHealthGate(adapter1.HealthGateHook()),
		)
		if err != nil {
			t.Fatalf("kafka.New run1: %v", err)
		}

		run1Ctx, run1Cancel := context.WithCancel(ctx)
		done1 := make(chan error, 1)
		go func() { done1 <- client1.Run(run1Ctx, adapter1.ProcessFunc()) }()

		// Produce 6 input records.
		repartProduce(t, ctx, brokers, inputTopic, inputs)
		t.Log("ALO: produced 6 input records")

		// Wait for FULL materialization: A=4, D=2 confirmed in changelog.
		// Ensures the crash scenario is "commit race" not "incomplete state write".
		// NOTE: this may also trigger a "failed to commit offsets ... context canceled"
		// WARN on cancel — that is the desirable ALO race we test for.
		_, _ = pollRepartitionChangelog(t, ctx, brokers, changelogTopic, storeName, wantCounts)
		t.Logf("ALO: changelog confirmed A=%d, D=%d — cancelling run1 (crash simulation)", wantCounts["A"], wantCounts["D"])

		run1Cancel()
		select {
		case err := <-done1:
			if err != nil {
				t.Logf("run1 returned (expected on cancel): %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("run1 did not stop within 15s")
		}
		client1.Close()
		t.Log("ALO: run1 closed")

		// --- Run 2: restart on same appID+stateDir, drain until stable ---
		// If run1's last offset commit raced with cancel: redelivery adds ≤1 extra
		// pass. If commit succeeded: no redelivery, counts stay at 1x.

		bt2 := buildRepartCountTopology(inputTopic, storeName)
		adapter2, err := runtime.NewAdapter(bt2, cfg, slog.Default())
		if err != nil {
			t.Fatalf("NewAdapter run2: %v", err)
		}
		srcTopics2 := adapter2.SourceTopics()

		// Wrap OnAssigned to detect when restore completes (mirrors stateful + join E2Es).
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

		client2, err := kafka.New(cfg, srcTopics2, slog.Default(),
			kafka.WithLifecycle(wrappedAssigned, onRevoked2),
			kafka.WithPostBatch(adapter2.PostBatchHook()),
			kafka.WithHealthGate(adapter2.HealthGateHook()),
		)
		if err != nil {
			t.Fatalf("kafka.New run2: %v", err)
		}

		run2Ctx, run2Cancel := context.WithCancel(ctx)
		defer run2Cancel()
		done2 := make(chan error, 1)
		go func() { done2 <- client2.Run(run2Ctx, adapter2.ProcessFunc()) }()

		// Wait for restore to complete.
		select {
		case <-restoreDone:
			t.Log("ALO: run2 OnAssigned fired, state restore complete")
		case <-time.After(30 * time.Second):
			t.Fatal("ALO: timed out waiting for run2 OnAssigned")
		}

		// Drain: wait for minimum counts (no loss) AND quiescence (no new changelog
		// records for stabilityWindow). Final counts may be 1x (commit succeeded) or
		// 2x (commit raced with cancel and lost).
		finalTotals := pollChangelogStableMin(t, ctx, brokers, changelogTopic, storeName,
			wantCounts, 3*time.Second)
		t.Logf("ALO: final counts after restart+drain: %v", finalTotals)

		// Assert ALO invariants:
		//   1. count[key] >= input_count[key]  — no data loss (at-least-once).
		//   2. count[key] <= 2*input_count[key] — ≤2x duplication (NOT 3x or 4x).
		//
		// The ≤2x bound holds because:
		//   - Changelog was written BEFORE cancel (verified above).
		//   - Run2 restores from changelog → state starts at 1x.
		//   - If offset commit raced and failed, BOTH source and repartition topic
		//     offsets are uncommitted (single CommitRecords call covers all assigned
		//     partitions). Redelivery replays the last in-flight batch, adding ≤1x.
		//   - 3x would require TWO uncommitted epochs, impossible with one cancel.
		//   - 4x requires repartition offsets committed independently of source offsets
		//     (a design bug): source redelivery re-produces repartition records AND the
		//     original uncommitted repartition records also replay. This test catches
		//     that bug if it exists.
		allOK := true
		for _, nk := range []string{"A", "D"} {
			got := finalTotals[nk]
			want := wantCounts[nk]
			if got < want {
				t.Errorf("ALO LOSS: newKey %q count=%d < input=%d — data loss", nk, got, want)
				allOK = false
			} else if got > 2*want {
				t.Errorf("ALO EXCESS: newKey %q count=%d > 2×input=%d — more than 2x (4x bug? repartition offsets committed independently of source offsets)",
					nk, got, want)
				allOK = false
			} else {
				t.Logf("ALO OK: %q count=%d in [%d, %d] (1x input=%d)", nk, got, want, 2*want, want)
			}
		}
		if allOK {
			t.Log("ALO PASSED: no loss (≥1x), bounded (≤2x), not 4x — repartition offset commit is atomic with source offset commit")
		}

		run2Cancel()
		select {
		case err := <-done2:
			if err != nil {
				t.Errorf("run2 returned error: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("run2 did not stop within 15s")
		}
		client2.Close()
	})
}

// ─── topology ────────────────────────────────────────────────────────────────

// buildRepartCountTopology builds:
//
//	Stream[string,string](inputTopic, "src") → SelectKey → Repartition("rp",2) →
//	GroupByKey → Count(storeName)
//
// SelectKey fn: key starting with 'a' or 'b' → "A", else → "D".
// Repartition uses 2 partitions; repartition topic = <appID>-rp-repartition.
func buildRepartCountTopology(inputTopic, storeName string) *gstream.BuiltTopology {
	b := gstream.NewStreamBuilder()
	ks := gstream.JSONSerde[string]{}
	vs := gstream.JSONSerde[string]{}

	gstream.Stream[string, string](b, inputTopic, "src", ks, vs).
		SelectKey(func(k, _ string) string {
			if len(k) > 0 && (k[0] == 'a' || k[0] == 'b') {
				return "A"
			}
			return "D"
		}).
		Repartition("rp", 2, ks, vs).
		GroupByKey(ks, vs).
		Count(storeName)

	return b.Build()
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// repartProduce produces records to topic using JSONSerde[string] for both key
// and value — matching the topology's ks/vs serdes.
func repartProduce(t *testing.T, ctx context.Context, brokers []string, topic string, inputs []struct{ key, value string }) {
	t.Helper()
	serde := gstream.JSONSerde[string]{}
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("repartProduce: new client: %v", err)
	}
	defer producer.Close()
	for _, inp := range inputs {
		kb, _ := serde.Serialize(inp.key)
		vb, _ := serde.Serialize(inp.value)
		res := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: kb, Value: vb})
		if res.FirstErr() != nil {
			t.Fatalf("repartProduce: produce %q: %v", inp.key, res.FirstErr())
		}
	}
}

// pollRepartitionChangelog consumes the changelog topic from offset 0 across ALL
// partitions and blocks until every key in expected has reached its expected
// int64 value. Returns the full latest-value map and the last-seen partition per
// key (for contamination checks). Calls t.Fatalf on 30s timeout.
//
// Uses ConsumeTopics+ConsumeResetOffset to start from partition 0 (start) of
// every partition simultaneously, matching the multi-partition (2p) changelog.
func pollRepartitionChangelog(
	t *testing.T,
	ctx context.Context,
	brokers []string,
	topic, storeName string,
	expected map[string]int64,
) (map[string]int64, map[string]int32) {
	t.Helper()

	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("pollRepartitionChangelog: create consumer: %v", err)
	}
	defer consumer.Close()

	prefix := append([]byte(storeName), 0x00)
	latest := make(map[string]int64)
	keyPartition := make(map[string]int32) // last-seen changelog partition per key

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
			t.Fatalf("pollRepartitionChangelog: timed out; expected %v, got %v", expected, latest)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if !bytes.HasPrefix(r.Key, prefix) {
				return
			}
			encodedKey := r.Key[len(prefix):]
			var strKey string
			if err := json.Unmarshal(encodedKey, &strKey); err != nil {
				return
			}
			keyPartition[strKey] = r.Partition
			if len(r.Value) == 0 {
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

	return latest, keyPartition
}

// pollChangelogStableMin polls the changelog (all partitions) until minCounts
// are satisfied AND no new records arrive for stabilityWindow. This guards
// against returning before in-flight ALO redeliveries have settled.
// Returns the final latest-value map. Calls t.Fatalf on 60s overall timeout.
func pollChangelogStableMin(
	t *testing.T,
	ctx context.Context,
	brokers []string,
	topic, storeName string,
	minCounts map[string]int64,
	stabilityWindow time.Duration,
) map[string]int64 {
	t.Helper()

	readyCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("pollChangelogStableMin: create consumer: %v", err)
	}
	defer consumer.Close()

	prefix := append([]byte(storeName), 0x00)
	latest := make(map[string]int64)
	lastUpdate := time.Now()
	minMet := false

	allMinMet := func() bool {
		for k, want := range minCounts {
			if latest[k] < want {
				return false
			}
		}
		return true
	}

	for {
		// Short-window poll to detect quiescence without blocking indefinitely.
		shortCtx, shortCancel := context.WithTimeout(readyCtx, 300*time.Millisecond)
		fetches := consumer.PollFetches(shortCtx)
		shortCancel()

		if readyCtx.Err() != nil {
			t.Fatalf("pollChangelogStableMin: overall timeout; latest: %v, minCounts: %v", latest, minCounts)
		}

		newData := false
		fetches.EachRecord(func(r *kgo.Record) {
			if !bytes.HasPrefix(r.Key, prefix) {
				return
			}
			encodedKey := r.Key[len(prefix):]
			var strKey string
			if err := json.Unmarshal(encodedKey, &strKey); err != nil {
				return
			}
			var count int64
			if len(r.Value) == 0 {
				delete(latest, strKey)
				newData = true
				return
			}
			if err := json.Unmarshal(r.Value, &count); err != nil {
				return
			}
			if prev, ok := latest[strKey]; !ok || count != prev {
				newData = true
			}
			latest[strKey] = count
		})

		if newData {
			lastUpdate = time.Now()
		}

		if !minMet && allMinMet() {
			minMet = true
			lastUpdate = time.Now() // reset stability timer after min is first met
		}

		if minMet && time.Since(lastUpdate) >= stabilityWindow {
			break
		}
	}

	return latest
}
