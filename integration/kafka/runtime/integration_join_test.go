//go:build integration

package runtime_test

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	kafkamodule "github.com/mortezaPRK/gstream/integration/kafka"
	"github.com/mortezaPRK/gstream/internal/kafka"
	kgo "github.com/twmb/franz-go/pkg/kgo"
)

// TestE2E_StreamTableJoin is the P4a-C6 exit criterion.
//
// It proves:
//  1. HIT:     stream record whose key exists in the table produces a joined output.
//  2. MISS:    stream record whose key is absent from the table is silently dropped
//     (inner-join). A control key proves the pipeline is alive so absence
//     of output for the miss key is meaningful.
//  3. RESTORE: after client1 is stopped, client2 (same appID + stateDir) restores
//     the KTable state from the changelog and joins against restored counts.
//
// Topology (two co-partitioned topics, 1 partition each):
//
//	tableTopic  → Stream[string,string] → GroupByKey → Count("join-store", JSONSerde[int64]{}) → KTable[string,int64]
//	streamTopic → Stream[string,string] → JoinTable(table, joiner, ...) → outTopic
//
// Joiner: func(v string, c int64) string { return fmt.Sprintf("%s#%d", v, c) }
//
// Single partition is chosen for determinism: co-partitioning holds trivially and
// there is no key-routing ambiguity across partitions.
//
// Changelog topic name derivation (must match TaskManager.openTask exactly):
//
//	changelogTopic = AppID + "-" + binding.ChangelogTopic + "-changelog"
//	             = "join-e2e" + "-" + "join-store" + "-changelog"
//	             = "join-e2e-join-store-changelog"
//
// CRITICAL: kafka.New receives adapter.SourceTopics() — both tableTopic and
// streamTopic — so the consumer subscribes to both. Passing only one topic
// causes the other source to starve and every join to miss (test hangs).
func TestE2E_StreamTableJoin(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping join E2E integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	// --- 1. Start Kafka broker ---
	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-cluster-join"),
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
		appID          = "join-e2e"
		tableTopic     = "jt-table"
		streamTopic    = "jt-stream"
		outTopic       = "jt-out"
		storeName      = "join-store"
		changelogTopic = "join-e2e-join-store-changelog"
	)

	// --- 2. Temp state dir ---
	stateDir, err := os.MkdirTemp("", "gstream-join-e2e-*")
	if err != nil {
		t.Fatalf("mktemp: %v", err)
	}
	defer os.RemoveAll(stateDir)
	t.Logf("stateDir: %s", stateDir)

	// --- 3. Create topics (1 partition each — co-partitioning trivially holds) ---
	if err := kafka.EnsureTopics(ctx, brokers, []kafka.TopicSpec{
		{Name: tableTopic, Partitions: 1, ReplicationFactor: 1},
		{Name: streamTopic, Partitions: 1, ReplicationFactor: 1},
		{Name: outTopic, Partitions: 1, ReplicationFactor: 1},
		{
			Name:              changelogTopic,
			Partitions:        1,
			ReplicationFactor: 1,
			Configs:           map[string]string{"cleanup.policy": "compact"},
		},
	}); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}
	t.Logf("topics created: %s, %s, %s, %s", tableTopic, streamTopic, outTopic, changelogTopic)

	// --- 4. Validate co-partitioning ---
	if err := kafka.ValidateCoPartitioned(ctx, brokers, []string{tableTopic, streamTopic}); err != nil {
		t.Fatalf("ValidateCoPartitioned: %v", err)
	}

	cfg, err := gstream.Configure(
		gstream.WithName(appID),
		gstream.WithBrokers(brokers...),
		gstream.WithStateDir(stateDir),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	// ==========================================================================
	// PHASE 1: HIT + MISS
	// ==========================================================================

	bt1 := buildJoinE2ETopology(tableTopic, streamTopic, storeName, outTopic)

	adapter1, err := newTestAdapter(bt1, cfg, slog.Default())
	if err != nil {
		t.Fatalf("NewAdapter p1: %v", err)
	}

	// CRITICAL: pass adapter1.SourceTopics() — both tableTopic and streamTopic.
	// A single-topic slice would starve the other source and all joins would miss.
	srcTopics1 := adapter1.SourceTopics()
	t.Logf("source topics: %v", srcTopics1)

	client1, err := kafka.New(cfg, srcTopics1, slog.Default(),
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

	// Set up output consumer before producing any records so no output is missed.
	outConsumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			outTopic: {0: kgo.NewOffset().AtStart()},
		}),
	)
	if err != nil {
		t.Fatalf("outConsumer: %v", err)
	}
	defer outConsumer.Close()

	// --- 5. Produce table records for key "k": 2 records → count=2 ---
	produceJoinRecord(t, ctx, brokers, tableTopic, "k", "ignored1")
	produceJoinRecord(t, ctx, brokers, tableTopic, "k", "ignored2")
	t.Log("phase-1: produced 2 table records for k")

	// Deterministic wait: poll changelog until count["k"]==2.
	// No bare sleep. pollJoinChangelog blocks until the expected state is seen or
	// a 30s timeout fires.
	pollJoinChangelog(t, ctx, brokers, changelogTopic, storeName, map[string]int64{"k": 2})
	t.Log("phase-1: changelog confirmed k=2")

	// --- 6. HIT: produce stream record key "k" val "order" ---
	produceJoinRecord(t, ctx, brokers, streamTopic, "k", "order")
	t.Log("phase-1: produced stream record k=order")

	// Wait for exactly 1 output record; assert key="k" value="order#2".
	hitRecord := waitJoinOutput(t, ctx, outConsumer, 1, 30*time.Second)
	if len(hitRecord) < 1 {
		t.Fatalf("HIT: expected 1 output, got 0")
	}
	assertJoinRecord(t, "HIT", hitRecord[0], "k", "order#2")
	t.Logf("phase-1 HIT passed: %v", hitRecord[0])

	// --- 7. MISS: produce stream record for key "nope" (no table entry) ---
	// Also send a control key "ctrl" that DOES join (after populating table)
	// to prove the pipeline is alive, so absence-of-output for "nope" is meaningful.
	produceJoinRecord(t, ctx, brokers, tableTopic, "ctrl", "x")
	pollJoinChangelog(t, ctx, brokers, changelogTopic, storeName, map[string]int64{"ctrl": 1})
	t.Log("phase-1: ctrl table populated")

	produceJoinRecord(t, ctx, brokers, streamTopic, "nope", "should-miss")
	produceJoinRecord(t, ctx, brokers, streamTopic, "ctrl", "sentinel")
	t.Log("phase-1: produced nope (miss) and ctrl (sentinel hit)")

	// Wait for the sentinel (ctrl#1) to arrive. The nope record must NOT appear.
	sentinelAndMore := waitJoinOutput(t, ctx, outConsumer, 1, 30*time.Second)
	// Verify the sentinel arrived.
	found := false
	for _, r := range sentinelAndMore {
		if r.key == "ctrl" {
			assertJoinRecord(t, "SENTINEL", r, "ctrl", "sentinel#1")
			found = true
		}
		if r.key == "nope" {
			t.Errorf("MISS violation: 'nope' appeared in output (inner-join must drop it)")
		}
	}
	if !found {
		t.Errorf("MISS: sentinel ctrl record did not arrive within timeout")
	}
	t.Log("phase-1 MISS confirmed: nope absent, ctrl arrived")

	// --- 8. Clean shutdown ---
	run1Cancel()
	select {
	case err := <-done1:
		if err != nil {
			t.Errorf("client1.Run returned error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("client1.Run did not stop within 15 s")
	}
	client1.Close()
	t.Log("phase-1: shutdown complete")

	// ==========================================================================
	// PHASE 2: RESTORE — same appID/stateDir, same store
	// ==========================================================================

	// Delete local Pebble partition dir to force changelog restore.
	partitionDir := filepath.Join(stateDir, appID, "partition-0")
	if err := os.RemoveAll(partitionDir); err != nil {
		t.Fatalf("RemoveAll partition dir: %v", err)
	}
	t.Logf("phase-2: deleted partition dir %s", partitionDir)

	bt2 := buildJoinE2ETopology(tableTopic, streamTopic, storeName, outTopic)
	adapter2, err := newTestAdapter(bt2, cfg, slog.Default())
	if err != nil {
		t.Fatalf("NewAdapter p2: %v", err)
	}

	// Wrap onAssigned to detect when restore is complete (mirrors stateful E2E).
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

	// CRITICAL: pass adapter2.SourceTopics() here too — both topics.
	srcTopics2 := adapter2.SourceTopics()
	client2, err := kafka.New(cfg, srcTopics2, slog.Default(),
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

	// Wait for OnAssigned / restore complete.
	select {
	case <-restoreDone:
		t.Log("phase-2: OnAssigned fired; RestoreFromChangelog complete")
	case <-time.After(30 * time.Second):
		t.Fatal("phase-2: timed out waiting for OnAssigned/restore")
	}

	// Produce another stream record for "k"; restored state should have k=2,
	// so output must be "again#2".
	produceJoinRecord(t, ctx, brokers, streamTopic, "k", "again")
	t.Log("phase-2: produced stream record k=again")

	restoreRecord := waitJoinOutput(t, ctx, outConsumer, 1, 30*time.Second)
	// Filter for key "k" (earlier output may be re-read from outTopic offset 0).
	var restoreHit *joinOutputRecord
	for i := range restoreRecord {
		if restoreRecord[i].key == "k" && restoreRecord[i].value == "again#2" {
			restoreHit = &restoreRecord[i]
			break
		}
	}
	if restoreHit == nil {
		// Consume more — outConsumer started at offset 0 and may still be reading earlier records.
		extra := waitJoinOutputBounded(t, ctx, outConsumer, 10, 15*time.Second)
		for i := range extra {
			if extra[i].key == "k" && extra[i].value == "again#2" {
				restoreHit = &extra[i]
				break
			}
		}
	}
	if restoreHit == nil {
		t.Errorf("RESTORE: did not find output record key=k value=again#2 (count not restored from changelog)")
	} else {
		t.Logf("phase-2 RESTORE passed: %v", *restoreHit)
	}

	run2Cancel()
	select {
	case err := <-done2:
		if err != nil {
			t.Errorf("client2.Run returned error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("client2.Run did not stop within 15 s")
	}
	client2.Close()
}

// ─── topology ────────────────────────────────────────────────────────────────

// buildJoinE2ETopology constructs the join topology:
//
//	tableTopic  → Stream[string,string] → GroupByKey → Count(storeName, JSONSerde[int64]{}) → KTable[string,int64]
//	streamTopic → Stream[string,string] → JoinTable(table, joiner, ...) → outTopic
//
// Joiner: "%s#%d" % (streamValue, count)
func buildJoinE2ETopology(tableTopic, streamTopic, storeName, outTopic string) *gstream.BuiltTopology {
	b := gstream.NewStreamBuilder()

	// table sub-graph
	tableSrc := gstream.Stream[string, string](
		b, tableTopic, "tsrc",
		JSONSerde[string]{}, JSONSerde[string]{},
	)
	table := tableSrc.
		GroupByKey(JSONSerde[string]{}, JSONSerde[string]{}).
		Count(storeName, JSONSerde[int64]{})

	// stream sub-graph
	streamSrc := gstream.Stream[string, string](
		b, streamTopic, "ssrc",
		JSONSerde[string]{}, JSONSerde[string]{},
	)

	joined := streamSrc.JoinTable[int64, string](
		table,
		func(v string, c int64) string { return fmt.Sprintf("%s#%d", v, c) },
		JSONSerde[string]{},
		JSONSerde[string]{},
	)

	joined.To(outTopic, "out", JSONSerde[string]{}, JSONSerde[string]{})

	return b.Build()
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// joinOutputRecord is a decoded record from the output topic.
type joinOutputRecord struct {
	key   string
	value string // decoded Go string (JSONSerde[string])
}

func (r joinOutputRecord) String() string {
	return fmt.Sprintf("{key=%s value=%s}", r.key, r.value)
}

// produceJoinRecord produces a single record to topic. Key is raw bytes;
// value is JSON-encoded string (matching JSONSerde[string] in the topology).
func produceJoinRecord(t *testing.T, ctx context.Context, brokers []string, topic, key, value string) {
	t.Helper()
	serde := JSONSerde[string]{}
	kb, _ := serde.Serialize(key)
	vb, _ := serde.Serialize(value)
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("produceJoinRecord: new client: %v", err)
	}
	defer producer.Close()
	res := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: kb, Value: vb})
	if res.FirstErr() != nil {
		t.Fatalf("produceJoinRecord: produce key=%q: %v", key, res.FirstErr())
	}
}

// pollJoinChangelog polls the changelog for the join-store and blocks until all
// expected counts are observed, or a 30s timeout fires.
// Mirrors pollChangelog in integration_stateful_test.go.
func pollJoinChangelog(
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
		t.Fatalf("pollJoinChangelog: create consumer: %v", err)
	}
	defer consumer.Close()

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
			t.Fatalf("pollJoinChangelog: timed out waiting for %v; latest: %v", expected, latest)
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
	return latest
}

// waitJoinOutput polls outConsumer until n records arrive or timeout elapses.
// Returns whatever records arrived.
func waitJoinOutput(t *testing.T, ctx context.Context, consumer *kgo.Client, n int, timeout time.Duration) []joinOutputRecord {
	t.Helper()
	return waitJoinOutputBounded(t, ctx, consumer, n, timeout)
}

// waitJoinOutputBounded collects up to n records from consumer within timeout.
func waitJoinOutputBounded(t *testing.T, ctx context.Context, consumer *kgo.Client, n int, timeout time.Duration) []joinOutputRecord {
	t.Helper()
	serde := JSONSerde[string]{}
	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var out []joinOutputRecord
	for len(out) < n {
		fetches := consumer.PollFetches(readyCtx)
		if fetches.IsClientClosed() {
			break
		}
		if err := readyCtx.Err(); err != nil {
			// Timeout — return whatever we have.
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			key, err := serde.Deserialize(r.Key)
			if err != nil {
				t.Logf("waitJoinOutput: skip record (key decode error): %v", err)
				return
			}
			value, err := serde.Deserialize(r.Value)
			if err != nil {
				t.Logf("waitJoinOutput: skip record (val decode error): %v", err)
				return
			}
			out = append(out, joinOutputRecord{key: key, value: value})
			t.Logf("waitJoinOutput: received key=%s value=%s", key, value)
		})
	}
	return out
}

// assertJoinRecord checks that rec has the expected key and value.
func assertJoinRecord(t *testing.T, label string, rec joinOutputRecord, wantKey, wantValue string) {
	t.Helper()
	if rec.key != wantKey {
		t.Errorf("%s: key: got %q, want %q", label, rec.key, wantKey)
	}
	if rec.value != wantValue {
		t.Errorf("%s: value: got %q, want %q", label, rec.value, wantValue)
	}
}
