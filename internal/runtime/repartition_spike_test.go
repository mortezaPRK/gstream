//go:build integration

package runtime_test

// TestRepartitionRoundTrip is the P4b-F1-C0 spike: prove that a SINGLE kgo.Client
// can produce to an internal repartition topic AND re-consume that same topic in
// the same Run loop, that murmur2 spreads keys across both repartition partitions,
// and that per-key counts are correct after the round-trip.
//
// Design:
//   - inputTopic     (2 partitions): source records with original keys.
//   - repartTopic    (2 partitions, cleanup.policy=delete): internal repartition topic.
//
// ProcessFunc routing:
//   - records from inputTopic  → re-key (newKey = "A" or "B") + produce to repartTopic
//     via OutRecord{Partition.IsValid=false} so murmur2 routes by newKey.
//   - records from repartTopic → accumulate per-(partition,newKey) count in an
//     in-memory map (simulates aggregation; detects cross-partition contamination).
//
// Proves:
//  1. NO DEADLOCK  — test completes without hanging to timeout.
//  2. MURMUR2 SPREAD — both partitions of repartTopic receive records (not all to p0).
//  3. PER-KEY CORRECTNESS — each newKey's total count equals input records that map to it;
//     no cross-partition contamination (each key lands on exactly one partition).
//  4. ALO CRASH (best-effort) — noted as deferred if infeasible in spike.

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/kafka"
	"github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestRepartitionRoundTrip(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping repartition spike")
	}

	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// -------------------------------------------------------------------------
	// 1. Start Kafka broker.
	// -------------------------------------------------------------------------
	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-cluster-repart"),
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
		inputTopic  = "repart-input"
		repartTopic = "repart-internal"
		appID       = "gstream-repart-spike"
	)

	// -------------------------------------------------------------------------
	// 2. Create topics with 2 partitions each.
	// -------------------------------------------------------------------------
	if err := kafka.EnsureTopics(ctx, brokers, []kafka.TopicSpec{
		{Name: inputTopic, Partitions: 2, ReplicationFactor: 1},
		{Name: repartTopic, Partitions: 2, ReplicationFactor: 1, Configs: map[string]string{
			"cleanup.policy": "delete",
		}},
	}); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}
	t.Logf("created topics %s (2p) and %s (2p)", inputTopic, repartTopic)

	// -------------------------------------------------------------------------
	// 3. Pre-produce input records. Keys are chosen so their re-keyed values
	//    (newKey "A" and "B") hash to DIFFERENT partitions under murmur2.
	//    We verify partition spread after the round-trip; the correctness claim
	//    is independent of which partition each key hashes to.
	//
	//    Input: 6 records — 4 map to newKey="A", 2 map to newKey="D".
	//    Pre-verified murmur2 assignments (2 partitions):
	//      "A" → partition 0
	//      "D" → partition 1
	//    newKey assignment: if original key starts with 'a' or 'b' → "A", else → "D".
	// -------------------------------------------------------------------------
	type inputRecord struct {
		key   string
		value string
	}
	inputs := []inputRecord{
		{"alpha-1", "v1"},   // newKey="A" → p0
		{"alpha-2", "v2"},   // newKey="A" → p0
		{"bravo-1", "v3"},   // newKey="A" → p0
		{"bravo-2", "v4"},   // newKey="A" → p0
		{"charlie-1", "v5"}, // newKey="D" → p1
		{"charlie-2", "v6"}, // newKey="D" → p1
	}
	// newKey mapping — "A" hashes to p0, "D" hashes to p1 (murmur2, 2 partitions)
	newKeyFor := func(origKey string) string {
		if origKey[0] == 'a' || origKey[0] == 'b' {
			return "A"
		}
		return "D"
	}
	// Expected per-newKey total after round-trip
	wantCounts := map[string]int{
		"A": 4,
		"D": 2,
	}

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	for _, inp := range inputs {
		res := producer.ProduceSync(ctx, &kgo.Record{
			Topic: inputTopic,
			Key:   []byte(inp.key),
			Value: []byte(inp.value),
		})
		if res.FirstErr() != nil {
			t.Fatalf("produce input %q: %v", inp.key, res.FirstErr())
		}
	}
	producer.Close()
	t.Logf("produced %d records to %s", len(inputs), inputTopic)

	// -------------------------------------------------------------------------
	// 4. Build the gstream kafka.Client subscribed to BOTH topics.
	//    ProcessFunc:
	//      - inputTopic record  → produce to repartTopic with new key (Partition.IsValid=false)
	//      - repartTopic record → accumulate into in-memory aggregation map
	// -------------------------------------------------------------------------
	cfg, err := gstream.Configure(
		gstream.WithName(appID),
		gstream.WithBrokers(brokers...),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	// aggregation: map[partition]map[newKey]count — per-partition to detect contamination
	type aggKey struct {
		partition int32
		key       string
	}
	var aggMu sync.Mutex
	agg := make(map[aggKey]int)
	var repartConsumed atomic.Int32 // total repartition records seen

	processFunc := func(ctx context.Context, in kafka.InRecord) ([]kafka.OutRecord, error) {
		switch in.Topic {
		case inputTopic:
			nk := newKeyFor(string(in.Key))
			t.Logf("inputTopic: key=%s partition=%d → newKey=%s → produce to %s",
				in.Key, in.Partition, nk, repartTopic)
			return []kafka.OutRecord{
				{
					Topic: repartTopic,
					Key:   []byte(nk),
					Value: in.Value,
					// Partition.IsValid=false → murmur2 routes by key
				},
			}, nil

		case repartTopic:
			repartConsumed.Add(1)
			aggMu.Lock()
			agg[aggKey{partition: in.Partition, key: string(in.Key)}]++
			aggMu.Unlock()
			t.Logf("repartTopic: key=%s partition=%d offset=%d (total so far=%d)",
				in.Key, in.Partition, in.Offset, repartConsumed.Load())
			return nil, nil

		default:
			return nil, fmt.Errorf("unexpected topic %q", in.Topic)
		}
	}

	client, err := kafka.New(cfg, []string{inputTopic, repartTopic}, slog.Default())
	if err != nil {
		t.Fatalf("kafka.New: %v", err)
	}
	defer client.Close()

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- client.Run(runCtx, processFunc)
	}()

	// -------------------------------------------------------------------------
	// 5. Deterministic wait: poll until repartConsumed == len(inputs).
	//    Hard timeout 60s.
	// -------------------------------------------------------------------------
	const wantTotal = 6
	waitCtx, waitCancel := context.WithTimeout(ctx, 60*time.Second)
	defer waitCancel()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
waitLoop:
	for {
		select {
		case <-ticker.C:
			got := repartConsumed.Load()
			if got >= wantTotal {
				break waitLoop
			}
		case <-waitCtx.Done():
			t.Fatalf("FINDING 1 — DEADLOCK or TIMEOUT: only %d/%d repartition records consumed after 60s; ProduceSync or PollFetches blocked — round-trip design is NOT sound",
				repartConsumed.Load(), wantTotal)
		case err := <-done:
			t.Fatalf("client.Run exited early with: %v", err)
		}
	}

	elapsed := time.Since(start)
	t.Logf("FINDING 1 — NO DEADLOCK: round-trip completed in %v (all %d records consumed)", elapsed, wantTotal)

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
	// 6. FINDING 2 — MURMUR2 SPREAD: query per-partition HWM of repartTopic.
	//    Both partitions must have HWM > 0.
	// -------------------------------------------------------------------------
	hwm, err := fetchPartitionHWMs(ctx, brokers, repartTopic, 2)
	if err != nil {
		t.Fatalf("fetchPartitionHWMs: %v", err)
	}
	t.Logf("FINDING 2 — repartTopic HWMs: p0=%d p1=%d", hwm[0], hwm[1])

	if hwm[0] == 0 || hwm[1] == 0 {
		t.Errorf("FINDING 2 — MURMUR2 NOT ACTIVE: one partition has HWM=0 (all records pinned to single partition); murmur2 path is NOT active for repartition — round-trip design is NOT sound. p0=%d p1=%d", hwm[0], hwm[1])
	} else {
		t.Logf("FINDING 2 — MURMUR2 SPREAD CONFIRMED: keys spread across both partitions (p0=%d, p1=%d)", hwm[0], hwm[1])
	}

	// -------------------------------------------------------------------------
	// 7. FINDING 3 — PER-KEY CORRECTNESS: total count per newKey must match
	//    expected, and each newKey must appear on exactly ONE partition.
	// -------------------------------------------------------------------------
	aggMu.Lock()
	defer aggMu.Unlock()

	// Collapse agg to per-key totals and per-key partition sets.
	totals := make(map[string]int)
	partitions := make(map[string]map[int32]bool)
	for k, cnt := range agg {
		totals[k.key] += cnt
		if partitions[k.key] == nil {
			partitions[k.key] = make(map[int32]bool)
		}
		partitions[k.key][k.partition] = true
	}

	t.Logf("FINDING 3 — aggregation results: %v", totals)
	t.Logf("FINDING 3 — per-key partitions: A=%v D=%v", partitions["A"], partitions["D"])

	contaminated := false
	for _, nk := range []string{"A", "D"} {
		want := wantCounts[nk]
		got := totals[nk]
		if got != want {
			t.Errorf("FINDING 3 — newKey %q: count mismatch: got %d, want %d", nk, got, want)
		}
		if len(partitions[nk]) > 1 {
			t.Errorf("FINDING 3 — CONTAMINATION: newKey %q landed on multiple partitions %v — co-location BROKEN", nk, partitions[nk])
			contaminated = true
		}
	}
	if !contaminated && totals["A"] == wantCounts["A"] && totals["D"] == wantCounts["D"] {
		t.Logf("FINDING 3 — PER-KEY CORRECTNESS CONFIRMED: A=%d/4 D=%d/2, each key on single partition", totals["A"], totals["D"])
	}

	// -------------------------------------------------------------------------
	// 8. FINDING 4 — ALO CRASH: deferred to F1 E2E.
	//    A mid-batch crash (cancel after produce, before commit) is too heavy
	//    to inject cleanly in a spike without instrumenting client internals.
	//    The existing TestE2E_StatelessFilterMap already covers ALO commit
	//    correctness for the single-topic case; the two-topic case will be
	//    covered in the F1 E2E test when the DSL is frozen.
	// -------------------------------------------------------------------------
	t.Log("FINDING 4 — ALO CRASH: deferred to P4b-F1 E2E (spike too shallow to inject mid-batch crash cleanly without client internals)")
}

// fetchPartitionHWMs returns the log-end-offset (high-water mark) for each
// partition [0..numPartitions) of the given topic using a ListOffsets request
// against the provided brokers.
func fetchPartitionHWMs(ctx context.Context, brokers []string, topic string, numPartitions int) (map[int32]int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("fetchPartitionHWMs: new client: %w", err)
	}
	defer cl.Close()

	req := kmsg.NewPtrListOffsetsRequest()
	req.IsolationLevel = 0 // READ_UNCOMMITTED
	t := kmsg.NewListOffsetsRequestTopic()
	t.Topic = topic
	for i := 0; i < numPartitions; i++ {
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition = int32(i)
		p.Timestamp = -1 // latest offset (log end offset)
		t.Partitions = append(t.Partitions, p)
	}
	req.Topics = append(req.Topics, t)

	resp, err := cl.Request(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("fetchPartitionHWMs: request: %w", err)
	}
	lor := resp.(*kmsg.ListOffsetsResponse)

	result := make(map[int32]int64, numPartitions)
	for _, rt := range lor.Topics {
		if rt.Topic != topic {
			continue
		}
		for _, rp := range rt.Partitions {
			result[rp.Partition] = rp.Offset
		}
	}
	return result, nil
}
