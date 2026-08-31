//go:build integration

package runtime_test

// TestGlobalConsumeSpike is the P4c-S1 spike: de-risk GlobalKTable all-partition
// consumption BEFORE any production code. Two make-or-break questions:
//
//  1. Can a single kgo client with kgo.ConsumePartitions (no ConsumerGroup) consume
//     a multi-partition topic to its HWM and detect caught-up so bootstrap terminates?
//  2. Does that non-group direct-assign client interfere with a separate ConsumerGroup
//     client on the same topic (steal partitions / trigger rebalances / corrupt offsets)?
//     It must NOT.
//
// Extended from state.RestoreFromChangelog's single-partition ConsumePartitions pattern
// (stores/pebble/restore.go lines 69–76, 86–138).

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kafkamodule "github.com/mortezaPRK/gstream/integration/kafka"
	"github.com/mortezaPRK/gstream/internal/kafka"
	kgo "github.com/twmb/franz-go/pkg/kgo"
)

// partDone tracks per-partition bootstrap progress.
type partDone struct {
	hwm  int64
	done bool
}

func TestGlobalConsumeSpike(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping GlobalKTable spike")
	}

	overallStart := time.Now()
	t.Log("docker: reachable")

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// -------------------------------------------------------------------------
	// 1. Start Kafka broker.
	// -------------------------------------------------------------------------
	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-cluster-global"),
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
		globalTopic = "global-spike"
		nPartitions = 3
	)

	// -------------------------------------------------------------------------
	// 2. Create "global-spike" with N=3 partitions.
	// -------------------------------------------------------------------------
	if err := kafka.EnsureTopics(ctx, brokers, []kafka.TopicSpec{
		{Name: globalTopic, Partitions: nPartitions, ReplicationFactor: 1},
	}); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}
	t.Logf("created topic %s with %d partitions", globalTopic, nPartitions)

	// -------------------------------------------------------------------------
	// 3. Produce 10 records spread across all 3 partitions. Keys repeat only
	//    within the same partition so last-wins is deterministic: intra-partition
	//    ordering is guaranteed by Kafka; keys are disjoint across partitions so
	//    cross-partition ordering doesn't affect the expected final map.
	//
	//    p0: (a,a1) off=0  (b,b1) off=1  (a,a2) off=2   → a→a2, b→b1
	//    p1: (c,c1) off=0  (d,d1) off=1  (c,c2) off=2   → c→c2, d→d1
	//    p2: (e,e1) off=0  (f,f1) off=1  (e,e2) off=2   (f,f2) off=3
	//                                                    → e→e2, f→f2
	//
	//    HWMs: p0=3, p1=3, p2=4.  Stop condition per partition: offset >= hwm-1.
	// -------------------------------------------------------------------------
	type kv struct {
		key  string
		val  string
		part int32
	}
	records := []kv{
		{"a", "a1", 0},
		{"b", "b1", 0},
		{"a", "a2", 0},
		{"c", "c1", 1},
		{"d", "d1", 1},
		{"c", "c2", 1},
		{"e", "e1", 2},
		{"f", "f1", 2},
		{"e", "e2", 2},
		{"f", "f2", 2},
	}
	expectedMap := map[string]string{
		"a": "a2", "b": "b1",
		"c": "c2", "d": "d1",
		"e": "e2", "f": "f2",
	}

	// Manual partitioner: returns r.Partition if in range, else 0.
	// Mirrors mixedPartitionerFn's pinned-record path (client.go:54–56).
	manualPart := kgo.BasicConsistentPartitioner(func(_ string) func(*kgo.Record, int) int {
		return func(r *kgo.Record, n int) int {
			p := int(r.Partition)
			if p >= 0 && p < n {
				return p
			}
			return 0
		}
	})

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordPartitioner(manualPart),
	)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	for _, rec := range records {
		res := producer.ProduceSync(ctx, &kgo.Record{
			Topic:     globalTopic,
			Partition: rec.part,
			Key:       []byte(rec.key),
			Value:     []byte(rec.val),
		})
		if res.FirstErr() != nil {
			t.Fatalf("produce %q → p%d: %v", rec.key, rec.part, res.FirstErr())
		}
	}
	producer.Close()
	t.Logf("produced %d records to %s", len(records), globalTopic)

	// -------------------------------------------------------------------------
	// FINDING A + B: multi-partition bootstrap to HWM using a single
	// kgo.ConsumePartitions client with NO ConsumerGroup.
	//
	// This extends restore.go's single-partition pattern (lines 69–76) to N
	// partitions in one client. Stop condition extended: track per-partition
	// state; exit when ALL partitions have consumed offset >= hwm-1 (same logic
	// as restore.go line 128: `r.Offset >= targetLastOffset`).
	// -------------------------------------------------------------------------
	hwms, err := fetchPartitionHWMs(ctx, brokers, globalTopic, nPartitions)
	if err != nil {
		t.Fatalf("fetchPartitionHWMs: %v", err)
	}
	t.Logf("HWMs before bootstrap: p0=%d p1=%d p2=%d", hwms[0], hwms[1], hwms[2])

	// Build per-partition state and ConsumePartitions assignment map.
	pstates := make(map[int32]*partDone, nPartitions)
	partAssign := make(map[int32]kgo.Offset, nPartitions)
	for i := 0; i < nPartitions; i++ {
		hw := hwms[int32(i)]
		pstates[int32(i)] = &partDone{hwm: hw, done: hw == 0}
		if hw > 0 {
			partAssign[int32(i)] = kgo.NewOffset().At(0)
		}
	}

	// Direct-assign client: NO ConsumerGroup — does not participate in group
	// coordination, fetches partitions directly (same as restore.go).
	directClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			globalTopic: partAssign,
		}),
	)
	if err != nil {
		t.Fatalf("create direct consumer: %v", err)
	}
	defer directClient.Close()

	bootstrapStart := time.Now()
	kvMap := make(map[string]string, len(expectedMap))

	// Bootstrap loop. Check-before-poll: once all partitions reach HWM-1,
	// the loop exits without calling PollFetches again — no idle hang (FINDING B).
	for {
		allDone := true
		for _, s := range pstates {
			if !s.done {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}

		select {
		case <-ctx.Done():
			t.Fatalf("FINDING A — context expired during bootstrap: %v", ctx.Err())
		default:
		}

		fetches := directClient.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}
		if fetchErr := fetches.Err(); fetchErr != nil {
			t.Fatalf("FINDING A — poll error: %v", fetchErr)
		}

		fetches.EachRecord(func(r *kgo.Record) {
			// Last-wins: overwrite any previous value for this key.
			kvMap[string(r.Key)] = string(r.Value)
			// Mark partition done when we reach its last offset (hwm-1).
			s, ok := pstates[r.Partition]
			if ok && r.Offset >= s.hwm-1 {
				s.done = true
			}
		})
	}

	bootstrapElapsed := time.Since(bootstrapStart)
	t.Logf("FINDING A — bootstrap loop TERMINATED in %v (wall-clock)", bootstrapElapsed)
	t.Logf("FINDING A — consumed key→value map: %v", kvMap)

	// Assert final map equals expected.
	mapOK := true
	if len(kvMap) != len(expectedMap) {
		t.Errorf("FINDING A — map size: got %d, want %d", len(kvMap), len(expectedMap))
		mapOK = false
	}
	for k, want := range expectedMap {
		got, ok := kvMap[k]
		if !ok {
			t.Errorf("FINDING A — key %q missing from bootstrap map", k)
			mapOK = false
		} else if got != want {
			t.Errorf("FINDING A — key %q: got %q, want %q", k, got, want)
			mapOK = false
		}
	}
	if mapOK {
		t.Logf("FINDING A — PASS: final map correct, bootstrap terminated in %v", bootstrapElapsed)
	} else {
		t.Fatal("FINDING A — FAIL: bootstrap map mismatch")
	}

	// FINDING B: idle-exit confirmation.
	// The check-before-poll structure guarantees the loop exits the moment all
	// partitions are marked done, without issuing another PollFetches. With no
	// new records arriving, the loop cannot block — verified by the loop exiting
	// above with correct data.
	{
		allDone := true
		for p, s := range pstates {
			if !s.done {
				allDone = false
				t.Errorf("FINDING B — partition %d not marked done (hwm=%d)", p, s.hwm)
			}
		}
		if allDone {
			t.Logf("FINDING B — PASS: all %d partitions marked done; loop exited before next PollFetches — no idle hang possible", nPartitions)
		}
	}

	// -------------------------------------------------------------------------
	// FINDING C: interference test.
	//
	// Run a second direct-assign client (no group) AND a ConsumerGroup client
	// on the same topic concurrently. The group client must:
	//   (a) successfully consume all len(records) records, and
	//   (b) NOT lose partitions / rebalance-thrash due to the direct client.
	//
	// Expected: direct client uses direct Fetch protocol (no JoinGroup/SyncGroup),
	// so it is invisible to the group coordinator. Group client gets all 3
	// partitions in a single JoinGroup round (1 assign event, 0 revoke events).
	// -------------------------------------------------------------------------
	var (
		rebalanceAssigns atomic.Int32
		rebalanceRevokes atomic.Int32
		rebalanceMu      sync.Mutex
		rebalanceLog     []string
	)
	logRebalance := func(event string) {
		rebalanceMu.Lock()
		rebalanceLog = append(rebalanceLog, fmt.Sprintf("[+%v] %s", time.Since(overallStart).Truncate(time.Millisecond), event))
		rebalanceMu.Unlock()
		t.Log(event)
	}

	// Second direct-assign client: same pattern as the bootstrap above,
	// running concurrently with the group client.
	direct2Client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			globalTopic: func() map[int32]kgo.Offset {
				m := make(map[int32]kgo.Offset, nPartitions)
				for i := 0; i < nPartitions; i++ {
					m[int32(i)] = kgo.NewOffset().At(0)
				}
				return m
			}(),
		}),
	)
	if err != nil {
		t.Fatalf("create second direct consumer: %v", err)
	}

	// Group consumer. New group "task-grp" has no committed offsets; AtStart
	// resets to offset 0 so it reads all pre-produced records.
	var groupConsumed atomic.Int32
	groupClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("task-grp"),
		kgo.ConsumeTopics(globalTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			rebalanceAssigns.Add(1)
			logRebalance(fmt.Sprintf("FINDING C — group ASSIGNED: %v", assigned))
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			rebalanceRevokes.Add(1)
			logRebalance(fmt.Sprintf("FINDING C — group REVOKED: %v", revoked))
		}),
	)
	if err != nil {
		t.Fatalf("create group consumer: %v", err)
	}

	// Launch second direct client bootstrap goroutine.
	direct2Done := make(chan error, 1)
	go func() {
		defer direct2Client.Close()
		d2states := make(map[int32]*partDone, nPartitions)
		for i := 0; i < nPartitions; i++ {
			hw := hwms[int32(i)]
			d2states[int32(i)] = &partDone{hwm: hw, done: hw == 0}
		}
		for {
			allDone := true
			for _, s := range d2states {
				if !s.done {
					allDone = false
					break
				}
			}
			if allDone {
				direct2Done <- nil
				return
			}
			if ctx.Err() != nil {
				direct2Done <- fmt.Errorf("context: %w", ctx.Err())
				return
			}
			fetches := direct2Client.PollFetches(ctx)
			if fetches.IsClientClosed() {
				direct2Done <- fmt.Errorf("client closed unexpectedly")
				return
			}
			fetches.EachRecord(func(r *kgo.Record) {
				s, ok := d2states[r.Partition]
				if ok && r.Offset >= s.hwm-1 {
					s.done = true
				}
			})
		}
	}()

	// Launch group consumer goroutine; exits once all len(records) records consumed.
	groupDone := make(chan error, 1)
	go func() {
		defer groupClient.Close()
		for groupConsumed.Load() < int32(len(records)) {
			if ctx.Err() != nil {
				groupDone <- fmt.Errorf("context: %w", ctx.Err())
				return
			}
			fetches := groupClient.PollFetches(ctx)
			if fetches.IsClientClosed() {
				groupDone <- fmt.Errorf("group client closed unexpectedly")
				return
			}
			fetches.EachRecord(func(r *kgo.Record) {
				n := groupConsumed.Add(1)
				t.Logf("FINDING C — group: key=%s partition=%d offset=%d total=%d",
					r.Key, r.Partition, r.Offset, n)
			})
		}
		groupDone <- nil
	}()

	// Wait for both goroutines with a per-finding timeout.
	waitCtx, waitCancel := context.WithTimeout(ctx, 60*time.Second)
	defer waitCancel()

	select {
	case err := <-direct2Done:
		if err != nil {
			t.Errorf("FINDING C — second direct client error: %v", err)
		} else {
			t.Log("FINDING C — second direct client bootstrap finished")
		}
	case <-waitCtx.Done():
		t.Errorf("FINDING C — TIMEOUT waiting for second direct client (60s)")
	}

	select {
	case err := <-groupDone:
		if err != nil {
			t.Errorf("FINDING C — group client error: %v", err)
		} else {
			t.Log("FINDING C — group client consumed all records")
		}
	case <-waitCtx.Done():
		t.Errorf("FINDING C — TIMEOUT waiting for group client: consumed %d/%d",
			groupConsumed.Load(), len(records))
	}

	// Assert group got all records.
	got := groupConsumed.Load()
	if got != int32(len(records)) {
		t.Errorf("FINDING C — FAIL: group consumed %d records, want %d (direct client may have interfered)",
			got, len(records))
	} else {
		t.Logf("FINDING C — PASS: group client got all %d records with direct client also active", got)
	}

	// Report rebalance events.
	assigns := rebalanceAssigns.Load()
	revokes := rebalanceRevokes.Load()
	t.Logf("FINDING C — rebalance events: %d assigns, %d revokes", assigns, revokes)

	rebalanceMu.Lock()
	if len(rebalanceLog) > 0 {
		for _, entry := range rebalanceLog {
			t.Logf("FINDING C — rebalance: %s", entry)
		}
	}
	rebalanceMu.Unlock()

	if revokes > 0 {
		t.Logf("FINDING C — WARNING: %d revoke events (direct client triggered rebalance — unexpected)", revokes)
	}
	if assigns > 1 {
		t.Logf("FINDING C — WARNING: %d assign events (expected 1 for single-member group)", assigns)
	}
	if revokes == 0 {
		t.Logf("FINDING C — PASS: 0 revoke events — direct-assign client did NOT interfere with group coordinator")
	}

	t.Logf("VERDICT — spike wall-clock: %v", time.Since(overallStart))
}
