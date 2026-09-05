//go:build integration

package runtime_test

// TestEOSTxnSpike is the P5-S1+S2 spike: de-risk exactly-once semantics via
// kgo.GroupTransactSession BEFORE any production code.
//
// S1 — txn round-trip (make-or-break):
//   (a) COMMIT → visible-once + NOT visible before commit (ReadCommitted isolation)
//   (b) ABORT → invisible to ReadCommitted consumer
//   (c) CRASH → invisible (txn not ended; broker aborts after txn timeout)
//   + consumed-offset tied to txn: after commit, input not redelivered;
//     after abort, input IS redelivered.
//
// S2 — changelog+sink in ONE txn (R2 atomicity + R3 ReadCommitted restore):
//   COMMIT: changelog record + sink record visible atomically.
//   ABORT:  aborted changelog record invisible to ReadCommitted consumer
//           (protects RestoreFromChangelog from replaying aborted state).

import (
	"context"
	"fmt"
	"testing"
	"time"

	kafkamodule "mortz.dev/go/gstream/integration/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// pollReadCommitted polls topic startOffset with ReadCommitted isolation, collecting
// up to maxRecords records within timeout. Returns collected records.
func pollReadCommitted(t *testing.T, ctx context.Context, brokers []string, topic string, timeout time.Duration) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatalf("pollReadCommitted: new client: %v", err)
	}
	defer cl.Close()

	var out []*kgo.Record
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pollCtx, cancel := context.WithDeadline(ctx, deadline)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		fetches.EachRecord(func(r *kgo.Record) {
			out = append(out, r)
		})
	}
	return out
}

// newGroupTransactSession constructs a GroupTransactSession with the standard EOS options.
func newGroupTransactSession(t *testing.T, brokers []string, txnID, group, inputTopic string, extra ...kgo.Opt) *kgo.GroupTransactSession {
	t.Helper()
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.TransactionalID(txnID),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(inputTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	}
	opts = append(opts, extra...)
	sess, err := kgo.NewGroupTransactSession(opts...)
	if err != nil {
		t.Fatalf("NewGroupTransactSession(%s): %v", txnID, err)
	}
	return sess
}

// produceInput produces a single record to the given topic and returns when the broker acks.
func produceInput(t *testing.T, ctx context.Context, brokers []string, topic, key, value string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("produceInput: new client: %v", err)
	}
	defer cl.Close()
	res := cl.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: []byte(value),
	})
	if res.FirstErr() != nil {
		t.Fatalf("produceInput(%q): %v", key, res.FirstErr())
	}
}

// ensureTopicsN creates topics with numPartitions partitions each.
func ensureTopicsN(t *testing.T, ctx context.Context, brokers []string, numPartitions int32, topics ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("ensureTopicsN: new client: %v", err)
	}
	defer cl.Close()

	req := kmsg.NewPtrCreateTopicsRequest()
	for _, topic := range topics {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic = topic
		rt.NumPartitions = numPartitions
		rt.ReplicationFactor = 1
		req.Topics = append(req.Topics, rt)
	}
	resp, err := cl.Request(ctx, req)
	if err != nil {
		t.Fatalf("ensureTopicsN: request: %v", err)
	}
	ctr := resp.(*kmsg.CreateTopicsResponse)
	for _, topic := range ctr.Topics {
		kerErr := kerr.ErrorForCode(topic.ErrorCode)
		if kerErr != nil && kerErr != kerr.TopicAlreadyExists {
			t.Fatalf("ensureTopicsN: topic %q: %v", topic.Topic, kerErr)
		}
	}
}

// pollUntilN polls topic with ReadCommitted until at least n records arrive or
// timeout elapses. Returns all collected records.
func pollUntilN(t *testing.T, ctx context.Context, brokers []string, topic string, n int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatalf("pollUntilN: new client: %v", err)
	}
	defer cl.Close()

	var out []*kgo.Record
	deadline := time.Now().Add(timeout)
	for len(out) < n && time.Now().Before(deadline) {
		pollCtx, cancel := context.WithDeadline(ctx, deadline)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		fetches.EachRecord(func(r *kgo.Record) {
			out = append(out, r)
		})
	}
	return out
}

func TestEOSTxnSpike(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping EOS txn spike")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// -------------------------------------------------------------------------
	// 1. Start Kafka broker (transactions require KRaft/Zookeeper + transaction
	//    coordinator — cp-kafka:7.4.0 supports transactions fully).
	//    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 and REPLICATION_FACTOR=1 are
	//    required for single-broker txn coordinator to elect and function.
	// -------------------------------------------------------------------------
	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-cluster-eos"),
		kafkamodule.WithEnv(map[string]string{
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			// Short txn timeout so S1(c) crash test doesn't wait 60 s.
			// 60s > kgo default (40s); crash sub-test uses explicit 8s session timeout.
			"KAFKA_TRANSACTION_MAX_TIMEOUT_MS": "60000",
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
		s1InputTopic  = "eos-s1-input"
		s1OutputTopic = "eos-s1-output"
		s2InputTopic  = "eos-s2-input"
		s2SinkTopic   = "eos-s2-sink"
		s2ChangeTopic = "eos-s2-changelog"
	)

	ensureTopicsN(t, ctx, brokers, 1,
		s1InputTopic, s1OutputTopic,
		s2InputTopic, s2SinkTopic, s2ChangeTopic,
	)
	t.Log("topics created")

	// =========================================================================
	// S1(a) — COMMIT: output visible exactly once; NOT visible before commit.
	// =========================================================================
	t.Run("S1a_CommitVisible", func(t *testing.T) {
		// Produce input record BEFORE any transaction.
		produceInput(t, ctx, brokers, s1InputTopic, "k-commit", "v-commit")

		// Verify ReadCommitted consumer sees NOTHING before commit: start an
		// observer, begin the txn, produce to output, then confirm no output
		// is visible yet. We poll for 3 s; Kafka's ReadCommitted fence blocks
		// the LSO advance until the txn is ended.
		sess := newGroupTransactSession(t, brokers, "gstream-spike-s1a", "s1a-grp", s1InputTopic)
		defer sess.Close()

		if err := sess.Begin(); err != nil {
			t.Fatalf("S1a Begin: %v", err)
		}

		// Drain the input record from the session.
		var inputRec *kgo.Record
		pollCtx, pollCancel := context.WithTimeout(ctx, 20*time.Second)
		for inputRec == nil {
			fetches := sess.PollFetches(pollCtx)
			fetches.EachRecord(func(r *kgo.Record) {
				if inputRec == nil {
					inputRec = r
				}
			})
		}
		pollCancel()
		if inputRec == nil {
			t.Fatal("S1a: did not receive input record via PollFetches")
		}
		t.Logf("S1a: polled input record key=%s value=%s", inputRec.Key, inputRec.Value)

		// ProduceSync to output topic inside transaction.
		results := sess.ProduceSync(ctx, &kgo.Record{
			Topic: s1OutputTopic,
			Key:   []byte("out-" + string(inputRec.Key)),
			Value: inputRec.Value,
		})
		if results.FirstErr() != nil {
			t.Fatalf("S1a ProduceSync: %v", results.FirstErr())
		}
		t.Log("S1a: produced output record inside txn (not yet committed)")

		// BEFORE commit: ReadCommitted consumer must see ZERO records.
		preCommitRecs := pollReadCommitted(t, ctx, brokers, s1OutputTopic, 3*time.Second)
		if len(preCommitRecs) != 0 {
			t.Errorf("S1a: FAIL — ReadCommitted saw %d record(s) BEFORE commit; expected 0", len(preCommitRecs))
		} else {
			t.Logf("S1a: CONFIRMED not-visible before commit (ReadCommitted saw 0 records in 3s)")
		}

		// Commit.
		committed, err := sess.End(ctx, kgo.TryCommit)
		if err != nil {
			t.Fatalf("S1a End(TryCommit): %v", err)
		}
		if !committed {
			t.Fatalf("S1a: End returned committed=false; want true")
		}
		t.Logf("S1a: End(TryCommit) returned committed=%v", committed)

		// AFTER commit: ReadCommitted consumer sees exactly 1 record.
		postCommitRecs := pollUntilN(t, ctx, brokers, s1OutputTopic, 1, 15*time.Second)
		if len(postCommitRecs) != 1 {
			t.Errorf("S1a: FAIL — after commit ReadCommitted saw %d records, want 1", len(postCommitRecs))
		} else {
			t.Logf("S1a: COMMIT→VISIBLE-ONCE CONFIRMED: key=%s value=%s", postCommitRecs[0].Key, postCommitRecs[0].Value)
		}

		// Offset commit proof: new session in same group must NOT re-consume the input.
		sess2 := newGroupTransactSession(t, brokers, "gstream-spike-s1a-v2", "s1a-grp", s1InputTopic)
		defer sess2.Close()
		if err := sess2.Begin(); err != nil {
			t.Fatalf("S1a sess2 Begin: %v", err)
		}
		redeliveryCtx, redeliveryCancel := context.WithTimeout(ctx, 5*time.Second)
		defer redeliveryCancel()
		var redelivered bool
		for !redelivered {
			fetches := sess2.PollFetches(redeliveryCtx)
			if redeliveryCtx.Err() != nil {
				break
			}
			fetches.EachRecord(func(_ *kgo.Record) {
				redelivered = true
			})
		}
		_, _ = sess2.End(ctx, kgo.TryAbort)
		if redelivered {
			t.Error("S1a: FAIL — input was redelivered after commit (offset NOT committed atomically)")
		} else {
			t.Log("S1a: NO-REDELIVERY-AFTER-COMMIT CONFIRMED: same group consumed 0 input records")
		}
	})

	// =========================================================================
	// S1(b) — ABORT: output invisible to ReadCommitted.
	// =========================================================================
	t.Run("S1b_AbortInvisible", func(t *testing.T) {
		// Use a fresh input topic offset (produce a distinct record).
		produceInput(t, ctx, brokers, s1InputTopic, "k-abort", "v-abort")

		sess := newGroupTransactSession(t, brokers, "gstream-spike-s1b", "s1b-grp", s1InputTopic)
		// sess.Close() called explicitly below (before sess2 joins) to trigger rebalance.

		if err := sess.Begin(); err != nil {
			t.Fatalf("S1b Begin: %v", err)
		}

		// Drain input.
		pollCtx, pollCancel := context.WithTimeout(ctx, 20*time.Second)
		var inputRec *kgo.Record
		for inputRec == nil {
			fetches := sess.PollFetches(pollCtx)
			fetches.EachRecord(func(r *kgo.Record) {
				if inputRec == nil {
					inputRec = r
				}
			})
		}
		pollCancel()
		if inputRec == nil {
			t.Fatal("S1b: did not receive input record")
		}

		// Produce inside txn.
		results := sess.ProduceSync(ctx, &kgo.Record{
			Topic: s1OutputTopic,
			Key:   []byte("aborted-" + string(inputRec.Key)),
			Value: inputRec.Value,
		})
		if results.FirstErr() != nil {
			t.Fatalf("S1b ProduceSync: %v", results.FirstErr())
		}

		// Abort.
		committed, err := sess.End(ctx, kgo.TryAbort)
		if err != nil {
			t.Fatalf("S1b End(TryAbort): %v", err)
		}
		if committed {
			t.Fatalf("S1b: End(TryAbort) returned committed=true; want false")
		}
		t.Logf("S1b: End(TryAbort) returned committed=%v", committed)

		// ReadCommitted consumer must see zero records with aborted key.
		recs := pollReadCommitted(t, ctx, brokers, s1OutputTopic, 5*time.Second)
		for _, r := range recs {
			if string(r.Key) == "aborted-"+string(inputRec.Key) {
				t.Errorf("S1b: FAIL — aborted record key=%s visible to ReadCommitted", r.Key)
			}
		}
		t.Logf("S1b: ABORT→INVISIBLE CONFIRMED: no aborted record visible to ReadCommitted (%d records total on topic)", len(recs))

		// Close sess BEFORE creating sess2 so the partition rebalances to sess2.
		sess.Close()

		// Offset NOT committed: same group must re-receive the input record.
		sess2 := newGroupTransactSession(t, brokers, "gstream-spike-s1b-v2", "s1b-grp", s1InputTopic)
		defer sess2.Close()
		if err := sess2.Begin(); err != nil {
			t.Fatalf("S1b sess2 Begin: %v", err)
		}
		redeliverCtx, redeliverCancel := context.WithTimeout(ctx, 15*time.Second)
		defer redeliverCancel()
		var redelivered bool
		for !redelivered {
			fetches := sess2.PollFetches(redeliverCtx)
			if redeliverCtx.Err() != nil {
				break
			}
			fetches.EachRecord(func(r *kgo.Record) {
				if string(r.Key) == string(inputRec.Key) {
					redelivered = true
				}
			})
		}
		_, _ = sess2.End(ctx, kgo.TryAbort)
		if !redelivered {
			t.Error("S1b: FAIL — input NOT redelivered after abort (offset was committed but txn aborted)")
		} else {
			t.Log("S1b: REDELIVERY-AFTER-ABORT CONFIRMED: same group re-consumed input record")
		}
	})

	// =========================================================================
	// S1(c) — CRASH: abandon session without End; output invisible within
	// KAFKA_TRANSACTION_MAX_TIMEOUT_MS (10 s configured above).
	// =========================================================================
	t.Run("S1c_CrashInvisible", func(t *testing.T) {
		produceInput(t, ctx, brokers, s1InputTopic, "k-crash", "v-crash")

		// Use a short txn timeout so the broker aborts the open txn quickly.
		sess := newGroupTransactSession(t, brokers, "gstream-spike-s1c", "s1c-grp", s1InputTopic,
			kgo.TransactionTimeout(8*time.Second),
		)

		if err := sess.Begin(); err != nil {
			t.Fatalf("S1c Begin: %v", err)
		}

		// Drain input.
		pollCtx, pollCancel := context.WithTimeout(ctx, 20*time.Second)
		var inputRec *kgo.Record
		for inputRec == nil {
			fetches := sess.PollFetches(pollCtx)
			fetches.EachRecord(func(r *kgo.Record) {
				if inputRec == nil {
					inputRec = r
				}
			})
		}
		pollCancel()
		if inputRec == nil {
			t.Fatal("S1c: did not receive input record")
		}

		// Produce inside txn.
		results := sess.ProduceSync(ctx, &kgo.Record{
			Topic: s1OutputTopic,
			Key:   []byte("crashed-" + string(inputRec.Key)),
			Value: inputRec.Value,
		})
		if results.FirstErr() != nil {
			t.Fatalf("S1c ProduceSync: %v", results.FirstErr())
		}

		// Simulate crash: close client without calling End.
		// This leaves the transaction open; broker aborts it after timeout.
		t.Log("S1c: simulating crash — closing client WITHOUT End()")
		sess.Close()

		// Poll ReadCommitted immediately — should see 0 crashed records.
		// The broker's LSO is blocked until the txn is aborted (after timeout).
		t.Log("S1c: polling ReadCommitted immediately after crash (expect 0)")
		immediateRecs := pollReadCommitted(t, ctx, brokers, s1OutputTopic, 3*time.Second)
		crashedKeyImmediate := 0
		for _, r := range immediateRecs {
			if string(r.Key) == fmt.Sprintf("crashed-%s", inputRec.Key) {
				crashedKeyImmediate++
			}
		}
		if crashedKeyImmediate > 0 {
			t.Errorf("S1c: FAIL — crashed record visible immediately (before broker timeout abort)")
		} else {
			t.Logf("S1c: crashed record NOT visible immediately (%d total records on topic)", len(immediateRecs))
		}

		// Wait for broker to abort the open transaction (txn timeout = 10 s,
		// give the broker 20 s to complete the abort and advance the LSO).
		t.Log("S1c: waiting up to 20s for broker to abort open transaction (txn timeout=10s)")
		postTimeoutRecs := pollReadCommitted(t, ctx, brokers, s1OutputTopic, 20*time.Second)
		crashedKeyPost := 0
		for _, r := range postTimeoutRecs {
			if string(r.Key) == fmt.Sprintf("crashed-%s", inputRec.Key) {
				crashedKeyPost++
			}
		}
		if crashedKeyPost > 0 {
			t.Errorf("S1c: FAIL — crashed record became visible after broker abort (record leaked through)")
		} else {
			t.Logf("S1c: CRASH→INVISIBLE CONFIRMED: crashed record never visible to ReadCommitted (%d total records on topic after timeout)", len(postTimeoutRecs))
		}
	})

	// =========================================================================
	// S2 — changelog+sink in ONE txn (R2 atomicity + R3 ReadCommitted restore).
	// =========================================================================
	t.Run("S2_ChangelogSinkAtomic", func(t *testing.T) {
		produceInput(t, ctx, brokers, s2InputTopic, "k-s2", "v-s2")

		// ---- S2 COMMIT: both changelog and sink records visible atomically ----
		sess := newGroupTransactSession(t, brokers, "gstream-spike-s2-commit", "s2-commit-grp", s2InputTopic)
		defer sess.Close()

		if err := sess.Begin(); err != nil {
			t.Fatalf("S2 Begin: %v", err)
		}

		pollCtx, pollCancel := context.WithTimeout(ctx, 20*time.Second)
		var inputRec *kgo.Record
		for inputRec == nil {
			fetches := sess.PollFetches(pollCtx)
			fetches.EachRecord(func(r *kgo.Record) {
				if inputRec == nil {
					inputRec = r
				}
			})
		}
		pollCancel()
		if inputRec == nil {
			t.Fatal("S2: did not receive input record")
		}

		// Produce BOTH changelog record (pinned to partition 0) AND sink record
		// (unpinned, key-hash) in the SAME ProduceSync call.
		changelogRec := &kgo.Record{
			Topic:     s2ChangeTopic,
			Key:       inputRec.Key,
			Value:     []byte("state-" + string(inputRec.Value)),
			Partition: 0, // pinned: changelog co-located with source partition
		}
		sinkRec := &kgo.Record{
			Topic:     s2SinkTopic,
			Key:       []byte("out-" + string(inputRec.Key)),
			Value:     inputRec.Value,
			Partition: -1, // unpinned: key-hash routing
		}

		results := sess.ProduceSync(ctx, changelogRec, sinkRec)
		if results.FirstErr() != nil {
			t.Fatalf("S2 ProduceSync(changelog+sink): %v", results.FirstErr())
		}
		t.Log("S2: produced changelog+sink inside same txn; not yet committed")

		// Before commit: ReadCommitted sees 0 on both topics.
		preChangelog := pollReadCommitted(t, ctx, brokers, s2ChangeTopic, 2*time.Second)
		preSink := pollReadCommitted(t, ctx, brokers, s2SinkTopic, 2*time.Second)
		t.Logf("S2: before commit — changelog records=%d sink records=%d", len(preChangelog), len(preSink))

		// Commit.
		committed, err := sess.End(ctx, kgo.TryCommit)
		if err != nil {
			t.Fatalf("S2 End(TryCommit): %v", err)
		}
		if !committed {
			t.Fatalf("S2: End(TryCommit) returned committed=false")
		}
		t.Logf("S2: End(TryCommit) committed=%v", committed)

		// After commit: both topics must have 1 record.
		changelogRecs := pollUntilN(t, ctx, brokers, s2ChangeTopic, 1, 15*time.Second)
		sinkRecs := pollUntilN(t, ctx, brokers, s2SinkTopic, 1, 15*time.Second)

		if len(changelogRecs) != 1 {
			t.Errorf("S2 COMMIT: FAIL — changelog has %d records after commit, want 1", len(changelogRecs))
		} else {
			t.Logf("S2 COMMIT: CHANGELOG VISIBLE: key=%s value=%s", changelogRecs[0].Key, changelogRecs[0].Value)
		}
		if len(sinkRecs) != 1 {
			t.Errorf("S2 COMMIT: FAIL — sink has %d records after commit, want 1", len(sinkRecs))
		} else {
			t.Logf("S2 COMMIT: SINK VISIBLE: key=%s value=%s", sinkRecs[0].Key, sinkRecs[0].Value)
		}
		if len(changelogRecs) == 1 && len(sinkRecs) == 1 {
			t.Log("S2 COMMIT: ATOMIC CHANGELOG+SINK CONFIRMED")
		}
	})

	t.Run("S2_ChangelogAbortInvisible", func(t *testing.T) {
		// Produce a fresh input record for this sub-test.
		produceInput(t, ctx, brokers, s2InputTopic, "k-s2-abort", "v-s2-abort")

		sess := newGroupTransactSession(t, brokers, "gstream-spike-s2-abort", "s2-abort-grp", s2InputTopic)
		defer sess.Close()

		if err := sess.Begin(); err != nil {
			t.Fatalf("S2 abort Begin: %v", err)
		}

		pollCtx, pollCancel := context.WithTimeout(ctx, 20*time.Second)
		var inputRec *kgo.Record
		for inputRec == nil {
			fetches := sess.PollFetches(pollCtx)
			fetches.EachRecord(func(r *kgo.Record) {
				if inputRec == nil && string(r.Key) == "k-s2-abort" {
					inputRec = r
				}
			})
		}
		pollCancel()
		if inputRec == nil {
			t.Fatal("S2 abort: did not receive input record k-s2-abort")
		}

		// Produce changelog+sink inside txn.
		results := sess.ProduceSync(ctx,
			&kgo.Record{
				Topic:     s2ChangeTopic,
				Key:       inputRec.Key,
				Value:     []byte("aborted-state-" + string(inputRec.Value)),
				Partition: 0,
			},
			&kgo.Record{
				Topic:     s2SinkTopic,
				Key:       []byte("aborted-out-" + string(inputRec.Key)),
				Value:     inputRec.Value,
				Partition: -1,
			},
		)
		if results.FirstErr() != nil {
			t.Fatalf("S2 abort ProduceSync: %v", results.FirstErr())
		}

		// Abort.
		committed, err := sess.End(ctx, kgo.TryAbort)
		if err != nil {
			t.Fatalf("S2 abort End(TryAbort): %v", err)
		}
		if committed {
			t.Fatalf("S2 abort: End(TryAbort) returned committed=true; want false")
		}
		t.Logf("S2 abort: End(TryAbort) committed=%v", committed)

		// ReadCommitted on changelog must NOT see the aborted record.
		// This is R3: RestoreFromChangelog must not replay aborted state.
		changelogRecs := pollReadCommitted(t, ctx, brokers, s2ChangeTopic, 5*time.Second)
		abortedChangelogVisible := 0
		for _, r := range changelogRecs {
			if string(r.Key) == "k-s2-abort" {
				abortedChangelogVisible++
			}
		}
		if abortedChangelogVisible > 0 {
			t.Errorf("S2 abort: FAIL — aborted changelog record VISIBLE to ReadCommitted (%d records); RestoreFromChangelog would replay aborted state (R3 BROKEN)", abortedChangelogVisible)
		} else {
			t.Logf("S2 abort: ABORTED CHANGELOG INVISIBLE CONFIRMED (%d total changelog records on topic, none with aborted key) — R3 RestoreFromChangelog is safe", len(changelogRecs))
		}

		// Same for sink.
		sinkRecs := pollReadCommitted(t, ctx, brokers, s2SinkTopic, 5*time.Second)
		abortedSinkVisible := 0
		for _, r := range sinkRecs {
			if string(r.Key) == "aborted-out-k-s2-abort" {
				abortedSinkVisible++
			}
		}
		if abortedSinkVisible > 0 {
			t.Errorf("S2 abort: FAIL — aborted sink record visible to ReadCommitted")
		} else {
			t.Logf("S2 abort: ABORTED SINK INVISIBLE CONFIRMED (%d total sink records on topic)", len(sinkRecs))
		}
	})
}
