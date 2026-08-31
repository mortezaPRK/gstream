//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestEOSFence_SingleInstanceRestart proves the single-instance TransactionalID
// scheme ("gstream-<appID>") is crash-safe on restart.
//
// Scenario:
//  1. Instance A starts a transaction, produces an output record, then
//     simulates a crash by calling Close() WITHOUT EndTransaction.
//     This leaves A's transaction pending on the broker.
//  2. Instance B starts with the SAME TransactionalID "gstream-fence-s3" and
//     the SAME ConsumerGroup "fence-grp". B's Begin() triggers InitProducerID,
//     which causes the broker to bump the epoch and abort A's pending transaction.
//  3. B re-consumes (A never committed offsets, so the input record is
//     redelivered), produces, and commits cleanly.
//
// Claims verified:
//  1. B starts and commits cleanly despite A's abandoned in-flight transaction.
//  2. A's output is NOT visible to a ReadCommitted consumer (LSO blocked until
//     A's txn is resolved; after B's init the txn is aborted and skipped).
//  3. After B commits, exactly B's output (and not A's aborted output) is
//     visible to a ReadCommitted consumer.
//  4. A's fencing is proven implicitly: B's InitProducerID with the same txnID
//     bumped the epoch and the broker aborted A's pending txn (confirmed by 2+3).
//     Direct PRODUCER_FENCED on A is not tested — A is already closed.
func TestEOSFence_SingleInstanceRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Transaction-state topic requires min.isr=1 and rf=1 on a single-broker cluster.
	brokers := integrationBrokers(t)
	t.Logf("brokers: %v", brokers)

	const (
		inputTopic  = "fence-input"
		outputTopic = "fence-output"
		txnID       = "gstream-fence-s3"
		groupID     = "fence-grp"
	)

	createTopics(t, ctx, brokers, inputTopic, outputTopic)

	// Pre-produce 1 non-transactional seed record to the input topic.
	// It is already committed so ReadCommitted consumers can see it immediately.
	seeder, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("seeder: %v", err)
	}
	if res := seeder.ProduceSync(ctx, &kgo.Record{
		Topic: inputTopic, Key: []byte("k1"), Value: []byte("v1"),
	}); res.FirstErr() != nil {
		t.Fatalf("seed produce: %v", res.FirstErr())
	}
	seeder.Close()
	t.Log("seed record produced to input topic")

	// -------------------------------------------------------------------------
	// Phase 1: Instance A — begin transaction, produce output, simulate crash.
	// -------------------------------------------------------------------------
	sessA, err := kgo.NewGroupTransactSession(
		kgo.SeedBrokers(brokers...),
		kgo.TransactionalID(txnID),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(inputTopic),
		kgo.DisableAutoCommit(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("Instance A NewGroupTransactSession: %v", err)
	}

	if err := sessA.Begin(); err != nil {
		t.Fatalf("Instance A Begin: %v", err)
	}
	t.Log("Instance A: transaction begun")

	var aRec *kgo.Record
	{
		pCtx, pCancel := context.WithTimeout(ctx, 30*time.Second)
		for aRec == nil {
			fetches := sessA.PollFetches(pCtx)
			if pCtx.Err() != nil {
				pCancel()
				t.Fatalf("Instance A: timed out polling input: %v", pCtx.Err())
			}
			fetches.EachRecord(func(r *kgo.Record) {
				if aRec == nil {
					aRec = r
				}
			})
		}
		pCancel()
	}
	t.Logf("Instance A: consumed key=%s value=%s", aRec.Key, aRec.Value)

	// Produce to output topic inside the open (uncommitted) transaction.
	if res := sessA.ProduceSync(ctx, &kgo.Record{
		Topic: outputTopic, Key: []byte("out-A"), Value: []byte("val-A"),
	}); res.FirstErr() != nil {
		t.Fatalf("Instance A ProduceSync: %v", res.FirstErr())
	}
	t.Log("Instance A: produced output record (inside uncommitted transaction)")

	// CRASH SIMULATION: Close without EndTransaction.
	// A's transaction remains open on the broker; the output records are produced
	// but the transaction is neither committed nor aborted.
	sessA.Close()
	t.Log("Instance A: CRASH SIMULATED — closed without EndTransaction")

	// -------------------------------------------------------------------------
	// Phase 2: ReadCommitted check before B initializes.
	// The LSO of the output topic is blocked at A's uncommitted record, so
	// a ReadCommitted consumer should see 0 records.
	// -------------------------------------------------------------------------
	{
		rcA, err := kgo.NewClient(
			kgo.SeedBrokers(brokers...),
			kgo.ConsumeTopics(outputTopic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		)
		if err != nil {
			t.Fatalf("RC consumer (pre-B-init): %v", err)
		}

		chkCtx, chkCancel := context.WithTimeout(ctx, 5*time.Second)
		var seen []*kgo.Record
		for chkCtx.Err() == nil && len(seen) == 0 {
			fs := rcA.PollFetches(chkCtx)
			fs.EachRecord(func(r *kgo.Record) { seen = append(seen, r) })
		}
		chkCancel()
		rcA.Close()

		if len(seen) > 0 {
			t.Errorf("CLAIM-2 FAIL: ReadCommitted saw %d record(s) from A's uncommitted transaction; want 0",
				len(seen))
		} else {
			t.Log("CLAIM-2 OK: A's uncommitted output is NOT visible to ReadCommitted consumer")
		}
	}

	// -------------------------------------------------------------------------
	// Phase 3: Instance B — same txnID, same group.
	// Begin() triggers InitProducerID: broker bumps epoch, aborts A's pending txn.
	// -------------------------------------------------------------------------
	sessB, err := kgo.NewGroupTransactSession(
		kgo.SeedBrokers(brokers...),
		kgo.TransactionalID(txnID),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(inputTopic),
		kgo.DisableAutoCommit(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("Instance B NewGroupTransactSession: %v", err)
	}
	defer sessB.Close()

	if err := sessB.Begin(); err != nil {
		t.Fatalf("Instance B Begin: %v", err)
	}
	t.Log("Instance B: Begin() called — InitProducerID fences A (epoch bumped, broker aborts A's pending txn)")

	// Re-consume the input record (redelivered: A never committed offsets).
	var bRec *kgo.Record
	{
		pCtx, pCancel := context.WithTimeout(ctx, 60*time.Second)
		for bRec == nil {
			fetches := sessB.PollFetches(pCtx)
			if pCtx.Err() != nil {
				pCancel()
				t.Fatalf("Instance B: timed out polling input: %v", pCtx.Err())
			}
			fetches.EachRecord(func(r *kgo.Record) {
				if bRec == nil {
					bRec = r
				}
			})
		}
		pCancel()
	}
	t.Logf("Instance B: re-consumed key=%s value=%s (redelivered from A's aborted txn)", bRec.Key, bRec.Value)

	if string(bRec.Key) != "k1" || string(bRec.Value) != "v1" {
		t.Errorf("Instance B: unexpected redelivered record: key=%s value=%s; want key=k1 value=v1",
			bRec.Key, bRec.Value)
	}

	// B produces and commits.
	if res := sessB.ProduceSync(ctx, &kgo.Record{
		Topic: outputTopic, Key: []byte("out-B"), Value: []byte("val-B"),
	}); res.FirstErr() != nil {
		t.Fatalf("Instance B ProduceSync: %v", res.FirstErr())
	}

	committed, err := sessB.End(ctx, kgo.TryCommit)
	if err != nil {
		t.Fatalf("Instance B End: %v", err)
	}
	if !committed {
		t.Fatal("Instance B: End returned committed=false; want true")
	}
	t.Log("CLAIM-1 OK: Instance B committed cleanly despite A's abandoned in-flight transaction")

	// -------------------------------------------------------------------------
	// Phase 4: Verify B's output is visible and A's aborted output is NOT visible.
	// After B commits, ReadCommitted consumers see exactly 1 record (B's).
	// A's aborted records are skipped via the AbortedTransactions field in the
	// fetch response.
	// -------------------------------------------------------------------------
	{
		rcB, err := kgo.NewClient(
			kgo.SeedBrokers(brokers...),
			kgo.ConsumeTopics(outputTopic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		)
		if err != nil {
			t.Fatalf("RC consumer (post-B-commit): %v", err)
		}
		defer rcB.Close()

		colCtx, colCancel := context.WithTimeout(ctx, 30*time.Second)
		defer colCancel()

		// Collect all records until we have at least 1 or the context expires.
		var final []*kgo.Record
		for len(final) < 1 && colCtx.Err() == nil {
			fs := rcB.PollFetches(colCtx)
			fs.EachRecord(func(r *kgo.Record) {
				final = append(final, r)
				t.Logf("  ReadCommitted output: key=%s value=%s", r.Key, r.Value)
			})
		}

		// Give a brief extra window to catch any phantom second record.
		drainCtx, drainCancel := context.WithTimeout(ctx, 3*time.Second)
		for drainCtx.Err() == nil {
			fs := rcB.PollFetches(drainCtx)
			fs.EachRecord(func(r *kgo.Record) {
				final = append(final, r)
				t.Logf("  ReadCommitted output (drain): key=%s value=%s", r.Key, r.Value)
			})
		}
		drainCancel()

		if len(final) == 0 {
			t.Fatal("CLAIM-3 FAIL: ReadCommitted consumer saw 0 records after B committed; want exactly 1")
		}
		if len(final) > 1 {
			t.Errorf("CLAIM-3 PARTIAL: ReadCommitted consumer saw %d records; want exactly 1 "+
				"(A's aborted record should be invisible)", len(final))
		}

		if string(final[0].Key) != "out-B" {
			t.Errorf("CLAIM-3 FAIL: expected key=out-B (B's committed record), got %q "+
				"(A's aborted output may have leaked)", string(final[0].Key))
		} else {
			t.Log("CLAIM-3 OK: exactly B's committed output is visible; A's aborted output is invisible")
		}
	}

	// Fencing note: A was already closed before B's InitProducerID, so we cannot
	// directly observe PRODUCER_FENCED on A's transport. The implicit proof is:
	// - B's InitProducerID with the same txnID bumped the epoch (otherwise the
	//   broker would have rejected B's produce with INVALID_PRODUCER_EPOCH or
	//   similar, and End would not have returned committed=true).
	// - The broker aborted A's pending transaction as a consequence (confirmed by
	//   CLAIM-2: invisible before B init, and CLAIM-3: invisible after B commits).
	t.Log("CLAIM-4 NOTE: PRODUCER_FENCED on A proven implicitly — B's InitProducerID bumped the epoch " +
		"and the broker aborted A's pending transaction (CLAIM-2 + CLAIM-3 confirmed)")
	t.Log("VERDICT: single-instance restart with gstream-<appID> TransactionalID is CRASH-SAFE")
}
