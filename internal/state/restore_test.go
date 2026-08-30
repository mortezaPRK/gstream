//go:build integration

package state

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	testcontainers "github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// txnEnv holds the broker addresses and topic for transactional produce helpers.
type txnEnv struct {
	brokers   []string
	topic     string
	partition int32
}

// produceCommittedTxn produces records using a transactional producer and commits.
// Returns the offsets assigned by the broker (for verification).
func produceCommittedTxn(t *testing.T, ctx context.Context, e txnEnv, txnID string, recs []*kgo.Record) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(e.brokers...),
		kgo.TransactionalID(txnID),
	)
	if err != nil {
		t.Fatalf("produceCommittedTxn(%s): new client: %v", txnID, err)
	}
	defer cl.Close()

	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("produceCommittedTxn(%s): BeginTransaction: %v", txnID, err)
	}
	results := cl.ProduceSync(ctx, recs...)
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("produceCommittedTxn(%s): produce record %d: %v", txnID, i, res.Err)
		}
	}
	if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("produceCommittedTxn(%s): EndTransaction(commit): %v", txnID, err)
	}
}

// produceAbortedTxn produces records using a transactional producer and ABORTS.
// The records occupy offsets in the partition but are invisible to ReadCommitted.
func produceAbortedTxn(t *testing.T, ctx context.Context, e txnEnv, txnID string, recs []*kgo.Record) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(e.brokers...),
		kgo.TransactionalID(txnID),
	)
	if err != nil {
		t.Fatalf("produceAbortedTxn(%s): new client: %v", txnID, err)
	}
	defer cl.Close()

	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("produceAbortedTxn(%s): BeginTransaction: %v", txnID, err)
	}
	results := cl.ProduceSync(ctx, recs...)
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("produceAbortedTxn(%s): produce record %d: %v", txnID, i, res.Err)
		}
	}
	if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("produceAbortedTxn(%s): EndTransaction(abort): %v", txnID, err)
	}
}

// TestMain is defined in changelog_test.go and applies to the whole test binary.
// We share it here; this file must not redefine TestMain.

// TestRestoreFromChangelog_Basic is a full integration test that:
//  1. Spins up a single-broker Kafka cluster via testcontainers.
//  2. Produces a known changelog to (topic, partition 0):
//     offset 0: Put  key=a value=v1
//     offset 1: Put  key=b value=v2
//     offset 2: Put  key=a value=v3  (overwrites a)
//     offset 3: Del  key=b value=nil  (tombstone for b)
//  3. RestoreFromChangelog into a fresh OpenDB. Asserts:
//     - raw Pebble key "a" == "v3" (latest value wins on replay)
//     - raw Pebble key "b" absent (tombstoned)
//     - ReadCheckpoint == 3 (last applied offset)
//     - returned HW == 4
func TestRestoreFromChangelog_Basic(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-restore-cluster"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
		}),
	)
	if err != nil {
		t.Skipf("failed to start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = kc.Terminate(ctx) })

	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get broker addresses: %v", err)
	}

	const (
		topic     = "restore-changelog-test"
		storeName = "teststore"
		partition = int32(0)
	)
	createChangelogTopic(t, ctx, brokers, topic)

	// Produce 4 records to the changelog.
	// Records are the raw Pebble key/value bytes as ChangelogProducer.Flush would produce.
	// key=[]byte("a"), value=[]byte("v1")   -> Put
	// key=[]byte("b"), value=[]byte("v2")   -> Put
	// key=[]byte("a"), value=[]byte("v3")   -> Put (update a)
	// key=[]byte("b"), value=nil            -> Delete (tombstone)
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	records := []*kgo.Record{
		{Topic: topic, Partition: partition, Key: []byte("a"), Value: []byte("v1")},
		{Topic: topic, Partition: partition, Key: []byte("b"), Value: []byte("v2")},
		{Topic: topic, Partition: partition, Key: []byte("a"), Value: []byte("v3")},
		{Topic: topic, Partition: partition, Key: []byte("b"), Value: nil}, // tombstone
	}
	results := producer.ProduceSync(ctx, records...)
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("produce record %d: %v", i, res.Err)
		}
	}

	// Open a fresh Pebble DB in a temp directory.
	tmpDir := t.TempDir()
	db, err := OpenDB(tmpDir)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	// RestoreFromChangelog from scratch (checkpointOffset=-1).
	hw, err := RestoreFromChangelog(ctx, brokers, topic, partition, -1, db, storeName, 2*time.Second)
	if err != nil {
		t.Fatalf("RestoreFromChangelog: %v", err)
	}

	// HW should be 4 (4 records produced, next offset = 4).
	if hw != 4 {
		t.Errorf("returned HW: got %d, want 4", hw)
	}

	// Key "a" should have value "v3" (latest put wins).
	valA, closerA, err := db.Get([]byte("a"))
	if err != nil {
		t.Fatalf("db.Get(a): %v", err)
	}
	gotA := make([]byte, len(valA))
	copy(gotA, valA)
	closerA.Close()
	if string(gotA) != "v3" {
		t.Errorf("key a: got %q, want %q", string(gotA), "v3")
	}

	// Key "b" should be absent (tombstoned).
	_, _, err = db.Get([]byte("b"))
	if err == nil {
		t.Error("key b should be absent after tombstone, but found in pebble")
	}

	// Checkpoint should be 3 (last applied offset == HW-1 == 3).
	ckpt, found, err := ReadCheckpoint(db, storeName)
	if err != nil {
		t.Fatalf("ReadCheckpoint: %v", err)
	}
	if !found {
		t.Fatal("checkpoint not found after restore")
	}
	if ckpt != 3 {
		t.Errorf("checkpoint: got %d, want 3", ckpt)
	}
}

// TestRestoreFromChangelog_FromNonZeroCheckpoint verifies that restore skips
// records at or before the provided checkpointOffset and only applies the tail.
//
// Changelog (same 4 records as Basic):
//
//	offset 0: Put a=v1
//	offset 1: Put b=v2
//	offset 2: Put a=v3
//	offset 3: Del b (tombstone)
//
// Starting with checkpointOffset=1 (we've already applied offsets 0 and 1),
// only offsets 2 and 3 should be applied: a=v3, b deleted.
func TestRestoreFromChangelog_FromNonZeroCheckpoint(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-restore-ckpt-cluster"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
		}),
	)
	if err != nil {
		t.Skipf("failed to start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = kc.Terminate(ctx) })

	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get broker addresses: %v", err)
	}

	const (
		topic     = "restore-ckpt-test"
		storeName = "ckptstore"
		partition = int32(0)
	)
	createChangelogTopic(t, ctx, brokers, topic)

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	records := []*kgo.Record{
		{Topic: topic, Partition: partition, Key: []byte("a"), Value: []byte("v1")},
		{Topic: topic, Partition: partition, Key: []byte("b"), Value: []byte("v2")},
		{Topic: topic, Partition: partition, Key: []byte("a"), Value: []byte("v3")},
		{Topic: topic, Partition: partition, Key: []byte("b"), Value: nil},
	}
	results := producer.ProduceSync(ctx, records...)
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("produce record %d: %v", i, res.Err)
		}
	}

	tmpDir := t.TempDir()
	db, err := OpenDB(tmpDir)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	// Seed the DB with the "already applied" state (offsets 0 and 1):
	// a=v1, b=v2 — as if a previous restore ran through offset 1.
	seedBatch := db.NewBatch()
	if err := seedBatch.Set([]byte("a"), []byte("v1"), nil); err != nil {
		t.Fatalf("seed batch.Set(a): %v", err)
	}
	if err := seedBatch.Set([]byte("b"), []byte("v2"), nil); err != nil {
		t.Fatalf("seed batch.Set(b): %v", err)
	}
	if err := WriteCheckpoint(seedBatch, storeName, 1); err != nil {
		t.Fatalf("seed WriteCheckpoint: %v", err)
	}
	if err := seedBatch.Commit(pebble.Sync); err != nil {
		t.Fatalf("seed batch commit: %v", err)
	}

	// RestoreFromChangelog starting from checkpointOffset=1
	// (will start consuming at offset 2).
	hw, err := RestoreFromChangelog(ctx, brokers, topic, partition, 1, db, storeName, 2*time.Second)
	if err != nil {
		t.Fatalf("RestoreFromChangelog: %v", err)
	}

	if hw != 4 {
		t.Errorf("returned HW: got %d, want 4", hw)
	}

	// Key "a" should have been updated to "v3".
	valA, closerA, dbErr := db.Get([]byte("a"))
	if dbErr != nil {
		t.Fatalf("db.Get(a): %v", dbErr)
	}
	gotA := make([]byte, len(valA))
	copy(gotA, valA)
	closerA.Close()
	if string(gotA) != "v3" {
		t.Errorf("key a: got %q, want %q", string(gotA), "v3")
	}

	// Key "b" should be absent (tombstoned by offset 3).
	_, _, dbErr = db.Get([]byte("b"))
	if dbErr == nil {
		t.Error("key b should be absent after tombstone, but found in pebble")
	}

	// Checkpoint should be 3.
	ckpt, found, err := ReadCheckpoint(db, storeName)
	if err != nil {
		t.Fatalf("ReadCheckpoint: %v", err)
	}
	if !found {
		t.Fatal("checkpoint not found after restore")
	}
	if ckpt != 3 {
		t.Errorf("checkpoint: got %d, want 3", ckpt)
	}
}

// TestRestoreFromChangelog_AlreadyAtHW verifies that when checkpointOffset is
// already at HW-1 (fully caught up), RestoreFromChangelog is a no-op:
// state is unchanged and no new checkpoint is written.
func TestRestoreFromChangelog_AlreadyAtHW(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-restore-athw-cluster"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
		}),
	)
	if err != nil {
		t.Skipf("failed to start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = kc.Terminate(ctx) })

	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get broker addresses: %v", err)
	}

	const (
		topic     = "restore-athw-test"
		storeName = "athwstore"
		partition = int32(0)
	)
	createChangelogTopic(t, ctx, brokers, topic)

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	// Produce 2 records (offsets 0,1). HW = 2.
	records := []*kgo.Record{
		{Topic: topic, Partition: partition, Key: []byte("x"), Value: []byte("xval")},
		{Topic: topic, Partition: partition, Key: []byte("y"), Value: []byte("yval")},
	}
	results := producer.ProduceSync(ctx, records...)
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("produce record %d: %v", i, res.Err)
		}
	}

	tmpDir := t.TempDir()
	db, err := OpenDB(tmpDir)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	// Seed state as if we already applied everything through offset 1 (== HW-1).
	seedBatch := db.NewBatch()
	if err := seedBatch.Set([]byte("x"), []byte("xval"), nil); err != nil {
		t.Fatalf("seed Set x: %v", err)
	}
	if err := seedBatch.Set([]byte("y"), []byte("yval"), nil); err != nil {
		t.Fatalf("seed Set y: %v", err)
	}
	if err := WriteCheckpoint(seedBatch, storeName, 1); err != nil {
		t.Fatalf("seed WriteCheckpoint: %v", err)
	}
	if err := seedBatch.Commit(pebble.Sync); err != nil {
		t.Fatalf("seed commit: %v", err)
	}

	// RestoreFromChangelog with checkpointOffset=1 (== HW-1 = 2-1 = 1).
	// startOffset = 2 >= HW = 2: nothing to restore.
	hw, err := RestoreFromChangelog(ctx, brokers, topic, partition, 1, db, storeName, 2*time.Second)
	if err != nil {
		t.Fatalf("RestoreFromChangelog: %v", err)
	}

	if hw != 2 {
		t.Errorf("returned HW: got %d, want 2", hw)
	}

	// State should be unchanged — x=xval, y=yval still present.
	valX, closerX, dbErr := db.Get([]byte("x"))
	if dbErr != nil {
		t.Fatalf("db.Get(x): %v", dbErr)
	}
	gotX := make([]byte, len(valX))
	copy(gotX, valX)
	closerX.Close()
	if string(gotX) != "xval" {
		t.Errorf("key x: got %q, want %q", string(gotX), "xval")
	}

	// Checkpoint should still be 1 (unchanged).
	ckpt, found, err := ReadCheckpoint(db, storeName)
	if err != nil {
		t.Fatalf("ReadCheckpoint: %v", err)
	}
	if !found {
		t.Fatal("checkpoint should still be present")
	}
	if ckpt != 1 {
		t.Errorf("checkpoint: got %d, want 1 (should be unchanged)", ckpt)
	}
}

// TestRestoreFromChangelog_EmptyChangelog verifies that an empty changelog
// (HW==0) returns 0,nil without touching the database.
func TestRestoreFromChangelog_EmptyChangelog(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-restore-empty-cluster"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
		}),
	)
	if err != nil {
		t.Skipf("failed to start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = kc.Terminate(ctx) })

	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get broker addresses: %v", err)
	}

	const (
		topic     = "restore-empty-test"
		storeName = "emptystore"
		partition = int32(0)
	)
	createChangelogTopic(t, ctx, brokers, topic)
	// Intentionally produce nothing — HW == 0.

	tmpDir := t.TempDir()
	db, err := OpenDB(tmpDir)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	hw, err := RestoreFromChangelog(ctx, brokers, topic, partition, -1, db, storeName, 2*time.Second)
	if err != nil {
		t.Fatalf("RestoreFromChangelog on empty changelog: %v", err)
	}
	if hw != 0 {
		t.Errorf("returned HW: got %d, want 0", hw)
	}

	// No checkpoint should have been written.
	_, found, err := ReadCheckpoint(db, storeName)
	if err != nil {
		t.Fatalf("ReadCheckpoint: %v", err)
	}
	if found {
		t.Error("checkpoint should not be present for empty changelog")
	}
}

// TestRestoreFromChangelog_AbortedTail verifies the EOS crash-recovery scenario:
// N committed changelog records followed by ONE aborted record (the aborted
// record is at offset hw-1, i.e. the changelog tail).
//
// Before the fix, RestoreFromChangelog hung forever because the aborted tail
// record is never delivered by ReadCommitted, so `done` was never set. This
// test asserts:
//   - RestoreFromChangelog terminates (the test timeout guards against a hang).
//   - The restored Pebble state contains exactly the N committed records.
//   - The aborted record's key/value are NOT present.
//   - The checkpoint == last committed offset (not -1, not the aborted offset).
//
// Setup matches the P5-S1b/S2 spike (eos_txn_spike_test.go):
//   - Single-broker with KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 and
//     KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 required for txn coordinator.
//   - Committed records produced via one transactional producer (commit).
//   - Aborted record produced via a second transactional producer (abort).
//     The abort marker advances the HWM past the aborted batch; LSO == HWM
//     once the marker is written.
func TestRestoreFromChangelog_AbortedTail(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping integration test")
	}

	// Generous timeout: container start + produce + restore. The test-timeout
	// is the guard against a hang (the bug under test).
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-restore-aborted-tail"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
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
		topic     = "restore-aborted-tail-test"
		storeName = "aborted-tail-store"
		partition = int32(0)
	)
	createChangelogTopic(t, ctx, brokers, topic)

	env := txnEnv{brokers: brokers, topic: topic, partition: partition}

	// Step 1: produce 3 COMMITTED records via a transactional producer.
	// These occupy offsets 0, 1, 2 (plus a commit-marker at offset 3, which is
	// a control batch invisible to ReadCommitted consumers).
	//
	// After commit:
	//   offset 0: Put  key=a value=v1
	//   offset 1: Put  key=b value=v2
	//   offset 2: Put  key=c value=v3
	//   offset 3: control batch (commit marker, invisible)
	produceCommittedTxn(t, ctx, env, "restore-aborted-tail-committed", []*kgo.Record{
		{Topic: topic, Partition: partition, Key: []byte("a"), Value: []byte("v1")},
		{Topic: topic, Partition: partition, Key: []byte("b"), Value: []byte("v2")},
		{Topic: topic, Partition: partition, Key: []byte("c"), Value: []byte("v3")},
	})
	t.Log("committed txn produced (keys a,b,c)")

	// Step 2: produce 1 ABORTED record via a second transactional producer.
	// This is the changelog tail — the EOS crash-mid-transaction scenario.
	// The aborted record occupies an offset but is invisible to ReadCommitted.
	// The abort marker also occupies an offset and advances the HWM. After
	// the abort marker, HWM > last committed offset and LSO == HWM.
	//
	// After abort:
	//   offset 4: data batch (aborted, key=should-not-appear)
	//   offset 5: control batch (abort marker, invisible)
	//   HWM = 6, LSO = 6
	produceAbortedTxn(t, ctx, env, "restore-aborted-tail-aborted", []*kgo.Record{
		{Topic: topic, Partition: partition, Key: []byte("should-not-appear"), Value: []byte("ABORTED")},
	})
	t.Log("aborted txn produced (key=should-not-appear)")

	// Verify the HWM advanced past the committed records before RestoreFromChangelog.
	hw, err := fetchHighWatermark(ctx, brokers, topic, partition)
	if err != nil {
		t.Fatalf("fetchHighWatermark: %v", err)
	}
	t.Logf("HWM after commit+abort: %d", hw)
	if hw < 5 {
		// Minimum: 3 data + 1 commit-marker + 1 abort-data + 1 abort-marker = 6
		// (exact count depends on broker batch packing). We just need hw > 3.
		t.Fatalf("expected HWM > 3 after committed+aborted txns, got %d", hw)
	}

	// Step 3: RestoreFromChangelog — this is the bug target.
	// With the old code this hangs because hw-1 is an aborted (invisible) record.
	// With the fix it must terminate.
	tmpDir := t.TempDir()
	db, err := OpenDB(tmpDir)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	t.Log("calling RestoreFromChangelog (must not hang)")
	gotHW, err := RestoreFromChangelog(ctx, brokers, topic, partition, -1, db, storeName, 2*time.Second)
	if err != nil {
		t.Fatalf("RestoreFromChangelog: %v", err)
	}
	t.Logf("RestoreFromChangelog returned hw=%d", gotHW)

	if gotHW < hw {
		t.Errorf("returned HW regressed: got %d, observed before restore %d", gotHW, hw)
	}

	// Key "a" must have value "v1" (committed).
	valA, closerA, dbErr := db.Get([]byte("a"))
	if dbErr != nil {
		t.Fatalf("db.Get(a): %v", dbErr)
	}
	gotA := string(valA)
	closerA.Close()
	if gotA != "v1" {
		t.Errorf("key a: got %q, want %q", gotA, "v1")
	}

	// Key "b" must have value "v2" (committed).
	valB, closerB, dbErr := db.Get([]byte("b"))
	if dbErr != nil {
		t.Fatalf("db.Get(b): %v", dbErr)
	}
	gotB := string(valB)
	closerB.Close()
	if gotB != "v2" {
		t.Errorf("key b: got %q, want %q", gotB, "v2")
	}

	// Key "c" must have value "v3" (committed).
	valC, closerC, dbErr := db.Get([]byte("c"))
	if dbErr != nil {
		t.Fatalf("db.Get(c): %v", dbErr)
	}
	gotC := string(valC)
	closerC.Close()
	if gotC != "v3" {
		t.Errorf("key c: got %q, want %q", gotC, "v3")
	}

	// Aborted key must NOT be present.
	_, _, dbErr = db.Get([]byte("should-not-appear"))
	if dbErr == nil {
		t.Error("aborted key 'should-not-appear' should be absent from Pebble but was found")
	}

	// Checkpoint must be the last committed offset (offset 2, the last data record
	// in the committed batch). Control batches (commit/abort markers) do not
	// produce delivered records, so lastAppliedOffset == 2.
	ckpt, found, err := ReadCheckpoint(db, storeName)
	if err != nil {
		t.Fatalf("ReadCheckpoint: %v", err)
	}
	if !found {
		t.Fatal("checkpoint not found after restore")
	}
	// The committed batch has 3 records at offsets 0,1,2; lastAppliedOffset = 2.
	if ckpt != 2 {
		t.Errorf("checkpoint: got %d, want 2 (last committed record offset)", ckpt)
	}
	t.Logf("PASS: RestoreFromChangelog terminated correctly; checkpoint=%d, state has committed keys only", ckpt)
}

// TestRestoreFromChangelog_LargeMultiResponse verifies that RestoreFromChangelog
// consumes ALL records when the changelog spans MULTIPLE fetch responses.
//
// The bug (C4b): hwmReached fires on the FIRST broker response. The old code
// broke out of the loop immediately, silently dropping records in responses 2..N.
// The fix requires blocking-polling until an empty response signals cursor-at-HW.
//
// Multi-response is forced by setting kgo.FetchMaxPartitionBytes to a small value
// (1024 bytes) via the extraOpts parameter added to RestoreFromChangelog. With 200
// records of ~100 bytes each (~20KB total) and a 1KB fetch limit, the broker splits
// the changelog into at least 20 separate responses. The assertion that every key
// is present in Pebble detects partial restores.
func TestRestoreFromChangelog_LargeMultiResponse(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("Docker not available; skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	kc, err := kafkamodule.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkamodule.WithClusterID("test-restore-multi-resp"),
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
		topic     = "restore-multi-resp-test"
		storeName = "multi-resp-store"
		partition = int32(0)
		numRecs   = 200 // ~20KB at ~100B per record; far exceeds 1KB FetchMaxPartitionBytes
	)
	createChangelogTopic(t, ctx, brokers, topic)

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	// Build numRecs records with distinct keys and ~80-byte values.
	recs := make([]*kgo.Record, numRecs)
	for i := 0; i < numRecs; i++ {
		key := fmt.Sprintf("key-%04d", i)
		// 80-byte value so each record is ~100B; 200 records ≈ 20KB >> 1KB FetchMaxPartitionBytes.
		value := fmt.Sprintf("value-%04d-%064d", i, i)
		recs[i] = &kgo.Record{
			Topic:     topic,
			Partition: partition,
			Key:       []byte(key),
			Value:     []byte(value),
		}
	}
	results := producer.ProduceSync(ctx, recs...)
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("produce record %d: %v", i, res.Err)
		}
	}
	t.Logf("produced %d records", numRecs)

	hw, err := fetchHighWatermark(ctx, brokers, topic, partition)
	if err != nil {
		t.Fatalf("fetchHighWatermark: %v", err)
	}
	t.Logf("HWM after produce: %d (expected %d)", hw, numRecs)
	if hw != numRecs {
		t.Fatalf("expected HWM %d, got %d", numRecs, hw)
	}

	tmpDir := t.TempDir()
	db, err := OpenDB(tmpDir)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	// Inject kgo.FetchMaxPartitionBytes(1024) to force the broker to split the
	// ~20KB changelog across many small fetch responses. This replicates the
	// multi-response scenario that exposed the C4b silent-partial-restore bug.
	t.Log("calling RestoreFromChangelog with FetchMaxPartitionBytes=1024 to force multi-response")
	gotHW, err := RestoreFromChangelog(ctx, brokers, topic, partition, -1, db, storeName, 2*time.Second,
		kgo.FetchMaxPartitionBytes(1024),
	)
	if err != nil {
		t.Fatalf("RestoreFromChangelog: %v", err)
	}
	t.Logf("RestoreFromChangelog returned hw=%d", gotHW)

	if gotHW != hw {
		t.Errorf("returned HW: got %d, want %d", gotHW, hw)
	}

	// Assert ALL numRecs keys are present in Pebble. A partial restore (the C4b bug)
	// would leave some suffix of keys missing; this detects any silent data loss.
	missing := 0
	for i := 0; i < numRecs; i++ {
		key := fmt.Sprintf("key-%04d", i)
		wantValue := fmt.Sprintf("value-%04d-%064d", i, i)
		val, closer, dbErr := db.Get([]byte(key))
		if dbErr != nil {
			t.Errorf("db.Get(%q): %v (key missing — partial restore?)", key, dbErr)
			missing++
			continue
		}
		got := string(val)
		closer.Close()
		if got != wantValue {
			t.Errorf("key %q: got %q, want %q", key, got, wantValue)
		}
	}
	if missing > 0 {
		t.Errorf("FAIL: %d/%d keys missing — partial restore detected (C4b regression)", missing, numRecs)
	} else {
		t.Logf("PASS: all %d keys restored correctly", numRecs)
	}

	// Checkpoint must be the last applied offset (numRecs-1).
	ckpt, found, err := ReadCheckpoint(db, storeName)
	if err != nil {
		t.Fatalf("ReadCheckpoint: %v", err)
	}
	if !found {
		t.Fatal("checkpoint not found after restore")
	}
	if ckpt != int64(numRecs-1) {
		t.Errorf("checkpoint: got %d, want %d", ckpt, numRecs-1)
	}
	t.Logf("PASS: checkpoint=%d, all %d records restored", ckpt, numRecs)
}
