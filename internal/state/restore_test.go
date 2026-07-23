//go:build integration

package state

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	testcontainers "github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

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
	hw, err := RestoreFromChangelog(ctx, brokers, topic, partition, -1, db, storeName)
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
	hw, err := RestoreFromChangelog(ctx, brokers, topic, partition, 1, db, storeName)
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
	hw, err := RestoreFromChangelog(ctx, brokers, topic, partition, 1, db, storeName)
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

	hw, err := RestoreFromChangelog(ctx, brokers, topic, partition, -1, db, storeName)
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
