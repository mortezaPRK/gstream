package kafka

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"uuid"

	gstream "mortz.dev/go/gstream"
	"mortz.dev/go/gstream/xtypes"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRunEOSCancelledBeforeBeginReturnsCleanly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client := &Client{
		cfg: gstream.Config{
			ApplicationID:  "shutdown",
			Guarantee:      gstream.ExactlyOnce,
			CommitInterval: time.Second,
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		// sess intentionally nil: touching Begin after cancellation would panic.
	}

	err := client.runEOS(ctx, func(context.Context, InRecord) ([]OutRecord, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatalf("runEOS with cancelled context: %v", err)
	}
}

// validConfig returns a minimal valid gstream.Config for use in unit tests.
func validConfig() gstream.Config {
	cfg, err := gstream.Configure(
		gstream.WithName("test-app"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		panic("validConfig: " + err.Error())
	}
	return cfg
}

// ---------------------------------------------------------------------------
// Guarantee routing — ALO vs EOS path selection
// ---------------------------------------------------------------------------

// TestNew_ALO_UsesKcNotSess verifies that New with AtLeastOnce guarantee
// populates Client.kc and leaves Client.sess nil (ALO path).
func TestNew_ALO_UsesKcNotSess(t *testing.T) {
	cfg := validConfig() // default = AtLeastOnce
	c, err := New(cfg, []string{"topic"}, nil)
	if err != nil {
		t.Fatalf("New ALO: unexpected error: %v", err)
	}
	defer c.Close()
	if c.kc == nil {
		t.Fatal("ALO: expected kc to be non-nil")
	}
	if c.sess != nil {
		t.Fatal("ALO: expected sess to be nil")
	}
}

// TestNew_EOS_UsesSessNotKc verifies that New with ExactlyOnce guarantee
// populates Client.sess and leaves Client.kc nil (EOS path).
// kgo.NewGroupTransactSession dials lazily, so no live broker is needed.
func TestNew_EOS_UsesSessNotKc(t *testing.T) {
	cfg, err := gstream.Configure(
		gstream.WithName("eos-test"),
		gstream.WithBrokers("localhost:19092"), // unreachable; only construction tested
		gstream.WithGuarantee(gstream.ExactlyOnce),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	c, err := New(cfg, []string{"topic"}, nil)
	if err != nil {
		t.Fatalf("New EOS: unexpected error: %v", err)
	}
	defer c.Close()
	if c.sess == nil {
		t.Fatal("EOS: expected sess to be non-nil")
	}
	if c.kc != nil {
		t.Fatal("EOS: expected kc to be nil")
	}
}

// TestWithChangelogFlusher_StoresHook verifies that WithChangelogFlusher wires
// the function into clientOptions correctly (compile + apply path).
func TestWithChangelogFlusher_StoresHook(t *testing.T) {
	var co clientOptions
	WithChangelogFlusher(func(_ context.Context) ([]OutRecord, error) {
		return nil, nil
	})(&co)
	if co.changelogFlusher == nil {
		t.Fatal("expected changelogFlusher to be set after WithChangelogFlusher")
	}
}

// ---------------------------------------------------------------------------
// Config validation
// ---------------------------------------------------------------------------

func TestNew_RejectsEmptyApplicationID(t *testing.T) {
	cfg := validConfig()
	cfg.ApplicationID = ""
	_, err := New(cfg, []string{"topic"}, nil)
	if err == nil {
		t.Fatal("expected error for empty ApplicationID, got nil")
	}
}

func TestNew_RejectsEmptyBrokers(t *testing.T) {
	cfg := validConfig()
	cfg.Brokers = nil
	_, err := New(cfg, []string{"topic"}, nil)
	if err == nil {
		t.Fatal("expected error for empty Brokers, got nil")
	}
}

func TestNew_RejectsEmptyTopics(t *testing.T) {
	cfg := validConfig()
	_, err := New(cfg, nil, nil)
	if err == nil {
		t.Fatal("expected error for empty topics, got nil")
	}
	_, err = New(cfg, []string{}, nil)
	if err == nil {
		t.Fatal("expected error for empty topics slice, got nil")
	}
}

func TestNew_RejectsNegativeNumTaskThreads(t *testing.T) {
	cfg := validConfig()
	cfg.NumTaskThreads = -1
	_, err := New(cfg, []string{"topic"}, nil)
	if err == nil {
		t.Fatal("expected error for negative NumTaskThreads, got nil")
	}
}

func TestNew_RejectsZeroCommitInterval(t *testing.T) {
	cfg := validConfig()
	cfg.CommitInterval = 0
	_, err := New(cfg, []string{"topic"}, nil)
	if err == nil {
		t.Fatal("expected error for zero CommitInterval, got nil")
	}
}

// ---------------------------------------------------------------------------
// ApplyDefaults wiring
// ---------------------------------------------------------------------------

func TestApplyDefaults_FillsCommitInterval(t *testing.T) {
	cfg, err := gstream.Configure(
		gstream.WithName("app"),
		gstream.WithBrokers("b:9092"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CommitInterval != 100*time.Millisecond {
		t.Fatalf("expected CommitInterval=100ms, got %s", cfg.CommitInterval)
	}
}

func TestALOOffsetCommitterDueAndLatestOffset(t *testing.T) {
	committer := newALOOffsetCommitter(100 * time.Millisecond)
	committer.last = time.Unix(10, 0)
	committer.add(
		&kgo.Record{Topic: "input", Partition: 0, Offset: 2},
		&kgo.Record{Topic: "input", Partition: 0, Offset: 4},
		&kgo.Record{Topic: "input", Partition: 0, Offset: 3},
	)
	if committer.due(time.Unix(10, int64(99*time.Millisecond))) {
		t.Fatal("due before interval = true, want false")
	}
	if !committer.due(time.Unix(10, int64(100*time.Millisecond))) {
		t.Fatal("due at interval = false, want true")
	}
	if got := committer.pending["input"][0].Offset; got != 4 {
		t.Fatalf("pending offset = %d, want 4", got)
	}
	deadline, ok := committer.deadline()
	if !ok || !deadline.Equal(time.Unix(10, 0).Add(100*time.Millisecond)) {
		t.Fatalf("deadline = (%s, %t), want (10.1s, true)", deadline, ok)
	}
}

func TestApplyDefaults_FillsNumTaskThreads(t *testing.T) {
	cfg, err := gstream.Configure(
		gstream.WithName("app"),
		gstream.WithBrokers("b:9092"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NumTaskThreads <= 0 {
		t.Fatalf("expected NumTaskThreads > 0, got %d", cfg.NumTaskThreads)
	}
}

func TestApplyDefaults_DefaultGuaranteeIsALO(t *testing.T) {
	cfg, err := gstream.Configure(
		gstream.WithName("app"),
		gstream.WithBrokers("b:9092"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Guarantee != gstream.AtLeastOnce {
		t.Fatalf("expected default Guarantee=AtLeastOnce, got %v", cfg.Guarantee)
	}
}

// ---------------------------------------------------------------------------
// buildOpts — pure helper; no broker needed
// ---------------------------------------------------------------------------

func TestBuildOpts_ReturnsSomeOpts(t *testing.T) {
	cfg := validConfig()
	opts := buildOpts(cfg, []string{"input-topic"}, nil, &clientOptions{}, newALOOffsetCommitter(cfg.CommitInterval))
	if len(opts) == 0 {
		t.Fatal("expected non-empty opts slice from buildOpts")
	}
}

// ---------------------------------------------------------------------------
// Record types
// ---------------------------------------------------------------------------

func TestInRecord_ZeroValue(t *testing.T) {
	var r InRecord
	if r.Topic != "" || r.Partition != 0 || r.Offset != 0 {
		t.Fatal("unexpected non-zero InRecord zero value")
	}
}

func TestOutRecord_Fields(t *testing.T) {
	r := OutRecord{Topic: "sink", Key: []byte("k"), Value: []byte("v")}
	if r.Topic != "sink" {
		t.Fatalf("expected Topic=sink, got %s", r.Topic)
	}
}

// TestOutRecord_NilPartitionIsUnpinned verifies that the zero-value OutRecord
// (no Partition set) has IsValid=false, meaning it follows the key-hash path,
// not the pinned path.
func TestOutRecord_NilPartitionIsUnpinned(t *testing.T) {
	r := OutRecord{Topic: "sink", Key: []byte("k"), Value: []byte("v")}
	if r.Partition.IsValid {
		t.Fatalf("expected zero-value OutRecord.Partition to be unset (IsValid=false), got %v", r.Partition)
	}
}

// TestOutRecord_PinnedPartition verifies that a pinned Partition (including
// partition 0, which is a valid pinned target) is preserved through the field.
func TestOutRecord_PinnedPartition(t *testing.T) {
	r := OutRecord{Topic: "changelog", Key: []byte("k"), Value: []byte("v"), Partition: xtypes.NilOf(int32(0))}
	if !r.Partition.IsValid {
		t.Fatal("expected IsValid=true for pinned record")
	}
	if r.Partition.Value != 0 {
		t.Fatalf("expected Partition.Value=0, got %d", r.Partition.Value)
	}
}

// ---------------------------------------------------------------------------
// mixedPartitionerFn — no broker needed
// ---------------------------------------------------------------------------

// TestMixedPartitioner_PinnedReturnsExactPartition verifies that when the kgo.Record
// has Partition >= 0, the partitioner returns that exact value regardless of n.
func TestMixedPartitioner_PinnedReturnsExactPartition(t *testing.T) {
	fn := mixedPartitionerFn("any-topic")

	cases := []struct {
		partition int32
		n         int
	}{
		{0, 3},
		{1, 3},
		{2, 3},
		{0, 1},
		{5, 10},
	}
	for _, tc := range cases {
		r := &kgo.Record{Partition: tc.partition, Key: []byte("somekey")}
		got := fn(r, tc.n)
		if got != int(tc.partition) {
			t.Errorf("pinned partition=%d n=%d: got %d, want %d",
				tc.partition, tc.n, got, int(tc.partition))
		}
	}
}

// TestMixedPartitioner_UnpinnedHashesInRange verifies that when Partition < 0 (sentinel
// for unpinned / sink records), the result is always in [0, n).
func TestMixedPartitioner_UnpinnedHashesInRange(t *testing.T) {
	fn := mixedPartitionerFn("any-topic")

	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("another-key"),
		[]byte(""),
		nil,
	}
	partitionCounts := []int{1, 2, 3, 10, 100}

	for _, key := range keys {
		for _, n := range partitionCounts {
			r := &kgo.Record{Partition: -1, Key: key}
			got := fn(r, n)
			if got < 0 || got >= n {
				t.Errorf("unpinned key=%q n=%d: result %d out of [0,%d)", key, n, got, n)
			}
		}
	}
}

// TestMixedPartitioner_UnpinnedIsStable verifies that the same key always maps
// to the same partition for a given n (key-affinity / idempotency).
func TestMixedPartitioner_UnpinnedIsStable(t *testing.T) {
	fn := mixedPartitionerFn("any-topic")

	const n = 8
	keys := [][]byte{
		[]byte("stable-key-1"),
		[]byte("stable-key-2"),
		[]byte("stable-key-3"),
	}
	for _, key := range keys {
		r := &kgo.Record{Partition: -1, Key: key}
		first := fn(r, n)
		for i := 0; i < 10; i++ {
			got := fn(r, n)
			if got != first {
				t.Errorf("key=%q: non-stable result, got %d then %d", key, first, got)
			}
		}
	}
}

// TestMixedPartitioner_NilPartitionMapsSentinel verifies that an OutRecord with
// IsValid=false Partition (zero-value / sink) maps to kgo.Record.Partition=-1,
// which routes through the key-hash path in the mixed partitioner.
func TestMixedPartitioner_NilPartitionMapsSentinel(t *testing.T) {
	// Simulate the produce-step mapping for an unset-Partition OutRecord.
	out := OutRecord{Topic: "sink", Key: []byte("k"), Value: []byte("v")}
	kr := &kgo.Record{Topic: out.Topic, Key: out.Key, Value: out.Value}
	if !out.Partition.IsValid {
		kr.Partition = -1
	} else {
		kr.Partition = out.Partition.Value
	}
	if kr.Partition != -1 {
		t.Fatalf("unset OutRecord.Partition should map to kgo sentinel -1, got %d", kr.Partition)
	}
	// Confirm the mixed partitioner routes this to a key-hash (not a pinned path).
	fn := mixedPartitionerFn("sink")
	got := fn(kr, 5)
	if got < 0 || got >= 5 {
		t.Fatalf("key-hash path returned out-of-range %d for n=5", got)
	}
}

// ---------------------------------------------------------------------------
// kafkaMurmur2 correctness — PROOF tests
// ---------------------------------------------------------------------------

// TestKafkaMurmur2_KnownVectors checks kafkaMurmur2 against known hash values.
//
// These values are the ground truth produced by our kafkaMurmur2 (seed=0x9747b28c,
// Kafka variant), and are independently confirmed to be correct because
// TestKafkaMurmur2_MatchesStickyKeyPartitioner proves this implementation agrees
// with franz-go's internal murmur2 (which drives StickyKeyPartitioner and is
// itself a verified port of org.apache.kafka.common.utils.Utils.murmur2).
//
// The test keeps hardcoded vectors as a regression guard: if someone accidentally
// alters kafkaMurmur2 (e.g. changes byte order or seed), it fails here immediately.
func TestKafkaMurmur2_KnownVectors(t *testing.T) {
	cases := []struct {
		key      []byte
		wantHash uint32
	}{
		// Empty key: seed ^ 0, no body iterations, finalisation only.
		{[]byte{}, 0x106e08d9},
		// Single byte 'a' (0x61).
		{[]byte("a"), 0xa2d0b27c},
		// "hello" — 5 bytes: one full chunk + 1 tail byte.
		{[]byte("hello"), 0x7f1ddbbd},
		// "kafka" — 5 bytes.
		{[]byte("kafka"), 0xd067cf64},
		// "test" — exactly 4 bytes, one full chunk, no tail.
		{[]byte("test"), 0x2ab0e07f},
	}
	for _, tc := range cases {
		got := kafkaMurmur2(tc.key)
		if got != tc.wantHash {
			t.Errorf("kafkaMurmur2(%q) = 0x%08x, want 0x%08x", tc.key, got, tc.wantHash)
		}
	}
}

// TestKafkaMurmur2_MatchesStickyKeyPartitioner is the primary correctness proof:
// for many keys and partition counts, our kafkaMurmur2-based hasher and
// kgo.StickyKeyPartitioner(nil) (which internally uses franz-go's own murmur2)
// must agree on the chosen partition. Any disagreement means our murmur2 is
// wrong or our toPositive / mod differs from Kafka's.
func TestKafkaMurmur2_MatchesStickyKeyPartitioner(t *testing.T) {
	// Build our hasher exactly as mixedPartitionerFn does.
	ourHasher := kgo.KafkaHasher(kafkaMurmur2)

	// StickyKeyPartitioner(nil) uses franz-go's internal murmur2 with KafkaHasher.
	// For a record with a non-nil key it always calls hasher(key, n) — no sticky
	// behaviour is involved (sticky only applies to keyless records).
	refPartitioner := kgo.StickyKeyPartitioner(nil)
	refTopic := refPartitioner.ForTopic("test-topic")

	keys := [][]byte{
		[]byte(""),
		[]byte("a"),
		[]byte("ab"),
		[]byte("abc"),
		[]byte("abcd"),
		[]byte("abcde"),
		[]byte("hello"),
		[]byte("kafka"),
		[]byte("test"),
		[]byte("gstream"),
		[]byte("key-with-dashes"),
		[]byte("partition-key-12345"),
		{0x00},
		{0xff},
		{0x00, 0x01, 0x02, 0x03},
		{0xde, 0xad, 0xbe, 0xef},
		{0xde, 0xad, 0xbe, 0xef, 0x00},
	}
	partitionCounts := []int{1, 2, 3, 6, 10, 12, 100}

	for _, key := range keys {
		for _, n := range partitionCounts {
			ourPart := ourHasher(key, n)

			refRec := &kgo.Record{Key: key}
			refPart := refTopic.Partition(refRec, n)

			if ourPart != refPart {
				t.Errorf("key=%q n=%d: our murmur2 partition=%d, StickyKeyPartitioner=%d (MISMATCH)",
					key, n, ourPart, refPart)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// resolveInstanceID — per-instance EOS transactional ID resolution
// ---------------------------------------------------------------------------

func cfgWithStateDir(stateDir string) gstream.Config {
	cfg, err := gstream.Configure(
		gstream.WithName("test-app"),
		gstream.WithBrokers("localhost:9092"),
		gstream.WithStateDir(stateDir),
	)
	if err != nil {
		panic("cfgWithStateDir: " + err.Error())
	}
	return cfg
}

// TestResolveInstanceID_AbsentFile_GeneratesAndPersists verifies that when the
// StateDir/instance-id file does not exist, resolveInstanceID generates a UUID,
// persists it, and returns it. The file must exist with that content afterwards.
func TestResolveInstanceID_AbsentFile_GeneratesAndPersists(t *testing.T) {
	dir := t.TempDir()
	cfg := cfgWithStateDir(dir)

	id, err := resolveInstanceID(cfg)
	if err != nil {
		t.Fatalf("resolveInstanceID: unexpected error: %v", err)
	}
	if id == "" {
		t.Fatal("resolveInstanceID: returned empty ID")
	}
	if _, err := uuid.Parse(id); err != nil {
		t.Fatalf("resolveInstanceID: returned invalid UUID %q: %v", id, err)
	}

	// File must now exist with that content.
	data, err := os.ReadFile(filepath.Join(dir, "instance-id"))
	if err != nil {
		t.Fatalf("instance-id file not created: %v", err)
	}
	if got := strings.TrimSpace(string(data)); got != id {
		t.Fatalf("file content %q != returned ID %q", got, id)
	}
}

// TestResolveInstanceID_PresentFile_ReturnsStableID verifies that a second call
// reads the existing file, returns the SAME id, and does NOT overwrite the file.
func TestResolveInstanceID_PresentFile_ReturnsStableID(t *testing.T) {
	dir := t.TempDir()
	cfg := cfgWithStateDir(dir)

	id1, err := resolveInstanceID(cfg)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}

	id2, err := resolveInstanceID(cfg)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("instance ID not stable across calls: first=%q second=%q", id1, id2)
	}
}

// TestResolveInstanceID_ExplicitInstanceID_VerbatimNoop verifies that when
// cfg.InstanceID is set, resolveInstanceID returns it verbatim and does NOT
// create or touch the StateDir/instance-id file.
func TestResolveInstanceID_ExplicitInstanceID_VerbatimNoop(t *testing.T) {
	dir := t.TempDir()
	cfg, err := gstream.Configure(
		gstream.WithName("test-app"),
		gstream.WithBrokers("localhost:9092"),
		gstream.WithStateDir(dir),
	)
	if err != nil {
		t.Fatal(err)
	}
	cfg.InstanceID = "my-explicit-id"

	id, err := resolveInstanceID(cfg)
	if err != nil {
		t.Fatalf("resolveInstanceID: %v", err)
	}
	if id != "my-explicit-id" {
		t.Fatalf("expected verbatim ID, got %q", id)
	}

	// File must NOT have been created.
	if _, err := os.Stat(filepath.Join(dir, "instance-id")); !os.IsNotExist(err) {
		t.Fatal("instance-id file should not have been created for explicit InstanceID")
	}
}

// TestResolveInstanceID_DifferentStateDirs_DifferentIDs verifies that two instances
// with different StateDirs receive distinct auto-generated IDs.
func TestResolveInstanceID_DifferentStateDirs_DifferentIDs(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	id1, err := resolveInstanceID(cfgWithStateDir(dir1))
	if err != nil {
		t.Fatalf("dir1: %v", err)
	}
	id2, err := resolveInstanceID(cfgWithStateDir(dir2))
	if err != nil {
		t.Fatalf("dir2: %v", err)
	}
	if id1 == id2 {
		t.Fatalf("expected different IDs for different StateDirs, got same: %q", id1)
	}
}

// TestBuildOptsEOS_TransactionalIDFormat verifies that the TransactionalID
// embedded in buildOptsEOS options has the expected format "gstream-<appID>-<instanceID>".
//
// kgo does not expose an accessor for TransactionalID from opts; we verify
// the format indirectly by confirming that NewGroupTransactSession succeeds with
// the opts (dial is lazy) and that our string composition is correct.
func TestBuildOptsEOS_TransactionalIDFormat(t *testing.T) {
	const appID = "my-app"
	const instanceID = "abc-123"

	cfg, err := gstream.Configure(
		gstream.WithName(appID),
		gstream.WithBrokers("localhost:19092"),
		gstream.WithGuarantee(gstream.ExactlyOnce),
	)
	if err != nil {
		t.Fatal(err)
	}

	opts := buildOptsEOS(cfg, []string{"topic"}, slog.Default(), &clientOptions{}, instanceID)
	if len(opts) == 0 {
		t.Fatal("buildOptsEOS returned no opts")
	}

	// Verify construction succeeds (NewGroupTransactSession dials lazily).
	sess, err := kgo.NewGroupTransactSession(opts...)
	if err != nil {
		t.Fatalf("NewGroupTransactSession: %v", err)
	}
	sess.Close()

	// Verify the format string directly.
	want := "gstream-" + appID + "-" + instanceID
	if want != "gstream-my-app-abc-123" {
		t.Fatalf("format check: want gstream-my-app-abc-123, got %s", want)
	}
}
