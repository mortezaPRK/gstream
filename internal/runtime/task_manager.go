package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/state"
	"github.com/mortezaPRK/gstream/internal/topology"
	"github.com/cockroachdb/pebble"
)

// task holds the per-partition state for stateful stream processing (§7, P2-S7).
//
// Each task owns:
//   - A Pebble DB shard at StateDir/<appID>/partition-<N>.
//   - One topology.Executor instance with its own private sink buffers and the
//     stores map wired to the KeyValueStores opened for this partition.
//   - One MutationCollector per store, collecting mutations during processing.
//   - One ChangelogProducer per store, flushing mutations to the changelog topic.
//
// For windowed topologies (P3-T4):
//   - streamTime is the per-task stream-time watermark; the Executor holds
//     &streamTime so windowed processors advance it via ctx.AdvanceStreamTime.
//   - lastSweepTime tracks the stream-time at the last retention sweep for
//     amortization (sweep only once per window-size of stream-time advancement).
//   - stores retains the concrete store references so PostBatch can access them
//     for the retention sweep.
type task struct {
	db            *pebble.DB
	executor      *topology.Executor
	collectors    map[string]*state.MutationCollector  // keyed by store name
	producers     map[string]*state.ChangelogProducer  // keyed by store name
	stores        map[string]any                       // keyed by store name; both regular + window
	partition     int32
	streamTime    int64 // per-task stream-time watermark; 0 if no windowed stores
	lastSweepTime int64 // stream-time at last retention sweep (amortization)
}

// TaskManager manages per-partition tasks for stateful topologies. It implements
// the lifecycle callbacks (onAssigned, onRevoked) injected into the kafka.Client
// via WithLifecycle, and the post-batch hook injected via WithPostBatch.
//
// TaskManager is NOT safe for concurrent use from outside its own callbacks; the
// kafka.Client's cooperative-sticky rebalance ensures onAssigned and onRevoked
// are not concurrent with each other or with Run's processBatch.
type TaskManager struct {
	mu     sync.Mutex
	tasks  map[int32]*task // keyed by partition

	bt      *gstream.BuiltTopology
	cfg     gstream.Config
	logger  *slog.Logger
	appID   string
}

// NewTaskManager creates a TaskManager for the given topology and config.
//
// It is only meaningful when bt.StoreBindings or bt.WindowStoreBindings is
// non-empty (stateful topologies). The caller must wire its lifecycle methods
// into the kafka.Client via WithLifecycle and WithPostBatch.
func NewTaskManager(bt *gstream.BuiltTopology, cfg gstream.Config, logger *slog.Logger) *TaskManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &TaskManager{
		tasks:  make(map[int32]*task),
		bt:     bt,
		cfg:    cfg,
		logger: logger,
		appID:  cfg.ApplicationID,
	}
}

// OnAssigned is the partition-assignment lifecycle callback. For each assigned
// partition it:
//  1. Opens a Pebble DB at StateDir/<appID>/partition-<N>.
//  2. For each StoreBinding and WindowStoreBinding: reads the checkpoint, then
//     restores from the changelog topic (synchronous — R6 verdict A).
//  3. Creates a MutationCollector + KeyValueStoreWithChangelog per store.
//  4. Creates a ChangelogProducer per store.
//  5. For windowed topologies: reads the persisted stream-time watermark.
//  6. Builds a topology.Executor (with stream-time pointer for windowed paths).
//
// OnAssigned blocks until all partitions are restored. The kafka.Client
// (cooperative-sticky) will not deliver fetches for these partitions until
// the callback returns.
func (tm *TaskManager) OnAssigned(ctx context.Context, assigned map[string][]int32) error {
	// Collect all partition numbers from the assigned map (source topic → []partitions).
	partitions := collectPartitions(assigned)

	for _, p := range partitions {
		if err := tm.openTask(ctx, p); err != nil {
			return fmt.Errorf("TaskManager.OnAssigned: partition %d: %w", p, err)
		}
	}
	return nil
}

// OnRevoked is the partition-revocation lifecycle callback. For each revoked
// partition it:
//  1. Drains any pending mutations and flushes them to the changelog.
//  2. For windowed topologies: persists the final stream-time watermark.
//  3. Writes a final checkpoint to Pebble for each store.
//  4. Closes the ChangelogProducer(s).
//  5. Closes the Pebble DB.
//  6. Removes the task from the map.
func (tm *TaskManager) OnRevoked(ctx context.Context, revoked map[string][]int32) {
	partitions := collectPartitions(revoked)
	for _, p := range partitions {
		tm.closeTask(ctx, p)
	}
}

// PostBatch is the post-batch hook. For every live task it:
//  1. Runs the retention sweep for windowed stores (amortized; before flush so
//     tombstones from expired windows are included in this batch's flush).
//  2. Drains the MutationCollector(s) and flushes mutations via the
//     ChangelogProducer(s), pinned to the task's partition.
//  3. Persists the stream-time watermark (windowed topologies only).
//
// This runs AFTER processBatch and BEFORE produce+commit in the ALO write order.
//
// ALO caveat (stream-time ordering): WriteStreamTime uses pebble.Sync which
// commits to disk BEFORE the source offsets are committed to Kafka. On a crash
// between the Sync and the Kafka commit, the persisted stream-time may be ahead
// of the replayed source offset on restart. Windows already swept or expired per
// the persisted stream-time may not be rebuilt from the redelivered records —
// acceptable under at-least-once semantics. EOS (P5) closes this gap.
func (tm *TaskManager) PostBatch(ctx context.Context) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for partition, t := range tm.tasks {
		// Retention sweep (windowed stores only). Runs before the changelog flush
		// so tombstones for expired windows are drained and flushed in the same batch.
		// Amortized: sweeps only when stream-time advances by >= MaxSizeMs since last sweep.
		if len(tm.bt.WindowStoreBindings) > 0 {
			if err := tm.runSweep(t); err != nil {
				return fmt.Errorf("TaskManager.PostBatch: partition %d: sweep: %w", partition, err)
			}
		}

		// Flush all mutations (state updates + sweep tombstones) to changelog.
		for storeName, collector := range t.collectors {
			muts := collector.Drain()
			if len(muts) == 0 {
				continue
			}
			producer := t.producers[storeName]
			if producer == nil {
				return fmt.Errorf("TaskManager.PostBatch: partition %d store %q: no producer", partition, storeName)
			}
			if err := producer.Flush(ctx, partition, muts); err != nil {
				return fmt.Errorf("TaskManager.PostBatch: partition %d store %q: flush: %w", partition, storeName, err)
			}
		}

		// Persist stream-time after changelog flush (windowed topologies only).
		// See ALO caveat in the method doc above.
		if len(tm.bt.WindowStoreBindings) > 0 {
			if err := state.WriteStreamTime(t.db, t.streamTime); err != nil {
				// Non-fatal: on crash, stream-time will be rebuilt from redelivered records.
				tm.logger.Warn("PostBatch: WriteStreamTime failed",
					slog.Int("partition", int(partition)),
					slog.Any("error", err),
				)
			}
		}
	}
	return nil
}

// WriteCheckpoints writes the current changelog high-watermark checkpoint for
// each store on each live task. This must be called AFTER produce+commit so
// the checkpoint reflects offsets whose corresponding source records are already
// committed.
//
// Covers both regular StoreBindings and WindowStoreBindings.
func (tm *TaskManager) WriteCheckpoints(ctx context.Context, brokers []string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for partition, t := range tm.tasks {
		for storeName, changelogTopic := range tm.allChangelogTopics() {
			hw, err := state.RestoreFromChangelog(ctx, brokers, changelogTopic, partition, /*checkpointOffset=*/int64(^uint64(0)>>1), t.db, storeName)
			if err != nil {
				tm.logger.Warn("WriteCheckpoints: fetch HW via restore failed",
					slog.Int("partition", int(partition)),
					slog.String("store", storeName),
					slog.Any("error", err),
				)
				continue
			}
			if hw == 0 {
				continue
			}
			if err := state.WriteCheckpointSync(t.db, storeName, hw-1); err != nil {
				tm.logger.Warn("WriteCheckpoints: WriteCheckpointSync failed",
					slog.Int("partition", int(partition)),
					slog.String("store", storeName),
					slog.Any("error", err),
				)
			}
		}
	}
}

// Executor returns the executor for the given partition, or nil if no task is
// assigned for that partition. Used by the Adapter to route records.
func (tm *TaskManager) Executor(partition int32) *topology.Executor {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if t, ok := tm.tasks[partition]; ok {
		return t.executor
	}
	return nil
}

// allChangelogTopics returns a map of storeName → full changelog topic for all
// store bindings (regular + windowed). Used by WriteCheckpoints and closeTask.
func (tm *TaskManager) allChangelogTopics() map[string]string {
	out := make(map[string]string, len(tm.bt.StoreBindings)+len(tm.bt.WindowStoreBindings))
	for storeName, binding := range tm.bt.StoreBindings {
		out[storeName] = tm.appID + "-" + binding.ChangelogTopic + "-changelog"
	}
	for storeName, binding := range tm.bt.WindowStoreBindings {
		out[storeName] = tm.appID + "-" + binding.ChangelogTopic + "-changelog"
	}
	return out
}

// openTask creates and restores a single per-partition task. Called from OnAssigned.
func (tm *TaskManager) openTask(ctx context.Context, partition int32) error {
	// Build the Pebble directory: StateDir/<appID>/partition-<N>
	dbDir := filepath.Join(tm.cfg.StateDir, tm.appID, fmt.Sprintf("partition-%d", partition))
	db, err := state.OpenDB(dbDir)
	if err != nil {
		return fmt.Errorf("open pebble at %q: %w", dbDir, err)
	}

	totalStores := len(tm.bt.StoreBindings) + len(tm.bt.WindowStoreBindings)
	stores := make(map[string]any, totalStores)
	collectors := make(map[string]*state.MutationCollector, totalStores)
	producers := make(map[string]*state.ChangelogProducer, totalStores)

	// Regular (non-windowed) store bindings.
	for storeName, binding := range tm.bt.StoreBindings {
		changelogTopic := tm.appID + "-" + binding.ChangelogTopic + "-changelog"

		checkpoint, found, err := state.ReadCheckpoint(db, storeName)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("ReadCheckpoint store %q: %w", storeName, err)
		}
		if !found {
			checkpoint = -1 // RestoreFromChangelog: start from beginning
		}

		_, err = state.RestoreFromChangelog(ctx, tm.cfg.Brokers, changelogTopic, partition, checkpoint, db, storeName)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("RestoreFromChangelog store %q partition %d: %w", storeName, partition, err)
		}

		collector := &state.MutationCollector{}
		producer, err := state.NewChangelogProducer(tm.cfg.Brokers, changelogTopic)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("NewChangelogProducer store %q: %w", storeName, err)
		}

		// Option A (P2-S7fix): raw-bytes KeyValueStore. The DSL processors hold
		// their concrete serdes and encode/decode themselves at the DSL boundary.
		store := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
			storeName,
			db,
			gstream.BytesSerde{},
			gstream.BytesSerde{},
			collector,
		)

		stores[storeName] = store
		collectors[storeName] = collector
		producers[storeName] = producer
	}

	// Windowed store bindings (P3-T4). Same byte-store pattern as regular stores;
	// the window composite key encoding lives in internal/state and is transparent
	// to the runtime — it's just bytes from this layer's perspective.
	// RestoreFromChangelog replays windowed keys exactly like regular keys (including
	// any retention tombstones produced by previous sweeps).
	for storeName, binding := range tm.bt.WindowStoreBindings {
		changelogTopic := tm.appID + "-" + binding.ChangelogTopic + "-changelog"

		checkpoint, found, err := state.ReadCheckpoint(db, storeName)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("ReadCheckpoint window store %q: %w", storeName, err)
		}
		if !found {
			checkpoint = -1
		}

		// Restore windowed state from changelog. Window composite keys are just bytes;
		// RestoreFromChangelog replays them — including tombstones from past sweeps
		// which will delete any expired windows that were previously written.
		_, err = state.RestoreFromChangelog(ctx, tm.cfg.Brokers, changelogTopic, partition, checkpoint, db, storeName)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("RestoreFromChangelog window store %q partition %d: %w", storeName, partition, err)
		}

		collector := &state.MutationCollector{}
		producer, err := state.NewChangelogProducer(tm.cfg.Brokers, changelogTopic)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("NewChangelogProducer window store %q: %w", storeName, err)
		}

		store := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
			storeName,
			db,
			gstream.BytesSerde{},
			gstream.BytesSerde{},
			collector,
		)

		stores[storeName] = store
		collectors[storeName] = collector
		producers[storeName] = producer
	}

	// Read persisted stream-time (0 if not found — safe initial watermark).
	var streamTime int64
	if len(tm.bt.WindowStoreBindings) > 0 {
		ts, _, err := state.ReadStreamTime(db)
		if err != nil {
			for _, p := range producers {
				p.Close()
			}
			_ = db.Close()
			return fmt.Errorf("ReadStreamTime partition %d: %w", partition, err)
		}
		streamTime = ts
	}

	// Create the task first so its streamTime field has a stable address for the Executor.
	t := &task{
		db:         db,
		collectors: collectors,
		producers:  producers,
		stores:     stores,
		partition:  partition,
		streamTime: streamTime,
	}

	// Wire executor. Use NewExecutorWithStreamTime when windowed stores are present so
	// windowed processors can advance the shared stream-time watermark via
	// ctx.AdvanceStreamTime. Non-windowed processors never touch stream-time — harmless.
	// Keep NewExecutor for the no-window path so stateless/non-windowed tests are unaffected.
	if len(tm.bt.WindowStoreBindings) > 0 {
		t.executor = topology.NewExecutorWithStreamTime(tm.bt.Topology, stores, &t.streamTime)
	} else {
		t.executor = topology.NewExecutor(tm.bt.Topology, stores)
	}

	tm.mu.Lock()
	tm.tasks[partition] = t
	tm.mu.Unlock()

	tm.logger.Info("task opened",
		slog.Int("partition", int(partition)),
		slog.Int("stores", len(tm.bt.StoreBindings)),
		slog.Int("windowStores", len(tm.bt.WindowStoreBindings)),
	)
	return nil
}

// closeTask flushes, checkpoints, and closes a single task. Called from OnRevoked.
func (tm *TaskManager) closeTask(ctx context.Context, partition int32) {
	tm.mu.Lock()
	t, ok := tm.tasks[partition]
	if ok {
		delete(tm.tasks, partition)
	}
	tm.mu.Unlock()

	if !ok {
		return
	}

	// Flush any pending mutations before closing.
	for storeName, collector := range t.collectors {
		muts := collector.Drain()
		if len(muts) > 0 {
			if err := t.producers[storeName].Flush(ctx, partition, muts); err != nil {
				tm.logger.Warn("closeTask: flush failed",
					slog.Int("partition", int(partition)),
					slog.String("store", storeName),
					slog.Any("error", err),
				)
			}
		}
	}

	// Persist stream-time before close so a clean shutdown does not lose watermark.
	// On the next assignment, ReadStreamTime in openTask will recover this value,
	// allowing windowed processors to resume from the correct watermark.
	if len(tm.bt.WindowStoreBindings) > 0 {
		if err := state.WriteStreamTime(t.db, t.streamTime); err != nil {
			tm.logger.Warn("closeTask: WriteStreamTime failed",
				slog.Int("partition", int(partition)),
				slog.Any("error", err),
			)
		}
	}

	// Write a final checkpoint (read HW, then checkpoint HW-1) for all stores
	// (regular + windowed). Covering window stores ensures restore on the next
	// assignment skips already-applied changelog entries.
	for storeName, changelogTopic := range tm.allChangelogTopics() {
		hw, err := state.RestoreFromChangelog(ctx, tm.cfg.Brokers, changelogTopic, partition, int64(^uint64(0)>>1), t.db, storeName)
		if err == nil && hw > 0 {
			if err := state.WriteCheckpointSync(t.db, storeName, hw-1); err != nil {
				tm.logger.Warn("closeTask: WriteCheckpointSync failed",
					slog.Int("partition", int(partition)),
					slog.String("store", storeName),
					slog.Any("error", err),
				)
			}
		}
	}

	// Close changelog producers.
	for _, producer := range t.producers {
		producer.Close()
	}

	// Close the Pebble DB last.
	if err := t.db.Close(); err != nil {
		tm.logger.Warn("closeTask: pebble close failed",
			slog.Int("partition", int(partition)),
			slog.Any("error", err),
		)
	}

	tm.logger.Info("task closed", slog.Int("partition", int(partition)))
}

// computeSweepInterval returns the maximum MaxSizeMs across all window store
// bindings. PostBatch uses this to amortize sweeps: sweep only once per
// window-size of stream-time advancement.
func (tm *TaskManager) computeSweepInterval() int64 {
	var maxSize int64
	for _, binding := range tm.bt.WindowStoreBindings {
		if s := binding.WindowDef.MaxSizeMs(); s > maxSize {
			maxSize = s
		}
	}
	return maxSize
}

// runSweep runs the retention sweep for all windowed stores on t, but only
// if stream-time has advanced by at least sweepInterval ms since the last sweep.
//
// Amortization cadence: sweep only when task.streamTime - task.lastSweepTime >=
// max(MaxSizeMs across all window bindings). This caps sweep cost to at most once
// per window-size of stream-time advancement. Correctness is preserved: a window
// swept late was already past its grace period and could no longer be updated;
// late sweeping only wastes disk briefly.
//
// After sweeping, tombstone Mutations are in each store's collector. They will be
// drained and flushed to the changelog in the same PostBatch call (the flush loop
// runs after runSweep), ensuring changelog consumers never rebuild expired windows.
func (tm *TaskManager) runSweep(t *task) error {
	sweepInterval := tm.computeSweepInterval()
	if sweepInterval <= 0 {
		return nil
	}
	// Skip if stream-time hasn't advanced enough since the last sweep.
	if t.streamTime-t.lastSweepTime < sweepInterval {
		return nil
	}

	for storeName, binding := range tm.bt.WindowStoreBindings {
		store, ok := t.stores[storeName].(*state.KeyValueStore[[]byte, []byte])
		if !ok {
			return fmt.Errorf("runSweep: store %q: unexpected type %T", storeName, t.stores[storeName])
		}
		if _, err := sweepWindowStore(store, binding.WindowDef, binding.GraceMs, t.streamTime); err != nil {
			return fmt.Errorf("runSweep: store %q: %w", storeName, err)
		}
	}
	t.lastSweepTime = t.streamTime
	return nil
}

// sweepWindowStore scans every entry in store and deletes those whose window
// start is before the expiry boundary (streamTime - windowDef.MaxSizeMs() - graceMs).
// Returns the number of entries deleted.
//
// ITERATE-ALL-AND-FILTER (not a front-scan): the window composite key sorts by
//
//	(uint32(len(kBytes)), kBytes, int64(windowStart))
//
// so expired windows are scattered across every key's sub-range, NOT clustered at
// the store front. A scan of just the lowest range would miss all entries for keys
// that sort after the first key in the store. Iterating the full store and checking
// each windowStart is the only correct approach.
//
// Each store.Delete appends a Mutation{IsDelete:true} tombstone to the store's
// attached MutationCollector. The caller (PostBatch) drains the collector and
// flushes tombstones to the changelog so that restore correctly skips or deletes
// expired windows on the next assignment.
//
// sweepWindowStore is intentionally a package-level function (not a method) so it
// can be exercised in unit tests without a running Kafka broker (the store and
// collector are constructed from an in-memory Pebble DB).
func sweepWindowStore(
	store *state.KeyValueStore[[]byte, []byte],
	windowDef gstream.WindowDefinition,
	graceMs, streamTime int64,
) (int, error) {
	expiryBoundary := streamTime - windowDef.MaxSizeMs() - graceMs
	if expiryBoundary <= 0 {
		// No stream-time advancement yet, or boundary is non-positive: nothing to expire.
		return 0, nil
	}

	// Collect expired composite keys first; do not delete while iterating.
	// Range calls fn with slices backed by Pebble's iterator buffer — we must copy
	// before the next iterator step invalidates them.
	var expired [][]byte
	if err := store.Range(func(compositeKey, _ []byte) bool {
		_, windowStart, decErr := state.DecodeWindowCompositeKey(compositeKey)
		if decErr != nil {
			// Malformed key (should not happen with well-formed topologies): skip.
			return true
		}
		if windowStart < expiryBoundary {
			kCopy := make([]byte, len(compositeKey))
			copy(kCopy, compositeKey)
			expired = append(expired, kCopy)
		}
		return true // always continue — all keys must be examined
	}); err != nil {
		return 0, fmt.Errorf("sweepWindowStore: full-store range scan: %w", err)
	}

	// Delete expired entries. store.Delete(k) encodes k via BytesSerde (identity),
	// prepends the store prefix to get the full Pebble key, deletes it, and appends
	// Mutation{Key: prefix+k, IsDelete: true} to the store's MutationCollector.
	// The caller's PostBatch flush loop will drain this collector and push the
	// tombstones to the changelog in the same batch.
	for _, ck := range expired {
		if err := store.Delete(ck); err != nil {
			return 0, fmt.Errorf("sweepWindowStore: delete expired window key: %w", err)
		}
	}
	return len(expired), nil
}

// collectPartitions flattens a topic→[]partition map into a deduplicated slice.
// The map comes from kgo rebalance callbacks; keys are source topic names.
// Because one task runs per partition (regardless of how many source topics
// share that partition index), we deduplicate by partition number.
func collectPartitions(m map[string][]int32) []int32 {
	seen := make(map[int32]struct{})
	var out []int32
	for _, parts := range m {
		for _, p := range parts {
			if _, ok := seen[p]; !ok {
				seen[p] = struct{}{}
				out = append(out, p)
			}
		}
	}
	return out
}

// No erasedStore / bytesSerde here — Option A (P2-S7fix) puts the bytes boundary
// directly in the store construction above (NewKeyValueStoreWithChangelog[[]byte,[]byte]
// with gstream.BytesSerde{}). The Aggregate/Count processors capture their serdes at
// DSL-build time and encode/decode themselves, so the runtime does not need to know
// the concrete K,A types at all.
