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
type task struct {
	db         *pebble.DB
	executor   *topology.Executor
	collectors map[string]*state.MutationCollector  // keyed by store name
	producers  map[string]*state.ChangelogProducer  // keyed by store name
	partition  int32
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
// It is only meaningful when bt.StoreBindings is non-empty (stateful topologies).
// The caller must wire its lifecycle methods into the kafka.Client via
// WithLifecycle and WithPostBatch.
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
//  2. For each StoreBinding: reads the checkpoint, then restores from the
//     changelog topic (synchronous — R6 verdict A).
//  3. Creates a MutationCollector + KeyValueStoreWithChangelog per store.
//  4. Creates a ChangelogProducer per store.
//  5. Builds a topology.Executor with the stores map.
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
//  2. Writes a final checkpoint to Pebble for each store.
//  3. Closes the ChangelogProducer(s).
//  4. Closes the Pebble DB.
//  5. Removes the task from the map.
func (tm *TaskManager) OnRevoked(ctx context.Context, revoked map[string][]int32) {
	partitions := collectPartitions(revoked)
	for _, p := range partitions {
		tm.closeTask(ctx, p)
	}
}

// PostBatch is the post-batch hook. For every live task it drains the
// MutationCollector(s) and flushes mutations via the ChangelogProducer(s),
// pinned to the task's partition. This runs AFTER processBatch and BEFORE
// produce+commit in the ALO write order.
func (tm *TaskManager) PostBatch(ctx context.Context) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for partition, t := range tm.tasks {
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
	}
	return nil
}

// WriteCheckpoints writes the current changelog high-watermark checkpoint for
// each store on each live task. This must be called AFTER produce+commit so
// the checkpoint reflects offsets whose corresponding source records are already
// committed.
//
// Checkpoint approach: after Flush the changelog partition contains our
// mutations. We fetch the high-watermark of the changelog partition to get the
// next-offset-to-be-written, then checkpoint at HW-1 (the offset of the last
// record we produced). Because ChangelogProducer.Flush (frozen) does not return
// produced offsets, we use state.RestoreFromChangelog's fetchHighWatermark
// indirectly: we call state.ReadCheckpoint after the RestoreFromChangelog path
// sets it during restore. For the post-batch write-path checkpoint we use the
// changelog HW fetched via a lightweight admin call.
//
// NOTE: WriteCheckpoints is called from the Adapter after the kafka.Client
// commits source offsets. It uses state.WriteCheckpointSync, which is a Pebble
// write — it cannot fail the Kafka commit (already done). On failure we log and
// continue; the checkpoint will be stale but restore will self-heal on next
// assignment.
func (tm *TaskManager) WriteCheckpoints(ctx context.Context, brokers []string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for partition, t := range tm.tasks {
		for storeName, binding := range tm.bt.StoreBindings {
			// Derive the full changelog topic name: <AppID>-<storeName>-changelog.
			changelogTopic := tm.appID + "-" + binding.ChangelogTopic + "-changelog"

			// Fetch HW to determine what offset we last wrote.
			// We restore from changelog with startOffset=HW so nothing is replayed;
			// we just want the HW. RestoreFromChangelog returns HW immediately when
			// startOffset >= HW.
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
				// Changelog is empty; nothing to checkpoint.
				continue
			}
			// The last actually-written offset is HW-1.
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

// openTask creates and restores a single per-partition task. Called from OnAssigned.
func (tm *TaskManager) openTask(ctx context.Context, partition int32) error {
	// Build the Pebble directory: StateDir/<appID>/partition-<N>
	dbDir := filepath.Join(tm.cfg.StateDir, tm.appID, fmt.Sprintf("partition-%d", partition))
	db, err := state.OpenDB(dbDir)
	if err != nil {
		return fmt.Errorf("open pebble at %q: %w", dbDir, err)
	}

	stores := make(map[string]any, len(tm.bt.StoreBindings))
	collectors := make(map[string]*state.MutationCollector, len(tm.bt.StoreBindings))
	producers := make(map[string]*state.ChangelogProducer, len(tm.bt.StoreBindings))

	for storeName, binding := range tm.bt.StoreBindings {
		// Derive the full changelog topic name.
		changelogTopic := tm.appID + "-" + binding.ChangelogTopic + "-changelog"

		// Read the local checkpoint (may be absent on first run).
		checkpoint, found, err := state.ReadCheckpoint(db, storeName)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("ReadCheckpoint store %q: %w", storeName, err)
		}
		if !found {
			checkpoint = -1 // RestoreFromChangelog: start from beginning
		}

		// Restore state from the changelog (synchronous — R6 verdict A).
		_, err = state.RestoreFromChangelog(ctx, tm.cfg.Brokers, changelogTopic, partition, checkpoint, db, storeName)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("RestoreFromChangelog store %q partition %d: %w", storeName, partition, err)
		}

		// Create a MutationCollector and ChangelogProducer for ongoing writes.
		collector := &state.MutationCollector{}
		producer, err := state.NewChangelogProducer(tm.cfg.Brokers, changelogTopic)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("NewChangelogProducer store %q: %w", storeName, err)
		}

		// Build a raw-bytes KeyValueStore with changelog capture.
		// Option A (P2-S7fix): the store operates exclusively on []byte keys and
		// []byte values. The Aggregate/Count StatefulProcessFunc (in grouped.go)
		// holds the concrete serdes captured at DSL-build time and encodes/decodes
		// itself — the store never needs to know the concrete K or A types.
		// This eliminates the type-erasure boundary bug where the old erasedStore
		// implemented kvStoreI[any,any] which does NOT satisfy kvStoreI[K,A] for
		// any concrete K,A at runtime.
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

	exec := topology.NewExecutor(tm.bt.Topology, stores)

	t := &task{
		db:         db,
		executor:   exec,
		collectors: collectors,
		producers:  producers,
		partition:  partition,
	}

	tm.mu.Lock()
	tm.tasks[partition] = t
	tm.mu.Unlock()

	tm.logger.Info("task opened",
		slog.Int("partition", int(partition)),
		slog.Int("stores", len(tm.bt.StoreBindings)),
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

	// Write a final checkpoint (read HW, then checkpoint HW-1).
	for storeName, binding := range tm.bt.StoreBindings {
		changelogTopic := tm.appID + "-" + binding.ChangelogTopic + "-changelog"
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
		producer.Close() // ChangelogProducer.Close() returns nothing
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
