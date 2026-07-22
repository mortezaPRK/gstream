package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/state"
	"github.com/mortezaPRK/gstream/internal/topology"
)

// task holds the per-partition state for stateful stream processing.
//
// Each task owns:
//   - A Pebble DB shard at StateDir/<appID>/partition-<N>.
//   - One topology.Executor instance with its own private sink buffers and the
//     stores map wired to the KeyValueStores opened for this partition.
//   - One MutationCollector per store, collecting mutations during processing.
//   - One ChangelogProducer per store, flushing mutations to the changelog topic.
//
// For windowed topologies:
//   - streamTime is the per-task stream-time watermark; the Executor holds
//     &streamTime so windowed processors advance it via ctx.AdvanceStreamTime.
//   - lastSweepTime tracks the stream-time at the last retention sweep for
//     amortization (sweep only once per window-size of stream-time advancement).
//   - stores retains the concrete store references so PostBatch can access them.
type task struct {
	db            *pebble.DB
	executor      *topology.Executor
	collectors    map[string]*state.MutationCollector // keyed by store name
	producers     map[string]*state.ChangelogProducer // keyed by store name
	stores        map[string]any                      // keyed by store name; both regular + window
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
	mu    sync.Mutex
	tasks map[int32]*task // keyed by partition

	bt     *gstream.BuiltTopology
	cfg    gstream.Config
	logger *slog.Logger
	appID  string
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
// partition it opens a Pebble DB, restores from the changelog, creates stores,
// and builds a topology.Executor. Blocks until all partitions are restored.
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
// partition it flushes pending mutations, persists stream-time and checkpoints,
// closes producers, and closes the Pebble DB.
func (tm *TaskManager) OnRevoked(ctx context.Context, revoked map[string][]int32) {
	partitions := collectPartitions(revoked)
	for _, p := range partitions {
		tm.closeTask(ctx, p)
	}
}

// PostBatch is the post-batch hook. For every live task it runs the retention
// sweep (windowed stores), flushes mutations via the ChangelogProducers, and
// persists the stream-time watermark. Runs AFTER processBatch and BEFORE
// produce+commit in the ALO write order.
//
// ALO caveat: WriteStreamTime uses pebble.Sync which commits to disk BEFORE
// the source offsets are committed to Kafka. On a crash between the Sync and
// the Kafka commit, the persisted stream-time may be ahead of the replayed
// source offset on restart. Windows already swept per the persisted stream-time
// may not be rebuilt from the redelivered records — acceptable under ALO.
// EOS closes this gap.
func (tm *TaskManager) PostBatch(ctx context.Context) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for partition, t := range tm.tasks {
		if t.db == nil {
			continue
		}

		// Retention sweep runs before the changelog flush so tombstones for expired
		// windows are drained and flushed in the same batch (amortized).
		if len(tm.bt.WindowStoreBindings) > 0 {
			if err := tm.runSweep(t); err != nil {
				return fmt.Errorf("TaskManager.PostBatch: partition %d: sweep: %w", partition, err)
			}
		}

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
// each store on each live task. Must be called AFTER produce+commit.
func (tm *TaskManager) WriteCheckpoints(ctx context.Context, brokers []string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for partition, t := range tm.tasks {
		for storeName, changelogTopic := range tm.allChangelogTopics() {
			hw, err := state.RestoreFromChangelog(ctx, brokers, changelogTopic, partition /*checkpointOffset=*/, int64(^uint64(0)>>1), t.db, storeName)
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

// Executor returns the executor for the given partition.
//
// For stateful topologies it returns nil if no task is assigned for that partition
// (OnAssigned must fire first). For zero-store topologies the task is created
// lazily on first access so callers can process records without the assignment lifecycle.
func (tm *TaskManager) Executor(partition int32) *topology.Executor {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if t, ok := tm.tasks[partition]; ok {
		return t.executor
	}
	if len(tm.bt.StoreBindings) == 0 && len(tm.bt.WindowStoreBindings) == 0 {
		t := &task{
			db:         nil,
			collectors: make(map[string]*state.MutationCollector),
			producers:  make(map[string]*state.ChangelogProducer),
			stores:     make(map[string]any),
			partition:  partition,
			streamTime: 0,
		}
		t.executor = topology.NewExecutor(tm.bt.Topology, t.stores)
		tm.tasks[partition] = t
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
// Zero-store topologies skip Pebble entirely; an Executor is still created.
func (tm *TaskManager) openTask(ctx context.Context, partition int32) error {
	totalStores := len(tm.bt.StoreBindings) + len(tm.bt.WindowStoreBindings)
	stores := make(map[string]any, totalStores)
	collectors := make(map[string]*state.MutationCollector, totalStores)
	producers := make(map[string]*state.ChangelogProducer, totalStores)

	if totalStores == 0 {
		t := &task{
			db:         nil,
			collectors: collectors,
			producers:  producers,
			stores:     stores,
			partition:  partition,
			streamTime: 0,
		}
		t.executor = topology.NewExecutor(tm.bt.Topology, stores)
		tm.mu.Lock()
		tm.tasks[partition] = t
		tm.mu.Unlock()
		tm.logger.Info("task opened (zero-store)",
			slog.Int("partition", int(partition)),
		)
		return nil
	}

	dbDir := filepath.Join(tm.cfg.StateDir, tm.appID, fmt.Sprintf("partition-%d", partition))
	db, err := state.OpenDB(dbDir)
	if err != nil {
		return fmt.Errorf("open pebble at %q: %w", dbDir, err)
	}

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

	// Create the task first so its streamTime field has a stable address for the Executor
	// (NewExecutorWithStreamTime stores &t.streamTime).
	t := &task{
		db:         db,
		collectors: collectors,
		producers:  producers,
		stores:     stores,
		partition:  partition,
		streamTime: streamTime,
	}

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

	if t.db == nil {
		tm.logger.Info("task closed (zero-store)", slog.Int("partition", int(partition)))
		return
	}

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

	if len(tm.bt.WindowStoreBindings) > 0 {
		if err := state.WriteStreamTime(t.db, t.streamTime); err != nil {
			tm.logger.Warn("closeTask: WriteStreamTime failed",
				slog.Int("partition", int(partition)),
				slog.Any("error", err),
			)
		}
	}

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

	for _, producer := range t.producers {
		producer.Close()
	}

	if err := t.db.Close(); err != nil {
		tm.logger.Warn("closeTask: pebble close failed",
			slog.Int("partition", int(partition)),
			slog.Any("error", err),
		)
	}

	tm.logger.Info("task closed", slog.Int("partition", int(partition)))
}

// computeSweepInterval returns the maximum MaxSizeMs across all window store bindings.
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
// Tombstone mutations end up in each store's collector and are flushed in the
// same PostBatch call, keeping the changelog consistent.
func (tm *TaskManager) runSweep(t *task) error {
	sweepInterval := tm.computeSweepInterval()
	if sweepInterval <= 0 {
		return nil
	}
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
// Each store.Delete appends a tombstone Mutation to the store's MutationCollector.
// The caller (PostBatch) drains the collector and flushes tombstones to the changelog
// so that restore correctly skips or deletes expired windows on the next assignment.
//
// sweepWindowStore is a package-level function (not a method) so it can be exercised
// in unit tests without a running Kafka broker.
func sweepWindowStore(
	store *state.KeyValueStore[[]byte, []byte],
	windowDef gstream.WindowDefinition,
	graceMs, streamTime int64,
) (int, error) {
	expiryBoundary := streamTime - windowDef.MaxSizeMs() - graceMs
	if expiryBoundary <= 0 {
		return 0, nil
	}

	// Collect expired keys first; Range slices are backed by Pebble's iterator buffer
	// and must be copied before the next iterator step invalidates them.
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

	for _, ck := range expired {
		if err := store.Delete(ck); err != nil {
			return 0, fmt.Errorf("sweepWindowStore: delete expired window key: %w", err)
		}
	}
	return len(expired), nil
}

// collectPartitions flattens a topic→[]partition map into a deduplicated slice.
// Deduplication is needed because multiple source topics may share the same partition index.
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
