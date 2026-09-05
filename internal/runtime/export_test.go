package runtime

import (
	gstream "mortz.dev/go/gstream"
	state "mortz.dev/go/gstream/internal/testutil"
	"mortz.dev/go/gstream/internal/topology"
)

// TaskManagerStoresForPartition returns the stores map for the given partition's
// task. Returns nil if no task is assigned for that partition. Exported for
// unit tests only (via the internal test package access pattern).
func TaskManagerStoresForPartition(tm *TaskManager, partition int32) map[string]any {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	t, ok := tm.tasks[partition]
	if !ok {
		return nil
	}
	return t.stores
}

// TaskManagerAllChangelogTopics calls allChangelogTopics and returns the result.
// Exported for unit tests only.
func TaskManagerAllChangelogTopics(tm *TaskManager) map[string]string {
	return tm.allChangelogTopics()
}

// TaskManagerDrainCollectorPreview drains mutations from a collector and
// returns them without re-adding. Used by tests to peek at pending mutations.
func TaskManagerDrainCollectorPreview(c *state.MutationCollector) []state.Mutation {
	return c.Drain()
}

// TaskManagerInjectTask registers a pre-built task for the given partition
// into the TaskManager. The task has:
//   - A real Pebble DB (db) for state.
//   - A single store+collector+producer wired by storeName/changelogTopic.
//   - A stub Executor (no topology needed for drain tests).
//
// Used only by unit tests that need DrainChangelogRecords without OnAssigned
// (which requires a live broker).
func TaskManagerInjectTask(
	tm *TaskManager,
	partition int32,
	storeName, changelogTopic string,
	collector *state.MutationCollector,
	backend *state.MemoryBackend,
) {
	// NewChangelogProducer requires live brokers; use the stub constructor that
	// returns a ChangelogProducer with a nil kc. Encode does not use kc, so
	// this is safe for tests that only call DrainChangelogRecords (never Flush).
	producer := newTestChangelogProducer(changelogTopic)
	store, err := backend.OpenStore(storeName, true)
	if err != nil {
		panic("TaskManagerInjectTask: " + err.Error())
	}
	for _, mutation := range collector.Drain() {
		switch mutation := mutation.(type) {
		case state.Put:
			if err := store.Put(mutation.Key, mutation.Value); err != nil {
				panic("TaskManagerInjectTask: " + err.Error())
			}
		case state.Delete:
			if err := store.Delete(mutation.Key); err != nil {
				panic("TaskManagerInjectTask: " + err.Error())
			}
		}
	}

	stores := map[string]any{storeName: store}
	t := &task{
		backend:    backend,
		executor:   topology.NewExecutor(tm.bt.Topology, stores),
		producers:  map[string]*ChangelogProducer{storeName: producer},
		stores:     stores,
		partition:  partition,
		streamTime: 0,
	}
	tm.mu.Lock()
	tm.tasks[partition] = t
	tm.mu.Unlock()
}

// newTestChangelogProducer returns a ChangelogProducer pointing at a fake
// broker address. kgo.NewClient does not dial at construction time, so this
// succeeds without a live broker. Only Encode is safe to call on the result
// (Flush will block waiting for a broker that never responds).
func newTestChangelogProducer(topic string) *ChangelogProducer {
	p, err := NewChangelogProducer([]string{"localhost:19092"}, topic)
	if err != nil {
		panic("newTestChangelogProducer: " + err.Error())
	}
	return p
}

var _ gstream.Store = (*state.MemoryStore)(nil)
