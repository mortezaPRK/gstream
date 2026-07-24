package runtime

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
