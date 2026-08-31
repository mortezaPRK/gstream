// Package pebble provides Pebble-backed local state stores for stateful stream
// processing (§5). Runtime storage behavior and Pebble configuration are
// confined here; broker-free callers can use store/memory instead.
//
// Responsibilities:
//   - KeyValueStore[K,V], WindowStore[K,V], and SessionStore[K,V] backed by
//     Pebble LSM (§5.1). Keys are binary-encoded with a per-store prefix to share
//     one Pebble instance per task.
//   - Application-layer TTL/retention: timestamp-suffixed window keys reclaimed via
//     Batch.DeleteRange sweepers tied to stream-time, with tombstones propagated to
//     the changelog (§5.2).
//   - Local checkpointing: tracking the changelog offset up to which Pebble state
//     has been applied, so recovery only replays the tail (§5.3).
//   - Changelog restore: replaying a changelog topic range into Pebble on task
//     assignment before processing resumes (§5.3, §11).
package pebble
