// Package runtime is the task scheduler and execution engine for gstream (§7, §11).
//
// # P1 — Stateless E2E wiring with BuiltTopology (§15, §20)
//
// The current implementation provides [Adapter], which bridges the stateless
// consume-transform-produce pipeline:
//
//	InRecord (bytes) → decode → topology DAG → encode → []OutRecord (bytes)
//
// Adapter is constructed from a *gstream.BuiltTopology and delegates key/value
// decode and encode to per-source SourceBinding and per-sink SinkBinding closures
// captured at DSL build time. This correctly handles type-changing operators such as
// Map and SelectKey (the P0 Adapter[V] single-serde silent-drop bug is fixed here).
//
// Adapter uses [topology.TestDriver] for synchronous, depth-first DAG traversal
// — the same execution model that topology unit tests rely on.  For the P1 ALO
// path this is correct: kafka.Client calls the ProcessFunc once per record from a
// single goroutine, so per-record synchronous dispatch is safe and deterministic.
//
// # Full runtime responsibilities (P2+)
//
//   - Task lifecycle: one task per source partition; tasks own their Pebble state
//     shard and their committed offset position (§7).
//   - Thread pool: runs min(assigned partitions, NumTaskThreads) goroutines; wires
//     backpressure through PollFetches so slow downstreams naturally throttle the
//     source pull (§7).
//   - Commit cadence: drives periodic offset commits (ALO §4.1) or Kafka transaction
//     boundaries (EOS §4.2) at the configured CommitInterval (§13).
//   - Rebalance integration: on partition revoke, flushes in-flight state,
//     checkpoints, and closes the task cleanly; on assignment, triggers changelog
//     restore before resuming processing (§11).
//   - Graceful shutdown: drains in-flight records, commits pending offsets, and
//     closes all stores and Kafka clients in the correct order.
package runtime
