# gstream — A Kafka Streams-style Processing DSL for Go

> A stateful stream-processing library for Go built on **franz-go** (Kafka client) and
> **Pebble** (local state), exposing a type-safe, generics-based DSL (`KStream`, `KTable`,
> `GlobalKTable`) with selectable at-least-once / exactly-once guarantees.

*Document version*: 2.0 — 2026-07-17 · *Status*: Draft for review

---

## 1. Vision & Goals

Build a **production-grade** Go library that lets developers express stateful stream
topologies (`map`, `filter`, `join`, windowing, aggregations) with the ergonomics of Kafka
Streams (Java) but idiomatic Go generics and no JVM.

### Goals
- **Type-safe DSL** over Go generics: `KStream[K,V]`, `KTable[K,V]`, `GlobalKTable[K,V]`.
- **Selectable processing guarantees**: at-least-once (ALO, default) and exactly-once (EOS)
  via Kafka transactions — chosen per application, mirroring `processing.guarantee`.
- **Durable local state** in Pebble, made fault-tolerant by **changelog topics** (state is a
  materialized cache; the changelog is the source of truth).
- **Correct event-time semantics**: timestamp extraction, stream-time, windowing with grace
  periods for late/out-of-order data.
- **Operational maturity**: metrics, structured logging, clean rebalance handling, graceful
  shutdown, documented recovery behavior.

### Non-Goals (v1)
- **No SQL / KSQL layer** — DSL only.
- **No multi-instance state migration / standby replicas** (see §3; deferred to v2).
- **No cross-cluster / MirrorMaker features.**
- **No schema registry integration** in v1 (pluggable serdes instead; Avro/Protobuf/JSON
  Schema registry adapters are a follow-up).
- **Not a drop-in Kafka Streams API clone** — we borrow concepts, not signatures.

---

## 2. Scope for v1

| Decision | v1 choice | Rationale |
|----------|-----------|-----------|
| **Ambition** | Full library, built incrementally | Target real adoption; every phase ships something usable and tested. |
| **Guarantees** | ALO + EOS, selectable | Latency-sensitive users pick ALO; correctness-sensitive users pick EOS. See §4. |
| **Distribution** | **Single cooperating instance** | Still a real consumer-group member (partitions assigned via group protocol), but v1 assumes one live instance owns its assigned partitions. Multi-instance state handoff & standbys are v2. |
| **Go version** | **1.27** | Method type parameters verified working (see §6), enabling a fluent type-changing DSL. |

> **Important — single instance ≠ single partition.** One instance still processes *all* its
> assigned partitions concurrently (one task per partition). "Single instance" only means we
> defer live state migration between *instances* during rebalances.

---

## 3. Architecture Overview

| Component | Library / Tech | Responsibility |
|-----------|----------------|----------------|
| **Kafka client** | franz-go (`github.com/twmb/franz-go`) | Group membership, fetch/produce, offset commits, transactions (EOS). |
| **State store** | Root `StoreProvider` contract; Pebble and memory modules | Local KV / Window / Session stores without forcing implementation dependencies into root. |
| **Changelog** | Kafka compacted topics | Durable, replicated backing log for every state store; source of truth for recovery. |
| **DSL core** | Go 1.27 generics | `KStream`, `KTable`, `GlobalKTable`; compiled to a processor topology (DAG). |
| **Runtime** | Task scheduler (1 task ⇒ 1 partition of the source) | Owns processing threads, commit cadence, restore, rebalance callbacks. |
| **Serdes** | Pluggable `Serde[T]` | Encode/decode keys & values; JSON + primitives built in. |
| **Observability** | Prometheus + `slog` | Throughput, latency, lag, commit rate, restore progress, store size. |

### Data flow (EOS consume-transform-produce loop)
```
                    ┌─────────────────────────── Kafka ───────────────────────────┐
                    │  source topics        changelog topics       sink topics     │
                    └──────┬──────────────────────▲───────────────────▲───────────┘
                           │ fetch (ReadCommitted) │ produce (txn)     │ produce (txn)
                     ┌─────▼─────────────────────────────────────────────────┐
                     │  Task (per partition)                                  │
                     │   PollFetches → topology.process(record)               │
                     │        │            │                                  │
                     │        │       state read/write (Pebble)               │
                     │        │            │                                  │
                     │   within one txn:  changelog writes + sink writes +    │
                     │                    offset commit  →  End(TryCommit)     │
                     └────────────────────────────────────────────────────────┘
```

---

## 4. Processing Guarantees

gstream offers **two selectable guarantees**, resolving the classic latency-vs-correctness
tension explicitly rather than promising both.

### 4.1 At-least-once (ALO) — default, low latency
- Standard group consumer with manual offset commits **after** outputs are acknowledged.
- On crash, records after the last commit are reprocessed → duplicate outputs possible.
- Commit cadence tunable; this is the path targeted by the low-latency budget (§15).

### 4.2 Exactly-once (EOS) — opt-in, correctness first
- Uses franz-go's transactional session so that **sink writes + changelog writes + consumed
  offsets** commit **atomically**.
- Correct franz-go surface (the original PRD's `GTransactionID` does not exist):
  ```go
  sess, err := kgo.NewGroupTransactSession(
      kgo.TransactionalID("gstream-"+appID),      // stable per task/instance
      kgo.FetchIsolationLevel(kgo.ReadCommitted()),
      kgo.ConsumerGroup(appID),
      kgo.ConsumeTopics(sources...),
      kgo.RequireStableFetchOffsets(),
  )
  // per batch:
  sess.Begin()
  // ... produce sink + changelog records via the session's client ...
  committed, err := sess.End(ctx, kgo.TryCommit) // atomically commits records + offsets
  ```
- **Local state + EOS = the changelog pattern.** Pebble's WAL provides *local* durability only;
  it does **not** make state atomic with Kafka offsets. Correctness comes from: every state
  mutation is also written to a Kafka changelog **inside the same transaction** as the offset
  commit. Pebble is a fast local materialization that is rebuilt from the changelog on recovery.
  *(This corrects the original "atomic writes via Pebble WAL" claim.)*

> **Latency implication (was a contradiction in v1.0):** Kafka transactions commit on an
> interval (typically ~100 ms), so **EOS cannot meet a 5 ms p99**. The 5 ms budget applies to
> **ALO only**. EOS gets an honest, separate target tied to the commit interval (§15).

---

## 5. State Management

### 5.1 Store types
| Store | Interface (sketch) | Backing |
|-------|--------------------|---------|
| **KeyValueStore[K,V]** | `Get/Put/Delete/Range` | Pebble keyspace, prefixed per store. |
| **WindowStore[K,V]** | keyed by `(key, windowStart)` | Composite Pebble keys: `store‖key‖bigEndian(ts)`. |
| **SessionStore[K,V]** | keyed by `(key, start, end)` | Merge-able session bounds. |

### 5.2 TTL / retention — **application-layer, not a Pebble feature**
> The original PRD's "Pebble DB with TTL options" is inaccurate — **Pebble has no built-in
> TTL.** Retention is implemented by us:
- Window/session keys are **timestamp-suffixed**; expiry = stream-time − (window size + grace).
- Reclamation via a periodic sweeper using **`Batch.DeleteRange`** over expired key ranges
  (efficient LSM range tombstones), plus tombstones propagated to the compacted changelog.

### 5.3 Durability & recovery
- **Changelog topic per store** (compacted; window/session stores use compact+delete with
  retention ≥ store retention).
- **Local checkpoint**: periodically persist the changelog offset that local Pebble state has
  applied up to. On restart, restore only `[checkpoint, high-watermark)` rather than the whole
  log.
- **Restore path**: on task assignment, replay the changelog from the checkpoint into Pebble
  before processing resumes for that task.

---

## 6. DSL API & the Generics Model

### 6.1 Method type parameters — **verified available in Go 1.27**
This was historically the biggest constraint (Go ≤ 1.26 disallowed methods with their own type
parameters). Confirmed by compiling against the stable local toolchain (`go1.27.0`):
```go
type Stream[K, V any] struct{}
func (s Stream[K, V]) Map[K2, V2 any](fn func(K, V) (K2, V2)) Stream[K2, V2] { /* ... */ }
// s.Map(func(k, v string) (string, int) { return k, len(v) })  → compiles; infers K2=string,V2=int
```
**Consequence:** a **fluent, fully type-safe** chain is viable — no need to fall back to
package-level functions or erase to `any` for type-changing operators.

### 6.2 API sketch (revised to valid Go 1.27)
```go
// Type-changing operators as methods with their own type params:
KStream[K, V].Map[K2, V2 any](func(K, V) (K2, V2))              -> KStream[K2, V2]
KStream[K, V].Filter(func(K, V) bool)                           -> KStream[K, V]
KStream[K, V].SelectKey[K2 any](func(K, V) K2)                  -> KStream[K2, V]  // marks repartition
KStream[K, V].GroupByKey()                                      -> KGroupedStream[K, V]

// Windowing + aggregation:
grouped.WindowedBy(gstream.Tumbling(5 * time.Minute))           -> TimeWindowedStream[K, V]
windowed.Count("counts", jsonserde.Serde[int64]{})             -> KTable[gstream.Windowed[K], int64]
grouped.Aggregate[A any](initFn, aggFn)                         -> KTable[K, A]

// Joins (co-partitioning enforced/inserted — see §8):
left.Join[VR, VO any](right KStream[K, VR], joiner func(V, VR) VO,
    window gstream.JoinWindows)                                 -> KStream[K, VO]
```

### 6.3 Topology, not just chaining
The DSL **builds a topology (processor DAG)**; it does not process eagerly. `Build()` returns an
immutable topology that the runtime instantiates as one **task per source partition**.
Key-changing operators (`SelectKey`, `Map` on key, `GroupBy`) mark a **repartition boundary**;
the builder inserts an internal repartition topic so downstream co-partitioning holds (§8).

---

## 7. Execution & Parallelism Model

> The original "GoroutinePool (16 workers), autoscaling up to 32" mismodels stream parallelism.
> **The unit of parallelism is the partition (task), not an arbitrary worker count.** A single
> partition cannot be split across workers without breaking per-key ordering and EOS.

- **1 task = 1 partition** of the source topic(s). Tasks are independent (own state shards,
  own offsets).
- Concurrency = **min(assigned partitions, configured task-thread count)**. There is no benefit
  to more threads than assigned partitions.
- **Backpressure** is natural: a task pulls (`PollFetches`) only when ready; slow downstreams
  slow the pull.
- **"Autoscaling"** in Kafka-land = adding *instances* (or partitions). For a single-instance
  v1, we expose the task-thread count as config and document that horizontal scaling arrives
  with multi-instance support (v2).

---

## 8. Time & Windowing

> The original "synchronized wall clocks / monotonic timestamps" risk is **backwards** for
> stream processing. Correct design is **event-time**, not wall-clock synchronization.

- **Timestamp extractor**: pluggable; default uses the Kafka record timestamp. Users may extract
  from the payload.
- **Stream-time**: per-task max observed event timestamp; drives window advancement and expiry
  (monotonic *per task*, not a wall clock).
- **Grace period**: late records within grace update their window; beyond grace they're dropped
  (and counted in a `late_records` metric).
- **Window types**: **Tumbling**, **Hopping/Sliding** (fixed windows, backed by `WindowStore`),
  **Session** (activity-gap based, backed by `SessionStore`).

---

## 9. Joins

| Join | Requirement | Mechanism |
|------|-------------|-----------|
| **Stream-Stream** | Co-partitioned; windowed | Both sides buffered in `WindowStore`; emit on match within `JoinWindows`. |
| **Stream-Table** | Co-partitioned | Stream lookup against the table's local materialization. |
| **Stream-GlobalTable** | Not co-partitioned | `GlobalKTable` fully replicated on every instance; keyed by a mapper — no repartition needed. |

- **Co-partitioning is enforced**: same partition count + same key & partitioner. When a
  key-changing operator precedes a join/aggregation, the builder **auto-inserts a repartition
  topic** (this was missing from v1.0).

---

## 10. Serialization

Users implement a single generic interface; **T is the value type** (the generic parameter is
the domain type, not `[]byte`). gstream ships **two** implementations: JSON and Protobuf.

```go
// The interface consumers implement (or use a built-in).
type Serde[T any] interface {
    Serialize(T) ([]byte, error)
    Deserialize([]byte) (T, error)
}
```

### 10.1 JSON serde — parameterized by the value type
```go
type JSONSerde[T any] struct{}
func (JSONSerde[T]) Serialize(v T) ([]byte, error)   { return json.Marshal(v) }
func (JSONSerde[T]) Deserialize(b []byte) (T, error) { var v T; err := json.Unmarshal(b, &v); return v, err }

// usage: jsonserde.Serde[Order]{}  ->  gstream.Serde[Order]
```

### 10.2 Protobuf serde — generated message pointer
Protobuf needs to allocate and mutate a message, so `T` is a generated message pointer that
satisfies `proto.Message`:
```go
protoserde.Serde[*pb.Order]{}  // implements gstream.Serde[*pb.Order]

type ProtoSerde[T any, PT ProtoMessage[T]] struct{}
func (ProtoSerde[T, PT]) Serialize(v T) ([]byte, error)   { return proto.Marshal(PT(&v)) }
func (ProtoSerde[T, PT]) Deserialize(b []byte) (T, error) { var v T; err := proto.Unmarshal(b, PT(&v)); return v, err }

// usage: gstream.ProtoSerde[pb.Order, *pb.Order]{}  ->  Serde[pb.Order]
```

- Both built-ins expose the **domain type** as the generic parameter, per the design decision.
- Serdes are attached at source/sink/store creation; a store's **changelog reuses that store's
  serde**, so encoding is consistent between local Pebble and the changelog.
- Avro / JSON-Schema / schema-registry adapters remain a post-v1 follow-up; they implement the
  same `Serde[T]` interface.

---

## 11. Fault Tolerance & Recovery

- **Rebalance callbacks** (`OnPartitionsAssigned` / `Revoked`): on revoke, flush + commit +
  checkpoint and **close** the affected task's store cleanly; on assign, **restore** from
  changelog checkpoint before processing.
- **Crash recovery**: replay `[local checkpoint, high-watermark)` of each changelog into Pebble.
- **Deferred to v2**: **standby replicas** (warm state on other instances) and live
  cross-instance state handoff — these, not "100 GB in 10 s", are how real recovery time is
  bounded. See §15 for the honest v1 recovery target.

---

## 12. Observability

- **Metrics (Prometheus)**: records/sec in/out, processing latency histogram, **consumer lag**,
  commit rate & duration, **restore progress/duration**, late/dropped records, store size on
  disk, Pebble cache hit rate, transaction abort rate.
- **Logging**: `slog`, structured, per-task correlation fields.

*Interactive queries (read-only access to materialized `KTable` state) are **out of scope** —
state is observed via metrics and the changelog, not a query API.*

---

## 13. Configuration

**Decision: franz-go stays hidden; state storage uses root contracts.**
Users never see `kgo.*` types. Stateful applications select a separate store module through
`StoreProvider`, keeping Pebble and other implementation dependencies out of root.

```go
type Guarantee int
const ( AtLeastOnce Guarantee = iota; ExactlyOnce )

type Config struct {
    ApplicationID string          // → consumer group id + TransactionalID prefix (required)
    Brokers       []string        // required
    Guarantee     Guarantee       // default AtLeastOnce
    StateDir      string          // local-state root; default OS temp-derived path
    StoreProvider StoreProvider   // required for stateful topologies
    NumTaskThreads int            // default = GOMAXPROCS; capped at assigned partitions (§7)
    CommitInterval time.Duration  // default 100ms (also EOS txn boundary)
    // ... curated, documented knobs only — no raw kgo.Opt exposed.
}
```

- **Escape hatch, later**: if power users need raw franz-go/Pebble tuning, we add a clearly
  labeled `Advanced` sub-struct (or functional options) in a later version — deliberately not v1,
  to keep the contract small.
- **Validation**: `Config.validate()` rejects impossible combinations (e.g. empty `ApplicationID`,
  EOS with a caller-supplied non-read-committed setting — which isn't even reachable since we
  own it).

---

## 14. Consumer Group & Internal Topics

**Assignor — decision: cooperative-sticky (incremental rebalancing) is the v1 default.**
- Chosen over eager assignment to minimize stop-the-world rebalances and unnecessary state
  restore: partitions not being moved keep processing during a rebalance.
- franz-go's cooperative balancer is configured internally (hidden per §13).

**Internal topics — decision: gstream auto-creates and manages them.**
- **Changelog topics** (per state store) and **repartition topics** (per repartition boundary,
  §6.3/§9) are created automatically with a deterministic naming convention:
  `<ApplicationID>-<storeOrNode>-changelog` / `-repartition`.
- gstream sets the correct configs: changelogs are **compacted** (window/session stores use
  `compact,delete` with retention ≥ store retention); repartition topics use short retention.
- Auto-creation requires topic-management ACLs; if creation is denied, gstream **fails fast at
  startup** with a clear error listing the expected topics + configs, so operators can
  pre-provision instead.
- Partition counts of internal topics are matched to the source for co-partitioning; a mismatch
  on a pre-existing topic is a startup error.

---

## 15. Performance Targets

> Targets are **per-configuration and to be measured**, not contractual across all workloads.
> Throughput depends heavily on message size, guarantee level, and hardware. Benchmarks
> (§16) must state message size, partition count, guarantee mode, and instance specs.

| Metric | ALO target | EOS target | Notes |
|--------|-----------|------------|-------|
| **Throughput** | ≥ 1M msg/s (small msgs, batched, multi-partition) | Lower; report measured | Per instance, saturating assigned partitions. |
| **p99 processing latency** | ≤ 5 ms (stateless/light-state) | ≈ commit interval + processing (e.g. ~100–150 ms) | EOS bounded by txn commit cadence — **not 5 ms**. |
| **State store read/write** | p99 ≤ ~50 µs cached / bounded by Pebble on cache miss | same | Original "≤20 µs" is optimistic once disk I/O is involved; measure. |
| **Memory** | ≤ ~500 MB/core budget (dominated by Pebble block cache) | same | Tunable via cache size; treat as a budget knob, not a hard invariant. |
| **Recovery** | Proportional to changelog bytes since checkpoint | same | With per-store checkpoints, restore only the tail. **No 10 s/100 GB claim** — v2 standbys address warm recovery. |

---

## 16. Testing Strategy

- **Topology test driver**: drive a built topology with synthetic records, assert outputs and
  store contents **without a broker** (the single most important test tool; mirrors Kafka
  Streams' `TopologyTestDriver`).
- **State store unit tests**: KV/window/session semantics, TTL sweep, range correctness.
- **Integration tests**: real Kafka + Pebble via the isolated `integration/kafka` Testcontainers module; group membership, restore,
  and **internal-topic auto-creation** (§14).
- **EOS correctness**: fault-injection (kill mid-transaction) → assert no duplicate/lost output
  on the read-committed side.
- **Property/fuzz tests**: serde round-trips (JSON + proto); windowing under shuffled/late timestamps.
- **Benchmarks**: throughput/latency harness with documented workload parameters (§15).

---

## 17. Development Roadmap

| Phase | Milestones | Exit criteria |
|-------|------------|---------------|
| **P0 — Stateless E2E** | franz-go group consumer/producer wiring (hidden behind `Config`); `KStream` source→`filter`/`map`→sink; graceful shutdown; ALO commits. | Records flow consume→transform→produce against a real broker; offsets committed after output. |
| **P1 — DSL & topology core** | Topology builder/DAG; type-safe operators via 1.27 method type params; `Serde[T]` + JSON/proto impls; repartition-boundary detection. | Topology test driver runs a multi-operator pipeline with no broker. |
| **P2 — State layer** | Pebble KV store; changelog write + restore; local checkpointing; TTL sweeper; internal-topic auto-management. | `Count`/`Aggregate` materialize to Pebble and **restore correctly after restart**. |
| **P3 — Time & windowing** | Timestamp extractor, stream-time, tumbling/hopping/session windows, grace handling. | Windowed count correct under out-of-order + late data (within/beyond grace). |
| **P4 — Joins** | Stream-Stream (windowed), Stream-Table, GlobalKTable; auto-repartition; cooperative-sticky assignor. | Co-partitioning enforced; join tests pass in topology driver + integration. |
| **P5 — EOS** | Transactional session; atomic changelog+sink+offset commit; abort/retry handling. | Fault-injection shows no duplicates on read-committed consumers. |
| **P6 — Production hardening** | Metrics, structured logging, rebalance robustness, docs, examples, CI/CD, benchmarks. | Green CI, published docs + runnable examples, benchmark report with stated params. |

*(Durations intentionally omitted pending capacity; sequencing is the commitment.)*

---

## 18. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Pebble write amplification (changelog + local writes) | zstd compression; tune compaction; monitor WAL/size; batch writes per poll. |
| EOS latency vs. throughput | Tune commit interval; document the trade-off; keep ALO as the low-latency default. |
| Rebalance storms / state thrash | **Cooperative-sticky assignor** (§14); checkpoint on revoke to shrink restore. |
| Late/out-of-order data | Event-time + grace periods; explicit late-record metric; document dropping policy. |
| Repartition correctness (key-changing ops) | Builder auto-inserts repartition topics; co-partition assertions in tests. |
| Unbounded window/session state | Retention-driven `DeleteRange` sweeper tied to stream-time; changelog tombstones. |
| Transaction hangs / zombie producers | Stable `TransactionalID` per task; fencing via epoch; bounded txn timeout + retry. |
| Internal-topic auto-creation denied by ACLs | **Fail fast at startup** listing expected topics + configs (§14) so operators can pre-provision. |
| Hidden franz-go/Pebble limits power users | Curated `Config` for v1; documented `Advanced` escape hatch planned post-v1 (§13). |
| Single-instance limitation surprises users | Document clearly; expose lag/restore metrics; scope multi-instance to v2. |

---

## 19. Resolved Decisions & Remaining Questions

**Resolved:**
1. **Assignor** → **cooperative-sticky** (incremental) is the v1 default (§14).
2. **Internal topics** → gstream **auto-creates and manages** changelog + repartition topics; fails fast if ACLs deny creation (§14).
3. **Interactive queries** → **out of scope** (§12).
4. **Serdes** → user-implemented `Serde[T]` interface; **two built-ins shipped: JSON and Protobuf**, both parameterized by the domain value type (§10).
5. **Config surface** → curated **`Config`**; **franz-go and Pebble are hidden** in v1 (§13).
6. **Guarantees** → tiered **ALO (default) + EOS (opt-in)** (§4).
7. **Distribution** → **single cooperating instance** for v1 (§2).

**Still open:**
- **Standby replicas timing** — deferral to v2 assumed; confirm acceptable given the production-adoption goal.
- **Advanced escape hatch** — when (not if) to expose raw franz-go/Pebble tuning post-v1, and in what form (sub-struct vs. functional options).
- **Repartition topic naming** — confirm the `<ApplicationID>-<node>-{changelog,repartition}` convention is collision-safe for your topic-naming policies.

---

## 20. Immediate Next Steps

1. **Repo scaffolding**: root contracts plus separate `serdes/*`, `stores/*`, `loggers/*`, and `integration/kafka` modules; `internal/runtime` and `internal/kafka` keep franz-go runtime behavior; CI verifies every module independently.
2. **P0 spike**: hidden franz-go group consumer + producer behind `Config`; minimal `KStream` source→filter→sink with ALO commit against a Testcontainers Kafka broker.
3. **Topology test driver skeleton** (unblocks all later phases with broker-free testing).
4. **Serde implementations**: `serdes/json`, `serdes/bytes`, and `serdes/proto` modules with round-trip tests.
5. **Pebble KV wrapper** with a `KeyValueStore[K,V]` interface + serde plumbing (no TTL yet).

---

### Appendix — Key corrections from v1.0
- ❌ `GTransactionID` → ✅ `kgo.TransactionalID` + `GroupTransactSession` / `FetchIsolationLevel(ReadCommitted)`.
- ❌ "Exactly-once via Pebble WAL" → ✅ EOS via Kafka transactions + **changelog pattern**; Pebble is a materialized cache.
- ❌ "Pebble TTL options" → ✅ **App-layer** retention via timestamped keys + `DeleteRange` sweeper.
- ❌ "Synchronized wall clocks / monotonic timestamps" → ✅ **Event-time** + stream-time + grace periods.
- ❌ EOS *and* 5 ms p99 → ✅ **Tiered**: 5 ms is ALO-only; EOS bounded by commit interval.
- ❌ "GoroutinePool 16→32, autoscale" → ✅ **1 task per partition**; parallelism bounded by partitions/instances.
- ❌ "Recover 100 GB in 10 s" → ✅ Checkpoint + tail-restore now; **standby replicas** for warm recovery in v2.
- ➕ Added: topology/DAG model, repartitioning, serdes (JSON + proto), rebalance handling, topology test driver, curated `Config`, auto-managed internal topics.
- ✅ Verified: **Go 1.27 method type parameters compile**, enabling the fluent type-safe DSL.
- ✅ Verified: **generic `Serde[T]` with JSON + Protobuf built-ins** compiles and round-trips on Go 1.27 (proto needs the `[T, PT proto.Message]` two-param form).
