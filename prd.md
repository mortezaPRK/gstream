# Kafka Stream DSL Implementation Plan (Markdown)

## 1. Vision
Producing a scalable, exactly‑once Kafka Streams DSL for Go that:
- Uses **Franz‑Go** as the Kafka client library.
- Uses **Pebble** for state store persistence.
- Supports functional DSL patterns (`map`, `filter`, `join`, windowing, aggregations).
- Targets high‑throughput use cases (log processing, stream analytics).

---

## 2. Architecture Overview
| Component | Library / Technology | Responsibility |
|-----------|--------------------|----------------|
| **Kafka Client** | Franz‑Go | Producer/Consumer with exactly‑once semantics (transaction IDs, offset commits). |
| **State Store** | Pebble | KeyValueStore, WindowStore, SessionStore backed by Pebble LSM engine. |
| **DSL Core** | Go generics | `KStream[K,V]`, `KTable[K,V]`, `GlobalKTable[K,V]`. |
| **Worker Pool** | GoroutinePool (16 workers) | Parallel processing of partitions, autoscaling trigger. |
| **Metrics** | Prometheus / /metrics endpoint | Throughput, latency, error rates, memory usage. |

---

## 3. Key Requirements
| Requirement | Implementation Details |
|-------------|------------------------|
| **Exactly‑once** | Franz‑Go `GTransactionID` + atomic writes via Pebble WAL. |
| **State Management** | Pebble DB with TTL options, changelog topic integration. |
| **Windowing** | Tumbling, sliding, session windows via Pebble TTL & session store. |
| **Joins** | Stream‑Stream (co‑partitioned), Stream‑Table (broadcast/global). |
| **Performance** | ≥1 M msg/sec throughput, ≤5 ms p99 latency, ≤500 MB/core memory. |
| **Scalability** | Partition sharding + GoroutinePool autoscaling (up to 32 workers). |

---

## 4. DSL API Sketch
```go
stream.
  Map(func(k, v string) (string, int) { return k, len(v) })
  .WindowedBy(TumblingWindow{5 * time.Minute})
  .Count()                     // → KTable[string, int]
```

```go
// Join example (co‑partitioned)
left.Join(right, func(a, b KV) KV { return KV{Key: a.Key, Value: a.Value + b.Value} })
```

```go
// Aggregation example
stream.ReduceByCount()   // Count per key with stateful reduction
```

---

## 5. Development Roadmap

| Phase | Duration | Milestones |
|-------|----------|------------|
| **Prototype** | Weeks 1‑3 | Franz‑Go consumer/producer wiring, single‑topic integration. |
| **State Layer** | Weeks 4‑6 | Pebble KV/TTL/Sstore wrappers, changelog topic coordination. |
| **Core DSL** | Weeks 7‑9 | `KStream`, `KTable`, `GlobalKTable` abstractions; windowing & joins. |
| **Worker Pool** | Weeks 10‑11 | GoroutinePool, autoscaling logic, backpressure handling. |
| **Testing & Benchmarking** | Weeks 12‑13 | Load‑gen/YCSB tests, latency/throughput benchmarking. |
| **Production Harden** | Weeks 14‑15 | Metrics, bulletproof recovery, documentation, CI/CD pipeline. |

---

## 6. Risks & Mitigations
| Risk | Mitigation |
|------|------------|
| Pebble write amplification | Use zstd compression, monitor WAL size, adjust compaction parameters. |
| Consumer group state drift | Use mutex around epoch commits; validate offsets on restart. |
| Window skew | Require synchronized wall clocks; use monotonic timestamps for windows. |
| Statelessness during recovery | Persist changelog topics; ensure snapshots are crash‑consistent. |
| Goroutine pool over‑scale | Implement latency‑based throttling; cap max workers at 32. |

---

## 7. Benchmarks (Pre‑liminary)
| Metric | Value (target) |
|--------|----------------|
| **Throughput** | 1–2 M msg/sec (p99 ≤ 5 ms) |
| **State Store Latency** | ≤ 20 µs read/write |
| **Memory Usage** | ≤ 500 MB/core (steady state) |
| **Recovery Time** | ≤ 10 s for 100 GB snapshot |

---

## 8. Acceptance Criteria
- Exactly‑once processing verified end‑to‑end on a test stream of 10 M+ messages/hour.  
- All core DSL operations (`map`, `filter`, `join`, `window`, `aggregate`) behave as expected.  
- State recovery within 10 seconds of a simulated failure.  
- Benchmarks meet or exceed the target throughput/latency numbers above.

---

## 9. Next Immediate Tasks
1. **File Setup**: Create `kafka_stream_dsl_plan.md` in the session workspace (already saved).  
2. **Prototype Kafka Consumer**: Implement a simple Franz‑Go consumer that can join a consumer group and track offsets.  
3. **Pebble KV Store Wrapper**: Build a thin Go interface (`KeyValueStore`) backed by Pebble, include TTL support.  
4. **Commit Initial State**: Save all MVP code to `/tmp/memory/kafka_stream_dsl_workspace` for future iteration.

--- 

*Document version*: 1.0 – 2026‑07‑17  
*Prepared by*: Claude Code (generated)