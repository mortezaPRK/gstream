// Package topology implements the internal processor DAG that backs the gstream DSL (§6.3)
// and the broker-free TestDriver for deterministic testing (§16).
//
// # Processor model (§6.3)
//
// The DSL (KStream, KTable, GlobalKTable) does not process eagerly; instead it
// compiles into an immutable Topology — a directed acyclic graph (DAG) of processor
// nodes. Build() seals the Topology; the runtime or TestDriver then drives it.
// One Topology description is shared across all tasks; each task owns its own
// mutable state separately (§7 — 1 task = 1 partition).
//
// # Internal record representation
//
// Inside the DAG all data flows as Record{Key, Value any; Timestamp int64}.
// The type-safe generic KStream[K,V] DSL layer (planned; not in this package)
// encodes/decodes to this internal type at source and sink boundaries, keeping
// the processor core generic and reusable across all type combinations.
//
// # Node types
//
//   - Source: entry point; receives injected records from Kafka (runtime) or
//     TestDriver.PipeInput (tests).
//   - Processor: transforms or filters; forwards zero or more records downstream.
//     Concrete examples: Filter (gate by predicate), Mapper (1-to-1 transform).
//   - Sink: terminal; captures records to Kafka (runtime) or TestDriver's buffer.
//
// Key-changing operators (SelectKey, type-changing Map, GroupBy) will mark a
// repartition boundary in later phases; the builder will auto-insert a repartition
// node so downstream co-partitioning invariants hold (§6.3, §9). Reserved for P1+.
//
// # Topology test driver (§16)
//
// TestDriver is the primary unit-testing tool for all DSL operators. It drives a
// sealed Topology synchronously and deterministically with synthetic Records —
// no Kafka broker, no Pebble store required. This mirrors Kafka Streams'
// TopologyTestDriver:
//
//	builder := topology.NewBuilder()
//	src  := builder.AddSource("my-source")
//	filt := builder.AddProcessor("filter", topology.Filter(fn), src)
//	mapp := builder.AddProcessor("map",    topology.Mapper(fn), filt)
//	builder.AddSink("my-sink", mapp)
//	topo := builder.Build()
//
//	d := topology.NewTestDriver(topo)
//	d.PipeInput("my-source", topology.Record{Key: "k", Value: "hello", Timestamp: 1})
//	out, _ := d.ReadOutput("my-sink")
//
// See §16 of prd.md for the full testing strategy.
package topology
