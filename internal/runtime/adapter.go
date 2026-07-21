package runtime

import (
	"context"
	"fmt"
	"log/slog"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/kafka"
	"github.com/mortezaPRK/gstream/internal/topology"
)

// Adapter bridges the kafka.ProcessFunc protocol and a *gstream.BuiltTopology.
//
// # Stateless path (bt.StoreBindings is empty)
//
// Behaviour is identical to P1:
//
//  1. Incoming kafka.InRecord key/value bytes are decoded using the source
//     SourceBinding's DecodeKey and DecodeVal closures.
//  2. A topology.Record is injected into the single source node via TestDriver.
//  3. The synchronous, depth-first DAG traversal drives every processor and sink.
//  4. Sink outputs are drained; each record's key/value is encoded using the sink's
//     SinkBinding.EncodeKey and SinkBinding.EncodeVal closures, and the results are
//     returned as []kafka.OutRecord directed at the configured Kafka topics.
//
// # Stateful path (bt.StoreBindings is non-empty)
//
// A [TaskManager] is created and manages per-partition state. Each InRecord is
// routed to the Executor owned by its source partition. The lifecycle callbacks
// (OnAssigned/OnRevoked) and post-batch hook (PostBatch) must be wired into the
// kafka.Client via [Adapter.LifecycleCallbacks] and [Adapter.PostBatchHook].
//
// # ALO write order (stateful)
//
//	process(record→store+collector) → PostBatch(flush changelog) →
//	produce sinks → commit source offsets
//
// ALO caveat: a crash between flush and commit leaves the changelog ahead of
// the committed source offset. On restart the batch is redelivered and aggFn
// is applied again (at-least-once). ExactlyOnce (P5) closes this window.
//
// # Thread safety
//
// Adapter is NOT safe for concurrent use. kafka.Client calls ProcessFunc for
// each record sequentially from a single goroutine (P1 contract). A future
// multi-threaded task model (P4+) will instantiate one Adapter per thread.
type Adapter struct {
	driver      *topology.TestDriver // non-nil only in stateless mode
	taskManager *TaskManager         // non-nil only in stateful mode
	bt          *gstream.BuiltTopology
	logger      *slog.Logger
	sourceName  string // P1: exactly one source
}

// NewAdapter constructs an Adapter driven by a *gstream.BuiltTopology.
//
//   - bt must not be nil.
//   - bt.Topology must have exactly one source node (P1 constraint; multi-source
//     support is P4+).
//   - Every source in bt.Topology.SourceNames() must have a matching entry in
//     bt.Sources (error if missing).
//   - Every sink declared in bt.Topology.SinkNames() must have a matching entry in
//     bt.Sinks (error if missing).
//   - logger may be nil (falls back to slog.Default()).
//
// For stateful topologies (bt.StoreBindings non-empty) a TaskManager is created
// internally. The caller must wire [Adapter.LifecycleCallbacks] and
// [Adapter.PostBatchHook] into the kafka.Client before calling Run:
//
//	client, _ := kafka.New(cfg, topics, logger,
//	    kafka.WithLifecycle(adapter.LifecycleCallbacks()),
//	    kafka.WithPostBatch(adapter.PostBatchHook()),
//	)
//
// The returned Adapter is wired and ready; call ProcessFunc to obtain a
// kafka.ProcessFunc that can be passed directly to kafka.Client.Run.
func NewAdapter(bt *gstream.BuiltTopology, logger *slog.Logger) (*Adapter, error) {
	return NewAdapterWithConfig(bt, gstream.Config{}, logger)
}

// NewAdapterWithConfig constructs an Adapter with a full gstream.Config (required
// for the stateful path where cfg supplies StateDir, Brokers, and ApplicationID).
// For stateless topologies cfg may be the zero value.
func NewAdapterWithConfig(bt *gstream.BuiltTopology, cfg gstream.Config, logger *slog.Logger) (*Adapter, error) {
	if bt == nil {
		return nil, fmt.Errorf("runtime.NewAdapter: bt must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	// P1 constraint: exactly one source.
	srcs := bt.Topology.SourceNames()
	if len(srcs) != 1 {
		return nil, fmt.Errorf(
			"runtime.NewAdapter: P1 requires exactly one source node, got %d: %v",
			len(srcs), srcs,
		)
	}

	sourceName := srcs[0]

	// Validate that the single source has a binding in bt.Sources.
	if _, ok := bt.Sources[sourceName]; !ok {
		return nil, fmt.Errorf(
			"runtime.NewAdapter: source %q has no entry in bt.Sources",
			sourceName,
		)
	}

	// Validate that every sink declared in the topology has a binding in bt.Sinks.
	// Exception: internal ktable-out sinks (no entry in bt.Sinks) are skipped; they
	// are terminal sinks that the stateful processor never forwards to.
	for _, sinkName := range bt.Topology.SinkNames() {
		if _, ok := bt.Sinks[sinkName]; !ok {
			// Check whether it is a known internal sink (no Kafka mapping needed).
			// Internal sinks are registered in the topology but absent from bt.Sinks.
			// For now we only skip if StoreBindings is non-empty (stateful topology
			// has ktable-out-N internal sinks). For stateless topologies all sinks
			// must have bindings.
			if len(bt.StoreBindings) == 0 {
				return nil, fmt.Errorf(
					"runtime.NewAdapter: sink %q has no entry in bt.Sinks (provide a SinkBinding with Topic, EncodeKey, and EncodeVal)",
					sinkName,
				)
			}
			// Stateful: skip internal sinks (e.g. ktable-out-N) silently.
		}
	}

	a := &Adapter{
		bt:         bt,
		logger:     logger,
		sourceName: sourceName,
	}

	if len(bt.StoreBindings) > 0 {
		// Stateful path: create a TaskManager to manage per-partition state.
		a.taskManager = NewTaskManager(bt, cfg, logger)
	} else {
		// Stateless path: single shared TestDriver (unchanged from P1).
		a.driver = topology.NewTestDriver(bt.Topology)
	}

	return a, nil
}

// LifecycleCallbacks returns the onAssigned and onRevoked callbacks for wiring
// into [kafka.WithLifecycle]. Returns (nil, nil) for stateless topologies.
func (a *Adapter) LifecycleCallbacks() (
	onAssigned func(ctx context.Context, assigned map[string][]int32) error,
	onRevoked func(ctx context.Context, revoked map[string][]int32),
) {
	if a.taskManager == nil {
		return nil, nil
	}
	return a.taskManager.OnAssigned, a.taskManager.OnRevoked
}

// PostBatchHook returns the post-batch function for wiring into
// [kafka.WithPostBatch]. Returns nil for stateless topologies.
func (a *Adapter) PostBatchHook() func(ctx context.Context) error {
	if a.taskManager == nil {
		return nil
	}
	return a.taskManager.PostBatch
}

// ProcessFunc returns a kafka.ProcessFunc that can be passed to kafka.Client.Run.
//
// The returned function implements the full decode → topology → encode pipeline and
// satisfies the ALO commit contract: on success it returns all output records; on any
// error it returns nil so the kafka.Client skips produce and commit (whole-batch
// redelivery, §4.1).
func (a *Adapter) ProcessFunc() kafka.ProcessFunc {
	return func(ctx context.Context, in kafka.InRecord) ([]kafka.OutRecord, error) {
		return a.process(ctx, in)
	}
}

// process is the internal implementation of the per-record pipeline (§15, §7).
func (a *Adapter) process(_ context.Context, in kafka.InRecord) ([]kafka.OutRecord, error) {
	// Step 1: decode the key and value using the source binding.
	binding := a.bt.Sources[a.sourceName]

	key, err := binding.DecodeKey(in.Key)
	if err != nil {
		a.logger.Error("failed to decode record key",
			slog.String("topic", in.Topic),
			slog.Int("partition", int(in.Partition)),
			slog.Int64("offset", in.Offset),
			slog.Any("error", err),
		)
		return nil, fmt.Errorf("runtime: decode key: %w", err)
	}

	val, err := binding.DecodeVal(in.Value)
	if err != nil {
		a.logger.Error("failed to decode record value",
			slog.String("topic", in.Topic),
			slog.Int("partition", int(in.Partition)),
			slog.Int64("offset", in.Offset),
			slog.Any("error", err),
		)
		return nil, fmt.Errorf("runtime: decode val: %w", err)
	}

	// Step 2: inject into the topology source.
	// topology.Record.Key and .Value carry the decoded domain values. Timestamp is
	// converted from wall-clock to Unix milliseconds for the stream-time field
	// (§8; full event-time semantics are P3+).
	rec := topology.Record{
		Key:       key,
		Value:     val,
		Timestamp: in.Timestamp.UnixMilli(),
	}

	// Step 2b: dispatch to either the stateless shared TestDriver or the per-
	// partition Executor from the TaskManager.
	if a.taskManager != nil {
		return a.processStateful(in.Partition, rec)
	}
	return a.processStateless(rec)
}

// processStateless drives the shared TestDriver (stateless path, unchanged from P1).
func (a *Adapter) processStateless(rec topology.Record) ([]kafka.OutRecord, error) {
	if err := a.driver.PipeInput(a.sourceName, rec); err != nil {
		return nil, fmt.Errorf("runtime: topology: %w", err)
	}
	return a.drainSinks(func(sinkName string) ([]topology.Record, error) {
		return a.driver.ReadOutput(sinkName)
	})
}

// processStateful routes the record to the per-partition Executor (stateful path).
func (a *Adapter) processStateful(partition int32, rec topology.Record) ([]kafka.OutRecord, error) {
	exec := a.taskManager.Executor(partition)
	if exec == nil {
		return nil, fmt.Errorf("runtime: no task for partition %d (not yet assigned or already revoked)", partition)
	}
	if err := exec.Process(a.sourceName, rec); err != nil {
		return nil, fmt.Errorf("runtime: topology: %w", err)
	}
	return a.drainSinks(func(sinkName string) ([]topology.Record, error) {
		return exec.DrainSink(sinkName)
	})
}

// drainSinks collects output records from all sinks using the supplied drain
// function, encodes them, and returns the resulting []kafka.OutRecord.
//
// Internal sinks (absent from bt.Sinks — e.g. ktable-out-N) are skipped
// silently: they have no Kafka topic mapping and are never forwarded to.
func (a *Adapter) drainSinks(drainFn func(string) ([]topology.Record, error)) ([]kafka.OutRecord, error) {
	var outs []kafka.OutRecord
	for _, sinkName := range a.bt.Topology.SinkNames() {
		sb, ok := a.bt.Sinks[sinkName]
		if !ok {
			// Internal sink (e.g. ktable-out-N); drain the buffer so it doesn't
			// grow unboundedly, but discard the records — no Kafka topic.
			_, _ = drainFn(sinkName)
			continue
		}

		records, err := drainFn(sinkName)
		if err != nil {
			return nil, fmt.Errorf("runtime: drain(%q): %w", sinkName, err)
		}

		for _, r := range records {
			kb, err := sb.EncodeKey(r.Key)
			if err != nil {
				a.logger.Error("failed to encode output key",
					slog.String("sink", sinkName),
					slog.String("topic", sb.Topic),
					slog.Any("error", err),
				)
				return nil, fmt.Errorf("runtime: encode key (sink %q): %w", sinkName, err)
			}

			vb, err := sb.EncodeVal(r.Value)
			if err != nil {
				a.logger.Error("failed to encode output value",
					slog.String("sink", sinkName),
					slog.String("topic", sb.Topic),
					slog.Any("error", err),
				)
				return nil, fmt.Errorf("runtime: encode val (sink %q): %w", sinkName, err)
			}

			outs = append(outs, kafka.OutRecord{
				Topic: sb.Topic,
				Key:   kb,
				Value: vb,
			})
		}
	}
	return outs, nil
}
