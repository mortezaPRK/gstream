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
// On each call to the ProcessFunc returned closure:
//
//  1. Incoming kafka.InRecord key/value bytes are decoded using the source
//     SourceBinding's DecodeKey and DecodeVal closures.
//  2. A topology.Record is injected into the single source node via TestDriver.
//  3. The synchronous, depth-first DAG traversal drives every processor and sink.
//  4. Sink outputs are drained; each record's key/value is encoded using the sink's
//     SinkBinding.EncodeKey and SinkBinding.EncodeVal closures, and the results are
//     returned as []kafka.OutRecord directed at the configured Kafka topics.
//
// Using per-sink EncodeKey/EncodeVal closures (captured at To() call site in the DSL)
// correctly handles type-changing operators such as Map and SelectKey — this is the
// fix for the P0 Adapter[V] single-serde bug that silently dropped records whose
// output type differed from V (§8 regression guard, task #8).
//
// Adapter uses topology.TestDriver for synchronous, depth-first DAG traversal, which
// is the same deterministic execution model used by topology unit tests. For the P1
// ALO path this is correct: kafka.Client calls ProcessFunc once per record from a
// single goroutine.
//
// Adapter is NOT safe for concurrent use. The kafka.Client calls ProcessFunc for each
// record sequentially within a single goroutine. A future task-per-partition model
// (P2) will instantiate one Adapter per task.
type Adapter struct {
	driver     *topology.TestDriver
	bt         *gstream.BuiltTopology
	logger     *slog.Logger
	sourceName string // P1: exactly one source
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
// The returned Adapter is wired and ready; call ProcessFunc to obtain a
// kafka.ProcessFunc that can be passed directly to kafka.Client.Run.
func NewAdapter(bt *gstream.BuiltTopology, logger *slog.Logger) (*Adapter, error) {
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
	for _, sinkName := range bt.Topology.SinkNames() {
		if _, ok := bt.Sinks[sinkName]; !ok {
			return nil, fmt.Errorf(
				"runtime.NewAdapter: sink %q has no entry in bt.Sinks (provide a SinkBinding with Topic, EncodeKey, and EncodeVal)",
				sinkName,
			)
		}
	}

	driver := topology.NewTestDriver(bt.Topology)

	return &Adapter{
		driver:     driver,
		bt:         bt,
		logger:     logger,
		sourceName: sourceName,
	}, nil
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

	if err := a.driver.PipeInput(a.sourceName, rec); err != nil {
		a.logger.Error("topology processing error",
			slog.String("topic", in.Topic),
			slog.Int("partition", int(in.Partition)),
			slog.Int64("offset", in.Offset),
			slog.Any("error", err),
		)
		return nil, fmt.Errorf("runtime: topology: %w", err)
	}

	// Step 3: drain all sinks and encode outputs back to bytes.
	var outs []kafka.OutRecord
	for _, sinkName := range a.bt.Topology.SinkNames() {
		records, err := a.driver.ReadOutput(sinkName)
		if err != nil {
			// This should never happen since NewAdapter validated the sink names.
			return nil, fmt.Errorf("runtime: ReadOutput(%q): %w", sinkName, err)
		}

		sb := a.bt.Sinks[sinkName]
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
