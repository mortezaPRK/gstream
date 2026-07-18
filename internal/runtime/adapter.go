package runtime

import (
	"context"
	"fmt"
	"log/slog"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/kafka"
	"github.com/mortezaPRK/gstream/internal/topology"
)

// SinkRoute maps each topology sink name to the Kafka topic it should produce to.
// Every sink declared in the Topology must appear in this map; a missing entry is
// detected by [NewAdapter] and returned as an error.
type SinkRoute map[string]string

// Adapter bridges the kafka.ProcessFunc protocol and a topology.Topology.
//
// On each call to [Adapter.Process]:
//
//  1. The incoming kafka.InRecord value bytes are decoded with the value serde.
//  2. A topology.Record is injected into every source node (one source per call, or
//     all sources when the topology has a single source — P0 only has one source).
//  3. The synchronous, depth-first DAG traversal drives every processor and sink.
//  4. Sink outputs are drained, their values are encoded with the output serde, and
//     the result is returned as []kafka.OutRecord directed at the configured topics.
//
// Adapter uses topology.TestDriver for the synchronous DAG traversal, which is the
// same depth-first, deterministic execution model used by topology unit tests.  For
// ALO P0 (single-goroutine ProcessFunc) this is correct and avoids duplication of
// the traversal logic.
//
// Adapter is NOT safe for concurrent use.  The kafka.Client calls ProcessFunc for
// each record sequentially within a single goroutine, so this is fine for P0.
// A future task-per-partition model (P2) will instantiate one Adapter per task.
type Adapter[V any] struct {
	driver  *topology.TestDriver
	topo    *topology.Topology
	valSerde gstream.Serde[V]
	routes  SinkRoute
	logger  *slog.Logger
	// sourceName is the single source node name; P0 requires exactly one source.
	sourceName string
}

// NewAdapter constructs an Adapter for a single-source topology.
//
//   - topo must have exactly one source node (P0 constraint; multi-source support is P4+).
//   - valSerde is used to decode the incoming kafka.InRecord.Value and to encode every
//     sink output before wrapping it in a kafka.OutRecord.
//   - routes must contain a topic name for every sink declared in topo.
//   - logger may be nil (falls back to slog.Default()).
//
// The returned Adapter is wired and ready; call [Adapter.ProcessFunc] to obtain a
// kafka.ProcessFunc that can be passed directly to kafka.Client.Run.
func NewAdapter[V any](
	topo *topology.Topology,
	valSerde gstream.Serde[V],
	routes SinkRoute,
	logger *slog.Logger,
) (*Adapter[V], error) {
	if topo == nil {
		return nil, fmt.Errorf("runtime.NewAdapter: topo must not be nil")
	}
	if valSerde == nil {
		return nil, fmt.Errorf("runtime.NewAdapter: valSerde must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	// P0 constraint: exactly one source.
	srcs := topo.SourceNames()
	if len(srcs) != 1 {
		return nil, fmt.Errorf(
			"runtime.NewAdapter: P0 requires exactly one source node, got %d: %v",
			len(srcs), srcs,
		)
	}

	// Validate that every sink has a route.
	for _, sinkName := range topo.SinkNames() {
		if _, ok := routes[sinkName]; !ok {
			return nil, fmt.Errorf(
				"runtime.NewAdapter: sink %q has no entry in routes (provide a target Kafka topic)",
				sinkName,
			)
		}
	}

	driver := topology.NewTestDriver(topo)

	return &Adapter[V]{
		driver:     driver,
		topo:       topo,
		valSerde:   valSerde,
		routes:     routes,
		logger:     logger,
		sourceName: srcs[0],
	}, nil
}

// ProcessFunc returns a kafka.ProcessFunc that can be passed to kafka.Client.Run.
//
// The returned function implements the full decode → topology → encode pipeline and
// satisfies the ALO commit contract: on success it returns all output records; on any
// error it returns nil so the kafka.Client skips produce and commit (whole-batch
// redelivery, §4.1).
func (a *Adapter[V]) ProcessFunc() kafka.ProcessFunc {
	return func(ctx context.Context, in kafka.InRecord) ([]kafka.OutRecord, error) {
		return a.process(ctx, in)
	}
}

// process is the internal implementation of the per-record pipeline (§15, §7).
func (a *Adapter[V]) process(_ context.Context, in kafka.InRecord) ([]kafka.OutRecord, error) {
	// Step 1: decode the value from the incoming record.
	val, err := a.valSerde.Deserialize(in.Value)
	if err != nil {
		a.logger.Error("failed to deserialize record value",
			slog.String("topic", in.Topic),
			slog.Int("partition", int(in.Partition)),
			slog.Int64("offset", in.Offset),
			slog.Any("error", err),
		)
		return nil, fmt.Errorf("runtime: deserialize: %w", err)
	}

	// Step 2: inject into the topology source.
	// topology.Record.Key carries the raw key bytes; topology.Record.Value carries the
	// decoded domain value.  Timestamp is converted from wall-clock to Unix milliseconds
	// for the stream-time field (§8; full event-time semantics are P3+).
	rec := topology.Record{
		Key:       in.Key,
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
	for _, sinkName := range a.topo.SinkNames() {
		records, err := a.driver.ReadOutput(sinkName)
		if err != nil {
			// This should never happen since NewAdapter validated the sink names.
			return nil, fmt.Errorf("runtime: ReadOutput(%q): %w", sinkName, err)
		}

		topic := a.routes[sinkName]
		for _, r := range records {
			// Encode the output value.  Key is forwarded as-is (raw bytes from source
			// or whatever the topology processors set; type assertion handles both []byte
			// and typed keys for P0's simple filter/map pipeline).
			outVal, ok := r.Value.(V)
			if !ok {
				a.logger.Warn("sink record value type mismatch; skipping",
					slog.String("sink", sinkName),
					slog.String("topic", topic),
				)
				continue
			}

			encoded, err := a.valSerde.Serialize(outVal)
			if err != nil {
				a.logger.Error("failed to serialize output value",
					slog.String("sink", sinkName),
					slog.String("topic", topic),
					slog.Any("error", err),
				)
				return nil, fmt.Errorf("runtime: serialize: %w", err)
			}

			// Key may be []byte (from source) or a string/typed value after a Mapper.
			// For P0 we accept both: []byte passes through; anything else is formatted
			// as a UTF-8 string for simplicity.
			var outKey []byte
			switch k := r.Key.(type) {
			case []byte:
				outKey = k
			case string:
				outKey = []byte(k)
			default:
				outKey = []byte(fmt.Sprintf("%v", k))
			}

			outs = append(outs, kafka.OutRecord{
				Topic: topic,
				Key:   outKey,
				Value: encoded,
			})
		}
	}

	return outs, nil
}
