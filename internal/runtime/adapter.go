package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/kafka"
	"github.com/mortezaPRK/gstream/internal/state"
	"github.com/mortezaPRK/gstream/internal/topology"
)

// Adapter bridges the kafka.ProcessFunc protocol and a *gstream.BuiltTopology.
// It handles serde (decode InRecord → topology.Record, encode topology.Record
// → OutRecord) and routes records to the per-partition Executor held by a
// TaskManager.
//
// Both stateless (zero-store) and stateful topologies use the same code path.
// A stateless topology is a degenerate zero-store TaskManager: OnAssigned builds
// a plain Executor with no stores; PostBatch is a no-op; no Pebble DB is opened.
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
// # Global KTable lifecycle (C5)
//
// When the topology contains GlobalKTable bindings the caller MUST invoke these
// methods in order BEFORE calling kafka.New / kafka.Client.Run:
//
//  1. adapter.BootstrapGlobalStores(ctx) — blocks until every global store is
//     caught up to its high-watermark.  Calls TaskManager.RegisterGlobalStore
//     for each store so per-partition tasks see global lookups.
//  2. adapter.RunGlobalConsumers(ctx)   — starts tail-consume goroutines for
//     ongoing updates.
//
// Teardown: call adapter.Close() after kafka.Client.Run returns to stop the
// tail goroutines and release Pebble DBs.
//
// # Thread safety
//
// Adapter is NOT safe for concurrent use. kafka.Client calls ProcessFunc
// sequentially from a single goroutine.
type Adapter struct {
	taskManager     *TaskManager
	bt              *gstream.BuiltTopology
	cfg             gstream.Config
	logger          *slog.Logger
	topicToSource   map[string]string                // Kafka topic → topology source-node name
	resolvedSources map[string]gstream.SourceBinding // source-node name → SourceBinding (includes repartition)
	resolvedSinks   map[string]gstream.SinkBinding   // sink-node name → SinkBinding (includes repartition)

	// globalConsumers holds one GlobalConsumer per GlobalTableBinding. Nil until
	// BootstrapGlobalStores is called. Closed by Close().
	globalConsumers []*GlobalConsumer

	// health is the shared fatal-error signal.  Allocated in NewAdapter; shared
	// between global consumers (tail loop) and the kafka.Client run loop via
	// HealthGateHook.
	health *PipelineHealth
}

// NewAdapter constructs an Adapter driven by a *gstream.BuiltTopology.
//
// Handles both stateless (zero-store) and stateful topologies. For zero-store
// topologies cfg may be the zero value.
//
// Constraints:
//   - bt must not be nil.
//   - bt.Topology must have at least one source node.
//   - Every source in bt.Topology.SourceNames() must have a matching entry in bt.Sources.
//   - For zero-store topologies every sink must have a matching entry in bt.Sinks.
//   - For stateful topologies internal sinks (e.g. ktable-out-N) absent from bt.Sinks
//     are silently skipped.
//
// Wire LifecycleCallbacks, post-batch hooks, and SourceTopics into the kafka.Client.
//
// ALO (default, Guarantee != ExactlyOnce):
//
//	client, _ := kafka.New(cfg, adapter.SourceTopics(), logger,
//	    kafka.WithLifecycle(adapter.LifecycleCallbacks()),
//	    kafka.WithPostBatch(adapter.PostBatchHook()),
//	    kafka.WithHealthGate(adapter.HealthGateHook()), // fail-fast on store-write errors
//	)
//
// EOS (Guarantee == ExactlyOnce):
//
//	client, _ := kafka.New(cfg, adapter.SourceTopics(), logger,
//	    kafka.WithLifecycle(adapter.LifecycleCallbacks()),
//	    kafka.WithPostBatch(adapter.PostBatchSweepHook()),
//	    kafka.WithChangelogFlusher(adapter.ChangelogFlusherHook()),
//	    kafka.WithHealthGate(adapter.HealthGateHook()), // fail-fast on store-write errors
//	)
//
// WithHealthGate is optional but recommended: it provides faster detection of
// fatal errors between batches (during idle polling).  PostBatchHook and
// PostBatchSweepHook also embed a health check that halts Run via
// kafka.ErrFatalPipeline, so store-write failures ALWAYS halt the loop even
// without WithHealthGate — but WithHealthGate catches failures faster during
// idle (no records) periods.
func NewAdapter(bt *gstream.BuiltTopology, cfg gstream.Config, logger *slog.Logger) (*Adapter, error) {
	if bt == nil {
		return nil, fmt.Errorf("runtime.NewAdapter: bt must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	srcs := bt.Topology.SourceNames()
	if len(srcs) == 0 {
		return nil, fmt.Errorf("runtime.NewAdapter: topology has no source nodes")
	}

	// Build resolved source and sink maps seeded from bt.Sources and bt.Sinks.
	// Repartition bindings are merged in BEFORE the topicToSource-building loop
	// so the source validation step finds repartition source nodes without error.
	resolvedSources := make(map[string]gstream.SourceBinding, len(bt.Sources)+len(bt.RepartitionBindings))
	for name, sb := range bt.Sources {
		resolvedSources[name] = sb
	}
	resolvedSinks := make(map[string]gstream.SinkBinding, len(bt.Sinks)+len(bt.RepartitionBindings))
	for name, sb := range bt.Sinks {
		resolvedSinks[name] = sb
	}

	// Register repartition bindings: each adds a full Kafka topic to topicToSource
	// (consumed by the client) and populates both resolvedSources and resolvedSinks
	// so process() and drainSinks() need no repartition-specific logic.
	// Must run BEFORE the topicToSource-building loop because SourceNames() includes
	// the repartition source nodes, which are not present in bt.Sources.
	topicToSource := make(map[string]string, len(srcs)+len(bt.RepartitionBindings))
	for _, rb := range bt.RepartitionBindings {
		fullTopic := cfg.ApplicationID + "-" + rb.Name + "-repartition"

		// Write side: repartition sink node → SinkBinding for the full topic.
		resolvedSinks[rb.SinkName] = gstream.SinkBinding{
			Topic:     fullTopic,
			EncodeKey: rb.EncodeKey,
			EncodeVal: rb.EncodeVal,
		}

		// Read side: repartition source node → SourceBinding for the full topic.
		// topicToSource maps the full topic to the source node so process() routes
		// re-consumed records from the repartition topic to the repartition source
		// node — same lookup P4a uses for regular sources.
		resolvedSources[rb.SourceName] = gstream.SourceBinding{
			Topic:     fullTopic,
			DecodeKey: rb.DecodeKey,
			DecodeVal: rb.DecodeVal,
		}
		topicToSource[fullTopic] = rb.SourceName
	}

	// Build topic → source-node-name map for all source nodes. Repartition source
	// nodes are already in topicToSource (registered above); skip them to avoid the
	// "same topic" duplicate check firing on itself.
	for _, name := range srcs {
		binding, ok := resolvedSources[name]
		if !ok {
			return nil, fmt.Errorf(
				"runtime.NewAdapter: source %q has no entry in bt.Sources",
				name,
			)
		}
		if existing, dup := topicToSource[binding.Topic]; dup {
			if existing == name {
				// Already registered (repartition source node) — skip duplicate insert.
				continue
			}
			return nil, fmt.Errorf(
				"runtime.NewAdapter: sources %q and %q map to the same topic %q",
				existing, name, binding.Topic,
			)
		}
		topicToSource[binding.Topic] = name
	}

	// Internal sinks (absent from resolvedSinks — e.g. ktable-out-N) are valid for
	// stateful topologies; for zero-store topologies all sinks must have bindings.
	// Include GlobalTableBindings: when only a global table exists the stateful
	// sink-skip logic must still activate (internal-sink drainage).
	hasStores := len(bt.StoreBindings) > 0 || len(bt.WindowStoreBindings) > 0 ||
		len(bt.SessionStoreBindings) > 0 || len(bt.GlobalTableBindings) > 0
	for _, sinkName := range bt.Topology.SinkNames() {
		if _, ok := resolvedSinks[sinkName]; !ok {
			if !hasStores {
				return nil, fmt.Errorf(
					"runtime.NewAdapter: sink %q has no entry in bt.Sinks (provide a SinkBinding with Topic, EncodeKey, and EncodeVal)",
					sinkName,
				)
			}
			// Stateful: skip internal sinks silently.
		}
	}

	return &Adapter{
		taskManager:     NewTaskManager(bt, cfg, logger),
		bt:              bt,
		cfg:             cfg,
		logger:          logger,
		topicToSource:   topicToSource,
		resolvedSources: resolvedSources,
		resolvedSinks:   resolvedSinks,
		health:          &PipelineHealth{},
	}, nil
}

// BootstrapGlobalStores initializes all GlobalKTable consumers: for each binding it
// opens a Pebble DB (via NewGlobalConsumer), performs a blocking catch-up consume
// (Bootstrap) until every partition reaches its high-watermark, and registers the
// populated store with the TaskManager (RegisterGlobalStore) so per-partition tasks
// can resolve global lookups via ctx.Store(storeName).
//
// Ordering contract (R2): BootstrapGlobalStores MUST be called BEFORE kafka.New /
// kafka.Client.Run. RegisterGlobalStore is NOT thread-safe; all registrations happen
// here, before any OnAssigned callback fires.
//
// Returns nil and is a no-op when the topology has no GlobalTableBindings.
// Returns the first error encountered; already-opened consumers are closed on error.
func (a *Adapter) BootstrapGlobalStores(ctx context.Context) error {
	if len(a.bt.GlobalTableBindings) == 0 {
		return nil
	}

	var opened []*GlobalConsumer
	cleanup := func() {
		for _, gc := range opened {
			if err := gc.Close(); err != nil {
				a.logger.Warn("BootstrapGlobalStores: cleanup close failed",
					slog.String("store", gc.storeName),
					slog.Any("error", err),
				)
			}
		}
	}

	for _, binding := range a.bt.GlobalTableBindings {
		gc, err := NewGlobalConsumer(a.cfg, binding, a.logger)
		if err != nil {
			cleanup()
			return fmt.Errorf("runtime.Adapter.BootstrapGlobalStores: NewGlobalConsumer(%q): %w",
				binding.StoreName, err)
		}
		opened = append(opened, gc)

		if err := gc.Bootstrap(ctx); err != nil {
			cleanup()
			return fmt.Errorf("runtime.Adapter.BootstrapGlobalStores: Bootstrap(%q): %w",
				binding.StoreName, err)
		}

		a.taskManager.RegisterGlobalStore(binding.StoreName, gc.Store())
	}

	a.globalConsumers = opened
	return nil
}

// RunGlobalConsumers starts a background tail-consume goroutine for each global
// consumer that was bootstrapped by BootstrapGlobalStores. Must be called AFTER
// BootstrapGlobalStores and BEFORE kafka.Client.Run.
//
// Each consumer is wired with the shared PipelineHealth so a fatal store-write
// error in any tail goroutine trips health and is surfaced to the run loop via
// HealthGateHook.
//
// Returns nil and is a no-op when there are no global consumers.
func (a *Adapter) RunGlobalConsumers(ctx context.Context) error {
	for _, gc := range a.globalConsumers {
		gc.SetHealth(a.health)
		if err := gc.TailConsume(ctx); err != nil {
			return fmt.Errorf("runtime.Adapter.RunGlobalConsumers: TailConsume(%q): %w",
				gc.storeName, err)
		}
	}
	return nil
}

// Close stops all global tail-consume goroutines and closes their Pebble DBs.
// Follows GlobalConsumer.Close CONTRACT: cancel → wg.Wait → client.Close → db.Close.
//
// Returns the first error encountered; all consumers are closed regardless of
// intermediate errors (best-effort).
func (a *Adapter) Close() error {
	var errs []error
	for _, gc := range a.globalConsumers {
		if err := gc.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close global store %q: %w", gc.storeName, err))
		}
	}
	return errors.Join(errs...)
}

// SourceTopics returns the list of Kafka topics this Adapter consumes — one per
// source node in the topology. Pass this to kafka.New as the topics argument so
// the client subscribes to all source topics.
func (a *Adapter) SourceTopics() []string {
	topics := make([]string, 0, len(a.topicToSource))
	for topic := range a.topicToSource {
		topics = append(topics, topic)
	}
	return topics
}

// LifecycleCallbacks returns the onAssigned and onRevoked callbacks for wiring
// into kafka.WithLifecycle.
func (a *Adapter) LifecycleCallbacks() (
	onAssigned func(ctx context.Context, assigned map[string][]int32) error,
	onRevoked func(ctx context.Context, revoked map[string][]int32),
) {
	return a.taskManager.OnAssigned, a.taskManager.OnRevoked
}

// PostBatchHook returns the ALO post-batch function for wiring into
// kafka.WithPostBatch. It performs sweep + WriteStreamTime + changelog
// producer.Flush (full flush). Use for ALO only.
//
// Health check (always-on for stateful topologies): if the pipeline health
// has been tripped (e.g. by an un-retryable Pebble write failure in the global
// consumer tail loop or per-partition process path), the returned function
// returns kafka.ErrFatalPipeline before any flush work.  The runALO loop
// detects ErrFatalPipeline and exits Run so the caller can restart and restore
// from the changelog.  Stateless topologies never wire PostBatchHook, so this
// check is co-located with store writes: always-on where needed, absent where not.
func (a *Adapter) PostBatchHook() func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if err := a.health.Err(); err != nil {
			return fmt.Errorf("%w: %w", kafka.ErrFatalPipeline, err)
		}
		return a.taskManager.PostBatch(ctx)
	}
}

// PostBatchSweepHook returns the EOS post-batch function for wiring into
// kafka.WithPostBatch. It performs sweep + WriteStreamTime but NO Kafka I/O.
// Use together with ChangelogFlusherHook for EOS so changelog records are
// drained into the transaction by the kafka.Client.
//
// Health check (always-on for stateful EOS topologies): same as PostBatchHook —
// if health is tripped, returns kafka.ErrFatalPipeline.  runEOS detects this,
// aborts the open transaction cleanly, and exits Run.
func (a *Adapter) PostBatchSweepHook() func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if err := a.health.Err(); err != nil {
			return fmt.Errorf("%w: %w", kafka.ErrFatalPipeline, err)
		}
		return a.taskManager.PostBatchSweep(ctx)
	}
}

// ChangelogFlusherHook returns the changelog flusher for wiring into
// kafka.WithChangelogFlusher for EOS. It drains encoded changelog records
// from every live task's MutationCollectors so the kafka.Client can produce
// them inside the Kafka transaction alongside sink records (R2 atomicity).
// Returns nil for ALO (caller should not wire it under ALO).
func (a *Adapter) ChangelogFlusherHook() func(ctx context.Context) ([]kafka.OutRecord, error) {
	if a.cfg.Guarantee != gstream.ExactlyOnce {
		return nil
	}
	return func(ctx context.Context) ([]kafka.OutRecord, error) {
		return a.taskManager.DrainChangelogRecords()
	}
}

// HealthGateHook returns a function that kafka.Client.Run can call at the start
// of each batch iteration to detect a fatal pipeline error (e.g. an un-retryable
// Pebble store-write failure from the global tail consumer or from a per-partition
// process step).  If the pipeline is unhealthy, the returned function returns the
// fatal error so the run loop exits and the process can restart (changelog restore).
//
// Wire into kafka.WithHealthGate (ALO and EOS run loops check it before processing
// each batch):
//
//	kafka.WithHealthGate(adapter.HealthGateHook())
func (a *Adapter) HealthGateHook() func() error {
	return func() error {
		return a.health.Err()
	}
}

// ProcessFunc returns a kafka.ProcessFunc that can be passed to kafka.Client.Run.
// On success it returns all output records; on any error it returns nil so the
// kafka.Client skips produce and commit (whole-batch redelivery).
func (a *Adapter) ProcessFunc() kafka.ProcessFunc {
	return func(ctx context.Context, in kafka.InRecord) ([]kafka.OutRecord, error) {
		return a.process(ctx, in)
	}
}

func (a *Adapter) process(ctx context.Context, in kafka.InRecord) ([]kafka.OutRecord, error) {
	sourceName, ok := a.topicToSource[in.Topic]
	if !ok {
		return nil, fmt.Errorf("runtime: no source for topic %q", in.Topic)
	}
	binding := a.resolvedSources[sourceName]

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

	rec := topology.Record{
		Key:       key,
		Value:     val,
		Timestamp: in.Timestamp.UnixMilli(),
	}

	exec := a.taskManager.Executor(in.Partition)
	if exec == nil {
		return nil, fmt.Errorf("runtime: no task for partition %d (not yet assigned or already revoked)", in.Partition)
	}
	if err := exec.Process(ctx, sourceName, rec); err != nil {
		// A store-write error (ErrStoreWrite) is un-retryable: retrying will
		// livelock forever on the same disk error.  Trip health so the run loop
		// halts after this batch and the process can restart (changelog restore).
		// Non-store errors (serde / user-logic) use the existing abort path.
		if errors.Is(err, state.ErrStoreWriteSentinel) {
			a.health.Fail(err)
		}
		return nil, fmt.Errorf("runtime: topology: %w", err)
	}
	return a.drainSinks(func(sinkName string) ([]topology.Record, error) {
		return exec.DrainSink(sinkName)
	})
}

// drainSinks collects output records from all sinks, encodes them, and returns
// the resulting []kafka.OutRecord. Internal sinks (absent from resolvedSinks — e.g.
// ktable-out-N) are drained and discarded. Repartition sinks produce OutRecords
// with Partition UNSET (IsValid=false) so the murmur2 partitioner routes by key.
func (a *Adapter) drainSinks(drainFn func(string) ([]topology.Record, error)) ([]kafka.OutRecord, error) {
	var outs []kafka.OutRecord
	for _, sinkName := range a.bt.Topology.SinkNames() {
		sb, ok := a.resolvedSinks[sinkName]
		if !ok {
			// Internal sink (e.g. ktable-out-N); drain so the buffer doesn't grow
			// unboundedly, but discard — no Kafka topic.
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
