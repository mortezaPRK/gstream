package kafka

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/twmb/franz-go/pkg/kgo"
)

// mixedPartitionerFn is the per-topic partitioning function installed via
// kgo.BasicConsistentPartitioner (verdict C from P2 spike).
//
// Routing rules:
//   - r.Partition < 0 (sentinel = -1): unpinned sink record; hash Key using
//     FNV-1a mod n for stable key-affinity. FNV-1a over murmur2 is chosen here
//     because murmur2 is not exported from franz-go; FNV-1a is stable, zero-
//     allocation, and provides adequate key distribution for changelog-free sinks.
//     NOTE: this does NOT reproduce Java Kafka's murmur2 placement; that is
//     acceptable for P2 sink records (no co-location requirement). Document any
//     P3+ compatibility need there.
//   - r.Partition >= 0: pinned changelog record; return the stored value directly.
//
// The sentinel (-1) is set in the produce step when OutRecord.Partition == nil
// so that all legacy sink OutRecords (zero-value Partition pointer = nil) follow
// the key-hash path, preserving TestRoundTrip_ALO behaviour without changes.
func mixedPartitionerFn(_ string) func(*kgo.Record, int) int {
	return func(r *kgo.Record, n int) int {
		if r.Partition >= 0 {
			// Pinned: changelog record specifies exact target partition.
			return int(r.Partition)
		}
		// Unpinned: hash key with FNV-1a mod n for key-affinity distribution.
		h := fnv.New32a()
		h.Write(r.Key)
		return int(h.Sum32()) % n
	}
}

// ProcessFunc is the caller-supplied record handler. It receives one consumed
// record and returns zero or more output records to be produced, plus any
// processing error. A non-nil error aborts the batch and does NOT commit offsets
// (the record will be reprocessed on the next poll — ALO semantics).
type ProcessFunc func(ctx context.Context, in InRecord) ([]OutRecord, error)

// Client wraps a kgo.Client and owns the consume-transform-produce-commit loop.
// All kgo types are private; callers interact only through New, Run, and Close.
type Client struct {
	kc        *kgo.Client
	cfg       gstream.Config
	logger    *slog.Logger
	postBatch func(ctx context.Context) error // nil = no-op
}

// clientOptions holds optional configuration injected via ClientOption functions.
type clientOptions struct {
	onAssigned func(ctx context.Context, assigned map[string][]int32) error
	onRevoked  func(ctx context.Context, revoked map[string][]int32)
	postBatch  func(ctx context.Context) error
}

// ClientOption is a functional option for [New].
type ClientOption func(*clientOptions)

// WithLifecycle registers callbacks for partition assignment and revocation
// rebalance events. Both callbacks are called synchronously inside the kgo
// rebalance handler (cooperative-sticky fires onAssigned after SyncGroup; fetches
// do not flow until the callback returns — R6 verdict A).
//
//   - onAssigned is called with the newly assigned (topic→[]partition) map. A
//     non-nil error is logged but does not abort the rebalance; the callback is
//     responsible for its own cleanup on error.
//   - onRevoked is called with the revoked (topic→[]partition) map. It must not
//     return an error; cleanup errors should be handled internally.
//
// If WithLifecycle is not supplied, the client falls back to log-only stubs.
func WithLifecycle(
	onAssigned func(ctx context.Context, assigned map[string][]int32) error,
	onRevoked func(ctx context.Context, revoked map[string][]int32),
) ClientOption {
	return func(o *clientOptions) {
		o.onAssigned = onAssigned
		o.onRevoked = onRevoked
	}
}

// WithPostBatch registers a hook called after processBatch succeeds and BEFORE
// produce+commit. This is the changelog-flush point in the ALO write order:
//
//	process → postBatch(flush changelog) → produce sinks → commit offsets → checkpoint
//
// If the hook returns a non-nil error, the batch is aborted (no produce, no
// commit) — the same ALO discipline as a process error.
//
// If WithPostBatch is not supplied no hook is called.
func WithPostBatch(fn func(ctx context.Context) error) ClientOption {
	return func(o *clientOptions) {
		o.postBatch = fn
	}
}

// New constructs a Client from a validated gstream.Config. topics is the list of
// source topics to consume. ApplyDefaults must have been called on cfg before New;
// Validate is called internally and returns an error on an invalid config.
//
// opts is optional; callers that do not need lifecycle hooks or post-batch hooks
// can omit it entirely — the existing call signature New(cfg, topics, logger) is
// preserved and all behaviour is unchanged.
//
// TODO(EOS/P5): For ExactlyOnce, replace kgo.NewClient with kgo.NewGroupTransactSession,
// add kgo.TransactionalID("gstream-"+cfg.ApplicationID), and set
// kgo.FetchIsolationLevel(kgo.ReadCommitted()). The ALO commit path in Run must then
// be replaced by sess.Begin()/sess.End(ctx, kgo.TryCommit).
func New(cfg gstream.Config, topics []string, logger *slog.Logger, opts ...ClientOption) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka.New: invalid config: %w", err)
	}
	if len(topics) == 0 {
		return nil, fmt.Errorf("kafka.New: topics must not be empty")
	}
	if logger == nil {
		logger = slog.Default()
	}

	// Apply functional options.
	co := &clientOptions{}
	for _, o := range opts {
		o(co)
	}

	kgoOpts := buildOpts(cfg, topics, logger, co)
	kc, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("kafka.New: failed to create kgo client: %w", err)
	}
	return &Client{kc: kc, cfg: cfg, logger: logger, postBatch: co.postBatch}, nil
}

// buildOpts translates a gstream.Config into kgo.Opt slice. This is a pure helper
// kept separate so unit tests can reason about option construction independently.
//
// co carries optional lifecycle and post-batch hooks supplied via ClientOption.
// When co.onAssigned / co.onRevoked are nil, the callbacks fall back to the
// log-only stubs that were present before P2 wiring — no behaviour change for
// callers that do not pass WithLifecycle.
//
// Options set:
//  1. kgo.SeedBrokers(cfg.Brokers...)         — bootstrap addresses
//  2. kgo.ConsumerGroup(cfg.ApplicationID)    — group id = ApplicationID
//  3. kgo.ConsumeTopics(topics...)            — source topics
//  4. kgo.Balancers(kgo.CooperativeStickyBalancer()) — cooperative-sticky assignor (§14)
//  5. kgo.DisableAutoCommit()                 — manual commit; we commit after produce (ALO §4.1)
//  6. kgo.WithLogger(...)                     — bridge kgo log to slog
//  7. kgo.OnPartitionsAssigned(...)           — calls co.onAssigned if set, else log-only stub
//  8. kgo.OnPartitionsRevoked(...)            — calls co.onRevoked if set, then commits offsets
//  9. kgo.RecordPartitioner(...)              — mixed partitioner: pin when Partition>=0, key-hash otherwise
func buildOpts(cfg gstream.Config, topics []string, logger *slog.Logger, co *clientOptions) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ApplicationID),
		kgo.ConsumeTopics(topics...),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.WithLogger(newKgoLogger(logger)),
		// Mixed partitioner (P2 verdict C): pinned changelog records go to their
		// specified partition; unpinned sink records are distributed by key hash.
		// See mixedPartitionerFn for routing rules.
		kgo.RecordPartitioner(kgo.BasicConsistentPartitioner(mixedPartitionerFn)),
		kgo.OnPartitionsAssigned(func(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
			logger.Info("partitions assigned", slog.Any("partitions", assigned))
			if co.onAssigned != nil {
				if err := co.onAssigned(ctx, assigned); err != nil {
					logger.Error("onAssigned hook failed", slog.Any("error", err))
				}
			}
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
			logger.Info("partitions revoked", slog.Any("partitions", revoked))
			if co.onRevoked != nil {
				co.onRevoked(ctx, revoked)
			}
			// Commit any pending offsets for revoked partitions synchronously before
			// returning, so we don't lose ALO progress.
			if err := cl.CommitUncommittedOffsets(ctx); err != nil {
				logger.Warn("failed to commit offsets on revoke", slog.Any("error", err))
			}
		}),
	}
}

// Run is the main consume-transform-produce-commit loop. It blocks until ctx is
// cancelled or a fatal error is encountered.
//
// ALO commit ordering (§4.1): for each polled batch —
//  1. process(record) → outputs
//  2. produce outputs synchronously
//  3. commit offsets for all processed records
//
// If process returns an error the batch is aborted: no produce, no commit. The
// records will be redelivered on the next poll (at-least-once).
func (c *Client) Run(ctx context.Context, process ProcessFunc) error {
	c.logger.Info("kafka client run started",
		slog.String("applicationID", c.cfg.ApplicationID),
		slog.Duration("commitInterval", c.cfg.CommitInterval),
	)

	for {
		fetches := c.kc.PollFetches(ctx)
		if fetches.IsClientClosed() {
			c.logger.Info("kafka client closed; stopping run loop")
			return nil
		}
		if err := ctx.Err(); err != nil {
			c.logger.Info("context cancelled; stopping run loop", slog.Any("reason", err))
			return nil
		}

		// Log any fetch-level errors (partition leadership changes, etc.) but do not
		// abort the whole loop — other partitions in the same fetch may be healthy.
		fetches.EachError(func(topic string, partition int32, err error) {
			c.logger.Error("fetch error",
				slog.String("topic", topic),
				slog.Int("partition", int(partition)),
				slog.Any("error", err),
			)
		})

		if fetches.Empty() {
			continue
		}

		// Collect all records from this poll and their kgo pointers (needed for commit).
		var (
			inRecords  []InRecord
			kgoRecords []*kgo.Record
		)
		fetches.EachRecord(func(r *kgo.Record) {
			inRecords = append(inRecords, InRecord{
				Topic:     r.Topic,
				Partition: r.Partition,
				Offset:    r.Offset,
				Key:       r.Key,
				Value:     r.Value,
				Timestamp: r.Timestamp,
			})
			kgoRecords = append(kgoRecords, r)
		})

		// Step 1: process each record; collect outputs.
		// processBatch returns ok=false on any process error, discarding partial
		// outputs so that Steps 2 and 3 are skipped entirely — whole-batch redelivery.
		allOut, ok := processBatch(ctx, c.logger, inRecords, process)
		if !ok {
			// A process error was already logged inside processBatch. Skip produce
			// and commit so the whole batch is redelivered on the next poll (ALO §4.1).
			goto nextBatch
		}

		// Step 1b (stateful path): post-batch hook — flush changelog mutations to
		// Kafka BEFORE producing sink records and committing offsets.
		//
		// ALO write order (§P2-S7):
		//   process(record→store+collector) → postBatch(flush changelog) →
		//   produce sinks → commit source offsets → checkpoint.
		//
		// ALO caveat: a crash between postBatch(flush) and commit leaves the
		// changelog ahead of the committed source offset. On restart, the batch
		// is re-processed (record replayed), so aggFn may be applied twice.
		// ExactlyOnce (P5) eliminates this window by making store write, changelog
		// write, and offset commit atomic in a single Kafka transaction.
		if c.postBatch != nil {
			if err := c.postBatch(ctx); err != nil {
				c.logger.Error("post-batch hook failed; not committing offsets",
					slog.Any("error", err),
				)
				goto nextBatch
			}
		}

		// Step 2: produce all output records synchronously before committing.
		if len(allOut) > 0 {
			kgoOuts := make([]*kgo.Record, len(allOut))
			for i, o := range allOut {
				kr := &kgo.Record{
					Topic: o.Topic,
					Key:   o.Key,
					Value: o.Value,
				}
				// Map OutRecord.Partition pointer to the kgo.Record sentinel:
				//   nil (sink / zero-value) → -1 so mixedPartitionerFn routes by key hash.
				//   non-nil                 → *Partition so the record is pinned.
				if o.Partition == nil {
					kr.Partition = -1
				} else {
					kr.Partition = *o.Partition
				}
				kgoOuts[i] = kr
			}
			results := c.kc.ProduceSync(ctx, kgoOuts...)
			for _, res := range results {
				if res.Err != nil {
					c.logger.Error("produce error; not committing offsets",
						slog.String("topic", res.Record.Topic),
						slog.Any("error", res.Err),
					)
					// Do not commit; records will be redelivered (ALO).
					goto nextBatch
				}
			}
		}

		// Step 3: commit offsets for all records in this batch (ALO: after produce).
		// A commit failure is non-fatal: the broker will re-deliver uncommitted records
		// on the next session, which is the expected ALO behaviour (may produce
		// duplicates but no data is lost). We warn rather than abort so a transient
		// network hiccup does not stall the processing loop.
		if err := c.kc.CommitRecords(ctx, kgoRecords...); err != nil {
			c.logger.Warn("failed to commit offsets; batch will be reprocessed on reconnect",
				slog.Any("error", err),
			)
		}

	nextBatch:
	}
}

// Close shuts down the underlying kgo client gracefully. It flushes any pending
// produce requests and commits pending offsets before returning.
func (c *Client) Close() {
	c.logger.Info("closing kafka client")
	c.kc.Close()
}
