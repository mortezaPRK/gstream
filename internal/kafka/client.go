package kafka

import (
	"context"
	"fmt"
	"log/slog"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ProcessFunc is the caller-supplied record handler. It receives one consumed
// record and returns zero or more output records to be produced, plus any
// processing error. A non-nil error aborts the batch and does NOT commit offsets
// (the record will be reprocessed on the next poll — ALO semantics).
type ProcessFunc func(ctx context.Context, in InRecord) ([]OutRecord, error)

// Client wraps a kgo.Client and owns the consume-transform-produce-commit loop.
// All kgo types are private; callers interact only through New, Run, and Close.
type Client struct {
	kc     *kgo.Client
	cfg    gstream.Config
	logger *slog.Logger
}

// New constructs a Client from a validated gstream.Config. topics is the list of
// source topics to consume. ApplyDefaults must have been called on cfg before New;
// Validate is called internally and returns an error on an invalid config.
//
// TODO(EOS/P5): For ExactlyOnce, replace kgo.NewClient with kgo.NewGroupTransactSession,
// add kgo.TransactionalID("gstream-"+cfg.ApplicationID), and set
// kgo.FetchIsolationLevel(kgo.ReadCommitted()). The ALO commit path in Run must then
// be replaced by sess.Begin()/sess.End(ctx, kgo.TryCommit).
func New(cfg gstream.Config, topics []string, logger *slog.Logger) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka.New: invalid config: %w", err)
	}
	if len(topics) == 0 {
		return nil, fmt.Errorf("kafka.New: topics must not be empty")
	}
	if logger == nil {
		logger = slog.Default()
	}

	opts := buildOpts(cfg, topics, logger)
	kc, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka.New: failed to create kgo client: %w", err)
	}
	return &Client{kc: kc, cfg: cfg, logger: logger}, nil
}

// buildOpts translates a gstream.Config into kgo.Opt slice. This is a pure helper
// kept separate so unit tests can reason about option construction independently.
//
// Options set:
//  1. kgo.SeedBrokers(cfg.Brokers...)         — bootstrap addresses
//  2. kgo.ConsumerGroup(cfg.ApplicationID)    — group id = ApplicationID
//  3. kgo.ConsumeTopics(topics...)            — source topics
//  4. kgo.Balancers(kgo.CooperativeStickyBalancer()) — cooperative-sticky assignor (§14)
//  5. kgo.DisableAutoCommit()                 — manual commit; we commit after produce (ALO §4.1)
//  6. kgo.WithLogger(...)                     — bridge kgo log to slog
//  7. kgo.OnPartitionsAssigned(...)           — rebalance callback; TODO: restore state (§11)
//  8. kgo.OnPartitionsRevoked(...)            — rebalance callback; TODO: flush + checkpoint (§11)
func buildOpts(cfg gstream.Config, topics []string, logger *slog.Logger) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ApplicationID),
		kgo.ConsumeTopics(topics...),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.WithLogger(newKgoLogger(logger)),
		kgo.OnPartitionsAssigned(func(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
			// TODO(P2): restore changelog state into Pebble for each newly assigned
			// partition before allowing Run to process records for that partition.
			logger.Info("partitions assigned", slog.Any("partitions", assigned))
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
			// TODO(P2): flush pending state writes, write a local checkpoint, and close
			// the Pebble store for each revoked partition to minimise restore time on
			// the next assignment.
			logger.Info("partitions revoked", slog.Any("partitions", revoked))
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

		// Step 2: produce all output records synchronously before committing.
		if len(allOut) > 0 {
			kgoOuts := make([]*kgo.Record, len(allOut))
			for i, o := range allOut {
				kgoOuts[i] = &kgo.Record{
					Topic: o.Topic,
					Key:   o.Key,
					Value: o.Value,
				}
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
