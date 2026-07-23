package kafka

import (
	"context"
	"fmt"
	"log/slog"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaMurmur2 replicates Kafka's default partitioner hash (franz-go's murmur2
// is unexported). Correctness is pinned by TestKafkaMurmur2_MatchesStickyKeyPartitioner.
func kafkaMurmur2(b []byte) uint32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)
	h := seed ^ uint32(len(b))
	for len(b) >= 4 {
		k := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		b = b[4:]
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}
	switch len(b) {
	case 3:
		h ^= uint32(b[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(b[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(b[0])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

// mixedPartitionerFn is the per-topic partitioning function installed via
// kgo.BasicConsistentPartitioner.
//
// Routing rules:
//   - r.Partition < 0 (sentinel = -1): unpinned sink record; hash Key using
//     Kafka's murmur2 via kgo.KafkaHasher so gstream sink output co-partitions
//     with standard Kafka producers (toPositive(murmur2(key)) % n).
//   - r.Partition >= 0: pinned changelog record; return the stored value directly.
//
// The sentinel (-1) is set in the produce step when OutRecord.Partition.IsValid==false
// so that all sink OutRecords follow the key-hash path.
func mixedPartitionerFn(_ string) func(*kgo.Record, int) int {
	hasher := kgo.KafkaHasher(kafkaMurmur2)
	return func(r *kgo.Record, n int) int {
		if r.Partition >= 0 {
			// Pinned: changelog record specifies exact target partition.
			return int(r.Partition)
		}
		// Unpinned: hash key with Kafka murmur2 for co-partitioning with
		// standard Kafka producers (toPositive(murmur2(key)) % n).
		return hasher(r.Key, n)
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
// do not flow until the callback returns).
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

// WithPostBatch registers a hook called after processing and BEFORE produce+commit.
// This is the changelog-flush point in the ALO write order:
//
//	process → postBatch(flush changelog) → produce sinks → commit offsets
//
// A non-nil error aborts the batch (same ALO discipline as a process error).
// If WithPostBatch is not supplied no hook is called.
func WithPostBatch(fn func(ctx context.Context) error) ClientOption {
	return func(o *clientOptions) {
		o.postBatch = fn
	}
}

// New constructs a Client from a validated gstream.Config. topics is the list of
// source topics to consume. Validate is called internally.
//
// TODO(EOS): For ExactlyOnce, replace kgo.NewClient with kgo.NewGroupTransactSession,
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

// buildOpts translates a gstream.Config into a kgo.Opt slice. Pure helper kept
// separate so unit tests can reason about option construction independently.
func buildOpts(cfg gstream.Config, topics []string, logger *slog.Logger, co *clientOptions) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ApplicationID),
		kgo.ConsumeTopics(topics...),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.WithLogger(newKgoLogger(logger)),
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
			if err := cl.CommitUncommittedOffsets(ctx); err != nil {
				logger.Warn("failed to commit offsets on revoke", slog.Any("error", err))
			}
		}),
	}
}

// Run is the main consume-transform-produce-commit loop. It blocks until ctx is
// cancelled or a fatal error is encountered.
//
// ALO commit ordering: for each polled batch — process → produce → commit offsets.
// If process returns an error the batch is aborted (at-least-once redelivery).
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

		allOut, ok := processBatch(ctx, c.logger, inRecords, process)
		if !ok {
			goto nextBatch
		}

		// Post-batch hook: flush changelog mutations to Kafka BEFORE producing sink
		// records and committing offsets (ALO write order).
		// ALO caveat: a crash between flush and commit leaves the changelog ahead of
		// the committed source offset. On restart aggFn may be applied twice.
		// ExactlyOnce makes the window atomic via a single Kafka transaction.
		if c.postBatch != nil {
			if err := c.postBatch(ctx); err != nil {
				c.logger.Error("post-batch hook failed; not committing offsets",
					slog.Any("error", err),
				)
				goto nextBatch
			}
		}

		if len(allOut) > 0 {
			kgoOuts := make([]*kgo.Record, len(allOut))
			for i, o := range allOut {
				kr := &kgo.Record{
					Topic: o.Topic,
					Key:   o.Key,
					Value: o.Value,
				}
				// IsValid=false → -1 (key-hash path); IsValid=true → pinned partition.
				if !o.Partition.IsValid {
					kr.Partition = -1
				} else {
					kr.Partition = o.Partition.Value
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

		// Commit offsets after produce (ALO). A commit failure is non-fatal:
		// the broker re-delivers uncommitted records on the next session (may
		// produce duplicates but no data is lost).
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
