package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	gstream "github.com/mortezaPRK/gstream"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrFatalPipeline is a sentinel error returned by the postBatch hook when the
// pipeline health has been tripped by an un-retryable store-write failure.
// The run loop checks errors.Is(err, ErrFatalPipeline) on the postBatch return
// value; if matched, Run exits with the fatal error rather than aborting the
// batch and redelivering (which would livelock on an un-retryable disk error).
//
// Callers should wrap it with %w, not compare with == .
var ErrFatalPipeline = errors.New("kafka: fatal pipeline error; restart required")

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

// Client wraps a kgo.Client (ALO) or kgo.GroupTransactSession (EOS) and owns
// the consume-transform-produce-commit loop.
// All kgo types are private; callers interact only through New, Run, and Close.
type Client struct {
	// ALO path: kc is non-nil, sess is nil.
	kc *kgo.Client
	// EOS path: sess is non-nil, kc is nil.
	sess *kgo.GroupTransactSession

	cfg       gstream.Config
	logger    *slog.Logger
	postBatch func(ctx context.Context) error // nil = no-op (ALO: PostBatch; EOS: PostBatchSweep)

	// changelogFlusher is called inside the EOS transaction, after PostBatchSweep,
	// to drain changelog records for in-transaction produce. Nil for ALO.
	changelogFlusher func(ctx context.Context) ([]OutRecord, error)

	// healthGate is called at the top of each batch iteration to detect a fatal
	// pipeline error (e.g. un-retryable Pebble store-write from the global tail
	// consumer or from a per-partition process step).  Nil = no gate (healthy).
	healthGate func() error
}

// clientOptions holds optional configuration injected via ClientOption functions.
type clientOptions struct {
	onAssigned       func(ctx context.Context, assigned map[string][]int32) error
	onRevoked        func(ctx context.Context, revoked map[string][]int32)
	postBatch        func(ctx context.Context) error
	changelogFlusher func(ctx context.Context) ([]OutRecord, error)
	healthGate       func() error
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

// WithChangelogFlusher registers a function called inside the EOS transaction
// (after PostBatchSweep and before ProduceSync) to drain encoded changelog
// records for atomic in-transaction produce. Ignored under ALO.
//
// Wire adapter.TaskManager.DrainChangelogRecords here so changelog and sink
// records are produced in the same transaction (R2 requirement).
func WithChangelogFlusher(fn func(ctx context.Context) ([]OutRecord, error)) ClientOption {
	return func(o *clientOptions) {
		o.changelogFlusher = fn
	}
}

// WithHealthGate registers a function called at the top of each batch
// iteration (before polling) in both runALO and runEOS.  If it returns a
// non-nil error the run loop exits immediately with that error, allowing the
// caller to restart the application and trigger changelog restore.
//
// The intended use is to detect fatal un-retryable errors (e.g. a Pebble
// store-write failure signalled by PipelineHealth) so the loop halts cleanly
// rather than livelocking on infinite redelivery of an un-processable batch.
//
// Wire adapter.HealthGateHook() here for both ALO and EOS:
//
//	kafka.WithHealthGate(adapter.HealthGateHook())
func WithHealthGate(fn func() error) ClientOption {
	return func(o *clientOptions) {
		o.healthGate = fn
	}
}

// New constructs a Client from a validated gstream.Config. topics is the list of
// source topics to consume. Validate is called internally.
//
// For ExactlyOnce, New builds a kgo.GroupTransactSession instead of a plain
// kgo.Client. The EOS session adds:
//   - kgo.TransactionalID("gstream-"+cfg.ApplicationID): single-instance fencing.
//   - kgo.FetchIsolationLevel(kgo.ReadCommitted()): no aborted changelog replayed.
//   - kgo.TransactionTimeout(60s): explicit timeout within the broker max.
//   - OnPartitionsRevoked WITHOUT CommitUncommittedOffsets: the session aborts on
//     revoke; offset commits happen only via End(TryCommit).
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

	if cfg.Guarantee == gstream.ExactlyOnce {
		instanceID, err := resolveInstanceID(cfg)
		if err != nil {
			return nil, fmt.Errorf("kafka.New: failed to resolve EOS instance ID: %w", err)
		}
		eosOpts := buildOptsEOS(cfg, topics, logger, co, instanceID)
		sess, err := kgo.NewGroupTransactSession(eosOpts...)
		if err != nil {
			return nil, fmt.Errorf("kafka.New: failed to create EOS session: %w", err)
		}
		return &Client{
			sess:             sess,
			cfg:              cfg,
			logger:           logger,
			postBatch:        co.postBatch,
			changelogFlusher: co.changelogFlusher,
			healthGate:       co.healthGate,
		}, nil
	}

	kgoOpts := buildOpts(cfg, topics, logger, co)
	kc, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("kafka.New: failed to create kgo client: %w", err)
	}
	return &Client{kc: kc, cfg: cfg, logger: logger, postBatch: co.postBatch, healthGate: co.healthGate}, nil
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

// resolveInstanceID determines the per-instance ID for EOS TransactionalID construction.
//
// CORRECTNESS INVARIANT: the instance ID MUST be stable across restarts of the same
// instance (same StateDir). Stability is what makes EOS crash-safe: a restarted process
// must reuse its TransactionalID so the broker can fence its own zombie producer and
// recover/abort the prior pending transaction. Generating a fresh UUID on every start
// would break this invariant.
//
// Resolution order:
//  1. If cfg.InstanceID != "" → use it verbatim (operator override; no file I/O).
//  2. Read StateDir/instance-id:
//     - file exists and non-empty → use trimmed contents (stable restart).
//     - file absent/empty → generate uuid.NewString(), mkdir StateDir (0o755),
//     - write the file (0o600), use the new ID.
//  3. Any read/write/mkdir error → return error; startup fails. No silent fallback.
func resolveInstanceID(cfg gstream.Config) (string, error) {
	if cfg.InstanceID != "" {
		return cfg.InstanceID, nil
	}

	path := filepath.Join(cfg.StateDir, "instance-id")
	data, err := os.ReadFile(path)
	if err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return id, nil
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("resolveInstanceID: read %s: %w", path, err)
	}

	// File absent or empty: generate a new UUID and persist it.
	id := uuid.NewString()
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		return "", fmt.Errorf("resolveInstanceID: mkdir %s: %w", cfg.StateDir, err)
	}
	if err := os.WriteFile(path, []byte(id), 0o600); err != nil {
		return "", fmt.Errorf("resolveInstanceID: write %s: %w", path, err)
	}
	return id, nil
}

// buildOptsEOS builds kgo options for ExactlyOnce mode (kgo.NewGroupTransactSession).
//
// Key differences from buildOpts (ALO):
//   - kgo.TransactionalID: enables producer transactions; triggers InitProducerID
//     on Begin(), which fences any zombie instance holding the same ID.
//   - kgo.FetchIsolationLevel(ReadCommitted): changelog restore and input polling
//     skip aborted transactional records (R3). Without this, a crashed instance's
//     aborted changelog records would be replayed on task reassignment.
//   - kgo.TransactionTimeout(60s): explicit; must be <= broker's
//     transaction.max.timeout.ms. The kgo default is 40s; 60s gives more headroom
//     for slower batches without risking broker rejection on well-configured clusters.
//   - OnPartitionsRevoked does NOT call CommitUncommittedOffsets: under EOS, offset
//     commits are part of End(TryCommit); committing on revoke outside the transaction
//     would break the atomic guarantee (R4). The session aborts the in-flight txn on
//     rebalance automatically.
//
// instanceID is the resolved per-instance suffix (from resolveInstanceID); it forms
// the full TransactionalID "gstream-<ApplicationID>-<instanceID>".
func buildOptsEOS(cfg gstream.Config, topics []string, logger *slog.Logger, co *clientOptions, instanceID string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.TransactionalID("gstream-" + cfg.ApplicationID + "-" + instanceID),
		kgo.ConsumerGroup(cfg.ApplicationID),
		kgo.ConsumeTopics(topics...),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.TransactionTimeout(60 * time.Second),
		kgo.WithLogger(newKgoLogger(logger)),
		kgo.RecordPartitioner(kgo.BasicConsistentPartitioner(mixedPartitionerFn)),
		kgo.OnPartitionsAssigned(func(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
			logger.Info("EOS: partitions assigned", slog.Any("partitions", assigned))
			if co.onAssigned != nil {
				if err := co.onAssigned(ctx, assigned); err != nil {
					logger.Error("EOS: onAssigned hook failed", slog.Any("error", err))
				}
			}
		}),
		// OnPartitionsRevoked: user callback only — NO CommitUncommittedOffsets.
		// EOS offset commits happen atomically inside End(TryCommit); committing
		// outside the transaction on revoke would expose partial state (R4 violation).
		// The session internally aborts the in-flight txn when a rebalance occurs.
		kgo.OnPartitionsRevoked(func(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
			logger.Info("EOS: partitions revoked", slog.Any("partitions", revoked))
			if co.onRevoked != nil {
				co.onRevoked(ctx, revoked)
			}
		}),
	}
}

// Run is the main consume-transform-produce-commit loop. It blocks until ctx is
// cancelled or a fatal error is encountered.
//
// ALO commit ordering: process → produce → commit offsets.
// EOS commit ordering: Begin → process → PostBatchSweep → ProduceSync(sinks+changelog) → End(TryCommit).
func (c *Client) Run(ctx context.Context, process ProcessFunc) error {
	if c.cfg.Guarantee == gstream.ExactlyOnce {
		return c.runEOS(ctx, process)
	}
	return c.runALO(ctx, process)
}

// runALO is the original at-least-once loop. It is byte-for-byte identical to
// the previous Run implementation; gated by cfg.Guarantee != ExactlyOnce.
func (c *Client) runALO(ctx context.Context, process ProcessFunc) error {
	c.logger.Info("kafka client ALO run started",
		slog.String("applicationID", c.cfg.ApplicationID),
		slog.Duration("commitInterval", c.cfg.CommitInterval),
	)

	for {
		// Health gate: check for a fatal pipeline error before each batch.
		// A non-nil error means an un-retryable failure (e.g. Pebble store-write)
		// has been signalled; exit the loop so the process can restart and trigger
		// changelog restore.  This check is a cheap atomic load on the hot path.
		if c.healthGate != nil {
			if err := c.healthGate(); err != nil {
				c.logger.Error("ALO: pipeline health gate tripped; stopping run loop",
					slog.Any("error", err),
				)
				return fmt.Errorf("runALO: pipeline unhealthy: %w", err)
			}
		}

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
		//
		// ErrFatalPipeline: if the hook signals a fatal un-retryable failure (e.g.
		// Pebble store-write), exit Run so the caller can restart and restore.
		if c.postBatch != nil {
			if err := c.postBatch(ctx); err != nil {
				if errors.Is(err, ErrFatalPipeline) {
					c.logger.Error("ALO: post-batch hook returned fatal error; stopping run loop",
						slog.Any("error", err),
					)
					return fmt.Errorf("runALO: fatal post-batch: %w", err)
				}
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

// outRecordsToKgo encodes []OutRecord to []*kgo.Record using the same sentinel
// convention as runALO: IsValid=false → Partition=-1 (key-hash); IsValid=true →
// pinned partition (changelog co-partitioning).
func outRecordsToKgo(outs []OutRecord) []*kgo.Record {
	krs := make([]*kgo.Record, len(outs))
	for i, o := range outs {
		kr := &kgo.Record{
			Topic: o.Topic,
			Key:   o.Key,
			Value: o.Value,
		}
		if !o.Partition.IsValid {
			kr.Partition = -1
		} else {
			kr.Partition = o.Partition.Value
		}
		krs[i] = kr
	}
	return krs
}

// runEOS is the exactly-once consume-transform-produce loop. It uses
// kgo.GroupTransactSession to make sink writes, changelog writes, and
// source-offset commits atomic (one Kafka transaction per batch).
//
// Loop invariant: for every polled batch exactly one of the following is true
// on loop iteration exit:
//   - End(TryCommit) returned (committed=true, err=nil): state + sinks advanced.
//   - End(TryAbort) returned or rebalance aborted (committed=false, err=nil):
//     input will be redelivered; local Pebble state may be ahead of the
//     committed changelog (transient divergence — see note below).
//   - err!=nil returned from End(TryCommit): fatal/unknown txn state; loop exits
//     and the caller should restart the application.
//
// # Drain/abort consistency (local-Pebble-ahead-of-changelog)
//
// On abort, local Pebble may already contain mutations absent from committed
// changelog. Continuing would expose uncommitted state to later records. Every
// abort after processing begins therefore stops run loop. Application restart
// reopens tasks and restores committed state before processing resumes.
func (c *Client) runEOS(ctx context.Context, process ProcessFunc) error {
	c.logger.Info("kafka client EOS run started",
		slog.String("applicationID", c.cfg.ApplicationID),
		slog.Duration("commitInterval", c.cfg.CommitInterval),
	)

	for {
		if err := ctx.Err(); err != nil {
			c.logger.Info("EOS: context cancelled before transaction begin", slog.Any("reason", err))
			return nil
		}

		// Health gate: check for a fatal pipeline error before each batch.
		// A non-nil error means an un-retryable failure (e.g. Pebble store-write)
		// has been signalled; exit the loop so the process can restart and trigger
		// changelog restore.  This check is a cheap atomic load on the hot path.
		if c.healthGate != nil {
			if err := c.healthGate(); err != nil {
				c.logger.Error("EOS: pipeline health gate tripped; stopping run loop",
					slog.Any("error", err),
				)
				return fmt.Errorf("runEOS: pipeline unhealthy: %w", err)
			}
		}

		// --- Begin transaction ---
		if err := c.sess.Begin(); err != nil {
			// Begin failure is fatal: cannot start a new transaction.
			c.logger.Error("EOS: Begin failed; stopping loop", slog.Any("error", err))
			return fmt.Errorf("runEOS: Begin: %w", err)
		}

		// --- Poll ---
		fetches := c.sess.PollFetches(ctx)
		if fetches.IsClientClosed() {
			c.logger.Info("EOS: kafka client closed; stopping run loop")
			return nil
		}
		if err := ctx.Err(); err != nil {
			// Context cancelled: abort open transaction cleanly, then exit.
			_, _ = c.sess.End(ctx, kgo.TryAbort)
			c.logger.Info("EOS: context cancelled; stopping run loop", slog.Any("reason", err))
			return nil
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			c.logger.Error("EOS: fetch error",
				slog.String("topic", topic),
				slog.Int("partition", int(partition)),
				slog.Any("error", err),
			)
		})

		if fetches.Empty() {
			// No records: abort the empty transaction cleanly and continue.
			if _, err := c.sess.End(ctx, kgo.TryAbort); err != nil {
				c.logger.Warn("EOS: End(TryAbort) on empty batch failed", slog.Any("error", err))
			}
			continue
		}

		// --- Build InRecord slice ---
		var inRecords []InRecord
		fetches.EachRecord(func(r *kgo.Record) {
			inRecords = append(inRecords, InRecord{
				Topic:     r.Topic,
				Partition: r.Partition,
				Offset:    r.Offset,
				Key:       r.Key,
				Value:     r.Value,
				Timestamp: r.Timestamp,
			})
		})

		// --- Process ---
		sinkOuts, ok := processBatch(ctx, c.logger, inRecords, process)
		if !ok {
			if _, err := c.sess.End(ctx, kgo.TryAbort); err != nil {
				c.logger.Warn("EOS: End(TryAbort) after process failure failed", slog.Any("error", err))
			}
			return fmt.Errorf("runEOS: %w: processing failed after transaction began", ErrFatalPipeline)
		}

		// --- PostBatchSweep (sweep + WriteStreamTime; no Kafka I/O) ---
		// ErrFatalPipeline: abort the open transaction, then return so the caller
		// can restart and restore from the changelog.
		if c.postBatch != nil {
			if err := c.postBatch(ctx); err != nil {
				if errors.Is(err, ErrFatalPipeline) {
					c.logger.Error("EOS: post-batch hook returned fatal error; stopping run loop",
						slog.Any("error", err),
					)
					if _, err2 := c.sess.End(ctx, kgo.TryAbort); err2 != nil {
						c.logger.Warn("EOS: End(TryAbort) on fatal error failed", slog.Any("error", err2))
					}
					return fmt.Errorf("runEOS: fatal post-batch: %w", err)
				}
				c.logger.Error("EOS: PostBatchSweep failed; aborting txn",
					slog.Any("error", err),
				)
				if _, err2 := c.sess.End(ctx, kgo.TryAbort); err2 != nil {
					c.logger.Warn("EOS: End(TryAbort) after sweep failure failed", slog.Any("error", err2))
				}
				return fmt.Errorf("runEOS: %w: post-batch failed: %w", ErrFatalPipeline, err)
			}
		}

		// --- Drain changelog records ---
		var changelogOuts []OutRecord
		if c.changelogFlusher != nil {
			var err error
			changelogOuts, err = c.changelogFlusher(ctx)
			if err != nil {
				c.logger.Error("EOS: changelog drain failed; aborting txn",
					slog.Any("error", err),
				)
				if _, err2 := c.sess.End(ctx, kgo.TryAbort); err2 != nil {
					c.logger.Warn("EOS: End(TryAbort) after drain failure failed", slog.Any("error", err2))
				}
				return fmt.Errorf("runEOS: %w: changelog drain failed: %w", ErrFatalPipeline, err)
			}
		}

		// --- ProduceSync: sinks + changelog in ONE call (R2 atomicity) ---
		allOuts := append(sinkOuts, changelogOuts...)
		if len(allOuts) > 0 {
			krs := outRecordsToKgo(allOuts)
			results := c.sess.ProduceSync(ctx, krs...)
			produceFailed := false
			for _, res := range results {
				if res.Err != nil {
					c.logger.Error("EOS: ProduceSync failed; aborting txn",
						slog.String("topic", res.Record.Topic),
						slog.Any("error", res.Err),
					)
					produceFailed = true
					break
				}
			}
			if produceFailed {
				if _, err := c.sess.End(ctx, kgo.TryAbort); err != nil {
					c.logger.Warn("EOS: End(TryAbort) after ProduceSync failure failed", slog.Any("error", err))
				}
				return fmt.Errorf("runEOS: %w: produce failed after local state mutation", ErrFatalPipeline)
			}
		}

		// --- End(TryCommit): commit offsets + flush + EndTransaction atomically ---
		committed, err := c.sess.End(ctx, kgo.TryCommit)
		if err != nil {
			// err!=nil from End(TryCommit): txn state is UNKNOWN (not safe to retry
			// TryCommit on the same batch — could double-commit). The only safe
			// action is to stop the loop and restart the application.
			c.logger.Error("EOS: End(TryCommit) returned error; stopping loop (txn state unknown)",
				slog.Any("error", err),
			)
			return fmt.Errorf("runEOS: End(TryCommit): %w", err)
		}
		if !committed {
			// Session aborted during rebalance. Local state may be ahead, so force
			// task reopen and committed changelog restore before redelivery.
			return fmt.Errorf("runEOS: %w: transaction aborted by session", ErrFatalPipeline)
		}
	}
}

// Close shuts down the underlying kgo client (ALO) or GroupTransactSession (EOS)
// gracefully. For ALO it flushes any pending produce requests and commits pending
// offsets. For EOS it closes the session (no commit; any open txn is left for
// the broker to abort on timeout — callers should End before Close).
func (c *Client) Close() {
	c.logger.Info("closing kafka client")
	if c.sess != nil {
		c.sess.Close()
		return
	}
	c.kc.Close()
}
