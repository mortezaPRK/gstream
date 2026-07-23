package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/kafka"
	"github.com/mortezaPRK/gstream/internal/state"
)

// GlobalConsumer bootstraps a GlobalKTable from a Kafka topic — reading ALL
// partitions from offset 0 to the high-watermark at bootstrap time — and then
// tail-consumes ongoing updates into a shared Pebble-backed KeyValueStore.
//
// The store is keyed by raw Kafka record key bytes and valued by raw Kafka record
// value bytes. See Bootstrap for the encoding-boundary reasoning.
//
// Lifecycle:
//  1. NewGlobalConsumer — opens the Pebble DB and creates the store.
//  2. Bootstrap(ctx)    — catch-up consume; blocks until every partition reaches
//     its high-watermark (as of the start of this call).
//  3. TailConsume(ctx)  — ongoing consume; starts a background goroutine that
//     applies new records until ctx is cancelled.
//  4. Close()           — stops the tail goroutine (see Close CONTRACT) and closes
//     the Pebble DB.
type GlobalConsumer struct {
	store     *state.KeyValueStore[[]byte, []byte]
	db        *pebble.DB
	storeName string
	topic     string
	binding   gstream.GlobalTableBinding
	brokers   []string
	logger    *slog.Logger

	// client is the kgo client created during Bootstrap. TailConsume reuses it.
	// Nil until Bootstrap completes successfully.
	client *kgo.Client

	// wg tracks the tail goroutine launched by TailConsume.
	wg sync.WaitGroup

	// tailCancel is the cancel func for the tail goroutine's context.
	// Set by TailConsume; guarded by tailMu.
	tailCancel context.CancelFunc
	tailMu     sync.Mutex
}

// NewGlobalConsumer opens a Pebble DB for the global store and creates the
// nil-collector KeyValueStore[[]byte,[]byte]. The caller must call Bootstrap
// before TailConsume, and Close when done.
//
// DB path: filepath.Join(cfg.StateDir, cfg.ApplicationID, "global-"+binding.StoreName).
//
// The nil collector means mutations are NOT written to any changelog — global
// stores are rebuilt from the source topic on restart via Bootstrap, not from
// a separate changelog topic.
func NewGlobalConsumer(
	cfg gstream.Config,
	binding gstream.GlobalTableBinding,
	logger *slog.Logger,
) (*GlobalConsumer, error) {
	if logger == nil {
		logger = slog.Default()
	}

	dbDir := filepath.Join(cfg.StateDir, cfg.ApplicationID, "global-"+binding.StoreName)
	db, err := state.OpenDB(dbDir)
	if err != nil {
		return nil, fmt.Errorf("runtime.NewGlobalConsumer: open pebble at %q: %w", dbDir, err)
	}

	// nil collector: global tables have no changelog; they are restored from the
	// source topic (Bootstrap) rather than from a per-partition changelog.
	store := state.NewKeyValueStore[[]byte, []byte](
		binding.StoreName,
		db,
		gstream.BytesSerde{},
		gstream.BytesSerde{},
	)

	return &GlobalConsumer{
		store:     store,
		db:        db,
		storeName: binding.StoreName,
		topic:     binding.Topic,
		binding:   binding,
		brokers:   cfg.Brokers,
		logger:    logger,
	}, nil
}

// Bootstrap performs a full catch-up consume of the global topic. It reads ALL
// partitions from offset 0 to their respective high-watermarks recorded at the
// start of the call, applying each record to the store. Blocks until every
// partition has consumed offset >= hwm-1 (i.e. the last record at call time).
//
// A context cancellation aborts bootstrap and returns ctx.Err().
//
// Encoding boundary (raw bytes):
// The store is keyed by RAW Kafka record key bytes (rec.Key) and valued by RAW
// record value bytes (rec.Value). The binding.EncodeKey/DecodeKey closures are
// NOT used during bootstrap. This is correct because JoinGlobal/C2 looks up
// values by calling binding.EncodeKey(mappedKey), which produces the same bytes
// the global topic producer wrote (assuming the same keySerde at both ends). So
// store.Get(binding.EncodeKey(mappedKey)) == store.Get(rec.Key), matching what
// Bootstrap stored. The binding closures exist for type-safe round-trips at the
// DSL boundary, not as an extra encoding layer inside the store.
//
// Tombstone: len(rec.Value)==0 → store.Delete(rec.Key). Non-tombstone: store.Put.
// This mirrors state.RestoreFromChangelog's tombstone convention.
//
// Bootstrap creates a kgo client with kgo.ConsumePartitions (NO ConsumerGroup),
// which uses the direct Fetch protocol and is invisible to the group coordinator —
// zero interference with any ConsumerGroup consuming the same topic (spike S1).
//
// The bootstrap client is retained for TailConsume so no reconnection is needed.
func (gc *GlobalConsumer) Bootstrap(ctx context.Context) error {
	start := time.Now()

	// Step 1: fetch partition count via Metadata.
	nPartitions, err := kafka.FetchPartitionCount(ctx, gc.brokers, gc.topic)
	if err != nil {
		return fmt.Errorf("runtime.GlobalConsumer.Bootstrap: fetch partition count for %q: %w",
			gc.topic, err)
	}
	if nPartitions == 0 {
		return fmt.Errorf("runtime.GlobalConsumer.Bootstrap: topic %q has 0 partitions", gc.topic)
	}

	// Step 2: fetch per-partition high-watermarks. Extended from state.fetchHighWatermark
	// (internal/state/restore.go) to N partitions in a single ListOffsets request.
	hwms, err := fetchAllPartitionHWMs(ctx, gc.brokers, gc.topic, int(nPartitions))
	if err != nil {
		return fmt.Errorf("runtime.GlobalConsumer.Bootstrap: fetch HWMs for %q: %w",
			gc.topic, err)
	}

	// Step 3: build per-partition state and ConsumePartitions assignment map.
	// Always assign all partitions from offset 0; partitions with HWM==0 are marked
	// done immediately so the bootstrap loop skips them without polling.
	type pState struct {
		hwm  int64
		done bool
	}
	pstates := make(map[int32]*pState, nPartitions)
	partAssign := make(map[int32]kgo.Offset, nPartitions)
	for i := int32(0); i < nPartitions; i++ {
		hw := hwms[i]
		pstates[i] = &pState{hwm: hw, done: hw == 0}
		partAssign[i] = kgo.NewOffset().At(0)
	}

	// Step 4: create bootstrap+tail client — direct Fetch, NO ConsumerGroup.
	// Mirrors state.RestoreFromChangelog lines 69–76: kgo.ConsumePartitions with no
	// ConsumerGroup option so the client never participates in group coordination.
	// Extended here from 1 partition to N in a single client.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(gc.brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			gc.topic: partAssign,
		}),
	)
	if err != nil {
		return fmt.Errorf("runtime.GlobalConsumer.Bootstrap: create client: %w", err)
	}

	// Step 5: poll+apply loop with check-before-poll pattern (no idle hang).
	// Mirrors state.RestoreFromChangelog lines 86–138, extended to N partitions.
	// Stop condition: every partition's pstate.done == true.
	// Check-before-poll ensures the loop exits the moment the last partition is marked
	// done, without issuing another PollFetches that would block on empty partitions.
	var applied int64
	for {
		// Check-before-poll (spike FINDING B): exit if all partitions are caught up.
		allDone := true
		for _, s := range pstates {
			if !s.done {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}

		select {
		case <-ctx.Done():
			client.Close()
			return fmt.Errorf("runtime.GlobalConsumer.Bootstrap: context done during consume: %w",
				ctx.Err())
		default:
		}

		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}
		if err := fetches.Err(); err != nil {
			client.Close()
			return fmt.Errorf("runtime.GlobalConsumer.Bootstrap: poll fetches: %w", err)
		}

		var applyErr error
		fetches.EachRecord(func(r *kgo.Record) {
			if applyErr != nil {
				return
			}
			if err := gc.applyRecord(r); err != nil {
				applyErr = err
				return
			}
			applied++
			// Mark partition done when the last record at bootstrap HWM is consumed.
			// Last record offset == hwm-1 (mirrors restore.go line 128).
			if s, ok := pstates[r.Partition]; ok && r.Offset >= s.hwm-1 {
				s.done = true
			}
		})
		if applyErr != nil {
			client.Close()
			return fmt.Errorf("runtime.GlobalConsumer.Bootstrap: apply record: %w", applyErr)
		}
	}

	gc.client = client // retain for TailConsume — already positioned past HWM

	gc.logger.Info("GlobalConsumer.Bootstrap: complete",
		slog.String("topic", gc.topic),
		slog.Int("partitions", int(nPartitions)),
		slog.Int64("records_applied", applied),
		slog.Duration("elapsed", time.Since(start)),
	)
	return nil
}

// TailConsume starts a background goroutine that continues consuming the global
// topic from the post-bootstrap offsets, applying updates (Put/Delete raw) until
// ctx is cancelled. Must be called after Bootstrap.
//
// TailConsume returns nil immediately; the goroutine runs under gc.wg so Close
// can wait for it to drain before releasing the Pebble DB. Fetch errors are
// logged as warnings; individual record-apply errors are logged as errors.
//
// Close MUST be called after TailConsume to stop the goroutine and release
// resources — see Close CONTRACT.
func (gc *GlobalConsumer) TailConsume(ctx context.Context) error {
	if gc.client == nil {
		return fmt.Errorf("runtime.GlobalConsumer.TailConsume: Bootstrap must be called first")
	}

	// Wrap ctx so Close can cancel the goroutine independently of the caller's ctx.
	tailCtx, cancel := context.WithCancel(ctx)
	gc.tailMu.Lock()
	gc.tailCancel = cancel
	gc.tailMu.Unlock()

	gc.wg.Add(1)
	go func() {
		defer gc.wg.Done()
		defer cancel() // release cancel on exit (idempotent if Close already called it)

		gc.logger.Info("GlobalConsumer.TailConsume: started", slog.String("topic", gc.topic))
		for {
			// Check cancellation before polling to exit promptly.
			if tailCtx.Err() != nil {
				gc.logger.Info("GlobalConsumer.TailConsume: stopped",
					slog.String("topic", gc.topic),
				)
				return
			}

			fetches := gc.client.PollFetches(tailCtx)
			if fetches.IsClientClosed() {
				return
			}
			// Context cancelled while blocked in PollFetches.
			if tailCtx.Err() != nil {
				return
			}

			// Log per-partition transient errors; continue — tail is resilient.
			fetches.EachError(func(topic string, partition int32, err error) {
				gc.logger.Warn("GlobalConsumer.TailConsume: fetch error",
					slog.String("topic", topic),
					slog.Int("partition", int(partition)),
					slog.Any("error", err),
				)
			})

			// Apply records from non-errored partitions.
			fetches.EachRecord(func(r *kgo.Record) {
				if err := gc.applyRecord(r); err != nil {
					gc.logger.Error("GlobalConsumer.TailConsume: apply record failed",
						slog.String("topic", r.Topic),
						slog.Int("partition", int(r.Partition)),
						slog.Int64("offset", r.Offset),
						slog.Any("error", err),
					)
				}
			})
		}
	}()
	return nil
}

// Store returns the underlying KeyValueStore for use by JoinGlobal/C2 processors.
// Concurrent reads from multiple task goroutines and the single tail-write goroutine
// are safe: all operations delegate to the concurrent-safe Pebble DB (S2).
//
// The returned value is *state.KeyValueStore[[]byte,[]byte]; callers type-assert
// as needed. The any type avoids importing internal/state at call sites.
func (gc *GlobalConsumer) Store() any {
	return gc.store
}

// Close stops the tail consumer goroutine (if running) and closes the Pebble DB.
//
// Close CONTRACT (S2 — race safety):
//
//	KeyValueStore.closed is an unsynchronized bool (internal/state/keyvalue.go).
//	If the tail goroutine were still calling store.Put or store.Delete when
//	db.Close() ran, Pebble would receive operations on a closed database.
//
//	Close ALWAYS sequences as:
//	  1. cancel(tailCtx)  — signals the goroutine to stop.
//	  2. gc.wg.Wait()     — blocks until the goroutine exits (drains any in-flight
//	                        record application).
//	  3. gc.client.Close() — releases the kgo client.
//	  4. gc.db.Close()    — closes Pebble AFTER the goroutine is confirmed stopped.
//
//	This ordering is maintained regardless of whether the caller already cancelled
//	the tail context before calling Close: wg.Wait() is always called.
//	Callers may call:  cancel(); wg.Wait(); gc.Close()  OR  gc.Close() — both safe.
func (gc *GlobalConsumer) Close() error {
	// Cancel the tail goroutine if it is running.
	gc.tailMu.Lock()
	cancel := gc.tailCancel
	gc.tailMu.Unlock()
	if cancel != nil {
		cancel()
	}

	// REQUIRED (S2): wait for the tail goroutine to exit BEFORE closing any
	// shared resource. KeyValueStore.closed is unsynchronized; writing to the store
	// after db.Close() causes undefined behavior. wg.Wait() ensures the goroutine
	// has exited and no further store operations are in flight.
	gc.wg.Wait()

	// Close the kgo client after the goroutine has stopped.
	if gc.client != nil {
		gc.client.Close()
	}

	// Close Pebble DB last — all store operations are complete by this point.
	if gc.db != nil {
		return gc.db.Close()
	}
	return nil
}

// applyRecord writes a single Kafka record to the store using raw bytes.
// Delegates to applyKV.
func (gc *GlobalConsumer) applyRecord(r *kgo.Record) error {
	return gc.applyKV(r.Key, r.Value, r.Offset, r.Partition)
}

// applyKV writes key→value into the store using raw bytes.
// Tombstone (len(value)==0): store.Delete(key).
// Non-tombstone: store.Put(key, value).
// Mirrors the tombstone convention in state.RestoreFromChangelog (restore.go:111–126).
// Separated from applyRecord so unit tests can call it without constructing kgo.Record.
func (gc *GlobalConsumer) applyKV(key, value []byte, offset int64, partition int32) error {
	if len(value) == 0 {
		if err := gc.store.Delete(key); err != nil {
			return fmt.Errorf("applyKV: Delete (offset %d partition %d): %w",
				offset, partition, err)
		}
		return nil
	}
	if err := gc.store.Put(key, value); err != nil {
		return fmt.Errorf("applyKV: Put (offset %d partition %d): %w",
			offset, partition, err)
	}
	return nil
}

// fetchAllPartitionHWMs returns the high-watermark (log-end-offset, i.e. next
// offset to be written) for each partition [0..numPartitions) using a single
// ListOffsets request with Timestamp=-1.
//
// This extends state.fetchHighWatermark (internal/state/restore.go:160–204) from
// a single partition to N partitions in one request. The same kmsg pattern is
// used: kgo.NewClient + kmsg.NewPtrListOffsetsRequest + req.RequestWith.
//
// Return: map[partitionID]hwm. All N partitions must be present in the response
// or an error is returned.
func fetchAllPartitionHWMs(ctx context.Context, brokers []string, topic string, numPartitions int) (map[int32]int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("fetchAllPartitionHWMs: create client: %w", err)
	}
	defer cl.Close()

	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1

	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	for i := 0; i < numPartitions; i++ {
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition = int32(i)
		p.Timestamp = -1 // latest (high-watermark); mirrors restore.go:175
		rt.Partitions = append(rt.Partitions, p)
	}
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("fetchAllPartitionHWMs: ListOffsets request: %w", err)
	}

	result := make(map[int32]int64, numPartitions)
	for _, topicResp := range resp.Topics {
		if topicResp.Topic != topic {
			continue
		}
		for _, partResp := range topicResp.Partitions {
			if kerErr := kerr.ErrorForCode(partResp.ErrorCode); kerErr != nil {
				return nil, fmt.Errorf("fetchAllPartitionHWMs: topic %q partition %d: %w",
					topic, partResp.Partition, kerErr)
			}
			hw := partResp.Offset
			if hw < 0 {
				hw = 0 // empty partition; normalise to 0
			}
			result[partResp.Partition] = hw
		}
	}

	// Verify all partitions were returned.
	for i := 0; i < numPartitions; i++ {
		if _, ok := result[int32(i)]; !ok {
			return nil, fmt.Errorf("fetchAllPartitionHWMs: topic %q partition %d not found in response",
				topic, i)
		}
	}
	return result, nil
}
