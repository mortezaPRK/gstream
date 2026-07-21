package state

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/twmb/franz-go/pkg/kgo"
)

// changelogMixedPartitionerFn is a replica of the mixed partitioner installed in
// internal/kafka (P2 verdict C / S2). It is duplicated here intentionally to
// keep ChangelogProducer self-contained and avoid a state↔kafka import cycle.
//
// Routing rules:
//   - r.Partition >= 0: pinned; return the stored value directly (used by Flush).
//   - r.Partition < 0 (sentinel -1): unpinned hash path — DEAD CODE in practice.
//
// The hash path (Partition < 0) is never reached: Flush always sets
// records[i].Partition = partition (>= 0) so every changelog record is pinned.
// This branch is kept for structural symmetry with kafka.mixedPartitionerFn but
// will never execute under normal operation.
//
// NOTE: the hash path here still uses FNV-1a (not Kafka murmur2). Since it is
// unreachable, alignment is deferred. If a future code path ever produces an
// unpinned changelog record, align this hash to kafkaMurmur2 at that time.
func changelogMixedPartitionerFn(_ string) func(*kgo.Record, int) int {
	return func(r *kgo.Record, n int) int {
		if r.Partition >= 0 {
			return int(r.Partition)
		}
		// Dead code path: changelog records are always pinned (Partition >= 0).
		// FNV-1a is left intentionally; see comment above.
		h := fnv.New32a()
		h.Write(r.Key)
		return int(h.Sum32()) % n
	}
}

// ChangelogProducer writes state mutations as Kafka records to a changelog
// topic. Each mutation is pinned to the same partition as its source input
// record so that changelog partitions and state-store partitions are
// co-located (P2 design).
//
// ChangelogProducer owns its kgo.Client; callers must call Close when done.
type ChangelogProducer struct {
	kc    *kgo.Client
	topic string
}

// NewChangelogProducer creates a ChangelogProducer that produces to the given
// topic on the provided brokers. The underlying kgo.Client uses the same mixed
// partitioner logic as the main pipeline client (P2 verdict C / S2) so that
// pinned records go to the exact requested partition.
func NewChangelogProducer(brokers []string, topic string) (*ChangelogProducer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("state.NewChangelogProducer: brokers must not be empty")
	}
	if topic == "" {
		return nil, fmt.Errorf("state.NewChangelogProducer: topic must not be empty")
	}

	kc, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// Mixed partitioner: pin when Partition>=0, key-hash otherwise.
		// Consistent with internal/kafka/client.go — verdict C, S2.
		kgo.RecordPartitioner(kgo.BasicConsistentPartitioner(changelogMixedPartitionerFn)),
	)
	if err != nil {
		return nil, fmt.Errorf("state.NewChangelogProducer: create kgo client: %w", err)
	}
	return &ChangelogProducer{kc: kc, topic: topic}, nil
}

// Flush produces all mutations to the changelog topic, pinned to partition.
// Each Mutation is mapped to a kgo.Record:
//   - Key   = m.Key  (full Pebble key including store prefix)
//   - Value = m.Value (nil for tombstones / IsDelete=true)
//
// Records are produced synchronously (ProduceSync). The first produce error is
// returned; on error no further records in the batch are attempted.
func (p *ChangelogProducer) Flush(ctx context.Context, partition int32, muts []Mutation) error {
	if len(muts) == 0 {
		return nil
	}

	records := make([]*kgo.Record, len(muts))
	for i, m := range muts {
		records[i] = &kgo.Record{
			Topic: p.topic,
			Key:   m.Key,
			// Value is nil for deletions (IsDelete=true) — this produces a
			// Kafka tombstone, which signals consumers to delete the key.
			Value:     m.Value,
			Partition: partition, // pinned: >= 0 routes through changelogMixedPartitionerFn
		}
	}

	results := p.kc.ProduceSync(ctx, records...)
	for _, res := range results {
		if res.Err != nil {
			return fmt.Errorf("state.ChangelogProducer.Flush: produce to %q partition %d: %w",
				p.topic, partition, res.Err)
		}
	}
	return nil
}

// Close shuts down the underlying kgo client, flushing any pending produce
// requests before returning.
func (p *ChangelogProducer) Close() {
	p.kc.Close()
}
