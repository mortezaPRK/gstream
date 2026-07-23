package state

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/twmb/franz-go/pkg/kgo"
)

// changelogMixedPartitionerFn is a replica of the mixed partitioner in internal/kafka,
// duplicated to keep ChangelogProducer self-contained and avoid a state↔kafka import cycle.
//
// The hash path (Partition < 0) is dead code in practice: Flush always pins records to
// a specific partition (>= 0), so the hash branch is never reached. It is kept for
// structural symmetry with kafka.mixedPartitionerFn. The hash still uses FNV-1a (not
// Kafka murmur2); if a future code path ever produces an unpinned changelog record,
// align this hash to kafkaMurmur2 at that time.
func changelogMixedPartitionerFn(_ string) func(*kgo.Record, int) int {
	return func(r *kgo.Record, n int) int {
		if r.Partition >= 0 {
			return int(r.Partition)
		}
		// Dead code: changelog records are always pinned; see function comment.
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
// partitioner as the main pipeline client so that pinned records go to the
// exact requested partition.
func NewChangelogProducer(brokers []string, topic string) (*ChangelogProducer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("state.NewChangelogProducer: brokers must not be empty")
	}
	if topic == "" {
		return nil, fmt.Errorf("state.NewChangelogProducer: topic must not be empty")
	}

	kc, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordPartitioner(kgo.BasicConsistentPartitioner(changelogMixedPartitionerFn)),
	)
	if err != nil {
		return nil, fmt.Errorf("state.NewChangelogProducer: create kgo client: %w", err)
	}
	return &ChangelogProducer{kc: kc, topic: topic}, nil
}

// Flush produces all mutations to the changelog topic, pinned to partition.
// Each Mutation is mapped to a kgo.Record:
//   - Key   = Put.Key or Delete.Key (full Pebble key including store prefix)
//   - Value = Put.Value (non-nil), or nil for Delete (Kafka tombstone)
//
// Records are produced synchronously (ProduceSync). The first produce error is
// returned; on error no further records in the batch are attempted.
func (p *ChangelogProducer) Flush(ctx context.Context, partition int32, muts []Mutation) error {
	if len(muts) == 0 {
		return nil
	}

	records := make([]*kgo.Record, len(muts))
	for i, m := range muts {
		var rec *kgo.Record
		switch mut := m.(type) {
		case Put:
			rec = &kgo.Record{
				Topic:     p.topic,
				Key:       mut.Key,
				Value:     mut.Value,
				Partition: partition,
			}
		case Delete:
			rec = &kgo.Record{
				Topic:     p.topic,
				Key:       mut.Key,
				Value:     nil, // Kafka tombstone
				Partition: partition,
			}
		}
		records[i] = rec
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
