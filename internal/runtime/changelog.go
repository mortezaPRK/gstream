package runtime

import (
	"context"
	"fmt"
	"hash/fnv"

	gstream "mortz.dev/go/gstream"
	"mortz.dev/go/gstream/internal/kafka"
	"mortz.dev/go/gstream/xtypes"
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

// Encode converts mutations into []kafka.OutRecord pinned to partition.
// Each Mutation maps to one OutRecord:
//   - Key   = Put.Key or Delete.Key (full Pebble key including store prefix)
//   - Value = Put.Value (non-nil), or nil for Delete (Kafka tombstone)
//   - Partition is pinned (IsValid=true, Value=partition) so the record is
//     routed to the exact partition regardless of key hash.
//
// Encode contains all encoding logic; Flush and the EOS path (C1) both
// delegate encoding here so the mapping lives in one place.
//
// Return type is []kafka.OutRecord (not []*kgo.Record) because the runtime
// layer speaks OutRecord — the kafka transport layer turns OutRecord→kgo.Record
// at produce time. Encode is the bridge from the state layer's Mutation world
// to the runtime layer's produce vocabulary. (stores/pebble already imports
// internal/kafka for record.go; internal/kafka does not import stores/pebble,
// so there is no import cycle.)
func (p *ChangelogProducer) Encode(partition int32, muts []gstream.StoreMutation) []kafka.OutRecord {
	if len(muts) == 0 {
		return nil
	}
	out := make([]kafka.OutRecord, len(muts))
	for i, mutation := range muts {
		out[i] = kafka.OutRecord{
			Topic:     p.topic,
			Key:       mutation.Key,
			Value:     mutation.Value,
			Partition: xtypes.NilOf(partition),
		}
	}
	return out
}

// Flush produces all mutations to the changelog topic, pinned to partition.
// Encoding is delegated to Encode; the ALO semantics are unchanged:
//   - Records are produced synchronously via ProduceSync.
//   - The first produce error is returned; no further records are attempted.
//
// EOS callers must NOT use Flush — they call Encode and hand the resulting
// OutRecords to the transactional session's ProduceSync instead.
func (p *ChangelogProducer) Flush(ctx context.Context, partition int32, muts []gstream.StoreMutation) error {
	if len(muts) == 0 {
		return nil
	}

	outs := p.Encode(partition, muts)
	records := make([]*kgo.Record, len(outs))
	for i, o := range outs {
		records[i] = &kgo.Record{
			Topic:     o.Topic,
			Key:       o.Key,
			Value:     o.Value,
			Partition: o.Partition.Value, // always pinned (IsValid=true from Encode)
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
