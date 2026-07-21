package state

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// RestoreFromChangelog replays a changelog topic partition into db, rebuilding
// local state from [checkpointOffset+1, highWatermark). It uses a short-lived
// non-group consumer; it does not touch the source consumer group.
//
// checkpointOffset is the last-applied changelog offset (from ReadCheckpoint);
// pass -1 to restore from the beginning. Returns the high-watermark reached.
// The caller should persist this only if they choose to update the checkpoint
// externally; RestoreFromChangelog itself writes the checkpoint atomically with
// the restored state (checkpoint == offset of last applied record, i.e. HW-1).
//
// Return semantics:
//   - returned highWatermark is the Kafka end-offset (next-offset-to-be-written),
//     i.e. the value from ListOffsets(Timestamp=-1). The written checkpoint is
//     HW-1 (the offset of the last actually consumed record).
//   - If the changelog is empty (HW==0) or startOffset>=HW (nothing to restore),
//     no data is written. The existing checkpoint and state are left untouched.
//     Returns HW, nil.
//
// Tombstone detection: ChangelogProducer.Flush (S4) produces nil Value for
// IsDelete=true mutations (Kafka tombstone). A record with len(Value)==0 is
// therefore treated as a Pebble delete. Non-empty Value is a Pebble set.
//
// Batch strategy: all records are accumulated in a single pebble.Batch and
// committed once with pebble.Sync. The checkpoint is written in the same batch
// atomically. For very large changelogs callers should consider snapshotting
// multiple checkpoints; this implementation is correct-first.
func RestoreFromChangelog(
	ctx context.Context,
	brokers []string,
	changelogTopic string,
	partition int32,
	checkpointOffset int64,
	db *pebble.DB,
	storeName string,
) (highWatermark int64, err error) {
	if len(brokers) == 0 {
		return 0, fmt.Errorf("state.RestoreFromChangelog: brokers must not be empty")
	}
	if changelogTopic == "" {
		return 0, fmt.Errorf("state.RestoreFromChangelog: changelogTopic must not be empty")
	}
	if storeName == "" {
		return 0, fmt.Errorf("state.RestoreFromChangelog: storeName must not be empty")
	}

	// Step 1: Fetch the high-watermark for (changelogTopic, partition).
	// We use a dedicated short-lived kgo.Client for this admin-style request.
	hw, err := fetchHighWatermark(ctx, brokers, changelogTopic, partition)
	if err != nil {
		return 0, fmt.Errorf("state.RestoreFromChangelog: fetch high-watermark: %w", err)
	}

	// Step 2: If HW==0 (empty changelog) or startOffset>=HW, nothing to restore.
	startOffset := checkpointOffset + 1
	if hw == 0 || startOffset >= hw {
		// Nothing to restore; leave state and checkpoint untouched.
		return hw, nil
	}

	// Step 3: Non-group consumer starting at startOffset. Read records until we
	// have consumed up to offset HW-1 (inclusive). This is deterministic: HW is
	// the next-to-be-written offset, so the last existing record is at offset HW-1.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			changelogTopic: {
				partition: kgo.NewOffset().At(startOffset),
			},
		}),
	)
	if err != nil {
		return 0, fmt.Errorf("state.RestoreFromChangelog: create consumer: %w", err)
	}
	defer consumer.Close()

	// Step 4: Accumulate all records into a single Pebble batch.
	// We apply all mutations and the checkpoint in one atomic Sync commit.
	batch := db.NewBatch()
	defer func() {
		// Close the batch if we return without committing (error path).
		// Batch.Close on an already-committed batch is a no-op.
		batch.Close()
	}()

	lastAppliedOffset := int64(-1)
	targetLastOffset := hw - 1 // last record offset we need to consume

	for {
		// Check context before each poll to respect cancellation/timeout.
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("state.RestoreFromChangelog: context done during consume: %w", ctx.Err())
		default:
		}

		fetches := consumer.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}
		if err := fetches.Err(); err != nil {
			return 0, fmt.Errorf("state.RestoreFromChangelog: poll fetches: %w", err)
		}

		done := false
		fetches.EachRecord(func(r *kgo.Record) {
			if done {
				return
			}
			// Apply this record to the batch.
			// Tombstone detection: ChangelogProducer.Flush sets Value=nil for
			// IsDelete mutations (S4, line ~80: "Value: m.Value" where m.Value
			// is nil when IsDelete=true). A Kafka consumer receives nil/empty
			// slice for a tombstone.
			if len(r.Value) == 0 {
				// Delete: tombstone record.
				if batchErr := batch.Delete(r.Key, nil); batchErr != nil {
					// Capture the error via the outer err variable and signal done.
					err = fmt.Errorf("state.RestoreFromChangelog: batch.Delete offset %d: %w",
						r.Offset, batchErr)
					done = true
					return
				}
			} else {
				// Put: regular value record.
				if batchErr := batch.Set(r.Key, r.Value, nil); batchErr != nil {
					err = fmt.Errorf("state.RestoreFromChangelog: batch.Set offset %d: %w",
						r.Offset, batchErr)
					done = true
					return
				}
			}
			lastAppliedOffset = r.Offset

			// Stop after consuming HW-1 (the last existing record).
			if r.Offset >= targetLastOffset {
				done = true
			}
		})

		if err != nil {
			return 0, err
		}
		if done {
			break
		}
	}

	// If we consumed nothing at all (consumer returned before any record), treat
	// as a no-op. This shouldn't happen given startOffset < hw, but be defensive.
	if lastAppliedOffset < 0 {
		return hw, nil
	}

	// Step 5: Write the checkpoint into the same batch at the last applied offset
	// so that state + checkpoint are updated atomically.
	if err := WriteCheckpoint(batch, storeName, lastAppliedOffset); err != nil {
		return 0, fmt.Errorf("state.RestoreFromChangelog: write checkpoint into batch: %w", err)
	}

	// Commit the batch with pebble.Sync for durability.
	if err := batch.Commit(pebble.Sync); err != nil {
		return 0, fmt.Errorf("state.RestoreFromChangelog: commit batch: %w", err)
	}

	// Return hw (the Kafka end-offset / next-to-be-written offset).
	return hw, nil
}

// fetchHighWatermark fetches the Kafka high-watermark (end offset) for the
// given (topic, partition) using ListOffsets with Timestamp=-1 (latest).
// It creates a short-lived kgo.Client and closes it after the request.
//
// The high-watermark is the next offset to be written; the last existing
// record (if any) is at offset HW-1.
func fetchHighWatermark(ctx context.Context, brokers []string, topic string, partition int32) (int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return 0, fmt.Errorf("fetchHighWatermark: create kgo client: %w", err)
	}
	defer cl.Close()

	// Build a ListOffsetsRequest for Timestamp=-1 (latest / high-watermark).
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1 // consumer replica ID

	topicPartition := kmsg.NewListOffsetsRequestTopicPartition()
	topicPartition.Partition = partition
	topicPartition.Timestamp = -1 // -1 = latest (high-watermark)

	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rt.Partitions = append(rt.Partitions, topicPartition)
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return 0, fmt.Errorf("fetchHighWatermark: ListOffsets request: %w", err)
	}

	// Parse the response.
	for _, topicResp := range resp.Topics {
		if topicResp.Topic != topic {
			continue
		}
		for _, partResp := range topicResp.Partitions {
			if partResp.Partition != partition {
				continue
			}
			if kerErr := kerr.ErrorForCode(partResp.ErrorCode); kerErr != nil {
				return 0, fmt.Errorf("fetchHighWatermark: topic %q partition %d: %w",
					topic, partition, kerErr)
			}
			// Offset from ListOffsets(Timestamp=-1) is the high-watermark
			// (next offset to be written); -1 means the partition is empty.
			if partResp.Offset < 0 {
				return 0, nil
			}
			return partResp.Offset, nil
		}
	}
	return 0, fmt.Errorf("fetchHighWatermark: topic %q partition %d not found in response",
		topic, partition)
}
