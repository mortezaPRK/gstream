package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	gstream "mortz.dev/go/gstream"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// pollLSOConfirmTimeout is the deadline given to each PollFetches call after
// lsoReachedHW is true (LastStableOffset >= hw seen in a fetch response).
// LSO >= hw is a deterministic signal: all transactions up to hw are resolved,
// so no more committed records will ever arrive for this partition. After LSO
// fires, the only reason to keep polling is to drain any committed records that
// arrived in the same response cycle but haven't been delivered yet (kgo
// pre-fetches eagerly; any such records arrive within one broker RTT, typically
// <50ms). 500ms gives 10x RTT headroom while eliminating the old 2s tax on the
// aborted-tail termination path.
//
// This timeout is the fallback for two cases:
//  1. Aborted tail: LSO fires when committed records arrive (which also report
//     LSO == HW), the bounded poll expires, and we break — typically in <500ms
//     rather than the old 2s.
//  2. All-aborted (no committed records at all): kgo does not buffer empty-record
//     responses (source.go:hasErrorsOrRecords), so LSO is never observed via a
//     delivered response. PollFetches blocks indefinitely on the caller context;
//     this timeout is never reached. Callers must supply a context with a deadline
//     to bound this pathological edge.
const pollLSOConfirmTimeout = 500 * time.Millisecond

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
//
// EOS / aborted-tail: With ReadCommitted isolation, aborted-transaction records
// are never delivered by PollFetches. If the changelog tail (offset hw-1) is an
// aborted record (the normal EOS crash-mid-transaction scenario), waiting for a
// delivered record at offset hw-1 hangs forever. Instead, termination uses a
// two-signal strategy:
//
//  1. lsoReachedHW: set when any fetch response reports LastStableOffset >= hw.
//     LSO >= hw means all transactions up to the high-watermark are resolved
//     (committed or aborted), so no more committed records will ever arrive.
//     Unlike HighWatermark, LSO does NOT imply "all committed records already
//     delivered" — it only implies "no new ones are coming". For committed
//     changelogs (including large multi-response), LSO == hw is reported as
//     early as the first fetch response, while the actual committed records may
//     span many subsequent responses. Therefore lsoReachedHW alone is NOT a
//     safe break condition.
//
//  2. Bounded-poll confirm: after lsoReachedHW is true, switch to
//     pollLSOConfirmTimeout for each subsequent PollFetches call. kgo
//     pre-fetches eagerly; any committed records still in flight arrive within
//     one broker RTT (typically <50ms), well under the timeout. A deadline
//     expiry after lsoReachedHW means the cursor is at HW with no more
//     committed records — deterministic termination with at most 500ms overhead
//     on the aborted-tail path (down from 2s).
//
// Termination cases:
//   - ALO / committed-tail fast path: lastAppliedOffset reaches hw-1. Break
//     immediately; no bounded poll needed.
//   - Aborted tail (committed records + aborted tail): lsoReachedHW fires on
//     the response delivering committed records (LSO==hw once abort marker
//     written). The bounded confirm-poll expires quickly (cursor at HW, no
//     more committed records) and we break. Typically <500ms.
//   - Large multi-response (committed only): lsoReachedHW fires on response 1
//     (LSO==hw for committed-only changelogs). The bounded confirm-polls
//     continue to drain responses 2..N (each arrives within RTT). Eventually
//     lastAppliedOffset reaches hw-1 and the ALO fast-path fires, or the final
//     confirm-poll expires after all committed records are consumed.
//   - All-aborted (no committed records): kgo does not buffer empty-record
//     responses (hasErrorsOrRecords returns false). PollFetches never returns,
//     lsoReachedHW is never set, and the loop blocks on the caller context.
//     The caller must supply a context with a deadline for this edge.
//
// extraOpts are appended to the internal kgo consumer options; tests may use
// them to override fetch limits (e.g. kgo.FetchMaxPartitionBytes).
func RestoreFromChangelog(
	ctx context.Context,
	brokers []string,
	changelogTopic string,
	partition int32,
	checkpointOffset int64,
	backend gstream.StoreBackend,
	storeName string,
	catchUpTimeout time.Duration,
	extraOpts ...kgo.Opt,
) (highWatermark int64, err error) {
	if catchUpTimeout <= 0 {
		catchUpTimeout = pollLSOConfirmTimeout
	}
	if len(brokers) == 0 {
		return 0, fmt.Errorf("state.RestoreFromChangelog: brokers must not be empty")
	}
	if changelogTopic == "" {
		return 0, fmt.Errorf("state.RestoreFromChangelog: changelogTopic must not be empty")
	}
	if storeName == "" {
		return 0, fmt.Errorf("state.RestoreFromChangelog: storeName must not be empty")
	}

	hw, err := fetchHighWatermark(ctx, brokers, changelogTopic, partition)
	if err != nil {
		return 0, fmt.Errorf("state.RestoreFromChangelog: fetch high-watermark: %w", err)
	}

	earliest, err := fetchEarliestOffset(ctx, brokers, changelogTopic, partition)
	if err != nil {
		return 0, fmt.Errorf("state.RestoreFromChangelog: fetch earliest offset: %w", err)
	}
	startOffset := checkpointOffset + 1
	if startOffset < earliest {
		startOffset = earliest
	}
	if hw == 0 || startOffset >= hw {
		// Nothing to restore; leave state and checkpoint untouched.
		return hw, nil
	}

	// kgo suppresses empty fetch responses. If every offset in the remaining
	// range belongs to aborted transactions, PollFetches cannot expose the LSO
	// and may block until the caller context expires. Probe the raw response so
	// this case terminates without relying on a caller deadline.
	lso, _, err := fetchReadCommittedProbe(
		ctx, brokers, changelogTopic, partition, startOffset,
	)
	if err != nil {
		return 0, fmt.Errorf("state.RestoreFromChangelog: stable-offset probe: %w", err)
	}

	consumerOpts := append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			changelogTopic: {
				partition: kgo.NewOffset().At(startOffset),
			},
		}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	}, extraOpts...)
	consumer, err := kgo.NewClient(consumerOpts...)
	if err != nil {
		return 0, fmt.Errorf("state.RestoreFromChangelog: create consumer: %w", err)
	}
	defer consumer.Close()

	lastAppliedOffset := int64(-1)
	var mutations []gstream.StoreMutation

	// lsoReachedHW is set to true when a fetch response reports
	// FetchPartition.LastStableOffset >= hw. LSO >= hw is a deterministic
	// signal that all transactions up to the high-watermark are resolved
	// (committed or aborted), so no new committed records will ever arrive.
	// This is superior to HighWatermark as a termination hint because it
	// directly reflects transaction resolution state. However, LSO >= hw
	// does NOT mean all committed records have already been delivered
	// (especially for large changelogs spanning multiple fetch responses),
	// so it triggers a bounded confirm-poll rather than an immediate break.
	lsoReachedHW := lso >= hw

	// applyFetches accumulates opaque backend mutations, tracks
	// lastAppliedOffset, and updates lsoReachedHW via partition metadata.
	// Returns false (and sets err) on the first Pebble batch error.
	applyFetches := func(fs kgo.Fetches) bool {
		if fs.Err() != nil {
			err = fmt.Errorf("state.RestoreFromChangelog: poll fetches: %w", fs.Err())
			return false
		}
		fs.EachRecord(func(r *kgo.Record) {
			value := append([]byte(nil), r.Value...)
			if len(r.Value) == 0 {
				value = nil
			}
			mutations = append(mutations, gstream.StoreMutation{
				Key: append([]byte(nil), r.Key...), Value: value,
			})
			lastAppliedOffset = r.Offset
		})
		// Check LSO from partition metadata in this fetch response.
		// LastStableOffset >= hw means all transactions up to HW are
		// resolved; no more committed records will ever arrive for this
		// partition. This fires as soon as the broker has resolved all
		// transactions — for committed-only changelogs this is typically
		// on the first response (LSO==hw immediately), while for
		// aborted-tail changelogs it fires after the abort marker is
		// written (also on the first response delivering committed records,
		// since the abort was pre-written before restore started).
		fs.EachPartition(func(ftp kgo.FetchTopicPartition) {
			if ftp.Topic == changelogTopic && ftp.Partition == partition {
				if ftp.LastStableOffset >= hw {
					lsoReachedHW = true
				}
			}
		})
		return true
	}

	for {
		// Check context before each blocking poll.
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("state.RestoreFromChangelog: context done during consume: %w", ctx.Err())
		default:
		}

		// Choose the poll context:
		//   - Before lsoReachedHW: use caller ctx (no deadline; we must wait
		//     for the broker to report LSO >= hw in a fetch response).
		//   - After lsoReachedHW: use a bounded confirm-poll context
		//     (pollLSOConfirmTimeout). LSO >= hw means no new committed records
		//     will arrive, but records from the current fetch cycle may still
		//     be in flight (kgo pre-fetches eagerly; they arrive within one
		//     broker RTT, typically <50ms). A deadline expiry after lsoReachedHW
		//     means the cursor is at HW with no more committed records to drain.
		var pollCtx context.Context
		var pollCancel context.CancelFunc
		if lsoReachedHW {
			pollCtx, pollCancel = context.WithTimeout(ctx, catchUpTimeout)
		} else {
			pollCtx = ctx
			pollCancel = func() {} // no-op
		}

		// Blocking poll: waits for the next fetch response (or pollCtx deadline).
		fetches := consumer.PollFetches(pollCtx)
		// Capture whether deadline fired before cancelling (pollCancel() would
		// change pollCtx.Err() to Canceled, masking DeadlineExceeded).
		pollDeadlineExceeded := lsoReachedHW && errors.Is(pollCtx.Err(), context.DeadlineExceeded)
		pollCancel()

		if pollDeadlineExceeded {
			// Deadline expired during post-LSO confirm-poll. This is the
			// deterministic signal that kgo's cursor is at HW with no more
			// committed records to deliver (LSO >= hw already confirmed that
			// no new committed records will arrive). Not an error unless the
			// caller's ctx was also cancelled.
			if ctx.Err() != nil {
				return 0, fmt.Errorf("state.RestoreFromChangelog: context done during consume: %w", ctx.Err())
			}
			// LSO confirmed; cursor at HW; all committed records consumed. Done.
			break
		}

		if fetches.IsClientClosed() {
			break
		}
		if !applyFetches(fetches) {
			return 0, err
		}

		// ALO / committed-tail fast path: last record delivered exactly at hw-1.
		// No bounded poll needed — nothing more exists.
		if lastAppliedOffset >= hw-1 {
			break
		}
	}

	if lastAppliedOffset < 0 {
		if lsoReachedHW {
			if err := backend.Restore(storeName, nil, hw-1); err != nil {
				return 0, fmt.Errorf("state.RestoreFromChangelog: checkpoint resolved empty range: %w", err)
			}
		}
		return hw, nil
	}

	if err := backend.Restore(storeName, mutations, lastAppliedOffset); err != nil {
		return 0, fmt.Errorf("state.RestoreFromChangelog: restore backend: %w", err)
	}

	return hw, nil
}

// fetchReadCommittedProbe performs one raw Fetch request at startOffset. Unlike
// kgo.PollFetches, the raw protocol response is returned even when every record
// is filtered by read-committed isolation, allowing callers to observe LSO.
func fetchReadCommittedProbe(
	ctx context.Context,
	brokers []string,
	topic string,
	partition int32,
	startOffset int64,
) (lastStableOffset int64, hasVisibleBatch bool, err error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return 0, false, fmt.Errorf("create kgo client: %w", err)
	}
	defer client.Close()
	leader, err := fetchPartitionLeader(ctx, client, topic, partition)
	if err != nil {
		return 0, false, err
	}

	request := kmsg.NewPtrFetchRequest()
	request.IsolationLevel = 1 // READ_COMMITTED
	request.MaxWaitMillis = 0
	request.MinBytes = 1

	requestPartition := kmsg.NewFetchRequestTopicPartition()
	requestPartition.Partition = partition
	requestPartition.FetchOffset = startOffset
	requestPartition.PartitionMaxBytes = 1 << 20
	requestTopic := kmsg.NewFetchRequestTopic()
	requestTopic.Topic = topic
	requestTopic.Partitions = append(requestTopic.Partitions, requestPartition)
	request.Topics = append(request.Topics, requestTopic)

	responseAny, err := client.Broker(int(leader)).Request(ctx, &fetchRequestV12{FetchRequest: request})
	if err != nil {
		return 0, false, fmt.Errorf("fetch request: %w", err)
	}
	response, ok := responseAny.(*kmsg.FetchResponse)
	if !ok {
		return 0, false, fmt.Errorf("unexpected response type %T", responseAny)
	}
	for _, topicResponse := range response.Topics {
		if topicResponse.Topic != topic {
			continue
		}
		for _, partitionResponse := range topicResponse.Partitions {
			if partitionResponse.Partition != partition {
				continue
			}
			if kafkaErr := kerr.ErrorForCode(partitionResponse.ErrorCode); kafkaErr != nil {
				return 0, false, fmt.Errorf("topic %q partition %d: %w", topic, partition, kafkaErr)
			}
			return partitionResponse.LastStableOffset, len(partitionResponse.RecordBatches) > 0, nil
		}
	}
	return 0, false, fmt.Errorf("topic %q partition %d not found in response", topic, partition)
}

// fetchRequestV12 keeps topic names on the wire. Fetch v13+ requires topic IDs,
// which raw callers do not have unless they reproduce franz-go's fetch session.
type fetchRequestV12 struct {
	*kmsg.FetchRequest
}

func (*fetchRequestV12) MaxVersion() int16 { return 12 }

func fetchPartitionLeader(ctx context.Context, client *kgo.Client, topic string, partition int32) (int32, error) {
	request := kmsg.NewPtrMetadataRequest()
	request.AllowAutoTopicCreation = false
	requestTopic := kmsg.NewMetadataRequestTopic()
	requestTopic.Topic = &topic
	request.Topics = append(request.Topics, requestTopic)
	responseAny, err := client.Request(ctx, request)
	if err != nil {
		return 0, fmt.Errorf("metadata request: %w", err)
	}
	response := responseAny.(*kmsg.MetadataResponse)
	for _, topicResponse := range response.Topics {
		if topicResponse.Topic == nil || *topicResponse.Topic != topic {
			continue
		}
		if kafkaErr := kerr.ErrorForCode(topicResponse.ErrorCode); kafkaErr != nil {
			return 0, fmt.Errorf("metadata topic %q: %w", topic, kafkaErr)
		}
		for _, partitionResponse := range topicResponse.Partitions {
			if partitionResponse.Partition == partition {
				return partitionResponse.Leader, nil
			}
		}
	}
	return 0, fmt.Errorf("metadata topic %q partition %d not found", topic, partition)
}

// fetchHighWatermark fetches the Kafka high-watermark (end offset) for the given
// (topic, partition) using ListOffsets with Timestamp=-1. HW is the next offset to
// be written; the last existing record (if any) is at offset HW-1.
func fetchHighWatermark(ctx context.Context, brokers []string, topic string, partition int32) (int64, error) {
	return fetchOffset(ctx, brokers, topic, partition, -1, "fetchHighWatermark")
}

func fetchEarliestOffset(ctx context.Context, brokers []string, topic string, partition int32) (int64, error) {
	return fetchOffset(ctx, brokers, topic, partition, -2, "fetchEarliestOffset")
}

func fetchOffset(
	ctx context.Context,
	brokers []string,
	topic string,
	partition int32,
	timestamp int64,
	operation string,
) (int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return 0, fmt.Errorf("%s: create kgo client: %w", operation, err)
	}
	defer cl.Close()

	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1

	topicPartition := kmsg.NewListOffsetsRequestTopicPartition()
	topicPartition.Partition = partition
	topicPartition.Timestamp = timestamp

	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rt.Partitions = append(rt.Partitions, topicPartition)
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return 0, fmt.Errorf("%s: ListOffsets request: %w", operation, err)
	}

	for _, topicResp := range resp.Topics {
		if topicResp.Topic != topic {
			continue
		}
		for _, partResp := range topicResp.Partitions {
			if partResp.Partition != partition {
				continue
			}
			if kerErr := kerr.ErrorForCode(partResp.ErrorCode); kerErr != nil {
				return 0, fmt.Errorf("%s: topic %q partition %d: %w",
					operation, topic, partition, kerErr)
			}
			if partResp.Offset < 0 {
				return 0, nil
			}
			return partResp.Offset, nil
		}
	}
	return 0, fmt.Errorf("%s: topic %q partition %d not found in response",
		operation, topic, partition)
}
