package kafka

import (
	"context"
	"log/slog"
	"sync"

	gstream "mortz.dev/go/gstream"
)

// processBatch runs Step 1 of the ALO loop for a single polled batch: it calls
// process for each record in order and collects outputs.
//
// Return semantics:
//   - (outputs, true)  — all records processed successfully; caller should proceed
//     to produce outputs then commit offsets.
//   - (nil, false)     — at least one record returned an error; caller must skip
//     BOTH produce and commit so the whole batch is redelivered (ALO §4.1).
//
// Batch-failure semantics: whole-batch redelivery. On any process error we discard
// all partial outputs accumulated so far and return immediately. This is the
// simplest correct choice for ALO: it avoids partial produces whose corresponding
// offsets are never committed, and it defers per-record retry logic to a later phase.
func processBatch(
	ctx context.Context,
	logger gstream.Logger,
	inRecords []InRecord,
	process ProcessFunc,
) (outputs []OutRecord, ok bool) {
	for _, in := range inRecords {
		out, err := process(ctx, in)
		if err != nil {
			logger.Error("process error; aborting batch",
				slog.String("topic", in.Topic),
				slog.Int("partition", int(in.Partition)),
				slog.Int64("offset", in.Offset),
				slog.Any("error", err),
			)
			// Return nil outputs and ok=false so the caller skips produce AND commit.
			// The whole batch will be redelivered on the next poll — at-least-once.
			return nil, false
		}
		outputs = append(outputs, out...)
	}
	return outputs, true
}

// processBatchConcurrent preserves record order within each partition while
// processing independent partitions concurrently. Output ordering is stable by
// first partition appearance in fetched batch.
func processBatchConcurrent(
	ctx context.Context,
	logger gstream.Logger,
	inRecords []InRecord,
	process ProcessFunc,
	maxThreads int,
) ([]OutRecord, bool) {
	if maxThreads <= 1 || len(inRecords) < 2 {
		return processBatch(ctx, logger, inRecords, process)
	}

	type partitionBatch struct {
		records []InRecord
	}
	type partitionResult struct {
		outputs []OutRecord
		ok      bool
	}

	groups := make([]partitionBatch, 0)
	groupIndex := make(map[int32]int)
	for _, record := range inRecords {
		index, exists := groupIndex[record.Partition]
		if !exists {
			index = len(groups)
			groupIndex[record.Partition] = index
			groups = append(groups, partitionBatch{})
		}
		groups[index].records = append(groups[index].records, record)
	}
	if len(groups) == 1 {
		return processBatch(ctx, logger, inRecords, process)
	}

	workers := min(maxThreads, len(groups))
	semaphore := make(chan struct{}, workers)
	results := make([]partitionResult, len(groups))
	var waitGroup sync.WaitGroup
	for index := range groups {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			outputs, ok := processBatch(ctx, logger, groups[index].records, process)
			results[index] = partitionResult{outputs: outputs, ok: ok}
		}(index)
	}
	waitGroup.Wait()

	var outputs []OutRecord
	for _, result := range results {
		if !result.ok {
			return nil, false
		}
		outputs = append(outputs, result.outputs...)
	}
	return outputs, true
}
