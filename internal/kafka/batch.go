package kafka

import (
	"context"
	"log/slog"
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
	logger *slog.Logger,
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
