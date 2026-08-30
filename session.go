package gstream

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/mortezaPRK/gstream/internal/topology"
)

// sessionStore is the interface the session-merge processor asserts against at
// runtime. The runtime supplies a *state.KeyValueStore[[]byte,[]byte] which
// satisfies this interface via its RangeForKey / WindowPut / WindowDelete methods.
//
// Using a narrow interface avoids importing internal/state directly (which would
// create an import cycle: internal/state → gstream via Serde[T]).
//
// RangeForKey is a T1-amendment method that decodes composite keys store-side and
// hands sessionStart directly to the DSL, keeping key-format knowledge in internal/state.
type sessionStore interface {
	// RangeForKey iterates all entries stored under kBytes, calling fn with each
	// entry's sessionStart (int64) and raw value bytes (safe to retain).
	RangeForKey(kBytes []byte, fn func(sessionStart int64, val []byte) bool) error
	// WindowPut stores val under the composite (kBytes, sessionStart) key.
	WindowPut(kBytes []byte, sessionStart int64, val []byte) error
	// WindowDelete removes the entry for composite (kBytes, sessionStart).
	WindowDelete(kBytes []byte, sessionStart int64) error
}

// SessionWindows defines an inactivity-gap session policy.
// Records within gapMs of an existing session boundary are merged into it.
type SessionWindows struct{ gapMs int64 }

// SessionWindow creates a SessionWindows with the given inactivity gap.
// Panics when gap <= 0.
func SessionWindow(gap time.Duration) SessionWindows {
	gapMs := gap.Milliseconds()
	if gapMs <= 0 {
		panic("gstream: SessionWindow gap must be > 0")
	}
	return SessionWindows{gapMs: gapMs}
}

// GapMs returns the inactivity gap in milliseconds.
func (s SessionWindows) GapMs() int64 { return s.gapMs }

// SessionWindowedStream is the intermediate typed stream produced by
// KGroupedStream.SessionWindowedBy. Callers proceed to Count or Aggregate
// to produce a windowed KTable.
type SessionWindowedStream[K, V any] struct {
	builder     *StreamBuilder
	nodeName    string
	keySerde    Serde[K]
	valSerde    Serde[V]
	sessions    SessionWindows
	graceMs     int64
	extractorFn TimestampExtractor

	// lateCount is an atomic counter incremented for every record dropped as
	// late (ts < lateBoundary). Shared across all Aggregate/Count invocations
	// built from this stream; exposed via LateCount() for tests.
	lateCount *atomic.Int64
}

// SessionWindowedBy attaches a SessionWindows to a KGroupedStream, returning a
// SessionWindowedStream ready for Count or Aggregate.
func (g KGroupedStream[K, V]) SessionWindowedBy(w SessionWindows) SessionWindowedStream[K, V] {
	return SessionWindowedStream[K, V]{
		builder:     g.builder,
		nodeName:    g.nodeName,
		keySerde:    g.keySerde,
		valSerde:    g.valSerde,
		sessions:    w,
		graceMs:     0,
		extractorFn: nil,
		lateCount:   new(atomic.Int64),
	}
}

// WithGrace returns a copy of s with the late-record grace period set to d.
func (s SessionWindowedStream[K, V]) WithGrace(d time.Duration) SessionWindowedStream[K, V] {
	s.graceMs = d.Milliseconds()
	return s
}

// WithTimestampExtractor returns a copy of s that uses fn to extract the event
// timestamp from each record instead of r.Timestamp.
func (s SessionWindowedStream[K, V]) WithTimestampExtractor(fn TimestampExtractor) SessionWindowedStream[K, V] {
	s.extractorFn = fn
	return s
}

// LateCount returns the current number of records dropped as late.
func (s SessionWindowedStream[K, V]) LateCount() int64 {
	return s.lateCount.Load()
}

// Count accumulates the number of records per session key, storing counts in storeName.
// Delegates to Aggregate[int64] with zero=0, agg=+1, merge=a+b, JSONSerde[int64].
func (s SessionWindowedStream[K, V]) Count(storeName string) KTable[Windowed[K], int64] {
	return s.Aggregate[int64](
		storeName,
		func() int64 { return 0 },
		func(_ K, _ V, acc int64) int64 { return acc + 1 },
		func(_ K, a, b int64) int64 { return a + b },
		JSONSerde[int64]{},
	)
}

// Aggregate accumulates records per session key using caller-supplied functions,
// storing the result in storeName.
//
//   - initFn: returns the zero accumulator for a session not yet seen.
//   - aggFn: combines current key, incoming value, and existing accumulator.
//   - mergeFn: merges two session sub-accumulators when sessions are bridged;
//     must be associative and commutative.
//   - accSerde: serializes/deserializes the accumulator.
//
// Session merge predicate (spike-confirmed): an existing session [sStart, sEnd] matches
// record ts iff sEnd+gapMs >= ts && sStart-gapMs <= ts. All matching sessions and the
// new record are merged into one session [min(sStarts,ts), max(sEnds,ts)] with accumulated
// values folded via mergeFn then aggFn for the new record.
//
// The processor does NOT call ctx.Forward; KTable has no downstream consumer yet.
func (s SessionWindowedStream[K, V]) Aggregate[A any](
	storeName string,
	initFn func() A,
	aggFn func(K, V, A) A,
	mergeFn func(K, A, A) A, // must be associative & commutative
	accSerde Serde[A],
) KTable[Windowed[K], A] {
	name := s.builder.nextName("session-aggregate")

	gapMs := s.sessions.gapMs
	graceMs := s.graceMs
	extractorFn := s.extractorFn
	keySerde := s.keySerde
	lateCount := s.lateCount

	s.builder.internal.AddStatefulProcessor(name, func(_ context.Context, r topology.Record, ctx topology.ProcessorContext) error {
		var ts int64
		if extractorFn != nil {
			ts = extractorFn(r)
		} else {
			ts = r.Timestamp
		}

		ctx.AdvanceStreamTime(ts)

		lateBoundary := ctx.StreamTime() - gapMs - graceMs
		if ts < lateBoundary {
			lateCount.Add(1)
			slog.Debug("session aggregate: late record dropped",
				"storeName", storeName,
				"ts", ts,
				"streamTime", ctx.StreamTime(),
				"lateBoundary", lateBoundary,
			)
			return nil
		}

		k, ok := r.Key.(K)
		if !ok {
			return fmt.Errorf("session aggregate %q: key type mismatch: got %T, want %T", storeName, r.Key, *new(K))
		}
		v, ok := r.Value.(V)
		if !ok {
			return fmt.Errorf("session aggregate %q: value type mismatch: got %T, want %T", storeName, r.Value, *new(V))
		}

		raw := ctx.Store(storeName)
		if raw == nil {
			return fmt.Errorf("session aggregate %q: store not wired", storeName)
		}
		store, ok := raw.(sessionStore)
		if !ok {
			return fmt.Errorf("session aggregate %q: store type mismatch: got %T, want sessionStore", storeName, raw)
		}

		kBytes, err := keySerde.Serialize(k)
		if err != nil {
			return fmt.Errorf("session aggregate %q: encode key: %w", storeName, err)
		}

		// Scan all existing sessions for this key and collect those that match ts.
		type matchedSession struct {
			start int64
			end   int64
			acc   A
		}
		var matched []matchedSession

		if rangeErr := store.RangeForKey(kBytes, func(sStart int64, valBytes []byte) bool {
			sEnd, accBytes, decErr := DecodeSessionValue(valBytes)
			if decErr != nil {
				slog.Warn("session aggregate: skip malformed session value",
					"storeName", storeName, "sStart", sStart, "err", decErr)
				return true
			}
			// Spike-confirmed match predicate.
			if sEnd+gapMs >= ts && sStart-gapMs <= ts {
				acc, decErr := accSerde.Deserialize(accBytes)
				if decErr != nil {
					slog.Warn("session aggregate: skip corrupt accumulator",
						"storeName", storeName, "sStart", sStart, "err", decErr)
					return true
				}
				matched = append(matched, matchedSession{start: sStart, end: sEnd, acc: acc})
			}
			return true
		}); rangeErr != nil {
			return fmt.Errorf("session aggregate %q: scan sessions: %w", storeName, rangeErr)
		}

		// Compute merged bounds: min of all sStarts and ts, max of all sEnds and ts.
		mergedStart := ts
		mergedEnd := ts

		// Seed the accumulator from matched sessions.  Only use initFn() when
		// there are no matched sessions (brand-new session), because initFn() is
		// not guaranteed to be an identity element for mergeFn.  For example,
		// init=100 with mergeFn=min would wrongly fold 100 into the result if we
		// always start from initFn().
		var mergedAcc A
		if len(matched) == 0 {
			mergedAcc = initFn()
		} else {
			mergedAcc = matched[0].acc
			for _, m := range matched[1:] {
				mergedAcc = mergeFn(k, mergedAcc, m.acc)
			}
		}

		for _, m := range matched {
			if m.start < mergedStart {
				mergedStart = m.start
			}
			if m.end > mergedEnd {
				mergedEnd = m.end
			}
		}

		// Fold in new record.
		mergedAcc = aggFn(k, v, mergedAcc)

		// Delete all matched old sessions before writing the merged one.
		for _, m := range matched {
			if delErr := store.WindowDelete(kBytes, m.start); delErr != nil {
				return fmt.Errorf("session aggregate %q: delete old session sStart=%d: %w", storeName, m.start, delErr)
			}
		}

		// Write merged session: EncodeSessionValue(mergedEnd, serialize(mergedAcc)).
		accBytes, err := accSerde.Serialize(mergedAcc)
		if err != nil {
			return fmt.Errorf("session aggregate %q: encode accumulator: %w", storeName, err)
		}
		if putErr := store.WindowPut(kBytes, mergedStart, EncodeSessionValue(mergedEnd, accBytes)); putErr != nil {
			return fmt.Errorf("session aggregate %q: write merged session: %w", storeName, putErr)
		}

		// KTable has no downstream consumer; ctx.Forward intentionally omitted.
		return nil
	}, []string{storeName}, s.nodeName)

	// Internal sink to satisfy topology.Builder.Build()'s >=1 sink invariant.
	sinkName := s.builder.nextName("session-ktable-out")
	s.builder.internal.AddSink(sinkName, name)
	s.builder.internalSinks[sinkName] = struct{}{}

	// Register SessionStoreBinding so the runtime can open the store and configure
	// the session sweeper. EncodeKey/DecodeKey are stubs: the active processing path
	// uses the sessionStore interface. gstream cannot import internal/state (cycle).
	s.builder.sessionStores[storeName] = SessionStoreBinding{
		StoreBinding: StoreBinding{
			StoreName:      storeName,
			ChangelogTopic: storeName,
			EncodeKey: func(x any) ([]byte, error) {
				return nil, fmt.Errorf("SessionStoreBinding %q EncodeKey: use sessionStore interface", storeName)
			},
			DecodeKey: func(b []byte) (any, error) {
				return nil, fmt.Errorf("SessionStoreBinding %q DecodeKey: use sessionStore interface", storeName)
			},
			EncodeVal: func(x any) ([]byte, error) {
				a, ok := x.(A)
				if !ok {
					return nil, fmt.Errorf("SessionStoreBinding %q EncodeVal: expected %T, got %T", storeName, *new(A), x)
				}
				return accSerde.Serialize(a)
			},
			DecodeVal: func(b []byte) (any, error) {
				return accSerde.Deserialize(b)
			},
		},
		GapMs:     gapMs,
		GraceMs:   graceMs,
		LateCount: lateCount.Load,
	}

	// keySerde unset: windowed/session KTables are not stream-joinable in P4a (key is Windowed[K]).
	return KTable[Windowed[K], A]{
		builder:   s.builder,
		nodeName:  name,
		storeName: storeName,
	}
}

// EncodeSessionValue encodes the session end timestamp and accumulator bytes
// into a single byte slice:
//
//	int64(sessionEnd) big-endian (8 bytes) ‖ accBytes
//
// This format is owned by the gstream package (not internal/state), so the DSL
// can decode it without importing internal/state.
// Exported so T4 runtime sweep can reuse it.
func EncodeSessionValue(sessionEnd int64, accBytes []byte) []byte {
	out := make([]byte, 8+len(accBytes))
	binary.BigEndian.PutUint64(out[0:8], uint64(sessionEnd))
	copy(out[8:], accBytes)
	return out
}

// DecodeSessionValue reverses EncodeSessionValue.
// Returns an error when raw is shorter than 8 bytes.
// Exported so T4 runtime sweep can reuse it.
func DecodeSessionValue(raw []byte) (sessionEnd int64, accBytes []byte, err error) {
	if len(raw) < 8 {
		return 0, nil, fmt.Errorf("gstream: DecodeSessionValue: too short (%d bytes, need >= 8)", len(raw))
	}
	sessionEnd = int64(binary.BigEndian.Uint64(raw[0:8]))
	acc := make([]byte, len(raw)-8)
	copy(acc, raw[8:])
	return sessionEnd, acc, nil
}
