package gstream

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/mortezaPRK/gstream/internal/topology"
	"github.com/mortezaPRK/gstream/internal/winkey"
)

// JoinWindows configures a stream-stream windowed inner join.
// A B-side record at timestamp tsB is joined with an A-side record at tsA when:
//
//	max(0, tsA-Before-Grace) <= tsB <= tsA+After
//
// Before and After define the (optionally asymmetric) join window;
// Grace extends the lower lookup bound to accept late-arriving B-side records.
// Formula frozen by the P4b-F2-C0 bounds spike (internal/state/ssjoin_bounds_spike_test.go).
type JoinWindows struct {
	Before time.Duration // lower half-width: look back Before from tsA
	After  time.Duration // upper half-width: look forward After from tsA
	Grace  time.Duration // late-arrival tolerance: extends the lower bound leftward
}

// joinWindowDef implements WindowDefinition so the runtime's sweep/restore loops
// (which iterate WindowStoreBindings generically) treat a stream-stream join's
// B-side store like any other windowed store.
//
// retentionMs = Before.Milliseconds() + After.Milliseconds() — the total span a
// buffered B-side record must be retained before it can be evicted by the sweeper.
// graceMs is tracked separately so the sweep boundary can account for late records.
//
// Assign returns nil: stream-stream join does not assign records to fixed windows
// the way tumbling/hopping does.  The C2 processor performs a RangeCompositeBytes
// scan instead; the sweeper does not call Assign for join stores.
type joinWindowDef struct {
	retentionMs int64 // Before.Milliseconds() + After.Milliseconds()
	graceMs     int64 // Grace.Milliseconds()
}

// newJoinWindowDef constructs a joinWindowDef from a JoinWindows config.
func newJoinWindowDef(w JoinWindows) joinWindowDef {
	return joinWindowDef{
		retentionMs: w.Before.Milliseconds() + w.After.Milliseconds(),
		graceMs:     w.Grace.Milliseconds(),
	}
}

// Assign returns nil: stream-stream join uses range scans, not per-record window assignment.
func (d joinWindowDef) Assign(_ int64) []Window { return nil }

// MaxSizeMs returns the total retention span in milliseconds.
// The sweeper uses this to compute the eviction boundary:
//
//	evictBefore = streamTime - MaxSizeMs() - graceMs
func (d joinWindowDef) MaxSizeMs() int64 { return d.retentionMs }

// Compile-time proof: joinWindowDef satisfies WindowDefinition.
var _ WindowDefinition = joinWindowDef{}

// ssJoinStore is the minimal store surface the C2 join processor needs to buffer
// B-side records and scan them when an A-side record arrives.
//
// The runtime supplies a *state.KeyValueStore[[]byte,[]byte]; gstream cannot
// import internal/state directly (import cycle: internal/state → gstream via
// Serde[T]), so a narrow interface is used and the runtime type-asserts at
// processor startup.  The compile-time proof that *state.KeyValueStore[[]byte,[]byte]
// satisfies this interface lives in internal/state/ssjoin_store_assert.go.
//
// Signatures match *state.KeyValueStore[[]byte,[]byte] exactly (P4b-F2-C1 freeze):
//
//	WindowPut(kBytes []byte, windowStart int64, val []byte) error
//	RangeCompositeBytes(lower, upper []byte, fn func(compositeKey, val []byte) bool) error
type ssJoinStore interface {
	// WindowPut stores val under the composite (kBytes, windowStart) key.
	WindowPut(kBytes []byte, windowStart int64, val []byte) error
	// RangeCompositeBytes iterates per-store composite keys in [lower, upper),
	// calling fn with the composite key and value bytes (safe to retain after fn
	// returns).  Return false from fn to stop early.
	RangeCompositeBytes(lower, upper []byte, fn func(compositeKey, val []byte) bool) error
}

// ssJoinScanBounds computes the FROZEN inclusive lower and exclusive upper composite
// keys for scanning the other store when a record arrives at ts.
// The composite key format is owned by internal/winkey.CompositeKey.
//
//	loMs = max(0, ts - before - grace)
//	hiMs = ts + after, capped at MaxInt64
//	lower = winkey.CompositeKey(kBytes, loMs)   // inclusive
//	upper = winkey.CompositeKey(kBytes, hiMs+1) // exclusive (hiMs+1 may wrap; see spike)
func ssJoinScanBounds(kBytes []byte, ts, beforeMs, afterMs, graceMs int64) (lower, upper []byte) {
	sub := beforeMs + graceMs
	var loMs int64
	if ts > sub {
		loMs = ts - sub
	}
	var hiMs int64
	if afterMs > 0 && ts > math.MaxInt64-afterMs {
		hiMs = math.MaxInt64
	} else {
		hiMs = ts + afterMs
	}
	return winkey.CompositeKey(kBytes, loMs), winkey.CompositeKey(kBytes, hiMs+1)
}

// Join performs a stream-stream windowed inner join between s (left, KStream[K,V1])
// and other (right, KStream[K,V2]).
//
// Each side buffers incoming records in its own window store. When a left-side
// record arrives it scans the right store for temporally overlapping records (and
// vice versa). Overlapping means:
//
//	max(0, tsA - before - grace) <= tsB <= tsA + after
//
// where A is the triggering side and B is the opposite side. The bounds formula is
// frozen by the P4b-F2-C0 spike (internal/state/ssjoin_bounds_spike_test.go).
//
// joiner(v1, v2) always receives the LEFT value as the first argument and the RIGHT
// value as the second, regardless of which side triggered the emit.
//
// Retention: MaxSizeMs() = Before+After. Sweeper evicts at streamTime-MaxSizeMs-Grace.
// A right record at tsB must survive until the latest matching left at tsB+Before+Grace.
// Eviction fires at streamTime > tsB+Before+After+Grace, which is always later than
// tsB+Before+Grace for non-negative After. Both stores are correctly retained for all
// non-negative Before/After/Grace values.
func (s KStream[K, V1]) Join[V2, VR any](
	other KStream[K, V2],
	joiner func(V1, V2) VR,
	windows JoinWindows,
	keySerde Serde[K],
	leftSerde Serde[V1],
	rightSerde Serde[V2],
	outValSerde Serde[VR],
) KStream[K, VR] {
	s = s.ensureRepartition(keySerde, leftSerde)
	other = other.ensureRepartition(keySerde, rightSerde)
	b := s.builder

	leftStoreName := b.nextName("ssjoin-left-store")
	rightStoreName := b.nextName("ssjoin-right-store")

	winDef := newJoinWindowDef(windows)
	beforeMs := windows.Before.Milliseconds()
	afterMs := windows.After.Milliseconds()
	graceMs := windows.Grace.Milliseconds()

	// Register two WindowStoreBindings — same structure as windowed.go.
	// Both stores hold raw []byte → []byte; the processor performs its own serde.
	for _, name := range []string{leftStoreName, rightStoreName} {
		storeName := name // capture loop var
		b.windowStores[storeName] = WindowStoreBinding{
			StoreBinding: StoreBinding{
				StoreName:      storeName,
				ChangelogTopic: storeName,
				EncodeKey: func(_ any) ([]byte, error) {
					return nil, fmt.Errorf("WindowStoreBinding %q EncodeKey: use ssJoinStore.WindowPut interface", storeName)
				},
				DecodeKey: func(_ []byte) (any, error) {
					return nil, fmt.Errorf("WindowStoreBinding %q DecodeKey: use ssJoinStore.WindowPut interface", storeName)
				},
				EncodeVal: func(x any) ([]byte, error) {
					raw, ok := x.([]byte)
					if !ok {
						return nil, fmt.Errorf("WindowStoreBinding %q EncodeVal: expected []byte, got %T", storeName, x)
					}
					return raw, nil
				},
				DecodeVal: func(raw []byte) (any, error) { return raw, nil },
			},
			WindowDef: winDef,
			GraceMs:   graceMs,
		}
	}

	// Left processor: buffer V1 in leftStore, scan rightStore, emit joiner(v1, v2).
	leftNode := b.nextName("ssjoin-left")
	b.internal.AddStatefulProcessor(leftNode,
		func(_ context.Context, r topology.Record, ctx topology.ProcessorContext) error {
			ts := r.Timestamp
			ctx.AdvanceStreamTime(ts)

			// Late-drop: same boundary formula as windowed.go line 122.
			lateBoundary := ctx.StreamTime() - winDef.MaxSizeMs() - graceMs
			if ts < lateBoundary {
				slog.Debug("ssjoin left: late record dropped",
					"ts", ts, "streamTime", ctx.StreamTime(), "lateBoundary", lateBoundary)
				return nil
			}

			k, ok := r.Key.(K)
			if !ok {
				return fmt.Errorf("ssjoin left %q: key type mismatch: got %T, want %T",
					leftStoreName, r.Key, *new(K))
			}
			v1, ok := r.Value.(V1)
			if !ok {
				return fmt.Errorf("ssjoin left %q: value type mismatch: got %T, want %T",
					leftStoreName, r.Value, *new(V1))
			}

			kBytes, err := keySerde.Serialize(k)
			if err != nil {
				return fmt.Errorf("ssjoin left %q: encode key: %w", leftStoreName, err)
			}
			v1Bytes, err := leftSerde.Serialize(v1)
			if err != nil {
				return fmt.Errorf("ssjoin left %q: encode value: %w", leftStoreName, err)
			}

			rawLeft := ctx.Store(leftStoreName)
			if rawLeft == nil {
				return fmt.Errorf("ssjoin left %q: store not wired", leftStoreName)
			}
			leftStore, ok := rawLeft.(ssJoinStore)
			if !ok {
				return fmt.Errorf("ssjoin left %q: store type mismatch: got %T, want ssJoinStore",
					leftStoreName, rawLeft)
			}
			if err := leftStore.WindowPut(kBytes, ts, v1Bytes); err != nil {
				return fmt.Errorf("ssjoin left %q: WindowPut: %w", leftStoreName, err)
			}

			rawRight := ctx.Store(rightStoreName)
			if rawRight == nil {
				return fmt.Errorf("ssjoin left %q: right store not wired", rightStoreName)
			}
			rightStore, ok := rawRight.(ssJoinStore)
			if !ok {
				return fmt.Errorf("ssjoin left %q: right store type mismatch: got %T, want ssJoinStore",
					rightStoreName, rawRight)
			}

			lower, upper := ssJoinScanBounds(kBytes, ts, beforeMs, afterMs, graceMs)
			var scanErr error
			err = rightStore.RangeCompositeBytes(lower, upper, func(_, rightValBytes []byte) bool {
				v2, dErr := rightSerde.Deserialize(rightValBytes)
				if dErr != nil {
					scanErr = fmt.Errorf("ssjoin left %q: decode right value: %w", rightStoreName, dErr)
					return false
				}
				ctx.Forward(topology.Record{Key: k, Value: joiner(v1, v2), Timestamp: ts})
				return true
			})
			if err != nil {
				return fmt.Errorf("ssjoin left %q: RangeCompositeBytes: %w", rightStoreName, err)
			}
			return scanErr
		},
		[]string{leftStoreName, rightStoreName}, s.nodeName,
	)

	// Right processor: buffer V2 in rightStore, scan leftStore, emit joiner(v1, v2).
	// SYMMETRIC — joiner arg order preserved: left always arg1, right always arg2.
	rightNode := b.nextName("ssjoin-right")
	b.internal.AddStatefulProcessor(rightNode,
		func(_ context.Context, r topology.Record, ctx topology.ProcessorContext) error {
			ts := r.Timestamp
			ctx.AdvanceStreamTime(ts)

			lateBoundary := ctx.StreamTime() - winDef.MaxSizeMs() - graceMs
			if ts < lateBoundary {
				slog.Debug("ssjoin right: late record dropped",
					"ts", ts, "streamTime", ctx.StreamTime(), "lateBoundary", lateBoundary)
				return nil
			}

			k, ok := r.Key.(K)
			if !ok {
				return fmt.Errorf("ssjoin right %q: key type mismatch: got %T, want %T",
					rightStoreName, r.Key, *new(K))
			}
			v2, ok := r.Value.(V2)
			if !ok {
				return fmt.Errorf("ssjoin right %q: value type mismatch: got %T, want %T",
					rightStoreName, r.Value, *new(V2))
			}

			kBytes, err := keySerde.Serialize(k)
			if err != nil {
				return fmt.Errorf("ssjoin right %q: encode key: %w", rightStoreName, err)
			}
			v2Bytes, err := rightSerde.Serialize(v2)
			if err != nil {
				return fmt.Errorf("ssjoin right %q: encode value: %w", rightStoreName, err)
			}

			rawRight := ctx.Store(rightStoreName)
			if rawRight == nil {
				return fmt.Errorf("ssjoin right %q: store not wired", rightStoreName)
			}
			rightStore, ok := rawRight.(ssJoinStore)
			if !ok {
				return fmt.Errorf("ssjoin right %q: store type mismatch: got %T, want ssJoinStore",
					rightStoreName, rawRight)
			}
			if err := rightStore.WindowPut(kBytes, ts, v2Bytes); err != nil {
				return fmt.Errorf("ssjoin right %q: WindowPut: %w", rightStoreName, err)
			}

			rawLeft := ctx.Store(leftStoreName)
			if rawLeft == nil {
				return fmt.Errorf("ssjoin right %q: left store not wired", leftStoreName)
			}
			leftStore, ok := rawLeft.(ssJoinStore)
			if !ok {
				return fmt.Errorf("ssjoin right %q: left store type mismatch: got %T, want ssJoinStore",
					leftStoreName, rawLeft)
			}

			lower, upper := ssJoinScanBounds(kBytes, ts, beforeMs, afterMs, graceMs)
			var scanErr error
			err = leftStore.RangeCompositeBytes(lower, upper, func(_, leftValBytes []byte) bool {
				v1, dErr := leftSerde.Deserialize(leftValBytes)
				if dErr != nil {
					scanErr = fmt.Errorf("ssjoin right %q: decode left value: %w", leftStoreName, dErr)
					return false
				}
				// joiner arg order: left always first, right always second.
				ctx.Forward(topology.Record{Key: k, Value: joiner(v1, v2), Timestamp: ts})
				return true
			})
			if err != nil {
				return fmt.Errorf("ssjoin right %q: RangeCompositeBytes: %w", leftStoreName, err)
			}
			return scanErr
		},
		[]string{leftStoreName, rightStoreName}, other.nodeName,
	)

	// Merge node: plain passthrough with two parents; both processor outputs flow through.
	mergeNode := b.nextName("ssjoin-merge")
	b.internal.AddProcessor(mergeNode,
		func(_ context.Context, r topology.Record, forward topology.Forwarder) error {
			forward(r)
			return nil
		},
		leftNode, rightNode,
	)

	return KStream[K, VR]{builder: b, nodeName: mergeNode}
}
