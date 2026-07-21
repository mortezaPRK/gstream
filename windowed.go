package gstream

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/mortezaPRK/gstream/internal/topology"
)

// windowStore is the interface the windowed processor asserts against at runtime.
// The runtime supplies a *state.KeyValueStore[[]byte,[]byte] in the stores map,
// which satisfies this interface via its WindowGet/WindowPut methods. The
// composite-key encoding lives once in internal/state (WindowCompositeKey); the
// DSL only provides (kBytes, windowStart) and never knows the byte layout.
//
// Using a narrow interface avoids importing internal/state directly (which would
// create a gstream→internal/state→gstream cycle via Serde[T]).
type windowStore interface {
	WindowGet(kBytes []byte, windowStart int64) ([]byte, bool, error)
	WindowPut(kBytes []byte, windowStart int64, val []byte) error
}

// TimeWindowedStream is an intermediate typed stream produced by
// KGroupedStream.WindowedBy. It carries the window definition and optional
// grace period; callers proceed to Count or Aggregate to produce a windowed
// KTable.
type TimeWindowedStream[K, V any] struct {
	builder     *StreamBuilder
	nodeName    string
	keySerde    Serde[K]
	valSerde    Serde[V]
	windows     WindowDefinition
	graceMs     int64
	extractorFn TimestampExtractor

	// lateCount is an atomic counter incremented for every record dropped as
	// late (ts < lateBoundary). It is shared across all Aggregate/Count
	// invocations built from this stream and is exposed via LateCount() for
	// tests. The pointer is allocated once in WindowedBy and shared.
	lateCount *atomic.Int64
}

// WindowedBy attaches a WindowDefinition to a KGroupedStream, returning a
// TimeWindowedStream ready for Count or Aggregate.
func (g KGroupedStream[K, V]) WindowedBy(w WindowDefinition) TimeWindowedStream[K, V] {
	return TimeWindowedStream[K, V]{
		builder:     g.builder,
		nodeName:    g.nodeName,
		keySerde:    g.keySerde,
		valSerde:    g.valSerde,
		windows:     w,
		graceMs:     0,
		extractorFn: nil,
		lateCount:   new(atomic.Int64),
	}
}

// WithGrace returns a copy of s with the late-record grace period set to d.
// A record with ts >= (streamTime - MaxSizeMs - graceMs) is still accepted;
// records with ts below that boundary are dropped and counted as late.
func (s TimeWindowedStream[K, V]) WithGrace(d time.Duration) TimeWindowedStream[K, V] {
	s.graceMs = d.Milliseconds()
	return s
}

// WithTimestampExtractor returns a copy of s that uses fn to extract the event
// timestamp from each record instead of r.Timestamp.
func (s TimeWindowedStream[K, V]) WithTimestampExtractor(fn TimestampExtractor) TimeWindowedStream[K, V] {
	s.extractorFn = fn
	return s
}

// LateCount returns the current number of records dropped as late. Useful in
// tests to assert late-drop behaviour without inspecting logs.
func (s TimeWindowedStream[K, V]) LateCount() int64 {
	return s.lateCount.Load()
}

// Count accumulates the number of records per windowed key, storing counts in
// storeName. Delegates to Aggregate[int64] with JSONSerde[int64].
func (s TimeWindowedStream[K, V]) Count(storeName string) KTable[Windowed[K], int64] {
	return s.Aggregate(
		storeName,
		func() int64 { return 0 },
		func(_ K, _ V, acc int64) int64 { return acc + 1 },
		JSONSerde[int64]{},
	)
}

// Aggregate accumulates records per windowed key using a caller-supplied
// accumulator. The store is accessed via the windowStore interface whose
// WindowGet/WindowPut methods embed the WindowCompositeKey encoding; the DSL
// never sees the raw byte layout.
//
// Late-record semantics:
//   - ts < (streamTime - MaxSizeMs - graceMs): record is dropped; lateCount++.
//   - ts >= lateBoundary: record is accepted into all assigned windows.
//
// The processor does NOT call ctx.Forward (P2 rule: KTable has no downstream
// consumer until P4/P5).
func (s TimeWindowedStream[K, V]) Aggregate[A any](
	storeName string,
	initFn func() A,
	aggFn func(K, V, A) A,
	accSerde Serde[A],
) KTable[Windowed[K], A] {
	name := s.builder.nextName("windowed-aggregate")

	// Capture fields at build time so the closure is self-contained.
	windows := s.windows
	graceMs := s.graceMs
	extractorFn := s.extractorFn
	keySerde := s.keySerde
	lateCount := s.lateCount

	s.builder.internal.AddStatefulProcessor(name, func(r topology.Record, ctx topology.ProcessorContext) error {
		// 1. Determine event timestamp.
		var ts int64
		if extractorFn != nil {
			ts = extractorFn(r)
		} else {
			ts = r.Timestamp
		}

		// 2. Advance stream-time watermark.
		ctx.AdvanceStreamTime(ts)

		// 3. Late-record check.
		lateBoundary := ctx.StreamTime() - windows.MaxSizeMs() - graceMs
		if ts < lateBoundary {
			lateCount.Add(1)
			slog.Debug("windowed aggregate: late record dropped",
				"storeName", storeName,
				"ts", ts,
				"streamTime", ctx.StreamTime(),
				"lateBoundary", lateBoundary,
			)
			return nil
		}

		// 4. Type-assert key and value.
		k, ok := r.Key.(K)
		if !ok {
			return fmt.Errorf("windowed aggregate %q: key type mismatch: got %T, want %T", storeName, r.Key, *new(K))
		}
		v, ok := r.Value.(V)
		if !ok {
			return fmt.Errorf("windowed aggregate %q: value type mismatch: got %T, want %T", storeName, r.Value, *new(V))
		}

		// 5. Assert the window store interface. The runtime supplies a
		// *state.KeyValueStore[[]byte,[]byte] which satisfies windowStore via
		// WindowGet/WindowPut. The composite-key encoding stays in internal/state.
		raw := ctx.Store(storeName)
		if raw == nil {
			return fmt.Errorf("windowed aggregate %q: store not wired", storeName)
		}
		store, ok := raw.(windowStore)
		if !ok {
			return fmt.Errorf("windowed aggregate %q: store type mismatch: got %T, want windowStore", storeName, raw)
		}

		// 6. Serialize the record key once; reuse for every window.
		kBytes, err := keySerde.Serialize(k)
		if err != nil {
			return fmt.Errorf("windowed aggregate %q: encode key: %w", storeName, err)
		}

		// 7. Fan out into all assigned windows.
		for _, win := range windows.Assign(ts) {
			// WindowGet/WindowPut own the composite-key encoding.
			valBytes, found, err := store.WindowGet(kBytes, win.Start)
			if err != nil {
				return fmt.Errorf("windowed aggregate %q: store WindowGet window [%d,%d): %w", storeName, win.Start, win.End, err)
			}

			var cur A
			if found {
				cur, err = accSerde.Deserialize(valBytes)
				if err != nil {
					return fmt.Errorf("windowed aggregate %q: decode accumulator window [%d,%d): %w", storeName, win.Start, win.End, err)
				}
			} else {
				cur = initFn()
			}

			next := aggFn(k, v, cur)
			nextBytes, err := accSerde.Serialize(next)
			if err != nil {
				return fmt.Errorf("windowed aggregate %q: encode accumulator window [%d,%d): %w", storeName, win.Start, win.End, err)
			}
			if err := store.WindowPut(kBytes, win.Start, nextBytes); err != nil {
				return fmt.Errorf("windowed aggregate %q: store WindowPut window [%d,%d): %w", storeName, win.Start, win.End, err)
			}
		}

		// P2: no ctx.Forward — KTable has no downstream consumer until P4.
		return nil
	}, []string{storeName}, s.nodeName)

	// Internal sink to satisfy topology.Builder.Build()'s >=1 sink invariant.
	// The StatefulProcessFunc never calls ctx.Forward so no records reach it.
	sinkName := s.builder.nextName("windowed-ktable-out")
	s.builder.internal.AddSink(sinkName, name)

	// Register a WindowStoreBinding so the runtime (T4) can open the store and
	// configure the window sweeper. EncodeKey/DecodeKey are stubs: the active
	// processing path uses the windowStore interface (WindowGet/WindowPut) which
	// owns the composite-key encoding inside internal/state. The restore path
	// (T4, internal/runtime) can import internal/state directly and use
	// state.WindowCompositeKey/DecodeWindowCompositeKey there. gstream cannot
	// import internal/state (cycle: internal/state → gstream via Serde[T]).
	s.builder.windowStores[storeName] = WindowStoreBinding{
		StoreBinding: StoreBinding{
			StoreName:      storeName,
			ChangelogTopic: storeName,
			EncodeKey: func(x any) ([]byte, error) {
				// The restore path (T4) should use the windowStore.WindowGet/WindowPut
				// interface directly. This closure is a placeholder.
				return nil, fmt.Errorf("WindowStoreBinding %q EncodeKey: use windowStore.WindowGet/WindowPut interface", storeName)
			},
			DecodeKey: func(b []byte) (any, error) {
				return nil, fmt.Errorf("WindowStoreBinding %q DecodeKey: use windowStore.WindowGet/WindowPut interface", storeName)
			},
			EncodeVal: func(x any) ([]byte, error) {
				a, ok := x.(A)
				if !ok {
					return nil, fmt.Errorf("WindowStoreBinding %q EncodeVal: expected %T, got %T", storeName, *new(A), x)
				}
				return accSerde.Serialize(a)
			},
			DecodeVal: func(b []byte) (any, error) {
				return accSerde.Deserialize(b)
			},
		},
		WindowDef: windows,
		GraceMs:   graceMs,
	}

	return KTable[Windowed[K], A]{
		builder:   s.builder,
		nodeName:  name,
		storeName: storeName,
	}
}
