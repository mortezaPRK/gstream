// ssjoin_bounds_spike_test.go — P4b-F2-C0 spike.
//
// Proves the stream-stream windowed-join window bounds formula using the REAL
// WindowCompositeKey encoder and REAL Pebble exclusive-upper-bound semantics.
// Pure unit test; no broker, no production code changes.
//
// # Formula under test (FROZEN after all 9 rows pass)
//
//	loMs  = max(0, tsA - before - grace)          // underflow guard; grace extends lower
//	hiMs  = tsA + after, capped at MaxInt64        // overflow: let hiMs+1 wrap to MinInt64
//	lower = WindowCompositeKey(kBytes, loMs)       // inclusive
//	upper = WindowCompositeKey(kBytes, hiMs+1)     // exclusive
//
// Overflow note for the MaxInt64 cap:
//
//	When tsA+after > MaxInt64, hiMs is set to MaxInt64.
//	hiMs+1 then wraps to MinInt64 (−2^63) in int64 arithmetic.
//	WindowCompositeKey encodes the timestamp as uint64, so MinInt64 becomes
//	0x80_00_00_00_00_00_00_00 in big-endian bytes.
//	Since 0x80... > 0x7F... (MaxInt64's encoding), Pebble's exclusive upper bound
//	correctly includes all non-negative timestamps including MaxInt64.
//	This is NOT an accidental overflow — it is correct by the big-endian property
//	documented in window_key.go ("negative int64 values sort ABOVE positive ones").
//
// Grace semantics:
//
//	Grace extends the LOWER lookup bound for late B-side records:
//	loMs = max(0, tsA - before - grace)
//	Row 8 (tsB=49, grace=10): loMs = max(0,100-50-10) = 40 → 49 >= 40 → YES.
//	Row 9 (tsB=39, grace=10): loMs = 40                  → 39 <  40 → NO.
//	Both rows hold under this single formula.
package pebble_test

import (
	"math"
	"testing"

	state "mortz.dev/go/gstream/stores/pebble"
)

// ssjoinLoMs computes the inclusive lower bound timestamp for the B-side scan.
// Grace extends the window leftward to accept late-arriving B records.
func ssjoinLoMs(tsA, before, grace int64) int64 {
	sub := before + grace // assumes before+grace fits in int64 (reasonable for real windows)
	if tsA < sub {
		return 0
	}
	return tsA - sub
}

// ssjoinHiMs computes the inclusive upper bound timestamp for the B-side scan.
// When tsA+after would overflow MaxInt64 the result is capped at MaxInt64 — see
// the package-level overflow note for why hiMs+1 wrapping to MinInt64 is correct.
func ssjoinHiMs(tsA, after int64) int64 {
	if after > 0 && tsA > math.MaxInt64-after {
		return math.MaxInt64
	}
	return tsA + after
}

// TestSSJoinBounds exercises every row of the P4b-F2-C0 truth table.
// For each row it:
//  1. WindowPuts a B record at tsB into a real in-memory Pebble store.
//  2. Computes lower/upper from (tsA, before, after, grace) with the production formula.
//  3. Calls RangeCompositeBytes and asserts match or no-match.
func TestSSJoinBounds(t *testing.T) {
	type row struct {
		name          string
		tsA, tsB      int64
		before, after int64
		grace         int64
		wantMatch     bool
	}

	rows := []row{
		{
			// Row 1: same timestamp on both sides — always in range.
			name: "row1_same_ts",
			tsA:  100, tsB: 100, before: 50, after: 50, grace: 0,
			wantMatch: true,
		},
		{
			// Row 2: tsB == loMs — inclusive lower bound.
			name: "row2_exactly_lower_bound",
			tsA:  100, tsB: 50, before: 50, after: 50, grace: 0,
			wantMatch: true,
		},
		{
			// Row 3: tsB == loMs-1 — just below lower, excluded.
			name: "row3_just_below_lower",
			tsA:  100, tsB: 49, before: 50, after: 50, grace: 0,
			wantMatch: false,
		},
		{
			// Row 4: tsB == hiMs — inclusive upper; exclusive upper is hiMs+1.
			name: "row4_exactly_upper_bound",
			tsA:  100, tsB: 150, before: 50, after: 50, grace: 0,
			wantMatch: true,
		},
		{
			// Row 5: tsB == hiMs+1 — at the exclusive upper, not returned.
			name: "row5_just_above_upper",
			tsA:  100, tsB: 151, before: 50, after: 50, grace: 0,
			wantMatch: false,
		},
		{
			// Row 6: tsA == 0, large before — loMs underflow floored to 0, no panic.
			name: "row6_zero_ts_loMs_floor",
			tsA:  0, tsB: 0, before: 100, after: 100, grace: 0,
			wantMatch: true,
		},
		{
			// Row 7: tsA+after overflows MaxInt64 — hiMs capped, hiMs+1 wraps to MinInt64;
			// big-endian 0x80... > 0x7F... so tsB=MaxInt64 is still included.
			name: "row7_hiMs_overflow_cap",
			tsA:  math.MaxInt64 - 100, tsB: math.MaxInt64, before: 50, after: 200, grace: 0,
			wantMatch: true,
		},
		{
			// Row 8: tsB=49 is outside [50,150] without grace, but within grace=10 extension.
			// loMs = max(0, 100-50-10) = 40 → 49 >= 40 → YES.
			name: "row8_grace_within",
			tsA:  100, tsB: 49, before: 50, after: 50, grace: 10,
			wantMatch: true,
		},
		{
			// Row 9: tsB=39 < loMs=40 even with grace=10 — beyond grace.
			name: "row9_grace_beyond",
			tsA:  100, tsB: 39, before: 50, after: 50, grace: 10,
			wantMatch: false,
		},
	}

	for _, r := range rows {
		r := r
		t.Run(r.name, func(t *testing.T) {
			db, err := state.OpenMemDB()
			if err != nil {
				t.Fatalf("OpenMemDB: %v", err)
			}
			defer db.Close()

			store := state.NewKeyValueStore[[]byte, []byte](
				"ssjoin-spike", db, rawBytesSerde{}, rawBytesSerde{},
			)
			defer store.Close()

			kBytes := []byte("k")
			bVal := []byte("bval")

			if err := store.WindowPut(kBytes, r.tsB, bVal); err != nil {
				t.Fatalf("WindowPut tsB=%d: %v", r.tsB, err)
			}

			loMs := ssjoinLoMs(r.tsA, r.before, r.grace)
			hiMs := ssjoinHiMs(r.tsA, r.after)

			// hiMs+1 may overflow to MinInt64 when hiMs==MaxInt64; see overflow note.
			lower := state.WindowCompositeKey(kBytes, loMs)
			upper := state.WindowCompositeKey(kBytes, hiMs+1)

			var matched bool
			err = store.RangeCompositeBytes(lower, upper, func(_, _ []byte) bool {
				matched = true
				return true
			})
			if err != nil {
				t.Fatalf("RangeCompositeBytes: %v", err)
			}

			if matched != r.wantMatch {
				t.Errorf(
					"FAIL tsA=%d tsB=%d before=%d after=%d grace=%d: matched=%v want=%v (loMs=%d hiMs=%d)",
					r.tsA, r.tsB, r.before, r.after, r.grace, matched, r.wantMatch, loMs, hiMs,
				)
			}
		})
	}
}
