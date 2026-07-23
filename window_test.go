package gstream_test

import (
	"testing"
	"time"

	"github.com/mortezaPRK/gstream"
)

// ---------------------------------------------------------------------------
// TumblingWindows.Assign
// ---------------------------------------------------------------------------

func TestTumbling_Assign_Zero(t *testing.T) {
	t.Parallel()
	w := gstream.TumblingWindows(10 * time.Second)
	wins := w.Assign(0)
	if len(wins) != 1 {
		t.Fatalf("len(Assign(0)): got %d, want 1", len(wins))
	}
	if wins[0].Start != 0 || wins[0].End != 10000 {
		t.Errorf("Assign(0): got [%d,%d), want [0,10000)", wins[0].Start, wins[0].End)
	}
}

func TestTumbling_Assign_Mid(t *testing.T) {
	t.Parallel()
	w := gstream.TumblingWindows(10 * time.Second)
	// ts=12000ms is in window [10000,20000)
	wins := w.Assign(12000)
	if len(wins) != 1 {
		t.Fatalf("len(Assign(12000)): got %d, want 1", len(wins))
	}
	if wins[0].Start != 10000 || wins[0].End != 20000 {
		t.Errorf("Assign(12000): got [%d,%d), want [10000,20000)", wins[0].Start, wins[0].End)
	}
}

func TestTumbling_Assign_ExactBoundary(t *testing.T) {
	t.Parallel()
	w := gstream.TumblingWindows(10 * time.Second)
	// ts=10000 is the START of window [10000,20000), not [0,10000)
	wins := w.Assign(10000)
	if len(wins) != 1 {
		t.Fatalf("len(Assign(10000)): got %d, want 1", len(wins))
	}
	if wins[0].Start != 10000 || wins[0].End != 20000 {
		t.Errorf("Assign(10000): got [%d,%d), want [10000,20000)", wins[0].Start, wins[0].End)
	}
}

func TestTumbling_MaxSizeMs(t *testing.T) {
	t.Parallel()
	w := gstream.TumblingWindows(5 * time.Second)
	if w.MaxSizeMs() != 5000 {
		t.Errorf("MaxSizeMs(): got %d, want 5000", w.MaxSizeMs())
	}
}

// ---------------------------------------------------------------------------
// HoppingWindows.Assign
// ---------------------------------------------------------------------------

// size=10s, advance=5s → 2 windows per record (except at the start boundary).
// Window starts are multiples of 5000ms.
// For ts=12000: windows starting at 5000 and 10000 both contain 12000.
//
//	[5000,15000): 5000<=12000<15000 ✓
//	[10000,20000): 10000<=12000<20000 ✓
//	[15000,25000): 15000>12000 ✗ (would need ts>=15000)
func TestHopping_Assign_Two(t *testing.T) {
	t.Parallel()
	w := gstream.HoppingWindows(10*time.Second, 5*time.Second)
	wins := w.Assign(12000)
	// Must be exactly: [5000,15000) and [10000,20000)
	want := []gstream.Window{
		{Start: 5000, End: 15000},
		{Start: 10000, End: 20000},
	}
	assertWindowSet(t, wins, want, "Assign(12000) size=10s advance=5s")
}

// ts=4000 with size=10s advance=5s:
//
//	latestStart   = floor(4000/5000)*5000 = 0
//	earliestStart = floor((4000-10000+5000)/5000)*5000 = floor(-1000/5000)*5000 = -1*5000 = -5000, clamped to 0
//
// So only [0,10000).
func TestHopping_Assign_One_AtStart(t *testing.T) {
	t.Parallel()
	w := gstream.HoppingWindows(10*time.Second, 5*time.Second)
	wins := w.Assign(4000)
	want := []gstream.Window{{Start: 0, End: 10000}}
	assertWindowSet(t, wins, want, "Assign(4000) size=10s advance=5s")
}

// ts=5000 with size=10s advance=5s:
//
//	latestStart   = floor(5000/5000)*5000 = 5000
//	earliestStart = floor((5000-10000+5000)/5000)*5000 = floor(0/5000)*5000 = 0
//
// Windows: [0,10000) and [5000,15000).
func TestHopping_Assign_ExactAdvanceBoundary(t *testing.T) {
	t.Parallel()
	w := gstream.HoppingWindows(10*time.Second, 5*time.Second)
	wins := w.Assign(5000)
	want := []gstream.Window{
		{Start: 0, End: 10000},
		{Start: 5000, End: 15000},
	}
	assertWindowSet(t, wins, want, "Assign(5000) size=10s advance=5s")
}

// ts=0 with size=10s advance=5s → only [0,10000).
func TestHopping_Assign_Zero(t *testing.T) {
	t.Parallel()
	w := gstream.HoppingWindows(10*time.Second, 5*time.Second)
	wins := w.Assign(0)
	want := []gstream.Window{{Start: 0, End: 10000}}
	assertWindowSet(t, wins, want, "Assign(0) size=10s advance=5s")
}

func TestHopping_MaxSizeMs(t *testing.T) {
	t.Parallel()
	w := gstream.HoppingWindows(10*time.Second, 5*time.Second)
	if w.MaxSizeMs() != 10000 {
		t.Errorf("MaxSizeMs(): got %d, want 10000", w.MaxSizeMs())
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func assertWindowSet(t *testing.T, got, want []gstream.Window, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: len=%d want %d; got=%v want=%v", label, len(got), len(want), got, want)
		return
	}
	for i, g := range got {
		if g != want[i] {
			t.Errorf("%s: [%d] got [%d,%d), want [%d,%d)", label, i, g.Start, g.End, want[i].Start, want[i].End)
		}
	}
}
