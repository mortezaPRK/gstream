// Package gstream — tests for KStream DSL operators and the full topology
// pipeline (P1 exit criterion, §15: "topology test driver runs a multi-operator
// pipeline with no broker").
//
// The test file lives in package gstream (not gstream_test) so that tests can
// inspect unexported StreamBuilder fields such as repartitions — used to assert
// that Map and SelectKey correctly mark repartition boundaries.
package gstream

import (
	"fmt"
	"strings"
	"testing"

	"github.com/mortezaPRK/gstream/internal/topology"
)

// ---------------------------------------------------------------------------
// P1 exit-criterion test
// ---------------------------------------------------------------------------

// TestMultiOperatorPipeline is the P1 exit criterion (§15): build a multi-step
// stateless pipeline via StreamBuilder, drive it through the topology TestDriver
// with no broker, and assert end-to-end correctness.
//
// Pipeline: source[string,string] → Filter → Map[string,int] → MapValues[string] → sink
//
//   - Filter: keep records whose value starts with "hello"
//   - Map:    (k,v) → (k+"!", len(v))      [type-changing: string,string → string,int]
//   - MapValues: int → formatted string      [type-changing value: int → string]
func TestMultiOperatorPipeline(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	src := Stream[string, string](b, "input-topic", "src",
		JSONSerde[string]{}, JSONSerde[string]{})

	filtered := src.Filter(func(k, v string) bool {
		return strings.HasPrefix(v, "hello")
	})

	mapped := filtered.Map[string, int](func(k, v string) (string, int) {
		return k + "!", len(v)
	})

	final := mapped.MapValues[string](func(_ string, v int) string {
		return fmt.Sprintf("%d chars", v)
	})

	final.To("output-topic", "sink", JSONSerde[string]{}, JSONSerde[string]{})

	bt := b.Build()
	driver := topology.NewTestDriver(bt.Topology)

	// A record whose value does NOT start with "hello" — must be filtered out.
	if err := driver.PipeInput("src", topology.Record{Key: "a", Value: "world", Timestamp: 1}); err != nil {
		t.Fatalf("PipeInput(filtered-out): unexpected error: %v", err)
	}
	dropped, err := driver.ReadOutput("sink")
	if err != nil {
		t.Fatalf("ReadOutput after dropped record: %v", err)
	}
	if len(dropped) != 0 {
		t.Errorf("expected 0 output records for filtered-out input; got %d: %+v", len(dropped), dropped)
	}

	// A record that passes the filter — must be transformed end-to-end.
	// value "hello" has length 5 → Map produces int 5 → MapValues produces "5 chars"
	const ts int64 = 42
	if err := driver.PipeInput("src", topology.Record{Key: "b", Value: "hello", Timestamp: ts}); err != nil {
		t.Fatalf("PipeInput(passing): unexpected error: %v", err)
	}
	out, err := driver.ReadOutput("sink")
	if err != nil {
		t.Fatalf("ReadOutput: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output record; got %d: %+v", len(out), out)
	}
	rec := out[0]
	if rec.Key != "b!" {
		t.Errorf("Key: got %v, want %q", rec.Key, "b!")
	}
	if rec.Value != "5 chars" {
		t.Errorf("Value: got %v, want %q", rec.Value, "5 chars")
	}
	if rec.Timestamp != ts {
		t.Errorf("Timestamp: got %d, want %d", rec.Timestamp, ts)
	}
}

// ---------------------------------------------------------------------------
// Filter unit tests
// ---------------------------------------------------------------------------

// TestFilter_Pass checks that a record satisfying the predicate reaches the sink.
func TestFilter_Pass(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	src := Stream[string, int](b, "t", "src", JSONSerde[string]{}, JSONSerde[int]{})
	src.Filter(func(_ string, v int) bool { return v > 0 }).
		To("t", "sink", JSONSerde[string]{}, JSONSerde[int]{})
	driver := topology.NewTestDriver(b.Build().Topology)

	if err := driver.PipeInput("src", topology.Record{Key: "k", Value: 1, Timestamp: 10}); err != nil {
		t.Fatalf("PipeInput: %v", err)
	}
	out, _ := driver.ReadOutput("sink")
	if len(out) != 1 {
		t.Fatalf("expected 1 record; got %d", len(out))
	}
	if out[0].Value != 1 {
		t.Errorf("Value: got %v, want 1", out[0].Value)
	}
}

// TestFilter_Drop checks that a record NOT satisfying the predicate is dropped.
func TestFilter_Drop(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	src := Stream[string, int](b, "t", "src", JSONSerde[string]{}, JSONSerde[int]{})
	src.Filter(func(_ string, v int) bool { return v > 0 }).
		To("t", "sink", JSONSerde[string]{}, JSONSerde[int]{})
	driver := topology.NewTestDriver(b.Build().Topology)

	if err := driver.PipeInput("src", topology.Record{Key: "k", Value: -1, Timestamp: 10}); err != nil {
		t.Fatalf("PipeInput: %v", err)
	}
	out, _ := driver.ReadOutput("sink")
	if len(out) != 0 {
		t.Errorf("expected 0 records after drop; got %d: %+v", len(out), out)
	}
}

// TestFilter_TypeMismatch_ReturnsError proves that a Record with a wrong Value
// type causes PipeInput to return a non-nil error — no silent drop.
//
// We build a KStream[string,string] but inject a Record with Value: int(99).
// The Filter processor's two-value assertion must detect the mismatch and
// propagate the error up through PipeInput.
func TestFilter_TypeMismatch_ReturnsError(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	src := Stream[string, string](b, "t", "src", JSONSerde[string]{}, JSONSerde[string]{})
	src.Filter(func(_ string, v string) bool { return len(v) > 0 }).
		To("t", "sink", JSONSerde[string]{}, JSONSerde[string]{})
	driver := topology.NewTestDriver(b.Build().Topology)

	// Inject a record whose Value is int — should cause a type-mismatch error.
	err := driver.PipeInput("src", topology.Record{Key: "k", Value: int(99), Timestamp: 1})
	if err == nil {
		t.Fatal("expected non-nil error for value type mismatch in Filter; got nil")
	}
}

// TestFilter_KeyTypeMismatch_ReturnsError proves that a wrong Key type also
// surfaces as an error (not a silent drop) from PipeInput.
func TestFilter_KeyTypeMismatch_ReturnsError(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	src := Stream[string, string](b, "t", "src", JSONSerde[string]{}, JSONSerde[string]{})
	src.Filter(func(k, v string) bool { return true }).
		To("t", "sink", JSONSerde[string]{}, JSONSerde[string]{})
	driver := topology.NewTestDriver(b.Build().Topology)

	// Inject a record whose Key is int — should cause a type-mismatch error.
	err := driver.PipeInput("src", topology.Record{Key: int(42), Value: "ok", Timestamp: 1})
	if err == nil {
		t.Fatal("expected non-nil error for key type mismatch in Filter; got nil")
	}
}

// ---------------------------------------------------------------------------
// MapValues unit tests
// ---------------------------------------------------------------------------

// TestMapValues_ChangesValuePreservesKeyAndTimestamp verifies that MapValues
// transforms the value, keeps the original key and timestamp unchanged.
func TestMapValues_ChangesValuePreservesKeyAndTimestamp(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	src := Stream[string, string](b, "t", "src", JSONSerde[string]{}, JSONSerde[string]{})
	// string → int (length)
	src.MapValues[int](func(_ string, v string) int { return len(v) }).
		To("t", "sink", JSONSerde[string]{}, JSONSerde[int]{})
	driver := topology.NewTestDriver(b.Build().Topology)

	const ts int64 = 77
	if err := driver.PipeInput("src", topology.Record{Key: "mykey", Value: "hello", Timestamp: ts}); err != nil {
		t.Fatalf("PipeInput: %v", err)
	}
	out, _ := driver.ReadOutput("sink")
	if len(out) != 1 {
		t.Fatalf("expected 1 record; got %d", len(out))
	}
	rec := out[0]
	if rec.Key != "mykey" {
		t.Errorf("Key: got %v, want %q", rec.Key, "mykey")
	}
	if rec.Value != 5 {
		t.Errorf("Value: got %v, want 5 (len of 'hello')", rec.Value)
	}
	if rec.Timestamp != ts {
		t.Errorf("Timestamp: got %d, want %d", rec.Timestamp, ts)
	}
}

// TestMapValues_TypeMismatch_ReturnsError proves that MapValues returns a non-nil
// error when the Record carries a Value of the wrong type.
func TestMapValues_TypeMismatch_ReturnsError(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	src := Stream[string, string](b, "t", "src", JSONSerde[string]{}, JSONSerde[string]{})
	src.MapValues[int](func(_ string, v string) int { return len(v) }).
		To("t", "sink", JSONSerde[string]{}, JSONSerde[int]{})
	driver := topology.NewTestDriver(b.Build().Topology)

	err := driver.PipeInput("src", topology.Record{Key: "k", Value: float64(3.14), Timestamp: 1})
	if err == nil {
		t.Fatal("expected non-nil error for value type mismatch in MapValues; got nil")
	}
}

// ---------------------------------------------------------------------------
// Map unit tests
// ---------------------------------------------------------------------------

// TestMap_ChangesBothTypes verifies that Map transforms both key and value types
// and sets the repartition marker.
func TestMap_ChangesBothTypes(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	src := Stream[string, string](b, "t", "src", JSONSerde[string]{}, JSONSerde[string]{})
	// (string, string) → (int, int): key length, value length
	mapped := src.Map[int, int](func(k, v string) (int, int) {
		return len(k), len(v)
	})
	mapped.To("t", "sink", JSONSerde[int]{}, JSONSerde[int]{})
	bt := b.Build()
	driver := topology.NewTestDriver(bt.Topology)

	const ts int64 = 5
	if err := driver.PipeInput("src", topology.Record{Key: "ab", Value: "hello", Timestamp: ts}); err != nil {
		t.Fatalf("PipeInput: %v", err)
	}
	out, _ := driver.ReadOutput("sink")
	if len(out) != 1 {
		t.Fatalf("expected 1 record; got %d", len(out))
	}
	rec := out[0]
	if rec.Key != 2 { // len("ab") == 2
		t.Errorf("Key: got %v, want 2", rec.Key)
	}
	if rec.Value != 5 { // len("hello") == 5
		t.Errorf("Value: got %v, want 5", rec.Value)
	}
	if rec.Timestamp != ts {
		t.Errorf("Timestamp: got %d, want %d", rec.Timestamp, ts)
	}

	// The Map node must have been registered as a repartition boundary.
	if len(b.repartitions) == 0 {
		t.Fatal("expected at least one repartition marker; repartitions map is empty")
	}
	foundRepartition := false
	for name, marked := range b.repartitions {
		if marked && strings.HasPrefix(name, "map-") {
			foundRepartition = true
			break
		}
	}
	if !foundRepartition {
		t.Errorf("no map-* node found in repartitions; got: %v", b.repartitions)
	}
}

// TestMap_TypeMismatch_ReturnsError verifies that a wrong key type causes
// PipeInput to return a non-nil error.
func TestMap_TypeMismatch_ReturnsError(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	src := Stream[string, string](b, "t", "src", JSONSerde[string]{}, JSONSerde[string]{})
	src.Map[int, int](func(k, v string) (int, int) { return len(k), len(v) }).
		To("t", "sink", JSONSerde[int]{}, JSONSerde[int]{})
	driver := topology.NewTestDriver(b.Build().Topology)

	err := driver.PipeInput("src", topology.Record{Key: int(1), Value: "v", Timestamp: 1})
	if err == nil {
		t.Fatal("expected non-nil error for key type mismatch in Map; got nil")
	}
}

// ---------------------------------------------------------------------------
// SelectKey unit tests
// ---------------------------------------------------------------------------

// TestSelectKey_ChangesKeyPreservesValue verifies that SelectKey replaces the
// key, keeps the value unchanged, and sets the repartition marker.
func TestSelectKey_ChangesKeyPreservesValue(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	src := Stream[string, string](b, "t", "src", JSONSerde[string]{}, JSONSerde[string]{})
	// Replace key with its length as an int.
	withNewKey := src.SelectKey[int](func(k, v string) int { return len(k) })
	withNewKey.To("t", "sink", JSONSerde[int]{}, JSONSerde[string]{})
	bt := b.Build()
	driver := topology.NewTestDriver(bt.Topology)

	const ts int64 = 99
	if err := driver.PipeInput("src", topology.Record{Key: "abc", Value: "world", Timestamp: ts}); err != nil {
		t.Fatalf("PipeInput: %v", err)
	}
	out, _ := driver.ReadOutput("sink")
	if len(out) != 1 {
		t.Fatalf("expected 1 record; got %d", len(out))
	}
	rec := out[0]
	if rec.Key != 3 { // len("abc") == 3
		t.Errorf("Key: got %v, want 3", rec.Key)
	}
	if rec.Value != "world" {
		t.Errorf("Value: got %v, want %q", rec.Value, "world")
	}
	if rec.Timestamp != ts {
		t.Errorf("Timestamp: got %d, want %d", rec.Timestamp, ts)
	}

	// The SelectKey node must have been registered as a repartition boundary.
	if len(b.repartitions) == 0 {
		t.Fatal("expected at least one repartition marker; repartitions map is empty")
	}
	foundRepartition := false
	for name, marked := range b.repartitions {
		if marked && strings.HasPrefix(name, "selectkey-") {
			foundRepartition = true
			break
		}
	}
	if !foundRepartition {
		t.Errorf("no selectkey-* node found in repartitions; got: %v", b.repartitions)
	}
}

// TestSelectKey_TypeMismatch_ReturnsError verifies that a wrong value type
// causes PipeInput to return a non-nil error (no silent drop).
func TestSelectKey_TypeMismatch_ReturnsError(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	src := Stream[string, string](b, "t", "src", JSONSerde[string]{}, JSONSerde[string]{})
	src.SelectKey[int](func(k, v string) int { return len(k) }).
		To("t", "sink", JSONSerde[int]{}, JSONSerde[string]{})
	driver := topology.NewTestDriver(b.Build().Topology)

	// Value is int, but KStream expects string.
	err := driver.PipeInput("src", topology.Record{Key: "k", Value: int(0), Timestamp: 1})
	if err == nil {
		t.Fatal("expected non-nil error for value type mismatch in SelectKey; got nil")
	}
}

// ---------------------------------------------------------------------------
// StreamBuilder construction and invariant tests
// ---------------------------------------------------------------------------

// TestNewStreamBuilder_InitialState verifies that NewStreamBuilder returns a
// builder with all internal maps non-nil and empty so that the first operator
// or source call does not panic with a nil-map write.
func TestNewStreamBuilder_InitialState(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	if b.sources == nil {
		t.Error("sources map is nil; want empty non-nil map")
	}
	if b.sinks == nil {
		t.Error("sinks map is nil; want empty non-nil map")
	}
	if b.repartitions == nil {
		t.Error("repartitions map is nil; want empty non-nil map")
	}
	if b.internal == nil {
		t.Error("internal topology.Builder is nil")
	}
	if len(b.sources) != 0 {
		t.Errorf("sources: want 0 entries, got %d", len(b.sources))
	}
	if len(b.sinks) != 0 {
		t.Errorf("sinks: want 0 entries, got %d", len(b.sinks))
	}
	if len(b.repartitions) != 0 {
		t.Errorf("repartitions: want 0 entries, got %d", len(b.repartitions))
	}
}

// TestStream_DuplicateSourceName_Panics verifies that calling Stream() twice with
// the same sourceName panics. The topology.Builder.AddSource implementation
// panics on duplicate node names, and we rely on that invariant.
func TestStream_DuplicateSourceName_Panics(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate source name; got none")
		}
	}()

	b := NewStreamBuilder()
	Stream[string, string](b, "topic", "src", JSONSerde[string]{}, JSONSerde[string]{})
	// Second call with same source name must panic.
	Stream[string, string](b, "topic", "src", JSONSerde[string]{}, JSONSerde[string]{})
}

// TestBuild_NoSinks_Panics verifies that calling Build() on a StreamBuilder
// that has sources but no sinks panics. The underlying topology.Builder.Build()
// enforces this invariant.
func TestBuild_NoSinks_Panics(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when building topology with no sinks; got none")
		}
	}()

	b := NewStreamBuilder()
	Stream[string, string](b, "topic", "src", JSONSerde[string]{}, JSONSerde[string]{})
	// No To() call — no sinks registered.
	b.Build() // must panic
}

// ---------------------------------------------------------------------------
// Repartition-marker assertions (direct builder inspection)
// ---------------------------------------------------------------------------

// TestRepartitionMarkers_MapAndSelectKeyBothMark verifies that Map and SelectKey
// set repartitions[name] = true, while Filter and MapValues do not.
func TestRepartitionMarkers_MapAndSelectKeyBothMark(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	src := Stream[string, string](b, "t", "src", JSONSerde[string]{}, JSONSerde[string]{})

	// Filter and MapValues should NOT mark repartitions.
	afterFilter := src.Filter(func(k, v string) bool { return true })
	afterMV := afterFilter.MapValues[int](func(_ string, v string) int { return len(v) })

	// Map and SelectKey SHOULD mark repartitions.
	afterMap := afterMV.Map[string, string](func(k string, v int) (string, string) {
		return k, fmt.Sprintf("%d", v)
	})
	afterSK := afterMap.SelectKey[int](func(k, v string) int { return len(k) })
	afterSK.To("t", "sink", JSONSerde[int]{}, JSONSerde[string]{})

	_ = b.Build()

	// Two repartition markers expected (one for Map, one for SelectKey).
	if len(b.repartitions) != 2 {
		t.Errorf("expected 2 repartition markers; got %d: %v", len(b.repartitions), b.repartitions)
	}
	for name, marked := range b.repartitions {
		if !marked {
			t.Errorf("repartition[%q] = false; expected true", name)
		}
		if !strings.HasPrefix(name, "map-") && !strings.HasPrefix(name, "selectkey-") {
			t.Errorf("unexpected repartition node name %q", name)
		}
	}
}
