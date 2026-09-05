package gstream_test

import (
	"fmt"
	"strings"
	"time"

	gstream "mortz.dev/go/gstream"
	memory "mortz.dev/go/gstream/internal/testutil"
)

func ExampleBytesSerde() {
	s := memory.BytesSerde{}
	in := []byte("hello")

	enc, _ := s.Serialize(in)
	dec, _ := s.Deserialize(enc)

	fmt.Println(string(dec))
	// Output: hello
}

func ExampleJSONSerde() {
	type Point struct{ X, Y int }

	s := memory.JSONSerde[Point]{}
	in := Point{X: 3, Y: 7}

	enc, _ := s.Serialize(in)
	dec, _ := s.Deserialize(enc)

	fmt.Printf("X=%d Y=%d\n", dec.X, dec.Y)
	// Output: X=3 Y=7
}

// ExampleStream shows how to wire a source → filter → map → sink topology
// using StreamBuilder. The example calls Build() but does not run the topology
// (running requires a Kafka broker); it prints the source and sink topic names
// to confirm the topology was assembled correctly.
func ExampleStream() {
	b := gstream.NewStreamBuilder()

	src := gstream.Stream[string, string](b, "input-topic", "src",
		memory.JSONSerde[string]{}, memory.JSONSerde[string]{})

	src.
		Filter(func(k, v string) bool { return strings.HasPrefix(v, "keep:") }).
		MapValues(func(_ string, v string) string { return strings.TrimPrefix(v, "keep:") }).
		To("output-topic", "sink", memory.JSONSerde[string]{}, memory.JSONSerde[string]{})

	bt := b.Build()

	fmt.Println("sources:", bt.Sources["src"].Topic)
	fmt.Println("sinks:", bt.Sinks["sink"].Topic)
	// Output:
	// sources: input-topic
	// sinks: output-topic
}

// ExampleKStream_Filter demonstrates the Filter operator in a compile-only
// topology (no broker needed; Output: line omitted intentionally).
func ExampleKStream_Filter() {
	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, int](b, "events", "src",
		memory.JSONSerde[string]{}, memory.JSONSerde[int]{})

	src.
		Filter(func(_ string, v int) bool { return v > 0 }).
		To("positive-events", "sink", memory.JSONSerde[string]{}, memory.JSONSerde[int]{})

	_ = b.Build() // topology sealed; ready to pass to runtime.NewAdapter
}

// ExampleKStream_Map demonstrates the Map operator (repartition boundary).
// Map changes both key and value types; no // Output: because no broker is needed.
func ExampleKStream_Map() {
	b := gstream.NewStreamBuilder()

	// Map changes both key and value type; marks a repartition boundary.
	gstream.Stream[string, int](b, "numbers", "src",
		memory.JSONSerde[string]{}, memory.JSONSerde[int]{}).
		Map(func(k string, v int) (string, string) {
			return k + "-mapped", fmt.Sprintf("%d", v*2)
		}).
		To("doubled", "sink", memory.JSONSerde[string]{}, memory.JSONSerde[string]{})

	_ = b.Build()
}

// ExampleKStream_GroupByKey shows the Count stateful aggregation.
func ExampleKStream_GroupByKey() {
	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "clicks", "src",
		memory.JSONSerde[string]{}, memory.JSONSerde[string]{})

	src.
		GroupByKey(memory.JSONSerde[string]{}, memory.JSONSerde[string]{}).
		Count("click-counts", memory.JSONSerde[int64]{})

	bt := b.Build()
	_, hasStore := bt.StoreBindings["click-counts"]
	fmt.Println("store registered:", hasStore)
	// Output: store registered: true
}

// ExampleTumblingWindows shows TumblingWindows window assignment.
func ExampleTumblingWindows() {
	w := gstream.TumblingWindows(10 * time.Second)

	// Assign a record with timestamp 15_000 ms (15 s).
	windows := w.Assign(15_000)
	fmt.Printf("window [%d, %d)\n", windows[0].Start, windows[0].End)
	// Output: window [10000, 20000)
}

// ExampleHoppingWindows shows HoppingWindows window assignment.
func ExampleHoppingWindows() {
	// 30-second windows that advance every 10 seconds.
	w := gstream.HoppingWindows(30*time.Second, 10*time.Second)

	// A record at 25_000 ms belongs to three overlapping windows.
	windows := w.Assign(25_000)
	fmt.Println("number of windows:", len(windows))
	// Output: number of windows: 3
}
