// Command stateful runs count, time-window, and session-window branches.
package main

import (
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/examples/internal/runapp"
)

func main() {
	builder := gstream.NewStreamBuilder()
	input := gstream.Stream(
		builder, "stateful-input", "input",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{},
	)
	grouped := input.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	grouped.Count("counts").To("stateful-count-output", gstream.JSONSerde[string]{}, gstream.JSONSerde[int64]{})
	grouped.WindowedBy(gstream.TumblingWindows(time.Minute)).
		WithGrace(10 * time.Second).
		Count("minute-counts")
	grouped.SessionWindowedBy(gstream.SessionWindow(30 * time.Second)).
		WithGrace(10 * time.Second).
		Count("sessions")
	runapp.Run("stateful-example", gstream.AtLeastOnce, builder.Build())
}
