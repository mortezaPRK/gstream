// Command filter-map runs stateless at-least-once filter/map pipeline.
package main

import (
	"strings"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/examples/internal/runapp"
)

func main() {
	builder := gstream.NewStreamBuilder()
	input := gstream.Stream(
		builder, "filter-map-input", "input",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{},
	)
	input.Filter(func(_ string, value string) bool { return len(value) >= 4 }).
		MapValues(func(_ string, value string) string { return strings.ToUpper(value) }).
		To("filter-map-output", "output", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	runapp.Run("filter-map-example", gstream.AtLeastOnce, builder.Build())
}
