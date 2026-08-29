// Command joins demonstrates stream-table, stream-stream, and global-table joins.
package main

import (
	"fmt"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/examples/internal/runapp"
)

type order struct {
	ID     string `json:"id"`
	UserID string `json:"user_id"`
}

func main() {
	builder := gstream.NewStreamBuilder()
	stringsSerde := gstream.JSONSerde[string]{}

	tableInput := gstream.Stream(builder, "join-table-input", "table-input", stringsSerde, stringsSerde)
	table := tableInput.GroupByKey(stringsSerde, stringsSerde).Count("table-counts")
	streamInput := gstream.Stream(builder, "join-stream-input", "stream-input", stringsSerde, stringsSerde)
	streamInput.JoinTable(
		table,
		func(value string, count int64) string { return fmt.Sprintf("%s:%d", value, count) },
		stringsSerde,
		stringsSerde,
	).To("join-table-output", "table-output", stringsSerde, stringsSerde)

	left := gstream.Stream(builder, "join-left-input", "left-input", stringsSerde, stringsSerde)
	right := gstream.Stream(builder, "join-right-input", "right-input", stringsSerde, stringsSerde)
	left.Join(
		right,
		func(leftValue, rightValue string) string { return leftValue + ":" + rightValue },
		gstream.JoinWindows{Before: time.Minute, After: time.Minute, Grace: 10 * time.Second},
		stringsSerde, stringsSerde, stringsSerde, stringsSerde,
	).To("join-stream-output", "stream-output", stringsSerde, stringsSerde)

	profiles := gstream.GlobalTable(builder, "join-profiles", "profiles", stringsSerde, stringsSerde)
	orders := gstream.Stream(builder, "join-orders", "orders", stringsSerde, gstream.JSONSerde[order]{})
	orders.JoinGlobal(
		profiles,
		func(_ string, value order) string { return value.UserID },
		func(value order, profile string) string { return value.ID + ":" + profile },
		stringsSerde,
	).To("join-global-output", "global-output", stringsSerde, stringsSerde)

	runapp.Run("joins-example", gstream.AtLeastOnce, builder.Build())
}
