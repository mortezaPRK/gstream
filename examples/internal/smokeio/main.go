// Command smokeio produces and verifies records for examples/smoke.sh.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if len(os.Args) != 3 {
		fatalf("usage: smokeio <profile|initial|second|verify-initial|verify-final> <key>")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command, key := os.Args[1], os.Args[2]
	switch command {
	case "profile":
		produce(ctx, record("join-profiles", key, `"gold"`))
	case "initial":
		produce(ctx,
			record("filter-map-input", key, `"hello"`),
			record("stateful-input", key, `"value"`),
			record("eos-input", key, `"value"`),
			record("join-table-input", key, `"table"`),
		)
		time.Sleep(2 * time.Second)
		produce(ctx,
			record("join-stream-input", key, `"stream"`),
			record("join-left-input", key, `"left"`),
			record("join-right-input", key, `"right"`),
			&kgo.Record{Topic: "join-orders", Key: jsonString("order-" + key), Value: []byte(fmt.Sprintf(`{"id":%q,"user_id":%q}`, "order-"+key, key))},
		)
	case "second":
		produce(ctx, record("eos-input", key, `"value"`))
	case "verify-initial":
		verify(ctx, map[string]expectation{
			"filter-map-output":                 {key: jsonString(key), value: []byte(`"HELLO"`)},
			"stateful-count-output":             {key: jsonString(key), value: []byte("1")},
			"eos-output":                        {key: jsonString(key), value: []byte("1")},
			"join-table-output":                 {key: jsonString(key), value: []byte(`"stream:1"`)},
			"join-stream-output":                {key: jsonString(key), value: []byte(`"left:right"`)},
			"join-global-output":                {key: jsonString("order-" + key), value: jsonString("order-" + key + ":gold")},
			"stateful-example-counts-changelog": {any: true},
		})
	case "verify-final":
		verify(ctx, map[string]expectation{
			"eos-output":                            {key: jsonString(key), value: []byte("2")},
			"eos-recovery-example-counts-changelog": {any: true},
		})
	default:
		fatalf("unknown command %q", command)
	}
}

type expectation struct {
	key   []byte
	value []byte
	any   bool
}

func record(topic, key, value string) *kgo.Record {
	return &kgo.Record{Topic: topic, Key: jsonString(key), Value: []byte(value)}
}

func jsonString(value string) []byte { return []byte(fmt.Sprintf("%q", value)) }

func produce(ctx context.Context, records ...*kgo.Record) {
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		fatalf("create producer: %v", err)
	}
	defer client.Close()
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		fatalf("produce: %v", err)
	}
}

func verify(ctx context.Context, expected map[string]expectation) {
	topics := make([]string, 0, len(expected))
	for topic := range expected {
		topics = append(topics, topic)
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		fatalf("create verifier: %v", err)
	}
	defer client.Close()
	for len(expected) > 0 {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			fatalf("verify fetch: %v", err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			want, ok := expected[record.Topic]
			if !ok {
				return
			}
			if want.any || (string(record.Key) == string(want.key) && string(record.Value) == string(want.value)) {
				delete(expected, record.Topic)
			}
		})
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
