// Package main demonstrates the P1 stateless E2E pipeline using the gstream DSL:
//
//	consume → filter → mapvalues → produce (ALO)
//
// The pipeline reads JSON-encoded strings from "input-topic", filters out records
// whose value is shorter than 4 characters, maps each surviving value to uppercase,
// and produces the results to "output-topic".
//
// Run against a local broker:
//
//	BROKERS=localhost:9092 go run ./examples/filter-map/
//
// A real Kafka broker is required. For a broker-free unit test of the same
// topology, see the topology.TestDriver examples in internal/topology.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/kafka"
	"github.com/mortezaPRK/gstream/internal/runtime"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// -------------------------------------------------------------------------
	// 1. Configuration
	// -------------------------------------------------------------------------
	brokers := os.Getenv("BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	cfg := gstream.Config{
		ApplicationID: "filter-map-example",
		Brokers:       strings.Split(brokers, ","),
	}
	cfg.ApplyDefaults()

	// -------------------------------------------------------------------------
	// 2. Build the topology using the StreamBuilder DSL: source → filter → mapvalues → sink
	//
	//    Stream[K, V] registers a typed source node, wires keySerde/valSerde for
	//    decoding raw Kafka bytes, and returns a KStream[K,V] for further chaining.
	// -------------------------------------------------------------------------
	b := gstream.NewStreamBuilder()

	src := gstream.Stream[string, string](
		b,
		"input-topic", // Kafka source topic
		"source",      // unique node name in the DAG
		gstream.JSONSerde[string]{},
		gstream.JSONSerde[string]{},
	)

	// Filter: keep only records whose value has at least 4 characters.
	// MapValues: transform surviving values to uppercase (key is passed through).
	// To: register a typed sink node, wiring keySerde/valSerde for encoding outputs.
	src.
		Filter(func(_ string, v string) bool {
			return len(v) >= 4
		}).
		MapValues(func(v string) string {
			return strings.ToUpper(v)
		}).
		To(
			"output-topic", // Kafka sink topic
			"sink",         // unique node name in the DAG
			gstream.JSONSerde[string]{},
			gstream.JSONSerde[string]{},
		)

	bt := b.Build()

	// -------------------------------------------------------------------------
	// 3. Wire the sealed topology into the kafka client via runtime.Adapter.
	//
	//    NewAdapter(bt, logger) validates that every source and sink declared in
	//    the topology has a matching binding, then wraps the topology in a
	//    synchronous TestDriver for per-record execution.
	// -------------------------------------------------------------------------
	adapter, err := runtime.NewAdapter(bt, logger)
	if err != nil {
		logger.Error("failed to create adapter", slog.Any("error", err))
		os.Exit(1)
	}

	// -------------------------------------------------------------------------
	// 4. Create the Kafka client and run the pipeline with graceful shutdown.
	// -------------------------------------------------------------------------
	client, err := kafka.New(cfg, []string{"input-topic"}, logger)
	if err != nil {
		logger.Error("failed to create kafka client", slog.Any("error", err))
		os.Exit(1)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("starting filter-map pipeline",
		slog.String("source", "input-topic"),
		slog.String("sink", "output-topic"),
		slog.String("guarantee", cfg.Guarantee.String()),
	)

	// adapter.ProcessFunc() returns a kafka.ProcessFunc that decodes each incoming
	// record, drives the topology DAG, and encodes the sink outputs back to bytes.
	if err := client.Run(ctx, adapter.ProcessFunc()); err != nil {
		logger.Error("pipeline error", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info("pipeline stopped cleanly")
}
