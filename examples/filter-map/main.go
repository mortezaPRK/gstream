// Package main demonstrates the P0 stateless E2E pipeline:
//
//	consume → filter → map → produce (ALO)
//
// The pipeline reads JSON-encoded strings from "input-topic", filters out records
// whose value is shorter than 4 characters, maps each surviving value to uppercase,
// and produces the results to "output-topic".
//
// Run against a local broker:
//
//	BROKERS=localhost:9092 go run ./examples/filter-map/
//
// A real Kafka broker is required.  For a broker-free unit test of the same
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
	"github.com/mortezaPRK/gstream/internal/topology"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// -------------------------------------------------------------------------
	// 1. Configuration (§13)
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
	// 2. Build the topology: source → filter → map → sink (§6.3, §17 P0)
	// -------------------------------------------------------------------------
	b := topology.NewBuilder()

	src := b.AddSource("source")

	// Filter: keep only values with at least 4 characters.
	filt := b.AddProcessor("filter",
		topology.Filter(func(_, v any) bool {
			s, ok := v.(string)
			return ok && len(s) >= 4
		}),
		src,
	)

	// Map: transform the value to uppercase and prefix the key.
	mapped := b.AddProcessor("map",
		topology.Mapper(func(k, v any) (any, any) {
			return "out-" + string(k.([]byte)), strings.ToUpper(v.(string))
		}),
		filt,
	)

	b.AddSink("sink", mapped)
	topo := b.Build()

	// -------------------------------------------------------------------------
	// 3. Wire the topology into the kafka client via runtime.Adapter (§7, §15)
	//    Each kafka.InRecord.Value is decoded as a plain string via JSONSerde[string].
	// -------------------------------------------------------------------------
	serde := gstream.JSONSerde[string]{}
	adapter, err := runtime.NewAdapter(
		topo,
		serde,
		runtime.SinkRoute{"sink": "output-topic"},
		logger,
	)
	if err != nil {
		logger.Error("failed to create adapter", slog.Any("error", err))
		os.Exit(1)
	}

	// -------------------------------------------------------------------------
	// 4. Create the kafka client and run the pipeline.
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

	if err := client.Run(ctx, adapter.ProcessFunc()); err != nil {
		logger.Error("pipeline error", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info("pipeline stopped cleanly")
}
