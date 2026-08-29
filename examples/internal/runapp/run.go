// Package runapp contains shared process lifecycle wiring for runnable examples.
package runapp

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/app"
	"github.com/prometheus/client_golang/prometheus"
)

// Run starts topology until SIGINT, SIGTERM, or EXAMPLE_STOP_AFTER elapses.
func Run(applicationID string, guarantee gstream.Guarantee, topology *gstream.BuiltTopology) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	brokers := os.Getenv("BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	cfg, err := gstream.Configure(
		gstream.WithName(applicationID),
		gstream.WithBrokerStr(brokers),
		gstream.WithGuarantee(guarantee),
	)
	if err != nil {
		logger.Error("configuration failed", slog.Any("error", err))
		os.Exit(1)
	}
	runtime, err := app.New(
		cfg, topology,
		app.WithLogger(logger),
		app.WithPrometheusRegisterer(prometheus.NewRegistry()),
	)
	if err != nil {
		logger.Error("application creation failed", slog.Any("error", err))
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if raw := os.Getenv("EXAMPLE_STOP_AFTER"); raw != "" {
		duration, parseErr := time.ParseDuration(raw)
		if parseErr != nil {
			logger.Error("invalid EXAMPLE_STOP_AFTER", slog.Any("error", parseErr))
			os.Exit(1)
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, duration)
		defer cancel()
	}
	if err := runtime.Run(ctx); err != nil {
		logger.Error("application stopped with error", slog.Any("error", err))
		os.Exit(1)
	}
}
