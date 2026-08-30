// Command eos-recovery runs EOS count, stops it, then starts from empty local
// state so committed changelog recovery is observable before processing resumes.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/app"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	builder := gstream.NewStreamBuilder()
	input := gstream.Stream(
		builder, "eos-input", "input",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{},
	)
	input.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		Count("counts").
		To("eos-output", gstream.JSONSerde[string]{}, gstream.JSONSerde[int64]{})
	topology := builder.Build()

	baseStateDir, err := os.MkdirTemp("", "gstream-eos-recovery-")
	if err != nil {
		logger.Error("create temporary state directory", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() { _ = os.RemoveAll(baseStateDir) }()

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	restartAfter := durationFromEnv(logger, "EXAMPLE_RESTART_AFTER", 8*time.Second)
	if raw := os.Getenv("EXAMPLE_STOP_AFTER"); raw != "" {
		stopAfter := durationFromEnv(logger, "EXAMPLE_STOP_AFTER", 0)
		var cancel context.CancelFunc
		rootCtx, cancel = context.WithTimeout(rootCtx, stopAfter)
		defer cancel()
	}

	logger.Info("starting EOS phase 1", slog.Duration("restartAfter", restartAfter))
	phaseOneCtx, stopPhaseOne := context.WithTimeout(rootCtx, restartAfter)
	runPhase(phaseOneCtx, logger, topology, filepath.Join(baseStateDir, "phase-1"))
	stopPhaseOne()
	if rootCtx.Err() != nil {
		return
	}

	logger.Info("starting EOS phase 2 from empty local state; restoring committed changelog")
	runPhase(rootCtx, logger, topology, filepath.Join(baseStateDir, "phase-2"))
}

func runPhase(ctx context.Context, logger *slog.Logger, topology *gstream.BuiltTopology, stateDir string) {
	brokers := os.Getenv("BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	cfg, err := gstream.Configure(
		gstream.WithName("eos-recovery-example"),
		gstream.WithBrokerStr(brokers),
		gstream.WithGuarantee(gstream.ExactlyOnce),
		gstream.WithStateDir(stateDir),
	)
	if err != nil {
		logger.Error("configuration failed", slog.Any("error", err))
		os.Exit(1)
	}
	cfg.InstanceID = "controlled-restart"
	runtime, err := app.New(
		cfg, topology,
		app.WithLogger(logger),
		app.WithPrometheusRegisterer(prometheus.NewRegistry()),
	)
	if err != nil {
		logger.Error("application creation failed", slog.Any("error", err))
		os.Exit(1)
	}
	if err := runtime.Run(ctx); err != nil {
		logger.Error("EOS phase stopped with error", slog.Any("error", err))
		os.Exit(1)
	}
}

func durationFromEnv(logger *slog.Logger, name string, fallback time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		logger.Error("invalid duration", slog.String("variable", name), slog.String("value", raw))
		os.Exit(1)
	}
	return duration
}
