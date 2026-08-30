// Package app provides public lifecycle runtime for built gstream topologies.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/kafka"
	streamruntime "github.com/mortezaPRK/gstream/internal/runtime"
	"github.com/prometheus/client_golang/prometheus"
)

// Config is public gstream application configuration.
type Config = gstream.Config

// Option configures an App without exposing runtime implementation types.
type Option func(*options) error

type options struct {
	logger     *slog.Logger
	registerer prometheus.Registerer
}

// WithLogger sets structured logger used by runtime.
func WithLogger(logger *slog.Logger) Option {
	return func(options *options) error {
		if logger == nil {
			return errors.New("app.WithLogger: logger must not be nil")
		}
		options.logger = logger
		return nil
	}
}

// WithPrometheusRegisterer registers application metrics with registerer.
// Registration conflicts make New fail; gstream never uses global registry.
func WithPrometheusRegisterer(registerer prometheus.Registerer) Option {
	return func(options *options) error {
		if registerer == nil {
			return errors.New("app.WithPrometheusRegisterer: registerer must not be nil")
		}
		options.registerer = registerer
		return nil
	}
}

// App validates, starts, runs, and closes one built topology.
type App struct {
	cfg      Config
	topology *gstream.BuiltTopology
	logger   *slog.Logger
	metrics  *metrics

	mu      sync.Mutex
	running bool
	started bool
	closed  bool
	cancel  context.CancelFunc
	done    chan struct{}
	client  *kafka.Client
	adapter *streamruntime.Adapter
}

// New constructs public application runtime. Kafka access starts in Run.
func New(cfg Config, topology *gstream.BuiltTopology, appOptions ...Option) (*App, error) {
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("app.New: invalid config: %w", err)
	}
	if topology == nil {
		return nil, errors.New("app.New: topology must not be nil")
	}
	if err := validateTopology(topology); err != nil {
		return nil, fmt.Errorf("app.New: invalid topology: %w", err)
	}
	settings := options{logger: slog.Default()}
	for _, option := range appOptions {
		if option == nil {
			return nil, errors.New("app.New: option must not be nil")
		}
		if err := option(&settings); err != nil {
			return nil, err
		}
	}
	metricSet, err := newMetrics(cfg.ApplicationID, settings.registerer)
	if err != nil {
		return nil, fmt.Errorf("app.New: metrics: %w", err)
	}
	return &App{cfg: cfg, topology: topology, logger: settings.logger, metrics: metricSet}, nil
}

func validateTopology(topology *gstream.BuiltTopology) error {
	if topology.Topology == nil {
		return errors.New("processor topology must not be nil")
	}
	knownSources := make(map[string]struct{}, len(topology.Sources)+len(topology.RepartitionBindings))
	for name := range topology.Sources {
		knownSources[name] = struct{}{}
	}
	knownSinks := make(map[string]struct{}, len(topology.Sinks)+len(topology.RepartitionBindings))
	for name := range topology.Sinks {
		knownSinks[name] = struct{}{}
	}
	for _, binding := range topology.RepartitionBindings {
		knownSources[binding.SourceName] = struct{}{}
		knownSinks[binding.SinkName] = struct{}{}
	}
	sources := topology.Topology.SourceNames()
	if len(sources) == 0 {
		return errors.New("topology has no source nodes")
	}
	for _, name := range sources {
		if _, ok := knownSources[name]; !ok {
			return fmt.Errorf("source node %q has no binding", name)
		}
	}
	for _, name := range topology.Topology.SinkNames() {
		if _, ok := knownSinks[name]; !ok {
			if _, internal := topology.InternalSinks[name]; internal {
				continue
			}
			return fmt.Errorf("sink node %q has no binding", name)
		}
	}
	return nil
}

// Run performs startup planning, restores state, and blocks until cancellation,
// Close, or fatal processing error. App may run once.
func (a *App) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("app.Run: context must not be nil")
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return errors.New("app.Run: app is closed")
	}
	if a.running {
		a.mu.Unlock()
		return errors.New("app.Run: app is already running")
	}
	if a.started {
		a.mu.Unlock()
		return errors.New("app.Run: app has already run")
	}
	a.running = true
	a.started = true
	a.done = make(chan struct{})
	runCtx, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	done := a.done
	a.mu.Unlock()

	defer func() {
		cancel()
		a.mu.Lock()
		a.running = false
		a.cancel = nil
		a.client = nil
		a.adapter = nil
		a.mu.Unlock()
		close(done)
	}()
	if runCtx.Err() != nil {
		return nil
	}

	if err := kafka.PrepareTopology(runCtx, a.cfg, a.topology); err != nil {
		return fmt.Errorf("app.Run: prepare topology: %w", err)
	}
	adapter, err := streamruntime.NewAdapter(a.topology, a.cfg, a.logger)
	if err != nil {
		return fmt.Errorf("app.Run: create runtime: %w", err)
	}
	a.mu.Lock()
	a.adapter = adapter
	a.mu.Unlock()
	defer func() {
		if closeErr := adapter.Close(); closeErr != nil {
			a.logger.Error("close runtime", slog.Any("error", closeErr))
		}
	}()

	restoreTimer := prometheus.NewTimer(a.metrics.restoreDuration)
	if err := adapter.BootstrapGlobalStores(runCtx); err != nil {
		restoreTimer.ObserveDuration()
		return fmt.Errorf("app.Run: bootstrap global stores: %w", err)
	}
	restoreTimer.ObserveDuration()
	if err := adapter.RunGlobalConsumers(runCtx); err != nil {
		return fmt.Errorf("app.Run: start global consumers: %w", err)
	}

	metricsCtx, metricsCancel := context.WithCancel(runCtx)
	var metricsWaitGroup sync.WaitGroup
	metricsWaitGroup.Add(1)
	go func() {
		defer metricsWaitGroup.Done()
		a.collectMetrics(metricsCtx, adapter)
	}()
	defer func() {
		metricsCancel()
		metricsWaitGroup.Wait()
	}()

	onAssigned, onRevoked := adapter.LifecycleCallbacks()
	measuredAssigned := func(ctx context.Context, assigned map[string][]int32) error {
		timer := prometheus.NewTimer(a.metrics.restoreDuration)
		err := onAssigned(ctx, assigned)
		timer.ObserveDuration()
		if err == nil {
			a.metrics.restoreProgress.Set(1)
		}
		return err
	}
	clientOptions := []kafka.ClientOption{
		kafka.WithLifecycle(measuredAssigned, onRevoked),
		kafka.WithHealthGate(adapter.HealthGateHook()),
		kafka.WithObserver(a.metrics.observer()),
	}
	if a.cfg.Guarantee == gstream.ExactlyOnce {
		clientOptions = append(clientOptions,
			kafka.WithPostBatch(adapter.PostBatchSweepHook()),
			kafka.WithChangelogFlusher(adapter.ChangelogFlusherHook()),
		)
	} else {
		clientOptions = append(clientOptions, kafka.WithPostBatch(adapter.PostBatchHook()))
	}
	client, err := kafka.New(a.cfg, adapter.SourceTopics(), a.logger, clientOptions...)
	if err != nil {
		return fmt.Errorf("app.Run: create Kafka client: %w", err)
	}
	a.mu.Lock()
	a.client = client
	a.mu.Unlock()
	defer client.Close()

	if err := client.Run(runCtx, a.metrics.wrapProcess(adapter.ProcessFunc())); err != nil {
		return fmt.Errorf("app.Run: %w", err)
	}
	return nil
}

func (a *App) collectMetrics(ctx context.Context, adapter *streamruntime.Adapter) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		snapshot := adapter.StorageMetrics()
		a.metrics.observeStorage(snapshot.SizeBytes, snapshot.CacheHits, snapshot.CacheMisses)
		var late int64
		for _, binding := range a.topology.WindowStoreBindings {
			if binding.LateCount != nil {
				late += binding.LateCount()
			}
		}
		for _, binding := range a.topology.SessionStoreBindings {
			if binding.LateCount != nil {
				late += binding.LateCount()
			}
		}
		a.metrics.observeLate(late)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Close stops Run and waits for lifecycle cleanup. It is idempotent.
func (a *App) Close() error {
	a.mu.Lock()
	if a.closed {
		done := a.done
		a.mu.Unlock()
		if done != nil {
			<-done
		}
		return nil
	}
	a.closed = true
	cancel := a.cancel
	done := a.done
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	return nil
}
