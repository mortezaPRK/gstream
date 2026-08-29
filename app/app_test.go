package app_test

import (
	"context"
	"strings"
	"testing"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/app"
	"github.com/prometheus/client_golang/prometheus"
)

func testTopology() *gstream.BuiltTopology {
	builder := gstream.NewStreamBuilder()
	stream := gstream.Stream(
		builder, "input", "source", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{},
	)
	stream.To("output", "sink", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	return builder.Build()
}

func testConfig() app.Config {
	return app.Config{ApplicationID: "test-app", Brokers: []string{"localhost:9092"}}
}

func TestNewValidatesInputs(t *testing.T) {
	if _, err := app.New(app.Config{}, testTopology()); err == nil {
		t.Fatal("New() error = nil, want invalid config error")
	}
	if _, err := app.New(testConfig(), nil); err == nil {
		t.Fatal("New() error = nil, want nil topology error")
	}
	if _, err := app.New(testConfig(), &gstream.BuiltTopology{}); err == nil {
		t.Fatal("New() error = nil, want malformed topology error")
	}
	if _, err := app.New(testConfig(), testTopology(), nil); err == nil {
		t.Fatal("New() error = nil, want nil option error")
	}
	if _, err := app.New(testConfig(), testTopology(), app.WithLogger(nil)); err == nil {
		t.Fatal("New() error = nil, want nil logger error")
	}
}

func TestPrometheusRegistrationConflict(t *testing.T) {
	registry := prometheus.NewRegistry()
	if _, err := app.New(testConfig(), testTopology(), app.WithPrometheusRegisterer(registry)); err != nil {
		t.Fatalf("first New() error = %v", err)
	}
	before, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() before conflict: %v", err)
	}
	_, err = app.New(testConfig(), testTopology(), app.WithPrometheusRegisterer(registry))
	if err == nil || !strings.Contains(err.Error(), "duplicate metrics collector") {
		t.Fatalf("second New() error = %v, want registration conflict", err)
	}
	after, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() after conflict: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("metric families after conflict = %d, want %d", len(after), len(before))
	}
}

func TestCloseIsIdempotentAndPreventsRun(t *testing.T) {
	runtime, err := app.New(testConfig(), testTopology())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if err := runtime.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Run() error = %v, want closed error", err)
	}
}

func TestRunRejectsNilContext(t *testing.T) {
	runtime, err := app.New(testConfig(), testTopology())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Run(nil); err == nil || !strings.Contains(err.Error(), "context") { //nolint:staticcheck // defensive nil guard
		t.Fatalf("Run(nil) error = %v, want context error", err)
	}
}

func TestRunWithCancelledContextStopsCleanlyAndRunsOnce(t *testing.T) {
	runtime, err := app.New(testConfig(), testTopology())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runtime.Run(ctx); err != nil {
		t.Fatalf("Run(cancelled) error = %v, want nil", err)
	}
	if err := runtime.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "already run") {
		t.Fatalf("second Run() error = %v, want already run", err)
	}
}
