package app

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserveStorageAndLateUseCounterDeltas(t *testing.T) {
	metrics, err := newMetrics("metrics-test", nil)
	if err != nil {
		t.Fatalf("newMetrics() error = %v", err)
	}
	metrics.observeStorage(100, 5, 2)
	metrics.observeStorage(120, 8, 4)
	metrics.observeLate(3)
	metrics.observeLate(7)

	if got := testutil.ToFloat64(metrics.storeSize); got != 120 {
		t.Errorf("store size = %v, want 120", got)
	}
	if got := testutil.ToFloat64(metrics.pebbleCacheHits); got != 8 {
		t.Errorf("cache hits = %v, want 8", got)
	}
	if got := testutil.ToFloat64(metrics.pebbleCacheMiss); got != 4 {
		t.Errorf("cache misses = %v, want 4", got)
	}
	if got := testutil.ToFloat64(metrics.lateRecords); got != 7 {
		t.Errorf("late records = %v, want 7", got)
	}
}

func TestMetricRegistrationFailureRollsBackEarlierCollectors(t *testing.T) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "gstream",
		Name:      "store_size_bytes",
		ConstLabels: prometheus.Labels{
			"application_id": "rollback-test",
		},
	}))
	if _, err := newMetrics("rollback-test", registry); err == nil {
		t.Fatal("newMetrics() error = nil, want conflict")
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	if len(families) != 1 || families[0].GetName() != "gstream_store_size_bytes" {
		t.Fatalf("families after rollback = %v, want only pre-existing store size", families)
	}
}
