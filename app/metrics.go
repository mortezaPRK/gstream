package app

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mortezaPRK/gstream/internal/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	mu               sync.Mutex
	lastCacheHits    int64
	lastCacheMisses  int64
	lastLateRecords  int64
	recordsIn        prometheus.Counter
	recordsOut       prometheus.Counter
	processing       prometheus.Histogram
	lag              prometheus.Gauge
	commits          prometheus.Counter
	commitDuration   prometheus.Histogram
	restoreProgress  prometheus.Gauge
	restoreDuration  prometheus.Histogram
	lateRecords      prometheus.Counter
	droppedRecords   prometheus.Counter
	transactionAbort prometheus.Counter
	storeSize        prometheus.Gauge
	pebbleCacheHits  prometheus.Counter
	pebbleCacheMiss  prometheus.Counter
}

func newMetrics(applicationID string, registerer prometheus.Registerer) (*metrics, error) {
	labels := prometheus.Labels{"application_id": applicationID}
	metricSet := &metrics{
		recordsIn:        prometheus.NewCounter(prometheus.CounterOpts{Namespace: "gstream", Name: "records_in_total", ConstLabels: labels}),
		recordsOut:       prometheus.NewCounter(prometheus.CounterOpts{Namespace: "gstream", Name: "records_out_total", ConstLabels: labels}),
		processing:       prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "gstream", Name: "processing_duration_seconds", ConstLabels: labels}),
		lag:              prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "gstream", Name: "consumer_lag", ConstLabels: labels}),
		commits:          prometheus.NewCounter(prometheus.CounterOpts{Namespace: "gstream", Name: "commits_total", ConstLabels: labels}),
		commitDuration:   prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "gstream", Name: "commit_duration_seconds", ConstLabels: labels}),
		restoreProgress:  prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "gstream", Name: "restore_progress_ratio", ConstLabels: labels}),
		restoreDuration:  prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "gstream", Name: "restore_duration_seconds", ConstLabels: labels}),
		lateRecords:      prometheus.NewCounter(prometheus.CounterOpts{Namespace: "gstream", Name: "late_records_total", ConstLabels: labels}),
		droppedRecords:   prometheus.NewCounter(prometheus.CounterOpts{Namespace: "gstream", Name: "dropped_records_total", ConstLabels: labels}),
		transactionAbort: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "gstream", Name: "transaction_aborts_total", ConstLabels: labels}),
		storeSize:        prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "gstream", Name: "store_size_bytes", ConstLabels: labels}),
		pebbleCacheHits:  prometheus.NewCounter(prometheus.CounterOpts{Namespace: "gstream", Name: "pebble_cache_hits_total", ConstLabels: labels}),
		pebbleCacheMiss:  prometheus.NewCounter(prometheus.CounterOpts{Namespace: "gstream", Name: "pebble_cache_misses_total", ConstLabels: labels}),
	}
	if registerer == nil {
		return metricSet, nil
	}
	collectors := []prometheus.Collector{
		metricSet.recordsIn, metricSet.recordsOut, metricSet.processing, metricSet.lag,
		metricSet.commits, metricSet.commitDuration, metricSet.restoreProgress,
		metricSet.restoreDuration, metricSet.lateRecords, metricSet.droppedRecords,
		metricSet.transactionAbort, metricSet.storeSize, metricSet.pebbleCacheHits,
		metricSet.pebbleCacheMiss,
	}
	registered := make([]prometheus.Collector, 0, len(collectors))
	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			for _, previous := range registered {
				registerer.Unregister(previous)
			}
			return nil, fmt.Errorf("register collector: %w", err)
		}
		registered = append(registered, collector)
	}
	return metricSet, nil
}

func (m *metrics) wrapProcess(process kafka.ProcessFunc) kafka.ProcessFunc {
	return func(ctx context.Context, record kafka.InRecord) ([]kafka.OutRecord, error) {
		m.recordsIn.Inc()
		started := time.Now()
		out, err := process(ctx, record)
		m.processing.Observe(time.Since(started).Seconds())
		if err != nil {
			m.droppedRecords.Inc()
			return nil, err
		}
		m.recordsOut.Add(float64(len(out)))
		return out, nil
	}
}

func (m *metrics) observer() kafka.Observer {
	return kafka.Observer{
		Commit: func(duration time.Duration) {
			m.commits.Inc()
			m.commitDuration.Observe(duration.Seconds())
		},
		TransactionAbort: m.transactionAbort.Inc,
		Lag:              func(lag int64) { m.lag.Set(float64(lag)) },
	}
}

func (m *metrics) observeStorage(sizeBytes uint64, cacheHits, cacheMisses int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.storeSize.Set(float64(sizeBytes))
	if cacheHits >= m.lastCacheHits {
		m.pebbleCacheHits.Add(float64(cacheHits - m.lastCacheHits))
	}
	if cacheMisses >= m.lastCacheMisses {
		m.pebbleCacheMiss.Add(float64(cacheMisses - m.lastCacheMisses))
	}
	m.lastCacheHits = cacheHits
	m.lastCacheMisses = cacheMisses
}

func (m *metrics) observeLate(total int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if total >= m.lastLateRecords {
		m.lateRecords.Add(float64(total - m.lastLateRecords))
	}
	m.lastLateRecords = total
}
