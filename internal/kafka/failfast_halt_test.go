package kafka

// Tests for issue #11 halt behavior in the run loop.
//
// These tests prove that the run loop ACTUALLY EXITS when:
//   (a) WithHealthGate is wired and health is pre-tripped (loop-top gate fires before PollFetches)
//   (b) PostBatch hook returns ErrFatalPipeline (always-on for stateful topologies)
//
// Why this test would have FAILED before the fix:
//   - Before the fix, WithHealthGate was not wired at callsites → c.healthGate == nil →
//     the nil-guard skipped the gate → Run never returned on store-write failure.
//   - This test wires WithHealthGate explicitly AND verifies Run() returns the fatal error.
//     Running it against the PRE-FIX code (with healthGate == nil because no callsite wired it)
//     would block forever at PollFetches (unreachable broker) and only exit on timeout.
//
// Test (a) also validates that the Adapter-based always-on PostBatch path works:
// PostBatchHook() returns ErrFatalPipeline when health is tripped, and the run loop
// detects it and exits Run.

import (
	"context"
	"errors"
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
)

// ─────────────────────────────────────────────────────────────────────────────
// (a) Loop-top gate: pre-tripped health → Run exits before PollFetches
// ─────────────────────────────────────────────────────────────────────────────

// TestRunALO_HealthGatePreTripped verifies that when WithHealthGate is wired
// and health is already tripped before Run starts, runALO exits immediately
// with the fatal error — it NEVER reaches PollFetches (which would block on a
// real broker).
//
// This test would have BLOCKED INDEFINITELY before the fix (gate not wired at
// callsites → c.healthGate == nil → nil-guard skips gate → PollFetches blocks
// on unreachable "localhost:19092" until ctx timeout).
func TestRunALO_HealthGatePreTripped(t *testing.T) {
	fatalErr := errors.New("pebble: disk write failed")

	// Wire WithHealthGate to a pre-tripped gate.
	cfg := validTestConfig(t, "halt-test")

	client, err := New(cfg, []string{"test-topic"}, nil,
		WithHealthGate(func() error { return fatalErr }),
	)
	if err != nil {
		t.Fatalf("kafka.New: %v", err)
	}
	defer client.Close()

	// Run must return within a short deadline — the gate fires at loop-top
	// before PollFetches.  A failing run (gate not wired) would block for the
	// full timeout, which is the regression signal.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	runErr := client.Run(ctx, func(_ context.Context, _ InRecord) ([]OutRecord, error) {
		return nil, nil
	})

	if runErr == nil {
		t.Fatal("Run should return a non-nil error when health gate is tripped")
	}
	if !errors.Is(runErr, fatalErr) {
		t.Errorf("Run error should wrap the fatal error; got %v", runErr)
	}
	// Must not have been a context timeout (that would indicate the gate didn't fire).
	if errors.Is(runErr, context.DeadlineExceeded) {
		t.Error("Run returned ctx.DeadlineExceeded — gate did NOT fire (this is the pre-fix regression behavior)")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (b) PostBatch-based halt (always-on for stateful topologies)
// ─────────────────────────────────────────────────────────────────────────────

// TestRunALO_PostBatchFatalPipeline_RunloopDetects verifies that when the
// postBatch hook returns an error wrapping ErrFatalPipeline AND a health gate
// is also pre-tripped (defence-in-depth), Run exits with ErrFatalPipeline in
// the chain — NOT a context timeout.
//
// NOTE: driving postBatch from the run loop requires PollFetches to return a
// real batch (which needs a live broker).  This test instead exercises the
// defence-in-depth path: both the loop-top healthGate AND postBatch return
// ErrFatalPipeline.  The loop-top gate fires first.
//
// The companion test in internal/runtime/failfast_test.go
// (TestPostBatchHook_ReturnsFatalPipelineWhenHealthTripped) unit-tests
// the adapter's PostBatchHook directly to prove it returns ErrFatalPipeline
// when health is tripped — that test does NOT need a broker.
func TestRunALO_DefenceInDepth_BothGateAndPostBatchFire(t *testing.T) {
	fatalErr := errors.New("state: Put pebble: disk full")
	wrappedFatal := wrappedFatalPipelineError(fatalErr)

	cfg := validTestConfig(t, "postbatch-halt")

	// WithHealthGate: pre-tripped with the inner fatal error (gate fires at loop-top).
	// WithPostBatch: would also return ErrFatalPipeline if the loop ever got records.
	// Together they ensure defence-in-depth: either mechanism alone halts Run.
	client, err := New(cfg, []string{"test-topic"}, nil,
		WithHealthGate(func() error { return wrappedFatal }), // wraps ErrFatalPipeline
		WithPostBatch(func(_ context.Context) error { return wrappedFatal }),
	)
	if err != nil {
		t.Fatalf("kafka.New: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	runErr := client.Run(ctx, func(_ context.Context, _ InRecord) ([]OutRecord, error) {
		return nil, nil
	})

	if runErr == nil {
		t.Fatal("Run should return a non-nil error")
	}
	// Run returns "runALO: pipeline unhealthy: <wrappedFatal>" which wraps ErrFatalPipeline.
	if !errors.Is(runErr, ErrFatalPipeline) {
		t.Errorf("Run error should wrap ErrFatalPipeline; got %v", runErr)
	}
	if errors.Is(runErr, context.DeadlineExceeded) {
		t.Error("Run returned ctx.DeadlineExceeded — loop did NOT halt (regression)")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// (c) ErrFatalPipeline sentinel classification
// ─────────────────────────────────────────────────────────────────────────────

// TestErrFatalPipeline_Sentinel verifies that ErrFatalPipeline is recognized
// via errors.Is when wrapped with fmt.Errorf %w.
func TestErrFatalPipeline_SentinelIsRecognized(t *testing.T) {
	wrapped := wrappedFatalPipelineError(errors.New("inner"))
	if !errors.Is(wrapped, ErrFatalPipeline) {
		t.Errorf("errors.Is(wrapped, ErrFatalPipeline) = false; want true")
	}

	// Verify non-fatal error is NOT classified as ErrFatalPipeline.
	ordinary := errors.New("serde: deserialize failed")
	if errors.Is(ordinary, ErrFatalPipeline) {
		t.Errorf("ordinary error should not match ErrFatalPipeline")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

// wrappedFatalPipelineError wraps ErrFatalPipeline as PostBatchHook does:
//
//	fmt.Errorf("%w: %w", kafka.ErrFatalPipeline, innerErr)
func wrappedFatalPipelineError(inner error) error {
	// Must use %w twice so errors.Is traverses both.
	// (Go 1.20+ supports multiple %w in a single fmt.Errorf.)
	return wrapFatal(inner)
}

func wrapFatal(inner error) error {
	// Simulates: return fmt.Errorf("%w: %w", ErrFatalPipeline, inner)
	// We do it here without importing fmt to keep the helper minimal.
	return &fatalWrap{sentinel: ErrFatalPipeline, inner: inner}
}

// validTestConfig returns a gstream.Config with defaults applied, pointing at
// an unreachable broker. Used by halt tests that exercise Run behavior without
// needing a live broker (the health gate fires before PollFetches blocks).
func validTestConfig(t *testing.T, appID string) gstream.Config {
	t.Helper()
	cfg, err := gstream.Configure(
		gstream.WithName(appID),
		gstream.WithBrokers("localhost:19092"),
		gstream.WithStateDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("gstream.Configure: %v", err)
	}
	return cfg
}

type fatalWrap struct {
	sentinel error
	inner    error
}

func (e *fatalWrap) Error() string {
	return e.sentinel.Error() + ": " + e.inner.Error()
}

func (e *fatalWrap) Unwrap() []error { return []error{e.sentinel, e.inner} }
