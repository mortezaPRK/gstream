package gstream

import (
	"testing"
	"time"
)

// TestApplyDefaults_RestoreCatchUpTimeout asserts that ApplyDefaults fills in
// 2s when RestoreCatchUpTimeout is zero, and leaves an explicit value unchanged.
func TestApplyDefaults_RestoreCatchUpTimeout(t *testing.T) {
	t.Run("zero fills 2s", func(t *testing.T) {
		var c Config
		c.ApplyDefaults()
		if c.RestoreCatchUpTimeout != 2*time.Second {
			t.Errorf("RestoreCatchUpTimeout: got %s, want 2s", c.RestoreCatchUpTimeout)
		}
	})

	t.Run("non-zero preserved", func(t *testing.T) {
		c := Config{RestoreCatchUpTimeout: 500 * time.Millisecond}
		c.ApplyDefaults()
		if c.RestoreCatchUpTimeout != 500*time.Millisecond {
			t.Errorf("RestoreCatchUpTimeout: got %s, want 500ms", c.RestoreCatchUpTimeout)
		}
	})
}

// TestConfigure_RestoreCatchUpTimeout asserts that Configure applies the default
// and that WithRestoreCatchUpTimeout overrides it.
func TestConfigure_RestoreCatchUpTimeout(t *testing.T) {
	t.Run("default via Configure", func(t *testing.T) {
		cfg, err := Configure(
			WithName("app"),
			WithBrokers("broker:9092"),
		)
		if err != nil {
			t.Fatalf("Configure: %v", err)
		}
		if cfg.RestoreCatchUpTimeout != 2*time.Second {
			t.Errorf("RestoreCatchUpTimeout default: got %s, want 2s", cfg.RestoreCatchUpTimeout)
		}
	})

	t.Run("WithRestoreCatchUpTimeout sets value", func(t *testing.T) {
		cfg, err := Configure(
			WithName("app"),
			WithBrokers("broker:9092"),
			WithRestoreCatchUpTimeout(750*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("Configure: %v", err)
		}
		if cfg.RestoreCatchUpTimeout != 750*time.Millisecond {
			t.Errorf("RestoreCatchUpTimeout: got %s, want 750ms", cfg.RestoreCatchUpTimeout)
		}
	})
}

// TestValidate_RestoreCatchUpTimeout asserts that Validate rejects a negative
// RestoreCatchUpTimeout (explicit negative bypasses zero-check in ApplyDefaults).
func TestValidate_RestoreCatchUpTimeout(t *testing.T) {
	c := Config{
		ApplicationID:         "app",
		Brokers:               []string{"b:9092"},
		NumTaskThreads:        1,
		CommitInterval:        100 * time.Millisecond,
		RestoreCatchUpTimeout: -1 * time.Second,
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate: expected error for negative RestoreCatchUpTimeout, got nil")
	}
	got := err.Error()
	if got == "" {
		t.Fatalf("Validate error empty")
	}
	const want = "RestoreCatchUpTimeout must be positive"
	if !containsSubstr(got, want) {
		t.Errorf("Validate error %q does not contain %q", got, want)
	}
}

func containsSubstr(s, sub string) bool {
	if len(sub) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
