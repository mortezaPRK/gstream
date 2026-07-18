package kafka

import (
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
)

// validConfig returns a minimal valid gstream.Config for use in unit tests.
func validConfig() gstream.Config {
	cfg := gstream.Config{
		ApplicationID: "test-app",
		Brokers:       []string{"localhost:9092"},
	}
	cfg.ApplyDefaults()
	return cfg
}

// ---------------------------------------------------------------------------
// Config validation
// ---------------------------------------------------------------------------

func TestNew_RejectsEmptyApplicationID(t *testing.T) {
	cfg := validConfig()
	cfg.ApplicationID = ""
	_, err := New(cfg, []string{"topic"}, nil)
	if err == nil {
		t.Fatal("expected error for empty ApplicationID, got nil")
	}
}

func TestNew_RejectsEmptyBrokers(t *testing.T) {
	cfg := validConfig()
	cfg.Brokers = nil
	_, err := New(cfg, []string{"topic"}, nil)
	if err == nil {
		t.Fatal("expected error for empty Brokers, got nil")
	}
}

func TestNew_RejectsEmptyTopics(t *testing.T) {
	cfg := validConfig()
	_, err := New(cfg, nil, nil)
	if err == nil {
		t.Fatal("expected error for empty topics, got nil")
	}
	_, err = New(cfg, []string{}, nil)
	if err == nil {
		t.Fatal("expected error for empty topics slice, got nil")
	}
}

func TestNew_RejectsNegativeNumTaskThreads(t *testing.T) {
	cfg := validConfig()
	cfg.NumTaskThreads = -1
	_, err := New(cfg, []string{"topic"}, nil)
	if err == nil {
		t.Fatal("expected error for negative NumTaskThreads, got nil")
	}
}

func TestNew_RejectsZeroCommitInterval(t *testing.T) {
	cfg := validConfig()
	cfg.CommitInterval = 0
	_, err := New(cfg, []string{"topic"}, nil)
	if err == nil {
		t.Fatal("expected error for zero CommitInterval, got nil")
	}
}

// ---------------------------------------------------------------------------
// ApplyDefaults wiring
// ---------------------------------------------------------------------------

func TestApplyDefaults_FillsCommitInterval(t *testing.T) {
	cfg := gstream.Config{
		ApplicationID: "app",
		Brokers:       []string{"b:9092"},
	}
	cfg.ApplyDefaults()
	if cfg.CommitInterval != 100*time.Millisecond {
		t.Fatalf("expected CommitInterval=100ms, got %s", cfg.CommitInterval)
	}
}

func TestApplyDefaults_FillsNumTaskThreads(t *testing.T) {
	cfg := gstream.Config{
		ApplicationID: "app",
		Brokers:       []string{"b:9092"},
	}
	cfg.ApplyDefaults()
	if cfg.NumTaskThreads <= 0 {
		t.Fatalf("expected NumTaskThreads > 0, got %d", cfg.NumTaskThreads)
	}
}

func TestApplyDefaults_DefaultGuaranteeIsALO(t *testing.T) {
	cfg := gstream.Config{
		ApplicationID: "app",
		Brokers:       []string{"b:9092"},
	}
	cfg.ApplyDefaults()
	if cfg.Guarantee != gstream.AtLeastOnce {
		t.Fatalf("expected default Guarantee=AtLeastOnce, got %v", cfg.Guarantee)
	}
}

// ---------------------------------------------------------------------------
// buildOpts — pure helper; no broker needed
// ---------------------------------------------------------------------------

func TestBuildOpts_ReturnsSomeOpts(t *testing.T) {
	cfg := validConfig()
	opts := buildOpts(cfg, []string{"input-topic"}, nil)
	if len(opts) == 0 {
		t.Fatal("expected non-empty opts slice from buildOpts")
	}
}

// ---------------------------------------------------------------------------
// Record types
// ---------------------------------------------------------------------------

func TestInRecord_ZeroValue(t *testing.T) {
	var r InRecord
	if r.Topic != "" || r.Partition != 0 || r.Offset != 0 {
		t.Fatal("unexpected non-zero InRecord zero value")
	}
}

func TestOutRecord_Fields(t *testing.T) {
	r := OutRecord{Topic: "sink", Key: []byte("k"), Value: []byte("v")}
	if r.Topic != "sink" {
		t.Fatalf("expected Topic=sink, got %s", r.Topic)
	}
}
