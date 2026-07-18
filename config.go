package gstream

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// Guarantee selects the processing guarantee for the application (§4, §13).
type Guarantee int

const (
	// AtLeastOnce is the default guarantee. Offsets are committed after outputs are
	// acknowledged. On crash, records after the last commit may be reprocessed,
	// so duplicate outputs are possible. Lowest latency path (§4.1).
	AtLeastOnce Guarantee = iota

	// ExactlyOnce uses Kafka transactions so that sink writes, changelog writes,
	// and consumed offsets commit atomically. No duplicate outputs, but latency is
	// bounded by the commit interval rather than by processing time (§4.2).
	ExactlyOnce
)

// String returns a human-readable name for the guarantee level.
func (g Guarantee) String() string {
	switch g {
	case AtLeastOnce:
		return "AtLeastOnce"
	case ExactlyOnce:
		return "ExactlyOnce"
	default:
		return fmt.Sprintf("Guarantee(%d)", int(g))
	}
}

// Config is the only public configuration surface for gstream (§13).
//
// franz-go and Pebble are intentionally hidden: callers never import or reference
// kgo.* or pebble.* types. gstream picks sane defaults for both libraries and
// enforces invariants (e.g. ReadCommitted isolation under EOS) that cannot be
// overridden by callers. An Advanced escape hatch for power users is planned
// post-v1 (§13).
//
// Use ApplyDefaults to fill in zero values, then Validate to confirm the result
// is sound before passing Config to a topology builder or runtime.
type Config struct {
	// ApplicationID is the logical name of this stream application. It becomes the
	// Kafka consumer-group ID and the TransactionalID prefix for EOS (§4.2, §14).
	// Must be non-empty.
	ApplicationID string

	// Brokers is the list of Kafka bootstrap broker addresses (host:port). At least
	// one must be supplied.
	Brokers []string

	// Guarantee selects the processing guarantee (ALO or EOS). Defaults to
	// AtLeastOnce if zero (§4, §13).
	Guarantee Guarantee

	// StateDir is the root directory under which Pebble stores local state. Each
	// store gets its own sub-directory keyed by ApplicationID and store name.
	// Defaults to an OS-temp-derived path: os.TempDir()/gstream-<ApplicationID>.
	// The directory is created on first use (§5, §13).
	StateDir string

	// NumTaskThreads is the maximum number of concurrent task-processing goroutines.
	// Concurrency is min(assigned partitions, NumTaskThreads) at runtime, so setting
	// this higher than your partition count is harmless (§7).
	// Defaults to runtime.GOMAXPROCS(0). Must be non-negative after defaults.
	NumTaskThreads int

	// CommitInterval controls how frequently offsets (ALO) or Kafka transactions
	// (EOS) are committed. Smaller values reduce reprocessing on restart but
	// increase commit overhead. Defaults to 100 ms (§4, §11, §13).
	CommitInterval time.Duration
}

// ApplyDefaults fills in zero-value fields with sensible production defaults.
// Most fields follow a "don't overwrite non-zero" rule, with one notable
// exception: Guarantee's zero value IS the default (AtLeastOnce == 0), so an
// explicitly set AtLeastOnce and an unset Guarantee are indistinguishable — both
// resolve to AtLeastOnce. EOS is always set explicitly (ExactlyOnce != 0) and is
// therefore never overwritten. This is intentional by design (§4.1).
func (c *Config) ApplyDefaults() {
	if c.Guarantee == 0 {
		c.Guarantee = AtLeastOnce
	}
	if c.NumTaskThreads == 0 {
		c.NumTaskThreads = runtime.GOMAXPROCS(0)
	}
	if c.CommitInterval == 0 {
		c.CommitInterval = 100 * time.Millisecond
	}
	if c.StateDir == "" && c.ApplicationID != "" {
		c.StateDir = filepath.Join(os.TempDir(), "gstream-"+c.ApplicationID)
	}
}

// Validate checks that the Config is complete and self-consistent after defaults
// have been applied. It returns a non-nil error describing every violated constraint.
// Call ApplyDefaults before Validate so defaults are in place.
func (c *Config) Validate() error {
	var errs []error

	if c.ApplicationID == "" {
		errs = append(errs, errors.New("ApplicationID must not be empty"))
	}
	if len(c.Brokers) == 0 {
		errs = append(errs, errors.New("Brokers must contain at least one address"))
	}
	if c.NumTaskThreads < 0 {
		errs = append(errs, fmt.Errorf("NumTaskThreads must be non-negative, got %d", c.NumTaskThreads))
	}
	if c.CommitInterval <= 0 {
		errs = append(errs, fmt.Errorf("CommitInterval must be positive, got %s", c.CommitInterval))
	}

	return errors.Join(errs...)
}
