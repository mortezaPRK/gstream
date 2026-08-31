package gstream

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Guarantee selects the processing guarantee for the application.
type Guarantee int

const (
	// AtLeastOnce is the default guarantee. Offsets are committed after outputs are
	// acknowledged. On crash, records after the last commit may be reprocessed,
	// so duplicate outputs are possible.
	AtLeastOnce Guarantee = iota

	// ExactlyOnce uses Kafka transactions so that sink writes, changelog writes,
	// and consumed offsets commit atomically. No duplicate outputs, but latency is
	// bounded by the commit interval rather than by processing time.
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

// Config is the public configuration surface for gstream.
//
// franz-go is intentionally hidden: callers never import or reference kgo types.
// Stateful applications supply a StoreProvider implementation.
//
// Use ApplyDefaults to fill in zero values, then Validate to confirm the result
// is sound before passing Config to a topology builder or runtime.
type Config struct {
	// ApplicationID is the logical name of this stream application. It becomes the
	// Kafka consumer-group ID and the TransactionalID prefix for EOS.
	// Must be non-empty.
	ApplicationID string

	// Brokers is the list of Kafka bootstrap broker addresses (host:port). At least
	// one must be supplied.
	Brokers []string

	// Guarantee selects the processing guarantee (ALO or EOS). Defaults to
	// AtLeastOnce if zero.
	Guarantee Guarantee

	// StateDir is the root directory passed to StoreProvider for local state.
	// Defaults to os.TempDir()/gstream-<ApplicationID>.
	StateDir string

	// InstanceID is an optional operator-supplied identifier for this specific
	// process instance. When non-empty it is used verbatim as the per-instance
	// suffix in the EOS TransactionalID ("gstream-<ApplicationID>-<InstanceID>").
	//
	// When empty (the common case) gstream auto-derives the instance ID at EOS
	// startup: it reads StateDir/instance-id, creating and persisting a new UUID
	// if the file does not exist. Persisting the ID ensures the same instance
	// reuses its TransactionalID across restarts, which is required for EOS
	// crash-safety: a restarted process must present the same TransactionalID so
	// the broker can fence its own zombie producer and recover/abort any prior
	// pending transaction.
	//
	// Note: if StateDir is ephemeral (wiped on restart) the persisted ID is also
	// lost, so a new UUID is generated. This is acceptable because local
	// state is likewise lost; the instance restores from changelog on startup,
	// and the old zombie transactional producer times out via TransactionTimeout.
	// Operators running on ephemeral storage who want a stable ID should set
	// InstanceID explicitly (e.g. from a pod name or environment variable).
	//
	// Ignored for AtLeastOnce (ALO) guarantee; only used in ExactlyOnce (EOS) mode.
	InstanceID string

	// NumTaskThreads is the maximum number of concurrent task-processing goroutines.
	// Defaults to runtime.GOMAXPROCS(0). Must be non-negative after defaults.
	NumTaskThreads int

	// CommitInterval controls how frequently offsets (ALO) or Kafka transactions
	// (EOS) are committed. Defaults to 100 ms.
	CommitInterval time.Duration

	// RestoreCatchUpTimeout bounds how long changelog restore waits for straggler
	// committed records after the partition's stable offset reaches the
	// high-watermark, before concluding restore is complete. Needed because Kafka
	// exposes no deterministic "caught up" signal for direct-partition consumers;
	// restore detects end-of-committed-data past an aborted transaction tail via
	// this bounded poll. Defaults to 2s. Lower only for low-latency brokers — too
	// low risks incomplete restore on large multi-response changelogs under load.
	RestoreCatchUpTimeout time.Duration

	// StoreProvider opens local state backends for stateful topologies. It may be
	// nil for stateless topologies. Implementations live under
	// github.com/mortezaPRK/gstream/stores.
	StoreProvider StoreProvider
}

// Option is a functional option for Configure.
type Option func(*Config)

// WithName sets ApplicationID.
func WithName(name string) Option {
	return func(c *Config) { c.ApplicationID = name }
}

// WithBrokers sets the broker list from individual address strings.
func WithBrokers(brokers ...string) Option {
	return func(c *Config) { c.Brokers = brokers }
}

// WithBrokerStr parses a comma-separated broker string (e.g. "b1:9092,b2:9092"),
// trims spaces from each element, and sets Brokers.
func WithBrokerStr(csv string) Option {
	return func(c *Config) {
		parts := strings.Split(csv, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			if s := strings.TrimSpace(p); s != "" {
				out = append(out, s)
			}
		}
		c.Brokers = out
	}
}

// WithStateDir sets StateDir.
func WithStateDir(dir string) Option {
	return func(c *Config) { c.StateDir = dir }
}

// WithGuarantee sets the processing guarantee.
func WithGuarantee(g Guarantee) Option {
	return func(c *Config) { c.Guarantee = g }
}

// WithCommitInterval sets CommitInterval.
func WithCommitInterval(d time.Duration) Option {
	return func(c *Config) { c.CommitInterval = d }
}

// WithRestoreCatchUpTimeout sets RestoreCatchUpTimeout.
func WithRestoreCatchUpTimeout(d time.Duration) Option {
	return func(c *Config) { c.RestoreCatchUpTimeout = d }
}

// WithNumTaskThreads sets NumTaskThreads.
func WithNumTaskThreads(n int) Option {
	return func(c *Config) { c.NumTaskThreads = n }
}

// WithStoreProvider sets local state backend implementation.
func WithStoreProvider(provider StoreProvider) Option {
	return func(c *Config) { c.StoreProvider = provider }
}

// WithDefaults is an explicit-intent marker accepted by Configure.
// Defaults are always applied automatically by Configure regardless of whether
// this option is present; callers may include it to document intent.
func WithDefaults() Option { return func(*Config) {} }

// Configure builds a Config from the supplied options, applies defaults for any
// unset fields, validates the result, and returns it. An error is returned if
// validation fails (e.g. empty ApplicationID or no brokers).
//
// Defaults are always applied unconditionally; WithDefaults() is an optional
// explicit-intent no-op that documents the caller's desire for defaults.
func Configure(opts ...Option) (Config, error) {
	var cfg Config
	for _, o := range opts {
		o(&cfg)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// ApplyDefaults fills in zero-value fields with sensible production defaults.
// Guarantee's zero value IS the default (AtLeastOnce == 0), so an explicitly
// set AtLeastOnce and an unset Guarantee are indistinguishable — both resolve
// to AtLeastOnce. EOS is always set explicitly (ExactlyOnce != 0).
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
	if c.RestoreCatchUpTimeout == 0 {
		c.RestoreCatchUpTimeout = 2 * time.Second
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
		errs = append(errs, errors.New("brokers must contain at least one address"))
	}
	if c.NumTaskThreads < 0 {
		errs = append(errs, fmt.Errorf("NumTaskThreads must be non-negative, got %d", c.NumTaskThreads))
	}
	if c.CommitInterval <= 0 {
		errs = append(errs, fmt.Errorf("CommitInterval must be positive, got %s", c.CommitInterval))
	}
	if c.RestoreCatchUpTimeout <= 0 {
		errs = append(errs, fmt.Errorf("RestoreCatchUpTimeout must be positive, got %s", c.RestoreCatchUpTimeout))
	}

	return errors.Join(errs...)
}
