// Package kafka wraps the franz-go Kafka client (github.com/twmb/franz-go) and
// exposes an internal interface used by the gstream runtime. All franz-go and
// Kafka-protocol details are confined here so that the rest of gstream never
// imports kgo.* types directly (§3, §13, §14).
//
// # Processing guarantee
//
// This package currently implements the ALO (at-least-once) path (§4.1):
//   - A standard consumer group with manual offset commits.
//   - Offsets are committed only after outputs are acknowledged (process → produce → commit).
//   - On crash, records after the last commit may be reprocessed; duplicate outputs
//     are possible and expected.
//
// EOS (exactly-once via Kafka transactions, §4.2) is a later phase (P5) and is not
// implemented here. A TODO comment in client.go marks the wiring point.
//
// # Consumer group / assignor
//
// The cooperative-sticky balancer (§14) is the v1 default. It is configured internally
// via kgo.CooperativeStickyBalancer() and is never exposed to callers.
//
// # franz-go encapsulation
//
// All kgo.* types are private to this package. The public interface of this package
// uses only standard library types plus the root gstream.Config. Callers never need
// to import github.com/twmb/franz-go/pkg/kgo.
package kafka
