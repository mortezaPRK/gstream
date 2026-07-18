// Package deps exists solely to pin third-party dependencies in go.mod during
// scaffolding. Because go mod tidy prunes modules that have no real imports,
// this file holds blank imports of one representative package from each
// third-party module that wave-2 agents will use. Once real code in other
// packages imports these modules, this file should be deleted and go mod tidy
// re-run to clean up.
package deps

import (
	// franz-go: Kafka client wrapping franz-go internals (§3, §14)
	_ "github.com/twmb/franz-go/pkg/kgo"

	// pebble: local LSM state store (§5)
	_ "github.com/cockroachdb/pebble"

	// protobuf: ProtoSerde[T, PT] built-in serde (§10)
	_ "google.golang.org/protobuf/proto"

	// testcontainers: integration test broker provisioning (§16, §20)
	_ "github.com/testcontainers/testcontainers-go"

	// testcontainers kafka module: Redpanda/Kafka container for integration tests
	_ "github.com/testcontainers/testcontainers-go/modules/kafka"
)
