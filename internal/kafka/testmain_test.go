//go:build integration

package kafka

import (
	"os"
	"testing"
)

// TestMain sets testcontainers configuration that must be in place before any
// container is started.
//
// Why each knob:
//   - TESTCONTAINERS_RYUK_DISABLED=true — the resource-reaper (Ryuk) tries to
//     attach to the Docker "bridge" network. Under Podman (which exposes a Docker-
//     compatible socket but uses its own networking model) the network named
//     "bridge" may or may not exist, and Ryuk is a convenience — not required for
//     correctness. Disabling it removes the only container that fails on Podman by
//     default while leaving test containers (Kafka) unaffected.
func TestMain(m *testing.M) {
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	os.Exit(m.Run())
}
