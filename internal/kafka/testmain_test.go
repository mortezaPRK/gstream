//go:build integration

package kafka

import (
	"os"
	"testing"
)

// TestMain is the integration-test entry point for this package.
// TESTCONTAINERS_RYUK_DISABLED is set by the Makefile integration-test target,
// not here — see "make integration-test".
func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
