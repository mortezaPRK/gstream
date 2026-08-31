//go:build integration

package kafka

import (
	"os"
	"testing"
)

// TestMain is the integration-test entry point for this package.
// Container lifecycle is owned by integration/kafka — see "make integration-test".
func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
