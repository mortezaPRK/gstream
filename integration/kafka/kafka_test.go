//go:build integration

package kafka_test

import (
	"testing"

	kafkatest "github.com/mortezaPRK/gstream/integration/kafka"
)

func TestOptions(t *testing.T) {
	if kafkatest.WithClusterID("test-cluster") == nil {
		t.Fatal("WithClusterID() = nil")
	}
	if kafkatest.WithEnv(map[string]string{"KEY": "value"}) == nil {
		t.Fatal("WithEnv() = nil")
	}
}
