//go:build integration

package kafka_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kafkatest "mortz.dev/go/gstream/integration/kafka"
)

func TestRootWhiteBoxSuites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	container, err := kafkatest.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkatest.WithClusterID("gstream-root-integration"),
		kafkatest.WithEnv(map[string]string{
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "true",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
		}),
	)
	if err != nil {
		t.Skipf("start Kafka fixture: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka fixture brokers: %v", err)
	}

	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	command := exec.CommandContext(
		ctx,
		"go", "test", "-p", "1", "-tags", "integration",
		"./internal/kafka",
	)
	command.Dir = repositoryRoot
	command.Env = append(
		os.Environ(),
		"GSTREAM_TEST_KAFKA_BROKERS="+strings.Join(brokers, ","),
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("root integration suites: %v\n%s", err, output)
	}
	t.Logf("root integration suites:\n%s", output)
}
