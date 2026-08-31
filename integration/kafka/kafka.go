//go:build integration

// Package kafka isolates Testcontainers-backed Kafka fixtures from production
// gstream packages.
package kafka

import (
	"context"

	testcontainers "github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// Option customizes Kafka test container creation.
type Option = testcontainers.ContainerCustomizer

// Container is Testcontainers Kafka fixture.
type Container = kafkamodule.KafkaContainer

// WithClusterID sets Kafka KRaft cluster ID.
func WithClusterID(clusterID string) Option {
	return kafkamodule.WithClusterID(clusterID)
}

// WithEnv adds container environment variables.
func WithEnv(environment map[string]string) Option {
	return testcontainers.WithEnv(environment)
}

// Run starts Kafka through Testcontainers.
func Run(ctx context.Context, image string, options ...Option) (*Container, error) {
	return kafkamodule.Run(ctx, image, options...)
}
