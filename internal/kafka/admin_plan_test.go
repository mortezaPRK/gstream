package kafka

import (
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
)

func TestInternalTopicSpecs(t *testing.T) {
	cfg := gstream.Config{ApplicationID: "orders"}
	topology := &gstream.BuiltTopology{
		StoreBindings: map[string]gstream.StoreBinding{
			"count": {ChangelogTopic: "count"},
		},
		WindowStoreBindings: map[string]gstream.WindowStoreBinding{
			"hourly": {
				StoreBinding: gstream.StoreBinding{ChangelogTopic: "hourly"},
				WindowDef:    gstream.TumblingWindows(time.Hour),
				GraceMs:      int64((5 * time.Minute).Milliseconds()),
			},
		},
		SessionStoreBindings: map[string]gstream.SessionStoreBinding{
			"sessions": {
				StoreBinding: gstream.StoreBinding{ChangelogTopic: "sessions"},
				GapMs:        60_000, GraceMs: 30_000,
			},
		},
		RepartitionBindings: map[string]gstream.RepartitionBinding{
			"auto":  {Name: "auto", Partitions: 0},
			"fixed": {Name: "fixed", Partitions: 3},
		},
	}

	specs := internalTopicSpecs(cfg, topology, 3)
	got := make(map[string]TopicSpec, len(specs))
	for _, spec := range specs {
		got[spec.Name] = spec
	}
	if got["orders-auto-repartition"].Partitions != 3 {
		t.Errorf("automatic repartition partitions = %d, want 3", got["orders-auto-repartition"].Partitions)
	}
	if got["orders-fixed-repartition"].Partitions != 3 {
		t.Errorf("explicit repartition partitions = %d, want 3", got["orders-fixed-repartition"].Partitions)
	}
	if got["orders-count-changelog"].Configs["cleanup.policy"] != "compact" {
		t.Errorf("count cleanup policy = %q, want compact", got["orders-count-changelog"].Configs["cleanup.policy"])
	}
	if got["orders-hourly-changelog"].Configs["retention.ms"] != "3900000" {
		t.Errorf("window retention = %q, want 3900000", got["orders-hourly-changelog"].Configs["retention.ms"])
	}
	if got["orders-sessions-changelog"].Configs["retention.ms"] != "90000" {
		t.Errorf("session retention = %q, want 90000", got["orders-sessions-changelog"].Configs["retention.ms"])
	}
}

func TestSourcePartitionCountRejectsMismatch(t *testing.T) {
	topology := &gstream.BuiltTopology{Sources: map[string]gstream.SourceBinding{
		"left": {Topic: "left"}, "right": {Topic: "right"},
	}}
	_, err := sourcePartitionCount(topology, map[string]topicMeta{
		"left": {partitions: 2}, "right": {partitions: 3},
	})
	if err == nil {
		t.Fatal("sourcePartitionCount() error = nil, want mismatch")
	}
}

func TestSourcePartitionCountRejectsMissingMetadata(t *testing.T) {
	topology := &gstream.BuiltTopology{Sources: map[string]gstream.SourceBinding{
		"orders": {Topic: "orders"},
	}}

	_, err := sourcePartitionCount(topology, map[string]topicMeta{})
	if err == nil {
		t.Fatal("sourcePartitionCount() error = nil, want missing metadata error")
	}
}

func TestSourcePartitionCountRejectsInvalidPartitionCount(t *testing.T) {
	topology := &gstream.BuiltTopology{Sources: map[string]gstream.SourceBinding{
		"orders": {Topic: "orders"},
	}}

	_, err := sourcePartitionCount(topology, map[string]topicMeta{
		"orders": {partitions: 0},
	})
	if err == nil {
		t.Fatal("sourcePartitionCount() error = nil, want invalid partition count error")
	}
}
