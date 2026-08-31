package pebble

import "github.com/mortezaPRK/gstream/internal/kafka"

// testEncoder is a minimal shim that exposes ChangelogProducer.Encode for
// unit tests without requiring a live Kafka broker. The underlying producer
// is created with a fake broker address — kgo.NewClient does not dial at
// construction time, so this succeeds. Encode never touches the network.
type testEncoder struct {
	p *ChangelogProducer
}

// NewTestEncoder returns a testEncoder backed by a real ChangelogProducer
// pointed at a non-existent broker. Only Encode may be called safely;
// Flush will block waiting for a broker that never responds.
func NewTestEncoder(topic string) *testEncoder {
	p, err := NewChangelogProducer([]string{"localhost:19092"}, topic)
	if err != nil {
		panic("NewTestEncoder: " + err.Error())
	}
	return &testEncoder{p: p}
}

// Encode delegates to ChangelogProducer.Encode.
func (e *testEncoder) Encode(partition int32, muts []Mutation) []kafka.OutRecord {
	return e.p.Encode(partition, muts)
}
