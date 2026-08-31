package runtime

import (
	"bytes"
	"testing"

	gstream "github.com/mortezaPRK/gstream"
)

func TestChangelogProducerEncode(t *testing.T) {
	producer := newTestChangelogProducer("test-changelog")
	t.Cleanup(producer.Close)

	records := producer.Encode(3, []gstream.StoreMutation{
		{Key: []byte("put"), Value: []byte("value")},
		{Key: []byte("delete"), Value: nil},
	})
	if len(records) != 2 {
		t.Fatalf("Encode() records = %d, want 2", len(records))
	}
	if records[0].Topic != "test-changelog" || !bytes.Equal(records[0].Key, []byte("put")) || !bytes.Equal(records[0].Value, []byte("value")) {
		t.Fatalf("Encode() put record = %#v", records[0])
	}
	if records[1].Value != nil {
		t.Fatalf("Encode() tombstone value = %v, want nil", records[1].Value)
	}
	for index, record := range records {
		if !record.Partition.IsValid || record.Partition.Value != 3 {
			t.Errorf("record %d partition = %+v, want pinned partition 3", index, record.Partition)
		}
	}
}

func TestChangelogProducerEncodeEmpty(t *testing.T) {
	producer := newTestChangelogProducer("test-changelog")
	t.Cleanup(producer.Close)
	if records := producer.Encode(0, nil); records != nil {
		t.Fatalf("Encode(nil) = %v, want nil", records)
	}
}
