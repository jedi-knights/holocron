package cluster

import (
	"bytes"
	"testing"

	"github.com/jedi-knights/holocron/proto"
)

func TestEncodeDecodeAppendRoundTrip(t *testing.T) {
	cmd := AppendCommand{
		Topic:     "events",
		Partition: 3,
		Record: proto.Record{
			Timestamp: 1234,
			Key:       []byte("k"),
			Value:     []byte("payload"),
			Headers:   []proto.Header{{Key: "h", Value: []byte("v")}},
		},
	}
	enc := EncodeAppend(cmd)
	kind, body, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	if kind != CmdAppend {
		t.Fatalf("kind: got 0x%02x, want CmdAppend", byte(kind))
	}
	got, err := DecodeAppend(body)
	if err != nil {
		t.Fatal(err)
	}
	if got.Topic != cmd.Topic || got.Partition != cmd.Partition {
		t.Fatalf("metadata mismatch: %+v", got)
	}
	if !bytes.Equal(got.Record.Key, cmd.Record.Key) || !bytes.Equal(got.Record.Value, cmd.Record.Value) {
		t.Fatalf("record mismatch: %+v", got.Record)
	}
}

func TestEncodeDecodeCreateTopicRoundTrip(t *testing.T) {
	cmd := CreateTopicCommand{
		Name:           "orders",
		PartitionCount: 8,
		RetentionMs:    3600000,
		SegmentBytes:   1 << 28,
	}
	enc := EncodeCreateTopic(cmd)
	kind, body, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	if kind != CmdCreateTopic {
		t.Fatalf("kind: got 0x%02x, want CmdCreateTopic", byte(kind))
	}
	got, err := DecodeCreateTopic(body)
	if err != nil {
		t.Fatal(err)
	}
	if got != cmd {
		t.Fatalf("got %+v, want %+v", got, cmd)
	}
}

func TestDecodeRejectsEmpty(t *testing.T) {
	if _, _, err := Decode(nil); err == nil {
		t.Fatal("expected error on empty input")
	}
}
