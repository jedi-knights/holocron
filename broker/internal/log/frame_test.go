package log

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/jedi-knights/holocron/proto"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	r := proto.Record{
		Offset:    42,
		Timestamp: 1_700_000_000_000_000_000,
		Key:       []byte("user-42"),
		Value:     []byte(`{"action":"login"}`),
		Headers: []proto.Header{
			{Key: "holocron.trace-id", Value: []byte("abc")},
			{Key: "holocron.schema", Value: []byte("v1")},
		},
	}
	buf := encodeRecord(nil, r)

	got, n, err := readRecordFrom(bytes.NewReader(buf))
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Fatalf("consumed %d, want %d", n, len(buf))
	}
	if got.Offset != r.Offset || got.Timestamp != r.Timestamp {
		t.Fatalf("metadata mismatch: %+v", got)
	}
	if !bytes.Equal(got.Key, r.Key) || !bytes.Equal(got.Value, r.Value) {
		t.Fatalf("payload mismatch: %+v", got)
	}
	if len(got.Headers) != len(r.Headers) {
		t.Fatalf("headers: got %d want %d", len(got.Headers), len(r.Headers))
	}
	for i, h := range r.Headers {
		if got.Headers[i].Key != h.Key || !bytes.Equal(got.Headers[i].Value, h.Value) {
			t.Fatalf("header %d mismatch: %+v vs %+v", i, got.Headers[i], h)
		}
	}
}

func TestEncodeMultipleRecordsConcatenate(t *testing.T) {
	var buf []byte
	buf = encodeRecord(buf, proto.Record{Offset: 0, Value: []byte("a")})
	buf = encodeRecord(buf, proto.Record{Offset: 1, Value: []byte("bb")})
	buf = encodeRecord(buf, proto.Record{Offset: 2, Value: []byte("ccc")})

	rd := bytes.NewReader(buf)
	for i := range 3 {
		got, _, err := readRecordFrom(rd)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if got.Offset != int64(i) {
			t.Fatalf("record %d: offset %d", i, got.Offset)
		}
	}
	if _, _, err := readRecordFrom(rd); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestReadDetectsCRCError(t *testing.T) {
	buf := encodeRecord(nil, proto.Record{Offset: 1, Value: []byte("hello")})
	// Flip a byte in the body.
	buf[8] ^= 0xff

	_, _, err := readRecordFrom(bytes.NewReader(buf))
	if !errors.Is(err, errTornFrame) {
		t.Fatalf("expected errTornFrame, got %v", err)
	}
}

func TestReadPartialFrameIsUnexpectedEOF(t *testing.T) {
	buf := encodeRecord(nil, proto.Record{Offset: 1, Value: []byte("hello")})
	_, _, err := readRecordFrom(bytes.NewReader(buf[:len(buf)-3]))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected ErrUnexpectedEOF, got %v", err)
	}
}

func TestReadCleanEOF(t *testing.T) {
	_, _, err := readRecordFrom(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

// TestDiskAndWireFramesAreByteIdentical proves the disk frame format and
// wire frame format produce byte-equivalent output for the same record.
// This is the sendfile-readiness invariant: io.Copy from a segment file
// to a network connection works only if the bytes match.
func TestDiskAndWireFramesAreByteIdentical(t *testing.T) {
	// Arrange: the same record encoded by both layers.
	r := proto.Record{
		Offset:    42,
		Timestamp: 1_700_000_000_000_000_000,
		Key:       []byte("sendfile-test"),
		Value:     []byte(`{"answer":42}`),
		Headers: []proto.Header{
			{Key: "trace", Value: []byte("xyz")},
		},
	}

	// Act
	diskBytes := encodeRecord(nil, r)

	// The wire format's framed-record encoder is package-private to
	// proto; we exercise it by encoding through FetchResponse and
	// stripping the leading 1-byte codec + 4-byte count.
	wireResp := proto.FetchResponse{Records: []proto.Record{r}}
	wireFull := wireResp.Encode()
	if len(wireFull) < 5 {
		t.Fatal("wire encoding too short")
	}
	wireBytes := wireFull[5:] // strip codec + count prefix

	// Assert
	if !bytes.Equal(diskBytes, wireBytes) {
		t.Fatalf("disk and wire frame bytes diverge:\n  disk len=%d  wire len=%d", len(diskBytes), len(wireBytes))
	}
}

func TestNilAndEmptyKeysAreEquivalent(t *testing.T) {
	a := encodeRecord(nil, proto.Record{Offset: 1, Key: nil, Value: []byte("x")})
	b := encodeRecord(nil, proto.Record{Offset: 1, Key: []byte{}, Value: []byte("x")})
	if !bytes.Equal(a, b) {
		t.Fatal("nil and empty key encode differently")
	}
}
