package proto

import (
	"bytes"
	"errors"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, OpProduce, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	op, body, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if op != OpProduce {
		t.Fatalf("opcode: got 0x%02x", byte(op))
	}
	if string(body) != "hello" {
		t.Fatalf("body: got %q", body)
	}
}

func TestProduceRequestRoundTrip(t *testing.T) {
	req := ProduceRequest{
		Topic:     "events",
		Partition: 3,
		Record: Record{
			Timestamp: 1234,
			Key:       []byte("k"),
			Value:     []byte("v"),
			Headers:   []Header{{Key: "h", Value: []byte("z")}},
		},
	}
	got, err := DecodeProduceRequest(req.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if got.Topic != req.Topic || got.Partition != req.Partition {
		t.Fatalf("metadata mismatch: %+v", got)
	}
	if !bytes.Equal(got.Record.Key, req.Record.Key) || !bytes.Equal(got.Record.Value, req.Record.Value) {
		t.Fatalf("payload mismatch: %+v", got.Record)
	}
	if len(got.Record.Headers) != 1 || got.Record.Headers[0].Key != "h" {
		t.Fatalf("headers mismatch: %+v", got.Record.Headers)
	}
}

func TestFetchResponseRoundTrip(t *testing.T) {
	resp := FetchResponse{
		Records: []Record{
			{Offset: 1, Timestamp: 100, Value: []byte("a")},
			{Offset: 2, Timestamp: 200, Value: []byte("bb")},
			{Offset: 3, Timestamp: 300, Value: []byte("ccc")},
		},
	}
	got, err := DecodeFetchResponse(resp.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Records) != 3 {
		t.Fatalf("got %d records", len(got.Records))
	}
	for i, r := range got.Records {
		if r.Offset != int64(i+1) {
			t.Errorf("record %d: offset %d", i, r.Offset)
		}
	}
}

func TestErrorResponseDecodes(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteErrorResponse(&buf, OpProduce, StatusUnknownTopic, "no such topic: foo"); err != nil {
		t.Fatal(err)
	}
	_, err := ReadResponse(&buf, OpProduce)
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("expected ProtocolError, got %T %v", err, err)
	}
	if pe.Status != StatusUnknownTopic {
		t.Fatalf("status: got 0x%02x", byte(pe.Status))
	}
	if pe.Message != "no such topic: foo" {
		t.Fatalf("message: %q", pe.Message)
	}
}

func TestOKResponseDecodes(t *testing.T) {
	var buf bytes.Buffer
	body := ProduceResponse{Offset: 42}.Encode()
	if err := WriteResponse(&buf, OpProduce, StatusOK, body); err != nil {
		t.Fatal(err)
	}
	got, err := ReadResponse(&buf, OpProduce)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := DecodeProduceResponse(got)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Offset != 42 {
		t.Fatalf("offset: got %d", resp.Offset)
	}
}

func TestReadResponseDetectsOpcodeMismatch(t *testing.T) {
	var buf bytes.Buffer
	_ = WriteResponse(&buf, OpFetch, StatusOK, nil)
	_, err := ReadResponse(&buf, OpProduce)
	if err == nil {
		t.Fatal("expected error on opcode mismatch")
	}
}

func TestFetchResponse_LZ4RoundTrip(t *testing.T) {
	// Arrange — large compressible payload.
	repeat := bytes.Repeat([]byte("event-bus-v5-fetch-payload "), 200)
	records := make([]Record, 5)
	for i := range records {
		records[i] = Record{
			Offset: int64(i),
			Key:    []byte{byte(i)},
			Value:  append([]byte(nil), repeat...),
		}
	}

	// Act — encode with LZ4 and verify it shrinks vs codec=None.
	resp := FetchResponse{Records: records, Codec: CodecLZ4}
	encoded := resp.Encode()
	plainSize := len(FetchResponse{Records: records, Codec: CodecNone}.Encode())
	if len(encoded) >= plainSize {
		t.Fatalf("LZ4 didn't shrink: compressed=%d plain=%d", len(encoded), plainSize)
	}

	// Assert — decode round-trips every record.
	got, err := DecodeFetchResponse(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Codec != CodecLZ4 {
		t.Errorf("decoded codec: got %d want CodecLZ4", got.Codec)
	}
	if len(got.Records) != len(records) {
		t.Fatalf("decoded count: got %d want %d", len(got.Records), len(records))
	}
	for i, r := range got.Records {
		if !bytes.Equal(r.Value, records[i].Value) {
			t.Errorf("record %d value diverged after LZ4 round-trip", i)
		}
	}
}

func TestFetchResponse_NoneCodecBackwardCompat(t *testing.T) {
	// Arrange — codec=None is what the v5 server picks for small
	// payloads or v5 clients that opt out.
	records := []Record{{Offset: 1, Key: []byte("k"), Value: []byte("v")}}

	// Act
	encoded := FetchResponse{Records: records, Codec: CodecNone}.Encode()
	got, err := DecodeFetchResponse(encoded)

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	if got.Codec != CodecNone {
		t.Errorf("codec: got %d want CodecNone", got.Codec)
	}
	if len(got.Records) != 1 || !bytes.Equal(got.Records[0].Value, []byte("v")) {
		t.Fatalf("records: %+v", got.Records)
	}
}

func TestFetchRequest_AcceptCodecRoundTrip(t *testing.T) {
	req := FetchRequest{
		Topic:       "events",
		Partition:   3,
		FromOffset:  42,
		MaxRecords:  64,
		MaxWaitMs:   100,
		AcceptCodec: CodecLZ4,
	}
	got, err := DecodeFetchRequest(req.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if got.AcceptCodec != CodecLZ4 {
		t.Errorf("AcceptCodec: got %d want CodecLZ4", got.AcceptCodec)
	}
	if got.FromOffset != 42 || got.MaxRecords != 64 {
		t.Errorf("scalar fields diverged: %+v", got)
	}
}

func TestProduceBatchRequest_LZ4RoundTrip(t *testing.T) {
	// Arrange: a batch of 5 highly-compressible records.
	repeat := bytes.Repeat([]byte("hello holocron "), 200)
	records := make([]Record, 5)
	for i := range records {
		records[i] = Record{
			Key:   []byte{byte(i)},
			Value: append([]byte(nil), repeat...),
		}
	}
	req := ProduceBatchRequest{
		Topic:     "events",
		Partition: 3,
		Codec:     CodecLZ4,
		Records:   records,
	}

	// Act
	encoded := req.Encode()

	// Assert: compressed size is materially smaller than the
	// uncompressed body would be.
	uncompressedReq := req
	uncompressedReq.Codec = CodecNone
	uncompressedSize := len(uncompressedReq.Encode())
	if len(encoded) >= uncompressedSize {
		t.Fatalf("LZ4 didn't shrink: compressed=%d uncompressed=%d", len(encoded), uncompressedSize)
	}

	got, err := DecodeProduceBatchRequest(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if got.Codec != CodecLZ4 {
		t.Errorf("codec round-trip: got 0x%02x want 0x%02x", byte(got.Codec), byte(CodecLZ4))
	}
	if len(got.Records) != len(records) {
		t.Fatalf("decoded %d records, want %d", len(got.Records), len(records))
	}
	for i, r := range got.Records {
		if !bytes.Equal(r.Value, records[i].Value) {
			t.Errorf("record %d: value mismatch after compress/decompress", i)
		}
	}
}

func TestProduceBatchRequest_NoCodecRoundTrip(t *testing.T) {
	req := ProduceBatchRequest{
		Topic:     "t",
		Partition: 0,
		Codec:     CodecNone,
		Records: []Record{
			{Key: []byte("k"), Value: []byte("v")},
		},
	}
	got, err := DecodeProduceBatchRequest(req.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if got.Codec != CodecNone {
		t.Errorf("codec: got 0x%02x", byte(got.Codec))
	}
	if len(got.Records) != 1 || !bytes.Equal(got.Records[0].Value, []byte("v")) {
		t.Fatalf("record mismatch: %+v", got.Records)
	}
}

func TestFetchResponse_FramedRecordRoundTrip(t *testing.T) {
	// Arrange
	resp := FetchResponse{
		Records: []Record{
			{Offset: 1, Timestamp: 100, Key: []byte("k1"), Value: []byte("v1")},
			{Offset: 2, Timestamp: 200, Key: []byte("k2"), Value: []byte("v2"),
				Headers: []Header{{Key: "trace", Value: []byte("abc")}}},
		},
	}

	// Act
	encoded := resp.Encode()
	got, err := DecodeFetchResponse(encoded)

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("got %d records, want 2", len(got.Records))
	}
	for i, r := range got.Records {
		if r.Offset != resp.Records[i].Offset || !bytes.Equal(r.Value, resp.Records[i].Value) {
			t.Errorf("record %d round-trip mismatch: %+v vs %+v", i, r, resp.Records[i])
		}
	}
}

func TestFetchResponse_FramedRecordDetectsCRCError(t *testing.T) {
	// Arrange
	resp := FetchResponse{
		Records: []Record{{Offset: 1, Value: []byte("hello")}},
	}
	encoded := resp.Encode()

	// Act: corrupt a byte in the body. Layout is [count u32][bodyLen u32][body][crc u32].
	encoded[10] ^= 0xff

	// Assert
	_, err := DecodeFetchResponse(encoded)
	if err == nil {
		t.Fatal("expected CRC error on corrupted FetchResponse, got nil")
	}
}

func TestIsStatus(t *testing.T) {
	pe := &ProtocolError{Status: StatusUnknownTopic, Message: "x"}
	if !IsStatus(pe, StatusUnknownTopic) {
		t.Fatal("IsStatus returned false")
	}
	if IsStatus(pe, StatusOK) {
		t.Fatal("IsStatus matched wrong status")
	}
}

func TestWireVersion_IsTen(t *testing.T) {
	// Wire v10 is the first version carrying CredentialKind on the
	// handshake. A regression that bumped it back would cause every
	// PR 5+ client to silently lose credentials.
	if WireVersion != 10 {
		t.Errorf("WireVersion: got %d, want 10", WireVersion)
	}
}

func TestCredentialKind_Constants(t *testing.T) {
	// The numeric values are wire-stable: PR 1 (broker/internal/auth)
	// chose the same encoding so the server-side conversion is a
	// no-op cast. Changing these breaks the auth package's contract.
	cases := []struct {
		name string
		got  CredentialKind
		want uint8
	}{
		{"None", CredentialNone, 0},
		{"APIKey", CredentialAPIKey, 1},
		{"JWT", CredentialJWT, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if uint8(c.got) != c.want {
				t.Errorf("got %d, want %d", c.got, c.want)
			}
		})
	}
}

func TestHandshakeRequest_RoundTripJWT(t *testing.T) {
	// Arrange
	req := HandshakeRequest{
		Version:        WireVersion,
		Credential:     []byte("eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJhbGljZSJ9.signature"),
		CredentialKind: CredentialJWT,
	}

	// Act
	got, err := DecodeHandshakeRequest(req.Encode())
	if err != nil {
		t.Fatalf("DecodeHandshakeRequest: %v", err)
	}

	// Assert
	if got.Version != req.Version {
		t.Errorf("Version: got %d, want %d", got.Version, req.Version)
	}
	if got.CredentialKind != CredentialJWT {
		t.Errorf("CredentialKind: got %d, want CredentialJWT", got.CredentialKind)
	}
	if !bytes.Equal(got.Credential, req.Credential) {
		t.Errorf("Credential: got %q, want %q", got.Credential, req.Credential)
	}
}

func TestHandshakeRequest_RoundTripAPIKey(t *testing.T) {
	// Arrange
	req := HandshakeRequest{
		Version:        WireVersion,
		Credential:     []byte("legacy-api-key-value"),
		CredentialKind: CredentialAPIKey,
	}

	// Act
	got, err := DecodeHandshakeRequest(req.Encode())
	if err != nil {
		t.Fatalf("DecodeHandshakeRequest: %v", err)
	}

	// Assert
	if got.CredentialKind != CredentialAPIKey {
		t.Errorf("CredentialKind: got %d, want CredentialAPIKey", got.CredentialKind)
	}
	if string(got.Credential) != "legacy-api-key-value" {
		t.Errorf("Credential: got %q", got.Credential)
	}
}

func TestHandshakeRequest_RoundTripNone(t *testing.T) {
	// Arrange: anonymous handshake — no credential.
	req := HandshakeRequest{Version: WireVersion}

	// Act
	got, err := DecodeHandshakeRequest(req.Encode())
	if err != nil {
		t.Fatalf("DecodeHandshakeRequest: %v", err)
	}

	// Assert
	if got.CredentialKind != CredentialNone {
		t.Errorf("CredentialKind: got %d, want CredentialNone", got.CredentialKind)
	}
	if len(got.Credential) != 0 {
		t.Errorf("Credential: got %q, want empty", got.Credential)
	}
}

func TestDecodeHandshakeRequest_RejectsTruncated(t *testing.T) {
	// Arrange / Act / Assert: empty buffer cannot carry even the
	// version byte.
	if _, err := DecodeHandshakeRequest(nil); err == nil {
		t.Fatal("expected error for empty handshake buffer")
	}
}
