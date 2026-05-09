package proto

// Record is the atomic unit of data in holocron. It is immutable once written.
//
// See docs/data-model.md#record for the field-level rules. Briefly:
//   - Offset is broker-assigned. Producers must not set it.
//   - Timestamp is broker-assigned at append time when zero.
//   - Key, Value, and Header values are opaque bytes; the broker does not
//     interpret them.
type Record struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   []Header
}

// Header is application-level metadata travelling alongside a Record. Keys
// are UTF-8; values are bytes.
type Header struct {
	Key   string
	Value []byte
}

// Reserved header keys carry holocron-internal metadata that the
// broker reads alongside the user-facing record body. Producers
// that don't set these headers behave exactly as before — the
// idempotency machinery is opt-in and inert when absent.
const (
	// HeaderProducerID identifies the producer instance that
	// originated the record. Combined with HeaderProducerSeq it
	// lets the broker recognize and deduplicate retried writes
	// from the same producer instance.
	HeaderProducerID = "holocron.producer.id"
	// HeaderProducerSeq is the producer's monotonic per-partition
	// sequence number for this record. Big-endian uint64.
	HeaderProducerSeq = "holocron.producer.seq"
)
