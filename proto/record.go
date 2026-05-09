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
