package sdk

// Standard header keys. Connectors and SDKs may rely on these conventions;
// the broker treats all headers as opaque metadata.
const (
	// HeaderSchema names the schema (or schema ID) for the record value.
	// Format and registry are application-defined.
	HeaderSchema = "holocron.schema"

	// HeaderIdempotencyKey is a producer-assigned key for downstream
	// at-least-once dedup.
	HeaderIdempotencyKey = "holocron.idempotency-key"

	// HeaderTraceID carries a distributed-trace identifier through the
	// broker so end-to-end spans stay connected.
	HeaderTraceID = "holocron.trace-id"
)
