// Package registry is holocron's optional schema registry: a service
// where producers register the schemas they intend to write, consumers
// fetch them by ID to deserialize records, and the registry assigns each
// (subject, version) tuple a globally unique integer ID so on-the-wire
// records carry only the ID rather than the full schema.
//
// The model mirrors Confluent Schema Registry. Two layers ship:
//
//   - Service is the embeddable kernel — a Go API plus state recovery
//     from a holocron topic. Embed it in your own program if you don't
//     want a separate process.
//   - Handler wraps Service in an HTTP API matching the Confluent shape.
//     cmd/holocron-registry is a thin binary around Handler.
//
// State lives on the broker in a topic named __holocron_schemas. Each
// registration appends a record; on startup the Service replays the
// topic from offset 0 to rebuild its in-memory tables. This is the same
// trick Kafka's controller and Connect's offset store use — the broker
// is its own metadata store.
package registry
