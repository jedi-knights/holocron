// Package connect is holocron's Connect-style framework: a runtime that
// hosts long-running source and sink connectors so ETL/ELT pipelines can
// be expressed as configuration rather than as bespoke producer or
// consumer code.
//
// The model:
//
//   - A SourceConnector reads from an external system (database, file,
//     API, queue) and produces records to a holocron topic.
//   - A SinkConnector consumes from a holocron topic (via a consumer
//     group) and writes records to an external system (warehouse, search
//     index, file, queue).
//   - A Worker hosts one or more connectors. It owns the lifecycle:
//     start, run, stop, and offset persistence.
//
// connect imports only sdk + proto; it does not reach into broker
// internals. A Worker can be pointed at any sdk.Transport — inproc for
// tests, network for production.
package connect
