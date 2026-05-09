// Package http ships reference HTTP source and sink connectors. They
// are intentionally small: enough to demonstrate how to bridge holocron
// to and from the world's most common request-response surface, without
// reaching for the full feature surface of Kafka Connect HTTP plugins.
//
// Source: poll a URL on an interval; split the response body by
// newlines; emit each non-empty line as a record. No cursor in V1 — the
// operator is responsible for making the endpoint idempotent or for
// accepting the resulting at-most-once-per-poll semantics.
//
// Sink: POST each record's Value to a configured URL. ContentType is
// set per-config; non-2xx responses surface as Put errors so the
// Worker's retry/DLQ paths apply.
package http
