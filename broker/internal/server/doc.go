// Package server is the broker's transport layer: TCP framing, request
// dispatch, and connection lifecycle. The in-process broker.Broker has no
// dependency on this package — server wraps Broker rather than the other
// way round, so the broker remains usable without a network listener.
//
// Implementation lands with stage 3.
package server
