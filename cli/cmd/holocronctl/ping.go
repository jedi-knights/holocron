package main

import (
	"context"
	"flag"
	"fmt"
	"time"
)

// runPing performs a quick liveness/auth probe against a broker:
// dial (which negotiates the handshake, including the API-key check
// when one is configured) and call ListTopics so the broker actually
// services a request rather than just accepting the connection. Used
// in scripts and health checks where "is the broker reachable and
// is my key valid?" is a one-line yes/no.
//
// With --json, emits `{"addr","ok","topics"}` so scripts can parse
// the result without grepping free-form text. Errors still surface
// via the return value (non-zero exit) so the caller can branch on
// success/failure even without --json.
//
// Pure CLI sugar — no new wire op; the existing ListTopics handler
// is the cheapest no-op the broker exposes.
func runPing(args []string) error {
	fs := flag.NewFlagSet("ping", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	jsonOut := fs.Bool("json", false, "emit machine-readable JSON")
	timeout := fs.Duration("timeout", 2*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return fmt.Errorf("dial %s: %w", *addr, err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	topics, err := tr.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("ping %s: %w", *addr, err)
	}
	if *jsonOut {
		return printJSON(struct {
			Addr   string `json:"addr"`
			OK     bool   `json:"ok"`
			Topics int    `json:"topics"`
		}{
			Addr:   *addr,
			OK:     true,
			Topics: len(topics),
		})
	}
	fmt.Printf("ok %s topics=%d\n", *addr, len(topics))
	return nil
}
