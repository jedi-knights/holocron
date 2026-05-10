package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jedi-knights/holocron/cli/internal/clienttls"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// runTopicLoad reads a JSONL file produced by `topic dump` and
// re-publishes each record to the target topic. Inverse of dump
// for snapshot+restore workflows: export from one cluster, import
// into another, or roll back a topic to a known-good snapshot.
//
// Doesn't preserve broker offsets — the destination broker
// assigns new ones. Source-side keys, values, and headers carry
// over (UTF-8 fields decode as text; *_b64 fields decode from
// base64 so binary records round-trip losslessly).
func runTopicLoad(args []string) error {
	fs := flag.NewFlagSet("topic load", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	topic := fs.String("topic", "", "destination topic (required, must already exist)")
	file := fs.String("file", "", "input JSONL file path (required; format produced by topic dump)")
	batch := fs.Bool("batch", false, "ship every record as one SendBatch instead of N Sends (faster for bulk imports)")
	timeout := fs.Duration("timeout", 30*time.Second, "RPC timeout (covers the full load)")
	tlsCfg := clienttls.RegisterFlags(fs)
	credFile := fs.String("credential-file", os.Getenv("HOLOCRON_CREDENTIAL_FILE"), "path to a JWT file (mutually exclusive with --api-key)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" || *file == "" {
		return errors.New("topic load: --topic and --file are required")
	}

	cfg, err := tlsCfg()
	if err != nil {
		return err
	}
	opts, err := credentialOpts(*credFile, *apiKey, dialOpts(cfg)...)
	if err != nil {
		return err
	}
	tr, err := dial(*addr, opts...)
	if err != nil {
		return err
	}
	defer tr.Close()

	in, err := os.Open(*file)
	if err != nil {
		return fmt.Errorf("open %s: %w", *file, err)
	}
	defer in.Close()

	prod, err := sdk.NewProducer(tr)
	if err != nil {
		return err
	}
	defer prod.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// Read JSONL line-by-line. bufio with a generous buffer so
	// records with megabyte-class values don't trip the default
	// 64 KiB scanner cap.
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var batched []proto.Record
	loaded := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var d dumpRecord
		if err := json.Unmarshal(line, &d); err != nil {
			return fmt.Errorf("parse line %d: %w", loaded+1, err)
		}
		rec, err := fromDumpRecord(d)
		if err != nil {
			return fmt.Errorf("decode line %d: %w", loaded+1, err)
		}
		if *batch {
			batched = append(batched, rec)
			loaded++
			continue
		}
		if _, err := prod.Send(ctx, *topic, rec); err != nil {
			return fmt.Errorf("send line %d: %w", loaded+1, err)
		}
		loaded++
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read %s: %w", *file, err)
	}
	if *batch && len(batched) > 0 {
		if _, err := prod.SendBatch(ctx, *topic, batched); err != nil {
			return fmt.Errorf("send batch: %w", err)
		}
	}
	fmt.Printf("loaded %d record(s) into %s from %s\n", loaded, *topic, *file)
	return nil
}

// fromDumpRecord reverses toDumpRecord — picks the UTF-8 string
// field if present, falls back to the matching base64 field. The
// dump format is symmetric: exactly one of `key` / `key_b64` (and
// `value` / `value_b64`) is set per record.
func fromDumpRecord(d dumpRecord) (proto.Record, error) {
	rec := proto.Record{Timestamp: d.Timestamp}
	switch {
	case d.Key != "":
		rec.Key = []byte(d.Key)
	case d.KeyB64 != "":
		raw, err := base64.StdEncoding.DecodeString(d.KeyB64)
		if err != nil {
			return rec, fmt.Errorf("decode key_b64: %w", err)
		}
		rec.Key = raw
	}
	switch {
	case d.Value != "":
		rec.Value = []byte(d.Value)
	case d.ValueB64 != "":
		raw, err := base64.StdEncoding.DecodeString(d.ValueB64)
		if err != nil {
			return rec, fmt.Errorf("decode value_b64: %w", err)
		}
		rec.Value = raw
	}
	for _, h := range d.Headers {
		ph := proto.Header{Key: h.Key}
		switch {
		case h.Value != "":
			ph.Value = []byte(h.Value)
		case h.ValueB64 != "":
			raw, err := base64.StdEncoding.DecodeString(h.ValueB64)
			if err != nil {
				return rec, fmt.Errorf("decode header %q value_b64: %w", h.Key, err)
			}
			ph.Value = raw
		}
		rec.Headers = append(rec.Headers, ph)
	}
	return rec, nil
}
