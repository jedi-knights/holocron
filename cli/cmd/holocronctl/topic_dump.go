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
	"unicode/utf8"

	"github.com/jedi-knights/holocron/cli/internal/clienttls"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// dumpRecord is the JSON shape one line of `topic dump` writes.
// Bytes that aren't valid UTF-8 are base64-encoded into the
// matching *_b64 field instead, so the dump round-trips
// losslessly without breaking jq pipelines on text payloads.
type dumpRecord struct {
	Offset    int64        `json:"offset"`
	Timestamp int64        `json:"timestamp"`
	Key       string       `json:"key,omitempty"`
	KeyB64    string       `json:"key_b64,omitempty"`
	Value     string       `json:"value,omitempty"`
	ValueB64  string       `json:"value_b64,omitempty"`
	Headers   []dumpHeader `json:"headers,omitempty"`
}

type dumpHeader struct {
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	ValueB64 string `json:"value_b64,omitempty"`
}

// runTopicDump writes every record from a source partition (up to
// the high-water snapshot at start) as JSON-lines — one record per
// line. With --file, the output goes to that path; without --file,
// JSONL streams to stdout so `holocronctl topic dump --topic X | jq`
// works without a temp file. Status messages always go to stderr so
// they don't pollute the JSONL stream.
//
// Reads bounded by the partition's HighWater at start of dump:
// records produced after the snapshot is taken are not included.
// For a continuously-streaming dump, pair with `tail` instead.
func runTopicDump(args []string) error {
	fs := flag.NewFlagSet("topic dump", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	topic := fs.String("topic", "", "source topic (required)")
	partition := fs.Int("partition", 0, "partition index")
	file := fs.String("file", "", "output file path (omit for stdout)")
	timeout := fs.Duration("timeout", 30*time.Second, "RPC timeout (covers the full dump)")
	tlsCfg := clienttls.RegisterFlags(fs)
	credFile := fs.String("credential-file", os.Getenv("HOLOCRON_CREDENTIAL_FILE"), "path to a JWT file (mutually exclusive with --api-key)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" {
		return errors.New("topic dump: --topic is required")
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

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	pref := proto.PartitionRef{Topic: *topic, Index: int32(*partition)}
	hw, err := tr.HighWater(ctx, pref)
	if err != nil {
		return fmt.Errorf("high-water: %w", err)
	}

	// Route JSONL to file or stdout. Status messages always
	// go to stderr so they don't corrupt the JSONL stream when
	// it's piped through jq or similar.
	var sink io.Writer = os.Stdout
	dest := "stdout"
	if *file != "" {
		f, err := os.Create(*file)
		if err != nil {
			return fmt.Errorf("create %s: %w", *file, err)
		}
		defer f.Close()
		sink = f
		dest = *file
	}
	w := bufio.NewWriter(sink)
	defer w.Flush()
	enc := json.NewEncoder(w)

	if hw == 0 {
		fmt.Fprintf(os.Stderr, "(source %s/%d is empty; wrote 0 records to %s)\n", *topic, *partition, dest)
		return nil
	}

	cons, err := sdk.NewConsumer(tr)
	if err != nil {
		return err
	}
	defer cons.Close()
	if err := cons.Assign(ctx, pref, 0); err != nil {
		return fmt.Errorf("assign: %w", err)
	}

	written := int64(0)
	for written < hw {
		batch := 256
		if remaining := hw - written; int64(batch) > remaining {
			batch = int(remaining)
		}
		records, err := cons.Poll(ctx, batch)
		if err != nil {
			return fmt.Errorf("poll: %w", err)
		}
		if len(records) == 0 {
			break
		}
		for _, r := range records {
			if err := enc.Encode(toDumpRecord(r)); err != nil {
				return fmt.Errorf("encode offset %d: %w", r.Offset, err)
			}
			written++
		}
	}
	fmt.Fprintf(os.Stderr, "dumped %d record(s) from %s/%d -> %s\n", written, *topic, *partition, dest)
	return nil
}

// toDumpRecord chooses a UTF-8 string field or a base64 _b64
// field for every byte slice so the dump round-trips losslessly
// — text records read like JSON, binary records survive jq.
func toDumpRecord(r proto.Record) dumpRecord {
	d := dumpRecord{Offset: r.Offset, Timestamp: r.Timestamp}
	if utf8.Valid(r.Key) {
		d.Key = string(r.Key)
	} else {
		d.KeyB64 = base64.StdEncoding.EncodeToString(r.Key)
	}
	if utf8.Valid(r.Value) {
		d.Value = string(r.Value)
	} else {
		d.ValueB64 = base64.StdEncoding.EncodeToString(r.Value)
	}
	for _, h := range r.Headers {
		dh := dumpHeader{Key: h.Key}
		if utf8.Valid(h.Value) {
			dh.Value = string(h.Value)
		} else {
			dh.ValueB64 = base64.StdEncoding.EncodeToString(h.Value)
		}
		d.Headers = append(d.Headers, dh)
	}
	return d
}
