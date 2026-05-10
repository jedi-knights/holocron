package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jedi-knights/holocron/cli/internal/clienttls"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// headerFlag is a flag.Value that accumulates repeated --header
// invocations into a slice of proto.Header. Each value is parsed as
// `key=value`; the first '=' separates the two parts so values
// containing '=' work correctly.
type headerFlag []proto.Header

func (h *headerFlag) String() string {
	if h == nil || len(*h) == 0 {
		return ""
	}
	parts := make([]string, 0, len(*h))
	for _, hdr := range *h {
		parts = append(parts, fmt.Sprintf("%s=%s", hdr.Key, hdr.Value))
	}
	return strings.Join(parts, ",")
}

func (h *headerFlag) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok {
		return fmt.Errorf("header %q: missing '=' separator", s)
	}
	*h = append(*h, proto.Header{Key: k, Value: []byte(v)})
	return nil
}

// runProduce sends records to a topic. Two modes:
//
//   - Single-record mode: --value (and optionally --key) supplies
//     one record's contents. Same shape as previous batches.
//   - Stdin mode: omit --value and the command reads stdin one
//     line per record. With --key-sep set, each line is split on
//     the first occurrence of the separator into (key, value);
//     otherwise the whole line is the value.
//
// Stdin mode lets operators pipe data through without writing a
// one-shot Go program.
func runProduce(args []string) error {
	fs := flag.NewFlagSet("produce", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	topic := fs.String("topic", "", "destination topic (required)")
	key := fs.String("key", "", "record key (single-record mode only)")
	value := fs.String("value", "", "record value (omit to read records from stdin)")
	keySep := fs.String("key-sep", "", "stdin mode: split each line on first occurrence into key+value")
	batch := fs.Bool("batch", false, "stdin mode: ship all stdin records as one SendBatch instead of N Sends")
	idempotent := fs.Bool("idempotent", false, "stamp producer-id/sequence headers so the broker dedups retried writes")
	timeout := fs.Duration("timeout", 30*time.Second, "RPC timeout (covers all records in stdin mode)")
	var headers headerFlag
	fs.Var(&headers, "header", "header in key=value form (repeatable; applies to every record)")
	tlsCfg := clienttls.RegisterFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" {
		return errors.New("produce: --topic is required")
	}

	cfg, err := tlsCfg()
	if err != nil {
		return err
	}
	tr, err := dial(*addr, *apiKey, dialOpts(cfg)...)
	if err != nil {
		return err
	}
	defer tr.Close()

	var prodOpts []sdk.ProducerOption
	if *idempotent {
		prodOpts = append(prodOpts, sdk.WithIdempotency())
	}
	prod, err := sdk.NewProducer(tr, prodOpts...)
	if err != nil {
		return err
	}
	defer prod.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// Single-record mode: --value supplied directly.
	if *value != "" {
		rec := proto.Record{Value: []byte(*value)}
		if *key != "" {
			rec.Key = []byte(*key)
		}
		if len(headers) > 0 {
			rec.Headers = append([]proto.Header(nil), headers...)
		}
		off, err := prod.Send(ctx, *topic, rec)
		if err != nil {
			return fmt.Errorf("send: %w", err)
		}
		fmt.Printf("topic=%s offset=%d\n", *topic, off)
		return nil
	}

	// Stdin mode: one record per line. Headers, when supplied,
	// apply to every record.
	if *batch {
		count, err := produceBatchFromReader(ctx, prod, *topic, os.Stdin, *keySep, headers)
		if err != nil {
			return err
		}
		fmt.Printf("produced %d record(s) to %s in one batch\n", count, *topic)
		return nil
	}
	count, err := produceFromReader(ctx, prod, *topic, os.Stdin, *keySep, headers)
	if err != nil {
		return err
	}
	fmt.Printf("produced %d record(s) to %s\n", count, *topic)
	return nil
}

// produceBatchFromReader reads every line from r into a single
// SendBatch call. Faster than line-by-line Send for bulk imports
// because the broker sees one ProduceBatch RPC instead of N
// Publishes — plus the producer compresses the whole batch when
// WithCompression is set.
//
// All records go to the partition the producer picks for the
// FIRST record (broker's per-partition append guarantees ordering
// only within a partition). For best results pre-key your records
// or accept that the bulk lands together.
func produceBatchFromReader(ctx context.Context, prod *sdk.Producer, topic string, r io.Reader, keySep string, headers []proto.Header) (int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var records []proto.Record
	for scanner.Scan() {
		line := scanner.Text()
		var rec proto.Record
		if keySep == "" {
			rec.Value = []byte(line)
		} else {
			k, v, ok := strings.Cut(line, keySep)
			if ok {
				rec.Key = []byte(k)
				rec.Value = []byte(v)
			} else {
				rec.Value = []byte(line)
			}
		}
		if len(headers) > 0 {
			rec.Headers = append([]proto.Header(nil), headers...)
		}
		records = append(records, rec)
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("read stdin: %w", err)
	}
	if len(records) == 0 {
		return 0, nil
	}
	if _, err := prod.SendBatch(ctx, topic, records); err != nil {
		return 0, fmt.Errorf("send batch: %w", err)
	}
	return len(records), nil
}

// produceFromReader sends one record per line read from r. Returns
// the count of successfully-sent records. With keySep non-empty,
// each line is split on the first occurrence into (key, value);
// otherwise the whole line becomes the record value.
func produceFromReader(ctx context.Context, prod *sdk.Producer, topic string, r io.Reader, keySep string, headers []proto.Header) (int, error) {
	scanner := bufio.NewScanner(r)
	// Allow long lines (default Scanner cap is 64KiB).
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		var rec proto.Record
		if keySep == "" {
			rec.Value = []byte(line)
		} else {
			k, v, ok := strings.Cut(line, keySep)
			if ok {
				rec.Key = []byte(k)
				rec.Value = []byte(v)
			} else {
				rec.Value = []byte(line)
			}
		}
		if len(headers) > 0 {
			rec.Headers = append([]proto.Header(nil), headers...)
		}
		if _, err := prod.Send(ctx, topic, rec); err != nil {
			return count, fmt.Errorf("send line %d: %w", count+1, err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("read stdin: %w", err)
	}
	return count, nil
}
