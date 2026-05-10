package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/cli/internal/clienttls"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// runBench produces N records of configurable size to a topic and
// reports throughput plus latency percentiles. Single-shot load
// generator suitable for capacity planning and perf regression
// checks without writing a separate Go program.
//
// With --consume, bench instead reads N records from the topic
// and reports the same metrics from the consume path. Use a
// produce run first (or pre-populate the topic) so the consume
// run has records to read.
func runBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	topic := fs.String("topic", "", "target topic (required; must already exist)")
	count := fs.Int("count", 10_000, "number of records to produce")
	size := fs.Int("size", 256, "record value size in bytes (produce mode)")
	linger := fs.Duration("linger", 0, "producer linger window (0 = no batching)")
	batchSize := fs.Int("batch-size", 256, "producer batch size cap")
	producerCount := fs.Int("producer-count", 1, "number of concurrent Producers (--count is split across them)")
	consume := fs.Bool("consume", false, "consume mode: read N records and measure the fetch path")
	fromOffset := fs.Int64("from-offset", 0, "consume mode: starting offset")
	pollSize := fs.Int("poll-size", 256, "consume mode: max records per Poll call")
	timeout := fs.Duration("timeout", 60*time.Second, "overall run timeout")
	tlsCfg := clienttls.RegisterFlags(fs)
	credFile := fs.String("credential-file", os.Getenv("HOLOCRON_CREDENTIAL_FILE"), "path to a JWT file (mutually exclusive with --api-key)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" {
		return errors.New("bench: --topic is required")
	}
	if *count <= 0 {
		return errors.New("bench: --count must be > 0")
	}
	if !*consume && *size <= 0 {
		return errors.New("bench: --size must be > 0 in produce mode")
	}
	if *producerCount <= 0 {
		return errors.New("bench: --producer-count must be > 0")
	}

	cfg, err := tlsCfg()
	if err != nil {
		return err
	}
	dialOptsSlice, err := credentialOpts(*credFile, *apiKey, dialOpts(cfg)...)
	if err != nil {
		return err
	}
	tr, err := dial(*addr, dialOptsSlice...)
	if err != nil {
		return err
	}
	defer tr.Close()

	if *consume {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		return runBenchConsume(ctx, tr, *topic, *count, *fromOffset, *pollSize)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	value := make([]byte, *size)
	for i := range value {
		value[i] = byte('a' + (i % 26))
	}

	// Split count across producers. The remainder lands on the
	// last producer so the grand total equals --count exactly.
	perProducer := *count / *producerCount
	remainder := *count % *producerCount

	prodOpts := []sdk.ProducerOption{sdk.WithBatchSize(*batchSize)}
	if *linger > 0 {
		prodOpts = append(prodOpts, sdk.WithLinger(*linger))
	}

	type producerResult struct {
		latencies []time.Duration
		err       error
	}
	results := make([]producerResult, *producerCount)
	var wg sync.WaitGroup
	wg.Add(*producerCount)
	start := time.Now()
	for i := 0; i < *producerCount; i++ {
		myCount := perProducer
		if i == *producerCount-1 {
			myCount += remainder
		}
		go func(idx, recs int) {
			defer wg.Done()
			// Each producer dials its own connection so the
			// concurrent load actually parallelizes on the wire
			// rather than serializing through one socket.
			myTr, err := dial(*addr, dialOptsSlice...)
			if err != nil {
				results[idx] = producerResult{err: fmt.Errorf("producer %d dial: %w", idx, err)}
				return
			}
			defer myTr.Close()
			prod, err := sdk.NewProducer(myTr, prodOpts...)
			if err != nil {
				results[idx] = producerResult{err: fmt.Errorf("producer %d new: %w", idx, err)}
				return
			}
			defer prod.Close()
			lats := make([]time.Duration, 0, recs)
			for j := 0; j < recs; j++ {
				t0 := time.Now()
				if _, err := prod.Send(ctx, *topic, proto.Record{Value: value}); err != nil {
					results[idx] = producerResult{latencies: lats, err: fmt.Errorf("producer %d send %d: %w", idx, j, err)}
					return
				}
				lats = append(lats, time.Since(t0))
			}
			if err := prod.Flush(ctx); err != nil {
				results[idx] = producerResult{latencies: lats, err: fmt.Errorf("producer %d flush: %w", idx, err)}
				return
			}
			results[idx] = producerResult{latencies: lats}
		}(i, myCount)
	}
	wg.Wait()
	elapsed := time.Since(start)

	latencies := make([]time.Duration, 0, *count)
	for _, r := range results {
		if r.err != nil {
			return r.err
		}
		latencies = append(latencies, r.latencies...)
	}

	totalBytes := int64(*count) * int64(*size)
	rps := float64(*count) / elapsed.Seconds()
	mbps := float64(totalBytes) / elapsed.Seconds() / (1 << 20)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(p float64) time.Duration {
		idx := int(float64(len(latencies)-1) * p)
		return latencies[idx]
	}

	fmt.Printf("records:    %d\n", *count)
	fmt.Printf("producers:  %d\n", *producerCount)
	fmt.Printf("size/rec:   %d bytes\n", *size)
	fmt.Printf("elapsed:    %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput: %.0f records/sec, %.2f MB/sec\n", rps, mbps)
	fmt.Printf("latency:    p50=%v p99=%v max=%v\n",
		pct(0.50).Round(time.Microsecond),
		pct(0.99).Round(time.Microsecond),
		latencies[len(latencies)-1].Round(time.Microsecond))
	return nil
}

// runBenchConsume measures the read path: subscribe to topic from
// fromOffset, read count records in batches of pollSize, report
// throughput + per-batch latency. Useful for fetch-path
// regression checks and broker scaling tests; pre-populate the
// topic with `bench --topic X --count N` first if it's empty.
func runBenchConsume(ctx context.Context, tr sdk.Transport, topic string, count int, fromOffset int64, pollSize int) error {
	cons, err := sdk.NewConsumer(tr)
	if err != nil {
		return err
	}
	defer cons.Close()
	if err := cons.Subscribe(ctx, topic, fromOffset); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	pollLatencies := make([]time.Duration, 0, count/pollSize+1)
	got := 0
	var totalBytes int64
	start := time.Now()
	for got < count {
		want := pollSize
		if want > count-got {
			want = count - got
		}
		t0 := time.Now()
		recs, err := cons.Poll(ctx, want)
		if err != nil {
			return fmt.Errorf("bench poll @%d: %w", got, err)
		}
		pollLatencies = append(pollLatencies, time.Since(t0))
		for _, r := range recs {
			totalBytes += int64(len(r.Value))
		}
		got += len(recs)
	}
	elapsed := time.Since(start)

	rps := float64(got) / elapsed.Seconds()
	mbps := float64(totalBytes) / elapsed.Seconds() / (1 << 20)
	sort.Slice(pollLatencies, func(i, j int) bool { return pollLatencies[i] < pollLatencies[j] })
	pct := func(p float64) time.Duration {
		idx := int(float64(len(pollLatencies)-1) * p)
		return pollLatencies[idx]
	}

	fmt.Printf("mode:       consume\n")
	fmt.Printf("records:    %d\n", got)
	fmt.Printf("bytes:      %d\n", totalBytes)
	fmt.Printf("elapsed:    %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput: %.0f records/sec, %.2f MB/sec\n", rps, mbps)
	fmt.Printf("poll lat:   p50=%v p99=%v max=%v\n",
		pct(0.50).Round(time.Microsecond),
		pct(0.99).Round(time.Microsecond),
		pollLatencies[len(pollLatencies)-1].Round(time.Microsecond))
	return nil
}
