// Package metrics holds the broker's runtime counters and exposes them
// in Prometheus text format. No external dependency — the format is
// stable enough to write by hand and keep the broker's runtime narrow.
package metrics

import (
	"fmt"
	"io"
	"sync/atomic"
)

// Registry holds the broker's counters. Zero value is ready to use.
type Registry struct {
	RecordsProduced atomic.Int64
	RecordsFetched  atomic.Int64
	ProduceRequests atomic.Int64
	FetchRequests   atomic.Int64
	BytesProduced   atomic.Int64
	BytesFetched    atomic.Int64
}

// New returns a fresh Registry. Equivalent to &Registry{}; provided so
// callers can write `metrics.New()` next to other constructors.
func New() *Registry { return &Registry{} }

// IncProduce records one record produced of the given byte size.
func (r *Registry) IncProduce(bodyBytes int64) {
	r.RecordsProduced.Add(1)
	r.BytesProduced.Add(bodyBytes)
}

// IncProduceRequest records one ProduceRequest RPC.
func (r *Registry) IncProduceRequest() {
	r.ProduceRequests.Add(1)
}

// IncFetch records `count` records returned with a total of `bytes` bytes.
func (r *Registry) IncFetch(count, bodyBytes int64) {
	r.RecordsFetched.Add(count)
	r.BytesFetched.Add(bodyBytes)
}

// IncFetchRequest records one FetchRequest RPC.
func (r *Registry) IncFetchRequest() {
	r.FetchRequests.Add(1)
}

// WritePrometheus emits the registry in Prometheus text format. Output
// is deterministic for a given snapshot of counters; intended for
// scrapes (HTTP GET /metrics).
func (r *Registry) WritePrometheus(w io.Writer) error {
	entries := []struct {
		name string
		help string
		val  int64
	}{
		{"holocron_records_produced_total", "Total records appended via Publish or ProduceBatch.", r.RecordsProduced.Load()},
		{"holocron_records_fetched_total", "Total records returned by Fetch.", r.RecordsFetched.Load()},
		{"holocron_produce_requests_total", "Total Produce + ProduceBatch RPCs handled.", r.ProduceRequests.Load()},
		{"holocron_fetch_requests_total", "Total Fetch RPCs handled.", r.FetchRequests.Load()},
		{"holocron_bytes_produced_total", "Total record-value bytes accepted by the broker.", r.BytesProduced.Load()},
		{"holocron_bytes_fetched_total", "Total record-value bytes served by the broker.", r.BytesFetched.Load()},
	}
	for _, e := range entries {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n",
			e.name, e.help, e.name, e.name, e.val); err != nil {
			return err
		}
	}
	return nil
}
