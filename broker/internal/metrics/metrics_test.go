package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestRegistry_PrometheusFormat(t *testing.T) {
	// Arrange
	r := New()
	r.IncProduce(42)
	r.IncProduce(100)
	r.IncProduceRequest()
	r.IncFetch(3, 200)
	r.IncFetchRequest()

	// Act
	var buf bytes.Buffer
	if err := r.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()

	// Assert
	for _, want := range []string{
		"holocron_records_produced_total 2",
		"holocron_bytes_produced_total 142",
		"holocron_records_fetched_total 3",
		"holocron_bytes_fetched_total 200",
		"holocron_produce_requests_total 1",
		"holocron_fetch_requests_total 1",
		"# TYPE holocron_records_produced_total counter",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output:\n%s", want, out)
		}
	}
}
