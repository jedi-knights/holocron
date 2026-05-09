package http

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"

	"github.com/jedi-knights/holocron/connect"
	"github.com/jedi-knights/holocron/proto"
)

const defaultContentType = "application/octet-stream"

// SinkConfig configures an HTTP sink. The sink POSTs each record's
// Value as the request body to URL with Content-Type ContentType.
// Non-2xx responses surface as Put errors so the Worker's retry/DLQ
// paths apply.
type SinkConfig struct {
	// Name identifies the connector. Required; also used as the
	// consumer-group ID.
	Name string
	// Topic is the source holocron topic. Required.
	Topic string
	// URL is the endpoint each record's value is POSTed to. Required.
	URL string
	// Method overrides the HTTP method. Defaults to POST.
	Method string
	// ContentType is set on every outgoing request. Defaults to
	// "application/octet-stream".
	ContentType string
	// Client is the http.Client used for requests. Defaults to
	// http.DefaultClient when nil.
	Client *stdhttp.Client
	// Headers are sent on every request, in addition to Content-Type.
	Headers map[string]string
}

// Sink is a connect.SinkConnector that POSTs each record to an HTTP
// endpoint.
type Sink struct {
	cfg SinkConfig
}

// NewSink returns a Sink.
func NewSink(cfg SinkConfig) *Sink { return &Sink{cfg: cfg} }

// Name implements connect.SinkConnector.
func (s *Sink) Name() string { return s.cfg.Name }

// Topics implements connect.SinkConnector.
func (s *Sink) Topics() []string { return []string{s.cfg.Topic} }

// Tasks implements connect.SinkConnector. Each task carries its own
// http.Client.
func (s *Sink) Tasks(maxTasks int) ([]connect.SinkTask, error) {
	if s.cfg.URL == "" {
		return nil, errors.New("http: sink URL is required")
	}
	if s.cfg.Topic == "" {
		return nil, errors.New("http: sink Topic is required")
	}
	cfg := s.cfg
	if cfg.Method == "" {
		cfg.Method = stdhttp.MethodPost
	}
	if cfg.ContentType == "" {
		cfg.ContentType = defaultContentType
	}
	if cfg.Client == nil {
		cfg.Client = stdhttp.DefaultClient
	}
	tasks := make([]connect.SinkTask, 0, maxTasks)
	for range maxTasks {
		tasks = append(tasks, &sinkTask{cfg: cfg})
	}
	if len(tasks) == 0 {
		tasks = append(tasks, &sinkTask{cfg: cfg})
	}
	return tasks, nil
}

type sinkTask struct {
	cfg SinkConfig
}

func (t *sinkTask) Init(_ context.Context) error { return nil }

func (t *sinkTask) Put(ctx context.Context, records []proto.Record) error {
	for _, r := range records {
		req, err := stdhttp.NewRequestWithContext(ctx, t.cfg.Method, t.cfg.URL, bytes.NewReader(r.Value))
		if err != nil {
			return fmt.Errorf("http: build request: %w", err)
		}
		req.Header.Set("Content-Type", t.cfg.ContentType)
		for k, v := range t.cfg.Headers {
			req.Header.Set(k, v)
		}
		resp, err := t.cfg.Client.Do(req)
		if err != nil {
			return fmt.Errorf("http: %s %s: %w", t.cfg.Method, t.cfg.URL, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("http: %s %s returned %d", t.cfg.Method, t.cfg.URL, resp.StatusCode)
		}
	}
	return nil
}

// Flush is a no-op: each Put posts synchronously with no buffering.
func (t *sinkTask) Flush(_ context.Context) error { return nil }

func (t *sinkTask) Close() error { return nil }
