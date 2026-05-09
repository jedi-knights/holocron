package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"
	"time"

	"github.com/jedi-knights/holocron/connect"
)

const defaultPollInterval = 1 * time.Second

// SourceConfig configures a poll-based HTTP source. The source GETs URL
// on every PollInterval, splits the response body by newlines, and
// emits each non-empty line as a record on Topic.
//
// Cursor support is opt-in: set CursorParam (the query string key the
// server expects) and CursorHeader (the response header that carries
// the next cursor value). Each poll appends `?<CursorParam>=<cursor>`
// to URL, reads the next cursor from the response header, and writes
// it onto every emitted record's SourceOffset. With an OffsetStore
// configured on the Worker, the cursor survives restarts.
//
// Without cursor configuration the source behaves as in batch-8 V1:
// every poll re-fetches the full URL. Endpoints that already return
// only "new" records work cleanly; static endpoints will produce
// duplicates each poll.
type SourceConfig struct {
	// Name identifies the connector and is used as the offset namespace
	// when paired with an OffsetStore. Required.
	Name string
	// URL is fetched on every poll. Required.
	URL string
	// Topic is the destination holocron topic. Required.
	Topic string
	// PollInterval is the time between successive GETs. Defaults to
	// 1s when zero.
	PollInterval time.Duration
	// Client is the http.Client used for requests. Defaults to
	// http.DefaultClient when nil. Override to set Timeout, custom
	// transport, etc.
	Client *stdhttp.Client
	// Headers are added to every request. The map is read once at
	// task construction; later mutations are not observed.
	Headers map[string]string

	// CursorParam, when set, names the URL query parameter that carries
	// the current cursor value. Empty disables cursor mode.
	CursorParam string
	// CursorHeader names the response header that holds the next cursor.
	// Required alongside CursorParam.
	CursorHeader string
	// InitialCursor seeds the cursor on first run when no offset is
	// stored. Empty omits the cursor parameter on the first request.
	InitialCursor string
}

// Source is a connect.SourceConnector that polls an HTTP endpoint.
type Source struct {
	cfg SourceConfig
}

// NewSource returns a Source.
func NewSource(cfg SourceConfig) *Source { return &Source{cfg: cfg} }

// Name implements connect.SourceConnector.
func (s *Source) Name() string { return s.cfg.Name }

// Tasks implements connect.SourceConnector. The HTTP source produces a
// single task — one URL, one task. maxTasks beyond 1 is ignored.
func (s *Source) Tasks(maxTasks int) ([]connect.SourceTask, error) {
	if s.cfg.URL == "" {
		return nil, errors.New("http: source URL is required")
	}
	if s.cfg.Topic == "" {
		return nil, errors.New("http: source Topic is required")
	}
	cfg := s.cfg
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.Client == nil {
		cfg.Client = stdhttp.DefaultClient
	}
	_ = maxTasks
	return []connect.SourceTask{&sourceTask{cfg: cfg}}, nil
}

type sourceTask struct {
	cfg     SourceConfig
	lastHit time.Time
	cursor  string
}

func (t *sourceTask) Init(_ context.Context, storedOffsets []map[string]any) error {
	t.cursor = t.cfg.InitialCursor
	for _, off := range storedOffsets {
		if v, ok := off["cursor"].(string); ok && v != "" {
			t.cursor = v
		}
	}
	return nil
}

func (t *sourceTask) Poll(ctx context.Context) ([]connect.SourceRecord, error) {
	// Throttle to PollInterval. The Worker calls Poll in a tight loop,
	// so the task itself is responsible for pacing HTTP requests.
	if !t.lastHit.IsZero() {
		elapsed := time.Since(t.lastHit)
		if elapsed < t.cfg.PollInterval {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(t.cfg.PollInterval - elapsed):
			}
		}
	}
	t.lastHit = time.Now()

	url := t.buildURL()
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("http: build request: %w", err)
	}
	for k, v := range t.cfg.Headers {
		req.Header.Set(k, v)
	}
	resp, err := t.cfg.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http: GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http: GET %s returned %d", url, resp.StatusCode)
	}

	if t.cfg.CursorHeader != "" {
		if next := resp.Header.Get(t.cfg.CursorHeader); next != "" {
			t.cursor = next
		}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("http: read body %s: %w", url, err)
	}

	var out []connect.SourceRecord
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" {
			continue
		}
		rec := connect.SourceRecord{
			Topic: t.cfg.Topic,
			Value: []byte(line),
		}
		if t.cfg.CursorParam != "" {
			rec.SourceOffset = map[string]any{"cursor": t.cursor}
		}
		out = append(out, rec)
	}
	return out, nil
}

// buildURL appends ?<CursorParam>=<cursor> when cursor mode is enabled
// and a non-empty cursor is in hand. Preserves any existing query
// string by inspecting the URL for "?".
func (t *sourceTask) buildURL() string {
	if t.cfg.CursorParam == "" || t.cursor == "" {
		return t.cfg.URL
	}
	sep := "?"
	if strings.Contains(t.cfg.URL, "?") {
		sep = "&"
	}
	return t.cfg.URL + sep + t.cfg.CursorParam + "=" + t.cursor
}

func (t *sourceTask) Commit(_ context.Context, _ []connect.SourceRecord) error {
	// SourceOffset on each emitted record carries the latest cursor;
	// the Worker persists it through OffsetStore.Save after this call.
	return nil
}

func (t *sourceTask) Close() error { return nil }
