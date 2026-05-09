// Package file ships reference source/sink connectors that read and
// write line-oriented files. They are intentionally small: 50–100 lines
// each is enough to demonstrate the connect package's lifecycle and
// offset handling.
package file

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/connect"
)

const sourcePollSleep = 100 * time.Millisecond

// SourceConfig configures a file source: which file to read, and the
// holocron topic to publish each line to.
type SourceConfig struct {
	Name  string // connector name (also used as offset namespace)
	Path  string // file to tail
	Topic string // destination topic
}

// Source is a connect.SourceConnector that reads a file line by line and
// produces each line as a record. On restart it resumes from the last
// committed byte offset.
type Source struct {
	cfg SourceConfig
}

// NewSource returns a Source.
func NewSource(cfg SourceConfig) *Source { return &Source{cfg: cfg} }

// Name implements connect.SourceConnector.
func (s *Source) Name() string { return s.cfg.Name }

// Tasks implements connect.SourceConnector. The file source produces a
// single task — files are not partitionable. maxTasks is ignored beyond
// the implicit max-of-1.
func (s *Source) Tasks(maxTasks int) ([]connect.SourceTask, error) {
	if s.cfg.Path == "" {
		return nil, errors.New("file: source Path is required")
	}
	if s.cfg.Topic == "" {
		return nil, errors.New("file: source Topic is required")
	}
	return []connect.SourceTask{&sourceTask{cfg: s.cfg}}, nil
}

type sourceTask struct {
	cfg SourceConfig

	mu     sync.Mutex
	file   *os.File
	reader *bufio.Reader
	pos    int64
}

func (t *sourceTask) Init(ctx context.Context, storedOffsets []map[string]any) error {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()

	resumeAt := int64(0)
	for _, off := range storedOffsets {
		if v, ok := off["pos"].(int64); ok && v > resumeAt {
			resumeAt = v
		}
		if v, ok := off["pos"].(float64); ok && int64(v) > resumeAt {
			// JSON unmarshalling sometimes hands us float64.
			resumeAt = int64(v)
		}
	}

	f, err := os.Open(t.cfg.Path)
	if err != nil {
		return fmt.Errorf("file: open %s: %w", t.cfg.Path, err)
	}
	if resumeAt > 0 {
		if _, err := f.Seek(resumeAt, io.SeekStart); err != nil {
			_ = f.Close()
			return fmt.Errorf("file: seek %s to %d: %w", t.cfg.Path, resumeAt, err)
		}
	}
	t.file = f
	t.reader = bufio.NewReader(f)
	t.pos = resumeAt
	return nil
}

func (t *sourceTask) Poll(ctx context.Context) ([]connect.SourceRecord, error) {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.file == nil {
		return nil, errors.New("file: source not initialized")
	}

	var out []connect.SourceRecord
	for {
		line, err := t.reader.ReadString('\n')
		if len(line) > 0 {
			t.pos += int64(len(line))
			out = append(out, connect.SourceRecord{
				Topic: t.cfg.Topic,
				Value: []byte(trimNewline(line)),
				SourceOffset: map[string]any{
					"pos": t.pos,
				},
			})
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(out) == 0 {
					// Sleep briefly so the worker doesn't busy-loop.
					time.Sleep(sourcePollSleep)
				}
				return out, nil
			}
			return out, fmt.Errorf("file: read %s: %w", t.cfg.Path, err)
		}
	}
}

func (t *sourceTask) Commit(ctx context.Context, records []connect.SourceRecord) error {
	// File source is restart-tolerant via its `pos` offset; there's
	// nothing else to do at commit time. A real implementation would
	// persist the highest record's pos to durable storage so a fresh
	// task could resume.
	_, _ = ctx, records
	return nil
}

func (t *sourceTask) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.file == nil {
		return nil
	}
	err := t.file.Close()
	t.file = nil
	t.reader = nil
	return err
}

func trimNewline(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}
