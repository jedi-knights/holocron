package file

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/jedi-knights/holocron/connect"
	"github.com/jedi-knights/holocron/proto"
)

// SinkConfig configures a file sink: which topic to consume, and the
// file each record's value should be appended to (with a trailing
// newline).
type SinkConfig struct {
	Name  string
	Topic string
	Path  string
}

// Sink is a connect.SinkConnector that appends each record's value as a
// line to a file.
type Sink struct {
	cfg SinkConfig
}

// NewSink returns a Sink.
func NewSink(cfg SinkConfig) *Sink { return &Sink{cfg: cfg} }

// Name implements connect.SinkConnector.
func (s *Sink) Name() string { return s.cfg.Name }

// Topics implements connect.SinkConnector.
func (s *Sink) Topics() []string { return []string{s.cfg.Topic} }

// Tasks implements connect.SinkConnector. File sinks are inherently
// single-writer: returning more than one task would interleave writes.
func (s *Sink) Tasks(maxTasks int) ([]connect.SinkTask, error) {
	if s.cfg.Path == "" {
		return nil, errors.New("file: sink Path is required")
	}
	if s.cfg.Topic == "" {
		return nil, errors.New("file: sink Topic is required")
	}
	_ = maxTasks
	return []connect.SinkTask{&sinkTask{cfg: s.cfg}}, nil
}

type sinkTask struct {
	cfg SinkConfig

	mu   sync.Mutex
	file *os.File
}

func (t *sinkTask) Init(ctx context.Context) error {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()
	f, err := os.OpenFile(t.cfg.Path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("file: open sink %s: %w", t.cfg.Path, err)
	}
	t.file = f
	return nil
}

func (t *sinkTask) Put(ctx context.Context, records []proto.Record) error {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.file == nil {
		return errors.New("file: sink not initialized")
	}
	for _, r := range records {
		if _, err := t.file.Write(r.Value); err != nil {
			return fmt.Errorf("file: write sink %s: %w", t.cfg.Path, err)
		}
		if _, err := t.file.Write([]byte{'\n'}); err != nil {
			return fmt.Errorf("file: write newline %s: %w", t.cfg.Path, err)
		}
	}
	return nil
}

func (t *sinkTask) Flush(ctx context.Context) error {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.file == nil {
		return nil
	}
	return t.file.Sync()
}

func (t *sinkTask) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.file == nil {
		return nil
	}
	err := t.file.Close()
	t.file = nil
	return err
}
