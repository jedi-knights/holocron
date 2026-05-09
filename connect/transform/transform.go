// Package transform is a content-based transform connector. It consumes
// one input topic, applies a user-supplied function to each record, and
// publishes non-dropped results to a target topic.
//
// Pairs naturally with the router connector: route records to type-
// specific topics, then run a transform per topic to project / parse /
// reshape. Both live inside holocron's "broker stays dumb" rule —
// transformation logic lives in a connector, not the broker.
//
// Records returned with the second value `false` are dropped (filter-
// in-transform). Errors abort the batch so the Worker's retry/DLQ paths
// can intervene.
package transform

import (
	"context"
	"errors"
	"fmt"

	"github.com/jedi-knights/holocron/connect"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// Func transforms a record into zero or one output records. Returning
// `keep=false` drops the record (no output emitted). Returning a
// non-nil error aborts the current batch.
type Func func(in proto.Record) (out proto.Record, keep bool, err error)

// Config configures a transform Connector.
type Config struct {
	// Name identifies the connector and is used as the consumer group
	// ID; pick something stable across restarts so offsets resume.
	Name string
	// SourceTopic is the topic this transform consumes from.
	SourceTopic string
	// TargetTopic is the topic transformed records are published to.
	// Required; differs from SourceTopic to avoid feedback loops.
	TargetTopic string
	// Fn is applied to each consumed record.
	Fn Func
}

// Connector is a connect.SinkConnector that applies Fn to each record
// and publishes the transformed result to TargetTopic.
type Connector struct {
	cfg       Config
	transport sdk.Transport
}

// New returns a Connector. The Transport is required so the transform
// task can publish to its target topic.
func New(transport sdk.Transport, cfg Config) (*Connector, error) {
	if transport == nil {
		return nil, errors.New("transform: New requires a Transport")
	}
	if cfg.Name == "" {
		return nil, errors.New("transform: Config.Name is required")
	}
	if cfg.SourceTopic == "" {
		return nil, errors.New("transform: Config.SourceTopic is required")
	}
	if cfg.TargetTopic == "" {
		return nil, errors.New("transform: Config.TargetTopic is required")
	}
	if cfg.SourceTopic == cfg.TargetTopic {
		return nil, errors.New("transform: SourceTopic and TargetTopic must differ")
	}
	if cfg.Fn == nil {
		return nil, errors.New("transform: Config.Fn is required")
	}
	return &Connector{cfg: cfg, transport: transport}, nil
}

// Name implements connect.SinkConnector.
func (c *Connector) Name() string { return c.cfg.Name }

// Topics implements connect.SinkConnector.
func (c *Connector) Topics() []string { return []string{c.cfg.SourceTopic} }

// Tasks implements connect.SinkConnector. The transform publishes
// internally, so each task carries its own Producer.
func (c *Connector) Tasks(maxTasks int) ([]connect.SinkTask, error) {
	_ = maxTasks
	return []connect.SinkTask{&task{
		fn:        c.cfg.Fn,
		target:    c.cfg.TargetTopic,
		transport: c.transport,
	}}, nil
}

type task struct {
	fn        Func
	target    string
	transport sdk.Transport
	producer  *sdk.Producer
}

func (t *task) Init(_ context.Context) error {
	p, err := sdk.NewProducer(t.transport)
	if err != nil {
		return err
	}
	t.producer = p
	return nil
}

func (t *task) Put(ctx context.Context, records []proto.Record) error {
	for _, r := range records {
		out, keep, err := t.fn(r)
		if err != nil {
			return fmt.Errorf("transform: %w", err)
		}
		if !keep {
			continue
		}
		if _, err := t.producer.Send(ctx, t.target, out); err != nil {
			return err
		}
	}
	return nil
}

// Flush is a no-op: each Put publishes synchronously.
func (t *task) Flush(_ context.Context) error { return nil }

func (t *task) Close() error {
	if t.producer != nil {
		return t.producer.Close()
	}
	return nil
}
