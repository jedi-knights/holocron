// Package router is a content-based routing connector. It consumes one
// input topic, evaluates a list of rules against each record, and
// publishes matching records to the rule's target topics. This is the
// EventBridge / NATS-routing taxonomy category, expressed inside
// holocron's "broker stays dumb" architecture: routing logic runs in a
// connector, not the broker.
//
// V1 predicate vocabulary (all composed inside a Match value):
//
//   - HeaderKey + HeaderValue — exact-match a header
//   - HeaderExists — header present, any value
//   - KeyPrefix — key string starts with prefix
//   - KeyRegex — key matches a compiled *regexp.Regexp
//   - Any — disjunction of nested Matches (OR)
//
// Top-level fields are AND-ed; Any (when non-empty) is conjoined with the
// AND of top-level fields. A full expression DSL (jq, CEL) is a stretch
// goal; this shape covers the typical "dispatch by event-type header"
// and "regex on key" cases.
package router

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/jedi-knights/holocron/connect"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// Match is a predicate over a record. Evaluation:
//
//  1. Every non-empty top-level field must hold (AND of HeaderKey,
//     HeaderExists, KeyPrefix, KeyRegex).
//  2. If Any is non-empty, at least one nested Match must hold (OR).
//
// Steps 1 and 2 are conjoined: both must succeed for the record to
// match. A zero-value Match (no fields set, no Any) is the universal
// match-all.
type Match struct {
	// HeaderKey + HeaderValue: exact-match a header. Both required;
	// empty HeaderKey skips the check.
	HeaderKey   string
	HeaderValue string

	// HeaderExists: matches when the record carries a header with this
	// key, regardless of its value. Empty means "do not check."
	HeaderExists string

	// KeyPrefix matches when the record's Key starts with this prefix.
	// Empty means "do not check key prefix."
	KeyPrefix string

	// KeyRegex matches when the record's Key matches the compiled
	// pattern. Caller is responsible for compilation; nil means
	// "do not check key regex." Compile upfront via regexp.MustCompile
	// so per-record evaluation is just a method call.
	KeyRegex *regexp.Regexp

	// Any is a disjunction: if non-empty, at least one nested Match
	// must hold. Use this to express OR composition. Top-level fields
	// are still AND-ed alongside Any — i.e., (top-level AND any-of-Any).
	Any []Match
}

// Matches reports whether r satisfies the Match. See Match's comment for
// the AND/OR composition rules.
func (m Match) Matches(r proto.Record) bool {
	if m.HeaderKey != "" {
		if !headerEquals(r.Headers, m.HeaderKey, m.HeaderValue) {
			return false
		}
	}
	if m.HeaderExists != "" {
		if !headerPresent(r.Headers, m.HeaderExists) {
			return false
		}
	}
	if m.KeyPrefix != "" {
		if !strings.HasPrefix(string(r.Key), m.KeyPrefix) {
			return false
		}
	}
	if m.KeyRegex != nil {
		if !m.KeyRegex.Match(r.Key) {
			return false
		}
	}
	if len(m.Any) > 0 {
		anyHit := false
		for _, sub := range m.Any {
			if sub.Matches(r) {
				anyHit = true
				break
			}
		}
		if !anyHit {
			return false
		}
	}
	return true
}

func headerEquals(headers []proto.Header, key, value string) bool {
	for _, h := range headers {
		if h.Key == key && string(h.Value) == value {
			return true
		}
	}
	return false
}

func headerPresent(headers []proto.Header, key string) bool {
	for _, h := range headers {
		if h.Key == key {
			return true
		}
	}
	return false
}

// Rule pairs a predicate with the target topics records matching it
// should be published to. A record may match multiple rules; it is
// published to every target in every matching rule (deduped).
type Rule struct {
	Match   Match
	Targets []string
}

// Config configures a router Connector.
type Config struct {
	// Name identifies the connector and is used as the consumer group
	// ID; pick something stable across restarts so offsets resume.
	Name string
	// SourceTopic is the topic this router consumes from.
	SourceTopic string
	// Rules are evaluated in order on each record; matches are unioned.
	Rules []Rule
}

// Connector is a connect.SinkConnector that fans out matching records
// to per-rule target topics.
type Connector struct {
	cfg       Config
	transport sdk.Transport
}

// New returns a Connector. The Transport is required so the router task
// can publish to its target topics; sink tasks otherwise only receive
// records, but routing is half-source by nature.
func New(transport sdk.Transport, cfg Config) (*Connector, error) {
	if transport == nil {
		return nil, errors.New("router: New requires a Transport")
	}
	if cfg.Name == "" {
		return nil, errors.New("router: Config.Name is required")
	}
	if cfg.SourceTopic == "" {
		return nil, errors.New("router: Config.SourceTopic is required")
	}
	return &Connector{cfg: cfg, transport: transport}, nil
}

// Name implements connect.SinkConnector.
func (c *Connector) Name() string { return c.cfg.Name }

// Topics implements connect.SinkConnector.
func (c *Connector) Topics() []string { return []string{c.cfg.SourceTopic} }

// Tasks implements connect.SinkConnector. The router publishes
// internally, so each task carries its own Producer.
func (c *Connector) Tasks(maxTasks int) ([]connect.SinkTask, error) {
	_ = maxTasks
	return []connect.SinkTask{&task{
		rules:     c.cfg.Rules,
		transport: c.transport,
	}}, nil
}

type task struct {
	rules     []Rule
	transport sdk.Transport
	producer  *sdk.Producer
}

func (t *task) Init(ctx context.Context) error {
	_ = ctx
	p, err := sdk.NewProducer(t.transport)
	if err != nil {
		return err
	}
	t.producer = p
	return nil
}

func (t *task) Put(ctx context.Context, records []proto.Record) error {
	for _, r := range records {
		// Collect deduped target topics for this record across all
		// matching rules.
		seen := make(map[string]struct{})
		for _, rule := range t.rules {
			if !rule.Match.Matches(r) {
				continue
			}
			for _, target := range rule.Targets {
				if _, dup := seen[target]; dup {
					continue
				}
				seen[target] = struct{}{}
				if _, err := t.producer.Send(ctx, target, proto.Record{
					Key:     r.Key,
					Value:   r.Value,
					Headers: r.Headers,
				}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Flush is a no-op: each Put publishes synchronously, so there's
// nothing buffered to flush.
func (t *task) Flush(ctx context.Context) error {
	_ = ctx
	return nil
}

func (t *task) Close() error {
	if t.producer != nil {
		return t.producer.Close()
	}
	return nil
}
