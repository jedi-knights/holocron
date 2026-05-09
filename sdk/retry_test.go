package sdk_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// flakyTransport wraps recordingTransport behavior with a Publish that
// returns StatusRateLimited the first failTimes calls, then succeeds.
type flakyTransport struct {
	mu        sync.Mutex
	failTimes int
	calls     int
}

func (t *flakyTransport) Publish(_ context.Context, _ proto.PartitionRef, _ proto.Record) (int64, error) {
	t.mu.Lock()
	t.calls++
	if t.calls <= t.failTimes {
		t.mu.Unlock()
		return 0, &proto.ProtocolError{Status: proto.StatusRateLimited, Message: "produce quota exceeded"}
	}
	t.mu.Unlock()
	return int64(t.calls), nil
}

func (t *flakyTransport) PublishBatch(_ context.Context, _ proto.PartitionRef, _ []proto.Record) (int64, error) {
	return 0, nil
}
func (t *flakyTransport) Subscribe(_ context.Context, _ proto.PartitionRef, _ int64) (<-chan proto.Record, <-chan error, error) {
	return nil, nil, nil
}
func (t *flakyTransport) Commit(_ context.Context, _ string, _ proto.PartitionRef, _ int64) error {
	return nil
}
func (t *flakyTransport) PartitionsFor(_ context.Context, _ string) (int32, error) { return 1, nil }
func (t *flakyTransport) JoinGroup(_ context.Context, _, _ string, _ []string) (sdk.JoinResult, error) {
	return sdk.JoinResult{}, nil
}
func (t *flakyTransport) Heartbeat(_ context.Context, _, _ string, _ int32, _ time.Duration) (sdk.HeartbeatResult, error) {
	return sdk.HeartbeatResult{}, nil
}
func (t *flakyTransport) LeaveGroup(_ context.Context, _, _ string) error { return nil }
func (t *flakyTransport) Sync(_ context.Context, _ proto.PartitionRef) error {
	return nil
}
func (t *flakyTransport) Close() error { return nil }

func (t *flakyTransport) callCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

// TestProducer_RateLimitRetry_EventuallySucceeds proves WithRateLimitRetry
// retries past a transient StatusRateLimited and returns the eventual
// successful offset.
func TestProducer_RateLimitRetry_EventuallySucceeds(t *testing.T) {
	// Arrange — first 2 publishes fail rate-limited, third succeeds.
	tr := &flakyTransport{failTimes: 2}
	p, err := sdk.NewProducer(tr, sdk.WithRateLimitRetry(5, 5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Act
	off, err := p.Send(context.Background(), "events", proto.Record{Value: []byte("x")})

	// Assert
	if err != nil {
		t.Fatalf("expected retry success, got %v", err)
	}
	if off != 3 {
		t.Errorf("expected offset 3 (after 2 failures + 1 success), got %d", off)
	}
	if got := tr.callCount(); got != 3 {
		t.Errorf("expected 3 publish calls (2 fail + 1 success), got %d", got)
	}
}

// TestProducer_RateLimitRetry_ExhaustsAttempts proves the retry surface
// surfaces the rate-limit error after `tries` attempts.
func TestProducer_RateLimitRetry_ExhaustsAttempts(t *testing.T) {
	// Arrange — every publish rate-limits.
	tr := &flakyTransport{failTimes: 99}
	p, err := sdk.NewProducer(tr, sdk.WithRateLimitRetry(2, 1*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Act
	_, err = p.Send(context.Background(), "events", proto.Record{Value: []byte("x")})

	// Assert
	if err == nil {
		t.Fatal("expected rate-limit error after exhausted retries")
	}
	var pe *proto.ProtocolError
	if !errors.As(err, &pe) || pe.Status != proto.StatusRateLimited {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestProducer_NoRetryByDefault proves that without WithRateLimitRetry
// the rate-limit error fails fast — preserves the existing fail-fast
// behavior for callers that haven't opted in.
func TestProducer_NoRetryByDefault(t *testing.T) {
	tr := &flakyTransport{failTimes: 1}
	p, err := sdk.NewProducer(tr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	_, err = p.Send(context.Background(), "events", proto.Record{Value: []byte("x")})
	if err == nil {
		t.Fatal("expected rate-limit error without retry option")
	}
	if got := tr.callCount(); got != 1 {
		t.Errorf("expected exactly 1 publish call, got %d", got)
	}
}
