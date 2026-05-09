package server

import (
	"testing"
)

// TestSetQuotas_BuildsBothLimiters proves SetQuotas wires both produce
// and fetch limiters when a Quota carries both rates. Each lookup must
// return an independent token bucket so produce traffic doesn't drain
// the fetch budget.
func TestSetQuotas_BuildsBothLimiters(t *testing.T) {
	// Arrange
	s := New(nil)
	s.SetQuotas(map[string]Quota{
		"k1": {ProduceBytesPerSec: 100, FetchBytesPerSec: 200},
	})

	// Act
	prod := s.limiterFor("k1")
	fetch := s.fetchLimiterFor("k1")

	// Assert
	if prod == nil {
		t.Fatal("produce limiter missing")
	}
	if fetch == nil {
		t.Fatal("fetch limiter missing")
	}
	if prod == fetch {
		t.Fatal("produce and fetch limiters are the same instance")
	}
}

// TestSetQuotas_OmitsZeroRates proves a zero rate disables that
// direction's bucket without affecting the other.
func TestSetQuotas_OmitsZeroRates(t *testing.T) {
	// Arrange
	s := New(nil)
	s.SetQuotas(map[string]Quota{
		"prod-only":  {ProduceBytesPerSec: 100},
		"fetch-only": {FetchBytesPerSec: 100},
	})

	// Assert
	if s.limiterFor("prod-only") == nil {
		t.Error("prod-only: missing produce limiter")
	}
	if s.fetchLimiterFor("prod-only") != nil {
		t.Error("prod-only: unexpected fetch limiter")
	}
	if s.limiterFor("fetch-only") != nil {
		t.Error("fetch-only: unexpected produce limiter")
	}
	if s.fetchLimiterFor("fetch-only") == nil {
		t.Error("fetch-only: missing fetch limiter")
	}
}

// TestSetQuotas_ClearsOnEmpty proves passing an empty map clears any
// previously-configured limits.
func TestSetQuotas_ClearsOnEmpty(t *testing.T) {
	// Arrange
	s := New(nil)
	s.SetQuotas(map[string]Quota{
		"k1": {ProduceBytesPerSec: 100, FetchBytesPerSec: 100},
	})

	// Act
	s.SetQuotas(nil)

	// Assert
	if s.limiterFor("k1") != nil {
		t.Error("produce limiter survived clear")
	}
	if s.fetchLimiterFor("k1") != nil {
		t.Error("fetch limiter survived clear")
	}
}

// TestTokenBucket_TakeRespectsCapacity proves the bucket rejects a
// take that exceeds remaining tokens, preserving the bucket state for
// the next call.
func TestTokenBucket_TakeRespectsCapacity(t *testing.T) {
	// Arrange — 100 tokens/sec, 100 capacity, starts full.
	b := newTokenBucket(100, 100)

	// Act + Assert — take 60, then 50: second fails.
	if !b.take(60) {
		t.Fatal("first take of 60 failed")
	}
	if b.take(50) {
		t.Fatal("second take of 50 should fail (only 40 left)")
	}
	// Token state preserved: a smaller take still succeeds.
	if !b.take(40) {
		t.Fatal("take of remaining 40 failed")
	}
}
