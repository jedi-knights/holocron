package embed_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/broker/internal/auth"
	"github.com/jedi-knights/holocron/sdk"
	holocronnet "github.com/jedi-knights/holocron/sdk/net"
)

// End-to-end coverage of the JWT path landed in PRs 1-5 of the auth
// wave: a broker configured with an Ed25519 verifier accepts a
// matching JWT credential at handshake and rejects a forged one,
// using the public sdk.JWTCredential / holocronnet.WithCredential
// surface SDK callers will use.

func TestEmbed_JWTAuth_AcceptsValidToken(t *testing.T) {
	// Arrange
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	verifier := auth.NewEd25519Verifier(pub)
	token, err := auth.IssueJWT(priv, auth.Claims{
		Subject: "alice",
		Account: "default",
		Scopes:  []string{"produce:events", "consume:events"},
		Expires: time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("IssueJWT: %v", err)
	}

	b := embed.NewMemory()
	defer b.Close()
	// Pre-create the topic via the embed handle so the JWT under
	// test (no admin scope) can still produce/consume.
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	addr, err := b.Listen("127.0.0.1:0", embed.WithAuthVerifier(verifier))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	// Act
	tr, err := holocronnet.Dial(addr, holocronnet.WithCredential(sdk.JWTCredential(token)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer tr.Close()

	// Assert: handshake-and-RPC round-trip works end-to-end.
	// ListTopics is the cheapest call that exercises both — and it's
	// not gated by ACL.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := tr.ListTopics(ctx); err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
}

func TestEmbed_JWTAuth_RejectsForgedToken(t *testing.T) {
	// Arrange: broker trusts pub; attacker signs with otherPriv.
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey other: %v", err)
	}
	token, err := auth.IssueJWT(otherPriv, auth.Claims{
		Subject: "attacker",
		Expires: time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("IssueJWT: %v", err)
	}

	b := embed.NewMemory()
	defer b.Close()
	addr, err := b.Listen("127.0.0.1:0", embed.WithAuthVerifier(auth.NewEd25519Verifier(pub)))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	// Act / Assert: handshake fails — Dial may surface the error
	// directly, or the first RPC may; either is correct.
	tr, dialErr := holocronnet.Dial(addr, holocronnet.WithCredential(sdk.JWTCredential(token)))
	if dialErr != nil {
		return
	}
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tr.CreateTopic(ctx, "events", 1); err == nil {
		t.Fatal("forged token should not have been accepted")
	}
}

func TestEmbed_JWTAuth_RejectsAnonymousWhenRequired(t *testing.T) {
	// Arrange
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	b := embed.NewMemory()
	defer b.Close()
	addr, err := b.Listen("127.0.0.1:0", embed.WithAuthVerifier(auth.NewEd25519Verifier(pub)))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	// Act / Assert: dialing without WithCredential sends an anonymous
	// handshake which the Ed25519Verifier rejects.
	tr, dialErr := holocronnet.Dial(addr, holocronnet.WithDialTimeout(500*time.Millisecond))
	if dialErr != nil {
		return
	}
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := tr.CreateTopic(ctx, "events", 1); err == nil {
		t.Fatal("anonymous handshake should not have been accepted")
	}
}
