package embed_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/broker/internal/auth"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
	holocronnet "github.com/jedi-knights/holocron/sdk/net"
)

// End-to-end coverage for PR 2 of the ACL wave. With a JWT verifier
// configured, the server installs a ScopeAuthorizer by default and
// enforces holocron.scopes claims on produce + fetch.

func TestEmbed_Authz_DeniesProduceWithoutScope(t *testing.T) {
	pub, priv := mustEd25519Keypair(t)
	token := mustIssueJWT(t, priv, auth.Claims{
		Subject: "alice",
		Expires: time.Now().Add(time.Hour).Unix(),
		// No Scopes — must be denied.
	})

	addr := startVerifierBroker(t, pub)
	tr := mustDialWithJWT(t, addr, token)
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tr.CreateTopic(ctx, "events", 1); err != nil {
		// CreateTopic isn't authorized in PR 2; only produce/fetch are.
		// Topic-create stays open so the rest of the test can proceed.
		t.Fatalf("CreateTopic should still admit (PR 3 adds admin authz): %v", err)
	}

	p, _ := sdk.NewProducer(tr)
	defer p.Close()
	_, err := p.Send(ctx, "events", proto.Record{Value: []byte("payload")})

	if err == nil {
		t.Fatal("produce without produce:events scope must be denied")
	}
	if !proto.IsStatus(err, proto.StatusForbidden) {
		t.Errorf("expected StatusForbidden, got %v", err)
	}
}

func TestEmbed_Authz_AllowsProduceWithMatchingScope(t *testing.T) {
	pub, priv := mustEd25519Keypair(t)
	token := mustIssueJWT(t, priv, auth.Claims{
		Subject: "alice",
		Scopes:  []string{"produce:events"},
		Expires: time.Now().Add(time.Hour).Unix(),
	})

	addr := startVerifierBroker(t, pub)
	tr := mustDialWithJWT(t, addr, token)
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tr.CreateTopic(ctx, "events", 1); err != nil {
		t.Fatal(err)
	}

	p, _ := sdk.NewProducer(tr)
	defer p.Close()
	if _, err := p.Send(ctx, "events", proto.Record{Value: []byte("payload")}); err != nil {
		t.Errorf("produce with produce:events scope should succeed: %v", err)
	}
}

func TestEmbed_Authz_AllowsProduceWildcard(t *testing.T) {
	pub, priv := mustEd25519Keypair(t)
	token := mustIssueJWT(t, priv, auth.Claims{
		Subject: "alice",
		Scopes:  []string{"produce:*"},
		Expires: time.Now().Add(time.Hour).Unix(),
	})

	addr := startVerifierBroker(t, pub)
	tr := mustDialWithJWT(t, addr, token)
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tr.CreateTopic(ctx, "anything", 1); err != nil {
		t.Fatal(err)
	}
	p, _ := sdk.NewProducer(tr)
	defer p.Close()
	if _, err := p.Send(ctx, "anything", proto.Record{Value: []byte("x")}); err != nil {
		t.Errorf("produce:* should authorize any topic: %v", err)
	}
}

func TestEmbed_Authz_DeniesProduceCrossVerb(t *testing.T) {
	// consume:events does NOT grant produce:events.
	pub, priv := mustEd25519Keypair(t)
	token := mustIssueJWT(t, priv, auth.Claims{
		Subject: "alice",
		Scopes:  []string{"consume:events"},
		Expires: time.Now().Add(time.Hour).Unix(),
	})

	addr := startVerifierBroker(t, pub)
	tr := mustDialWithJWT(t, addr, token)
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tr.CreateTopic(ctx, "events", 1); err != nil {
		t.Fatal(err)
	}
	p, _ := sdk.NewProducer(tr)
	defer p.Close()
	_, err := p.Send(ctx, "events", proto.Record{Value: []byte("payload")})
	if err == nil {
		t.Fatal("consume scope must NOT authorize produce")
	}
	if !proto.IsStatus(err, proto.StatusForbidden) {
		t.Errorf("expected StatusForbidden, got %v", err)
	}
}

func TestEmbed_Authz_DeniesFetchWithoutScope(t *testing.T) {
	pub, priv := mustEd25519Keypair(t)
	// Issue with produce-only — must be denied for consume.
	token := mustIssueJWT(t, priv, auth.Claims{
		Subject: "alice",
		Scopes:  []string{"produce:events"},
		Expires: time.Now().Add(time.Hour).Unix(),
	})

	addr := startVerifierBroker(t, pub)
	tr := mustDialWithJWT(t, addr, token)
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tr.CreateTopic(ctx, "events", 1); err != nil {
		t.Fatal(err)
	}
	// Direct fetch via the transport — surfaces the StatusForbidden
	// without going through the long-poll consumer wrapper.
	_, err := tr.HighWater(ctx, proto.PartitionRef{Topic: "events", Index: 0})
	_ = err // HighWater isn't gated by ACL today; use Subscribe instead
	_, errCh, err := tr.Subscribe(ctx, proto.PartitionRef{Topic: "events", Index: 0}, 0)
	if err != nil {
		// Subscribe could surface immediately or via errCh; either path is correct.
		if !proto.IsStatus(err, proto.StatusForbidden) {
			t.Errorf("expected StatusForbidden from Subscribe, got %v", err)
		}
		return
	}
	select {
	case e := <-errCh:
		if e == nil {
			t.Fatal("expected ErrCh to receive a forbidden error")
		}
		if !proto.IsStatus(e, proto.StatusForbidden) {
			t.Errorf("expected StatusForbidden on errCh, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscribe did not surface forbidden error within 2s")
	}
}

// startVerifierBroker brings up an in-memory broker with the given
// public Ed25519 key configured as the JWT verifier. Returns the
// listener's bind address.
func startVerifierBroker(t *testing.T, pub ed25519.PublicKey) string {
	t.Helper()
	b := embed.NewMemory()
	t.Cleanup(func() { _ = b.Close() })
	addr, err := b.Listen("127.0.0.1:0", embed.WithAuthVerifier(auth.NewEd25519Verifier(pub)))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	return addr
}

func mustDialWithJWT(t *testing.T, addr string, token []byte) *holocronnet.Transport {
	t.Helper()
	tr, err := holocronnet.Dial(addr, holocronnet.WithCredential(sdk.JWTCredential(token)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return tr
}

func mustEd25519Keypair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return pub, priv
}

func mustIssueJWT(t *testing.T, priv ed25519.PrivateKey, claims auth.Claims) []byte {
	t.Helper()
	token, err := auth.IssueJWT(priv, claims)
	if err != nil {
		t.Fatalf("IssueJWT: %v", err)
	}
	return token
}
