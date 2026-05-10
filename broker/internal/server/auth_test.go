package server

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/internal/auth"
	"github.com/jedi-knights/holocron/proto"
)

// These tests exercise the handshake() integration with the auth
// package. The handshake function is the single enforcement point for
// PR 3; once a Principal lands here, the rest of the request path
// continues to flow apiKey-as-string until PR 7 threads the full
// Principal through cluster.Apply.

func TestServer_Handshake_AnonymousByDefault(t *testing.T) {
	// Arrange: no verifier configured → AnonymousVerifier default.
	s := New(nil)
	in, out := newHandshakeFrame(t, proto.HandshakeRequest{
		Version: proto.WireVersion,
	})

	// Act
	p, err := s.handshake(in, out)

	// Assert
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if p.Subject != "" {
		t.Errorf("Principal.Subject: got %q, want empty (Anonymous principal)", p.Subject)
	}
}

func TestServer_Handshake_AcceptsValidJWT(t *testing.T) {
	// Arrange
	pub, priv := mustEd25519Keypair(t)
	issued := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	claims := auth.Claims{
		Subject: "alice",
		Account: "default",
		Scopes:  []string{"produce:events"},
		Expires: issued.Add(time.Hour).Unix(),
	}
	token, err := auth.IssueJWT(priv, claims)
	if err != nil {
		t.Fatalf("IssueJWT: %v", err)
	}

	s := New(nil)
	s.SetVerifier(auth.NewEd25519Verifier(pub,
		auth.WithClock(func() time.Time { return issued.Add(time.Minute) }),
	))

	in, out := newHandshakeFrame(t, proto.HandshakeRequest{
		Version:        proto.WireVersion,
		CredentialKind: proto.CredentialJWT,
		Credential:     token,
	})

	// Act
	p, err := s.handshake(in, out)

	// Assert
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if p.Subject != "alice" {
		t.Errorf("Principal.Subject: got %q, want %q (= Principal.Subject)", p.Subject, "alice")
	}
}

func TestServer_Handshake_RejectsForgedJWT(t *testing.T) {
	// Arrange: token signed by a key the broker does not trust.
	trustedPub, _ := mustEd25519Keypair(t)
	_, attackerPriv := mustEd25519Keypair(t)
	issued := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	token, err := auth.IssueJWT(attackerPriv, auth.Claims{
		Subject: "attacker",
		Expires: issued.Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("IssueJWT: %v", err)
	}

	s := New(nil)
	s.SetVerifier(auth.NewEd25519Verifier(trustedPub,
		auth.WithClock(func() time.Time { return issued.Add(time.Minute) }),
	))

	in, out := newHandshakeFrame(t, proto.HandshakeRequest{
		Version:        proto.WireVersion,
		CredentialKind: proto.CredentialJWT,
		Credential:     token,
	})

	// Act
	_, err = s.handshake(in, out)

	// Assert
	if err == nil {
		t.Fatal("expected error for token signed by untrusted key")
	}
}

func TestServer_Handshake_RejectsAnonymousWhenVerifierConfigured(t *testing.T) {
	// Arrange: any non-anonymous verifier rejects None credentials.
	pub, _ := mustEd25519Keypair(t)
	s := New(nil)
	s.SetVerifier(auth.NewEd25519Verifier(pub))

	in, out := newHandshakeFrame(t, proto.HandshakeRequest{
		Version: proto.WireVersion,
	})

	// Act
	_, err := s.handshake(in, out)

	// Assert
	if err == nil {
		t.Fatal("expected error for anonymous handshake against Ed25519 verifier")
	}
}

func TestServer_SetAPIKeys_RoutesThroughAPIKeyVerifier(t *testing.T) {
	// Arrange: legacy WithAPIKeys plumbing must still admit valid
	// bearer-token handshakes and reject invalid ones, because the
	// internal path now constructs an APIKeyVerifier rather than the
	// old apiKeys map.
	s := New(nil)
	s.SetAPIKeys([]string{"alpha", "bravo"})

	// Act / Assert: valid key admits, Subject == the matched key.
	in, out := newHandshakeFrame(t, proto.HandshakeRequest{
		Version:        proto.WireVersion,
		CredentialKind: proto.CredentialAPIKey,
		Credential:     []byte("alpha"),
	})
	p, err := s.handshake(in, out)
	if err != nil {
		t.Fatalf("handshake(valid key): %v", err)
	}
	if p.Subject != "alpha" {
		t.Errorf("Principal.Subject: got %q, want %q", p.Subject, "alpha")
	}

	// Act / Assert: invalid key rejected.
	in, out = newHandshakeFrame(t, proto.HandshakeRequest{
		Version:        proto.WireVersion,
		CredentialKind: proto.CredentialAPIKey,
		Credential:     []byte("charlie"),
	})
	if _, err := s.handshake(in, out); err == nil {
		t.Fatal("expected error for unknown API key")
	}
}

func TestServer_SetAPIKeys_EmptyClearsVerifier(t *testing.T) {
	// Arrange: configure then clear.
	s := New(nil)
	s.SetAPIKeys([]string{"key1"})
	s.SetAPIKeys(nil)

	// Act / Assert: anonymous handshake admits — verifier was cleared
	// back to AnonymousVerifier.
	in, out := newHandshakeFrame(t, proto.HandshakeRequest{
		Version: proto.WireVersion,
	})
	if _, err := s.handshake(in, out); err != nil {
		t.Errorf("handshake should admit anonymous after SetAPIKeys(nil): %v", err)
	}
}

// newHandshakeFrame encodes a handshake request into a wire frame and
// returns reader / writer buffers ready for s.handshake().
func newHandshakeFrame(t *testing.T, hs proto.HandshakeRequest) (in *bytes.Buffer, out *bytes.Buffer) {
	t.Helper()
	in = &bytes.Buffer{}
	if err := proto.WriteFrame(in, proto.OpHandshake, hs.Encode()); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	return in, &bytes.Buffer{}
}

func mustEd25519Keypair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	return pub, priv
}
