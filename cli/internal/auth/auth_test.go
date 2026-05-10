package auth_test

import (
	"crypto/ed25519"
	cryptorand "crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/jedi-knights/holocron/cli/internal/auth"
)

func TestIssueJWT_ProducesParseableToken(t *testing.T) {
	// Arrange
	_, priv, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	claims := auth.Claims{
		Subject:  "alice",
		Account:  "default",
		Scopes:   []string{"produce:events"},
		Expires:  9_999_999_999,
		IssuedAt: 1_700_000_000,
	}

	// Act
	token, err := auth.IssueJWT(priv, claims)
	if err != nil {
		t.Fatalf("IssueJWT: %v", err)
	}

	// Assert: token has the three-segment JWT shape.
	if dots := bytesCount(token, '.'); dots != 2 {
		t.Errorf("token must have 2 dots (3 segments), got %d", dots)
	}

	// Decoding the issued token must round-trip the claims.
	got, _, err := auth.DecodeClaimsUnverified(token)
	if err != nil {
		t.Fatalf("DecodeClaimsUnverified: %v", err)
	}
	if got.Subject != claims.Subject {
		t.Errorf("Subject: got %q, want %q", got.Subject, claims.Subject)
	}
	if got.Account != claims.Account {
		t.Errorf("Account: got %q, want %q", got.Account, claims.Account)
	}
	if got.Expires != claims.Expires {
		t.Errorf("Expires: got %d, want %d", got.Expires, claims.Expires)
	}
}

func TestDecodeClaimsUnverified_RejectsMalformedToken(t *testing.T) {
	cases := []string{"", "garbage", "header.payload", "a.b.c.d"}
	for _, bad := range cases {
		if _, _, err := auth.DecodeClaimsUnverified([]byte(bad)); err == nil {
			t.Errorf("expected error for malformed token %q", bad)
		}
	}
}

func TestLoadEd25519PrivateKey_HappyPath(t *testing.T) {
	// Arrange: write a PKCS8 PEM to disk, then load it.
	_, priv, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}
	path := filepath.Join(t.TempDir(), "issuer-key.pem")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := pem.Encode(f, &pem.Block{Type: "PRIVATE KEY", Bytes: der}); err != nil {
		_ = f.Close()
		t.Fatalf("encode pem: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Act
	loaded, err := auth.LoadEd25519PrivateKey(path)
	if err != nil {
		t.Fatalf("LoadEd25519PrivateKey: %v", err)
	}

	// Assert: round-trip a signature with the loaded key.
	msg := []byte("hello")
	sig := ed25519.Sign(loaded, msg)
	if !ed25519.Verify(loaded.Public().(ed25519.PublicKey), msg, sig) {
		t.Error("loaded key did not produce a verifiable signature")
	}
}

func TestLoadEd25519PrivateKey_BadPath(t *testing.T) {
	if _, err := auth.LoadEd25519PrivateKey(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("expected error for nonexistent path")
	}
}

func TestLoadEd25519PrivateKey_NotPEM(t *testing.T) {
	path := filepath.Join(t.TempDir(), "junk.pem")
	if err := os.WriteFile(path, []byte("not pem"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := auth.LoadEd25519PrivateKey(path); err == nil {
		t.Fatal("expected error for non-PEM file")
	}
}

func bytesCount(b []byte, c byte) int {
	n := 0
	for _, x := range b {
		if x == c {
			n++
		}
	}
	return n
}
