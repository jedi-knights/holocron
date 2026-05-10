package main

import (
	"crypto/ed25519"
	cryptorand "crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cliauth "github.com/jedi-knights/holocron/cli/internal/auth"
)

func TestRun_AuthRequiresSubcommand(t *testing.T) {
	if err := run([]string{"auth"}); err == nil {
		t.Fatal("expected error when `auth` is invoked without a subcommand")
	}
}

func TestRun_AuthUnknownSubcommand(t *testing.T) {
	if err := run([]string{"auth", "nope"}); err == nil {
		t.Fatal("expected error for unknown auth subcommand")
	}
}

func TestRun_AuthIssue_RequiresKey(t *testing.T) {
	if err := run([]string{"auth", "issue", "--subject", "alice"}); err == nil {
		t.Fatal("expected error when --key missing")
	}
}

func TestRun_AuthIssue_RequiresSubject(t *testing.T) {
	keyPath := writeEd25519PrivateKeyPEM(t)
	if err := run([]string{"auth", "issue", "--key", keyPath}); err == nil {
		t.Fatal("expected error when --subject missing")
	}
}

func TestRun_AuthIssue_BadKeyPath(t *testing.T) {
	bogus := filepath.Join(t.TempDir(), "missing.pem")
	if err := run([]string{"auth", "issue", "--key", bogus, "--subject", "alice"}); err == nil {
		t.Fatal("expected error for nonexistent key path")
	}
}

func TestRun_AuthIssue_NonPositiveTTL(t *testing.T) {
	keyPath := writeEd25519PrivateKeyPEM(t)
	if err := run([]string{"auth", "issue", "--key", keyPath, "--subject", "alice", "--ttl", "0"}); err == nil {
		t.Fatal("expected error for --ttl 0")
	}
}

func TestRun_AuthIssue_AllAccessExpandsToWildcardScopes(t *testing.T) {
	// --all-access is the dev/ops convenience: equivalent to writing
	// --scope produce:* --scope consume:* --scope admin:* by hand.
	// The issued token must carry exactly those three scopes so the
	// broker's ScopeAuthorizer admits any produce / consume / admin
	// action.
	keyPath := writeEd25519PrivateKeyPEM(t)
	out := filepath.Join(t.TempDir(), "all-access.jwt")
	err := run([]string{
		"auth", "issue",
		"--key", keyPath,
		"--subject", "dev-laptop",
		"--all-access",
		"--ttl", "1h",
		"--output", out,
	})
	if err != nil {
		t.Fatalf("auth issue --all-access: %v", err)
	}

	tokenBytes, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read token: %v", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	claims, _, err := cliauth.DecodeClaimsUnverified([]byte(token))
	if err != nil {
		t.Fatalf("DecodeClaimsUnverified: %v", err)
	}
	want := map[string]bool{"produce:*": false, "consume:*": false, "admin:*": false}
	for _, s := range claims.Scopes {
		if _, ok := want[s]; ok {
			want[s] = true
		}
	}
	for s, found := range want {
		if !found {
			t.Errorf("scope %q missing from --all-access token; got Scopes=%v", s, claims.Scopes)
		}
	}
	if len(claims.Scopes) != 3 {
		t.Errorf("Scopes: got %d, want 3 (produce:*, consume:*, admin:*)", len(claims.Scopes))
	}
}

func TestRun_AuthIssue_AllAccessRejectsExplicitScope(t *testing.T) {
	// --all-access and --scope are mutually exclusive — combining
	// them is almost always operator confusion. Surface the error
	// at parse time so the operator sees the contradiction.
	keyPath := writeEd25519PrivateKeyPEM(t)
	err := run([]string{
		"auth", "issue",
		"--key", keyPath,
		"--subject", "dev-laptop",
		"--all-access",
		"--scope", "produce:events",
	})
	if err == nil {
		t.Fatal("expected error when --all-access combined with --scope")
	}
	if !strings.Contains(err.Error(), "all-access") {
		t.Errorf("expected error to mention --all-access, got %q", err)
	}
}

func TestRun_AuthIssue_WritesToOutputFile(t *testing.T) {
	// Arrange
	keyPath := writeEd25519PrivateKeyPEM(t)
	out := filepath.Join(t.TempDir(), "token.jwt")

	// Act
	err := run([]string{
		"auth", "issue",
		"--key", keyPath,
		"--subject", "alice",
		"--account", "default",
		"--scope", "produce:events",
		"--scope", "consume:events",
		"--ttl", "1h",
		"--output", out,
	})
	if err != nil {
		t.Fatalf("auth issue: %v", err)
	}

	// Assert: file exists, contains a 3-segment JWT
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	token := strings.TrimSpace(string(data))
	if strings.Count(token, ".") != 2 {
		t.Errorf("output is not a JWT (expected 2 dots), got %q", token)
	}
}

func TestAuthIssue_TokenSignatureVerifiesWithPublicKey(t *testing.T) {
	// Generate a keypair, write the private key to disk, run
	// `auth issue`, then verify the signature with the matching
	// public key via stdlib. Confirms the issued token would be
	// accepted by any EdDSA verifier holding the same public key —
	// which is exactly what broker/internal/auth.Ed25519Verifier
	// does on the wire.

	// Arrange
	pub, priv, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	keyPath := writePrivateKey(t, priv)
	out := filepath.Join(t.TempDir(), "token.jwt")
	err = run([]string{
		"auth", "issue",
		"--key", keyPath,
		"--subject", "alice",
		"--ttl", "1h",
		"--output", out,
	})
	if err != nil {
		t.Fatalf("auth issue: %v", err)
	}
	tokenBytes, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read token: %v", err)
	}
	token := strings.TrimSpace(string(tokenBytes))

	// Act / Assert: split header.payload.signature, verify EdDSA sig
	// over header.payload bytes.
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d segments, want 3", len(parts))
	}
	signingInput := parts[0] + "." + parts[1]
	sig, err := decodeURL(parts[2])
	if err != nil {
		t.Fatalf("decode sig: %v", err)
	}
	if !ed25519.Verify(pub, []byte(signingInput), sig) {
		t.Fatal("signature did not verify with corresponding public key")
	}

	// Round-trip via CLI's own decoder, too.
	claims, _, err := cliauth.DecodeClaimsUnverified([]byte(token))
	if err != nil {
		t.Fatalf("DecodeClaimsUnverified: %v", err)
	}
	if claims.Subject != "alice" {
		t.Errorf("Subject: got %q, want alice", claims.Subject)
	}
}

func TestRun_AuthInspect_DecodesIssuedToken(t *testing.T) {
	// Arrange: issue a token to a temp file, then inspect it.
	keyPath := writeEd25519PrivateKeyPEM(t)
	out := filepath.Join(t.TempDir(), "token.jwt")
	if err := run([]string{
		"auth", "issue",
		"--key", keyPath,
		"--subject", "alice",
		"--scope", "produce:events",
		"--ttl", "1h",
		"--output", out,
	}); err != nil {
		t.Fatalf("auth issue: %v", err)
	}
	tokenBytes, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read token: %v", err)
	}
	token := strings.TrimSpace(string(tokenBytes))

	// Act: inspect via --token, --json so we can leave stdout output
	// shape uninspected here (it varies with format).
	if err := run([]string{"auth", "inspect", "--token", token, "--json"}); err != nil {
		t.Fatalf("auth inspect: %v", err)
	}
}

func TestRun_AuthInspect_RejectsMalformedToken(t *testing.T) {
	if err := run([]string{"auth", "inspect", "--token", "not-a-jwt"}); err == nil {
		t.Fatal("expected error for malformed token")
	}
}

// writeEd25519PrivateKeyPEM generates a fresh Ed25519 private key and
// writes it to a PKCS8 PEM file in t.TempDir(), returning the path.
func writeEd25519PrivateKeyPEM(t *testing.T) string {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return writePrivateKey(t, priv)
}

func writePrivateKey(t *testing.T, priv ed25519.PrivateKey) string {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}
	path := filepath.Join(t.TempDir(), "key.pem")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create key file: %v", err)
	}
	if err := pem.Encode(f, &pem.Block{Type: "PRIVATE KEY", Bytes: der}); err != nil {
		_ = f.Close()
		t.Fatalf("encode pem: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close key file: %v", err)
	}
	return path
}

// decodeURL wraps base64.RawURLEncoding.DecodeString — a one-liner
// kept named so the call site reads naturally.
func decodeURL(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}
