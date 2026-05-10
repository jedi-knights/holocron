package auth_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/internal/auth"
)

func TestPrincipal_AnonymousIsZeroValue(t *testing.T) {
	// Arrange
	var p auth.Principal

	// Act / Assert
	if !p.IsAnonymous() {
		t.Errorf("zero Principal must satisfy IsAnonymous")
	}
	if !auth.Anonymous.IsAnonymous() {
		t.Errorf("auth.Anonymous must satisfy IsAnonymous")
	}
	if auth.Anonymous.Subject != "" {
		t.Errorf("auth.Anonymous.Subject must be empty, got %q", auth.Anonymous.Subject)
	}
}

func TestPrincipal_NonEmptySubject_IsNotAnonymous(t *testing.T) {
	// Arrange
	p := auth.Principal{Subject: "alice"}

	// Act / Assert
	if p.IsAnonymous() {
		t.Error("Principal with Subject must not be anonymous")
	}
}

func TestAnonymousVerifier_ReturnsAnonymousRegardlessOfCredential(t *testing.T) {
	// Arrange
	v := auth.AnonymousVerifier{}

	// Act / Assert
	p, err := v.Verify(auth.Credential{Kind: auth.CredentialNone})
	if err != nil {
		t.Fatalf("Verify(None): %v", err)
	}
	if !p.IsAnonymous() {
		t.Error("expected anonymous Principal for None credential")
	}

	// Even a JWT credential is ignored — anonymous is anonymous.
	p, err = v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: []byte("ignored")})
	if err != nil {
		t.Fatalf("Verify(JWT): %v", err)
	}
	if !p.IsAnonymous() {
		t.Error("AnonymousVerifier must ignore credential content")
	}
}

func TestEd25519Verifier_AcceptsValidJWT(t *testing.T) {
	// Arrange
	pub, priv := mustEd25519Keypair(t)
	issued := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	claims := auth.Claims{
		Subject:  "alice",
		Account:  "default",
		Scopes:   []string{"produce:events", "consume:events"},
		Issuer:   "test-operator",
		IssuedAt: issued.Unix(),
		Expires:  issued.Add(time.Hour).Unix(),
	}
	token, err := auth.IssueJWT(priv, claims)
	if err != nil {
		t.Fatalf("IssueJWT: %v", err)
	}
	v := auth.NewEd25519Verifier(pub, auth.WithClock(func() time.Time { return issued.Add(5 * time.Minute) }))

	// Act
	p, err := v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: token})

	// Assert
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if p.Subject != "alice" {
		t.Errorf("Subject: got %q, want %q", p.Subject, "alice")
	}
	if p.Account != "default" {
		t.Errorf("Account: got %q, want %q", p.Account, "default")
	}
	if len(p.Scopes) != 2 {
		t.Errorf("Scopes: got %d, want 2", len(p.Scopes))
	}
	if p.Source != auth.SourceJWT {
		t.Errorf("Source: got %q, want %q", p.Source, auth.SourceJWT)
	}
}

func TestEd25519Verifier_RejectsBadSignature(t *testing.T) {
	// Arrange
	pub, _ := mustEd25519Keypair(t)
	_, otherPriv := mustEd25519Keypair(t)
	issued := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	claims := auth.Claims{
		Subject:  "alice",
		IssuedAt: issued.Unix(),
		Expires:  issued.Add(time.Hour).Unix(),
	}
	token, err := auth.IssueJWT(otherPriv, claims)
	if err != nil {
		t.Fatalf("IssueJWT: %v", err)
	}
	v := auth.NewEd25519Verifier(pub, auth.WithClock(func() time.Time { return issued.Add(5 * time.Minute) }))

	// Act
	_, err = v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: token})

	// Assert
	if err == nil {
		t.Fatal("expected error for token signed by wrong key, got nil")
	}
}

func TestEd25519Verifier_RejectsExpiredToken(t *testing.T) {
	// Arrange
	pub, priv := mustEd25519Keypair(t)
	issued := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	claims := auth.Claims{
		Subject:  "alice",
		IssuedAt: issued.Unix(),
		Expires:  issued.Add(time.Hour).Unix(),
	}
	token, _ := auth.IssueJWT(priv, claims)
	// Verify two hours later — well past expiry, outside any reasonable leeway.
	v := auth.NewEd25519Verifier(pub, auth.WithClock(func() time.Time { return issued.Add(2 * time.Hour) }))

	// Act
	_, err := v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: token})

	// Assert
	if err == nil {
		t.Fatal("expected error for expired token, got nil")
	}
}

func TestEd25519Verifier_RejectsNotYetValidToken(t *testing.T) {
	// Arrange
	pub, priv := mustEd25519Keypair(t)
	base := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	claims := auth.Claims{
		Subject:   "alice",
		IssuedAt:  base.Unix(),
		NotBefore: base.Add(time.Hour).Unix(),
		Expires:   base.Add(2 * time.Hour).Unix(),
	}
	token, _ := auth.IssueJWT(priv, claims)
	// Verify before NotBefore, outside leeway.
	v := auth.NewEd25519Verifier(pub, auth.WithClock(func() time.Time { return base.Add(30 * time.Minute) }))

	// Act
	_, err := v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: token})

	// Assert
	if err == nil {
		t.Fatal("expected error for not-yet-valid token, got nil")
	}
}

func TestEd25519Verifier_HonorsExpiryLeeway(t *testing.T) {
	// Arrange
	pub, priv := mustEd25519Keypair(t)
	issued := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	claims := auth.Claims{
		Subject:  "alice",
		IssuedAt: issued.Unix(),
		Expires:  issued.Add(time.Hour).Unix(),
	}
	token, _ := auth.IssueJWT(priv, claims)
	// Verify 10 seconds after expiry with 30s leeway — still valid.
	v := auth.NewEd25519Verifier(pub,
		auth.WithClock(func() time.Time { return issued.Add(time.Hour + 10*time.Second) }),
		auth.WithLeeway(30*time.Second),
	)

	// Act
	_, err := v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: token})

	// Assert
	if err != nil {
		t.Errorf("expected leeway window to accept token, got: %v", err)
	}
}

func TestEd25519Verifier_RejectsCredentialKindNone(t *testing.T) {
	// Arrange
	pub, _ := mustEd25519Keypair(t)
	v := auth.NewEd25519Verifier(pub)

	// Act
	_, err := v.Verify(auth.Credential{Kind: auth.CredentialNone})

	// Assert
	if err == nil {
		t.Fatal("Ed25519Verifier must reject CredentialNone (anonymous is the AnonymousVerifier's job)")
	}
}

func TestEd25519Verifier_RejectsUnknownCredentialKind(t *testing.T) {
	// Arrange
	pub, _ := mustEd25519Keypair(t)
	v := auth.NewEd25519Verifier(pub)

	// Act
	_, err := v.Verify(auth.Credential{Kind: 99, Bytes: []byte("payload")})

	// Assert
	if err == nil {
		t.Fatal("expected error for unknown CredentialKind")
	}
}

func TestEd25519Verifier_RejectsDeniedSubject(t *testing.T) {
	// Arrange
	pub, priv := mustEd25519Keypair(t)
	issued := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	claims := auth.Claims{
		Subject:  "compromised-svc",
		IssuedAt: issued.Unix(),
		Expires:  issued.Add(time.Hour).Unix(),
	}
	token, _ := auth.IssueJWT(priv, claims)
	deny := auth.NewMemoryDenyList("compromised-svc", "other-svc")
	v := auth.NewEd25519Verifier(pub,
		auth.WithClock(func() time.Time { return issued.Add(5 * time.Minute) }),
		auth.WithDenyList(deny),
	)

	// Act
	_, err := v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: token})

	// Assert
	if err == nil {
		t.Fatal("expected error for denied subject, got nil")
	}
}

func TestEd25519Verifier_RejectsTamperedPayload(t *testing.T) {
	// Arrange
	pub, priv := mustEd25519Keypair(t)
	issued := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	claims := auth.Claims{
		Subject:  "alice",
		IssuedAt: issued.Unix(),
		Expires:  issued.Add(time.Hour).Unix(),
	}
	token, _ := auth.IssueJWT(priv, claims)
	// Flip a byte in the middle of the token.
	tampered := bytes.Clone(token)
	tampered[len(tampered)/2] ^= 0x01
	v := auth.NewEd25519Verifier(pub, auth.WithClock(func() time.Time { return issued.Add(5 * time.Minute) }))

	// Act
	_, err := v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: tampered})

	// Assert
	if err == nil {
		t.Fatal("expected error for tampered token, got nil")
	}
}

func TestEd25519Verifier_RejectsMalformedJWT(t *testing.T) {
	// Arrange
	pub, _ := mustEd25519Keypair(t)
	v := auth.NewEd25519Verifier(pub)

	// Act / Assert
	for _, bad := range []string{"", "garbage", "header.payload", "a.b.c.d"} {
		if _, err := v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: []byte(bad)}); err == nil {
			t.Errorf("expected error for malformed JWT %q, got nil", bad)
		}
	}
}

func TestEd25519Verifier_RejectsAlgNoneAttack(t *testing.T) {
	// Arrange: hand-craft a JWT with `{"alg":"none"}` and an empty
	// signature segment — the canonical alg-none attack.
	pub, _ := mustEd25519Keypair(t)
	v := auth.NewEd25519Verifier(pub)
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"attacker","exp":9999999999}`))
	token := header + "." + payload + "."

	// Act
	_, err := v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: []byte(token)})

	// Assert
	if err == nil {
		t.Fatal("expected error for alg:none token, got nil")
	}
}

func TestEd25519Verifier_RejectsOversizedToken(t *testing.T) {
	// Arrange
	pub, _ := mustEd25519Keypair(t)
	v := auth.NewEd25519Verifier(pub)
	// 16 KiB of arbitrary bytes — well past the 8 KiB cap.
	huge := make([]byte, 16*1024)
	for i := range huge {
		huge[i] = 'A'
	}

	// Act
	_, err := v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: huge})

	// Assert
	if err == nil {
		t.Fatal("expected error for oversized token, got nil")
	}
}

func TestEd25519Verifier_RejectsTokenWithoutExp(t *testing.T) {
	// Arrange: hand-craft a properly-signed JWT that omits exp
	// entirely (json marshalling of Claims{Expires: 0} would emit
	// "exp":0 — we want absent, not zero).
	pub, priv := mustEd25519Keypair(t)
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"EdDSA","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"alice"}`))
	signingInput := header + "." + payload
	sig := ed25519.Sign(priv, []byte(signingInput))
	token := signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
	v := auth.NewEd25519Verifier(pub)

	// Act
	_, err := v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: []byte(token)})

	// Assert
	if err == nil {
		t.Fatal("expected error for token missing exp claim, got nil")
	}
}

func TestEd25519Verifier_RejectsAPIKeyCredentialKind(t *testing.T) {
	// Arrange
	pub, _ := mustEd25519Keypair(t)
	v := auth.NewEd25519Verifier(pub)

	// Act
	_, err := v.Verify(auth.Credential{Kind: auth.CredentialAPIKey, Bytes: []byte("legacy-key")})

	// Assert
	if err == nil {
		t.Fatal("Ed25519Verifier must reject CredentialAPIKey explicitly")
	}
}

func TestEd25519Verifier_HonorsNbfLeeway(t *testing.T) {
	// Arrange
	pub, priv := mustEd25519Keypair(t)
	base := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	claims := auth.Claims{
		Subject:   "alice",
		IssuedAt:  base.Unix(),
		NotBefore: base.Add(time.Minute).Unix(),
		Expires:   base.Add(time.Hour).Unix(),
	}
	token, _ := auth.IssueJWT(priv, claims)

	// Act / Assert: 10s before nbf with 30s leeway — accepted.
	v := auth.NewEd25519Verifier(pub,
		auth.WithClock(func() time.Time { return base.Add(50 * time.Second) }),
		auth.WithLeeway(30*time.Second),
	)
	if _, err := v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: token}); err != nil {
		t.Errorf("nbf leeway should accept token: %v", err)
	}

	// Act / Assert: 40s before nbf with 30s leeway — rejected.
	v = auth.NewEd25519Verifier(pub,
		auth.WithClock(func() time.Time { return base.Add(20 * time.Second) }),
		auth.WithLeeway(30*time.Second),
	)
	if _, err := v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: token}); err == nil {
		t.Error("nbf leeway should reject token outside window")
	}
}

func TestAPIKeyVerifier_AcceptsKnownKey(t *testing.T) {
	// Arrange
	v := auth.NewAPIKeyVerifier("alpha", "bravo")

	// Act
	p, err := v.Verify(auth.Credential{Kind: auth.CredentialAPIKey, Bytes: []byte("alpha")})

	// Assert
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if p.Subject != "alpha" {
		t.Errorf("Subject: got %q, want %q", p.Subject, "alpha")
	}
	if p.Source != auth.SourceAPIKey {
		t.Errorf("Source: got %q, want %q", p.Source, auth.SourceAPIKey)
	}
}

func TestAPIKeyVerifier_RejectsUnknownKey(t *testing.T) {
	// Arrange
	v := auth.NewAPIKeyVerifier("alpha")

	// Act
	_, err := v.Verify(auth.Credential{Kind: auth.CredentialAPIKey, Bytes: []byte("beta")})

	// Assert
	if err == nil {
		t.Fatal("expected error for unknown API key")
	}
}

func TestAPIKeyVerifier_RejectsAnonymous(t *testing.T) {
	// An API-key verifier is opt-in to auth-required mode; anonymous
	// handshake must be rejected.
	v := auth.NewAPIKeyVerifier("alpha")
	if _, err := v.Verify(auth.Credential{Kind: auth.CredentialNone}); err == nil {
		t.Fatal("expected error for anonymous credential")
	}
}

func TestAPIKeyVerifier_RejectsJWT(t *testing.T) {
	// APIKeyVerifier handles only API-key credentials; a JWT
	// credential is the wrong kind for this verifier.
	v := auth.NewAPIKeyVerifier("alpha")
	if _, err := v.Verify(auth.Credential{Kind: auth.CredentialJWT, Bytes: []byte("eyJ...")}); err == nil {
		t.Fatal("expected error for JWT credential against APIKeyVerifier")
	}
}

func TestMemoryDenyList_SetAtomicallyReplaces(t *testing.T) {
	// Arrange
	d := auth.NewMemoryDenyList("alice", "bob")

	// Act / Assert: initial population
	if !d.Contains("alice") {
		t.Error("alice should be denied")
	}
	if !d.Contains("bob") {
		t.Error("bob should be denied")
	}
	if d.Contains("carol") {
		t.Error("carol should not be denied")
	}

	// Act: replace with a different set
	d.Set([]string{"carol"})

	// Assert: replacement is atomic — old entries gone, new entries present
	if d.Contains("alice") {
		t.Error("alice should not be denied after Set")
	}
	if !d.Contains("carol") {
		t.Error("carol should be denied after Set")
	}
}

func mustEd25519Keypair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	return pub, priv
}
