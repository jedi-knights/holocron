package auth

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// TokenVerifier turns a wire Credential into a Principal, or returns
// an error if the credential is unacceptable (bad signature, expired,
// denied, unknown kind).
//
// Verify is synchronous and in-memory in v1; the interface deliberately
// omits a context.Context parameter. When a future verifier implementation
// needs I/O (for example, an OIDC verifier fetching a JWKS), the
// interface gains a context-bearing method at that point — adding one
// now would be speculative.
//
// Server-layer wiring rule: when auth is disabled, wire
// AnonymousVerifier; when auth is enabled, wire Ed25519Verifier (or a
// future verifier). No caller should branch on CredentialNone after
// Verify returns — the verifier choice is the policy decision.
type TokenVerifier interface {
	Verify(cred Credential) (Principal, error)
}

// AnonymousVerifier accepts any credential and always returns the
// Anonymous Principal. Used when the broker is configured without
// auth, so the server's enforcement code path is identical regardless
// of whether auth is enabled.
type AnonymousVerifier struct{}

// Verify returns Anonymous, ignoring the supplied credential.
func (AnonymousVerifier) Verify(_ Credential) (Principal, error) {
	return Anonymous, nil
}

// Ed25519Verifier verifies JWTs signed by an operator-held Ed25519
// public key. CredentialNone is rejected — that's the
// AnonymousVerifier's job; any verifier configured with a key has
// opted into auth-required mode.
type Ed25519Verifier struct {
	pub      ed25519.PublicKey
	clock    func() time.Time
	leeway   time.Duration
	denylist DenyList
}

// Ed25519VerifierOption configures NewEd25519Verifier.
type Ed25519VerifierOption func(*Ed25519Verifier)

// WithClock injects a time source. Defaults to time.Now. Tests use
// this to drive expiry / not-before checks deterministically.
func WithClock(fn func() time.Time) Ed25519VerifierOption {
	return func(v *Ed25519Verifier) { v.clock = fn }
}

// WithLeeway sets the symmetric clock-skew window applied to expiry
// and not-before checks. Defaults to 30 seconds — small enough that
// a leaked token's blast radius stays bounded, large enough to
// absorb routine clock drift between issuer and verifier.
func WithLeeway(d time.Duration) Ed25519VerifierOption {
	return func(v *Ed25519Verifier) { v.leeway = d }
}

// WithDenyList attaches a subject denylist. Defaults to nil (no
// denial). The verifier consults the list on every Verify, so the
// daemon's SIGHUP-reload path can swap entries with no verifier
// rebuild.
func WithDenyList(d DenyList) Ed25519VerifierOption {
	return func(v *Ed25519Verifier) { v.denylist = d }
}

// NewEd25519Verifier returns a verifier that accepts JWTs signed by
// pub. At least one — and exactly one — key is supported in v1; multi-
// key trust (for rotation rollover) is a planned follow-on.
func NewEd25519Verifier(pub ed25519.PublicKey, opts ...Ed25519VerifierOption) *Ed25519Verifier {
	v := &Ed25519Verifier{
		pub:    pub,
		clock:  time.Now,
		leeway: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(v)
	}
	return v
}

// Verify implements TokenVerifier.
func (v *Ed25519Verifier) Verify(cred Credential) (Principal, error) {
	switch cred.Kind {
	case CredentialJWT:
		// fall through
	case CredentialNone:
		return Principal{}, errors.New("auth: anonymous credential rejected (auth-required mode)")
	case CredentialAPIKey:
		return Principal{}, errors.New("auth: API-key credentials are not supported; use a JWT")
	default:
		return Principal{}, fmt.Errorf("auth: unsupported credential kind %d", cred.Kind)
	}

	claims, err := parseAndVerifyJWT(cred.Bytes, v.pub)
	if err != nil {
		return Principal{}, err
	}

	now := v.clock()
	if now.Add(-v.leeway).Unix() > claims.Expires {
		return Principal{}, errors.New("auth: token expired")
	}
	if claims.NotBefore > 0 && now.Add(v.leeway).Unix() < claims.NotBefore {
		return Principal{}, errors.New("auth: token not yet valid")
	}
	if v.denylist != nil && v.denylist.Contains(claims.Subject) {
		return Principal{}, fmt.Errorf("auth: subject %q is denied", claims.Subject)
	}

	return Principal{
		Subject: claims.Subject,
		Account: claims.Account,
		Scopes:  claims.Scopes,
		Source:  SourceJWT,
	}, nil
}

// IssueJWT signs a JWT containing the supplied claims with priv. Used
// by the holocronctl auth issue subcommand (PR 6) and by tests; the
// broker itself never calls it.
func IssueJWT(priv ed25519.PrivateKey, claims Claims) ([]byte, error) {
	header := jwtHeader{Alg: "EdDSA", Typ: "JWT"}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("auth: marshal header: %w", err)
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return nil, fmt.Errorf("auth: marshal claims: %w", err)
	}

	enc := base64.RawURLEncoding
	signingInput := make([]byte, 0, enc.EncodedLen(len(headerJSON))+1+enc.EncodedLen(len(claimsJSON)))
	signingInput = enc.AppendEncode(signingInput, headerJSON)
	signingInput = append(signingInput, '.')
	signingInput = enc.AppendEncode(signingInput, claimsJSON)

	sig := ed25519.Sign(priv, signingInput)

	out := make([]byte, 0, len(signingInput)+1+enc.EncodedLen(len(sig)))
	out = append(out, signingInput...)
	out = append(out, '.')
	out = enc.AppendEncode(out, sig)
	return out, nil
}

// jwtHeader is the JOSE header Holocron writes. Only EdDSA is
// supported — verifiers reject any other alg.
type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// maxTokenBytes bounds the size of an inbound JWT before any parsing
// work. Any well-formed Holocron JWT (EdDSA signature + small claim
// shape) fits in well under 1 KiB; 8 KiB is generous and still keeps
// adversarial allocations bounded.
const maxTokenBytes = 8192

// parseAndVerifyJWT splits the token, verifies the signature against
// pub, decodes the claims, and rejects tokens missing required
// structural claims (currently: exp). It does not perform expiry,
// not-before, or denylist checks — those are policy decisions the
// caller applies against the returned Claims.
func parseAndVerifyJWT(token []byte, pub ed25519.PublicKey) (Claims, error) {
	if len(token) > maxTokenBytes {
		return Claims{}, fmt.Errorf("auth: JWT exceeds maximum size (%d > %d bytes)", len(token), maxTokenBytes)
	}
	parts := bytes.Split(token, []byte("."))
	if len(parts) != 3 {
		return Claims{}, fmt.Errorf("auth: malformed JWT (expected 3 segments, got %d)", len(parts))
	}
	headerBytes, payloadBytes, sigSegment := parts[0], parts[1], parts[2]

	enc := base64.RawURLEncoding
	headerJSON, err := enc.DecodeString(string(headerBytes))
	if err != nil {
		return Claims{}, fmt.Errorf("auth: decode header: %w", err)
	}
	var header jwtHeader
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return Claims{}, fmt.Errorf("auth: parse header: %w", err)
	}
	if header.Alg != "EdDSA" {
		return Claims{}, fmt.Errorf("auth: unsupported alg %q (only EdDSA accepted)", header.Alg)
	}

	sig, err := enc.DecodeString(string(sigSegment))
	if err != nil {
		return Claims{}, fmt.Errorf("auth: decode signature: %w", err)
	}
	signingInput := token[:len(headerBytes)+1+len(payloadBytes)]
	if !ed25519.Verify(pub, signingInput, sig) {
		return Claims{}, errors.New("auth: signature verification failed")
	}

	payloadJSON, err := enc.DecodeString(string(payloadBytes))
	if err != nil {
		return Claims{}, fmt.Errorf("auth: decode payload: %w", err)
	}
	var claims Claims
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return Claims{}, fmt.Errorf("auth: parse claims: %w", err)
	}
	if claims.Expires == 0 {
		return Claims{}, errors.New("auth: JWT missing required exp claim")
	}
	return claims, nil
}
