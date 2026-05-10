// Package auth implements the JWT issuance and inspection used by
// `holocronctl auth issue` and `holocronctl auth inspect`.
//
// The JWT format and Claims shape mirror broker/internal/auth — the
// numeric claim values are wire-compatible. The two packages are
// duplicated rather than shared because broker/internal is private to
// the broker module; if a third surface needs the same code (a
// `holocron-issuer` daemon, a SaaS console), the cleanest extraction
// is a top-level holocron/auth module.
package auth

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
)

// Claims is the JWT payload Holocron issues. Field tags match the
// broker's parser exactly — drift here breaks the round-trip.
type Claims struct {
	Subject   string   `json:"sub"`
	Account   string   `json:"holocron.account,omitempty"`
	Scopes    []string `json:"holocron.scopes,omitempty"`
	Issuer    string   `json:"iss,omitempty"`
	IssuedAt  int64    `json:"iat,omitempty"`
	NotBefore int64    `json:"nbf,omitempty"`
	Expires   int64    `json:"exp"`
}

// Header is the JOSE header Holocron writes. EdDSA only — verifiers
// reject any other alg. Returned alongside Claims by
// DecodeClaimsUnverified so `holocronctl auth inspect` can show the
// algorithm a token claims to use.
type Header struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// IssueJWT signs a JWT containing claims with priv. Output is the
// three-segment header.payload.signature triple the broker's
// Ed25519Verifier accepts.
func IssueJWT(priv ed25519.PrivateKey, claims Claims) ([]byte, error) {
	header := Header{Alg: "EdDSA", Typ: "JWT"}
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

// DecodeClaimsUnverified splits the token, decodes the header and
// payload, and returns them. **It does not verify the signature** —
// `holocronctl auth inspect` shows the contents of any token regardless
// of issuer; verification is the broker's job.
func DecodeClaimsUnverified(token []byte) (Claims, Header, error) {
	parts := bytes.Split(token, []byte("."))
	if len(parts) != 3 {
		return Claims{}, Header{}, fmt.Errorf("auth: malformed JWT (expected 3 segments, got %d)", len(parts))
	}
	enc := base64.RawURLEncoding

	headerBytes, err := enc.DecodeString(string(parts[0]))
	if err != nil {
		return Claims{}, Header{}, fmt.Errorf("auth: decode header: %w", err)
	}
	var header Header
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return Claims{}, Header{}, fmt.Errorf("auth: parse header: %w", err)
	}

	payloadBytes, err := enc.DecodeString(string(parts[1]))
	if err != nil {
		return Claims{}, Header{}, fmt.Errorf("auth: decode payload: %w", err)
	}
	var claims Claims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return Claims{}, Header{}, fmt.Errorf("auth: parse claims: %w", err)
	}
	return claims, header, nil
}

// LoadEd25519PrivateKey parses a PEM-encoded PKCS8 Ed25519 private
// key from path. The format is what `openssl genpkey -algorithm
// Ed25519` produces. Any other algorithm (RSA, ECDSA) is rejected.
func LoadEd25519PrivateKey(path string) (ed25519.PrivateKey, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("auth: read key %q: %w", path, err)
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("auth: key %q: contains no PEM block", path)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("auth: key %q: parse PKCS8: %w", path, err)
	}
	priv, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("auth: key is not Ed25519")
	}
	return priv, nil
}
