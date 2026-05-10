package sdk

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jedi-knights/holocron/proto"
)

// Credential identifies a client at handshake. The Kind tags the
// payload format; Bytes is opaque to the SDK — the broker's
// configured TokenVerifier (broker/internal/auth) interprets it.
//
// Use APIKeyCredential or JWTCredential for the common cases; the
// struct is exported so callers carrying credentials in their own
// types can convert without going through a constructor.
type Credential struct {
	Kind  CredentialKind
	Bytes []byte
}

// CredentialKind mirrors proto.CredentialKind. The alias keeps the
// SDK's constants in lockstep with the wire-stable numeric values.
type CredentialKind = proto.CredentialKind

// Re-exports of the wire-stable kind values so SDK callers don't
// need to import proto for the common case.
const (
	CredentialNone   = proto.CredentialNone
	CredentialAPIKey = proto.CredentialAPIKey
	CredentialJWT    = proto.CredentialJWT
)

// APIKeyCredential wraps a legacy bearer key as a Credential. Kept
// for the transition from the pre-v10 deployment shape — new
// deployments should issue JWTs via the operator's signing key and
// use JWTCredential / LoadCredentialFile.
func APIKeyCredential(key string) Credential {
	return Credential{Kind: CredentialAPIKey, Bytes: []byte(key)}
}

// JWTCredential wraps a JWT token (issued by the operator's signing
// key, typically by `holocronctl auth issue`) as a Credential.
func JWTCredential(token []byte) Credential {
	return Credential{Kind: CredentialJWT, Bytes: token}
}

// LoadCredentialFile reads a JWT from path and returns it as a
// CredentialJWT-tagged Credential. Trailing whitespace is trimmed so
// `holocronctl auth issue ... > /tmp/token` (which appends a newline)
// produces a usable credential.
//
// The function rejects clearly-invalid content — empty files and
// content without the three dot-separated JWT segments — so a
// misconfigured file surfaces here rather than as an obscure
// signature error at the broker.
func LoadCredentialFile(path string) (Credential, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Credential{}, fmt.Errorf("sdk: read credential file %q: %w", path, err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return Credential{}, fmt.Errorf("sdk: credential file %q is empty", path)
	}
	if strings.Count(token, ".") != 2 {
		return Credential{}, errors.New("sdk: credential file does not contain a JWT (expected three dot-separated segments)")
	}
	return JWTCredential([]byte(token)), nil
}
