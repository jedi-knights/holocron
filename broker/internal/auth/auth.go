// Package auth provides Holocron's authentication and authorization
// primitives.
//
// A Principal is the authenticated identity behind an inbound RPC. It
// is produced by a TokenVerifier from a wire Credential — typically a
// JWT signed by the operator's Ed25519 key, or (when mTLS-CN auth is
// configured) the cert's CN mapped to a subject. The zero value of
// Principal is the Anonymous identity, returned by the
// AnonymousVerifier when the broker is configured without auth.
//
// This package is the single place that decides Holocron's auth
// policy: how a Credential is parsed, how a JWT is verified, what a
// Principal looks like, and how the denylist behaves. Callers — the
// wire server, the cluster FSM, future per-account quota code — bring
// the resulting Principal through their decision points; they do not
// reinvent the verification logic.
//
// The Account field on Principal is carried from day one but
// inert until the multi-tenancy item on the Wave 1 roadmap lands.
// Every existing call site that touches identity will already plumb
// the field through — when enforcement turns on, no schema change is
// required.
package auth

// Principal is the authenticated identity behind an inbound RPC. The
// zero value (also exported as Anonymous) represents an
// unauthenticated caller. The struct is intentionally a value type:
// it is small, it is read-only on the hot path, and copying it
// through call chains is cheaper than tracking pointer aliasing.
type Principal struct {
	// Subject is the authenticated identity — typically the JWT
	// claim "sub" or, for mTLS auth, the Common Name on the client
	// cert resolved through the operator's CN-to-subject mapping.
	Subject string
	// Account is the tenant/namespace this Principal belongs to.
	// Empty for v1 anonymous and for any single-tenant deployment.
	// Carried so that multi-tenancy enforcement can be added without
	// a wire-format or FSM change.
	Account string
	// Scopes lists the actions this Principal is permitted to take.
	// The shape is "verb:resource" (for example, "produce:events"
	// or "consume:orders"). Authorization policy lives in
	// authorizer.go (added in PR 3); v1 verifies that scopes are
	// well-formed but does not yet enforce them.
	Scopes []string
	// Source records how the Principal was authenticated:
	// SourceJWT, SourceMTLS, or SourceAnonymous. Useful for audit
	// logging and for future policy that depends on auth strength.
	Source string
}

// IsAnonymous reports whether this Principal represents an
// unauthenticated caller. The check is by Subject only — an empty
// Subject is the anonymous marker.
func (p Principal) IsAnonymous() bool {
	return p.Subject == ""
}

// Anonymous is the Principal returned by AnonymousVerifier. It is
// always equal to the zero value of Principal.
var Anonymous = Principal{}

// Source identifies how a Principal was authenticated.
const (
	// SourceAnonymous is the source for the Anonymous Principal.
	SourceAnonymous = ""
	// SourceJWT indicates the Principal was derived from a JWT
	// presented at handshake.
	SourceJWT = "jwt"
	// SourceMTLS indicates the Principal was derived from a verified
	// TLS client certificate (CN-to-subject mapping configured via
	// the daemon's --auth-mtls-cn-mapping flag, landing in PR 4).
	SourceMTLS = "mtls"
	// SourceAPIKey indicates the Principal was derived from a legacy
	// opaque bearer key (CredentialAPIKey). Carried through wire v10
	// as a transition shape; deployments are expected to migrate to
	// SourceJWT.
	SourceAPIKey = "api-key"
)

// Credential is what a client presents at handshake to identify
// itself. Bytes is opaque to the wire layer; its format is
// determined by Kind.
type Credential struct {
	Kind  CredentialKind
	Bytes []byte
}

// CredentialKind tags the format of Credential.Bytes. New kinds may
// be added; verifiers must reject any Kind they do not recognise.
type CredentialKind uint8

const (
	// CredentialNone is the absence of a credential. AnonymousVerifier
	// accepts it; every other verifier rejects it.
	CredentialNone CredentialKind = 0
	// CredentialAPIKey is the legacy pre-v10 opaque bearer token.
	// Reserved for the wire-protocol bump in PR 2; this package
	// does not implement an API-key verifier (the model is
	// superseded by JWT).
	CredentialAPIKey CredentialKind = 1
	// CredentialJWT is a JWT signed with the operator's Ed25519
	// key. The standard kind for v1.
	CredentialJWT CredentialKind = 2
)

// Claims is the JWT payload Holocron issues and verifies. The
// shape is intentionally Holocron-native rather than NATS-compatible:
// teams migrating from JetStream rewrite their auth setup anyway,
// and a clean schema beats a half-borrowed one.
type Claims struct {
	// Subject is the JWT "sub" claim — the authenticated identity.
	Subject string `json:"sub"`
	// Account is the Holocron-native tenant claim. Carried but
	// inert in v1.
	Account string `json:"holocron.account,omitempty"`
	// Scopes lists the verb:resource pairs the holder may invoke.
	Scopes []string `json:"holocron.scopes,omitempty"`
	// Issuer is the JWT "iss" claim — the operator who signed.
	Issuer string `json:"iss,omitempty"`
	// IssuedAt is the JWT "iat" claim, seconds since epoch.
	IssuedAt int64 `json:"iat,omitempty"`
	// NotBefore is the JWT "nbf" claim, seconds since epoch.
	// Optional; when zero, the token is valid immediately on issue.
	NotBefore int64 `json:"nbf,omitempty"`
	// Expires is the JWT "exp" claim, seconds since epoch. Required;
	// a token without an expiry is rejected.
	Expires int64 `json:"exp"`
}
