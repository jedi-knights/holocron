package auth

import (
	"errors"
	"fmt"
)

// Action names a verb in the authorization grammar. The string values
// are wire-stable: JWT scopes carry these names verbatim
// (`produce:events`, `consume:orders`, `admin:billing`). A rename
// would silently break every existing token.
type Action string

const (
	// ActionProduce gates publishing records to a topic. Required by
	// every produce / produce-batch handler.
	ActionProduce Action = "produce"
	// ActionConsume gates fetch + subscribe + commit + join-group on
	// a topic. There is no separate "subscribe" or "commit" verb —
	// consume covers the read-side surface.
	ActionConsume Action = "consume"
	// ActionAdmin gates topic-management ops (CreateTopic,
	// DeleteTopic, UpdateTopicConfig) when paired with a topic
	// resource, and cluster-management ops (AddVoter, RemoveVoter)
	// when invoked with an empty resource.
	ActionAdmin Action = "admin"
)

// ErrUnauthorized is returned by Authorize when the principal lacks a
// matching scope. Use errors.Is at call sites that need to translate
// to wire StatusForbidden.
var ErrUnauthorized = errors.New("auth: unauthorized")

// Authorizer decides whether a Principal may perform an Action on a
// resource. Server handlers consult an Authorizer at every per-op
// boundary; the result is the deny/allow decision the broker enforces.
//
// Implementations must be safe for concurrent use — the broker calls
// Authorize from many goroutines.
type Authorizer interface {
	Authorize(p Principal, action Action, resource string) error
}

// AllowAllAuthorizer admits every Authorize call. Used when the
// broker is configured without auth — the AnonymousVerifier produces
// every Principal as anonymous, and the matching authorizer must let
// them through (otherwise the no-auth deployment shape breaks).
type AllowAllAuthorizer struct{}

// Authorize always returns nil.
func (AllowAllAuthorizer) Authorize(_ Principal, _ Action, _ string) error {
	return nil
}

// ScopeAuthorizer authorizes by walking the principal's Scopes, parsed
// as the verb:resource[*] grammar (see Scope). Used when auth is
// configured. Deny by default — empty Scopes (or only-malformed
// Scopes) yields ErrUnauthorized — and reject anonymous principals
// outright (a configured authorizer must never silently grant).
//
// The zero value is usable; the type carries no state beyond the
// scope-matching behavior.
type ScopeAuthorizer struct{}

// Authorize implements Authorizer. Returns nil when at least one of
// p.Scopes parses successfully and matches (action, resource);
// returns an ErrUnauthorized-wrapped error otherwise.
//
// Malformed scope strings are skipped silently — they don't grant,
// they don't crash. A principal whose only scopes are malformed is
// indistinguishable from one with empty scopes: denied.
func (ScopeAuthorizer) Authorize(p Principal, action Action, resource string) error {
	if p.IsAnonymous() {
		return fmt.Errorf("%w: anonymous principal", ErrUnauthorized)
	}
	for _, raw := range p.Scopes {
		s, err := ParseScope(raw)
		if err != nil {
			continue
		}
		if s.Matches(action, resource) {
			return nil
		}
	}
	return fmt.Errorf("%w: %s on %q not allowed for subject %q", ErrUnauthorized, action, resource, p.Subject)
}
