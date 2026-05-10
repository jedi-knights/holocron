package auth

import (
	"fmt"
	"strings"
)

// Scope is a parsed `verb:resource[*]` authorization claim.
//
// The wire form (what appears in a JWT's holocron.scopes claim) is a
// short string with deliberate grammar:
//
//	produce:events       — produce on exactly the topic "events"
//	produce:orders.*     — produce on any topic with prefix "orders."
//	produce:*            — produce on any topic (including empty resource)
//	admin                — admin on the empty resource (cluster ops)
//	admin:billing        — admin on exactly the topic "billing"
//	admin:*              — admin on any resource (topics + cluster ops)
//
// No regex, no mid-string globs, no hierarchical implication. The
// grammar is small enough to fit on one screen of docs and big enough
// to express every realistic ACL.
type Scope struct {
	// Verb is the action this scope authorizes — typically one of
	// "produce", "consume", "admin". An unknown verb parses fine
	// but never matches a known Action.
	Verb string
	// Pattern is the literal resource (when Wildcard is false) or
	// the prefix to match against (when Wildcard is true). Empty
	// when Verb has no colon (bare-verb form) or when the scope is
	// "verb:*" (bare wildcard).
	Pattern string
	// Wildcard is true for scopes ending in "*". A bare wildcard
	// ("verb:*") has empty Pattern and matches any resource; a
	// prefix wildcard ("verb:foo.*") has Pattern "foo." and
	// matches any resource starting with that prefix.
	Wildcard bool
}

// ParseScope parses a single scope string. Returns an error for empty
// input, mid-string globs, more than one colon, the verb-as-wildcard
// form, or any internal whitespace.
func ParseScope(s string) (Scope, error) {
	if s == "" {
		return Scope{}, fmt.Errorf("auth: empty scope")
	}
	if strings.ContainsAny(s, " \t\n") {
		return Scope{}, fmt.Errorf("auth: scope %q contains whitespace", s)
	}
	verb, pattern, hasColon := strings.Cut(s, ":")
	if !hasColon {
		// Bare verb (e.g. "admin") — matches empty resource only.
		if strings.Contains(s, "*") {
			return Scope{}, fmt.Errorf("auth: scope %q: bare verbs cannot contain wildcards", s)
		}
		return Scope{Verb: s}, nil
	}
	if verb == "" {
		return Scope{}, fmt.Errorf("auth: scope %q: empty verb", s)
	}
	if strings.Contains(verb, "*") {
		return Scope{}, fmt.Errorf("auth: scope %q: verb cannot contain wildcards", s)
	}
	if pattern == "" {
		return Scope{}, fmt.Errorf("auth: scope %q: trailing colon (use bare verb instead)", s)
	}
	if strings.Contains(pattern, ":") {
		return Scope{}, fmt.Errorf("auth: scope %q: only one colon permitted", s)
	}
	if pattern == "*" {
		return Scope{Verb: verb, Wildcard: true}, nil
	}
	if strings.HasSuffix(pattern, "*") {
		body := pattern[:len(pattern)-1]
		if strings.Contains(body, "*") {
			return Scope{}, fmt.Errorf("auth: scope %q: only trailing wildcards permitted", s)
		}
		return Scope{Verb: verb, Pattern: body, Wildcard: true}, nil
	}
	if strings.Contains(pattern, "*") {
		return Scope{}, fmt.Errorf("auth: scope %q: only trailing wildcards permitted", s)
	}
	return Scope{Verb: verb, Pattern: pattern}, nil
}

// Matches reports whether this scope grants action on resource.
// Match logic:
//   - verb must equal action (no cross-verb implication)
//   - bare wildcard ("verb:*"): matches any resource including empty
//   - prefix wildcard ("verb:foo.*"): matches any resource starting
//     with the body
//   - exact (no wildcard): matches only when resource == pattern
//     (this is also how bare-verb scopes match — pattern is empty
//     and only an empty resource matches)
func (s Scope) Matches(action Action, resource string) bool {
	if s.Verb != string(action) {
		return false
	}
	if s.Wildcard {
		return strings.HasPrefix(resource, s.Pattern)
	}
	return resource == s.Pattern
}
