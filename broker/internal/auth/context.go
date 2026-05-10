package auth

import "context"

// principalKey is the unexported context.Value key under which a
// Principal travels. The unexported sentinel prevents accidental
// collisions with other ctx values.
type principalKey struct{}

// WithPrincipal returns ctx with p attached. Downstream code that
// participates in audit logging — most notably cluster.Apply — pulls
// the Principal back out via PrincipalFromContext.
//
// The Principal stays in the ctx for the rest of the call chain;
// nesting WithPrincipal calls overwrites the inherited value, which
// is the right behaviour for any code that re-issues a request on
// behalf of a different identity.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFromContext returns the Principal carried in ctx, or
// Anonymous when none is present. Audit-logging code is the typical
// consumer; never branch on the return value to enforce policy
// (verifier choice is the policy decision — see the TokenVerifier
// doc comment).
func PrincipalFromContext(ctx context.Context) Principal {
	p, ok := ctx.Value(principalKey{}).(Principal)
	if !ok {
		return Anonymous
	}
	return p
}
