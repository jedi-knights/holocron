package auth_test

import (
	"errors"
	"testing"

	"github.com/jedi-knights/holocron/broker/internal/auth"
)

func TestAllowAllAuthorizer_AlwaysAllows(t *testing.T) {
	// Arrange
	az := auth.AllowAllAuthorizer{}

	// Act / Assert
	for _, action := range []auth.Action{auth.ActionProduce, auth.ActionConsume, auth.ActionAdmin} {
		if err := az.Authorize(auth.Anonymous, action, "events"); err != nil {
			t.Errorf("Authorize(%v) on AllowAll must accept anonymous: %v", action, err)
		}
		if err := az.Authorize(auth.Principal{Subject: "alice"}, action, "events"); err != nil {
			t.Errorf("Authorize(%v) on AllowAll must accept named principal: %v", action, err)
		}
	}
}

func TestScopeAuthorizer_DeniesAnonymousPrincipal(t *testing.T) {
	// An authorizer is only constructed when auth is configured;
	// in that mode an anonymous (empty Subject) principal must be
	// denied even though the verifier never produces one.
	az := auth.ScopeAuthorizer{}
	err := az.Authorize(auth.Anonymous, auth.ActionProduce, "events")
	if err == nil {
		t.Fatal("ScopeAuthorizer must deny anonymous principal")
	}
	if !errors.Is(err, auth.ErrUnauthorized) {
		t.Errorf("expected ErrUnauthorized, got %v", err)
	}
}

func TestScopeAuthorizer_DeniesEmptyScopes(t *testing.T) {
	az := auth.ScopeAuthorizer{}
	p := auth.Principal{Subject: "alice"} // no scopes
	if err := az.Authorize(p, auth.ActionProduce, "events"); err == nil {
		t.Fatal("ScopeAuthorizer must deny principal with empty scopes")
	}
}

func TestScopeAuthorizer_AllowsExactMatch(t *testing.T) {
	az := auth.ScopeAuthorizer{}
	p := auth.Principal{Subject: "alice", Scopes: []string{"produce:events"}}
	if err := az.Authorize(p, auth.ActionProduce, "events"); err != nil {
		t.Errorf("expected allow for produce:events on events, got %v", err)
	}
}

func TestScopeAuthorizer_AllowsPrefixWildcard(t *testing.T) {
	az := auth.ScopeAuthorizer{}
	p := auth.Principal{Subject: "alice", Scopes: []string{"produce:orders.*"}}
	if err := az.Authorize(p, auth.ActionProduce, "orders.placed"); err != nil {
		t.Errorf("prefix wildcard must allow orders.placed: %v", err)
	}
	if err := az.Authorize(p, auth.ActionProduce, "events"); err == nil {
		t.Error("prefix wildcard must deny non-matching topic")
	}
}

func TestScopeAuthorizer_AllowsBareWildcard(t *testing.T) {
	az := auth.ScopeAuthorizer{}
	p := auth.Principal{Subject: "alice", Scopes: []string{"produce:*"}}
	if err := az.Authorize(p, auth.ActionProduce, "anything"); err != nil {
		t.Errorf("bare wildcard must allow any topic: %v", err)
	}
	if err := az.Authorize(p, auth.ActionConsume, "anything"); err == nil {
		t.Error("bare wildcard must NOT cross verbs")
	}
}

func TestScopeAuthorizer_RejectsCrossVerb(t *testing.T) {
	az := auth.ScopeAuthorizer{}
	p := auth.Principal{Subject: "alice", Scopes: []string{"produce:events"}}
	if err := az.Authorize(p, auth.ActionConsume, "events"); err == nil {
		t.Fatal("produce:events must NOT authorize consume on events")
	}
}

func TestScopeAuthorizer_AdminScopes(t *testing.T) {
	az := auth.ScopeAuthorizer{}

	// Bare "admin" matches cluster ops (empty resource) only
	bareAdmin := auth.Principal{Subject: "ops", Scopes: []string{"admin"}}
	if err := az.Authorize(bareAdmin, auth.ActionAdmin, ""); err != nil {
		t.Errorf("bare admin must allow cluster op (empty resource): %v", err)
	}
	if err := az.Authorize(bareAdmin, auth.ActionAdmin, "events"); err == nil {
		t.Error("bare admin must NOT allow per-topic admin")
	}

	// admin:topic matches that topic only
	topicAdmin := auth.Principal{Subject: "ops", Scopes: []string{"admin:events"}}
	if err := az.Authorize(topicAdmin, auth.ActionAdmin, "events"); err != nil {
		t.Errorf("admin:events must allow admin on events: %v", err)
	}
	if err := az.Authorize(topicAdmin, auth.ActionAdmin, "orders"); err == nil {
		t.Error("admin:events must NOT allow admin on orders")
	}

	// admin:* matches any topic AND empty resource
	wildcardAdmin := auth.Principal{Subject: "ops", Scopes: []string{"admin:*"}}
	if err := az.Authorize(wildcardAdmin, auth.ActionAdmin, "anything"); err != nil {
		t.Errorf("admin:* must allow any topic: %v", err)
	}
	if err := az.Authorize(wildcardAdmin, auth.ActionAdmin, ""); err != nil {
		t.Errorf("admin:* must also allow cluster ops (empty resource): %v", err)
	}
}

func TestScopeAuthorizer_MultipleScopes(t *testing.T) {
	az := auth.ScopeAuthorizer{}
	p := auth.Principal{
		Subject: "billing-svc",
		Scopes: []string{
			"produce:events",
			"consume:orders.*",
			"admin:billing",
		},
	}

	// Each scope works independently
	if err := az.Authorize(p, auth.ActionProduce, "events"); err != nil {
		t.Errorf("produce:events should authorize: %v", err)
	}
	if err := az.Authorize(p, auth.ActionConsume, "orders.placed"); err != nil {
		t.Errorf("consume:orders.* should authorize orders.placed: %v", err)
	}
	if err := az.Authorize(p, auth.ActionAdmin, "billing"); err != nil {
		t.Errorf("admin:billing should authorize: %v", err)
	}

	// Cross-action across scopes: produce:events does not grant consume:events
	if err := az.Authorize(p, auth.ActionConsume, "events"); err == nil {
		t.Error("scopes must not cross-grant — consume:events should be denied")
	}
}

func TestScopeAuthorizer_IgnoresMalformedScopes(t *testing.T) {
	// A malformed scope in the JWT should not crash the authorizer and
	// should not silently grant — it's skipped during evaluation. A
	// principal whose only scopes are malformed gets denied by the
	// "no matching scope" branch.
	az := auth.ScopeAuthorizer{}
	p := auth.Principal{Subject: "alice", Scopes: []string{"garbage::input", "produce:events"}}
	if err := az.Authorize(p, auth.ActionProduce, "events"); err != nil {
		t.Errorf("good scope alongside garbage should still authorize: %v", err)
	}
	bad := auth.Principal{Subject: "bob", Scopes: []string{"garbage::input"}}
	if err := az.Authorize(bad, auth.ActionProduce, "events"); err == nil {
		t.Error("only-malformed scopes must deny")
	}
}

func TestActionConstants(t *testing.T) {
	// The wire-stable string values matter — JWT scopes carry these
	// names verbatim. A rename would silently break every existing
	// token.
	if string(auth.ActionProduce) != "produce" {
		t.Errorf("ActionProduce: got %q, want produce", auth.ActionProduce)
	}
	if string(auth.ActionConsume) != "consume" {
		t.Errorf("ActionConsume: got %q, want consume", auth.ActionConsume)
	}
	if string(auth.ActionAdmin) != "admin" {
		t.Errorf("ActionAdmin: got %q, want admin", auth.ActionAdmin)
	}
}
