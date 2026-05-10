package auth_test

import (
	"context"
	"testing"

	"github.com/jedi-knights/holocron/broker/internal/auth"
)

func TestPrincipalFromContext_AbsentReturnsAnonymous(t *testing.T) {
	// Arrange / Act
	p := auth.PrincipalFromContext(context.Background())

	// Assert
	if !p.IsAnonymous() {
		t.Errorf("missing principal must surface as Anonymous, got %+v", p)
	}
}

func TestWithPrincipal_RoundTrip(t *testing.T) {
	// Arrange
	want := auth.Principal{
		Subject: "alice",
		Account: "default",
		Scopes:  []string{"produce:events"},
		Source:  auth.SourceJWT,
	}

	// Act
	ctx := auth.WithPrincipal(context.Background(), want)
	got := auth.PrincipalFromContext(ctx)

	// Assert
	if got.Subject != want.Subject {
		t.Errorf("Subject: got %q, want %q", got.Subject, want.Subject)
	}
	if got.Account != want.Account {
		t.Errorf("Account: got %q, want %q", got.Account, want.Account)
	}
	if len(got.Scopes) != 1 || got.Scopes[0] != "produce:events" {
		t.Errorf("Scopes: got %v, want [produce:events]", got.Scopes)
	}
	if got.Source != auth.SourceJWT {
		t.Errorf("Source: got %q, want %q", got.Source, auth.SourceJWT)
	}
}

func TestWithPrincipal_NestedOverwrites(t *testing.T) {
	// Arrange
	first := auth.Principal{Subject: "alice"}
	second := auth.Principal{Subject: "bob"}

	// Act
	ctx := auth.WithPrincipal(context.Background(), first)
	ctx = auth.WithPrincipal(ctx, second)

	// Assert
	got := auth.PrincipalFromContext(ctx)
	if got.Subject != "bob" {
		t.Errorf("nested principal must shadow parent: got %q, want bob", got.Subject)
	}
}
