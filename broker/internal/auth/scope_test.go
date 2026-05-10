package auth_test

import (
	"testing"

	"github.com/jedi-knights/holocron/broker/internal/auth"
)

func TestParseScope_VerbOnly(t *testing.T) {
	// Arrange / Act
	s, err := auth.ParseScope("admin")

	// Assert
	if err != nil {
		t.Fatalf("ParseScope: %v", err)
	}
	if s.Verb != "admin" {
		t.Errorf("Verb: got %q, want %q", s.Verb, "admin")
	}
	if s.Pattern != "" {
		t.Errorf("Pattern: got %q, want empty", s.Pattern)
	}
	if s.Wildcard {
		t.Error("Wildcard: got true, want false (bare verb)")
	}
}

func TestParseScope_VerbExactResource(t *testing.T) {
	s, err := auth.ParseScope("produce:events")
	if err != nil {
		t.Fatalf("ParseScope: %v", err)
	}
	if s.Verb != "produce" || s.Pattern != "events" || s.Wildcard {
		t.Errorf("got %+v, want {Verb:produce Pattern:events Wildcard:false}", s)
	}
}

func TestParseScope_VerbPrefixWildcard(t *testing.T) {
	s, err := auth.ParseScope("produce:orders.*")
	if err != nil {
		t.Fatalf("ParseScope: %v", err)
	}
	if s.Verb != "produce" || s.Pattern != "orders." || !s.Wildcard {
		t.Errorf("got %+v, want {Verb:produce Pattern:orders. Wildcard:true}", s)
	}
}

func TestParseScope_VerbBareWildcard(t *testing.T) {
	s, err := auth.ParseScope("produce:*")
	if err != nil {
		t.Fatalf("ParseScope: %v", err)
	}
	if s.Verb != "produce" || s.Pattern != "" || !s.Wildcard {
		t.Errorf("got %+v, want {Verb:produce Pattern: Wildcard:true}", s)
	}
}

func TestParseScope_Rejects(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"trailing colon", "produce:"},
		{"leading colon", ":events"},
		{"mid-string glob", "produce:foo*bar"},
		{"two colons", "produce:foo:bar"},
		{"whitespace", "produce events"},
		{"verb wildcard", "*:events"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := auth.ParseScope(c.input); err == nil {
				t.Errorf("expected ParseScope(%q) to fail", c.input)
			}
		})
	}
}

func TestScope_Matches(t *testing.T) {
	cases := []struct {
		name     string
		scope    string
		action   auth.Action
		resource string
		want     bool
	}{
		// Exact verb + resource
		{"exact match", "produce:events", auth.ActionProduce, "events", true},
		{"wrong verb", "produce:events", auth.ActionConsume, "events", false},
		{"wrong resource", "produce:events", auth.ActionProduce, "orders", false},

		// Prefix wildcard
		{"prefix matches", "produce:orders.*", auth.ActionProduce, "orders.placed", true},
		{"prefix matches deep", "produce:orders.*", auth.ActionProduce, "orders.placed.v2", true},
		{"prefix exact prefix without sep", "produce:orders.*", auth.ActionProduce, "orders.", true},
		{"prefix no match", "produce:orders.*", auth.ActionProduce, "events", false},

		// Bare wildcard
		{"bare wildcard matches anything", "produce:*", auth.ActionProduce, "anything", true},
		{"bare wildcard matches empty resource", "produce:*", auth.ActionProduce, "", true},
		{"bare wildcard wrong verb", "produce:*", auth.ActionConsume, "anything", false},

		// Bare verb (no colon) — for cluster ops with empty resource
		{"bare verb matches empty resource", "admin", auth.ActionAdmin, "", true},
		{"bare verb does not match topic", "admin", auth.ActionAdmin, "events", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := auth.ParseScope(c.scope)
			if err != nil {
				t.Fatalf("ParseScope(%q): %v", c.scope, err)
			}
			got := s.Matches(c.action, c.resource)
			if got != c.want {
				t.Errorf("Matches(%v, %q): got %v, want %v", c.action, c.resource, got, c.want)
			}
		})
	}
}
