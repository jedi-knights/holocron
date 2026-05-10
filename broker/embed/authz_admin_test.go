package embed_test

import (
	"context"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/internal/auth"
	"github.com/jedi-knights/holocron/proto"
)

// PR 3 of the ACL wave: admin operations (CreateTopic, DeleteTopic,
// UpdateTopicConfig) require admin:<topic> or admin:* scope. Cluster
// ops (AddVoter, RemoveVoter) require bare admin (no resource).

func TestEmbed_AdminAuthz_DeniesCreateTopicWithoutAdminScope(t *testing.T) {
	pub, priv := mustEd25519Keypair(t)
	token := mustIssueJWT(t, priv, auth.Claims{
		Subject: "alice",
		Scopes:  []string{"produce:events"}, // No admin scope
		Expires: time.Now().Add(time.Hour).Unix(),
	})
	addr := startVerifierBroker(t, pub)
	tr := mustDialWithJWT(t, addr, token)
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := tr.CreateTopic(ctx, "events", 1)
	if err == nil {
		t.Fatal("CreateTopic without admin scope must be denied")
	}
	if !proto.IsStatus(err, proto.StatusForbidden) {
		t.Errorf("expected StatusForbidden, got %v", err)
	}
}

func TestEmbed_AdminAuthz_AllowsCreateTopicWithMatchingScope(t *testing.T) {
	pub, priv := mustEd25519Keypair(t)
	token := mustIssueJWT(t, priv, auth.Claims{
		Subject: "ops",
		Scopes:  []string{"admin:events"},
		Expires: time.Now().Add(time.Hour).Unix(),
	})
	addr := startVerifierBroker(t, pub)
	tr := mustDialWithJWT(t, addr, token)
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tr.CreateTopic(ctx, "events", 1); err != nil {
		t.Fatalf("CreateTopic with admin:events scope should succeed: %v", err)
	}
}

func TestEmbed_AdminAuthz_AllowsAdminWildcard(t *testing.T) {
	pub, priv := mustEd25519Keypair(t)
	token := mustIssueJWT(t, priv, auth.Claims{
		Subject: "ops",
		Scopes:  []string{"admin:*"},
		Expires: time.Now().Add(time.Hour).Unix(),
	})
	addr := startVerifierBroker(t, pub)
	tr := mustDialWithJWT(t, addr, token)
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tr.CreateTopic(ctx, "anything", 1); err != nil {
		t.Errorf("admin:* should authorize any topic: %v", err)
	}
	// admin:* matches both topic and empty resource — also authorizes
	// cluster-level admin (AddVoter / RemoveVoter), exercised in a
	// dedicated test below.
}

func TestEmbed_AdminAuthz_DeniesDeleteTopicWithoutScope(t *testing.T) {
	pub, priv := mustEd25519Keypair(t)
	// Issue first with admin to set up the topic, then dial again
	// with a non-admin token to attempt delete.
	adminToken := mustIssueJWT(t, priv, auth.Claims{
		Subject: "ops",
		Scopes:  []string{"admin:*"},
		Expires: time.Now().Add(time.Hour).Unix(),
	})
	addr := startVerifierBroker(t, pub)
	{
		tr := mustDialWithJWT(t, addr, adminToken)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := tr.CreateTopic(ctx, "doomed", 1); err != nil {
			cancel()
			tr.Close()
			t.Fatal(err)
		}
		cancel()
		tr.Close()
	}

	consumerToken := mustIssueJWT(t, priv, auth.Claims{
		Subject: "alice",
		Scopes:  []string{"consume:doomed"},
		Expires: time.Now().Add(time.Hour).Unix(),
	})
	tr := mustDialWithJWT(t, addr, consumerToken)
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := tr.DeleteTopic(ctx, "doomed")
	if err == nil {
		t.Fatal("DeleteTopic without admin scope must be denied")
	}
	if !proto.IsStatus(err, proto.StatusForbidden) {
		t.Errorf("expected StatusForbidden, got %v", err)
	}
}

func TestEmbed_AdminAuthz_DeniesUpdateTopicConfigWithoutScope(t *testing.T) {
	pub, priv := mustEd25519Keypair(t)
	adminToken := mustIssueJWT(t, priv, auth.Claims{
		Subject: "ops",
		Scopes:  []string{"admin:*"},
		Expires: time.Now().Add(time.Hour).Unix(),
	})
	addr := startVerifierBroker(t, pub)
	{
		tr := mustDialWithJWT(t, addr, adminToken)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := tr.CreateTopic(ctx, "tunable", 1); err != nil {
			cancel()
			tr.Close()
			t.Fatal(err)
		}
		cancel()
		tr.Close()
	}

	noAdminToken := mustIssueJWT(t, priv, auth.Claims{
		Subject: "alice",
		Scopes:  []string{"produce:tunable", "consume:tunable"},
		Expires: time.Now().Add(time.Hour).Unix(),
	})
	tr := mustDialWithJWT(t, addr, noAdminToken)
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := tr.UpdateTopicConfig(ctx, "tunable", 60_000, 0)
	if err == nil {
		t.Fatal("UpdateTopicConfig without admin scope must be denied")
	}
	if !proto.IsStatus(err, proto.StatusForbidden) {
		t.Errorf("expected StatusForbidden, got %v", err)
	}
}

func TestEmbed_AdminAuthz_BareAdminDoesNotAuthorizeTopicAdmin(t *testing.T) {
	// The bare-verb scope "admin" matches only empty resource (cluster
	// ops). Per-topic admin requires admin:<topic> or admin:*.
	pub, priv := mustEd25519Keypair(t)
	token := mustIssueJWT(t, priv, auth.Claims{
		Subject: "ops",
		Scopes:  []string{"admin"}, // bare verb
		Expires: time.Now().Add(time.Hour).Unix(),
	})
	addr := startVerifierBroker(t, pub)
	tr := mustDialWithJWT(t, addr, token)
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := tr.CreateTopic(ctx, "events", 1)
	if err == nil {
		t.Fatal("bare admin must NOT authorize per-topic CreateTopic")
	}
	if !proto.IsStatus(err, proto.StatusForbidden) {
		t.Errorf("expected StatusForbidden, got %v", err)
	}
}
