package cluster

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/jedi-knights/holocron/broker/internal/auth"
)

// TestCluster_Apply_AuditLogsPrincipal proves PR 7's headline
// behaviour: the Principal carried in the ctx surfaces in the
// audit-log line emitted by every successful (or attempted) Apply.
// Multi-tenancy and per-account quotas attach to this seam — without
// it, audit trails can't connect a state change to the identity that
// requested it.
func TestCluster_Apply_AuditLogsPrincipal(t *testing.T) {
	// Arrange: capture audit lines into a buffer via an injected logger.
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	nodes := startClusterWithLogger(t, 1, logger)
	if err := nodes[0].cluster.WaitForLeader(2 * 1_000_000_000); err != nil { // 2s
		t.Fatal(err)
	}

	ctx := auth.WithPrincipal(context.Background(), auth.Principal{
		Subject: "alice",
		Account: "default",
		Source:  auth.SourceJWT,
	})

	// Act
	if _, err := nodes[0].cluster.Apply(ctx, EncodeCreateTopic(CreateTopicCommand{
		Name:           "events",
		PartitionCount: 1,
	})); err != nil {
		t.Fatal(err)
	}

	// Assert: the captured log carries the principal's subject, account,
	// and source so downstream tooling can index and query it.
	out := buf.String()
	for _, want := range []string{
		`"subject":"alice"`,
		`"account":"default"`,
		`"source":"jwt"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("audit log missing %q\n--- log ---\n%s", want, out)
		}
	}
}

func TestCluster_Apply_AuditLogsAnonymousByDefault(t *testing.T) {
	// Arrange: no Principal in ctx.
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	nodes := startClusterWithLogger(t, 1, logger)
	if err := nodes[0].cluster.WaitForLeader(2 * 1_000_000_000); err != nil {
		t.Fatal(err)
	}

	// Act
	if _, err := nodes[0].cluster.Apply(context.Background(), EncodeCreateTopic(CreateTopicCommand{
		Name:           "events",
		PartitionCount: 1,
	})); err != nil {
		t.Fatal(err)
	}

	// Assert: the audit line records an empty subject — anonymous —
	// so the trail is consistent regardless of auth configuration.
	if !strings.Contains(buf.String(), `"subject":""`) {
		t.Errorf("expected anonymous audit line, got: %s", buf.String())
	}
}
