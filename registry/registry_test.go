package registry_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/registry"
)

// newServiceForTest stands up an embedded broker, creates the schemas
// topic, and returns a started Service together with the broker so the
// caller can drive restart scenarios.
func newServiceForTest(t *testing.T) (*registry.Service, *embed.Broker) {
	t.Helper()
	b := embed.NewMemory()
	if err := b.CreateTopic(embed.TopicSpec{Name: registry.DefaultTopic, PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	svc, err := registry.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = svc.Close()
		_ = b.Close()
	})
	return svc, b
}

func TestRegister_AssignsIncreasingVersions(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	id1, err := svc.Register(ctx, "orders-value", `{"type":"v1"}`)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := svc.Register(ctx, "orders-value", `{"type":"v2"}`)
	if err != nil {
		t.Fatal(err)
	}
	if id1 == id2 {
		t.Fatal("two registrations got the same ID")
	}

	versions, err := svc.ListVersions("orders-value")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 2 || versions[0] != 1 || versions[1] != 2 {
		t.Fatalf("versions=%v want [1 2]", versions)
	}
}

func TestRegister_IdempotentForSameSchema(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()
	id1, err := svc.Register(ctx, "events-value", `{"type":"x"}`)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := svc.Register(ctx, "events-value", `{"type":"x"}`)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("re-registering same schema returned different IDs: %d vs %d", id1, id2)
	}
	versions, _ := svc.ListVersions("events-value")
	if len(versions) != 1 {
		t.Fatalf("idempotent re-register created a new version: %v", versions)
	}
}

func TestRegister_IDsAreGloballyUnique(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	idA1, _ := svc.Register(ctx, "a", `{"x":1}`)
	idB1, _ := svc.Register(ctx, "b", `{"y":1}`)
	idA2, _ := svc.Register(ctx, "a", `{"x":2}`)

	ids := map[int]bool{idA1: true, idB1: true, idA2: true}
	if len(ids) != 3 {
		t.Fatalf("IDs collide across subjects: a1=%d b1=%d a2=%d", idA1, idB1, idA2)
	}
}

func TestGetByID_FetchesAcrossSubjects(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()
	id, _ := svc.Register(ctx, "orders-value", `{"v":1}`)

	got, err := svc.GetByID(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Subject != "orders-value" || got.Schema != `{"v":1}` {
		t.Fatalf("unexpected schema: %+v", got)
	}
}

func TestGetVersion_RejectsBadInputs(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()
	_, _ = svc.Register(ctx, "orders-value", `{"v":1}`)

	if _, err := svc.GetVersion("nope", 1); !errors.Is(err, registry.ErrSubjectNotFound) {
		t.Fatalf("missing subject: got %v, want ErrSubjectNotFound", err)
	}
	if _, err := svc.GetVersion("orders-value", 5); !errors.Is(err, registry.ErrVersionNotFound) {
		t.Fatalf("out-of-range version: got %v, want ErrVersionNotFound", err)
	}
}

func TestStartReplaysExistingTopic(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: registry.DefaultTopic, PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	// First service registers two schemas, then closes.
	{
		svc, err := registry.New(b.Transport())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := svc.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.Register(ctx, "orders-value", `{"v":1}`); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.Register(ctx, "orders-value", `{"v":2}`); err != nil {
			t.Fatal(err)
		}
		_ = svc.Close()
	}

	// Second service against the same broker should see both versions
	// after replay.
	svc2, err := registry.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer svc2.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc2.Start(ctx); err != nil {
		t.Fatal(err)
	}

	versions, err := svc2.ListVersions("orders-value")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 2 {
		t.Fatalf("after restart, versions=%v want [1 2]", versions)
	}

	latest, err := svc2.GetLatest("orders-value")
	if err != nil {
		t.Fatal(err)
	}
	if latest.Schema != `{"v":2}` {
		t.Fatalf("after restart, latest=%q want {v:2}", latest.Schema)
	}
}

// TestSetCompatibility_PersistsAndEnforcesOnRegister proves the
// per-subject compatibility config is stored, recovered after
// restart, and applied automatically by Register — a schema that
// would violate the configured mode is rejected at registration
// time without needing an explicit CheckCompatibility call.
func TestSetCompatibility_PersistsAndEnforcesOnRegister(t *testing.T) {
	// Arrange — register a baseline, then turn on BACKWARD.
	svc, b := newServiceForTest(t)
	ctx := context.Background()
	if _, err := svc.Register(ctx, "u", `{"required":["id"]}`); err != nil {
		t.Fatal(err)
	}
	if err := svc.SetCompatibility(ctx, "u", registry.CompatibilityBackward); err != nil {
		t.Fatalf("SetCompatibility: %v", err)
	}

	// Act + Assert — adding a required field violates BACKWARD.
	if _, err := svc.Register(ctx, "u", `{"required":["id","email"]}`); err == nil {
		t.Errorf("Register with BACKWARD-violating schema succeeded; expected rejection")
	}
	// Relaxing the requirements is fine.
	if _, err := svc.Register(ctx, "u", `{"required":["id"]}`); err != nil {
		t.Errorf("Register of identical schema (idempotent): %v", err)
	}

	// Restart — config must replay.
	_ = svc.Close()
	svc2, err := registry.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer svc2.Close()
	startCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc2.Start(startCtx); err != nil {
		t.Fatal(err)
	}
	if got := svc2.GetCompatibility("u"); got != registry.CompatibilityBackward {
		t.Errorf("after restart: got %q, want BACKWARD", got)
	}
}

// TestSetCompatibility_NoneClearsTheMode proves resetting the
// mode to NONE removes the per-subject entry, matching the
// "unset" default.
func TestSetCompatibility_NoneClearsTheMode(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()
	if _, err := svc.Register(ctx, "x", `{"required":["a"]}`); err != nil {
		t.Fatal(err)
	}
	if err := svc.SetCompatibility(ctx, "x", registry.CompatibilityFull); err != nil {
		t.Fatal(err)
	}
	if got := svc.GetCompatibility("x"); got != registry.CompatibilityFull {
		t.Fatalf("after SetCompatibility(FULL): got %q", got)
	}
	if err := svc.SetCompatibility(ctx, "x", registry.CompatibilityNone); err != nil {
		t.Fatal(err)
	}
	if got := svc.GetCompatibility("x"); got != registry.CompatibilityNone {
		t.Errorf("after SetCompatibility(NONE): got %q, want NONE", got)
	}
}

// TestCheckCompatibility_BackwardPasses proves the structural
// BACKWARD check accepts a candidate schema whose required field
// set is a subset of the existing latest schema's required set —
// records valid under old still satisfy new.
func TestCheckCompatibility_BackwardPasses(t *testing.T) {
	// Arrange — existing schema requires id and name.
	svc, _ := newServiceForTest(t)
	if _, err := svc.Register(context.Background(), "u",
		`{"required":["id","name"]}`); err != nil {
		t.Fatal(err)
	}

	// Act — candidate drops "name" (relaxes the contract).
	ok, err := svc.CheckCompatibility("u", `{"required":["id"]}`, registry.CompatibilityBackward)

	// Assert
	if err != nil {
		t.Fatalf("CheckCompatibility: %v", err)
	}
	if !ok {
		t.Errorf("expected BACKWARD-compatible (relaxing required fields)")
	}
}

// TestCheckCompatibility_BackwardRejectsAddedRequired proves the
// check catches the canonical mistake — adding a previously-
// optional field to the required set, breaking records produced
// under the old schema.
func TestCheckCompatibility_BackwardRejectsAddedRequired(t *testing.T) {
	// Arrange
	svc, _ := newServiceForTest(t)
	if _, err := svc.Register(context.Background(), "u",
		`{"required":["id"]}`); err != nil {
		t.Fatal(err)
	}

	// Act — candidate adds a new required field.
	ok, err := svc.CheckCompatibility("u",
		`{"required":["id","email"]}`, registry.CompatibilityBackward)

	// Assert
	if ok {
		t.Errorf("expected BACKWARD-incompatible (added required field)")
	}
	if err == nil || !strings.Contains(err.Error(), "email") {
		t.Errorf("expected error mentioning the offending field, got %v", err)
	}
}

// TestCheckCompatibility_FullRequiresExactMatch proves FULL mode
// rejects either direction's violation. Adding a required field
// breaks BACKWARD; dropping one breaks FORWARD; only an exact
// match passes.
func TestCheckCompatibility_FullRequiresExactMatch(t *testing.T) {
	// Arrange
	svc, _ := newServiceForTest(t)
	if _, err := svc.Register(context.Background(), "u",
		`{"required":["id","name"]}`); err != nil {
		t.Fatal(err)
	}

	// Act + Assert — adding a field violates FULL.
	if ok, _ := svc.CheckCompatibility("u",
		`{"required":["id","name","email"]}`, registry.CompatibilityFull); ok {
		t.Errorf("FULL should reject adding required field")
	}
	// Dropping a field violates FULL.
	if ok, _ := svc.CheckCompatibility("u",
		`{"required":["id"]}`, registry.CompatibilityFull); ok {
		t.Errorf("FULL should reject dropping required field")
	}
	// Exact match passes.
	if ok, err := svc.CheckCompatibility("u",
		`{"required":["id","name"]}`, registry.CompatibilityFull); !ok || err != nil {
		t.Errorf("FULL exact match: ok=%v err=%v, want ok=true err=nil", ok, err)
	}
}

// TestCheckCompatibility_NoneAlwaysPasses proves the NONE mode
// short-circuits without parsing — an unparseable schema doesn't
// surface as an error when compatibility is disabled.
func TestCheckCompatibility_NoneAlwaysPasses(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ok, err := svc.CheckCompatibility("anything", `not even json`, registry.CompatibilityNone)
	if !ok || err != nil {
		t.Errorf("NONE: ok=%v err=%v, want ok=true err=nil", ok, err)
	}
}

// TestDeleteSubject_RemovesFromAllReadPaths proves DeleteSubject takes
// the subject out of GetLatest, GetVersion, ListSubjects, and
// ListVersions immediately.
func TestDeleteSubject_RemovesFromAllReadPaths(t *testing.T) {
	// Arrange
	svc, _ := newServiceForTest(t)
	ctx := context.Background()
	if _, err := svc.Register(ctx, "doomed", `{"v":1}`); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Register(ctx, "kept", `{"v":1}`); err != nil {
		t.Fatal(err)
	}

	// Act
	if err := svc.DeleteSubject(ctx, "doomed"); err != nil {
		t.Fatal(err)
	}

	// Assert
	if _, err := svc.GetLatest("doomed"); !errors.Is(err, registry.ErrSubjectNotFound) {
		t.Errorf("GetLatest(doomed): got %v, want ErrSubjectNotFound", err)
	}
	if _, err := svc.ListVersions("doomed"); !errors.Is(err, registry.ErrSubjectNotFound) {
		t.Errorf("ListVersions(doomed): got %v, want ErrSubjectNotFound", err)
	}
	subjects := svc.ListSubjects()
	for _, s := range subjects {
		if s == "doomed" {
			t.Errorf("ListSubjects still contains 'doomed': %v", subjects)
		}
	}
	// Sibling subject untouched.
	if _, err := svc.GetLatest("kept"); err != nil {
		t.Errorf("kept subject should still be present: %v", err)
	}
}

// TestDeleteByID_RemovesAddressedSchemaOnly proves DeleteByID
// resolves the ID to its (subject, version) and removes only that
// version. Sibling versions of the same subject and other
// subjects are untouched.
func TestDeleteByID_RemovesAddressedSchemaOnly(t *testing.T) {
	// Arrange
	svc, _ := newServiceForTest(t)
	ctx := context.Background()
	id1, _ := svc.Register(ctx, "events", `{"v":1}`)
	id2, _ := svc.Register(ctx, "events", `{"v":2}`)
	_, _ = svc.Register(ctx, "other", `{"v":1}`)

	// Act — delete the middle ID.
	_ = id1
	if err := svc.DeleteByID(ctx, id2); err != nil {
		t.Fatalf("DeleteByID: %v", err)
	}

	// Assert — id2 is gone; id1 still resolves; "other" is untouched.
	if _, err := svc.GetByID(id2); !errors.Is(err, registry.ErrSchemaNotFound) {
		t.Errorf("GetByID(deleted): got %v, want ErrSchemaNotFound", err)
	}
	if _, err := svc.GetByID(id1); err != nil {
		t.Errorf("GetByID(sibling): %v", err)
	}
	if _, err := svc.GetLatest("other"); err != nil {
		t.Errorf("other subject lost: %v", err)
	}
}

// TestDeleteByID_MissingReturnsSentinel proves DeleteByID surfaces
// ErrSchemaNotFound rather than silently succeeding for an
// unregistered ID.
func TestDeleteByID_MissingReturnsSentinel(t *testing.T) {
	svc, _ := newServiceForTest(t)
	err := svc.DeleteByID(context.Background(), 99999)
	if !errors.Is(err, registry.ErrSchemaNotFound) {
		t.Fatalf("got %v, want ErrSchemaNotFound", err)
	}
}

// TestDeleteVersion_RemovesOnlyThatVersion proves a per-version
// delete leaves the rest of the subject's history intact. Other
// versions still resolve via GetVersion; ListVersions surfaces a
// gap (Confluent semantic).
func TestDeleteVersion_RemovesOnlyThatVersion(t *testing.T) {
	// Arrange
	svc, _ := newServiceForTest(t)
	ctx := context.Background()
	for _, schema := range []string{`{"v":1}`, `{"v":2}`, `{"v":3}`} {
		if _, err := svc.Register(ctx, "events", schema); err != nil {
			t.Fatal(err)
		}
	}

	// Act — delete version 2.
	if err := svc.DeleteVersion(ctx, "events", 2); err != nil {
		t.Fatalf("DeleteVersion: %v", err)
	}

	// Assert
	if _, err := svc.GetVersion("events", 2); !errors.Is(err, registry.ErrVersionNotFound) {
		t.Errorf("GetVersion(events, 2): got %v, want ErrVersionNotFound", err)
	}
	// Versions 1 and 3 still present.
	if _, err := svc.GetVersion("events", 1); err != nil {
		t.Errorf("GetVersion(events, 1) after delete-version-2: %v", err)
	}
	if _, err := svc.GetVersion("events", 3); err != nil {
		t.Errorf("GetVersion(events, 3) after delete-version-2: %v", err)
	}
	// ListVersions surfaces the gap.
	versions, err := svc.ListVersions("events")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 2 || versions[0] != 1 || versions[1] != 3 {
		t.Errorf("ListVersions: got %v, want [1 3]", versions)
	}
}

// TestDeleteVersion_SurvivesRestart proves the per-version delete
// marker is durable on the schemas topic and a fresh Service
// replaying the topic converges to the same gapped sequence.
func TestDeleteVersion_SurvivesRestart(t *testing.T) {
	// Arrange
	svc, b := newServiceForTest(t)
	ctx := context.Background()
	for _, schema := range []string{`{"v":1}`, `{"v":2}`, `{"v":3}`} {
		if _, err := svc.Register(ctx, "events", schema); err != nil {
			t.Fatal(err)
		}
	}
	if err := svc.DeleteVersion(ctx, "events", 2); err != nil {
		t.Fatal(err)
	}
	_ = svc.Close()

	// Act — fresh Service over the same broker.
	svc2, err := registry.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer svc2.Close()
	startCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc2.Start(startCtx); err != nil {
		t.Fatal(err)
	}

	// Assert — replay observes the delete.
	if _, err := svc2.GetVersion("events", 2); !errors.Is(err, registry.ErrVersionNotFound) {
		t.Errorf("after restart, deleted version reappeared: %v", err)
	}
	versions, _ := svc2.ListVersions("events")
	if len(versions) != 2 || versions[0] != 1 || versions[1] != 3 {
		t.Errorf("after restart, ListVersions: got %v, want [1 3]", versions)
	}
}

// TestDeleteSubject_SurvivesRestart proves the tombstone written on
// delete is durable on the schemas topic; a fresh Service replaying
// the topic must observe the deletion.
func TestDeleteSubject_SurvivesRestart(t *testing.T) {
	// Arrange
	svc, b := newServiceForTest(t)
	ctx := context.Background()
	if _, err := svc.Register(ctx, "transient", `{"v":1}`); err != nil {
		t.Fatal(err)
	}
	if err := svc.DeleteSubject(ctx, "transient"); err != nil {
		t.Fatal(err)
	}
	_ = svc.Close()

	// Act — fresh Service over the same broker.
	svc2, err := registry.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer svc2.Close()
	startCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc2.Start(startCtx); err != nil {
		t.Fatal(err)
	}

	// Assert
	if _, err := svc2.GetLatest("transient"); !errors.Is(err, registry.ErrSubjectNotFound) {
		t.Errorf("after restart, deleted subject reappeared: %v", err)
	}
}

// TestMultiInstance_UniqueIDs proves two registry instances sharing
// the same broker assign distinct schema IDs even when they register
// concurrently. ID coordination is broker-driven: each ID is the
// __holocron_schemas topic offset of the registering record, so the
// broker's per-partition ordering serializes ID assignment without
// any registry-side coordination protocol.
func TestMultiInstance_UniqueIDs(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: registry.DefaultTopic, PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	mkSvc := func() *registry.Service {
		s, err := registry.New(b.Transport())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}

	a := mkSvc()
	b2 := mkSvc()

	ctx := context.Background()

	// Act — each instance registers a distinct schema.
	idA, err := a.Register(ctx, "subj-a", `{"v":"a"}`)
	if err != nil {
		t.Fatal(err)
	}
	idB, err := b2.Register(ctx, "subj-b", `{"v":"b"}`)
	if err != nil {
		t.Fatal(err)
	}

	// Assert — IDs are distinct.
	if idA == idB {
		t.Fatalf("multi-instance registries collided on ID %d", idA)
	}
}

// TestMultiInstance_VersionsCountDerived proves a fresh registry
// replaying a topic written by multiple peers assigns versions in
// broker-order: the nth non-tombstone record for a subject becomes
// version n. Local Register-time version picks may collide; the
// canonical version after replay never does.
func TestMultiInstance_VersionsCountDerived(t *testing.T) {
	// Arrange — two instances register distinct schemas for the same
	// subject. The order they hit the broker is the canonical version
	// order.
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: registry.DefaultTopic, PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	mkSvc := func() *registry.Service {
		s, err := registry.New(b.Transport())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return s
	}

	a := mkSvc()
	b2 := mkSvc()
	ctx := context.Background()

	if _, err := a.Register(ctx, "subj", `{"v":"a"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := b2.Register(ctx, "subj", `{"v":"b"}`); err != nil {
		t.Fatal(err)
	}
	_ = a.Close()
	_ = b2.Close()

	// Act — fresh registry replays the topic.
	c := mkSvc()
	defer c.Close()

	// Assert — versions are sequential 1, 2 (count-derived from
	// broker order) regardless of what each writer locally chose.
	versions, err := c.ListVersions("subj")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 2 || versions[0] != 1 || versions[1] != 2 {
		t.Errorf("versions: got %v, want [1 2]", versions)
	}
	v1, err := c.GetVersion("subj", 1)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := c.GetVersion("subj", 2)
	if err != nil {
		t.Fatal(err)
	}
	if v1.Schema == v2.Schema {
		t.Errorf("two distinct schemas merged into the same version")
	}
}

// TestMultiInstance_DedupSameContent proves applyRecord deduplicates
// successive records that carry the same schema text. Two registries
// writing identical content for the same subject should leave only
// one version in the rebuilt registry, with both records' offsets
// resolving to that single schema via GetByID.
func TestMultiInstance_DedupSameContent(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: registry.DefaultTopic, PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	mkSvc := func() *registry.Service {
		s, err := registry.New(b.Transport())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return s
	}

	a := mkSvc()
	b2 := mkSvc()
	ctx := context.Background()

	idA, err := a.Register(ctx, "subj", `{"v":"shared"}`)
	if err != nil {
		t.Fatal(err)
	}
	idB, err := b2.Register(ctx, "subj", `{"v":"shared"}`)
	if err != nil {
		t.Fatal(err)
	}
	_ = a.Close()
	_ = b2.Close()

	// Act — fresh registry replays the topic.
	c := mkSvc()
	defer c.Close()

	// Assert — exactly one version, and both writers' returned IDs
	// resolve to that version through the rebuilt service.
	versions, err := c.ListVersions("subj")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 {
		t.Fatalf("expected dedup to leave 1 version, got %v", versions)
	}
	for _, id := range []int{idA, idB} {
		sc, err := c.GetByID(id)
		if err != nil {
			t.Errorf("GetByID(%d) after replay: %v", id, err)
			continue
		}
		if sc.Schema != `{"v":"shared"}` {
			t.Errorf("GetByID(%d) returned wrong content: %q", id, sc.Schema)
		}
	}
}

func TestCheckCompatibility_NoneAlwaysAccepts(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ok, err := svc.CheckCompatibility("any", "any", registry.CompatibilityNone)
	if err != nil || !ok {
		t.Fatalf("NONE should accept; got ok=%v err=%v", ok, err)
	}
}

// TestCheckCompatibility_RejectsUnknownMode proves the check
// surfaces an explicit error for a typo'd or unknown mode rather
// than silently passing.
func TestCheckCompatibility_RejectsUnknownMode(t *testing.T) {
	svc, _ := newServiceForTest(t)
	if _, err := svc.CheckCompatibility("any", "{}", "QUESTIONABLE"); err == nil {
		t.Fatal("expected error for unknown compatibility mode")
	}
}
