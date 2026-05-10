package registry_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/registry"
)

func newHTTPHandler(t *testing.T) (*httptest.Server, *registry.Service) {
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
	srv := httptest.NewServer(registry.NewHandler(svc))
	t.Cleanup(func() {
		srv.Close()
		_ = svc.Close()
		_ = b.Close()
	})
	return srv, svc
}

// newAuthedHTTPHandler stands up an httptest server whose Handler
// requires the given API keys. Mirrors newHTTPHandler but exercises the
// auth middleware.
func newAuthedHTTPHandler(t *testing.T, keys ...string) (*httptest.Server, *registry.Service) {
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
	srv := httptest.NewServer(registry.NewHandler(svc, registry.WithAPIKeys(keys...)))
	t.Cleanup(func() {
		srv.Close()
		_ = svc.Close()
		_ = b.Close()
	})
	return srv, svc
}

func TestHTTP_AuthRejectsMissingKey(t *testing.T) {
	// Arrange
	srv, _ := newAuthedHTTPHandler(t, "secret-A")

	// Act
	resp, err := http.Get(srv.URL + "/subjects")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Assert
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want 401", resp.StatusCode)
	}
}

func TestHTTP_AuthAcceptsValidKey(t *testing.T) {
	// Arrange
	srv, _ := newAuthedHTTPHandler(t, "secret-A", "secret-B")

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/subjects", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer secret-B")

	// Act
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Assert
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
}

func TestHTTP_AuthRejectsBadKey(t *testing.T) {
	// Arrange
	srv, _ := newAuthedHTTPHandler(t, "secret-A")

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/subjects", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")

	// Act
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Assert
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want 401", resp.StatusCode)
	}
}

func TestHTTP_DeleteSubject(t *testing.T) {
	// Arrange
	srv, svc := newHTTPHandler(t)
	if _, err := svc.Register(context.Background(), "doomed", `{"v":1}`); err != nil {
		t.Fatal(err)
	}

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/subjects/doomed", nil)

	// Act
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Assert
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200", resp.StatusCode)
	}
	if subjects := svc.ListSubjects(); len(subjects) != 0 {
		t.Errorf("subject still present after DELETE: %v", subjects)
	}
}

// TestHTTP_ConfigEndpoints proves PUT /config/{subject} persists
// a compatibility mode and GET /config/{subject} reads it back.
// Default for unset subjects is "NONE".
func TestHTTP_ConfigEndpoints(t *testing.T) {
	srv, _ := newHTTPHandler(t)

	// Default = NONE.
	resp, err := http.Get(srv.URL + "/config/anything")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["compatibilityLevel"] != "NONE" {
		t.Errorf("default: got %q, want NONE", body["compatibilityLevel"])
	}

	// PUT BACKWARD.
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/config/u",
		strings.NewReader(`{"compatibility":"BACKWARD"}`))
	req.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp2.StatusCode)
	}

	// GET reads the new value.
	resp3, err := http.Get(srv.URL + "/config/u")
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	body = nil
	_ = json.NewDecoder(resp3.Body).Decode(&body)
	if body["compatibilityLevel"] != "BACKWARD" {
		t.Errorf("after PUT: got %q, want BACKWARD", body["compatibilityLevel"])
	}
}

// TestHTTP_SchemaOnlyEndpoints proves the Confluent-compat
// /subjects/{s}/versions/{v}/schema and /schemas/ids/{id}/schema
// endpoints return the bare schema text — no JSON wrapper.
func TestHTTP_SchemaOnlyEndpoints(t *testing.T) {
	// Arrange
	srv, svc := newHTTPHandler(t)
	id, err := svc.Register(context.Background(), "events", `{"required":["id"]}`)
	if err != nil {
		t.Fatal(err)
	}

	cases := []string{
		fmt.Sprintf("%s/subjects/events/versions/1/schema", srv.URL),
		fmt.Sprintf("%s/subjects/events/versions/latest/schema", srv.URL),
		fmt.Sprintf("%s/schemas/ids/%d/schema", srv.URL, id),
	}
	for _, url := range cases {
		t.Run(url, func(t *testing.T) {
			resp, err := http.Get(url)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status: got %d, want 200", resp.StatusCode)
			}
			body, _ := io.ReadAll(resp.Body)
			if string(body) != `{"required":["id"]}` {
				t.Errorf("body: got %q, want bare schema text", body)
			}
		})
	}
}

// TestHTTP_DeleteByID proves DELETE /schemas/ids/{id} drops the
// addressed schema. Subsequent GET /schemas/ids/{id} returns 404;
// sibling schemas under the same subject still resolve.
func TestHTTP_DeleteByID(t *testing.T) {
	// Arrange
	srv, svc := newHTTPHandler(t)
	ctx := context.Background()
	id1, _ := svc.Register(ctx, "events", `{"v":1}`)
	id2, _ := svc.Register(ctx, "events", `{"v":2}`)

	req, _ := http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s/schemas/ids/%d", srv.URL, id2), nil)

	// Act
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Assert
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200", resp.StatusCode)
	}
	if got, _ := http.Get(fmt.Sprintf("%s/schemas/ids/%d", srv.URL, id2)); got.StatusCode != http.StatusNotFound {
		t.Errorf("GET deleted ID: got %d, want 404", got.StatusCode)
	}
	if got, _ := http.Get(fmt.Sprintf("%s/schemas/ids/%d", srv.URL, id1)); got.StatusCode != http.StatusOK {
		t.Errorf("GET sibling ID: got %d, want 200", got.StatusCode)
	}
}

// TestHTTP_DeleteVersion proves the DELETE /subjects/{s}/versions/{v}
// route removes a single version while leaving the rest of the
// subject's history intact. Re-fetching the deleted version returns
// 404; siblings still resolve.
func TestHTTP_DeleteVersion(t *testing.T) {
	// Arrange
	srv, svc := newHTTPHandler(t)
	for _, schema := range []string{`{"v":1}`, `{"v":2}`, `{"v":3}`} {
		if _, err := svc.Register(context.Background(), "events", schema); err != nil {
			t.Fatal(err)
		}
	}

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/subjects/events/versions/2", nil)

	// Act
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Assert — DELETE succeeds with 200.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200", resp.StatusCode)
	}

	// Version 2 now 404s; versions 1 and 3 still resolve.
	if got, _ := http.Get(srv.URL + "/subjects/events/versions/2"); got.StatusCode != http.StatusNotFound {
		t.Errorf("GET deleted version: got %d, want 404", got.StatusCode)
	}
	if got, _ := http.Get(srv.URL + "/subjects/events/versions/1"); got.StatusCode != http.StatusOK {
		t.Errorf("GET version 1: got %d, want 200", got.StatusCode)
	}
	if got, _ := http.Get(srv.URL + "/subjects/events/versions/3"); got.StatusCode != http.StatusOK {
		t.Errorf("GET version 3: got %d, want 200", got.StatusCode)
	}
}

func TestHTTP_RegisterReturnsID(t *testing.T) {
	srv, _ := newHTTPHandler(t)

	resp, err := http.Post(srv.URL+"/subjects/orders-value/versions",
		"application/json",
		strings.NewReader(`{"schema":"{\"type\":\"v1\"}"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	var body struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.ID < 0 {
		t.Fatalf("unexpected id: %d", body.ID)
	}
}

func TestHTTP_GetByIDRoundTrip(t *testing.T) {
	srv, svc := newHTTPHandler(t)
	id, err := svc.Register(context.Background(), "orders-value", `{"v":1}`)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(srv.URL + "/schemas/ids/" + itoa(id))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
	var body struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Schema != `{"v":1}` {
		t.Fatalf("schema: got %q", body.Schema)
	}
}

func TestHTTP_ListSubjectsAndVersions(t *testing.T) {
	srv, svc := newHTTPHandler(t)
	ctx := context.Background()
	_, _ = svc.Register(ctx, "a-value", `{"v":1}`)
	_, _ = svc.Register(ctx, "a-value", `{"v":2}`)
	_, _ = svc.Register(ctx, "b-value", `{"v":1}`)

	resp, err := http.Get(srv.URL + "/subjects")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var subjects []string
	if err := json.NewDecoder(resp.Body).Decode(&subjects); err != nil {
		t.Fatal(err)
	}
	if len(subjects) != 2 {
		t.Fatalf("subjects=%v want 2", subjects)
	}

	resp, err = http.Get(srv.URL + "/subjects/a-value/versions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var versions []int
	if err := json.NewDecoder(resp.Body).Decode(&versions); err != nil {
		t.Fatal(err)
	}
	if len(versions) != 2 {
		t.Fatalf("versions=%v want 2", versions)
	}
}

func TestHTTP_GetLatest(t *testing.T) {
	srv, svc := newHTTPHandler(t)
	ctx := context.Background()
	_, _ = svc.Register(ctx, "x", `{"v":1}`)
	_, _ = svc.Register(ctx, "x", `{"v":2}`)

	resp, err := http.Get(srv.URL + "/subjects/x/versions/latest")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var sc registry.Schema
	if err := json.NewDecoder(resp.Body).Decode(&sc); err != nil {
		t.Fatal(err)
	}
	if sc.Version != 2 {
		t.Fatalf("latest version: got %d, want 2", sc.Version)
	}
}

func TestHTTP_NotFoundReturns404(t *testing.T) {
	srv, _ := newHTTPHandler(t)
	resp, err := http.Get(srv.URL + "/subjects/nope/versions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", resp.StatusCode)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	negative := i < 0
	if negative {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if negative {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
