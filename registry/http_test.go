package registry_test

import (
	"context"
	"encoding/json"
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

	resp, _ := http.Get(srv.URL + "/subjects")
	defer resp.Body.Close()
	var subjects []string
	if err := json.NewDecoder(resp.Body).Decode(&subjects); err != nil {
		t.Fatal(err)
	}
	if len(subjects) != 2 {
		t.Fatalf("subjects=%v want 2", subjects)
	}

	resp, _ = http.Get(srv.URL + "/subjects/a-value/versions")
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

	resp, _ := http.Get(srv.URL + "/subjects/x/versions/latest")
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
	resp, _ := http.Get(srv.URL + "/subjects/nope/versions")
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
