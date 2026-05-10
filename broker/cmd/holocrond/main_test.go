package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jedi-knights/holocron/broker/internal/tlsconfig/tlstest"
)

// TLS flag validation must fail fast — before the broker is opened or the
// signal-handling loop begins — so that misconfiguration surfaces as an
// immediate error from run() rather than a hung process.

func TestRun_TLSCertWithoutKey(t *testing.T) {
	cert := filepath.Join(t.TempDir(), "cert.pem")
	err := run([]string{"--memory", "--listen=", "--tls-cert", cert})
	if err == nil {
		t.Fatal("expected error when --tls-key missing, got nil")
	}
	if !strings.Contains(err.Error(), "tls") && !strings.Contains(err.Error(), "TLS") {
		t.Errorf("expected TLS-related error, got %q", err)
	}
}

func TestRun_TLSKeyWithoutCert(t *testing.T) {
	key := filepath.Join(t.TempDir(), "key.pem")
	err := run([]string{"--memory", "--listen=", "--tls-key", key})
	if err == nil {
		t.Fatal("expected error when --tls-cert missing, got nil")
	}
}

func TestRun_TLSRequireClientCertWithoutCA(t *testing.T) {
	cert, key, _ := tlstest.GenerateCertPair(t)
	err := run([]string{
		"--memory", "--listen=",
		"--tls-cert", cert, "--tls-key", key,
		"--tls-require-client-cert",
	})
	if err == nil {
		t.Fatal("expected error when --tls-require-client-cert without --tls-client-ca, got nil")
	}
}

func TestRun_TLSBadCertPath(t *testing.T) {
	bogus := filepath.Join(t.TempDir(), "nope.pem")
	err := run([]string{
		"--memory", "--listen=",
		"--tls-cert", bogus, "--tls-key", bogus,
	})
	if err == nil {
		t.Fatal("expected error for nonexistent cert/key files, got nil")
	}
}

func TestRun_TLSMinVersionInvalid(t *testing.T) {
	cert, key, _ := tlstest.GenerateCertPair(t)
	err := run([]string{
		"--memory", "--listen=",
		"--tls-cert", cert, "--tls-key", key,
		"--tls-min-version", "1.1",
	})
	if err == nil {
		t.Fatal("expected error for invalid --tls-min-version, got nil")
	}
}

func TestRun_TLSFlagsRequireListener(t *testing.T) {
	cert, key, _ := tlstest.GenerateCertPair(t)
	err := run([]string{
		"--memory", "--listen=",
		"--tls-cert", cert, "--tls-key", key,
	})
	if err == nil {
		t.Fatal("expected error when TLS flags supplied with empty --listen, got nil")
	}
	if !strings.Contains(err.Error(), "listen") {
		t.Errorf("expected error to mention --listen, got %q", err)
	}
}

// Cluster TLS is symmetric mTLS: every node is both a server (accepting
// peer connections) and a client (dialing peers), so all three of cert,
// key, and CA are mandatory together. Half-encrypted Raft is not a
// supported state.

func TestRun_ClusterTLSCertOnly(t *testing.T) {
	cert := filepath.Join(t.TempDir(), "cert.pem")
	err := run([]string{
		"--memory", "--listen=",
		"--cluster", "--node-id=n1",
		"--cluster-tls-cert", cert,
	})
	if err == nil {
		t.Fatal("expected error when --cluster-tls-cert supplied alone, got nil")
	}
}

func TestRun_ClusterTLSCertKeyWithoutCA(t *testing.T) {
	cert, key, _ := tlstest.GenerateCertPair(t)
	err := run([]string{
		"--memory", "--listen=",
		"--cluster", "--node-id=n1",
		"--cluster-tls-cert", cert, "--cluster-tls-key", key,
	})
	if err == nil {
		t.Fatal("expected error when --cluster-tls-ca missing, got nil")
	}
	if !strings.Contains(err.Error(), "cluster-tls-ca") {
		t.Errorf("expected error to mention --cluster-tls-ca, got %q", err)
	}
}

func TestRun_ClusterTLSCAWithoutCertKey(t *testing.T) {
	_, _, ca := tlstest.GenerateCertPair(t)
	err := run([]string{
		"--memory", "--listen=",
		"--cluster", "--node-id=n1",
		"--cluster-tls-ca", ca,
	})
	if err == nil {
		t.Fatal("expected error when --cluster-tls-ca supplied without cert+key, got nil")
	}
}

func TestRun_ClusterTLSBadCertPath(t *testing.T) {
	bogus := filepath.Join(t.TempDir(), "nope.pem")
	_, _, ca := tlstest.GenerateCertPair(t)
	err := run([]string{
		"--memory", "--listen=",
		"--cluster", "--node-id=n1",
		"--cluster-tls-cert", bogus,
		"--cluster-tls-key", bogus,
		"--cluster-tls-ca", ca,
	})
	if err == nil {
		t.Fatal("expected error for nonexistent cluster cert/key, got nil")
	}
}

func TestRun_ClusterTLSWithoutClusterMode(t *testing.T) {
	cert, key, ca := tlstest.GenerateCertPair(t)
	err := run([]string{
		"--memory", "--listen=",
		"--cluster-tls-cert", cert,
		"--cluster-tls-key", key,
		"--cluster-tls-ca", ca,
	})
	if err == nil {
		t.Fatal("expected error when cluster TLS supplied without --cluster, got nil")
	}
	if !strings.Contains(err.Error(), "--cluster") {
		t.Errorf("expected error to mention --cluster, got %q", err)
	}
}

// Auth flag validation must fail fast — before the broker is opened
// or the signal-handling loop begins — so bad config surfaces as an
// immediate error from run() rather than a hung process.

func TestRun_AuthIssuerKeyBadPath(t *testing.T) {
	bogus := filepath.Join(t.TempDir(), "missing.pem")
	err := run([]string{
		"--memory", "--listen=",
		"--auth-issuer-key", bogus,
	})
	if err == nil {
		t.Fatal("expected error for nonexistent --auth-issuer-key path, got nil")
	}
}

func TestRun_AuthIssuerKeyBadPEM(t *testing.T) {
	junk := filepath.Join(t.TempDir(), "junk.pem")
	if err := os.WriteFile(junk, []byte("not pem material\n"), 0o600); err != nil {
		t.Fatalf("write junk: %v", err)
	}
	err := run([]string{
		"--memory", "--listen=",
		"--auth-issuer-key", junk,
	})
	if err == nil {
		t.Fatal("expected error for non-PEM --auth-issuer-key file, got nil")
	}
}

func TestRun_AuthIssuerKeyRequiresListener(t *testing.T) {
	keyPath := writeEd25519PublicKeyPEM(t)
	err := run([]string{
		"--memory", "--listen=",
		"--auth-issuer-key", keyPath,
	})
	if err == nil {
		t.Fatal("expected error when --auth-issuer-key supplied without --listen, got nil")
	}
	if !strings.Contains(err.Error(), "listen") {
		t.Errorf("expected error to mention --listen, got %q", err)
	}
}

func TestRun_AuthDenylistWithoutIssuer(t *testing.T) {
	denylistPath := filepath.Join(t.TempDir(), "deny.txt")
	if err := os.WriteFile(denylistPath, []byte("alice\n"), 0o600); err != nil {
		t.Fatalf("write denylist: %v", err)
	}
	err := run([]string{
		"--memory", "--listen=",
		"--auth-denylist", denylistPath,
	})
	if err == nil {
		t.Fatal("expected error when --auth-denylist supplied without --auth-issuer-key, got nil")
	}
	if !strings.Contains(err.Error(), "issuer") {
		t.Errorf("expected error to mention --auth-issuer-key, got %q", err)
	}
}

func TestRun_AuthDenylistBadPath(t *testing.T) {
	keyPath := writeEd25519PublicKeyPEM(t)
	bogus := filepath.Join(t.TempDir(), "missing-deny.txt")
	err := run([]string{
		"--memory", "--listen=:0",
		"--auth-issuer-key", keyPath,
		"--auth-denylist", bogus,
	})
	if err == nil {
		t.Fatal("expected error for nonexistent --auth-denylist file, got nil")
	}
}

// writeEd25519PublicKeyPEM generates an Ed25519 keypair, encodes the
// public key as a PKIX PEM block, writes it to t.TempDir(), and
// returns the path. Used to validate --auth-issuer-key flag parsing
// without requiring openssl on the test runner.
func writeEd25519PublicKeyPEM(t *testing.T) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}
	path := filepath.Join(t.TempDir(), "issuer.pub.pem")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	if err := pem.Encode(f, &pem.Block{Type: "PUBLIC KEY", Bytes: der}); err != nil {
		_ = f.Close()
		t.Fatalf("encode pem: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
	return path
}
