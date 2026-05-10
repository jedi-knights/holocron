package main

import (
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
