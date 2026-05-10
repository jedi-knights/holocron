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
