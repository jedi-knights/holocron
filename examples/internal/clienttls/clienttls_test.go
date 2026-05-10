package clienttls_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/examples/internal/clienttls"
)

func TestConfig_NoneConfigured(t *testing.T) {
	cfg, err := clienttls.Config(clienttls.Options{})
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if cfg != nil {
		t.Errorf("expected nil tls.Config when no options set, got %+v", cfg)
	}
}

func TestConfig_WithCA(t *testing.T) {
	caPath := writeSelfSignedCA(t)
	cfg, err := clienttls.Config(clienttls.Options{CAFile: caPath})
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil tls.Config")
	}
	if cfg.RootCAs == nil {
		t.Error("expected RootCAs populated")
	}
	if cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should be false when CA is configured")
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion: got %#x, want TLS13 (%#x)", cfg.MinVersion, tls.VersionTLS13)
	}
}

func TestConfig_BadCAPath(t *testing.T) {
	bogus := filepath.Join(t.TempDir(), "missing.pem")
	if _, err := clienttls.Config(clienttls.Options{CAFile: bogus}); err == nil {
		t.Fatal("expected error for nonexistent CA file, got nil")
	}
}

func TestConfig_EmptyPEMCAFile(t *testing.T) {
	junk := filepath.Join(t.TempDir(), "junk.pem")
	if err := os.WriteFile(junk, []byte("not a pem\n"), 0o600); err != nil {
		t.Fatalf("write junk: %v", err)
	}
	if _, err := clienttls.Config(clienttls.Options{CAFile: junk}); err == nil {
		t.Fatal("expected error for non-PEM CA file, got nil")
	}
}

func TestConfig_SkipVerify(t *testing.T) {
	cfg, err := clienttls.Config(clienttls.Options{SkipVerify: true})
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil tls.Config when SkipVerify=true")
	}
	if !cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify=true")
	}
}

// writeSelfSignedCA writes a self-signed P-256 cert to t.TempDir() and
// returns its path. Mirrors broker/internal/tlsconfig/tlstest but is
// inlined here because that helper is in an internal package and cannot
// be imported from the examples module.
func writeSelfSignedCA(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generate serial: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "examples-test"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	path := filepath.Join(t.TempDir(), "ca.pem")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	if err := pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		_ = f.Close()
		t.Fatalf("encode pem: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
	return path
}
