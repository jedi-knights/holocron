package clienttls_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/cli/internal/clienttls"
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
	caPath, _, _ := writeCertPair(t)
	cfg, err := clienttls.Config(clienttls.Options{CAFile: caPath})
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if cfg == nil || cfg.RootCAs == nil {
		t.Fatal("expected RootCAs populated")
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion: got %#x, want TLS13", cfg.MinVersion)
	}
}

func TestConfig_BadCAPath(t *testing.T) {
	bogus := filepath.Join(t.TempDir(), "missing.pem")
	if _, err := clienttls.Config(clienttls.Options{CAFile: bogus}); err == nil {
		t.Fatal("expected error for nonexistent CA file, got nil")
	}
}

func TestConfig_SkipVerify(t *testing.T) {
	cfg, err := clienttls.Config(clienttls.Options{SkipVerify: true})
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if !cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify=true")
	}
}

func TestConfig_ClientCertWithoutKey(t *testing.T) {
	_, certPath, _ := writeCertPair(t)
	if _, err := clienttls.Config(clienttls.Options{CertFile: certPath}); err == nil {
		t.Fatal("expected error when --tls-cert supplied without --tls-key")
	}
}

func TestConfig_ClientCertAndKey(t *testing.T) {
	_, certPath, keyPath := writeCertPair(t)
	cfg, err := clienttls.Config(clienttls.Options{CertFile: certPath, KeyFile: keyPath})
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("expected one client cert, got %d", len(cfg.Certificates))
	}
}

func TestRegisterFlags_BuildsConfigFromArgs(t *testing.T) {
	caPath, _, _ := writeCertPair(t)
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	build := clienttls.RegisterFlags(fs)
	if err := fs.Parse([]string{"--tls-ca", caPath}); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cfg, err := build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if cfg == nil || cfg.RootCAs == nil {
		t.Fatal("expected RootCAs populated from --tls-ca")
	}
}

// writeCertPair generates a self-signed P-256 cert and writes it to
// t.TempDir() three times: as a CA bundle, as a cert, and as a key.
// Inlined here because broker/internal/tlsconfig/tlstest is in an
// internal package and cannot be imported from the cli module.
func writeCertPair(t *testing.T) (caPath, certPath, keyPath string) {
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
		Subject:               pkix.Name{CommonName: "cli-test"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	dir := t.TempDir()
	caPath = filepath.Join(dir, "ca.pem")
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")

	writePEM(t, caPath, "CERTIFICATE", der)
	writePEM(t, certPath, "CERTIFICATE", der)
	writePEM(t, keyPath, "EC PRIVATE KEY", keyDER)
	return caPath, certPath, keyPath
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		_ = f.Close()
		t.Fatalf("encode %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
}
