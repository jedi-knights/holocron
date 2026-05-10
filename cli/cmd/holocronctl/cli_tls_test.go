package main

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
	"strings"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
)

// TestCLI_DialTLS_VerifiesServerCert is the headline integration test
// for the CLI side of Wave 1: a TLS-only broker rejects a ping that
// has no --tls-ca, and accepts the same ping when the operator
// supplies the CA bundle. Without this test the wiring through every
// subcommand could silently regress to plaintext dial.
func TestCLI_DialTLS_VerifiesServerCert(t *testing.T) {
	// Arrange
	caPath, certPath, keyPath := writeServerCert(t)
	serverCfg := loadServerTLSConfig(t, certPath, keyPath)

	b := embed.NewMemory()
	defer b.Close()
	addr, err := b.Listen("127.0.0.1:0", embed.WithTLS(serverCfg))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	// Act / Assert: plain ping must fail — the SDK has no roots that
	// match the broker's self-signed cert, so the handshake aborts.
	if err := run([]string{"ping", "--addr", addr}); err == nil {
		t.Fatal("expected ping without --tls-ca to fail against TLS broker, got nil")
	}

	// Act / Assert: same ping with --tls-ca must succeed — the SDK
	// trusts the broker's cert via the supplied bundle.
	if err := run([]string{"ping", "--addr", addr, "--tls-ca", caPath}); err != nil {
		t.Fatalf("expected ping with --tls-ca to succeed, got: %v", err)
	}
}

// TestCLI_DialTLS_SkipVerify proves the lab escape hatch works: the
// operator can dial a TLS broker without supplying its CA by passing
// --tls-skip-verify. Useful for one-off debugging against a broker
// whose CA the operator does not yet have on hand.
func TestCLI_DialTLS_SkipVerify(t *testing.T) {
	// Arrange
	_, certPath, keyPath := writeServerCert(t)
	serverCfg := loadServerTLSConfig(t, certPath, keyPath)

	b := embed.NewMemory()
	defer b.Close()
	addr, err := b.Listen("127.0.0.1:0", embed.WithTLS(serverCfg))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	// Act / Assert
	if err := run([]string{"ping", "--addr", addr, "--tls-skip-verify"}); err != nil {
		t.Fatalf("ping with --tls-skip-verify: %v", err)
	}
}

// TestCLI_DialTLS_CertWithoutKey proves the helper's validation surface
// fires through the CLI: a half-supplied client cert is rejected
// before the dial is attempted.
func TestCLI_DialTLS_CertWithoutKey(t *testing.T) {
	// Arrange
	_, certPath, _ := writeServerCert(t)

	// Act
	err := run([]string{"ping", "--addr", "127.0.0.1:1", "--tls-cert", certPath})

	// Assert
	if err == nil {
		t.Fatal("expected error when --tls-cert supplied without --tls-key")
	}
	if !strings.Contains(err.Error(), "tls-cert") && !strings.Contains(err.Error(), "tls-key") {
		t.Errorf("expected error to mention --tls-cert/--tls-key pairing, got %q", err)
	}
}

// writeServerCert generates a self-signed P-256 cert valid for
// 127.0.0.1 + ::1 + localhost and writes three files into t.TempDir():
// a CA bundle (the cert itself, since it's self-signed), a server cert,
// and a private key. The same cert serves as both server identity and
// the trust root the client uses — sufficient for loopback handshakes.
func writeServerCert(t *testing.T) (caPath, certPath, keyPath string) {
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
		Subject:               pkix.Name{CommonName: "cli-tls-test"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
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
	writePEMFile(t, caPath, "CERTIFICATE", der)
	writePEMFile(t, certPath, "CERTIFICATE", der)
	writePEMFile(t, keyPath, "EC PRIVATE KEY", keyDER)
	return caPath, certPath, keyPath
}

func writePEMFile(t *testing.T, path, blockType string, der []byte) {
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

func loadServerTLSConfig(t *testing.T, certPath, keyPath string) *tls.Config {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
}
