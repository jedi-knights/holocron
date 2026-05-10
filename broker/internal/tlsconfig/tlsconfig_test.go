package tlsconfig_test

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"

	"github.com/jedi-knights/holocron/broker/internal/tlsconfig"
	"github.com/jedi-knights/holocron/broker/internal/tlsconfig/tlstest"
)

func TestLoad_RequiresCertAndKey(t *testing.T) {
	if _, err := tlsconfig.Load(tlsconfig.Options{}); err == nil {
		t.Fatal("expected error for empty Options, got nil")
	}
}

func TestLoad_RequiresKeyWhenCertGiven(t *testing.T) {
	if _, err := tlsconfig.Load(tlsconfig.Options{CertFile: "cert.pem"}); err == nil {
		t.Fatal("expected error when KeyFile missing, got nil")
	}
}

func TestLoad_RequiresCertWhenKeyGiven(t *testing.T) {
	if _, err := tlsconfig.Load(tlsconfig.Options{KeyFile: "key.pem"}); err == nil {
		t.Fatal("expected error when CertFile missing, got nil")
	}
}

func TestLoad_BadCertPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.pem")
	if _, err := tlsconfig.Load(tlsconfig.Options{CertFile: missing, KeyFile: missing}); err == nil {
		t.Fatal("expected error for nonexistent cert path, got nil")
	}
}

func TestLoad_RequireClientCertNeedsCA(t *testing.T) {
	cert, key, _ := tlstest.GenerateCertPair(t)
	_, err := tlsconfig.Load(tlsconfig.Options{
		CertFile:          cert,
		KeyFile:           key,
		RequireClientCert: true,
	})
	if err == nil {
		t.Fatal("expected error when RequireClientCert without ClientCAFile, got nil")
	}
}

func TestLoad_BadCAPath(t *testing.T) {
	cert, key, _ := tlstest.GenerateCertPair(t)
	bogus := filepath.Join(t.TempDir(), "ca.pem")
	_, err := tlsconfig.Load(tlsconfig.Options{
		CertFile:     cert,
		KeyFile:      key,
		ClientCAFile: bogus,
	})
	if err == nil {
		t.Fatal("expected error for nonexistent ClientCAFile, got nil")
	}
}

func TestLoad_EmptyPEMCAFile(t *testing.T) {
	cert, key, _ := tlstest.GenerateCertPair(t)
	junk := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(junk, []byte("this is not a PEM file\n"), 0o600); err != nil {
		t.Fatalf("write junk CA: %v", err)
	}
	_, err := tlsconfig.Load(tlsconfig.Options{
		CertFile:     cert,
		KeyFile:      key,
		ClientCAFile: junk,
	})
	if err == nil {
		t.Fatal("expected error for ClientCAFile containing no usable PEM blocks, got nil")
	}
}

func TestLoad_HappyPath_ServerOnly(t *testing.T) {
	cert, key, _ := tlstest.GenerateCertPair(t)
	cfg, err := tlsconfig.Load(tlsconfig.Options{CertFile: cert, KeyFile: key})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got, want := len(cfg.Certificates), 1; got != want {
		t.Errorf("Certificates: got %d, want %d", got, want)
	}
	if cfg.ClientAuth != tls.NoClientCert {
		t.Errorf("ClientAuth: got %v, want NoClientCert", cfg.ClientAuth)
	}
	if cfg.ClientCAs != nil {
		t.Error("ClientCAs: expected nil when no CA configured")
	}
}

func TestLoad_HappyPath_OptionalmTLS(t *testing.T) {
	cert, key, ca := tlstest.GenerateCertPair(t)
	cfg, err := tlsconfig.Load(tlsconfig.Options{
		CertFile:     cert,
		KeyFile:      key,
		ClientCAFile: ca,
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Errorf("ClientAuth: got %v, want VerifyClientCertIfGiven", cfg.ClientAuth)
	}
	if cfg.ClientCAs == nil {
		t.Error("ClientCAs: expected populated pool, got nil")
	}
}

func TestLoad_HappyPath_RequiredmTLS(t *testing.T) {
	cert, key, ca := tlstest.GenerateCertPair(t)
	cfg, err := tlsconfig.Load(tlsconfig.Options{
		CertFile:          cert,
		KeyFile:           key,
		ClientCAFile:      ca,
		RequireClientCert: true,
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("ClientAuth: got %v, want RequireAndVerifyClientCert", cfg.ClientAuth)
	}
}

func TestLoad_DefaultsToTLS13(t *testing.T) {
	cert, key, _ := tlstest.GenerateCertPair(t)
	cfg, err := tlsconfig.Load(tlsconfig.Options{CertFile: cert, KeyFile: key})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion: got %#x, want TLS13 (%#x)", cfg.MinVersion, tls.VersionTLS13)
	}
}

func TestLoad_HonorsMinVersion(t *testing.T) {
	cert, key, _ := tlstest.GenerateCertPair(t)
	cfg, err := tlsconfig.Load(tlsconfig.Options{
		CertFile:   cert,
		KeyFile:    key,
		MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion: got %#x, want TLS12 (%#x)", cfg.MinVersion, tls.VersionTLS12)
	}
}
