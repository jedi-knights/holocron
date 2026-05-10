// Package tlsconfig loads TLS material (cert, key, optional CA bundle)
// from disk and returns a *tls.Config suitable for the broker's wire
// listener and intra-cluster Raft transport.
//
// This package is the single place that decides Holocron's TLS policy:
// minimum protocol version (TLS 1.3 by default), how the ClientAuth mode
// is derived from the supplied options, and how cert material is read
// from the filesystem. Callers — broker/cmd/holocrond, broker/embed, and
// broker/internal/cluster — bring this *tls.Config to the listener and
// transport setup; they do not reinvent any of these decisions.
//
// Session-ticket keys are managed by the standard library's crypto/tls
// defaults (rotated automatically, not persisted across restarts). This
// is the desired behavior for a broker: ticket-based resumption works
// within a process lifetime, and a restart forces a fresh handshake
// rather than carrying long-lived secrets across instances.
package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// Options describes a TLS configuration to load. The zero value is
// invalid: CertFile and KeyFile must both be set.
type Options struct {
	// CertFile is the path to a PEM-encoded certificate (or chain).
	CertFile string
	// KeyFile is the path to the PEM-encoded private key matching CertFile.
	KeyFile string
	// ClientCAFile is an optional PEM-encoded CA bundle. When set, inbound
	// client certs are verified against this bundle. When unset, client
	// certs are not requested.
	ClientCAFile string
	// RequireClientCert promotes client-cert verification from "verify if
	// presented" to "require and verify". Setting this with an empty
	// ClientCAFile is an error.
	RequireClientCert bool
	// MinVersion is the minimum TLS protocol version to negotiate. Zero
	// defaults to TLS 1.3. Use tls.VersionTLS12 only when a legitimate
	// legacy intermediary requires it; opting in to TLS 1.2 inherits the
	// standard library's default cipher-suite list, which an operator
	// enabling that path should audit for their threat model.
	MinVersion uint16
}

// Load reads the supplied cert material and returns a *tls.Config.
// Returns an error if any required path is missing, any file is
// unreadable, the cert and key don't pair up, or the CA bundle contains
// no usable PEM blocks.
func Load(opts Options) (*tls.Config, error) {
	if opts.CertFile == "" || opts.KeyFile == "" {
		return nil, errors.New("tlsconfig: CertFile and KeyFile are both required")
	}
	if opts.RequireClientCert && opts.ClientCAFile == "" {
		return nil, errors.New("tlsconfig: RequireClientCert needs ClientCAFile")
	}

	cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tlsconfig: load cert/key: %w", err)
	}

	min := opts.MinVersion
	if min == 0 {
		min = tls.VersionTLS13
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   min,
	}

	if opts.ClientCAFile != "" {
		pool, err := loadCABundle(opts.ClientCAFile)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		if opts.RequireClientCert {
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			cfg.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}

	return cfg, nil
}

func loadCABundle(path string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("tlsconfig: read CA bundle %q: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("tlsconfig: CA bundle %q contains no usable PEM blocks", path)
	}
	return pool, nil
}
