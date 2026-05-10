// Package clienttls builds a client-side *tls.Config from operator
// flags. Both examples/producer and examples/consumer accept the same
// --tls-* flags; this helper centralises the cert-loading and
// validation logic so the two example binaries stay consistent.
package clienttls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// Options describes how a client wants to verify the broker's TLS
// certificate. The zero value disables TLS — Config returns (nil, nil)
// and the caller dials in plaintext.
type Options struct {
	// CAFile is the path to a PEM CA bundle that signs the broker's
	// cert. When set, the returned config's RootCAs is populated from
	// this file and the system trust store is not consulted.
	CAFile string
	// SkipVerify disables certificate verification entirely. Lab use
	// only; never set in production.
	SkipVerify bool
}

// Config returns a *tls.Config when any TLS option is set, or
// (nil, nil) when the zero-value Options is supplied. The returned
// config negotiates TLS 1.3 minimum.
func Config(opts Options) (*tls.Config, error) {
	if opts.CAFile == "" && !opts.SkipVerify {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS13}
	if opts.SkipVerify {
		cfg.InsecureSkipVerify = true
	}
	if opts.CAFile != "" {
		pemBytes, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("clienttls: read CA bundle %q: %w", opts.CAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("clienttls: CA bundle %q contains no usable PEM blocks", opts.CAFile)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}
