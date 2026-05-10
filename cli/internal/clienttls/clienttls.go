// Package clienttls builds the *tls.Config that holocronctl subcommands
// use when dialing a TLS-protected broker, and exposes the standard
// flag bindings (--tls-ca, --tls-cert, --tls-key, --tls-skip-verify)
// so every subcommand registers the same surface in two lines.
//
// The flag set is intentionally small: a CA bundle to verify the
// broker's cert (or --tls-skip-verify for lab use), and a client cert
// pair when the broker is configured with --tls-require-client-cert.
package clienttls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"os"
)

// Options describes how a holocronctl subcommand should dial the
// broker. The zero value disables TLS — Config returns (nil, nil) and
// the caller dials in plaintext.
type Options struct {
	// CAFile is the path to a PEM CA bundle that signs the broker's
	// cert. When set, the returned config's RootCAs is populated from
	// this file.
	CAFile string
	// CertFile is the path to a PEM client cert presented to a broker
	// configured with --tls-require-client-cert. Must be paired with
	// KeyFile.
	CertFile string
	// KeyFile is the PEM private key matching CertFile.
	KeyFile string
	// SkipVerify disables certificate verification entirely. Lab use
	// only; never set in production.
	SkipVerify bool
}

// Config returns a *tls.Config when any TLS option is set, or
// (nil, nil) when the zero-value Options is supplied. The returned
// config negotiates TLS 1.3 minimum.
func Config(opts Options) (*tls.Config, error) {
	if opts.CAFile == "" && opts.CertFile == "" && opts.KeyFile == "" && !opts.SkipVerify {
		return nil, nil
	}
	if (opts.CertFile == "") != (opts.KeyFile == "") {
		return nil, errors.New("clienttls: --tls-cert and --tls-key must be supplied together")
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
	if opts.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("clienttls: load client cert/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// RegisterFlags adds the four standard TLS flags to fs and returns a
// closure that, after fs.Parse, builds the corresponding *tls.Config.
// The closure returns (nil, nil) when no TLS flag was supplied, so
// callers can pass the result straight into a holocronnet.Dial option
// chain without a separate "is TLS enabled?" branch.
func RegisterFlags(fs *flag.FlagSet) func() (*tls.Config, error) {
	caFile := fs.String("tls-ca", os.Getenv("HOLOCRON_TLS_CA"), "PEM CA bundle for verifying the broker's cert (enables TLS)")
	certFile := fs.String("tls-cert", os.Getenv("HOLOCRON_TLS_CERT"), "PEM client cert for mTLS (paired with --tls-key)")
	keyFile := fs.String("tls-key", os.Getenv("HOLOCRON_TLS_KEY"), "PEM private key matching --tls-cert")
	skipVerify := fs.Bool("tls-skip-verify", false, "disable broker-cert verification (lab use only)")
	return func() (*tls.Config, error) {
		return Config(Options{
			CAFile:     *caFile,
			CertFile:   *certFile,
			KeyFile:    *keyFile,
			SkipVerify: *skipVerify,
		})
	}
}
