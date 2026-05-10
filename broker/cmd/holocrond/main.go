// Command holocrond is the holocron broker daemon.
//
// Stage 5 keeps the Stage 3 disk + network defaults and adds optional
// Raft-replicated cluster mode behind --cluster. In cluster mode produce
// and topic-create operations replicate across N nodes via hashicorp/raft;
// followers redirect SDK clients to the leader's wire port.
package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/broker/internal/auth"
	"github.com/jedi-knights/holocron/broker/internal/tlsconfig"
)

const stage = "5 (cluster)"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "holocrond:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("holocrond", flag.ContinueOnError)
	dataDir := fs.String("data-dir", envOrDefault("HOLOCRON_DATA_DIR", "/var/lib/holocron"), "directory for persistent broker state")
	listen := fs.String("listen", envOrDefault("HOLOCRON_LISTEN", ":9092"), "TCP address to listen on (empty disables the listener)")
	retention := fs.Duration("retention", 0, "delete sealed segments older than this; 0 disables time retention")
	retentionBytes := fs.Int64("retention-bytes", 0, "delete oldest sealed segments while a partition exceeds this size; 0 disables size retention")
	memory := fs.Bool("memory", false, "use the in-memory store instead of disk (testing only)")
	clusterMode := fs.Bool("cluster", false, "enable Raft-replicated cluster mode")
	nodeID := fs.String("node-id", envOrDefault("HOLOCRON_NODE_ID", ""), "this node's ID (cluster mode)")
	raftBind := fs.String("raft-listen", envOrDefault("HOLOCRON_RAFT_LISTEN", ":9192"), "Raft RPC bind address (cluster mode)")
	peers := fs.String("peers", envOrDefault("HOLOCRON_PEERS", ""), "cluster membership as id=raft-addr=wire-addr,id=...,id=... (cluster mode)")
	bootstrap := fs.Bool("bootstrap", false, "bootstrap the cluster as the first node (cluster mode)")
	tlsCert := fs.String("tls-cert", envOrDefault("HOLOCRON_TLS_CERT", ""), "PEM cert chain for the wire listener (presence enables TLS)")
	tlsKey := fs.String("tls-key", envOrDefault("HOLOCRON_TLS_KEY", ""), "PEM private key matching --tls-cert")
	tlsClientCA := fs.String("tls-client-ca", envOrDefault("HOLOCRON_TLS_CLIENT_CA", ""), "PEM CA bundle for client-cert verification (optional mTLS unless --tls-require-client-cert)")
	tlsRequireClient := fs.Bool("tls-require-client-cert", boolEnvOrDefault("HOLOCRON_TLS_REQUIRE_CLIENT_CERT", false), "reject clients that do not present a cert verified by --tls-client-ca")
	tlsMinVer := fs.String("tls-min-version", envOrDefault("HOLOCRON_TLS_MIN_VERSION", "1.3"), "minimum TLS version: 1.2 or 1.3 (default 1.3)")
	clusterTLSCert := fs.String("cluster-tls-cert", envOrDefault("HOLOCRON_CLUSTER_TLS_CERT", ""), "PEM cert chain for the Raft transport (presence enables cluster TLS — mTLS mandatory)")
	clusterTLSKey := fs.String("cluster-tls-key", envOrDefault("HOLOCRON_CLUSTER_TLS_KEY", ""), "PEM private key matching --cluster-tls-cert")
	clusterTLSCA := fs.String("cluster-tls-ca", envOrDefault("HOLOCRON_CLUSTER_TLS_CA", ""), "PEM CA bundle that signs every peer's cert; used for both inbound and outbound verification")
	clusterTLSServerName := fs.String("cluster-tls-server-name", envOrDefault("HOLOCRON_CLUSTER_TLS_SERVER_NAME", ""), "expected SAN on peer certs when dialing (override only when peer certs do not carry their bind addresses)")
	authIssuerKey := fs.String("auth-issuer-key", envOrDefault("HOLOCRON_AUTH_ISSUER_KEY", ""), "PEM-encoded Ed25519 public key (PKIX) — presence enables JWT-required auth on the wire listener")
	authDenylist := fs.String("auth-denylist", envOrDefault("HOLOCRON_AUTH_DENYLIST", ""), "path to a denylist file (one subject per line); reloaded on SIGHUP")
	if err := fs.Parse(args); err != nil {
		return err
	}

	tlsCfg, err := loadWireTLS(*tlsCert, *tlsKey, *tlsClientCA, *tlsRequireClient, *tlsMinVer)
	if err != nil {
		return err
	}
	if tlsCfg != nil && *listen == "" {
		return errors.New("TLS flags require --listen (TLS applies to the wire listener; it cannot be configured without one)")
	}

	clusterTLSCfg, err := loadClusterTLS(*clusterTLSCert, *clusterTLSKey, *clusterTLSCA, *clusterTLSServerName)
	if err != nil {
		return err
	}
	if clusterTLSCfg != nil && !*clusterMode {
		return errors.New("--cluster-tls-* flags require --cluster (cluster TLS protects the Raft transport, which only runs in cluster mode)")
	}

	authVerifier, denylist, err := loadAuthVerifier(*authIssuerKey, *authDenylist)
	if err != nil {
		return err
	}
	if authVerifier != nil && *listen == "" {
		return errors.New("--auth-issuer-key requires --listen (auth applies to the wire listener; it cannot be configured without one)")
	}

	fmt.Printf("holocrond — stage %s\n", stage)

	var b *embed.Broker
	if *memory {
		fmt.Println("backend: in-memory (data lost on shutdown)")
		b = embed.NewMemory()
	} else {
		fmt.Printf("backend: disk at %s\n", *dataDir)
		opts := []embed.DiskOption{}
		if *retention > 0 {
			opts = append(opts, embed.WithRetention(*retention))
			fmt.Printf("retention: segments older than %s are deleted\n", *retention)
		}
		if *retentionBytes > 0 {
			opts = append(opts, embed.WithSizeRetention(*retentionBytes))
			fmt.Printf("retention: per-partition size capped at %d bytes\n", *retentionBytes)
		}
		if *clusterMode {
			peerList, err := parsePeers(*peers)
			if err != nil {
				return fmt.Errorf("--peers: %w", err)
			}
			if *nodeID == "" {
				return fmt.Errorf("--node-id is required in cluster mode")
			}
			opts = append(opts, embed.WithCluster(embed.ClusterConfig{
				NodeID:    *nodeID,
				BindAddr:  *raftBind,
				Peers:     peerList,
				Bootstrap: *bootstrap,
				TLSConfig: clusterTLSCfg,
			}))
			raftScheme := "plain"
			if clusterTLSCfg != nil {
				raftScheme = "TLS 1.3 (mTLS required)"
			}
			fmt.Printf("cluster: node %s, raft %s [%s], peers=%d, bootstrap=%v\n",
				*nodeID, *raftBind, raftScheme, len(peerList), *bootstrap)
		}
		var err error
		b, err = embed.NewDisk(*dataDir, opts...)
		if err != nil {
			return fmt.Errorf("open broker: %w", err)
		}
	}

	if topics := b.Topics(); len(topics) > 0 {
		fmt.Printf("recovered %d topic(s):\n", len(topics))
		for _, c := range topics {
			fmt.Printf("  %s (%d partitions)\n", c.Name, c.PartitionCount)
		}
	}

	if *listen != "" {
		listenOpts := []embed.ListenOption{}
		if tlsCfg != nil {
			listenOpts = append(listenOpts, embed.WithTLS(tlsCfg))
		}
		if authVerifier != nil {
			listenOpts = append(listenOpts, embed.WithAuthVerifier(authVerifier))
		}
		addr, err := b.Listen(*listen, listenOpts...)
		if err != nil {
			_ = b.Close()
			return fmt.Errorf("listen: %w", err)
		}
		fmt.Printf("listening on %s (wire v%d, %s, auth=%s)\n", addr, 1, wireSchemeDescription(tlsCfg), authSchemeDescription(authVerifier))
	} else {
		fmt.Println("broker ready (network listener disabled)")
	}

	// SIGHUP reloads the denylist file in place. The verifier holds
	// the *MemoryDenyList by interface, so a successful Set call is
	// visible to every subsequent Verify with no broker restart.
	if denylist != nil && *authDenylist != "" {
		hup := make(chan os.Signal, 1)
		signal.Notify(hup, syscall.SIGHUP)
		go func() {
			for range hup {
				subjects, err := readDenylistFile(*authDenylist)
				if err != nil {
					fmt.Fprintf(os.Stderr, "holocrond: SIGHUP denylist reload failed: %v\n", err)
					continue
				}
				denylist.Set(subjects)
				fmt.Printf("holocrond: denylist reloaded (%d entries)\n", len(subjects))
			}
		}()
	}
	fmt.Println("press Ctrl-C to exit")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	fmt.Println("shutting down")

	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = shutdown
	return b.Close()
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// boolEnvOrDefault treats "1", "true", and "yes" (case-insensitive) as
// true. Anything else, including unset, falls back to def.
func boolEnvOrDefault(key string, def bool) bool {
	v := strings.ToLower(os.Getenv(key))
	if v == "" {
		return def
	}
	return v == "1" || v == "true" || v == "yes"
}

// loadWireTLS returns a *tls.Config built from the daemon's --tls-* flags,
// or nil when no TLS flag is set. Returns an error on any malformed
// combination: missing key, bad path, mTLS required without a CA, or an
// unrecognised --tls-min-version.
func loadWireTLS(cert, key, clientCA string, requireClientCert bool, minVer string) (*tls.Config, error) {
	if cert == "" && key == "" && clientCA == "" && !requireClientCert {
		return nil, nil
	}
	min, err := parseTLSVersion(minVer)
	if err != nil {
		return nil, err
	}
	return tlsconfig.Load(tlsconfig.Options{
		CertFile:          cert,
		KeyFile:           key,
		ClientCAFile:      clientCA,
		RequireClientCert: requireClientCert,
		MinVersion:        min,
	})
}

// loadClusterTLS returns a *tls.Config for inter-node Raft traffic, or
// nil when no cluster-TLS flag is set. Cluster TLS is symmetric mTLS:
// every node both listens for and dials peers, so cert, key, and CA
// must all be supplied together. The same CA pool is used as both
// ClientCAs (verifying inbound peer certs) and RootCAs (verifying
// outbound peer certs). Serverside ClientAuth is RequireAndVerifyClient-
// Cert — half-encrypted Raft is not a supported state.
func loadClusterTLS(cert, key, ca, serverName string) (*tls.Config, error) {
	if cert == "" && key == "" && ca == "" {
		return nil, nil
	}
	if cert == "" || key == "" || ca == "" {
		return nil, errors.New("--cluster-tls-cert, --cluster-tls-key, and --cluster-tls-ca must all be supplied together")
	}
	cfg, err := tlsconfig.Load(tlsconfig.Options{
		CertFile:          cert,
		KeyFile:           key,
		ClientCAFile:      ca,
		RequireClientCert: true,
	})
	if err != nil {
		return nil, err
	}
	cfg.RootCAs = cfg.ClientCAs
	if serverName != "" {
		cfg.ServerName = serverName
	}
	return cfg, nil
}

// loadAuthVerifier returns an auth.TokenVerifier and (when applicable)
// the *MemoryDenyList backing it, derived from the daemon's --auth-*
// flags. Returns (nil, nil, nil) when no auth flag is set —
// AnonymousVerifier remains the default.
//
// Validation rules:
//   - --auth-denylist without --auth-issuer-key is rejected (a
//     denylist needs a verifier to attach to; AnonymousVerifier
//     ignores its credential).
//   - --auth-issuer-key must point at a PEM-encoded PKIX Ed25519
//     public key — exactly the format `openssl genpkey -algorithm
//     Ed25519` produces (its public-key counterpart).
func loadAuthVerifier(issuerKeyPath, denylistPath string) (auth.TokenVerifier, *auth.MemoryDenyList, error) {
	if issuerKeyPath == "" && denylistPath == "" {
		return nil, nil, nil
	}
	if issuerKeyPath == "" {
		return nil, nil, errors.New("--auth-denylist requires --auth-issuer-key (a denylist has nothing to attach to without a verifier)")
	}

	pubKey, err := loadEd25519PublicKey(issuerKeyPath)
	if err != nil {
		return nil, nil, err
	}

	opts := []auth.Ed25519VerifierOption{}
	var denylist *auth.MemoryDenyList
	if denylistPath != "" {
		subjects, err := readDenylistFile(denylistPath)
		if err != nil {
			return nil, nil, err
		}
		denylist = auth.NewMemoryDenyList(subjects...)
		opts = append(opts, auth.WithDenyList(denylist))
	}
	return auth.NewEd25519Verifier(pubKey, opts...), denylist, nil
}

// loadEd25519PublicKey parses a PEM-encoded PKIX Ed25519 public key
// from path. Any other algorithm (RSA, ECDSA) is rejected.
func loadEd25519PublicKey(path string) (ed25519.PublicKey, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("--auth-issuer-key %q: %w", path, err)
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("--auth-issuer-key %q: contains no PEM block", path)
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("--auth-issuer-key %q: parse PKIX: %w", path, err)
	}
	ed, ok := pub.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("--auth-issuer-key %q: expected Ed25519 public key, got %T", path, pub)
	}
	return ed, nil
}

// readDenylistFile reads subject IDs from a denylist file, one per
// line. Blank lines and lines beginning with `#` are skipped so an
// operator can annotate entries with the reason or ticket number.
func readDenylistFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("--auth-denylist %q: %w", path, err)
	}
	defer f.Close()

	var subjects []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		subjects = append(subjects, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("--auth-denylist %q: %w", path, err)
	}
	return subjects, nil
}

// authSchemeDescription summarises the wire-listener auth posture for
// the startup banner: "anonymous" when no verifier is configured,
// "jwt" when the Ed25519 verifier is in place. Future verifiers
// (mTLS-CN, OIDC) extend this map.
func authSchemeDescription(v auth.TokenVerifier) string {
	if v == nil {
		return "anonymous"
	}
	switch v.(type) {
	case *auth.Ed25519Verifier:
		return "jwt"
	case *auth.APIKeyVerifier:
		return "api-key"
	default:
		return "configured"
	}
}

// parseTLSVersion maps the operator-facing "1.2" / "1.3" string to the
// uint16 constants in crypto/tls. Empty defaults to 1.3.
func parseTLSVersion(v string) (uint16, error) {
	switch v {
	case "", "1.3":
		return tls.VersionTLS13, nil
	case "1.2":
		return tls.VersionTLS12, nil
	default:
		return 0, fmt.Errorf("--tls-min-version: expected 1.2 or 1.3, got %q", v)
	}
}

// wireSchemeDescription summarises the wire-listener security posture for
// the startup banner: "plain" when TLS is off, "TLS 1.3" when on, and a
// suffix for the mTLS mode when client-cert verification is configured.
func wireSchemeDescription(cfg *tls.Config) string {
	if cfg == nil {
		return "plain"
	}
	scheme := "TLS " + tlsVersionLabel(cfg.MinVersion)
	switch cfg.ClientAuth {
	case tls.RequireAndVerifyClientCert:
		scheme += " (mTLS required)"
	case tls.VerifyClientCertIfGiven:
		scheme += " (mTLS optional)"
	}
	return scheme
}

func tlsVersionLabel(v uint16) string {
	switch v {
	case tls.VersionTLS13:
		return "1.3"
	case tls.VersionTLS12:
		return "1.2"
	default:
		return fmt.Sprintf("%#x", v)
	}
}

// parsePeers parses a "--peers" string of the form
// "id=raftaddr=wireaddr,id=raftaddr=wireaddr,..." into ClusterPeers.
// Empty input yields an empty slice.
func parsePeers(s string) ([]embed.ClusterPeer, error) {
	if s == "" {
		return nil, nil
	}
	var peers []embed.ClusterPeer
	for _, part := range strings.Split(s, ",") {
		fields := strings.Split(strings.TrimSpace(part), "=")
		if len(fields) != 3 {
			return nil, fmt.Errorf("expected id=raftaddr=wireaddr, got %q", part)
		}
		peers = append(peers, embed.ClusterPeer{
			ID:       fields[0],
			Addr:     fields[1],
			WireAddr: fields[2],
		})
	}
	return peers, nil
}
