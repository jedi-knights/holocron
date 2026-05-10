package cluster

import (
	"crypto/tls"
	"errors"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// tlsStreamLayer satisfies raft.StreamLayer (a net.Listener that can
// also Dial outbound) over TLS. The hashicorp/raft library's
// NewTCPTransport hard-codes plaintext TCP; using a custom stream
// layer is the supported way to encrypt inter-node Raft traffic.
//
// The same tls.Config drives both directions. Operators typically
// supply a cert chain via Certificates and pin the peer set via
// RootCAs (when ClientAuth is set to RequireAndVerifyClientCert,
// inbound peers must present a matching cert too — which is the
// recommended setup for a closed cluster).
type tlsStreamLayer struct {
	listener  net.Listener
	advertise net.Addr
	cfg       *tls.Config
}

// newTLSStreamLayer binds a TLS-wrapped TCP listener at bindAddr and
// returns a StreamLayer that dials peer-to-peer over the same
// tls.Config. advertise is the address peers will use to reach this
// node — the listener's Addr() reports this so Raft uses it in
// configuration entries instead of the bind address (which may be
// "0.0.0.0:port").
func newTLSStreamLayer(bindAddr string, advertise net.Addr, cfg *tls.Config) (*tlsStreamLayer, error) {
	if cfg == nil {
		return nil, errors.New("cluster: TLSConfig required for tlsStreamLayer")
	}
	ln, err := tls.Listen("tcp", bindAddr, cfg)
	if err != nil {
		return nil, err
	}
	return &tlsStreamLayer{listener: ln, advertise: advertise, cfg: cfg}, nil
}

// Accept implements net.Listener.
func (t *tlsStreamLayer) Accept() (net.Conn, error) { return t.listener.Accept() }

// Close implements net.Listener.
func (t *tlsStreamLayer) Close() error { return t.listener.Close() }

// Addr returns the advertised address so Raft records peers as the
// outward-reachable host:port.
func (t *tlsStreamLayer) Addr() net.Addr { return t.advertise }

// Dial implements raft.StreamLayer. Always uses TLS — peers without a
// matching cert chain fail the handshake.
func (t *tlsStreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(dialer, "tcp", string(addr), t.cfg)
}
