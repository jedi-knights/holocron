package cluster

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Defaults tuned for educational visibility.
const (
	defaultApplyTimeout = 5 * time.Second
	defaultRaftTimeout  = 10 * time.Second
)

// Peer identifies one node in a cluster.
type Peer struct {
	ID       string // unique within the cluster
	Addr     string // host:port for Raft RPC
	WireAddr string // host:port for the broker's wire protocol (optional, used for leader redirects)
}

// Config configures a Cluster.
type Config struct {
	NodeID        string // this node's ID (must match one of Peers when bootstrapping)
	BindAddr      string // address Raft listens on for inter-node RPC ("host:port" or ":port")
	AdvertiseAddr string // address peers should dial to reach this node (defaults to BindAddr)
	DataDir       string // directory for raft-boltdb files + snapshots
	Peers         []Peer // initial cluster membership
	Bootstrap     bool   // true on the first node only; false for joiners
	ApplyTimeout  time.Duration
	// TLSConfig, when non-nil, encrypts inter-node Raft traffic. The
	// same config is used for both the listener (server-side handshake)
	// and outbound dials (client-side handshake), so it must carry the
	// cert chain plus a RootCAs / GetCertificate setup that satisfies
	// both directions. Nil keeps the wire-compatible plaintext path.
	TLSConfig *tls.Config
}

// Cluster is a Raft-replicated wrapper around an FSM. It exposes Apply
// methods that submit commands and wait for them to be committed.
type Cluster struct {
	cfg       Config
	fsm       *FSM
	transport *raft.NetworkTransport
	logStore  *raftboltdb.BoltStore
	stable    *raftboltdb.BoltStore
	snaps     *raft.FileSnapshotStore
	raft      *raft.Raft
}

// New brings up a Raft cluster with the given configuration and FSM.
// The data directory is created if absent. On bootstrap the local node
// starts as the only voter; followers are added by Cluster.AddVoter or
// arrive via the cluster's own gossip/discovery (Stage 5 uses static
// configs only).
func New(cfg Config, fsm *FSM) (*Cluster, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("cluster: NodeID is required")
	}
	if cfg.BindAddr == "" {
		return nil, errors.New("cluster: BindAddr is required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("cluster: DataDir is required")
	}
	if cfg.ApplyTimeout == 0 {
		cfg.ApplyTimeout = defaultApplyTimeout
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("cluster: mkdir %s: %w", cfg.DataDir, err)
	}

	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("cluster: resolve %s: %w", cfg.BindAddr, err)
	}
	advertise := cfg.AdvertiseAddr
	if advertise == "" {
		advertise = cfg.BindAddr
	}
	advAddr, err := net.ResolveTCPAddr("tcp", advertise)
	if err != nil {
		return nil, fmt.Errorf("cluster: resolve advertise %s: %w", advertise, err)
	}
	var transport *raft.NetworkTransport
	if cfg.TLSConfig != nil {
		stream, err := newTLSStreamLayer(addr.String(), advAddr, cfg.TLSConfig)
		if err != nil {
			return nil, fmt.Errorf("cluster: tls transport: %w", err)
		}
		transport = raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
			Stream:  stream,
			MaxPool: 3,
			Timeout: defaultRaftTimeout,
			Logger:  nil,
		})
	} else {
		var err error
		transport, err = raft.NewTCPTransport(addr.String(), advAddr, 3, defaultRaftTimeout, os.Stderr)
		if err != nil {
			return nil, fmt.Errorf("cluster: tcp transport: %w", err)
		}
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-log.db"))
	if err != nil {
		_ = transport.Close()
		return nil, fmt.Errorf("cluster: log store: %w", err)
	}
	stable, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-stable.db"))
	if err != nil {
		_ = transport.Close()
		_ = logStore.Close()
		return nil, fmt.Errorf("cluster: stable store: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		_ = transport.Close()
		_ = logStore.Close()
		_ = stable.Close()
		return nil, fmt.Errorf("cluster: snapshot store: %w", err)
	}

	rcfg := raft.DefaultConfig()
	rcfg.LocalID = raft.ServerID(cfg.NodeID)
	// Snapshots are no-ops in Stage 5 (FSM persists nothing through Raft);
	// keep the threshold high so the Raft log isn't trimmed prematurely.
	rcfg.SnapshotThreshold = 1 << 30

	r, err := raft.NewRaft(rcfg, fsm, logStore, stable, snaps, transport)
	if err != nil {
		_ = transport.Close()
		_ = logStore.Close()
		_ = stable.Close()
		return nil, fmt.Errorf("cluster: raft: %w", err)
	}

	c := &Cluster{
		cfg:       cfg,
		fsm:       fsm,
		transport: transport,
		logStore:  logStore,
		stable:    stable,
		snaps:     snaps,
		raft:      r,
	}

	if cfg.Bootstrap {
		servers := make([]raft.Server, 0, len(cfg.Peers))
		seen := false
		for _, p := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(p.ID),
				Address: raft.ServerAddress(p.Addr),
			})
			if p.ID == cfg.NodeID {
				seen = true
			}
		}
		if !seen {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(cfg.NodeID),
				Address: raft.ServerAddress(advertise),
			})
		}
		future := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := future.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			_ = c.Close()
			return nil, fmt.Errorf("cluster: bootstrap: %w", err)
		}
	}

	return c, nil
}

// IsLeader reports whether this node currently believes itself to be the
// Raft leader.
func (c *Cluster) IsLeader() bool {
	return c.raft.State() == raft.Leader
}

// NodeID returns this node's Raft ID — the value passed in at
// construction. Useful for status reporting so an operator can see
// which broker they're talking to alongside the leader's identity.
func (c *Cluster) NodeID() string { return c.cfg.NodeID }

// LeaderAddr returns the Raft address of the current cluster leader, or
// "" if no leader is known. Use LeaderWireAddr for the broker port.
func (c *Cluster) LeaderAddr() string {
	addr, _ := c.raft.LeaderWithID()
	return string(addr)
}

// LeaderID returns the ID of the current cluster leader, or "" if no
// leader is known.
func (c *Cluster) LeaderID() string {
	_, id := c.raft.LeaderWithID()
	return string(id)
}

// LeaderWireAddr returns the broker wire-protocol address of the current
// leader, looked up via the static peer configuration. Returns "" if the
// leader's wire address is unknown (config didn't list one) or no leader
// is currently known.
func (c *Cluster) LeaderWireAddr() string {
	leaderID := c.LeaderID()
	if leaderID == "" {
		return ""
	}
	for _, p := range c.cfg.Peers {
		if p.ID == leaderID {
			return p.WireAddr
		}
	}
	return ""
}

// WaitForLeader blocks until a leader is elected (any node, this one or a
// peer) or until ctx is cancelled.
func (c *Cluster) WaitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if c.LeaderAddr() != "" {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("cluster: timed out waiting for leader")
		}
		<-tick.C
	}
}

// Apply submits a command for replication and waits for it to be
// committed across the cluster (Raft majority). Only valid on the leader
// — followers return raft.ErrNotLeader.
func (c *Cluster) Apply(cmd []byte) (any, error) {
	future := c.raft.Apply(cmd, c.cfg.ApplyTimeout)
	if err := future.Error(); err != nil {
		return nil, err
	}
	resp := future.Response()
	if err, ok := resp.(error); ok {
		return nil, err
	}
	return resp, nil
}

// AddVoter adds a peer to the cluster. Only the leader can call this.
func (c *Cluster) AddVoter(p Peer) error {
	future := c.raft.AddVoter(raft.ServerID(p.ID), raft.ServerAddress(p.Addr), 0, 0)
	return future.Error()
}

// RemoveVoter drops a peer from the cluster. Only the leader can call
// this. After removal the peer's Raft instance still owns its local
// state — the operator should Close it (or take down the process) to
// reclaim resources.
//
// The cluster needs a quorum AFTER removal: if the cluster has 3
// voters and one is unreachable, removing one of the two reachable
// voters drops the quorum to 1-of-2 and the cluster stalls. Operators
// are responsible for ordering: remove unreachable peers first, then
// reachable ones.
func (c *Cluster) RemoveVoter(id string) error {
	future := c.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return future.Error()
}

// Members returns the current Raft configuration: every voter's ID
// and address as Raft sees it. Reads from local state so it works on
// any node, not just the leader.
func (c *Cluster) Members() []Peer {
	cfg := c.raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return nil
	}
	servers := cfg.Configuration().Servers
	out := make([]Peer, 0, len(servers))
	for _, s := range servers {
		out = append(out, Peer{ID: string(s.ID), Addr: string(s.Address)})
	}
	return out
}

// Close shuts down Raft, the transport, and the bolt stores. After Close
// the Cluster cannot be reused.
func (c *Cluster) Close() error {
	var firstErr error
	if c.raft != nil {
		if err := c.raft.Shutdown().Error(); err != nil {
			firstErr = err
		}
	}
	if c.transport != nil {
		if err := c.transport.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if c.logStore != nil {
		if err := c.logStore.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if c.stable != nil {
		if err := c.stable.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
