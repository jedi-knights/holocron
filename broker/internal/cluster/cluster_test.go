package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/jedi-knights/holocron/broker/internal/storage"
	"github.com/jedi-knights/holocron/broker/internal/topic"
	"github.com/jedi-knights/holocron/proto"
)

// freePort returns a localhost address with an OS-chosen port. The
// listener is closed before return; tests racing for the same port are
// rare enough at the scales this suite runs.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

type node struct {
	id      string
	dir     string
	store   *storage.MemoryStore
	regsity *topic.Registry
	cluster *Cluster
}

func startCluster(t *testing.T, n int) []*node {
	t.Helper()
	addrs := make([]string, n)
	for i := range n {
		addrs[i] = freePort(t)
	}
	peers := make([]Peer, n)
	for i := range n {
		peers[i] = Peer{ID: fmt.Sprintf("n%d", i+1), Addr: addrs[i]}
	}

	nodes := make([]*node, n)
	for i := range n {
		dir := filepath.Join(t.TempDir(), fmt.Sprintf("n%d", i+1))
		store := storage.NewMemoryStore()
		registry := topic.NewRegistry()
		fsm := NewFSM(store, registry)
		cl, err := New(Config{
			NodeID:    peers[i].ID,
			BindAddr:  addrs[i],
			DataDir:   dir,
			Peers:     peers,
			Bootstrap: i == 0,
		}, fsm)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = &node{
			id:      peers[i].ID,
			dir:     dir,
			store:   store,
			regsity: registry,
			cluster: cl,
		}
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.cluster.Close()
		}
	})
	return nodes
}

// leader scans the cluster, returning the leader node. Fails the test if
// none is found within timeout.
func leader(t *testing.T, nodes []*node, timeout time.Duration) *node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.cluster.IsLeader() {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no leader within %s", timeout)
	return nil
}

func TestCluster_SingleNodeBootstrapElectsLeader(t *testing.T) {
	nodes := startCluster(t, 1)
	if err := nodes[0].cluster.WaitForLeader(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	if !nodes[0].cluster.IsLeader() {
		t.Fatal("single-node cluster did not elect itself")
	}
}

func TestCluster_ThreeNodeReplicatesAppends(t *testing.T) {
	nodes := startCluster(t, 3)
	for _, n := range nodes {
		if err := n.cluster.WaitForLeader(5 * time.Second); err != nil {
			t.Fatal(err)
		}
	}
	ldr := leader(t, nodes, 5*time.Second)

	// Create a topic via the FSM so partitions exist on every node.
	if _, err := ldr.cluster.Apply(EncodeCreateTopic(CreateTopicCommand{
		Name:           "events",
		PartitionCount: 1,
	})); err != nil {
		t.Fatal(err)
	}

	const records = 5
	for i := range records {
		_, err := ldr.cluster.Apply(EncodeAppend(AppendCommand{
			Topic:     "events",
			Partition: 0,
			Record: proto.Record{
				Key:   []byte{byte(i)},
				Value: []byte("payload"),
			},
		}))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Every node's local store should now hold all 5 records on partition 0.
	deadline := time.Now().Add(5 * time.Second)
	for _, n := range nodes {
		var hw int64
		for time.Now().Before(deadline) {
			hw, _ = n.store.HighWater(context.Background(), proto.PartitionRef{Topic: "events", Index: 0})
			if hw == records {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if hw != records {
			t.Errorf("node %s high water = %d, want %d", n.id, hw, records)
		}
	}
}

func TestCluster_FollowerRejectsApply(t *testing.T) {
	nodes := startCluster(t, 3)
	for _, n := range nodes {
		if err := n.cluster.WaitForLeader(5 * time.Second); err != nil {
			t.Fatal(err)
		}
	}
	var follower *node
	for _, n := range nodes {
		if !n.cluster.IsLeader() {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}
	_, err := follower.cluster.Apply(EncodeCreateTopic(CreateTopicCommand{Name: "x", PartitionCount: 1}))
	if err == nil {
		t.Fatal("follower accepted Apply; expected error")
	}
}

// TestCluster_RemoveVoterShrinksMembership proves the leader's
// RemoveVoter call permanently drops a peer from the configuration:
// the remaining members no longer list it, and a new Apply still
// commits with the smaller quorum.
func TestCluster_RemoveVoterShrinksMembership(t *testing.T) {
	// Arrange — three-node cluster, leader identified.
	nodes := startCluster(t, 3)
	for _, n := range nodes {
		if err := n.cluster.WaitForLeader(5 * time.Second); err != nil {
			t.Fatal(err)
		}
	}
	ldr := leader(t, nodes, 5*time.Second)

	// Pick a follower to evict.
	var victim *node
	for _, n := range nodes {
		if n != ldr {
			victim = n
			break
		}
	}
	if victim == nil {
		t.Fatal("no follower to evict")
	}

	// Act
	if err := ldr.cluster.RemoveVoter(victim.id); err != nil {
		t.Fatal(err)
	}

	// Wait briefly for the configuration change to commit.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		members := ldr.cluster.Members()
		if !containsID(members, victim.id) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Assert — membership shrunk and Apply still works.
	members := ldr.cluster.Members()
	if containsID(members, victim.id) {
		t.Fatalf("evicted node still in membership: %v", members)
	}
	if len(members) != 2 {
		t.Errorf("expected 2 voters after removal, got %d", len(members))
	}
	if _, err := ldr.cluster.Apply(EncodeCreateTopic(CreateTopicCommand{
		Name:           "post-evict",
		PartitionCount: 1,
	})); err != nil {
		t.Errorf("Apply after RemoveVoter failed: %v", err)
	}
}

func containsID(peers []Peer, id string) bool {
	for _, p := range peers {
		if p.ID == id {
			return true
		}
	}
	return false
}

// TestFSM_SnapshotRestoreRebuildsRegistry proves the FSM's snapshot
// payload captures every registered topic, and Restore on a fresh
// registry rebuilds the same view.
func TestFSM_SnapshotRestoreRebuildsRegistry(t *testing.T) {
	// Arrange — populate a registry through the FSM's Apply path.
	store := storage.NewMemoryStore()
	reg := topic.NewRegistry()
	fsm := NewFSM(store, reg)
	for _, name := range []string{"orders", "events", "shipments"} {
		if r := fsm.Apply(&raft.Log{
			Data: EncodeCreateTopic(CreateTopicCommand{Name: name, PartitionCount: 2}),
		}); r != nil {
			if err, ok := r.(error); ok {
				t.Fatalf("apply %s: %v", name, err)
			}
		}
	}

	// Act — snapshot, then restore into a fresh registry.
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	freshStore := storage.NewMemoryStore()
	freshReg := topic.NewRegistry()
	freshFSM := NewFSM(freshStore, freshReg)
	if err := freshFSM.Restore(io.NopCloser(bytes.NewReader(sink.bytes()))); err != nil {
		t.Fatal(err)
	}

	// Assert — every original topic is back.
	for _, name := range []string{"orders", "events", "shipments"} {
		n, err := freshReg.PartitionsFor(name)
		if err != nil {
			t.Errorf("topic %q missing after restore: %v", name, err)
			continue
		}
		if n != 2 {
			t.Errorf("topic %q partitions: got %d, want 2", name, n)
		}
	}
}

// memSink is a zero-config raft.SnapshotSink for tests.
type memSink struct {
	buf      []byte
	cancelled bool
	closed    bool
}

func (s *memSink) Write(p []byte) (int, error) {
	s.buf = append(s.buf, p...)
	return len(p), nil
}
func (s *memSink) Close() error          { s.closed = true; return nil }
func (s *memSink) Cancel() error         { s.cancelled = true; return nil }
func (s *memSink) ID() string            { return "test-sink" }
func (s *memSink) bytes() []byte         { return s.buf }

// raftTLSConfig builds a self-signed cert + matching tls.Config that
// works for both Listen and Dial against 127.0.0.1. Used to drive the
// TLS-on-Raft test without needing on-disk fixtures.
func raftTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "holocron-raft-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	cert := tls.Certificate{Certificate: [][]byte{derBytes}, PrivateKey: priv}
	parsed, err := x509.ParseCertificate(derBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   "holocron-raft-test",
		MinVersion:   tls.VersionTLS12,
	}
}

// TestCluster_TLSTransport_BootstrapsWithTLS proves a single-node
// cluster wired up with a TLS Raft transport elects itself leader —
// confirming the TLS stream layer accepts inbound connections and the
// node can dial itself for things like leadership probes.
func TestCluster_TLSTransport_BootstrapsWithTLS(t *testing.T) {
	// Arrange
	addr := freePort(t)
	dir := t.TempDir()
	store := storage.NewMemoryStore()
	registry := topic.NewRegistry()
	fsm := NewFSM(store, registry)

	cl, err := New(Config{
		NodeID:    "tls-1",
		BindAddr:  addr,
		DataDir:   dir,
		Peers:     []Peer{{ID: "tls-1", Addr: addr}},
		Bootstrap: true,
		TLSConfig: raftTLSConfig(t),
	}, fsm)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	// Act / Assert
	if err := cl.WaitForLeader(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	if !cl.IsLeader() {
		t.Fatal("TLS-transport node failed to elect itself leader")
	}

	// And exercise the FSM through the TLS transport.
	if _, err := cl.Apply(EncodeCreateTopic(CreateTopicCommand{
		Name:           "events",
		PartitionCount: 1,
	})); err != nil {
		t.Fatalf("Apply over TLS transport failed: %v", err)
	}
	if _, err := registry.PartitionsFor("events"); err != nil {
		t.Errorf("topic missing after TLS Apply: %v", err)
	}
}
