package embed_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/broker/internal/tlsconfig/tlstest"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
	holocronnet "github.com/jedi-knights/holocron/sdk/net"
)

// TestEmbed_ClusterTLS_LeaderElection brings up a 3-node cluster whose
// Raft transport is wrapped in TLS via embed.ClusterConfig.TLSConfig and
// asserts that leadership election still completes. Without the new
// TLSConfig field on embed.ClusterConfig (PR 4 of the TLS wave), this
// test will not compile.
func TestEmbed_ClusterTLS_LeaderElection(t *testing.T) {
	// Arrange
	tlsCfg := raftClusterTLSConfig(t)

	raftAddrs := make([]string, 3)
	wireAddrs := make([]string, 3)
	for i := range 3 {
		raftAddrs[i] = mustFreePort(t)
		wireAddrs[i] = mustFreePort(t)
	}
	peers := []embed.ClusterPeer{
		{ID: "tls-n1", Addr: raftAddrs[0], WireAddr: wireAddrs[0]},
		{ID: "tls-n2", Addr: raftAddrs[1], WireAddr: wireAddrs[1]},
		{ID: "tls-n3", Addr: raftAddrs[2], WireAddr: wireAddrs[2]},
	}

	brokers := make([]*embed.Broker, 3)
	for i := range 3 {
		b, err := embed.NewDisk(t.TempDir(), embed.WithCluster(embed.ClusterConfig{
			NodeID:    peers[i].ID,
			BindAddr:  raftAddrs[i],
			Peers:     peers,
			Bootstrap: i == 0,
			TLSConfig: tlsCfg.Clone(),
		}))
		if err != nil {
			t.Fatalf("NewDisk node %d: %v", i, err)
		}
		brokers[i] = b
		t.Cleanup(func() { _ = b.Close() })
	}

	// Act
	for i, b := range brokers {
		if err := b.WaitForLeader(10 * time.Second); err != nil {
			t.Fatalf("node %d WaitForLeader: %v", i, err)
		}
	}

	// Assert: exactly one node believes it is the leader.
	leaderCount := 0
	for _, b := range brokers {
		if b.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Errorf("expected exactly 1 leader, got %d", leaderCount)
	}
}

// TestEmbed_ClusterTLS_ProduceReplicates extends the leader-election
// test by performing a real produce against the leader's wire port and
// verifying the FSM applies on every node. This validates that the TLS
// transport is healthy enough to carry application traffic, not just
// election messages.
func TestEmbed_ClusterTLS_ProduceReplicates(t *testing.T) {
	// Arrange
	tlsCfg := raftClusterTLSConfig(t)

	raftAddrs := make([]string, 3)
	wireAddrs := make([]string, 3)
	for i := range 3 {
		raftAddrs[i] = mustFreePort(t)
		wireAddrs[i] = mustFreePort(t)
	}
	peers := []embed.ClusterPeer{
		{ID: "rep-n1", Addr: raftAddrs[0], WireAddr: wireAddrs[0]},
		{ID: "rep-n2", Addr: raftAddrs[1], WireAddr: wireAddrs[1]},
		{ID: "rep-n3", Addr: raftAddrs[2], WireAddr: wireAddrs[2]},
	}

	brokers := make([]*embed.Broker, 3)
	for i := range 3 {
		b, err := embed.NewDisk(t.TempDir(), embed.WithCluster(embed.ClusterConfig{
			NodeID:    peers[i].ID,
			BindAddr:  raftAddrs[i],
			Peers:     peers,
			Bootstrap: i == 0,
			TLSConfig: tlsCfg.Clone(),
		}))
		if err != nil {
			t.Fatalf("NewDisk node %d: %v", i, err)
		}
		brokers[i] = b
		t.Cleanup(func() { _ = b.Close() })
		if _, err := b.Listen(wireAddrs[i]); err != nil {
			t.Fatalf("Listen node %d: %v", i, err)
		}
	}

	for i, b := range brokers {
		if err := b.WaitForLeader(10 * time.Second); err != nil {
			t.Fatalf("node %d WaitForLeader: %v", i, err)
		}
	}

	var leaderWire string
	for i, b := range brokers {
		if b.IsLeader() {
			leaderWire = wireAddrs[i]
			break
		}
	}
	if leaderWire == "" {
		t.Fatal("no leader found")
	}

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tr, err := holocronnet.Dial(leaderWire)
	if err != nil {
		t.Fatalf("Dial leader wire: %v", err)
	}
	defer tr.Close()

	if err := tr.CreateTopic(ctx, "events", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	p, err := sdk.NewProducer(tr)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	if _, err := p.Send(ctx, "events", proto.Record{Value: []byte("hello")}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Assert: every node's local registry sees the topic (FSM applied).
	deadline := time.Now().Add(5 * time.Second)
	for i, b := range brokers {
		for time.Now().Before(deadline) && len(b.Topics()) == 0 {
			time.Sleep(50 * time.Millisecond)
		}
		if len(b.Topics()) == 0 {
			t.Errorf("node %d did not apply CreateTopic via TLS Raft", i)
		}
	}
}

// raftClusterTLSConfig builds a self-signed TLS config suitable for
// inter-node Raft traffic. The same cert acts as server and client cert
// (each node both listens and dials), and as the trust root (since it
// is self-signed). MinVersion is TLS 1.3 to mirror the production
// default; ServerName is "localhost" to match the SAN baked into the
// cert by tlstest.GenerateCertPair.
func raftClusterTLSConfig(t *testing.T) *tls.Config {
	t.Helper()

	certPath, keyPath, caPath := tlstest.GenerateCertPair(t)

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}

	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatalf("read CA: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("failed to add CA to pool")
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS13,
	}
}
