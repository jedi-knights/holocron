package embed_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
	holocronnet "github.com/jedi-knights/holocron/sdk/net"
)

// TestEndToEnd is the Stage 1 acceptance test: producer → broker → consumer
// through the public surface only.
func TestEndToEnd(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()

	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}

	producer, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	consumer, err := sdk.NewConsumer(b.Transport(), sdk.WithGroup("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := consumer.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}

	const n = 10
	for i := range n {
		_, err := producer.Send(ctx, "events", proto.Record{
			Key:   []byte{byte(i)},
			Value: []byte("payload"),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	got := 0
	for got < n {
		records, err := consumer.Poll(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
		got += len(records)
	}
	if got != n {
		t.Fatalf("got %d records, want %d", got, n)
	}
}

func TestCreateTopicRejectsDuplicates(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "t", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "t", PartitionCount: 1}); err == nil {
		t.Fatal("expected error on duplicate create")
	}
}

func TestListen_NetworkRoundTrip(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tr, err := holocronnet.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	producer, err := sdk.NewProducer(tr)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	consumer, err := sdk.NewConsumer(tr, sdk.WithGroup("net-test"))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	if err := consumer.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}

	const n = 8
	for i := range n {
		_, err := producer.Send(ctx, "events", proto.Record{
			Key:   []byte{byte(i)},
			Value: []byte("payload"),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	got := 0
	for got < n {
		records, err := consumer.Poll(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
		got += len(records)
	}
	if got != n {
		t.Fatalf("got %d records over the network, want %d", got, n)
	}
}

func TestListen_RejectsVersionMismatch(t *testing.T) {
	// Sanity check that the handshake actually rejects a wrong version.
	// Done by constructing a transport that lies about its version is not
	// practical without exporting the internals; instead we rely on the
	// happy-path test above and the unit tests in proto/ to cover the
	// version-mismatch branch via WriteErrorResponse + ReadResponse.
	t.Skip("covered by proto/wire_test.go and server unit tests")
}

func TestConsumerGroup_SharesPartitionsWithoutOverlap(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 4}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	producer, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	// Subscribe both consumers before producing, so neither consumer ever
	// sees the period when the group had a single member with the full
	// partition set. This isolates the "no overlap during steady state"
	// property the test means to assert.
	c1, err := sdk.NewConsumer(b.Transport(),
		sdk.WithGroup("test"),
		sdk.WithHeartbeatInterval(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := sdk.NewConsumer(b.Transport(),
		sdk.WithGroup("test"),
		sdk.WithHeartbeatInterval(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	if err := c1.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}
	if err := c2.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}
	// Let c1 discover the rebalance triggered by c2's join.
	time.Sleep(300 * time.Millisecond)

	const total = 40
	for i := range total {
		_, err := producer.Send(ctx, "events", proto.Record{
			Key:   []byte{byte(i)},
			Value: []byte("payload"),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Each consumer drains its assigned partitions; combined they must see
	// every record exactly once. Dedup by record key (offsets are
	// partition-local, so the same offset legitimately exists in multiple
	// partitions).
	collect := func(c *sdk.Consumer) map[byte]bool {
		seen := make(map[byte]bool)
		drainCtx, drainCancel := context.WithTimeout(ctx, 3*time.Second)
		defer drainCancel()
		for {
			records, err := c.Poll(drainCtx, 32)
			if err != nil {
				return seen
			}
			for _, r := range records {
				if len(r.Key) == 1 {
					seen[r.Key[0]] = true
				}
			}
			if drainCtx.Err() != nil {
				return seen
			}
			if len(records) == 0 {
				return seen
			}
		}
	}

	doneA := make(chan map[byte]bool, 1)
	doneB := make(chan map[byte]bool, 1)
	go func() { doneA <- collect(c1) }()
	go func() { doneB <- collect(c2) }()
	a := <-doneA
	b2 := <-doneB

	combined := make(map[byte]struct{})
	for k := range a {
		combined[k] = struct{}{}
		if _, dup := b2[k]; dup {
			t.Errorf("key %d delivered to both consumers", k)
		}
	}
	for k := range b2 {
		combined[k] = struct{}{}
	}
	if len(a) == 0 || len(b2) == 0 {
		t.Fatalf("uneven distribution: c1=%d c2=%d", len(a), len(b2))
	}
	if len(combined) != total {
		t.Fatalf("combined coverage: got %d, want %d", len(combined), total)
	}
}

func TestConsumerGroup_CommitSurvivesRestart(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	p, _ := sdk.NewProducer(b.Transport())
	defer p.Close()
	for i := range 10 {
		_, _ = p.Send(ctx, "events", proto.Record{Value: []byte{byte(i)}})
	}

	// Commit offset 5 explicitly.
	if err := b.Transport().Commit(ctx, "g", proto.PartitionRef{Topic: "events", Index: 0}, 5); err != nil {
		t.Fatal(err)
	}

	// New consumer in same group should resume from offset 5.
	c, err := sdk.NewConsumer(b.Transport(), sdk.WithGroup("g"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}

	got := 0
	firstOffset := int64(-1)
	for got < 5 {
		records, err := c.Poll(ctx, 5)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range records {
			if firstOffset < 0 {
				firstOffset = r.Offset
			}
			got++
		}
	}
	if firstOffset != 5 {
		t.Fatalf("first record after commit: got offset %d, want 5", firstOffset)
	}
}

func TestCluster_ThreeNodeReplicatesViaNetworkSDK(t *testing.T) {
	// Allocate three Raft + three wire ports up front.
	raftAddrs := make([]string, 3)
	wireAddrs := make([]string, 3)
	for i := range 3 {
		raftAddrs[i] = mustFreePort(t)
		wireAddrs[i] = mustFreePort(t)
	}
	peers := []embed.ClusterPeer{
		{ID: "n1", Addr: raftAddrs[0], WireAddr: wireAddrs[0]},
		{ID: "n2", Addr: raftAddrs[1], WireAddr: wireAddrs[1]},
		{ID: "n3", Addr: raftAddrs[2], WireAddr: wireAddrs[2]},
	}

	dirs := make([]string, 3)
	for i := range 3 {
		dirs[i] = t.TempDir()
	}

	brokers := make([]*embed.Broker, 3)
	for i := range 3 {
		b, err := embed.NewDisk(dirs[i], embed.WithCluster(embed.ClusterConfig{
			NodeID:    peers[i].ID,
			BindAddr:  raftAddrs[i],
			Peers:     peers,
			Bootstrap: i == 0,
		}))
		if err != nil {
			t.Fatal(err)
		}
		brokers[i] = b
		t.Cleanup(func() { _ = b.Close() })
		if _, err := b.Listen(wireAddrs[i]); err != nil {
			t.Fatal(err)
		}
	}

	for _, b := range brokers {
		if err := b.WaitForLeader(10 * time.Second); err != nil {
			t.Fatal(err)
		}
	}

	// Connect the SDK to a follower; redirect to leader should kick in.
	var followerWire string
	for i, b := range brokers {
		if !b.IsLeader() {
			followerWire = wireAddrs[i]
			break
		}
	}
	if followerWire == "" {
		t.Fatal("no follower found")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tr, err := holocronnet.Dial(followerWire)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	if err := tr.CreateTopic(ctx, "events", 1); err != nil {
		t.Fatal(err)
	}

	producer, err := sdk.NewProducer(tr)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	const records = 5
	for i := range records {
		_, err := producer.Send(ctx, "events", proto.Record{
			Key:   []byte{byte(i)},
			Value: []byte("x"),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Every node's local store should have the records.
	deadline := time.Now().Add(5 * time.Second)
	for _, b := range brokers {
		topics := b.Topics()
		if len(topics) == 0 {
			// Wait briefly for FSM apply to land on this node.
			for time.Now().Before(deadline) && len(b.Topics()) == 0 {
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
	for _, b := range brokers {
		if len(b.Topics()) == 0 {
			t.Errorf("topic not replicated to a node")
		}
	}
}

func mustFreePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// TestConsumer_Run_HandlerErrorStopsLoop verifies Run propagates a
// handler's error and stops the loop.
func TestConsumer_Run_HandlerErrorStopsLoop(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	p, _ := sdk.NewProducer(b.Transport())
	defer p.Close()
	pctx, pcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pcancel()
	for _, v := range []string{"a", "b"} {
		if _, err := p.Send(pctx, "events", proto.Record{Value: []byte(v)}); err != nil {
			t.Fatal(err)
		}
	}

	c, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}

	// Act
	want := errors.New("handler said no")
	got := c.Run(ctx, 32, func(_ context.Context, _ []proto.Record) error {
		return want
	})

	// Assert
	if !errors.Is(got, want) {
		t.Fatalf("Run error: got %v, want %v", got, want)
	}
}

// TestConsumer_Run_ContextCancelStopsCleanly: cancelling the ctx
// returns a nil error.
func TestConsumer_Run_ContextCancelStopsCleanly(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	c, _ := sdk.NewConsumer(b.Transport())
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := c.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}

	if err := c.Run(ctx, 32, func(_ context.Context, _ []proto.Record) error { return nil }); err != nil {
		t.Fatalf("Run returned %v on ctx cancel; want nil", err)
	}
}

func TestConsumer_RevokeListenerFiresBeforeRebalance(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 4}); err != nil {
		t.Fatal(err)
	}

	revokedAt := make(chan []proto.PartitionRef, 1)

	c1, err := sdk.NewConsumer(b.Transport(),
		sdk.WithGroup("revoke-test"),
		sdk.WithHeartbeatInterval(50*time.Millisecond),
		sdk.WithRevokeListener(func(_ context.Context, parts []proto.PartitionRef) error {
			select {
			case revokedAt <- append([]proto.PartitionRef(nil), parts...):
			default:
			}
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	// Act: c1 joins alone (gets all 4 partitions). c2 joining triggers
	// a rebalance; c1's heartbeat sees RebalanceNeeded and runs rejoin,
	// which must fire the revoke listener with c1's stale assignment.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c1.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}

	c2, err := sdk.NewConsumer(b.Transport(),
		sdk.WithGroup("revoke-test"),
		sdk.WithHeartbeatInterval(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if err := c2.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}

	// Assert
	select {
	case revoked := <-revokedAt:
		if len(revoked) != 4 {
			t.Fatalf("revoke listener saw %d partitions; expected the full pre-rebalance set of 4", len(revoked))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("revoke listener never fired within 2s")
	}
}

// TestListen_TLS_RoundTrip: broker accepts TLS connections; an SDK
// transport configured with a matching root pool can produce + consume.
func TestListen_TLS_RoundTrip(t *testing.T) {
	// Arrange: generate a self-signed cert.
	serverCfg, clientCfg := selfSignedTLS(t)

	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0", embed.WithTLS(serverCfg))
	if err != nil {
		t.Fatal(err)
	}

	tr, err := holocronnet.Dial(addr, holocronnet.WithTLS(clientCfg))
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	p, err := sdk.NewProducer(tr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Send(ctx, "events", proto.Record{Value: []byte("over-tls")}); err != nil {
		t.Fatal(err)
	}

	c, err := sdk.NewConsumer(tr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}
	got, err := c.Poll(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0].Value) != "over-tls" {
		t.Fatalf("TLS round-trip failed: %+v", got)
	}
}

// TestListen_TLS_RejectsPlainClient: a TLS-enabled broker should not
// accept handshakes from a plain TCP client.
func TestListen_TLS_RejectsPlainClient(t *testing.T) {
	serverCfg, _ := selfSignedTLS(t)

	b := embed.NewMemory()
	defer b.Close()
	addr, err := b.Listen("127.0.0.1:0", embed.WithTLS(serverCfg))
	if err != nil {
		t.Fatal(err)
	}

	// Plain Dial — no WithTLS — should fail handshake.
	tr, err := holocronnet.Dial(addr, holocronnet.WithDialTimeout(500*time.Millisecond))
	if err != nil {
		// Some TLS servers refuse the connection at TCP level — that's acceptable.
		return
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err = tr.PartitionsFor(ctx, "any")
	if err == nil {
		t.Fatal("plain client succeeded against TLS broker; expected failure")
	}
}

// selfSignedTLS produces a server tls.Config and a matching client
// tls.Config (with the server's cert in its root pool). All in memory.
func selfSignedTLS(t *testing.T) (server, client *tls.Config) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "holocron-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	cert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  priv,
	}
	server = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	pool := x509.NewCertPool()
	parsed, err := x509.ParseCertificate(derBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool.AddCert(parsed)
	client = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return server, client
}

// TestListen_APIKey_AdmitsValid: a broker configured with an API-key
// allow-list accepts handshakes carrying a matching key.
func TestListen_APIKey_AdmitsValid(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	addr, err := b.Listen("127.0.0.1:0", embed.WithAPIKeys("secret-A", "secret-B"))
	if err != nil {
		t.Fatal(err)
	}

	// Act
	tr, err := holocronnet.Dial(addr, holocronnet.WithAPIKey("secret-A"))
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Assert: a metadata RPC succeeds — handshake passed.
	if err := tr.CreateTopic(ctx, "events", 1); err != nil {
		t.Fatal(err)
	}
}

// TestListen_APIKey_RejectsMissing: a broker with an allow-list rejects
// SDK clients that don't send a key.
func TestListen_APIKey_RejectsMissing(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	addr, err := b.Listen("127.0.0.1:0", embed.WithAPIKeys("secret"))
	if err != nil {
		t.Fatal(err)
	}

	// Dial without WithAPIKey.
	tr, err := holocronnet.Dial(addr, holocronnet.WithDialTimeout(500*time.Millisecond))
	if err != nil {
		// Some servers might tear down the conn at handshake; that's fine too.
		return
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := tr.CreateTopic(ctx, "events", 1); err == nil {
		t.Fatal("unauthenticated client succeeded against API-keyed broker")
	}
}

// TestConsumer_PauseResume proves Pause halts fetching from a
// partition (records produced while paused don't arrive) and
// Resume picks up from the offset just past the last record the
// consumer saw. Useful for backpressure-aware sinks.
func TestConsumer_PauseResume(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	c, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p := proto.PartitionRef{Topic: "events", Index: 0}
	if err := c.Assign(ctx, p, 0); err != nil {
		t.Fatal(err)
	}

	// Act + Assert — produce one record and read it.
	if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte("v0")}); err != nil {
		t.Fatal(err)
	}
	got, err := c.Poll(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0].Value) != "v0" {
		t.Fatalf("first poll: got %v, want one v0", got)
	}

	// Pause and produce more — should not arrive.
	if err := c.Pause(p); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"v1", "v2"} {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte(v)}); err != nil {
			t.Fatal(err)
		}
	}
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	got, _ = c.Poll(pollCtx, 5)
	pollCancel()
	if len(got) > 0 {
		t.Fatalf("paused poll got %d records, want 0", len(got))
	}

	// Resume — the two records produced while paused should now
	// arrive, in order, starting at offset 1.
	if err := c.Resume(ctx, p, 0); err != nil {
		t.Fatal(err)
	}
	got = nil
	for len(got) < 2 {
		recs, err := c.Poll(ctx, 2-len(got))
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, recs...)
	}
	if string(got[0].Value) != "v1" || string(got[1].Value) != "v2" {
		t.Errorf("resume reads: got %q,%q, want v1,v2", got[0].Value, got[1].Value)
	}
}

// TestProducer_IdempotentDedupSurvivesBrokerRestart proves the
// broker's dedup table persists across restart on a disk broker:
// a record produced with (producer-id, seq=0), then a fresh broker
// process opening the same data dir, then a retry of the same
// (producer-id, seq=0) — the retry returns the original offset
// rather than appending a duplicate.
//
// Without persistence (batch 23's in-memory-only state) the broker
// would forget the checkpoint on restart and the retry would land
// at offset 1, breaking exactly-once semantics across restarts.
func TestProducer_IdempotentDedupSurvivesBrokerRestart(t *testing.T) {
	// Arrange — disk broker on a temp data dir.
	dir := t.TempDir()
	b, err := embed.NewDisk(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	withProducer := func(id string, seq uint64, value string) proto.Record {
		var seqBytes [8]byte
		for i := 7; i >= 0; i-- {
			seqBytes[i] = byte(seq)
			seq >>= 8
		}
		return proto.Record{
			Value: []byte(value),
			Headers: []proto.Header{
				{Key: proto.HeaderProducerID, Value: []byte(id)},
				{Key: proto.HeaderProducerSeq, Value: seqBytes[:]},
			},
		}
	}

	prod1, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	off, err := prod1.Send(ctx, "events", withProducer("producer-A", 0, "first"))
	if err != nil {
		t.Fatal(err)
	}
	if off != 0 {
		t.Fatalf("first publish: got offset %d, want 0", off)
	}
	_ = prod1.Close()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// Act — fresh broker over the same data dir.
	b2, err := embed.NewDisk(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	prod2, err := sdk.NewProducer(b2.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod2.Close()

	off2, err := prod2.Send(ctx, "events", withProducer("producer-A", 0, "first"))
	if err != nil {
		t.Fatal(err)
	}

	// Assert — the retry returns the original offset (dedup persisted).
	if off2 != 0 {
		t.Errorf("retry across restart: got offset %d, want 0 — dedup state did not persist", off2)
	}
}

// TestProducer_IdempotentRetryDeduplicates proves the end-to-end
// idempotent-retry path: a Producer constructed with
// WithIdempotency stamps each record with a producer ID + sequence
// number, and the broker dedups retries of an already-applied
// write. The test simulates a retry by calling Send twice with the
// same payload after manually rewinding the producer's per-
// partition sequence counter.
//
// Without this, even with WithIdempotency on, a retry would land
// at a fresh offset and consumers would see two records.
func TestProducer_IdempotentRetryDeduplicates(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	prod, err := sdk.NewProducer(b.Transport(), sdk.WithIdempotency())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Act — first send lands at offset 0.
	off0, err := prod.Send(ctx, "events", proto.Record{Value: []byte("v0")})
	if err != nil {
		t.Fatal(err)
	}
	if off0 != 0 {
		t.Fatalf("first send: got offset %d, want 0", off0)
	}

	// Simulate a retry: rewind the producer's seq counter for this
	// partition and re-send the same record. The broker should
	// recognize the duplicate (matching producer-id + sequence) and
	// return the original offset.
	prod.RewindIdempotencySequence(proto.PartitionRef{Topic: "events", Index: 0})
	off0Retry, err := prod.Send(ctx, "events", proto.Record{Value: []byte("v0")})
	if err != nil {
		t.Fatalf("retry send: %v", err)
	}
	if off0Retry != 0 {
		t.Errorf("retry send: got offset %d, want 0 (broker dedup failed)", off0Retry)
	}

	// A fresh send (now at seq 1 again, which is past the retry's
	// rewound seq) lands at offset 1.
	off1, err := prod.Send(ctx, "events", proto.Record{Value: []byte("v1")})
	if err != nil {
		t.Fatal(err)
	}
	if off1 != 1 {
		t.Fatalf("fresh send: got offset %d, want 1", off1)
	}

	// Assert — only two records in the partition (offset 0 and 1).
	c, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}
	got, err := c.Poll(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("partition contents: got %d records, want 2 (dedup failed)", len(got))
	}
}

// TestDeleteTopic_ClearsRecordsAndAllowsReuse proves a deleted
// topic's records vanish from disk and a re-created topic of the
// same name starts fresh at offset 0. Without this, holocron has
// no inverse for CreateTopic — operationally a glaring gap.
func TestDeleteTopic_ClearsRecordsAndAllowsReuse(t *testing.T) {
	// Arrange — disk broker so we can verify the on-disk dir is gone.
	dir := t.TempDir()
	b, err := embed.NewDisk(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "ephemeral", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tr, err := holocronnet.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Produce two records.
	prod, err := sdk.NewProducer(tr)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	for i := range 2 {
		if _, err := prod.Send(ctx, "ephemeral", proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	// Act — delete the topic.
	if err := tr.DeleteTopic(ctx, "ephemeral"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}

	// Assert — the on-disk topic dir is gone.
	if _, err := os.Stat(filepath.Join(dir, "ephemeral")); !os.IsNotExist(err) {
		t.Errorf("data dir still exists after delete: %v", err)
	}

	// Re-creating the topic and producing should succeed; the new
	// records should start at offset 0 (no historical data).
	if err := tr.CreateTopic(ctx, "ephemeral", 1); err != nil {
		t.Fatalf("recreate after delete: %v", err)
	}
	off, err := prod.Send(ctx, "ephemeral", proto.Record{Value: []byte("fresh")})
	if err != nil {
		t.Fatal(err)
	}
	if off != 0 {
		t.Errorf("first offset after delete + recreate: got %d, want 0", off)
	}
}

// TestACL_EnforcesPerTopicPermissions proves the per-topic
// authorization layer: a key restricted to {Produce: ["allowed"]}
// can publish to "allowed", but produces to "other" and consumes
// from "allowed" both fail with a forbidden error.
//
// Authentication (WithAPIKeys) gates which keys can connect at all;
// authorization (WithACL) gates what each key can do once admitted.
// Without this layer, a single API key would have full access to
// every topic.
func TestACL_EnforcesPerTopicPermissions(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "allowed", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "other", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0",
		embed.WithAPIKeys("k1"),
		embed.WithACL(map[string]embed.ACL{
			"k1": {Produce: []string{"allowed"}},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	tr, err := holocronnet.Dial(addr, holocronnet.WithAPIKey("k1"))
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Act + Assert — produce to allowed: succeeds.
	prod, err := sdk.NewProducer(tr)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	if _, err := prod.Send(ctx, "allowed", proto.Record{Value: []byte("ok")}); err != nil {
		t.Fatalf("produce to allowed topic: %v", err)
	}

	// Produce to other: forbidden.
	if _, err := prod.Send(ctx, "other", proto.Record{Value: []byte("nope")}); err == nil {
		t.Fatal("produce to non-permitted topic succeeded — ACL not enforced")
	}

	// Consume from allowed: forbidden (no consume permission).
	c, err := sdk.NewConsumer(tr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, "allowed", 0); err != nil {
		// Subscribe itself may succeed (it's metadata-only); the
		// poll is where the fetch fires.
		if !isForbidden(err) {
			t.Fatalf("expected forbidden on subscribe, got %v", err)
		}
		return
	}
	if _, err := c.Poll(ctx, 1); err == nil || !isForbidden(err) {
		t.Fatalf("expected forbidden on consume, got %v", err)
	}
}

// isForbidden reports whether err is a wire-protocol forbidden
// status. Used in ACL tests to distinguish authorization failures
// from other RPC errors.
func isForbidden(err error) bool {
	var pe *proto.ProtocolError
	if !errors.As(err, &pe) {
		return false
	}
	return pe.Status == proto.StatusForbidden
}

// TestListenMetrics_ScrapeAfterProduce: produce a record, scrape
// /metrics, verify the produced counter advanced.
func TestListenMetrics_ScrapeAfterProduce(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	addr, err := b.ListenMetrics("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Act: produce 3 records.
	p, _ := sdk.NewProducer(b.Transport())
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for range 3 {
		if _, err := p.Send(ctx, "events", proto.Record{Value: []byte("payload")}); err != nil {
			t.Fatal(err)
		}
	}

	// Assert: scrape /metrics and find the counter at >= 3.
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "holocron_records_produced_total 3") {
		t.Fatalf("expected counter at 3, scrape body:\n%s", body)
	}
	if !strings.Contains(string(body), "# TYPE holocron_records_produced_total counter") {
		t.Errorf("missing TYPE line in scrape body")
	}
}

func TestProducer_AcksDurable_RoundTripsAgainstDiskBroker(t *testing.T) {
	dir := t.TempDir()
	b, err := embed.NewDisk(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	producer, err := sdk.NewProducer(b.Transport(), sdk.WithAcks(sdk.AcksDurable))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range 5 {
		if _, err := producer.Send(ctx, "events", proto.Record{
			Key:   []byte{byte(i)},
			Value: []byte("durable"),
		}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
}

func TestNewDisk_PersistsTopicsAndDataAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	{
		b, err := embed.NewDisk(dir, embed.WithSegmentBytes(1024))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
			t.Fatal(err)
		}

		producer, err := sdk.NewProducer(b.Transport())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		for i := range 5 {
			if _, err := producer.Send(ctx, "events", proto.Record{
				Key:   []byte{byte('a' + i)},
				Value: []byte("payload"),
			}); err != nil {
				t.Fatal(err)
			}
		}
		_ = producer.Close()
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}

	b, err := embed.NewDisk(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// Look for "events" specifically — internal topics like
	// __holocron_offsets also appear here. (b.Topics() returns a
	// fresh snapshot each call; do one pass and capture by value.)
	var found bool
	for _, c := range b.Topics() {
		if c.Name == "events" && c.PartitionCount == 2 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("topic metadata not recovered: %+v", b.Topics())
	}

	consumer, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := consumer.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}

	got := 0
	for got < 5 {
		records, err := consumer.Poll(ctx, 5)
		if err != nil {
			t.Fatal(err)
		}
		got += len(records)
	}
	if got != 5 {
		t.Fatalf("got %d records after restart, want 5", got)
	}
}

// TestQuota_RejectsOverQuotaProduce proves WithQuotas applies a
// per-API-key produce-bandwidth limit: once the key's bucket is
// depleted, subsequent produces return StatusRateLimited until the
// bucket replenishes at the configured rate.
func TestQuota_RejectsOverQuotaProduce(t *testing.T) {
	// Arrange — a 100 byte/sec quota with default 100-byte burst.
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0",
		embed.WithAPIKeys("key-A"),
		embed.WithQuotas(map[string]embed.Quota{
			"key-A": {ProduceBytesPerSec: 100},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	tr, err := holocronnet.Dial(addr, holocronnet.WithAPIKey("key-A"))
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	producer, err := sdk.NewProducer(tr)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Act — first 50-byte produce fits inside the burst; second
	// 80-byte produce exceeds the remaining 50 → expect rate-limited.
	if _, err := producer.Send(ctx, "events", proto.Record{
		Value: make([]byte, 50),
	}); err != nil {
		t.Fatalf("first produce: unexpected %v", err)
	}
	_, err = producer.Send(ctx, "events", proto.Record{
		Value: make([]byte, 80),
	})

	// Assert
	if err == nil {
		t.Fatal("expected rate-limited error from over-quota produce, got nil")
	}
	var pe *proto.ProtocolError
	if !errors.As(err, &pe) || pe.Status != proto.StatusRateLimited {
		t.Fatalf("status: got %v, want StatusRateLimited", err)
	}
}

// TestQuota_FetchSurfacesRateLimitedThroughPoll proves the consumer's
// pump now propagates a wire-level StatusRateLimited from the broker
// up through Poll. Before batch 15 this error was silently swallowed
// and Poll blocked until ctx-cancellation.
func TestQuota_FetchSurfacesRateLimitedThroughPoll(t *testing.T) {
	// Arrange — quota-free producer fills the topic; consumer's tiny
	// 10-byte/sec bucket trips on its first poll.
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0",
		embed.WithAPIKeys("producer", "consumer"),
		embed.WithQuotas(map[string]embed.Quota{
			"consumer": {FetchBytesPerSec: 10},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	prodTr, err := holocronnet.Dial(addr, holocronnet.WithAPIKey("producer"))
	if err != nil {
		t.Fatal(err)
	}
	defer prodTr.Close()
	producer, err := sdk.NewProducer(prodTr)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := producer.Send(ctx, "events", proto.Record{Value: make([]byte, 80)}); err != nil {
		t.Fatal(err)
	}

	// Act
	consTr, err := holocronnet.Dial(addr, holocronnet.WithAPIKey("consumer"))
	if err != nil {
		t.Fatal(err)
	}
	defer consTr.Close()
	consumer, err := sdk.NewConsumer(consTr)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if err := consumer.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}

	// Drain whatever Poll returns first; the rate-limit error follows
	// when the next fetch's response exceeds the bucket.
	deadline := time.Now().Add(2 * time.Second)
	var pe *proto.ProtocolError
	for time.Now().Before(deadline) {
		_, perr := consumer.Poll(ctx, 10)
		if errors.As(perr, &pe) && pe.Status == proto.StatusRateLimited {
			break
		}
	}

	// Assert
	if pe == nil || pe.Status != proto.StatusRateLimited {
		t.Fatalf("expected fetch quota rate-limit through Poll, got pe=%v", pe)
	}
}

// TestStickyMemberID_NoChurnAcrossRestart proves that a consumer
// constructed with WithMemberID, closed, and reopened with the same ID
// keeps its assignment without bumping the broker's group generation.
// The sticky-member optimization makes the restart effectively free
// for any other consumers in the same group.
func TestStickyMemberID_NoChurnAcrossRestart(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	tr, err := holocronnet.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First consumer with sticky ID — capture initial generation.
	c1, err := sdk.NewConsumer(tr, sdk.WithGroup("sticky"), sdk.WithMemberID("worker-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c1.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}
	gen1, err := tr.JoinGroup(ctx, "sticky", "worker-1", []string{"events"})
	if err != nil {
		t.Fatal(err)
	}
	_ = c1.Close()

	// Act — fresh consumer with same sticky ID.
	c2, err := sdk.NewConsumer(tr, sdk.WithGroup("sticky"), sdk.WithMemberID("worker-1"))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if err := c2.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}
	gen2, err := tr.JoinGroup(ctx, "sticky", "worker-1", []string{"events"})
	if err != nil {
		t.Fatal(err)
	}

	// Assert — generation didn't bump because the same member returned
	// with the same topic set.
	if gen2.Generation != gen1.Generation {
		t.Errorf("sticky restart bumped generation: %d → %d", gen1.Generation, gen2.Generation)
	}
	if len(gen2.Assignments) != len(gen1.Assignments) {
		t.Errorf("assignment size changed across restart: %d → %d",
			len(gen1.Assignments), len(gen2.Assignments))
	}
}

// TestNetwork_HighWater_RoundTrip proves the OpHighWater wire op
// reports the next-to-be-appended offset over the network.
func TestNetwork_HighWater_RoundTrip(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	tr, err := holocronnet.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Empty topic: high-water is 0.
	hw, err := tr.HighWater(ctx, proto.PartitionRef{Topic: "events", Index: 0})
	if err != nil {
		t.Fatal(err)
	}
	if hw != 0 {
		t.Errorf("empty topic HighWater: got %d, want 0", hw)
	}

	// Produce 3 records, expect HighWater=3.
	producer, err := sdk.NewProducer(tr)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	for i := range 3 {
		if _, err := producer.Send(ctx, "events", proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	hw, err = tr.HighWater(ctx, proto.PartitionRef{Topic: "events", Index: 0})
	if err != nil {
		t.Fatal(err)
	}
	if hw != 3 {
		t.Errorf("HighWater after 3 produces: got %d, want 3", hw)
	}
}

// TestHeartbeat_LongPollPushedRebalance proves the server-pushed
// rebalance signal: an existing group member running a long-poll
// heartbeat receives RebalanceNeeded the moment a peer joins,
// without waiting for the next ticker interval.
//
// The consumer's heartbeat interval (which is also the long-poll
// deadline) is 5 seconds. A peer joins at T+50ms. The first member's
// AssignFunc fires twice — once on Subscribe, once after the
// rebalance — and the second fire must arrive far before the
// 5-second deadline. Without server push the second fire would
// only happen after the 5s heartbeat tick.
func TestHeartbeat_LongPollPushedRebalance(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 4}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	tr1, err := holocronnet.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr1.Close()
	tr2, err := holocronnet.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr2.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// reassigned fires every time c1's group assignment changes.
	reassigned := make(chan int, 4)
	c1, err := sdk.NewConsumer(tr1,
		sdk.WithGroup("push-test"),
		sdk.WithHeartbeatInterval(5*time.Second),
		sdk.WithAssignListener(func(_ context.Context, parts []proto.PartitionRef) error {
			select {
			case reassigned <- len(parts):
			default:
			}
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	if err := c1.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}

	// Drain the initial-assign signal (4 partitions on a sole member).
	select {
	case n := <-reassigned:
		if n != 4 {
			t.Fatalf("initial assignment: got %d partitions, want 4", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("initial assignment never fired")
	}

	// Act — second consumer joins after a brief delay. With server
	// push this triggers a rebalance, which c1's long-poll heartbeat
	// observes near-instantly and rejoins.
	start := time.Now()
	go func() {
		time.Sleep(50 * time.Millisecond)
		c2, err := sdk.NewConsumer(tr2,
			sdk.WithGroup("push-test"),
			sdk.WithHeartbeatInterval(5*time.Second),
		)
		if err != nil {
			return
		}
		_ = c2.Subscribe(ctx, "events", 0)
		<-ctx.Done()
		_ = c2.Close()
	}()

	// Assert — c1 reassigns within 1 second (well under the 5s
	// heartbeat deadline). On the new generation it should hold half
	// of the four partitions.
	select {
	case n := <-reassigned:
		elapsed := time.Since(start)
		if elapsed > 1*time.Second {
			t.Errorf("rebalance arrived in %v — should be near-instant via server push, not heartbeat-deadline (5s)", elapsed)
		}
		if n != 2 {
			t.Errorf("post-rebalance partitions: got %d, want 2", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("rebalance did not propagate to c1 within 2s — server-pushed signal not delivered")
	}
}

// TestBootstrap_IncludesActiveSegment proves the recipient observes
// every record the donor had at snapshot time — including the records
// still in the donor's currently-open active segment.
//
// Without active-segment inclusion (the batch-21 limit), only the
// sealed-segment prefix would transfer, leaving the recipient with
// fewer records than the donor. Batch 22's chunked snapshot flow
// captures the active segment's size under the partition's mu and
// streams those bytes too, so the recipient sees the donor's full
// snapshot-time state.
func TestBootstrap_IncludesActiveSegment(t *testing.T) {
	// Arrange — donor with disk store, small segment-roll threshold,
	// and a partial-segment's worth of records appended last so a
	// non-empty active segment exists at snapshot time. Each record
	// frames to 63 bytes, segmentSize=256 holds 5 records, so 103
	// records fill 20 sealed segments and leave 3 in the active
	// segment — the records the test asserts must transfer.
	const totalRecords = 103
	donorDir := t.TempDir()
	donor, err := embed.NewDisk(donorDir, embed.WithSegmentBytes(256))
	if err != nil {
		t.Fatal(err)
	}
	defer donor.Close()
	if err := donor.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	donorAddr, err := donor.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prod, err := sdk.NewProducer(donor.Transport())
	if err != nil {
		t.Fatal(err)
	}
	for i := range totalRecords {
		if _, err := prod.Send(ctx, "events", proto.Record{
			Key:   []byte{byte(i)},
			Value: []byte("xxxxxxxxxxxxxxxxxxxxxxxxxx"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	_ = prod.Close()

	// Act — bootstrap recipient from donor over the wire.
	recipientDir := t.TempDir()
	tr, err := holocronnet.Dial(donorAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	p := proto.PartitionRef{Topic: "events", Index: 0}
	if _, err := embed.BootstrapPartitionFromPeer(ctx, tr, recipientDir, p); err != nil {
		t.Fatalf("BootstrapPartitionFromPeer: %v", err)
	}

	// Open recipient against the seeded dir.
	recipient, err := embed.NewDisk(recipientDir, embed.WithSegmentBytes(256))
	if err != nil {
		t.Fatal(err)
	}
	defer recipient.Close()
	if err := recipient.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	// Assert — every record the donor had is now readable from the
	// recipient. The full count proves the active segment transferred,
	// not just the sealed prefix.
	consumer, err := sdk.NewConsumer(recipient.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if err := consumer.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}
	got := make([]proto.Record, 0, totalRecords)
	for len(got) < totalRecords {
		recs, err := consumer.Poll(ctx, totalRecords-len(got))
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, recs...)
	}
	if len(got) != totalRecords {
		t.Fatalf("recipient saw %d records, want %d — active segment did not transfer", len(got), totalRecords)
	}
	for i, r := range got {
		if r.Offset != int64(i) {
			t.Fatalf("record %d: offset=%d, want %d", i, r.Offset, i)
		}
	}
}

// TestBootstrap_PartitionFromPeerOverWire proves a brand-new broker
// can seed its data dir from an existing peer's sealed segments,
// then serve those records itself — the foundation of the cluster
// follower bootstrap.
func TestBootstrap_PartitionFromPeerOverWire(t *testing.T) {
	// Arrange — donor on disk with a small segment-roll threshold so
	// multiple sealed segments accumulate before bootstrap.
	donorDir := t.TempDir()
	donor, err := embed.NewDisk(donorDir, embed.WithSegmentBytes(256))
	if err != nil {
		t.Fatal(err)
	}
	defer donor.Close()
	if err := donor.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	donorAddr, err := donor.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prod, err := sdk.NewProducer(donor.Transport())
	if err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		if _, err := prod.Send(ctx, "events", proto.Record{
			Key:   []byte{byte(i)},
			Value: []byte("xxxxxxxxxxxxxxxxxxxxxxxxxx"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	_ = prod.Close()

	// Act — recipient is a brand-new broker. Before opening its own
	// FileStore, ask the donor over the wire for its sealed snapshot
	// and write it under recipientDir.
	recipientDir := t.TempDir()
	tr, err := holocronnet.Dial(donorAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	p := proto.PartitionRef{Topic: "events", Index: 0}
	written, err := embed.BootstrapPartitionFromPeer(ctx, tr, recipientDir, p)
	if err != nil {
		t.Fatalf("BootstrapPartitionFromPeer: %v", err)
	}
	if written < 4 {
		// At ~26 bytes/record × 100 records ÷ 256-byte segments we
		// should have rolled enough times to ship ≥2 sealed segments,
		// each with both .log and .idx (4 files minimum).
		t.Fatalf("seeded files: got %d, want >= 4", written)
	}

	// Open recipient against the seeded dir; pre-create the topic so
	// the partition validates. The on-disk segments should be picked
	// up by the FileStore.
	recipient, err := embed.NewDisk(recipientDir, embed.WithSegmentBytes(256))
	if err != nil {
		t.Fatal(err)
	}
	defer recipient.Close()
	if err := recipient.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	// Assert — recipient can read records from offset 0. They came
	// exclusively from the donor's sealed segments, so the recipient
	// has at least one record without ever receiving a Publish.
	consumer, err := sdk.NewConsumer(recipient.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if err := consumer.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}
	got, err := consumer.Poll(ctx, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("recipient saw 0 records — bootstrap did not seed any sealed records into the recipient's data dir")
	}
	for i, r := range got {
		if r.Offset != int64(i) {
			t.Fatalf("record %d: offset=%d, want %d", i, r.Offset, i)
		}
	}
}
