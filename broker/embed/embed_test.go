package embed_test

import (
	"bytes"
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
	tr, err := holocronnet.Dial(addr, holocronnet.WithCredential(sdk.APIKeyCredential("secret-A")))
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

// TestProducer_CompressionLevel_RoundTripsHighRatio proves a
// Producer with WithCompression(LZ4) + WithCompressionLevel(9)
// round-trips a compressible payload through the wire — the broker
// decompresses correctly regardless of which compressor variant
// (fast vs HC) the producer used. Level=0 picks the fast path
// (existing behavior); level>=1 switches to LZ4-HC at that level
// for a higher ratio at the cost of CPU.
func TestProducer_CompressionLevel_RoundTripsHighRatio(t *testing.T) {
	// Arrange — a compressible payload (repeating bytes).
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

	prod, err := sdk.NewProducer(tr,
		sdk.WithCompression(sdk.CodecLZ4),
		sdk.WithCompressionLevel(9),
		sdk.WithLinger(50*time.Millisecond),
		sdk.WithBatchSize(100),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	value := bytes.Repeat([]byte("aaaa"), 256) // 1 KiB highly compressible
	for range 5 {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: value}); err != nil {
			t.Fatal(err)
		}
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Assert — broker received and stored 5 records intact.
	c, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Assign(ctx, proto.PartitionRef{Topic: "events", Index: 0}, 0); err != nil {
		t.Fatal(err)
	}
	got := drainAtLeast(t, ctx, c, 5)
	if len(got) < 5 {
		t.Fatalf("post-decompress reads: got %d, want >= 5", len(got))
	}
	for i, r := range got[:5] {
		if !bytes.Equal(r.Value, value) {
			t.Errorf("record %d value mismatch (HC level=9 round-trip broke)", i)
		}
	}
}

// TestConsumer_Topics_ReturnsSubscribedTopics proves Topics()
// surfaces the consumer's topic list. For group consumers, the list
// is what was passed to Subscribe / SubscribeMany; for self-managed
// consumers, the unique topics across the current assignment.
func TestConsumer_Topics_ReturnsSubscribedTopics(t *testing.T) {
	t.Run("group", func(t *testing.T) {
		b := embed.NewMemory()
		defer b.Close()
		for _, n := range []string{"events", "audits"} {
			if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
				t.Fatal(err)
			}
		}
		c, err := sdk.NewConsumer(b.Transport(), sdk.WithGroup("g"))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := c.SubscribeMany(ctx, []string{"events", "audits"}, 0); err != nil {
			t.Fatal(err)
		}
		got := c.Topics()
		seen := map[string]bool{}
		for _, n := range got {
			seen[n] = true
		}
		if !seen["events"] || !seen["audits"] {
			t.Errorf("Topics: got %v, want both events and audits", got)
		}
	})

	t.Run("self-managed", func(t *testing.T) {
		b := embed.NewMemory()
		defer b.Close()
		if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
			t.Fatal(err)
		}
		c, err := sdk.NewConsumer(b.Transport())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := c.Assign(ctx, proto.PartitionRef{Topic: "events", Index: 0}, 0); err != nil {
			t.Fatal(err)
		}
		if err := c.Assign(ctx, proto.PartitionRef{Topic: "events", Index: 1}, 0); err != nil {
			t.Fatal(err)
		}
		got := c.Topics()
		if len(got) != 1 || got[0] != "events" {
			t.Errorf("Topics (self-managed, two partitions of one topic): got %v, want [events]", got)
		}
	})
}

// TestConsumer_SubscribeMany_JoinsAllTopicsAtOnce proves a group
// consumer can subscribe to multiple topics with one JoinGroup
// call. Today repeating Subscribe(topic1) and Subscribe(topic2)
// works but re-triggers JoinGroup each time — wasteful for
// multi-topic services that know their full topic set up front.
//
// SubscribeMany takes the slice and joins once.
func TestConsumer_SubscribeMany_JoinsAllTopicsAtOnce(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, name := range []string{"events", "audits"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: name, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}
	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	c, err := sdk.NewConsumer(b.Transport(), sdk.WithGroup("g"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Act — single call covers both topics.
	if err := c.SubscribeMany(ctx, []string{"events", "audits"}, 0); err != nil {
		t.Fatalf("SubscribeMany: %v", err)
	}

	// Send one record on each topic.
	if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte("e1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := prod.Send(ctx, "audits", proto.Record{Value: []byte("a1")}); err != nil {
		t.Fatal(err)
	}

	// Assert — both arrive on this consumer's fan-in.
	got := drainAtLeast(t, ctx, c, 2)
	if len(got) < 2 {
		t.Fatalf("got %d records, want >= 2", len(got))
	}
	seen := map[string]bool{}
	for _, r := range got {
		seen[string(r.Value)] = true
	}
	if !seen["e1"] || !seen["a1"] {
		t.Errorf("missing topic record: seen=%v", seen)
	}

	// Assignment should cover both topics.
	parts := c.Assignment()
	topics := map[string]bool{}
	for _, p := range parts {
		topics[p.Topic] = true
	}
	if !topics["events"] || !topics["audits"] {
		t.Errorf("Assignment: got %v, want both events and audits", topics)
	}
}

// TestConsumer_Assignment_SnapshotOfCurrentPartitions proves
// Assignment returns the partitions this consumer is currently
// pumping. Closes the gap where AssignFunc/RevokeFunc callbacks
// were the only way to learn the assignment — synchronous code
// that wants "what am I responsible for?" had to track it
// out-of-band.
func TestConsumer_Assignment_SnapshotOfCurrentPartitions(t *testing.T) {
	// Arrange — self-managed consumer with two explicit Assigns.
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}
	c, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p0 := proto.PartitionRef{Topic: "events", Index: 0}
	p1 := proto.PartitionRef{Topic: "events", Index: 1}
	if err := c.Assign(ctx, p0, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.Assign(ctx, p1, 0); err != nil {
		t.Fatal(err)
	}

	// Act
	got := c.Assignment()

	// Assert — both partitions surface (order unspecified).
	if len(got) != 2 {
		t.Fatalf("Assignment: got %d partitions, want 2 (got=%v)", len(got), got)
	}
	seen := map[proto.PartitionRef]bool{}
	for _, p := range got {
		seen[p] = true
	}
	if !seen[p0] || !seen[p1] {
		t.Errorf("Assignment: missing one of {p0, p1} in %v", got)
	}
}

// TestConsumer_CommitAll_BulkCommitsAcrossAssignment proves
// CommitAll commits each assigned partition's Position for the
// consumer's group in one call. Pairs with TotalLag for the
// "introspect everything → commit everything" pattern; without it
// callers had to enumerate Assignment() and call Commit per
// partition.
func TestConsumer_CommitAll_BulkCommitsAcrossAssignment(t *testing.T) {
	// Arrange — group consumer with two-partition topic.
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}
	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	// Pre-seed both partitions.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 4; i++ {
		if _, err := b.Transport().Publish(ctx, proto.PartitionRef{Topic: "events", Index: 0}, proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := b.Transport().Publish(ctx, proto.PartitionRef{Topic: "events", Index: 1}, proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	c, err := sdk.NewConsumer(b.Transport(), sdk.WithGroup("g"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}

	// Drain everything so Position lands at HW for both partitions.
	_ = drainAtLeast(t, ctx, c, 6)

	// Act
	if err := c.CommitAll(ctx); err != nil {
		t.Fatalf("CommitAll: %v", err)
	}

	// Assert — committed offsets visible via the broker's
	// ListGroupOffsets surface match the next-to-read positions
	// (4 and 2 respectively).
	tr, err := holocronnet.Dial(addrFromBroker(t, b))
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	entries, err := tr.ListGroupOffsets(ctx, "g")
	if err != nil {
		t.Fatal(err)
	}
	got := map[int32]int64{}
	for _, e := range entries {
		got[e.Partition] = e.Committed
	}
	if got[0] != 4 {
		t.Errorf("partition 0 committed: got %d, want 4", got[0])
	}
	if got[1] != 2 {
		t.Errorf("partition 1 committed: got %d, want 2", got[1])
	}
}

// addrFromBroker spins up the broker's TCP listener for tests that
// need to round-trip via the wire transport.
func addrFromBroker(t *testing.T, b *embed.Broker) string {
	t.Helper()
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

// TestEmbed_SyncFromLeader_FillsEmptyBroker proves the Stage 9
// milestone-4 orchestrator: a fresh broker with the same topic
// registry as a donor catches up on records by calling
// SyncFromLeader against the donor's wire transport. Closes the
// gap that exists when a fresh follower joins a cluster after the
// Raft log has been truncated past the FSM snapshot — the
// snapshot carries metadata only, so records have to come from a
// peer's segment-streaming path.
//
// This test exercises the orchestration without going through
// Raft AddVoter; M5/M6 add the cluster-integration scenarios with
// forced log truncation.
func TestEmbed_SyncFromLeader_FillsEmptyBroker(t *testing.T) {
	// Arrange — donor broker with 5 records on each of 2 partitions.
	donor := embed.NewMemory()
	defer donor.Close()
	if err := donor.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}
	donorAddr, err := donor.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for p := int32(0); p < 2; p++ {
		for i := 0; i < 5; i++ {
			if _, err := donor.Transport().Publish(ctx, proto.PartitionRef{Topic: "events", Index: p}, proto.Record{Value: []byte{byte(i)}}); err != nil {
				t.Fatal(err)
			}
		}
	}

	// A fresh recipient broker — same registry shape (topic with 2
	// partitions) so SyncFromLeader has something to enumerate; an
	// empty store so there's an actual gap to fill.
	recipient := embed.NewMemory()
	defer recipient.Close()
	if err := recipient.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}

	// Act — sync from the donor's wire transport.
	tr, err := holocronnet.Dial(donorAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	total, err := recipient.SyncFromLeader(ctx, tr)
	if err != nil {
		t.Fatalf("SyncFromLeader: %v", err)
	}

	// Assert — 10 records appended (5 per partition × 2).
	if total != 10 {
		t.Errorf("total appended: got %d, want 10", total)
	}
	for p := int32(0); p < 2; p++ {
		c, err := sdk.NewConsumer(recipient.Transport())
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Assign(ctx, proto.PartitionRef{Topic: "events", Index: p}, 0); err != nil {
			t.Fatal(err)
		}
		got := make([]proto.Record, 0, 5)
		for len(got) < 5 {
			recs, err := c.Poll(ctx, 5-len(got))
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, recs...)
		}
		_ = c.Close()
		if len(got) != 5 {
			t.Errorf("partition %d: got %d records, want 5", p, len(got))
		}
		for i, r := range got {
			if r.Offset != int64(i) {
				t.Errorf("partition %d record %d: offset %d, want %d", p, i, r.Offset, i)
			}
		}
	}

	// Idempotency — second call is a no-op (already caught up).
	again, err := recipient.SyncFromLeader(ctx, tr)
	if err != nil {
		t.Fatalf("second SyncFromLeader: %v", err)
	}
	if again != 0 {
		t.Errorf("second sync appended %d, want 0 (already caught up)", again)
	}
}

// TestEmbed_BrokerStats proves Broker.Stats() reports a one-call
// observability snapshot of the broker — topic count, partition
// count totaled across topics, and clustering info. Useful for
// monitoring loops that want a single broker-health readout.
func TestEmbed_BrokerStats(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()

	// Empty broker: zero topics, zero partitions.
	stats := b.Stats()
	if stats.TopicCount != 0 || stats.PartitionCount != 0 {
		t.Errorf("empty broker: got topics=%d partitions=%d, want 0/0", stats.TopicCount, stats.PartitionCount)
	}
	// In-memory broker is a single node — not clustered, but
	// IsLeader convention is true (single-node "leader of itself").
	if !stats.IsLeader {
		t.Errorf("single-node broker: IsLeader=false, want true")
	}

	// Add 2 topics with different partition counts.
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 4}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "audits", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	stats = b.Stats()
	if stats.TopicCount != 2 {
		t.Errorf("TopicCount: got %d, want 2", stats.TopicCount)
	}
	if stats.PartitionCount != 5 {
		t.Errorf("PartitionCount: got %d, want 5 (4+1)", stats.PartitionCount)
	}
}

// TestConsumer_Stats_AggregatesObservability proves Stats() returns
// a single ConsumerStats covering Topics, Assignment, PolledCount,
// and per-partition Position. The fields a monitoring caller wants
// in one go rather than five method invocations.
func TestConsumer_Stats_AggregatesObservability(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
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
	p0 := proto.PartitionRef{Topic: "events", Index: 0}
	p1 := proto.PartitionRef{Topic: "events", Index: 1}
	if err := c.Assign(ctx, p0, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.Assign(ctx, p1, 0); err != nil {
		t.Fatal(err)
	}

	// Produce 3 + 2 records and drain everything.
	for i := 0; i < 3; i++ {
		if _, err := b.Transport().Publish(ctx, p0, proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := b.Transport().Publish(ctx, p1, proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	_ = drainAtLeast(t, ctx, c, 5)

	stats := c.Stats()
	if len(stats.Topics) != 1 || stats.Topics[0] != "events" {
		t.Errorf("Topics: got %v, want [events]", stats.Topics)
	}
	if len(stats.Assignment) != 2 {
		t.Errorf("Assignment: got %d, want 2", len(stats.Assignment))
	}
	if stats.PolledCount != 5 {
		t.Errorf("PolledCount: got %d, want 5", stats.PolledCount)
	}
	if stats.PerPartition[p0] != 3 {
		t.Errorf("PerPartition[p0]: got %d, want 3 (Position post-drain)", stats.PerPartition[p0])
	}
	if stats.PerPartition[p1] != 2 {
		t.Errorf("PerPartition[p1]: got %d, want 2", stats.PerPartition[p1])
	}
}

// TestConsumer_WithAutoCommit_PersistsPositionPeriodically proves
// a group consumer configured with WithAutoCommit ticks a
// background goroutine that calls CommitAll every interval, so the
// broker-side committed offset advances without manual Commit
// calls. Closes the gap where SDK consumers outside the streams /
// connect wrappers had to roll their own auto-commit.
func TestConsumer_WithAutoCommit_PersistsPositionPeriodically(t *testing.T) {
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
	c, err := sdk.NewConsumer(b.Transport(),
		sdk.WithGroup("g"),
		sdk.WithAutoCommit(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}

	// Produce 3 records, drain, wait for auto-commit tick.
	for i := 0; i < 3; i++ {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	_ = drainAtLeast(t, ctx, c, 3)

	// Wait for at least one auto-commit tick (50ms × 3 to give margin).
	time.Sleep(200 * time.Millisecond)

	// Assert — broker has committed offset 3 (next-to-read).
	tr, err := holocronnet.Dial(addrFromBroker(t, b))
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	entries, err := tr.ListGroupOffsets(ctx, "g")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("offsets: got %d entries, want 1", len(entries))
	}
	if entries[0].Committed != 3 {
		t.Errorf("committed: got %d, want 3 (auto-commit didn't fire)", entries[0].Committed)
	}
}

// TestConsumer_PauseAll_StopsEveryPartition proves PauseAll cancels
// every active pump in one call and ResumeAll restarts them. Sister
// to per-partition Pause/Resume for the common "halt the whole
// consumer for backpressure, then catch up" pattern.
func TestConsumer_PauseAll_StopsEveryPartition(t *testing.T) {
	// Arrange — two-partition topic with a self-managed consumer.
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
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
	p0 := proto.PartitionRef{Topic: "events", Index: 0}
	p1 := proto.PartitionRef{Topic: "events", Index: 1}
	if err := c.Assign(ctx, p0, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.Assign(ctx, p1, 0); err != nil {
		t.Fatal(err)
	}

	// Pre-prime — produce one record on each partition and drain.
	if _, err := b.Transport().Publish(ctx, p0, proto.Record{Value: []byte("p0-r0")}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Transport().Publish(ctx, p1, proto.Record{Value: []byte("p1-r0")}); err != nil {
		t.Fatal(err)
	}
	_ = drainAtLeast(t, ctx, c, 2)

	// Act — pause everything.
	if err := c.PauseAll(); err != nil {
		t.Fatalf("PauseAll: %v", err)
	}
	// Produce more on each — should NOT arrive while paused.
	if _, err := b.Transport().Publish(ctx, p0, proto.Record{Value: []byte("paused-p0")}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Transport().Publish(ctx, p1, proto.Record{Value: []byte("paused-p1")}); err != nil {
		t.Fatal(err)
	}
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	got, _ := c.Poll(pollCtx, 5)
	pollCancel()
	if len(got) > 0 {
		t.Fatalf("paused poll: got %d records, want 0 (PauseAll didn't stop pumps)", len(got))
	}

	// ResumeAll — both partitions should now deliver.
	if err := c.ResumeAll(ctx, 0); err != nil {
		t.Fatalf("ResumeAll: %v", err)
	}
	got = drainAtLeast(t, ctx, c, 2)
	if len(got) < 2 {
		t.Fatalf("post-ResumeAll drain: got %d, want >= 2", len(got))
	}
	values := map[string]bool{}
	for _, r := range got {
		values[string(r.Value)] = true
	}
	if !values["paused-p0"] || !values["paused-p1"] {
		t.Errorf("ResumeAll missed records: got values=%v", values)
	}
}

// TestConsumer_TotalLag_SumsAcrossPartitions proves TotalLag
// aggregates per-partition lag across every partition the consumer
// is actively pumping. One number — "is this consumer keeping up?"
// — without the caller having to enumerate partitions and sum
// manually.
func TestConsumer_TotalLag_SumsAcrossPartitions(t *testing.T) {
	// Arrange — two-partition topic so TotalLag has more than
	// one partition to aggregate.
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
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
	p0 := proto.PartitionRef{Topic: "events", Index: 0}
	p1 := proto.PartitionRef{Topic: "events", Index: 1}
	if err := c.Assign(ctx, p0, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.Assign(ctx, p1, 0); err != nil {
		t.Fatal(err)
	}

	// Send 3 records to partition 0 and 2 to partition 1 by
	// directly publishing to the partition (bypasses the
	// partitioner).
	for i := 0; i < 3; i++ {
		if _, err := b.Transport().Publish(ctx, p0, proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := b.Transport().Publish(ctx, p1, proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	// Act — without polling, TotalLag should be 5 (3 + 2).
	lag, err := c.TotalLag(ctx)
	if err != nil {
		t.Fatalf("TotalLag: %v", err)
	}
	if lag != 5 {
		t.Errorf("TotalLag before any poll: got %d, want 5", lag)
	}

	// Drain everything; TotalLag should drop to 0.
	_ = drainAtLeast(t, ctx, c, 5)
	lag, err = c.TotalLag(ctx)
	if err != nil {
		t.Fatalf("TotalLag after drain: %v", err)
	}
	if lag != 0 {
		t.Errorf("TotalLag after full drain: got %d, want 0", lag)
	}
}

// TestConsumer_PositionAndLag proves Position returns the next-to-
// read offset for an assigned partition (latest+1, or 0 when nothing
// has been read yet) and Lag returns high-water - position. Both
// answer "where am I and how far behind am I?" for self-managed
// Consumers — group-coordinated consumers commit through the
// broker, but a self-managed consumer using Assign/Subscribe has no
// other introspection path.
func TestConsumer_PositionAndLag(t *testing.T) {
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

	// Produce 5 records — high-water = 5.
	for i := 0; i < 5; i++ {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	// Read just two; Position should be 2 (next-to-read), Lag should be 3.
	got := drainAtLeast(t, ctx, c, 2)
	if len(got) < 2 {
		t.Fatalf("first drain: got %d, want >= 2", len(got))
	}

	pos, ok := c.Position(p)
	if !ok {
		t.Fatal("Position: ok=false after reading 2 records")
	}
	if pos != 2 {
		t.Errorf("Position after 2 reads: got %d, want 2", pos)
	}

	lag, err := c.Lag(ctx, p)
	if err != nil {
		t.Fatalf("Lag: %v", err)
	}
	if lag != 3 {
		t.Errorf("Lag after 2 reads of 5: got %d, want 3", lag)
	}
}

// TestConsumer_SeekToEnd_AttachesAtHighWater proves SeekToEnd
// repositions the consumer to the partition's current high-water,
// so subsequent records produced after the call are observed but
// pre-existing records are skipped. The live-tail-from-here
// pattern in one call.
func TestConsumer_SeekToEnd_AttachesAtHighWater(t *testing.T) {
	// Arrange — pre-seed 3 records before SeekToEnd.
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
	for _, v := range []string{"old1", "old2", "old3"} {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte(v)}); err != nil {
			t.Fatal(err)
		}
	}

	// Act — seek past the historical records.
	if err := c.SeekToEnd(ctx, p); err != nil {
		t.Fatalf("SeekToEnd: %v", err)
	}

	// Produce one record AFTER the seek. Only this should arrive.
	if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte("new1")}); err != nil {
		t.Fatal(err)
	}

	got := drainAtLeast(t, ctx, c, 1)
	if len(got) < 1 {
		t.Fatalf("post-seek read: got %d, want >= 1", len(got))
	}
	if string(got[0].Value) != "new1" {
		t.Errorf("first post-seek record: got %q, want new1", got[0].Value)
	}
}

// TestConsumer_SeekToBeginning_RewindsToZero proves SeekToBeginning
// is sugar for Seek(p, 0) — the partition's pump restarts at offset
// 0 and re-delivers historical records.
func TestConsumer_SeekToBeginning_RewindsToZero(t *testing.T) {
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

	for _, v := range []string{"a", "b", "c"} {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte(v)}); err != nil {
			t.Fatal(err)
		}
	}

	// Drain everything once.
	_ = drainAtLeast(t, ctx, c, 3)

	// Act
	if err := c.SeekToBeginning(ctx, p); err != nil {
		t.Fatalf("SeekToBeginning: %v", err)
	}

	// Assert — all 3 records re-arrive.
	got := drainAtLeast(t, ctx, c, 3)
	if len(got) < 3 || string(got[0].Value) != "a" {
		t.Fatalf("post-rewind: got %d records (first=%q), want >= 3 starting at 'a'", len(got), got[0].Value)
	}
}

// TestConsumer_SeekRewindsToOffset proves Seek repositions an
// already-assigned partition's pump to a fresh offset mid-session.
// After consuming the first of three records, a Seek back to offset
// 0 makes the next Poll surface offset 0 again — the same record
// is re-delivered without leaving the assignment or restarting the
// Consumer.
//
// Without Seek the only way to replay a record is to construct a
// fresh Consumer and Assign at the desired offset, which loses
// other assigned partitions and any in-flight buffer state.
func TestConsumer_SeekRewindsToOffset(t *testing.T) {
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

	// Produce three records — offsets 0,1,2.
	for _, v := range []string{"v0", "v1", "v2"} {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte(v)}); err != nil {
			t.Fatal(err)
		}
	}

	// Read all three through the initial pump.
	first := drainAtLeast(t, ctx, c, 3)
	if len(first) != 3 || string(first[0].Value) != "v0" {
		t.Fatalf("first read: got %d records (first=%q), want 3 starting at v0", len(first), first[0].Value)
	}

	// Act — seek back to offset 0.
	if err := c.Seek(ctx, p, 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	// Assert — the next reads see the same three records again,
	// starting at v0. drainAtLeast will block until 3 arrive or ctx
	// expires.
	again := drainAtLeast(t, ctx, c, 3)
	if len(again) < 3 {
		t.Fatalf("post-seek read: got %d records, want >= 3", len(again))
	}
	for i, want := range []string{"v0", "v1", "v2"} {
		if string(again[i].Value) != want {
			t.Errorf("post-seek record %d: got %q, want %q", i, again[i].Value, want)
		}
	}
}

// drainAtLeast Poll-loops until at least want records have arrived
// or ctx expires.
func drainAtLeast(t *testing.T, ctx context.Context, c *sdk.Consumer, want int) []proto.Record {
	t.Helper()
	out := make([]proto.Record, 0, want)
	for len(out) < want {
		recs, err := c.Poll(ctx, want-len(out))
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		out = append(out, recs...)
	}
	return out
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

	tr, err := holocronnet.Dial(addr, holocronnet.WithCredential(sdk.APIKeyCredential("k1")))
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

	tr, err := holocronnet.Dial(addr, holocronnet.WithCredential(sdk.APIKeyCredential("key-A")))
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

	prodTr, err := holocronnet.Dial(addr, holocronnet.WithCredential(sdk.APIKeyCredential("producer")))
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
	consTr, err := holocronnet.Dial(addr, holocronnet.WithCredential(sdk.APIKeyCredential("consumer")))
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
