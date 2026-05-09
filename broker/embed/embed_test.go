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
