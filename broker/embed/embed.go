// Package embed bundles a broker, storage, and registry behind a single
// public handle. It is the entry point for tests, demos, and the daemon
// process — anything that wants to run a broker in the same process as
// itself without reaching into broker/internal.
package embed

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/broker/inproc"
	"github.com/jedi-knights/holocron/broker/internal/broker"
	"github.com/jedi-knights/holocron/broker/internal/cluster"
	"github.com/jedi-knights/holocron/broker/internal/groups"
	"github.com/jedi-knights/holocron/broker/internal/metrics"
	"github.com/jedi-knights/holocron/broker/internal/server"
	"github.com/jedi-knights/holocron/broker/internal/storage"
	"github.com/jedi-knights/holocron/broker/internal/topic"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// TopicSpec re-exports the registry's topic creation spec so callers don't
// need to import broker/internal/topic.
type TopicSpec = topic.Spec

// topicsFileName is the JSON file inside the data dir that persists topic
// metadata across restarts.
const topicsFileName = "topics.json"

// offsetsFileName is the JSON file inside the data dir that persists
// committed consumer-group offsets across restarts.
const offsetsFileName = "offsets.json"

// Broker is an in-process broker handle. It owns the storage backend, the
// topic registry, and the pub/sub coordinator, and exposes the SDK
// Transport that producers and consumers connect through.
type Broker struct {
	store     storage.Store
	registry  *topic.Registry
	core      *broker.Broker
	transport *inproc.Transport
	cluster   *cluster.Cluster
	metrics   *metrics.Registry

	mu          sync.Mutex
	closed      bool
	stopRet     context.CancelFunc
	retDoneCh   chan struct{}
	srv         *server.Server
	metricsSrv  *http.Server
	metricsAddr net.Addr
}

// ClusterPeer re-exports cluster.Peer so callers don't import broker/internal.
type ClusterPeer = cluster.Peer

// ClusterConfig configures multi-node cluster mode.
type ClusterConfig struct {
	NodeID        string        // unique per node
	BindAddr      string        // host:port for Raft RPC
	AdvertiseAddr string        // address peers should dial; defaults to BindAddr
	Peers         []ClusterPeer // initial cluster membership
	Bootstrap     bool          // true on the first node only
}

// NewMemory returns a Broker backed by an in-memory store. Topic metadata,
// committed offsets, and group state all live only in RAM. Suitable for
// tests and the in-memory demo.
func NewMemory() *Broker {
	store := storage.NewMemoryStore()
	registry := topic.NewRegistry()
	gm := groups.NewManager(groups.NewMemoryOffsetStore(), registry.PartitionsFor)
	m := metrics.New()
	core := broker.New(store, registry, broker.WithGroupManager(gm), broker.WithMetrics(m))
	return &Broker{
		store:     store,
		registry:  registry,
		core:      core,
		transport: inproc.New(core),
		metrics:   m,
	}
}

// DiskOption configures a disk-backed Broker.
type DiskOption func(*diskConfig)

type diskConfig struct {
	segmentBytes     int64
	retention        time.Duration
	retentionBytes   int64
	retentionTickInt time.Duration
	compactionEnabled bool
	cluster          *ClusterConfig
}

// WithSegmentBytes sets the segment-rollover threshold (default 1 GiB).
func WithSegmentBytes(n int64) DiskOption {
	return func(c *diskConfig) { c.segmentBytes = n }
}

// WithRetention enables time-based retention with the given window. A
// background sweeper deletes whole segments whose last record is older
// than the cutoff. Zero (the default) disables retention.
func WithRetention(d time.Duration) DiskOption {
	return func(c *diskConfig) { c.retention = d }
}

// WithSizeRetention enables size-based retention. The sweeper deletes
// oldest sealed segments per partition while the partition's total
// on-disk size exceeds maxBytes. The active segment is never removed.
// Zero (the default) disables size retention. Time and size retention
// can both be enabled; both run on the same sweeper interval.
func WithSizeRetention(maxBytes int64) DiskOption {
	return func(c *diskConfig) { c.retentionBytes = maxBytes }
}

// WithCompaction enables Kafka-style log compaction across every
// partition's sealed segments. The sweeper, on each tick, rewrites
// sealed segments to keep only the latest record per key. Tombstones
// (records with nil Value) drop their key. The active segment is
// untouched. Off by default.
func WithCompaction() DiskOption {
	return func(c *diskConfig) { c.compactionEnabled = true }
}

// WithRetentionInterval sets how often the sweeper runs (default 5m).
func WithRetentionInterval(d time.Duration) DiskOption {
	return func(c *diskConfig) { c.retentionTickInt = d }
}

// WithCluster turns on multi-node Raft replication. Produces and topic
// creation route through Raft Apply; followers reply with NotLeader so
// the SDK can redirect.
func WithCluster(cfg ClusterConfig) DiskOption {
	return func(c *diskConfig) { c.cluster = &cfg }
}

// NewDisk returns a Broker rooted at dir. Existing topic metadata and
// segment data are recovered; new topics are persisted to <dir>/topics.json.
func NewDisk(dir string, opts ...DiskOption) (*Broker, error) {
	cfg := diskConfig{
		segmentBytes:     0,
		retention:        0,
		retentionTickInt: 5 * time.Minute,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	storeOpts := []storage.FileStoreOption{}
	if cfg.segmentBytes > 0 {
		storeOpts = append(storeOpts, storage.WithSegmentBytes(cfg.segmentBytes))
	}
	store, err := storage.NewFileStore(dir, storeOpts...)
	if err != nil {
		return nil, err
	}

	registry := topic.NewRegistry()
	topicsPath := filepath.Join(dir, topicsFileName)
	if err := topic.LoadFile(registry, topicsPath); err != nil {
		return nil, err
	}
	registry.SetPersistHook(func(snapshot []proto.TopicConfig) error {
		return topic.SaveFile(topicsPath, snapshot)
	})

	// Persist offsets via the broker's own log: __holocron_offsets,
	// single partition, compaction-enabled. This is the same trick the
	// schema registry uses for its metadata. Falls back to the
	// JSON-file store if topic creation fails for any reason.
	if err := registry.Create(topic.Spec{Name: groups.DefaultOffsetsTopic, PartitionCount: 1}); err != nil &&
		!errors.Is(err, topic.ErrTopicExists) {
		return nil, fmt.Errorf("offsets topic: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	offsetsStore, err := groups.OpenTopicOffsetStore(ctx, store, groups.DefaultOffsetsTopic)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("offsets store: %w", err)
	}
	gm := groups.NewManager(offsetsStore, registry.PartitionsFor)

	m := metrics.New()
	brokerOpts := []broker.Option{broker.WithGroupManager(gm), broker.WithMetrics(m)}
	var cl *cluster.Cluster
	if cfg.cluster != nil {
		fsm := cluster.NewFSM(store, registry)
		peers := make([]cluster.Peer, 0, len(cfg.cluster.Peers))
		for _, p := range cfg.cluster.Peers {
			peers = append(peers, cluster.Peer{ID: p.ID, Addr: p.Addr, WireAddr: p.WireAddr})
		}
		cl, err = cluster.New(cluster.Config{
			NodeID:        cfg.cluster.NodeID,
			BindAddr:      cfg.cluster.BindAddr,
			AdvertiseAddr: cfg.cluster.AdvertiseAddr,
			DataDir:       filepath.Join(dir, "raft"),
			Peers:         peers,
			Bootstrap:     cfg.cluster.Bootstrap,
		}, fsm)
		if err != nil {
			return nil, err
		}
		brokerOpts = append(brokerOpts, broker.WithCluster(cl))
	}

	core := broker.New(store, registry, brokerOpts...)
	b := &Broker{
		store:     store,
		registry:  registry,
		core:      core,
		transport: inproc.New(core),
		cluster:   cl,
		metrics:   m,
	}

	if cfg.retention > 0 || cfg.retentionBytes > 0 || cfg.compactionEnabled {
		ctx, cancel := context.WithCancel(context.Background())
		b.stopRet = cancel
		b.retDoneCh = make(chan struct{})
		go b.runRetention(ctx, cfg.retention, cfg.retentionBytes, cfg.compactionEnabled, cfg.retentionTickInt)
	}
	return b, nil
}

// Transport returns the sdk.Transport that producers and consumers use.
func (b *Broker) Transport() sdk.Transport { return b.transport }

// CreateTopic registers a topic with the given spec.
func (b *Broker) CreateTopic(spec TopicSpec) error {
	return b.registry.Create(spec)
}

// Topics returns a snapshot of registered topics.
func (b *Broker) Topics() []proto.TopicConfig { return b.registry.List() }

// ListenMetrics starts an HTTP server on addr serving GET /metrics in
// Prometheus text format. Returns the bound address. The server runs
// until the Broker is Closed.
func (b *Broker) ListenMetrics(addr string) (string, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return "", errors.New("embed: broker is closed")
	}
	if b.metricsSrv != nil {
		b.mu.Unlock()
		return "", errors.New("embed: metrics server already running")
	}
	b.mu.Unlock()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("embed: metrics listen %s: %w", addr, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = b.metrics.WritePrometheus(w)
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	b.mu.Lock()
	b.metricsSrv = srv
	b.metricsAddr = ln.Addr()
	b.mu.Unlock()

	go func() {
		_ = srv.Serve(ln)
	}()

	return ln.Addr().String(), nil
}

// IsLeader reports whether this broker is currently the cluster Raft
// leader. Always true for non-cluster brokers.
func (b *Broker) IsLeader() bool {
	if b.cluster == nil {
		return true
	}
	return b.cluster.IsLeader()
}

// LeaderAddr returns the network address of the current cluster leader,
// or "" for non-cluster brokers.
func (b *Broker) LeaderAddr() string {
	if b.cluster == nil {
		return ""
	}
	return b.cluster.LeaderAddr()
}

// WaitForLeader blocks until any node in the cluster has been elected
// leader (or until timeout). For non-cluster brokers it returns nil
// immediately.
func (b *Broker) WaitForLeader(timeout time.Duration) error {
	if b.cluster == nil {
		return nil
	}
	return b.cluster.WaitForLeader(timeout)
}

// ListenOption configures a Listen call.
type ListenOption func(*listenOpts)

type listenOpts struct {
	tlsConfig *tls.Config
	apiKeys   []string
	quotas    map[string]server.Quota
}

// WithTLS wraps the broker's wire-protocol listener in TLS using the
// supplied config. Producer + consumer SDK callers must dial with
// `holocronnet.WithTLS(...)` (or any tls-aware Transport) to match.
func WithTLS(cfg *tls.Config) ListenOption {
	return func(o *listenOpts) { o.tlsConfig = cfg }
}

// WithAPIKeys configures the set of API keys this broker will accept
// in the wire handshake. SDK clients send their key via
// `holocronnet.WithAPIKey(...)`. Empty list disables authentication.
func WithAPIKeys(keys ...string) ListenOption {
	return func(o *listenOpts) { o.apiKeys = keys }
}

// Quota is re-exported from the internal server package so callers
// can build per-key quota maps without importing internal/.
type Quota = server.Quota

// WithQuotas applies per-API-key produce-bandwidth limits. Each
// authenticated produce request decrements its key's token bucket by
// the payload size; an exhausted bucket fails the request with
// StatusRateLimited until tokens replenish at Quota.BytesPerSec.
//
// Keys without an entry in the map are unlimited. Quotas only fire on
// authenticated connections — a broker without WithAPIKeys treats
// every connection as anonymous and applies no quota.
func WithQuotas(quotas map[string]Quota) ListenOption {
	return func(o *listenOpts) { o.quotas = quotas }
}

// Listen starts a network listener on addr (":9092", ":0", etc.) so
// remote producers and consumers can connect. The listener runs until
// the Broker is Closed. Returns the bound address — useful when addr
// was ":0" so callers know which port the OS chose.
func (b *Broker) Listen(addr string, opts ...ListenOption) (string, error) {
	cfg := listenOpts{}
	for _, o := range opts {
		o(&cfg)
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return "", errors.New("embed: broker is closed")
	}
	if b.srv != nil {
		b.mu.Unlock()
		return "", errors.New("embed: already listening")
	}
	srv := server.New(b.core)
	if len(cfg.apiKeys) > 0 {
		srv.SetAPIKeys(cfg.apiKeys)
	}
	if len(cfg.quotas) > 0 {
		srv.SetQuotas(cfg.quotas)
	}
	b.srv = srv
	b.mu.Unlock()

	var srvOpts []server.ListenOption
	if cfg.tlsConfig != nil {
		srvOpts = append(srvOpts, server.WithTLS(cfg.tlsConfig))
	}
	netAddr, err := srv.Listen(addr, srvOpts...)
	if err != nil {
		b.mu.Lock()
		b.srv = nil
		b.mu.Unlock()
		return "", err
	}
	return netAddr.String(), nil
}

// Close releases resources held by the embedded broker.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	stopRet := b.stopRet
	doneCh := b.retDoneCh
	b.mu.Unlock()

	if stopRet != nil {
		stopRet()
		<-doneCh
	}

	var firstErr error
	if b.metricsSrv != nil {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := b.metricsSrv.Shutdown(shutCtx); err != nil && firstErr == nil {
			firstErr = err
		}
		shutCancel()
	}
	if b.srv != nil {
		if err := b.srv.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if b.cluster != nil {
		if err := b.cluster.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := b.transport.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := b.store.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (b *Broker) runRetention(ctx context.Context, retention time.Duration, retentionBytes int64, compactionEnabled bool, tick time.Duration) {
	defer close(b.retDoneCh)
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fs, ok := b.store.(*storage.FileStore)
			if !ok {
				return
			}
			if retention > 0 {
				if err := fs.EnforceRetention(retention); err != nil {
					if !errors.Is(err, context.Canceled) {
						_ = fmt.Errorf("retention: %w", err)
					}
				}
			}
			if retentionBytes > 0 {
				if err := fs.EnforceSizeRetention(retentionBytes); err != nil {
					if !errors.Is(err, context.Canceled) {
						_ = fmt.Errorf("size retention: %w", err)
					}
				}
			}
			if compactionEnabled {
				if err := fs.Compact(); err != nil {
					if !errors.Is(err, context.Canceled) {
						_ = fmt.Errorf("compaction: %w", err)
					}
				}
			}
		}
	}
}
