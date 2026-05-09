// Package server is the broker's transport layer: a TCP listener that
// translates wire frames into broker.Broker calls.
//
// Each connection is request-response synchronous: the client sends one
// frame, the server replies with one frame. Concurrent operations require
// multiple connections. Subscription is implemented as long-poll Fetch
// rather than server push, so the wire stays simple and consumers control
// their own backpressure.
package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/broker/internal/broker"
	"github.com/jedi-knights/holocron/broker/internal/cluster"
	"github.com/jedi-knights/holocron/broker/internal/topic"
	"github.com/jedi-knights/holocron/proto"
)

// fetchPollInterval is how often a long-poll Fetch checks for new records
// while waiting. Short enough to feel responsive; long enough to avoid
// spinning a CPU.
const fetchPollInterval = 25 * time.Millisecond

// fetchCompressionThreshold is the minimum total payload size at which
// LZ4 actually beats the codec=None inline path. Below this, the
// per-response framing overhead and per-call CPU cost outweigh the
// bandwidth savings.
const fetchCompressionThreshold = 256

// fetchCompressionWorthIt reports whether the total record-value size
// across records crosses the threshold for LZ4 to pay off.
func fetchCompressionWorthIt(records []proto.Record) bool {
	var total int
	for _, r := range records {
		total += len(r.Value)
		if total >= fetchCompressionThreshold {
			return true
		}
	}
	return false
}

// Server accepts TCP connections and dispatches wire requests to a Broker.
type Server struct {
	core *broker.Broker

	mu       sync.Mutex
	listener net.Listener
	conns    map[net.Conn]struct{}
	closing  bool
	wg       sync.WaitGroup
	apiKeys       map[string]struct{}
	produceQuotas map[string]*tokenBucket // per-API-key produce limiter
	fetchQuotas   map[string]*tokenBucket // per-API-key fetch limiter
}

// Quota configures a per-API-key bandwidth limit. Produce quotas count
// outgoing record-value bytes; fetch quotas count returned bytes. Apply
// via SetQuotas (or embed.WithQuotas at the Listen layer).
type Quota struct {
	// ProduceBytesPerSec is the steady-state produce throughput
	// allowed for the API key. Zero disables produce limiting.
	ProduceBytesPerSec int64
	// ProduceBurstBytes is the produce bucket size. Zero defaults to
	// one second's worth (ProduceBytesPerSec).
	ProduceBurstBytes int64
	// FetchBytesPerSec is the steady-state fetch throughput allowed
	// for the API key. Zero disables fetch limiting.
	FetchBytesPerSec int64
	// FetchBurstBytes is the fetch bucket size. Zero defaults to one
	// second's worth (FetchBytesPerSec).
	FetchBurstBytes int64
}

// SetQuotas installs per-API-key produce + fetch quotas. Existing
// connections pick up the new limits on their next request. An empty
// map clears every limit (the default).
func (s *Server) SetQuotas(quotas map[string]Quota) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(quotas) == 0 {
		s.produceQuotas = nil
		s.fetchQuotas = nil
		return
	}
	s.produceQuotas = make(map[string]*tokenBucket, len(quotas))
	s.fetchQuotas = make(map[string]*tokenBucket, len(quotas))
	for key, q := range quotas {
		if q.ProduceBytesPerSec > 0 {
			burst := q.ProduceBurstBytes
			if burst <= 0 {
				burst = q.ProduceBytesPerSec
			}
			s.produceQuotas[key] = newTokenBucket(q.ProduceBytesPerSec, burst)
		}
		if q.FetchBytesPerSec > 0 {
			burst := q.FetchBurstBytes
			if burst <= 0 {
				burst = q.FetchBytesPerSec
			}
			s.fetchQuotas[key] = newTokenBucket(q.FetchBytesPerSec, burst)
		}
	}
}

// limiterFor returns the produce token bucket for apiKey, or nil when
// no produce quota applies. Safe for concurrent use.
func (s *Server) limiterFor(apiKey string) *tokenBucket {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.produceQuotas == nil {
		return nil
	}
	return s.produceQuotas[apiKey]
}

// fetchLimiterFor returns the fetch token bucket for apiKey, or nil
// when no fetch quota applies.
func (s *Server) fetchLimiterFor(apiKey string) *tokenBucket {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fetchQuotas == nil {
		return nil
	}
	return s.fetchQuotas[apiKey]
}

// SetAPIKeys configures the set of API keys this Server accepts. An
// empty set disables authentication (any handshake is admitted).
func (s *Server) SetAPIKeys(keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(keys) == 0 {
		s.apiKeys = nil
		return
	}
	s.apiKeys = make(map[string]struct{}, len(keys))
	for _, k := range keys {
		s.apiKeys[k] = struct{}{}
	}
}

// New returns a Server bound to the given Broker.
func New(b *broker.Broker) *Server {
	return &Server{
		core:  b,
		conns: make(map[net.Conn]struct{}),
	}
}

// ListenOption configures a Listen call.
type ListenOption func(*listenOpts)

type listenOpts struct {
	tlsConfig *tls.Config
}

// WithTLS wraps the listener in TLS using the supplied config. Both
// server certs and (optionally) client-cert verification flow through
// the standard tls.Config.
func WithTLS(cfg *tls.Config) ListenOption {
	return func(o *listenOpts) { o.tlsConfig = cfg }
}

// Listen starts accepting connections on addr. The listener runs until
// Close is called. Returns the listener address (useful when addr was ":0"
// for tests).
func (s *Server) Listen(addr string, opts ...ListenOption) (net.Addr, error) {
	cfg := listenOpts{}
	for _, o := range opts {
		o(&cfg)
	}
	var (
		ln  net.Listener
		err error
	)
	if cfg.tlsConfig != nil {
		ln, err = tls.Listen("tcp", addr, cfg.tlsConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("server: listen %s: %w", addr, err)
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()
	s.wg.Add(1)
	go s.acceptLoop(ln)
	return ln.Addr(), nil
}

func (s *Server) acceptLoop(ln net.Listener) {
	defer s.wg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if closing {
				return
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serveConn(conn)
	}
}

// Close stops accepting new connections, closes existing ones, and waits
// for all in-flight handlers to return.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	ln := s.listener
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}
	for _, c := range conns {
		_ = c.Close()
	}
	s.wg.Wait()
	return nil
}

func (s *Server) serveConn(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		_ = conn.Close()
	}()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	apiKey, err := s.handshake(r, w)
	if err != nil {
		return
	}
	if err := w.Flush(); err != nil {
		return
	}

	for {
		op, body, err := proto.ReadFrame(r)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				_ = proto.WriteErrorResponse(w, op, proto.StatusInternal, err.Error())
				_ = w.Flush()
			}
			return
		}
		if err := s.dispatch(op, apiKey, body, w); err != nil {
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

// handshake validates the wire version + API key. Returns the
// authenticated API key (or "" if no auth is configured) so the per-
// connection serve loop can apply per-key quotas to subsequent
// requests.
func (s *Server) handshake(r io.Reader, w io.Writer) (string, error) {
	op, body, err := proto.ReadFrame(r)
	if err != nil {
		return "", err
	}
	if op != proto.OpHandshake {
		_ = proto.WriteErrorResponse(w, op, proto.StatusInvalidRequest, "expected handshake")
		return "", errors.New("handshake: wrong opcode")
	}
	hs, err := proto.DecodeHandshakeRequest(body)
	if err != nil {
		_ = proto.WriteErrorResponse(w, op, proto.StatusInvalidRequest, err.Error())
		return "", err
	}
	if hs.Version != proto.WireVersion {
		_ = proto.WriteErrorResponse(w, op, proto.StatusVersionMismatch,
			fmt.Sprintf("server speaks v%d, client v%d", proto.WireVersion, hs.Version))
		return "", errors.New("handshake: version mismatch")
	}
	s.mu.Lock()
	keys := s.apiKeys
	s.mu.Unlock()
	if len(keys) > 0 {
		if _, ok := keys[hs.APIKey]; !ok {
			_ = proto.WriteErrorResponse(w, op, proto.StatusUnauthorized, "invalid API key")
			return "", errors.New("handshake: unauthorized")
		}
	}
	return hs.APIKey, proto.WriteResponse(w, op, proto.StatusOK, []byte{proto.WireVersion})
}

func (s *Server) dispatch(op proto.OpCode, apiKey string, body []byte, w io.Writer) error {
	switch op {
	case proto.OpProduce:
		return s.handleProduce(apiKey, body, w)
	case proto.OpFetch:
		return s.handleFetch(apiKey, body, w)
	case proto.OpMetadata:
		return s.handleMetadata(body, w)
	case proto.OpCreateTopic:
		return s.handleCreateTopic(body, w)
	case proto.OpHighWater:
		return s.handleHighWater(body, w)
	case proto.OpClusterMembers:
		return s.handleClusterMembers(body, w)
	case proto.OpAddVoter:
		return s.handleAddVoter(body, w)
	case proto.OpRemoveVoter:
		return s.handleRemoveVoter(body, w)
	case proto.OpCommit:
		return s.handleCommit(body, w)
	case proto.OpJoinGroup:
		return s.handleJoinGroup(body, w)
	case proto.OpHeartbeat:
		return s.handleHeartbeat(body, w)
	case proto.OpLeaveGroup:
		return s.handleLeaveGroup(body, w)
	case proto.OpSync:
		return s.handleSync(body, w)
	case proto.OpProduceBatch:
		return s.handleProduceBatch(apiKey, body, w)
	default:
		return proto.WriteErrorResponse(w, op, proto.StatusInvalidRequest,
			fmt.Sprintf("unknown opcode 0x%02x", byte(op)))
	}
}

func (s *Server) handleProduce(apiKey string, body []byte, w io.Writer) error {
	req, err := proto.DecodeProduceRequest(body)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpProduce, proto.StatusInvalidRequest, err.Error())
	}
	if limiter := s.limiterFor(apiKey); limiter != nil {
		if !limiter.take(int64(len(req.Record.Value))) {
			return proto.WriteErrorResponse(w, proto.OpProduce, proto.StatusRateLimited,
				"produce quota exceeded")
		}
	}
	pref := proto.PartitionRef{Topic: req.Topic, Index: req.Partition}
	offset, err := s.core.Publish(context.Background(), pref, req.Record)
	if err != nil {
		return s.respondError(w, proto.OpProduce, err)
	}
	return proto.WriteResponse(w, proto.OpProduce, proto.StatusOK,
		proto.ProduceResponse{Offset: offset}.Encode())
}

// respondError writes the appropriate wire response for a broker error,
// translating cluster-mode "not leader" into StatusNotLeader with the
// leader's network address as the message body so the SDK can redirect.
func (s *Server) respondError(w io.Writer, op proto.OpCode, err error) error {
	var notLeader *broker.ErrNotLeader
	if errors.As(err, &notLeader) {
		return proto.WriteErrorResponse(w, op, proto.StatusNotLeader, notLeader.LeaderAddr)
	}
	return proto.WriteErrorResponse(w, op, classifyBrokerError(err), err.Error())
}

func (s *Server) handleFetch(apiKey string, body []byte, w io.Writer) error {
	req, err := proto.DecodeFetchRequest(body)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpFetch, proto.StatusInvalidRequest, err.Error())
	}
	pref := proto.PartitionRef{Topic: req.Topic, Index: req.Partition}
	deadline := time.Now().Add(time.Duration(req.MaxWaitMs) * time.Millisecond)
	maxRecs := int(req.MaxRecords)
	if maxRecs <= 0 {
		maxRecs = 1
	}

	for {
		records, err := s.core.Read(context.Background(), pref, req.FromOffset, maxRecs)
		if err != nil {
			return proto.WriteErrorResponse(w, proto.OpFetch, classifyBrokerError(err), err.Error())
		}
		if len(records) > 0 || time.Now().After(deadline) {
			if limiter := s.fetchLimiterFor(apiKey); limiter != nil && len(records) > 0 {
				var bytes int64
				for _, r := range records {
					bytes += int64(len(r.Value))
				}
				if !limiter.take(bytes) {
					return proto.WriteErrorResponse(w, proto.OpFetch, proto.StatusRateLimited,
						"fetch quota exceeded")
				}
			}
			// Pick a response codec the client accepts. LZ4 only pays
			// for itself once the payload exceeds the per-response
			// frame overhead; below that, the codec=None inline path
			// is cheaper.
			codec := proto.CodecNone
			if req.AcceptCodec == proto.CodecLZ4 && fetchCompressionWorthIt(records) {
				codec = proto.CodecLZ4
			}
			return proto.WriteResponse(w, proto.OpFetch, proto.StatusOK,
				proto.FetchResponse{Records: records, Codec: codec}.Encode())
		}
		time.Sleep(fetchPollInterval)
	}
}

func (s *Server) handleMetadata(body []byte, w io.Writer) error {
	req, err := proto.DecodeMetadataRequest(body)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpMetadata, proto.StatusInvalidRequest, err.Error())
	}
	n, err := s.core.Registry().PartitionsFor(req.Topic)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpMetadata, proto.StatusUnknownTopic, err.Error())
	}
	return proto.WriteResponse(w, proto.OpMetadata, proto.StatusOK,
		proto.MetadataResponse{PartitionCount: n}.Encode())
}

// handleClusterMembers reports the current Raft membership. Available
// on every node — followers don't redirect.
func (s *Server) handleClusterMembers(body []byte, w io.Writer) error {
	_, err := proto.DecodeClusterMembersRequest(body)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpClusterMembers, proto.StatusInvalidRequest, err.Error())
	}
	cluster := s.core.Cluster()
	if cluster == nil {
		return proto.WriteResponse(w, proto.OpClusterMembers, proto.StatusOK,
			proto.ClusterMembersResponse{}.Encode())
	}
	peers := cluster.Members()
	out := proto.ClusterMembersResponse{
		Members: make([]proto.ClusterMember, 0, len(peers)),
	}
	for _, p := range peers {
		out.Members = append(out.Members, proto.ClusterMember{ID: p.ID, Addr: p.Addr})
	}
	return proto.WriteResponse(w, proto.OpClusterMembers, proto.StatusOK, out.Encode())
}

// handleAddVoter is leader-only. Followers redirect via StatusNotLeader
// so the SDK can re-dial the leader.
func (s *Server) handleAddVoter(body []byte, w io.Writer) error {
	req, err := proto.DecodeAddVoterRequest(body)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpAddVoter, proto.StatusInvalidRequest, err.Error())
	}
	cl := s.core.Cluster()
	if cl == nil {
		return proto.WriteErrorResponse(w, proto.OpAddVoter, proto.StatusInvalidRequest,
			"broker is not part of a cluster")
	}
	if !cl.IsLeader() {
		return proto.WriteErrorResponse(w, proto.OpAddVoter, proto.StatusNotLeader, cl.LeaderWireAddr())
	}
	if err := cl.AddVoter(cluster.Peer{ID: req.ID, Addr: req.Addr}); err != nil {
		return proto.WriteErrorResponse(w, proto.OpAddVoter, proto.StatusInternal, err.Error())
	}
	return proto.WriteResponse(w, proto.OpAddVoter, proto.StatusOK, nil)
}

// handleRemoveVoter is leader-only.
func (s *Server) handleRemoveVoter(body []byte, w io.Writer) error {
	req, err := proto.DecodeRemoveVoterRequest(body)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpRemoveVoter, proto.StatusInvalidRequest, err.Error())
	}
	cl := s.core.Cluster()
	if cl == nil {
		return proto.WriteErrorResponse(w, proto.OpRemoveVoter, proto.StatusInvalidRequest,
			"broker is not part of a cluster")
	}
	if !cl.IsLeader() {
		return proto.WriteErrorResponse(w, proto.OpRemoveVoter, proto.StatusNotLeader, cl.LeaderWireAddr())
	}
	if err := cl.RemoveVoter(req.ID); err != nil {
		return proto.WriteErrorResponse(w, proto.OpRemoveVoter, proto.StatusInternal, err.Error())
	}
	return proto.WriteResponse(w, proto.OpRemoveVoter, proto.StatusOK, nil)
}

func (s *Server) handleHighWater(body []byte, w io.Writer) error {
	req, err := proto.DecodeHighWaterRequest(body)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpHighWater, proto.StatusInvalidRequest, err.Error())
	}
	pref := proto.PartitionRef{Topic: req.Topic, Index: req.Partition}
	hw, err := s.core.HighWater(context.Background(), pref)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpHighWater, classifyBrokerError(err), err.Error())
	}
	return proto.WriteResponse(w, proto.OpHighWater, proto.StatusOK,
		proto.HighWaterResponse{HighWater: hw}.Encode())
}

func (s *Server) handleCreateTopic(body []byte, w io.Writer) error {
	req, err := proto.DecodeCreateTopicRequest(body)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpCreateTopic, proto.StatusInvalidRequest, err.Error())
	}
	err = s.core.CreateTopic(topic.Spec{
		Name:           req.Name,
		PartitionCount: req.PartitionCount,
		RetentionMs:    req.RetentionMs,
		SegmentBytes:   req.SegmentBytes,
	})
	if err != nil {
		return s.respondError(w, proto.OpCreateTopic, err)
	}
	return proto.WriteResponse(w, proto.OpCreateTopic, proto.StatusOK, nil)
}

func (s *Server) handleCommit(body []byte, w io.Writer) error {
	req, err := proto.DecodeCommitRequest(body)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpCommit, proto.StatusInvalidRequest, err.Error())
	}
	mgr := s.core.Groups()
	if mgr == nil {
		return proto.WriteResponse(w, proto.OpCommit, proto.StatusOK, nil)
	}
	if err := mgr.Commit(req.Group, req.Topic, req.Partition, req.Offset); err != nil {
		return proto.WriteErrorResponse(w, proto.OpCommit, proto.StatusInternal, err.Error())
	}
	return proto.WriteResponse(w, proto.OpCommit, proto.StatusOK, nil)
}

func (s *Server) handleJoinGroup(body []byte, w io.Writer) error {
	req, err := proto.DecodeJoinGroupRequest(body)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpJoinGroup, proto.StatusInvalidRequest, err.Error())
	}
	mgr := s.core.Groups()
	if mgr == nil {
		return proto.WriteErrorResponse(w, proto.OpJoinGroup, proto.StatusInvalidRequest,
			"server has no group manager")
	}
	res, err := mgr.Join(req.Group, req.MemberID, req.Topics)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpJoinGroup, classifyBrokerError(err), err.Error())
	}
	resp := proto.JoinGroupResponse{
		MemberID:    res.MemberID,
		Generation:  res.Generation,
		Assignments: make([]proto.AssignmentEntry, 0, len(res.Assignments)),
	}
	for _, a := range res.Assignments {
		resp.Assignments = append(resp.Assignments, proto.AssignmentEntry{
			Topic:           a.Partition.Topic,
			Partition:       a.Partition.Index,
			CommittedOffset: a.CommittedOffset,
		})
	}
	return proto.WriteResponse(w, proto.OpJoinGroup, proto.StatusOK, resp.Encode())
}

func (s *Server) handleHeartbeat(body []byte, w io.Writer) error {
	req, err := proto.DecodeHeartbeatRequest(body)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpHeartbeat, proto.StatusInvalidRequest, err.Error())
	}
	mgr := s.core.Groups()
	if mgr == nil {
		return proto.WriteErrorResponse(w, proto.OpHeartbeat, proto.StatusInvalidRequest,
			"server has no group manager")
	}
	res, err := mgr.Heartbeat(req.Group, req.MemberID, req.Generation)
	if err != nil {
		// Unknown member is reported as RebalanceNeeded so the SDK rejoins.
		return proto.WriteErrorResponse(w, proto.OpHeartbeat, proto.StatusUnknownMember, err.Error())
	}
	return proto.WriteResponse(w, proto.OpHeartbeat, proto.StatusOK,
		proto.HeartbeatResponse{RebalanceNeeded: res.RebalanceNeeded}.Encode())
}

func (s *Server) handleProduceBatch(apiKey string, body []byte, w io.Writer) error {
	req, err := proto.DecodeProduceBatchRequest(body)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpProduceBatch, proto.StatusInvalidRequest, err.Error())
	}
	if len(req.Records) == 0 {
		return proto.WriteResponse(w, proto.OpProduceBatch, proto.StatusOK,
			proto.ProduceBatchResponse{}.Encode())
	}
	if limiter := s.limiterFor(apiKey); limiter != nil {
		var totalBytes int64
		for _, r := range req.Records {
			totalBytes += int64(len(r.Value))
		}
		if !limiter.take(totalBytes) {
			return proto.WriteErrorResponse(w, proto.OpProduceBatch, proto.StatusRateLimited,
				"produce quota exceeded")
		}
	}
	pref := proto.PartitionRef{Topic: req.Topic, Index: req.Partition}
	baseOffset, err := s.core.Publish(context.Background(), pref, req.Records[0])
	if err != nil {
		return s.respondError(w, proto.OpProduceBatch, err)
	}
	for i := 1; i < len(req.Records); i++ {
		if _, err := s.core.Publish(context.Background(), pref, req.Records[i]); err != nil {
			return s.respondError(w, proto.OpProduceBatch, err)
		}
	}
	return proto.WriteResponse(w, proto.OpProduceBatch, proto.StatusOK,
		proto.ProduceBatchResponse{BaseOffset: baseOffset, Count: int32(len(req.Records))}.Encode())
}

func (s *Server) handleSync(body []byte, w io.Writer) error {
	req, err := proto.DecodeSyncRequest(body)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpSync, proto.StatusInvalidRequest, err.Error())
	}
	pref := proto.PartitionRef{Topic: req.Topic, Index: req.Partition}
	if err := s.core.Sync(context.Background(), pref); err != nil {
		return s.respondError(w, proto.OpSync, err)
	}
	return proto.WriteResponse(w, proto.OpSync, proto.StatusOK, nil)
}

func (s *Server) handleLeaveGroup(body []byte, w io.Writer) error {
	req, err := proto.DecodeLeaveGroupRequest(body)
	if err != nil {
		return proto.WriteErrorResponse(w, proto.OpLeaveGroup, proto.StatusInvalidRequest, err.Error())
	}
	mgr := s.core.Groups()
	if mgr == nil {
		return proto.WriteResponse(w, proto.OpLeaveGroup, proto.StatusOK, nil)
	}
	if err := mgr.Leave(req.Group, req.MemberID); err != nil {
		return proto.WriteErrorResponse(w, proto.OpLeaveGroup, proto.StatusInternal, err.Error())
	}
	return proto.WriteResponse(w, proto.OpLeaveGroup, proto.StatusOK, nil)
}

// classifyBrokerError maps internal broker errors to wire status codes.
// The match is by substring so callers don't depend on sentinel-error
// import paths from the internal packages.
func classifyBrokerError(err error) proto.Status {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "already exists"):
		return proto.StatusTopicExists
	case strings.Contains(msg, "not found"):
		return proto.StatusUnknownTopic
	case strings.Contains(msg, "out of range"):
		return proto.StatusInvalidPartition
	default:
		return proto.StatusInternal
	}
}
