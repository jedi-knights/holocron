// Package net is the network implementation of sdk.Transport. It speaks
// the wire protocol defined in proto/wire.go.
//
// One Transport holds a long-lived TCP connection used for produce,
// metadata, and commit calls. Subscribe spawns a dedicated connection
// per partition because long-poll Fetch blocks the connection for the
// duration of the wait window.
package net

import (
	"bufio"
	"context"
	"errors"
	"crypto/tls"
	"fmt"
	"strings"
	stdnet "net"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// fetchMaxWait controls the per-Fetch long-poll window in milliseconds.
const fetchMaxWaitMs = 1000

// fetchBatchSize is the per-Fetch record cap requested from the broker.
const fetchBatchSize = 256

// subscribeBuffer sizes the per-partition output channel returned to the
// SDK Consumer.
const subscribeBuffer = 256

// Transport is a network sdk.Transport.
type Transport struct {
	addr        string
	dialTimeout time.Duration
	tlsConfig   *tls.Config
	apiKey      string

	mu     sync.Mutex
	rpc    *connection // shared connection for unary RPCs (produce, metadata, commit, create)
	codec  proto.Codec
	closed bool

	subWG     sync.WaitGroup
	subCancel []context.CancelFunc
}

// SetCompression configures the wire-level compression codec applied to
// every PublishBatch this Transport sends. Producers call this via a
// type assertion when their WithCompression option is set.
func (t *Transport) SetCompression(c proto.Codec) {
	t.mu.Lock()
	t.codec = c
	t.mu.Unlock()
}

// Option configures a Transport.
type Option func(*Transport)

// WithDialTimeout sets the per-dial timeout.
func WithDialTimeout(d time.Duration) Option {
	return func(t *Transport) { t.dialTimeout = d }
}

// WithTLS uses TLS for every connection (RPC and subscription pumps).
// The supplied tls.Config is forwarded to crypto/tls; common cases are
// `&tls.Config{RootCAs: pool}` (server-cert verification) or
// `&tls.Config{InsecureSkipVerify: true}` (lab use only).
func WithTLS(cfg *tls.Config) Option {
	return func(t *Transport) { t.tlsConfig = cfg }
}

// WithAPIKey sends the given key in the handshake on every new
// connection. Brokers configured with an allow-list of keys reject
// connections that don't present a matching one.
func WithAPIKey(key string) Option {
	return func(t *Transport) { t.apiKey = key }
}

// Dial opens a network Transport to addr. The returned Transport is ready
// for use; the underlying connection is established lazily on the first
// RPC and reconnected on transport-level errors.
func Dial(addr string, opts ...Option) (*Transport, error) {
	t := &Transport{
		addr:        addr,
		dialTimeout: 5 * time.Second,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t, nil
}

// Compile-time check that *Transport satisfies sdk.Transport.
var _ sdk.Transport = (*Transport)(nil)

// Publish appends a record over the network.
func (t *Transport) Publish(ctx context.Context, p proto.PartitionRef, r proto.Record) (int64, error) {
	body, err := t.do(ctx, proto.OpProduce, proto.ProduceRequest{
		Topic:     p.Topic,
		Partition: p.Index,
		Record:    r,
	}.Encode())
	if err != nil {
		return 0, err
	}
	resp, err := proto.DecodeProduceResponse(body)
	if err != nil {
		return 0, err
	}
	return resp.Offset, nil
}

// PublishBatch sends N records in one wire frame. The server appends
// them in order; the response carries the offset of the first record.
// If SetCompression was called with a non-zero codec, the records
// portion of the request is compressed in transit.
func (t *Transport) PublishBatch(ctx context.Context, p proto.PartitionRef, records []proto.Record) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}
	t.mu.Lock()
	codec := t.codec
	t.mu.Unlock()
	body, err := t.do(ctx, proto.OpProduceBatch, proto.ProduceBatchRequest{
		Topic:     p.Topic,
		Partition: p.Index,
		Codec:     codec,
		Records:   records,
	}.Encode())
	if err != nil {
		return 0, err
	}
	resp, err := proto.DecodeProduceBatchResponse(body)
	if err != nil {
		return 0, err
	}
	return resp.BaseOffset, nil
}

// Subscribe spawns a goroutine that long-polls Fetch on a dedicated
// connection and pumps records onto the returned channel.
func (t *Transport) Subscribe(ctx context.Context, p proto.PartitionRef, fromOffset int64) (<-chan proto.Record, <-chan error, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, nil, errors.New("net: transport is closed")
	}
	pumpCtx, cancel := context.WithCancel(ctx)
	t.subCancel = append(t.subCancel, cancel)
	t.subWG.Add(1)
	t.mu.Unlock()

	out := make(chan proto.Record, subscribeBuffer)
	errCh := make(chan error, 1)
	go t.subscribePump(pumpCtx, p, fromOffset, out, errCh)
	return out, errCh, nil
}

// subscribePump drives one Subscribe stream. On a protocol-level
// failure it pushes the error onto errCh before exiting; the records
// channel always closes via defer so the consumer can distinguish
// "stream ended" from "stream ended with error" by polling errCh.
func (t *Transport) subscribePump(ctx context.Context, p proto.PartitionRef, fromOffset int64, out chan<- proto.Record, errCh chan<- error) {
	defer t.subWG.Done()
	defer close(out)
	defer close(errCh)

	conn, err := t.dialAndHandshake(ctx)
	if err != nil {
		select {
		case errCh <- err:
		default:
		}
		return
	}
	defer conn.close()

	cursor := fromOffset
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		req := proto.FetchRequest{
			Topic:       p.Topic,
			Partition:   p.Index,
			FromOffset:  cursor,
			MaxRecords:  fetchBatchSize,
			MaxWaitMs:   fetchMaxWaitMs,
			AcceptCodec: proto.CodecLZ4,
		}
		body, err := conn.call(proto.OpFetch, req.Encode())
		if err != nil {
			if ctx.Err() == nil {
				select {
				case errCh <- err:
				default:
				}
			}
			return
		}
		resp, err := proto.DecodeFetchResponse(body)
		if err != nil {
			select {
			case errCh <- err:
			default:
			}
			return
		}
		for _, r := range resp.Records {
			select {
			case out <- r:
				cursor = r.Offset + 1
			case <-ctx.Done():
				return
			}
		}
	}
}

// Commit is a no-op through Stage 3 but still routes to the broker so
// future stages can light it up without an SDK change.
func (t *Transport) Commit(ctx context.Context, group string, p proto.PartitionRef, offset int64) error {
	_, err := t.do(ctx, proto.OpCommit, proto.CommitRequest{
		Group:     group,
		Topic:     p.Topic,
		Partition: p.Index,
		Offset:    offset,
	}.Encode())
	return err
}

// PartitionsFor reads a topic's partition count from the broker.
func (t *Transport) PartitionsFor(ctx context.Context, topic string) (int32, error) {
	body, err := t.do(ctx, proto.OpMetadata, proto.MetadataRequest{Topic: topic}.Encode())
	if err != nil {
		return 0, err
	}
	resp, err := proto.DecodeMetadataResponse(body)
	if err != nil {
		return 0, err
	}
	return resp.PartitionCount, nil
}

// CreateTopic registers a topic via the network. Not part of sdk.Transport
// — exposed as a convenience for examples and tests since topic creation
// does not currently flow through Producer/Consumer.
func (t *Transport) CreateTopic(ctx context.Context, name string, partitionCount int32) error {
	_, err := t.do(ctx, proto.OpCreateTopic, proto.CreateTopicRequest{
		Name:           name,
		PartitionCount: partitionCount,
	}.Encode())
	return err
}

// Member is a (id, addr) tuple returned by ClusterMembers. Mirrors
// proto.ClusterMember without dragging the wire-types into callers.
type Member struct {
	ID   string
	Addr string
}

// ClusterMembers asks the broker for the current Raft membership.
// Available on every node (followers don't redirect). Returns an
// empty slice when the broker isn't part of a cluster.
func (t *Transport) ClusterMembers(ctx context.Context) ([]Member, error) {
	body, err := t.do(ctx, proto.OpClusterMembers, proto.ClusterMembersRequest{}.Encode())
	if err != nil {
		return nil, err
	}
	resp, err := proto.DecodeClusterMembersResponse(body)
	if err != nil {
		return nil, err
	}
	out := make([]Member, 0, len(resp.Members))
	for _, m := range resp.Members {
		out = append(out, Member{ID: m.ID, Addr: m.Addr})
	}
	return out, nil
}

// AddVoter requests the leader add a peer to the cluster. Followers
// surface StatusNotLeader and the SDK auto-redirects to the leader's
// wire address before retrying.
func (t *Transport) AddVoter(ctx context.Context, id, addr string) error {
	_, err := t.do(ctx, proto.OpAddVoter, proto.AddVoterRequest{ID: id, Addr: addr}.Encode())
	return err
}

// RemoveVoter requests the leader drop a peer from the cluster.
func (t *Transport) RemoveVoter(ctx context.Context, id string) error {
	_, err := t.do(ctx, proto.OpRemoveVoter, proto.RemoveVoterRequest{ID: id}.Encode())
	return err
}

// HighWater returns the next-to-be-appended offset for the partition.
// Mirrors inproc.Transport.HighWater so registry's bounded replay path
// works over the wire too.
func (t *Transport) HighWater(ctx context.Context, p proto.PartitionRef) (int64, error) {
	body, err := t.do(ctx, proto.OpHighWater, proto.HighWaterRequest{
		Topic:     p.Topic,
		Partition: p.Index,
	}.Encode())
	if err != nil {
		return 0, err
	}
	resp, err := proto.DecodeHighWaterResponse(body)
	if err != nil {
		return 0, err
	}
	return resp.HighWater, nil
}

// EnsureTopic creates the topic if it doesn't exist, returning nil if a
// topic by that name already exists. Matches the inproc.Transport
// semantic so connect can auto-create coordination topics regardless of
// whether the worker talks to a local or networked broker.
//
// Detection of "already exists" is by ProtocolError.Status; the
// substring fallback covers older brokers that don't yet emit
// StatusTopicExists.
func (t *Transport) EnsureTopic(ctx context.Context, name string, partitions int32) error {
	if partitions <= 0 {
		partitions = 1
	}
	err := t.CreateTopic(ctx, name, partitions)
	if err == nil {
		return nil
	}
	var pe *proto.ProtocolError
	if errors.As(err, &pe) && pe.Status == proto.StatusTopicExists {
		return nil
	}
	if strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return err
}

// JoinGroup signs the caller into a consumer group over the wire.
func (t *Transport) JoinGroup(ctx context.Context, group, memberID string, topics []string) (sdk.JoinResult, error) {
	body, err := t.do(ctx, proto.OpJoinGroup, proto.JoinGroupRequest{
		Group:    group,
		MemberID: memberID,
		Topics:   topics,
	}.Encode())
	if err != nil {
		return sdk.JoinResult{}, err
	}
	resp, err := proto.DecodeJoinGroupResponse(body)
	if err != nil {
		return sdk.JoinResult{}, err
	}
	out := sdk.JoinResult{
		MemberID:    resp.MemberID,
		Generation:  resp.Generation,
		Assignments: make([]sdk.Assignment, 0, len(resp.Assignments)),
	}
	for _, a := range resp.Assignments {
		out.Assignments = append(out.Assignments, sdk.Assignment{
			Partition:       proto.PartitionRef{Topic: a.Topic, Index: a.Partition},
			CommittedOffset: a.CommittedOffset,
		})
	}
	return out, nil
}

// Heartbeat reports liveness over the wire. RebalanceNeeded is also
// returned when the broker responds with StatusRebalanceNeeded.
func (t *Transport) Heartbeat(ctx context.Context, group, memberID string, generation int32) (sdk.HeartbeatResult, error) {
	body, err := t.do(ctx, proto.OpHeartbeat, proto.HeartbeatRequest{
		Group:      group,
		MemberID:   memberID,
		Generation: generation,
	}.Encode())
	if err != nil {
		if proto.IsStatus(err, proto.StatusRebalanceNeeded) {
			return sdk.HeartbeatResult{RebalanceNeeded: true}, nil
		}
		return sdk.HeartbeatResult{}, err
	}
	resp, err := proto.DecodeHeartbeatResponse(body)
	if err != nil {
		return sdk.HeartbeatResult{}, err
	}
	return sdk.HeartbeatResult{RebalanceNeeded: resp.RebalanceNeeded}, nil
}

// LeaveGroup deregisters memberID over the wire.
func (t *Transport) LeaveGroup(ctx context.Context, group, memberID string) error {
	_, err := t.do(ctx, proto.OpLeaveGroup, proto.LeaveGroupRequest{
		Group:    group,
		MemberID: memberID,
	}.Encode())
	return err
}

// Sync requests durable persistence of the partition over the wire.
func (t *Transport) Sync(ctx context.Context, p proto.PartitionRef) error {
	_, err := t.do(ctx, proto.OpSync, proto.SyncRequest{
		Topic:     p.Topic,
		Partition: p.Index,
	}.Encode())
	return err
}

// Close closes the shared connection and cancels every active
// subscription pump, then waits for all pumps to return.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	rpc := t.rpc
	cancels := t.subCancel
	t.rpc = nil
	t.subCancel = nil
	t.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	if rpc != nil {
		_ = rpc.close()
	}
	t.subWG.Wait()
	return nil
}

// do sends a unary RPC on the shared connection, reconnecting if needed.
func (t *Transport) do(ctx context.Context, op proto.OpCode, payload []byte) ([]byte, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, errors.New("net: transport is closed")
	}
	t.mu.Unlock()

	for attempt := range 4 {
		conn, err := t.rpcConn(ctx)
		if err != nil {
			return nil, err
		}
		body, err := conn.call(op, payload)
		if err == nil {
			return body, nil
		}
		var pe *proto.ProtocolError
		if errors.As(err, &pe) {
			// Cluster mode: redirect to the leader and retry. The
			// broker puts the leader's address in the error message.
			if pe.Status == proto.StatusNotLeader && pe.Message != "" {
				t.redirectTo(pe.Message)
				continue
			}
			return nil, err
		}
		// On a transport-level error, drop the connection and retry once.
		t.dropRPC()
		if attempt == 3 {
			return nil, fmt.Errorf("net: opcode 0x%02x: %w", byte(op), err)
		}
	}
	return nil, errors.New("net: opcode redirected too many times")
}

// redirectTo points the shared RPC connection at a new address. Used when
// the broker reports StatusNotLeader; the caller's next call will dial
// the leader instead.
func (t *Transport) redirectTo(addr string) {
	t.mu.Lock()
	t.addr = addr
	rpc := t.rpc
	t.rpc = nil
	t.mu.Unlock()
	if rpc != nil {
		_ = rpc.close()
	}
}

func (t *Transport) rpcConn(ctx context.Context) (*connection, error) {
	t.mu.Lock()
	if t.rpc != nil {
		c := t.rpc
		t.mu.Unlock()
		return c, nil
	}
	t.mu.Unlock()

	conn, err := t.dialAndHandshake(ctx)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = conn.close()
		return nil, errors.New("net: transport is closed")
	}
	if t.rpc != nil {
		// Lost the race — another goroutine dialed first.
		_ = conn.close()
		conn = t.rpc
	} else {
		t.rpc = conn
	}
	t.mu.Unlock()
	return conn, nil
}

func (t *Transport) dropRPC() {
	t.mu.Lock()
	c := t.rpc
	t.rpc = nil
	t.mu.Unlock()
	if c != nil {
		_ = c.close()
	}
}

func (t *Transport) dialAndHandshake(ctx context.Context) (*connection, error) {
	d := stdnet.Dialer{Timeout: t.dialTimeout}
	var raw stdnet.Conn
	var err error
	if t.tlsConfig != nil {
		td := tls.Dialer{NetDialer: &d, Config: t.tlsConfig}
		raw, err = td.DialContext(ctx, "tcp", t.addr)
	} else {
		raw, err = d.DialContext(ctx, "tcp", t.addr)
	}
	if err != nil {
		return nil, fmt.Errorf("net: dial %s: %w", t.addr, err)
	}
	c := &connection{
		conn:   raw,
		r:      bufio.NewReader(raw),
		w:      bufio.NewWriter(raw),
		apiKey: t.apiKey,
	}
	if err := c.handshake(); err != nil {
		_ = c.close()
		return nil, err
	}
	return c, nil
}

// connection is a single TCP connection to the broker. Each connection is
// request-response synchronous; concurrent callers must serialize at a
// higher level (the Transport's mutex on the shared connection, or one
// connection per subscription).
type connection struct {
	conn   stdnet.Conn
	r      *bufio.Reader
	w      *bufio.Writer
	apiKey string
	mu     sync.Mutex
}

func (c *connection) handshake() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	body := proto.HandshakeRequest{Version: proto.WireVersion, APIKey: c.apiKey}.Encode()
	if err := proto.WriteFrame(c.w, proto.OpHandshake, body); err != nil {
		return err
	}
	if err := c.w.Flush(); err != nil {
		return err
	}
	resp, err := proto.ReadResponse(c.r, proto.OpHandshake)
	if err != nil {
		return err
	}
	if len(resp) == 0 || resp[0] != proto.WireVersion {
		return fmt.Errorf("net: server reported unexpected wire version: %v", resp)
	}
	return nil
}

func (c *connection) call(op proto.OpCode, payload []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := proto.WriteFrame(c.w, op, payload); err != nil {
		return nil, err
	}
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	return proto.ReadResponse(c.r, op)
}

func (c *connection) close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

