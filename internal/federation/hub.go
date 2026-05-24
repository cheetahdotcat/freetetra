package federation

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	federationv2pb "github.com/freetetra/server/internal/federation/proto/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	grpcpeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	federationSubpathHeader = "x-brew-subpath"

	// ProtocolVersion is the highest federation protocol version we speak.
	ProtocolVersion = 2

	// MinSupportedProtocolVersion is the lowest peer version we accept.
	// A peer outside [Min, ProtocolVersion] gets rejected at Hello time
	// with FailedPrecondition; the dialer treats this as a permanent
	// error and stops reconnecting.
	MinSupportedProtocolVersion = 2

	// peerRedemptionWindow keeps a disconnected peer's ISSI/GSSI tables
	// around so a fast reconnect can adopt them instead of waiting for a
	// fresh sync round-trip. Short flaps (e.g. an upstream nginx reset)
	// no longer cause federated routing for that peer's subscribers to
	// drop out, and the "cleaned up peer X (N ISSIs removed)" log only
	// fires when the peer really stays gone.
	peerRedemptionWindow = 30 * time.Second
)

// PeerConfig is the configuration for a federation peer.
type PeerConfig struct {
	Name string `json:"name"`
	URL  string `json:"url"`
	Key  string `json:"key"`
}

// CallHandler is the interface that the Brew service must implement
// to receive federation events.
type CallHandler interface {
	OnPeerCallStart(peerName string, callUUID string, sourceISSI, destGSSI uint32, priority uint8, service uint16)
	OnPeerPrivateCallStart(peerName string, callUUID string, sourceISSI, destISSI uint32, priority uint8, service uint16)
	OnPeerCallEnd(peerName string, callUUID string, cause uint8)
	// OnPeerCallReply receives post-CallStart, pre-CallEnd signaling
	// (brew SetupAccept=5, SetupReject=6, ConnectRequest=8) routed across
	// the federation back to the originating side of a private call.
	OnPeerCallReply(peerName string, callUUID string, state, cause uint8)
	OnPeerVoiceFrame(peerName string, callUUID string, frameData []byte)
	OnPeerSDSRelay(peerName string, sourceISSI, destISSI uint32, sdsDataHex string)
	OnPeerPositionSample(peerName string, issi uint32, lat, lon float64, repeater string)
	// OnPeerStationUpdate is invoked for inbound StationUpdate ctrl messages.
	// `origin` is ctrl.Origin (the originating server name); `peerName` is the
	// immediate sender. Implementations should attribute the station to origin
	// — peerName may be a relay several hops removed.
	OnPeerStationUpdate(origin, peerName string, station map[string]any)
	GetLocalSubscribers() map[uint32][]uint32

	// Users DB sync (optional — return empty if not available)
	GetUsersDBInfo() (timestamp string, count int)
	// DownloadUsersDBFrom fetches the users DB from a peer's URL
	DownloadUsersDBFrom(url string) error
}

// FreeTetra GSSI Schema:
//
//	1-9:    Local — only this cell, never forwarded between servers
//	10-90:  FreeTetra global — shared between all FreeTetra servers
//	91+:    BrandMeister-compatible — shared between FreeTetra servers AND
//	        bridged to DMR/BrandMeister on servers where dmrbridge is configured.
//	        TG numbers map 1:1 (e.g. TG 262 = DL, TG 2621 = DL Cluster Nord).
const (
	federationGSSIMin uint32 = 10
)

// isFederatedGSSI returns true if a GSSI should be shared between FreeTetra
// servers. TG 91+ is also federated; whether it's additionally bridged to
// BrandMeister depends on per-server dmrbridge config.
func isFederatedGSSI(gssi uint32) bool {
	return gssi >= federationGSSIMin
}

// pendingPeer is a disconnected peer being held in the redemption window —
// its subscriber tables stay intact in case the same peer reconnects soon,
// at which point registerPeer/renamePeer transfers the state to the new
// peer object and cancels the timer. If the timer fires first, the peer is
// fully cleaned up.
type pendingPeer struct {
	peer  *Peer
	timer *time.Timer
}

// Hub manages all federation peer connections.
type Hub struct {
	federationv2pb.UnimplementedFederationTransportV2Server

	serverName string
	peerKey    string // shared key for incoming peer auth
	rpcListen  string
	handler    CallHandler
	logger     *log.Logger

	mu    sync.RWMutex
	peers map[string]*Peer // peer name -> Peer

	// Peers whose stream just died but whose subscriber tables we're
	// holding onto for peerRedemptionWindow in case the same peer
	// reconnects. Keyed by the same direction-suffixed peerKey() as
	// h.peers so a redemption matches the original slot exactly.
	pendingMu    sync.Mutex
	pendingPeers map[string]*pendingPeer

	// Active calls routed to peers
	callMu       sync.RWMutex
	activeCalls  map[string]map[string]bool // callUUID -> set of peer names (group / broadcast)
	privateCalls map[string]string          // callUUID -> peer.Name (subscriber-to-subscriber, point-to-point)

	// Gossip: known peer URLs (discovered via peer exchange)
	knownMu    sync.RWMutex
	knownPeers map[string]string // name -> URL (all peers we've ever heard of)

	// Our own public URL for advertising to peers
	selfURL string

	// Mesh routing: deduplication and TTL
	mesh *MeshRouter

	ctx context.Context

	serveStandaloneRPC bool
}

// NewHub creates a new federation hub.
func NewHub(serverName, peerKey, selfURL, rpcListen string, handler CallHandler, logger *log.Logger) *Hub {
	if rpcListen == "" {
		rpcListen = ":8092"
	}
	return &Hub{
		serverName:         serverName,
		peerKey:            peerKey,
		selfURL:            selfURL,
		rpcListen:          rpcListen,
		handler:            handler,
		logger:             logger,
		peers:              make(map[string]*Peer),
		pendingPeers:       make(map[string]*pendingPeer),
		activeCalls:        make(map[string]map[string]bool),
		privateCalls:       make(map[string]string),
		knownPeers:         make(map[string]string),
		mesh:               newMeshRouter(serverName),
		serveStandaloneRPC: true,
	}
}

// UseSharedPortRPC disables the dedicated RPC listener. Incoming federation
// gRPC traffic must then be served via NewGRPCServer on the HTTP listener.
func (h *Hub) UseSharedPortRPC() {
	h.serveStandaloneRPC = false
}

// NewGRPCServer returns a gRPC server with the federation service registered.
func (h *Hub) NewGRPCServer() *grpc.Server {
	server := grpc.NewServer(federationServerOptions()...)
	federationv2pb.RegisterFederationTransportV2Server(server, h)
	return server
}

// federationServerOptions bumps the HTTP/2 initial windows from the gRPC
// default 64 KB to 1 MB on each direction. Anti-entropy bursts ship dozens
// of small control messages back-to-back at peer-connect time; the default
// window fills before the peer's first WINDOW_UPDATE arrives, stalling
// writeLoop and filling the 256-slot Go send channel within ~32 ms (the
// "send buffer full" pattern we saw on every fresh handshake). 1 MB gives
// roughly 2000 messages of headroom before backpressure shows.
func federationServerOptions() []grpc.ServerOption {
	const window = 1 << 20
	return []grpc.ServerOption{
		grpc.InitialWindowSize(window),
		grpc.InitialConnWindowSize(window),
	}
}

// Start connects to all configured peers and begins the federation loop.
func (h *Hub) Start(ctx context.Context, peerConfigs []PeerConfig) {
	h.ctx = ctx
	if h.serveStandaloneRPC {
		go h.serveRPC(ctx)
	}

	go h.statsLoop(ctx)

	// Add configured peers to known list
	for _, pc := range peerConfigs {
		h.knownMu.Lock()
		h.knownPeers[pc.Name] = pc.URL
		h.knownMu.Unlock()
		go h.maintainOutgoingPeer(ctx, pc)
	}
}

// statsLoop emits a one-line snapshot of every peer's send-buffer state
// every 30s. The line is "<name>/<dir> q=L/C sent=cN/vM dropped=cD/vV"
// where q is the Go-channel occupancy, sent/dropped are lifetime totals
// for control vs voice frames. The window is the same as the anti-entropy
// cadence, which is the main burst source — line up the two in the log to
// see whether the burst was fully absorbed or whether some peer's queue
// stayed high afterwards.
func (h *Hub) statsLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.logPeerStats()
		}
	}
}

func (h *Hub) logPeerStats() {
	h.mu.RLock()
	if len(h.peers) == 0 {
		h.mu.RUnlock()
		return
	}
	parts := make([]string, 0, len(h.peers))
	for _, p := range h.peers {
		bs := p.BufferStats()
		dir := "out"
		if p.Direction == "incoming" {
			dir = "in"
		}
		parts = append(parts, fmt.Sprintf("%s/%s q=%d/%d sent=c%d/v%d dropped=c%d/v%d",
			p.Name, dir, bs.QueueLen, bs.QueueCap,
			bs.SentControl, bs.SentVoice,
			bs.DroppedControl, bs.DroppedVoice))
	}
	h.mu.RUnlock()
	h.logger.Printf("federation: peer stats: %s", strings.Join(parts, " | "))
}

func (h *Hub) serveRPC(ctx context.Context) {
	lis, err := net.Listen("tcp", h.rpcListen)
	if err != nil {
		h.logger.Printf("federation: RPC listen failed on %s: %v", h.rpcListen, err)
		return
	}
	server := grpc.NewServer(federationServerOptions()...)
	federationv2pb.RegisterFederationTransportV2Server(server, h)
	h.logger.Printf("federation: protobuf RPC listening on %s", h.rpcListen)

	go func() {
		<-ctx.Done()
		server.GracefulStop()
		_ = lis.Close()
	}()

	if err := server.Serve(lis); err != nil && ctx.Err() == nil {
		h.logger.Printf("federation: RPC server stopped: %v", err)
	}
}

// buildHello constructs the Hello control message advertised to peers.
func (h *Hub) buildHello(_ string) *federationv2pb.Control {
	return &federationv2pb.Control{
		Origin:          h.serverName,
		ProtocolVersion: ProtocolVersion,
		Payload: &federationv2pb.Control_Hello{
			Hello: &federationv2pb.Hello{},
		},
	}
}

// maintainOutgoingPeer keeps a persistent connection to an outgoing peer.
// Reconnects with exponential backoff: 10s → 20s → 40s → … capped at 15min.
// Backoff resets after a stream is successfully established.
func (h *Hub) maintainOutgoingPeer(ctx context.Context, pc PeerConfig) {
	const (
		baseDelay = 10 * time.Second
		maxDelay  = 15 * time.Minute
	)
	delay := baseDelay
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		h.logger.Printf("federation: connecting to peer %s at %s", pc.Name, pc.URL)
		target, secure, err := normalizeRPCTarget(pc.URL)
		if err != nil {
			// Config-level error — no amount of retrying fixes a malformed URL.
			h.logger.Printf("federation: invalid peer target %s: %v", pc.URL, err)
			return
		}

		var creds credentials.TransportCredentials
		if secure {
			creds = credentials.NewTLS(&tls.Config{})
		} else {
			creds = insecure.NewCredentials()
		}
		conn, err := grpc.NewClient(target,
			grpc.WithTransportCredentials(creds),
			grpc.WithInitialWindowSize(1<<20),
			grpc.WithInitialConnWindowSize(1<<20),
		)
		if err != nil {
			h.logger.Printf("federation: failed to connect to %s: %v (retry in %s)", pc.Name, err, delay)
			if !waitBackoff(ctx, &delay, baseDelay, maxDelay) {
				return
			}
			continue
		}

		callCtx, cancel := context.WithCancel(ctx)
		md := metadata.New(map[string]string{
			"x-brew-peer":           h.serverName,
			"x-brew-key":            pc.Key,
			federationSubpathHeader: federationSubpathForKey(pc.Key),
		})
		stream, err := federationv2pb.NewFederationTransportV2Client(conn).Connect(metadata.NewOutgoingContext(callCtx, md))
		if err != nil {
			cancel()
			_ = conn.Close()
			h.logHandshakeError(pc.Name, err, delay)
			if !waitBackoff(ctx, &delay, baseDelay, maxDelay) {
				return
			}
			continue
		}

		peer := newPeer(pc.Name, "outgoing", stream, cancel, h.logger)
		h.registerPeer(peer)
		h.logger.Printf("federation: connected to %s", pc.Name)
		// A successful stream means the endpoint is healthy — reset backoff.
		delay = baseDelay

		_ = peer.SendControl(h.buildHello(pc.Name))
		h.sendFullSync(peer)

		go peer.writeLoop()
		h.readLoop(peer)
		_ = conn.Close()
		// readLoop's defer already ran unregisterPeer — don't double-cleanup.

		// If the peer was rejected for a permanent reason (e.g. version
		// mismatch) there is no point retrying — config or code on one
		// side has to change first.
		if closeErr := peer.CloseErr(); closeErr != nil && isPermanentRejection(closeErr) {
			h.logger.Printf("federation: peer %s permanently disabled: %v (no reconnect)", pc.Name, closeErr)
			return
		}

		h.logger.Printf("federation: reconnecting to %s in %s...", pc.Name, delay)

		if !waitBackoff(ctx, &delay, baseDelay, maxDelay) {
			return
		}
	}
}

// waitBackoff sleeps for *delay, then doubles it up to max. Returns false if
// the context was canceled during the sleep. delay is mutated in place so the
// caller can reset it on success.
func waitBackoff(ctx context.Context, delay *time.Duration, base, max time.Duration) bool {
	current := *delay
	if current < base {
		current = base
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(current):
	}
	next := current * 2
	if next > max {
		next = max
	}
	*delay = next
	return true
}

// readLoop reads messages from a peer.
//
// Recv() runs in a separate goroutine so the main loop can also wake on
// peer.done — that's what lets registerPeer's eviction of an old incoming
// peer actually unblock readLoop. Without this, an incoming peer's stream
// stays alive after Close() (cancel is nil for incoming peers) and keeps
// feeding handler.OnPeerVoiceFrame / OnPeerCallStart for traffic that's
// also flowing through its replacement — the audio-duplication bug.
func (h *Hub) readLoop(peer *Peer) {
	defer h.unregisterPeer(peer)

	type recvResult struct {
		frame *federationv2pb.StreamFrame
		err   error
	}
	recvCh := make(chan recvResult, 1)
	go func() {
		for {
			frame, err := peer.stream.Recv()
			select {
			case recvCh <- recvResult{frame: frame, err: err}:
			case <-peer.done:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		var r recvResult
		select {
		case r = <-recvCh:
		case <-peer.done:
			return
		}

		if r.err != nil {
			if r.err != io.EOF {
				h.logger.Printf("federation: %s read error: %v", peer.Name, r.err)
			} else {
				h.logger.Printf("federation: %s disconnected", peer.Name)
			}
			return
		}

		if ctrl := r.frame.GetControl(); ctrl != nil {
			h.handleControlMessage(peer, ctrl)
			// A handler (e.g. handleHello version-check) may have marked
			// the peer for rejection. Exit the loop so Connect returns
			// the status to gRPC and maintainOutgoingPeer can decide
			// whether to retry.
			if peer.CloseErr() != nil {
				return
			}
			continue
		}
		if vf := r.frame.GetVoiceFrame(); vf != nil {
			h.handleVoiceFrame(peer, vf.GetCallUuid(), vf.GetFrameData())
		}
	}
}

// Connect handles an incoming protobuf RPC stream from another federation peer.
//
// Errors returned here are gRPC status errors so the remote dialer sees the
// reason (Unauthenticated for bad key, PermissionDenied for bad subpath,
// FailedPrecondition for version mismatch, etc.) instead of a generic
// transport error. Every rejection is also logged locally with the peer name
// and remote address.
func (h *Hub) Connect(stream grpc.BidiStreamingServer[federationv2pb.StreamFrame, federationv2pb.StreamFrame]) error {
	remote := remoteAddrFromContext(stream.Context())

	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		h.logger.Printf("federation: rejected incoming connection from %s — no gRPC metadata", remote)
		return status.Error(codes.InvalidArgument, "missing metadata")
	}
	peerName := firstMD(md, "x-brew-peer")
	peerKey := firstMD(md, "x-brew-key")
	peerSubpath := firstMD(md, federationSubpathHeader)

	if peerName == "" {
		h.logger.Printf("federation: rejected incoming connection from %s — missing x-brew-peer header", remote)
		return status.Error(codes.InvalidArgument, "missing x-brew-peer header")
	}
	if peerKey == "" {
		h.logger.Printf("federation: rejected incoming peer %s (%s) — missing x-brew-key header", peerName, remote)
		return status.Error(codes.Unauthenticated, "missing x-brew-key header")
	}
	if peerKey != h.peerKey {
		h.logger.Printf("federation: rejected incoming peer %s (%s) — invalid shared key", peerName, remote)
		return status.Error(codes.Unauthenticated, "invalid shared key")
	}
	expectedSubpath := federationSubpathForKey(h.peerKey)
	if peerSubpath != expectedSubpath {
		h.logger.Printf("federation: rejected incoming peer %s (%s) — invalid federation subpath", peerName, remote)
		return status.Error(codes.PermissionDenied, "invalid federation subpath")
	}

	peer := newPeer(peerName, "incoming", stream, nil, h.logger)
	h.registerPeer(peer)
	h.logger.Printf("federation: accepted incoming peer %s (%s)", peerName, remote)

	_ = peer.SendControl(h.buildHello(peerName))
	h.sendFullSync(peer)

	go peer.writeLoop()
	h.readLoop(peer)

	// readLoop returns either on normal disconnect (nil) or on a handler
	// rejection (e.g. version mismatch). Surface the latter to gRPC so the
	// dialer sees the status code.
	return peer.CloseErr()
}

// remoteAddrFromContext extracts "host:port" of the peer's transport address
// from a gRPC server context, or "unknown" if it isn't available.
func remoteAddrFromContext(ctx context.Context) string {
	p, ok := grpcpeer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return "unknown"
	}
	return p.Addr.String()
}

func (h *Hub) handleControlMessage(peer *Peer, ctrl *federationv2pb.Control) {
	if ctrl == nil {
		return
	}

	// Mesh routing gate — preserves the exact set of message types treated
	// as control-plane (origin-only check) by the legacy handleJSONMessage.
	// NOTE: UsersDbOffer / UsersDbRequest fall into the default (data-plane)
	// branch here, matching pre-refactor behavior. The sender does not call
	// PrepareOutgoing on those messages, so TTL stays 0 and ShouldProcess
	// drops them. That is a pre-existing bug in users-DB federation, not a
	// regression introduced here; out of scope for this refactor.
	switch ctrl.GetPayload().(type) {
	case *federationv2pb.Control_Hello,
		*federationv2pb.Control_SyncRequest,
		*federationv2pb.Control_SyncResponse,
		*federationv2pb.Control_PeerExchange:
		if ctrl.GetOrigin() == h.serverName {
			return
		}
	default:
		if !h.mesh.ShouldProcess(ctrl) {
			return
		}
	}

	switch p := ctrl.GetPayload().(type) {
	case *federationv2pb.Control_Hello:
		h.handleHello(peer, ctrl, p.Hello)
	case *federationv2pb.Control_SyncRequest:
		h.sendFullSync(peer)
	case *federationv2pb.Control_SyncResponse:
		h.handleSyncResponse(peer, p.SyncResponse)
	case *federationv2pb.Control_UsersDbOffer:
		h.handleUsersDBOffer(peer, p.UsersDbOffer)
	case *federationv2pb.Control_UsersDbRequest:
		h.sendUsersDBOffer(peer)
	case *federationv2pb.Control_PeerExchange:
		h.handlePeerExchange(peer, p.PeerExchange)
	case *federationv2pb.Control_SubscriberUpdate:
		h.handleSubscriberUpdate(peer, ctrl, p.SubscriberUpdate)
	case *federationv2pb.Control_AffiliateUpdate:
		h.handleAffiliateUpdate(peer, ctrl, p.AffiliateUpdate)
	case *federationv2pb.Control_CallStart:
		h.handleCallStart(peer, ctrl, p.CallStart)
	case *federationv2pb.Control_CallEnd:
		h.handleCallEnd(peer, ctrl, p.CallEnd)
	case *federationv2pb.Control_SdsRelay:
		h.handleSdsRelay(peer, ctrl, p.SdsRelay)
	case *federationv2pb.Control_PositionSample:
		h.handlePositionSample(peer, ctrl, p.PositionSample)
	case *federationv2pb.Control_StationUpdate:
		h.handleStationUpdate(peer, ctrl, p.StationUpdate)
	case *federationv2pb.Control_CallReply:
		h.handleCallReply(peer, ctrl, p.CallReply)
	}
}

func firstMD(md metadata.MD, key string) string {
	vals := md.Get(key)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

// normalizeRPCTarget parses a peer URL and returns the gRPC dial target plus
// a flag indicating whether the dialer should use TLS. Supported forms:
//
//	host:port               → h2c, target as-is
//	https://host[:port]      → TLS, default port 443
//	grpcs://host[:port]      → TLS, default port 443
//	http://host[:port]       → h2c, default port 80
//	grpc://host[:port]       → h2c, default port 80
func normalizeRPCTarget(raw string) (string, bool, error) {
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return "", false, err
		}
		if u.Host == "" {
			return "", false, fmt.Errorf("empty host")
		}
		scheme := strings.ToLower(u.Scheme)
		secure := scheme == "https" || scheme == "grpcs"
		host := u.Host
		if !strings.Contains(host, ":") {
			if secure {
				host += ":443"
			} else {
				host += ":80"
			}
		}
		return host, secure, nil
	}
	if strings.TrimSpace(raw) == "" {
		return "", false, fmt.Errorf("empty target")
	}
	return raw, false, nil
}

func isIncompatibleGRPCEndpoint(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "failed reading server preface") ||
		strings.Contains(s, "frame too large") ||
		strings.Contains(s, "http/1.1")
}

// isPermanentRejection reports whether the peer's reason for closing the
// stream means retrying is pointless. Today: protocol version mismatch
// (FailedPrecondition) and credential-level rejections from the remote
// (Unauthenticated, PermissionDenied) — none of these recover without a
// config or code change on one side.
func isPermanentRejection(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.FailedPrecondition,
		codes.Unauthenticated,
		codes.PermissionDenied:
		return true
	default:
		return false
	}
}

// logHandshakeError emits a categorized log line for a stream-open failure
// so operators can tell auth issues, proxy misconfig, and transport problems
// apart at a glance.
func (h *Hub) logHandshakeError(peerName string, err error, retryIn time.Duration) {
	if isIncompatibleGRPCEndpoint(err) {
		h.logger.Printf("federation: peer %s endpoint is not h2c gRPC (reverse-proxy misconfigured?): %v (retry in %s)",
			peerName, err, retryIn)
		return
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unauthenticated:
			h.logger.Printf("federation: peer %s rejected our credentials — %s (retry in %s; check FEDERATION_KEY matches)",
				peerName, st.Message(), retryIn)
		case codes.PermissionDenied:
			h.logger.Printf("federation: peer %s denied federation subpath — %s (retry in %s)",
				peerName, st.Message(), retryIn)
		case codes.InvalidArgument:
			h.logger.Printf("federation: peer %s rejected request — %s (retry in %s)",
				peerName, st.Message(), retryIn)
		case codes.FailedPrecondition:
			h.logger.Printf("federation: peer %s precondition failed — %s (retry in %s)",
				peerName, st.Message(), retryIn)
		case codes.Unavailable:
			h.logger.Printf("federation: peer %s unreachable — %s (retry in %s)",
				peerName, st.Message(), retryIn)
		default:
			h.logger.Printf("federation: peer %s handshake failed code=%s msg=%q (retry in %s)",
				peerName, st.Code(), st.Message(), retryIn)
		}
		return
	}
	h.logger.Printf("federation: peer %s handshake error: %v (retry in %s)", peerName, err, retryIn)
}

func federationSubpathForKey(key string) string {
	normalized := strings.TrimSpace(key)
	sum := sha256.Sum256([]byte(normalized))
	return "/federation/" + hex.EncodeToString(sum[:12])
}

func (h *Hub) handleHello(peer *Peer, ctrl *federationv2pb.Control, hello *federationv2pb.Hello) {
	origin := ctrl.GetOrigin()
	peerVer := ctrl.GetProtocolVersion()
	h.logger.Printf("federation: hello from %s (version %d)", origin, peerVer)

	if peerVer < MinSupportedProtocolVersion || peerVer > ProtocolVersion {
		err := status.Errorf(
			codes.FailedPrecondition,
			"incompatible protocol version %d (we support %d-%d)",
			peerVer, MinSupportedProtocolVersion, ProtocolVersion,
		)
		h.logger.Printf("federation: peer %s rejected — %v", origin, err)
		peer.SetCloseErr(err)
		peer.Close()
		return
	}

	if origin != "" && origin != peer.Name {
		old := peer.Name
		h.renamePeer(peer, origin)
		h.logger.Printf("federation: renamed peer %s -> %s", old, origin)
	}

	h.sendPeerExchange(peer)
	h.sendUsersDBOffer(peer)
}

func (h *Hub) handleSyncResponse(peer *Peer, sr *federationv2pb.SyncResponse) {
	for issiStr, info := range sr.GetSubscribers() {
		var issi uint32
		fmt.Sscanf(issiStr, "%d", &issi)
		// sendFullSync only ships the sender's local subscribers, so for
		// sync-path ISSIs the peer is itself the origin.
		peer.RegisterISSI(issi, peer.Name)
		peer.AffiliateISSI(issi, info.GetGssis())
	}
	h.logger.Printf("federation: synced %d subscribers from %s", len(sr.GetSubscribers()), peer.Name)
}

func (h *Hub) handleUsersDBOffer(peer *Peer, off *federationv2pb.UsersDbOffer) {
	if h.handler == nil || off.GetUrl() == "" {
		return
	}
	ourTS, _ := h.handler.GetUsersDBInfo()
	if ourTS == "" || off.GetTimestamp() > ourTS {
		h.logger.Printf("federation: downloading users DB from %s (%d users, ts=%s)",
			peer.Name, off.GetCount(), off.GetTimestamp())
		if err := h.handler.DownloadUsersDBFrom(off.GetUrl()); err != nil {
			h.logger.Printf("federation: users DB download failed: %v", err)
		} else {
			h.logger.Printf("federation: users DB updated from %s", peer.Name)
		}
	}
}

func (h *Hub) handlePeerExchange(peer *Peer, px *federationv2pb.PeerExchange) {
	newPeers := 0
	for _, gp := range px.GetPeers() {
		if gp.GetName() == h.serverName || gp.GetUrl() == "" {
			continue
		}
		if h.tryAddDiscoveredPeer(gp.GetName(), gp.GetUrl()) {
			newPeers++
		}
	}
	if newPeers > 0 {
		h.logger.Printf("federation: discovered %d new peer(s) via %s", newPeers, peer.Name)
	}
}

func (h *Hub) handleSubscriberUpdate(peer *Peer, ctrl *federationv2pb.Control, up *federationv2pb.SubscriberUpdate) {
	switch up.GetAction() {
	case federationv2pb.SubscriberUpdate_ACTION_REGISTER:
		peer.RegisterISSI(up.GetIssi(), ctrl.GetOrigin())
		peer.AffiliateISSI(up.GetIssi(), up.GetGssis())
		h.logger.Printf("federation: %s registered ISSI %d (GSSIs=%v origin=%s) [ttl=%d path=%v]",
			peer.Name, up.GetIssi(), up.GetGssis(), ctrl.GetOrigin(), ctrl.GetTtl(), ctrl.GetPath())
	case federationv2pb.SubscriberUpdate_ACTION_DEREGISTER:
		peer.DeregisterISSI(up.GetIssi())
		h.logger.Printf("federation: %s deregistered ISSI %d [ttl=%d]", peer.Name, up.GetIssi(), ctrl.GetTtl())
	}
	if h.mesh.ShouldRelay(ctrl) {
		h.relayToPeers(h.mesh.PrepareRelay(ctrl), peer.Name)
	}
}

func (h *Hub) handleAffiliateUpdate(peer *Peer, ctrl *federationv2pb.Control, up *federationv2pb.AffiliateUpdate) {
	switch up.GetAction() {
	case federationv2pb.AffiliateUpdate_ACTION_AFFILIATE:
		peer.AffiliateISSI(up.GetIssi(), up.GetGssis())
		h.logger.Printf("federation: %s affiliated ISSI %d -> GSSIs %v [ttl=%d]",
			peer.Name, up.GetIssi(), up.GetGssis(), ctrl.GetTtl())
	case federationv2pb.AffiliateUpdate_ACTION_DEAFFILIATE:
		peer.DeaffiliateISSI(up.GetIssi(), up.GetGssis())
		h.logger.Printf("federation: %s deaffiliated ISSI %d from GSSIs %v [ttl=%d]",
			peer.Name, up.GetIssi(), up.GetGssis(), ctrl.GetTtl())
	}
	if h.mesh.ShouldRelay(ctrl) {
		h.relayToPeers(h.mesh.PrepareRelay(ctrl), peer.Name)
	}
}

func (h *Hub) handleCallStart(peer *Peer, ctrl *federationv2pb.Control, cs *federationv2pb.CallStart) {
	// Private (subscriber-to-subscriber) calls are point-to-point: the
	// destination peer plays voice out locally and does NOT relay further.
	if cs.GetDestIssi() != 0 {
		if h.handler != nil {
			h.handler.OnPeerPrivateCallStart(peer.Name, cs.GetUuid(), cs.GetSourceIssi(),
				cs.GetDestIssi(), uint8(cs.GetPriority()), uint16(cs.GetService()))
		}
		h.callMu.Lock()
		h.privateCalls[cs.GetUuid()] = peer.Name
		h.callMu.Unlock()
		return
	}

	if h.handler != nil {
		h.handler.OnPeerCallStart(peer.Name, cs.GetUuid(), cs.GetSourceIssi(),
			cs.GetDestGssi(), uint8(cs.GetPriority()), uint16(cs.GetService()))
	}
	h.callMu.Lock()
	if h.activeCalls[cs.GetUuid()] == nil {
		h.activeCalls[cs.GetUuid()] = make(map[string]bool)
	}
	h.callMu.Unlock()

	if !h.mesh.ShouldRelay(ctrl) {
		return
	}
	relay := h.mesh.PrepareRelay(ctrl)
	h.mu.RLock()
	defer h.mu.RUnlock()
	h.callMu.Lock()
	defer h.callMu.Unlock()
	seen := map[string]bool{peer.Name: true}
	for _, p := range h.peers {
		if seen[p.Name] || IsInPath(ctrl, p.Name) {
			continue
		}
		seen[p.Name] = true
		_ = p.SendControl(relay)
		h.activeCalls[cs.GetUuid()][p.Name] = true
	}
}

func (h *Hub) handleCallEnd(peer *Peer, ctrl *federationv2pb.Control, ce *federationv2pb.CallEnd) {
	if h.handler != nil {
		h.handler.OnPeerCallEnd(peer.Name, ce.GetUuid(), uint8(ce.GetCause()))
	}
	h.callMu.Lock()
	_, wasPrivate := h.privateCalls[ce.GetUuid()]
	delete(h.privateCalls, ce.GetUuid())
	delete(h.activeCalls, ce.GetUuid())
	h.callMu.Unlock()

	// Private calls are point-to-point: the destination peer never relays.
	if wasPrivate {
		return
	}

	if !h.mesh.ShouldRelay(ctrl) {
		return
	}
	relay := h.mesh.PrepareRelay(ctrl)
	h.mu.RLock()
	defer h.mu.RUnlock()
	seen := map[string]bool{peer.Name: true}
	for _, p := range h.peers {
		if seen[p.Name] || IsInPath(ctrl, p.Name) {
			continue
		}
		seen[p.Name] = true
		_ = p.SendControl(relay)
	}
}

// handleCallReply receives SetupAccept / SetupReject / ConnectRequest routed
// across the federation from the answerer's side of a private call. The
// handler (federation_bridge) re-injects the appropriate brew
// CallControlMessage to local clients tracking the call. Point-to-point —
// no further relay.
func (h *Hub) handleCallReply(peer *Peer, _ *federationv2pb.Control, cr *federationv2pb.CallReply) {
	if h.handler != nil {
		h.handler.OnPeerCallReply(peer.Name, cr.GetUuid(), uint8(cr.GetState()), uint8(cr.GetCause()))
	}
}

func (h *Hub) handleSdsRelay(peer *Peer, ctrl *federationv2pb.Control, sr *federationv2pb.SdsRelay) {
	if h.handler != nil {
		h.handler.OnPeerSDSRelay(peer.Name, sr.GetSourceIssi(), sr.GetDestIssi(), hex.EncodeToString(sr.GetSdsData()))
	}
	if h.mesh.ShouldRelay(ctrl) {
		h.relayToPeers(h.mesh.PrepareRelay(ctrl), peer.Name)
	}
}

func (h *Hub) handlePositionSample(peer *Peer, ctrl *federationv2pb.Control, ps *federationv2pb.PositionSample) {
	if h.handler != nil {
		h.handler.OnPeerPositionSample(peer.Name, ps.GetIssi(), ps.GetLat(), ps.GetLon(), ps.GetTmoSite())
	}
	if h.mesh.ShouldRelay(ctrl) {
		h.relayToPeers(h.mesh.PrepareRelay(ctrl), peer.Name)
	}
}

func (h *Hub) handleStationUpdate(peer *Peer, ctrl *federationv2pb.Control, su *federationv2pb.StationUpdate) {
	if h.handler != nil && su.GetStation() != nil {
		h.handler.OnPeerStationUpdate(ctrl.GetOrigin(), peer.Name, su.GetStation().AsMap())
	}
	if h.mesh.ShouldRelay(ctrl) {
		h.relayToPeers(h.mesh.PrepareRelay(ctrl), peer.Name)
	}
}

// handleVoiceFrame delivers an incoming voice frame to the local Brew
// service. It does NOT fan the frame out to other federation peers.
//
// Voice frames carry no msg_id, TTL, or path, so there is no receive-side
// dedup; in a full mesh (every node dials every other node directly) every
// peer already receives each frame once via the originator's
// BroadcastVoiceFrame. A receive-side relay creates an infinite loop —
// frames bounce between nodes forever, multiplying every hop. Under load
// this saturated the 256-slot send buffers (millions of drops per minute
// in the peer-stats log) and crowded out the control plane.
//
// Trade-off: nodes that can't directly reach each other (e.g. behind
// asymmetric NAT) will not receive voice via a relaying middleman. Adding
// msg_id + path to VoiceFrame in the proto would restore multi-hop with
// safe dedup; for now, full mesh + no relay is correct.
func (h *Hub) handleVoiceFrame(peer *Peer, callUUID string, frameData []byte) {
	if len(callUUID) != 36 {
		return
	}
	if h.handler != nil {
		h.handler.OnPeerVoiceFrame(peer.Name, callUUID, frameData)
	}
}

// ==================================================================
// Broadcasting to peers (called by the Brew service)
// ==================================================================

// BroadcastSubscriber notifies all peers about a subscriber change.
func (h *Hub) BroadcastSubscriber(issi uint32, action string, gssis []uint32) {
	subAction := federationv2pb.SubscriberUpdate_ACTION_UNSPECIFIED
	switch action {
	case "register":
		subAction = federationv2pb.SubscriberUpdate_ACTION_REGISTER
	case "deregister":
		subAction = federationv2pb.SubscriberUpdate_ACTION_DEREGISTER
	}
	ctrl := &federationv2pb.Control{
		Origin: h.serverName,
		Payload: &federationv2pb.Control_SubscriberUpdate{
			SubscriberUpdate: &federationv2pb.SubscriberUpdate{
				Issi:   issi,
				Action: subAction,
				Gssis:  append([]uint32(nil), gssis...),
			},
		},
	}
	h.mesh.PrepareOutgoing(ctrl)
	h.broadcastToAllPeers(ctrl)
}

// BroadcastAffiliate notifies all peers about an affiliation change.
// Only federated GSSIs (23-90) are shared.
func (h *Hub) BroadcastAffiliate(issi uint32, action string, gssis []uint32) {
	fedGSSIs := make([]uint32, 0, len(gssis))
	for _, g := range gssis {
		if isFederatedGSSI(g) {
			fedGSSIs = append(fedGSSIs, g)
		}
	}
	if len(fedGSSIs) == 0 {
		return
	}
	affAction := federationv2pb.AffiliateUpdate_ACTION_UNSPECIFIED
	switch action {
	case "affiliate":
		affAction = federationv2pb.AffiliateUpdate_ACTION_AFFILIATE
	case "deaffiliate":
		affAction = federationv2pb.AffiliateUpdate_ACTION_DEAFFILIATE
	}
	ctrl := &federationv2pb.Control{
		Origin: h.serverName,
		Payload: &federationv2pb.Control_AffiliateUpdate{
			AffiliateUpdate: &federationv2pb.AffiliateUpdate{
				Issi:   issi,
				Action: affAction,
				Gssis:  append([]uint32(nil), fedGSSIs...),
			},
		},
	}
	h.mesh.PrepareOutgoing(ctrl)
	h.broadcastToAllPeers(ctrl)
}

// BroadcastCallStart notifies peers that have subscribers on the target GSSI.
// Only federated GSSIs (23-90) are shared between servers.
func (h *Hub) BroadcastCallStart(callUUID string, sourceISSI, destGSSI uint32, priority uint8, service uint16) {
	if !isFederatedGSSI(destGSSI) {
		return
	}
	ctrl := &federationv2pb.Control{
		Origin: h.serverName,
		Payload: &federationv2pb.Control_CallStart{
			CallStart: &federationv2pb.CallStart{
				Uuid:       callUUID,
				SourceIssi: sourceISSI,
				DestGssi:   destGSSI,
				Priority:   uint32(priority),
				Service:    uint32(service),
			},
		},
	}
	h.mesh.PrepareOutgoing(ctrl)

	h.mu.RLock()
	defer h.mu.RUnlock()
	h.callMu.Lock()
	defer h.callMu.Unlock()
	if h.activeCalls[callUUID] == nil {
		h.activeCalls[callUUID] = make(map[string]bool)
	}
	seen := make(map[string]bool, len(h.peers))
	for _, peer := range h.peers {
		if seen[peer.Name] {
			continue
		}
		seen[peer.Name] = true
		_ = peer.SendControl(ctrl)
		h.activeCalls[callUUID][peer.Name] = true
	}
}

// BroadcastStation sendet einen BlueStation-Heartbeat an alle Peers
// (Stations-Federation). Damit zeigen alle Server die gleiche Station-Liste.
//
// `origin` is the originating server name to stamp on ctrl.Origin. Pass "" for
// a locally-owned station; the hub falls back to its own serverName. For a
// relayed/anti-entropy broadcast, pass the station's known origin so the
// receiver does not re-attribute it to us.
func (h *Hub) BroadcastStation(origin string, station map[string]any) {
	st, err := structpb.NewStruct(station)
	if err != nil {
		h.logger.Printf("federation: cannot encode station map: %v", err)
		return
	}
	if origin == "" {
		origin = h.serverName
	}
	ctrl := &federationv2pb.Control{
		Origin: origin,
		Payload: &federationv2pb.Control_StationUpdate{
			StationUpdate: &federationv2pb.StationUpdate{Station: st},
		},
	}
	h.mesh.PrepareOutgoing(ctrl)
	h.broadcastToAllPeers(ctrl)
}

// BroadcastPositionSample sendet einen empfangenen Position-Sample an alle Peers
// (Coverage-Federation). Mesh-Router dedupliziert + relayed.
func (h *Hub) BroadcastPositionSample(issi uint32, lat, lon float64, tmoSite string) {
	ctrl := &federationv2pb.Control{
		Origin: h.serverName,
		Payload: &federationv2pb.Control_PositionSample{
			PositionSample: &federationv2pb.PositionSample{
				Issi:     issi,
				Lat:      lat,
				Lon:      lon,
				TmoSite:  tmoSite,
			},
		},
	}
	h.mesh.PrepareOutgoing(ctrl)
	h.broadcastToAllPeers(ctrl)
}

// BroadcastCallEnd notifies all peers about a call ending.
func (h *Hub) BroadcastCallEnd(callUUID string, cause uint8) {
	h.callMu.Lock()
	delete(h.activeCalls, callUUID)
	h.callMu.Unlock()

	ctrl := &federationv2pb.Control{
		Origin: h.serverName,
		Payload: &federationv2pb.Control_CallEnd{
			CallEnd: &federationv2pb.CallEnd{
				Uuid:  callUUID,
				Cause: uint32(cause),
			},
		},
	}
	h.mesh.PrepareOutgoing(ctrl)
	h.broadcastToAllPeers(ctrl)
}

// BroadcastVoiceFrame sends a voice frame to each connected peer name once.
// h.peers stores outgoing and incoming connections to the same peer as
// separate entries (keys "name" and "name:in"); voice frames carry no
// msg_id so the receiver cannot dedup. Dedup by peer.Name at send time.
func (h *Hub) BroadcastVoiceFrame(callUUID string, frameData []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	seen := make(map[string]bool, len(h.peers))
	for _, peer := range h.peers {
		if seen[peer.Name] {
			continue
		}
		seen[peer.Name] = true
		_ = peer.SendVoiceFrame(callUUID, frameData)
	}
}

// BroadcastSDS relays an SDS message through the mesh.
func (h *Hub) BroadcastSDS(sourceISSI, destISSI uint32, sdsDataHex string) {
	raw, err := hex.DecodeString(sdsDataHex)
	if err != nil {
		h.logger.Printf("federation: invalid local SDS hex: %v", err)
		return
	}
	ctrl := &federationv2pb.Control{
		Origin: h.serverName,
		Payload: &federationv2pb.Control_SdsRelay{
			SdsRelay: &federationv2pb.SdsRelay{
				SourceIssi: sourceISSI,
				DestIssi:   destISSI,
				SdsData:    raw,
			},
		},
	}
	h.mesh.PrepareOutgoing(ctrl)
	h.broadcastToAllPeers(ctrl)
}

// FindPeerForISSI returns the peer that has the given ISSI, or nil.
func (h *Hub) FindPeerForISSI(issi uint32) *Peer {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, peer := range h.peers {
		if peer.HasISSI(issi) {
			return peer
		}
	}
	return nil
}

// HasPeersForGSSI returns true if any peer has subscribers on the GSSI.
func (h *Hub) HasPeersForGSSI(gssi uint32) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, peer := range h.peers {
		if peer.HasSubscribersOnGSSI(gssi) {
			return true
		}
	}
	return false
}

// PeerCount returns the number of connected peers.
func (h *Hub) PeerCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.peers)
}

// PeerSnapshots returns info about all connected peers.
func (h *Hub) PeerSnapshots() []PeerSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]PeerSnapshot, 0, len(h.peers))
	for _, p := range h.peers {
		origins := p.Origins()
		originStr := make(map[string]string, len(origins))
		for issi, o := range origins {
			originStr[fmt.Sprintf("%d", issi)] = o
		}
		out = append(out, PeerSnapshot{
			Name:      p.Name,
			Direction: p.Direction,
			ISSIs:     p.ISSIs(),
			Origins:   originStr,
			GSSIs:     p.GSSIs(),
			Buffer:    p.BufferStats(),
		})
	}
	return out
}

// PeerSnapshot is a read-only snapshot of a peer's state. Origins maps an
// ISSI (decimal string for direct JSON consumption) to the server where
// the subscriber is physically attached — this is the multi-hop origin
// from Control.origin, not necessarily Name. Lets the admin dashboard
// distinguish "this peer relayed it" from "this peer owns it".
type PeerSnapshot struct {
	Name      string              `json:"name"`
	Direction string              `json:"direction"`
	ISSIs     []uint32            `json:"issis"`
	Origins   map[string]string   `json:"origins"`
	GSSIs     map[uint32][]uint32 `json:"gssis"`
	Buffer    PeerBufferStats     `json:"buffer"`
}

// ==================================================================
// Internal helpers
// ==================================================================

// sendUsersDBOffer tells the peer when our users.txt was last updated.
// The peer can decide to download ours if it's newer than theirs.
func (h *Hub) sendUsersDBOffer(peer *Peer) {
	if h.handler == nil || h.selfURL == "" {
		return
	}
	ts, count := h.handler.GetUsersDBInfo()
	if ts == "" || count == 0 {
		return
	}
	// Convert wss://host/peer/ → https://host/api/users.txt
	baseURL := h.selfURL
	baseURL = strings.Replace(baseURL, "wss://", "https://", 1)
	baseURL = strings.Replace(baseURL, "ws://", "http://", 1)
	baseURL = strings.TrimSuffix(baseURL, "/peer/")
	baseURL = strings.TrimSuffix(baseURL, "/peer")
	dbURL := baseURL + "/api/users.txt"

	_ = peer.SendControl(&federationv2pb.Control{
		Origin: h.serverName,
		Payload: &federationv2pb.Control_UsersDbOffer{
			UsersDbOffer: &federationv2pb.UsersDbOffer{
				Timestamp: ts,
				Url:       dbURL,
				Count:     uint32(count),
			},
		},
	})
}

// sendPeerExchange sends our list of known peers to a peer.
func (h *Hub) sendPeerExchange(peer *Peer) {
	h.knownMu.RLock()
	gp := make([]*federationv2pb.GossipPeer, 0, len(h.knownPeers)+1)
	if h.selfURL != "" {
		gp = append(gp, &federationv2pb.GossipPeer{Name: h.serverName, Url: h.selfURL})
	}
	for name, u := range h.knownPeers {
		if name != peer.Name {
			gp = append(gp, &federationv2pb.GossipPeer{Name: name, Url: u})
		}
	}
	h.knownMu.RUnlock()

	if len(gp) == 0 {
		return
	}
	_ = peer.SendControl(&federationv2pb.Control{
		Origin: h.serverName,
		Payload: &federationv2pb.Control_PeerExchange{
			PeerExchange: &federationv2pb.PeerExchange{Peers: gp},
		},
	})
	h.logger.Printf("federation: sent %d known peer(s) to %s", len(gp), peer.Name)
}

// tryAddDiscoveredPeer adds a newly discovered peer and connects to it.
// Returns true if the peer was new. Skips are silent for the common
// self-reference / already-known cases (gossip echoes are normal traffic),
// logged for the surprising cases (legacy URL, conflicting URL for a known
// name) and audit-logged once when a new peer is actually dialed.
func (h *Hub) tryAddDiscoveredPeer(name, url string) bool {
	// Common silent skip: gossip echoed us back.
	if name == h.serverName || url == h.selfURL {
		return false
	}
	// Legacy websocket gossip URLs (e.g. .../peer/) are not valid for the
	// v2 gRPC federation transport. Surface this once.
	if strings.Contains(strings.ToLower(url), "/peer") {
		h.logger.Printf("federation: discovery: skip %s at %s — legacy WS URL not supported by v2 transport", name, url)
		return false
	}

	h.knownMu.Lock()
	// Dedup by name OR by URL — a bootstrap peer (e.g. "peer-0") may already
	// point at the same URL under a different label.
	if existingURL, exists := h.knownPeers[name]; exists {
		h.knownMu.Unlock()
		// Surprising case: same name, different URL. Don't silently
		// switch — operator should know.
		if existingURL != url {
			h.logger.Printf("federation: discovery: peer %s already known at %s; ignoring gossip URL %s",
				name, existingURL, url)
		}
		return false
	}
	for existingName, existingURL := range h.knownPeers {
		if existingURL == url {
			h.knownMu.Unlock()
			h.logger.Printf("federation: discovery: skip %s — URL %s already known as %s",
				name, url, existingName)
			return false
		}
	}
	h.knownPeers[name] = url
	h.knownMu.Unlock()

	// Check if already connected (by name).
	h.mu.RLock()
	alreadyConnected := false
	for _, p := range h.peers {
		if p.Name == name {
			alreadyConnected = true
			break
		}
	}
	h.mu.RUnlock()

	if alreadyConnected {
		return false
	}

	h.logger.Printf("federation: discovery: new peer %s at %s — dialing", name, url)
	go h.maintainOutgoingPeer(h.ctx, PeerConfig{
		Name: name,
		URL:  url,
		Key:  h.peerKey,
	})
	return true
}

func (h *Hub) registerPeer(peer *Peer) {
	key := peerKey(peer.Name, peer.Direction)
	h.redeemPending(peer, key)

	h.mu.Lock()
	defer h.mu.Unlock()
	if old, ok := h.peers[key]; ok {
		old.Close()
	}
	h.peers[key] = peer
}

// redeemPending transfers ISSI/GSSI tables from a pending (disconnected)
// peer at the given key into the freshly-connected peer, and cancels the
// pending-cleanup timer. No-op if there's nothing pending. Called from
// registerPeer for the bootstrap-name case and from renamePeer for the
// Hello-discovered-name case.
func (h *Hub) redeemPending(newPeer *Peer, key string) {
	h.pendingMu.Lock()
	pending, ok := h.pendingPeers[key]
	if !ok {
		h.pendingMu.Unlock()
		return
	}
	pending.timer.Stop()
	delete(h.pendingPeers, key)
	h.pendingMu.Unlock()

	adoptPeerState(newPeer, pending.peer)
	h.logger.Printf("federation: redeemed peer %s — kept %d ISSIs across reconnect",
		newPeer.Name, len(newPeer.ISSIs()))
}

// adoptPeerState copies subscriber tables from src into dst. The dst peer
// is assumed to be freshly created (its tables empty); we replace rather
// than merge.
func adoptPeerState(dst, src *Peer) {
	src.mu.RLock()
	issis := make(map[uint32]string, len(src.issis))
	for k, v := range src.issis {
		issis[k] = v
	}
	gssis := make(map[uint32]map[uint32]bool, len(src.gssiAffiliations))
	for g, members := range src.gssiAffiliations {
		m := make(map[uint32]bool, len(members))
		for k, v := range members {
			m[k] = v
		}
		gssis[g] = m
	}
	src.mu.RUnlock()

	dst.mu.Lock()
	dst.issis = issis
	dst.gssiAffiliations = gssis
	dst.mu.Unlock()

	// Empty src so a late-firing cleanup timer doesn't double-log the
	// ISSI count we just transferred.
	src.mu.Lock()
	src.issis = make(map[uint32]string)
	src.gssiAffiliations = make(map[uint32]map[uint32]bool)
	src.mu.Unlock()
}

// renamePeer updates a peer's Name and moves it to the correct map slot.
// Used when a bootstrap-configured name differs from the Hello-reported Origin.
func (h *Hub) renamePeer(peer *Peer, newName string) {
	h.mu.Lock()
	oldName := peer.Name
	oldKey := peerKey(oldName, peer.Direction)
	newKey := peerKey(newName, peer.Direction)
	if oldKey == newKey {
		h.mu.Unlock()
		return
	}
	if existing, ok := h.peers[newKey]; ok && existing != peer {
		existing.Close()
	}
	delete(h.peers, oldKey)
	peer.Name = newName
	h.peers[newKey] = peer
	h.mu.Unlock()

	// Bootstrap dials register under e.g. "peer-0"; the real name only
	// arrives via Hello. Any redemption state was filed under the real
	// name, so the second chance to adopt it is here.
	h.redeemPending(peer, newKey)

	// Also relabel in the known-peers map so future gossip dedup works.
	h.knownMu.Lock()
	if url, ok := h.knownPeers[oldName]; ok {
		delete(h.knownPeers, oldName)
		if _, exists := h.knownPeers[newName]; !exists {
			h.knownPeers[newName] = url
		}
	}
	h.knownMu.Unlock()
}

func peerKey(name, direction string) string {
	if direction == "incoming" {
		return name + ":in"
	}
	return name
}

func (h *Hub) unregisterPeer(peer *Peer) {
	peer.Close()

	h.mu.Lock()
	var key string
	for k, p := range h.peers {
		if p == peer {
			key = k
			delete(h.peers, k)
			break
		}
	}
	h.mu.Unlock()

	if key == "" {
		// Peer was already removed — either redemption already adopted
		// its state into a replacement, or this is the (now-removed)
		// double cleanup call. Nothing else to do.
		return
	}

	// Hold the peer's subscriber tables for a grace window so a quick
	// reconnect can pick them back up without a sync round-trip.
	h.schedulePendingCleanup(key, peer)
}

func (h *Hub) schedulePendingCleanup(key string, peer *Peer) {
	h.pendingMu.Lock()
	defer h.pendingMu.Unlock()
	// If an older pending entry still sits at this key (shouldn't normally
	// happen — registerPeer redeems on reconnect), retire it now.
	if old, ok := h.pendingPeers[key]; ok && old.peer != peer {
		old.timer.Stop()
		old.peer.Cleanup()
		delete(h.pendingPeers, key)
	}
	if _, ok := h.pendingPeers[key]; ok {
		return
	}
	timer := time.AfterFunc(peerRedemptionWindow, func() {
		h.pendingMu.Lock()
		cur, ok := h.pendingPeers[key]
		if ok && cur.peer == peer {
			delete(h.pendingPeers, key)
		}
		h.pendingMu.Unlock()
		peer.Cleanup()
	})
	h.pendingPeers[key] = &pendingPeer{peer: peer, timer: timer}
}

func (h *Hub) sendFullSync(peer *Peer) {
	if h.handler == nil {
		return
	}
	localSubs := h.handler.GetLocalSubscribers()
	subs := make(map[string]*federationv2pb.SyncSubscriber, len(localSubs))
	for issi, gssis := range localSubs {
		subs[fmt.Sprintf("%d", issi)] = &federationv2pb.SyncSubscriber{
			Gssis: append([]uint32(nil), gssis...),
		}
	}
	_ = peer.SendControl(&federationv2pb.Control{
		Origin: h.serverName,
		Payload: &federationv2pb.Control_SyncResponse{
			SyncResponse: &federationv2pb.SyncResponse{Subscribers: subs},
		},
	})
	h.logger.Printf("federation: sent sync to %s (%d subscribers)", peer.Name, len(subs))
}

// broadcastToAllPeers sends ctrl once per distinct peer name. h.peers may
// hold both an outgoing and an incoming Peer for the same remote (keyed
// "name" and "name:in"); without dedup every broadcast would be delivered
// twice, doubling control-plane traffic and the per-peer 256-slot send
// buffer pressure.
func (h *Hub) broadcastToAllPeers(ctrl *federationv2pb.Control) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	seen := make(map[string]bool, len(h.peers))
	for _, peer := range h.peers {
		if seen[peer.Name] {
			continue
		}
		seen[peer.Name] = true
		_ = peer.SendControl(ctrl)
	}
}

// relayToPeers forwards ctrl to every peer except excludeName (the source)
// and any peer already in ctrl.Path. The path check avoids enqueueing
// messages the remote would just drop via mesh-dedup — important because
// every queued-then-dropped message still costs a 256-slot buffer slot.
func (h *Hub) relayToPeers(ctrl *federationv2pb.Control, excludeName string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	seen := map[string]bool{excludeName: true}
	for _, peer := range h.peers {
		if seen[peer.Name] || IsInPath(ctrl, peer.Name) {
			continue
		}
		seen[peer.Name] = true
		_ = peer.SendControl(ctrl)
	}
}

// =============================================================================
// Private (subscriber-to-subscriber) call routing.
//
// Group calls broadcast to every peer; private calls go to exactly one peer
// (the one that owns the destination ISSI). The privateCalls map records
// the call->peer binding made at CallStart so the matching voice frames and
// CallEnd take the same point-to-point path.

// RouteCallStartToPeerForISSI sends a private-call CallStart to the single
// peer that owns destISSI and records the routing in privateCalls so the
// matching voice frames and CallEnd take the same one-to-one path. Returns
// the peer name and true if the ISSI was found; ok=false leaves the caller
// to handle "destination not federated" (no broadcast).
func (h *Hub) RouteCallStartToPeerForISSI(callUUID string, sourceISSI, destISSI uint32, priority uint8, service uint16) (string, bool) {
	peer := h.FindPeerForISSI(destISSI)
	if peer == nil {
		return "", false
	}
	ctrl := &federationv2pb.Control{
		Origin: h.serverName,
		Payload: &federationv2pb.Control_CallStart{
			CallStart: &federationv2pb.CallStart{
				Uuid:       callUUID,
				SourceIssi: sourceISSI,
				DestIssi:   destISSI,
				Priority:   uint32(priority),
				Service:    uint32(service),
			},
		},
	}
	h.mesh.PrepareOutgoing(ctrl)
	_ = peer.SendControl(ctrl)
	h.callMu.Lock()
	h.privateCalls[callUUID] = peer.Name
	h.callMu.Unlock()
	return peer.Name, true
}

// RouteVoiceFrameForCall forwards a voice frame to the single peer recorded
// for a private call, or falls through to the all-peers broadcast used by
// group calls when there is no per-call peer.
func (h *Hub) RouteVoiceFrameForCall(callUUID string, frameData []byte) {
	h.callMu.RLock()
	peerName, ok := h.privateCalls[callUUID]
	h.callMu.RUnlock()
	if !ok {
		h.BroadcastVoiceFrame(callUUID, frameData)
		return
	}
	h.mu.RLock()
	peer := h.peers[peerName]
	if peer == nil {
		peer = h.peers[peerName+":in"]
	}
	h.mu.RUnlock()
	if peer == nil {
		return
	}
	_ = peer.SendVoiceFrame(callUUID, frameData)
}

// RouteCallEndForCall sends a CallEnd to the single peer recorded for a
// private call (then forgets it), or falls through to the all-peers
// broadcast used by group calls.
func (h *Hub) RouteCallEndForCall(callUUID string, cause uint8) {
	h.callMu.Lock()
	peerName, ok := h.privateCalls[callUUID]
	delete(h.privateCalls, callUUID)
	h.callMu.Unlock()
	if !ok {
		h.BroadcastCallEnd(callUUID, cause)
		return
	}
	h.mu.RLock()
	peer := h.peers[peerName]
	if peer == nil {
		peer = h.peers[peerName+":in"]
	}
	h.mu.RUnlock()
	if peer == nil {
		return
	}
	ctrl := &federationv2pb.Control{
		Origin: h.serverName,
		Payload: &federationv2pb.Control_CallEnd{
			CallEnd: &federationv2pb.CallEnd{
				Uuid:  callUUID,
				Cause: uint32(cause),
			},
		},
	}
	h.mesh.PrepareOutgoing(ctrl)
	_ = peer.SendControl(ctrl)
}

// RouteCallReplyForCall forwards a post-CallStart, pre-CallEnd signaling
// reply (SetupAccept / SetupReject / ConnectRequest, as brew CallState
// codes 5/6/8) to the single peer that owns the private call. Looks up
// the peer via privateCalls; returns false if the call isn't tracked as
// private (the caller falls back to whatever non-federation routing it
// would use for a group call).
func (h *Hub) RouteCallReplyForCall(callUUID string, state, cause uint8) bool {
	h.callMu.RLock()
	peerName, ok := h.privateCalls[callUUID]
	h.callMu.RUnlock()
	if !ok {
		return false
	}
	h.mu.RLock()
	peer := h.peers[peerName]
	if peer == nil {
		peer = h.peers[peerName+":in"]
	}
	h.mu.RUnlock()
	if peer == nil {
		return false
	}
	ctrl := &federationv2pb.Control{
		Origin: h.serverName,
		Payload: &federationv2pb.Control_CallReply{
			CallReply: &federationv2pb.CallReply{
				Uuid:  callUUID,
				State: uint32(state),
				Cause: uint32(cause),
			},
		},
	}
	h.mesh.PrepareOutgoing(ctrl)
	return peer.SendControl(ctrl) == nil
}
