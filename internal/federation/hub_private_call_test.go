package federation

import (
	"context"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	federationv2pb "github.com/freetetra/server/internal/federation/proto/v2"
)

// fakeStream is a minimal rpcStream that records every Send() and lets the
// tests inspect what the hub sent to a particular peer. Recv blocks until
// the test closes the stream — none of these tests drive incoming traffic.
type fakeStream struct {
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	sent     []*federationv2pb.StreamFrame
	closedCh chan struct{}
}

func newFakeStream() *fakeStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &fakeStream{
		ctx:      ctx,
		cancel:   cancel,
		closedCh: make(chan struct{}),
	}
}

func (f *fakeStream) Send(frame *federationv2pb.StreamFrame) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, frame)
	return nil
}

func (f *fakeStream) Recv() (*federationv2pb.StreamFrame, error) {
	<-f.closedCh
	return nil, io.EOF
}

func (f *fakeStream) Context() context.Context { return f.ctx }

func (f *fakeStream) frames() []*federationv2pb.StreamFrame {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*federationv2pb.StreamFrame, len(f.sent))
	copy(out, f.sent)
	return out
}

// noopHandler satisfies CallHandler for tests that only exercise hub routing.
type noopHandler struct{}

func (noopHandler) OnPeerCallStart(string, string, uint32, uint32, uint8, uint16) {}
func (noopHandler) OnPeerPrivateCallStart(string, string, uint32, uint32, uint8, uint16) {
}
func (noopHandler) OnPeerCallEnd(string, string, uint8)                          {}
func (noopHandler) OnPeerCallReply(string, string, uint8, uint8)                 {}
func (noopHandler) OnPeerVoiceFrame(string, string, []byte)                      {}
func (noopHandler) OnPeerSDSRelay(string, uint32, uint32, string)                {}
func (noopHandler) OnPeerPositionSample(string, uint32, float64, float64, string) {}
func (noopHandler) OnPeerStationUpdate(string, string, map[string]any)           {}
func (noopHandler) GetLocalSubscribers() map[uint32][]uint32                     { return nil }
func (noopHandler) GetUsersDBInfo() (string, int)                                { return "", 0 }
func (noopHandler) DownloadUsersDBFrom(string) error                             { return nil }

// newTestHub builds a hub with a discarded logger and the no-op handler.
// Disables the standalone RPC listener so tests don't bind a port.
func newTestHub(t *testing.T, name string) *Hub {
	t.Helper()
	h := NewHub(name, "test-key", "", "", noopHandler{}, log.New(io.Discard, "", 0))
	h.serveStandaloneRPC = false
	return h
}

// attachPeer wires a fake peer with the given name into the hub and (when
// issis is non-empty) registers those ISSIs in the peer's remote table.
// Returns the fake stream so the test can inspect what the hub sent.
func attachPeer(t *testing.T, h *Hub, name string, issis ...uint32) *fakeStream {
	t.Helper()
	stream := newFakeStream()
	peer := newPeer(name, "outgoing", stream, stream.cancel, log.New(io.Discard, "", 0))
	for _, i := range issis {
		peer.RegisterISSI(i, name)
	}
	go peer.writeLoop()
	h.registerPeer(peer)
	return stream
}

func TestRouteCallStartToPeerForISSI_PeerFound(t *testing.T) {
	h := newTestHub(t, "selfsite")
	stream := attachPeer(t, h, "peerB", 2002)

	peerName, ok := h.RouteCallStartToPeerForISSI("uuid-1", 1001, 2002, 3, 7)
	if !ok {
		t.Fatalf("expected ok=true when peer owns ISSI")
	}
	if peerName != "peerB" {
		t.Fatalf("expected routed via peerB, got %q", peerName)
	}

	h.callMu.RLock()
	got := h.privateCalls["uuid-1"]
	h.callMu.RUnlock()
	if got != "peerB" {
		t.Fatalf("privateCalls[uuid-1] = %q, want peerB", got)
	}

	// writeLoop is async; give it one scheduler turn to flush the queue.
	waitForSend(t, stream, 1)
	frames := stream.frames()
	if len(frames) == 0 {
		t.Fatalf("expected at least one frame sent to peerB")
	}
	ctrl := frames[0].GetControl()
	if ctrl == nil {
		t.Fatalf("first frame to peerB is not a Control")
	}
	cs := ctrl.GetCallStart()
	if cs == nil {
		t.Fatalf("first frame to peerB is not a CallStart")
	}
	if cs.GetUuid() != "uuid-1" || cs.GetSourceIssi() != 1001 || cs.GetDestIssi() != 2002 {
		t.Fatalf("CallStart fields wrong: %+v", cs)
	}
	if cs.GetDestGssi() != 0 {
		t.Fatalf("private CallStart must not carry dest_gssi, got %d", cs.GetDestGssi())
	}
}

func TestRouteCallStartToPeerForISSI_PeerNotFound(t *testing.T) {
	h := newTestHub(t, "selfsite")
	// Peer exists but doesn't own destISSI 9999.
	stream := attachPeer(t, h, "peerB", 2002)

	peerName, ok := h.RouteCallStartToPeerForISSI("uuid-2", 1001, 9999, 0, 0)
	if ok {
		t.Fatalf("expected ok=false when no peer owns ISSI")
	}
	if peerName != "" {
		t.Fatalf("expected empty peerName, got %q", peerName)
	}

	h.callMu.RLock()
	_, recorded := h.privateCalls["uuid-2"]
	h.callMu.RUnlock()
	if recorded {
		t.Fatalf("privateCalls must not record a route when peer not found")
	}

	if len(stream.frames()) != 0 {
		t.Fatalf("no frames should be sent when no peer owns the ISSI; got %d", len(stream.frames()))
	}
}

func TestRouteCallReplyForCall_PeerFound(t *testing.T) {
	h := newTestHub(t, "selfsite")
	stream := attachPeer(t, h, "peerB", 2002)

	// First record a private call so RouteCallReplyForCall has a peer to find.
	if _, ok := h.RouteCallStartToPeerForISSI("uuid-3", 1001, 2002, 0, 0); !ok {
		t.Fatalf("setup: RouteCallStartToPeerForISSI failed")
	}
	waitForSend(t, stream, 1) // drain the CallStart frame

	// SetupAccept reply (state 5) — must reach peerB as a CallReply.
	if ok := h.RouteCallReplyForCall("uuid-3", 5, 0); !ok {
		t.Fatalf("RouteCallReplyForCall returned false; expected true (peer recorded)")
	}
	waitForSend(t, stream, 2)
	frames := stream.frames()
	if len(frames) < 2 {
		t.Fatalf("expected 2 frames (CallStart + CallReply), got %d", len(frames))
	}
	ctrl := frames[1].GetControl()
	if ctrl == nil {
		t.Fatalf("second frame is not a Control")
	}
	cr := ctrl.GetCallReply()
	if cr == nil {
		t.Fatalf("second frame is not a CallReply (got payload %T)", ctrl.GetPayload())
	}
	if cr.GetUuid() != "uuid-3" {
		t.Fatalf("CallReply uuid = %q, want uuid-3", cr.GetUuid())
	}
	if cr.GetState() != 5 {
		t.Fatalf("CallReply state = %d, want 5 (SetupAccept)", cr.GetState())
	}
}

func TestRouteCallReplyForCall_UnknownCall(t *testing.T) {
	h := newTestHub(t, "selfsite")
	_ = attachPeer(t, h, "peerB", 2002)

	if ok := h.RouteCallReplyForCall("uuid-not-tracked", 5, 0); ok {
		t.Fatalf("RouteCallReplyForCall returned true for an unknown call")
	}
}

// waitForSend polls the fake stream until it has at least n frames, or the
// test times out. The writeLoop pumps async so a direct len() check races.
func waitForSend(t *testing.T, s *fakeStream, n int) {
	t.Helper()
	const tries = 100
	for i := 0; i < tries; i++ {
		if len(s.frames()) >= n {
			return
		}
		// Yield so writeLoop can run.
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d frames; got %d", n, len(s.frames()))
}
