package service

import (
	"bytes"
	"context"
	"log"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/freetetra/server/internal/brew"
	"github.com/freetetra/server/internal/config"
)

type linkStartEvent struct {
	callID     uuid.UUID
	sourceISSI uint32
	destGSSI   uint32
	priority   uint8
	access     uint8
	service    uint16
}

type linkReleaseEvent struct {
	callID uuid.UUID
	cause  uint8
}

type linkPlaneStub struct {
	mu          sync.Mutex
	voiceByCall map[uuid.UUID][][]byte
	starts      []linkStartEvent
	idles       []linkReleaseEvent
	releases    []linkReleaseEvent
}

func newLinkPlaneStub() *linkPlaneStub {
	return &linkPlaneStub{voiceByCall: make(map[uuid.UUID][][]byte)}
}

func (s *linkPlaneStub) StartInjectedCall(_ string, callID uuid.UUID, source uint32, dest uint32) bool {
	return s.StartInjectedGroupTX("", callID, source, dest, 0, 0, 0)
}

func (s *linkPlaneStub) StartInjectedGroupTX(
	_ string,
	callID uuid.UUID,
	source uint32,
	dest uint32,
	priority uint8,
	access uint8,
	service uint16,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.starts = append(s.starts, linkStartEvent{
		callID:     callID,
		sourceISSI: source,
		destGSSI:   dest,
		priority:   priority,
		access:     access,
		service:    service,
	})
	return true
}

func (s *linkPlaneStub) IdleInjectedCall(_ string, callID uuid.UUID, cause uint8) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idles = append(s.idles, linkReleaseEvent{callID: callID, cause: cause})
}

func (s *linkPlaneStub) ReleaseInjectedCall(_ string, callID uuid.UUID, cause uint8) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releases = append(s.releases, linkReleaseEvent{callID: callID, cause: cause})
}

func (s *linkPlaneStub) InjectedVoiceFrame(_ string, callID uuid.UUID, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.voiceByCall[callID] = append(s.voiceByCall[callID], append([]byte(nil), data...))
}

func (s *linkPlaneStub) InjectedPacketFrame(_ string, _ uuid.UUID, _ []byte) {}
func (s *linkPlaneStub) GroupSubscriberCount(_ uint32) int                   { return 0 }

func (s *linkPlaneStub) snapshot() (starts []linkStartEvent, idles []linkReleaseEvent, releases []linkReleaseEvent, voice map[uuid.UUID][][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	starts = append(starts, s.starts...)
	idles = append(idles, s.idles...)
	releases = append(releases, s.releases...)
	voice = make(map[uuid.UUID][][]byte, len(s.voiceByCall))
	for callID, frames := range s.voiceByCall {
		copied := make([][]byte, 0, len(frames))
		for _, f := range frames {
			copied = append(copied, append([]byte(nil), f...))
		}
		voice[callID] = copied
	}
	return starts, idles, releases, voice
}

func newLinkTestBridge(t *testing.T) (*LinkBridge, *linkPlaneStub, context.CancelFunc) {
	t.Helper()
	cfg := config.Config{Link: config.LinkConfig{
		Pairs: []config.LinkPair{
			{A: 10, B: 10000},
			{A: 11, B: 10001},
			{A: 12, B: 10002},
		},
		BrewISSI:      899004,
		SourceISSI:    899004,
		ReleaseCause:  7,
		MirrorHangoff: 5 * time.Millisecond,
	}}
	plane := newLinkPlaneStub()
	logger := log.New(bytes.NewBuffer(nil), "", 0)
	bridge, err := NewLinkBridge(cfg, logger, plane)
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := bridge.Start(ctx); err != nil {
		cancel()
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(bridge.Stop)
	return bridge, plane, cancel
}

func TestLinkAttachTGsCoversBothSidesOfEachPair(t *testing.T) {
	got := LinkAttachTGs([]config.LinkPair{
		{A: 10, B: 10000},
		{A: 11, B: 10001},
		{A: 12, B: 10002},
	})
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	want := []uint32{10, 11, 12, 10000, 10001, 10002}
	if len(got) != len(want) {
		t.Fatalf("attach list len=%d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("attach list[%d]=%d, want %d (got %v)", i, got[i], want[i], got)
		}
	}
}

func TestLinkBridgeMirrorsBothDirections(t *testing.T) {
	// TG 10 → TG 10000, then TG 10000 → TG 10, using the same bridge.
	bridge, plane, cancel := newLinkTestBridge(t)
	defer cancel()

	// Direction 1: real user talks on TG 10, mirror should land on TG 10000.
	callA := uuid.New()
	bridge.OnBrewCallControl(&brew.CallControlMessage{
		CallState:  brew.CallStateGroupTX,
		Identifier: callA,
		Payload:    brew.GroupTransmissionPayload{Source: 501, Destination: 10, Priority: 2, Access: 3, Service: 5},
	})
	fA1 := []byte{0x80, 0xAA, 0x01}
	fA2 := []byte{0x80, 0xAA, 0x02}
	bridge.OnBrewFrame(callA, brew.FrameTypeTrafficChannel, fA1)
	bridge.OnBrewFrame(callA, brew.FrameTypeTrafficChannel, fA2)

	// Direction 2: real user talks on TG 10000, mirror should land on TG 10.
	callB := uuid.New()
	bridge.OnBrewCallControl(&brew.CallControlMessage{
		CallState:  brew.CallStateGroupTX,
		Identifier: callB,
		Payload:    brew.GroupTransmissionPayload{Source: 601, Destination: 10000, Priority: 1, Access: 0, Service: 0},
	})
	fB1 := []byte{0x80, 0xBB, 0x01}
	bridge.OnBrewFrame(callB, brew.FrameTypeTrafficChannel, fB1)

	// Wait for both start events.
	waitFor(t, 200*time.Millisecond, func() bool {
		s, _, _, _ := plane.snapshot()
		return len(s) == 2
	})

	starts, _, _, voice := plane.snapshot()
	byDest := map[uint32]linkStartEvent{}
	for _, s := range starts {
		byDest[s.destGSSI] = s
	}

	mirrorA, ok := byDest[10000]
	if !ok {
		t.Fatalf("no mirror on TG 10000: %+v", starts)
	}
	if mirrorA.sourceISSI != 899004 {
		t.Fatalf("mirror-to-10000 source=%d, want 899004", mirrorA.sourceISSI)
	}
	if mirrorA.priority != 2 || mirrorA.access != 3 || mirrorA.service != 5 {
		t.Fatalf("mirror-to-10000 dropped call params: %+v", mirrorA)
	}
	if got := voice[mirrorA.callID]; len(got) != 2 || !bytes.Equal(got[0], fA1) || !bytes.Equal(got[1], fA2) {
		t.Fatalf("mirror-to-10000 voice frames = %v, want [%v %v]", got, fA1, fA2)
	}

	mirrorB, ok := byDest[10]
	if !ok {
		t.Fatalf("no mirror on TG 10: %+v", starts)
	}
	if mirrorB.sourceISSI != 899004 {
		t.Fatalf("mirror-to-10 source=%d, want 899004", mirrorB.sourceISSI)
	}
	if got := voice[mirrorB.callID]; len(got) != 1 || !bytes.Equal(got[0], fB1) {
		t.Fatalf("mirror-to-10 voice frames = %v, want [%v]", got, fB1)
	}
}

func TestLinkBridgeDropsSelfSourceFeedback(t *testing.T) {
	// The bridge's own mirror TX on TG 10000 comes back as a GroupTX with
	// Source=899004 (our SourceISSI). If we didn't filter that out, we'd
	// spawn a counter-mirror on TG 10 that echoes our own audio forever.
	bridge, plane, cancel := newLinkTestBridge(t)
	defer cancel()

	realCall := uuid.New()
	bridge.OnBrewCallControl(&brew.CallControlMessage{
		CallState:  brew.CallStateGroupTX,
		Identifier: realCall,
		Payload:    brew.GroupTransmissionPayload{Source: 501, Destination: 10, Priority: 0, Access: 0, Service: 0},
	})

	// Now simulate our own mirror landing back on TG 10000. Source == our own.
	selfEcho := uuid.New()
	bridge.OnBrewCallControl(&brew.CallControlMessage{
		CallState:  brew.CallStateGroupTX,
		Identifier: selfEcho,
		Payload:    brew.GroupTransmissionPayload{Source: 899004, Destination: 10000, Priority: 0, Access: 0, Service: 0},
	})
	bridge.OnBrewFrame(selfEcho, brew.FrameTypeTrafficChannel, []byte{0xDE, 0xAD})

	// Give any spurious counter-mirror time to appear (it shouldn't).
	waitFor(t, 50*time.Millisecond, func() bool { return true })

	starts, _, _, _ := plane.snapshot()
	if len(starts) != 1 {
		t.Fatalf("expected exactly one mirror (for the real call), got %d — self-source loop guard failed", len(starts))
	}
	if starts[0].destGSSI != 10000 {
		t.Fatalf("mirror landed on TG %d, want 10000", starts[0].destGSSI)
	}
}

func TestLinkBridgeIgnoresUnpairedTG(t *testing.T) {
	// A call on a TG that isn't part of any pair must not spawn a mirror.
	bridge, plane, cancel := newLinkTestBridge(t)
	defer cancel()

	stray := uuid.New()
	bridge.OnBrewCallControl(&brew.CallControlMessage{
		CallState:  brew.CallStateGroupTX,
		Identifier: stray,
		Payload:    brew.GroupTransmissionPayload{Source: 501, Destination: 99999},
	})
	bridge.OnBrewFrame(stray, brew.FrameTypeTrafficChannel, []byte{0x11})

	waitFor(t, 30*time.Millisecond, func() bool { return true })

	starts, _, _, _ := plane.snapshot()
	if len(starts) != 0 {
		t.Fatalf("unexpected mirror for unpaired TG: %+v", starts)
	}
}

func TestLinkBridgeReleasesMirrorAfterHangoff(t *testing.T) {
	bridge, plane, cancel := newLinkTestBridge(t)
	defer cancel()

	callID := uuid.New()
	bridge.OnBrewCallControl(&brew.CallControlMessage{
		CallState:  brew.CallStateGroupTX,
		Identifier: callID,
		Payload:    brew.GroupTransmissionPayload{Source: 501, Destination: 10},
	})
	bridge.OnBrewFrame(callID, brew.FrameTypeTrafficChannel, []byte{0xAA})
	bridge.OnBrewCallControl(&brew.CallControlMessage{
		CallState:  brew.CallStateCallRelease,
		Identifier: callID,
	})

	waitFor(t, 200*time.Millisecond, func() bool {
		_, idles, releases, _ := plane.snapshot()
		return len(idles) == 1 && len(releases) == 1
	})

	_, idles, releases, _ := plane.snapshot()
	if idles[0].cause != 7 || releases[0].cause != 7 {
		t.Fatalf("release cause not propagated: idle=%d release=%d", idles[0].cause, releases[0].cause)
	}
}

func TestLinkBridgeRejectsEmptyPairs(t *testing.T) {
	cfg := config.Config{}
	logger := log.New(bytes.NewBuffer(nil), "", 0)
	_, err := NewLinkBridge(cfg, logger, newLinkPlaneStub())
	if err == nil {
		t.Fatalf("expected error when LINK_PAIRS is empty")
	}
}

