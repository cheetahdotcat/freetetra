package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/freetetra/server/internal/brew"
	"github.com/freetetra/server/internal/config"
)

// LinkBridge mirrors traffic between configured TG pairs, bidirectionally
// and in real time. When a real subscriber talks on TG A of a pair, the
// bridge opens a mirror call on TG B (stamped with LinkSourceISSI) and
// forwards each voice frame as it arrives. On call release, the mirror is
// held open briefly to catch late-arriving frames before being torn down.
//
// The bridge relies on a self-source filter to avoid feedback loops: our
// own mirror TX comes back on the paired TG as a GroupTX whose Source is
// our LinkSourceISSI — we drop those so the mirror doesn't mirror itself.
type LinkBridge struct {
	cfg    config.Config
	logger *log.Logger
	plane  InjectionPlane

	// mirrorDest maps every configured TG to its pair partner.
	mirrorDest map[uint32]uint32
	hangoff    time.Duration

	mu      sync.Mutex
	tracked map[uuid.UUID]*linkTrack

	cancel context.CancelFunc
	done   chan struct{}

	mirrored atomic.Uint64
}

type linkTrack struct {
	mirrorCall uuid.UUID
	destTG     uint32
	released   bool
	releaseAt  time.Time // when the origin released; used to gate late-frame drain
	releaseT   *time.Timer
}

func NewLinkBridge(cfg config.Config, logger *log.Logger, plane InjectionPlane) (*LinkBridge, error) {
	if len(cfg.Link.Pairs) == 0 {
		return nil, fmt.Errorf("LINK_PAIRS must define at least one pair (e.g. LINK_PAIRS=10:10000)")
	}
	mirror := make(map[uint32]uint32, 2*len(cfg.Link.Pairs))
	for _, p := range cfg.Link.Pairs {
		if p.A == 0 || p.B == 0 || p.A == p.B {
			continue
		}
		if existing, ok := mirror[p.A]; ok && existing != p.B {
			return nil, fmt.Errorf("LINK_PAIRS conflict: TG %d already linked to %d, cannot re-link to %d", p.A, existing, p.B)
		}
		if existing, ok := mirror[p.B]; ok && existing != p.A {
			return nil, fmt.Errorf("LINK_PAIRS conflict: TG %d already linked to %d, cannot re-link to %d", p.B, existing, p.A)
		}
		mirror[p.A] = p.B
		mirror[p.B] = p.A
	}
	if len(mirror) == 0 {
		return nil, fmt.Errorf("LINK_PAIRS contained no usable pairs")
	}
	hangoff := cfg.Link.MirrorHangoff
	if hangoff < 0 {
		hangoff = 0
	}
	return &LinkBridge{
		cfg:        cfg,
		logger:     logger,
		plane:      plane,
		mirrorDest: mirror,
		hangoff:    hangoff,
		tracked:    make(map[uuid.UUID]*linkTrack),
	}, nil
}

// LinkAttachTGs returns the full flat list of TGs across all pairs — the
// brew-module plane needs this at construction time to affiliate before
// any bridge instance exists.
func LinkAttachTGs(pairs []config.LinkPair) []uint32 {
	seen := make(map[uint32]struct{}, 2*len(pairs))
	out := make([]uint32, 0, 2*len(pairs))
	for _, p := range pairs {
		for _, tg := range [2]uint32{p.A, p.B} {
			if tg == 0 {
				continue
			}
			if _, ok := seen[tg]; ok {
				continue
			}
			seen[tg] = struct{}{}
			out = append(out, tg)
		}
	}
	return out
}

func (l *LinkBridge) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	l.mu.Lock()
	if l.cancel != nil {
		l.mu.Unlock()
		cancel()
		return fmt.Errorf("link bridge already started")
	}
	l.cancel = cancel
	l.done = make(chan struct{})
	done := l.done
	l.mu.Unlock()

	go func() {
		<-runCtx.Done()
		l.tearDownAll()
		close(done)
	}()

	pairs := make([]string, 0)
	for _, p := range l.cfg.Link.Pairs {
		pairs = append(pairs, fmt.Sprintf("%d<->%d", p.A, p.B))
	}
	l.logger.Printf(
		"link bridge enabled pairs=%v source=%d hangoff=%s",
		pairs,
		l.mirrorSource(0),
		l.hangoff.String(),
	)
	return nil
}

func (l *LinkBridge) Stop() {
	l.mu.Lock()
	cancel := l.cancel
	done := l.done
	l.cancel = nil
	l.done = nil
	l.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (l *LinkBridge) OnBrewCallControl(m *brew.CallControlMessage) {
	if m == nil {
		return
	}
	switch m.CallState {
	case brew.CallStateGroupTX:
		p, ok := m.Payload.(brew.GroupTransmissionPayload)
		if !ok {
			return
		}
		destTG, linked := l.mirrorDest[p.Destination]
		if !linked {
			return
		}
		// Self-loop guard: our own mirror TX on the paired TG comes back to
		// us as a GroupTX (the brew server broadcasts to all subscribers,
		// including this bridge). Skip anything with our source ISSI or
		// we'd bounce it back and forth forever.
		if mySrc := l.mirrorSource(0); mySrc != 0 && p.Source == mySrc {
			return
		}
		l.startMirror(m.Identifier, p, destTG)
	case brew.CallStateGroupIdle, brew.CallStateCallRelease:
		l.scheduleRelease(m.Identifier, m.CallState)
	}
}

func (l *LinkBridge) OnBrewFrame(callID uuid.UUID, frameType uint8, data []byte) {
	if frameType != brew.FrameTypeTrafficChannel {
		return
	}
	l.mu.Lock()
	track := l.tracked[callID]
	if track == nil {
		l.mu.Unlock()
		return
	}
	mirror := track.mirrorCall
	l.mu.Unlock()

	frameCopy := append([]byte(nil), data...)
	l.plane.InjectedVoiceFrame("link", mirror, frameCopy)
	total := l.mirrored.Add(1)
	if total == 1 || total%50 == 0 {
		l.logger.Printf("link mirror frames_total=%d origin_call=%s mirror_call=%s", total, callID.String(), mirror.String())
	}
}

func (l *LinkBridge) startMirror(originCall uuid.UUID, p brew.GroupTransmissionPayload, destTG uint32) {
	l.mu.Lock()
	if _, ok := l.tracked[originCall]; ok {
		l.mu.Unlock()
		return
	}
	mirrorCall := uuid.New()
	l.tracked[originCall] = &linkTrack{
		mirrorCall: mirrorCall,
		destTG:     destTG,
	}
	l.mu.Unlock()

	source := l.mirrorSource(p.Source)
	if !l.plane.StartInjectedGroupTX("link", mirrorCall, source, destTG, p.Priority, p.Access, p.Service) {
		l.mu.Lock()
		delete(l.tracked, originCall)
		l.mu.Unlock()
		l.logger.Printf(
			"link mirror drop origin_call=%s reason=start-failed src_tg=%d dst_tg=%d source=%d",
			originCall.String(),
			p.Destination,
			destTG,
			source,
		)
		return
	}
	l.logger.Printf(
		"link mirror start origin_call=%s mirror_call=%s src_tg=%d dst_tg=%d origin_source=%d mirror_source=%d priority=%d access=%d service=%d",
		originCall.String(),
		mirrorCall.String(),
		p.Destination,
		destTG,
		p.Source,
		source,
		p.Priority,
		p.Access,
		p.Service,
	)
}

// scheduleRelease closes the mirror after `hangoff` so a late voice frame
// arriving after the origin's CallRelease still gets forwarded.
func (l *LinkBridge) scheduleRelease(originCall uuid.UUID, state uint8) {
	l.mu.Lock()
	track := l.tracked[originCall]
	if track == nil {
		l.mu.Unlock()
		return
	}
	if track.released {
		l.mu.Unlock()
		return
	}
	track.released = true
	if l.hangoff <= 0 {
		delete(l.tracked, originCall)
		mirror := track.mirrorCall
		l.mu.Unlock()
		l.releaseMirror(originCall, mirror, state)
		return
	}
	mirror := track.mirrorCall
	track.releaseT = time.AfterFunc(l.hangoff, func() {
		l.mu.Lock()
		if l.tracked[originCall] == track {
			delete(l.tracked, originCall)
		}
		l.mu.Unlock()
		l.releaseMirror(originCall, mirror, state)
	})
	l.mu.Unlock()
}

func (l *LinkBridge) releaseMirror(originCall, mirrorCall uuid.UUID, state uint8) {
	cause := l.cfg.Link.ReleaseCause
	l.plane.IdleInjectedCall("link", mirrorCall, cause)
	l.plane.ReleaseInjectedCall("link", mirrorCall, cause)
	l.logger.Printf(
		"link mirror end origin_call=%s mirror_call=%s origin_state=%d",
		originCall.String(),
		mirrorCall.String(),
		state,
	)
}

func (l *LinkBridge) tearDownAll() {
	l.mu.Lock()
	tracks := l.tracked
	l.tracked = make(map[uuid.UUID]*linkTrack)
	l.mu.Unlock()

	cause := l.cfg.Link.ReleaseCause
	for _, t := range tracks {
		if t.releaseT != nil {
			t.releaseT.Stop()
		}
		l.plane.IdleInjectedCall("link", t.mirrorCall, cause)
		l.plane.ReleaseInjectedCall("link", t.mirrorCall, cause)
	}
}

func (l *LinkBridge) mirrorSource(fallback uint32) uint32 {
	if l.cfg.Link.SourceISSI != 0 {
		return l.cfg.Link.SourceISSI
	}
	if l.cfg.Link.BrewISSI != 0 {
		return l.cfg.Link.BrewISSI
	}
	return fallback
}
