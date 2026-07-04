package service

import (
	"bufio"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/google/uuid"
	"github.com/icholy/digest"

	"github.com/freetetra/server/internal/brew"
	"github.com/freetetra/server/internal/config"
)

type sipCallDirection uint8

const (
	sipDirectionOutbound sipCallDirection = iota + 1
	sipDirectionInbound

	sipCauseUserBusy uint8 = 17
)

type sipCallSession struct {
	callID      uuid.UUID
	direction   sipCallDirection
	sourceISSI  uint32
	targetISSI  uint32
	dialed      string
	callerLabel string
	sipCallID   string

	rtpConn      *net.UDPConn
	localRTPPort int
	remoteRTP    *net.UDPAddr
	payloadType  uint8
	seq          uint16
	timestamp    uint32
	ssrc         uint32

	pendingCodec18 []byte
	rtpRxSeen      bool
	rtpTxSeen      bool
	rtpPTWarned    bool
	rtpLastRx      time.Time

	sipDialogID string
	outDialog   *sipgo.DialogClientSession
	inDialog    *sipgo.DialogServerSession

	answered      bool
	alertSent     bool
	connectSent   bool
	connectSeen   bool
	terminating   bool
	closed        bool
	inviteCancel  context.CancelFunc
	rtpReaderDone chan struct{}
}

type SIPBridge struct {
	cfg    config.Config
	logger *log.Logger
	plane  *BrewModulePlane

	mu             sync.Mutex
	running        bool
	cancel         context.CancelFunc
	done           chan struct{}
	ua             *sipgo.UserAgent
	client         *sipgo.Client
	server         *sipgo.Server
	dialogClient   *sipgo.DialogClientCache
	dialogServer   *sipgo.DialogServerCache
	nextRTPPort    int
	sessionsByCall map[uuid.UUID]*sipCallSession
	sessionsByDlg  map[string]uuid.UUID
	sessionsBySIP  map[string]uuid.UUID
}

func NewSIPBridge(cfg config.Config, logger *log.Logger, plane *BrewModulePlane) (*SIPBridge, error) {
	if plane == nil {
		return nil, fmt.Errorf("sip bridge requires brew module plane")
	}
	if cfg.SIP.GatewayISSI == 0 {
		return nil, fmt.Errorf("SIP_GATEWAY_ISSI must be > 0")
	}
	if strings.TrimSpace(cfg.SIP.BindAddr) == "" {
		return nil, fmt.Errorf("SIP_BIND_ADDR is required")
	}
	if strings.TrimSpace(cfg.SIP.ServerAddr) == "" {
		return nil, fmt.Errorf("SIP_SERVER_ADDR is required")
	}
	if strings.TrimSpace(cfg.SIP.Domain) == "" {
		return nil, fmt.Errorf("SIP_DOMAIN is required")
	}
	if strings.TrimSpace(cfg.SIP.LocalUser) == "" {
		return nil, fmt.Errorf("SIP_LOCAL_USER is required")
	}
	if cfg.SIP.RTPPortStart <= 0 {
		return nil, fmt.Errorf("SIP_RTP_PORT_START must be > 0")
	}
	if strings.TrimSpace(cfg.SIP.RTPBindAddr) == "" {
		return nil, fmt.Errorf("SIP_RTP_BIND_ADDR is required")
	}
	if strings.TrimSpace(cfg.SIP.RTPAdvertiseIP) == "" {
		return nil, fmt.Errorf("SIP_RTP_ADVERTISE_IP is required")
	}
	if cfg.SIP.RegisterEnabled {
		if cfg.SIP.Username == "" || cfg.SIP.Password == "" {
			logger.Printf("sip register enabled but SIP_USERNAME/SIP_PASSWORD not fully set; registration will likely fail")
		}
	}
	if cfg.SIP.BrewISSI == 0 {
		cfg.SIP.BrewISSI = cfg.SIP.GatewayISSI
	}
	return &SIPBridge{
		cfg:            cfg,
		logger:         logger,
		plane:          plane,
		nextRTPPort:    cfg.SIP.RTPPortStart,
		sessionsByCall: make(map[uuid.UUID]*sipCallSession),
		sessionsByDlg:  make(map[string]uuid.UUID),
		sessionsBySIP:  make(map[string]uuid.UUID),
	}, nil
}

func (b *SIPBridge) Start(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.running {
		return fmt.Errorf("sip bridge already started")
	}

	runCtx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	b.done = make(chan struct{})

	signalHost, signalPort, err := splitHostPortLoose(b.cfg.SIP.BindAddr)
	if err != nil {
		cancel()
		return fmt.Errorf("parse SIP_BIND_ADDR: %w", err)
	}
	if signalHost == "" || signalHost == "0.0.0.0" || signalHost == "::" {
		signalHost = strings.TrimSpace(b.cfg.SIP.RTPAdvertiseIP)
	}
	contact := sip.ContactHeader{Address: sip.Uri{User: b.cfg.SIP.LocalUser, Host: signalHost, Port: signalPort}}
	if tran := strings.ToLower(strings.TrimSpace(b.cfg.SIP.Transport)); tran != "" && tran != "udp" {
		params := sip.NewParams()
		params.Add("transport", tran)
		contact.Address.UriParams = params
	}

	ua, err := sipgo.NewUA(sipgo.WithUserAgent(b.cfg.UserAgent))
	if err != nil {
		cancel()
		return fmt.Errorf("create sip ua: %w", err)
	}
	client, err := sipgo.NewClient(ua, sipgo.WithClientHostname(signalHost), sipgo.WithClientPort(signalPort))
	if err != nil {
		cancel()
		_ = ua.Close()
		return fmt.Errorf("create sip client: %w", err)
	}
	server, err := sipgo.NewServer(ua)
	if err != nil {
		cancel()
		_ = client.Close()
		_ = ua.Close()
		return fmt.Errorf("create sip server: %w", err)
	}

	b.ua = ua
	b.client = client
	b.server = server
	b.dialogClient = sipgo.NewDialogClientCache(client, contact)
	b.dialogServer = sipgo.NewDialogServerCache(client, contact)

	server.OnInvite(b.onSIPInvite)
	server.OnAck(b.onSIPAck)
	server.OnBye(b.onSIPBye)
	server.OnCancel(b.onSIPCancel)

	transport := strings.ToLower(strings.TrimSpace(b.cfg.SIP.Transport))
	if transport == "" {
		transport = "udp"
	}

	b.running = true
	b.logger.Printf(
		"sip bridge enabled gateway_issi=%d brew_issi=%d bind=%s transport=%s server=%s domain=%s rtp_bind=%s rtp_start=%d pt=%d",
		b.cfg.SIP.GatewayISSI,
		b.cfg.SIP.BrewISSI,
		b.cfg.SIP.BindAddr,
		transport,
		b.cfg.SIP.ServerAddr,
		b.cfg.SIP.Domain,
		b.cfg.SIP.RTPBindAddr,
		b.cfg.SIP.RTPPortStart,
		b.cfg.SIP.ACELPPayloadType,
	)

	go b.runBridge(runCtx, transport)
	return nil
}

func (b *SIPBridge) Stop() {
	b.mu.Lock()
	if !b.running {
		b.mu.Unlock()
		return
	}
	cancel := b.cancel
	done := b.done
	b.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (b *SIPBridge) runBridge(ctx context.Context, transport string) {
	defer func() {
		b.mu.Lock()
		for callID, sess := range b.sessionsByCall {
			b.closeSessionLocked(sess, "bridge-stop", 0, false)
			delete(b.sessionsByCall, callID)
			}
			b.sessionsByDlg = make(map[string]uuid.UUID)
			b.sessionsBySIP = make(map[string]uuid.UUID)
			if b.server != nil {
				_ = b.server.Close()
			}
		if b.client != nil {
			_ = b.client.Close()
		}
		if b.ua != nil {
			_ = b.ua.Close()
		}
		b.running = false
		close(b.done)
		b.mu.Unlock()
	}()

	if b.cfg.SIP.RegisterEnabled {
		go b.registrationLoop(ctx)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- b.server.ListenAndServe(ctx, transport, b.cfg.SIP.BindAddr)
	}()

	select {
	case <-ctx.Done():
		return
	case err := <-errCh:
		if err != nil && ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
			b.logger.Printf("sip bridge listen error: %v", err)
		}
	}
}

func (b *SIPBridge) OnBrewCallControl(m *brew.CallControlMessage) {
	if m == nil {
		return
	}

	b.mu.Lock()
	sess := b.sessionsByCall[m.Identifier]
	if sess == nil {
		if m.CallState == brew.CallStateSetupRequest {
			payload, ok := m.Payload.(brew.CircularCallPayload)
			if !ok || payload.Destination != b.cfg.SIP.GatewayISSI {
				b.mu.Unlock()
				return
			}
			session, err := b.newOutboundSessionLocked(m.Identifier, payload)
			if err != nil {
				b.mu.Unlock()
				b.logger.Printf("sip outbound setup rejected call=%s src=%d err=%v", m.Identifier.String(), payload.Source, err)
				_ = b.plane.SendCallControlWire(brew.BuildSetupReject(m.Identifier, b.cfg.SIP.ReleaseCause))
				return
			}
			b.sessionsByCall[m.Identifier] = session
			b.mu.Unlock()
			go b.startOutboundInvite(session)
			return
		}
		b.mu.Unlock()
		return
	}

	if sess.closed || sess.terminating {
		b.mu.Unlock()
		return
	}

	switch sess.direction {
	case sipDirectionOutbound:
		b.handleOutboundBrewControlLocked(sess, m)
	case sipDirectionInbound:
		b.handleInboundBrewControlLocked(sess, m)
	}
	b.mu.Unlock()
}

func (b *SIPBridge) OnBrewFrame(callID uuid.UUID, frameType uint8, data []byte) {
	if frameType != brew.FrameTypeTrafficChannel {
		return
	}

	b.mu.Lock()
	sess := b.sessionsByCall[callID]
	if sess == nil || sess.closed || sess.remoteRTP == nil || sess.rtpConn == nil {
		b.mu.Unlock()
		return
	}
	payloadType := sess.payloadType
	remote := *sess.remoteRTP
	conn := sess.rtpConn
	seq := sess.seq
	ts := sess.timestamp
	ssrc := sess.ssrc
	step := b.cfg.SIP.RTPTimestampStep
	b.mu.Unlock()

	ste, err := normalizeTrafficSTE(data)
	if err != nil {
		b.logger.Printf("sip drop traffic frame call=%s reason=%v", callID.String(), err)
		return
	}
	a, c := steToCodecFrames(ste)
	if len(a) != 18 || len(c) != 18 {
		return
	}
	// Match tetra-valence SIP worker framing for Asterisk interoperability:
	// 2x 18-byte ACELP codec frames in one RTP payload (ptime 60 ms).
	packet := make([]byte, 12+36)
	packet[0] = 0x80
	packet[1] = payloadType & 0x7f
	binary.BigEndian.PutUint16(packet[2:4], seq)
	binary.BigEndian.PutUint32(packet[4:8], ts)
	binary.BigEndian.PutUint32(packet[8:12], ssrc)
	copy(packet[12:30], a)
	copy(packet[30:48], c)

	if _, err := conn.WriteToUDP(packet, &remote); err != nil {
		b.logger.Printf("sip rtp send failed call=%s remote=%s: %v", callID.String(), remote.String(), err)
		return
	}
	seq++
	ts += step * 2

	firstOutbound := false
	b.mu.Lock()
	if current := b.sessionsByCall[callID]; current != nil && !current.closed {
		current.seq = seq
		current.timestamp = ts
		if !current.rtpTxSeen {
			current.rtpTxSeen = true
			firstOutbound = true
		}
	}
	b.mu.Unlock()
	if firstOutbound {
		b.logger.Printf("sip rtp outbound first packet call=%s remote=%s pt=%d", callID.String(), remote.String(), payloadType)
	}
}

func (b *SIPBridge) handleOutboundBrewControlLocked(sess *sipCallSession, m *brew.CallControlMessage) {
	switch m.CallState {
	case brew.CallStateConnectRequest:
		sess.connectSeen = true
		if !sess.connectSent {
			return
		}
		_ = b.plane.SendCallControlWire(brew.BuildConnectConfirm(m.Identifier, brew.CircularGrantPayload{Grant: 1, Permission: 1}))
	case brew.CallStateConnectConfirm:
		sess.connectSeen = true
	case brew.CallStateCallRelease, brew.CallStateSetupReject:
		cause := sipCallCause(m.Payload, b.cfg.SIP.ReleaseCause)
		b.closeSessionLocked(sess, "brew-release", cause, true)
	}
}

func (b *SIPBridge) handleInboundBrewControlLocked(sess *sipCallSession, m *brew.CallControlMessage) {
	switch m.CallState {
	case brew.CallStateSetupAccept, brew.CallStateCallAlert:
		if sess.alertSent || sess.inDialog == nil || sess.closed {
			return
		}
		sess.alertSent = true
		inDialog := sess.inDialog
		callID := sess.callID
		go func() {
			if err := inDialog.Respond(int(sip.StatusRinging), "Ringing", nil); err != nil {
				b.logger.Printf("sip inbound ring failed call=%s: %v", callID.String(), err)
			}
		}()
	case brew.CallStateConnectRequest, brew.CallStateConnectConfirm:
		if sess.answered || sess.inDialog == nil || sess.closed {
			return
		}
		sess.answered = true
		inDialog := sess.inDialog
		sdp := b.buildSDPOffer(sess.localRTPPort)
		callID := sess.callID
		go func() {
			if err := inDialog.RespondSDP(sdp); err != nil {
				b.logger.Printf("sip inbound answer failed call=%s: %v", callID.String(), err)
				_ = b.plane.SendCallControlWire(brew.BuildCallRelease(callID, b.cfg.SIP.ReleaseCause))
				b.mu.Lock()
				if current := b.sessionsByCall[callID]; current != nil {
					b.closeSessionLocked(current, "answer-failed", b.cfg.SIP.ReleaseCause, false)
				}
				b.mu.Unlock()
				return
			}
			b.logger.Printf("sip inbound connected call=%s dst=%d", callID.String(), sess.targetISSI)
			b.startInboundBootstrapTraffic(callID)
		}()
		if m.CallState == brew.CallStateConnectRequest {
			_ = b.plane.SendCallControlWire(brew.BuildConnectConfirm(m.Identifier, brew.CircularGrantPayload{Grant: 1, Permission: 1}))
		}
	case brew.CallStateCallRelease, brew.CallStateSetupReject:
		cause := sipCallCause(m.Payload, b.cfg.SIP.ReleaseCause)
		if !sess.answered && sess.inDialog != nil {
			sess.terminating = true
			inDialog := sess.inDialog
			callID := sess.callID
			status, reason := sipInboundEarlyReleaseStatus(m.CallState, cause)
			go func() {
				if err := inDialog.Respond(status, reason, nil); err != nil {
					b.logger.Printf("sip inbound reject failed call=%s code=%d: %v", callID.String(), status, err)
				}
				b.mu.Lock()
				if current := b.sessionsByCall[callID]; current != nil {
					b.closeSessionLocked(current, "brew-release", cause, false)
				}
				b.mu.Unlock()
			}()
			return
		}
		b.closeSessionLocked(sess, "brew-release", cause, true)
	}
}

func (b *SIPBridge) startInboundBootstrapTraffic(callID uuid.UUID) {
	go func() {
		ticker := time.NewTicker(60 * time.Millisecond)
		defer ticker.Stop()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()

		ste := pairCodec18ToSTE(make([]byte, 18), make([]byte, 18))
		lengthBits := uint16(len(ste) * 8)
		for {
			select {
			case <-ticker.C:
				b.mu.Lock()
				sess := b.sessionsByCall[callID]
				keepSending := sess != nil &&
					!sess.closed &&
					sess.direction == sipDirectionInbound
				recentRTP := keepSending &&
					!sess.rtpLastRx.IsZero() &&
					time.Since(sess.rtpLastRx) < 180*time.Millisecond
				b.mu.Unlock()
				if !keepSending {
					return
				}
				if recentRTP {
					continue
				}
				_ = b.plane.SendFrame(brew.FrameTypeTrafficChannel, callID, lengthBits, ste)
			case <-timer.C:
				return
			}
		}
	}()
}

func (b *SIPBridge) newOutboundSessionLocked(callID uuid.UUID, payload brew.CircularCallPayload) (*sipCallSession, error) {
	rtpConn, rtpPort, err := b.allocateRTPConnLocked()
	if err != nil {
		return nil, err
	}
	dialed := strings.TrimSpace(payload.Number)
	if dialed == "" {
		dialed = strconv.FormatUint(uint64(payload.Destination), 10)
	}
	caller := outboundSIPCallerIdentity(payload.Source, payload.Number)
	return &sipCallSession{
		callID:        callID,
		direction:     sipDirectionOutbound,
		sourceISSI:    payload.Source,
		targetISSI:    payload.Destination,
		dialed:        dialed,
		callerLabel:   caller,
		rtpConn:       rtpConn,
		localRTPPort:  rtpPort,
		payloadType:   b.cfg.SIP.ACELPPayloadType,
		ssrc:          randomSSRC(),
		rtpReaderDone: make(chan struct{}),
	}, nil
}

func (b *SIPBridge) startOutboundInvite(sess *sipCallSession) {
	if sess == nil {
		return
	}

	go b.readRTPLoop(sess)

	if !b.plane.SendCallControlWire(brew.BuildSetupAccept(sess.callID)) {
		b.logger.Printf("sip outbound setup-accept enqueue failed call=%s", sess.callID.String())
	}

	recipient := sip.Uri{User: sess.dialed, Host: b.cfg.SIP.Domain}
	req := sip.NewRequest(sip.INVITE, recipient)
	addOutboundCallerIdentityHeaders(req, sess.callerLabel, b.cfg.SIP.Domain)
	req.SetTransport(strings.ToUpper(strings.TrimSpace(b.cfg.SIP.Transport)))
	req.SetDestination(b.cfg.SIP.ServerAddr)
	req.SetBody(b.buildSDPOffer(sess.localRTPPort))
	req.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))

	inviteCtx, inviteCancel := context.WithCancel(context.Background())
	b.mu.Lock()
	if current := b.sessionsByCall[sess.callID]; current != nil {
		current.inviteCancel = inviteCancel
	}
	b.mu.Unlock()

	dialog, err := b.dialogClient.WriteInvite(inviteCtx, req)
	if err != nil {
		inviteCancel()
		b.logger.Printf("sip outbound invite failed call=%s dial=%q: %v", sess.callID.String(), sess.dialed, err)
		_ = b.plane.SendCallControlWire(brew.BuildSetupReject(sess.callID, b.cfg.SIP.ReleaseCause))
		b.mu.Lock()
		if current := b.sessionsByCall[sess.callID]; current != nil {
			b.closeSessionLocked(current, "invite-failed", b.cfg.SIP.ReleaseCause, false)
		}
		b.mu.Unlock()
		return
	}

	var (
		seenAnswerRemote *net.UDPAddr
		seenAnswerPT     uint8
		seenAnswerSDPOK  bool
	)
	err = dialog.WaitAnswer(inviteCtx, sipgo.AnswerOptions{
		Username: b.cfg.SIP.Username,
		Password: b.cfg.SIP.Password,
		OnResponse: func(res *sip.Response) error {
			if int(res.StatusCode) >= int(sip.StatusRinging) && int(res.StatusCode) < 200 {
				b.mu.Lock()
				if current := b.sessionsByCall[sess.callID]; current != nil && !current.alertSent {
					current.alertSent = true
					_ = b.plane.SendCallControlWire(brew.BuildCallAlert(sess.callID))
				}
				b.mu.Unlock()
			}
			// Cache SDP answer from any response; some PBXs place it in 18x, and
			// certain dialog flows may not expose final-body reliably on InviteResponse.
			if remote, pt, ok := parseRTPFromSDP(res.Body(), b.cfg.SIP.ACELPPayloadType); ok {
				seenAnswerRemote = remote
				seenAnswerPT = pt
				seenAnswerSDPOK = true
			}
			return nil
		},
	})
	if err != nil {
		inviteCancel()
		cause, sipStatus := sipOutboundRejectCause(err, b.cfg.SIP.ReleaseCause)
		if sipStatus > 0 {
			b.logger.Printf(
				"sip outbound answer rejected call=%s dial=%q status=%d mapped_cause=%d err=%v",
				sess.callID.String(),
				sess.dialed,
				sipStatus,
				cause,
				err,
			)
		} else {
			b.logger.Printf("sip outbound answer failed call=%s dial=%q: %v", sess.callID.String(), sess.dialed, err)
		}
		_ = b.plane.SendCallControlWire(brew.BuildSetupReject(sess.callID, cause))
		b.mu.Lock()
		if current := b.sessionsByCall[sess.callID]; current != nil {
			b.closeSessionLocked(current, "answer-failed", cause, false)
		}
		b.mu.Unlock()
		return
	}

	if err := dialog.Ack(context.Background()); err != nil {
		b.logger.Printf("sip outbound ack failed call=%s: %v", sess.callID.String(), err)
	}

	resp := dialog.InviteResponse
	remote, pt, ok := parseRTPFromSDP(resp.Body(), b.cfg.SIP.ACELPPayloadType)
	if !ok && seenAnswerSDPOK {
		remote = seenAnswerRemote
		pt = seenAnswerPT
		ok = true
	}
	if !ok {
		inviteCancel()
		b.logger.Printf("sip outbound missing RTP SDP answer call=%s", sess.callID.String())
		_ = b.plane.SendCallControlWire(brew.BuildCallRelease(sess.callID, b.cfg.SIP.ReleaseCause))
		b.mu.Lock()
		if current := b.sessionsByCall[sess.callID]; current != nil {
			b.closeSessionLocked(current, "missing-sdp", b.cfg.SIP.ReleaseCause, false)
		}
		b.mu.Unlock()
		return
	}

	b.mu.Lock()
	current := b.sessionsByCall[sess.callID]
	if current == nil || current.closed {
		b.mu.Unlock()
		_ = dialog.Bye(context.Background())
		dialog.Close()
		inviteCancel()
		return
	}
	current.outDialog = dialog
	current.sipDialogID = dialog.ID
	current.remoteRTP = remote
	current.payloadType = pt
	current.connectSent = true
	if dialog.ID != "" {
		b.sessionsByDlg[dialog.ID] = current.callID
	}
	if sipCallID := requestCallID(req); sipCallID != "" {
		current.sipCallID = sipCallID
		b.sessionsBySIP[sipCallID] = current.callID
	}
	b.mu.Unlock()

	connect := brew.CircularCallPayload{
		Source:      b.cfg.SIP.GatewayISSI,
		Destination: sess.sourceISSI,
		Number:      sess.dialed,
		Grant:       1,
		Permission:  1,
		Duplex:      1,
	}
	_ = b.plane.SendCallControlWire(brew.BuildConnectRequest(sess.callID, connect))
	b.logger.Printf("sip outbound connected call=%s src=%d dial=%q remote_rtp=%s pt=%d", sess.callID.String(), sess.sourceISSI, sess.dialed, remote.String(), pt)
}

func (b *SIPBridge) onSIPInvite(req *sip.Request, tx sip.ServerTransaction) {
	dialog, err := b.dialogServer.ReadInvite(req, tx)
	if err != nil {
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusBadRequest, "Bad Request", nil))
		return
	}

	targetISSI := parseTargetISSI(req.Recipient.User, b.cfg.SIP.InboundDefaultISSI, b.cfg.SIP.GatewayISSI)
	if targetISSI == 0 {
		_ = dialog.Respond(int(sip.StatusNotFound), "No Route", nil)
		_ = dialog.Close()
		return
	}

	rtpConn, rtpPort, err := b.allocateRTPConn()
	if err != nil {
		_ = dialog.Respond(int(sip.StatusServiceUnavailable), "RTP Error", nil)
		_ = dialog.Close()
		return
	}

	callID := uuid.New()
	remoteRTP, pt, _ := parseRTPFromSDP(req.Body(), b.cfg.SIP.ACELPPayloadType)
	callerLabel := parseSIPCallerLabel(req)
	sourceISSI := parseSIPSourceISSI(req, b.cfg.SIP.GatewayISSI, b.cfg.SIP.GatewayISSI)
	callerNumber := normalizeSIPCallerNumber(callerLabel, sourceISSI)
	sipCallID := requestCallID(req)
	sess := &sipCallSession{
		callID:        callID,
		direction:     sipDirectionInbound,
		sourceISSI:    sourceISSI,
		targetISSI:    targetISSI,
		dialed:        req.Recipient.User,
		callerLabel:   callerLabel,
		sipCallID:     sipCallID,
		rtpConn:       rtpConn,
		localRTPPort:  rtpPort,
		remoteRTP:     remoteRTP,
		payloadType:   pt,
		ssrc:          randomSSRC(),
		inDialog:      dialog,
		sipDialogID:   dialog.ID,
		rtpReaderDone: make(chan struct{}),
	}

	b.mu.Lock()
	b.sessionsByCall[callID] = sess
	if dialog.ID != "" {
		b.sessionsByDlg[dialog.ID] = callID
	}
	if sipCallID != "" {
		b.sessionsBySIP[sipCallID] = callID
	}
	b.mu.Unlock()

	go b.readRTPLoop(sess)

	setup := brew.CircularCallPayload{
		Source:      sourceISSI,
		Destination: targetISSI,
		Number:      callerNumber,
		Duplex:      1,
	}
	if !b.plane.SendCallControlWire(brew.BuildSetupRequest(callID, setup)) {
		b.logger.Printf("sip inbound setup enqueue failed call=%s dst=%d", callID.String(), targetISSI)
		_ = dialog.Respond(int(sip.StatusServiceUnavailable), "Brew unavailable", nil)
		b.mu.Lock()
		if current := b.sessionsByCall[callID]; current != nil {
			b.closeSessionLocked(current, "brew-enqueue-failed", b.cfg.SIP.ReleaseCause, false)
		}
		b.mu.Unlock()
		return
	}
	b.logger.Printf(
		"sip inbound invite call=%s caller=%q src=%d dst=%d remote_rtp=%s",
		callID.String(),
		callerNumber,
		sourceISSI,
		targetISSI,
		udpAddrString(remoteRTP),
	)
	<-tx.Done()

	// Fallback for early transaction termination: if INVITE ended before answer,
	// force-release the Brew leg so MS ringing stops promptly.
	releaseBrew := false
	b.mu.Lock()
	if current := b.sessionsByCall[callID]; current != nil && !current.closed && !current.answered {
		b.closeSessionLocked(current, "invite-tx-ended-before-answer", b.cfg.SIP.ReleaseCause, false)
		releaseBrew = true
	}
	b.mu.Unlock()
	if releaseBrew {
		b.logger.Printf("sip inbound invite transaction ended before answer call=%s", callID.String())
		_ = b.plane.SendCallControlWire(brew.BuildCallRelease(callID, b.cfg.SIP.ReleaseCause))
	}
}

func (b *SIPBridge) onSIPAck(req *sip.Request, tx sip.ServerTransaction) {
	if err := b.dialogServer.ReadAck(req, tx); err != nil {
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusBadRequest, err.Error(), nil))
	}
}

func (b *SIPBridge) onSIPBye(req *sip.Request, tx sip.ServerTransaction) {
	if err := b.dialogServer.ReadBye(req, tx); err == nil {
		b.handleRemoteSIPHangup(req)
		return
	}
	if err := b.dialogClient.ReadBye(req, tx); err == nil {
		b.handleRemoteSIPHangup(req)
		return
	}
	_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusCallTransactionDoesNotExists, "Call/Transaction Does Not Exist", nil))
}

func (b *SIPBridge) onSIPCancel(req *sip.Request, tx sip.ServerTransaction) {
	_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusOK, "OK", nil))
	b.logger.Printf("sip cancel received sip_call_id=%q", requestCallID(req))
	b.handleRemoteSIPHangup(req)
}

func (b *SIPBridge) handleRemoteSIPHangup(req *sip.Request) {
	method := "unknown"
	if req != nil {
		method = strings.ToLower(req.Method.String())
	}

	b.mu.Lock()
	callID, sess := b.findSessionByDialogRequestLocked(req)
	if sess == nil {
		b.mu.Unlock()
		b.logger.Printf("sip remote hangup unmatched method=%s sip_call_id=%q", method, requestCallID(req))
		return
	}
	b.closeSessionLocked(sess, "sip-remote-"+method, b.cfg.SIP.ReleaseCause, false)
	b.mu.Unlock()
	_ = b.plane.SendCallControlWire(brew.BuildCallRelease(callID, b.cfg.SIP.ReleaseCause))
}

func (b *SIPBridge) findSessionByDialogRequestLocked(req *sip.Request) (uuid.UUID, *sipCallSession) {
	var nilID uuid.UUID
	if req == nil {
		return nilID, nil
	}
	if dlgID, err := sip.DialogIDFromRequestUAS(req); err == nil {
		if callID, ok := b.sessionsByDlg[dlgID]; ok {
			return callID, b.sessionsByCall[callID]
		}
	}
	if dlgID, err := sip.DialogIDFromRequestUAC(req); err == nil {
		if callID, ok := b.sessionsByDlg[dlgID]; ok {
			return callID, b.sessionsByCall[callID]
		}
	}
	if sipCallID := requestCallID(req); sipCallID != "" {
		if callID, ok := b.sessionsBySIP[sipCallID]; ok {
			return callID, b.sessionsByCall[callID]
		}
	}
	return nilID, nil
}

func (b *SIPBridge) closeSessionLocked(sess *sipCallSession, reason string, cause uint8, sendSIPBye bool) {
	if sess == nil || sess.closed {
		return
	}
	sess.closed = true
	delete(b.sessionsByCall, sess.callID)
	if sess.sipDialogID != "" {
		delete(b.sessionsByDlg, sess.sipDialogID)
	}
	if sess.sipCallID != "" {
		delete(b.sessionsBySIP, sess.sipCallID)
	}
	if sess.inviteCancel != nil {
		sess.inviteCancel()
		sess.inviteCancel = nil
	}
	if sess.rtpConn != nil {
		_ = sess.rtpConn.Close()
		sess.rtpConn = nil
	}
	if sendSIPBye {
		if sess.outDialog != nil {
			go func(d *sipgo.DialogClientSession) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = d.Bye(ctx)
				d.Close()
			}(sess.outDialog)
		}
		if sess.inDialog != nil {
			go func(d *sipgo.DialogServerSession) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = d.Bye(ctx)
				d.Close()
			}(sess.inDialog)
		}
	} else {
		if sess.outDialog != nil {
			sess.outDialog.Close()
		}
		if sess.inDialog != nil {
			sess.inDialog.Close()
		}
	}
	b.logger.Printf("sip session closed call=%s dir=%d reason=%s cause=%d", sess.callID.String(), sess.direction, reason, cause)
}

func (b *SIPBridge) readRTPLoop(sess *sipCallSession) {
	if sess == nil || sess.rtpConn == nil {
		return
	}
	defer close(sess.rtpReaderDone)

	buf := make([]byte, 2048)
	for {
		n, _, err := sess.rtpConn.ReadFromUDP(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				b.logger.Printf("sip rtp read failed call=%s: %v", sess.callID.String(), err)
			}
			return
		}
		if n < 12 {
			continue
		}
		pt := buf[1] & 0x7f
		payload := append([]byte(nil), buf[12:n]...)
		b.handleRTPPayload(sess.callID, pt, payload)
	}
}

func (b *SIPBridge) handleRTPPayload(callID uuid.UUID, payloadType byte, payload []byte) {
	b.mu.Lock()
	sess := b.sessionsByCall[callID]
	if sess == nil || sess.closed {
		b.mu.Unlock()
		return
	}
	if sess.payloadType != 0 && payloadType != sess.payloadType {
		// Some SIP stacks negotiate ACELP on one dynamic PT but send media on
		// another dynamic PT once bridged/transcoded. Accept the first inbound
		// packet as authoritative to avoid false media drops.
		if !sess.rtpRxSeen {
			oldPT := sess.payloadType
			sess.payloadType = payloadType
			b.logger.Printf(
				"sip rtp inbound payload remap call=%s from_pt=%d to_pt=%d bytes=%d",
				callID.String(),
				oldPT,
				payloadType,
				len(payload),
			)
		} else {
			if !sess.rtpPTWarned {
				sess.rtpPTWarned = true
				b.logger.Printf(
					"sip rtp inbound drop payload mismatch call=%s got_pt=%d want_pt=%d bytes=%d",
					callID.String(),
					payloadType,
					sess.payloadType,
					len(payload),
				)
			}
			b.mu.Unlock()
			return
		}
	}
	firstInbound := false
	if !sess.rtpRxSeen {
		sess.rtpRxSeen = true
		firstInbound = true
	}
	sess.rtpLastRx = time.Now()
	stes := decodeACELPRTPPayload(payload, &sess.pendingCodec18)
	b.mu.Unlock()
	if firstInbound {
		b.logger.Printf("sip rtp inbound first packet call=%s pt=%d bytes=%d", callID.String(), payloadType, len(payload))
	}

	for _, ste := range stes {
		_ = b.plane.SendFrame(brew.FrameTypeTrafficChannel, callID, uint16(len(ste)*8), ste)
	}
}

func decodeACELPRTPPayload(payload []byte, pendingCodec18 *[]byte) [][]byte {
	if len(payload) == 0 {
		return nil
	}

	out := make([][]byte, 0, 2)
	if len(payload)%18 == 0 {
		for i := 0; i < len(payload); i += 18 {
			frame := append([]byte(nil), payload[i:i+18]...)
			if len(*pendingCodec18) == 0 {
				*pendingCodec18 = frame
				continue
			}
			ste := pairCodec18ToSTE(*pendingCodec18, frame)
			*pendingCodec18 = nil
			out = append(out, ste)
		}
		return out
	}

	ste, ready, err := normalizeRadioFrame(payload, pendingCodec18)
	if err == nil && ready {
		out = append(out, ste)
	}
	return out
}

func (b *SIPBridge) allocateRTPConn() (*net.UDPConn, int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.allocateRTPConnLocked()
}

func (b *SIPBridge) allocateRTPConnLocked() (*net.UDPConn, int, error) {
	for i := 0; i < 500; i++ {
		port := b.nextRTPPort
		if port <= 0 {
			port = b.cfg.SIP.RTPPortStart
		}
		b.nextRTPPort = port + 2
		if b.nextRTPPort >= 65534 {
			b.nextRTPPort = b.cfg.SIP.RTPPortStart
		}

		addr := &net.UDPAddr{IP: net.ParseIP(b.cfg.SIP.RTPBindAddr), Port: port}
		if addr.IP == nil {
			addr.IP = net.IPv4zero
		}
		conn, err := net.ListenUDP("udp", addr)
		if err == nil {
			return conn, port, nil
		}
	}
	return nil, 0, fmt.Errorf("failed to allocate RTP socket from start port %d", b.cfg.SIP.RTPPortStart)
}

func (b *SIPBridge) buildSDPOffer(localPort int) []byte {
	return []byte(fmt.Sprintf(
		"v=0\r\no=- 0 0 IN IP4 %s\r\ns=tetra-brew-sip\r\nc=IN IP4 %s\r\nt=0 0\r\nm=audio %d RTP/AVP %d\r\na=rtpmap:%d ACELP/8000\r\na=ptime:60\r\na=maxptime:60\r\na=sendrecv\r\n",
		b.cfg.SIP.RTPAdvertiseIP,
		b.cfg.SIP.RTPAdvertiseIP,
		localPort,
		b.cfg.SIP.ACELPPayloadType,
		b.cfg.SIP.ACELPPayloadType,
	))
}

func parseRTPFromSDP(body []byte, defaultPayloadType uint8) (*net.UDPAddr, uint8, bool) {
	sdp := string(body)
	var (
		ip              net.IP
		port            int
		payloadT        = int(defaultPayloadType)
		mAudioPayloadPT []int
	)
	scanner := bufio.NewScanner(strings.NewReader(sdp))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "c=") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				ip = net.ParseIP(strings.TrimSpace(parts[2]))
			}
			continue
		}
		if strings.HasPrefix(line, "m=audio ") {
			rest := strings.TrimPrefix(line, "m=audio ")
			parts := strings.Fields(rest)
			if len(parts) >= 3 {
				port, _ = strconv.Atoi(parts[0])
				mAudioPayloadPT = mAudioPayloadPT[:0]
				for _, rawPT := range parts[2:] {
					if pt, err := strconv.Atoi(rawPT); err == nil {
						mAudioPayloadPT = append(mAudioPayloadPT, pt)
					}
				}
			}
			continue
		}
	}

	if len(mAudioPayloadPT) > 0 {
		for _, pt := range mAudioPayloadPT {
			if pt == int(defaultPayloadType) {
				payloadT = pt
				goto doneSelectPT
			}
		}
		payloadT = mAudioPayloadPT[0]
	}

doneSelectPT:
	if ip == nil || port <= 0 {
		return nil, defaultPayloadType, false
	}
	return &net.UDPAddr{IP: ip, Port: port}, uint8(payloadT), true
}

func parseTargetISSI(user string, fallback uint32, gatewayISSI uint32) uint32 {
	user = strings.TrimSpace(user)
	if user == "" {
		return fallback
	}
	val, err := strconv.ParseUint(user, 10, 32)
	if err != nil {
		return fallback
	}
	issi := uint32(val)
	if issi == gatewayISSI && fallback != 0 {
		return fallback
	}
	return issi
}

func parseSIPCallerLabel(req *sip.Request) string {
	if req == nil {
		return ""
	}
	if req.From() != nil {
		if v := strings.TrimSpace(req.From().Address.User); v != "" {
			return v
		}
		if v := strings.Trim(strings.TrimSpace(req.From().DisplayName), "\""); v != "" {
			return v
		}
	}
	if req.Contact() != nil {
		if v := strings.TrimSpace(req.Contact().Address.User); v != "" {
			return v
		}
	}
	return "sip"
}

func normalizeSIPCallerNumber(raw string, fallbackISSI uint32) string {
	if digits := digitsOnly(raw); digits != "" {
		return digits
	}
	if fallbackISSI != 0 {
		return strconv.FormatUint(uint64(fallbackISSI), 10)
	}
	return ""
}

func outboundSIPCallerIdentity(sourceISSI uint32, fallback string) string {
	if sourceISSI != 0 {
		return strconv.FormatUint(uint64(sourceISSI), 10)
	}
	if digits := digitsOnly(fallback); digits != "" {
		return digits
	}
	return strings.TrimSpace(fallback)
}

func addOutboundCallerIdentityHeaders(req *sip.Request, callerIdentity string, domain string) {
	if req == nil {
		return
	}
	callerIdentity = strings.TrimSpace(callerIdentity)
	if callerIdentity == "" {
		return
	}
	domain = strings.TrimSpace(domain)
	if domain == "" {
		return
	}

	params := sip.NewParams()
	params.Add("tag", sip.GenerateTagN(16))
	from := &sip.FromHeader{
		DisplayName: callerIdentity,
		Address: sip.Uri{
			User: callerIdentity,
			Host: domain,
		},
		Params: params,
	}
	if req.From() != nil {
		req.ReplaceHeader(from)
	} else {
		req.AppendHeader(from)
	}
	if req.GetHeader("P-Asserted-Identity") == nil {
		req.AppendHeader(sip.NewHeader("P-Asserted-Identity", fmt.Sprintf("<sip:%s@%s>", callerIdentity, domain)))
	}
}

func digitsOnly(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(raw))
	for _, r := range raw {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func parseSIPSourceISSI(req *sip.Request, fallback uint32, gatewayISSI uint32) uint32 {
	const maxISSI = 0x00FF_FFFF
	if req == nil {
		return fallback
	}
	candidates := []string{}
	if req.From() != nil {
		candidates = append(candidates, strings.TrimSpace(req.From().Address.User))
	}
	if req.Contact() != nil {
		candidates = append(candidates, strings.TrimSpace(req.Contact().Address.User))
	}
	for _, raw := range candidates {
		if raw == "" {
			continue
		}
		val, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			continue
		}
		issi := uint32(val)
		if issi == 0 || issi == gatewayISSI || issi > maxISSI {
			continue
		}
		return issi
	}
	return fallback
}

func (b *SIPBridge) registrationLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		next, err := b.registerOnce(ctx)
		if err != nil {
			b.logger.Printf("sip register failed: %v", err)
		}
		if next <= 0 {
			next = b.cfg.SIP.ReconnectDelay
			if next <= 0 {
				next = 5 * time.Second
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(next):
		}
	}
}

func (b *SIPBridge) registerOnce(ctx context.Context) (time.Duration, error) {
	req := b.buildRegisterRequest()
	tx, err := b.client.TransactionRequest(ctx, req, sipgo.ClientRequestRegisterBuild)
	if err != nil {
		return 0, fmt.Errorf("create register transaction: %w", err)
	}
	res, err := waitFinalSIPResponse(tx)
	tx.Terminate()
	if err != nil {
		return 0, err
	}

	if int(res.StatusCode) == int(sip.StatusUnauthorized) && b.cfg.SIP.Username != "" {
		wwwAuth := res.GetHeader("WWW-Authenticate")
		if wwwAuth == nil {
			return 0, fmt.Errorf("register unauthorized without challenge")
		}
		chal, err := digest.ParseChallenge(wwwAuth.Value())
		if err != nil {
			return 0, fmt.Errorf("parse register challenge: %w", err)
		}
		cred, err := digest.Digest(chal, digest.Options{
			Method:   req.Method.String(),
			URI:      req.Recipient.Host,
			Username: b.cfg.SIP.Username,
			Password: b.cfg.SIP.Password,
		})
		if err != nil {
			return 0, fmt.Errorf("build register digest: %w", err)
		}

		newReq := req.Clone()
		newReq.RemoveHeader("Via")
		newReq.AppendHeader(sip.NewHeader("Authorization", cred.String()))
		tx, err := b.client.TransactionRequest(ctx, newReq, sipgo.ClientRequestIncreaseCSEQ, sipgo.ClientRequestAddVia)
		if err != nil {
			return 0, fmt.Errorf("create authenticated register transaction: %w", err)
		}
		res, err = waitFinalSIPResponse(tx)
		tx.Terminate()
		if err != nil {
			return 0, err
		}
	}

	if int(res.StatusCode) != int(sip.StatusOK) {
		return 0, fmt.Errorf("register status=%d", res.StatusCode)
	}

	expiresSec := b.cfg.SIP.RegisterExpires
	if expiresSec <= 0 {
		expiresSec = 120
	}
	if hdr := res.GetHeader("Expires"); hdr != nil {
		if sec, err := strconv.Atoi(strings.TrimSpace(hdr.Value())); err == nil && sec > 0 {
			expiresSec = sec
		}
	}
	expires := time.Duration(expiresSec) * time.Second
	next := (expires * 3) / 4
	if next < 15*time.Second {
		next = 15 * time.Second
	}
	b.logger.Printf("sip register ok expires=%s refresh_in=%s", expires.String(), next.String())
	return next, nil
}

func (b *SIPBridge) buildRegisterRequest() *sip.Request {
	recipient := sip.Uri{Host: b.cfg.SIP.Domain, User: b.cfg.SIP.LocalUser}
	req := sip.NewRequest(sip.REGISTER, recipient)
	req.SetTransport(strings.ToUpper(strings.TrimSpace(b.cfg.SIP.Transport)))
	req.SetDestination(b.cfg.SIP.ServerAddr)
	req.AppendHeader(sip.NewHeader("Contact", b.registerContactHeaderValue()))
	expiresSec := b.cfg.SIP.RegisterExpires
	if expiresSec <= 0 {
		expiresSec = 120
	}
	req.AppendHeader(sip.NewHeader("Expires", strconv.Itoa(expiresSec)))
	return req
}

func (b *SIPBridge) registerContactHeaderValue() string {
	host, port, err := splitHostPortLoose(b.cfg.SIP.BindAddr)
	if err != nil {
		host = b.cfg.SIP.RTPAdvertiseIP
		port = 25060
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = b.cfg.SIP.RTPAdvertiseIP
	}
	return fmt.Sprintf("<sip:%s@%s:%d>", b.cfg.SIP.LocalUser, host, port)
}

func waitFinalSIPResponse(tx sip.ClientTransaction) (*sip.Response, error) {
	for {
		select {
		case <-tx.Done():
			if err := tx.Err(); err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("sip transaction terminated")
		case res := <-tx.Responses():
			if res == nil || res.IsProvisional() {
				continue
			}
			return res, nil
		}
	}
}

func splitHostPortLoose(addr string) (string, int, error) {
	host, portRaw, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}

func udpAddrString(addr *net.UDPAddr) string {
	if addr == nil {
		return ""
	}
	return addr.String()
}

func requestCallID(req *sip.Request) string {
	if req == nil {
		return ""
	}
	callID := req.CallID()
	if callID == nil {
		return ""
	}
	return strings.TrimSpace(string(*callID))
}

func sipInboundEarlyReleaseStatus(callState uint8, cause uint8) (int, string) {
	_ = cause
	switch callState {
	case brew.CallStateSetupReject:
		return int(sip.StatusBusyHere), "Busy Here"
	default:
		return int(sip.StatusRequestTerminated), "Request Terminated"
	}
}

func sipOutboundRejectCause(err error, fallback uint8) (uint8, int) {
	var derr *sipgo.ErrDialogResponse
	if !errors.As(err, &derr) || derr == nil || derr.Res == nil {
		return fallback, 0
	}
	status := int(derr.Res.StatusCode)
	switch derr.Res.StatusCode {
	case sip.StatusBusyHere, sip.StatusGlobalBusyEverywhere:
		return sipCauseUserBusy, status
	default:
		return fallback, status
	}
}

func sipCallCause(payload any, fallback uint8) uint8 {
	switch p := payload.(type) {
	case brew.CausePayload:
		return p.Cause
	default:
		return fallback
	}
}

func randomSSRC() uint32 {
	var b [4]byte
	if _, err := crand.Read(b[:]); err == nil {
		return binary.BigEndian.Uint32(b[:])
	}
	return uint32(time.Now().UnixNano())
}
