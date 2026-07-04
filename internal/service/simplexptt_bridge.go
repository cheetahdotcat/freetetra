package service

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/freetetra/server/internal/brew"
	"github.com/freetetra/server/internal/config"
)

//go:embed simplexptt_assets/*
var simplexPTTAssets embed.FS

// pttSilentFrame is a 36-byte STE frame the plane sanitizes into silence. It
// pads the key-up (while receivers open squelch) and key-down (so the tail of
// the last real frame isn't clipped by call teardown) edges of a transmission.
var pttSilentFrame = make([]byte, 36)

// pttPCMFrameBytes is the byte length of one decoded voice frame: 240 samples
// (30 ms @ 8 kHz) of signed 16-bit mono PCM. The decoder emits exactly this
// per 18-byte ACELP frame, and it's the broadcast granularity to browsers.
const pttPCMFrameBytes = 240 * 2

// SimplexPTTBridge is a browser push-to-talk module. It serves a small web UI
// plus a WebSocket that carries raw PCM (s16le, 8 kHz, mono) both directions:
// a browser holds the PTT button to stream its microphone up (encoded into
// TETRA voice on the selected talkgroup), and any traffic received on a
// configured TG is decoded and streamed down to every connected listener.
//
// The channel is half-duplex, like a real radio: a single talk-lock lets one
// browser key the channel at a time, and a key-up is refused while another
// station (local or on-air) is already transmitting on that TG.
type SimplexPTTBridge struct {
	cfg    config.Config
	logger *log.Logger
	plane  *BrewModulePlane

	tgSet     map[uint32]bool
	tgList    []uint32
	defaultTG uint32

	server   *http.Server
	upgrader websocket.Upgrader

	dec *pttDecoder

	clientsMu sync.RWMutex
	clients   map[*pttClient]struct{}

	// txMu guards the talk-lock and the set of session ISSIs currently keyed.
	txMu       sync.Mutex
	talker     *pttClient
	talkerISSI uint32
	talkTG     uint32

	// rxMu guards the active inbound-call table used for the channel-busy
	// check and the "who is talking" indicator pushed to clients.
	rxMu    sync.Mutex
	rxCalls map[uuid.UUID]pttRXCall
}

type pttRXCall struct {
	tg     uint32
	source uint32
}

// NewSimplexPTTBridge builds the bridge. The plane must already be registered
// on every configured talkgroup (main passes cfg.SimplexPTT.Talkgroups into
// NewBrewModulePlane) so the brew server both routes RX and accepts TX.
func NewSimplexPTTBridge(cfg config.Config, logger *log.Logger, plane *BrewModulePlane) (*SimplexPTTBridge, error) {
	if plane == nil {
		return nil, fmt.Errorf("brew module plane is nil")
	}
	tgs := cfg.SimplexPTT.Talkgroups
	if len(tgs) == 0 {
		return nil, fmt.Errorf("SIMPLEXPTT_TALKGROUPS must list at least one talkgroup")
	}
	if cfg.SimplexPTT.EncoderBin == "" {
		return nil, fmt.Errorf("SIMPLEXPTT_ENCODER_BIN is required")
	}
	if cfg.SimplexPTT.DecoderBin == "" {
		return nil, fmt.Errorf("SIMPLEXPTT_DECODER_BIN is required")
	}

	tgSet := make(map[uint32]bool, len(tgs))
	tgList := make([]uint32, 0, len(tgs))
	for _, tg := range tgs {
		if tg == 0 || tgSet[tg] {
			continue
		}
		tgSet[tg] = true
		tgList = append(tgList, tg)
	}
	if len(tgList) == 0 {
		return nil, fmt.Errorf("SIMPLEXPTT_TALKGROUPS has no non-zero talkgroup")
	}

	return &SimplexPTTBridge{
		cfg:       cfg,
		logger:    logger,
		plane:     plane,
		tgSet:     tgSet,
		tgList:    tgList,
		defaultTG: tgList[0],
		clients:   make(map[*pttClient]struct{}),
		rxCalls:   make(map[uuid.UUID]pttRXCall),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			// The module is a self-contained app on its own port; any origin
			// that can reach it is allowed to use it.
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}, nil
}

// SimplexPTTTalkgroups returns the unique configured TG set, for main to seed
// the BrewModulePlane's registration list before the bridge is built.
func SimplexPTTTalkgroups(cfg config.Config) []uint32 {
	seen := make(map[uint32]bool, len(cfg.SimplexPTT.Talkgroups))
	out := make([]uint32, 0, len(cfg.SimplexPTT.Talkgroups))
	for _, tg := range cfg.SimplexPTT.Talkgroups {
		if tg == 0 || seen[tg] {
			continue
		}
		seen[tg] = true
		out = append(out, tg)
	}
	return out
}

// ----- HTTP / WebSocket server -----

func (b *SimplexPTTBridge) Start(ctx context.Context) error {
	b.dec = newPTTDecoder(b.cfg.SimplexPTT.DecoderBin, b.logger, b.broadcastPCM)
	go b.dec.run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/", b.handleIndex)
	mux.HandleFunc("/ws", b.handleWS)

	b.server = &http.Server{Addr: b.cfg.SimplexPTT.ListenAddr, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.server.Shutdown(shutdownCtx)
	}()

	b.logger.Printf("simplex-ptt listening on %s (talkgroups=%v issi=%d)",
		b.cfg.SimplexPTT.ListenAddr, b.tgList, b.cfg.SimplexPTT.BrewISSI)

	if err := b.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (b *SimplexPTTBridge) Stop() {
	if b.server != nil {
		_ = b.server.Close()
	}
}

func (b *SimplexPTTBridge) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		http.NotFound(w, r)
		return
	}
	data, err := fs.ReadFile(simplexPTTAssets, "simplexptt_assets/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

// ----- client -----

type pttClient struct {
	bridge *SimplexPTTBridge
	conn   *websocket.Conn
	issi   uint32

	send chan pttOut

	// session is the in-flight TX for this client. It is only ever touched by
	// the client's read loop (key-up creates it, PCM writes to it, key-down/
	// disconnect tears it down), so it needs no lock of its own.
	session *pttSession
	tg      uint32
}

type pttOut struct {
	binary bool
	data   []byte
}

type pttSession struct {
	callID uuid.UUID
	issi   uint32
	tg     uint32
	enc    *exec.Cmd
	encIn  io.WriteCloser
	done   chan struct{} // closed once the encoder output is fully drained
}

func (b *SimplexPTTBridge) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := b.upgrader.Upgrade(w, r, nil)
	if err != nil {
		b.logger.Printf("simplex-ptt websocket upgrade failed: %v", err)
		return
	}

	c := &pttClient{
		bridge: b,
		conn:   conn,
		issi:   b.sessionISSI(uuid.New().String()),
		send:   make(chan pttOut, 256),
		tg:     b.defaultTG,
	}

	b.clientsMu.Lock()
	b.clients[c] = struct{}{}
	b.clientsMu.Unlock()

	b.logger.Printf("simplex-ptt client connected addr=%s issi=%d", conn.RemoteAddr(), c.issi)

	go c.writeLoop()
	c.sendHello()
	c.readLoop()

	// readLoop returned → client is gone.
	b.clientsMu.Lock()
	delete(b.clients, c)
	b.clientsMu.Unlock()
	c.endSession() // release the talk-lock + call if this client was talking
	close(c.send)
	_ = conn.Close()
	b.logger.Printf("simplex-ptt client disconnected issi=%d", c.issi)
}

// sessionISSI derives a stable per-connection source ISSI from a random token
// so RX call-control on the network shows which browser user is talking.
func (b *SimplexPTTBridge) sessionISSI(token string) uint32 {
	base := b.cfg.SimplexPTT.SourceISSIBase
	if base == 0 {
		base = 897000
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(token))
	return base + (h.Sum32() % 100000)
}

const (
	pttReadLimit   = 64 * 1024
	pttPongTimeout = 60 * time.Second
	pttPingPeriod  = 25 * time.Second
)

func (c *pttClient) writeLoop() {
	ping := time.NewTicker(pttPingPeriod)
	defer ping.Stop()
	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				return
			}
			mt := websocket.TextMessage
			if msg.binary {
				mt = websocket.BinaryMessage
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := c.conn.WriteMessage(mt, msg.data); err != nil {
				return
			}
		case <-ping.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *pttClient) readLoop() {
	c.conn.SetReadLimit(pttReadLimit)
	_ = c.conn.SetReadDeadline(time.Now().Add(pttPongTimeout))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pttPongTimeout))
	})

	for {
		mt, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		switch mt {
		case websocket.BinaryMessage:
			c.onPCM(data)
		case websocket.TextMessage:
			c.onControl(data)
		}
	}
}

// ----- control protocol (browser -> bridge) -----

type pttControl struct {
	Type  string `json:"type"`
	State string `json:"state"` // "up" / "down" for a ptt message
	TG    uint32 `json:"tg"`    // for a select_tg message
}

func (c *pttClient) onControl(raw []byte) {
	var msg pttControl
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	switch msg.Type {
	case "select_tg":
		if c.bridge.tgSet[msg.TG] {
			// Changing TG mid-transmission would strand the open call on the
			// old TG, so only honor it while idle.
			if c.session == nil {
				c.tg = msg.TG
				c.sendState()
			}
		}
	case "ptt":
		switch msg.State {
		case "up":
			c.startTalk()
		case "down":
			c.endSession()
			c.sendJSON(map[string]any{"type": "ptt_ended"})
			c.sendState()
		}
	}
}

// onPCM forwards a chunk of microphone PCM (s16le, 8 kHz, mono) from the
// browser into the active encoder. Chunks that arrive without an open session
// (a late frame after key-down, or before a grant) are simply dropped.
func (c *pttClient) onPCM(data []byte) {
	s := c.session
	if s == nil || len(data) == 0 {
		return
	}
	if _, err := s.encIn.Write(data); err != nil {
		// Encoder went away — tear the session down so the channel frees up.
		c.endSession()
		c.sendJSON(map[string]any{"type": "ptt_ended"})
		c.sendState()
	}
}

// startTalk attempts to acquire the talk-lock and open a TETRA call, then wires
// a per-session encoder whose 18-byte output is paired into STE frames and
// injected as voice. Called only from the client read loop.
func (c *pttClient) startTalk() {
	if c.session != nil {
		return // already talking
	}
	b := c.bridge

	b.txMu.Lock()
	if b.talker != nil && b.talker != c {
		busyISSI := b.talkerISSI
		b.txMu.Unlock()
		c.sendJSON(map[string]any{"type": "ptt_denied", "reason": "busy", "by": busyISSI})
		return
	}
	if src, busy := b.externalActiveOn(c.tg); busy {
		b.txMu.Unlock()
		c.sendJSON(map[string]any{"type": "ptt_denied", "reason": "channel_busy", "by": src})
		return
	}
	b.talker = c
	b.talkerISSI = c.issi
	b.talkTG = c.tg
	b.txMu.Unlock()

	session, err := c.openSession()
	if err != nil {
		b.logger.Printf("simplex-ptt start talk failed issi=%d tg=%d: %v", c.issi, c.tg, err)
		b.releaseTalkLock(c)
		c.sendJSON(map[string]any{"type": "ptt_denied", "reason": "error"})
		return
	}
	c.session = session
	c.sendJSON(map[string]any{"type": "ptt_granted", "tg": c.tg, "issi": c.issi})
	b.broadcastState()
	b.logger.Printf("simplex-ptt tx start issi=%d tg=%d call=%s", c.issi, c.tg, session.callID)
}

func (c *pttClient) openSession() (*pttSession, error) {
	b := c.bridge
	callID := uuid.New()
	if !b.plane.StartInjectedGroupTX("simplexptt", callID, c.issi, c.tg, 0, 0, 0) {
		return nil, fmt.Errorf("plane refused StartInjectedGroupTX")
	}

	// Lead-in silence so receivers finish opening squelch before real audio.
	interval := b.frameInterval()
	for i := 0; i < durationToFrameCount(b.cfg.SimplexPTT.LeadInPadding, interval); i++ {
		b.plane.InjectedVoiceFrame("simplexptt", callID, pttSilentFrame)
	}

	enc := exec.Command(b.cfg.SimplexPTT.EncoderBin)
	encIn, err := enc.StdinPipe()
	if err != nil {
		b.releaseCall(callID)
		return nil, fmt.Errorf("encoder stdin: %w", err)
	}
	encOut, err := enc.StdoutPipe()
	if err != nil {
		b.releaseCall(callID)
		return nil, fmt.Errorf("encoder stdout: %w", err)
	}
	encErr, _ := enc.StderrPipe()
	if err := enc.Start(); err != nil {
		b.releaseCall(callID)
		return nil, fmt.Errorf("start encoder: %w", err)
	}
	go logCommandOutput(b.logger, "simplex-ptt encoder", encErr)

	s := &pttSession{
		callID: callID,
		issi:   c.issi,
		tg:     c.tg,
		enc:    enc,
		encIn:  encIn,
		done:   make(chan struct{}),
	}

	// Drain the encoder: pair 18-byte codec frames into STE and inject them as
	// they are produced. Live microphone audio arrives in real time, so the
	// encoder emits frames at the radio's real-time cadence on its own — no
	// artificial pacing needed here.
	go func() {
		defer close(s.done)
		var pending []byte
		frame := make([]byte, 18)
		for {
			if _, rerr := io.ReadFull(encOut, frame); rerr != nil {
				return
			}
			ste, ready, nerr := normalizeRadioFrame(frame, &pending)
			if nerr != nil {
				b.logger.Printf("simplex-ptt frame error issi=%d: %v", c.issi, nerr)
				return
			}
			if ready {
				b.plane.InjectedVoiceFrame("simplexptt", callID, ste)
			}
		}
	}()

	return s, nil
}

// endSession closes the active transmission (if any): stop the encoder, let it
// drain, pad the tail with silence, release the call, and free the talk-lock.
// Safe to call when idle. Called only from the client read loop / cleanup.
func (c *pttClient) endSession() {
	s := c.session
	if s == nil {
		return
	}
	c.session = nil
	b := c.bridge

	// Closing stdin signals EOF; the encoder flushes its last frames and exits,
	// which ends the drain goroutine.
	_ = s.encIn.Close()
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		if s.enc.Process != nil {
			_ = s.enc.Process.Kill()
		}
		<-s.done
	}
	_ = s.enc.Wait()

	// Tail-out silence so the last real frame isn't clipped by teardown.
	interval := b.frameInterval()
	for i := 0; i < durationToFrameCount(b.cfg.SimplexPTT.TailOutPadding, interval); i++ {
		b.plane.InjectedVoiceFrame("simplexptt", s.callID, pttSilentFrame)
	}

	b.releaseCall(s.callID)
	b.releaseTalkLock(c)
	b.broadcastState()
	b.logger.Printf("simplex-ptt tx end issi=%d tg=%d call=%s", s.issi, s.tg, s.callID)
}

func (b *SimplexPTTBridge) releaseCall(callID uuid.UUID) {
	b.plane.IdleInjectedCall("simplexptt", callID, b.cfg.SimplexPTT.ReleaseCause)
	b.plane.ReleaseInjectedCall("simplexptt", callID, b.cfg.SimplexPTT.ReleaseCause)
}

func (b *SimplexPTTBridge) releaseTalkLock(c *pttClient) {
	b.txMu.Lock()
	if b.talker == c {
		b.talker = nil
		b.talkerISSI = 0
		b.talkTG = 0
	}
	b.txMu.Unlock()
}

func (b *SimplexPTTBridge) frameInterval() time.Duration {
	if b.cfg.SimplexPTT.FrameInterval <= 0 {
		return 60 * time.Millisecond
	}
	return b.cfg.SimplexPTT.FrameInterval
}

// ----- RX (network -> browsers) -----

// OnBrewCallControl tracks inbound calls on configured TGs: it maintains the
// channel-busy table used to gate local key-ups and pushes a "who is talking"
// indicator to every connected browser.
func (b *SimplexPTTBridge) OnBrewCallControl(m *brew.CallControlMessage) {
	if m == nil {
		return
	}
	switch m.CallState {
	case brew.CallStateGroupTX:
		p, ok := m.Payload.(brew.GroupTransmissionPayload)
		if !ok || !b.tgSet[p.Destination] {
			return
		}
		b.rxMu.Lock()
		b.rxCalls[m.Identifier] = pttRXCall{tg: p.Destination, source: p.Source}
		b.rxMu.Unlock()
		b.broadcastState()
	case brew.CallStateGroupIdle, brew.CallStateCallRelease:
		b.rxMu.Lock()
		_, existed := b.rxCalls[m.Identifier]
		delete(b.rxCalls, m.Identifier)
		b.rxMu.Unlock()
		if existed {
			b.broadcastState()
		}
	}
}

// OnBrewFrame feeds received traffic-channel audio into the shared decoder. The
// decoder's PCM output is broadcast to all browsers by the decoder's read loop.
func (b *SimplexPTTBridge) OnBrewFrame(callID uuid.UUID, frameType uint8, data []byte) {
	if frameType != brew.FrameTypeTrafficChannel {
		return
	}
	ste, err := normalizeTrafficSTE(data)
	if err != nil {
		return
	}
	a, c := steToCodecFrames(ste)
	b.dec.write(a)
	b.dec.write(c)
}

// externalActiveOn reports whether a station other than the client about to key
// is transmitting on tg right now. Called with txMu held. The current talker's
// own injected call echoes back as an inbound call, but by then the lock is
// already held so this only ever blocks a *new* key-up against a genuinely busy
// channel.
func (b *SimplexPTTBridge) externalActiveOn(tg uint32) (uint32, bool) {
	b.rxMu.Lock()
	defer b.rxMu.Unlock()
	for _, call := range b.rxCalls {
		if call.tg == tg && call.source != b.talkerISSI {
			return call.source, true
		}
	}
	return 0, false
}

// ----- broadcast helpers -----

func (b *SimplexPTTBridge) broadcastPCM(pcm []byte) {
	b.clientsMu.RLock()
	defer b.clientsMu.RUnlock()
	for c := range b.clients {
		c.trySend(pttOut{binary: true, data: pcm})
	}
}

func (b *SimplexPTTBridge) broadcastState() {
	state := b.stateMessage()
	raw, err := json.Marshal(state)
	if err != nil {
		return
	}
	b.clientsMu.RLock()
	defer b.clientsMu.RUnlock()
	for c := range b.clients {
		c.trySend(pttOut{binary: false, data: raw})
	}
}

// stateMessage summarizes who holds the local talk-lock and which network
// stations are currently transmitting, per configured TG.
func (b *SimplexPTTBridge) stateMessage() map[string]any {
	b.txMu.Lock()
	localTalker := b.talkerISSI
	localTG := b.talkTG
	b.txMu.Unlock()

	b.rxMu.Lock()
	rx := make([]map[string]any, 0, len(b.rxCalls))
	for _, call := range b.rxCalls {
		rx = append(rx, map[string]any{"tg": call.tg, "source": call.source})
	}
	b.rxMu.Unlock()

	return map[string]any{
		"type":         "state",
		"local_talker": localTalker,
		"local_tg":     localTG,
		"rx":           rx,
	}
}

func (c *pttClient) sendHello() {
	c.sendJSON(map[string]any{
		"type":        "hello",
		"issi":        c.issi,
		"talkgroups":  c.bridge.tgList,
		"default_tg":  c.bridge.defaultTG,
		"selected_tg": c.tg,
		"sample_rate": 8000,
	})
	c.sendState()
}

func (c *pttClient) sendState() {
	raw, err := json.Marshal(c.bridge.stateMessage())
	if err != nil {
		return
	}
	c.trySend(pttOut{binary: false, data: raw})
}

func (c *pttClient) sendJSON(v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		return
	}
	c.trySend(pttOut{binary: false, data: raw})
}

// trySend enqueues a message without blocking. A client whose buffer is full
// (slow/dead connection) drops the message rather than stalling the broadcast.
func (c *pttClient) trySend(msg pttOut) {
	defer func() { _ = recover() }() // send on a closed channel during teardown
	select {
	case c.send <- msg:
	default:
	}
}

// ----- shared RX decoder -----

// pttDecoder runs a single long-lived streaming ACELP decoder for the module.
// Received codec frames are written to its stdin; its PCM stdout is read in a
// loop and handed to broadcast. If the process dies it is respawned.
type pttDecoder struct {
	bin       string
	logger    *log.Logger
	broadcast func([]byte)

	mu sync.Mutex
	in io.WriteCloser // current stdin; nil while the process is down
}

func newPTTDecoder(bin string, logger *log.Logger, broadcast func([]byte)) *pttDecoder {
	return &pttDecoder{bin: bin, logger: logger, broadcast: broadcast}
}

func (d *pttDecoder) run(ctx context.Context) {
	for ctx.Err() == nil {
		d.session(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (d *pttDecoder) session(ctx context.Context) {
	cmd := exec.CommandContext(ctx, d.bin)
	in, err := cmd.StdinPipe()
	if err != nil {
		d.logger.Printf("simplex-ptt decoder stdin: %v", err)
		return
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		d.logger.Printf("simplex-ptt decoder stdout: %v", err)
		return
	}
	errPipe, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		d.logger.Printf("simplex-ptt decoder start: %v", err)
		return
	}
	go logCommandOutput(d.logger, "simplex-ptt decoder", errPipe)

	d.mu.Lock()
	d.in = in
	d.mu.Unlock()

	d.readLoop(out)

	d.mu.Lock()
	d.in = nil
	d.mu.Unlock()
	_ = in.Close()
	_ = cmd.Wait()
}

func (d *pttDecoder) readLoop(r io.Reader) {
	buf := make([]byte, pttPCMFrameBytes)
	for {
		if _, err := io.ReadFull(r, buf); err != nil {
			return
		}
		frame := make([]byte, pttPCMFrameBytes)
		copy(frame, buf)
		d.broadcast(frame)
	}
}

func (d *pttDecoder) write(frame []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.in == nil {
		return
	}
	_, _ = d.in.Write(frame)
}
