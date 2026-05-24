package service

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/freetetra/server/internal/brew"
	"github.com/freetetra/server/internal/config"
)

type BrewModulePlane struct {
	cfg    config.Config
	logger *log.Logger

	// Optional override of cfg.Client. When set, all per-connection params
	// (BaseURL, Path, Username, Password, timeouts) are taken from here
	// instead of cfg.Client. Lets one process run multiple Brew clients
	// with different connection targets (e.g. local + BrandMeister).
	clientOverride *config.BrewClientConfig
	label          string // optional log prefix to disambiguate multiple instances

	subscriberISSI uint32
	groups         []uint32

	send chan []byte

	mu        sync.RWMutex
	onCall    func(*brew.CallControlMessage)
	onFrame   func(*brew.FrameMessage)
	onSub     func(*brew.SubscriberMessage)
	connected bool

	attachMu          sync.RWMutex
	attachmentSeen    bool
	channelAttachSubs map[uint32]int
}

func NewBrewModulePlane(cfg config.Config, logger *log.Logger, subscriberISSI uint32, groups []uint32) *BrewModulePlane {
	sortedGroups := append([]uint32(nil), groups...)
	return &BrewModulePlane{
		cfg:               cfg,
		logger:            logger,
		subscriberISSI:    subscriberISSI,
		groups:            sortedGroups,
		send:              make(chan []byte, 1024),
		channelAttachSubs: make(map[uint32]int),
	}
}

// WithClient overrides the connection-level config (URL/path/auth/timeouts).
// Returns the same plane for chaining.
func (p *BrewModulePlane) WithClient(cc config.BrewClientConfig) *BrewModulePlane {
	p.clientOverride = &cc
	return p
}

// WithLabel sets a short prefix for log lines so multiple planes can be told apart.
func (p *BrewModulePlane) WithLabel(label string) *BrewModulePlane {
	p.label = label
	return p
}

func (p *BrewModulePlane) client() config.BrewClientConfig {
	if p.clientOverride != nil {
		return *p.clientOverride
	}
	return p.cfg.Client
}

func (p *BrewModulePlane) SetMessageHandlers(
	onCall func(*brew.CallControlMessage),
	onFrame func(*brew.FrameMessage),
	onSub func(*brew.SubscriberMessage),
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onCall = onCall
	p.onFrame = onFrame
	p.onSub = onSub
}

func (p *BrewModulePlane) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := p.runSession(ctx); err != nil && ctx.Err() == nil {
			p.logger.Printf("brew module plane session error: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(p.reconnectDelay()):
		}
	}
}

func (p *BrewModulePlane) runSession(ctx context.Context) error {
	wsURL, err := p.discoverEndpoint(ctx)
	if err != nil {
		return err
	}

	dialer := websocket.Dialer{
		Subprotocols:     []string{"brew"},
		HandshakeTimeout: p.discoveryTimeout(),
	}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("brew websocket dial: %w", err)
	}
	defer conn.Close()
	p.setConnected(true)
	defer p.setConnected(false)

	p.logger.Printf("brew module plane connected ws=%s", wsURL)
	if err := p.sendAttachFrames(conn); err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go p.readLoop(conn, errCh)

	for {
		select {
		case <-ctx.Done():
			_ = conn.WriteControl(websocket.CloseMessage, []byte{}, time.Now().Add(p.writeTimeout()))
			return nil
		case err := <-errCh:
			return err
		case payload := <-p.send:
			if err := p.writeMessage(conn, payload); err != nil {
				return err
			}
		}
	}
}

func (p *BrewModulePlane) sendAttachFrames(conn *websocket.Conn) error {
	if p.subscriberISSI == 0 {
		return nil
	}
	if err := p.writeMessage(conn, brew.BuildSubscriberRegister(p.subscriberISSI, nil)); err != nil {
		return fmt.Errorf("send subscriber register: %w", err)
	}
	if len(p.groups) > 0 {
		if err := p.writeMessage(conn, brew.BuildSubscriberAffiliate(p.subscriberISSI, p.groups)); err != nil {
			return fmt.Errorf("send subscriber affiliate: %w", err)
		}
	}
	return nil
}

func (p *BrewModulePlane) readLoop(conn *websocket.Conn, errCh chan<- error) {
	for {
		msgType, payload, err := conn.ReadMessage()
		if err != nil {
			errCh <- err
			return
		}
		if msgType != websocket.BinaryMessage {
			continue
		}
		msg, err := brew.ParseMessage(payload)
		if err != nil {
			p.logger.Printf("brew module plane invalid packet: %v", err)
			continue
		}
		p.dispatchMessage(msg)
	}
}

func (p *BrewModulePlane) writeMessage(conn *websocket.Conn, payload []byte) error {
	_ = conn.SetWriteDeadline(time.Now().Add(p.writeTimeout()))
	err := conn.WriteMessage(websocket.BinaryMessage, payload)
	_ = conn.SetWriteDeadline(time.Time{})
	return err
}

func (p *BrewModulePlane) dispatchMessage(msg brew.ParsedMessage) {
	p.mu.RLock()
	onCall := p.onCall
	onFrame := p.onFrame
	onSub := p.onSub
	p.mu.RUnlock()

	switch m := msg.(type) {
	case *brew.CallControlMessage:
		if onCall != nil {
			onCall(m)
		}
	case *brew.FrameMessage:
		if onFrame != nil {
			onFrame(m)
		}
	case *brew.SubscriberMessage:
		if onSub != nil {
			onSub(m)
		}
	case *brew.ServiceMessage:
		if m.ServiceType == brew.ServiceTypeAttachmentControlV1 {
			p.updateAttachmentState(m.JSONData)
		}
	}
}

func (p *BrewModulePlane) enqueue(payload []byte) bool {
	if payload == nil {
		return false
	}
	wire := append([]byte(nil), payload...)
	select {
	case p.send <- wire:
		return true
	default:
		p.logger.Printf("brew module plane drop outbound frame len=%d reason=queue-full", len(wire))
		return false
	}
}

// EnsureGroup adds gssi to the plane's affiliation list if it isn't already
// present. If the plane has an active session, a fresh SubscriberAffiliate
// is queued with the full updated list so the brew server starts routing
// traffic for the new GSSI. Returns true if the set was modified.
func (p *BrewModulePlane) EnsureGroup(gssi uint32) bool {
	if gssi == 0 {
		return false
	}
	p.mu.Lock()
	for _, existing := range p.groups {
		if existing == gssi {
			p.mu.Unlock()
			return false
		}
	}
	p.groups = append(p.groups, gssi)
	snapshot := append([]uint32(nil), p.groups...)
	isi := p.subscriberISSI
	connected := p.connected
	p.mu.Unlock()

	if connected && isi != 0 {
		p.enqueue(brew.BuildSubscriberAffiliate(isi, snapshot))
	}
	return true
}

func (p *BrewModulePlane) setConnected(v bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connected = v
}

func (p *BrewModulePlane) discoverEndpoint(ctx context.Context) (string, error) {
	baseURL, err := url.Parse(strings.TrimSpace(p.client().BaseURL))
	if err != nil {
		return "", fmt.Errorf("invalid BREW_CLIENT_BASE_URL: %w", err)
	}
	if baseURL.Scheme == "" {
		baseURL.Scheme = "http"
	}
	discoveryURL := *baseURL
	discoveryURL.Path = joinURLPath(baseURL.Path, p.client().Path)
	if !strings.HasSuffix(discoveryURL.Path, "/") {
		discoveryURL.Path += "/"
	}

	client := &http.Client{Timeout: p.discoveryTimeout()}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryURL.String(), nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("brew discovery request failed: %w", err)
	}

	if resp.StatusCode == http.StatusUnauthorized && p.client().Username != "" {
		challenge := resp.Header.Get("WWW-Authenticate")
		_ = resp.Body.Close()
		authHeader := p.buildDigestAuthorization(challenge, req)
		if authHeader == "" {
			return "", fmt.Errorf("brew discovery unauthorized and digest auth could not be prepared")
		}
		req2, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryURL.String(), nil)
		if err != nil {
			return "", err
		}
		req2.Header.Set("Authorization", authHeader)
		resp, err = client.Do(req2)
		if err != nil {
			return "", fmt.Errorf("brew discovery digest request failed: %w", err)
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return "", fmt.Errorf("brew discovery failed status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	endpointRaw, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return "", fmt.Errorf("brew discovery read failed: %w", err)
	}
	endpoint := strings.TrimSpace(string(endpointRaw))
	if endpoint == "" {
		return "", fmt.Errorf("brew discovery returned empty endpoint")
	}
	return resolveWSEndpoint(baseURL, endpoint)
}

func (p *BrewModulePlane) buildDigestAuthorization(challenge string, req *http.Request) string {
	if challenge == "" {
		return ""
	}
	parts := parseDigestHeaderKV(challenge)
	realm := parts["realm"]
	nonce := parts["nonce"]
	if realm == "" || nonce == "" {
		return ""
	}

	uri := req.URL.RequestURI()
	if uri == "" {
		uri = "/"
	}
	qop := "auth"
	if raw := strings.TrimSpace(parts["qop"]); raw != "" {
		choices := strings.Split(raw, ",")
		found := false
		for _, choice := range choices {
			if strings.TrimSpace(choice) == "auth" {
				found = true
				break
			}
		}
		if !found {
			qop = ""
		}
	}
	nc := "00000001"
	cnonce := randomHex(8)

	ha1 := md5HexString(fmt.Sprintf("%s:%s:%s", p.client().Username, realm, p.client().Password))
	ha2 := md5HexString(fmt.Sprintf("%s:%s", req.Method, uri))
	response := ""
	if qop != "" {
		response = md5HexString(fmt.Sprintf("%s:%s:%s:%s:%s:%s", ha1, nonce, nc, cnonce, qop, ha2))
	} else {
		response = md5HexString(fmt.Sprintf("%s:%s:%s", ha1, nonce, ha2))
	}

	header := fmt.Sprintf(
		`Digest username="%s", realm="%s", nonce="%s", uri="%s", response="%s"`,
		p.client().Username,
		realm,
		nonce,
		uri,
		response,
	)
	if opaque := parts["opaque"]; opaque != "" {
		header += fmt.Sprintf(`, opaque="%s"`, opaque)
	}
	if qop != "" {
		header += fmt.Sprintf(`, qop=%s, nc=%s, cnonce="%s"`, qop, nc, cnonce)
	}
	return header
}

func (p *BrewModulePlane) StartInjectedCall(_ string, callID uuid.UUID, sourceISSI, destinationGSI uint32) bool {
	return p.StartInjectedGroupTX("", callID, sourceISSI, destinationGSI, 0, 0, 0)
}

func (p *BrewModulePlane) StartInjectedGroupTX(
	_ string,
	callID uuid.UUID,
	sourceISSI, destinationGSI uint32,
	priority uint8,
	access uint8,
	service uint16,
) bool {
	return p.enqueue(brew.BuildGroupTXWithAccess(callID, sourceISSI, destinationGSI, priority, access, service))
}

func (p *BrewModulePlane) IdleInjectedCall(_ string, callID uuid.UUID, cause uint8) {
	_ = p.enqueue(brew.BuildGroupIdle(callID, cause))
}

func (p *BrewModulePlane) ReleaseInjectedCall(_ string, callID uuid.UUID, cause uint8) {
	_ = p.enqueue(brew.BuildCallRelease(callID, cause))
}

func (p *BrewModulePlane) SendSetupRequest(callID uuid.UUID, payload brew.CircularCallPayload) bool {
	return p.enqueue(brew.BuildSetupRequest(callID, payload))
}

func (p *BrewModulePlane) SendSetupAccept(callID uuid.UUID) bool {
	return p.enqueue(brew.BuildSetupAccept(callID))
}

func (p *BrewModulePlane) SendSetupReject(callID uuid.UUID, cause uint8) bool {
	return p.enqueue(brew.BuildSetupReject(callID, cause))
}

func (p *BrewModulePlane) SendConnectRequest(callID uuid.UUID, payload brew.CircularCallPayload) bool {
	return p.enqueue(brew.BuildConnectRequest(callID, payload))
}

func (p *BrewModulePlane) SendConnectConfirm(callID uuid.UUID, grant uint8, permission uint8) bool {
	return p.enqueue(brew.BuildConnectConfirm(callID, brew.CircularGrantPayload{
		Grant:      grant,
		Permission: permission,
	}))
}

func (p *BrewModulePlane) SendCallRelease(callID uuid.UUID, cause uint8) bool {
	return p.enqueue(brew.BuildCallRelease(callID, cause))
}

func (p *BrewModulePlane) InjectedVoiceFrame(_ string, callID uuid.UUID, data []byte) {
	ste, err := normalizeTrafficSTE(data)
	if err != nil {
		p.logger.Printf("brew module plane drop traffic frame call=%s: %v", callID.String(), err)
		return
	}
	_ = p.enqueue(brew.BuildVoiceFrame(callID, uint16(len(ste)*8), ste))
}

func (p *BrewModulePlane) InjectedPacketFrame(_ string, callID uuid.UUID, data []byte) {
	_ = p.enqueue(brew.BuildPacketDataFrame(callID, uint16(len(data)*8), data))
}

func (p *BrewModulePlane) GroupSubscriberCount(gssi uint32) int {
	p.attachMu.RLock()
	defer p.attachMu.RUnlock()
	return p.channelAttachSubs[gssi]
}

func (p *BrewModulePlane) reconnectDelay() time.Duration {
	if p.client().ReconnectDelay <= 0 {
		return 3 * time.Second
	}
	return p.client().ReconnectDelay
}

func (p *BrewModulePlane) discoveryTimeout() time.Duration {
	if p.client().DiscoveryTimeout <= 0 {
		return 5 * time.Second
	}
	return p.client().DiscoveryTimeout
}

func (p *BrewModulePlane) writeTimeout() time.Duration {
	if p.client().WriteTimeout <= 0 {
		return 5 * time.Second
	}
	return p.client().WriteTimeout
}

func joinURLPath(basePath, appendPath string) string {
	basePath = strings.TrimSpace(basePath)
	appendPath = strings.TrimSpace(appendPath)
	if appendPath == "" {
		appendPath = "/brew"
	}
	if !strings.HasPrefix(appendPath, "/") {
		appendPath = "/" + appendPath
	}
	basePath = strings.TrimSuffix(basePath, "/")
	return basePath + appendPath
}

func resolveWSEndpoint(base *url.URL, endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err == nil && parsed.Scheme != "" {
		switch parsed.Scheme {
		case "http":
			parsed.Scheme = "ws"
		case "https":
			parsed.Scheme = "wss"
		}
		return parsed.String(), nil
	}

	wsScheme := "ws"
	if strings.EqualFold(base.Scheme, "https") || strings.EqualFold(base.Scheme, "wss") {
		wsScheme = "wss"
	}
	u := url.URL{
		Scheme: wsScheme,
		Host:   base.Host,
		Path:   endpoint,
	}
	if !strings.HasPrefix(endpoint, "/") {
		u.Path = "/" + endpoint
	}
	return u.String(), nil
}

func parseDigestHeaderKV(header string) map[string]string {
	header = strings.TrimSpace(header)
	header = strings.TrimPrefix(header, "Digest ")
	header = strings.TrimPrefix(header, "digest ")
	out := make(map[string]string)
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		pair := strings.SplitN(part, "=", 2)
		if len(pair) != 2 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(pair[0]))
		val := strings.Trim(strings.TrimSpace(pair[1]), `"`)
		out[key] = val
	}
	return out
}

func md5HexString(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

type attachmentControlWire struct {
	Type     string                     `json:"type"`
	Version  int                        `json:"version"`
	Channels []attachmentControlChannel `json:"channels"`
}

type attachmentControlChannel struct {
	GSSI        uint32 `json:"gssi"`
	Subscribers int    `json:"subscribers"`
}

func (p *BrewModulePlane) updateAttachmentState(raw string) {
	if strings.TrimSpace(raw) == "" {
		return
	}
	var msg attachmentControlWire
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		p.logger.Printf("brew module plane attachment-control decode failed: %v", err)
		return
	}
	if msg.Type != "channel_attachment" || msg.Version != 1 {
		return
	}

	next := make(map[uint32]int, len(msg.Channels))
	for _, ch := range msg.Channels {
		if ch.GSSI == 0 {
			continue
		}
		if ch.Subscribers < 0 {
			ch.Subscribers = 0
		}
		next[ch.GSSI] = ch.Subscribers
	}

	p.attachMu.Lock()
	p.channelAttachSubs = next
	p.attachmentSeen = true
	p.attachMu.Unlock()
}
