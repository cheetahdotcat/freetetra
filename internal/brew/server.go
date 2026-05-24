package brew

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"

	"github.com/freetetra/server/internal/config"
)

type EventHandler interface {
	OnConnect(client *Client)
	OnDisconnect(client *Client)
	OnMessage(client *Client, msg ParsedMessage)
}

// AuthVerifier is called to check if a username/password combination is valid.
// If set, it replaces the static username/password check from config.
// Returns true if the user is allowed to connect.
type AuthVerifier func(username, password string) bool

type Server struct {
	cfg     config.Config
	logger  *log.Logger
	handler EventHandler

	httpServer *http.Server
	grpcServer *grpc.Server
	upgrader   websocket.Upgrader

	clientsMu sync.RWMutex
	clients   map[string]*Client

	nonceMu sync.Mutex
	nonces  map[string]time.Time

	tokenMu sync.Mutex
	tokens  map[string]tokenEntry

	extraHandlers []httpHandlerRegistration
	authVerifier  AuthVerifier
	authChecker   func(ip string) bool // returns false if IP is banned
	authOnFailure func(ip string)      // called on auth failure
	authOnSuccess func(ip string)      // called on auth success
}

type httpHandlerRegistration struct {
	pattern string
	handler http.HandlerFunc
}

// tokenEntry is what we hand back to the client from /discovery. The username
// is the digest-auth identity that just passed; carrying it lets the WS
// handler stamp Client.Username so callers like service.OnConnect can join the
// session to a Station via ByISSI(username).
type tokenEntry struct {
	exp      time.Time
	username string
}

func NewServer(cfg config.Config, logger *log.Logger, handler EventHandler) *Server {
	return &Server{
		cfg:     cfg,
		logger:  logger,
		handler: handler,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
			Subprotocols: []string{"brew"},
		},
		clients: make(map[string]*Client),
		nonces:  make(map[string]time.Time),
		tokens:  make(map[string]tokenEntry),
	}
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	basePath := s.cfg.Server.Path
	wsPrefix := basePath + "/ws/"
	mux.HandleFunc(basePath, s.handleDiscovery)
	mux.HandleFunc(basePath+"/", s.handleDiscovery)
	mux.HandleFunc(wsPrefix, s.handleWebsocket)
	for _, h := range s.extraHandlers {
		mux.HandleFunc(h.pattern, h.handler)
	}

	handler := http.Handler(mux)
	if s.grpcServer != nil {
		handler = h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isGRPCRequest(r) {
				s.grpcServer.ServeHTTP(w, r)
				return
			}
			mux.ServeHTTP(w, r)
		}), &http2.Server{})
	}

	s.httpServer = &http.Server{
		Addr:    s.cfg.HTTPListenAddr,
		Handler: handler,
	}

	listener, err := net.Listen("tcp", s.cfg.HTTPListenAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.logger.Printf("brew server listening on %s (discovery path %s)", s.cfg.HTTPListenAddr, s.cfg.Server.Path)

	go func() {
		<-ctx.Done()
		if s.grpcServer != nil {
			s.grpcServer.GracefulStop()
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(shutdownCtx)
	}()

	err = s.httpServer.Serve(listener)
	if err == nil || err == http.ErrServerClosed {
		return nil
	}
	return err
}

// SetGRPCServer installs a gRPC server that should share the HTTP listener.
func (s *Server) SetGRPCServer(server *grpc.Server) {
	s.grpcServer = server
}

func isGRPCRequest(r *http.Request) bool {
	if r.ProtoMajor < 2 {
		return false
	}
	ct := strings.ToLower(strings.TrimSpace(r.Header.Get("content-type")))
	return strings.HasPrefix(ct, "application/grpc")
}

func (s *Server) BroadcastToGroup(gssi uint32, payload []byte, excludeClientID string) int {
	clients := s.snapshotClients()
	delivered := 0
	for _, c := range clients {
		if c.ID == excludeClientID {
			continue
		}
		if !c.IsAttachedTo(gssi) {
			continue
		}
		if c.Enqueue(payload) {
			delivered++
		}
	}
	return delivered
}

func (s *Server) BroadcastAll(payload []byte, excludeClientID string) int {
	clients := s.snapshotClients()
	delivered := 0
	for _, c := range clients {
		if c.ID == excludeClientID {
			continue
		}
		if c.Enqueue(payload) {
			delivered++
		}
	}
	return delivered
}

func (s *Server) BroadcastToSubscriber(number uint32, payload []byte, excludeClientID string) int {
	clients := s.snapshotClients()
	delivered := 0
	for _, c := range clients {
		if c.ID == excludeClientID {
			continue
		}
		if !c.HasSubscriber(number) {
			continue
		}
		if c.Enqueue(payload) {
			delivered++
		}
	}
	return delivered
}

func (s *Server) SendToClient(clientID string, payload []byte) bool {
	clientID = strings.TrimSpace(clientID)
	if clientID == "" {
		return false
	}
	s.clientsMu.RLock()
	client := s.clients[clientID]
	s.clientsMu.RUnlock()
	if client == nil {
		return false
	}
	return client.Enqueue(payload)
}

func (s *Server) CountAttachedToGroup(gssi uint32) int {
	clients := s.snapshotClients()
	count := 0
	for _, c := range clients {
		if c.IsAttachedTo(gssi) {
			count++
		}
	}
	return count
}

func (s *Server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if websocket.IsWebSocketUpgrade(r) {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var username string
	if s.authEnabled() {
		clientIP := r.Header.Get("X-Real-IP")
		if clientIP == "" {
			clientIP = strings.Split(r.RemoteAddr, ":")[0]
		}

		// Check if IP is banned
		if s.authChecker != nil && !s.authChecker(clientIP) {
			s.logger.Printf("auth BANNED ip=%s", clientIP)
			http.Error(w, "too many failed attempts — try again later", http.StatusTooManyRequests)
			return
		}

		var ok bool
		username, ok = s.authorizeRequest(r)
		if !ok {
			if s.authOnFailure != nil {
				s.authOnFailure(clientIP)
			}
			s.issueDigestChallenge(w)
			return
		}

		if s.authOnSuccess != nil {
			s.authOnSuccess(clientIP)
		}
	}

	token := s.issueToken(username)
	endpoint := fmt.Sprintf("%s/ws/%s", s.cfg.Server.Path, token)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(endpoint))
}

func (s *Server) handleWebsocket(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.URL.Path, s.cfg.Server.Path+"/ws/")
	if token == "" || token == r.URL.Path {
		http.Error(w, "missing endpoint token", http.StatusUnauthorized)
		return
	}
	username, ok := s.consumeToken(token)
	if !ok {
		http.Error(w, "invalid endpoint token", http.StatusUnauthorized)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Printf("ws upgrade failed: %v", err)
		return
	}
	id := randHex(8)
	client := newClient(id, r.RemoteAddr, username, conn)
	s.registerClient(client)
	if s.handler != nil {
		s.handler.OnConnect(client)
	}
	s.logger.Printf("client connected id=%s remote=%s username=%s", id, r.RemoteAddr, username)

	go s.writeLoop(client)
	s.readLoop(client)
}

func (s *Server) RegisterHTTPHandler(pattern string, handler http.HandlerFunc) {
	if strings.TrimSpace(pattern) == "" || handler == nil {
		return
	}
	s.extraHandlers = append(s.extraHandlers, httpHandlerRegistration{
		pattern: pattern,
		handler: handler,
	})
}

func (s *Server) SnapshotClients() []ClientSnapshot {
	clients := s.snapshotClients()
	out := make([]ClientSnapshot, 0, len(clients))
	for _, c := range clients {
		out = append(out, c.Snapshot())
	}
	return out
}

func (s *Server) readLoop(client *Client) {
	defer s.disconnectClient(client)

	extendReadDeadline := func() {
		if s.cfg.Server.PongTimeout <= 0 {
			return
		}
		_ = client.conn.SetReadDeadline(time.Now().Add(s.cfg.Server.PongTimeout))
	}
	extendReadDeadline()
	client.conn.SetPingHandler(func(appData string) error {
		extendReadDeadline()
		return client.conn.WriteControl(
			websocket.PongMessage,
			[]byte(appData),
			time.Now().Add(s.cfg.Server.WriteTimeout),
		)
	})
	client.conn.SetPongHandler(func(string) error {
		extendReadDeadline()
		return nil
	})

	for {
		msgType, payload, err := client.conn.ReadMessage()
		if err != nil {
			if closeErr, ok := err.(*websocket.CloseError); ok {
				s.logger.Printf(
					"client=%s read closed code=%d text=%q",
					client.ID,
					closeErr.Code,
					closeErr.Text,
				)
			} else {
				s.logger.Printf("client=%s read error: %v", client.ID, err)
			}
			return
		}
		extendReadDeadline()
		if msgType != websocket.BinaryMessage {
			continue
		}
		msg, err := ParseMessage(payload)
		if err != nil {
			s.logger.Printf("client=%s invalid brew packet: %v", client.ID, err)
			continue
		}
		if sub, ok := msg.(*SubscriberMessage); ok {
			client.ApplySubscriber(sub)
		}
		if s.handler != nil {
			s.handler.OnMessage(client, msg)
		}
	}
}

func (s *Server) writeLoop(client *Client) {
	ticker := time.NewTicker(s.cfg.Server.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case payload, ok := <-client.send:
			if !ok {
				_ = client.conn.WriteControl(websocket.CloseMessage, []byte{}, time.Now().Add(s.cfg.Server.WriteTimeout))
				return
			}
			_ = client.conn.SetWriteDeadline(time.Now().Add(s.cfg.Server.WriteTimeout))
			if err := client.conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
				s.logger.Printf("client=%s write error: %v", client.ID, err)
				return
			}
			_ = client.conn.SetWriteDeadline(time.Time{})
		case <-ticker.C:
			if err := client.conn.WriteControl(websocket.PingMessage, []byte("hb"), time.Now().Add(s.cfg.Server.WriteTimeout)); err != nil {
				s.logger.Printf("client=%s ping error: %v", client.ID, err)
				return
			}
		}
	}
}

func (s *Server) disconnectClient(client *Client) {
	s.unregisterClient(client.ID)
	_ = client.Close()
	s.logger.Printf("client disconnected id=%s", client.ID)
	if s.handler != nil {
		s.handler.OnDisconnect(client)
	}
}

func (s *Server) registerClient(c *Client) {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	s.clients[c.ID] = c
}

func (s *Server) unregisterClient(id string) {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	delete(s.clients, id)
}

func (s *Server) snapshotClients() []*Client {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()
	out := make([]*Client, 0, len(s.clients))
	for _, c := range s.clients {
		out = append(out, c)
	}
	return out
}

// SetAuthVerifier sets a custom auth verifier function.
func (s *Server) SetAuthVerifier(v AuthVerifier) {
	s.authVerifier = v
}

// SetAuthRateLimiter sets callbacks for rate limiting auth attempts.
func (s *Server) SetAuthRateLimiter(checker func(string) bool, onFail func(string), onSuccess func(string)) {
	s.authChecker = checker
	s.authOnFailure = onFail
	s.authOnSuccess = onSuccess
}

func (s *Server) authEnabled() bool {
	return s.authVerifier != nil || strings.TrimSpace(s.cfg.Server.Username) != "" || strings.TrimSpace(s.cfg.Server.Password) != ""
}

// authorizeRequest validates the digest Authorization header. On success it
// returns the authenticated username so callers can stamp it on the issued
// token (and downstream onto Client.Username for station joins).
func (s *Server) authorizeRequest(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(strings.ToLower(header), "digest ") {
		return "", false
	}
	params := parseDigestHeader(header)
	username := params["username"]
	realm := params["realm"]
	if realm != s.cfg.Server.Realm {
		return "", false
	}

	nonce := params["nonce"]
	if nonce == "" || !s.nonceValid(nonce) {
		return "", false
	}

	uri := params["uri"]
	response := params["response"]
	qop := params["qop"]
	nc := params["nc"]
	cnonce := params["cnonce"]

	// If custom auth verifier is set, try all possible passwords
	if s.authVerifier != nil {
		// We need to find the password that matches the digest.
		// The verifier tells us if the username is valid.
		// For RadioID auth, we use a shared key as password.
		// Try the shared key from config first, then the static password.
		passwords := []string{}
		if s.cfg.Server.Password != "" {
			passwords = append(passwords, s.cfg.Server.Password)
		}
		if username == s.cfg.Server.Username && s.cfg.Server.Password != "" {
			// Static user always uses static password
			ha1 := md5Hex(fmt.Sprintf("%s:%s:%s", username, s.cfg.Server.Realm, s.cfg.Server.Password))
			ha2 := md5Hex(fmt.Sprintf("%s:%s", r.Method, uri))
			var expected string
			if qop != "" {
				expected = md5Hex(fmt.Sprintf("%s:%s:%s:%s:%s:%s", ha1, nonce, nc, cnonce, qop, ha2))
			} else {
				expected = md5Hex(fmt.Sprintf("%s:%s:%s", ha1, nonce, ha2))
			}
			if subtle.ConstantTimeCompare([]byte(strings.ToLower(response)), []byte(strings.ToLower(expected))) == 1 {
				return username, true
			}
		}

		// For dynamic users, verify the digest with each known password
		for _, pw := range passwords {
			ha1 := md5Hex(fmt.Sprintf("%s:%s:%s", username, s.cfg.Server.Realm, pw))
			ha2 := md5Hex(fmt.Sprintf("%s:%s", r.Method, uri))
			var expected string
			if qop != "" {
				expected = md5Hex(fmt.Sprintf("%s:%s:%s:%s:%s:%s", ha1, nonce, nc, cnonce, qop, ha2))
			} else {
				expected = md5Hex(fmt.Sprintf("%s:%s:%s", ha1, nonce, ha2))
			}
			if subtle.ConstantTimeCompare([]byte(strings.ToLower(response)), []byte(strings.ToLower(expected))) == 1 {
				// Digest matches — now verify username with RadioID
				if s.authVerifier(username, pw) {
					return username, true
				}
				return "", false
			}
		}
		return "", false
	}

	// Static auth: single username/password from config
	if username != s.cfg.Server.Username {
		return "", false
	}

	ha1 := md5Hex(fmt.Sprintf("%s:%s:%s", s.cfg.Server.Username, s.cfg.Server.Realm, s.cfg.Server.Password))
	ha2 := md5Hex(fmt.Sprintf("%s:%s", r.Method, uri))

	var expected string
	if qop != "" {
		expected = md5Hex(fmt.Sprintf("%s:%s:%s:%s:%s:%s", ha1, nonce, nc, cnonce, qop, ha2))
	} else {
		expected = md5Hex(fmt.Sprintf("%s:%s:%s", ha1, nonce, ha2))
	}

	if subtle.ConstantTimeCompare([]byte(strings.ToLower(response)), []byte(strings.ToLower(expected))) == 1 {
		return username, true
	}
	return "", false
}

func (s *Server) issueDigestChallenge(w http.ResponseWriter) {
	nonce := randHex(16)
	opaque := randHex(8)

	expiresAt := time.Now().Add(s.cfg.Server.TokenTTL)
	s.nonceMu.Lock()
	s.nonces[nonce] = expiresAt
	s.nonceMu.Unlock()

	header := fmt.Sprintf(
		`Digest realm="%s", qop="auth", nonce="%s", opaque="%s"`,
		s.cfg.Server.Realm,
		nonce,
		opaque,
	)
	w.Header().Set("WWW-Authenticate", header)
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte("unauthorized"))
}

func (s *Server) nonceValid(nonce string) bool {
	s.nonceMu.Lock()
	defer s.nonceMu.Unlock()
	exp, ok := s.nonces[nonce]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(s.nonces, nonce)
		return false
	}
	return true
}

func (s *Server) issueToken(username string) string {
	token := randHex(16)
	s.tokenMu.Lock()
	s.tokens[token] = tokenEntry{
		exp:      time.Now().Add(s.cfg.Server.TokenTTL),
		username: username,
	}
	s.tokenMu.Unlock()
	return token
}

// consumeToken returns the digest username captured at /discovery and ok=true
// when the token is valid and unexpired.
func (s *Server) consumeToken(token string) (string, bool) {
	s.tokenMu.Lock()
	defer s.tokenMu.Unlock()
	entry, ok := s.tokens[token]
	if !ok {
		return "", false
	}
	delete(s.tokens, token)
	if time.Now().After(entry.exp) {
		return "", false
	}
	return entry.username, true
}

func parseDigestHeader(header string) map[string]string {
	header = strings.TrimSpace(header)
	header = strings.TrimPrefix(header, "Digest ")
	header = strings.TrimPrefix(header, "digest ")

	params := make(map[string]string)
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
		params[key] = val
	}
	return params
}

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func randHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}
