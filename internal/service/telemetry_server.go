package service

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// TelemetryEvent is the event structure sent by BlueStation's net_telemetry module.
// See: tetra-bluestation/crates/tetra-entities/src/net_telemetry/events.rs
type TelemetryEvent struct {
	MsRegistration *struct {
		ISSI uint32 `json:"issi"`
	} `json:"MsRegistration,omitempty"`
	MsDeregistration *struct {
		ISSI uint32 `json:"issi"`
	} `json:"MsDeregistration,omitempty"`
	MsGroupAttach *struct {
		ISSI  uint32   `json:"issi"`
		GSSIs []uint32 `json:"gssis"`
	} `json:"MsGroupAttach,omitempty"`
	MsGroupDetach *struct {
		ISSI  uint32   `json:"issi"`
		GSSIs []uint32 `json:"gssis"`
	} `json:"MsGroupDetach,omitempty"`
}

// TelemetryClient represents a connected BlueStation telemetry feed.
type TelemetryClient struct {
	Name     string
	RemoteIP string
	// ISSI is the BlueStation's identity captured from the basic-auth user at
	// connect time. Zero when auth was skipped. Used to join this telemetry
	// feed to a Station via the station registry's ByISSI reverse-index.
	ISSI uint32
	// StationID is the resolved station this feed belongs to (set by
	// LinkOrCreate on connect; empty if no match and auto-create was off).
	StationID    string
	Conn         *websocket.Conn
	ConnectedAt  time.Time
	LastActivity time.Time

	mu          sync.RWMutex
	subscribers map[uint32]*subscriberState // ISSI -> state
}

type subscriberState struct {
	ISSI     uint32
	GSSIs    map[uint32]bool
	LastSeen time.Time
}

// TelemetryServer accepts WebSocket connections from BlueStation telemetry modules.
type TelemetryServer struct {
	logger *log.Logger
	svc    *Service

	mu      sync.RWMutex
	clients map[string]*TelemetryClient // remote addr -> client

	upgrader websocket.Upgrader
}

const telemetryProtocol = "bluestation-telemetry-v1"

func newTelemetryServer(logger *log.Logger, svc *Service) *TelemetryServer {
	return &TelemetryServer{
		logger:  logger,
		svc:     svc,
		clients: make(map[string]*TelemetryClient),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin:     func(r *http.Request) bool { return true },
			Subprotocols:    []string{telemetryProtocol},
		},
	}
}

func (ts *TelemetryServer) handleConnection(w http.ResponseWriter, r *http.Request) {
	// Optional HTTP Basic Auth. We also capture the auth-time ISSI here so
	// the resulting TelemetryClient can be joined to a Station record.
	clientName := r.URL.Query().Get("name")
	var authISSI uint32
	var authCallsign string
	if user, pass, ok := r.BasicAuth(); ok {
		fmt.Sscanf(user, "%d", &authISSI)
		// Verify with RadioID if enabled
		if ts.svc.radioIDAuth != nil {
			if authISSI != 0 {
				callsign, allowed := ts.svc.radioIDAuth.Verify(authISSI)
				if !allowed {
					ts.logger.Printf("telemetry: rejected %s (RadioID verification failed)", user)
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				authCallsign = callsign
				if clientName == "" {
					clientName = callsign
				}
			}
		}
		_ = pass // shared key, trusted via RadioID
	}

	if clientName == "" {
		clientName = r.RemoteAddr
	}

	conn, err := ts.upgrader.Upgrade(w, r, nil)
	if err != nil {
		ts.logger.Printf("telemetry: upgrade failed: %v", err)
		return
	}

	ip := r.Header.Get("X-Real-IP")
	if ip == "" {
		ip = strings.Split(r.RemoteAddr, ":")[0]
	}

	client := &TelemetryClient{
		Name:         clientName,
		RemoteIP:     ip,
		ISSI:         authISSI,
		Conn:         conn,
		ConnectedAt:  time.Now(),
		LastActivity: time.Now(),
		subscribers:  make(map[uint32]*subscriberState),
	}

	ts.mu.Lock()
	ts.clients[clientName] = client
	ts.mu.Unlock()

	// Join this telemetry feed to a Station via the registry. ByISSI is the
	// strong match; callsign fallback covers the case where the operator
	// already created a station record by callsign but never declared the
	// ISSI. Auto-create is gated on STATION_AUTO_CREATE.
	if ts.svc.stationStore != nil && authISSI != 0 {
		if st, ok := ts.svc.stationStore.LinkOrCreate(authISSI, authCallsign, "bluestation", ts.svc.cfg.Station.AutoCreate); ok {
			client.StationID = st.StationID
		}
	}

	ts.logger.Printf("telemetry: client connected name=%s ip=%s station=%s", clientName, ip, client.StationID)

	defer func() {
		ts.mu.Lock()
		delete(ts.clients, clientName)
		ts.mu.Unlock()
		conn.Close()
		ts.logger.Printf("telemetry: client disconnected name=%s", clientName)
	}()

	// Read loop
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		client.mu.Lock()
		client.LastActivity = time.Now()
		client.mu.Unlock()
		return nil
	})

	// Keep-alive: send ping every 10s
	done := make(chan struct{})
	defer close(done)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		client.mu.Lock()
		client.LastActivity = time.Now()
		client.mu.Unlock()

		var event TelemetryEvent
		if err := json.Unmarshal(data, &event); err != nil {
			ts.logger.Printf("telemetry: invalid JSON from %s: %v", clientName, err)
			continue
		}

		ts.handleEvent(client, &event)
	}
}

func (ts *TelemetryServer) handleEvent(client *TelemetryClient, event *TelemetryEvent) {
	client.mu.Lock()
	defer client.mu.Unlock()

	bumpStation := false

	switch {
	case event.MsRegistration != nil:
		issi := event.MsRegistration.ISSI
		if _, ok := client.subscribers[issi]; !ok {
			client.subscribers[issi] = &subscriberState{
				ISSI:  issi,
				GSSIs: make(map[uint32]bool),
			}
		}
		client.subscribers[issi].LastSeen = time.Now()
		ts.logger.Printf("telemetry: %s REGISTER ISSI=%d", client.Name, issi)
		bumpStation = true

	case event.MsDeregistration != nil:
		issi := event.MsDeregistration.ISSI
		delete(client.subscribers, issi)
		ts.logger.Printf("telemetry: %s DEREGISTER ISSI=%d", client.Name, issi)

	case event.MsGroupAttach != nil:
		issi := event.MsGroupAttach.ISSI
		if _, ok := client.subscribers[issi]; !ok {
			client.subscribers[issi] = &subscriberState{
				ISSI:  issi,
				GSSIs: make(map[uint32]bool),
			}
		}
		for _, g := range event.MsGroupAttach.GSSIs {
			client.subscribers[issi].GSSIs[g] = true
		}
		client.subscribers[issi].LastSeen = time.Now()
		ts.logger.Printf("telemetry: %s ATTACH ISSI=%d GSSIs=%v", client.Name, issi, event.MsGroupAttach.GSSIs)
		bumpStation = true

	case event.MsGroupDetach != nil:
		issi := event.MsGroupDetach.ISSI
		if sub, ok := client.subscribers[issi]; ok {
			for _, g := range event.MsGroupDetach.GSSIs {
				delete(sub.GSSIs, g)
			}
		}
	}

	if bumpStation && client.StationID != "" && ts.svc.stationStore != nil {
		ts.svc.stationStore.bumpLastSeen(client.StationID)
	}
}

// ActiveCount returns the number of connected telemetry clients (TMO-sites).
func (ts *TelemetryServer) ActiveCount() int {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return len(ts.clients)
}

// TotalSubscribers returns unique ISSIs across all telemetry clients.
func (ts *TelemetryServer) TotalSubscribers() int {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	unique := make(map[uint32]bool)
	for _, c := range ts.clients {
		c.mu.RLock()
		for issi := range c.subscribers {
			unique[issi] = true
		}
		c.mu.RUnlock()
	}
	return len(unique)
}

// TelemetryJoinRow is a thin projection of a telemetry client for the
// /api/stations join: just the resolved StationID and the live MS ISSIs.
// Distinct from Snapshot (which is for the admin debug endpoint) because the
// caller needs typed access, not a free-form map.
type TelemetryJoinRow struct {
	StationID   string
	Subscribers []uint32
}

// SnapshotForJoin returns one row per connected telemetry client, with the
// resolved StationID (set by LinkOrCreate at connect) and the live MS ISSIs.
// Clients that never matched a station (auto-create disabled + no callsign
// hit) have an empty StationID and are skipped by the caller.
func (ts *TelemetryServer) SnapshotForJoin() []TelemetryJoinRow {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	out := make([]TelemetryJoinRow, 0, len(ts.clients))
	for _, c := range ts.clients {
		c.mu.RLock()
		issis := make([]uint32, 0, len(c.subscribers))
		for issi := range c.subscribers {
			issis = append(issis, issi)
		}
		out = append(out, TelemetryJoinRow{StationID: c.StationID, Subscribers: issis})
		c.mu.RUnlock()
	}
	return out
}

// Snapshot returns info about all connected clients.
func (ts *TelemetryServer) Snapshot() []map[string]any {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	out := make([]map[string]any, 0, len(ts.clients))
	for _, c := range ts.clients {
		c.mu.RLock()
		issis := make([]uint32, 0, len(c.subscribers))
		for issi := range c.subscribers {
			issis = append(issis, issi)
		}
		out = append(out, map[string]any{
			"name":             c.Name,
			"connected_at":     c.ConnectedAt,
			"last_activity":    c.LastActivity,
			"subscriber_count": len(c.subscribers),
			"subscribers":      issis,
		})
		c.mu.RUnlock()
	}
	return out
}

func (s *Service) registerTelemetryServer() {
	if s.telemetry == nil {
		s.telemetry = newTelemetryServer(s.logger, s)
	}
	s.server.RegisterHTTPHandler("/telemetry", s.telemetry.handleConnection)
	s.server.RegisterHTTPHandler("/api/telemetry/clients", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"clients": s.telemetry.Snapshot(),
			"count":   s.telemetry.ActiveCount(),
		})
	})
}
