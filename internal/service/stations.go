package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Station is a user-declared FreeTetra station (hotspot / tmo_site / bluestation).
// Pushed from the Pi-side freetetra-agent via POST /api/stations/push.
type Station struct {
	StationID     string   `json:"station_id"`
	Callsign      string   `json:"callsign"`
	Type          string   `json:"type"` // hotspot | tmo_site | bluestation
	Lat           float64  `json:"lat"`
	Lon           float64  `json:"lon"`
	DLFreqMHz     float64  `json:"dl_freq"`
	ULFreqMHz     float64  `json:"ul_freq"`
	PowerW        float64  `json:"power_w"`
	Antenna       string   `json:"antenna"`
	Notes         string   `json:"notes"`
	Website       string   `json:"website"`
	LastSeenUnix  int64    `json:"last_seen"`
	FirstSeenUnix int64    `json:"first_seen"`
	// OwnedISSIs is the set of TETRA subscriber IDs that TX from this station.
	// Indexed for reverse lookup (ByISSI). Pushed by the agent; not auto-discovered.
	OwnedISSIs []uint32 `json:"owned_issis,omitempty"`
	// Origin is the federation peer name that originated this record.
	// Empty for locally-pushed stations; filled in on receive from ctrl.Origin.
	Origin string `json:"origin,omitempty"`
	// DeletedUnix is the soft-delete timestamp. 0 = live. A tombstoned station
	// is kept locally so it can converge across federation, then reaped after
	// staleAfter.
	DeletedUnix int64 `json:"deleted,omitempty"`
}

// Online returns whether the station pushed within `window`. Tombstoned
// stations are never online.
func (st Station) Online(window time.Duration) bool {
	if st.DeletedUnix > 0 {
		return false
	}
	return time.Since(time.Unix(st.LastSeenUnix, 0)) < window
}

const (
	stationsPath          = "data/stations.json"
	maxValidISSI   uint32 = 0x00FFFFFF // TETRA SSI is 24 bits
)

type stationStore struct {
	mu           sync.RWMutex
	items        map[string]*Station
	byISSI       map[uint32]string // ISSI -> StationID
	logger       *log.Logger
	onlineWindow time.Duration
	staleAfter   time.Duration
	reapInterval time.Duration
}

type stationStoreConfig struct {
	OnlineWindow time.Duration
	StaleAfter   time.Duration
	ReapInterval time.Duration
}

func newStationStore(logger *log.Logger, cfg stationStoreConfig) *stationStore {
	if cfg.OnlineWindow <= 0 {
		cfg.OnlineWindow = 15 * time.Minute
	}
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = 90 * 24 * time.Hour
	}
	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = time.Hour
	}
	s := &stationStore{
		items:        make(map[string]*Station),
		byISSI:       make(map[uint32]string),
		logger:       logger,
		onlineWindow: cfg.OnlineWindow,
		staleAfter:   cfg.StaleAfter,
		reapInterval: cfg.ReapInterval,
	}
	s.load()
	return s
}

func (s *stationStore) OnlineWindow() time.Duration { return s.onlineWindow }

func (s *stationStore) load() {
	data, err := os.ReadFile(stationsPath)
	if err != nil {
		return
	}
	var list []*Station
	if err := json.Unmarshal(data, &list); err != nil {
		s.logger.Printf("stations: load: %v", err)
		return
	}
	migrated := 0
	for _, st := range list {
		if st.StationID == "" {
			continue
		}
		if st.Type == "repeater" {
			st.Type = "tmo_site"
			migrated++
		}
		s.items[st.StationID] = st
		s.indexISSIsLocked(st)
	}
	s.logger.Printf("stations: loaded %d", len(s.items))
	if migrated > 0 {
		s.logger.Printf("stations: migrated %d entries from type=repeater to type=tmo_site", migrated)
		s.save()
	}
}

func (s *stationStore) save() {
	if err := os.MkdirAll(filepath.Dir(stationsPath), 0755); err != nil {
		s.logger.Printf("stations: mkdir: %v", err)
		return
	}
	list := make([]*Station, 0, len(s.items))
	for _, st := range s.items {
		list = append(list, st)
	}
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return
	}
	tmp := stationsPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		s.logger.Printf("stations: write: %v", err)
		return
	}
	_ = os.Rename(tmp, stationsPath)
}

// indexISSIsLocked points byISSI at st.StationID for every ISSI it owns.
// On conflict, the more-recent LastSeenUnix wins the slot; the loser is logged
// but left untouched in items (so its OwnedISSIs are preserved for inspection).
// Caller must hold s.mu (write).
func (s *stationStore) indexISSIsLocked(st *Station) {
	if st == nil || st.DeletedUnix > 0 {
		return
	}
	for _, issi := range st.OwnedISSIs {
		if issi == 0 {
			continue
		}
		holder, ok := s.byISSI[issi]
		if !ok || holder == st.StationID {
			s.byISSI[issi] = st.StationID
			continue
		}
		existing := s.items[holder]
		if existing == nil || st.LastSeenUnix >= existing.LastSeenUnix {
			s.logger.Printf("stations: ISSI %d reassigned from %s (%s) to %s (%s); last_seen wins",
				issi, holder, callsignOf(existing), st.StationID, st.Callsign)
			s.byISSI[issi] = st.StationID
		} else {
			s.logger.Printf("stations: ISSI %d claim by %s (%s) ignored; %s (%s) has newer last_seen",
				issi, st.StationID, st.Callsign, holder, callsignOf(existing))
		}
	}
}

// unindexISSIsLocked removes byISSI entries that pointed to st.StationID.
// Caller must hold s.mu (write).
func (s *stationStore) unindexISSIsLocked(stationID string) {
	for issi, holder := range s.byISSI {
		if holder == stationID {
			delete(s.byISSI, issi)
		}
	}
}

func callsignOf(st *Station) string {
	if st == nil {
		return "?"
	}
	return st.Callsign
}

// Upsert creates or updates a station keyed by StationID. Returns the stored copy.
func (s *stationStore) Upsert(in Station) (*Station, error) {
	if strings.TrimSpace(in.StationID) == "" {
		return nil, fmt.Errorf("station_id required")
	}
	if strings.TrimSpace(in.Callsign) == "" {
		return nil, fmt.Errorf("callsign required")
	}
	if in.Lat < -90 || in.Lat > 90 || in.Lon < -180 || in.Lon > 180 {
		return nil, fmt.Errorf("invalid coordinates")
	}
	t := strings.ToLower(strings.TrimSpace(in.Type))
	// Accept legacy "repeater" on the way in and migrate to "tmo_site" so a
	// peer running an older version doesn't get its station updates rejected.
	if t == "repeater" {
		t = "tmo_site"
	}
	if t != "hotspot" && t != "tmo_site" && t != "bluestation" {
		return nil, fmt.Errorf("type must be hotspot, tmo_site, or bluestation")
	}
	in.Type = t
	in.Callsign = strings.ToUpper(strings.TrimSpace(in.Callsign))
	for _, issi := range in.OwnedISSIs {
		if issi == 0 || issi > maxValidISSI {
			return nil, fmt.Errorf("invalid owned_issi %d (must be 1..%d)", issi, maxValidISSI)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().Unix()
	existing, ok := s.items[in.StationID]
	if ok {
		in.FirstSeenUnix = existing.FirstSeenUnix
	} else {
		in.FirstSeenUnix = now
	}
	// LastSeenUnix nur ueberschreiben wenn nicht im Input gesetzt (= lokal-push).
	// Bei Federation-Empfang behalten wir den Origin-Timestamp damit eine
	// laengst offline Station nicht jedes Mal "wieder online" wird wenn ein
	// Peer ihren alten Eintrag periodisch syncht.
	if in.LastSeenUnix <= 0 || in.LastSeenUnix > now {
		in.LastSeenUnix = now
	} else if existing != nil && existing.LastSeenUnix > in.LastSeenUnix {
		// Lokaler Stand ist neuer als der gefederierte — behalten.
		in.LastSeenUnix = existing.LastSeenUnix
	}
	if existing != nil && existing.Callsign != in.Callsign {
		s.logger.Printf("stations: WARN callsign change for %s: %s -> %s (origin %q -> %q)",
			in.StationID, existing.Callsign, in.Callsign, existing.Origin, in.Origin)
	}
	for sid, other := range s.items {
		if sid == in.StationID {
			continue
		}
		if other.Callsign == in.Callsign && other.Origin != in.Origin {
			s.logger.Printf("stations: WARN callsign %s claimed by two origins: %s@%q and %s@%q",
				in.Callsign, sid, other.Origin, in.StationID, in.Origin)
		}
	}
	if existing != nil {
		s.unindexISSIsLocked(in.StationID)
	}
	s.items[in.StationID] = &in
	s.indexISSIsLocked(&in)
	s.save()
	return &in, nil
}

// Delete soft-deletes a station: sets DeletedUnix=now, drops it from byISSI,
// keeps the row so federation peers converge. Returns the tombstone for
// federation broadcast.
func (s *stationStore) Delete(stationID string) (*Station, error) {
	stationID = strings.TrimSpace(stationID)
	if stationID == "" {
		return nil, fmt.Errorf("station_id required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.items[stationID]
	if !ok {
		return nil, fmt.Errorf("station %s not found", stationID)
	}
	if st.DeletedUnix == 0 {
		st.DeletedUnix = time.Now().Unix()
	}
	s.unindexISSIsLocked(stationID)
	s.save()
	out := *st
	return &out, nil
}

// Reap drops tombstones older than staleAfter from the local map. Returns the
// number of rows removed.
func (s *stationStore) Reap(now time.Time) int {
	cutoff := now.Add(-s.staleAfter).Unix()
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for sid, st := range s.items {
		if st.DeletedUnix > 0 && st.DeletedUnix < cutoff {
			delete(s.items, sid)
			removed++
		}
	}
	if removed > 0 {
		s.save()
		s.logger.Printf("stations: reaped %d tombstoned rows", removed)
	}
	return removed
}

// ReapLoop runs Reap on a ticker until ctx is cancelled.
func (s *stationStore) ReapLoop(ctx context.Context) {
	t := time.NewTicker(s.reapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			s.Reap(now)
		}
	}
}

// All returns live (non-tombstoned) stations, sorted by callsign.
func (s *stationStore) All() []Station {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Station, 0, len(s.items))
	for _, st := range s.items {
		if st.DeletedUnix > 0 {
			continue
		}
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Callsign < out[j].Callsign })
	return out
}

// AllIncludingDeleted returns every row including tombstones, for anti-entropy
// federation sync so deletions converge.
func (s *stationStore) AllIncludingDeleted() []Station {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Station, 0, len(s.items))
	for _, st := range s.items {
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Callsign < out[j].Callsign })
	return out
}

// ByISSI returns the station that owns this ISSI, if any. Live rows only.
func (s *stationStore) ByISSI(issi uint32) (Station, bool) {
	if issi == 0 {
		return Station{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	sid, ok := s.byISSI[issi]
	if !ok {
		return Station{}, false
	}
	st, ok := s.items[sid]
	if !ok || st.DeletedUnix > 0 {
		return Station{}, false
	}
	return *st, true
}

// ByCallsign returns the live station with the matching callsign. Comparison
// is case-insensitive; Upsert already uppercases stored callsigns so the
// equality check is effectively cheap. If two stations share a callsign the
// most-recently-updated one wins (stable iteration order is not guaranteed).
func (s *stationStore) ByCallsign(cs string) (Station, bool) {
	cs = strings.ToUpper(strings.TrimSpace(cs))
	if cs == "" {
		return Station{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var best *Station
	for _, st := range s.items {
		if st.DeletedUnix > 0 || st.Callsign != cs {
			continue
		}
		if best == nil || st.LastSeenUnix > best.LastSeenUnix {
			best = st
		}
	}
	if best == nil {
		return Station{}, false
	}
	return *best, true
}

// LinkOrCreate associates a live identity (issi + optional callsign) with a
// Station. Resolution order:
//  1. ByISSI(issi) — exact reverse-index hit.
//  2. ByCallsign(callsign) — extend that station's OwnedISSIs with this issi.
//  3. autoCreate=true — Upsert a fresh stub Station of the given type.
//  4. otherwise — return false; caller decides what to do.
//
// The returned Station is the final live row (post-Upsert if a write happened).
// All write paths go through Upsert, which federates and persists.
func (s *stationStore) LinkOrCreate(issi uint32, callsign, stationType string, autoCreate bool) (Station, bool) {
	if issi == 0 {
		return Station{}, false
	}
	if st, ok := s.ByISSI(issi); ok {
		return st, true
	}
	if st, ok := s.ByCallsign(callsign); ok {
		extended := append([]uint32(nil), st.OwnedISSIs...)
		extended = append(extended, issi)
		st.OwnedISSIs = extended
		saved, err := s.Upsert(st)
		if err != nil {
			s.logger.Printf("stations: LinkOrCreate extend %s with ISSI %d failed: %v", st.StationID, issi, err)
			return Station{}, false
		}
		return *saved, true
	}
	if !autoCreate {
		return Station{}, false
	}
	cs := strings.ToUpper(strings.TrimSpace(callsign))
	if cs == "" {
		cs = strconv.FormatUint(uint64(issi), 10)
	}
	stub := Station{
		StationID:  uuid.NewString(),
		Callsign:   cs,
		Type:       stationType,
		OwnedISSIs: []uint32{issi},
	}
	saved, err := s.Upsert(stub)
	if err != nil {
		s.logger.Printf("stations: LinkOrCreate auto-create for ISSI %d failed: %v", issi, err)
		return Station{}, false
	}
	s.logger.Printf("stations: auto-created %s (callsign=%s, issi=%d, type=%s)",
		saved.StationID, saved.Callsign, issi, stationType)
	return *saved, true
}

// bumpLastSeen marks the station as live now without going through the full
// Upsert validation+federation path. Used by the high-frequency liveness
// updaters (telemetry MS events, TMO heartbeats) that don't change static
// fields. Tombstoned rows are not revived. Persisted on a best-effort schedule
// — we save inline since the bump is rare relative to disk IO budget.
func (s *stationStore) bumpLastSeen(stationID string) {
	if stationID == "" {
		return
	}
	now := time.Now().Unix()
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.items[stationID]
	if !ok || st.DeletedUnix > 0 {
		return
	}
	st.LastSeenUnix = now
	s.save()
}

// --- HTTP handlers ---

func (s *Service) handleStationPush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if s.stationStore == nil {
		http.Error(w, "stations disabled", http.StatusServiceUnavailable)
		return
	}
	var in Station
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	// Locally-pushed stations clear Origin; the receiver fills it from ctrl.Origin
	// during federation. This way an admin can't spoof a federated origin via push.
	in.Origin = ""
	in.DeletedUnix = 0
	saved, err := s.stationStore.Upsert(in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Federation-Sync: Station an alle Peers melden, damit jeder Server die
	// gleiche Station-Liste hat (Welt-Map statt pro-Server-fragmentiert).
	if s.federation != nil {
		s.federation.NotifyStationUpdate(saved)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "station": saved})
}

func (s *Service) handleStationDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "DELETE required", http.StatusMethodNotAllowed)
		return
	}
	if s.stationStore == nil {
		http.Error(w, "stations disabled", http.StatusServiceUnavailable)
		return
	}
	sid := strings.TrimPrefix(r.URL.Path, "/api/stations/")
	sid = strings.TrimSuffix(sid, "/")
	if sid == "" || strings.Contains(sid, "/") {
		http.Error(w, "station_id required in path", http.StatusBadRequest)
		return
	}
	tombstone, err := s.stationStore.Delete(sid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if s.federation != nil {
		s.federation.NotifyStationUpdate(tombstone)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "station": tombstone})
}

func (s *Service) handleStationsList(w http.ResponseWriter, r *http.Request) {
	type out struct {
		Station
		Online              bool     `json:"online"`
		LiveBrewSessionID   string   `json:"live_brew_session_id,omitempty"`
		LiveBrewSubscribers []uint32 `json:"live_brew_subscribers,omitempty"`
		LiveTelemetry       bool     `json:"live_telemetry,omitempty"`
		LiveTelemetrySubs   []uint32 `json:"live_telemetry_subscribers,omitempty"`
	}
	var list []out
	if s.stationStore != nil {
		window := s.stationStore.OnlineWindow()
		// Build per-ISSI lookup tables for the live join. We index brew
		// clients by their authenticated Username (parsed as ISSI) and
		// telemetry clients by their StationID — telemetry already resolved
		// itself on connect via LinkOrCreate.
		type brewLive struct {
			sessionID string
			subs      []uint32
		}
		brewByISSI := map[uint32]brewLive{}
		for _, c := range s.server.SnapshotClients() {
			if c.Username == "" {
				continue
			}
			issi64, err := strconv.ParseUint(strings.TrimSpace(c.Username), 10, 32)
			if err != nil || issi64 == 0 {
				continue
			}
			subs := make([]uint32, 0, len(c.Subscribers))
			for _, sub := range c.Subscribers {
				subs = append(subs, sub.Number)
			}
			brewByISSI[uint32(issi64)] = brewLive{sessionID: c.ID, subs: subs}
		}
		telemetryByStation := map[string][]uint32{}
		if s.telemetry != nil {
			for _, t := range s.telemetry.SnapshotForJoin() {
				if t.StationID == "" {
					continue
				}
				telemetryByStation[t.StationID] = append(telemetryByStation[t.StationID], t.Subscribers...)
			}
		}

		for _, st := range s.stationStore.All() {
			row := out{Station: st, Online: st.Online(window)}
			for _, issi := range st.OwnedISSIs {
				if bl, ok := brewByISSI[issi]; ok {
					row.LiveBrewSessionID = bl.sessionID
					row.LiveBrewSubscribers = bl.subs
					break
				}
			}
			if subs, ok := telemetryByStation[st.StationID]; ok && len(subs) > 0 {
				row.LiveTelemetry = true
				row.LiveTelemetrySubs = subs
			}
			list = append(list, row)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	_ = json.NewEncoder(w).Encode(map[string]any{"stations": list, "count": len(list)})
}

func (s *Service) registerStationHandlers() {
	s.server.RegisterHTTPHandler("/api/stations/push", s.handleStationPush)
	s.server.RegisterHTTPHandler("/api/stations", s.handleStationsList)
	// DELETE /api/stations/{id} — soft-delete + federate tombstone.
	s.server.RegisterHTTPHandler("/api/stations/", s.handleStationDelete)
}
