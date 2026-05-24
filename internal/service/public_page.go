package service

import (
	"encoding/json"
	"net/http"
	"time"
)

// registerPublicHandlers adds HTTP handlers for the public landing page.
// The landing HTML itself is served by the Vue SPA via registerSPAFallback
// at "/"; this function now only registers the JSON API.
func (s *Service) registerPublicHandlers() {
	s.server.RegisterHTTPHandler("/api/public/status", s.handlePublicStatus)
}

func (s *Service) handlePublicStatus(w http.ResponseWriter, r *http.Request) {
	clients := s.server.SnapshotClients()

	// TMO-site count comes from the unified station registry (any
	// non-tombstoned station counted as "online" within STATION_ONLINE_WINDOW).
	// Subscriber count still comes from the live telemetry/heartbeat sources
	// since the station registry intentionally doesn't hold live MS counts.
	tmoSiteCount := 0
	subscriberCount := 0
	if s.stationStore != nil {
		window := s.stationStore.OnlineWindow()
		for _, st := range s.stationStore.All() {
			if st.Online(window) {
				tmoSiteCount++
			}
		}
	}
	if s.telemetry != nil && s.telemetry.ActiveCount() > 0 {
		if tmoSiteCount == 0 {
			tmoSiteCount = s.telemetry.ActiveCount()
		}
		subscriberCount = s.telemetry.TotalSubscribers()
	} else if s.tmoSites != nil {
		if tmoSiteCount == 0 {
			tmoSiteCount = s.tmoSites.ActiveCount()
		}
		subscriberCount = s.tmoSites.TotalSubscribers()
	}
	_ = clients

	positions := s.positionStore.Latest()

	serverName := "FreeTetra"
	if s.cfg.Federation.Name != "" {
		serverName = "FreeTetra " + s.cfg.Federation.Name
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "public, max-age=5")
	json.NewEncoder(w).Encode(map[string]any{
		"server":      serverName,
		"version":     "1.0",
		"uptime":      time.Since(startTime).String(),
		"tmo_sites":   tmoSiteCount,
		"subscribers": subscriberCount,
		"positions":   len(positions),
	})
}

var startTime = time.Now()

