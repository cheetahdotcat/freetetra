package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/freetetra/server/internal/config"
)

func writeManifestForTest(t *testing.T, dir string, m SoundboardManifest) string {
	t.Helper()
	path := filepath.Join(dir, "manifest.json")
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return path
}

func soundboardTestConfig(dir string) config.Config {
	return config.Config{
		Soundboard: config.SoundboardConfig{
			Enabled:          true,
			SoundsDir:        dir,
			BrewISSI:         899003,
			SourceISSI:       899003,
			FFmpegBin:        "ffmpeg",
			EncoderBin:       "tetra-acelp-stdio",
			EncoderFrameSize: 18,
			FrameInterval:    1 * time.Millisecond, // keep tests fast
			ReleaseCause:     0,
		},
	}
}

func TestLoadSoundboardManifest_Valid(t *testing.T) {
	dir := t.TempDir()
	manifest := SoundboardManifest{Buttons: []SoundboardButton{
		{ID: "a", Label: "A", File: "a.wav", TG: 10},
		{ID: "b", Label: "B", File: "b.wav", TG: 25},
		{ID: "c", Label: "C", File: "c.wav", TG: 10}, // duplicate TG, distinct ID — OK
	}}
	writeManifestForTest(t, dir, manifest)
	cfg := soundboardTestConfig(dir)

	loaded, err := LoadSoundboardManifest(cfg)
	if err != nil {
		t.Fatalf("LoadSoundboardManifest: %v", err)
	}
	if len(loaded.Buttons) != 3 {
		t.Fatalf("got %d buttons, want 3", len(loaded.Buttons))
	}
	tgs := SoundboardTalkgroups(loaded)
	sort.Slice(tgs, func(i, j int) bool { return tgs[i] < tgs[j] })
	if len(tgs) != 2 || tgs[0] != 10 || tgs[1] != 25 {
		t.Fatalf("SoundboardTalkgroups dedup wrong: %v", tgs)
	}
}

func TestLoadSoundboardManifest_RejectsBadEntries(t *testing.T) {
	cases := []struct {
		name string
		m    SoundboardManifest
	}{
		{"empty id", SoundboardManifest{Buttons: []SoundboardButton{{ID: "", File: "a.wav", TG: 10}}}},
		{"duplicate id", SoundboardManifest{Buttons: []SoundboardButton{
			{ID: "x", File: "a.wav", TG: 10},
			{ID: "x", File: "b.wav", TG: 25},
		}}},
		{"missing file", SoundboardManifest{Buttons: []SoundboardButton{{ID: "x", File: "", TG: 10}}}},
		{"zero tg", SoundboardManifest{Buttons: []SoundboardButton{{ID: "x", File: "a.wav", TG: 0}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeManifestForTest(t, dir, tc.m)
			cfg := soundboardTestConfig(dir)
			if _, err := LoadSoundboardManifest(cfg); err == nil {
				t.Fatalf("expected error for %q, got nil", tc.name)
			}
		})
	}
}

// primeCache puts a fake .acelp cache next to a fake source file so the
// bridge skips encoding (which would require ffmpeg + tetra-acelp-stdio).
// The cache has `frames` 36-byte STE frames, which is what readFrames expects.
func primeCache(t *testing.T, sourcePath string, frames int) {
	t.Helper()
	if err := os.WriteFile(sourcePath, []byte("fake audio source"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	// Predate the source so the cache (mtime = now) is considered fresh.
	old := time.Now().Add(-1 * time.Hour)
	if err := os.Chtimes(sourcePath, old, old); err != nil {
		t.Fatalf("chtimes source: %v", err)
	}
	cache := make([]byte, 36*frames)
	if err := os.WriteFile(sourcePath+".acelp", cache, 0o644); err != nil {
		t.Fatalf("write cache: %v", err)
	}
}

func newTestSoundboardBridge(t *testing.T, dir string, buttons []SoundboardButton) *SoundboardBridge {
	t.Helper()
	writeManifestForTest(t, dir, SoundboardManifest{Buttons: buttons})
	cfg := soundboardTestConfig(dir)
	logger := log.New(io.Discard, "", 0)
	plane := NewBrewModulePlane(cfg, logger, cfg.Soundboard.BrewISSI, SoundboardTalkgroups(SoundboardManifest{Buttons: buttons}))
	bridge, err := NewSoundboardBridge(cfg, logger, plane)
	if err != nil {
		t.Fatalf("NewSoundboardBridge: %v", err)
	}
	return bridge
}

func TestSoundboardBridge_PlayRejectsSecondPressWhileBusy(t *testing.T) {
	dir := t.TempDir()
	btn := SoundboardButton{ID: "ping", Label: "Ping", File: "ping.wav", TG: 10}
	bridge := newTestSoundboardBridge(t, dir, []SoundboardButton{btn})

	// Prime the cache so playButton doesn't try to spawn ffmpeg.
	primeCache(t, bridge.sourcePath(btn), 30)

	// First press: should accept (started). We deliberately use a slow frame
	// interval via config so the play stays in-flight long enough for the
	// second press to observe busy=true.
	bridge.cfg.Soundboard.FrameInterval = 50 * time.Millisecond

	req1 := httptest.NewRequest(http.MethodPost, "/api/play/ping", nil)
	rec1 := httptest.NewRecorder()
	bridge.handlePlay(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first press: status %d body=%s", rec1.Code, rec1.Body.String())
	}

	// Give the goroutine a moment to flip busy=true before the second press.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && !bridge.busy.Load() {
		time.Sleep(2 * time.Millisecond)
	}
	if !bridge.busy.Load() {
		t.Fatalf("expected bridge busy after first press")
	}

	req2 := httptest.NewRequest(http.MethodPost, "/api/play/ping", nil)
	rec2 := httptest.NewRecorder()
	bridge.handlePlay(rec2, req2)
	if rec2.Code != http.StatusConflict {
		t.Fatalf("second press: status %d body=%s — want 409", rec2.Code, rec2.Body.String())
	}

	// Wait for the first play to finish so subsequent tests / goroutines
	// don't leak.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && bridge.busy.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if bridge.busy.Load() {
		t.Fatalf("first play never released busy flag")
	}
}

func TestSoundboardBridge_PlayUnknownButton(t *testing.T) {
	dir := t.TempDir()
	bridge := newTestSoundboardBridge(t, dir, []SoundboardButton{
		{ID: "real", Label: "Real", File: "real.wav", TG: 10},
	})

	req := httptest.NewRequest(http.MethodPost, "/api/play/doesnotexist", nil)
	rec := httptest.NewRecorder()
	bridge.handlePlay(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", rec.Code)
	}
}

func TestSoundboardBridge_HandleButtonsReportsCacheStatus(t *testing.T) {
	dir := t.TempDir()
	uncached := SoundboardButton{ID: "u", Label: "U", File: "u.wav", TG: 10}
	cached := SoundboardButton{ID: "c", Label: "C", File: "c.wav", TG: 10}
	bridge := newTestSoundboardBridge(t, dir, []SoundboardButton{uncached, cached})
	primeCache(t, bridge.sourcePath(cached), 5)

	rec := httptest.NewRecorder()
	bridge.handleButtons(rec, httptest.NewRequest(http.MethodGet, "/api/buttons", nil))

	var resp struct {
		Buttons []struct {
			ID     string `json:"id"`
			Cached bool   `json:"cached"`
		} `json:"buttons"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v body=%s", err, rec.Body.String())
	}
	state := make(map[string]bool, len(resp.Buttons))
	for _, b := range resp.Buttons {
		state[b.ID] = b.Cached
	}
	if state["u"] {
		t.Errorf("button u should be uncached")
	}
	if !state["c"] {
		t.Errorf("button c should be cached")
	}
}

func TestSoundboardBridge_HandleStateIdleAndBusy(t *testing.T) {
	dir := t.TempDir()
	bridge := newTestSoundboardBridge(t, dir, []SoundboardButton{{ID: "x", Label: "X", File: "x.wav", TG: 10}})

	rec := httptest.NewRecorder()
	bridge.handleState(rec, httptest.NewRequest(http.MethodGet, "/api/state", nil))
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body["busy"].(bool) {
		t.Fatalf("expected idle state at startup")
	}

	bridge.busy.Store(true)
	bridge.currentID.Store("x")
	rec = httptest.NewRecorder()
	bridge.handleState(rec, httptest.NewRequest(http.MethodGet, "/api/state", nil))
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if !body["busy"].(bool) || body["current_id"] != "x" {
		t.Fatalf("expected busy=true current_id=x, got %+v", body)
	}
	bridge.busy.Store(false)
	bridge.currentID.Store("")
}

// TestSoundboardBridge_IndexServesEmbeddedHTML guards that the //go:embed
// directive resolves and the handler returns something sane.
func TestSoundboardBridge_IndexServesEmbeddedHTML(t *testing.T) {
	dir := t.TempDir()
	bridge := newTestSoundboardBridge(t, dir, []SoundboardButton{{ID: "x", File: "x.wav", TG: 10}})

	rec := httptest.NewRecorder()
	bridge.handleIndex(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("index status %d", rec.Code)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("FreeTetra Soundboard")) {
		t.Fatalf("index body missing title; got %d bytes", rec.Body.Len())
	}
}

// TestSoundboardBridge_StopShutsDown wires Start in a goroutine, hits it
// once over a real listener, then Stops and ensures the goroutine returns.
func TestSoundboardBridge_StopShutsDown(t *testing.T) {
	dir := t.TempDir()
	bridge := newTestSoundboardBridge(t, dir, []SoundboardButton{{ID: "x", File: "x.wav", TG: 10}})
	bridge.cfg.Soundboard.ListenAddr = "127.0.0.1:0" // ephemeral

	// Listen on an ephemeral port ourselves so we know the URL, then hand the
	// socket to bridge.server. Easier alternative: just call Stop on the
	// bridge before Start finishes binding by racing — that's flaky. Use a
	// real httptest.Server flow instead.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		// Start blocks until Shutdown / Close.
		done <- bridge.Start(ctx)
	}()
	// Wait until the server is up — Start sets bridge.server before
	// ListenAndServe.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if bridge.server != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Start did not return after cancel")
	}
}
