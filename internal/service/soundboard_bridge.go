package service

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/freetetra/server/internal/config"
)

//go:embed soundboard_assets/*
var soundboardAssets embed.FS

// SoundboardButton is one entry in the soundboard manifest.
type SoundboardButton struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	File  string `json:"file"`            // path relative to SoundsDir
	TG    uint32 `json:"tg"`              // talkgroup the button transmits on
	Color string `json:"color,omitempty"` // optional CSS color for the UI
}

// SoundboardManifest is the on-disk schema for manifest.json.
type SoundboardManifest struct {
	Buttons []SoundboardButton `json:"buttons"`
}

// SoundboardBridge serves a small web UI of clickable buttons that transmit
// pre-encoded audio into a chosen talkgroup. Source files (WAV/MP3/etc.) are
// re-encoded to STE ACELP frames on the first press and cached next to the
// source as <file>.acelp. A press while the bridge is already transmitting
// returns 409 — half-duplex behaviour matches a real radio.
type SoundboardBridge struct {
	cfg          config.Config
	logger       *log.Logger
	plane        *BrewModulePlane
	manifestPath string

	mu          sync.Mutex // guards manifest mutations + manifest-file writes
	manifest    SoundboardManifest
	busy        atomic.Bool
	currentID   atomic.Value // string
	server      *http.Server
	encodeLocks sync.Map // file path -> *sync.Mutex

	// cancelMu guards cancelFn — the cancel func for the in-flight playback
	// goroutine. Set when a play starts, cleared when it ends. A press for
	// the currently-playing button invokes this to abort the TX.
	cancelMu sync.Mutex
	cancelFn context.CancelFunc
}

// ErrSoundboardBusy is returned when a play is requested while another is in flight.
var ErrSoundboardBusy = errors.New("soundboard busy")

// NewSoundboardBridge builds the bridge from config + manifest on disk.
// The caller is responsible for the BrewModulePlane; the plane must already
// have the manifest's GSSIs in its registration list so the brew server
// accepts the TX.
func NewSoundboardBridge(cfg config.Config, logger *log.Logger, plane *BrewModulePlane) (*SoundboardBridge, error) {
	if strings.TrimSpace(cfg.Soundboard.SoundsDir) == "" {
		return nil, fmt.Errorf("SOUNDBOARD_SOUNDS_DIR is required")
	}
	if strings.TrimSpace(cfg.Soundboard.EncoderBin) == "" {
		return nil, fmt.Errorf("SOUNDBOARD_ENCODER_BIN is required")
	}
	if plane == nil {
		return nil, fmt.Errorf("brew module plane is nil")
	}

	manifest, err := loadSoundboardManifest(cfg)
	if err != nil {
		return nil, fmt.Errorf("load manifest: %w", err)
	}

	b := &SoundboardBridge{
		cfg:          cfg,
		logger:       logger,
		plane:        plane,
		manifest:     manifest,
		manifestPath: soundboardManifestPath(cfg),
	}
	b.currentID.Store("")
	return b, nil
}

func soundboardManifestPath(cfg config.Config) string {
	if p := strings.TrimSpace(cfg.Soundboard.ManifestPath); p != "" {
		return p
	}
	return filepath.Join(cfg.Soundboard.SoundsDir, "manifest.json")
}

// Manifest returns a copy for tests/inspection.
func (b *SoundboardBridge) Manifest() SoundboardManifest {
	b.mu.Lock()
	defer b.mu.Unlock()
	return SoundboardManifest{Buttons: append([]SoundboardButton(nil), b.manifest.Buttons...)}
}

// Talkgroups returns the unique set of GSSIs referenced in the manifest.
// Callers use this to build the BrewModulePlane's registration list.
func SoundboardTalkgroups(manifest SoundboardManifest) []uint32 {
	seen := make(map[uint32]bool, len(manifest.Buttons))
	out := make([]uint32, 0, len(manifest.Buttons))
	for _, btn := range manifest.Buttons {
		if btn.TG == 0 || seen[btn.TG] {
			continue
		}
		seen[btn.TG] = true
		out = append(out, btn.TG)
	}
	return out
}

// LoadSoundboardManifest is the exported form used by `main` to learn which
// TGs to register before constructing the plane.
func LoadSoundboardManifest(cfg config.Config) (SoundboardManifest, error) {
	return loadSoundboardManifest(cfg)
}

func loadSoundboardManifest(cfg config.Config) (SoundboardManifest, error) {
	path := cfg.Soundboard.ManifestPath
	if strings.TrimSpace(path) == "" {
		path = filepath.Join(cfg.Soundboard.SoundsDir, "manifest.json")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return SoundboardManifest{}, fmt.Errorf("read %s: %w", path, err)
	}
	var m SoundboardManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return SoundboardManifest{}, fmt.Errorf("parse %s: %w", path, err)
	}
	seenIDs := make(map[string]bool, len(m.Buttons))
	for i, btn := range m.Buttons {
		if strings.TrimSpace(btn.ID) == "" {
			return SoundboardManifest{}, fmt.Errorf("button[%d] has empty id", i)
		}
		if seenIDs[btn.ID] {
			return SoundboardManifest{}, fmt.Errorf("button[%d] duplicate id %q", i, btn.ID)
		}
		if strings.TrimSpace(btn.File) == "" {
			return SoundboardManifest{}, fmt.Errorf("button %q has empty file", btn.ID)
		}
		if btn.TG == 0 {
			return SoundboardManifest{}, fmt.Errorf("button %q has tg=0", btn.ID)
		}
		seenIDs[btn.ID] = true
	}
	return m, nil
}

// ----- HTTP server -----

// Start serves the soundboard UI + API. Blocks until Stop is called or ctx
// is cancelled.
func (b *SoundboardBridge) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", b.handleIndex)
	mux.HandleFunc("/api/buttons", b.handleButtons)
	mux.HandleFunc("/api/state", b.handleState)
	mux.HandleFunc("/api/play/", b.handlePlay)
	mux.HandleFunc("/api/talkgroups", b.handleTalkgroups)
	mux.HandleFunc("/api/myinstants/search", b.handleMyinstantsSearch)
	mux.HandleFunc("/api/myinstants/import", b.handleMyinstantsImport)
	mux.HandleFunc("/api/buttons/", b.handleButtonsItem)

	b.server = &http.Server{
		Addr:    b.cfg.Soundboard.ListenAddr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.server.Shutdown(shutdownCtx)
	}()

	tgs := SoundboardTalkgroups(b.manifest)
	b.logger.Printf("soundboard listening on %s (sounds_dir=%s buttons=%d tgs=%v)",
		b.cfg.Soundboard.ListenAddr, b.cfg.Soundboard.SoundsDir, len(b.manifest.Buttons), tgs)

	if err := b.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Stop forces the HTTP server down (used by tests + cmd shutdown).
func (b *SoundboardBridge) Stop() {
	if b.server != nil {
		_ = b.server.Close()
	}
}

func (b *SoundboardBridge) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		http.NotFound(w, r)
		return
	}
	data, err := fs.ReadFile(soundboardAssets, "soundboard_assets/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func (b *SoundboardBridge) handleButtons(w http.ResponseWriter, r *http.Request) {
	type view struct {
		ID     string `json:"id"`
		Label  string `json:"label"`
		TG     uint32 `json:"tg"`
		Color  string `json:"color,omitempty"`
		Cached bool   `json:"cached"`
	}
	buttons := b.snapshotButtons()
	out := make([]view, 0, len(buttons))
	for _, btn := range buttons {
		_, err := os.Stat(b.cachePath(btn))
		out = append(out, view{
			ID:     btn.ID,
			Label:  btn.Label,
			TG:     btn.TG,
			Color:  btn.Color,
			Cached: err == nil,
		})
	}
	writeJSON(w, map[string]any{"buttons": out})
}

// snapshotButtons returns a defensive copy of the current manifest under lock.
func (b *SoundboardBridge) snapshotButtons() []SoundboardButton {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]SoundboardButton(nil), b.manifest.Buttons...)
}

// handleButtonsItem supports DELETE /api/buttons/{id} so the UI can remove
// imported clips without an SSH session. The audio source and cache are
// left on disk — they're cheap and might still be wanted later. Only the
// manifest entry goes away.
func (b *SoundboardBridge) handleButtonsItem(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/buttons/")
	id = strings.TrimSuffix(id, "/")
	if id == "" {
		http.Error(w, "missing button id", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodDelete:
		removed, err := b.deleteButton(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !removed {
			http.Error(w, "unknown button", http.StatusNotFound)
			return
		}
		writeJSON(w, map[string]any{"deleted": id})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (b *SoundboardBridge) deleteButton(id string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, btn := range b.manifest.Buttons {
		if btn.ID == id {
			b.manifest.Buttons = append(b.manifest.Buttons[:i], b.manifest.Buttons[i+1:]...)
			if err := b.writeManifestLocked(); err != nil {
				// roll back so in-memory state stays consistent with disk
				b.manifest.Buttons = append(b.manifest.Buttons, SoundboardButton{})
				copy(b.manifest.Buttons[i+1:], b.manifest.Buttons[i:])
				b.manifest.Buttons[i] = btn
				return false, err
			}
			return true, nil
		}
	}
	return false, nil
}

func (b *SoundboardBridge) handleTalkgroups(w http.ResponseWriter, r *http.Request) {
	// Surface the unique TG set the manifest currently uses so the UI can
	// show a quick-pick dropdown. The import endpoint accepts any non-zero
	// TG (and registers it with the plane), so this is just a convenience.
	tgs := SoundboardTalkgroups(SoundboardManifest{Buttons: b.snapshotButtons()})
	writeJSON(w, map[string]any{"talkgroups": tgs})
}

func (b *SoundboardBridge) handleState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{
		"busy":       b.busy.Load(),
		"current_id": b.currentID.Load(),
	})
}

func (b *SoundboardBridge) handlePlay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/play/")
	id = strings.TrimSuffix(id, "/")
	if id == "" {
		http.Error(w, "missing button id", http.StatusBadRequest)
		return
	}
	btn, ok := b.buttonByID(id)
	if !ok {
		http.Error(w, "unknown button", http.StatusNotFound)
		return
	}

	// Toggle: a press for the currently-playing button aborts the TX. We
	// don't free busy here — the play goroutine sees ctx.Done() and clears
	// state itself, which keeps the busy lifecycle in one place.
	if b.busy.Load() {
		current, _ := b.currentID.Load().(string)
		if current == btn.ID {
			b.cancelMu.Lock()
			cancel := b.cancelFn
			b.cancelMu.Unlock()
			if cancel != nil {
				cancel()
			}
			writeJSON(w, map[string]any{"stopped": btn.ID})
			return
		}
	}

	// Optional per-press TG override (?tg=NN). When set, the press TX goes
	// to that GSSI instead of the button's manifest TG. The override is
	// ephemeral — the manifest is never mutated.
	overrideTG := uint32(0)
	if raw := strings.TrimSpace(r.URL.Query().Get("tg")); raw != "" {
		n, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || n == 0 {
			http.Error(w, "invalid tg override", http.StatusBadRequest)
			return
		}
		overrideTG = uint32(n)
	}

	if !b.busy.CompareAndSwap(false, true) {
		http.Error(w, "busy", http.StatusConflict)
		return
	}
	b.currentID.Store(btn.ID)

	playBtn := btn
	if overrideTG != 0 {
		playBtn.TG = overrideTG
		b.plane.EnsureGroup(overrideTG)
	}

	// Detach from the HTTP request lifetime — the press fires the call
	// and the HTTP response returns immediately. A long TX shouldn't be
	// cancelled by the browser closing the response. The cancel func is
	// stashed so a same-button press can stop the playback.
	ctx, cancel := context.WithCancel(context.Background())
	b.cancelMu.Lock()
	b.cancelFn = cancel
	b.cancelMu.Unlock()

	go func() {
		defer func() {
			b.cancelMu.Lock()
			b.cancelFn = nil
			b.cancelMu.Unlock()
			cancel()
			b.currentID.Store("")
			b.busy.Store(false)
		}()
		if err := b.playButton(ctx, playBtn); err != nil && !errors.Is(err, context.Canceled) {
			b.logger.Printf("soundboard play %q failed: %v", playBtn.ID, err)
		}
	}()
	writeJSON(w, map[string]any{"started": btn.ID, "tg": playBtn.TG})
}

func (b *SoundboardBridge) buttonByID(id string) (SoundboardButton, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, btn := range b.manifest.Buttons {
		if btn.ID == id {
			return btn, true
		}
	}
	return SoundboardButton{}, false
}

// ----- myinstants integration -----

// myinstantsUA is set on the upstream request because myinstants.com
// returns 403 to default Go http clients. The string is unremarkable —
// what matters is that "Mozilla/5.0" is present.
const myinstantsUA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"

var (
	myinstantsMP3Re   = regexp.MustCompile(`play\('(/media/sounds/[^']+\.mp3)'`)
	myinstantsTitleRe = regexp.MustCompile(`instant-link link-secondary"[^>]*>([^<]+)</a>`)
)

type myinstantsResult struct {
	Title  string `json:"title"`
	MP3URL string `json:"mp3_url"`
}

func (b *SoundboardBridge) handleMyinstantsSearch(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		http.Error(w, "missing q", http.StatusBadRequest)
		return
	}
	results, err := searchMyinstants(r.Context(), q)
	if err != nil {
		b.logger.Printf("soundboard myinstants search %q failed: %v", q, err)
		http.Error(w, "upstream error: "+err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, map[string]any{"results": results})
}

func searchMyinstants(ctx context.Context, query string) ([]myinstantsResult, error) {
	u := "https://www.myinstants.com/en/search/?name=" + url.QueryEscape(query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", myinstantsUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if err != nil {
		return nil, err
	}
	return parseMyinstantsResults(string(body)), nil
}

// parseMyinstantsResults scans the search HTML and zips the play('...') URLs
// with the visible link titles. The page emits one of each per result card
// in lockstep, so positional zip matches reliably; if myinstants changes the
// layout this is the spot to revisit.
func parseMyinstantsResults(html string) []myinstantsResult {
	mp3s := myinstantsMP3Re.FindAllStringSubmatch(html, -1)
	titles := myinstantsTitleRe.FindAllStringSubmatch(html, -1)
	n := len(mp3s)
	if len(titles) < n {
		n = len(titles)
	}
	out := make([]myinstantsResult, 0, n)
	for i := 0; i < n; i++ {
		title := htmlUnescape(strings.TrimSpace(titles[i][1]))
		out = append(out, myinstantsResult{
			Title:  title,
			MP3URL: "https://www.myinstants.com" + mp3s[i][1],
		})
	}
	return out
}

type importRequest struct {
	Title  string `json:"title"`
	MP3URL string `json:"mp3_url"`
	TG     uint32 `json:"tg"`
	Color  string `json:"color,omitempty"`
}

func (b *SoundboardBridge) handleMyinstantsImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req importRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Title) == "" {
		http.Error(w, "title required", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(req.MP3URL, "https://www.myinstants.com/media/sounds/") {
		http.Error(w, "mp3_url must be a myinstants /media/sounds/ URL", http.StatusBadRequest)
		return
	}
	if req.TG == 0 {
		http.Error(w, "tg must be > 0", http.StatusBadRequest)
		return
	}

	btn, err := b.importMyinstantsClip(r.Context(), req)
	if err != nil {
		b.logger.Printf("soundboard import %q failed: %v", req.Title, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, btn)
}

func (b *SoundboardBridge) importMyinstantsClip(ctx context.Context, req importRequest) (SoundboardButton, error) {
	slug := slugify(req.Title)
	if slug == "" {
		slug = "clip"
	}

	// Decide the on-disk filename + button id under the lock so two parallel
	// imports of the same title don't pick the same slug.
	b.mu.Lock()
	id := b.uniqueIDLocked(slug)
	filename := id + ".mp3"
	target := filepath.Join(b.cfg.Soundboard.SoundsDir, filename)
	// Pre-register the slot so a parallel import sees the id as taken.
	pending := SoundboardButton{ID: id, Label: req.Title, File: filename, TG: req.TG, Color: req.Color}
	b.manifest.Buttons = append(b.manifest.Buttons, pending)
	b.mu.Unlock()

	if err := downloadFile(ctx, req.MP3URL, target); err != nil {
		// Undo the optimistic manifest append.
		b.removeButtonByID(id)
		return SoundboardButton{}, fmt.Errorf("download: %w", err)
	}

	// Persist manifest + register the GSSI with the brew plane so TX is
	// accepted on this TG. Both are recoverable failures; if either errors
	// we still keep the file on disk.
	b.plane.EnsureGroup(req.TG)

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.writeManifestLocked(); err != nil {
		return SoundboardButton{}, fmt.Errorf("persist manifest: %w", err)
	}
	b.logger.Printf("soundboard imported %q -> %s (tg=%d id=%s)", req.Title, filename, req.TG, id)
	return pending, nil
}

func (b *SoundboardBridge) removeButtonByID(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, btn := range b.manifest.Buttons {
		if btn.ID == id {
			b.manifest.Buttons = append(b.manifest.Buttons[:i], b.manifest.Buttons[i+1:]...)
			return
		}
	}
}

// uniqueIDLocked returns slug, or slug_2, slug_3, ... if slug is already used.
// Caller must hold b.mu.
func (b *SoundboardBridge) uniqueIDLocked(slug string) string {
	taken := make(map[string]bool, len(b.manifest.Buttons))
	for _, btn := range b.manifest.Buttons {
		taken[btn.ID] = true
	}
	if !taken[slug] {
		return slug
	}
	for n := 2; n < 1000; n++ {
		cand := fmt.Sprintf("%s_%d", slug, n)
		if !taken[cand] {
			return cand
		}
	}
	return fmt.Sprintf("%s_%d", slug, time.Now().UnixNano())
}

// writeManifestLocked persists the in-memory manifest atomically. Caller
// must hold b.mu.
func (b *SoundboardBridge) writeManifestLocked() error {
	tmp := b.manifestPath + ".tmp"
	raw, err := json.MarshalIndent(b.manifest, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, b.manifestPath)
}

// ----- TX path -----

// soundboardSilentFrame is the 36-byte STE frame used for lead-in and
// tail-out padding around an audio clip. Zero-filled — the TETRA codec
// decodes this as silence/very-quiet noise; the point is to keep the TX
// alive on both edges so listeners never get clipped by call setup or
// teardown delays.
var soundboardSilentFrame = make([]byte, 36)

func (b *SoundboardBridge) playButton(ctx context.Context, btn SoundboardButton) error {
	audio, err := b.cachedFrames(ctx, btn)
	if err != nil {
		return fmt.Errorf("cached frames: %w", err)
	}
	if len(audio) == 0 {
		return fmt.Errorf("no frames for button %q", btn.ID)
	}

	interval := b.frameInterval()
	leadInCount := durationToFrameCount(b.cfg.Soundboard.LeadInPadding, interval)
	tailOutCount := durationToFrameCount(b.cfg.Soundboard.TailOutPadding, interval)

	callID := uuid.New()
	source := b.sourceISSI()
	if !b.plane.StartInjectedGroupTX("soundboard", callID, source, btn.TG, 0, 0, 0) {
		return fmt.Errorf("plane refused StartInjectedGroupTX (call=%s)", callID.String())
	}
	b.logger.Printf("soundboard tx start id=%s call=%s tg=%d audio=%d lead=%d tail=%d source=%d",
		btn.ID, callID.String(), btn.TG, len(audio), leadInCount, tailOutCount, source)

	release := func() {
		b.plane.IdleInjectedCall("soundboard", callID, b.cfg.Soundboard.ReleaseCause)
		b.plane.ReleaseInjectedCall("soundboard", callID, b.cfg.Soundboard.ReleaseCause)
	}

	// Lead-in silence keeps the TX alive while receivers complete call
	// setup — without it the first ~100-200ms of audio frequently get
	// clipped by listeners that haven't fully opened their squelch yet.
	for i := 0; i < leadInCount; i++ {
		if err := b.sendFrameWithDelay(ctx, callID, soundboardSilentFrame, interval); err != nil {
			release()
			return err
		}
	}

	for i, frame := range audio {
		select {
		case <-ctx.Done():
			release()
			return ctx.Err()
		default:
		}
		b.plane.InjectedVoiceFrame("soundboard", callID, frame)
		if interval > 0 && (i < len(audio)-1 || tailOutCount > 0) {
			select {
			case <-ctx.Done():
				release()
				return ctx.Err()
			case <-time.After(interval):
			}
		}
	}

	// Tail-out silence: the receiver's last decoded frame keeps playing until
	// the next one arrives, so without padding the very end of the audio gets
	// chopped when the call teardown reaches the radio.
	for i := 0; i < tailOutCount; i++ {
		if err := b.sendFrameWithDelay(ctx, callID, soundboardSilentFrame, interval); err != nil {
			release()
			return err
		}
	}

	release()
	b.logger.Printf("soundboard tx end id=%s call=%s", btn.ID, callID.String())
	return nil
}

// sendFrameWithDelay enqueues one frame and then waits `interval`, with
// ctx cancellation honored on both halves. Used by the padding loops where
// every iteration is uniform.
func (b *SoundboardBridge) sendFrameWithDelay(ctx context.Context, callID uuid.UUID, frame []byte, interval time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	b.plane.InjectedVoiceFrame("soundboard", callID, frame)
	if interval <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(interval):
		return nil
	}
}

// durationToFrameCount rounds a configured padding duration up to a whole
// number of frames at the given cadence. Zero or negative padding → 0.
func durationToFrameCount(d, interval time.Duration) int {
	if d <= 0 || interval <= 0 {
		return 0
	}
	n := int(d / interval)
	if d%interval != 0 {
		n++
	}
	return n
}

func (b *SoundboardBridge) sourceISSI() uint32 {
	if b.cfg.Soundboard.SourceISSI != 0 {
		return b.cfg.Soundboard.SourceISSI
	}
	return b.cfg.Soundboard.BrewISSI
}

func (b *SoundboardBridge) frameInterval() time.Duration {
	if b.cfg.Soundboard.FrameInterval <= 0 {
		return 60 * time.Millisecond
	}
	return b.cfg.Soundboard.FrameInterval
}

// ----- encoding + cache -----

// cachedFrames returns the STE frames for a button, encoding from the source
// file if no cache exists or the cache is older than the source.
func (b *SoundboardBridge) cachedFrames(ctx context.Context, btn SoundboardButton) ([][]byte, error) {
	src := b.sourcePath(btn)
	cache := b.cachePath(btn)

	srcInfo, err := os.Stat(src)
	if err != nil {
		return nil, fmt.Errorf("stat source %s: %w", src, err)
	}
	cacheInfo, cacheErr := os.Stat(cache)
	needsEncode := errors.Is(cacheErr, fs.ErrNotExist) ||
		(cacheErr == nil && cacheInfo.ModTime().Before(srcInfo.ModTime()))

	if needsEncode {
		lockAny, _ := b.encodeLocks.LoadOrStore(cache, &sync.Mutex{})
		lock := lockAny.(*sync.Mutex)
		lock.Lock()
		defer lock.Unlock()
		// Re-check after acquiring the lock — another caller may have just
		// finished encoding while we waited.
		cacheInfo, cacheErr = os.Stat(cache)
		needsEncode = errors.Is(cacheErr, fs.ErrNotExist) ||
			(cacheErr == nil && cacheInfo.ModTime().Before(srcInfo.ModTime()))
		if needsEncode {
			if err := b.encodeToCache(ctx, src, cache); err != nil {
				return nil, fmt.Errorf("encode %s -> %s: %w", src, cache, err)
			}
		}
	}

	return readFrames(cache)
}

func (b *SoundboardBridge) sourcePath(btn SoundboardButton) string {
	if filepath.IsAbs(btn.File) {
		return btn.File
	}
	return filepath.Join(b.cfg.Soundboard.SoundsDir, btn.File)
}

func (b *SoundboardBridge) cachePath(btn SoundboardButton) string {
	return b.sourcePath(btn) + ".acelp"
}

// encodeToCache pipes the source file through ffmpeg into the configured
// ACELP encoder, normalizes each frame to the 36-byte STE form expected by
// the brew TX path, and writes a flat <num_frames * 36> byte cache file.
func (b *SoundboardBridge) encodeToCache(ctx context.Context, src, cache string) error {
	ffmpegArgs := []string{
		"-nostdin",
		"-hide_banner",
		"-loglevel", "error",
		"-i", src,
		"-af", "volume=-14dB,acompressor=threshold=-20dB:ratio=4:attack=5:release=50",
		"-f", "s16le",
		"-ac", "1",
		"-ar", "8000",
		"pipe:1",
	}
	ffmpegCmd := exec.CommandContext(ctx, b.cfg.Soundboard.FFmpegBin, ffmpegArgs...)
	ffmpegOut, err := ffmpegCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("ffmpeg stdout: %w", err)
	}
	ffmpegErr, _ := ffmpegCmd.StderrPipe()

	encoderCmd := exec.CommandContext(ctx, b.cfg.Soundboard.EncoderBin, b.encoderArgs()...)
	encoderCmd.Stdin = ffmpegOut
	encoderOut, err := encoderCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("encoder stdout: %w", err)
	}
	encoderErrPipe, _ := encoderCmd.StderrPipe()

	if err := ffmpegCmd.Start(); err != nil {
		return fmt.Errorf("start ffmpeg: %w", err)
	}
	if err := encoderCmd.Start(); err != nil {
		_ = ffmpegCmd.Process.Kill()
		return fmt.Errorf("start encoder: %w", err)
	}

	go drain(b.logger, "soundboard ffmpeg", ffmpegErr)
	go drain(b.logger, "soundboard encoder", encoderErrPipe)

	frames, err := b.collectFrames(encoderOut)
	if cerr := ffmpegCmd.Wait(); cerr != nil && err == nil {
		// ffmpeg can exit non-zero on broken pipes once the encoder closes;
		// only surface if we don't already have a parse error.
		if !strings.Contains(cerr.Error(), "signal: broken pipe") {
			err = fmt.Errorf("ffmpeg: %w", cerr)
		}
	}
	if cerr := encoderCmd.Wait(); cerr != nil && err == nil {
		err = fmt.Errorf("encoder: %w", cerr)
	}
	if err != nil {
		return err
	}
	if len(frames) == 0 {
		return fmt.Errorf("encoder produced no frames")
	}

	tmp := cache + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create cache: %w", err)
	}
	for _, frame := range frames {
		if _, err := f.Write(frame); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("write cache: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close cache: %w", err)
	}
	if err := os.Rename(tmp, cache); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename cache: %w", err)
	}
	b.logger.Printf("soundboard encoded %s -> %s (%d frames)", src, cache, len(frames))
	return nil
}

func (b *SoundboardBridge) collectFrames(r io.Reader) ([][]byte, error) {
	frameSize := b.cfg.Soundboard.EncoderFrameSize
	if frameSize < 1 {
		frameSize = 18
	}
	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, r); err != nil {
		return nil, fmt.Errorf("read encoder: %w", err)
	}
	raw := buf.Bytes()
	if len(raw)%frameSize != 0 {
		// Truncate to the last complete frame — partial trailing bytes mean
		// ffmpeg or the encoder was killed mid-frame. The audio is still
		// usable up to that point.
		raw = raw[:len(raw)-(len(raw)%frameSize)]
	}

	var pendingCodec18 []byte
	out := make([][]byte, 0, len(raw)/frameSize)
	for off := 0; off+frameSize <= len(raw); off += frameSize {
		ste, ready, err := normalizeRadioFrame(raw[off:off+frameSize], &pendingCodec18)
		if err != nil {
			return nil, err
		}
		if !ready {
			continue
		}
		out = append(out, ste)
	}
	return out, nil
}

func (b *SoundboardBridge) encoderArgs() []string {
	if strings.TrimSpace(b.cfg.Soundboard.EncoderArgs) == "" {
		return nil
	}
	return strings.Fields(b.cfg.Soundboard.EncoderArgs)
}

// readFrames loads a .acelp cache file into a slice of 36-byte STE frames.
// The cache layout is a flat concatenation produced by encodeToCache.
func readFrames(path string) ([][]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw)%36 != 0 {
		return nil, fmt.Errorf("cache %s: size %d not a multiple of 36 — re-encode required", path, len(raw))
	}
	out := make([][]byte, 0, len(raw)/36)
	for off := 0; off < len(raw); off += 36 {
		frame := make([]byte, 36)
		copy(frame, raw[off:off+36])
		out = append(out, frame)
	}
	return out, nil
}

// ----- helpers -----

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

// slugify reduces an arbitrary title to a filename/id-safe ASCII string.
// Lowercased, non-alphanumeric runs collapsed to underscores, trimmed.
func slugify(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	prevUnderscore := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevUnderscore = false
		} else if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}

// htmlUnescape is a thin alias so the call site reads cleanly.
func htmlUnescape(s string) string { return html.UnescapeString(s) }

// downloadFile fetches u with a browser User-Agent (myinstants returns 403
// to default Go clients) and writes the body atomically to dst. The download
// is capped at 25 MiB to keep an attacker from filling the disk via the
// import endpoint.
func downloadFile(ctx context.Context, u, dst string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", myinstantsUA)
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	tmp := dst + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, io.LimitReader(resp.Body, 25*1024*1024)); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

func drain(logger *log.Logger, tag string, r io.Reader) {
	if r == nil {
		return
	}
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			logger.Printf("%s: %s", tag, strings.TrimRight(string(buf[:n]), "\n"))
		}
		if err != nil {
			return
		}
	}
}
