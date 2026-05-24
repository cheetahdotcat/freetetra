package service

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
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
	cfg      config.Config
	logger   *log.Logger
	plane    *BrewModulePlane
	manifest SoundboardManifest

	mu          sync.Mutex // guards startup (no concurrent encodes for the same button)
	busy        atomic.Bool
	currentID   atomic.Value // string
	server      *http.Server
	encodeLocks sync.Map // file path -> *sync.Mutex
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
		cfg:      cfg,
		logger:   logger,
		plane:    plane,
		manifest: manifest,
	}
	b.currentID.Store("")
	return b, nil
}

// Manifest returns a copy for tests/inspection.
func (b *SoundboardBridge) Manifest() SoundboardManifest {
	out := SoundboardManifest{Buttons: append([]SoundboardButton(nil), b.manifest.Buttons...)}
	return out
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
	out := make([]view, 0, len(b.manifest.Buttons))
	for _, btn := range b.manifest.Buttons {
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

	if !b.busy.CompareAndSwap(false, true) {
		http.Error(w, "busy", http.StatusConflict)
		return
	}
	b.currentID.Store(btn.ID)

	go func() {
		defer func() {
			b.currentID.Store("")
			b.busy.Store(false)
		}()
		// Detach from the HTTP request lifetime — the press fires the call
		// and the HTTP response returns immediately. A long TX shouldn't be
		// cancelled by the browser closing the response.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := b.playButton(ctx, btn); err != nil {
			b.logger.Printf("soundboard play %q failed: %v", btn.ID, err)
		}
	}()
	writeJSON(w, map[string]any{"started": btn.ID})
}

func (b *SoundboardBridge) buttonByID(id string) (SoundboardButton, bool) {
	for _, btn := range b.manifest.Buttons {
		if btn.ID == id {
			return btn, true
		}
	}
	return SoundboardButton{}, false
}

// ----- TX path -----

func (b *SoundboardBridge) playButton(ctx context.Context, btn SoundboardButton) error {
	frames, err := b.cachedFrames(ctx, btn)
	if err != nil {
		return fmt.Errorf("cached frames: %w", err)
	}
	if len(frames) == 0 {
		return fmt.Errorf("no frames for button %q", btn.ID)
	}

	callID := uuid.New()
	source := b.sourceISSI()
	if !b.plane.StartInjectedGroupTX("soundboard", callID, source, btn.TG, 0, 0, 0) {
		return fmt.Errorf("plane refused StartInjectedGroupTX (call=%s)", callID.String())
	}
	b.logger.Printf("soundboard tx start id=%s call=%s tg=%d frames=%d source=%d",
		btn.ID, callID.String(), btn.TG, len(frames), source)

	interval := b.frameInterval()
	for i, frame := range frames {
		select {
		case <-ctx.Done():
			b.plane.IdleInjectedCall("soundboard", callID, b.cfg.Soundboard.ReleaseCause)
			b.plane.ReleaseInjectedCall("soundboard", callID, b.cfg.Soundboard.ReleaseCause)
			return ctx.Err()
		default:
		}
		b.plane.InjectedVoiceFrame("soundboard", callID, frame)
		if interval > 0 && i < len(frames)-1 {
			select {
			case <-ctx.Done():
				b.plane.IdleInjectedCall("soundboard", callID, b.cfg.Soundboard.ReleaseCause)
				b.plane.ReleaseInjectedCall("soundboard", callID, b.cfg.Soundboard.ReleaseCause)
				return ctx.Err()
			case <-time.After(interval):
			}
		}
	}
	b.plane.IdleInjectedCall("soundboard", callID, b.cfg.Soundboard.ReleaseCause)
	b.plane.ReleaseInjectedCall("soundboard", callID, b.cfg.Soundboard.ReleaseCause)
	b.logger.Printf("soundboard tx end id=%s call=%s", btn.ID, callID.String())
	return nil
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
