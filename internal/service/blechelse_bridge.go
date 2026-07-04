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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/freetetra/server/internal/config"
)

//go:embed blechelse_assets/*
var blechelseAssets embed.FS

// BlechelseSample is one entry in the sample catalog.
type BlechelseSample struct {
	// ID is the forward-slash relative path from SamplesDir, e.g.
	// "dt/abschnitte/hoch/a_bis_c.wav". It doubles as a stable key for the
	// browser and as the filesystem lookup after joining with SamplesDir.
	ID string `json:"id"`
	// Name is the filename stem (no extension), used for autocomplete match.
	Name string `json:"name"`
	// Category is the parent path from SamplesDir, e.g. "dt/abschnitte/hoch".
	Category string `json:"category"`
	// Lang is the top-level folder ("dt", "en", "gong").
	Lang string `json:"lang"`
	// Content is the spoken text pulled from the XLS manifest, when known —
	// e.g. "Frankfurt am Main Hauptbahnhof" for a ziele/ station-code file
	// or "Abfahrt" for module/0002.wav. Empty when no mapping exists.
	Content string `json:"content,omitempty"`
}

// blechelseManifest mirrors the JSON produced by the XLS extractor. Keys are
// full sample IDs; values are the spoken text.
type blechelseManifest struct {
	Source  string            `json:"source"`
	Count   int               `json:"count"`
	Entries map[string]string `json:"entries"`
}

// BlechelseBridge serves the Blechelse UI + play API. A play concatenates
// the queued samples' encoded frames and transmits them as one call.
type BlechelseBridge struct {
	cfg    config.Config
	logger *log.Logger
	plane  *BrewModulePlane

	samples []BlechelseSample
	byID    map[string]int

	// nameLower, catLower, contentLower are lowercase copies kept for cheap
	// case-insensitive search — computed once at startup.
	nameLower    []string
	catLower     []string
	contentLower []string

	busy      atomic.Bool
	currentID atomic.Value // string
	server    *http.Server

	encodeLocks sync.Map // cache path -> *sync.Mutex

	cancelMu sync.Mutex
	cancelFn context.CancelFunc
}

func NewBlechelseBridge(cfg config.Config, logger *log.Logger, plane *BrewModulePlane) (*BlechelseBridge, error) {
	if strings.TrimSpace(cfg.Blechelse.SamplesDir) == "" {
		return nil, fmt.Errorf("BLECHELSE_SAMPLES_DIR is required")
	}
	if strings.TrimSpace(cfg.Blechelse.EncoderBin) == "" {
		return nil, fmt.Errorf("BLECHELSE_ENCODER_BIN is required")
	}
	if len(cfg.Blechelse.Talkgroups) == 0 {
		return nil, fmt.Errorf("BLECHELSE_TALKGROUPS must list at least one TG")
	}
	if plane == nil {
		return nil, fmt.Errorf("brew module plane is nil")
	}

	catalog, err := scanBlechelseCatalog(cfg.Blechelse.SamplesDir, cfg.Blechelse.Languages)
	if err != nil {
		return nil, fmt.Errorf("scan catalog: %w", err)
	}
	if len(catalog) == 0 {
		return nil, fmt.Errorf("no .wav samples found under %s", cfg.Blechelse.SamplesDir)
	}

	// Optional manifest: <SamplesDir>/manifest.json holds spoken-text for
	// every known filename (extracted from the DB Blechelse XLS). Missing
	// or unreadable is not fatal — the catalog still works, just without
	// text-content search.
	manifestPath := filepath.Join(cfg.Blechelse.SamplesDir, "manifest.json")
	manifestEntries, manifestErr := loadBlechelseManifest(manifestPath)
	if manifestErr != nil {
		logger.Printf("blechelse manifest not loaded (%s): %v — filename search only", manifestPath, manifestErr)
	}
	matched := 0
	for i := range catalog {
		if text, ok := manifestEntries[catalog[i].ID]; ok {
			catalog[i].Content = text
			matched++
		}
	}

	byID := make(map[string]int, len(catalog))
	nameLower := make([]string, len(catalog))
	catLower := make([]string, len(catalog))
	contentLower := make([]string, len(catalog))
	for i, s := range catalog {
		byID[s.ID] = i
		nameLower[i] = strings.ToLower(s.Name)
		catLower[i] = strings.ToLower(s.Category)
		contentLower[i] = strings.ToLower(s.Content)
	}

	b := &BlechelseBridge{
		cfg:          cfg,
		logger:       logger,
		plane:        plane,
		samples:      catalog,
		byID:         byID,
		nameLower:    nameLower,
		catLower:     catLower,
		contentLower: contentLower,
	}
	b.currentID.Store("")
	logger.Printf(
		"blechelse catalog loaded samples=%d dir=%s tgs=%v manifest_matched=%d/%d",
		len(catalog),
		cfg.Blechelse.SamplesDir,
		cfg.Blechelse.Talkgroups,
		matched,
		len(manifestEntries),
	)
	return b, nil
}

// loadBlechelseManifest reads the XLS-derived manifest.json if present.
// Missing file returns an empty map + a non-nil error the caller can log.
func loadBlechelseManifest(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m blechelseManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return m.Entries, nil
}

// scanBlechelseCatalog walks SamplesDir looking for .wav files. If languages
// is non-empty, only files whose first path segment is in that set are kept.
func scanBlechelseCatalog(root string, languages []string) ([]BlechelseSample, error) {
	root = filepath.Clean(root)
	langFilter := make(map[string]struct{}, len(languages))
	for _, l := range languages {
		l = strings.TrimSpace(l)
		if l != "" {
			langFilter[l] = struct{}{}
		}
	}

	var out []BlechelseSample
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.EqualFold(filepath.Ext(path), ".wav") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		id := filepath.ToSlash(rel)
		parts := strings.Split(id, "/")
		if len(parts) < 2 {
			return nil
		}
		lang := parts[0]
		if len(langFilter) > 0 {
			if _, ok := langFilter[lang]; !ok {
				return nil
			}
		}
		category := strings.Join(parts[:len(parts)-1], "/")
		name := strings.TrimSuffix(parts[len(parts)-1], filepath.Ext(parts[len(parts)-1]))
		out = append(out, BlechelseSample{
			ID:       id,
			Name:     name,
			Category: category,
			Lang:     lang,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// ----- HTTP -----

func (b *BlechelseBridge) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", b.handleIndex)
	mux.HandleFunc("/api/search", b.handleSearch)
	mux.HandleFunc("/api/talkgroups", b.handleTalkgroups)
	mux.HandleFunc("/api/state", b.handleState)
	mux.HandleFunc("/api/play", b.handlePlay)
	mux.HandleFunc("/api/stop", b.handleStop)

	b.server = &http.Server{
		Addr:              b.cfg.Blechelse.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.server.Shutdown(shutdownCtx)
	}()

	b.logger.Printf("blechelse listening on %s (samples=%d)", b.cfg.Blechelse.ListenAddr, len(b.samples))
	if err := b.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (b *BlechelseBridge) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data, err := blechelseAssets.ReadFile("blechelse_assets/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func (b *BlechelseBridge) handleTalkgroups(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"talkgroups": b.cfg.Blechelse.Talkgroups})
}

func (b *BlechelseBridge) handleState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{
		"busy":       b.busy.Load(),
		"current_id": b.currentID.Load(),
		"catalog":    len(b.samples),
	})
}

// handleSearch returns ranked matches for q, up to limit results.
func (b *BlechelseBridge) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	lang := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("lang")))
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 && parsed <= 500 {
			limit = parsed
		}
	}

	type scored struct {
		idx   int
		score int
	}
	// Score model (higher = better). Content (spoken text from the XLS
	// manifest) wins over filename which wins over folder-path — a user
	// typing "Frankfurt" wants the station-code sample whose content is
	// "Frankfurt am Main Hauptbahnhof", not every filename under a folder
	// called "abschnitte". Empty q returns the catalog in ID order.
	//   120 content equals q exactly
	//   110 content starts with q
	//    90 content contains q
	//   100 name equals q exactly
	//    80 name starts with q
	//    60 name contains q
	//    30 category contains q
	var hits []scored
	for i := range b.samples {
		if lang != "" && !strings.EqualFold(b.samples[i].Lang, lang) {
			continue
		}
		if q == "" {
			hits = append(hits, scored{idx: i, score: 0})
			if len(hits) >= limit {
				break
			}
			continue
		}
		s := 0
		switch {
		case b.contentLower[i] != "" && b.contentLower[i] == q:
			s = 120
		case b.contentLower[i] != "" && strings.HasPrefix(b.contentLower[i], q):
			s = 110
		case b.nameLower[i] == q:
			s = 100
		case b.contentLower[i] != "" && strings.Contains(b.contentLower[i], q):
			s = 90
		case strings.HasPrefix(b.nameLower[i], q):
			s = 80
		case strings.Contains(b.nameLower[i], q):
			s = 60
		case strings.Contains(b.catLower[i], q):
			s = 30
		default:
			continue
		}
		hits = append(hits, scored{idx: i, score: s})
	}

	if q != "" {
		sort.SliceStable(hits, func(i, j int) bool {
			if hits[i].score != hits[j].score {
				return hits[i].score > hits[j].score
			}
			return b.samples[hits[i].idx].ID < b.samples[hits[j].idx].ID
		})
	}

	if len(hits) > limit {
		hits = hits[:limit]
	}
	results := make([]BlechelseSample, 0, len(hits))
	for _, h := range hits {
		results = append(results, b.samples[h.idx])
	}
	writeJSON(w, map[string]any{
		"results": results,
		"total":   len(b.samples),
	})
}

type blechelsePlayRequest struct {
	Queue []string `json:"queue"`
	TG    uint32   `json:"tg"`
}

func (b *BlechelseBridge) handlePlay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req blechelsePlayRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Queue) == 0 {
		http.Error(w, "queue is empty", http.StatusBadRequest)
		return
	}
	maxLen := b.cfg.Blechelse.MaxQueueLength
	if maxLen <= 0 {
		maxLen = 50
	}
	if len(req.Queue) > maxLen {
		http.Error(w, fmt.Sprintf("queue too long (max %d)", maxLen), http.StatusBadRequest)
		return
	}
	if req.TG == 0 {
		http.Error(w, "tg is required", http.StatusBadRequest)
		return
	}
	// Validate TG is in the allowed set — the plane is only affiliated with
	// those, so anything else would silently drop on the brew server.
	allowed := false
	for _, tg := range b.cfg.Blechelse.Talkgroups {
		if tg == req.TG {
			allowed = true
			break
		}
	}
	if !allowed {
		http.Error(w, fmt.Sprintf("tg %d not in allowed set %v", req.TG, b.cfg.Blechelse.Talkgroups), http.StatusBadRequest)
		return
	}

	// Resolve every queue entry to a sample before we start — reject the whole
	// play on the first bad ID so the caller sees a clean 404 instead of a
	// half-played call.
	resolved := make([]BlechelseSample, 0, len(req.Queue))
	for _, id := range req.Queue {
		idx, ok := b.byID[id]
		if !ok {
			http.Error(w, fmt.Sprintf("unknown sample id %q", id), http.StatusNotFound)
			return
		}
		resolved = append(resolved, b.samples[idx])
	}

	// If a play is already in flight, treat a fresh request as "stop the
	// current one and start mine" so the UI feels responsive.
	if b.busy.Load() {
		b.cancelCurrent()
		for waited := 0; b.busy.Load() && waited < 20; waited++ {
			time.Sleep(25 * time.Millisecond)
		}
		if b.busy.Load() {
			http.Error(w, "busy", http.StatusConflict)
			return
		}
	}
	if !b.busy.CompareAndSwap(false, true) {
		http.Error(w, "busy", http.StatusConflict)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.cancelMu.Lock()
	b.cancelFn = cancel
	b.cancelMu.Unlock()

	go func() {
		defer func() {
			b.cancelMu.Lock()
			b.cancelFn = nil
			b.cancelMu.Unlock()
			b.busy.Store(false)
			b.currentID.Store("")
		}()
		if err := b.playQueue(ctx, resolved, req.TG); err != nil {
			b.logger.Printf("blechelse play error tg=%d queue_len=%d: %v", req.TG, len(resolved), err)
		}
	}()

	writeJSON(w, map[string]any{"ok": true, "queued": len(resolved), "tg": req.TG})
}

func (b *BlechelseBridge) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	stopped := b.cancelCurrent()
	writeJSON(w, map[string]any{"stopped": stopped})
}

func (b *BlechelseBridge) cancelCurrent() bool {
	b.cancelMu.Lock()
	fn := b.cancelFn
	b.cancelMu.Unlock()
	if fn == nil {
		return false
	}
	fn()
	return true
}

// ----- TX -----

// playQueue encodes each queued sample (or reads its cached .acelp), then
// transmits lead-in silence + concatenated audio (with optional inter-sample
// silence padding) + tail-out silence as one call.
func (b *BlechelseBridge) playQueue(ctx context.Context, queue []BlechelseSample, tg uint32) error {
	// Encode-and-collect every sample first, before opening the call, so a
	// missing file doesn't leave a dangling TX with no audio on the wire.
	var allFrames [][]byte
	interval := b.frameInterval()
	interPadCount := durationToFrameCount(b.cfg.Blechelse.InterSamplePadding, interval)

	for qi, s := range queue {
		frames, err := b.cachedFrames(ctx, s)
		if err != nil {
			return fmt.Errorf("sample %q: %w", s.ID, err)
		}
		if len(frames) == 0 {
			b.logger.Printf("blechelse skip empty sample id=%s", s.ID)
			continue
		}
		allFrames = append(allFrames, frames...)
		if qi < len(queue)-1 && interPadCount > 0 {
			for i := 0; i < interPadCount; i++ {
				allFrames = append(allFrames, blechelseSilentFrame)
			}
		}
	}
	if len(allFrames) == 0 {
		return fmt.Errorf("no audio frames after encoding %d samples", len(queue))
	}

	leadIn := durationToFrameCount(b.cfg.Blechelse.LeadInPadding, interval)
	tailOut := durationToFrameCount(b.cfg.Blechelse.TailOutPadding, interval)

	callID := uuid.New()
	source := b.sourceISSI()
	if !b.plane.StartInjectedGroupTX("blechelse", callID, source, tg, 0, 0, 0) {
		return fmt.Errorf("plane refused StartInjectedGroupTX (call=%s)", callID.String())
	}
	b.currentID.Store(callID.String())
	b.logger.Printf(
		"blechelse tx start call=%s tg=%d source=%d queue_len=%d frames=%d lead=%d tail=%d",
		callID.String(), tg, source, len(queue), len(allFrames), leadIn, tailOut,
	)

	release := func() {
		b.plane.IdleInjectedCall("blechelse", callID, b.cfg.Blechelse.ReleaseCause)
		b.plane.ReleaseInjectedCall("blechelse", callID, b.cfg.Blechelse.ReleaseCause)
	}

	// Lead-in silence keeps the TX alive while receivers complete call setup.
	for i := 0; i < leadIn; i++ {
		if err := b.sendFrameWithDelay(ctx, callID, blechelseSilentFrame, interval); err != nil {
			release()
			return err
		}
	}

	for i, frame := range allFrames {
		select {
		case <-ctx.Done():
			release()
			return ctx.Err()
		default:
		}
		b.plane.InjectedVoiceFrame("blechelse", callID, frame)
		if interval > 0 && (i < len(allFrames)-1 || tailOut > 0) {
			select {
			case <-ctx.Done():
				release()
				return ctx.Err()
			case <-time.After(interval):
			}
		}
	}

	for i := 0; i < tailOut; i++ {
		if err := b.sendFrameWithDelay(ctx, callID, blechelseSilentFrame, interval); err != nil {
			release()
			return err
		}
	}

	release()
	b.logger.Printf("blechelse tx end call=%s frames=%d", callID.String(), len(allFrames))
	return nil
}

// blechelseSilentFrame is a zero-filled 36-byte STE frame — same pattern as
// soundboard, used for lead-in / tail-out / inter-sample padding.
var blechelseSilentFrame = make([]byte, 36)

func (b *BlechelseBridge) sendFrameWithDelay(ctx context.Context, callID uuid.UUID, frame []byte, interval time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	b.plane.InjectedVoiceFrame("blechelse", callID, frame)
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

func (b *BlechelseBridge) sourceISSI() uint32 {
	if b.cfg.Blechelse.SourceISSI != 0 {
		return b.cfg.Blechelse.SourceISSI
	}
	return b.cfg.Blechelse.BrewISSI
}

func (b *BlechelseBridge) frameInterval() time.Duration {
	if b.cfg.Blechelse.FrameInterval <= 0 {
		return 60 * time.Millisecond
	}
	return b.cfg.Blechelse.FrameInterval
}

// ----- encoding + cache (parallel to soundboard's cachedFrames) -----

func (b *BlechelseBridge) cachedFrames(ctx context.Context, s BlechelseSample) ([][]byte, error) {
	src, err := b.samplePath(s)
	if err != nil {
		return nil, err
	}
	cache := src + ".acelp"

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
		cacheInfo, cacheErr = os.Stat(cache)
		needsEncode = errors.Is(cacheErr, fs.ErrNotExist) ||
			(cacheErr == nil && cacheInfo.ModTime().Before(srcInfo.ModTime()))
		if needsEncode {
			if err := b.encodeToCache(ctx, src, cache); err != nil {
				return nil, fmt.Errorf("encode %s: %w", src, err)
			}
		}
	}
	return readFrames(cache)
}

// samplePath resolves a sample ID to an absolute filesystem path. Rejects
// IDs that would escape SamplesDir via .. or absolute-path tricks.
func (b *BlechelseBridge) samplePath(s BlechelseSample) (string, error) {
	if strings.Contains(s.ID, "..") || strings.HasPrefix(s.ID, "/") || strings.Contains(s.ID, "\\") {
		return "", fmt.Errorf("invalid sample id %q", s.ID)
	}
	joined := filepath.Join(b.cfg.Blechelse.SamplesDir, filepath.FromSlash(s.ID))
	root, err := filepath.Abs(b.cfg.Blechelse.SamplesDir)
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(joined)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(abs, root+string(filepath.Separator)) && abs != root {
		return "", fmt.Errorf("path escape for %q", s.ID)
	}
	return abs, nil
}

func (b *BlechelseBridge) encodeToCache(ctx context.Context, src, cache string) error {
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
	ffmpegCmd := exec.CommandContext(ctx, b.cfg.Blechelse.FFmpegBin, ffmpegArgs...)
	ffmpegOut, err := ffmpegCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("ffmpeg stdout: %w", err)
	}
	ffmpegErr, _ := ffmpegCmd.StderrPipe()

	encoderCmd := exec.CommandContext(ctx, b.cfg.Blechelse.EncoderBin, b.encoderArgs()...)
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

	go drain(b.logger, "blechelse ffmpeg", ffmpegErr)
	go drain(b.logger, "blechelse encoder", encoderErrPipe)

	frames, err := b.collectFrames(encoderOut)
	if cerr := ffmpegCmd.Wait(); cerr != nil && err == nil {
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
	return nil
}

func (b *BlechelseBridge) collectFrames(r io.Reader) ([][]byte, error) {
	frameSize := b.cfg.Blechelse.EncoderFrameSize
	if frameSize < 1 {
		frameSize = 18
	}
	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, r); err != nil {
		return nil, fmt.Errorf("read encoder: %w", err)
	}
	raw := buf.Bytes()
	if len(raw)%frameSize != 0 {
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

func (b *BlechelseBridge) encoderArgs() []string {
	if strings.TrimSpace(b.cfg.Blechelse.EncoderArgs) == "" {
		return nil
	}
	return strings.Fields(b.cfg.Blechelse.EncoderArgs)
}
