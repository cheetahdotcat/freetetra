# Webradio Pre-Processing Improvements

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hardcoded ffmpeg filter chain in [internal/service/webradio_bridge.go](../../../internal/service/webradio_bridge.go) with a configurable pipeline that produces predictable, broadcast-quality audio at TETRA's 8 kHz ACELP. Surface what's actually on air via metrics, gate silence so dead streams stop transmitting, and accept multiple sources with failover.

**Architecture:** Single feature branch `feat/webradio-preprocessing`. Each task is one atomic commit that builds and passes `go vet ./...`. Filter composition moves into a dedicated builder; the `WebRadioBridge` retains the same external shape (Start/Stop/run loop) so callers don't change. The current single `WEBRADIO_STREAM_URL` becomes one of N sources; legacy env vars continue to work.

**Tech Stack:** Go 1.22+, ffmpeg ≥ 5.0 (for `loudnorm` two-pass + `soxr` resampler), `tetra-acelp-stdio`. No new Go deps. Tests piggy-back on the existing `internal/service` package — there's no integration harness for webradio audio so unit tests cover the filter builder, stderr parsing, and source-rotation logic.

**Spec:** This document. No separate design doc — every decision is captured below in the rationale paragraphs.

---

## Today's pain points (motivation)

The current pipeline:

```text
http stream → ffmpeg [-af volume=-14dB,acompressor=...]
            → s16le mono 8kHz → tetra-acelp-stdio → STE frames → brew TX
```

- **Static `-14 dB` cut**: works for one source, breaks for the next. Different streams arrive at different loudness levels; some clip the encoder, others vanish under the noise floor.
- **No speech-band filtering**: ACELP at 3.45 kbps lives in roughly 300 Hz – 3.4 kHz. Sub-bass and 4–8 kHz energy waste bits and add codec warble.
- **No loudness normalization**: no LUFS targeting, so on-air level is whatever the upstream feels like that hour.
- **No live monitoring**: the operator has no idea what level is going on air. ffmpeg's stderr scrolls by and gets dropped.
- **Single source, no failover**: if the stream URL stalls, the bridge reconnects in a loop and the TG goes silent.
- **No silence handling**: when the upstream goes quiet (between songs / dead air), the encoder pumps null frames over the TG.
- **Hardcoded filter**: any tuning requires a rebuild.

## What "better" looks like

After this plan lands:

- Filter chain is composed from typed config knobs, not a hand-edited string.
- Loudness is normalized to a target LUFS via `loudnorm` (EBU R128 single-pass).
- HPF/LPF clip the signal to the ACELP-friendly speech band.
- A soft limiter sits between the chain and the encoder so transients never clip the codec.
- ffmpeg stderr is parsed for `loudnorm` and `silencedetect` lines; current LUFS / peak / silence-state are exposed via `GET /api/webradio/status` and logged periodically.
- During detected silence the bridge stops emitting frames so the TG falls back to idle instead of broadcasting nothing.
- A list of source URLs is tried in order; a stream that stops producing frames for `STALL_TIMEOUT` triggers failover.
- Existing env (`WEBRADIO_STREAM_URL`, `WEBRADIO_TALKGROUP`, …) keeps working as a single-source compatibility shim.

Tasks 1–4 deliver the audio-quality wins by themselves; 5–8 layer on monitoring/control. Each task is independently shippable.

---

## Task 0: Baseline verification

Confirm the tree builds and the existing webradio unit tests pass on `feat/webradio-preprocessing` *before* any code change.

**Files:** none (read-only)

- [ ] **Step 1: Branch off master**

```bash
git checkout master
git pull --ff-only
git checkout -b feat/webradio-preprocessing
```

- [ ] **Step 2: Build the world**

```bash
go build ./...
```

Expected: exit 0. Stop if it fails.

- [ ] **Step 3: Vet + test**

```bash
go vet ./...
go test ./internal/service/...
```

Expected: exit 0 for both. The current `webradio_bridge_test.go` covers `normalizeRadioFrame` shapes — keep it green throughout.

- [ ] **Step 4: Note the current `ffmpegArgs()` filter string**

Read [internal/service/webradio_bridge.go:335-349](../../../internal/service/webradio_bridge.go#L335-L349). The single `-af` value is the regression target — task 1 must produce the *exact same* filter string from the new builder with default env, so we can verify by diffing the spawned arg list in tests.

---

## Task 1: Filter chain builder (behavior-preserving)

Extract the ffmpeg `-af` value into a `buildFilterChain(cfg) string` helper. With default env, it must produce the existing string verbatim. New env vars are introduced but defaulted to "off" so the produced chain is identical.

**Why this first:** Every later task adds a filter or a knob. Doing this once cleanly means later tasks are 5-line additions, not surgery on a 14-character literal.

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/service/webradio_bridge.go`
- Add: `internal/service/webradio_filters.go`
- Add: `internal/service/webradio_filters_test.go`

- [ ] **Step 1: Extend `WebRadioConfig`**

Add to the struct:

```go
VolumeDB        float64 // existing static volume; default -14 (matches legacy)
Compressor      string  // ffmpeg compressor expr; default "acompressor=threshold=-20dB:ratio=4:attack=5:release=50"
ExtraFilters    string  // raw passthrough appended at the end (escape hatch)
```

Add env loaders:

```go
VolumeDB:     envFloat("WEBRADIO_VOLUME_DB", -14),
Compressor:   env("WEBRADIO_COMPRESSOR", "acompressor=threshold=-20dB:ratio=4:attack=5:release=50"),
ExtraFilters: env("WEBRADIO_EXTRA_FILTERS", ""),
```

`envFloat` doesn't exist yet — add it alongside `envInt`/`envDuration`. Parse with `strconv.ParseFloat`, default on parse error or empty.

- [ ] **Step 2: New file `webradio_filters.go`**

```go
package service

import (
    "fmt"
    "strings"

    "github.com/freetetra/server/internal/config"
)

type filterChain struct{ parts []string }

func (f *filterChain) add(expr string) {
    if e := strings.TrimSpace(expr); e != "" {
        f.parts = append(f.parts, e)
    }
}

func (f *filterChain) String() string {
    return strings.Join(f.parts, ",")
}

// BuildWebRadioFilterChain returns the value passed to ffmpeg's -af flag.
// The chain is intentionally simple and linear — every order-sensitive
// stage (volume → dynamics → loudness → resample) reads top-to-bottom.
func BuildWebRadioFilterChain(cfg config.WebRadioConfig) string {
    fc := &filterChain{}
    if cfg.VolumeDB != 0 {
        fc.add(fmt.Sprintf("volume=%gdB", cfg.VolumeDB))
    }
    fc.add(cfg.Compressor)
    fc.add(cfg.ExtraFilters)
    return fc.String()
}
```

- [ ] **Step 3: Wire the builder into `ffmpegArgs()`**

Replace the literal `-af volume=-14dB,acompressor=…` with:

```go
filters := BuildWebRadioFilterChain(b.cfg.WebRadio)
args := []string{
    "-re", "-nostdin", "-hide_banner", "-loglevel", "error",
    "-i", b.cfg.WebRadio.StreamURL,
}
if filters != "" {
    args = append(args, "-af", filters)
}
args = append(args, "-f", "s16le", "-ac", "1", "-ar", "8000", "pipe:1")
return args
```

- [ ] **Step 4: Test — default config reproduces the legacy chain**

In `webradio_filters_test.go`:

```go
func TestBuildWebRadioFilterChain_DefaultsMatchLegacy(t *testing.T) {
    cfg := config.WebRadioConfig{
        VolumeDB:   -14,
        Compressor: "acompressor=threshold=-20dB:ratio=4:attack=5:release=50",
    }
    got := BuildWebRadioFilterChain(cfg)
    want := "volume=-14dB,acompressor=threshold=-20dB:ratio=4:attack=5:release=50"
    if got != want { t.Errorf("got %q want %q", got, want) }
}
```

Plus tests for empty chain, extra-filters appended last, custom compressor.

- [ ] **Step 5: Commit**

```bash
git commit -m "webradio: extract filter chain into a typed builder (no behavior change)"
```

---

## Task 2: Speech-band shaping (HPF + LPF) + better resampler

Add high-pass and low-pass filters to clip the signal to ACELP's useful band, and switch the implicit resampler to `soxr` for cleaner 44.1/48k → 8k downconversion. Both are off by default to keep the legacy filter exactly reproducible — operators flip them on per-deployment.

**Why now:** Free win for codec quality. ACELP at 8 kHz spends bits on everything below 200 Hz and above 3.4 kHz that it can't even reproduce; removing that energy lets the encoder spend its budget on speech-band content.

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/service/webradio_filters.go`
- Modify: `internal/service/webradio_filters_test.go`

- [ ] **Step 1: New config knobs**

```go
HPFHz     int    // default 0 (off); recommended 120
LPFHz     int    // default 0 (off); recommended 3500
Resampler string // default "" (ffmpeg default); recommended "soxr"
```

Env: `WEBRADIO_HPF_HZ`, `WEBRADIO_LPF_HZ`, `WEBRADIO_RESAMPLER`.

- [ ] **Step 2: Builder additions**

In `BuildWebRadioFilterChain`, before `ExtraFilters`:

```go
if cfg.HPFHz > 0 { fc.add(fmt.Sprintf("highpass=f=%d", cfg.HPFHz)) }
if cfg.LPFHz > 0 { fc.add(fmt.Sprintf("lowpass=f=%d", cfg.LPFHz)) }
```

After `ExtraFilters`:

```go
if r := strings.TrimSpace(cfg.Resampler); r != "" {
    fc.add(fmt.Sprintf("aresample=resampler=%s", r))
}
```

- [ ] **Step 3: Tests**

```go
func TestBuildWebRadioFilterChain_SpeechBand(t *testing.T) {
    cfg := config.WebRadioConfig{HPFHz: 120, LPFHz: 3500}
    got := BuildWebRadioFilterChain(cfg)
    want := "highpass=f=120,lowpass=f=3500"
    if got != want { t.Errorf("got %q want %q", got, want) }
}
```

Plus a test that `WEBRADIO_RESAMPLER=soxr` appends `aresample=resampler=soxr` at the *end* of the chain.

- [ ] **Step 4: Commit**

```bash
git commit -m "webradio: optional HPF/LPF and soxr resampler in the filter chain"
```

---

## Task 3: Loudness normalization (EBU R128 single-pass)

Replace the static volume cut with `loudnorm` in single-pass mode. This is the biggest perceived-quality jump in the whole plan — different sources end up at the same on-air level, sub-bass thumps don't blow the meter, and dead-air silence stops being inflated to "loud" levels.

**Why single-pass:** Two-pass needs a complete file. Live streams don't have one. Single-pass uses look-ahead (~3 s of buffering) and converges within the first 10 s of playback. That's fine for a backhaul radio — listeners aren't going to A/B the first three seconds of program material.

**Defaults:** `I=-16 LUFS, TP=-1.5 dBTP, LRA=11`. -16 LUFS is a common voice-radio target (between Spotify's -14 and EBU's -23); -1.5 dBTP keeps the limiter from clipping after resampling.

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/service/webradio_filters.go`
- Modify: `internal/service/webradio_filters_test.go`

- [ ] **Step 1: Config**

```go
LoudnormMode string  // "off" | "single" (default "off" for backwards compat; flip per-deploy)
LoudnormI    float64 // integrated loudness target LUFS; default -16
LoudnormTP   float64 // true-peak ceiling dBTP; default -1.5
LoudnormLRA  float64 // loudness range LU; default 11
```

Env: `WEBRADIO_LOUDNORM_MODE`, `WEBRADIO_LOUDNORM_I`, `WEBRADIO_LOUDNORM_TP`, `WEBRADIO_LOUDNORM_LRA`.

- [ ] **Step 2: Builder**

In `BuildWebRadioFilterChain`, between the compressor and HPF blocks (loudnorm wants the signal in roughly broadcast shape):

```go
if strings.EqualFold(cfg.LoudnormMode, "single") {
    fc.add(fmt.Sprintf("loudnorm=I=%g:TP=%g:LRA=%g", cfg.LoudnormI, cfg.LoudnormTP, cfg.LoudnormLRA))
}
```

When loudnorm is on, the static `volume=` cut becomes redundant — log a warning at startup if both `VolumeDB != 0` and `LoudnormMode == single`. Don't auto-disable; operators may want both.

- [ ] **Step 3: Tests**

`TestBuildWebRadioFilterChain_LoudnormOn` checks the produced string contains `loudnorm=I=-16:TP=-1.5:LRA=11` when mode=single.

- [ ] **Step 4: Commit**

```bash
git commit -m "webradio: single-pass EBU R128 loudness normalization (loudnorm)"
```

---

## Task 4: Soft limiter before the encoder

Insert `alimiter` as the very last filter in the chain so any remaining transient peaks get gently squashed instead of clipping the s16le buffer feeding tetra-acelp-stdio. Even with loudnorm's true-peak ceiling, resampling artifacts and DC transients can push individual samples to 0 dBFS.

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/service/webradio_filters.go`
- Modify: `internal/service/webradio_filters_test.go`

- [ ] **Step 1: Config**

```go
LimiterDBFS float64 // dBFS ceiling; default -0.5; set to 0 to disable
```

Env: `WEBRADIO_LIMITER_DBFS`.

- [ ] **Step 2: Builder**

Place after the resampler so the limiter sees the final 8 kHz signal:

```go
if cfg.LimiterDBFS < 0 {
    lin := math.Pow(10, cfg.LimiterDBFS/20.0)
    fc.add(fmt.Sprintf("alimiter=level_in=1:level_out=1:limit=%g", lin))
}
```

- [ ] **Step 3: Test**

Verify `-0.5 dBFS` produces `limit≈0.944` in the rendered string.

- [ ] **Step 4: Commit**

```bash
git commit -m "webradio: soft limiter as the last filter to protect the encoder"
```

---

## Task 5: Loudness + silence telemetry from ffmpeg stderr

Today `b.logCommandOutput("webradio ffmpeg", ffmpegErr)` just dumps stderr into the logger. Parse it instead: extract `loudnorm` runtime stats and `silencedetect` events into a small in-memory state object, and surface it.

**Outputs:**
- Periodic log line: `webradio loudness lufs_M=-15.8 lufs_S=-16.1 lufs_I=-16.4 peak=-2.3dBTP silence=false`
- `GET /api/webradio/status` JSON response on the main HTTP server.

**Files:**
- Modify: `internal/service/webradio_bridge.go`
- Modify: `internal/service/webradio_filters.go` (add `silencedetect` filter)
- Add: `internal/service/webradio_telemetry.go`
- Add: `internal/service/webradio_telemetry_test.go`
- Modify: `internal/service/service.go` (route registration)

- [ ] **Step 1: Add `silencedetect` to the filter chain**

```go
fc.add(fmt.Sprintf("silencedetect=noise=%ddB:d=%g", cfg.SilenceNoiseDB, cfg.SilenceMinDur.Seconds()))
```

Behind a `WEBRADIO_SILENCEDETECT` boolean (default true). New config: `SilenceNoiseDB int` (default `-50`), `SilenceMinDur time.Duration` (default `1500ms`).

- [ ] **Step 2: Telemetry state + parser**

```go
type webradioTelemetry struct {
    mu             sync.RWMutex
    LoudnessM      float64   // momentary LUFS
    LoudnessS      float64   // short-term LUFS
    LoudnessI      float64   // integrated LUFS (running)
    TruePeakDBTP   float64
    Silence        bool      // currently inside a silence run
    SilenceSince   time.Time
    LastLoudnessAt time.Time
}

func (t *webradioTelemetry) parseLine(line string) { /* regex match loudnorm + silencedetect */ }
func (t *webradioTelemetry) Snapshot() telemetrySnapshot { /* copy under RLock */ }
```

Regexes (kept narrow; ffmpeg's output format is stable across 5.x/6.x):
- `\[Parsed_loudnorm.*Loudness:.*M:\s*([-\d.]+)\s*S:\s*([-\d.]+)\s*I:\s*([-\d.]+)\s*LUFS\s*TPK:\s*([-\d.]+)`
- `silence_start: ([\d.]+)`
- `silence_end: ([\d.]+)`

- [ ] **Step 3: Replace the bare `logCommandOutput` with a tee**

```go
go b.consumeFFmpegStderr(ffmpegErr)
```

Where `consumeFFmpegStderr` scans line-by-line, calls `telemetry.parseLine`, and forwards uninteresting lines to the logger at debug level.

- [ ] **Step 4: HTTP endpoint**

In `service.go`, where other `/api/...` routes register, add a `/api/webradio/status` route that returns `telemetry.Snapshot()` as JSON. Surface it only if the webradio bridge is running (nil-check).

- [ ] **Step 5: Periodic log line**

Every 30 s (configurable via `WEBRADIO_TELEMETRY_LOG_EVERY`), emit a single summary line. Skip if nothing's changed since the previous emission.

- [ ] **Step 6: Tests for the parser**

Feed canned ffmpeg lines and assert the parsed state. No goroutine plumbing in tests.

- [ ] **Step 7: Commit**

```bash
git commit -m "webradio: parse loudnorm + silencedetect from ffmpeg stderr; /api/webradio/status"
```

---

## Task 6: Silence-aware TX gating

When the parser reports `Silence=true`, stop calling `plane.InjectedVoiceFrame`. Issue an `IdleInjectedCall` + `ReleaseInjectedCall` so the TG falls back to idle. On `silence_end`, generate a fresh callID and resume.

**Why:** Today, dead air gets encoded into ACELP nothing-frames and continues holding the TG. Receivers hear a steady carrier with no audio. Better to drop the call so listeners hear true silence and the TG is available for other traffic.

**Files:**
- Modify: `internal/service/webradio_bridge.go`

- [ ] **Step 1: Snapshot silence state per frame**

In `readEncoderFrames`, before each `plane.InjectedVoiceFrame`, check `b.telemetry.Snapshot().Silence`. If true:

```go
if callStarted {
    b.plane.IdleInjectedCall("webradio", currentCallID, b.cfg.WebRadio.ReleaseCause)
    callStarted = false
    activeCallID = uuid.Nil
}
continue
```

- [ ] **Step 2: Resume on silence exit**

The existing "if !callStarted, StartInjectedCall" branch already handles resumption — the next non-silent frame allocates a fresh callID and re-starts the TX. Confirm with a quick read; no code change expected.

- [ ] **Step 3: Behind a flag**

`WEBRADIO_SILENCE_GATING` (default true). Some operators want continuous TX as a keepalive; let them opt out.

- [ ] **Step 4: Test**

Hard to unit-test without a real ffmpeg pipe. Add an integration smoke test under `tests/webradio-silence/run.sh` that:
1. Starts the freetetra container + a webradio container pointed at a 5 s silent WAV → 5 s tone → 5 s silent WAV.
2. Tails freetetra logs for `GroupTX` allocations.
3. Asserts exactly two TX cycles: one for the tone, plus one short startup hiccup.

Mark the integration step as "manual smoke" if writing the test rig is too much in scope.

- [ ] **Step 5: Commit**

```bash
git commit -m "webradio: stop TX during detected silence; re-arm on silence_end"
```

---

## Task 7: Multi-source with stall-detection failover

Replace the single `WEBRADIO_STREAM_URL` with a list. Cycle through them on failure or stall (no frames for `STALL_TIMEOUT`, default 5 s).

**Compatibility:** `WEBRADIO_STREAM_URL` still works — it becomes element 0 of the source list if `WEBRADIO_SOURCES` is empty.

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/service/webradio_bridge.go`
- Add: `internal/service/webradio_sources.go`
- Add: `internal/service/webradio_sources_test.go`

- [ ] **Step 1: Config**

```go
Sources       []string      // ordered list; loaded from WEBRADIO_SOURCES csv, else [StreamURL]
StallTimeout  time.Duration // default 5s
```

`envCSV("WEBRADIO_SOURCES")` then fallback to `[]string{cfg.StreamURL}` if empty.

- [ ] **Step 2: Source rotator**

```go
type sourceRotator struct {
    sources []string
    idx     int
}

func (r *sourceRotator) Current() string { return r.sources[r.idx] }
func (r *sourceRotator) Next() string { r.idx = (r.idx + 1) % len(r.sources); return r.Current() }
```

- [ ] **Step 3: Stall detection in `readEncoderFrames`**

Already tracks `lastFrameTime`. Compare against a `time.NewTicker(stallTimeout / 2)` channel; if `time.Since(lastFrameTime) > StallTimeout`, return a sentinel `errStallTimeout`.

- [ ] **Step 4: Outer loop reacts**

The outer `runSession` (or whatever wraps `readEncoderFrames`) catches `errStallTimeout`, calls `rotator.Next()`, logs the failover, and restarts ffmpeg with the new URL. No reconnect-delay applied on stall — we want the next source up immediately.

- [ ] **Step 5: Tests**

`sourceRotator` round-trip; stall-detection regex over canned log lines from step 6 in task 5.

- [ ] **Step 6: Commit**

```bash
git commit -m "webradio: source list + stall-detection failover (WEBRADIO_SOURCES csv)"
```

---

## Task 8: Live control HTTP endpoints

Three minimal endpoints, mounted on the main service like `/api/webradio/status`:

- `POST /api/webradio/skip` — manual failover to the next source.
- `POST /api/webradio/mute` / `unmute` — pause/resume the brew TX (filter chain keeps running so loudnorm doesn't have to reconverge).
- `POST /api/webradio/reload` — re-read the env (process-restart-equivalent for the audio chain) **without dropping the brew session**. Restarts ffmpeg + encoder under a new callID.

Auth: same as the rest of the admin API (assumed protected by whatever sits in front of the freetetra HTTP server; this PR doesn't add auth — call out in the README).

**Files:**
- Modify: `internal/service/webradio_bridge.go` (expose Skip/Mute/Unmute/Reload methods)
- Modify: `internal/service/service.go` (route registration)
- Add: `internal/service/webradio_control_test.go`

- [ ] **Step 1: Add control channel to the bridge**

```go
type WebRadioBridge struct {
    ...
    control chan controlCmd
    muted   atomic.Bool
}
```

Where `controlCmd` is one of `skip|mute|unmute|reload`. The bridge's main loop selects on `control` alongside the existing `errCh` and `ctx.Done()`.

- [ ] **Step 2: Methods + handlers**

```go
func (b *WebRadioBridge) Skip()    { select { case b.control <- ctrlSkip: default: } }
func (b *WebRadioBridge) Mute()    { b.muted.Store(true) }
func (b *WebRadioBridge) Unmute()  { b.muted.Store(false) }
func (b *WebRadioBridge) Reload(cfg config.Config) { ... }
```

- [ ] **Step 3: HTTP routing**

In `service.go`, add the three routes. Each returns the status snapshot post-action for easy scripting.

- [ ] **Step 4: Tests**

Mute → next `InjectedVoiceFrame` is skipped (verify via a mock plane). Skip → source rotator advances. Reload → ffmpeg `exec.CommandContext` is invoked with the new args.

- [ ] **Step 5: Commit**

```bash
git commit -m "webradio: live control endpoints (skip / mute / unmute / reload)"
```

---

## Open questions (decide before starting)

- **Loudness target.** Plan defaults to `-16 LUFS`. Voice-only deployments might prefer `-14`; broadcast-rule purists want `-23`. Confirm with the operator(s) before flipping `WEBRADIO_LOUDNORM_MODE=single` in production.
- **Filter chain ordering.** The plan puts `loudnorm` *before* HPF/LPF so the normalizer sees the broadband signal. If field tests show better results with HPF/LPF first (less low-frequency rumble inflating LUFS), swap the order in task 3.
- **Profile presets.** The plan keeps a flat env-driven chain. If operators end up with different settings for talk vs music streams, fold that into a follow-up that introduces a `WEBRADIO_PROFILE=<name>` lookup mapping to bundled defaults. Out of scope for v1.
- **CPU budget.** `loudnorm` + `soxr` together typically run ~10-15% of one core per stream on a modern VPS. If multiple webradio containers share a small host, document the cost in the README.
- **Hot reload (task 8) and audio glitches.** Restarting ffmpeg under a held brew session is briefly silent (~200 ms). If that's audible enough to matter, cross-fade between old + new ffmpeg via two parallel pipes — that's complex and not in this plan.
