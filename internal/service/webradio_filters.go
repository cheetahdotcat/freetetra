package service

import (
	"fmt"
	"math"
	"strings"

	"github.com/freetetra/server/internal/config"
)

// filterChain accumulates ffmpeg `-af` filter expressions in order. Empty
// entries are skipped so optional stages can `add` unconditionally.
type filterChain struct{ parts []string }

func (f *filterChain) add(expr string) {
	if e := strings.TrimSpace(expr); e != "" {
		f.parts = append(f.parts, e)
	}
}

func (f *filterChain) String() string {
	return strings.Join(f.parts, ",")
}

// BuildWebRadioFilterChain returns the value passed to ffmpeg's `-af` flag.
// The chain is intentionally linear and order-sensitive: volume → dynamics
// → extra filters. Each stage is independently togglable from config so
// later phases (HPF/LPF, loudnorm, limiter) can slot in without touching
// callers.
func BuildWebRadioFilterChain(cfg config.WebRadioConfig) string {
	fc := &filterChain{}
	if cfg.VolumeDB != 0 {
		fc.add(fmt.Sprintf("volume=%gdB", cfg.VolumeDB))
	}
	fc.add(cfg.Compressor)
	// loudnorm sits before band-limiting: it should see the broadband
	// signal so its LUFS measurement reflects what listeners hear, not
	// just the speech-band slice.
	if strings.EqualFold(strings.TrimSpace(cfg.LoudnormMode), "single") {
		fc.add(fmt.Sprintf("loudnorm=I=%g:TP=%g:LRA=%g", cfg.LoudnormI, cfg.LoudnormTP, cfg.LoudnormLRA))
	}
	if cfg.HPFHz > 0 {
		fc.add(fmt.Sprintf("highpass=f=%d", cfg.HPFHz))
	}
	if cfg.LPFHz > 0 {
		fc.add(fmt.Sprintf("lowpass=f=%d", cfg.LPFHz))
	}
	fc.add(cfg.ExtraFilters)
	if r := strings.TrimSpace(cfg.Resampler); r != "" {
		fc.add(fmt.Sprintf("aresample=resampler=%s", r))
	}
	// Limiter is the very last stage so it catches anything the resampler
	// pushes back above the codec's safe input range. dBFS → linear via
	// the standard 20·log10 conversion.
	if cfg.LimiterDBFS < 0 {
		lin := math.Pow(10, cfg.LimiterDBFS/20.0)
		fc.add(fmt.Sprintf("alimiter=level_in=1:level_out=1:limit=%.6f", lin))
	}
	// silencedetect runs purely as a metadata sink — it doesn't alter audio,
	// just emits "silence_start"/"silence_end" lines to ffmpeg stderr that
	// the bridge's telemetry parser scrapes. Kept at the very end so it sees
	// the final post-limiter signal.
	if cfg.Silencedetect {
		fc.add(fmt.Sprintf("silencedetect=noise=%ddB:d=%g", cfg.SilenceNoiseDB, cfg.SilenceMinDur.Seconds()))
	}
	return fc.String()
}
