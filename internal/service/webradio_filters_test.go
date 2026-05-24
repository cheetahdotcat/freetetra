package service

import (
	"testing"

	"github.com/freetetra/server/internal/config"
)

func TestBuildWebRadioFilterChain_DefaultsMatchLegacy(t *testing.T) {
	// The legacy hardcoded chain was:
	//   volume=-14dB,acompressor=threshold=-20dB:ratio=4:attack=5:release=50
	// With config defaults loaded by LoadFromEnv (VolumeDB=-14, default
	// Compressor) the builder must reproduce it byte-for-byte so the audio
	// pipeline stays identical until a later task knowingly changes it.
	cfg := config.WebRadioConfig{
		VolumeDB:   -14,
		Compressor: "acompressor=threshold=-20dB:ratio=4:attack=5:release=50",
	}
	got := BuildWebRadioFilterChain(cfg)
	want := "volume=-14dB,acompressor=threshold=-20dB:ratio=4:attack=5:release=50"
	if got != want {
		t.Errorf("default chain mismatch:\n got = %q\nwant = %q", got, want)
	}
}

func TestBuildWebRadioFilterChain_EmptyConfigYieldsEmptyString(t *testing.T) {
	got := BuildWebRadioFilterChain(config.WebRadioConfig{})
	if got != "" {
		t.Errorf("empty config produced %q, want empty string", got)
	}
}

func TestBuildWebRadioFilterChain_VolumeOmittedAtZero(t *testing.T) {
	cfg := config.WebRadioConfig{Compressor: "acompressor=ratio=2"}
	got := BuildWebRadioFilterChain(cfg)
	want := "acompressor=ratio=2"
	if got != want {
		t.Errorf("zero VolumeDB should omit volume=; got %q want %q", got, want)
	}
}

func TestBuildWebRadioFilterChain_ExtraFiltersAppendedLast(t *testing.T) {
	cfg := config.WebRadioConfig{
		VolumeDB:     -10,
		Compressor:   "acompressor=ratio=3",
		ExtraFilters: "aecho=0.8:0.9:1000:0.3",
	}
	got := BuildWebRadioFilterChain(cfg)
	want := "volume=-10dB,acompressor=ratio=3,aecho=0.8:0.9:1000:0.3"
	if got != want {
		t.Errorf("extra filters position wrong:\n got = %q\nwant = %q", got, want)
	}
}

func TestBuildWebRadioFilterChain_CompressorOptional(t *testing.T) {
	cfg := config.WebRadioConfig{VolumeDB: -6}
	got := BuildWebRadioFilterChain(cfg)
	want := "volume=-6dB"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

func TestBuildWebRadioFilterChain_SpeechBand(t *testing.T) {
	cfg := config.WebRadioConfig{HPFHz: 120, LPFHz: 3500}
	got := BuildWebRadioFilterChain(cfg)
	want := "highpass=f=120,lowpass=f=3500"
	if got != want {
		t.Errorf("speech-band chain mismatch:\n got = %q\nwant = %q", got, want)
	}
}

func TestBuildWebRadioFilterChain_ResamplerAppendedLast(t *testing.T) {
	cfg := config.WebRadioConfig{
		VolumeDB:     -14,
		Compressor:   "acompressor=ratio=3",
		HPFHz:        120,
		ExtraFilters: "extra=1",
		Resampler:    "soxr",
	}
	got := BuildWebRadioFilterChain(cfg)
	want := "volume=-14dB,acompressor=ratio=3,highpass=f=120,extra=1,aresample=resampler=soxr"
	if got != want {
		t.Errorf("resampler position wrong:\n got = %q\nwant = %q", got, want)
	}
}

func TestBuildWebRadioFilterChain_LoudnormSinglePass(t *testing.T) {
	cfg := config.WebRadioConfig{
		LoudnormMode: "single",
		LoudnormI:    -16,
		LoudnormTP:   -1.5,
		LoudnormLRA:  11,
	}
	got := BuildWebRadioFilterChain(cfg)
	want := "loudnorm=I=-16:TP=-1.5:LRA=11"
	if got != want {
		t.Errorf("loudnorm mismatch:\n got = %q\nwant = %q", got, want)
	}
}

func TestBuildWebRadioFilterChain_LoudnormOffOmitsFilter(t *testing.T) {
	cfg := config.WebRadioConfig{
		LoudnormMode: "off",
		LoudnormI:    -16,
		VolumeDB:     -10,
	}
	got := BuildWebRadioFilterChain(cfg)
	if got != "volume=-10dB" {
		t.Errorf("loudnorm mode=off must not emit filter; got %q", got)
	}
}

func TestBuildWebRadioFilterChain_LoudnormPositionBeforeHPF(t *testing.T) {
	// Position matters: loudnorm needs broadband audio to measure LUFS
	// correctly, so it has to sit BEFORE the HPF/LPF stages.
	cfg := config.WebRadioConfig{
		Compressor:   "acompressor=ratio=3",
		LoudnormMode: "single",
		LoudnormI:    -16,
		LoudnormTP:   -1.5,
		LoudnormLRA:  11,
		HPFHz:        120,
	}
	got := BuildWebRadioFilterChain(cfg)
	want := "acompressor=ratio=3,loudnorm=I=-16:TP=-1.5:LRA=11,highpass=f=120"
	if got != want {
		t.Errorf("loudnorm ordering wrong:\n got = %q\nwant = %q", got, want)
	}
}

func TestBuildWebRadioFilterChain_LimiterIsLast(t *testing.T) {
	cfg := config.WebRadioConfig{
		VolumeDB:     -14,
		Resampler:    "soxr",
		LimiterDBFS:  -0.5,
	}
	got := BuildWebRadioFilterChain(cfg)
	// 10^(-0.5/20) ≈ 0.944061
	want := "volume=-14dB,aresample=resampler=soxr,alimiter=level_in=1:level_out=1:limit=0.944061"
	if got != want {
		t.Errorf("limiter chain mismatch:\n got = %q\nwant = %q", got, want)
	}
}

func TestBuildWebRadioFilterChain_LimiterDisabledAtZero(t *testing.T) {
	cfg := config.WebRadioConfig{VolumeDB: -10, LimiterDBFS: 0}
	got := BuildWebRadioFilterChain(cfg)
	if got != "volume=-10dB" {
		t.Errorf("zero/positive LimiterDBFS must disable alimiter; got %q", got)
	}
}

func TestBuildWebRadioFilterChain_HPFLPFInsertedBetweenCompressorAndExtra(t *testing.T) {
	// HPF/LPF must sit between dynamics and any user-supplied tail so that
	// codec-bound band-limiting is the operator's last *implicit* step
	// before whatever escape-hatch filter they tack on.
	cfg := config.WebRadioConfig{
		Compressor:   "acompressor=ratio=2",
		HPFHz:        120,
		LPFHz:        3500,
		ExtraFilters: "atempo=1.0",
	}
	got := BuildWebRadioFilterChain(cfg)
	want := "acompressor=ratio=2,highpass=f=120,lowpass=f=3500,atempo=1.0"
	if got != want {
		t.Errorf("order mismatch:\n got = %q\nwant = %q", got, want)
	}
}
