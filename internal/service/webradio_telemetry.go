package service

import (
	"regexp"
	"strconv"
	"sync"
	"time"
)

// webradioTelemetry holds the bridge's live observability state. It's
// populated from ffmpeg's stderr (silencedetect events, future loudnorm /
// ebur128 stats) and exposed via /api/webradio/status. All fields are
// guarded by `mu`; readers use Snapshot() which returns a defensive copy.
type webradioTelemetry struct {
	mu             sync.RWMutex
	silence        bool
	silenceStartAt time.Time // wall-clock time when current silence began
	silencePos     float64   // ffmpeg's reported stream position at silence_start
	silenceDur     float64   // duration reported on silence_end (seconds)

	// Loudness fields are present so /api/webradio/status has a stable
	// shape from day one; they stay zero-valued until a future task adds
	// an ebur128 stage that fills them.
	loudnessM    float64 // momentary LUFS (400ms window)
	loudnessS    float64 // short-term LUFS (3s window)
	loudnessI    float64 // integrated LUFS (running mean)
	truePeakDBTP float64
	lastLoudness time.Time
}

// telemetrySnapshot is the JSON-friendly shape returned by status endpoints.
type telemetrySnapshot struct {
	Silence        bool      `json:"silence"`
	SilenceStartAt time.Time `json:"silence_start_at,omitempty"`
	SilencePos     float64   `json:"silence_pos,omitempty"`
	SilenceDur     float64   `json:"silence_dur,omitempty"`
	LoudnessM      float64   `json:"loudness_m_lufs,omitempty"`
	LoudnessS      float64   `json:"loudness_s_lufs,omitempty"`
	LoudnessI      float64   `json:"loudness_i_lufs,omitempty"`
	TruePeakDBTP   float64   `json:"true_peak_dbtp,omitempty"`
}

func (t *webradioTelemetry) Snapshot() telemetrySnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return telemetrySnapshot{
		Silence:        t.silence,
		SilenceStartAt: t.silenceStartAt,
		SilencePos:     t.silencePos,
		SilenceDur:     t.silenceDur,
		LoudnessM:      t.loudnessM,
		LoudnessS:      t.loudnessS,
		LoudnessI:      t.loudnessI,
		TruePeakDBTP:   t.truePeakDBTP,
	}
}

// IsSilent is a fast-path accessor for the hot frame loop (task 6) that
// avoids the snapshot copy.
func (t *webradioTelemetry) IsSilent() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.silence
}

var (
	silenceStartRe = regexp.MustCompile(`silence_start:\s*([-\d.]+)`)
	silenceEndRe   = regexp.MustCompile(`silence_end:\s*([-\d.]+)\s*\|\s*silence_duration:\s*([-\d.]+)`)
)

// ParseLine ingests one stderr line and updates state if it matches a known
// telemetry pattern. Returns true if the line was recognized (the caller may
// then choose to suppress it from the default logger, or just leave it).
func (t *webradioTelemetry) ParseLine(line string) bool {
	if m := silenceStartRe.FindStringSubmatch(line); m != nil {
		pos, _ := strconv.ParseFloat(m[1], 64)
		t.mu.Lock()
		t.silence = true
		t.silenceStartAt = time.Now()
		t.silencePos = pos
		t.silenceDur = 0
		t.mu.Unlock()
		return true
	}
	if m := silenceEndRe.FindStringSubmatch(line); m != nil {
		dur, _ := strconv.ParseFloat(m[2], 64)
		t.mu.Lock()
		t.silence = false
		t.silenceDur = dur
		t.mu.Unlock()
		return true
	}
	return false
}
