package service

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/freetetra/server/internal/config"
)

func newWebRadioBridgeForControl(t *testing.T, sources []string) *WebRadioBridge {
	t.Helper()
	cfg := config.Config{}
	cfg.WebRadio = config.WebRadioConfig{
		Sources:    sources,
		Talkgroup:  10,
		FFmpegBin:  "ffmpeg",
		EncoderBin: "tetra-acelp-stdio",
	}
	logger := log.New(io.Discard, "", 0)
	bridge, err := NewWebRadioBridge(cfg, logger, nil)
	if err != nil {
		t.Fatalf("NewWebRadioBridge: %v", err)
	}
	return bridge
}

func TestWebRadioControl_MuteUnmuteFlag(t *testing.T) {
	b := newWebRadioBridgeForControl(t, []string{"https://a/", "https://b/"})
	if b.Muted() {
		t.Fatalf("fresh bridge should not be muted")
	}
	b.Mute()
	if !b.Muted() {
		t.Errorf("Muted() should report true after Mute()")
	}
	b.Unmute()
	if b.Muted() {
		t.Errorf("Muted() should report false after Unmute()")
	}
}

func TestWebRadioControl_SkipNoSessionIsNoop(t *testing.T) {
	// Skip with no live session must not panic — sessionCancel is nil
	// before runSession runs.
	b := newWebRadioBridgeForControl(t, []string{"https://a/"})
	b.Skip()
	b.Reload()
}

func TestWebRadioControl_HandleStatusReportsState(t *testing.T) {
	b := newWebRadioBridgeForControl(t, []string{"https://primary/", "https://backup/"})
	b.Mute()

	rec := httptest.NewRecorder()
	b.handleStatus(rec, httptest.NewRequest(http.MethodGet, "/api/webradio/status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body.String())
	}
	if resp["muted"] != true {
		t.Errorf("muted = %v, want true", resp["muted"])
	}
	if resp["current_source"] != "https://primary/" {
		t.Errorf("current_source = %v, want primary", resp["current_source"])
	}
	if got, _ := resp["sources"].(float64); int(got) != 2 {
		t.Errorf("sources count = %v, want 2", resp["sources"])
	}
}

func TestWebRadioControl_HandleControlRejectsGET(t *testing.T) {
	b := newWebRadioBridgeForControl(t, []string{"https://a/"})
	h := b.handleControl(b.Mute)
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/api/webradio/mute", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status %d, want 405", rec.Code)
	}
	if b.Muted() {
		t.Errorf("Mute action must not have run on rejected request")
	}
}


func TestNormalizeRadioFramePairs18ByteCodecFrames(t *testing.T) {
	var pending []byte

	frameA := make([]byte, 18)
	frameB := make([]byte, 18)
	frameA[0] = 0x80 // first codec bit = 1

	ste, ready, err := normalizeRadioFrame(frameA, &pending)
	if err != nil {
		t.Fatalf("normalizeRadioFrame(frameA) error: %v", err)
	}
	if ready || ste != nil {
		t.Fatalf("first 18-byte frame should not emit output yet")
	}

	ste, ready, err = normalizeRadioFrame(frameB, &pending)
	if err != nil {
		t.Fatalf("normalizeRadioFrame(frameB) error: %v", err)
	}
	if !ready {
		t.Fatalf("second 18-byte frame should emit a STE frame")
	}
	if len(ste) != 36 {
		t.Fatalf("unexpected STE length: %d", len(ste))
	}
	if ste[0] != 0x80 {
		t.Fatalf("unexpected STE header: 0x%02x", ste[0])
	}
	if ste[1] != 0x80 {
		t.Fatalf("unexpected first payload byte: 0x%02x", ste[1])
	}
	if ste[35]&0x3f != 0x3f {
		t.Fatalf("unexpected STE tail fill bits: 0x%02x", ste[35])
	}
}

func TestNormalizeRadioFrame35ByteCodecPayload(t *testing.T) {
	var pending []byte
	frame := make([]byte, 35)
	frame[0] = 0xAB

	ste, ready, err := normalizeRadioFrame(frame, &pending)
	if err != nil {
		t.Fatalf("normalizeRadioFrame error: %v", err)
	}
	if !ready {
		t.Fatalf("35-byte frame should emit output")
	}
	if len(ste) != 36 {
		t.Fatalf("unexpected STE length: %d", len(ste))
	}
	if ste[0] != 0x80 || ste[1] != 0xAB {
		t.Fatalf("unexpected STE conversion: %02x %02x", ste[0], ste[1])
	}
	if ste[35]&0x3f != 0x3f {
		t.Fatalf("unexpected STE tail fill bits: 0x%02x", ste[35])
	}
}

func TestNormalizeRadioFrameRejectsUnknownFrameSize(t *testing.T) {
	var pending []byte
	_, ready, err := normalizeRadioFrame(make([]byte, 19), &pending)
	if err == nil {
		t.Fatalf("expected error for unsupported frame size")
	}
	if ready {
		t.Fatalf("unsupported frame should not be ready")
	}
}
