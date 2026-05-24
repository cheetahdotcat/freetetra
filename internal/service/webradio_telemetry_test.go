package service

import (
	"testing"
)

func TestTelemetry_ParsesSilenceStartAndEnd(t *testing.T) {
	tel := &webradioTelemetry{}

	if tel.IsSilent() {
		t.Fatalf("initial state should not be silent")
	}

	// Real ffmpeg output sample.
	line := "[silencedetect @ 0x7f12cb] silence_start: 12.345"
	if !tel.ParseLine(line) {
		t.Fatalf("ParseLine(%q) returned false; want true", line)
	}
	snap := tel.Snapshot()
	if !snap.Silence {
		t.Errorf("expected Silence=true after silence_start; got %+v", snap)
	}
	if snap.SilencePos != 12.345 {
		t.Errorf("SilencePos = %v, want 12.345", snap.SilencePos)
	}
	if snap.SilenceStartAt.IsZero() {
		t.Errorf("SilenceStartAt should be set")
	}

	endLine := "[silencedetect @ 0x7f12cb] silence_end: 14.567 | silence_duration: 2.222"
	if !tel.ParseLine(endLine) {
		t.Fatalf("ParseLine(%q) returned false; want true", endLine)
	}
	snap = tel.Snapshot()
	if snap.Silence {
		t.Errorf("expected Silence=false after silence_end; got %+v", snap)
	}
	if snap.SilenceDur != 2.222 {
		t.Errorf("SilenceDur = %v, want 2.222", snap.SilenceDur)
	}
}

func TestTelemetry_IgnoresUnrelatedLines(t *testing.T) {
	tel := &webradioTelemetry{}
	if tel.ParseLine("Press [q] to stop") {
		t.Errorf("unrelated line should return false")
	}
	if tel.IsSilent() {
		t.Errorf("state must not flip on unrelated lines")
	}
}

func TestTelemetry_SnapshotIsCopy(t *testing.T) {
	tel := &webradioTelemetry{}
	tel.ParseLine("[silencedetect] silence_start: 1.0")
	snap1 := tel.Snapshot()
	snap1.Silence = false // mutate the copy
	if !tel.IsSilent() {
		t.Errorf("mutating snapshot must not affect bridge state")
	}
}
