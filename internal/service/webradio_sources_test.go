package service

import (
	"errors"
	"testing"

	"github.com/freetetra/server/internal/config"
)

func TestSourceRotator_LegacySingleURL(t *testing.T) {
	r := newSourceRotator(config.WebRadioConfig{StreamURL: "https://a/"})
	if r.Len() != 1 {
		t.Fatalf("len = %d, want 1", r.Len())
	}
	if got := r.Current(); got != "https://a/" {
		t.Errorf("Current = %q, want https://a/", got)
	}
	// Skip on a single-element list wraps to itself.
	if got := r.Skip(); got != "https://a/" {
		t.Errorf("Skip on len=1 returned %q, want https://a/", got)
	}
}

func TestSourceRotator_MultipleSourcesRoundRobin(t *testing.T) {
	r := newSourceRotator(config.WebRadioConfig{
		Sources: []string{"https://a/", "https://b/", "https://c/"},
	})
	if r.Len() != 3 {
		t.Fatalf("len = %d, want 3", r.Len())
	}
	want := []string{"https://b/", "https://c/", "https://a/", "https://b/"}
	for i, w := range want {
		if got := r.Skip(); got != w {
			t.Errorf("Skip #%d = %q, want %q", i+1, got, w)
		}
	}
}

func TestSourceRotator_SourcesOverrideStreamURL(t *testing.T) {
	r := newSourceRotator(config.WebRadioConfig{
		StreamURL: "https://legacy/",
		Sources:   []string{"https://new1/", "https://new2/"},
	})
	if got := r.Current(); got != "https://new1/" {
		t.Errorf("expected sources to override StreamURL, got Current=%q", got)
	}
	if r.Len() != 2 {
		t.Errorf("len = %d, want 2 (StreamURL must not be appended)", r.Len())
	}
}

func TestSourceRotator_TrimsBlankEntries(t *testing.T) {
	r := newSourceRotator(config.WebRadioConfig{
		Sources: []string{"  ", "https://a/", "", "  https://b/  "},
	})
	if r.Len() != 2 {
		t.Errorf("len = %d, want 2 after trimming blanks", r.Len())
	}
	if got := r.Current(); got != "https://a/" {
		t.Errorf("Current = %q, want https://a/", got)
	}
}

func TestSourceRotator_EmptyConfigYieldsEmptyRotator(t *testing.T) {
	r := newSourceRotator(config.WebRadioConfig{})
	if r.Len() != 0 {
		t.Errorf("expected empty rotator, len = %d", r.Len())
	}
	if got := r.Current(); got != "" {
		t.Errorf("Current on empty rotator = %q, want \"\"", got)
	}
}

func TestSentinels_AreDistinctButComparableViaErrorsIs(t *testing.T) {
	// runLoop differentiates Stall / Skip / Reload via errors.Is dispatches.
	// Lock the wiring with explicit assertions so an accidental aliasing
	// (e.g. someone defining errManualSkip = errStallTimeout) is caught.
	if errStallTimeout.Error() == errManualSkip.Error() ||
		errStallTimeout.Error() == errManualReload.Error() ||
		errManualSkip.Error() == errManualReload.Error() {
		t.Fatal("sentinels must have distinct error strings")
	}
	if errors.Is(errStallTimeout, errManualSkip) {
		t.Error("errStallTimeout must not match errManualSkip")
	}
	if errors.Is(errManualReload, errStallTimeout) {
		t.Error("errManualReload must not match errStallTimeout")
	}
}
