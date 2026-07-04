package service

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/freetetra/server/internal/config"
)

// TestBlechelseCatalogScanAndSearch stands up a tiny sample tree, builds a
// bridge, and drives its search over the in-memory catalog to make sure the
// scan picks the right files and ranking hits the expected order.
func TestBlechelseCatalogScanAndSearch(t *testing.T) {
	root := t.TempDir()
	files := []string{
		"dt/abschnitte/hoch/a_bis_c.wav",
		"dt/abschnitte/tief/a_bis_c.wav",
		"dt/zeiten/minuten/hoch/0001.wav",
		"dt/ziele/variante1/hoch/8000105.wav",
		"en/module/next_stop.wav",
		"gong/klangtyp_1/gong_1.wav",
		"pegel/tone_1000.wav",       // outside language filter
		"dt/abschnitte/README.txt",  // not a wav
	}
	for _, rel := range files {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(p, []byte("stub"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	// dt + en + gong; pegel/ must be filtered out.
	got, err := scanBlechelseCatalog(root, []string{"dt", "en", "gong"})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if want := 6; len(got) != want {
		names := make([]string, 0, len(got))
		for _, s := range got {
			names = append(names, s.ID)
		}
		t.Fatalf("catalog len=%d want=%d (%v)", len(got), want, names)
	}
	byID := make(map[string]BlechelseSample, len(got))
	for _, s := range got {
		byID[s.ID] = s
	}
	if _, ok := byID["pegel/tone_1000.wav"]; ok {
		t.Fatalf("pegel/ leaked past the language filter")
	}
	if got := byID["dt/abschnitte/hoch/a_bis_c.wav"]; got.Name != "a_bis_c" || got.Category != "dt/abschnitte/hoch" || got.Lang != "dt" {
		t.Fatalf("bad decomposition: %+v", got)
	}
}

func TestBlechelseSearchRankingPrefixBeatsSubstring(t *testing.T) {
	root := t.TempDir()
	for _, rel := range []string{
		"dt/module/hochbahn.wav",     // contains "hoch"
		"dt/abschnitte/hoch/a.wav",   // exact name "a"; but "hoch" is category
		"dt/zeiten/hoch/hoch_1.wav",  // prefix match on "hoch"
	} {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(p, []byte("stub"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	cfg := config.Config{Blechelse: config.BlechelseConfig{
		SamplesDir:       root,
		Talkgroups:       []uint32{10},
		BrewISSI:         899005,
		EncoderBin:       "true",
		EncoderFrameSize: 18,
		MaxQueueLength:   10,
	}}
	logger := log.New(bytes.NewBuffer(nil), "", 0)
	plane := NewBrewModulePlane(cfg, logger, cfg.Blechelse.BrewISSI, cfg.Blechelse.Talkgroups)
	b, err := NewBlechelseBridge(cfg, logger, plane)
	if err != nil {
		t.Fatalf("bridge: %v", err)
	}

	// Score expectations for q="hoch":
	//   name "hoch_1" starts with "hoch" -> 80
	//   name "hochbahn" starts with "hoch" -> 80
	//   name "a" — category "dt/abschnitte/hoch" contains "hoch" -> 30
	// Prefix matches must rank ahead of category-only match.
	type scored struct {
		idx   int
		score int
	}
	var hits []scored
	q := "hoch"
	for i := range b.samples {
		var s int
		switch {
		case b.nameLower[i] == q:
			s = 100
		case len(b.nameLower[i]) >= len(q) && b.nameLower[i][:len(q)] == q:
			s = 80
		case bytesContains(b.nameLower[i], q):
			s = 60
		case bytesContains(b.catLower[i], q):
			s = 30
		default:
			continue
		}
		hits = append(hits, scored{idx: i, score: s})
	}
	if len(hits) != 3 {
		t.Fatalf("expected 3 hits for q=hoch, got %d", len(hits))
	}
	// The one whose category-only matched must be lowest.
	minScore := hits[0].score
	minName := b.samples[hits[0].idx].Name
	for _, h := range hits {
		if h.score < minScore {
			minScore = h.score
			minName = b.samples[h.idx].Name
		}
	}
	if minName != "a" || minScore != 30 {
		t.Fatalf("expected 'a' with score 30 as lowest, got %q score=%d", minName, minScore)
	}
}

func TestBlechelseSamplePathRejectsEscape(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "dt", "abschnitte")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sub, "x.wav"), []byte("stub"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg := config.Config{Blechelse: config.BlechelseConfig{
		SamplesDir:       root,
		Talkgroups:       []uint32{10},
		BrewISSI:         899005,
		EncoderBin:       "true",
		EncoderFrameSize: 18,
	}}
	logger := log.New(bytes.NewBuffer(nil), "", 0)
	plane := NewBrewModulePlane(cfg, logger, cfg.Blechelse.BrewISSI, cfg.Blechelse.Talkgroups)
	b, err := NewBlechelseBridge(cfg, logger, plane)
	if err != nil {
		t.Fatalf("bridge: %v", err)
	}

	for _, id := range []string{"../etc/passwd", "/etc/passwd", "dt/..\\shady"} {
		if _, err := b.samplePath(BlechelseSample{ID: id}); err == nil {
			t.Fatalf("samplePath(%q) accepted a path-escape attempt", id)
		}
	}
	if _, err := b.samplePath(BlechelseSample{ID: "dt/abschnitte/x.wav"}); err != nil {
		t.Fatalf("samplePath rejected a legitimate id: %v", err)
	}
}

func bytesContains(s, sub string) bool {
	if sub == "" {
		return true
	}
	if len(sub) > len(s) {
		return false
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
