package service

import (
	"io"
	"log"
	"testing"
	"time"
)

func testStore(t *testing.T) *stationStore {
	t.Helper()
	t.Chdir(t.TempDir())
	return newStationStore(log.New(io.Discard, "", 0), stationStoreConfig{
		OnlineWindow: time.Minute,
		StaleAfter:   24 * time.Hour,
		ReapInterval: time.Hour, // not exercised in tests
	})
}

func sampleStation(id, callsign string) Station {
	return Station{
		StationID: id,
		Callsign:  callsign,
		Type:      "hotspot",
		Lat:       52.0,
		Lon:       13.0,
	}
}

func TestUpsertAndByISSI(t *testing.T) {
	s := testStore(t)
	in := sampleStation("a-1", "DO1AAA")
	in.OwnedISSIs = []uint32{1001, 1002}
	saved, err := s.Upsert(in)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if saved.FirstSeenUnix == 0 || saved.LastSeenUnix == 0 {
		t.Fatalf("timestamps not set: %+v", saved)
	}
	got, ok := s.ByISSI(1001)
	if !ok || got.StationID != "a-1" {
		t.Fatalf("ByISSI(1001) = %+v ok=%v; want station a-1", got, ok)
	}
	got, ok = s.ByISSI(1002)
	if !ok || got.StationID != "a-1" {
		t.Fatalf("ByISSI(1002) = %+v ok=%v; want station a-1", got, ok)
	}
	if _, ok := s.ByISSI(9999); ok {
		t.Fatalf("ByISSI(9999) should be missing")
	}
	if _, ok := s.ByISSI(0); ok {
		t.Fatalf("ByISSI(0) should always be missing")
	}
}

func TestUpsertRejectsInvalidISSI(t *testing.T) {
	s := testStore(t)
	in := sampleStation("a-1", "DO1AAA")
	in.OwnedISSIs = []uint32{0x01000000} // 1 over max
	if _, err := s.Upsert(in); err == nil {
		t.Fatalf("expected rejection for ISSI > maxValidISSI")
	}
	in.OwnedISSIs = []uint32{0}
	if _, err := s.Upsert(in); err == nil {
		t.Fatalf("expected rejection for ISSI 0")
	}
}

func TestDeleteCreatesTombstone(t *testing.T) {
	s := testStore(t)
	in := sampleStation("a-1", "DO1AAA")
	in.OwnedISSIs = []uint32{1001}
	if _, err := s.Upsert(in); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	tomb, err := s.Delete("a-1")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if tomb.DeletedUnix == 0 {
		t.Fatalf("tombstone missing DeletedUnix")
	}
	if got := len(s.All()); got != 0 {
		t.Fatalf("All() = %d entries after delete; want 0", got)
	}
	all := s.AllIncludingDeleted()
	if len(all) != 1 || all[0].DeletedUnix == 0 {
		t.Fatalf("AllIncludingDeleted = %+v; want 1 tombstone", all)
	}
	if _, ok := s.ByISSI(1001); ok {
		t.Fatalf("ByISSI should not find a tombstoned station")
	}
}

func TestReapDropsOldTombstones(t *testing.T) {
	s := testStore(t)
	in := sampleStation("a-1", "DO1AAA")
	if _, err := s.Upsert(in); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if _, err := s.Delete("a-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Backdate the tombstone past staleAfter so Reap removes it.
	s.mu.Lock()
	s.items["a-1"].DeletedUnix = time.Now().Add(-48 * time.Hour).Unix()
	s.mu.Unlock()

	if removed := s.Reap(time.Now()); removed != 1 {
		t.Fatalf("Reap removed %d; want 1", removed)
	}
	if len(s.AllIncludingDeleted()) != 0 {
		t.Fatalf("tombstone still present after reap")
	}
}

func TestReapKeepsFreshTombstones(t *testing.T) {
	s := testStore(t)
	in := sampleStation("a-1", "DO1AAA")
	if _, err := s.Upsert(in); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if _, err := s.Delete("a-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if removed := s.Reap(time.Now()); removed != 0 {
		t.Fatalf("Reap removed %d; want 0 (tombstone is fresh)", removed)
	}
}

func TestFederatedOriginPreserved(t *testing.T) {
	s := testStore(t)
	in := sampleStation("a-1", "DO1AAA")
	in.Origin = "peer-b"
	saved, err := s.Upsert(in)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if saved.Origin != "peer-b" {
		t.Fatalf("origin lost: %+v", saved)
	}
	// Locally-pushed (empty origin) should round-trip as empty.
	in2 := sampleStation("a-2", "DO1BBB")
	saved2, err := s.Upsert(in2)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if saved2.Origin != "" {
		t.Fatalf("local push picked up unexpected origin %q", saved2.Origin)
	}
}

func TestISSIConflictNewerWins(t *testing.T) {
	s := testStore(t)
	older := sampleStation("a-1", "DO1AAA")
	older.LastSeenUnix = time.Now().Add(-time.Hour).Unix()
	older.OwnedISSIs = []uint32{1001}
	if _, err := s.Upsert(older); err != nil {
		t.Fatalf("upsert older: %v", err)
	}
	newer := sampleStation("a-2", "DO1BBB")
	newer.LastSeenUnix = time.Now().Unix()
	newer.OwnedISSIs = []uint32{1001}
	if _, err := s.Upsert(newer); err != nil {
		t.Fatalf("upsert newer: %v", err)
	}
	got, ok := s.ByISSI(1001)
	if !ok || got.StationID != "a-2" {
		t.Fatalf("ByISSI(1001) = %+v ok=%v; want a-2 to win on newer last_seen", got, ok)
	}
}

func TestByCallsign(t *testing.T) {
	s := testStore(t)
	if _, ok := s.ByCallsign("DO1AAA"); ok {
		t.Fatalf("ByCallsign on empty store should miss")
	}
	if _, err := s.Upsert(sampleStation("a-1", "DO1AAA")); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, ok := s.ByCallsign("do1aaa") // lowercase input
	if !ok || got.StationID != "a-1" {
		t.Fatalf("ByCallsign(do1aaa) = %+v ok=%v; want a-1", got, ok)
	}
	if _, err := s.Delete("a-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := s.ByCallsign("DO1AAA"); ok {
		t.Fatalf("ByCallsign should not return tombstoned rows")
	}
}

func TestLinkOrCreate_ISSIHit(t *testing.T) {
	s := testStore(t)
	in := sampleStation("a-1", "DO1AAA")
	in.OwnedISSIs = []uint32{1001}
	if _, err := s.Upsert(in); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	st, ok := s.LinkOrCreate(1001, "ignored", "bluestation", false)
	if !ok || st.StationID != "a-1" {
		t.Fatalf("LinkOrCreate(1001) = %+v ok=%v; want a-1", st, ok)
	}
}

func TestLinkOrCreate_CallsignFallback(t *testing.T) {
	s := testStore(t)
	if _, err := s.Upsert(sampleStation("a-1", "DO1AAA")); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	st, ok := s.LinkOrCreate(1001, "DO1AAA", "bluestation", false)
	if !ok || st.StationID != "a-1" {
		t.Fatalf("LinkOrCreate(1001, DO1AAA) = %+v ok=%v; want a-1 extended", st, ok)
	}
	if len(st.OwnedISSIs) != 1 || st.OwnedISSIs[0] != 1001 {
		t.Fatalf("expected OwnedISSIs=[1001], got %v", st.OwnedISSIs)
	}
	if got, ok := s.ByISSI(1001); !ok || got.StationID != "a-1" {
		t.Fatalf("ByISSI after callsign-fallback link missed: %+v ok=%v", got, ok)
	}
}

func TestLinkOrCreate_AutoCreateOn(t *testing.T) {
	s := testStore(t)
	st, ok := s.LinkOrCreate(1001, "DO1NEW", "bluestation", true)
	if !ok {
		t.Fatalf("LinkOrCreate with autoCreate=true returned ok=false")
	}
	if st.Callsign != "DO1NEW" || st.Type != "bluestation" || len(st.OwnedISSIs) != 1 {
		t.Fatalf("stub looks wrong: %+v", st)
	}
	if _, ok := s.ByISSI(1001); !ok {
		t.Fatalf("auto-created station not indexed by ISSI")
	}
}

func TestLinkOrCreate_AutoCreateOff(t *testing.T) {
	s := testStore(t)
	if _, ok := s.LinkOrCreate(1001, "DO1NEW", "bluestation", false); ok {
		t.Fatalf("LinkOrCreate with autoCreate=false unexpectedly succeeded")
	}
	if len(s.All()) != 0 {
		t.Fatalf("expected zero stations after no-op, got %d", len(s.All()))
	}
}

func TestLinkOrCreate_AutoCreateUsesISSIWhenNoCallsign(t *testing.T) {
	s := testStore(t)
	st, ok := s.LinkOrCreate(1001, "", "bluestation", true)
	if !ok {
		t.Fatalf("LinkOrCreate(empty callsign) ok=false")
	}
	if st.Callsign != "1001" {
		t.Fatalf("callsign fallback to ISSI string failed: %+v", st)
	}
}

func TestBumpLastSeen(t *testing.T) {
	s := testStore(t)
	in := sampleStation("a-1", "DO1AAA")
	in.LastSeenUnix = time.Now().Add(-time.Hour).Unix()
	if _, err := s.Upsert(in); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	before := s.items["a-1"].LastSeenUnix
	s.bumpLastSeen("a-1")
	after := s.items["a-1"].LastSeenUnix
	if after <= before {
		t.Fatalf("bumpLastSeen did not advance LastSeenUnix (before=%d after=%d)", before, after)
	}
}

func TestBumpLastSeen_IgnoresTombstone(t *testing.T) {
	s := testStore(t)
	if _, err := s.Upsert(sampleStation("a-1", "DO1AAA")); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if _, err := s.Delete("a-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	tombDeletedAt := s.items["a-1"].DeletedUnix
	s.bumpLastSeen("a-1")
	if s.items["a-1"].DeletedUnix != tombDeletedAt {
		t.Fatalf("bumpLastSeen on tombstone should not change DeletedUnix")
	}
}

func TestUpsertReindexOnISSIChange(t *testing.T) {
	s := testStore(t)
	in := sampleStation("a-1", "DO1AAA")
	in.OwnedISSIs = []uint32{1001, 1002}
	if _, err := s.Upsert(in); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	// Re-push the same station with a different ISSI set; 1001 should
	// disappear from the index and 1003 should appear.
	in.OwnedISSIs = []uint32{1002, 1003}
	if _, err := s.Upsert(in); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	if _, ok := s.ByISSI(1001); ok {
		t.Fatalf("ByISSI(1001) should be gone after re-upsert without it")
	}
	if got, ok := s.ByISSI(1002); !ok || got.StationID != "a-1" {
		t.Fatalf("ByISSI(1002) lost: %+v ok=%v", got, ok)
	}
	if got, ok := s.ByISSI(1003); !ok || got.StationID != "a-1" {
		t.Fatalf("ByISSI(1003) missing after re-upsert: %+v ok=%v", got, ok)
	}
}
