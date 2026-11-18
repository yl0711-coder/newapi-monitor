package monitor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestGroupGovernancePublicationPackRoundTripAndLegacy(t *testing.T) {
	raw := []byte(`{"channels":[` + strings.Repeat(`{"at":1794931200,"requests":0,"success":0,"mean_ms":null},`, 2000) + `{}]}`)
	packed, err := ggPackPublication(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(packed)*10 > len(raw) {
		t.Fatalf("repetitive publication should compress >10x: %d -> %d", len(raw), len(packed))
	}
	got, err := ggUnpackPublication(packed)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("round trip failed: %v", err)
	}
	// Decompression is bounded like the uncompressed publication.
	var big bytes.Buffer
	zw := gzip.NewWriter(&big)
	zw.Write(bytes.Repeat([]byte("a"), ggPublicationMaxBytes+10))
	zw.Close()
	if _, err := ggUnpackPublication(big.Bytes()); err == nil {
		t.Fatal("oversized publication accepted")
	}
	if _, err := ggUnpackPublication([]byte("not gzip")); err == nil {
		t.Fatal("corrupt publication accepted")
	}
}

func ggPublicationFixture(t *testing.T) *Monitor {
	t.Helper()
	m, end := modelFRTFixture(t, true)
	m.cfg.GroupGovernanceEnabled = true
	channel := ggTestChannel(1)
	channel.UpdatedAt = modelFRTTestNow.Unix()
	m.storeDB.Create(&channel)
	m.storeDB.Create(&GroupGovernanceState{ID: 1, LastSuccessAt: modelFRTTestNow.Unix()})
	m.storeDB.Create(&GroupGovernanceGroup{Grp: "g", Current: true})
	r := ggTestRow(end-60, 1, 50, 1000)
	m.storeDB.Create(&CapacityUserMinuteSample{BucketTs: end - 60, UserID: 1, ChannelID: 1, Grp: "g", ModelName: "m", Success: 50, TtftObserved: 50, Ttft1k: 50, TtftMaxMs: 1000, FRTSumMs: &r.FRTSumMs, FRTSumObserved: 50, FRTSumVersion: frtSumSemanticsVersion})
	return m
}

func ggLoadPublication(t *testing.T, m *Monitor) (ggRealtimeView, ggControl) {
	t.Helper()
	v, err := loadGGRealtimeView(m.storeDB)
	if err != nil {
		t.Fatal(err)
	}
	var c ggControl
	if err := json.Unmarshal(v.Control, &c); err != nil {
		t.Fatal(err)
	}
	return v, c
}

func ggPlainRow(t *testing.T, m *Monitor) GroupGovernanceRealtimeState {
	t.Helper()
	var s GroupGovernanceRealtimeState
	if err := m.storeDB.Where("id=?", 1).First(&s).Error; err != nil {
		t.Fatal(err)
	}
	return s
}

// ggOldBinaryRead decodes the plain row exactly as v1.18.66 does.
func ggOldBinaryRead(t *testing.T, m *Monitor) (ggRealtimeReport, ggControl) {
	t.Helper()
	s := ggPlainRow(t, m)
	var r ggRealtimeReport
	var c ggControl
	if err := json.Unmarshal([]byte(s.ReportJSON), &r); err != nil {
		t.Fatalf("old binary cannot read the report: %v", err)
	}
	if err := json.Unmarshal([]byte(s.ControlJSON), &c); err != nil {
		t.Fatalf("old binary cannot read the control: %v", err)
	}
	return r, c
}

func ggAssertHistoriesKept(t *testing.T, before, after ggControl) {
	t.Helper()
	if len(before.Metrics)+len(before.Channels) == 0 {
		t.Fatal("fixture produced no control histories")
	}
	for key, mem := range before.Channels {
		next, ok := after.Channels[key]
		if !ok || len(next.Normal) < len(mem.Normal) {
			t.Fatalf("channel history %q lost: before=%d after=%d ok=%v", key, len(mem.Normal), len(next.Normal), ok)
		}
	}
	for key, mem := range before.Metrics {
		next, ok := after.Metrics[key]
		if !ok || len(next.Normal) < len(mem.Normal) {
			t.Fatalf("metric history %q lost", key)
		}
	}
}

// Upgrading from a v1.18.66 database (plain row only) keeps the prior control
// and does not rewrite the large plain row on the first pass.
func TestGroupGovernanceRealtimeUpgradesLegacyRowWithoutLosingControl(t *testing.T) {
	m := ggPublicationFixture(t)
	if err := m.syncGroupGovernanceRealtime(context.Background(), modelFRTTestNow); err != nil {
		t.Fatal(err)
	}
	_, firstControl := ggLoadPublication(t, m)
	// Turn the database into what v1.18.66 leaves behind: no compressed table row.
	m.storeDB.Where("id=1").Delete(&GroupGovernanceRealtimePublication{})
	m.storeDB.Model(&GroupGovernanceRealtimeState{}).Where("id=1").Update("updated_at", modelFRTTestNow.Unix())
	before := ggPlainRow(t, m)
	if v, err := loadGGRealtimeView(m.storeDB); err != nil || !v.FromLegacy {
		t.Fatalf("plain-only database must read from the plain row: %v %+v", err, v.FromLegacy)
	}
	if err := m.syncGroupGovernanceRealtime(context.Background(), modelFRTTestNow.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	v, secondControl := ggLoadPublication(t, m)
	after := ggPlainRow(t, m)
	if v.FromLegacy || v.Generation != before.Generation+1 {
		t.Fatalf("upgrade did not publish the compressed row: legacy=%v gen=%d", v.FromLegacy, v.Generation)
	}
	if after.Generation != before.Generation || after.ControlJSON != before.ControlJSON {
		t.Fatal("first pass after the upgrade rewrote the large plain row")
	}
	ggAssertHistoriesKept(t, firstControl, secondControl)
}

// Per-pass publications touch only the compressed row; the plain row is
// refreshed every ggLegacyCopyEvery and stays decodable by v1.18.66.
func TestGroupGovernanceRealtimePlainCopyCadence(t *testing.T) {
	m := ggPublicationFixture(t)
	now := modelFRTTestNow
	if err := m.syncGroupGovernanceRealtime(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	first := ggPlainRow(t, m)
	if first.Generation != 1 || first.ControlJSON == "" {
		t.Fatalf("first publication must create the plain copy: %+v", first.Generation)
	}
	for i := 1; i <= 3; i++ {
		if err := m.syncGroupGovernanceRealtime(context.Background(), now.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	mid, _ := ggLoadPublication(t, m)
	plain := ggPlainRow(t, m)
	if mid.Generation != 4 || mid.FromLegacy || plain.Generation != 1 || plain.UpdatedAt != first.UpdatedAt || plain.ControlJSON != first.ControlJSON {
		t.Fatalf("plain row touched inside the interval: pub gen=%d plain gen=%d", mid.Generation, plain.Generation)
	}
	ggOldBinaryRead(t, m)
	later := now.Add(ggLegacyCopyEvery + time.Minute)
	if err := m.syncGroupGovernanceRealtime(context.Background(), later); err != nil {
		t.Fatal(err)
	}
	if p := ggPlainRow(t, m); p.Generation != 5 {
		t.Fatalf("plain copy not refreshed after the interval: gen=%d", p.Generation)
	}
	if v, _ := ggLoadPublication(t, m); v.FromLegacy || v.Generation != 5 {
		t.Fatal("compressed row must stay current after a plain refresh")
	}
	// A legacy timestamp in the future must not suppress the copy forever.
	m.storeDB.Model(&GroupGovernanceRealtimePublication{}).Where("id=1").Update("legacy_written_at", later.Unix()+86400)
	if err := m.syncGroupGovernanceRealtime(context.Background(), later.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if p := ggPlainRow(t, m); p.Generation != 6 {
		t.Fatal("future legacy timestamp blocked the plain copy")
	}
	// A deferral is recorded on the small row, not the large plain row.
	plainBefore := ggPlainRow(t, m)
	m.recordGroupGovernanceRealtimeDeferral(context.Background(), later.Add(2*time.Minute))
	if p := ggPlainRow(t, m); p.LastAttemptAt != plainBefore.LastAttemptAt || p.LastError != "" {
		t.Fatal("deferral rewrote the plain row")
	}
	if v, _ := loadGGRealtimeView(m.storeDB); v.LastError == "" {
		t.Fatal("deferral not visible on the current publication")
	}
}

// Roll back to v1.18.66 and forward again: the old binary resumes from the
// plain copy; the new binary then prefers the old binary's newer plain
// publication over its own stale compressed row.
func TestGroupGovernanceRealtimeRollbackAndForward(t *testing.T) {
	m := ggPublicationFixture(t)
	if err := m.syncGroupGovernanceRealtime(context.Background(), modelFRTTestNow); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		if err := m.syncGroupGovernanceRealtime(context.Background(), modelFRTTestNow.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	newView, newControl := ggLoadPublication(t, m)
	_, oldSeen := ggOldBinaryRead(t, m)
	ggAssertHistoriesKept(t, newControl, oldSeen)
	// v1.18.66 publishes on the plain row with its own CAS.
	plain := ggPlainRow(t, m)
	for key, mem := range oldSeen.Channels {
		mem.Config = "written-by-old-binary"
		oldSeen.Channels[key] = mem
	}
	oc, _ := json.Marshal(oldSeen)
	if err := m.storeDB.Model(&GroupGovernanceRealtimeState{}).Where("id=1 AND generation=?", plain.Generation).
		Updates(map[string]any{"generation": plain.Generation + 1, "control_json": string(oc), "updated_at": modelFRTTestNow.Unix() + 200}).Error; err != nil {
		t.Fatal(err)
	}
	picked, pickedControl := ggLoadPublication(t, m)
	if !picked.FromLegacy {
		t.Fatal("stale compressed row treated as current after the old binary published")
	}
	for _, mem := range pickedControl.Channels {
		if mem.Config != "written-by-old-binary" {
			t.Fatal("new binary read its stale compressed row instead of the old binary's publication")
		}
	}
	if picked.Generation != max(newView.Generation, plain.Generation+1) {
		t.Fatalf("generation must not move backwards: %d", picked.Generation)
	}
	// Forward again: the next publication continues from there and is current.
	if err := m.syncGroupGovernanceRealtime(context.Background(), modelFRTTestNow.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	final, _ := ggLoadPublication(t, m)
	if final.FromLegacy || final.Generation != picked.Generation+1 {
		t.Fatalf("forward publication wrong: legacy=%v gen=%d", final.FromLegacy, final.Generation)
	}
}
