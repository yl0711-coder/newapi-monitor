package monitor

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestTokenDetectionDisplayUsesFrozenRevisionAndUnknownIdentity(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	f := detectionFact(t, m, 1, now-600, 3, 20000, 0, 1)
	detectionRun(t, m, now)
	var h TokenDetectionCurrent
	if err := m.storeDB.First(&h).Error; err != nil {
		t.Fatal(err)
	}
	// A later live fact must not change the operands displayed for this revision.
	if err := m.storeDB.Model(&f).Update("source_facts", "{}").Error; err != nil {
		t.Fatal(err)
	}
	rows, err := m.tokenDetectionDisplays(m.storeDB, h.SourceEpoch, []TokenDetectionCurrent{h})
	if err != nil {
		t.Fatal(err)
	}
	d := rows[h.LogID]
	if !d.EvidenceAvailable || d.Input == nil || *d.Input != "20000" || d.Output == nil || *d.Output != "2" || d.Upstream != "" || d.InputDifference != nil {
		t.Fatalf("frozen display: %+v", d)
	}
	if err := m.storeDB.Where("source_epoch=? AND log_id=?", h.SourceEpoch, h.LogID).Delete(&TokenDetectionRevision{}).Error; err != nil {
		t.Fatal(err)
	}
	rows, err = m.tokenDetectionDisplays(m.storeDB, h.SourceEpoch, []TokenDetectionCurrent{h})
	if err != nil {
		t.Fatal(err)
	}
	if d = rows[h.LogID]; d.EvidenceAvailable || d.Input != nil || d.Output != nil || d.InputDifference != nil {
		t.Fatalf("missing evidence became zero: %+v", d)
	}
}

func TestTokenDetectionCandidateFilterDoesNotHideInsufficientCoverage(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	f := detectionFact(t, m, 1, now-600, 3, 10, 0, 0)
	detectionRun(t, m, now)
	var h TokenDetectionCurrent
	if err := m.storeDB.First(&h).Error; err != nil {
		t.Fatal(err)
	}
	r := tokenDetectionResult{SourceComplete: true, Rules: []tokenDetectionRule{{Name: "input_inflation", Eligible: false}}}
	if err := m.storeDB.Model(&h).Updates(map[string]any{"candidate": false, "source_complete": true, "result_json": tokenDetectionJSON(r)}).Error; err != nil {
		t.Fatal(err)
	}
	got, err := m.tokenDetectionReport(context.Background(), tokenDetectionQuery{SourceEpoch: f.SourceEpoch, From: now - 3600, To: now, Status: "candidate", Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	s := got["summary"].(tokenDetectionSummary)
	if s.Insufficient != 1 || s.Candidates != 0 {
		t.Fatalf("outcome filter hid gap: %+v", s)
	}
	b, _ := json.Marshal(got)
	if !strings.Contains(string(b), "evidence_insufficient") {
		t.Fatal("missing explicit coverage gap")
	}
}

func TestTokenDetectionDisplayUpstreamSnapshotDoesNotRewriteRequestHistory(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	detectionFact(t, m, 1, now-600, 3, 20000, 0, 1)
	detectionRun(t, m, now)
	var h TokenDetectionCurrent
	if err := m.storeDB.First(&h).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.AutoMigrate(&ChannelSnap{}); err != nil {
		t.Fatal(err)
	}
	snapshot := ChannelSnap{ID: int(h.ChannelID), Name: "known channel", BaseDomain: "old.example.com", BaseHost: "api.old.example.com", UpdatedAt: now}
	if err := m.storeDB.Create(&snapshot).Error; err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		domain, source string
		deleted        int64
	}{
		{"old.example.com", "local_current_snapshot", 0},
		{"new.vendor.test", "local_current_snapshot", 0},
		{"new.vendor.test", "local_last_snapshot", now + 1},
	} {
		if err := m.storeDB.Model(&snapshot).Updates(map[string]any{"base_domain": tc.domain, "deleted_at": tc.deleted, "updated_at": now}).Error; err != nil {
			t.Fatal(err)
		}
		rows, err := m.tokenDetectionDisplays(m.storeDB, h.SourceEpoch, []TokenDetectionCurrent{h})
		if err != nil {
			t.Fatal(err)
		}
		d := rows[h.LogID]
		if d.Upstream != normalizeChannelBaseDomain(tc.domain) || d.UpstreamSource != tc.source || d.UpstreamObservedAt != now || d.UpstreamReason != "request_upstream_not_recorded" {
			t.Fatalf("snapshot was missing or claimed request provenance: %+v", d)
		}
	}
	if err := m.storeDB.Delete(&snapshot).Error; err != nil {
		t.Fatal(err)
	}
	rows, err := m.tokenDetectionDisplays(m.storeDB, h.SourceEpoch, []TokenDetectionCurrent{h})
	if err != nil {
		t.Fatal(err)
	}
	if d := rows[h.LogID]; d.Upstream != "" || d.UpstreamSource != "" {
		t.Fatalf("missing channel guessed upstream: %+v", d)
	}
}

func TestTokenDetectionDisplayKeepsStaleIdentityVisible(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	f := detectionFact(t, m, 1, now-600, 3, 20000, 0, 1)
	detectionRun(t, m, now)
	var h TokenDetectionCurrent
	if err := m.storeDB.First(&h).Error; err != nil {
		t.Fatal(err)
	}
	// A late fact revision invalidates the frozen head.  The list must keep the
	// identity and pending state visible while withholding the old decision.
	if err := m.storeDB.Model(&f).Updates(map[string]any{
		"revision":   2,
		"request_id": "req-stale-1",
	}).Error; err != nil {
		t.Fatal(err)
	}
	rows, err := m.tokenDetectionDisplays(m.storeDB, h.SourceEpoch, []TokenDetectionCurrent{h})
	if err != nil {
		t.Fatal(err)
	}
	d, ok := rows[h.LogID]
	if !ok || !d.Stale || d.EvidenceAvailable || d.RequestID != "req-stale-1" || d.DetectionState != "detection_pending" {
		t.Fatalf("stale identity was dropped or exposed old evidence: ok=%v display=%+v", ok, d)
	}
}
