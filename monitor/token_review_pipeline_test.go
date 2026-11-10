package monitor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenReviewRawRequestAssociationAndMissingEvidence(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "raw-exact-request")
	tokenMeteringSourceRow(t, m, 2, start+2, "raw-missing-request")
	tokenMeteringRun(t, m, now)
	length := int64(1234)
	if err := m.nginxEvidenceDB.Create(&NginxRequestEvidence{EventID: strings.Repeat("a", 64), EventMS: (start + 1) * 1000, OneAPIRequestID: "raw-exact-request", Route: "/v1/messages", Method: "POST", Status: 200, RequestLength: &length, RequestLengthState: "valid", RequestLengthVersion: "request-length-v1"}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.linkTokenMeteringBatch(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	f := tokenMeteringFactRead(t, m, 1)
	if f.RequestID != "raw-exact-request" || f.LinkState != "unique" || f.RequestLength == nil || *f.RequestLength != 1234 {
		t.Fatalf("raw ID did not link: %+v", f)
	}
	missing := tokenMeteringFactRead(t, m, 2)
	if missing.LinkState != "waiting" || missing.LinkReason != "exact_request_evidence_not_found" || missing.RequestLength != nil {
		t.Fatalf("nearby request substituted: %+v", missing)
	}
	before := f.LinkRevision
	if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Where("oneapi_request_id=?", f.RequestID).Update("request_length", 2345).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("log_id=1").Update("link_next_at", now).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.linkTokenMeteringBatch(context.Background(), now+1); err != nil {
		t.Fatal(err)
	}
	if got := tokenMeteringFactRead(t, m, 1); got.LinkRevision <= before || got.RequestLength == nil || *got.RequestLength != 2345 {
		t.Fatal("enriched raw evidence did not requeue a revision", got.LinkRevision, before)
	}
}

func TestTokenReviewLaneCursorAndAccountingSurviveRestart(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "lane-request")
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0)); err != nil {
		t.Fatal(err)
	}
	var lane TokenMeteringLaneState
	if err := m.storeDB.First(&lane, "source_epoch=? AND lane=?", m.cfg.TokenFactsSourceEpoch, tokenMeteringLaneForwardCatchup).Error; err != nil {
		t.Fatal(err)
	}
	if lane.CursorID != 1 || lane.RowsCommitted != 1 || lane.RowsRead != 1 || lane.Running != 0 || lane.LeaseUntil != 0 {
		t.Fatalf("incomplete durable lane: %+v", lane)
	}
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, prodDB: m.prodDB}
	if _, err := restarted.runTokenMeteringRound(context.Background(), time.Unix(now+1, 0)); err != nil {
		t.Fatal(err)
	}
	if state := tokenMeteringStateRead(t, m); state.FactCount != 1 || state.ScanThrough != start+300 {
		t.Fatal("restart changed prefix", state)
	}
}

func TestTokenReviewPendingDisplayKeepsRecordedOperands(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	f := detectionFact(t, m, 101, now-120, 3, 20000, 3000, 0)
	f.LinkState = "source_missing"
	f.CustomerNameSnapshot = "historical-customer"
	f.CustomerNameSource = "source_log"
	if err := m.storeDB.Save(&f).Error; err != nil {
		t.Fatal(err)
	}
	rows, err := m.tokenDetectionDisplays(m.storeDB, f.SourceEpoch, []TokenDetectionCurrent{{SourceEpoch: f.SourceEpoch, LogID: f.LogID}})
	if err != nil {
		t.Fatal(err)
	}
	d := rows[f.LogID]
	if !d.Stale || d.DetectionState != "association_pending" || d.RecordedInput == nil || *d.RecordedInput != "20000" || d.CacheRead == nil || *d.CacheRead != "3000" || d.CustomerName != "historical-customer" || d.FieldSources["prompt_tokens"] == "" {
		t.Fatalf("pending facts hidden: %+v", d)
	}
}

func TestTokenReviewPreparedDecisionRejectsChangedInput(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	f := detectionFact(t, m, 101, now-120, 3, 20000, 0, 0)
	if err := m.storeDB.Create(&TokenDetectionState{SourceEpoch: f.SourceEpoch, LeaseToken: "review-lease", LeaseUntil: now + 30}).Error; err != nil {
		t.Fatal(err)
	}
	var a *tokenDetectionPrepared
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		var err error
		a, err = prepareTokenDetection(tx, f.SourceEpoch, f.LogID, detectionPolicy(), now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&f).Update("link_revision", f.LinkRevision+1).Error; err != nil {
		t.Fatal(err)
	}
	err := m.tokenLocalWriteTransaction(context.Background(), func(tx *gorm.DB) error {
		return m.processTokenDetectionTx(tx, f.SourceEpoch, f.LogID, detectionPolicy(), "review-lease", now, a)
	})
	if !errors.Is(err, errTokenDetectionInputsChanged) {
		t.Fatalf("stale preparation committed: %v", err)
	}
	var heads int64
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Count(&heads).Error; err != nil || heads != 0 {
		t.Fatal("stale head persisted", heads, err)
	}
}

func TestTokenReviewCleanupNeverExpiresIncompleteCoverage(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	start := now - 10*86400
	start = start / 300 * 300
	if err := m.storeDB.Model(&TokenMeteringState{}).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).Updates(map[string]any{"start_at": start, "scan_through": start, "complete_through": start}).Error; err != nil {
		t.Fatal(err)
	}
	w := TokenMeteringWindow{SourceEpoch: m.cfg.TokenFactsSourceEpoch, StartAt: start, EndAt: start + 300, CursorAt: start, CursorID: 7, LastReason: "source_read_failed"}
	if err := m.storeDB.Create(&w).Error; err != nil {
		t.Fatal(err)
	}
	tokenCleanupAuthorizeFixture(t, m, now, now-4*86400)
	if err := m.cleanupTokenMetering(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	state := tokenMeteringStateRead(t, m)
	if state.StartAt != start || state.ScanThrough != start || state.CompleteThrough != start {
		t.Fatal("retention fabricated coverage", state)
	}
	var saved TokenMeteringWindow
	if err := m.storeDB.First(&saved, "source_epoch=? AND start_at=?", w.SourceEpoch, start).Error; err != nil || saved.CursorID != 7 || saved.LastReason != w.LastReason {
		t.Fatal("retention discarded failed cursor", saved, err)
	}
	m.cfg.TokenFactsRetentionDays = 3
	if validateTokenMeteringSettings(m.cfg) == nil || m.cfg.tokenMeteringRetentionSeconds() < 4*86400 {
		t.Fatal("less than four days accepted")
	}
}
