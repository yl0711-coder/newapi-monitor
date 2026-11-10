package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"testing"
)

func detectionLinkFixture(t *testing.T, m *Monitor, f TokenMeteringFact, now int64) {
	t.Helper()
	tokenMeteringEvidence(t, m, fmt.Sprintf("detection-entry-%d", f.LogID), f.CreatedAt, 1000, fmt.Sprintf("fixture-%d", f.LogID))
	detectionLinkAgain(t, m, now, f.LogID)
}
func detectionLinkAgain(t *testing.T, m *Monitor, now int64, ids ...int64) {
	t.Helper()
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("log_id IN ?", ids).Update("link_next_at", now).Error; err != nil {
		t.Fatal(err)
	}
	n, err := m.linkTokenMeteringBatch(context.Background(), now)
	if err != nil || n != len(ids) {
		t.Fatalf("actual local linking: count=%d err=%v", n, err)
	}
}
func detectionCorrectQuota(t *testing.T, m *Monitor, id int64) {
	t.Helper()
	f := tokenMeteringFactRead(t, m, id)
	u := tokenMeteringDecode(t, f)
	q := u.Fields["quota"]
	q.Value = "999999"
	u.Fields["quota"] = q
	f.SourceFacts = tokenDetectionJSON(u)
	f.FactHash = tokenDetectionHash(u)
	f.Revision++
	if err := m.storeDB.Save(&f).Error; err != nil {
		t.Fatal(err)
	}
}
func detectionFrozenRead(t *testing.T, m *Monitor, id, rev int64) TokenDetectionRevision {
	t.Helper()
	var r TokenDetectionRevision
	if err := m.storeDB.First(&r, "source_epoch = ? AND log_id = ? AND revision = ?", m.cfg.TokenFactsSourceEpoch, id, rev).Error; err != nil {
		t.Fatal(err)
	}
	return r
}
func TestTokenDetectionReviewUnchangedLinksDoNotSpendRevisions(t *testing.T) {
	m, now, _ := detectionFixture(t)
	f := detectionFact(t, m, 1, now-100, 1, 10000, 0, 0)
	detectionLinkFixture(t, m, f, now)
	detectionRun(t, m, now)
	initial := tokenMeteringFactRead(t, m, 1)
	head, _ := detectionHead(t, m, 1)
	original := detectionFrozenRead(t, m, 1, head.Revision)
	for i := int64(1); i < 24; i++ {
		detectionLinkAgain(t, m, now+i, 1)
		detectionRun(t, m, now+i)
	}
	current := tokenMeteringFactRead(t, m, 1)
	repeated, _ := detectionHead(t, m, 1)
	if current.LinkAttempts != 24 || current.LinkCheckedAt != now+23 {
		t.Fatal("check bookkeeping lost")
	}
	if current.LinkRevision != initial.LinkRevision || current.EvidenceSnapshotAt != initial.EvidenceSnapshotAt || repeated.Revision != head.Revision {
		t.Errorf("identical evidence consumed versions: link=%d->%d decision=%d->%d", initial.LinkRevision, current.LinkRevision, head.Revision, repeated.Revision)
	}
	detectionCorrectQuota(t, m, 1)
	n, err := m.runTokenDetectionRound(context.Background(), now+24)
	corrected, r := detectionHead(t, m, 1)
	if err != nil || n == 0 || corrected.FactRevision != 2 || !r.Candidate {
		t.Fatalf("late correction skipped: processed=%d revision=%d fact=%d err=%v", n, corrected.Revision, corrected.FactRevision, err)
	}
	if got := detectionFrozenRead(t, m, 1, head.Revision); got.FrozenJSON != original.FrozenJSON {
		t.Fatal("first evidence overwritten")
	}
}

func TestTokenDetectionDoesNotTurnAmbiguousAssociationIntoEvidenceInsufficient(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	f := detectionFact(t, m, 902, now-120, 3, 20000, 0, 0)
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("source_epoch=? AND log_id=?", f.SourceEpoch, f.LogID).Updates(map[string]any{
		"link_state": "ambiguous", "link_reason": "multiple_entry_events", "link_revision": gorm.Expr("link_revision + 1"), "link_next_at": now,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.runTokenDetectionRound(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	head, result := detectionHead(t, m, f.LogID)
	if head.DetectionState != "partially_evaluated" || !result.Rules[0].Eligible || result.Rules[1].Eligible || !result.Rules[3].Eligible {
		t.Fatal("ambiguous association erased independent rules or became normal", head, result.Rules)
	}
	var fact TokenMeteringFact
	if err := m.storeDB.First(&fact, "source_epoch=? AND log_id=?", f.SourceEpoch, f.LogID).Error; err != nil || fact.LinkState != "ambiguous" || fact.LinkReason != "multiple_entry_events" {
		t.Fatal("independent detection changed association evidence", fact, err)
	}
}
func TestTokenDetectionReviewLegacyExhaustedHeadCanRecover(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionFact(t, m, 1, now-100, 1, 10000, 0, 0)
	detectionRun(t, m, now)
	first := detectionFrozenRead(t, m, 1, 1)
	for rev := int64(2); rev <= 24; rev++ {
		copy := first
		copy.Revision = rev
		if err := m.storeDB.Create(&copy).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Where("log_id = ?", 1).Update("revision", 24).Error; err != nil {
		t.Fatal(err)
	}
	detectionCorrectQuota(t, m, 1)
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB}
	n, err := restarted.runTokenDetectionRound(context.Background(), now+1)
	head, result := detectionHead(t, m, 1)
	if err != nil || n == 0 || head.Revision != 25 || head.FactRevision != 2 || !result.Candidate {
		t.Fatalf("legacy limit stranded real correction: processed=%d revision=%d fact=%d err=%v", n, head.Revision, head.FactRevision, err)
	}
	if got := detectionFrozenRead(t, m, 1, 1); got.FrozenJSON != first.FrozenJSON {
		t.Fatal("legacy evidence rewritten")
	}
}
func detectionReferenceCapacity(t *testing.T, m *Monitor, expires int64) {
	t.Helper()
	rows := make([]TokenDetectionReference, 5000)
	for i := range rows {
		rows[i] = TokenDetectionReference{ID: fmt.Sprintf("capacity-%04d", i), Bucket: "capacity-bucket", Through: int64(i), ExpiresAt: expires, SnapshotJSON: `{"samples":[]}`, StoredBytes: 14}
	}
	if err := m.storeDB.CreateInBatches(rows, 100).Error; err != nil {
		t.Fatal(err)
	}
}
func TestTokenDetectionReviewExpiredReferencesReleaseSlotsKeepAnchor(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionReferenceCapacity(t, m, now-1)
	m.cfg.TokenDetectionEnabled = false
	tokenCleanupAuthorizeFixture(t, m, now, now-4*86400)
	for i := 0; i < 160; i++ {
		if err := m.cleanupTokenDetection(context.Background(), now); err != nil {
			t.Fatal(err)
		}
	}
	var count int64
	if err := m.storeDB.Model(&TokenDetectionReference{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expired non-anchor records still occupy capacity: %d", count)
	}
	var marker TokenDetectionReference
	if err := m.storeDB.First(&marker, "id = ?", "capacity-0000").Error; err != nil {
		t.Fatal(err)
	}
	if marker.InvalidReason != "anchor_expired_requires_calibration" || marker.StoredBytes != int64(len(marker.SnapshotJSON)) {
		t.Fatal("anchor marker lost its unavailable reason or accounting")
	}
	m.cfg.TokenDetectionEnabled = true
	detectionSeed(t, m, now)
	detectionFact(t, m, 10, now-100, 3, 100000, 0, 0)
	detectionRun(t, m, now+1)
	_, r := detectionHead(t, m, 10)
	if !r.Rules[1].Hit {
		t.Fatal("new reference could not recover after collection")
	}
}
func TestTokenDetectionReviewCapacityFailureDoesNotBlockRound(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionSeed(t, m, now)
	detectionRun(t, m, now)
	detectionReferenceCapacity(t, m, now+86400)
	detectionFact(t, m, 10, now-200, 3, 100000, 0, 0)
	f := detectionFact(t, m, 11, now-100, 3, 10000, 0, 0)
	f.RequestLength = nil
	if err := m.storeDB.Save(&f).Error; err != nil {
		t.Fatal(err)
	}
	n, err := m.runTokenDetectionRound(context.Background(), now+1)
	if !errors.Is(err, errTokenDetectionCapacity) {
		t.Fatalf("capacity must remain visible: %v", err)
	}
	var count int64
	if e := m.storeDB.Model(&TokenDetectionCurrent{}).Where("log_id = ?", 11).Count(&count).Error; e != nil {
		t.Fatal(e)
	}
	if n == 0 || count != 1 {
		t.Fatalf("one limited job blocked independent detection: processed=%d later=%d", n, count)
	}
	var state TokenDetectionState
	if e := m.storeDB.First(&state, "source_epoch = ?", m.cfg.TokenFactsSourceEpoch).Error; e != nil {
		t.Fatal(e)
	}
	if state.LastReason != "local_capacity_exhausted" || state.LeaseUntil != 0 {
		t.Fatalf("capacity health/lease lost: %+v", state)
	}
}
func TestTokenDetectionReviewLinkedReferenceChangeRevisesAndKeepsFreeze(t *testing.T) {
	m, now, _ := detectionFixture(t)
	for i := int64(1); i <= 3; i++ {
		f := detectionFact(t, m, i, now-600+i, 1+i%2, 10000, 0, 0)
		detectionLinkFixture(t, m, f, now)
	}
	f := detectionFact(t, m, 10, now-200, 3, 100000, 0, 0)
	detectionLinkFixture(t, m, f, now)
	detectionRun(t, m, now)
	head, first := detectionHead(t, m, 10)
	if !first.Rules[1].Hit || first.EstimatedLoss == nil {
		t.Fatal("fixture lacks initial reference")
	}
	frozen := detectionFrozenRead(t, m, 10, head.Revision)
	for i := int64(1); i <= 3; i++ {
		if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Where("event_id = ?", fmt.Sprintf("detection-entry-%d", i)).Update("request_length", 2000).Error; err != nil {
			t.Fatal(err)
		}
	}
	detectionLinkAgain(t, m, now+1, 1, 2, 3)
	f = detectionFact(t, m, 11, now-100, 3, 100000, 0, 0)
	detectionLinkFixture(t, m, f, now+1)
	detectionRun(t, m, now+2)
	detectionRun(t, m, now+3)
	for _, id := range []int64{10, 11} {
		_, r := detectionHead(t, m, id)
		if r.Rules[1].Eligible || r.EstimatedLoss != nil || r.Reference.Status == "statistical" {
			t.Errorf("request %d still trusts changed entry reference: status=%s reason=%s loss=%v", id, r.Reference.Status, r.Reference.Reason, r.EstimatedLoss)
		}
	}
	current, _ := detectionHead(t, m, 10)
	if current.Revision <= head.Revision {
		t.Error("dependent judgment did not revise")
	}
	if got := detectionFrozenRead(t, m, 10, head.Revision); got.FrozenJSON != frozen.FrozenJSON {
		t.Fatal("first frozen evidence overwritten")
	}
	var before tokenDetectionFrozen
	if err := json.Unmarshal([]byte(frozen.FrozenJSON), &before); err != nil {
		t.Fatal(err)
	}
	if again := tokenDetectionEvaluate(before.Input, before.Policy, before.Result.Reference, before.CachePriorSignals, before.Result.SourceComplete); tokenDetectionJSON(again) != tokenDetectionJSON(before.Result) {
		t.Fatal("first frozen evidence no longer recomputable")
	}
	// Cleaning live samples cannot undo the known invalidation.
	if err := m.storeDB.Where("log_id IN ?", []int64{1, 2, 3}).Delete(&TokenMeteringFact{}).Error; err != nil {
		t.Fatal(err)
	}
	detectionFact(t, m, 12, now-50, 3, 100000, 0, 0)
	detectionRun(t, m, now+4)
	_, after := detectionHead(t, m, 12)
	if after.Rules[1].Eligible || after.EstimatedLoss != nil {
		t.Fatal("expired sample revived a known-invalid anchor")
	}
}

func detectionReviewReopen(t *testing.T, m *Monitor, path string) *Monitor {
	t.Helper()
	conn, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	conn, err = db.DB()
	if err != nil {
		t.Fatal(err)
	}
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = conn.Close() })
	return &Monitor{cfg: m.cfg, storeDB: db, nginxEvidenceDB: m.nginxEvidenceDB}
}
func TestTokenDetectionReviewCapacityBackoffFairnessAndRestart(t *testing.T) {
	m, now, path := detectionFixture(t)
	detectionSeed(t, m, now)
	detectionRun(t, m, now)
	detectionReferenceCapacity(t, m, now+86400)
	for i := int64(10); i < 36; i++ {
		detectionFact(t, m, i, now-250+i, 3, 100000, 0, 0)
	}
	f := detectionFact(t, m, 36, now-50, 3, 10000, 0, 0)
	f.RequestLength = nil
	if err := m.storeDB.Save(&f).Error; err != nil {
		t.Fatal(err)
	}
	for _, at := range []int64{now + 1, now + 2, now + 3} {
		if _, err := m.runTokenDetectionRound(context.Background(), at); err != nil && !errors.Is(err, errTokenDetectionCapacity) {
			t.Fatal(err)
		}
	}
	if _, r := detectionHead(t, m, 36); r.Candidate {
		t.Fatal("unrelated normal result changed")
	}
	var retries []TokenDetectionRetry
	if err := m.storeDB.Find(&retries).Error; err != nil {
		t.Fatal(err)
	}
	if len(retries) != 26 {
		t.Fatalf("failed jobs were not individually deferred: %d", len(retries))
	}
	for _, r := range retries {
		if r.Attempts != 1 || r.NextAt < now+61 {
			t.Fatalf("unbounded retry: %+v", r)
		}
	}
	m = detectionReviewReopen(t, m, path)
	if n, err := m.runTokenDetectionRound(context.Background(), now+4); err != nil || n != 0 {
		t.Fatalf("restart ignored backoff: %d %v", n, err)
	}
	if err := m.storeDB.Model(&TokenDetectionReference{}).Where("bucket = ?", "capacity-bucket").Update("expires_at", now).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenDetection(context.Background(), now+5); err != nil {
		t.Fatal(err)
	}
	// A real fact correction bypasses a still-future capacity backoff.
	detectionCorrectQuota(t, m, 10)
	detectionRun(t, m, now+6)
	if h, _ := detectionHead(t, m, 10); h.FactRevision != 2 {
		t.Fatal("changed evidence stuck behind capacity backoff")
	}
	for _, at := range []int64{now + 64, now + 65, now + 66} {
		detectionRun(t, m, at)
	}
	var count int64
	if err := m.storeDB.Model(&TokenDetectionRetry{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("recovered work still deferred: %d %v", count, err)
	}
	for i := int64(10); i < 36; i++ {
		_, r := detectionHead(t, m, i)
		if !r.Rules[1].Hit {
			t.Fatalf("limited task %d not resumed", i)
		}
	}
}
func TestTokenDetectionReviewCompactAnchorsDoNotSpendLiveReferenceQuota(t *testing.T) {
	m, now, _ := detectionFixture(t)
	rows := make([]TokenDetectionReference, 5000)
	for i := range rows {
		rows[i] = TokenDetectionReference{ID: fmt.Sprintf("anchor-%04d", i), Bucket: fmt.Sprintf("anchor-bucket-%04d", i), Through: int64(i), ExpiresAt: now - 1}
	}
	if err := m.storeDB.CreateInBatches(rows, 100).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenDetection(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	detectionSeed(t, m, now)
	detectionFact(t, m, 10, now-100, 3, 100000, 0, 0)
	detectionRun(t, m, now+1)
	if _, r := detectionHead(t, m, 10); !r.Rules[1].Hit {
		t.Fatal("necessary markers spent the live-reference quota")
	}
	var count int64
	if err := m.storeDB.Model(&TokenDetectionReference{}).Where("snapshot_json = ?", "").Count(&count).Error; err != nil || count != 5000 {
		t.Fatalf("necessary anchors deleted: %d %v", count, err)
	}
}
func TestTokenDetectionReviewReferenceChecksEntryIdentityAndSemantics(t *testing.T) {
	for _, change := range []struct {
		name, field string
		value       any
	}{{"length", "request_length", 2000}, {"route", "route", "/v1/messages"}, {"node", "node", "synthetic-other"}, {"length_semantics", "request_length_version", "request-length-v2"}} {
		t.Run(change.name, func(t *testing.T) {
			m, now, path := detectionFixture(t)
			for i := int64(1); i <= 3; i++ {
				f := detectionFact(t, m, i, now-600+i, 1+i%2, 10000, 0, 0)
				detectionLinkFixture(t, m, f, now)
			}
			detectionFact(t, m, 10, now-200, 3, 100000, 0, 0)
			detectionRun(t, m, now)
			old := detectionFrozenRead(t, m, 10, 1)
			if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Where("event_id = ?", "detection-entry-1").Update(change.field, change.value).Error; err != nil {
				t.Fatal(err)
			}
			detectionLinkAgain(t, m, now+1, 1)
			detectionRun(t, m, now+2)
			detectionRun(t, m, now+3)
			_, r := detectionHead(t, m, 10)
			if r.Rules[1].Eligible || r.Reference.Reason != "anchor_evidence_changed_requires_review" || r.EstimatedLoss != nil {
				t.Fatalf("changed reference still trusted: %+v", r.Reference)
			}
			m = detectionReviewReopen(t, m, path)
			if err := m.storeDB.Where("log_id = ?", 1).Delete(&TokenMeteringFact{}).Error; err != nil {
				t.Fatal(err)
			}
			detectionFact(t, m, 11, now-100, 3, 100000, 0, 0)
			detectionRun(t, m, now+4)
			_, r = detectionHead(t, m, 11)
			if r.Rules[1].Eligible || r.Reference.Reason != "anchor_evidence_changed_requires_review" {
				t.Fatal("restart/retention lost invalidation")
			}
			if got := detectionFrozenRead(t, m, 10, 1); got.FrozenJSON != old.FrozenJSON {
				t.Fatal("original immutable evidence changed")
			}
		})
	}
}
func TestTokenDetectionReviewLegacyReferenceCannotInventAssociationProof(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionSeed(t, m, now)
	detectionFact(t, m, 10, now-200, 3, 100000, 0, 0)
	detectionRun(t, m, now)
	original := detectionFrozenRead(t, m, 10, 1)
	var refs []TokenDetectionReference
	if err := m.storeDB.Find(&refs).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range refs {
		var ref tokenReference
		if err := json.Unmarshal([]byte(row.SnapshotJSON), &ref); err != nil {
			t.Fatal(err)
		}
		for i := range ref.Samples {
			ref.Samples[i].EvidenceHash = ""
		}
		if err := m.storeDB.Model(&row).Updates(map[string]any{"snapshot_json": tokenDetectionJSON(ref), "invalid_reason": nil}).Error; err != nil {
			t.Fatal(err)
		}
	}
	// A policy digest from v64 must wake even a final-stage, unchanged head.
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Where("1=1").Updates(map[string]any{"policy_hash": tokenDetectionHash(detectionPolicy()), "stage": 3, "next_at": 0}).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now+1)
	_, r := detectionHead(t, m, 10)
	if r.Rules[1].Eligible || r.Reference.Reason != "anchor_evidence_version_unavailable" {
		t.Fatal("legacy reference silently fabricated missing proof")
	}
	var n int64
	if err := m.storeDB.Model(&TokenDetectionReference{}).Where("invalid_reason = ?", "anchor_evidence_version_unavailable").Count(&n).Error; err != nil || n == 0 {
		t.Fatalf("legacy NULL metadata not invalidated: %d %v", n, err)
	}
	if got := detectionFrozenRead(t, m, 10, 1); got.FrozenJSON != original.FrozenJSON {
		t.Fatal("legacy frozen evidence rewritten")
	}
}
func TestTokenDetectionReviewNoopLinkRejectsStaleBookkeeping(t *testing.T) {
	m, now, _ := detectionFixture(t)
	f := detectionFact(t, m, 1, now-100, 1, 10000, 0, 0)
	detectionLinkFixture(t, m, f, now)
	old := tokenMeteringFactRead(t, m, 1)
	current := old
	current.LinkAttempts++
	current.LinkCheckedAt = now + 1
	// Both attempts deliberately have the same next-at and effective version.
	if saved, err := m.saveTokenMeteringLink(context.Background(), old, current, now+1); err != nil || !saved {
		t.Fatalf("first no-op save: %v %v", saved, err)
	}
	if saved, err := m.saveTokenMeteringLink(context.Background(), old, old, now+2); err != nil || saved {
		t.Fatalf("stale scheduler overwrote bookkeeping: %v %v", saved, err)
	}
	got := tokenMeteringFactRead(t, m, 1)
	if got.LinkRevision != old.LinkRevision || got.LinkAttempts != current.LinkAttempts || got.LinkCheckedAt != now+1 {
		t.Fatal("content version or check state lost")
	}
}
func TestTokenDetectionReviewRetryPersistenceFailureIsNotSuccess(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionFact(t, m, 1, now-100, 1, 10000, 0, 0)
	if err := m.storeDB.Create(&TokenDetectionRevision{SourceEpoch: "capacity", LogID: 99, Revision: 1, StoredBytes: tokenDetectionMaxBytes, FrozenJSON: "{}"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec("CREATE TRIGGER fail_detection_retry BEFORE INSERT ON token_detection_retries BEGIN SELECT RAISE(ABORT,'retry_fixture'); END").Error; err != nil {
		t.Fatal(err)
	}
	if n, err := m.runTokenDetectionRound(context.Background(), now); n != 0 || err == nil {
		t.Fatalf("retry write failure consumed job: %d %v", n, err)
	}
	var count int64
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("failed retry left a partial head")
	}
	if err := m.storeDB.Exec("DROP TRIGGER fail_detection_retry").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Where("source_epoch = ?", "capacity").Delete(&TokenDetectionRevision{}).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now+1)
	if h, _ := detectionHead(t, m, 1); h.Revision != 1 {
		t.Fatal("retry write failure lost first revision")
	}
}

func TestTokenDetectionReviewV64MigrationPreservesFrozenRows(t *testing.T) {
	m, now, path := detectionFixture(t)
	detectionSeed(t, m, now)
	detectionFact(t, m, 10, now-200, 3, 100000, 0, 0)
	detectionRun(t, m, now)
	original := detectionFrozenRead(t, m, 10, 1)
	// Reconstruct the actual previous table layout, not only an empty new store.
	var triggers []struct{ Name string }
	if err := m.storeDB.Raw("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'token_report_pending_%'").Scan(&triggers).Error; err != nil {
		t.Fatal(err)
	}
	for _, trigger := range triggers {
		if err := m.storeDB.Exec("DROP TRIGGER " + trigger.Name).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Migrator().DropTable(&TokenDetectionRetry{}); err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"InvalidReason", "InvalidatedAt"} {
		if err := m.storeDB.Migrator().DropColumn(&TokenDetectionReference{}, column); err != nil {
			t.Fatal(err)
		}
	}
	m = detectionReviewReopen(t, m, path)
	if err := m.storeDB.AutoMigrate(&TokenDetectionRetry{}, &TokenDetectionReference{}); err != nil {
		t.Fatal(err)
	}
	if err := initTokenDetectionReportIndex(m.storeDB); err != nil {
		t.Fatal(err)
	}
	if got := detectionFrozenRead(t, m, 10, 1); got.FrozenJSON != original.FrozenJSON {
		t.Fatal("migration rewrote frozen operands")
	}
	detectionCorrectQuota(t, m, 10)
	detectionRun(t, m, now+1)
	if h, _ := detectionHead(t, m, 10); h.FactRevision != 2 {
		t.Fatal("migrated detection cannot resume")
	}
}
