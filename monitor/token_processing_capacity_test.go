package monitor

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func assertTokenCapacityLedger(t *testing.T, db *gorm.DB) {
	t.Helper()
	var got, want tokenDetectionCapacity
	if err := db.First(&got, 1).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Raw(`SELECT 1 AS id,
		(SELECT COUNT(*) FROM token_detection_currents) AS current_rows,
		(SELECT COALESCE(SUM(stored_bytes),0) FROM token_detection_revisions) AS revision_bytes,
		(SELECT COALESCE(SUM(stored_bytes),0) FROM token_detection_references) AS reference_bytes,
		(SELECT COALESCE(SUM(length(CAST(result_json AS BLOB))),0) FROM token_detection_currents) AS head_bytes,
		(SELECT COUNT(*) FROM token_detection_references) AS reference_rows`).Scan(&want).Error; err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("capacity drift: got=%+v want=%+v", got, want)
	}
}

func TestTokenDetectionCapacityLedgerTracksWritersRollbackAndRestart(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	// Backfill must include data predating this migration.
	first := TokenDetectionCurrent{SourceEpoch: "before", LogID: 1, ResultJSON: `{"note":"中文"}`, ExpiresAt: now + 100}
	if err := m.storeDB.Create(&first).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.ensureTokenDetectionCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTokenCapacityLedger(t, m.storeDB)
	rev := TokenDetectionRevision{SourceEpoch: "before", LogID: 1, Revision: 1, FrozenJSON: "{}", StoredBytes: 42}
	ref := TokenDetectionReference{ID: "ref", StoredBytes: 71, SnapshotJSON: "{}"}
	if err := m.storeDB.Create(&rev).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ref).Error; err != nil {
		t.Fatal(err)
	}
	second := &Monitor{storeDB: m.storeDB}
	if err := second.ensureTokenDetectionCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTokenCapacityLedger(t, m.storeDB)
	rollback := errors.New("rollback fixture")
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&first).Update("result_json", `{"note":"rollback"}`).Error; err != nil {
			return err
		}
		if err := tx.Model(&rev).Update("stored_bytes", 10000).Error; err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	assertTokenCapacityLedger(t, m.storeDB)
	if err := m.storeDB.Model(&rev).Update("stored_bytes", 100).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&first).Update("result_json", `{"note":"中文变更"}`).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Delete(&ref).Error; err != nil {
		t.Fatal(err)
	}
	assertTokenCapacityLedger(t, m.storeDB)
	if err := m.storeDB.Delete(&rev).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Delete(&first).Error; err != nil {
		t.Fatal(err)
	}
	assertTokenCapacityLedger(t, m.storeDB)
}

func TestTokenProcessingBudgetsDefaultClosedAndValidate(t *testing.T) {
	s := Settings{}
	if s.tokenProcessingPollSeconds() != 60 || s.tokenProcessingBatchRows() != 50 || s.tokenDetectionRetentionSeconds() != 90*86400 {
		t.Fatal("default changed")
	}
	for _, change := range []func(*Settings){
		func(s *Settings) { s.TokenProcessingPollSeconds = -1 }, func(s *Settings) { s.TokenProcessingPollSeconds = 9 },
		func(s *Settings) { s.TokenProcessingBatchRows = 501 }, func(s *Settings) { s.TokenDetectionMaxBytes = -1 },
		func(s *Settings) { s.TokenDetectionRetentionDays = 2 },
	} {
		bad := s
		change(&bad)
		if validateTokenProcessingResources(bad) == nil {
			t.Fatalf("bad resources accepted: %+v", bad)
		}
	}
	s.TokenProcessingPollSeconds = 15
	s.TokenProcessingBatchRows = 250
	s.TokenDetectionMaxRows = 2_000_000
	s.TokenDetectionMaxBytes = 16 << 30
	s.TokenDetectionRetentionDays = 4
	if err := validateTokenProcessingResources(s); err != nil {
		t.Fatal(err)
	}
	if s.TokenDetectionEnabled || s.TokenFactsEnabled || s.TokenNotificationsEnabled {
		t.Fatal("resources enabled a capability")
	}
}

func TestTokenDetectionConfiguredBatchDrainsEveryIdentityAndMeasuresPayload(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	m.cfg.TokenProcessingBatchRows = 250
	m.cfg.TokenProcessingPollSeconds = 15
	base := detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	rows := make([]TokenMeteringFact, 999)
	for i := range rows {
		rows[i] = base
		rows[i].LogID = int64(i + 2)
	}
	if err := m.storeDB.CreateInBatches(rows, 100).Error; err != nil {
		t.Fatal(err)
	}
	jobs, err := tokenDetectionJobsBounded(m.storeDB, base.SourceEpoch, tokenDetectionPolicyHash(detectionPolicy()), now, 250, 15)
	if err != nil || len(jobs) != 250 {
		t.Fatalf("fresh lane cannot borrow idle capacity: len=%d err=%v", len(jobs), err)
	}
	started := time.Now()
	processed := 0
	var count int64
	for round := 0; round < 16 && count < 1000; round++ {
		n, err := m.runTokenDetectionRound(context.Background(), now)
		if err != nil {
			t.Fatal(err)
		}
		if n < 1 {
			t.Fatal("progress stopped")
		}
		processed += n
		if err := m.storeDB.Model(&TokenDetectionCurrent{}).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
	}
	// Changes in peer buckets legitimately re-evaluate existing identities.
	// Throughput must count unique heads, not these extra processing jobs.
	if count != 1000 {
		t.Fatalf("missing identities: count=%d processed_jobs=%d", count, processed)
	}
	assertTokenCapacityLedger(t, m.storeDB)
	var capacity tokenDetectionCapacity
	if err := m.storeDB.First(&capacity).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("representative_fixture_only requests=%d processed_jobs=%d elapsed_ms=%d head_bytes=%d frozen_bytes=%d reference_bytes=%d fact_payload_bytes=%d", count, processed, time.Since(started).Milliseconds(), capacity.HeadBytes, capacity.RevisionBytes, capacity.ReferenceBytes, base.StoredBytes*count)
	// Account for all three scheduled reviews, not only the first decision.
	for round := 0; round < 48; round++ {
		at := now + int64(round+1)*3600
		jobs, err := tokenDetectionJobsBounded(m.storeDB, base.SourceEpoch, tokenDetectionPolicyHash(detectionPolicy()), at, 250, 15)
		if err != nil {
			t.Fatal(err)
		}
		if len(jobs) == 0 {
			break
		}
		if _, err := m.runTokenDetectionRound(context.Background(), at); err != nil {
			t.Fatal(err)
		}
	}
	var unfinished int64
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Where("stage != 3 OR next_at != 0").Count(&unfinished).Error; err != nil || unfinished != 0 {
		t.Fatalf("reviews incomplete: %d %v", unfinished, err)
	}
	if err := m.storeDB.First(&capacity).Error; err != nil {
		t.Fatal(err)
	}
	assertTokenCapacityLedger(t, m.storeDB)
	t.Logf("representative_fixture_all_reviews requests=%d elapsed_ms=%d head_bytes=%d frozen_bytes=%d reference_bytes=%d fact_payload_bytes=%d", count, time.Since(started).Milliseconds(), capacity.HeadBytes, capacity.RevisionBytes, capacity.ReferenceBytes, base.StoredBytes*count)
}

func TestTokenDetectionMissingEvidenceDoesNotInvalidatePeerHistory(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	first := detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	first.LinkState, first.EvidenceLevel, first.RequestLength = "waiting", "unavailable", nil
	if err := m.storeDB.Save(&first).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now)
	second := first
	second.LogID = 2
	if err := m.storeDB.Create(&second).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now)
	jobs, err := tokenDetectionJobs(m.storeDB, first.SourceEpoch, tokenDetectionPolicyHash(detectionPolicy()), now)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("incomparable new request invalidated existing evidence: %d %v", len(jobs), err)
	}
	// Own evidence corrections still need evaluation immediately.
	if err := m.storeDB.Model(&first).Update("link_revision", first.LinkRevision+1).Error; err != nil {
		t.Fatal(err)
	}
	jobs, err = tokenDetectionJobs(m.storeDB, first.SourceEpoch, tokenDetectionPolicyHash(detectionPolicy()), now)
	if err != nil || len(jobs) != 1 || jobs[0].LogID != first.LogID {
		t.Fatalf("own corrected evidence missed: %+v %v", jobs, err)
	}
}
