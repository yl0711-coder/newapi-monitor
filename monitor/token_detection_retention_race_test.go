package monitor

import (
	"context"
	"errors"
	"testing"

	"gorm.io/gorm"
)

func TestTokenDetectionRetiredQueueEntryDoesNotAbortRetainedWork(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	for id := int64(1); id <= 3; id++ {
		detectionFact(t, m, id, now-600+id, 3, 20000, 0, 0)
	}
	retired := false
	// The queue has been read when processing first opens a fact. Remove the
	// next identity in that transaction, reproducing retention during a writer
	// handoff without timing-dependent sleeps or a second database connection.
	if err := m.tokenStore().Callback().Query().Before("gorm:query").Register("expire_queued_fact", func(tx *gorm.DB) {
		if retired || tx.Statement.Table != "token_metering_facts" {
			return
		}
		retired = true
		local := tx.Session(&gorm.Session{NewDB: true})
		if err := local.Where("source_epoch=? AND log_id=2", m.cfg.TokenFactsSourceEpoch).Delete(&TokenMeteringRef{}).Error; err != nil {
			tx.AddError(err)
			return
		}
		tx.AddError(local.Where("source_epoch=? AND log_id=2", m.cfg.TokenFactsSourceEpoch).Delete(&TokenMeteringFact{}).Error)
	}); err != nil {
		t.Fatal(err)
	}
	defer m.tokenStore().Callback().Query().Remove("expire_queued_fact")
	processed, _, err := m.runTokenDetectionRoundProgress(context.Background(), now)
	if err != nil || !retired || processed != 2 {
		t.Fatal("retired queue entry interrupted retained work or counted as a decision", processed, retired, err)
	}
	var heads []TokenDetectionCurrent
	if err := m.tokenStore().Order("log_id").Find(&heads).Error; err != nil || len(heads) != 2 || heads[0].LogID != 1 || heads[1].LogID != 3 {
		t.Fatal("missing fact produced a decision or lost other results", heads, err)
	}
	var state TokenDetectionState
	if err := m.tokenStore().First(&state).Error; err != nil || state.LastReason != "" || state.LastSuccessAt != now {
		t.Fatal("completed retained work was marked failed", state, err)
	}
	assertTokenCapacityLedger(t, m.tokenStore())
}

func TestTokenDetectionMissingDependencyStillFails(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	if err := m.tokenStore().Callback().Query().Before("gorm:query").Register("missing_capacity_dependency", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_detection_capacity" {
			tx.AddError(gorm.ErrRecordNotFound)
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer m.tokenStore().Callback().Query().Remove("missing_capacity_dependency")
	processed, _, err := m.runTokenDetectionRoundProgress(context.Background(), now)
	if processed != 0 || !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("missing dependency was swallowed as retention", processed, err)
	}
}
