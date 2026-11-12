package monitor

import (
	"context"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenReferenceMigrationNotStarvedByPendingQueue(t *testing.T) {
	m, now, _ := detectionFixture(t)
	for i := int64(1); i <= 160; i++ {
		detectionFact(t, m, i, now-600+i, 3, 10000, 0, 0)
	}
	for _, table := range []string{tokenPendingMigrationTable, "token_report_state_migration_v1", tokenReportStatusCheckpoint, "token_reference_members_checkpoint_v1", "token_revision_members_checkpoint_v1"} {
		if err := m.tokenStore().Table(table).Where("id=1").Update("ready", 0).Error; err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		if _, _, err := m.migrateTokenMeteringProjectionBatchProgress(context.Background(), now); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"token_reference_members_checkpoint_v1", "token_revision_members_checkpoint_v1"} {
		var ready bool
		if err := m.tokenStore().Table(table).Select("ready").Where("id=1").Scan(&ready).Error; err != nil || !ready {
			t.Fatalf("%s starved: %v", table, err)
		}
	}
	ready, err := tokenPendingProjectionReady(m.tokenStore())
	if err != nil || ready {
		t.Fatal("history queue was falsely completed", ready, err)
	}
	var facts int64
	m.tokenStore().Model(&TokenMeteringFact{}).Count(&facts)
	if facts != 160 {
		t.Fatal("migration deleted facts", facts)
	}
}

func TestReportProjectionReusesOneSnapshotAndSeesNextCorrection(t *testing.T) {
	m := newTestMonitor(t)
	from, to := int64(6000), int64(6060)
	if err := m.storeDB.Create(&MetricFinalizeState{ID: 1, CoverageFromTs: from, NextTs: to, SemanticsVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&MetricSample{BucketTs: from, ChannelID: 1, ModelName: "m", Grp: "g", Success: 2, TrafficClassVersion: stabilityTrafficClassificationVersion, TTFTSemanticsVersion: ttftCoverageSemanticsVersion, TtftObserved: 2, Ttft1k: 2, TtftMaxMs: 1000}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&CapacityUserMinuteSample{BucketTs: from, UserID: 1, ChannelID: 1, ModelName: "m", Grp: "g", Success: 2, TrafficClassVersion: stabilityTrafficClassificationVersion, TTFTSemanticsVersion: ttftCoverageSemanticsVersion, TtftObserved: 2, Ttft1k: 2, TtftMaxMs: 1000}).Error; err != nil {
		t.Fatal(err)
	}
	queries := 0
	if err := m.storeDB.Callback().Row().Before("gorm:row").Register("projection_query_counter", func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "SUM(request_error)") {
			queries++
		}
	}); err != nil {
		t.Fatal(err)
	}
	read := func(want bool) {
		t.Helper()
		ctx := withReportProjectionCache(context.Background())
		if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
			local := &Monitor{cfg: m.cfg, storeDB: tx}
			requests, err := local.capacityWindowCoverage(ctx, from, to, true)
			if err != nil || requests != want {
				t.Fatalf("requests=%v err=%v", requests, err)
			}
			frt, err := capacityTTFTProjectionCoverage(ctx, tx, from, to)
			if err != nil || !frt {
				t.Fatalf("frt=%v err=%v", frt, err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	read(true)
	if queries != 1 {
		t.Fatal("same snapshot re-aggregated", queries)
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("bucket_ts=?", from).Update("success", 3).Error; err != nil {
		t.Fatal(err)
	}
	read(false)
	if queries != 2 {
		t.Fatal("late correction was hidden by cache", queries)
	}
}

func TestFRTRecomputeShrinksTimedOutSliceWithoutSkipping(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.BackgroundSourceMinStartIntervalMS = -1
	failures := &frtSourceQueryFailures{}
	m.prodDB = frtRecomputeProdDBWithFailures(t, failures)
	from := time.Now().Unix()/60*60 - 7200
	to := from + 600
	for i := int64(0); i < 10; i++ {
		frtRecomputeInsertLog(t, m.prodDB, int(i+1), from+i*60+1, 1, 2, 1, 1, "g", "m", `{"frt":1000}`, 1)
	}
	if _, err := m.sampleCustomerHealthRangeLow(context.Background(), from, to); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("bucket_ts>=? AND bucket_ts<?", from, to).UpdateColumns(map[string]any{"frt_sum_ms": nil, "frt_sum_version": 0, "frt_sum_observed": 0}).Error; err != nil {
		t.Fatal(err)
	}
	failures.maxSeconds = 60
	frtRecomputeRetryWithoutDelay(t)
	state, err := m.RecomputeFRTSum(context.Background(), from, to)
	if err != nil || state.Status != "complete" || state.DoneThroughTs != to || state.RowsUpdated != 10 || state.RowsSampleMismatch != 0 || state.Slices != 10 {
		t.Fatalf("bounded replay did not cover every minute: %+v %v", state, err)
	}
	var total int64
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Select("SUM(frt_sum_ms)").Scan(&total).Error; err != nil || total != 10000 {
		t.Fatal(total, err)
	}
}
