package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func TestStabilitySQLReadbackPreservesErrorsAndFRT(t *testing.T) {
	m := newStabilityTestMonitor(t)
	defer m.Close()
	hour := time.Now().Add(-4*time.Hour).Unix() / 3600 * 3600
	row := StabilityHourSample{HourTs: hour, ChannelID: 12, ModelName: "model", Grp: "group", Success: 21, Failed: 10,
		Err4xx: 1, Err5xx: 2, ErrTimeout: 3, ErrOther: 4,
		Ttft500: 1, Ttft1k: 2, Ttft2k: 3, Ttft5k: 4, Ttft10k: 5, TtftInf: 6, TtftObserved: 21, TtftOver3s: 13, TtftMaxMs: 15000}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	scope := stabilityScope{FromTs: hour, ToTs: hour + 3600}
	check := func(c stabilityCounts) {
		t.Helper()
		if c.Err4xx != 1 || c.Err5xx != 2 || c.ErrTimeout != 3 || c.ErrOther != 4 || c.Ttft500 != 1 || c.Ttft1k != 2 || c.Ttft2k != 3 || c.Ttft5k != 4 || c.Ttft10k != 5 || c.TtftInf != 6 || c.TtftOver3s != 13 {
			t.Fatalf("SQL scan lost columns: %+v", c)
		}
		v := c.metrics()
		if v.FRTObserved != 21 || v.FRTOver3s != 13 || v.FRTP95Ms <= 0 || v.Err4xx+v.Err5xx+v.ErrTimeout+v.ErrOther != v.Failed {
			t.Fatalf("metric contract: %+v", v)
		}
	}
	dims, _, err := m.queryStabilityDims(context.Background(), scope, 10)
	if err != nil || len(dims) != 1 {
		t.Fatalf("dims: %v %+v", err, dims)
	}
	check(dims[0].counts())
	days, _, err := m.queryStabilityDaily(context.Background(), scope, true, 10)
	if err != nil || len(days) != 1 {
		t.Fatalf("days: %v %+v", err, days)
	}
	check(days[0].counts())
	points, _, err := m.queryStabilityTimeline(context.Background(), scope, 3600, true, 10)
	if err != nil || len(points) != 1 {
		t.Fatalf("timeline: %v %+v", err, points)
	}
	check(points[0].counts())
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/stability/report?days=7", nil)
	m.serveStabilityReport(c)
	var report StabilityReport
	if w.Code != 200 {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Summary.Err4xx != 1 || report.Summary.Err5xx != 2 || report.Summary.FRTObserved != 21 || report.Summary.FRTOver3s != 13 {
		t.Fatalf("HTTP lost fields: %+v", report.Summary)
	}
}

func seedStabilityFRTRepair(t *testing.T, m *Monitor, hour int64) (StabilityHourSample, StabilityHourIngestState) {
	t.Helper()
	minute := MetricSample{BucketTs: hour, ChannelID: 12, ModelName: "model", Grp: "group", Success: 2, Failed: 1, Err5xx: 1, Tokens: 70, Quota: 900, RefundQuota: 20, RefundRecords: 1, SumUseTime: 12, MaxUseTime: 8,
		Ttft1k: 1, Ttft5k: 1, TtftObserved: 2, TtftOver3s: 1, TtftMaxMs: 4000, TTFTSemanticsVersion: ttftCoverageSemanticsVersion}
	if err := m.upsertSamples([]MetricSample{minute}); err != nil {
		t.Fatal(err)
	}
	old := StabilityHourSample{HourTs: hour, ChannelID: 12, ModelName: "model", Grp: "group", Success: 2, Failed: 1, Err5xx: 1, Tokens: 70, Quota: 900, RefundQuota: 20, RefundRecords: 1, SumUseTime: 12, MaxUseTime: 8}
	if err := m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	receipt := StabilityHourIngestState{HourTs: hour, Status: "complete", Rows: 1, Requests: 3, Tokens: 70, Quota: 900, UpdatedAt: hour + 3600, CompletedAt: hour + 3600, Attempts: 4, JobID: "old-job"}
	if err := m.storeDB.Create(&receipt).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&receipt).UpdateColumn("ttft_semantics_version", nil).Error; err != nil {
		t.Fatal(err)
	}
	receipt.TTFTSemanticsVersion = 0
	proof := MetricFinalizeState{ID: 1, NextTs: hour + 3600, TargetThroughTs: hour + 3600, SemanticsVersion: stabilityTrafficClassificationVersion, TTFTSemanticsVersion: ttftCoverageSemanticsVersion, TTFTCoverageFromTs: hour, TTFTCoverageThroughTs: hour + 3600}
	if err := m.storeDB.Save(&proof).Error; err != nil {
		t.Fatal(err)
	}
	return old, receipt
}

func TestStabilityFRTRepairPreservesAccountingAndIsIdempotent(t *testing.T) {
	m := newStabilityTestMonitor(t)
	defer m.Close()
	hour := int64(2_000_000_000) / 3600 * 3600
	old, receipt := seedStabilityFRTRepair(t, m, hour)
	for turn := 0; turn < 3; turn++ {
		m.stabilityFRTRepairAfter.Store(0) // restart loses only advisory scan progress
		if err := m.repairOneStabilityFRTFromMinutes(context.Background(), hour+7200); err != nil {
			t.Fatal(err)
		}
		var got StabilityHourSample
		var state StabilityHourIngestState
		if err := m.storeDB.First(&got, "hour_ts=?", hour).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.First(&state, "hour_ts=?", hour).Error; err != nil {
			t.Fatal(err)
		}
		if stabilityWithoutFRT(got) != stabilityWithoutFRT(old) {
			t.Fatalf("accounting changed: before=%+v after=%+v", old, got)
		}
		if got.TtftObserved != 2 || got.Ttft1k != 1 || got.Ttft5k != 1 || got.TtftOver3s != 1 || got.TTFTSemanticsVersion != ttftCoverageSemanticsVersion {
			t.Fatalf("FRT incomplete: %+v", got)
		}
		receipt.TTFTSemanticsVersion = ttftCoverageSemanticsVersion
		if !reflect.DeepEqual(receipt, state) {
			t.Fatalf("non-FRT receipt changed: %+v / %+v", receipt, state)
		}
	}
	cov := m.stabilityDataCoverage(context.Background(), hour, hour+3600, hour+7200)
	if !cov.Complete || !cov.FRTComplete {
		t.Fatalf("coverage not repaired: %+v", cov)
	}
}

func TestStabilityFRTRepairRejectsUnprovenOrConflictingHours(t *testing.T) {
	cases := []struct {
		name, sql string
		mismatch  bool
	}{
		{"prefix_not_started", "UPDATE metric_finalize_states SET ttft_coverage_from_ts=ttft_coverage_from_ts+60", false},
		{"prefix_incomplete", "UPDATE metric_finalize_states SET ttft_coverage_through_ts=ttft_coverage_through_ts-60", false},
		{"request_prefix_incomplete", "UPDATE metric_finalize_states SET next_ts=next_ts-60", false},
		{"target_prefix_incomplete", "UPDATE metric_finalize_states SET target_through_ts=target_through_ts-60", false},
		{"request_proof_old", "UPDATE metric_finalize_states SET semantics_version=0", false},
		{"receipt_not_complete", "UPDATE stability_hour_ingest_states SET status='running'", false},
		{"legacy_minutes", "UPDATE metric_samples SET ttft_semantics_version=0", true},
		{"wrong_requests", "UPDATE metric_samples SET success=success+1", true},
		{"wrong_quota", "UPDATE metric_samples SET quota=quota+1", true},
		{"wrong_refund", "UPDATE metric_samples SET refund_quota=refund_quota+1", true},
		{"wrong_dimension", "UPDATE metric_samples SET grp='other'", true},
		{"missing_minutes", "DELETE FROM metric_samples", true},
		{"invalid_histogram", "UPDATE metric_samples SET ttft_observed=99", true},
		{"invalid_receipt", "UPDATE stability_hour_ingest_states SET requests=99", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newStabilityTestMonitor(t)
			defer m.Close()
			hour := int64(2_000_000_000) / 3600 * 3600
			old, _ := seedStabilityFRTRepair(t, m, hour)
			if err := m.storeDB.Exec(tc.sql).Error; err != nil {
				t.Fatal(err)
			}
			err := m.repairOneStabilityFRTFromMinutes(context.Background(), hour+7200)
			if tc.mismatch != errors.Is(err, errStabilityFRTMismatch) || (!tc.mismatch && err != nil) {
				t.Fatalf("unexpected err: %v", err)
			}
			var got StabilityHourSample
			var state StabilityHourIngestState
			m.storeDB.First(&got, "hour_ts=?", hour)
			m.storeDB.First(&state, "hour_ts=?", hour)
			if got != old || state.TTFTSemanticsVersion != 0 {
				t.Fatalf("unproven hour published: %+v %+v", got, state)
			}
		})
	}
}

func TestStabilityFRTRepairRejectsInvalidMinuteHiddenByAggregate(t *testing.T) {
	m := newStabilityTestMonitor(t)
	defer m.Close()
	hour := int64(2_000_000_000) / 3600 * 3600
	seedStabilityFRTRepair(t, m, hour)
	var minute MetricSample
	if err := m.storeDB.First(&minute).Error; err != nil {
		t.Fatal(err)
	}
	minute.BucketTs += 60
	if err := m.upsertSamples([]MetricSample{minute}); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"UPDATE metric_samples SET ttft_max_ms=1000 WHERE bucket_ts=?",
		"UPDATE stability_hour_samples SET success=4,failed=2,err_5xx=2,tokens=140,quota=1800,refund_quota=40,refund_records=2,sum_use_time=24 WHERE hour_ts=?",
		"UPDATE stability_hour_ingest_states SET requests=6,tokens=140,quota=1800 WHERE hour_ts=?",
	} {
		if err := m.storeDB.Exec(statement, hour).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.repairOneStabilityFRTFromMinutes(context.Background(), hour+7200); !errors.Is(err, errStabilityFRTMismatch) {
		t.Fatalf("corrupt minute accepted: %v", err)
	}
}

func TestStabilityFRTRepairHonorsCancellation(t *testing.T) {
	m := newStabilityTestMonitor(t)
	defer m.Close()
	hour := int64(2_000_000_000) / 3600 * 3600
	seedStabilityFRTRepair(t, m, hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.repairOneStabilityFRTFromMinutes(ctx, hour+7200); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel ignored: %v", err)
	}
	var receipt StabilityHourIngestState
	if err := m.storeDB.First(&receipt).Error; err != nil {
		t.Fatal(err)
	}
	if receipt.TTFTSemanticsVersion != 0 {
		t.Fatal("cancelled work certified")
	}
}

func TestStabilityFRTRepairConcurrentReplay(t *testing.T) {
	m := newStabilityTestMonitor(t)
	defer m.Close()
	hour := int64(2_000_000_000) / 3600 * 3600
	old, _ := seedStabilityFRTRepair(t, m, hour)
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { results <- m.repairOneStabilityFRTFromMinutes(context.Background(), hour+7200) }()
	}
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	var row StabilityHourSample
	if err := m.storeDB.First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if stabilityWithoutFRT(row) != stabilityWithoutFRT(old) || row.TtftObserved != 2 {
		t.Fatalf("concurrent replay changed facts: %+v", row)
	}
}

func TestStabilityFRTRepairReceiptFailureRollsBackProjection(t *testing.T) {
	m := newStabilityTestMonitor(t)
	defer m.Close()
	hour := int64(2_000_000_000) / 3600 * 3600
	old, _ := seedStabilityFRTRepair(t, m, hour)
	if err := m.storeDB.Exec(`CREATE TRIGGER reject_frt_receipt BEFORE UPDATE OF ttft_semantics_version ON stability_hour_ingest_states BEGIN SELECT RAISE(ABORT,'injected receipt failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.repairOneStabilityFRTFromMinutes(context.Background(), hour+7200); err == nil {
		t.Fatal("wanted transaction failure")
	}
	var got StabilityHourSample
	if err := m.storeDB.First(&got, "hour_ts=?", hour).Error; err != nil {
		t.Fatal(err)
	}
	if got != old {
		t.Fatalf("partial projection committed: %+v", got)
	}
	if err := m.storeDB.Exec("DROP TRIGGER reject_frt_receipt").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.repairOneStabilityFRTFromMinutes(context.Background(), hour+7200); err != nil {
		t.Fatal(err)
	}
}

func TestStabilityFRTRepairRotatesPastMismatch(t *testing.T) {
	m := newStabilityTestMonitor(t)
	defer m.Close()
	hour := int64(2_000_000_000) / 3600 * 3600
	seedStabilityFRTRepair(t, m, hour)
	seedStabilityFRTRepair(t, m, hour+3600)
	if err := m.storeDB.Exec("UPDATE metric_finalize_states SET ttft_coverage_from_ts=?", hour).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec("UPDATE metric_samples SET quota=quota+1 WHERE bucket_ts=?", hour).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.repairOneStabilityFRTFromMinutes(context.Background(), hour+10800); !errors.Is(err, errStabilityFRTMismatch) {
		t.Fatalf("wanted mismatch: %v", err)
	}
	if err := m.repairOneStabilityFRTFromMinutes(context.Background(), hour+10800); err != nil {
		t.Fatal(err)
	}
	var rows []StabilityHourIngestState
	if err := m.storeDB.Order("hour_ts").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if rows[0].TTFTSemanticsVersion != 0 || rows[1].TTFTSemanticsVersion != ttftCoverageSemanticsVersion {
		t.Fatalf("starved or falsely certified: %+v", rows)
	}
}

// Empty hours are valid only with a complete source prefix and zero controls.
func TestStabilityFRTRepairProvenZero(t *testing.T) {
	m := newStabilityTestMonitor(t)
	defer m.Close()
	hour := int64(2_000_000_000) / 3600 * 3600
	seedStabilityFRTRepair(t, m, hour)
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("hour_ts=?", hour).Delete(&StabilityHourSample{}).Error; err != nil {
			return err
		}
		if err := tx.Where("bucket_ts=?", hour).Delete(&MetricSample{}).Error; err != nil {
			return err
		}
		return tx.Model(&StabilityHourIngestState{}).Where("hour_ts=?", hour).Updates(map[string]any{"rows": 0, "requests": 0, "tokens": 0, "quota": 0}).Error
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.repairOneStabilityFRTFromMinutes(context.Background(), hour+7200); err != nil {
		t.Fatal(err)
	}
	var state StabilityHourIngestState
	m.storeDB.First(&state, "hour_ts=?", hour)
	if state.TTFTSemanticsVersion != ttftCoverageSemanticsVersion {
		t.Fatal("proven zero not certified")
	}
}
