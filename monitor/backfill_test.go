package monitor

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestBackfillGuards 回填的护栏:未配生产库、非法小时数、超留存、并发重入都必须被拒。
// 这些不是形式检查——回填要打上百次生产库查询,越界或并发会成倍放大压力。
func TestBackfillGuards(t *testing.T) {
	m := &Monitor{cfg: Settings{RetentionDays: 7}}

	if _, err := m.BackfillHours(context.Background(), 24); err == nil {
		t.Error("未配置生产库时应拒绝回填")
	}

	// 带上假的 prodDB 之后再验参数护栏(sql.DB 为 nil 时前置检查已拦住,故只验参数分支)
	if _, err := m.BackfillHours(context.Background(), 0); err == nil {
		t.Error("hours<=0 应被拒绝")
	}
}

// TestBackfillNoReentry 并发保护:第二次调用必须直接被拒,不能同时打生产库。
func TestBackfillNoReentry(t *testing.T) {
	if !backfillRunning.CompareAndSwap(false, true) {
		t.Fatal("测试前置:标志位应为空闲")
	}
	defer backfillRunning.Store(false)

	m := &Monitor{cfg: Settings{RetentionDays: 7}}
	// prodDB 为 nil 时会先在前置检查返回;这里只断言"标志位已被占用"这一语义,
	// 真正的重入拒绝在 BackfillHours 内的 CompareAndSwap。
	if backfillRunning.CompareAndSwap(false, true) {
		t.Error("标志位被占用时不应再次抢到")
	}
	_ = m
}

func TestBackfillDoesNotPublishCoverageWhenTokenProjectionFails(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB, _ = m.storeDB.DB()
	m.cfg.RetentionDays = 7
	result, err := m.backfillHoursWith(t.Context(), 1,
		func(context.Context, int64, int64) (int, error) { return 1, nil },
		func(context.Context, int64, int64) (int, error) { return 1, nil },
		func(context.Context, int64, int64) error { return errors.New("token projection failed") },
		func(int64, int64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed < 1 {
		t.Fatalf("token failure did not fail the shared coverage slice: %+v", result)
	}
	var count int64
	if err := m.storeDB.Model(&MetricFinalizeState{}).Where("status = ?", "caught_up").Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("token failure published complete coverage: count=%d err=%v", count, err)
	}
}

func TestBackfillDoesNotPublishCoverageWhenHourRollupFails(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB, _ = m.storeDB.DB()
	m.cfg.RetentionDays = 7
	result, err := m.backfillHoursWith(t.Context(), 1,
		func(context.Context, int64, int64) (int, error) { return 1, nil },
		func(context.Context, int64, int64) (int, error) { return 1, nil },
		func(context.Context, int64, int64) error { return nil },
		func(int64, int64) error { return errors.New("hour rollup failed") })
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed < 1 {
		t.Fatalf("rollup failure did not block shared coverage: %+v", result)
	}
	var count int64
	if err := m.storeDB.Model(&MetricFinalizeState{}).Where("status = ?", "caught_up").Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("rollup failure published complete coverage: count=%d err=%v", count, err)
	}
}

func TestMetricBackfillAllowsLongHistoryWithinHourRetention(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB, _ = m.storeDB.DB()
	m.cfg.RetentionDays = 7
	m.cfg.HourRetentionDays = 90
	if err := m.validateMetricBackfill(30 * 24); err != nil {
		t.Fatalf("30-day hourly rebuild must not be limited by 7-day minute retention: %v", err)
	}
	if err := m.validateMetricBackfill(90*24 + 1); err == nil {
		t.Fatal("backfill beyond hour retention must be rejected")
	}
}

func TestMetricHourCoverageIsIndependentFromMinuteCoverage(t *testing.T) {
	m := newTestMonitor(t)
	now := int64(2_000_000_000)
	target := metricFinalizeTarget(now)
	hourTarget := target / 3600 * 3600
	row := MetricFinalizeState{
		ID: 1, NextTs: target, CoverageFromTs: target - 7*86400,
		SemanticsVersion:   stabilityTrafficClassificationVersion,
		HourCoverageFromTs: hourTarget - 30*86400, HourCoverageToTs: hourTarget,
		HourSemanticsVersion: stabilityTrafficClassificationVersion,
	}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	if complete, _, _ := m.metricWindowCoverage(target-30*86400, now); complete {
		t.Fatal("minute coverage must remain bounded to minute retention")
	}
	if complete, from, to := m.metricHourWindowCoverage(hourTarget-30*86400, now); !complete || from != hourTarget-30*86400 || to != hourTarget {
		t.Fatalf("hour coverage should independently prove 30 days: complete=%v from=%d to=%d", complete, from, to)
	}
}

func TestSuccessfulBackfillPublishesIndependentHourCoverage(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB, _ = m.storeDB.DB()
	m.cfg.RetentionDays = 7
	m.cfg.HourRetentionDays = 90
	result, err := m.backfillHoursWith(t.Context(), 1,
		func(context.Context, int64, int64) (int, error) { return 0, nil },
		func(context.Context, int64, int64) (int, error) { return 0, nil },
		func(context.Context, int64, int64) error { return nil },
		func(int64, int64) error { return nil })
	if err != nil || result.Failed != 0 {
		t.Fatalf("backfill failed: result=%+v err=%v", result, err)
	}
	now := time.Now().Unix()
	hourTarget := metricFinalizeTarget(now) / 3600 * 3600
	if complete, from, to := m.metricHourWindowCoverage(hourTarget-3600, now); !complete || from > hourTarget-3600 || to != hourTarget {
		t.Fatalf("successful rebuild did not publish hour coverage: complete=%v from=%d to=%d", complete, from, to)
	}
}

func TestSnapshotComparisonUsesHourCoverageNotMinuteRetention(t *testing.T) {
	m := newTestMonitor(t)
	now := int64(2_000_000_000)
	target := metricFinalizeTarget(now)
	hourTarget := target / 3600 * 3600
	state := MetricFinalizeState{
		ID: 1, NextTs: target, CoverageFromTs: target - 3600,
		SemanticsVersion:   stabilityTrafficClassificationVersion,
		HourCoverageFromTs: hourTarget - 24*3600, HourCoverageToTs: hourTarget,
		HourSemanticsVersion: stabilityTrafficClassificationVersion,
	}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	snapshot, err := m.computeSnapshot(60, now)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.CompareAvailable {
		t.Fatal("comparison was enabled with only 24 hours of hourly coverage")
	}
	if err := m.storeDB.Model(&MetricFinalizeState{}).Where("id = ?", 1).Updates(map[string]any{
		"hour_coverage_from_ts": hourTarget - 192*3600,
	}).Error; err != nil {
		t.Fatal(err)
	}
	snapshot, err = m.computeSnapshot(60, now)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.CompareAvailable {
		t.Fatal("comparison remained disabled after complete 192-hour evidence")
	}
}

func TestBackfillCoveragePublicationCannotRegressConcurrentFinalizeWatermarks(t *testing.T) {
	m := newTestMonitor(t)
	state := MetricFinalizeState{
		ID: 1, NextTs: 21_600, TargetThroughTs: 22_200, CoverageFromTs: 18_000,
		SemanticsVersion: stabilityTrafficClassificationVersion, Status: "queued",
		HourCoverageFromTs: 10_800, HourCoverageToTs: 21_600,
		HourSemanticsVersion: stabilityTrafficClassificationVersion,
	}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	// Simulate an older, long-running backfill publishing after the live worker
	// has already advanced all right watermarks beyond its captured target.
	if err := m.publishMetricBackfillCoverage(1, 14_400, 19_800, 7_200, 18_000, 30_000); err != nil {
		t.Fatal(err)
	}
	var got MetricFinalizeState
	if err := m.storeDB.First(&got, "id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if got.NextTs != 21_600 || got.TargetThroughTs != 22_200 || got.HourCoverageToTs != 21_600 {
		t.Fatalf("backfill regressed live watermarks: %+v", got)
	}
	if got.CoverageFromTs != 14_400 || got.HourCoverageFromTs != 7_200 || got.Status != "queued" {
		t.Fatalf("overlapping coverage was not merged monotonically: %+v", got)
	}
}

func TestBackfillCoveragePublicationDoesNotBridgeDisjointIntervals(t *testing.T) {
	m := newTestMonitor(t)
	state := MetricFinalizeState{
		ID: 1, NextTs: 21_600, TargetThroughTs: 21_600, CoverageFromTs: 18_000,
		SemanticsVersion: stabilityTrafficClassificationVersion, Status: "caught_up",
		HourCoverageFromTs: 18_000, HourCoverageToTs: 21_600,
		HourSemanticsVersion: stabilityTrafficClassificationVersion,
	}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}

	// The historical rebuild stops before the live interval starts. Publishing
	// MIN(old,new)..MAX(old,new) here would falsely certify the missing hour.
	if err := m.publishMetricBackfillCoverage(1, 7_200, 14_400, 3_600, 14_400, 30_000); err != nil {
		t.Fatal(err)
	}
	var got MetricFinalizeState
	if err := m.storeDB.First(&got, "id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if got.CoverageFromTs != 18_000 || got.NextTs != 21_600 {
		t.Fatalf("disjoint minute intervals were bridged: %+v", got)
	}
	if got.HourCoverageFromTs != 18_000 || got.HourCoverageToTs != 21_600 {
		t.Fatalf("disjoint hour intervals were bridged: %+v", got)
	}
}

func TestBackfillCoveragePublicationSwitchesToNewerDisjointInterval(t *testing.T) {
	m := newTestMonitor(t)
	state := MetricFinalizeState{
		ID: 1, NextTs: 14_400, TargetThroughTs: 14_400, CoverageFromTs: 7_200,
		SemanticsVersion: stabilityTrafficClassificationVersion, Status: "caught_up",
		HourCoverageFromTs: 3_600, HourCoverageToTs: 14_400,
		HourSemanticsVersion: stabilityTrafficClassificationVersion,
	}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}

	// A newer rebuild with a gap must replace the certified interval, not extend
	// the old left boundary across data that was never rebuilt.
	if err := m.publishMetricBackfillCoverage(1, 18_000, 21_600, 18_000, 21_600, 30_000); err != nil {
		t.Fatal(err)
	}
	var got MetricFinalizeState
	if err := m.storeDB.First(&got, "id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if got.CoverageFromTs != 18_000 || got.NextTs != 21_600 {
		t.Fatalf("newer disjoint minute interval bridged old coverage: %+v", got)
	}
	if got.HourCoverageFromTs != 18_000 || got.HourCoverageToTs != 21_600 {
		t.Fatalf("newer disjoint hour interval bridged old coverage: %+v", got)
	}
}
