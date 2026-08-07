package monitor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestStabilityHourSQLUsesBoundedHalfOpenRangeAndSuccessOnlyUsage(t *testing.T) {
	q := stabilityHourSQL()
	if strings.Count(q, "?") != 2 || !strings.Contains(q, "created_at >= ? AND created_at < ?") {
		t.Fatalf("小时补数必须只有一个半开小时区间:\n%s", q)
	}
	for _, want := range []string{
		"SUM(CASE WHEN type=2 THEN prompt_tokens+completion_tokens END)",
		"SUM(CASE WHEN type=2 THEN quota END)",
		"GROUP BY channel_id, model_name, grp",
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("小时补数 SQL 缺少 %q:\n%s", want, q)
		}
	}
	if strings.Contains(q, "SUM(quota)") {
		t.Fatal("错误日志的 quota 不得混入消费额")
	}
}

func TestReplaceStabilityHourIsAtomicIdempotentAndMarksZeroTrafficComplete(t *testing.T) {
	m := newStabilityTestMonitor(t)
	hour := time.Date(2026, 8, 5, 10, 0, 0, 0, cstLocation).Unix()
	first := []StabilityHourSample{{HourTs: hour, ChannelID: 1, ModelName: "m1", Grp: "g1", Success: 3, Tokens: 10, Quota: 20}}
	if err := m.replaceStabilityHour(hour, first, StabilityHourIngestState{JobID: "test", Attempts: 1}); err != nil {
		t.Fatal(err)
	}
	second := []StabilityHourSample{{HourTs: hour, ChannelID: 2, ModelName: "m2", Grp: "g2", Failed: 2}}
	if err := m.replaceStabilityHour(hour, second, StabilityHourIngestState{JobID: "test", Attempts: 2}); err != nil {
		t.Fatal(err)
	}
	var rows []StabilityHourSample
	if err := m.storeDB.Where("hour_ts = ?", hour).Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ChannelID != 2 || rows[0].Failed != 2 {
		t.Fatalf("重复补数必须完整替换而非累加: %+v", rows)
	}
	if err := m.replaceStabilityHour(hour+3600, nil, StabilityHourIngestState{JobID: "test", Attempts: 1}); err != nil {
		t.Fatal(err)
	}
	var state StabilityHourIngestState
	if err := m.storeDB.First(&state, "hour_ts = ?", hour+3600).Error; err != nil {
		t.Fatal(err)
	}
	if state.Status != "complete" || state.Rows != 0 || state.Requests != 0 {
		t.Fatalf("零流量小时也必须有完整性凭证: %+v", state)
	}
}

func TestStabilityCoverageCountsLedgerNotSamplePresence(t *testing.T) {
	m := newStabilityTestMonitor(t)
	from := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	// 有数据但没有完整台账，不得冒充已覆盖。
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: from, ChannelID: 1, ModelName: "m", Grp: "g", Success: 1}).Error; err != nil {
		t.Fatal(err)
	}
	cov := m.stabilityDataCoverage(context.Background(), from, from+3*3600, from+5*3600)
	if cov.CompletedHours != 0 || cov.MissingHours != 3 || cov.Complete {
		t.Fatalf("样本存在不能代替覆盖台账: %+v", cov)
	}
	states := []StabilityHourIngestState{
		{HourTs: from, Status: "complete"},
		{HourTs: from + 3600, Status: "complete"},
		{HourTs: from + 2*3600, Status: "failed"},
	}
	if err := m.storeDB.Create(&states).Error; err != nil {
		t.Fatal(err)
	}
	cov = m.stabilityDataCoverage(context.Background(), from, from+3*3600, from+5*3600)
	if cov.CompletedHours != 2 || cov.MissingHours != 1 || cov.Percent < 66 || cov.Percent > 67 {
		t.Fatalf("覆盖率应按完整小时计算: %+v", cov)
	}
}

func TestLocalRollupDoesNotOverwriteAuthoritativeCompleteHour(t *testing.T) {
	m := newStabilityTestMonitor(t)
	hour := time.Date(2026, 8, 5, 10, 0, 0, 0, cstLocation).Unix()
	if err := m.storeDB.Create(&MetricSample{BucketTs: hour, ChannelID: 1, ModelName: "m", Grp: "g", Success: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: hour, ChannelID: 1, ModelName: "m", Grp: "g", Success: 99}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.rollupStabilityHours(hour); err != nil {
		t.Fatal(err)
	}
	var got StabilityHourSample
	if err := m.storeDB.First(&got, "hour_ts = ?", hour).Error; err != nil {
		t.Fatal(err)
	}
	if got.Success != 99 {
		t.Fatalf("分钟级局部数据覆盖了权威小时补数: %+v", got)
	}
}

func TestStabilityBackfillInterruptionIsResumableButQueryTimeoutIsNotShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !stabilityBackfillInterrupted(ctx, context.Canceled) {
		t.Fatal("服务取消必须识别为可续跑中断")
	}
	if !stabilityBackfillInterrupted(context.Background(), context.Canceled) {
		t.Fatal("驱动返回 context canceled 也必须识别为可续跑中断")
	}
	if stabilityBackfillInterrupted(context.Background(), context.DeadlineExceeded) {
		t.Fatal("单小时查询超时不能伪装成服务停机，应按真实失败退避处理")
	}
	if stabilityBackfillInterrupted(context.Background(), errors.New("database unavailable")) {
		t.Fatal("普通数据库错误不能自动排队无限重试")
	}
}

func TestCanceledBackfillJobReturnsToQueuedForRestartResume(t *testing.T) {
	m := newStabilityTestMonitor(t)
	hour := time.Date(2026, 8, 5, 10, 0, 0, 0, cstLocation).Unix()
	job := StabilityBackfillJob{ID: "cancel-resume", FromTs: hour, ToTs: hour + 3600, Status: "queued", TotalHours: 1, UpdatedAt: time.Now().Unix()}
	if err := m.storeDB.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m.stabilityBackfillRunning.Store(true)
	m.runStabilityBackfill(ctx, job.ID)
	if err := m.storeDB.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != "queued" || !strings.Contains(job.LastError, "等待下次启动续跑") {
		t.Fatalf("停机取消后任务必须回到queued: %+v", job)
	}
}

func TestStabilityStorageCoversTwoMaximumQueryPeriods(t *testing.T) {
	for _, tc := range []struct {
		cfg         Settings
		wantQuery   int
		wantStorage int
	}{
		{Settings{}, 90, 181},
		{Settings{StabilityQueryMaxDays: 90, StabilityRetentionDays: 90}, 90, 181},
		{Settings{StabilityQueryMaxDays: 30, StabilityRetentionDays: 120}, 30, 120},
	} {
		if got := tc.cfg.stabilityQueryDays(); got != tc.wantQuery {
			t.Fatalf("query days=%d want=%d", got, tc.wantQuery)
		}
		if got := tc.cfg.stabilityStorageDays(); got != tc.wantStorage {
			t.Fatalf("storage days=%d want=%d", got, tc.wantStorage)
		}
	}
}

func TestDisabledStabilityBackfillRejectsManualStartAndDoesNotResume(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.StabilityBackfillEnabled = false
	hour := time.Date(2026, 8, 5, 10, 0, 0, 0, cstLocation).Unix()
	job := StabilityBackfillJob{ID: "must-not-resume", FromTs: hour, ToTs: hour + 3600, Status: "queued", TotalHours: 1, UpdatedAt: time.Now().Unix()}
	if err := m.storeDB.Create(&job).Error; err != nil {
		t.Fatal(err)
	}

	m.startStabilityBackfillMaintenance(context.Background())
	if m.stabilityBackfillRunning.Load() {
		t.Fatal("补数总开关关闭时不得续跑历史任务")
	}
	if err := m.storeDB.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != "queued" {
		t.Fatalf("禁用时不得改写历史任务: %+v", job)
	}
	if _, err := m.startStabilityBackfill(hour, hour+3600); !errors.Is(err, errStabilityBackfillDisabled) {
		t.Fatalf("人工调用应返回禁用错误，got=%v", err)
	}

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/stability/backfill?days=1", nil)
	m.startStabilityBackfillHandler(c)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("禁用的管理接口应返回 503，got=%d body=%s", w.Code, w.Body.String())
	}
}

func TestUsageAdminLabelsAndGroupOrderContract(t *testing.T) {
	for _, want := range []string{"关注客户累计总消耗", "关注客户数", ".sort((a,b)=>"} {
		if !strings.Contains(pageHTML, want) {
			t.Fatalf("管理端用户用量缺少 %q", want)
		}
	}
	if strings.Contains(portalHTML, "关注客户数") {
		t.Fatal("本轮管理端文案不得进入 Usage Portal")
	}
}
