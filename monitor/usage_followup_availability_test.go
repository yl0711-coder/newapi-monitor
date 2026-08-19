package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type followUpAvailabilityResponse struct {
	Enabled       bool              `json:"enabled"`
	Available     bool              `json:"available"`
	Companies     []FollowUpCompany `json:"companies"`
	MemberTotal   int               `json:"member_total"`
	WindowDays    int               `json:"window_days"`
	RequestedDays int               `json:"requested_days"`
	AvailableDays int               `json:"available_days"`
	RangePartial  bool              `json:"range_partial"`
	RequestedFrom string            `json:"requested_from"`
	RequestedTo   string            `json:"requested_to"`
	CoverageFrom  string            `json:"coverage_from"`
	CoverageTo    string            `json:"coverage_to"`
	Message       string            `json:"message"`
}

func newFactsFollowUpMonitor(t *testing.T) (*Monitor, *usageQueryCounts) {
	t.Helper()
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageCache = newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, 1<<20)
	enableUsageFactsForTest(m)
	if err := m.storeDB.Create(&CustomerGroup{ID: 1, Name: "测试公司", CreatedAt: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, GroupID: 1, Username: "名单名称"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&UsageUserSnapshot{
		UserID: 1, Username: "快照名称", BalanceQuota: 500000, Exists: true, CapturedAt: time.Now().Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	return m, counts
}

func requestFollowUpsAt(t *testing.T, m *Monitor, nowUnix int64) (int, followUpAvailabilityResponse) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/usage/followups", nil)
	m.serveFollowUpsAt(c, nowUnix)
	var payload followUpAvailabilityResponse
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("解析待跟进响应失败: status=%d body=%q err=%v", w.Code, w.Body.String(), err)
	}
	return w.Code, payload
}

func assertNoFollowUpSourceQueries(t *testing.T, counts *usageQueryCounts) {
	t.Helper()
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("待跟进不得扫描生产 logs，实际 %d 次", got)
	}
	if got := counts.users.Load(); got != 0 {
		t.Fatalf("待跟进 facts 模式不得查询生产 users，实际 %d 次", got)
	}
	if got := counts.tokens.Load(); got != 0 {
		t.Fatalf("待跟进 facts 模式不得查询生产 tokens，实际 %d 次", got)
	}
}

func followUpTestWindow(nowUnix int64) (int64, int64) {
	toTs := followUpDayStart(nowUnix + usageTZOffsetSec)
	return toTs - int64(followUpWindowDays)*usageFactDaySeconds, toTs
}

func TestServeFollowUpsFactsCoverage(t *testing.T) {
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, usageCST).Unix()
	fromTs, toTs := followUpTestWindow(now)

	t.Run("7_of_30_days_is_explicitly_unavailable", func(t *testing.T) {
		m, counts := newFactsFollowUpMonitor(t)
		seedPublishedUsageFactsForTest(t, m, []int64{1}, toTs-7*usageFactDaySeconds, toTs)
		counts.reset()

		status, got := requestFollowUpsAt(t, m, now)
		if status != http.StatusOK {
			t.Fatalf("status=%d want=200", status)
		}
		if !got.Enabled || got.Available || !got.RangePartial {
			t.Fatalf("7/30 覆盖必须显式不可判断: %+v", got)
		}
		if got.AvailableDays != 7 || got.RequestedDays != 30 || got.WindowDays != 30 {
			t.Fatalf("覆盖数错误: %+v", got)
		}
		if len(got.Companies) != 0 || got.MemberTotal != 0 {
			t.Fatalf("不完整窗口不得生成跟进结论: %+v", got)
		}
		if got.CoverageFrom == "" || got.CoverageTo == "" || !strings.Contains(got.Message, "7/30") {
			t.Fatalf("应返回可解释的覆盖范围: %+v", got)
		}
		assertNoFollowUpSourceQueries(t, counts)
	})

	t.Run("full_30_days_computes_from_facts", func(t *testing.T) {
		m, counts := newFactsFollowUpMonitor(t)
		seedPublishedUsageFactsForTest(t, m, []int64{1}, fromTs, toTs)
		counts.reset()

		status, got := requestFollowUpsAt(t, m, now)
		if status != http.StatusOK {
			t.Fatalf("status=%d want=200", status)
		}
		if !got.Enabled || !got.Available || got.RangePartial || got.AvailableDays != 30 {
			t.Fatalf("完整 30 天应可执行判断: %+v", got)
		}
		if len(got.Companies) != 1 || got.MemberTotal != 1 || len(got.Companies[0].Members) != 1 {
			t.Fatalf("本地空消费事实应判定该成员需跟进: %+v", got)
		}
		if got.Companies[0].Members[0].Username != "快照名称" {
			t.Fatalf("成员资料必须来自本地快照: %+v", got.Companies[0].Members[0])
		}
		assertNoFollowUpSourceQueries(t, counts)
	})

	t.Run("new_member_floor_is_not_counted_as_thirty_days_idle", func(t *testing.T) {
		m, counts := newFactsFollowUpMonitor(t)
		seedPublishedUsageFactsForTest(t, m, []int64{1}, fromTs, toTs)
		// The organization has a complete 30-day published window, but this
		// member only became observable two complete days ago.  The preceding
		// 28 days are outside this member's signed applicability range.
		if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).
			Where("user_id = ?", 1).
			Update("source_floor_hour", toTs-2*usageFactDaySeconds).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Model(&UsageUserSnapshot{}).
			Where("user_id = ?", 1).
			Update("balance_quota", int64(100*quotaPerUSD)).Error; err != nil {
			t.Fatal(err)
		}
		counts.reset()

		status, got := requestFollowUpsAt(t, m, now)
		if status != http.StatusOK || !got.Available || got.RangePartial {
			t.Fatalf("完整组织窗口应可判断: status=%d payload=%+v", status, got)
		}
		if len(got.Companies) != 0 || got.MemberTotal != 0 {
			t.Fatalf("仅存在两天的新成员不得被标成连续30天无消费: %+v", got)
		}
		assertNoFollowUpSourceQueries(t, counts)
	})

	t.Run("right_edge_gap_is_not_treated_as_zero_consumption", func(t *testing.T) {
		m, counts := newFactsFollowUpMonitor(t)
		seedPublishedUsageFactsForTest(t, m, []int64{1}, fromTs, fromTs+usageFactDaySeconds)
		counts.reset()

		status, got := requestFollowUpsAt(t, m, now)
		if status != http.StatusOK {
			t.Fatalf("status=%d want=200", status)
		}
		if !got.Enabled || got.Available || !got.RangePartial {
			t.Fatalf("右侧 29 天未发布时必须显式不可判断: %+v", got)
		}
		if got.AvailableDays != 1 || got.RequestedDays != followUpWindowDays {
			t.Fatalf("右侧覆盖数错误: %+v", got)
		}
		if len(got.Companies) != 0 || got.MemberTotal != 0 || !strings.Contains(got.Message, "1/30") {
			t.Fatalf("右侧缺口不得生成无消费/催充值结论: %+v", got)
		}
		assertNoFollowUpSourceQueries(t, counts)
	})

	t.Run("no_published_window_returns_200", func(t *testing.T) {
		m, counts := newFactsFollowUpMonitor(t)
		m.setUsageFactsReadiness(false, 0)
		counts.reset()

		status, got := requestFollowUpsAt(t, m, now)
		if status != http.StatusOK || !got.Enabled || got.Available {
			t.Fatalf("无发布窗口应 200 局部降级: status=%d payload=%+v", status, got)
		}
		if got.AvailableDays != 0 || got.RangePartial || len(got.Companies) != 0 || got.Message == "" {
			t.Fatalf("无发布窗口的覆盖说明不完整: %+v", got)
		}
		assertNoFollowUpSourceQueries(t, counts)
	})

	t.Run("facts_read_failure_returns_200_without_fallback", func(t *testing.T) {
		m, counts := newFactsFollowUpMonitor(t)
		seedPublishedUsageFactsForTest(t, m, []int64{1}, fromTs, toTs)
		if err := m.usageFactsStore().Exec("DROP TABLE usage_daily_facts").Error; err != nil {
			t.Fatal(err)
		}
		counts.reset()

		status, got := requestFollowUpsAt(t, m, now)
		if status != http.StatusOK || !got.Enabled || got.Available {
			t.Fatalf("facts 读故障应 200 局部降级: status=%d payload=%+v", status, got)
		}
		if len(got.Companies) != 0 || got.MemberTotal != 0 || got.Message == "" {
			t.Fatalf("facts 读故障不得伪造跟进结论: %+v", got)
		}
		if strings.Contains(strings.ToLower(got.Message), "database") || strings.Contains(strings.ToLower(got.Message), "sqlite") {
			t.Fatalf("响应不得暴露底层库错误: %q", got.Message)
		}
		assertNoFollowUpSourceQueries(t, counts)
	})
}

func TestFollowUpsUsesLastThirtyCompleteCSTDays(t *testing.T) {
	for _, now := range []time.Time{
		time.Date(2026, 7, 9, 0, 0, 0, 0, usageCST),
		time.Date(2026, 7, 9, 12, 34, 56, 0, usageCST),
		time.Date(2026, 7, 9, 23, 59, 59, 0, usageCST),
	} {
		fromTs, toTs := followUpTestWindow(now.Unix())
		wantTo := time.Date(2026, 7, 9, 0, 0, 0, 0, usageCST).Unix()
		if toTs != wantTo || toTs-fromTs != int64(followUpWindowDays)*usageFactDaySeconds {
			t.Fatalf("now=%s window=[%s,%s) want through=%s", now,
				time.Unix(fromTs, 0).In(usageCST), time.Unix(toTs, 0).In(usageCST),
				time.Unix(wantTo, 0).In(usageCST))
		}
	}
}
