package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func modelFRTThresholdReport(t *testing.T, m *Monitor, seconds float64) ModelFRTReport {
	t.Helper()
	ms := int64(seconds * 1000)
	r, err := m.buildModelFRTReportFiltered(context.Background(), "24h", "", ModelFRTFilter{MeanGtMs: &ms}, modelFRTTestNow)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// 阈值必须是「严格大于」，并且按渠道真实平均值判断，不看 P95 或最大值。
func TestModelFRTMeanThresholdStrictlyGreaterUsesRealChannelMean(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	// 造数须与 modelFRTInsert 的分桶一致：<=500 进 ttft500，其余进 ttft5k(>2000)，
	// 否则直方图与 ttft_max_ms 自相矛盾，会被正确判为 invalid_histogram。
	//
	// 渠道 1 平均 2000ms：两条样本 400 与 3600。它的最大值是 3600、P95 落在 5k 桶，
	// 都高于 3 秒；只有真实均值 2000ms 低于 3 秒。所以若实现误用 P95 或最大值，
	// 渠道 1 会在 3 秒阈值下被错误保留——这正是本断言要钉住的。
	modelFRTMeanInsert(t, m, end, 1, "g", "fast", 1, 400)
	modelFRTMeanInsert(t, m, end, 1, "g", "slow", 1, 3600)
	modelFRTMeanInsert(t, m, end, 2, "g", "slow", 1, 4000) // 平均 4000ms
	// modelFRTInsert 对 ms>500 会同时置 ttft_over_3s，而校验要求 over_3s>0 时
	// max_ms>3000。所以自洽的取值只有两段：ms<=500，或 ms>3000。
	// 渠道 3 用 400 与 3600 两条，均值正好 2000ms，用于验证「等于阈值不算命中」。
	modelFRTMeanInsert(t, m, end, 3, "g", "fast", 1, 400)
	modelFRTMeanInsert(t, m, end, 3, "g", "slow", 1, 3600)

	// 前置条件：三个渠道的均值都必须可计算，否则下面的断言证明不了筛选口径。
	base := modelFRTBuild(t, m, "24h", "")
	for _, c := range base.Channels {
		if c.FRT.MeanMs == nil {
			t.Fatalf("前置条件：渠道 %d 均值应可计算，实为 %q", c.ChannelID, c.FRT.MeanUnavailableReason)
		}
	}

	r := modelFRTThresholdReport(t, m, 3)
	if len(r.Channels) != 1 || r.Channels[0].ChannelID != 2 {
		t.Fatalf("阈值 3 秒应只留渠道 2（真实均值 4000ms）：%+v", r.Channels)
	}
	if !r.MeanFilterApplied || r.MeanFilterExcluded != 0 {
		t.Fatalf("已应用阈值且无不可计算渠道：applied=%v excluded=%d", r.MeanFilterApplied, r.MeanFilterExcluded)
	}

	// 等于阈值不算命中：渠道 1 和 3 的均值都正好是 2000ms，2 秒阈值下两者
	// 必须同时被排除，只留均值 4000ms 的渠道 2。
	r = modelFRTThresholdReport(t, m, 2)
	if len(r.Channels) != 1 || r.Channels[0].ChannelID != 2 {
		t.Fatalf("平均值等于阈值不得命中，应只留渠道 2：%+v", r.Channels)
	}

	// 低于两者的阈值下，三个渠道都应命中，证明排除确实来自阈值比较而非造数缺陷。
	if r = modelFRTThresholdReport(t, m, 1); len(r.Channels) != 3 {
		t.Fatalf("1 秒阈值下三个渠道都应命中：%+v", r.Channels)
	}
}

// 命中渠道下的分组和模型必须原样保留，不按各自均值再筛一遍。
func TestModelFRTMeanThresholdKeepsWholeSubtreeOfMatchedChannel(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	// modelFRTInsert 把 ms>500 的样本放进 ttft5k 桶（2000–5000ms），所以 max 必须
	// 落在该桶内：合法区间是 (3000, 5000]，再往上会被判 invalid_histogram。
	// 4000 与 100 自洽，渠道均值 (4000+100)/2 = 2050ms。
	modelFRTMeanInsert(t, m, end, 1, "slowgroup", "slowmodel", 1, 4000)
	modelFRTMeanInsert(t, m, end, 1, "fastgroup", "fastmodel", 1, 100)

	// 前置条件：渠道均值必须可计算，否则下面证明不了「子树未被二次过滤」。
	base := modelFRTBuild(t, m, "24h", "")
	if len(base.Channels) != 1 || base.Channels[0].FRT.MeanMs == nil {
		t.Fatalf("前置条件：渠道 1 均值应可计算：%+v", base.Channels)
	}

	r := modelFRTThresholdReport(t, m, 1)
	if len(r.Channels) != 1 {
		t.Fatalf("渠道 1 均值 2050ms 应命中 1 秒阈值：%+v", r.Channels)
	}
	groups := map[string]string{}
	for _, g := range r.Channels[0].Groups {
		if len(g.Models) != 1 {
			t.Fatalf("分组 %s 的模型应原样保留：%+v", g.Group, g.Models)
		}
		groups[g.Group] = g.Models[0].Model
	}
	// fastgroup 均值 100ms 远低于阈值，但它在命中渠道下，必须保留。
	if groups["fastgroup"] != "fastmodel" || groups["slowgroup"] != "slowmodel" {
		t.Fatalf("命中渠道的子树不得被阈值二次过滤：%+v", groups)
	}
}

// 不可计算的渠道不参与筛选：既不保留，也必须单独报数，不能静默消失。
func TestModelFRTMeanThresholdExcludesUncomputableChannelsWithCount(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	modelFRTMeanInsert(t, m, end, 1, "g", "slow", 1, 4000)
	// 这两行走 modelFRTInsert（不写 frt_sum_ms），所以均值因「缺历史总和」不可计算，
	// 而不是因为直方图非法——4000 在 ttft5k 桶内自洽，直方图本身有效。
	modelFRTInsert(t, m, end, 2, "g", "historical", 1, 4000)
	modelFRTInsert(t, m, end, 3, "g", "historical", 1, 4500)

	// 先确认不加阈值时这两个渠道的均值确实不可计算，否则本测试证明不了任何事。
	base := modelFRTBuild(t, m, "24h", "")
	uncomputable := 0
	for _, c := range base.Channels {
		if c.FRT.MeanMs != nil {
			continue
		}
		uncomputable++
		// 必须是「缺历史总和」，不能是直方图非法——否则本用例验的就不是预期场景。
		if c.FRT.MeanUnavailableReason != "historical_sum_unavailable" {
			t.Fatalf("渠道 %d 应因缺总和不可计算，实为 %q", c.ChannelID, c.FRT.MeanUnavailableReason)
		}
	}
	if uncomputable != 2 {
		t.Fatalf("前置条件：应有 2 个渠道均值不可计算，实为 %d", uncomputable)
	}

	r := modelFRTThresholdReport(t, m, 1)
	if len(r.Channels) != 1 || r.Channels[0].ChannelID != 1 {
		t.Fatalf("不可计算渠道不得作为命中保留：%+v", r.Channels)
	}
	if r.MeanFilterExcluded != 2 {
		t.Fatalf("不可计算渠道必须单独报数：excluded=%d", r.MeanFilterExcluded)
	}
}

// 不加阈值时行为与报数都不得改变，避免新筛选影响既有页面。
func TestModelFRTMeanThresholdAbsentLeavesReportUnchanged(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	modelFRTMeanInsert(t, m, end, 1, "g", "slow", 1, 4000)
	modelFRTInsert(t, m, end, 2, "g", "historical", 1, 4000)

	r := modelFRTBuild(t, m, "24h", "")
	if len(r.Channels) != 2 {
		t.Fatalf("未筛选时应保留全部渠道：%+v", r.Channels)
	}
	if r.MeanFilterApplied || r.MeanFilterExcluded != 0 {
		t.Fatalf("未筛选时不得报告剔除：applied=%v excluded=%d", r.MeanFilterApplied, r.MeanFilterExcluded)
	}
}

// HTTP 层：秒转毫秒、边界与拒绝非法输入。
func TestModelFRTMeanThresholdHTTPParsingAndRejection(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	// 400 与 3600 两条自洽样本，渠道均值 2000ms。
	modelFRTMeanInsert(t, m, end, 1, "g", "fast", 1, 400)
	modelFRTMeanInsert(t, m, end, 1, "g", "slow", 1, 3600)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/model-statistics/frt", m.serveModelFRTReport)

	get := func(query string) (int, ModelFRTReport) {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/model-statistics/frt?window=24h"+query, nil))
		var report ModelFRTReport
		_ = json.Unmarshal(w.Body.Bytes(), &report)
		return w.Code, report
	}

	// 小数秒必须生效：1.9 秒 → 1900ms，渠道均值 2000ms 严格大于它。
	code, report := get("&mean_gt_s=1.9")
	if code != http.StatusOK || len(report.Channels) != 1 {
		t.Fatalf("1.9 秒阈值应命中 2000ms 渠道：code=%d channels=%+v", code, report.Channels)
	}
	if report.Filter.MeanGtMs == nil || *report.Filter.MeanGtMs != 1900 {
		t.Fatalf("秒应转为毫秒整数 1900：%+v", report.Filter.MeanGtMs)
	}

	// 2 秒恰好等于均值，严格大于不成立。
	if code, report = get("&mean_gt_s=2"); code != http.StatusOK || len(report.Channels) != 0 {
		t.Fatalf("等于均值不得命中：code=%d channels=%+v", code, report.Channels)
	}

	// 空值等于不筛选。
	if code, report = get("&mean_gt_s="); code != http.StatusOK || report.MeanFilterApplied {
		t.Fatalf("空阈值应视为不筛选：code=%d applied=%v", code, report.MeanFilterApplied)
	}

	for _, bad := range []string{"abc", "-1", "3601", "NaN"} {
		if code, _ = get("&mean_gt_s=" + bad); code != http.StatusBadRequest {
			t.Fatalf("非法阈值 %q 应返回 400，实为 %d", bad, code)
		}
	}
}
