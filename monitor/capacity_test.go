package monitor

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestCapacityBucketPolicy(t *testing.T) {
	for _, tc := range []struct {
		hours int
		want  int64
	}{{1, 60}, {6, 60}, {24, 300}, {168, 900}} {
		if got := capacityBucketSeconds(tc.hours); got != tc.want {
			t.Fatalf("hours=%d bucket=%d want=%d", tc.hours, got, tc.want)
		}
	}
	if capacityHours("7") != 24 || capacityHours("168") != 168 {
		t.Fatal("时间范围必须只接受有界白名单")
	}
}

func TestAggregateCapacitySeriesKeepsTrueMinutePeak(t *testing.T) {
	rows := []capacityMinuteRow{
		{Ts: 300, Success: 8, Anomaly: 1, Failed: 1, Tokens: 1000, SumUseTime: 60, Lat2: 8},
		{Ts: 360, Success: 18, Anomaly: 1, Failed: 1, Tokens: 4000, SumUseTime: 120, Lat5: 18},
	}
	series, summary := aggregateCapacitySeries(rows, map[int64]int64{300: 2, 360: 3, 420: 4}, 300, 600, 300)
	if len(series) != 1 {
		t.Fatalf("应聚合为一个 5 分钟点，得 %d", len(series))
	}
	p := series[0]
	if p.BusinessRequests != 39 || p.RejectedRequests != 9 {
		t.Fatalf("业务/拒绝请求口径错误: %+v", p)
	}
	if p.BusinessRPM == nil || math.Abs(*p.BusinessRPM-7.8) > .0001 {
		t.Fatalf("5 分钟均值应为 (日志30+拒绝9)/5=7.8，得 %+v", p.BusinessRPM)
	}
	if summary.PeakBusinessRPM == nil || *summary.PeakBusinessRPM != 23 || summary.PeakAt != 360 {
		t.Fatalf("必须保留原始分钟峰值 20+3，得 %+v", summary)
	}
	if summary.CurrentBusinessRPM == nil || *summary.CurrentBusinessRPM != 4 || summary.CurrentTPM != nil {
		t.Fatalf("最新分钟只有前置拒绝时，RPM 应可观测且 TPM 必须保持未知: %+v", summary)
	}
	if summary.CurrentAt != 420 {
		t.Fatalf("最新观测必须携带精确分钟: %+v", summary)
	}
	if summary.PeakTPM == nil || *summary.PeakTPM != 4000 || summary.PeakConcurrency == nil || *summary.PeakConcurrency != 2 {
		t.Fatalf("TPM/成功请求占用分钟峰值计算错误: %+v", summary)
	}
	if p.P95Seconds == nil || *p.P95Seconds != 5 {
		t.Fatalf("延迟直方图 p95 计算错误: %+v", p.P95Seconds)
	}
	if summary.StabilityPct == nil || math.Abs(*summary.StabilityPct-66.6666667) > .001 {
		t.Fatalf("日志稳定率口径错误: %+v", summary.StabilityPct)
	}
}

func TestBuildCapacityReportUsesOnlyLocalFactsAndFilters(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.CapacityEnabled = true
	m.cfg.IngestToken = "configured"
	rows := []MetricSample{
		{BucketTs: 120, ChannelID: 7, ModelName: "m1", Grp: "g1", Success: 8, Failed: 2, Tokens: 1000, SumUseTime: 60},
		{BucketTs: 180, ChannelID: 8, ModelName: "m2", Grp: "g2", Success: 100, Tokens: 9000},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelSnap{ID: 7, Name: "channel-seven"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&RejectionSample{BucketTs: 120, Node: "master", Reason: "no_channel", Model: "m1", Grp: "g1", Count: 3}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&NginxMinuteSample{BucketTs: 120, Node: "master", Route: "/v1/responses", Method: "POST", Status: 200, UpstreamStatus: 200, Count: 12, RequestTimeSumMS: 1200, RequestTimeMaxMS: 200, UpstreamTimeSumMS: 900, UpstreamTimeCount: 12}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&NginxMinuteSample{BucketTs: 120, Node: "master", Route: "/api/status", Method: "GET", Status: 200, Count: 999}).Error; err != nil {
		t.Fatal(err)
	}

	// prodDB 故意保持 nil：若容量页偷读生产库，此测试必然失败。
	report, err := m.buildCapacityReport(context.Background(), 60, 240, 60, 7, "g1", "m1", 300)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.LoggedRequests != 10 || report.Summary.RejectedRequests != 0 {
		t.Fatalf("渠道筛选不能将无渠道维度的前置拒绝伪归因: %+v", report.Summary)
	}
	if len(report.Ingress) != 1 || report.Ingress[0].Requests != 12 {
		t.Fatalf("入口仅允许 POST 推理路由，得 %+v", report.Ingress)
	}
	if math.Abs(report.Ingress[0].EstimatedInflight-.02) > .0001 || math.Abs(report.Ingress[0].EstimatedUpstreamInflight-.015) > .0001 {
		t.Fatalf("入口在途占用估算错误: %+v", report.Ingress[0])
	}
	if got := report.Breakdowns["channels"]; len(got) != 1 || got[0].Label != "#7 channel-seven" {
		t.Fatalf("渠道维度映射错误: %+v", got)
	}
}

func TestCapacityHandlerDisabledDoesNoStoreRead(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := &Monitor{cfg: Settings{CapacityEnabled: false}}
	r := gin.New()
	r.GET("/capacity/report", m.serveCapacityReport)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/capacity/report?hours=168", nil))
	if w.Code != 200 {
		t.Fatalf("status=%d", w.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["enabled"] != false {
		t.Fatalf("默认关闭应只返回 enabled:false: %s", w.Body.String())
	}
}

func TestCapacityHandlerRejectsInvalidChannel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.cfg.CapacityEnabled = true
	r := gin.New()
	r.GET("/capacity/report", m.serveCapacityReport)
	for _, raw := range []string{"abc", "-1"} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/capacity/report?channel="+raw, nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("channel=%q status=%d body=%s", raw, w.Code, w.Body.String())
		}
	}
}

func TestCapacityEmptyDimensionsAndChannelZeroAreFilterable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.cfg.CapacityEnabled = true
	minute := time.Now().Unix()/60*60 - 60
	rows := []MetricSample{
		{BucketTs: minute, ChannelID: 0, ModelName: "", Grp: "", Success: 3},
		{BucketTs: minute, ChannelID: 8, ModelName: "named", Grp: "named", Success: 99},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.GET("/capacity/report", m.serveCapacityReport)
	path := "/capacity/report?hours=1&channel=0&group=" + capacityDimensionKey("") + "&model=" + capacityDimensionKey("")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var report capacityReport
	if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Summary.LoggedRequests != 3 {
		t.Fatalf("空维度/channel 0 筛选未命中真实空值: %+v", report.Summary)
	}
	foundEmptyGroup := false
	for _, option := range report.Options["groups"] {
		if option.Key == capacityDimensionKey("") && option.Label == "未标记" {
			foundEmptyGroup = true
		}
	}
	if !foundEmptyGroup {
		t.Fatalf("空维度必须使用独立 key 与展示名: %+v", report.Options["groups"])
	}
}

func TestCapacityOptionsAreCompleteAndRankingsAreBounded(t *testing.T) {
	m := newTestMonitor(t)
	rows := make([]capacityDimensionRow, 0, 25)
	for i := 1; i <= 25; i++ {
		rows = append(rows, capacityDimensionRow{ChannelID: i, ModelName: "model-" + strconv.Itoa(i), Grp: "group-" + strconv.Itoa(i), Success: int64(i)})
	}
	breakdowns, options := m.capacityBreakdowns(rows, nil, 3600, 0, "", "", false)
	if len(options["channels"]) != 25 || len(options["groups"]) != 25 || len(options["models"]) != 25 {
		t.Fatalf("筛选目录不能被 Top20 排名截断: %+v", options)
	}
	if len(breakdowns["channels"]) != 20 || len(breakdowns["groups"]) != 20 || len(breakdowns["models"]) != 20 {
		t.Fatalf("排名必须限制为 Top20: %+v", breakdowns)
	}
}

func TestCapacityBreakdownIncludesAttributableRejections(t *testing.T) {
	m := newTestMonitor(t)
	metrics := []capacityDimensionRow{{ChannelID: 7, ModelName: "m1", Grp: "g1", Success: 8, Failed: 2, Tokens: 100}}
	rejections := []capacityRejectionDimensionRow{{ModelName: "m1", Grp: "g1", Count: 5}}
	breakdowns, _ := m.capacityBreakdowns(metrics, rejections, 60, 0, "", "", true)
	for _, dimension := range []string{"groups", "models"} {
		got := breakdowns[dimension]
		if len(got) != 1 || got[0].Requests != 15 || got[0].RejectedRequests != 5 || got[0].StabilityScope != "log_plus_pre_route" {
			t.Fatalf("%s 排名必须纳入可归因前置拒绝: %+v", dimension, got)
		}
	}
	got := breakdowns["channels"]
	if len(got) != 1 || got[0].Requests != 10 || got[0].RejectedRequests != 0 || got[0].StabilityScope != "log_only" {
		t.Fatalf("渠道不能伪归因选路前拒绝: %+v", got)
	}
}

func TestCapacityHandlerExcludesCurrentMinuteAndOldTrafficVersion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.cfg.CapacityEnabled = true
	currentMinute := time.Now().Unix() / 60 * 60
	rows := []MetricSample{
		{BucketTs: currentMinute - 120, ChannelID: 1, ModelName: "current", Grp: "g", Success: 3},
		{BucketTs: currentMinute - 60, ChannelID: 8, ModelName: "other", Grp: "g", Success: 4},
		{BucketTs: currentMinute, ChannelID: 1, ModelName: "incomplete", Grp: "g", Success: 99},
		{BucketTs: currentMinute - 60, ChannelID: 2, ModelName: "old", Grp: "g", TrafficClassVersion: userTrafficClassificationVersion - 1, Success: 88},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.GET("/capacity/report", m.serveCapacityReport)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/capacity/report?hours=1&channel=1", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var report capacityReport
	if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Summary.LoggedRequests != 3 || report.Meta.ToTs != currentMinute {
		t.Fatalf("当前未完整分钟或旧口径混入容量事实: %+v", report.Summary)
	}
	source := report.Meta.Sources["business_log"]
	if source.Watermark != currentMinute-60 || source.FilteredWatermark != currentMinute-120 || report.Summary.CurrentAt != currentMinute-120 {
		t.Fatalf("全局采集水位、筛选最后事件和最新观测必须分离: source=%+v summary=%+v", source, report.Summary)
	}
}

func TestCapacityOptionalSourcesFailOpenButBusinessSourceIsRequired(t *testing.T) {
	m := newTestMonitor(t)
	if err := m.storeDB.Create(&MetricSample{BucketTs: 120, ChannelID: 1, ModelName: "m", Grp: "g", Success: 1}).Error; err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"rejection_samples", "rejection_ingest_batches", "nginx_minute_samples", "infra_samples"} {
		if err := m.storeDB.Migrator().DropTable(table); err != nil {
			t.Fatal(err)
		}
	}
	report, err := m.buildCapacityReport(context.Background(), 60, 180, 60, 0, "", "", 240)
	if err != nil || report.Summary.LoggedRequests != 1 {
		t.Fatalf("可选源缺失不能拖垮业务容量页: report=%+v err=%v", report.Summary, err)
	}
	if report.Meta.Sources["pre_route_rejection"].Available || report.Meta.Sources["nginx_ingress"].Available || report.Meta.Sources["infrastructure"].Available {
		t.Fatalf("缺失源不得伪报可用: %+v", report.Meta.Sources)
	}
	if err := m.storeDB.Migrator().DropTable("metric_samples"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.buildCapacityReport(context.Background(), 60, 180, 60, 0, "", "", 240); err == nil {
		t.Fatal("必需的业务分钟事实缺失时必须失败关闭")
	}
}

func TestCapacityMetricPlanUsesBucketIndex(t *testing.T) {
	m := newTestMonitor(t)
	type planRow struct{ Detail string }
	var plan []planRow
	err := m.storeDB.Raw(`EXPLAIN QUERY PLAN SELECT bucket_ts, SUM(success) FROM metric_samples
		WHERE bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ? GROUP BY bucket_ts`, 1, 2, userTrafficClassificationVersion).Scan(&plan).Error
	if err != nil {
		t.Fatal(err)
	}
	var joined strings.Builder
	for _, row := range plan {
		joined.WriteString(row.Detail)
		joined.WriteByte('\n')
	}
	if !strings.Contains(strings.ToLower(joined.String()), "index") {
		t.Fatalf("容量查询不得退化为无索引全表扫描: %s", joined.String())
	}
}

func TestCapacitySevenDaySeriesIsBounded(t *testing.T) {
	m := newTestMonitor(t)
	const minutes = 7 * 24 * 60
	rows := make([]MetricSample, 0, minutes)
	for i := 0; i < minutes; i++ {
		rows = append(rows, MetricSample{
			BucketTs: int64(i+1) * 60, ChannelID: i%25 + 1,
			ModelName: "m" + strconv.Itoa(i%30), Grp: "g" + strconv.Itoa(i%35),
			Success: 1, Tokens: 100, SumUseTime: 2,
		})
	}
	if err := m.storeDB.CreateInBatches(rows, 250).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), capacityQueryTimeout)
	defer cancel()
	start := time.Now()
	report, err := m.buildCapacityReport(ctx, 60, int64(minutes+1)*60, 900, 0, "", "", int64(minutes+2)*60)
	if err != nil {
		t.Fatal(err)
	}
	// 区间两端各可能有一个不完整的 UTC 15 分钟桶，因此最多 672+1 点。
	if len(report.Series) > 7*24*4+1 || len(report.Breakdowns["groups"]) > 20 || len(report.Options["groups"]) != 35 {
		t.Fatalf("7 天响应未保持有界: series=%d breakdown=%d options=%d", len(report.Series), len(report.Breakdowns["groups"]), len(report.Options["groups"]))
	}
	if elapsed := time.Since(start); elapsed >= capacityQueryTimeout {
		t.Fatalf("7 天本地事实查询超过硬超时: %s", elapsed)
	}
}
