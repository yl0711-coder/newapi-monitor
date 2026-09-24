package monitor

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestModelStatisticsMergesRoutedAndUnavailableChannelRequests(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.RetentionDays = 7
	now := time.Unix(1_800_000_123, 0)
	to := now.Unix() / 60 * 60
	if err := m.storeDB.Create([]CapacityUserMinuteSample{
		{BucketTs: to - 60, UserID: 1, Username: "old-alice", ChannelID: 1, ModelName: "gpt-main", Grp: "group-a", TrafficClassVersion: stabilityTrafficClassificationVersion, Success: 10, Failed: 2},
		{BucketTs: to - 60, UserID: 2, ChannelID: 2, ModelName: "gpt-other", Grp: "group-a", TrafficClassVersion: stabilityTrafficClassificationVersion, Success: 5},
		{BucketTs: to - 25*3600, UserID: 3, ChannelID: 3, ModelName: "old-model", Grp: "group-a", TrafficClassVersion: stabilityTrafficClassificationVersion, Success: 9},
		{BucketTs: to - 60, UserID: 4, ChannelID: 4, ModelName: "gpt-main", Grp: "group-b", TrafficClassVersion: stabilityTrafficClassificationVersion, Anomaly: 4},
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create([]UserDirectoryEntry{
		{UserID: 1, Username: "alice", SyncedAt: to},
		{UserID: 2, Username: "bob", SyncedAt: to},
		{UserID: 4, Username: "dana", SyncedAt: to},
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create([]RejectionSample{
		{BucketTs: to - 60, Node: "worker-a", Reason: "no_available_channel", Model: "gpt-other", Grp: "group-a", UserID: 1, Count: 3},
		{BucketTs: to - 60, Node: "worker-b", Reason: "no_channel", Model: "gpt-missing", Grp: "group-a", UserID: 2, Count: 2},
		{BucketTs: to - 60, Node: "worker-a", Reason: "invalid_token", Model: "ignored", Grp: "group-a", UserID: 1, Count: 99},
	}).Error; err != nil {
		t.Fatal(err)
	}

	report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
	if err != nil {
		t.Fatal(err)
	}
	if report.GroupCount != 2 || len(report.Models) != 3 || report.Models[0].Model != "gpt-main" {
		t.Fatalf("模型应按总请求数倒序并保留分组数: %+v", report)
	}
	model := report.Models[0]
	if model.Requests != 16 || model.RoutedRequests != 16 || model.UnavailableRoutes != 0 {
		t.Fatalf("模型总量口径错误: %+v", model)
	}
	if len(model.Groups) != 2 || model.Groups[0].Group != "group-a" || model.Groups[0].Requests != 12 || model.Groups[1].Group != "group-b" || model.Groups[1].Requests != 4 {
		t.Fatalf("模型展开后的分组排序错误: %+v", model.Groups)
	}
	if customers := model.Groups[0].Customers; len(customers) != 1 || customers[0].CustomerID != 1 || customers[0].CustomerName != "alice" || customers[0].Requests != 12 || customers[0].SharePct != 100 {
		t.Fatalf("模型分组下的客户明细错误: %+v", customers)
	}
	var other, missing ModelStatisticsModel
	for _, row := range report.Models {
		if row.Model == "gpt-other" {
			other = row
		}
		if row.Model == "gpt-missing" {
			missing = row
		}
	}
	if other.Requests != 8 || other.RoutedRequests != 5 || other.UnavailableRoutes != 3 || missing.Requests != 2 || missing.UnavailableRoutes != 2 {
		t.Fatalf("无可用渠道模型需求未计入或合并错误: other=%+v missing=%+v", other, missing)
	}
	if customers := other.Groups[0].Customers; len(customers) != 2 ||
		customers[0].CustomerID != 2 || customers[0].CustomerName != "bob" || customers[0].Requests != 5 || customers[0].SharePct != 62.5 ||
		customers[1].CustomerID != 1 || customers[1].CustomerName != "alice" || customers[1].Requests != 3 || customers[1].SharePct != 37.5 {
		t.Fatalf("已路由与无可用渠道请求没有按客户正确拆分: %+v", customers)
	}
	if customers := missing.Groups[0].Customers; len(customers) != 1 || customers[0].CustomerID != 2 || customers[0].CustomerName != "bob" || customers[0].Requests != 2 || customers[0].SharePct != 100 {
		t.Fatalf("纯无可用渠道请求没有关联到客户: %+v", customers)
	}
	for _, row := range report.Models {
		if row.Model == "ignored" {
			t.Fatal("普通前置拒绝不应混入模型统计")
		}
	}

	// CloudWatch 连续水位覆盖的分钟以 direct lane 为唯一事实；旧采集器在
	// Shadow 过渡期仍可能保留同一事件，不能把两个来源相加成双倍需求。
	m.cfg.CloudWatchPreRouteEnabled = true
	if err := m.storeDB.Create([]RejectionSample{
		{BucketTs: to - 60, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", Model: "gpt-other", Grp: "group-a", UserID: 1, Count: 3},
		{BucketTs: to - 60, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", Model: "gpt-missing", Grp: "group-a", UserID: 2, Count: 2},
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: to - 24*3600, NextTs: to,
		ThroughTs: to, TargetThroughTs: to, SemanticsVersion: cloudWatchPreRouteVersion, Status: "caught_up",
	}).Error; err != nil {
		t.Fatal(err)
	}
	deduplicated, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(deduplicated.Models) != len(report.Models) {
		t.Fatalf("切换 CloudWatch 覆盖后模型集合不应改变: before=%+v after=%+v", report.Models, deduplicated.Models)
	}
	for i := range report.Models {
		if deduplicated.Models[i].Model != report.Models[i].Model || deduplicated.Models[i].Requests != report.Models[i].Requests ||
			deduplicated.Models[i].UnavailableRoutes != report.Models[i].UnavailableRoutes {
			t.Fatalf("CloudWatch 与旧采集器重叠时发生重复计数: before=%+v after=%+v", report.Models, deduplicated.Models)
		}
	}

	short, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
	if err != nil || len(short.Models) != 3 {
		t.Fatalf("近24小时结果错误: err=%v report=%+v", err, short)
	}
	long, err := m.buildModelStatisticsReport(context.Background(), "7d", now)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range long.Models {
		if row.Model == "old-model" && row.Requests != 9 {
			t.Fatalf("近7天应包含旧记录且不影响近24小时: %+v", row)
		}
	}
}

func TestModelStatisticsRejectsUnsupportedWindowAndRetentionOverflow(t *testing.T) {
	if _, err := modelStatisticsWindowFor("30d"); err == nil {
		t.Fatal("不支持的时间范围必须拒绝")
	}
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.RetentionDays = 3
	if _, err := m.buildModelStatisticsReport(context.Background(), "7d", time.Unix(1_800_000_123, 0)); err == nil {
		t.Fatal("超过当前事实留存的近7天必须拒绝")
	}
}

func TestModelStatisticsMarksUnprovenFactCoverage(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.RetentionDays = 7
	m.cfg.CapacityEnabled = true
	now := time.Unix(1_800_000_123, 0)
	to := now.Unix() / 60 * 60
	if err := m.storeDB.Create(&CapacityUserMinuteSample{
		BucketTs: to - 60, UserID: 1, ChannelID: 1, ModelName: "gpt-main", Grp: "group-a",
		TrafficClassVersion: stabilityTrafficClassificationVersion, Success: 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
	if err != nil {
		t.Fatal(err)
	}
	if report.Source.FactsComplete {
		t.Fatalf("没有连续水位时不能把模型排名标为完整: %+v", report.Source)
	}
	if !strings.Contains(report.Source.Note, "不完整") {
		t.Fatalf("不完整覆盖必须在说明中明确提示: %q", report.Source.Note)
	}
}

// CloudWatch 直采覆盖并不等于“这一分钟所有旧行都已被观察到”。只有同一
// canonical reason、模型、分组和 user_id 的直采行存在时，旧行才是重叠副本；
// 身份缺失(user_id=0)或直采没有对应维度的旧行必须继续保留。
func TestModelStatisticsCloudWatchPrecedenceRequiresMatchingDimensions(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.RetentionDays = 7
	m.cfg.CloudWatchPreRouteEnabled = true
	now := time.Unix(1_800_000_123, 0)
	to := now.Unix() / 60 * 60
	from := to - 120
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: from, NextTs: to,
		ThroughTs: to, TargetThroughTs: to, SemanticsVersion: cloudWatchPreRouteVersion,
		Status: "caught_up",
	}).Error; err != nil {
		t.Fatal(err)
	}
	rows := []RejectionSample{
		// Same full dimension: legacy is a duplicate and must be suppressed.
		{BucketTs: to - 60, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", Model: "same", Grp: "g", UserID: 7, Count: 3},
		{BucketTs: to - 60, Node: "legacy", Reason: "no_available_channel", Model: "same", Grp: "g", UserID: 7, Count: 99},
		// Direct identity is known, but the legacy row has user_id=0.  It is
		// not provably the same request and must remain visible.
		{BucketTs: to - 60, Node: "legacy", Reason: "no.channel", Model: "same", Grp: "g", UserID: 0, Count: 11},
		// No direct row for this model: coverage alone must not hide it.
		{BucketTs: to - 60, Node: "legacy", Reason: "no_channel", Model: "missing", Grp: "g", UserID: 7, Count: 13},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
	if err != nil {
		t.Fatal(err)
	}
	var same, missing ModelStatisticsModel
	for _, model := range report.Models {
		switch model.Model {
		case "same":
			same = model
		case "missing":
			missing = model
		}
	}
	if same.Requests != 14 || same.UnavailableRoutes != 14 {
		t.Fatalf("只应去掉同维度 legacy，got same=%+v", same)
	}
	if len(same.Groups) != 1 || len(same.Groups[0].Customers) != 2 ||
		same.Groups[0].Customers[0].CustomerID != 0 || same.Groups[0].Customers[0].Requests != 11 ||
		same.Groups[0].Customers[1].CustomerID != 7 || same.Groups[0].Customers[1].Requests != 3 {
		t.Fatalf("user_id=0 与 direct user_id=7 不得错误去重: %+v", same.Groups)
	}
	if missing.Requests != 13 || missing.UnavailableRoutes != 13 {
		t.Fatalf("无对应 direct 维度的 legacy 行不能因覆盖水位被隐藏: %+v", missing)
	}
}

func TestModelStatisticsKeepsLegacyWhenCloudWatchLaneDisabled(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.RetentionDays = 7
	// Deliberately leave CloudWatchPreRouteEnabled false while retaining a
	// cursor from an earlier run.  The stale cursor must be ignored.
	now := time.Unix(1_800_000_123, 0)
	to := now.Unix() / 60 * 60
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: to - 60, NextTs: to,
		ThroughTs: to, TargetThroughTs: to, SemanticsVersion: cloudWatchPreRouteVersion,
		Status: "caught_up",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&RejectionSample{
		BucketTs: to - 60, Node: "legacy", Reason: "no_channel", Model: "legacy-only", Grp: "g", UserID: 4, Count: 17,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&CapacityUserMinuteSample{
		BucketTs: to - 60, UserID: -9, ChannelID: 1, ModelName: "invalid-customer", Grp: "g",
		TrafficClassVersion: stabilityTrafficClassificationVersion, Success: 99,
	}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Models) != 1 || report.Models[0].Model != "legacy-only" || report.Models[0].Requests != 17 {
		t.Fatalf("关闭 CloudWatch 直采时 stale cursor 不得隐藏 legacy: %+v", report)
	}
}

func TestModelStatisticsFallsBackWhenCloudWatchCursorTableMissing(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.RetentionDays = 7
	m.cfg.CloudWatchPreRouteEnabled = true
	now := time.Unix(1_800_000_123, 0)
	to := now.Unix() / 60 * 60
	if err := m.storeDB.Migrator().DropTable(&CloudWatchPreRouteCursor{}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&RejectionSample{
		BucketTs: to - 60, Node: "legacy", Reason: "no_channel", Model: "legacy-only", Grp: "g", UserID: 4, Count: 17,
	}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Models) != 1 || report.Models[0].Model != "legacy-only" || report.Models[0].Requests != 17 {
		t.Fatalf("missing cursor table must not fail or hide legacy facts: %+v", report)
	}
}

// When the direct lane is disabled, already-published direct facts can still
// overlap the legacy collector.  Suppress only a fully matching positive-user
// duplicate; keep unmatched legacy dimensions and unknown identities.
func TestModelStatisticsDeduplicatesResidualDirectWhenDisabled(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.RetentionDays = 7
	m.cfg.CloudWatchPreRouteEnabled = false
	now := time.Unix(1_800_000_123, 0)
	to := now.Unix() / 60 * 60
	from := to - 60
	if err := m.storeDB.Migrator().DropTable(&CloudWatchPreRouteCursor{}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create([]RejectionSample{
		{BucketTs: from, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", Model: "same", Grp: "g", UserID: 7, Count: 3},
		{BucketTs: from, Node: "legacy", Reason: "no_available_channel", Model: "same", Grp: "g", UserID: 7, Count: 99},
		{BucketTs: from, Node: "legacy", Reason: "no_channel", Model: "missing", Grp: "g", UserID: 7, Count: 5},
		{BucketTs: from, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", Model: "unknown", Grp: "g", UserID: 0, Count: 3},
		{BucketTs: from, Node: "legacy", Reason: "no_available_channel", Model: "unknown", Grp: "g", UserID: 0, Count: 99},
	}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
	if err != nil {
		t.Fatal(err)
	}
	var same, missing, unknown ModelStatisticsModel
	for _, model := range report.Models {
		switch model.Model {
		case "same":
			same = model
		case "missing":
			missing = model
		case "unknown":
			unknown = model
		}
	}
	if same.Requests != 3 || same.UnavailableRoutes != 3 {
		t.Fatalf("known residual duplicate was not suppressed: %+v", same)
	}
	if missing.Requests != 5 || missing.UnavailableRoutes != 5 {
		t.Fatalf("unmatched legacy model was hidden: %+v", missing)
	}
	if unknown.Requests != 102 || unknown.UnavailableRoutes != 102 {
		t.Fatalf("unknown identity overlap must remain fail-open: %+v", unknown)
	}
}

func TestModelStatisticsReportsCloudWatchCoverageRisk(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.RetentionDays = 7
	m.cfg.CloudWatchPreRouteEnabled = true
	m.cfg.CloudWatchPreRouteLookbackHours = 168
	now := time.Date(2026, 9, 22, 12, 34, 45, 0, time.UTC)
	_, target := cloudWatchPreRouteRange(now, m.cfg.CloudWatchPreRouteLookbackHours)
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: target - 24*3600,
		NextTs: target - 60, ThroughTs: target - 60, TargetThroughTs: target,
		SemanticsVersion: cloudWatchPreRouteVersion, Status: "running",
	}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(report.Source.Note, "覆盖范围不完整") {
		t.Fatalf("模型统计应暴露 CloudWatch 覆盖风险: %q", report.Source.Note)
	}
}

func TestModelStatisticsKeepsUnknownIdentityOverlap(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.RetentionDays = 7
	m.cfg.CloudWatchPreRouteEnabled = true
	now := time.Unix(1_800_000_123, 0)
	to := now.Unix() / 60 * 60
	from := to - 60
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: from, NextTs: to,
		ThroughTs: to, TargetThroughTs: to, SemanticsVersion: cloudWatchPreRouteVersion,
		Status: "caught_up",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create([]RejectionSample{
		{BucketTs: from, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", Model: "unknown", Grp: "g", UserID: 0, Count: 3},
		{BucketTs: from, Node: "legacy", Reason: "no_available_channel", Model: "unknown", Grp: "g", UserID: 0, Count: 99},
	}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Models) != 1 || report.Models[0].Model != "unknown" || report.Models[0].Requests != 102 || report.Models[0].UnavailableRoutes != 102 {
		t.Fatalf("unknown identity overlap must remain fail-open: %+v", report)
	}
	if report.Source.RequestsAreUnique || !strings.Contains(report.Source.Note, "user_id=0") || !strings.Contains(report.Source.Note, "重复计数") {
		t.Fatalf("模型统计必须明确未知身份可能保守重复: %+v", report.Source)
	}
}

func TestModelStatisticsCustomerDrilldownUIContract(t *testing.T) {
	js := string(modelStatisticsJS)
	for _, want := range []string{"data-ms-group-key", "ms-group-row", "customer_id", "customer_name", "客户 ID", "客户名", "占该分组", "ms-model-row-high-unavailable", "unavailable_channel_requests", ">0.4"} {
		if !strings.Contains(js, want) {
			t.Errorf("模型统计客户下钻展示缺少 %q", want)
		}
	}
}
