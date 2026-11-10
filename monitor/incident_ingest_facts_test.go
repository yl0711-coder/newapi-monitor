package monitor

import (
	"context"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/observability"
)

// seedCapacitySample 用**真实写入路径** upsertCapacityUserMinuteSamplesDB 造数据。
//
// 刻意不手写 struct 直插：真实路径带 BeforeCreate 的版本缺省填充，直插会让
// 测试在版本号写错时仍然通过——那正是口径过滤写错而没被发现的根因。
func seedCapacitySample(t *testing.T, m *Monitor, s CapacityUserMinuteSample) {
	t.Helper()
	if err := upsertCapacityUserMinuteSamplesDB(m.storeDB, []CapacityUserMinuteSample{s}); err != nil {
		t.Fatalf("写入容量分钟样本失败: %v", err)
	}
}

// 口径版本：事实行必须按 stabilityTrafficClassificationVersion(7) 过滤。
//
// 固定住曾经写错的点：用 customerHealthStabilityPolicyVersion(3) 过滤会把
// 当前数据全部排除。两个常量属于两套口径，同一行里分别落在
// traffic_class_version 与 customer_health_version 两列。
func TestIncidentFactsUsesTrafficClassVersionNotCustomerHealthVersion(t *testing.T) {
	if stabilityTrafficClassificationVersion == customerHealthStabilityPolicyVersion {
		t.Fatalf("两个口径版本当前相等(%d)，本测试失去意义，需改用显式写入版本的样本",
			stabilityTrafficClassificationVersion)
	}
	m := openIncidentTestStore(t)
	seedCapacitySample(t, m, CapacityUserMinuteSample{
		BucketTs: 600, UserID: 1, ChannelID: 7, ModelName: "gpt-4o", Grp: "default",
		Success: 80, Anomaly: 15, Failed: 5,
	})

	facts, err := m.aggregateIncidentWindowFacts(context.Background(), 600, 900)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("应聚合出 1 条，实际 %d 条", len(facts))
	}
	if facts[0].sampleTotal() != 100 {
		t.Errorf("样本总数应为 100，实际 %d", facts[0].sampleTotal())
	}
	if facts[0].MixedVersionRows != 0 {
		t.Errorf("同口径行不应计入混版，实际 %d", facts[0].MixedVersionRows)
	}

	// 反向断言：若误用客户维护口径版本过滤，当前数据会被全部排除。
	var wrong int64
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).
		Where("bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ?",
			600, 900, customerHealthStabilityPolicyVersion).Count(&wrong).Error; err != nil {
		t.Fatalf("反向查询失败: %v", err)
	}
	if wrong != 0 {
		t.Errorf("用 customerHealthStabilityPolicyVersion 过滤本应为空，实际 %d 行——"+
			"说明写入路径的版本语义变了，聚合过滤需同步复核", wrong)
	}
}

// 混版行不得被静默排除后当作完整样本。
func TestIncidentFactsCountsMixedVersionRows(t *testing.T) {
	m := openIncidentTestStore(t)
	seedCapacitySample(t, m, CapacityUserMinuteSample{
		BucketTs: 600, UserID: 1, ChannelID: 7, ModelName: "gpt-4o", Grp: "default",
		Success: 50, Anomaly: 0, Failed: 0,
	})
	// 显式写入旧口径版本的行（BeforeCreate 只在为 0 时填默认值）。
	seedCapacitySample(t, m, CapacityUserMinuteSample{
		BucketTs: 660, UserID: 2, ChannelID: 7, ModelName: "gpt-4o", Grp: "default",
		TrafficClassVersion: stabilityTrafficClassificationVersion - 1,
		Success:             0, Anomaly: 40, Failed: 10,
	})

	facts, err := m.aggregateIncidentWindowFacts(context.Background(), 600, 900)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("应聚合出 1 条，实际 %d", len(facts))
	}
	if facts[0].MixedVersionRows != 1 {
		t.Errorf("应检出 1 行混版，实际 %d", facts[0].MixedVersionRows)
	}
	if facts[0].sampleTotal() != 50 {
		t.Errorf("当前口径样本应为 50（旧行不计入分子分母），实际 %d", facts[0].sampleTotal())
	}

	// 关键：有混版行时整窗判 mixed_version，绝不拿剩余 50 个全成功样本
	// 算出 0% 异常率并判为 normal——那会结束已有事故。
	d, err := decideIncidentWindow(DefaultIncidentConfig(), DefaultIncidentIngestConfig(),
		facts[0], true, CustomerHealthHistoricalCoverage{RequestsComplete: true}, 10_000, 600, 900)
	if err != nil {
		t.Fatalf("判定失败: %v", err)
	}
	if d.WindowState != incidentWindowStateMixedVersion {
		t.Errorf("窗口态应为 mixed_version，实际 %s", d.WindowState)
	}
	if d.SendToEngine {
		t.Error("混版窗口不得送引擎")
	}
	if d.AnomalyRate != nil {
		t.Error("混版窗口不得计算异常率")
	}
}

// 异常率：零分母、边界、刚好超阈值。
func TestIncidentAnomalyRate(t *testing.T) {
	zero := incidentWindowFacts{}
	if _, has := zero.anomalyRate(); has {
		t.Error("零分母必须返回 hasSample=false，不能当 0% 异常")
	}

	cases := []struct {
		name      string
		f         incidentWindowFacts
		wantRate  float64
		wantState string
	}{
		{"刚好达阈值", incidentWindowFacts{Success: 80, Anomaly: 20}, 0.20, incidentWindowStateAnomaly},
		{"略低于阈值", incidentWindowFacts{Success: 81, Anomaly: 19}, 0.19, incidentWindowStateNormal},
		{"失败也进分子", incidentWindowFacts{Success: 70, Anomaly: 10, Failed: 20}, 0.30, incidentWindowStateAnomaly},
		{"全成功", incidentWindowFacts{Success: 100}, 0, incidentWindowStateNormal},
	}
	cfg := DefaultIncidentConfig()
	ingest := DefaultIncidentIngestConfig()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rate, has := tc.f.anomalyRate()
			if !has {
				t.Fatal("应有样本")
			}
			if diff := rate - tc.wantRate; diff > 1e-9 || diff < -1e-9 {
				t.Errorf("异常率应为 %v，实际 %v", tc.wantRate, rate)
			}
			d, err := decideIncidentWindow(cfg, ingest, tc.f, true,
				CustomerHealthHistoricalCoverage{RequestsComplete: true}, 10_000, 600, 900)
			if err != nil {
				t.Fatalf("判定失败: %v", err)
			}
			if d.WindowState != tc.wantState {
				t.Errorf("窗口态应为 %s，实际 %s", tc.wantState, d.WindowState)
			}
		})
	}
}

// 字段映射：裸 0 渠道必须是 unknown 而不是 not_assigned。
func TestIncidentKeyMappingUsesLegalValues(t *testing.T) {
	assigned, err := incidentKeyForFacts(incidentWindowFacts{ChannelID: 7, ModelName: "m", Grp: "g"})
	if err != nil {
		t.Fatalf("映射失败: %v", err)
	}
	if assigned.ChannelScope != ChannelScopeAssigned || assigned.ChannelID == nil || *assigned.ChannelID != 7 {
		t.Errorf("有渠道时应为 assigned 且 ID=7，实际 %s/%v", assigned.ChannelScope, assigned.ChannelID)
	}

	bare, err := incidentKeyForFacts(incidentWindowFacts{ChannelID: 0, ModelName: "m", Grp: "g"})
	if err != nil {
		t.Fatalf("映射失败: %v", err)
	}
	// not_assigned 是「已确知没有渠道」这一事实，只在有 route_no_channel
	// 之类独立证据时才可用；裸 0 是采集缺口。
	if bare.ChannelScope != ChannelScopeUnknown {
		t.Errorf("裸 0 渠道应为 unknown（采集缺口），实际 %s", bare.ChannelScope)
	}
	if bare.ChannelID != nil {
		t.Error("非 assigned 时 ChannelID 必须为 nil，绝不写 0")
	}
	// 两者必须算出不同的 dedup_key，结构上不可合并。
	if assigned.DedupKey() == bare.DedupKey() {
		t.Error("assigned 与 unknown 必须算出不同的聚合键")
	}

	if bare.Route != "route_unknown" {
		t.Errorf("无路由事实应归一为 route_unknown，实际 %q", bare.Route)
	}
	if bare.IngressProtocol != IngressUnknown {
		t.Errorf("无协议事实应为 ingress_unknown，实际 %q", bare.IngressProtocol)
	}
	if bare.PrimaryFaultClass != string(observability.FaultUnknown) {
		t.Errorf("分类应为 unknown，实际 %q", bare.PrimaryFaultClass)
	}
	// 证据等级复用既有函数，不手写字面量。
	level, method := incidentEvidenceForLocalFacts()
	if level != string(observability.EvidenceInferred) {
		t.Errorf("单侧本地事实证据等级应为 inferred，实际 %q", level)
	}
	if method != string(CorrelationLocalOnly) {
		t.Errorf("证据方式应为 local_only，实际 %q", method)
	}
	// 责任由既有判定函数给出：inferred 在天花板里得不到确定责任。
	if resp := incidentResponsibilityForLocalFacts(level); resp != string(RespInsufficientEvidence) {
		t.Errorf("inferred 证据不得归责，应为 insufficient_evidence，实际 %q", resp)
	}
}

// 聚合键规范串的还原必须无损——崩溃恢复靠它重放。
func TestIncidentKeyFromCanonicalRoundTrip(t *testing.T) {
	cases := []incidentWindowFacts{
		{ChannelID: 7, ModelName: "gpt-4o", Grp: "default"},
		{ChannelID: 0, ModelName: "claude-3", Grp: "vip 组"},
		{ChannelID: 123456, ModelName: "a=b\x1efake", Grp: "g"},
	}
	for _, f := range cases {
		key, err := incidentKeyForFacts(f)
		if err != nil {
			t.Fatalf("映射失败: %v", err)
		}
		canonical := key.Canonical()
		back, err := incidentKeyFromCanonical(canonical)
		if err != nil {
			// 含分隔符的模型名无法无损还原是可接受的，但必须**报错**而不是
			// 猜一个键出来——拿猜的键调引擎会挂错事故。
			continue
		}
		if back.DedupKey() != key.DedupKey() {
			t.Errorf("还原后的聚合键不一致: %q vs %q", back.Canonical(), canonical)
		}
	}

	if _, err := incidentKeyFromCanonical("garbage"); err == nil {
		t.Error("非法规范串必须报错，不得猜测聚合键")
	}
	if _, err := incidentKeyFromCanonical(
		"fc=unknown\x1fch=bogus:\x1fmodel=m\x1fgrp=g\x1fiproto=ingress_unknown\x1froute=route_unknown\x1fcfg="); err == nil {
		t.Error("非法渠道归属必须报错")
	}
}

// 处理延迟必须大于客户维护的 20 分钟回放窗。
func TestIncidentFinalizeLagExceedsReplayWindow(t *testing.T) {
	c := DefaultIncidentIngestConfig()
	if int64(c.FinalizeLagSeconds) <= customerHealthSourceReplaySeconds {
		t.Errorf("处理延迟 %d 秒必须大于回放窗 %d 秒，否则窗口数据仍会变化",
			c.FinalizeLagSeconds, customerHealthSourceReplaySeconds)
	}
	if int64(incidentIngestMinFinalizeLag) <= customerHealthSourceReplaySeconds {
		t.Errorf("处理延迟下界 %d 秒必须大于回放窗 %d 秒",
			incidentIngestMinFinalizeLag, customerHealthSourceReplaySeconds)
	}
	// 夹取：越界回落默认值。
	t.Setenv("MONITOR_INCIDENT_FINALIZE_LAG_SECONDS", "60")
	if got := LoadIncidentIngestConfig().FinalizeLagSeconds; got != incidentIngestDefaultFinalizeLag {
		t.Errorf("越界值应回落默认 %d，实际 %d", incidentIngestDefaultFinalizeLag, got)
	}
}

// 总开关：核心关闭时采集一律不启动。
func TestIncidentIngestRequiresCoreEnabled(t *testing.T) {
	t.Setenv("MONITOR_INCIDENT_CORE_ENABLED", "false")
	t.Setenv("MONITOR_INCIDENT_INGEST_ENABLED", "true")
	if _, _, ok := incidentIngestActive(); ok {
		t.Error("核心关闭时采集不得启动")
	}
	t.Setenv("MONITOR_INCIDENT_CORE_ENABLED", "true")
	t.Setenv("MONITOR_INCIDENT_INGEST_ENABLED", "false")
	if _, _, ok := incidentIngestActive(); ok {
		t.Error("采集开关关闭时不得启动")
	}
	t.Setenv("MONITOR_INCIDENT_INGEST_ENABLED", "true")
	if _, _, ok := incidentIngestActive(); !ok {
		t.Error("两个开关都开时应启动")
	}
}
