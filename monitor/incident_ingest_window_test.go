package monitor

import (
	"context"
	"testing"
)

// incidentIngestTestEnv 开启两个开关并返回配置。
func incidentIngestTestEnv(t *testing.T) (IncidentConfig, IncidentIngestConfig) {
	t.Helper()
	t.Setenv("MONITOR_INCIDENT_CORE_ENABLED", "true")
	t.Setenv("MONITOR_INCIDENT_INGEST_ENABLED", "true")
	return LoadIncidentConfig(), LoadIncidentIngestConfig()
}

// coverageComplete 构造「覆盖完整」凭证。
func coverageComplete(from, to int64) CustomerHealthHistoricalCoverage {
	return CustomerHealthHistoricalCoverage{
		RequestFromTs: from, RequestThroughTs: to, RequestsComplete: true,
	}
}

// ★ 无样本绝不结束事故 ★
//
// 引擎的 updateActiveIncident 里有 `if in.DataPresent && !in.Anomalous {
// status=resolved; closeSegment }`。把无样本传成 DataPresent=true/
// Anomalous=false 就等于把「没有流量」当成「已恢复」。
func TestIncidentNoSampleDoesNotResolveIncident(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	const winSec = int64(IncidentWindowSeconds)
	base := int64(100_000) / winSec * winSec

	// 1. 异常窗建单。
	anomalous := incidentWindowFacts{ChannelID: 7, ModelName: "gpt-4o", Grp: "default",
		Success: 50, Anomaly: 50}
	d, err := decideIncidentWindow(core, ingest, anomalous, true,
		coverageComplete(base, base+winSec), base+10*winSec, base, base+winSec)
	if err != nil {
		t.Fatalf("判定失败: %v", err)
	}
	if d.WindowState != incidentWindowStateAnomaly {
		t.Fatalf("应判异常，实际 %s", d.WindowState)
	}
	res, err := m.IngestIncidentObservation(ctx, d.observationInputFor(core.MinSample))
	if err != nil {
		t.Fatalf("写入事故失败: %v", err)
	}
	if res.IncidentID == "" {
		t.Fatalf("应建出事故，实际 action=%s", res.Action)
	}
	incidentID := res.IncidentID

	var before Incident
	if err := m.storeDB.Where("id = ?", incidentID).Take(&before).Error; err != nil {
		t.Fatalf("读事故失败: %v", err)
	}
	if before.Status != "open" {
		t.Fatalf("事故应为 open，实际 %s", before.Status)
	}

	// 2. 连续 3 个无样本窗（覆盖完整但该键无匹配行）。
	empty := incidentWindowFacts{ChannelID: 7, ModelName: "gpt-4o", Grp: "default"}
	obsBefore := incidentObservationCount(t, m, incidentID)
	for i := int64(1); i <= 3; i++ {
		from := base + i*winSec
		nd, err := decideIncidentWindow(core, ingest, empty, false,
			coverageComplete(from, from+winSec), base+10*winSec, from, from+winSec)
		if err != nil {
			t.Fatalf("判定失败: %v", err)
		}
		if nd.WindowState != incidentWindowStateNoSample {
			t.Fatalf("第 %d 个窗应判 no_sample，实际 %s", i, nd.WindowState)
		}
		if nd.SendToEngine {
			t.Fatalf("第 %d 个无样本窗不得送引擎", i)
		}
	}

	// 3. 事故必须仍然 open、分段未关闭、没有新观测行。
	var after Incident
	if err := m.storeDB.Where("id = ?", incidentID).Take(&after).Error; err != nil {
		t.Fatalf("读事故失败: %v", err)
	}
	if after.Status != "open" {
		t.Errorf("无样本不代表恢复，事故必须仍为 open，实际 %s", after.Status)
	}
	if after.ResolvedTs != 0 {
		t.Errorf("resolved_ts 必须为 0，实际 %d", after.ResolvedTs)
	}
	if after.PeakSeverity != before.PeakSeverity {
		t.Errorf("无样本不得改变 peak_severity: %q → %q", before.PeakSeverity, after.PeakSeverity)
	}
	var openSegs int64
	m.storeDB.Model(&IncidentSegment{}).
		Where("incident_id = ? AND end_ts = 0", incidentID).Count(&openSegs)
	if openSegs == 0 {
		t.Error("观测段不得因无样本而关闭")
	}
	if got := incidentObservationCount(t, m, incidentID); got != obsBefore {
		t.Errorf("无样本窗不得产生观测行: %d → %d", obsBefore, got)
	}

	// 4. 只有「有样本且正常」的窗口才结束事故。
	normal := incidentWindowFacts{ChannelID: 7, ModelName: "gpt-4o", Grp: "default", Success: 100}
	from := base + 4*winSec
	nd, err := decideIncidentWindow(core, ingest, normal, true,
		coverageComplete(from, from+winSec), base+10*winSec, from, from+winSec)
	if err != nil {
		t.Fatalf("判定失败: %v", err)
	}
	if nd.WindowState != incidentWindowStateNormal || !nd.SendToEngine {
		t.Fatalf("应判 normal 并送引擎，实际 %s/%v", nd.WindowState, nd.SendToEngine)
	}
	if _, err := m.IngestIncidentObservation(ctx, nd.observationInputFor(core.MinSample)); err != nil {
		t.Fatalf("写入正常窗失败: %v", err)
	}
	var final Incident
	if err := m.storeDB.Where("id = ?", incidentID).Take(&final).Error; err != nil {
		t.Fatalf("读事故失败: %v", err)
	}
	if final.Status != "resolved" {
		t.Errorf("有样本且正常的窗口应结束观测段，实际 status=%s", final.Status)
	}
}

func incidentObservationCount(t *testing.T, m *Monitor, incidentID string) int64 {
	t.Helper()
	var n int64
	m.storeDB.Model(&IncidentObservation{}).Where("incident_id = ?", incidentID).Count(&n)
	return n
}

// ★ 缺口窗口必须保持原聚合键 ★
//
// PrimaryFaultClass 进 DedupKey。换成 telemetry_gap 会算出另一个键，
// 引擎就找不到原活跃事故，无法推进其缺口状态，反而可能建第二个事故。
func TestIncidentGapWindowKeepsSameDedupKey(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	const winSec = int64(IncidentWindowSeconds)
	base := int64(200_000) / winSec * winSec
	facts := incidentWindowFacts{ChannelID: 9, ModelName: "gpt-4o", Grp: "default",
		Success: 40, Anomaly: 60}

	// 异常窗建单。
	d1, err := decideIncidentWindow(core, ingest, facts, true,
		coverageComplete(base, base+winSec), base+10*winSec, base, base+winSec)
	if err != nil {
		t.Fatalf("判定失败: %v", err)
	}
	res1, err := m.IngestIncidentObservation(ctx, d1.observationInputFor(core.MinSample))
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	if res1.IncidentID == "" {
		t.Fatalf("应建出事故")
	}

	// 缺口窗：覆盖不完整且水位已越过。
	from := base + winSec
	d2, err := decideIncidentWindow(core, ingest, facts, true,
		CustomerHealthHistoricalCoverage{RequestsComplete: false, RequestThroughTs: from},
		base+10*winSec, from, from+winSec)
	if err != nil {
		t.Fatalf("判定失败: %v", err)
	}
	if d2.WindowState != incidentWindowStateGapObserved {
		t.Fatalf("应判 gap_observed，实际 %s", d2.WindowState)
	}
	if d2.DataComplete {
		t.Error("缺口窗的 data_complete 必须为 false")
	}
	// 关键断言：聚合键一致。
	if d2.DedupKey != d1.DedupKey {
		t.Fatalf("缺口窗必须保持原聚合键: %s vs %s", d2.DedupKey, d1.DedupKey)
	}
	if d2.Key.PrimaryFaultClass != "unknown" {
		t.Errorf("分类必须保持 unknown，实际 %q", d2.Key.PrimaryFaultClass)
	}
	in2 := d2.observationInputFor(core.MinSample)
	if in2.DataPresent {
		t.Error("缺口窗必须 DataPresent=false")
	}
	res2, err := m.IngestIncidentObservation(ctx, in2)
	if err != nil {
		t.Fatalf("写入缺口窗失败: %v", err)
	}
	if res2.IncidentID != res1.IncidentID {
		t.Errorf("缺口窗应推进同一事故 %s，实际 %s", res1.IncidentID, res2.IncidentID)
	}

	// 全仓范围断言：没有任何 telemetry_gap 进入 IncidentKey。
	var gapKeyed int64
	m.storeDB.Model(&Incident{}).
		Where("primary_fault_class = ?", "telemetry_gap").Count(&gapKeyed)
	if gapKeyed != 0 {
		t.Errorf("telemetry_gap 不得作为聚合键的分类落库，实际 %d 条", gapKeyed)
	}
	var incidents int64
	m.storeDB.Model(&Incident{}).Count(&incidents)
	if incidents != 1 {
		t.Errorf("不得因缺口窗新建第二个事故，实际共 %d 条", incidents)
	}
}

// wait_coverage 与 gap_observed 的区分判据必须两条路径都可达。
//
// 修正点：RequestsComplete 的算式让任何不连续都先落 complete=false，
// 所以不能靠「已覆盖但不连续」区分，必须比较断点与全局已证明前沿。
func TestIncidentWaitCoverageVsGapObserved(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	facts := incidentWindowFacts{ChannelID: 3, ModelName: "m", Grp: "g", Success: 10}
	const winSec = int64(IncidentWindowSeconds)
	from := int64(300_000) / winSec * winSec
	to := from + winSec

	// 断点就是水位本身 → 仍可能推进 → 等待。
	waiting, err := decideIncidentWindow(core, ingest, facts, true,
		CustomerHealthHistoricalCoverage{RequestsComplete: false, RequestThroughTs: from},
		from, from, to)
	if err != nil {
		t.Fatalf("判定失败: %v", err)
	}
	if waiting.WindowState != incidentWindowStateWaitCoverage {
		t.Errorf("水位未越过本窗时应等待，实际 %s", waiting.WindowState)
	}
	if waiting.SendToEngine {
		t.Error("等待窗不得送引擎")
	}

	// 水位已越过本窗但本窗仍不完整 → 当前确认存在缺口。
	gap, err := decideIncidentWindow(core, ingest, facts, true,
		CustomerHealthHistoricalCoverage{RequestsComplete: false, RequestThroughTs: from},
		to+winSec, from, to)
	if err != nil {
		t.Fatalf("判定失败: %v", err)
	}
	if gap.WindowState != incidentWindowStateGapObserved {
		t.Errorf("水位已越过时应判 gap_observed，实际 %s", gap.WindowState)
	}
	if !gap.SendToEngine {
		t.Error("缺口窗应送引擎以推进原事故的缺口观测")
	}
}

// low_sample 不是独立窗口态：它与正常/异常正交。
func TestIncidentBelowMinSampleIsOrthogonal(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	const winSec = int64(IncidentWindowSeconds)
	from := int64(400_000) / winSec * winSec

	// 低样本且超阈值 → 窗口态仍是 anomalous，另有 below_min_sample 标记。
	lowAnom := incidentWindowFacts{ChannelID: 1, ModelName: "m", Grp: "g", Success: 1, Anomaly: 4}
	d, err := decideIncidentWindow(core, ingest, lowAnom, true,
		coverageComplete(from, from+winSec), from+winSec*2, from, from+winSec)
	if err != nil {
		t.Fatalf("判定失败: %v", err)
	}
	if d.WindowState != incidentWindowStateAnomaly {
		t.Errorf("低样本但超阈值仍应判 anomalous，实际 %s", d.WindowState)
	}
	if !d.BelowMinSample {
		t.Error("样本数低于 MinSample 应标 below_min_sample")
	}
	// 样本数必须如实传给引擎，由它按 MinSample 自行决定 Outcome。
	in := d.observationInputFor(core.MinSample)
	if in.SampleTotal == nil || *in.SampleTotal != 5 {
		t.Errorf("样本总数应如实传入为 5，实际 %v", in.SampleTotal)
	}

	// 低样本且未超阈值 → normal + below_min_sample。
	lowNorm := incidentWindowFacts{ChannelID: 1, ModelName: "m", Grp: "g", Success: 5}
	d2, err := decideIncidentWindow(core, ingest, lowNorm, true,
		coverageComplete(from, from+winSec), from+winSec*2, from, from+winSec)
	if err != nil {
		t.Fatalf("判定失败: %v", err)
	}
	if d2.WindowState != incidentWindowStateNormal || !d2.BelowMinSample {
		t.Errorf("低样本未超阈值应为 normal + below_min_sample，实际 %s/%v",
			d2.WindowState, d2.BelowMinSample)
	}
}

// 场景一律留空（本轮能证明的场景集合为空），且待确认状态显式写入。
func TestIncidentScenarioPendingIsExplicit(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	const winSec = int64(IncidentWindowSeconds)
	from := int64(500_000) / winSec * winSec
	facts := incidentWindowFacts{ChannelID: 2, ModelName: "m", Grp: "g", Success: 50, Anomaly: 50}
	d, err := decideIncidentWindow(core, ingest, facts, true,
		coverageComplete(from, from+winSec), from+winSec*2, from, from+winSec)
	if err != nil {
		t.Fatalf("判定失败: %v", err)
	}
	if in := d.observationInputFor(core.MinSample); in.Scenario != "" {
		t.Errorf("无法证明场景时必须留空，不得塞虚假场景，实际 %q", in.Scenario)
	}
	row := incidentWindowStateRowFrom(d, 7, 1000)
	if !row.ScenarioPending {
		t.Error("待确认状态必须显式写入状态行")
	}
	if row.PendingReason != incidentPendingReasonNoFallback {
		t.Errorf("原因应为固定枚举 %q，实际 %q", incidentPendingReasonNoFallback, row.PendingReason)
	}
	// 快照必须随 input_frozen 一起落库，不能是 awaiting_source 带快照。
	if row.SampleTotal == nil || *row.SampleTotal != 100 {
		t.Errorf("判定输入快照应落库，实际 %v", row.SampleTotal)
	}

	// 等待类窗口不得带快照（与 input_frozen 区分开）。
	waiting := d
	waiting.WindowState = incidentWindowStateWaitCoverage
	wrow := incidentWindowStateRowFrom(waiting, 7, 1000)
	if wrow.SampleTotal != nil {
		t.Error("等待来源的窗口不得带判定输入快照")
	}
}
