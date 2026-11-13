package monitor

// B2 引擎测试：全部经真实业务入口 IngestIncidentObservation 验收。
//
// 与 B1 的区别：B1 手工 INSERT 验存储约束，这里调用真实写入路径，
// 复发三态、并发去重、重启续接都由引擎逻辑决定，不靠索引。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// enableIncidentForTest 打开功能开关（默认关闭）。
func enableIncidentForTest(t *testing.T) {
	t.Helper()
	t.Setenv("MONITOR_INCIDENT_CORE_ENABLED", "true")
}

func testIncidentKey() IncidentKey {
	cid := int64(7)
	return IncidentKey{
		PrimaryFaultClass: "upstream_5xx",
		ChannelScope:      ChannelScopeAssigned,
		ChannelID:         &cid,
		Model:             "gpt-4o",
		UserGroup:         "default",
		IngressProtocol:   IngressChatSSE,
		Route:             "/v1/chat/completions",
	}
}

// anomalyAt 构造一个异常窗口观测（300 秒窗，对齐分钟桶）。
func anomalyAt(fromTs int64) IncidentObservationInput {
	sample := int64(50)
	req := int64(40)
	return IncidentObservationInput{
		Key:              testIncidentKey(),
		WindowFromTs:     fromTs,
		WindowToTs:       fromTs + 300,
		DataPresent:      true,
		Anomalous:        true,
		Scenario:         string(ScenarioPrimaryNoFallback),
		HardSignal:       true,
		SourceCount:      2,
		SourceIDs:        "logs,nginx",
		SampleTotal:      &sample,
		DistinctRequests: &req,
		EvidenceLevel:    "correlated",
		EvidenceMethod:   "request_id",
		Note:             "上游 502",
	}
}

// anomalyWithEvidence 指定证据等级的异常窗口。
func anomalyWithEvidence(fromTs int64, level string) IncidentObservationInput {
	in := anomalyAt(fromTs)
	in.EvidenceLevel = level
	return in
}

// lowSampleAt 构造一个样本不足的异常窗口。
//
// 必须关掉 HardSignal：按 02.1 §8 硬信号不受样本门限约束，
// 带硬信号的低样本窗口会照常计入连续链，测不到样本门槛。
func lowSampleAt(fromTs int64) IncidentObservationInput {
	in := anomalyAt(fromTs)
	tiny := int64(1)
	in.SampleTotal = &tiny
	in.HardSignal = false
	return in
}

// gapAt 构造一个采集缺口窗口。
func gapAt(fromTs int64) IncidentObservationInput {
	in := anomalyAt(fromTs)
	in.DataPresent = false
	in.Anomalous = false
	in.SampleTotal = nil
	return in
}

// normalAt 构造一个正常窗口观测，用于结束事故观测段。
func normalAt(fromTs int64) IncidentObservationInput {
	in := anomalyAt(fromTs)
	in.Anomalous = false
	in.HardSignal = false
	return in
}

// 开关关闭时：引擎不写任何业务数据。
func TestEngineSkipsWhenDisabled(t *testing.T) {
	t.Setenv("MONITOR_INCIDENT_CORE_ENABLED", "")
	m := openIncidentTestStore(t)
	res, err := m.IngestIncidentObservation(context.Background(), anomalyAt(3000))
	if err != nil {
		t.Fatalf("关闭时不应报错: %v", err)
	}
	if res.Action != "skipped_disabled" {
		t.Errorf("期望 skipped_disabled，得到 %q", res.Action)
	}
	var n int64
	m.storeDB.Model(&Incident{}).Count(&n)
	if n != 0 {
		t.Errorf("开关关闭时不得写事故业务数据，实际写了 %d 条", n)
	}
}

// 真实入口的复发三态验收。
func TestEngineRecurrenceThreeStatesViaRealEntry(t *testing.T) {
	enableIncidentForTest(t)

	// 态一：持续未结束，冷却到期也不新建。
	t.Run("持续未结束_冷却到期不新建", func(t *testing.T) {
		m := openIncidentTestStore(t)
		ctx := context.Background()
		first, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
		if err != nil {
			t.Fatalf("首次: %v", err)
		}
		if first.Action != "created" {
			t.Fatalf("首次应建单，得到 %q", first.Action)
		}
		// 把冷却强制设为已过期，再送一次异常。
		if err := m.storeDB.Model(&Incident{}).Where("id = ?", first.IncidentID).
			Update("cooldown_until_ts", 1).Error; err != nil {
			t.Fatalf("改冷却: %v", err)
		}
		second, err := m.IngestIncidentObservation(ctx, anomalyAt(3300))
		if err != nil {
			t.Fatalf("第二次: %v", err)
		}
		if second.Action != "updated" || second.IncidentID != first.IncidentID {
			t.Errorf("事故未结束时冷却到期不得新建，得到 action=%q id=%s（原 %s）",
				second.Action, second.IncidentID, first.IncidentID)
		}
		var n int64
		m.storeDB.Model(&Incident{}).Count(&n)
		if n != 1 {
			t.Errorf("应仍只有 1 条事故，实际 %d", n)
		}
	})

	// 态二：已结束但冷却未到期，复用原事故追加新段。
	t.Run("已结束但冷却未到期_复用并追加段", func(t *testing.T) {
		m := openIncidentTestStore(t)
		ctx := context.Background()
		first, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
		if err != nil {
			t.Fatalf("首次: %v", err)
		}
		// 正常窗口 → 观测段结束。
		if _, err := m.IngestIncidentObservation(ctx, normalAt(3300)); err != nil {
			t.Fatalf("结束: %v", err)
		}
		var closed Incident
		if err := m.storeDB.Where("id = ?", first.IncidentID).Take(&closed).Error; err != nil {
			t.Fatalf("读: %v", err)
		}
		if closed.Status != "resolved" {
			t.Fatalf("正常窗口后应 resolved，实际 %q", closed.Status)
		}
		// 冷却未到期（默认 15~180 分钟，这里窗口才过 300 秒）时复发。
		again, err := m.IngestIncidentObservation(ctx, anomalyAt(3600))
		if err != nil {
			t.Fatalf("复发: %v", err)
		}
		if again.IncidentID != first.IncidentID {
			t.Errorf("冷却未到期应复用原事故 %s，却得到 %s", first.IncidentID, again.IncidentID)
		}
		if again.Action != "segment_appended" {
			t.Errorf("应追加新段，得到 %q", again.Action)
		}
		var incN, segN int64
		m.storeDB.Model(&Incident{}).Count(&incN)
		m.storeDB.Model(&IncidentSegment{}).Where("incident_id = ?", first.IncidentID).Count(&segN)
		if incN != 1 {
			t.Errorf("冷却期内复发不得新建事故，实际 %d 条", incN)
		}
		if segN != 2 {
			t.Errorf("应有 2 个段，实际 %d", segN)
		}
	})

	// 态三：已结束且冷却到期，新建事故且保留历史。
	t.Run("已结束且冷却到期_新建并保留历史", func(t *testing.T) {
		m := openIncidentTestStore(t)
		ctx := context.Background()
		first, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
		if err != nil {
			t.Fatalf("首次: %v", err)
		}
		if _, err := m.IngestIncidentObservation(ctx, normalAt(3300)); err != nil {
			t.Fatalf("结束: %v", err)
		}
		// 冷却到期。
		if err := m.storeDB.Model(&Incident{}).Where("id = ?", first.IncidentID).
			Update("cooldown_until_ts", 1).Error; err != nil {
			t.Fatalf("改冷却: %v", err)
		}
		again, err := m.IngestIncidentObservation(ctx, anomalyAt(3600))
		if err != nil {
			t.Fatalf("复发: %v", err)
		}
		if again.Action != "created" {
			t.Errorf("已结束且冷却到期应新建，得到 %q", again.Action)
		}
		if again.IncidentID == first.IncidentID {
			t.Error("应是新事故实例，不得复用原 ID")
		}
		// 原事故历史必须完整保留。
		var old Incident
		if err := m.storeDB.Where("id = ?", first.IncidentID).Take(&old).Error; err != nil {
			t.Fatalf("原事故历史丢失: %v", err)
		}
		if old.Status != "resolved" || old.ResolvedTs == 0 {
			t.Errorf("原事故被改写: status=%q resolved_ts=%d", old.Status, old.ResolvedTs)
		}
		var n int64
		m.storeDB.Model(&Incident{}).Count(&n)
		if n != 2 {
			t.Errorf("应并存新旧两条事故，实际 %d", n)
		}
	})
}

// 并发：N 个 goroutine 同时送同键异常，只能产生一条活跃事故。
// 经真实入口，验证引擎的并发去重而非仅索引。
func TestEngineConcurrentIngestSingleIncident(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	const n = 10
	var wg sync.WaitGroup
	errs := make([]error, n)
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := m.IngestIncidentObservation(context.Background(), anomalyAt(3000))
			errs[i], ids[i] = err, res.IncidentID
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d 出错: %v", i, err)
		}
	}
	var total, active int64
	m.storeDB.Model(&Incident{}).Count(&total)
	m.storeDB.Model(&Incident{}).
		Where("active_marker = ?", incidentActiveMarker).Count(&active)
	if total != 1 || active != 1 {
		t.Errorf("并发同键应只建 1 条事故，实际 total=%d active=%d", total, active)
	}
	// 所有成功的调用必须拿到同一个事故 ID。
	for i, id := range ids {
		if id != "" && id != ids[0] {
			t.Errorf("goroutine %d 拿到不同事故 ID %s（期望 %s）", i, id, ids[0])
		}
	}
	// 同窗重复扫描不得堆重复证据。
	var obs int64
	m.storeDB.Model(&IncidentObservation{}).Count(&obs)
	if obs != 1 {
		t.Errorf("同一段同一窗口应只留 1 条观测证据，实际 %d", obs)
	}
}

// 重启续接：重开库后继续送观测，必须接上原事故而非新建。
func TestEngineResumesAfterRestart(t *testing.T) {
	enableIncidentForTest(t)
	dir := t.TempDir()
	path := dir + "/t.db"
	ctx := context.Background()

	m1 := &Monitor{cfg: Settings{SessionSecret: "s"}, chNames: map[string]string{}}
	if err := m1.openStore(path); err != nil {
		t.Fatalf("开库: %v", err)
	}
	first, err := m1.IngestIncidentObservation(ctx, anomalyAt(3000))
	if err != nil {
		t.Fatalf("首次: %v", err)
	}
	if db, err := m1.storeDB.DB(); err == nil {
		_ = db.Close()
	}

	// 重启。
	m2 := &Monitor{cfg: Settings{SessionSecret: "s"}, chNames: map[string]string{}}
	if err := m2.openStore(path); err != nil {
		t.Fatalf("重启开库: %v", err)
	}
	next, err := m2.IngestIncidentObservation(ctx, anomalyAt(3300))
	if err != nil {
		t.Fatalf("重启后观测: %v", err)
	}
	if next.IncidentID != first.IncidentID {
		t.Errorf("重启后应接上原事故 %s，却得到 %s", first.IncidentID, next.IncidentID)
	}
	if next.Action != "updated" {
		t.Errorf("重启后应更新原事故，得到 %q", next.Action)
	}
	var n int64
	m2.storeDB.Model(&Incident{}).Count(&n)
	if n != 1 {
		t.Errorf("重启不得产生重复事故，实际 %d 条", n)
	}
	// 连续窗口状态必须延续，不从零重算。
	var cont IncidentContinuity
	if err := m2.storeDB.Where("incident_id = ?", first.IncidentID).Take(&cont).Error; err != nil {
		t.Fatalf("连续状态丢失: %v", err)
	}
	if cont.LastCountedWindowToTs != 3600 {
		t.Errorf("窗口水位未延续，得到 %d（期望 3600）", cont.LastCountedWindowToTs)
	}
}

// 引擎写入的治理状态必须是 01.3 五种枚举之一，且只写前两层。
func TestEngineWritesOnlyFirstTwoLayersWithValidEnum(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	res, err := m.IngestIncidentObservation(context.Background(), anomalyAt(3000))
	if err != nil {
		t.Fatalf("写入: %v", err)
	}
	var st IncidentState
	if err := m.storeDB.Where("incident_id = ?", res.IncidentID).Take(&st).Error; err != nil {
		t.Fatalf("读四层状态: %v", err)
	}
	if !validGovernanceState(st.ObservedState) {
		t.Errorf("观测层写入非法枚举 %q", st.ObservedState)
	}
	if !validGovernanceState(st.RecommendedState) {
		t.Errorf("建议层写入非法枚举 %q", st.RecommendedState)
	}
	// 第三层：引擎绝不代写审批。
	if st.ApprovedState != "" || st.ApprovedBy != "" || st.ApprovedAt != 0 {
		t.Errorf("引擎不得写审批层: state=%q by=%q at=%d",
			st.ApprovedState, st.ApprovedBy, st.ApprovedAt)
	}
	// 第四层：引擎绝不代写实际配置，四字段须为未知（nil）。
	if st.AppliedNewapiEnabled != nil || st.AppliedNewapiGroup != nil ||
		st.AppliedNewapiPriority != nil || st.AppliedNewapiWeight != nil {
		t.Error("引擎不得写 NewAPI 实际配置层，四字段应为 nil（未知）")
	}
	if st.AppliedSyncOK {
		t.Error("引擎未做只读同步，applied_sync_ok 应为 false")
	}
	if st.DriftDetected {
		t.Error("B2 不产出漂移判定")
	}
}

// 非法治理状态必须被拒绝写入，不能落库。
func TestEngineRejectsInvalidGovernanceState(t *testing.T) {
	for _, bad := range []string{"degraded", "quarantine_approved", "enabled", "ok", "x", ""} {
		if validGovernanceState(bad) {
			t.Errorf("%q 不属 01.3 枚举，必须被拒", bad)
		}
	}
	// 映射函数在任何严重度下都只能产出合法枚举。
	for _, sev := range []string{"SEV0", "SEV1", "SEV2", "SEV3", "", "SEV9"} {
		if got := governanceStateForSeverity(sev); !validGovernanceState(got) {
			t.Errorf("severity=%q 映射出非法观测状态 %q", sev, got)
		}
		for _, ev := range []string{evidenceLevelExact, evidenceLevelCorrelated,
			evidenceLevelInferred, evidenceLevelUnavailable} {
			if got := governanceStateRecommendedFor(sev, ev); !validGovernanceState(got) {
				t.Errorf("severity=%q evidence=%q 映射出非法建议状态 %q", sev, ev, got)
			}
		}
	}
}

// 01.3 §5 的处置上限：每种证据等级能支持的最强建议。
//
// 纠正了原先「correlated 应建议隔离」的错误断言——契约写的是
// correlated 最多支持 DEGRADED，隔离前需补强证据。
func TestEngineEvidenceLevelCapsRecommendation(t *testing.T) {
	// 只有 exact 能到隔离，且需要 SEV0/SEV1。
	for _, sev := range []string{"SEV0", "SEV1"} {
		if got := governanceStateRecommendedFor(sev, evidenceLevelExact); got != governanceStateQuarantined {
			t.Errorf("%s + exact 应可建议隔离，得到 %q", sev, got)
		}
	}
	// correlated 封顶 DEGRADED，严重度再高也不能越过。
	for _, sev := range []string{"SEV0", "SEV1"} {
		if got := governanceStateRecommendedFor(sev, evidenceLevelCorrelated); got != governanceStateDegraded {
			t.Errorf("%s + correlated 最多 DEGRADED（01.3 §5），得到 %q", sev, got)
		}
	}
	// inferred 只用于排查方向，unavailable 不支持处置结论：都封顶 SUSPECT。
	for _, ev := range []string{evidenceLevelInferred, evidenceLevelUnavailable} {
		for _, sev := range []string{"SEV0", "SEV1", "SEV2", "SEV3"} {
			if got := governanceStateRecommendedFor(sev, ev); got != governanceStateSuspect {
				t.Errorf("%s + %s 不支持处置结论，应为 SUSPECT，得到 %q", sev, ev, got)
			}
		}
	}
	// 严重度低时证据强也不越级：SEV2 + exact 仍是 DEGRADED。
	if got := governanceStateRecommendedFor("SEV2", evidenceLevelExact); got != governanceStateDegraded {
		t.Errorf("SEV2 + exact 应为 DEGRADED，得到 %q", got)
	}
	if got := governanceStateRecommendedFor("SEV3", evidenceLevelExact); got != governanceStateSuspect {
		t.Errorf("SEV3 + exact 应为 SUSPECT，得到 %q", got)
	}
}

// 经真实入口确认 correlated 不会落库成 QUARANTINED（review 复现的那条）。
func TestEngineCorrelatedNeverQuarantinedViaRealEntry(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	in := anomalyAt(3000)
	in.EvidenceLevel = evidenceLevelCorrelated
	// 连送三窗推到较高严重度。
	var last IncidentIngestResult
	for i := 0; i < 3; i++ {
		r, err := m.IngestIncidentObservation(ctx, anomalyWithEvidence(3000+int64(i)*300, evidenceLevelCorrelated))
		if err != nil {
			t.Fatalf("第 %d 窗: %v", i, err)
		}
		last = r
	}
	var st IncidentState
	if err := m.storeDB.Where("incident_id = ?", last.IncidentID).Take(&st).Error; err != nil {
		t.Fatalf("读状态: %v", err)
	}
	if st.RecommendedState == governanceStateQuarantined {
		t.Errorf("correlated 证据落库为 QUARANTINED，违反 01.3 §5（severity=%s）", last.Severity)
	}
	if !validGovernanceState(st.RecommendedState) {
		t.Errorf("建议层非法枚举 %q", st.RecommendedState)
	}
}

// 非法证据等级必须被入口拒绝，不落库。
func TestEngineRejectsInvalidEvidenceLevel(t *testing.T) {
	enableIncidentForTest(t)
	for _, bad := range []string{"indicative", "insufficient_evidence", "", "high", "medium", "low", "EXACT"} {
		t.Run("拒绝_"+bad, func(t *testing.T) {
			m := openIncidentTestStore(t)
			in := anomalyAt(3000)
			in.EvidenceLevel = bad
			if _, err := m.IngestIncidentObservation(context.Background(), in); err == nil {
				t.Errorf("证据等级 %q 非法，应被拒绝", bad)
			}
			var n int64
			m.storeDB.Model(&Incident{}).Count(&n)
			if n != 0 {
				t.Errorf("非法证据等级不得落库，实际 %d 条", n)
			}
		})
	}
	// 四种合法等级都应被接受。
	for _, ok := range []string{evidenceLevelExact, evidenceLevelCorrelated,
		evidenceLevelInferred, evidenceLevelUnavailable} {
		t.Run("接受_"+ok, func(t *testing.T) {
			m := openIncidentTestStore(t)
			in := anomalyAt(3000)
			in.EvidenceLevel = ok
			if _, err := m.IngestIncidentObservation(context.Background(), in); err != nil {
				t.Errorf("证据等级 %q 合法，却被拒: %v", ok, err)
			}
		})
	}
}

// 采集缺口不建单：未知不是异常。
func TestEngineGapDoesNotCreateIncident(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	in := anomalyAt(3000)
	in.DataPresent = false
	in.Anomalous = false
	res, err := m.IngestIncidentObservation(context.Background(), in)
	if err != nil {
		t.Fatalf("缺口观测: %v", err)
	}
	if res.Action != "skipped_no_anomaly" {
		t.Errorf("采集缺口不应建单，得到 %q", res.Action)
	}
	var n int64
	m.storeDB.Model(&Incident{}).Count(&n)
	if n != 0 {
		t.Errorf("缺口不得建单，实际 %d 条", n)
	}
}

// 未知计数经真实入口往返后仍是 nil，不退化成 0。
func TestEngineKeepsUnknownCountsNil(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	in := anomalyAt(3000)
	in.DistinctRequests = nil // 算不出来
	in.VisibleAttempts = nil
	in.LogRows = nil
	res, err := m.IngestIncidentObservation(context.Background(), in)
	if err != nil {
		t.Fatalf("写入: %v", err)
	}
	var ob IncidentObservation
	if err := m.storeDB.Where("incident_id = ?", res.IncidentID).Take(&ob).Error; err != nil {
		t.Fatalf("读观测: %v", err)
	}
	if ob.DistinctRequests != nil || ob.VisibleAttempts != nil || ob.LogRows != nil {
		t.Error("未知计数经引擎往返后退化成了具体值")
	}
}

// 只读旁路：引擎全程不碰生产库。
func TestEngineNeverTouchesProdDB(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	// prodDB 保持 nil：若引擎试图访问生产库会直接 panic/报错。
	if m.prodDB != nil {
		t.Fatal("测试前置：prodDB 应为 nil")
	}
	if _, err := m.IngestIncidentObservation(context.Background(), anomalyAt(3000)); err != nil {
		t.Fatalf("引擎应只用本地库即可完成写入: %v", err)
	}
	if m.prodDB != nil {
		t.Error("引擎不得建立生产库连接")
	}
}

// 窗口非法必须拒绝，不写脏数据。
func TestEngineRejectsBadWindow(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	in := anomalyAt(3000)
	in.WindowToTs = in.WindowFromTs // 零长窗口
	if _, err := m.IngestIncidentObservation(context.Background(), in); err == nil {
		t.Error("零长窗口应被拒绝")
	}
	var n int64
	m.storeDB.Model(&Incident{}).Count(&n)
	if n != 0 {
		t.Errorf("非法输入不得留下数据，实际 %d 条", n)
	}
}

// 多次复发产生不同事故实例 ID，历史不互相覆盖。
func TestEngineRecurrenceProducesDistinctInstanceIDs(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	seen := map[string]bool{}
	ts := int64(3000)
	for round := 0; round < 3; round++ {
		res, err := m.IngestIncidentObservation(ctx, anomalyAt(ts))
		if err != nil {
			t.Fatalf("第 %d 轮异常: %v", round, err)
		}
		seen[res.IncidentID] = true
		ts += 300
		if _, err := m.IngestIncidentObservation(ctx, normalAt(ts)); err != nil {
			t.Fatalf("第 %d 轮结束: %v", round, err)
		}
		// 冷却到期，下一轮应新建。
		if err := m.storeDB.Model(&Incident{}).
			Where("dedup_key = ? AND status = ?", testIncidentKey().DedupKey(), "resolved").
			Update("cooldown_until_ts", 1).Error; err != nil {
			t.Fatalf("改冷却: %v", err)
		}
		ts += 300
	}
	if len(seen) != 3 {
		t.Errorf("三轮复发应产生 3 个不同事故实例，实际 %d 个", len(seen))
	}
	var n int64
	m.storeDB.Model(&Incident{}).Count(&n)
	if n != 3 {
		t.Errorf("三条事故历史应全部保留，实际 %d", n)
	}
}

// review 问题 2：乱序旧正常窗口不得关闭仍活跃的事故。
func TestEngineStaleNormalWindowDoesNotResolveActive(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	// 两个异常窗口，事故活跃。
	first, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
	if err != nil {
		t.Fatalf("窗1: %v", err)
	}
	if _, err := m.IngestIncidentObservation(ctx, anomalyAt(3300)); err != nil {
		t.Fatalf("窗2: %v", err)
	}

	// 现在收到一个**更早**的正常窗口（乱序到达）。
	res, err := m.IngestIncidentObservation(ctx, normalAt(3000))
	if err != nil {
		t.Fatalf("乱序正常窗口: %v", err)
	}
	if res.Action != "evidence_only" {
		t.Errorf("旧窗口只应补证据，得到 action=%q", res.Action)
	}

	var inc Incident
	if err := m.storeDB.Where("id = ?", first.IncidentID).Take(&inc).Error; err != nil {
		t.Fatalf("读事故: %v", err)
	}
	if inc.Status == "resolved" {
		t.Error("乱序旧正常窗口把仍活跃的事故误改成了 resolved")
	}
	if inc.ActiveMarker != incidentActiveMarker {
		t.Error("事故不应让出活跃槽位")
	}
	if inc.ResolvedTs != 0 {
		t.Errorf("不应写入 resolved_ts，实际 %d", inc.ResolvedTs)
	}
}

// review 问题 2：事故结束后重放最初的异常窗口，不得重新打开或追加段。
func TestEngineReplayedAnomalyDoesNotReopenResolved(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	first, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
	if err != nil {
		t.Fatalf("窗1: %v", err)
	}
	if _, err := m.IngestIncidentObservation(ctx, normalAt(3300)); err != nil {
		t.Fatalf("结束: %v", err)
	}
	var segBefore int64
	m.storeDB.Model(&IncidentSegment{}).Where("incident_id = ?", first.IncidentID).Count(&segBefore)

	// 重放最初那个异常窗口（右界 3300 已判定过）。
	res, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
	if err != nil {
		t.Fatalf("重放: %v", err)
	}
	if res.Action != "evidence_only" {
		t.Errorf("重放旧异常窗口只应补证据，得到 %q", res.Action)
	}

	var inc Incident
	if err := m.storeDB.Where("id = ?", first.IncidentID).Take(&inc).Error; err != nil {
		t.Fatalf("读: %v", err)
	}
	if inc.Status != "resolved" {
		t.Errorf("重放旧窗口把已结束事故改回了 %q", inc.Status)
	}
	var segAfter int64
	m.storeDB.Model(&IncidentSegment{}).Where("incident_id = ?", first.IncidentID).Count(&segAfter)
	if segAfter != segBefore {
		t.Errorf("重放旧窗口凭空追加了段：%d → %d", segBefore, segAfter)
	}
	var n int64
	m.storeDB.Model(&Incident{}).Count(&n)
	if n != 1 {
		t.Errorf("不应新建事故，实际 %d 条", n)
	}
}

// 第二轮 review 问题 1：跨段重放。
//
// 第一段结束、第二段复发后重放第一段的旧窗口，证据必须回到**第一段**，
// 不能写到当前（第二）段，也不能产生重复证据。
func TestEngineReplayAcrossSegmentsKeepsOriginalSegment(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	// 第一段。
	first, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
	if err != nil {
		t.Fatalf("段1窗1: %v", err)
	}
	seg1 := first.SegmentID
	if _, err := m.IngestIncidentObservation(ctx, normalAt(3300)); err != nil {
		t.Fatalf("结束段1: %v", err)
	}
	// 冷却期内复发 → 第二段。
	rec, err := m.IngestIncidentObservation(ctx, anomalyAt(3600))
	if err != nil {
		t.Fatalf("段2: %v", err)
	}
	if rec.Action != "segment_appended" {
		t.Fatalf("测试前置：应追加第二段，实际 %q", rec.Action)
	}
	seg2 := rec.SegmentID
	if seg1 == seg2 {
		t.Fatal("测试前置：两段应不同")
	}

	countBefore := func() int64 {
		var n int64
		m.storeDB.Model(&IncidentObservation{}).
			Where("window_from_ts = ? AND window_to_ts = ?", int64(3000), int64(3300)).Count(&n)
		return n
	}
	before := countBefore()

	// 重放第一段的旧窗口。
	res, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
	if err != nil {
		t.Fatalf("重放: %v", err)
	}
	if res.Action != "evidence_only" {
		t.Errorf("重放应只补证据，得到 %q", res.Action)
	}
	if res.SegmentID != seg1 {
		t.Errorf("重放证据应归属原段 %s，实际写到 %s", seg1, res.SegmentID)
	}

	// 幂等：不得堆出第二条同窗口证据。
	if after := countBefore(); after != before {
		t.Errorf("重放产生了重复证据：%d → %d", before, after)
	}
	// 该窗口的证据不得出现在第二段上。
	var inSeg2 int64
	m.storeDB.Model(&IncidentObservation{}).
		Where("segment_id = ? AND window_from_ts = ?", seg2, int64(3000)).Count(&inSeg2)
	if inSeg2 != 0 {
		t.Errorf("旧窗口证据被错误写到第二段，%d 条", inSeg2)
	}
	// 生命周期不受影响（保留上轮修复）。
	var inc Incident
	m.storeDB.Where("id = ?", first.IncidentID).Take(&inc)
	if inc.Status != "open" {
		t.Errorf("重放改变了生命周期：status=%q", inc.Status)
	}
}

// 第二轮 review 问题 1：跨事故重放。
//
// 冷却到期后已新建新事故，此时重放老事故的旧窗口，
// 证据必须回到**老事故的老段**，不能挂到新事故。
func TestEngineReplayAcrossIncidentsKeepsOriginalIncident(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	old, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
	if err != nil {
		t.Fatalf("老事故: %v", err)
	}
	oldSeg := old.SegmentID
	if _, err := m.IngestIncidentObservation(ctx, normalAt(3300)); err != nil {
		t.Fatalf("结束: %v", err)
	}

	// 跨过冷却期 → 新建事故。
	var inc Incident
	m.storeDB.Where("id = ?", old.IncidentID).Take(&inc)
	farTs := inc.CooldownUntilTs + 600
	fresh, err := m.IngestIncidentObservation(ctx, anomalyAt(farTs))
	if err != nil {
		t.Fatalf("新事故: %v", err)
	}
	if fresh.IncidentID == old.IncidentID {
		t.Fatalf("测试前置：冷却到期应新建事故")
	}

	// 重放老事故的旧窗口。
	res, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
	if err != nil {
		t.Fatalf("重放: %v", err)
	}
	if res.IncidentID != old.IncidentID {
		t.Errorf("重放证据应归属原事故 %s，实际挂到 %s", old.IncidentID, res.IncidentID)
	}
	if res.SegmentID != oldSeg {
		t.Errorf("重放证据应归属原段 %s，实际 %s", oldSeg, res.SegmentID)
	}
	// 新事故上不得出现该窗口的证据。
	var onFresh int64
	m.storeDB.Model(&IncidentObservation{}).
		Where("incident_id = ? AND window_from_ts = ?", fresh.IncidentID, int64(3000)).Count(&onFresh)
	if onFresh != 0 {
		t.Errorf("旧窗口证据被错误挂到新事故，%d 条", onFresh)
	}
	// 新事故生命周期不受影响。
	var fi Incident
	m.storeDB.Where("id = ?", fresh.IncidentID).Take(&fi)
	if fi.Status != "open" {
		t.Errorf("重放影响了新事故：status=%q", fi.Status)
	}
	// 总事故数仍为 2，没有被重放催生出第三个。
	var n int64
	m.storeDB.Model(&Incident{}).Count(&n)
	if n != 2 {
		t.Errorf("事故数应为 2，实际 %d", n)
	}
}

// buildLateArrivalFixture 搭出 review 复现用的现场。
//
// 异常[3000,3300) 建第一段 → 跳过[3300,3600) → 异常[3600,3900)
// → 正常[3900,4200) 结束第一段 → 异常[4200,4500) 开第二段。
// 返回事故 ID、第一段、第二段。
func buildLateArrivalFixture(t *testing.T, m *Monitor) (string, string, string) {
	t.Helper()
	ctx := context.Background()

	r1, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
	if err != nil {
		t.Fatalf("段1窗1: %v", err)
	}
	// 跳过 [3300,3600)：这就是后面要迟到补报的窗口。
	if _, err := m.IngestIncidentObservation(ctx, anomalyAt(3600)); err != nil {
		t.Fatalf("段1窗2: %v", err)
	}
	if _, err := m.IngestIncidentObservation(ctx, normalAt(3900)); err != nil {
		t.Fatalf("结束段1: %v", err)
	}
	r2, err := m.IngestIncidentObservation(ctx, anomalyAt(4200))
	if err != nil {
		t.Fatalf("段2: %v", err)
	}
	if r2.Action != "segment_appended" {
		t.Fatalf("测试前置：应开第二段，实际 %q", r2.Action)
	}
	if r1.SegmentID == r2.SegmentID {
		t.Fatal("测试前置：两段应不同")
	}
	return r1.IncidentID, r1.SegmentID, r2.SegmentID
}

// assertLateWindowOnSegment 断言迟到窗口 [3300,3600) 落在期望段上。
func assertLateWindowOnSegment(t *testing.T, m *Monitor,
	res IncidentIngestResult, wantIncident, wantSeg, wrongSeg string) {
	t.Helper()

	if res.Action != "evidence_only" {
		t.Errorf("迟到窗口只应补证据，得到 action=%q", res.Action)
	}
	if res.IncidentID != wantIncident {
		t.Errorf("迟到证据应归属事故 %s，实际 %s", wantIncident, res.IncidentID)
	}
	if res.SegmentID != wantSeg {
		t.Errorf("迟到证据应归属第一段 %s，实际写到 %s", wantSeg, res.SegmentID)
	}
	// 落库校验：证据必须在期望段上，且不在错误段上。
	var onWant, onWrong int64
	m.storeDB.Model(&IncidentObservation{}).
		Where("segment_id = ? AND window_from_ts = ? AND window_to_ts = ?",
			wantSeg, int64(3300), int64(3600)).Count(&onWant)
	if onWant != 1 {
		t.Errorf("第一段上应有 1 条迟到证据，实际 %d", onWant)
	}
	if wrongSeg != "" {
		m.storeDB.Model(&IncidentObservation{}).
			Where("segment_id = ? AND window_from_ts = ?", wrongSeg, int64(3300)).Count(&onWrong)
		if onWrong != 0 {
			t.Errorf("迟到证据被错误写到段 %s，%d 条", wrongSeg, onWrong)
		}
	}
}

// 情况 1：第二段仍活跃时，首次补报 [3300,3600)。
func TestEngineLateWindowWhileSecondSegmentActive(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	incID, seg1, seg2 := buildLateArrivalFixture(t, m)

	var before Incident
	m.storeDB.Where("id = ?", incID).Take(&before)
	beforeSt, beforeCont := snapshotGovernance(t, m, incID)

	res, err := m.IngestIncidentObservation(context.Background(), anomalyAt(3300))
	if err != nil {
		t.Fatalf("迟到补报: %v", err)
	}
	assertLateWindowOnSegment(t, m, res, incID, seg1, seg2)

	// 生命周期、严重度、冷却、水位、治理状态都不得变化。
	assertIncidentUnchanged(t, m, incID, before)
	assertGovernanceUnchanged(t, m, incID, beforeSt, beforeCont)
}

// 情况 2：第二段已结束（事故 resolved、仍在冷却期内）时补报。
func TestEngineLateWindowAfterSecondSegmentResolved(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	incID, seg1, seg2 := buildLateArrivalFixture(t, m)

	// 结束第二段。
	if _, err := m.IngestIncidentObservation(ctx, normalAt(4500)); err != nil {
		t.Fatalf("结束段2: %v", err)
	}
	var before Incident
	m.storeDB.Where("id = ?", incID).Take(&before)
	if before.Status != "resolved" {
		t.Fatalf("测试前置：事故应已结束，实际 %q", before.Status)
	}
	beforeSt, beforeCont := snapshotGovernance(t, m, incID)

	res, err := m.IngestIncidentObservation(ctx, anomalyAt(3300))
	if err != nil {
		t.Fatalf("迟到补报: %v", err)
	}
	assertLateWindowOnSegment(t, m, res, incID, seg1, seg2)
	assertIncidentUnchanged(t, m, incID, before)
	assertGovernanceUnchanged(t, m, incID, beforeSt, beforeCont)
}

// 情况 3：冷却到期后新事故已活跃时补报老事故的迟到窗口。
func TestEngineLateWindowWithNewIncidentActive(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	incID, seg1, seg2 := buildLateArrivalFixture(t, m)

	if _, err := m.IngestIncidentObservation(ctx, normalAt(4500)); err != nil {
		t.Fatalf("结束段2: %v", err)
	}
	var old Incident
	m.storeDB.Where("id = ?", incID).Take(&old)
	oldSt, oldCont := snapshotGovernance(t, m, incID)
	// 跨过冷却期 → 新事故。
	fresh, err := m.IngestIncidentObservation(ctx, anomalyAt(old.CooldownUntilTs+600))
	if err != nil {
		t.Fatalf("新事故: %v", err)
	}
	if fresh.IncidentID == incID {
		t.Fatalf("测试前置：应新建事故")
	}
	var beforeFresh Incident
	m.storeDB.Where("id = ?", fresh.IncidentID).Take(&beforeFresh)

	res, err := m.IngestIncidentObservation(ctx, anomalyAt(3300))
	if err != nil {
		t.Fatalf("迟到补报: %v", err)
	}
	// 必须回到老事故的第一段，不能挂到新事故。
	assertLateWindowOnSegment(t, m, res, incID, seg1, seg2)
	var onFresh int64
	m.storeDB.Model(&IncidentObservation{}).
		Where("incident_id = ? AND window_from_ts = ?", fresh.IncidentID, int64(3300)).Count(&onFresh)
	if onFresh != 0 {
		t.Errorf("迟到证据被错误挂到新事故，%d 条", onFresh)
	}
	// 新事故与老事故都不受影响。
	assertIncidentUnchanged(t, m, incID, old)
	assertGovernanceUnchanged(t, m, incID, oldSt, oldCont)
	assertIncidentUnchanged(t, m, fresh.IncidentID, beforeFresh)
	var n int64
	m.storeDB.Model(&Incident{}).Count(&n)
	if n != 2 {
		t.Errorf("事故数应为 2，实际 %d", n)
	}
}

// 情况 4：冷却到期后新事故也已结束时补报。
func TestEngineLateWindowWithNewIncidentResolved(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	incID, seg1, seg2 := buildLateArrivalFixture(t, m)

	if _, err := m.IngestIncidentObservation(ctx, normalAt(4500)); err != nil {
		t.Fatalf("结束段2: %v", err)
	}
	var old Incident
	m.storeDB.Where("id = ?", incID).Take(&old)
	oldSt, oldCont := snapshotGovernance(t, m, incID)
	farTs := old.CooldownUntilTs + 600
	fresh, err := m.IngestIncidentObservation(ctx, anomalyAt(farTs))
	if err != nil {
		t.Fatalf("新事故: %v", err)
	}
	if _, err := m.IngestIncidentObservation(ctx, normalAt(farTs+300)); err != nil {
		t.Fatalf("结束新事故: %v", err)
	}
	var beforeFresh Incident
	m.storeDB.Where("id = ?", fresh.IncidentID).Take(&beforeFresh)
	if beforeFresh.Status != "resolved" {
		t.Fatalf("测试前置：新事故应已结束，实际 %q", beforeFresh.Status)
	}

	res, err := m.IngestIncidentObservation(ctx, anomalyAt(3300))
	if err != nil {
		t.Fatalf("迟到补报: %v", err)
	}
	assertLateWindowOnSegment(t, m, res, incID, seg1, seg2)
	var onFresh int64
	m.storeDB.Model(&IncidentObservation{}).
		Where("incident_id = ? AND window_from_ts = ?", fresh.IncidentID, int64(3300)).Count(&onFresh)
	if onFresh != 0 {
		t.Errorf("迟到证据被错误挂到新事故，%d 条", onFresh)
	}
	assertIncidentUnchanged(t, m, incID, old)
	assertGovernanceUnchanged(t, m, incID, oldSt, oldCont)
	assertIncidentUnchanged(t, m, fresh.IncidentID, beforeFresh)
}

// 迟到窗口补报两次必须幂等。
func TestEngineLateWindowIdempotent(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	incID, seg1, _ := buildLateArrivalFixture(t, m)

	first, err := m.IngestIncidentObservation(ctx, anomalyAt(3300))
	if err != nil {
		t.Fatalf("首次补报: %v", err)
	}
	second, err := m.IngestIncidentObservation(ctx, anomalyAt(3300))
	if err != nil {
		t.Fatalf("重复补报: %v", err)
	}
	if second.SegmentID != first.SegmentID || second.SegmentID != seg1 {
		t.Errorf("重复补报归属不稳定：%s / %s（应为 %s）",
			first.SegmentID, second.SegmentID, seg1)
	}
	var n int64
	m.storeDB.Model(&IncidentObservation{}).
		Where("incident_id = ? AND window_from_ts = ? AND window_to_ts = ?",
			incID, int64(3300), int64(3600)).Count(&n)
	if n != 1 {
		t.Errorf("迟到窗口证据应恰好 1 条，实际 %d", n)
	}
}

// snapshotGovernance 取治理状态与水位快照，用于前后比对。
func snapshotGovernance(t *testing.T, m *Monitor, incID string) (IncidentState, IncidentContinuity) {
	t.Helper()
	var st IncidentState
	m.storeDB.Where("incident_id = ?", incID).Take(&st)
	var cont IncidentContinuity
	m.storeDB.Where("incident_id = ?", incID).Take(&cont)
	return st, cont
}

// assertIncidentUnchanged 断言事故的生命周期、严重度、冷却、水位、
// 治理状态都未被只补证据的写入改动。
func assertIncidentUnchanged(t *testing.T, m *Monitor, incID string, before Incident) {
	t.Helper()
	var after Incident
	if err := m.storeDB.Where("id = ?", incID).Take(&after).Error; err != nil {
		t.Fatalf("读事故 %s: %v", incID, err)
	}
	if after.Status != before.Status {
		t.Errorf("生命周期被改动：%q → %q", before.Status, after.Status)
	}
	if after.Severity != before.Severity {
		t.Errorf("严重度被改动：%q → %q", before.Severity, after.Severity)
	}
	if after.PeakSeverity != before.PeakSeverity {
		t.Errorf("历史峰值被改动：%q → %q", before.PeakSeverity, after.PeakSeverity)
	}
	if after.CooldownUntilTs != before.CooldownUntilTs {
		t.Errorf("冷却时间被改动：%d → %d", before.CooldownUntilTs, after.CooldownUntilTs)
	}
	if after.ResolvedTs != before.ResolvedTs {
		t.Errorf("resolved_ts 被改动：%d → %d", before.ResolvedTs, after.ResolvedTs)
	}
	if after.ActiveMarker != before.ActiveMarker {
		t.Errorf("活跃标记被改动：%d → %d", before.ActiveMarker, after.ActiveMarker)
	}

	if after.LastSeenTs != before.LastSeenTs {
		t.Errorf("last_seen_ts 被改动：%d → %d", before.LastSeenTs, after.LastSeenTs)
	}
}

// assertGovernanceUnchanged 比对治理状态与连续水位的前后快照。
func assertGovernanceUnchanged(t *testing.T, m *Monitor, incID string,
	beforeSt IncidentState, beforeCont IncidentContinuity) {
	t.Helper()
	afterSt, afterCont := snapshotGovernance(t, m, incID)

	if afterSt.ObservedState != beforeSt.ObservedState {
		t.Errorf("观测层治理状态被改动：%q → %q", beforeSt.ObservedState, afterSt.ObservedState)
	}
	if afterSt.RecommendedState != beforeSt.RecommendedState {
		t.Errorf("建议层治理状态被改动：%q → %q", beforeSt.RecommendedState, afterSt.RecommendedState)
	}
	if afterSt.ObservedAt != beforeSt.ObservedAt || afterSt.RecommendedAt != beforeSt.RecommendedAt {
		t.Error("治理状态时间戳被改动")
	}
	// 水位不得推进。
	if afterCont.LastCountedWindowToTs != beforeCont.LastCountedWindowToTs {
		t.Errorf("已计数水位被推进：%d → %d",
			beforeCont.LastCountedWindowToTs, afterCont.LastCountedWindowToTs)
	}
	if afterCont.LastFinalizedWindowToTs != beforeCont.LastFinalizedWindowToTs {
		t.Errorf("已判定水位被推进：%d → %d",
			beforeCont.LastFinalizedWindowToTs, afterCont.LastFinalizedWindowToTs)
	}
	if afterCont.LowSampleWindows != beforeCont.LowSampleWindows {
		t.Errorf("低样本窗口计数被改动：%d → %d",
			beforeCont.LowSampleWindows, afterCont.LowSampleWindows)
	}
	if afterCont.GapWindows != beforeCont.GapWindows {
		t.Errorf("缺口窗口计数被改动：%d → %d",
			beforeCont.GapWindows, afterCont.GapWindows)
	}
}

// assertUnattributed 断言一次摄入因无法归属而被明确拒绝，
// 且没有留下任何错误归属的证据。
func assertUnattributed(t *testing.T, m *Monitor, err error, in IncidentObservationInput) {
	t.Helper()
	if err == nil {
		t.Fatalf("窗口 [%d,%d) 无法归属，应报错而不是静默写入",
			in.WindowFromTs, in.WindowToTs)
	}
	if !errors.Is(err, ErrObservationUnattributed) {
		t.Errorf("应返回 ErrObservationUnattributed，实际 %v", err)
	}
	// 错误信息要能排查：至少带上窗口边界。
	if !strings.Contains(err.Error(), fmt.Sprint(in.WindowFromTs)) {
		t.Errorf("错误信息缺少窗口起点，不便排查：%v", err)
	}
	// 关键：不得留下任何该窗口的证据。
	var n int64
	m.storeDB.Model(&IncidentObservation{}).
		Where("window_from_ts = ? AND window_to_ts = ?", in.WindowFromTs, in.WindowToTs).Count(&n)
	if n != 0 {
		t.Errorf("无法归属的窗口仍写入了 %d 条证据", n)
	}
}

// buildGapFixture 造出「第一段 [3000,3600)、第二段从 secondStart 开始」
// 的现场，中间留出无分段覆盖的空隙。返回事故 ID 与两段 ID。
func buildGapFixture(t *testing.T, m *Monitor, secondStart int64) (string, string, string) {
	t.Helper()
	ctx := context.Background()

	r1, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
	if err != nil {
		t.Fatalf("段1: %v", err)
	}
	if _, err := m.IngestIncidentObservation(ctx, normalAt(3300)); err != nil {
		t.Fatalf("结束段1: %v", err)
	}
	r2, err := m.IngestIncidentObservation(ctx, anomalyAt(secondStart))
	if err != nil {
		t.Fatalf("段2: %v", err)
	}
	return r1.IncidentID, r1.SegmentID, r2.SegmentID
}

// 场景 1：分段间空隙，当前事故活跃。
//
// review 原始反例：第一段 [3000,3600)，第二段从 4800 开始，
// 迟到窗口 [3900,4200) 位于两段之间，不得被写入第二段。
func TestEngineGapBetweenSegmentsIncidentActive(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	incID, seg1, seg2 := buildGapFixture(t, m, 4800)

	var before Incident
	m.storeDB.Where("id = ?", incID).Take(&before)
	beforeSt, beforeCont := snapshotGovernance(t, m, incID)

	in := anomalyAt(3900) // [3900,4200)，落在两段之间
	_, err := m.IngestIncidentObservation(context.Background(), in)
	assertUnattributed(t, m, err, in)

	// 两段上都不得出现该窗口。
	for _, seg := range []string{seg1, seg2} {
		var n int64
		m.storeDB.Model(&IncidentObservation{}).
			Where("segment_id = ? AND window_from_ts = ?", seg, int64(3900)).Count(&n)
		if n != 0 {
			t.Errorf("空隙窗口被写到段 %s", seg)
		}
	}
	assertIncidentUnchanged(t, m, incID, before)
	assertGovernanceUnchanged(t, m, incID, beforeSt, beforeCont)
}

// 场景 2：分段间空隙，当前事故已结束。
func TestEngineGapBetweenSegmentsIncidentResolved(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	incID, seg1, seg2 := buildGapFixture(t, m, 4800)

	if _, err := m.IngestIncidentObservation(ctx, normalAt(5100)); err != nil {
		t.Fatalf("结束段2: %v", err)
	}
	var before Incident
	m.storeDB.Where("id = ?", incID).Take(&before)
	if before.Status != "resolved" {
		t.Fatalf("测试前置：事故应已结束，实际 %q", before.Status)
	}
	beforeSt, beforeCont := snapshotGovernance(t, m, incID)

	in := anomalyAt(3900)
	_, err := m.IngestIncidentObservation(ctx, in)
	assertUnattributed(t, m, err, in)

	for _, seg := range []string{seg1, seg2} {
		var n int64
		m.storeDB.Model(&IncidentObservation{}).
			Where("segment_id = ? AND window_from_ts = ?", seg, int64(3900)).Count(&n)
		if n != 0 {
			t.Errorf("空隙窗口被写到段 %s", seg)
		}
	}
	assertIncidentUnchanged(t, m, incID, before)
	assertGovernanceUnchanged(t, m, incID, beforeSt, beforeCont)
}

// buildTwoIncidentFixture 造出两个事故，之间留出空隙。
// 新事故起点远在迟到窗口之后（review 反例里是 15000）。
func buildTwoIncidentFixture(t *testing.T, m *Monitor) (string, string) {
	t.Helper()
	ctx := context.Background()

	r1, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
	if err != nil {
		t.Fatalf("老事故: %v", err)
	}
	if _, err := m.IngestIncidentObservation(ctx, normalAt(3300)); err != nil {
		t.Fatalf("结束老事故: %v", err)
	}
	var old Incident
	m.storeDB.Where("id = ?", r1.IncidentID).Take(&old)
	farStart := old.CooldownUntilTs + 7200
	if farStart < 15000 {
		farStart = 15000
	}
	r2, err := m.IngestIncidentObservation(ctx, anomalyAt(farStart))
	if err != nil {
		t.Fatalf("新事故: %v", err)
	}
	if r2.IncidentID == r1.IncidentID {
		t.Fatalf("测试前置：应新建事故")
	}
	return r1.IncidentID, r2.IncidentID
}

// 场景 3：事故间空隙，新事故活跃。
//
// review 原始反例：迟到窗口被挂到从 15000 才开始的新事故。
func TestEngineGapBetweenIncidentsNewActive(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	oldID, freshID := buildTwoIncidentFixture(t, m)

	var oldBefore, freshBefore Incident
	m.storeDB.Where("id = ?", oldID).Take(&oldBefore)
	m.storeDB.Where("id = ?", freshID).Take(&freshBefore)
	oldSt, oldCont := snapshotGovernance(t, m, oldID)
	freshSt, freshCont := snapshotGovernance(t, m, freshID)

	in := anomalyAt(3900) // 老事故已结束之后、新事故开始之前
	_, err := m.IngestIncidentObservation(context.Background(), in)
	assertUnattributed(t, m, err, in)

	// 决不能挂到起点远在其后的新事故。
	var onFresh int64
	m.storeDB.Model(&IncidentObservation{}).
		Where("incident_id = ? AND window_from_ts = ?", freshID, int64(3900)).Count(&onFresh)
	if onFresh != 0 {
		t.Errorf("空隙窗口被挂到新事故，%d 条", onFresh)
	}
	assertIncidentUnchanged(t, m, oldID, oldBefore)
	assertGovernanceUnchanged(t, m, oldID, oldSt, oldCont)
	assertIncidentUnchanged(t, m, freshID, freshBefore)
	assertGovernanceUnchanged(t, m, freshID, freshSt, freshCont)
}

// 场景 4：事故间空隙，新事故已结束。
func TestEngineGapBetweenIncidentsNewResolved(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	oldID, freshID := buildTwoIncidentFixture(t, m)

	var fresh Incident
	m.storeDB.Where("id = ?", freshID).Take(&fresh)
	if _, err := m.IngestIncidentObservation(ctx, normalAt(fresh.LastSeenTs)); err != nil {
		t.Fatalf("结束新事故: %v", err)
	}
	var oldBefore, freshBefore Incident
	m.storeDB.Where("id = ?", oldID).Take(&oldBefore)
	m.storeDB.Where("id = ?", freshID).Take(&freshBefore)
	if freshBefore.Status != "resolved" {
		t.Fatalf("测试前置：新事故应已结束，实际 %q", freshBefore.Status)
	}
	oldSt, oldCont := snapshotGovernance(t, m, oldID)
	freshSt, freshCont := snapshotGovernance(t, m, freshID)

	in := anomalyAt(3900)
	_, err := m.IngestIncidentObservation(ctx, in)
	assertUnattributed(t, m, err, in)

	var onFresh int64
	m.storeDB.Model(&IncidentObservation{}).
		Where("incident_id = ? AND window_from_ts = ?", freshID, int64(3900)).Count(&onFresh)
	if onFresh != 0 {
		t.Errorf("空隙窗口被挂到新事故，%d 条", onFresh)
	}
	assertIncidentUnchanged(t, m, oldID, oldBefore)
	assertGovernanceUnchanged(t, m, oldID, oldSt, oldCont)
	assertIncidentUnchanged(t, m, freshID, freshBefore)
	assertGovernanceUnchanged(t, m, freshID, freshSt, freshCont)
}

// 场景 5：窗口早于首段，当前事故活跃。
func TestEngineWindowBeforeFirstSegmentIncidentActive(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	first, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
	if err != nil {
		t.Fatalf("建段: %v", err)
	}
	var before Incident
	m.storeDB.Where("id = ?", first.IncidentID).Take(&before)
	beforeSt, beforeCont := snapshotGovernance(t, m, first.IncidentID)

	in := anomalyAt(1200) // 早于首段起点 3000
	_, err = m.IngestIncidentObservation(ctx, in)
	assertUnattributed(t, m, err, in)

	var n int64
	m.storeDB.Model(&IncidentObservation{}).
		Where("segment_id = ? AND window_from_ts = ?", first.SegmentID, int64(1200)).Count(&n)
	if n != 0 {
		t.Error("早于首段的窗口被写入首段")
	}
	assertIncidentUnchanged(t, m, first.IncidentID, before)
	assertGovernanceUnchanged(t, m, first.IncidentID, beforeSt, beforeCont)
}

// 场景 6：窗口早于首段，当前事故已结束。
func TestEngineWindowBeforeFirstSegmentIncidentResolved(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	first, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
	if err != nil {
		t.Fatalf("建段: %v", err)
	}
	if _, err := m.IngestIncidentObservation(ctx, normalAt(3300)); err != nil {
		t.Fatalf("结束: %v", err)
	}
	var before Incident
	m.storeDB.Where("id = ?", first.IncidentID).Take(&before)
	if before.Status != "resolved" {
		t.Fatalf("测试前置：事故应已结束，实际 %q", before.Status)
	}
	beforeSt, beforeCont := snapshotGovernance(t, m, first.IncidentID)

	in := anomalyAt(1200)
	_, err = m.IngestIncidentObservation(ctx, in)
	assertUnattributed(t, m, err, in)

	assertIncidentUnchanged(t, m, first.IncidentID, before)
	assertGovernanceUnchanged(t, m, first.IncidentID, beforeSt, beforeCont)
}

// 第二轮 review 问题 2：观测记录的 low_sample 必须与段一致地正确填写。
func TestEngineObservationLowSampleWritten(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	res, err := m.IngestIncidentObservation(ctx, lowSampleAt(3000))
	if err != nil {
		t.Fatalf("低样本窗: %v", err)
	}
	var ob IncidentObservation
	if err := m.storeDB.Where("segment_id = ? AND window_to_ts = ?",
		res.SegmentID, int64(3300)).Take(&ob).Error; err != nil {
		t.Fatalf("读观测: %v", err)
	}
	if ob.SampleTotal == nil || *ob.SampleTotal != 1 {
		t.Fatalf("测试前置：样本应为 1，实际 %v", ob.SampleTotal)
	}
	if !ob.LowSample {
		t.Error("样本 1 低于门限，观测记录 low_sample 仍为 false")
	}

	// 样本充足的窗口不得误标。
	ok, err := m.IngestIncidentObservation(ctx, anomalyAt(3300))
	if err != nil {
		t.Fatalf("正常样本窗: %v", err)
	}
	var ob2 IncidentObservation
	m.storeDB.Where("segment_id = ? AND window_to_ts = ?", ok.SegmentID, int64(3600)).Take(&ob2)
	if ob2.LowSample {
		t.Error("样本充足却被标成 low_sample")
	}
}

// 未知样本与真实零值必须区分开。
func TestEngineObservationLowSampleUnknownVsZero(t *testing.T) {
	// 未知：不判低样本，SampleTotal 保持 nil。
	if observationLowSample(nil, 20) {
		t.Error("样本未知不应判为 low_sample")
	}
	// 真实零值：确实没有样本，判低样本。
	zero := int64(0)
	if !observationLowSample(&zero, 20) {
		t.Error("真实零样本应判为 low_sample")
	}
	// 边界：等于门限不算低。
	at := int64(20)
	if observationLowSample(&at, 20) {
		t.Error("样本等于门限不应判为 low_sample")
	}
	below := int64(19)
	if !observationLowSample(&below, 20) {
		t.Error("样本低于门限应判为 low_sample")
	}
}

// 采集缺口窗口：样本未知，落库后 sample_total 仍须为 nil 且不标低样本。
func TestEngineGapObservationKeepsUnknownSample(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	first, err := m.IngestIncidentObservation(ctx, anomalyAt(3000))
	if err != nil {
		t.Fatalf("窗1: %v", err)
	}
	if _, err := m.IngestIncidentObservation(ctx, gapAt(3300)); err != nil {
		t.Fatalf("缺口窗: %v", err)
	}
	var ob IncidentObservation
	if err := m.storeDB.Where("incident_id = ? AND window_to_ts = ?",
		first.IncidentID, int64(3600)).Take(&ob).Error; err != nil {
		t.Fatalf("读缺口观测: %v", err)
	}
	if ob.SampleTotal != nil {
		t.Errorf("缺口窗口样本应为未知(nil)，实际 %d", *ob.SampleTotal)
	}
	if ob.LowSample {
		t.Error("样本未知的缺口窗口不应被标成 low_sample")
	}
	if ob.DataPresent {
		t.Error("缺口窗口 data_present 应为 false")
	}
}

// review 问题 3：事故升级后前两层必须同步更新。
func TestEngineRefreshesStateOnEscalation(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	// 首窗：单窗 + exact，严重度较低。
	first, err := m.IngestIncidentObservation(ctx, anomalyWithEvidence(3000, evidenceLevelExact))
	if err != nil {
		t.Fatalf("窗1: %v", err)
	}
	var before IncidentState
	if err := m.storeDB.Where("incident_id = ?", first.IncidentID).Take(&before).Error; err != nil {
		t.Fatalf("读初始状态: %v", err)
	}

	// 连送到升级。
	var lastSev string
	for i := 1; i <= 3; i++ {
		r, err := m.IngestIncidentObservation(ctx, anomalyWithEvidence(3000+int64(i)*300, evidenceLevelExact))
		if err != nil {
			t.Fatalf("窗%d: %v", i+1, err)
		}
		lastSev = r.Severity
	}

	var after IncidentState
	if err := m.storeDB.Where("incident_id = ?", first.IncidentID).Take(&after).Error; err != nil {
		t.Fatalf("读升级后状态: %v", err)
	}
	// 严重度确实变了的前提下，前两层必须跟着变。
	if lastSev != first.Severity {
		if after.ObservedState == before.ObservedState &&
			after.RecommendedState == before.RecommendedState {
			t.Errorf("严重度 %s→%s，但 observed/recommended 仍停在 %q/%q",
				first.Severity, lastSev, after.ObservedState, after.RecommendedState)
		}
	}
	if after.ObservedAt <= before.ObservedAt {
		t.Errorf("observed_at 未更新：%d → %d", before.ObservedAt, after.ObservedAt)
	}
	if after.RecommendedAt <= before.RecommendedAt {
		t.Errorf("recommended_at 未更新：%d → %d", before.RecommendedAt, after.RecommendedAt)
	}
	if !validGovernanceState(after.ObservedState) || !validGovernanceState(after.RecommendedState) {
		t.Errorf("更新后写入非法枚举: %q/%q", after.ObservedState, after.RecommendedState)
	}
	// 审批层与实际配置层不受影响。
	if after.ApprovedState != "" || after.ApprovedBy != "" {
		t.Error("更新前两层时污染了审批层")
	}
	if after.AppliedNewapiEnabled != nil || after.AppliedNewapiWeight != nil {
		t.Error("更新前两层时污染了实际配置层")
	}
}

// review 问题 4：低样本标记必须在重复扫描与采集缺口后保持，不得误升级。
func TestEngineLowSampleSurvivesDuplicateAndGap(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	// 三个正常样本窗口推到较高严重度。
	var res IncidentIngestResult
	var err error
	for i := 0; i < 3; i++ {
		res, err = m.IngestIncidentObservation(ctx, anomalyAt(3000+int64(i)*300))
		if err != nil {
			t.Fatalf("窗%d: %v", i, err)
		}
	}
	sevHigh := res.Severity

	// 低样本窗口 → 严重度应降级并打上低样本标记。
	lowRes, err := m.IngestIncidentObservation(ctx, lowSampleAt(3900))
	if err != nil {
		t.Fatalf("低样本窗: %v", err)
	}
	var segLow IncidentSegment
	if err := m.storeDB.Where("id = ?", lowRes.SegmentID).Take(&segLow).Error; err != nil {
		t.Fatalf("读段: %v", err)
	}
	if !segLow.LowSample {
		t.Fatal("低样本窗口未打上 low_sample 标记")
	}
	sevLow := lowRes.Severity

	// 重复提交同一个低样本窗口：不得清掉标记、不得重新升级。
	dup, err := m.IngestIncidentObservation(ctx, lowSampleAt(3900))
	if err != nil {
		t.Fatalf("重复低样本窗: %v", err)
	}
	if dup.Action != "evidence_only" {
		t.Errorf("重复窗口只应补证据，得到 %q", dup.Action)
	}
	var segDup IncidentSegment
	m.storeDB.Where("id = ?", lowRes.SegmentID).Take(&segDup)
	if !segDup.LowSample {
		t.Error("重复扫描清掉了 low_sample 标记")
	}
	if segDup.Severity != sevLow && segDup.Severity == sevHigh {
		t.Errorf("重复提交低样本窗口绕过样本门槛重新升级到 %s", segDup.Severity)
	}

	// 随后来一个采集缺口：计数冻结，同样不得清标记或升级。
	if _, err := m.IngestIncidentObservation(ctx, gapAt(4200)); err != nil {
		t.Fatalf("缺口窗: %v", err)
	}
	var segGap IncidentSegment
	m.storeDB.Where("id = ?", lowRes.SegmentID).Take(&segGap)
	if !segGap.LowSample {
		t.Error("采集缺口清掉了 low_sample 标记")
	}
	if segGap.Severity == sevHigh && sevHigh != sevLow {
		t.Errorf("采集缺口后无新证据却升回 %s", segGap.Severity)
	}
}

// review 问题 5：冷却期复发时历史峰值必须跟着抬高。
func TestEnginePeakSeverityUpdatedOnRecurrence(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	// 首次用 isolated_or_drift 场景：该场景封顶 SEV3，峰值不会先被推高。
	weak := anomalyWithEvidence(3000, evidenceLevelInferred)
	weak.Scenario = string(ScenarioIsolatedOrDrift)
	weak.HardSignal = false
	weak.SourceCount = 1
	first, err := m.IngestIncidentObservation(ctx, weak)
	if err != nil {
		t.Fatalf("首次: %v", err)
	}
	if _, err := m.IngestIncidentObservation(ctx, normalAt(3300)); err != nil {
		t.Fatalf("结束: %v", err)
	}
	var low Incident
	m.storeDB.Where("id = ?", first.IncidentID).Take(&low)
	lowPeak := low.PeakSeverity
	if lowPeak != "SEV3" {
		t.Fatalf("测试前置：首个事故峰值应为 SEV3，实际 %q", lowPeak)
	}

	// 冷却期内以 SEV1 级证据复发。
	//
	// 关键：必须在**复发那一窗**（segment_appended 路径）就断言峰值。
	// 后续窗口走 updateActiveIncident，那条路径本来就会补峰值，
	// 若等到第三窗再查，复发路径漏更新峰值的缺陷会被掩盖。
	rec, err := m.IngestIncidentObservation(ctx, anomalyWithEvidence(3600, evidenceLevelExact))
	if err != nil {
		t.Fatalf("复发窗: %v", err)
	}
	if rec.IncidentID != first.IncidentID {
		t.Fatalf("应复用原事故，得到 %s", rec.IncidentID)
	}
	if rec.Action != "segment_appended" {
		t.Fatalf("测试前置：应走复发追加段路径，实际 %q", rec.Action)
	}

	var after Incident
	m.storeDB.Where("id = ?", first.IncidentID).Take(&after)
	// 硬断言：原峰值 SEV3，复发到 SEV1，峰值必须被抬到 SEV1。
	if after.Severity != "SEV1" {
		t.Fatalf("测试前置：复发应达 SEV1，实际 %q", after.Severity)
	}
	if after.PeakSeverity != "SEV1" {
		t.Errorf("复发升级到 SEV1，历史峰值却仍是 %q（原 %s）", after.PeakSeverity, lowPeak)
	}
	// 反向边界：峰值不得抬高当前段判定。
	var seg IncidentSegment
	m.storeDB.Where("id = ?", rec.SegmentID).Take(&seg)
	if incidentSeverityRank(seg.Severity) > incidentSeverityRank(after.Severity) {
		t.Errorf("历史峰值反过来抬高了当前段判定：段 %s > 事故 %s",
			seg.Severity, after.Severity)
	}
}

// 峰值只升不降：事故恢复后历史峰值保留。
func TestEnginePeakSeverityNeverDowngrades(t *testing.T) {
	enableIncidentForTest(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	var res IncidentIngestResult
	var err error
	for i := 0; i < 3; i++ {
		res, err = m.IngestIncidentObservation(ctx, anomalyWithEvidence(3000+int64(i)*300, evidenceLevelExact))
		if err != nil {
			t.Fatalf("窗%d: %v", i, err)
		}
	}
	var peaked Incident
	m.storeDB.Where("id = ?", res.IncidentID).Take(&peaked)
	high := peaked.PeakSeverity

	// 低样本窗口降低当前严重度。
	if _, err := m.IngestIncidentObservation(ctx, lowSampleAt(3900)); err != nil {
		t.Fatalf("低样本: %v", err)
	}
	var after Incident
	m.storeDB.Where("id = ?", res.IncidentID).Take(&after)
	if after.PeakSeverity != high {
		t.Errorf("历史峰值被降级：%s → %s", high, after.PeakSeverity)
	}
}

// history marker 必须对同一事故 ID 恒定，且永不等于活跃 marker。
func TestIncidentHistoryMarkerStableAndNeverActive(t *testing.T) {
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("incident-%d", i)
		a, b := incidentHistoryMarker(id), incidentHistoryMarker(id)
		if a != b {
			t.Fatalf("marker 对同一 ID 不稳定: %d vs %d", a, b)
		}
		if a == incidentActiveMarker {
			t.Fatalf("历史 marker 不得等于活跃 marker: %d", a)
		}
		if a <= 0 {
			t.Fatalf("marker 应为正数，得到 %d", a)
		}
	}
}
