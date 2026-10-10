package monitor

import "testing"

// 构造一个已 Final 的异常窗口观测。
func anomalyWindow(from, to int64, sample int64) IncidentWindowObservation {
	return IncidentWindowObservation{
		Window:       IncidentWindow{FromTs: from, ToTs: to},
		Scope:        IncidentSampleScopeKeyedRequests,
		SampleTotal:  sample,
		AnomalyCount: sample,
		DataPresent:  true,
		Anomalous:    true,
		SourceCount:  1,
		Final:        true,
	}
}

func normalWindow(from, to int64, sample int64) IncidentWindowObservation {
	o := anomalyWindow(from, to, sample)
	o.Anomalous = false
	o.AnomalyCount = 0
	return o
}

func gapWindow(from, to int64) IncidentWindowObservation {
	return IncidentWindowObservation{
		Window:      IncidentWindow{FromTs: from, ToTs: to},
		Scope:       IncidentSampleScopeKeyedRequests,
		DataPresent: false,
		Final:       true,
	}
}

// 窗口固定对齐到格栅，而不是从首次异常起算。
func TestAlignIncidentWindowFixedGrid(t *testing.T) {
	w := AlignIncidentWindow(1760000123, 300)
	if w.FromTs%300 != 0 {
		t.Fatalf("窗口左界未对齐: %d", w.FromTs)
	}
	if w.ToTs-w.FromTs != 300 {
		t.Fatalf("窗口长度错误: %d", w.ToTs-w.FromTs)
	}
	// 同一窗口内的不同时刻必须对齐到同一窗口。
	a := AlignIncidentWindow(w.FromTs+1, 300)
	b := AlignIncidentWindow(w.ToTs-1, 300)
	if a != b || a != w {
		t.Fatalf("同窗口内时刻对齐不一致: %+v %+v %+v", a, b, w)
	}
}

// 连续相邻的异常窗口正常累计。
func TestIncidentContinuityCountsConsecutiveWindows(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	for i := int64(0); i < 3; i++ {
		from := 1000 + i*300
		out := AdvanceIncidentContinuity(st, anomalyWindow(from, from+300, 100), 20)
		if i == 0 && out != OutcomeCounted {
			t.Fatalf("首窗口 outcome=%q", out)
		}
		if i > 0 && out != OutcomeCounted {
			t.Fatalf("第 %d 个窗口 outcome=%q want counted", i+1, out)
		}
	}
	if st.ConsecutiveWindows != 3 {
		t.Fatalf("连续窗口数 =%d want 3", st.ConsecutiveWindows)
	}
}

// 同一窗口重复扫描三次只能算一个窗口（幂等）。
func TestIncidentSameWindowRescannedThreeTimesCountsOnce(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	obs := anomalyWindow(1000, 1300, 100)

	first := AdvanceIncidentContinuity(st, obs, 20)
	if first != OutcomeCounted {
		t.Fatalf("首次 outcome=%q", first)
	}
	for i := 0; i < 2; i++ {
		out := AdvanceIncidentContinuity(st, obs, 20)
		if out != OutcomeDuplicateIgnored {
			t.Fatalf("重复扫描第 %d 次 outcome=%q want duplicate_ignored", i+1, out)
		}
	}
	if st.ConsecutiveWindows != 1 {
		t.Fatalf("重复扫描被累计了: 连续窗口数=%d want 1", st.ConsecutiveWindows)
	}
	// 关键：不能因为重复扫描三次就达到 SEV2 的三窗口门槛。
	if got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioDegradedWithFallback, Continuity: *st,
	}); got == "SEV2" {
		t.Fatal("同窗口扫三次被当成三个连续窗口，升到了 SEV2")
	}
}

// 乱序到达的旧窗口不重复累计。
func TestIncidentOutOfOrderStaleWindowIgnored(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	AdvanceIncidentContinuity(st, anomalyWindow(1300, 1600, 100), 20)
	AdvanceIncidentContinuity(st, anomalyWindow(1600, 1900, 100), 20)
	before := st.ConsecutiveWindows

	out := AdvanceIncidentContinuity(st, anomalyWindow(1000, 1300, 100), 20)
	if out != OutcomeStaleIgnored {
		t.Fatalf("乱序旧窗口 outcome=%q want stale_ignored", out)
	}
	if st.ConsecutiveWindows != before {
		t.Fatalf("乱序窗口改变了计数: %d → %d", before, st.ConsecutiveWindows)
	}
}

// 连续性要求严格相邻，跳窗不接续。
func TestIncidentConsecutiveRequiresAdjacentWindows(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	AdvanceIncidentContinuity(st, anomalyWindow(1000, 1300, 100), 20)
	// 跳过 [1300,1600)，直接给 [1600,1900)。
	out := AdvanceIncidentContinuity(st, anomalyWindow(1600, 1900, 100), 20)
	if out != OutcomeRestartedAfterGap {
		t.Fatalf("跳窗 outcome=%q want restarted_after_gap", out)
	}
	if st.ConsecutiveWindows != 1 {
		t.Fatalf("跳窗后连续数 =%d want 1（重新起算）", st.ConsecutiveWindows)
	}
}

// 缺采集窗口不算异常达标。
func TestIncidentGapWindowNotCountedAsAnomaly(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	AdvanceIncidentContinuity(st, anomalyWindow(1000, 1300, 100), 20)
	before := st.ConsecutiveWindows

	out := AdvanceIncidentContinuity(st, gapWindow(1300, 1600), 20)
	if out != OutcomeGapFrozen {
		t.Fatalf("缺口窗口 outcome=%q want gap_frozen", out)
	}
	if st.ConsecutiveWindows != before {
		t.Fatalf("缺口被算作异常达标: %d → %d", before, st.ConsecutiveWindows)
	}
	if st.GapWindows != 1 {
		t.Fatalf("缺口窗口未计数: %d", st.GapWindows)
	}
}

// 缺采集窗口也不算恢复（不清零）。
func TestIncidentGapWindowNotCountedAsRecovery(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	AdvanceIncidentContinuity(st, anomalyWindow(1000, 1300, 100), 20)
	AdvanceIncidentContinuity(st, anomalyWindow(1300, 1600, 100), 20)
	if st.ConsecutiveWindows != 2 {
		t.Fatalf("前置条件失败: 连续数=%d", st.ConsecutiveWindows)
	}

	AdvanceIncidentContinuity(st, gapWindow(1600, 1900), 20)
	if st.ConsecutiveWindows == 0 {
		t.Fatal("缺口被当成恢复，连续计数被清零")
	}
	if st.ConsecutiveWindows != 2 {
		t.Fatalf("缺口期间计数应冻结在 2，实际 %d", st.ConsecutiveWindows)
	}
}

// 缺口打断相邻性：缺口后的窗口重新起算，不接续缺口前的计数。
func TestIncidentGapBreaksAdjacencyRestartsCount(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	AdvanceIncidentContinuity(st, anomalyWindow(1000, 1300, 100), 20)
	AdvanceIncidentContinuity(st, anomalyWindow(1300, 1600, 100), 20)
	AdvanceIncidentContinuity(st, gapWindow(1600, 1900), 20)

	out := AdvanceIncidentContinuity(st, anomalyWindow(1900, 2200, 100), 20)
	if out != OutcomeRestartedAfterGap {
		t.Fatalf("缺口后窗口 outcome=%q want restarted_after_gap", out)
	}
	if st.ConsecutiveWindows != 1 {
		t.Fatalf("缺口后应重新起算为 1，实际 %d", st.ConsecutiveWindows)
	}
}

// 正常窗口打断异常连续计数。
func TestIncidentNormalWindowBreaksConsecutiveCount(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	AdvanceIncidentContinuity(st, anomalyWindow(1000, 1300, 100), 20)
	AdvanceIncidentContinuity(st, anomalyWindow(1300, 1600, 100), 20)

	out := AdvanceIncidentContinuity(st, normalWindow(1600, 1900, 100), 20)
	if out != OutcomeNormalReset {
		t.Fatalf("正常窗口 outcome=%q want normal_reset", out)
	}
	if st.ConsecutiveWindows != 0 {
		t.Fatalf("正常窗口未打断连续计数: %d", st.ConsecutiveWindows)
	}
}

// 样本不足不算异常达标，但必须保留展示、不得当作正常。
func TestIncidentLowSampleHeldNotNormal(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	out := AdvanceIncidentContinuity(st, anomalyWindow(1000, 1300, 5), 20)
	if out != OutcomeLowSampleHeld {
		t.Fatalf("低样本 outcome=%q want low_sample_held", out)
	}
	if out == OutcomeNormalReset {
		t.Fatal("低样本被当成了正常窗口")
	}
	if st.ConsecutiveWindows != 0 {
		t.Fatalf("低样本不应计入连续达标: %d", st.ConsecutiveWindows)
	}
	if !st.LowSample {
		t.Fatal("低样本标记未置位，页面无法展示")
	}
	if st.LowSampleWindows != 1 {
		t.Fatalf("低样本窗口未计数: %d", st.LowSampleWindows)
	}
}

// 样本门不得拦住硬信号（02.1 §8：硬信号可立即触发）。
func TestIncidentHardSignalBypassesMinSample(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	obs := anomalyWindow(1000, 1300, 1) // 样本 1，远低于门槛 20
	obs.HardSignal = true

	out := AdvanceIncidentContinuity(st, obs, 20)
	if out == OutcomeLowSampleHeld {
		t.Fatal("硬信号被最小样本门拦下了")
	}
	if out != OutcomeCounted {
		t.Fatalf("硬信号 outcome=%q want counted", out)
	}
	if got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioPrimaryNoFallback, Continuity: *st,
	}); got != "SEV1" {
		t.Fatalf("硬信号严重度=%q want SEV1", got)
	}
}

// 未收齐的窗口不得提前当成最终结论。
func TestIncidentNonFinalWindowProducesNoVerdict(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	obs := anomalyWindow(1000, 1300, 100)
	obs.Final = false

	out := AdvanceIncidentContinuity(st, obs, 20)
	if out != OutcomePendingNotFinal {
		t.Fatalf("未收齐窗口 outcome=%q want pending_not_final", out)
	}
	if st.ConsecutiveWindows != 0 || st.LastCountedWindowToTs != 0 {
		t.Fatalf("未收齐窗口推进了状态: consec=%d lastTo=%d",
			st.ConsecutiveWindows, st.LastCountedWindowToTs)
	}
	// 收齐后再送入才判定。
	obs.Final = true
	if out2 := AdvanceIncidentContinuity(st, obs, 20); out2 != OutcomeCounted {
		t.Fatalf("收齐后 outcome=%q want counted", out2)
	}
	if st.ConsecutiveWindows != 1 {
		t.Fatalf("收齐后连续数=%d want 1", st.ConsecutiveWindows)
	}
}

// 判定之后到达的迟到数据不改写已判定窗口（保持连续计数单调）。
func TestIncidentLateDataDoesNotRewriteFinalizedWindow(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	AdvanceIncidentContinuity(st, anomalyWindow(1000, 1300, 100), 20)
	snapshot := *st

	// 同窗口迟到数据（样本更多）再次送入。
	late := anomalyWindow(1000, 1300, 500)
	out := AdvanceIncidentContinuity(st, late, 20)
	if out != OutcomeDuplicateIgnored {
		t.Fatalf("迟到数据 outcome=%q want duplicate_ignored", out)
	}
	if st.ConsecutiveWindows != snapshot.ConsecutiveWindows {
		t.Fatalf("迟到数据改写了连续计数: %d → %d",
			snapshot.ConsecutiveWindows, st.ConsecutiveWindows)
	}
}

// SEV1 场景（主渠道/主模型无备用）：连续两窗口达标。
func TestIncidentSev1RequiresTwoConsecutiveWindows(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	AdvanceIncidentContinuity(st, anomalyWindow(1000, 1300, 100), 20)
	got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioPrimaryNoFallback, Continuity: *st,
	})
	if got == "SEV1" {
		t.Fatalf("单窗口不应达到 SEV1，实际 %q", got)
	}
	AdvanceIncidentContinuity(st, anomalyWindow(1300, 1600, 100), 20)
	got = DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioPrimaryNoFallback, Continuity: *st,
	})
	if got != "SEV1" {
		t.Fatalf("连续两窗口严重度=%q want SEV1", got)
	}
}

// SEV1 的另一条路径：一源 + 其他来源佐证。
func TestIncidentSev1ViaMultiSourceConfirmation(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	obs := anomalyWindow(1000, 1300, 100)
	obs.SourceCount = 2
	AdvanceIncidentContinuity(st, obs, 20)
	got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioPrimaryNoFallback, Continuity: *st,
	})
	if got != "SEV1" {
		t.Fatalf("多源佐证严重度=%q want SEV1（单窗口）", got)
	}
}

// SEV2 正向测试：单渠道退化但有备用，满足最小样本且持续三个窗口。
// 这是本轮 review 指出的缺陷——原实现 SEV2 分支永远不可达。
func TestIncidentSev2ReachableWithThreeWindowsAndSample(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	for i := int64(0); i < 3; i++ {
		from := 1000 + i*300
		AdvanceIncidentContinuity(st, anomalyWindow(from, from+300, 100), 20)
	}
	if st.ConsecutiveWindows != 3 {
		t.Fatalf("前置条件失败: 连续窗口数=%d", st.ConsecutiveWindows)
	}
	if st.LowSample {
		t.Fatal("前置条件失败: 样本应充足")
	}
	got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioDegradedWithFallback, Continuity: *st,
	})
	if got != "SEV2" {
		t.Fatalf("严重度=%q want SEV2（该分支必须可达）", got)
	}
}

// SEV2 场景不足三窗口时降为 SEV3，不得升级。
func TestIncidentSev2SceneBelowThreeWindowsIsSev3(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	AdvanceIncidentContinuity(st, anomalyWindow(1000, 1300, 100), 20)
	AdvanceIncidentContinuity(st, anomalyWindow(1300, 1600, 100), 20)
	got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioDegradedWithFallback, Continuity: *st,
	})
	if got == "SEV1" {
		t.Fatal("有备用的单渠道退化因窗口数被升级为 SEV1")
	}
	if got != "SEV3" {
		t.Fatalf("严重度=%q want SEV3", got)
	}
}

// 第三个窗口不得让严重度反而降低（review 指出的反向风险）。
func TestIncidentThirdWindowNeverDowngrades(t *testing.T) {
	order := map[string]int{"": 0, "SEV3": 1, "SEV2": 2, "SEV1": 3, "SEV0": 4}
	for _, sc := range []IncidentScenario{
		ScenarioPrimaryNoFallback, ScenarioDegradedWithFallback, ScenarioIsolatedOrDrift,
	} {
		st := &IncidentContinuityState{DedupKey: "k"}
		prev := 0
		for i := int64(0); i < 5; i++ {
			from := 1000 + i*300
			AdvanceIncidentContinuity(st, anomalyWindow(from, from+300, 100), 20)
			got := DecideIncidentSeverity(IncidentSeverityInput{Scenario: sc, Continuity: *st})
			if order[got] < prev {
				t.Errorf("场景 %q 第 %d 个窗口严重度从 %d 降到 %q", sc, i+1, prev, got)
			}
			prev = order[got]
		}
	}
}

// SEV2 场景低样本时不得产出 SEV2。
func TestIncidentSev2SceneLowSampleNotSev2(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	// 三个低样本异常窗口。
	for i := int64(0); i < 3; i++ {
		from := 1000 + i*300
		AdvanceIncidentContinuity(st, anomalyWindow(from, from+300, 3), 20)
	}
	if !st.LowSample {
		t.Fatal("前置条件失败: 应标记低样本")
	}
	got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioDegradedWithFallback, Continuity: *st,
	})
	if got == "SEV2" {
		t.Fatal("低样本产出了 SEV2")
	}
	// 低样本异常仍要展示为事故，不能当作正常。
	if got != "SEV3" {
		t.Fatalf("严重度=%q want SEV3（低样本仍需展示）", got)
	}
}

// SEV0 场景：硬信号立即触发；无硬信号且无双源时不擅自定 SEV0。
func TestIncidentSev0RequiresHardSignalOrDualSource(t *testing.T) {
	// 硬信号。
	st := &IncidentContinuityState{DedupKey: "k"}
	hard := anomalyWindow(1000, 1300, 1)
	hard.HardSignal = true
	AdvanceIncidentContinuity(st, hard, 20)
	if got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioSiteWideOrSecurity, Continuity: *st,
	}); got != "SEV0" {
		t.Fatalf("硬信号严重度=%q want SEV0", got)
	}

	// 无硬信号、无双源。
	st2 := &IncidentContinuityState{DedupKey: "k2"}
	AdvanceIncidentContinuity(st2, anomalyWindow(1000, 1300, 100), 20)
	if got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioSiteWideOrSecurity, Continuity: *st2,
	}); got == "SEV0" {
		t.Fatal("无硬信号无双源却定为 SEV0")
	}

	// 双源确认。
	if got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioSiteWideOrSecurity, Continuity: *st2, DualSourceConfirmed: true,
	}); got != "SEV0" {
		t.Fatalf("双源确认严重度=%q want SEV0", got)
	}
}

// 场景决定候选严重度：同一窗口状态在不同场景下得到不同严重度。
func TestIncidentSeverityDependsOnScenarioNotOnlyWindows(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	for i := int64(0); i < 3; i++ {
		from := 1000 + i*300
		AdvanceIncidentContinuity(st, anomalyWindow(from, from+300, 100), 20)
	}
	primary := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioPrimaryNoFallback, Continuity: *st,
	})
	fallback := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioDegradedWithFallback, Continuity: *st,
	})
	if primary == fallback {
		t.Fatalf("同一窗口状态在两个场景下严重度相同（%q），场景未参与判定", primary)
	}
	if primary != "SEV1" || fallback != "SEV2" {
		t.Fatalf("primary=%q fallback=%q want SEV1/SEV2", primary, fallback)
	}
}

// 未知场景不得产出任何严重度。
func TestIncidentUnknownScenarioYieldsNoSeverity(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k", ConsecutiveWindows: 5}
	if got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: IncidentScenario("whatever"), Continuity: *st,
	}); got != "" {
		t.Fatalf("未知场景严重度=%q want 空", got)
	}
}

// 重启后计数不丢：状态全部在可持久化的结构体字段里，
// 用「复制状态再继续推进」模拟重启后从库里读回。
func TestIncidentContinuitySurvivesRestart(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	AdvanceIncidentContinuity(st, anomalyWindow(1000, 1300, 100), 20)
	AdvanceIncidentContinuity(st, anomalyWindow(1300, 1600, 100), 20)

	// 模拟重启：从持久化字段重建状态。
	reloaded := &IncidentContinuityState{
		DedupKey:                st.DedupKey,
		ConsecutiveWindows:      st.ConsecutiveWindows,
		LastCountedWindowToTs:   st.LastCountedWindowToTs,
		LastFinalizedWindowToTs: st.LastFinalizedWindowToTs,
		GapWindows:              st.GapWindows,
		WindowSeconds:           st.WindowSeconds,
	}
	if reloaded.ConsecutiveWindows != 2 {
		t.Fatalf("重启后连续数丢失: %d", reloaded.ConsecutiveWindows)
	}
	// 重启后继续推进第三个相邻窗口，应当接续而不是重新起算。
	out := AdvanceIncidentContinuity(reloaded, anomalyWindow(1600, 1900, 100), 20)
	if out != OutcomeCounted {
		t.Fatalf("重启后 outcome=%q want counted", out)
	}
	if reloaded.ConsecutiveWindows != 3 {
		t.Fatalf("重启后未接续计数: %d want 3", reloaded.ConsecutiveWindows)
	}
	// 重启后同窗口重复扫描仍然幂等。
	if out := AdvanceIncidentContinuity(reloaded, anomalyWindow(1600, 1900, 100), 20); out != OutcomeDuplicateIgnored {
		t.Fatalf("重启后幂等失效: outcome=%q", out)
	}
}

// 样本分母口径必须可追溯。
func TestIncidentSampleScopeDescribed(t *testing.T) {
	st := IncidentContinuityState{WindowSeconds: 300, SampleScope: IncidentSampleScopeKeyedRequests}
	desc := DescribeSampleScope(st, 20)
	if desc == "" {
		t.Fatal("样本口径说明为空")
	}
	for _, want := range []string{"300", "20", "用户拒绝不计入"} {
		if indexOf(desc, want) < 0 {
			t.Errorf("口径说明缺少 %q: %s", want, desc)
		}
	}
}

// 零样本但查询成功（确实零流量）与采集缺口是两件事。
func TestIncidentZeroTrafficDistinctFromGap(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	zero := IncidentWindowObservation{
		Window:      IncidentWindow{FromTs: 1000, ToTs: 1300},
		Scope:       IncidentSampleScopeKeyedRequests,
		SampleTotal: 0,
		DataPresent: true, // 查成功了，确实零流量
		Anomalous:   false,
		Final:       true,
	}
	out := AdvanceIncidentContinuity(st, zero, 20)
	if out == OutcomeGapFrozen {
		t.Fatal("零流量被误判为采集缺口")
	}
	if out != OutcomeNormalReset {
		t.Fatalf("零流量正常窗口 outcome=%q want normal_reset", out)
	}
	if st.GapWindows != 0 {
		t.Fatalf("零流量被计入缺口窗口: %d", st.GapWindows)
	}
}

// 恢复后再次异常：旧的多源确认不得让新一段异常跳过门槛。
func TestIncidentMultiSourceNotCarriedAcrossNormalWindow(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}

	// 第一段：双源异常窗口 → SEV1。
	obs := anomalyWindow(1000, 1300, 100)
	obs.SourceCount = 2
	AdvanceIncidentContinuity(st, obs, 20)
	if !st.MultiSourceConfirmed {
		t.Fatal("前置条件失败: 当前段多源确认未置位")
	}
	if got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioPrimaryNoFallback, Continuity: *st,
	}); got != "SEV1" {
		t.Fatalf("第一段严重度=%q want SEV1", got)
	}

	// 正常窗口打断。
	normal := anomalyWindow(1300, 1600, 100)
	normal.Anomalous = false
	if out := AdvanceIncidentContinuity(st, normal, 20); out != OutcomeNormalReset {
		t.Fatalf("正常窗口 outcome=%q want normal_reset", out)
	}
	if st.MultiSourceConfirmed {
		t.Fatal("正常窗口后当前段多源确认未重置")
	}
	// 历史证据应保留。
	if !st.EverMultiSourceConfirmed {
		t.Fatal("历史多源证据被一并清掉了")
	}

	// 第二段：单源、非硬信号的一个异常窗口。
	single := anomalyWindow(1600, 1900, 100)
	single.SourceCount = 1
	AdvanceIncidentContinuity(st, single, 20)
	if st.ConsecutiveWindows != 1 {
		t.Fatalf("前置条件失败: 连续数=%d want 1", st.ConsecutiveWindows)
	}
	got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioPrimaryNoFallback, Continuity: *st,
	})
	if got == "SEV1" {
		t.Fatal("新一段单源异常凭旧的多源确认被判 SEV1，绕过了确认门槛")
	}
	if got != "SEV3" {
		t.Fatalf("第二段严重度=%q want SEV3", got)
	}
}

// 恢复后再次异常：旧的硬信号不得让新一段异常跳过门槛。
func TestIncidentHardSignalNotCarriedAcrossNormalWindow(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}

	// 第一段：硬信号异常窗口 → SEV1（硬信号不要求连续窗口）。
	hard := anomalyWindow(1000, 1300, 100)
	hard.HardSignal = true
	AdvanceIncidentContinuity(st, hard, 20)
	if !st.HardSignal {
		t.Fatal("前置条件失败: 当前段硬信号未置位")
	}
	if got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioPrimaryNoFallback, Continuity: *st,
	}); got != "SEV1" {
		t.Fatalf("第一段严重度=%q want SEV1", got)
	}

	// 正常窗口打断。
	normal := anomalyWindow(1300, 1600, 100)
	normal.Anomalous = false
	AdvanceIncidentContinuity(st, normal, 20)
	if st.HardSignal {
		t.Fatal("正常窗口后当前段硬信号未重置")
	}
	if !st.EverHardSignal {
		t.Fatal("历史硬信号证据被一并清掉了")
	}

	// 第二段：单源、非硬信号的一个异常窗口。
	single := anomalyWindow(1600, 1900, 100)
	single.SourceCount = 1
	AdvanceIncidentContinuity(st, single, 20)
	got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioPrimaryNoFallback, Continuity: *st,
	})
	if got == "SEV1" {
		t.Fatal("新一段异常凭旧硬信号被判 SEV1，绕过了确认门槛")
	}
	if got != "SEV3" {
		t.Fatalf("第二段严重度=%q want SEV3", got)
	}

	// SEV0 场景同样不得凭旧硬信号直接触发。
	if got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioSiteWideOrSecurity, Continuity: *st,
	}); got == "SEV0" {
		t.Fatal("新一段异常凭旧硬信号被判 SEV0")
	}
}

// 新一段异常自己达标时仍应正常升级——重置不等于永久压低。
func TestIncidentNewSegmentCanReconfirmOnItsOwn(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	hard := anomalyWindow(1000, 1300, 100)
	hard.HardSignal = true
	AdvanceIncidentContinuity(st, hard, 20)

	normal := anomalyWindow(1300, 1600, 100)
	normal.Anomalous = false
	AdvanceIncidentContinuity(st, normal, 20)

	// 新一段自己带硬信号：应当重新达到 SEV1。
	hard2 := anomalyWindow(1600, 1900, 100)
	hard2.HardSignal = true
	AdvanceIncidentContinuity(st, hard2, 20)
	if got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioPrimaryNoFallback, Continuity: *st,
	}); got != "SEV1" {
		t.Fatalf("新一段自带硬信号严重度=%q want SEV1", got)
	}
}

// 新一段靠两个连续窗口达标（不依赖任何旧标记）。
func TestIncidentNewSegmentReachesSev1ByTwoWindows(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	obs := anomalyWindow(1000, 1300, 100)
	obs.SourceCount = 2
	AdvanceIncidentContinuity(st, obs, 20)

	normal := anomalyWindow(1300, 1600, 100)
	normal.Anomalous = false
	AdvanceIncidentContinuity(st, normal, 20)

	// 第二段第一个窗口：不应 SEV1。
	s1 := anomalyWindow(1600, 1900, 100)
	s1.SourceCount = 1
	AdvanceIncidentContinuity(st, s1, 20)
	if got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioPrimaryNoFallback, Continuity: *st,
	}); got == "SEV1" {
		t.Fatal("第二段首个窗口即判 SEV1")
	}
	// 第二个窗口：自己达标。
	s2 := anomalyWindow(1900, 2200, 100)
	s2.SourceCount = 1
	AdvanceIncidentContinuity(st, s2, 20)
	if got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioPrimaryNoFallback, Continuity: *st,
	}); got != "SEV1" {
		t.Fatalf("第二段两窗口严重度=%q want SEV1", got)
	}
}

// 采集缺口不算恢复：缺口窗口不得重置当前段证据标记。
func TestIncidentGapDoesNotResetSegmentEvidence(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	hard := anomalyWindow(1000, 1300, 100)
	hard.HardSignal = true
	AdvanceIncidentContinuity(st, hard, 20)

	gap := anomalyWindow(1300, 1600, 100)
	gap.DataPresent = false
	if out := AdvanceIncidentContinuity(st, gap, 20); out != OutcomeGapFrozen {
		t.Fatalf("缺口 outcome=%q want gap_frozen", out)
	}
	if !st.HardSignal {
		t.Fatal("采集缺口把当前段硬信号清掉了——缺口不算恢复")
	}
}

// 历史最高严重度与当前段判定分离：历史保留，但不回灌门槛。
func TestIncidentPeakSeverityDoesNotFeedBackIntoDecision(t *testing.T) {
	st := &IncidentContinuityState{DedupKey: "k"}
	hard := anomalyWindow(1000, 1300, 100)
	hard.HardSignal = true
	AdvanceIncidentContinuity(st, hard, 20)
	sev := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioPrimaryNoFallback, Continuity: *st,
	})
	RecordPeakSeverity(st, sev)
	if st.PeakSeverity != "SEV1" {
		t.Fatalf("历史最高=%q want SEV1", st.PeakSeverity)
	}

	normal := anomalyWindow(1300, 1600, 100)
	normal.Anomalous = false
	AdvanceIncidentContinuity(st, normal, 20)

	single := anomalyWindow(1600, 1900, 100)
	single.SourceCount = 1
	AdvanceIncidentContinuity(st, single, 20)
	got := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario: ScenarioPrimaryNoFallback, Continuity: *st,
	})
	if got == "SEV1" {
		t.Fatal("历史最高严重度回灌，新一段直接继承 SEV1")
	}
	// 历史最高仍应保留，供展示与审计。
	if st.PeakSeverity != "SEV1" {
		t.Fatalf("历史最高被覆盖为 %q", st.PeakSeverity)
	}
	RecordPeakSeverity(st, got)
	if st.PeakSeverity != "SEV1" {
		t.Fatalf("记录更低严重度后历史最高变为 %q，应保持 SEV1", st.PeakSeverity)
	}
}
