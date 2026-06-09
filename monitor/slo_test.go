package monitor

import "testing"

// TestComputeSLO:非错误率、达标判定、剩余错误预算、燃烧速度。
func TestComputeSLO(t *testing.T) {
	m := newTestMonitor(t)
	now := int64(1_700_000_400)
	bucket := now / 60 * 60
	// 窗口内 990 成功 + 10 失败 = 1000,非错误率 99%
	if err := m.upsertSamples([]MetricSample{
		{BucketTs: bucket, ChannelId: 1, ModelName: "m", Grp: "g", Success: 990, Failed: 10},
	}); err != nil {
		t.Fatal(err)
	}
	c := AlertConfig{
		SLOEnabled: true, SLOTargetPct: 99, SLOWindowDays: 1,
		BurnFastEnabled: true, BurnFastRate: 14, BurnFastWindowMin: 60,
		BurnSlowEnabled: true, BurnSlowRate: 3, BurnSlowWindowMin: 360,
	}
	s := m.computeSLO(c, now)
	if !approx(s.CurrentPct, 99) {
		t.Errorf("当前非错误率应 99%%,实际 %v", s.CurrentPct)
	}
	if !s.Compliant {
		t.Error("99%% ≥ 目标99%% 应达标")
	}
	if !approx(s.BudgetLeftPct, 0) { // 1% 错误 = 用满 99% 目标的预算
		t.Errorf("剩余预算应 0%%(用满),实际 %v", s.BudgetLeftPct)
	}
	if !approx(s.BurnFast, 1) || !approx(s.BurnSlow, 1) {
		t.Errorf("燃烧速度应 1×,实际 fast=%v slow=%v", s.BurnFast, s.BurnSlow)
	}

	// 目标提到 99.9% → 同样 1% 错误率 → 未达标、燃烧 ~10×、预算超支
	c.SLOTargetPct = 99.9
	s = m.computeSLO(c, now)
	if s.Compliant {
		t.Error("99%% < 目标99.9%% 应未达标")
	}
	if s.BurnFast < 9 { // 1% / 0.1% = 10×
		t.Errorf("目标99.9%%时燃烧应 ~10×,实际 %v", s.BurnFast)
	}
	if s.BudgetLeftPct > 0 {
		t.Errorf("超支时剩余预算应 ≤0,实际 %v", s.BudgetLeftPct)
	}

	// 未启用 → 空状态
	c.SLOEnabled = false
	if got := m.computeSLO(c, now); got.Enabled {
		t.Error("未启用应 Enabled=false")
	}
}
