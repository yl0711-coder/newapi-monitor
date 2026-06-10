package monitor

import "testing"

// TestRollupAndCompare:分钟→小时汇总、长期序列、同比环比(近24h / 前24h / 上周同期)。
func TestRollupAndCompare(t *testing.T) {
	m := newTestMonitor(t)
	const h = int64(3600)
	now := int64(1_700_000_000)
	nowH := now / h * h
	mk := func(bucket, succ, fail, quota int64) MetricSample {
		return MetricSample{BucketTs: bucket, ChannelID: 1, ModelName: "m", Grp: "g", Success: succ, Failed: fail, Quota: quota}
	}
	if err := m.upsertSamples([]MetricSample{
		mk(nowH-2*h, 90, 10, 500000), // 近 24h 内(成功率 90%、成本 $1)
		mk(nowH-30*h, 50, 50, 0),     // 前 24h 内(环比基)
		mk(nowH-180*h, 200, 0, 0),    // 上周同期(同比基)
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.rollupHours(nowH - 200*h); err != nil {
		t.Fatal(err)
	}

	if pts := m.storeHourSeries(nowH - 200*h); len(pts) != 3 {
		t.Fatalf("小时序列应 3 个点,实际 %d", len(pts))
	}

	c := m.storeCompare(now)
	if c.Now.Total != 100 || c.Now.Failed != 10 || !approx(c.Now.SuccessRate, 90) || !approx(c.Now.CostUSD, 1.0) {
		t.Errorf("近24h 应 total=100/failed=10/成功率90%%/成本$1,实际 %+v", c.Now)
	}
	if c.Prev.Total != 100 { // 50+50
		t.Errorf("前24h(环比基)应 total=100,实际 %d", c.Prev.Total)
	}
	if c.LastWeek.Total != 200 || c.LastWeek.Failed != 0 {
		t.Errorf("上周同期(同比基)应 total=200/failed=0,实际 %d/%d", c.LastWeek.Total, c.LastWeek.Failed)
	}

	// 清理:90 天上限删不到这些(都在近 200h),改用更短上限验证删除生效
	if n, err := m.pruneHoursOlderThan(nowH - 100*h); err != nil || n != 1 { // 仅 nowH-180h 那条被删
		t.Errorf("应删 1 条过期小时汇总,实际 n=%d err=%v", n, err)
	}
}
