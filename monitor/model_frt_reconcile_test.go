package monitor

import (
	"math"
	"testing"
)

// 页面三级均值必须与本地事实的直接聚合一致。
//
// 这是"对账"的可执行版本：不依赖线上隧道，而是用同一批本地分钟事实，
// 一边走页面的报告路径（modelFRTTree → view），一边用 SQL 直接 SUM，
// 两者在渠道、分组、模型三级都必须逐项相等。
// 若实现哪天改成按 P95、桶中点或部分总和除全量样本，这里会立刻不等。
func TestModelFRTPageMeanMatchesDirectAggregationAtAllThreeLevels(t *testing.T) {
	m, end := modelFRTFixture(t, true)

	// 造多渠道、多分组、多模型、多用户的分钟事实，确保三级聚合各不相同。
	// 合法毫秒区间：<=500 或 >3000（modelFRTInsert 的分桶约束）。
	type sample struct {
		channel      int
		group, model string
		count, ms    int64
	}
	for _, s := range []sample{
		{1, "ga", "fast", 3, 400},
		{1, "ga", "slow", 2, 4000},
		{1, "gb", "fast", 5, 400},
		{2, "ga", "slow", 4, 3600},
		{2, "gc", "fast", 1, 100},
		{3, "ga", "slow", 7, 4500},
	} {
		modelFRTMeanInsert(t, m, end, s.channel, s.group, s.model, s.count, s.ms)
	}

	report := modelFRTBuild(t, m, "24h", "")
	if len(report.Channels) != 3 {
		t.Fatalf("应有 3 个渠道：%d", len(report.Channels))
	}

	// 直接聚合：绕开报告路径，用 SQL 按三级分别 SUM。
	directMean := func(where string, args ...any) (float64, int64) {
		t.Helper()
		var row struct {
			SumMs    int64
			Observed int64
		}
		q := m.storeDB.Model(&CapacityUserMinuteSample{}).
			Where("bucket_ts >= ? AND bucket_ts < ?", end-86400, end).
			Select("COALESCE(SUM(frt_sum_ms),0) AS sum_ms, COALESCE(SUM(ttft_observed),0) AS observed")
		if where != "" {
			q = q.Where(where, args...)
		}
		if err := q.Scan(&row).Error; err != nil {
			t.Fatal(err)
		}
		if row.Observed == 0 {
			t.Fatalf("直接聚合样本数为 0，无法对账：%s %v", where, args)
		}
		return float64(row.SumMs) / float64(row.Observed), row.Observed
	}

	checked := 0
	for _, c := range report.Channels {
		// 渠道级
		wantMean, wantObserved := directMean("channel_id = ?", c.ChannelID)
		if c.FRT.MeanMs == nil {
			t.Fatalf("渠道 %d 均值应可计算", c.ChannelID)
		}
		if math.Abs(*c.FRT.MeanMs-wantMean) > 1e-9 {
			t.Fatalf("渠道 %d 均值不符：页面 %v vs 直接聚合 %v", c.ChannelID, *c.FRT.MeanMs, wantMean)
		}
		if c.FRT.Observed == nil || *c.FRT.Observed != wantObserved {
			t.Fatalf("渠道 %d 样本数不符：页面 %v vs 直接聚合 %d", c.ChannelID, c.FRT.Observed, wantObserved)
		}
		checked++

		for _, g := range c.Groups {
			// 分组级
			wantMean, wantObserved = directMean("channel_id = ? AND grp = ?", c.ChannelID, g.Group)
			if g.FRT.MeanMs == nil || math.Abs(*g.FRT.MeanMs-wantMean) > 1e-9 {
				t.Fatalf("渠道 %d 分组 %s 均值不符：页面 %v vs %v", c.ChannelID, g.Group, g.FRT.MeanMs, wantMean)
			}
			if g.FRT.Observed == nil || *g.FRT.Observed != wantObserved {
				t.Fatalf("渠道 %d 分组 %s 样本数不符", c.ChannelID, g.Group)
			}
			checked++

			for _, mo := range g.Models {
				// 模型级
				wantMean, wantObserved = directMean("channel_id = ? AND grp = ? AND model_name = ?",
					c.ChannelID, g.Group, mo.Model)
				if mo.FRT.MeanMs == nil || math.Abs(*mo.FRT.MeanMs-wantMean) > 1e-9 {
					t.Fatalf("渠道 %d 分组 %s 模型 %s 均值不符：页面 %v vs %v",
						c.ChannelID, g.Group, mo.Model, mo.FRT.MeanMs, wantMean)
				}
				if mo.FRT.Observed == nil || *mo.FRT.Observed != wantObserved {
					t.Fatalf("渠道 %d 分组 %s 模型 %s 样本数不符", c.ChannelID, g.Group, mo.Model)
				}
				checked++
			}
		}
	}
	// 节点总数按报告实际结构计算，避免把硬编码的期望值写错而掩盖真实覆盖。
	// 必须每个渠道、每个分组、每个模型都被核对到，一个不漏。
	expected := 0
	for _, c := range report.Channels {
		expected++
		for _, g := range c.Groups {
			expected++
			expected += len(g.Models)
		}
	}
	if checked != expected || checked == 0 {
		t.Fatalf("三级节点必须全部核对：已核对 %d，实际结构 %d", checked, expected)
	}
	t.Logf("已逐项对账三级节点 %d 个（渠道/分组/模型），页面均值与直接聚合一致", checked)

	// 父层必须是子层的加权合并，而不是子层均值的平均值。
	// 渠道 1：(3*400 + 2*4000 + 5*400)/(3+2+5) = 11200/10 = 1120ms。
	// 若错误地取各模型均值的算术平均，会得到 (400+4000+400)/3 = 1600ms。
	var channel1 *ModelFRTChannel
	for i := range report.Channels {
		if report.Channels[i].ChannelID == 1 {
			channel1 = &report.Channels[i]
		}
	}
	if channel1 == nil {
		t.Fatal("缺少渠道 1")
	}
	if math.Abs(*channel1.FRT.MeanMs-1120) > 1e-9 {
		t.Fatalf("渠道均值必须按样本数加权（期望 1120ms，模型均值平均会得 1600ms），实为 %v", *channel1.FRT.MeanMs)
	}
	if *channel1.FRT.Observed != 10 {
		t.Fatalf("渠道 1 样本数应为 10，实为 %d", *channel1.FRT.Observed)
	}
}
