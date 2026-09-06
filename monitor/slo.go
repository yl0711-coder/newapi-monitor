package monitor

import (
	"context"
	"fmt"
)

// slo.go:SLO / 错误预算 / 燃烧速度(burn rate)。
// SLI 用「非错误率」=(成功 + 异常)/ 总 —— 错误(type=5)才算不达标,客户端断开(异常)不算我们的锅。
// 错误预算 = 1 − 目标;燃烧速度 = 近窗口错误率 / 允许失败率。

// SLOStatus 是一次 SLO 计算结果,供看板展示与燃烧告警共用。
type SLOStatus struct {
	Enabled          bool    `json:"enabled"`
	DataAvailable    bool    `json:"data_available"`
	TargetPct        float64 `json:"target_pct"`      // 目标(如 99)
	WindowDays       int     `json:"window_days"`     // SLO 窗口(天)
	CurrentPct       float64 `json:"current_pct"`     // 窗口内非错误率
	Compliant        bool    `json:"compliant"`       // 当前 ≥ 目标
	BudgetUsedPct    float64 `json:"budget_used_pct"` // 错误预算已用 %
	BudgetLeftPct    float64 `json:"budget_left_pct"` // 剩余 %(可为负 = 已超支)
	BurnFast         float64 `json:"burn_fast"`       // 近 fast 窗口燃烧速度(倍)
	BurnSlow         float64 `json:"burn_slow"`       // 近 slow 窗口燃烧速度(倍)
	FastWindowMin    int     `json:"fast_window_min"` // 快烧观察窗(分钟),供看板标注
	SlowWindowMin    int     `json:"slow_window_min"` // 慢烧观察窗(分钟)
	CoveragePct      float64 `json:"coverage_pct"`
	CoverageComplete bool    `json:"coverage_complete"`
}

type stabilitySLOCounts struct {
	Success int64
	Anomaly int64
	Failed  int64
}

func (c stabilitySLOCounts) total() int64 {
	return c.Success + c.Anomaly + c.Failed
}

// queryStabilitySLOCounts reads the same signed hourly ledger whose ingest
// states prove coverage. Keeping evidence and aggregation on one source avoids
// treating a shorter-retained metric minute window as a complete multi-day
// SLO after a classification migration.
func (m *Monitor) queryStabilitySLOCounts(ctx context.Context, from, to int64) (stabilitySLOCounts, error) {
	var counts stabilitySLOCounts
	err := m.storeDB.WithContext(ctx).Raw(`SELECT
		COALESCE(SUM(sh.success),0) AS success,
		COALESCE(SUM(sh.anomaly),0) AS anomaly,
		COALESCE(SUM(sh.failed),0) AS failed
		FROM stability_hour_samples sh
		WHERE sh.hour_ts >= ? AND sh.hour_ts < ? AND `+stabilityEffectiveSampleSQL("sh"), from, to).
		Scan(&counts).Error
	return counts, err
}

// computeSLO 按配置算出 SLO 达标率、剩余错误预算、快/慢燃烧速度(数据取本地采样库,零生产负担)。
func (m *Monitor) computeSLO(c AlertConfig, nowUnix int64) SLOStatus {
	s := SLOStatus{Enabled: c.SLOEnabled, TargetPct: c.SLOTargetPct, WindowDays: c.SLOWindowDays}
	if !c.SLOEnabled || c.SLOTargetPct <= 0 || c.SLOTargetPct >= 100 || c.SLOWindowDays <= 0 {
		return s
	}
	budgetRate := (100 - c.SLOTargetPct) / 100 // 允许失败率,如目标 99% → 0.01

	to := finalizedStabilityHourTo(nowUnix)
	from := to - int64(c.SLOWindowDays)*86400
	coverage := m.stabilityDataCoverage(context.Background(), from, to, nowUnix)
	s.CoveragePct, s.CoverageComplete = coverage.Percent, coverage.Complete
	if !coverage.Complete {
		return s
	}
	counts, err := m.queryStabilitySLOCounts(context.Background(), from, to)
	if err != nil {
		return s
	}
	total := counts.total()
	if total > 0 {
		s.DataAvailable = true
		s.CurrentPct = float64(total-counts.Failed) / float64(total) * 100 // 非错误率
		allowed := budgetRate * float64(total)
		if allowed > 0 {
			s.BudgetUsedPct = float64(counts.Failed) / allowed * 100
			s.BudgetLeftPct = 100 - s.BudgetUsedPct
		}
	} else {
		// 无事实无法证明 100% 达标；保持不可用且不计算燃烧速度。
		return s
	}
	s.Compliant = s.CurrentPct >= c.SLOTargetPct

	s.BurnFast = m.burnRate(to, c.BurnFastWindowMin, budgetRate)
	s.BurnSlow = m.burnRate(to, c.BurnSlowWindowMin, budgetRate)
	s.FastWindowMin = c.BurnFastWindowMin
	s.SlowWindowMin = c.BurnSlowWindowMin
	return s
}

// burnRate 近 windowMin 分钟的「错误率 / 允许失败率」,即烧错误预算的倍速。
func (m *Monitor) burnRate(nowUnix int64, windowMin int, budgetRate float64) float64 {
	if windowMin <= 0 || budgetRate <= 0 {
		return 0
	}
	from := nowUnix - int64(windowMin)*60
	counts, err := m.queryStabilitySLOCounts(context.Background(), from, nowUnix)
	total := counts.total()
	if err != nil || total == 0 {
		return 0
	}
	errRate := float64(counts.Failed) / float64(total)
	return errRate / budgetRate
}

// evaluateBurn 燃烧告警:快烧(紧急)优先于慢烧;命中且过冷却则发邮件。由 evaluateAlerts 调用。
func (m *Monitor) evaluateBurn(c AlertConfig, nowUnix int64) {
	if !c.SLOEnabled {
		return
	}
	slo := m.computeSLO(c, nowUnix)
	if !slo.DataAvailable {
		return
	}
	if c.BurnFastEnabled && c.BurnFastRate > 0 && slo.BurnFast >= c.BurnFastRate {
		m.fire(c, "burn_fast", "", "🔴 错误预算快烧",
			fmt.Sprintf("近 %d 分钟错误预算燃烧 %.1f×(阈值 %.0f×)——SLO 即将告破,请立刻排查。\n当前成功率 %.2f%% / 目标 %.1f%%,剩余错误预算 %.0f%%。",
				c.BurnFastWindowMin, slo.BurnFast, c.BurnFastRate, slo.CurrentPct, c.SLOTargetPct, slo.BudgetLeftPct), nowUnix)
		return
	}
	if c.BurnSlowEnabled && c.BurnSlowRate > 0 && slo.BurnSlow >= c.BurnSlowRate {
		m.fire(c, "burn_slow", "", "🟡 错误预算慢烧",
			fmt.Sprintf("近 %d 小时错误预算燃烧 %.1f×(阈值 %.0f×)——趋势恶化,建议排查。\n当前成功率 %.2f%% / 目标 %.1f%%,剩余错误预算 %.0f%%。",
				c.BurnSlowWindowMin/60, slo.BurnSlow, c.BurnSlowRate, slo.CurrentPct, c.SLOTargetPct, slo.BudgetLeftPct), nowUnix)
	}
}
