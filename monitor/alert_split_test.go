package monitor

import "testing"

// TestAlertCategorySplit 守住「错误与交付异常两套报警机制分开控制」这条需求。
// 改造前 anomaly_* 归在 model 栏目,被 ModelAlertsEnabled 一起管——正是要修的问题。
func TestAlertCategorySplit(t *testing.T) {
	cases := map[string]string{
		"error_rate":     "model",
		"error_burst":    "model",
		"sampler_down":   "model",
		"burn_fast":      "model",
		"anomaly_rate":   "anomaly",
		"anomaly_billed": "anomaly",
		"anomaly_burst":  "anomaly",
		"infra_db_conn":  "server",
	}
	for kind, want := range cases {
		if got := alertCategory(kind); got != want {
			t.Errorf("alertCategory(%q)=%q, 期望 %q", kind, got, want)
		}
	}
}

// TestCategoryEmailEnabledIndependent 关掉交付异常邮件时,错误邮件必须照发;反之亦然。
func TestCategoryEmailEnabledIndependent(t *testing.T) {
	onlyErr := AlertConfig{ModelAlertsEnabled: true, AnomalyAlertsEnabled: false, ServerAlertsEnabled: true}
	if !categoryEmailEnabled(onlyErr, "error_rate") {
		t.Error("关闭交付异常后,错误告警不应受影响")
	}
	if categoryEmailEnabled(onlyErr, "anomaly_rate") {
		t.Error("AnomalyAlertsEnabled=false 时交付异常不应发信")
	}

	onlyAnom := AlertConfig{ModelAlertsEnabled: false, AnomalyAlertsEnabled: true}
	if categoryEmailEnabled(onlyAnom, "error_burst") {
		t.Error("ModelAlertsEnabled=false 时错误不应发信")
	}
	if !categoryEmailEnabled(onlyAnom, "anomaly_burst") {
		t.Error("关闭错误告警后,交付异常告警不应受影响")
	}
}

// TestCategoryCooldownIndependent 交付异常用自己的冷却(观察类,不需要和错误一样急)。
func TestCategoryCooldownIndependent(t *testing.T) {
	c := AlertConfig{CooldownMin: 30, AnomalyCooldownMin: 60}
	if got := categoryCooldownMin(c, "error_rate"); got != 30 {
		t.Errorf("错误冷却应为 30,得到 %d", got)
	}
	if got := categoryCooldownMin(c, "anomaly_rate"); got != 60 {
		t.Errorf("交付异常冷却应为 60,得到 %d", got)
	}
	// 未配独立冷却时回落到通用值,避免老库升级后冷却变 0 导致刷屏。
	if got := categoryCooldownMin(AlertConfig{CooldownMin: 30}, "anomaly_rate"); got != 30 {
		t.Errorf("未配独立冷却应回落 30,得到 %d", got)
	}
}

// TestAnomalyRuleAtMostOne 守住「一行最多触发一条交付异常规则」与优先级顺序。
// 三条规则的条件是重叠的(一个坏渠道往往同时满足钱/占比/成簇),
// 若各自独立判断就会一行连发三封邮件——这是刻意用单一选择函数收口的原因。
func TestAnomalyRuleAtMostOne(t *testing.T) {
	c := defaultAlertConfig()
	// 连续多桶,让成簇条件也成立,确保下面命中的是更高优先级的规则。
	burst := []TimePoint{{Anomaly: 3}, {Anomaly: 3}, {Anomaly: 3}, {Anomaly: 3}}

	cases := []struct {
		name string
		row  Row
		want string
	}{
		{"钱优先于占比与成簇",
			Row{Total: 100, Anomaly: 40, AnomalyRate: 40, AnomalyBilled: 40, AnomalyCostUSD: 40, Spark: burst},
			"anomaly_billed"},
		{"未扣费但占比高走占比",
			Row{Total: 100, Anomaly: 40, AnomalyRate: 40, AnomalyCostUSD: 0, Spark: burst},
			"anomaly_rate"},
		{"占比不到但成簇走成簇",
			Row{Total: 10000, Anomaly: 12, AnomalyRate: 0.12, AnomalyCostUSD: 0, Spark: burst},
			"anomaly_burst"},
		{"条数不足一律不报(小样本抖动)",
			Row{Total: 10, Anomaly: 3, AnomalyRate: 30, AnomalyCostUSD: 0, Spark: burst},
			""},
		{"干净行不报",
			Row{Total: 100, Anomaly: 0, AnomalyRate: 0}, ""},
	}
	for _, tc := range cases {
		if got := anomalyRuleFor(c, tc.row); got != tc.want {
			t.Errorf("%s: anomalyRuleFor=%q, 期望 %q", tc.name, got, tc.want)
		}
	}
}

// TestAnomalyBilledIgnoresMinCount 已扣费规则必须绕过最小条数门槛。
// 单笔大额(生产实测单条最高约 $2)不该因为"只有 1 条"被压住不报。
func TestAnomalyBilledIgnoresMinCount(t *testing.T) {
	c := defaultAlertConfig()
	r := Row{Total: 50, Anomaly: 1, AnomalyBilled: 1, AnomalyCostUSD: c.AnomalyBilledUSD + 1}
	if got := anomalyRuleFor(c, r); got != "anomaly_billed" {
		t.Errorf("单笔超额应报 anomaly_billed,得到 %q", got)
	}
}

// TestDefaultAnomalyThresholds 阈值是按真实数据标定的,改动需同步文档 09 §7.6。
func TestDefaultAnomalyThresholds(t *testing.T) {
	d := defaultAlertConfig()
	if d.AnomalyRatePct != 8 {
		t.Errorf("交付异常率阈值应为 8%%,得到 %v", d.AnomalyRatePct)
	}
	// 旧默认 8 是按含 219 条误报的旧口径定的;新口径下应为 5。
	if d.AnomalyMinCount != 5 {
		t.Errorf("最小条数应为 5,得到 %d", d.AnomalyMinCount)
	}
	if d.AnomalyCooldownMin <= d.CooldownMin {
		t.Errorf("观察类冷却应长于错误冷却:anomaly=%d error=%d", d.AnomalyCooldownMin, d.CooldownMin)
	}
	if d.AnomalyBilledUSD <= 0 {
		t.Error("已扣费阈值须启用:钱的事不该等成簇")
	}
}
