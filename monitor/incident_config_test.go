package monitor

import "testing"

// 默认值就是确认过的那一组，且默认关闭。
func TestIncidentConfigDefaults(t *testing.T) {
	c := DefaultIncidentConfig()
	if c.Enabled {
		t.Fatal("B 期开关默认必须关闭")
	}
	if c.CooldownSev0Min != 15 || c.CooldownSev1Min != 30 ||
		c.CooldownSev2Min != 60 || c.CooldownSev3Min != 180 {
		t.Fatalf("冷却默认值不符: %d/%d/%d/%d",
			c.CooldownSev0Min, c.CooldownSev1Min, c.CooldownSev2Min, c.CooldownSev3Min)
	}
	if c.WindowSeconds != 300 {
		t.Fatalf("窗口默认 =%d want 300", c.WindowSeconds)
	}
	if c.MinSample != 20 {
		t.Fatalf("最小样本默认 =%d want 20", c.MinSample)
	}
	// 事故查询门控必须独立且有上限，不能复用容量 1 的 usageDetailGate。
	if c.QueryGateCapacity != 4 {
		t.Fatalf("查询门控容量 =%d want 4", c.QueryGateCapacity)
	}
	if c.QueryGateCapacity == 1 {
		t.Fatal("门控容量为 1 会与 usageDetailGate 等价，失去独立性")
	}
}

// 按 SEV 取冷却值；未知严重度按最长处理。
func TestIncidentCooldownMinutesBySeverity(t *testing.T) {
	c := DefaultIncidentConfig()
	cases := map[string]int{"SEV0": 15, "SEV1": 30, "SEV2": 60, "SEV3": 180}
	for sev, want := range cases {
		if got := c.CooldownMinutesFor(sev); got != want {
			t.Errorf("%s 冷却 =%d want %d", sev, got, want)
		}
	}
	if got := c.CooldownMinutesFor(""); got != 180 {
		t.Errorf("未知严重度冷却 =%d want 180（按最长）", got)
	}
	if got := c.CooldownMinutesFor("SEV9"); got != 180 {
		t.Errorf("非法严重度冷却 =%d want 180", got)
	}
}

// 冷却截止时刻按秒落库，不放内存。
func TestIncidentCooldownUntil(t *testing.T) {
	c := DefaultIncidentConfig()
	now := int64(1760000000)
	if got := c.CooldownUntil(now, "SEV1"); got != now+30*60 {
		t.Fatalf("SEV1 冷却截止 =%d want %d", got, now+30*60)
	}
}

// 冷却期夹取：越界回落默认。
func TestIncidentCooldownClamp(t *testing.T) {
	cases := []struct{ in, def, want int }{
		{30, 30, 30},
		{4, 30, 30},      // 低于下界 5
		{1441, 30, 30},   // 高于上界 1440
		{5, 30, 5},       // 下界本身合法
		{1440, 30, 1440}, // 上界本身合法
		{0, 60, 60},
		{-1, 180, 180},
	}
	for _, c := range cases {
		if got := clampIncidentCooldown(c.in, c.def); got != c.want {
			t.Errorf("clampCooldown(%d, def=%d) =%d want %d", c.in, c.def, got, c.want)
		}
	}
}

// 窗口粒度夹取：越界或非 60 整数倍都回落默认。
func TestIncidentWindowSecondsClamp(t *testing.T) {
	cases := []struct{ in, want int }{
		{300, 300},
		{60, 60},
		{3600, 3600},
		{59, 300},   // 低于下界
		{3601, 300}, // 高于上界
		{90, 300},   // 不是 60 的整数倍——会切开分钟桶
		{301, 300},
		{0, 300},
	}
	for _, c := range cases {
		if got := clampIncidentWindowSeconds(c.in); got != c.want {
			t.Errorf("clampWindow(%d) =%d want %d", c.in, got, c.want)
		}
	}
}

// 最小样本夹取。
func TestIncidentMinSampleClamp(t *testing.T) {
	cases := []struct{ in, want int }{
		{20, 20}, {5, 5}, {1000, 1000},
		{4, 20}, {1001, 20}, {0, 20}, {-5, 20},
	}
	for _, c := range cases {
		if got := clampIncidentMinSample(c.in); got != c.want {
			t.Errorf("clampMinSample(%d) =%d want %d", c.in, got, c.want)
		}
	}
}

// 冷却到期本身不是新建条件：持续未结束的事故不得重复建单。
func TestIncidentCooldownExpiryAloneDoesNotReopen(t *testing.T) {
	now := int64(1760000000)
	expired := now - 1 // 冷却已到期

	// 事故尚未结束：即使冷却到期也不新建。
	if ShouldReopenAfterCooldown(false, now, expired) {
		t.Fatal("持续未结束的事故因冷却到期被重复建单")
	}
	// 事故已结束且冷却到期：才允许新建。
	if !ShouldReopenAfterCooldown(true, now, expired) {
		t.Fatal("已结束事故再次异常时应新建")
	}
	// 事故已结束但冷却未到期：仍不新建。
	if ShouldReopenAfterCooldown(true, now, now+600) {
		t.Fatal("冷却未到期不应新建")
	}
}

// 环境变量覆盖与夹取联动（显式设置非法值应回落默认）。
func TestLoadIncidentConfigEnvOverrideAndClamp(t *testing.T) {
	t.Setenv("MONITOR_INCIDENT_CORE_ENABLED", "true")
	t.Setenv("MONITOR_INCIDENT_COOLDOWN_SEV1_MIN", "45")
	t.Setenv("MONITOR_INCIDENT_WINDOW_SECONDS", "600")
	t.Setenv("MONITOR_INCIDENT_MIN_SAMPLE", "50")

	c := LoadIncidentConfig()
	if !c.Enabled {
		t.Fatal("开关未被环境变量开启")
	}
	if c.CooldownSev1Min != 45 {
		t.Fatalf("SEV1 冷却 =%d want 45", c.CooldownSev1Min)
	}
	if c.WindowSeconds != 600 {
		t.Fatalf("窗口 =%d want 600", c.WindowSeconds)
	}
	if c.MinSample != 50 {
		t.Fatalf("最小样本 =%d want 50", c.MinSample)
	}
	// 未设置的项保持默认。
	if c.CooldownSev0Min != 15 {
		t.Fatalf("未设置的 SEV0 冷却被改动: %d", c.CooldownSev0Min)
	}
}

// 非法环境变量值回落默认，不报错也不生效。
func TestLoadIncidentConfigRejectsOutOfRange(t *testing.T) {
	t.Setenv("MONITOR_INCIDENT_WINDOW_SECONDS", "90") // 非 60 整数倍
	t.Setenv("MONITOR_INCIDENT_MIN_SAMPLE", "99999")
	t.Setenv("MONITOR_INCIDENT_COOLDOWN_SEV2_MIN", "99999")

	c := LoadIncidentConfig()
	if c.WindowSeconds != 300 {
		t.Fatalf("非法窗口未回落: %d", c.WindowSeconds)
	}
	if c.MinSample != 20 {
		t.Fatalf("非法样本未回落: %d", c.MinSample)
	}
	if c.CooldownSev2Min != 60 {
		t.Fatalf("非法冷却未回落: %d", c.CooldownSev2Min)
	}
}

// 开关默认关闭时，环境变量未设置也必须是关闭。
func TestIncidentFlagDefaultOffWhenEnvAbsent(t *testing.T) {
	t.Setenv("MONITOR_INCIDENT_CORE_ENABLED", "")
	if LoadIncidentConfig().Enabled {
		t.Fatal("环境变量为空时开关应关闭")
	}
	t.Setenv("MONITOR_INCIDENT_CORE_ENABLED", "1")
	if LoadIncidentConfig().Enabled {
		t.Fatal("只接受 \"true\"，不应把 \"1\" 当成开启")
	}
}

// 既有告警的冷却规则不得被 Incident 配置改动。
//
// 用真实行为验证：读取 AlertConfig 默认值 → 加载 Incident 配置（含环境变量覆盖）
// → 再次读取 AlertConfig 默认值，必须完全一致。
func TestIncidentConfigDoesNotChangeAlertCooldowns(t *testing.T) {
	before := defaultAlertConfig()

	t.Setenv("MONITOR_INCIDENT_COOLDOWN_SEV1_MIN", "45")
	t.Setenv("MONITOR_INCIDENT_COOLDOWN_SEV2_MIN", "90")
	t.Setenv("MONITOR_INCIDENT_CORE_ENABLED", "true")
	c := LoadIncidentConfig()
	if c.CooldownSev1Min != 45 {
		t.Fatalf("前置条件失败，Incident 冷却未生效: %d", c.CooldownSev1Min)
	}

	after := defaultAlertConfig()
	if after.CooldownMin != before.CooldownMin {
		t.Errorf("既有告警 CooldownMin 被改动: %d → %d", before.CooldownMin, after.CooldownMin)
	}
	if after.AnomalyCooldownMin != before.AnomalyCooldownMin {
		t.Errorf("既有告警 AnomalyCooldownMin 被改动: %d → %d",
			before.AnomalyCooldownMin, after.AnomalyCooldownMin)
	}
	if after.UpstreamBalanceCooldownMin != before.UpstreamBalanceCooldownMin {
		t.Errorf("既有告警 UpstreamBalanceCooldownMin 被改动: %d → %d",
			before.UpstreamBalanceCooldownMin, after.UpstreamBalanceCooldownMin)
	}
	// 既有告警冷却读的是自己的环境变量，不应被 Incident 的变量影响。
	if after.CooldownMin != 30 {
		t.Errorf("既有告警错误类冷却默认值已非 30: %d", after.CooldownMin)
	}
}
