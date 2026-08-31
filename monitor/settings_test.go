package monitor

import "testing"

func TestLoadSettingsSourceLifecycleDefaults(t *testing.T) {
	t.Setenv("MONITOR_SOURCE_WORKER_ENABLED", "")
	t.Setenv("MONITOR_SOURCE_LEASE_REQUIRED", "")
	t.Setenv("MONITOR_SOURCE_LEASE_NAME", "")
	s := LoadSettings()
	if !s.SourceWorkerEnabled || !s.SourceLeaseRequired || s.SourceLeaseName != "newapi-monitor-source-worker-v1" {
		t.Fatalf("unsafe source lifecycle defaults: worker=%v lease=%v name=%q",
			s.SourceWorkerEnabled, s.SourceLeaseRequired, s.SourceLeaseName)
	}
	if !s.sourceLifecycleConfigured {
		t.Fatal("LoadSettings must distinguish production defaults from legacy Settings{} tests")
	}
	legacy := Settings{}
	if !legacy.sourceWorkerIsEnabled() || legacy.sourceLeaseIsRequired() {
		t.Fatalf("legacy Settings{} compatibility changed: worker=%v lease=%v",
			legacy.sourceWorkerIsEnabled(), legacy.sourceLeaseIsRequired())
	}
}

func TestLoadSettingsUpstreamErrorLogDefaultsFailClosed(t *testing.T) {
	t.Setenv("MONITOR_UPSTREAM_ERRORLOG_SYNC_ENABLED", "")
	t.Setenv("MONITOR_UPSTREAM_ERRORLOG_DOMAINS", "")
	defaults := LoadSettings()
	if defaults.UpstreamErrorLogSyncEnabled || len(defaults.UpstreamErrorLogDomains) != 0 {
		t.Fatalf("error-log collection must default to disabled with an empty allowlist: %+v", defaults.UpstreamErrorLogDomains)
	}
	t.Setenv("MONITOR_UPSTREAM_ERRORLOG_SYNC_ENABLED", "true")
	t.Setenv("MONITOR_UPSTREAM_ERRORLOG_DOMAINS", " a.example, b.example, a.example ")
	configured := LoadSettings()
	if !configured.UpstreamErrorLogSyncEnabled || len(configured.UpstreamErrorLogDomains) != 3 {
		t.Fatalf("explicit error-log settings were not loaded: enabled=%v domains=%v",
			configured.UpstreamErrorLogSyncEnabled, configured.UpstreamErrorLogDomains)
	}
}

func TestLocalAuthBypassIsExplicitAndFailsClosed(t *testing.T) {
	t.Setenv("MONITOR_LOCAL_AUTH_BYPASS", "")
	if LoadSettings().LocalAuthBypass {
		t.Fatal("本地免登录必须默认关闭")
	}
	t.Setenv("MONITOR_LOCAL_AUTH_BYPASS", "true")
	if !LoadSettings().LocalAuthBypass {
		t.Fatal("显式本地免登录开关未生效")
	}

	base := Settings{
		LocalAuthBypass: true, LocalSnapshotOnly: true,
		AlertsDisabled: true,
	}
	if err := validateLocalAuthBypassSettings(base); err != nil {
		t.Fatalf("完全离线快照配置应允许免登录: %v", err)
	}
	invalid := []Settings{
		{LocalAuthBypass: true, AlertsDisabled: true},
		{LocalAuthBypass: true, LocalSnapshotOnly: true, ProdDSN: "production", AlertsDisabled: true},
		{LocalAuthBypass: true, LocalSnapshotOnly: true, NewAPIBaseURL: "https://example.com", AlertsDisabled: true},
		{LocalAuthBypass: true, LocalSnapshotOnly: true, SourceWorkerEnabled: true, AlertsDisabled: true},
		{LocalAuthBypass: true, LocalSnapshotOnly: true, SourceLeaseRequired: true, AlertsDisabled: true},
		{LocalAuthBypass: true, LocalSnapshotOnly: true, UpstreamSyncEnabled: true, AlertsDisabled: true},
		{LocalAuthBypass: true, LocalSnapshotOnly: true, UpstreamUsageSyncEnabled: true, AlertsDisabled: true},
		{LocalAuthBypass: true, LocalSnapshotOnly: true, UpstreamPricingLedgerEnabled: true, AlertsDisabled: true},
		{LocalAuthBypass: true, LocalSnapshotOnly: true, ChannelCostClosureEnabled: true, AlertsDisabled: true},
		{LocalAuthBypass: true, LocalSnapshotOnly: true, NginxEnabled: true, AlertsDisabled: true},
		{LocalAuthBypass: true, LocalSnapshotOnly: true, InfraEnabled: true, AlertsDisabled: true},
		{LocalAuthBypass: true, LocalSnapshotOnly: true, HeartbeatURL: "https://example.com"},
		{LocalAuthBypass: true, LocalSnapshotOnly: true},
	}
	for i, cfg := range invalid {
		if err := validateLocalAuthBypassSettings(cfg); err == nil {
			t.Fatalf("危险的本地免登录配置[%d]应被拒绝", i)
		}
	}
}

func TestLoadSettingsChannelCostClosureDefaultsToFailClosed(t *testing.T) {
	t.Setenv("MONITOR_CHANNEL_COST_CLOSURE_ENABLED", "")
	t.Setenv("MONITOR_CHANNEL_COST_CLOSURE_DOMAINS", "")
	t.Setenv("MONITOR_CHANNEL_COST_HMAC_KEY", "")
	t.Setenv("MONITOR_CHANNEL_COST_HMAC_KEY_ID", "")
	t.Setenv("MONITOR_CHANNEL_ECONOMICS_REPORT_ENABLED", "")
	defaults := LoadSettings()
	if defaults.ChannelCostClosureEnabled || defaults.ChannelEconomicsReportEnabled {
		t.Fatal("channel cost closure and its report must remain disabled by default")
	}

	t.Setenv("MONITOR_CHANNEL_COST_CLOSURE_ENABLED", "true")
	t.Setenv("MONITOR_CHANNEL_COST_CLOSURE_DOMAINS", "4sapi.com")
	t.Setenv("MONITOR_CHANNEL_COST_HMAC_KEY", "0123456789abcdef0123456789abcdef")
	t.Setenv("MONITOR_CHANNEL_COST_HMAC_KEY_ID", "cost-source-2026-08")
	s := LoadSettings()
	if !s.ChannelCostClosureEnabled || len(s.ChannelCostClosureDomains) != 1 || s.ChannelCostHMACKeyID != "cost-source-2026-08" {
		t.Fatalf("channel cost settings not loaded: %+v", s.ChannelCostClosureDomains)
	}
}

func TestValidateChannelCostClosureSettings(t *testing.T) {
	valid := Settings{
		UpstreamUsageSyncEnabled: true, UpstreamPricingLedgerEnabled: true,
		UpstreamPricingLedgerDomains: []string{"4sapi.com", "other.example"},
		ChannelCostClosureEnabled:    true, ChannelCostClosureDomains: []string{"4sapi.com"},
		ChannelCostHMACKey: "0123456789abcdef0123456789abcdef", ChannelCostHMACKeyID: "cost-source-v1",
		ChannelEconomicsReportEnabled: true,
	}
	if err := validateChannelCostClosureSettings(valid); err != nil {
		t.Fatal(err)
	}
	invalid := []Settings{
		{ChannelEconomicsReportEnabled: true},
		{ChannelCostClosureEnabled: true},
		{ChannelCostClosureEnabled: true, UpstreamUsageSyncEnabled: true},
		{ChannelCostClosureEnabled: true, UpstreamUsageSyncEnabled: true, UpstreamPricingLedgerEnabled: true, UpstreamPricingLedgerDomains: []string{"4sapi.com"}},
		{ChannelCostClosureEnabled: true, UpstreamUsageSyncEnabled: true, UpstreamPricingLedgerEnabled: true, UpstreamPricingLedgerDomains: []string{"4sapi.com"}, ChannelCostClosureDomains: []string{"other.example"}, ChannelCostHMACKey: "0123456789abcdef0123456789abcdef", ChannelCostHMACKeyID: "v1"},
		{ChannelCostClosureEnabled: true, UpstreamUsageSyncEnabled: true, UpstreamPricingLedgerEnabled: true, UpstreamPricingLedgerDomains: []string{"4sapi.com"}, ChannelCostClosureDomains: []string{"4sapi.com"}, ChannelCostHMACKey: "too-short", ChannelCostHMACKeyID: "v1"},
		{ChannelCostClosureEnabled: true, UpstreamUsageSyncEnabled: true, UpstreamPricingLedgerEnabled: true, UpstreamPricingLedgerDomains: []string{"4sapi.com"}, ChannelCostClosureDomains: []string{"4sapi.com"}, ChannelCostHMACKey: "0123456789abcdef0123456789abcdef"},
		{ChannelCostClosureEnabled: true, UpstreamUsageSyncEnabled: true, UpstreamPricingLedgerEnabled: true, UpstreamPricingLedgerDomains: []string{"4sapi.com"}, ChannelCostClosureDomains: []string{"4sapi.com"}, ChannelCostHMACKey: "0123456789abcdef0123456789abcdef", ChannelCostHMACKeyID: "v1", LocalSnapshotOnly: true},
		{ChannelCostClosureEnabled: true, UpstreamUsageSyncEnabled: true, UpstreamPricingLedgerEnabled: true, UpstreamPricingLedgerDomains: []string{"4sapi.com"}, ChannelCostClosureDomains: []string{"4sapi.com"}, ChannelCostHMACKey: "0123456789abcdef0123456789abcdef", ChannelCostHMACKeyID: "bad key id"},
		{ChannelCostClosureEnabled: true, UpstreamUsageSyncEnabled: true, UpstreamPricingLedgerEnabled: true, UpstreamPricingLedgerDomains: []string{"4sapi.com"}, ChannelCostClosureDomains: []string{"4sapi.com"}, ChannelCostHMACKey: "0123456789abcdef0123456789abcdef", ChannelCostHMACKeyID: "v1", SessionSecret: "0123456789abcdef0123456789abcdef"},
	}
	for i, cfg := range invalid {
		if err := validateChannelCostClosureSettings(cfg); err == nil {
			t.Fatalf("invalid channel cost settings[%d] accepted", i)
		}
	}
}

func TestLoadSettingsUpstreamSyncEnabled(t *testing.T) {
	t.Setenv("MONITOR_UPSTREAM_SYNC_ENABLED", "")
	if !LoadSettings().UpstreamSyncEnabled {
		t.Fatal("upstream polling must remain enabled by default for existing deployments")
	}

	t.Setenv("MONITOR_UPSTREAM_SYNC_ENABLED", "false")
	if LoadSettings().UpstreamSyncEnabled {
		t.Fatal("explicitly disabling upstream polling must be honored")
	}
}

func TestLoadSettingsUpstreamUsageSyncDefaultsToGrayOff(t *testing.T) {
	t.Setenv("MONITOR_UPSTREAM_USAGE_SYNC_ENABLED", "")
	if LoadSettings().UpstreamUsageSyncEnabled {
		t.Fatal("new upstream usage polling must remain gray-off unless explicitly enabled")
	}

	t.Setenv("MONITOR_UPSTREAM_USAGE_SYNC_ENABLED", "true")
	if !LoadSettings().UpstreamUsageSyncEnabled {
		t.Fatal("explicitly enabling upstream usage polling must be honored")
	}
}

func TestLoadSettingsUpstreamPricingLedgerDefaultsToFailClosed(t *testing.T) {
	t.Setenv("MONITOR_UPSTREAM_PRICING_LEDGER_ENABLED", "")
	t.Setenv("MONITOR_UPSTREAM_PRICING_LEDGER_DOMAINS", "")
	if LoadSettings().UpstreamPricingLedgerEnabled {
		t.Fatal("pricing ledger must remain disabled by default")
	}

	t.Setenv("MONITOR_UPSTREAM_PRICING_LEDGER_ENABLED", "true")
	t.Setenv("MONITOR_UPSTREAM_PRICING_LEDGER_DOMAINS", "4sapi.com")
	t.Setenv("MONITOR_UPSTREAM_PRICING_BACKFILL_HOURS_PER_RUN", "1")
	s := LoadSettings()
	if !s.UpstreamPricingLedgerEnabled || len(s.UpstreamPricingLedgerDomains) != 1 || s.UpstreamPricingLedgerDomains[0] != "4sapi.com" {
		t.Fatalf("pricing ledger settings=%+v", s.UpstreamPricingLedgerDomains)
	}
}

func TestValidateUpstreamPricingLedgerSettings(t *testing.T) {
	valid := Settings{
		UpstreamPricingLedgerEnabled: true, UpstreamUsageSyncEnabled: true,
		UpstreamPricingLedgerDomains: []string{"4sapi.com"}, UpstreamPricingBackfillHoursPerRun: 1,
	}
	if err := validateUpstreamPricingLedgerSettings(valid); err != nil {
		t.Fatal(err)
	}
	invalid := []Settings{
		{UpstreamPricingLedgerEnabled: true, UpstreamPricingLedgerDomains: []string{"4sapi.com"}, UpstreamPricingBackfillHoursPerRun: 1},
		{UpstreamPricingLedgerEnabled: true, UpstreamUsageSyncEnabled: true, UpstreamPricingBackfillHoursPerRun: 1},
		{UpstreamPricingLedgerEnabled: true, UpstreamUsageSyncEnabled: true, UpstreamPricingLedgerDomains: []string{"4sapi.com"}},
		{UpstreamPricingLedgerEnabled: true, UpstreamUsageSyncEnabled: true, UpstreamPricingLedgerDomains: []string{"4sapi.com"}, UpstreamPricingBackfillHoursPerRun: 7},
		{UpstreamPricingLedgerEnabled: true, UpstreamUsageSyncEnabled: true, UpstreamPricingLedgerDomains: []string{"4sapi.com"}, UpstreamPricingBackfillHoursPerRun: 1, LocalSnapshotOnly: true},
	}
	for i, cfg := range invalid {
		if err := validateUpstreamPricingLedgerSettings(cfg); err == nil {
			t.Fatalf("invalid pricing ledger settings[%d] accepted", i)
		}
	}
}

func TestLoadSettingsInfraSnapshotReadOnly(t *testing.T) {
	t.Setenv("MONITOR_INFRA_ENABLED", "false")
	t.Setenv("MONITOR_INFRA_SNAPSHOT_READ_ONLY", "true")
	s := LoadSettings()
	if s.InfraEnabled {
		t.Fatal("snapshot-only mode must not enable active infra probes")
	}
	if !s.InfraSnapshotReadOnly {
		t.Fatal("snapshot-only mode must be loaded explicitly")
	}
	m := &Monitor{cfg: s}
	if !m.InfraEnabled() {
		t.Fatal("stored infra snapshots must remain readable in local acceptance")
	}
}

func TestInfraSnapshotReadOnlyIsOffByDefault(t *testing.T) {
	t.Setenv("MONITOR_INFRA_ENABLED", "false")
	t.Setenv("MONITOR_INFRA_SNAPSHOT_READ_ONLY", "false")
	s := LoadSettings()
	if (&Monitor{cfg: s}).InfraEnabled() {
		t.Fatal("infra page must remain disabled when neither active nor snapshot mode is enabled")
	}
}

func TestLoadSettingsUsageFactsStorePath(t *testing.T) {
	t.Setenv("MONITOR_USAGE_FACTS_STORE_PATH", "/data/usage-facts.db")
	if got := LoadSettings().UsageFactsStorePath; got != "/data/usage-facts.db" {
		t.Fatalf("usage facts store path=%q", got)
	}
}

func TestLoadSettingsUsageFactsRawPageImportDefaultsToBoundedPipeline(t *testing.T) {
	t.Setenv("MONITOR_USAGE_FACTS_RAW_PAGE_IMPORT_ENABLED", "")
	if !LoadSettings().UsageFactsRawPageImportEnabled {
		t.Fatal("bounded raw-page import must be the production default")
	}
	t.Setenv("MONITOR_USAGE_FACTS_RAW_PAGE_IMPORT_ENABLED", "false")
	if LoadSettings().UsageFactsRawPageImportEnabled {
		t.Fatal("explicit emergency compatibility rollback must still be honored")
	}
}

func TestLoadSettingsStabilitySourceProtectionDefaults(t *testing.T) {
	t.Setenv("MONITOR_BACKGROUND_SOURCE_MIN_START_INTERVAL_MS", "")
	t.Setenv("MONITOR_STABILITY_BACKFILL_SERVER_MAX_EXECUTION_MS", "")
	t.Setenv("MONITOR_STABILITY_BACKFILL_SOURCE_DUTY_PERCENT", "")
	t.Setenv("MONITOR_STABILITY_CLASSIFICATION_MIGRATION_ENABLED", "")
	s := LoadSettings()
	if s.BackgroundSourceMinStartIntervalMS != 2000 {
		t.Fatalf("background source spacing=%d want=2000ms", s.BackgroundSourceMinStartIntervalMS)
	}
	if s.StabilityBackfillSourceDutyPercent != 20 {
		t.Fatalf("stability source duty=%d want=20%%", s.StabilityBackfillSourceDutyPercent)
	}
	if s.StabilityBackfillServerMaxExecutionMS != 8000 {
		t.Fatalf("stability server max execution=%d want=8000ms", s.StabilityBackfillServerMaxExecutionMS)
	}
	if s.StabilityClassificationMigrationEnabled {
		t.Fatal("classification migration must be explicit and off by default")
	}

	t.Setenv("MONITOR_BACKGROUND_SOURCE_MIN_START_INTERVAL_MS", "3500")
	t.Setenv("MONITOR_STABILITY_BACKFILL_SERVER_MAX_EXECUTION_MS", "6000")
	t.Setenv("MONITOR_STABILITY_BACKFILL_SOURCE_DUTY_PERCENT", "15")
	t.Setenv("MONITOR_STABILITY_CLASSIFICATION_MIGRATION_ENABLED", "true")
	s = LoadSettings()
	if s.BackgroundSourceMinStartIntervalMS != 3500 || s.StabilityBackfillServerMaxExecutionMS != 6000 || s.StabilityBackfillSourceDutyPercent != 15 || !s.StabilityClassificationMigrationEnabled {
		t.Fatalf("explicit stability source settings were not honored: %+v", s)
	}
}

func TestValidateUsageFactsSettingsRejectsSilentSourceFallback(t *testing.T) {
	tests := []struct {
		name string
		cfg  Settings
	}{
		{
			name: "production read without collector",
			cfg:  Settings{UsageFactsReadEnabled: true},
		},
		{
			name: "offline read without explicit facts guard",
			cfg:  Settings{LocalSnapshotOnly: true, UsageFactsReadEnabled: true},
		},
		{
			name: "offline-only guard in production",
			cfg:  Settings{UsageFactsLocalReadOnly: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateUsageFactsSettings(tt.cfg); err == nil {
				t.Fatal("危险开关组合必须在启动前被拒绝")
			}
		})
	}

	valid := []Settings{
		{},
		{UsageFactsEnabled: true},
		{UsageFactsEnabled: true, UsageFactsReadEnabled: true},
		{LocalSnapshotOnly: true, UsageFactsLocalReadOnly: true, UsageFactsReadEnabled: true},
		{LocalSnapshotOnly: true, UsageFactsLocalReadOnly: true, UsageFactsReadEnabled: true,
			UsageFactsFullHistoryEnabled: true, UsageFactsHistorySourceMode: "complete", UsageFactsHistorySourceEpoch: "restore-v1"},
	}
	for i, cfg := range valid {
		if err := validateUsageFactsSettings(cfg); err != nil {
			t.Fatalf("合法配置[%d]被拒绝: %v", i, err)
		}
	}
}

func TestUsageFactsFullHistorySnapshotModeNeverEnablesWorker(t *testing.T) {
	cfg := Settings{
		LocalSnapshotOnly: true, UsageFactsLocalReadOnly: true, UsageFactsReadEnabled: true,
		UsageFactsFullHistoryEnabled: true, UsageFactsHistorySourceMode: "complete", UsageFactsHistorySourceEpoch: "restore-v1",
	}
	if err := validateUsageFactsSettings(cfg); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{cfg: cfg}
	if !m.usageFactsFullHistoryMode() {
		t.Fatal("restored snapshot lost its full-history read semantics")
	}
	if m.usageFactsFullHistoryEnabled() {
		t.Fatal("restored snapshot must not enable full-history source/mutation workers")
	}
}

func TestClassificationMigrationRequiresOnlineReadDisabledFullHistoryWindow(t *testing.T) {
	base := Settings{
		UsageFactsEnabled: true, UsageFactsFullHistoryEnabled: true,
		UsageFactsHistorySourceMode: "complete", UsageFactsHistorySourceEpoch: "source-v1",
		UsageFactsClassificationMigrationEnabled: true,
	}
	if err := validateUsageFactsSettings(base); err != nil {
		t.Fatalf("valid maintenance window rejected: %v", err)
	}
	readOn := base
	readOn.UsageFactsReadEnabled = true
	if err := validateUsageFactsSettings(readOn); err == nil {
		t.Fatal("classification migration must reject a live facts read surface")
	}
	snapshot := base
	snapshot.LocalSnapshotOnly = true
	snapshot.UsageFactsLocalReadOnly = true
	if err := validateUsageFactsSettings(snapshot); err == nil {
		t.Fatal("classification migration must never mutate a restored snapshot")
	}
}
