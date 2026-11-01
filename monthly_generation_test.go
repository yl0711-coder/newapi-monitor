package main

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlydraft"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestMonthlyGenerationConfigDefaultOffAndExplicitRequirements(t *testing.T) {
	env := map[string]string{}
	get := func(k string) string { return env[k] }
	s := monitor.Settings{PortalAddr: "127.0.0.1:8091", PortalMonthlyStatementEnabled: true, StorePath: "/private/tmp/monitor.sqlite"}
	cfg, err := monthlyGenerationConfig(get, s)
	if err != nil || cfg.Rollout.Enabled || len(cfg.Rollout.Targets) != 0 {
		t.Fatal(cfg, err)
	}
	env["MONITOR_PORTAL_MONTHLY_GENERATION_TARGETS"] = "not parsed while disabled"
	cfg, err = monthlyGenerationConfig(get, s)
	if err != nil || cfg.ReceiverPath != "" {
		t.Fatal("disabled config used stale paths", err)
	}
	env["MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED"] = "yes"
	if _, err := monthlyGenerationConfig(get, s); err == nil {
		t.Fatal("ambiguous switch accepted")
	}
	env["MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED"] = "true"
	for _, raw := range []string{"", "*", "1", "01:2026-09", "-1:2026-09", "1:2026-09,"} {
		env["MONITOR_PORTAL_MONTHLY_GENERATION_TARGETS"] = raw
		if _, err := monthlyGenerationConfig(get, s); err == nil {
			t.Fatal("invalid targets", raw)
		}
	}
	env["MONITOR_PORTAL_MONTHLY_GENERATION_TARGETS"] = "1:2026-09, 2:2026-08"
	env["MONITOR_PORTAL_MONTHLY_GENERATION_RECEIVER"] = "/private/tmp/jobs.sqlite"
	env["MONITOR_PORTAL_MONTHLY_GENERATION_FUNDING"] = "/private/tmp/funding.sqlite"
	env["MONITOR_PORTAL_MONTHLY_GENERATION_EVIDENCE"] = "/private/tmp/evidence"
	cfg, err = monthlyGenerationConfig(get, s)
	if err != nil || !cfg.Rollout.Enabled || len(cfg.Rollout.Targets) != 2 || cfg.Rollout.Targets[1].GroupID != 2 || cfg.AuthorityPath != s.StorePath || cfg.ReceiverPath != "/private/tmp/jobs.sqlite" {
		t.Fatal(cfg, err)
	}
	for _, bad := range []monitor.Settings{{PortalMonthlyStatementEnabled: true}, {PortalAddr: s.PortalAddr}} {
		if _, err := monthlyGenerationConfig(get, bad); err == nil {
			t.Fatal("missing portal dependency accepted")
		}
	}
}

func TestMonthlyGenerationExplicitAutomaticMode(t *testing.T) {
	s := monitor.Settings{PortalAddr: "127.0.0.1:8091", PortalMonthlyStatementEnabled: true, StorePath: "/private/tmp/monitor.sqlite"}
	env := map[string]string{"MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED": "true", "MONITOR_PORTAL_MONTHLY_GENERATION_TARGETS": "1:2026-09"}
	get := func(k string) string { return env[k] }
	for _, mode := range []string{"", "draft"} {
		env["MONITOR_PORTAL_MONTHLY_GENERATION_MODE"] = mode
		cfg, err := monthlyGenerationConfig(get, s)
		if err != nil || cfg.mode != "draft" || cfg.scopeSource != "" {
			t.Fatal("existing configuration stopped being draft-only", cfg, err)
		}
	}
	for _, mode := range []string{"", "draft", "auto", "Automatic", " automatic"} {
		env["MONITOR_PORTAL_MONTHLY_GENERATION_MODE"] = mode
		env["MONITOR_PORTAL_MONTHLY_GENERATION_SCOPE_SOURCE"] = "historical-scope.v1"
		if _, err := monthlyGenerationConfig(get, s); err == nil {
			t.Fatal("implicit or ambiguous automatic mode accepted", mode)
		}
	}
	env["MONITOR_PORTAL_MONTHLY_GENERATION_MODE"] = "automatic"
	for _, id := range []string{"", "../scope", "scope/file", "scope id", "令牌", strings.Repeat("x", 129)} {
		env["MONITOR_PORTAL_MONTHLY_GENERATION_SCOPE_SOURCE"] = id
		if _, err := monthlyGenerationConfig(get, s); err == nil {
			t.Fatal("invalid source ID", id)
		}
	}
	env["MONITOR_PORTAL_MONTHLY_GENERATION_SCOPE_SOURCE"] = "historical-scope.v1"
	cfg, err := monthlyGenerationConfig(get, s)
	if err != nil || cfg.mode != "automatic" || cfg.scopeSource != "historical-scope.v1" {
		t.Fatal(cfg, err)
	}
	if d, err := openMonthlyGeneration(context.Background(), cfg, nil); d != nil || err == nil {
		t.Fatal("missing service publisher accepted", err)
	}
	// Disabling ignores even stale automatic settings and touches no paths.
	env["MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED"] = "false"
	env["MONITOR_PORTAL_MONTHLY_GENERATION_MODE"] = "invalid"
	cfg, err = monthlyGenerationConfig(get, monitor.Settings{})
	if err != nil || cfg.mode != "" || cfg.scopeSource != "" {
		t.Fatal(cfg, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if d, err := openMonthlyGeneration(canceled, cfg, nil); d != nil || err != nil {
		t.Fatal("disabled startup used dependencies", err)
	}
	closeMonthlyGeneration(nil)
}

func TestMonthlyGenerationFixedOriginSeriesRequiresExplicitPrivateIndex(t *testing.T) {
	s := monitor.Settings{PortalAddr: "127.0.0.1:8091", PortalMonthlyStatementEnabled: true, StorePath: "/private/tmp/monitor.sqlite"}
	env := map[string]string{"MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED": "true", "MONITOR_PORTAL_MONTHLY_GENERATION_TARGETS": "1:2026-09", "MONITOR_PORTAL_MONTHLY_GENERATION_MODE": "automatic-series", "MONITOR_PORTAL_MONTHLY_GENERATION_SCOPE_SOURCE": "scope"}
	get := func(k string) string { return env[k] }
	for _, bad := range []string{"", "../ranges.json", "/tmp/ranges.json", ".ranges", "range space", strings.Repeat("x", 129)} {
		env["MONITOR_PORTAL_MONTHLY_GENERATION_DISPLAY_SERIES_INDEX"] = bad
		if _, err := monthlyGenerationConfig(get, s); err == nil {
			t.Fatal("unsafe private series index accepted", bad)
		}
	}
	env["MONITOR_PORTAL_MONTHLY_GENERATION_DISPLAY_SERIES_INDEX"] = "display-series.json"
	env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_INBOX"] = "prepared-index.json"
	cfg, err := monthlyGenerationConfig(get, s)
	if err != nil || cfg.mode != "automatic-series" || cfg.displaySeriesIndex != "display-series.json" || cfg.PreparationInbox != "prepared-index.json" {
		t.Fatal("explicit new mode not preserved", cfg, err)
	}
	if d, err := openMonthlyGeneration(context.Background(), cfg, nil); err == nil || d != nil {
		t.Fatal("new mode accepted missing publisher", err)
	}
	for _, mode := range []string{"draft", "automatic"} {
		env["MONITOR_PORTAL_MONTHLY_GENERATION_MODE"] = mode
		if _, err := monthlyGenerationConfig(get, s); err == nil {
			t.Fatal("new index silently enabled a legacy mode", mode)
		}
	}
	env["MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED"] = "false"
	cfg, err = monthlyGenerationConfig(get, monitor.Settings{})
	if err != nil || cfg.Rollout.Enabled || cfg.displaySeriesIndex != "" {
		t.Fatal("disabled mode inspected new range data", cfg, err)
	}
}

func TestMonthlyGenerationPreparationInboxRequiresAutomaticOptIn(t *testing.T) {
	s := monitor.Settings{PortalAddr: "127.0.0.1:8091", PortalMonthlyStatementEnabled: true, StorePath: "/private/tmp/monitor.sqlite"}
	env := map[string]string{
		"MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED":           "true",
		"MONITOR_PORTAL_MONTHLY_GENERATION_TARGETS":           "1:2026-09",
		"MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_INBOX": "prepared-index.json",
	}
	get := func(k string) string { return env[k] }
	if _, err := monthlyGenerationConfig(get, s); err == nil {
		t.Fatal("draft mode enabled automatic preparation")
	}
	env["MONITOR_PORTAL_MONTHLY_GENERATION_MODE"] = "automatic"
	env["MONITOR_PORTAL_MONTHLY_GENERATION_SCOPE_SOURCE"] = "scope.v1"
	cfg, err := monthlyGenerationConfig(get, s)
	if err != nil || cfg.PreparationInbox != "prepared-index.json" {
		t.Fatal("explicit inbox configuration lost", err)
	}
	env["MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED"] = "false"
	cfg, err = monthlyGenerationConfig(get, monitor.Settings{})
	if err != nil || cfg.PreparationInbox != "" {
		t.Fatal("disabled service touched stale inbox", err)
	}
}

func TestMonthlyGenerationCapturedPreparationExplicitAndDefaultOff(t *testing.T) {
	s := monitor.Settings{PortalAddr: "127.0.0.1:8091", PortalMonthlyStatementEnabled: true, StorePath: "/private/tmp/monitor.sqlite"}
	base := map[string]string{"MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED": "true", "MONITOR_PORTAL_MONTHLY_GENERATION_TARGETS": "1:2026-09",
		"MONITOR_PORTAL_MONTHLY_GENERATION_MODE": "automatic-series", "MONITOR_PORTAL_MONTHLY_GENERATION_SCOPE_SOURCE": "scope",
		"MONITOR_PORTAL_MONTHLY_GENERATION_DISPLAY_SERIES_INDEX": "series.json",
		"MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_CONFIG":   "captured-preparation.json",
		"MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_SHA256":   strings.Repeat("a", 64)}
	for _, scenario := range []string{"valid", "disabled", "draft", "automatic", "missing_pin", "missing_name", "unsafe_name", "uppercase_pin", "inbox", "snapshot"} {
		t.Run(scenario, func(t *testing.T) {
			env := map[string]string{}
			for k, v := range base {
				env[k] = v
			}
			settings := s
			switch scenario {
			case "disabled":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED"] = "false"
				env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_CONFIG"] = "../stale"
			case "draft", "automatic":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_MODE"] = scenario
				env["MONITOR_PORTAL_MONTHLY_GENERATION_DISPLAY_SERIES_INDEX"] = ""
			case "missing_pin":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_SHA256"] = ""
			case "missing_name":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_CONFIG"] = ""
			case "unsafe_name":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_CONFIG"] = "../preparation.json"
			case "uppercase_pin":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_SHA256"] = strings.Repeat("A", 64)
			case "inbox":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_INBOX"] = "old-inbox.json"
			case "snapshot":
				settings.LocalSnapshotOnly = true
			}
			cfg, err := monthlyGenerationConfig(func(k string) string { return env[k] }, settings)
			valid := scenario == "valid" || scenario == "disabled"
			if (err == nil) != valid {
				t.Fatal("wrong explicit startup policy", err)
			}
			if scenario == "valid" && cfg.preparationConfig != "captured-preparation.json" {
				t.Fatal("lost explicit configuration")
			}
			if scenario == "disabled" && (cfg.Rollout.Enabled || cfg.preparationConfig != "" || cfg.preparationSHA != "") {
				t.Fatal("disabled startup touched new preparation settings")
			}
		})
	}
}

// Real startup composition with empty synthetic databases: no listeners,
// collectors, customer credentials or environment-derived production settings.
func TestMonthlyGenerationServiceModeLifecycleAndFailedOpen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	s := monitor.Settings{StorePath: filepath.Join(dir, "monitor.sqlite"), SessionSecret: "synthetic-monthly-service-only", LocalSnapshotOnly: true,
		PortalAddr: "127.0.0.1:8091", PortalMonthlyStatementEnabled: true}
	m, err := monitor.New(s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	receiver := filepath.Join(dir, "receiver.sqlite")
	if _, err := monthlydraft.InitializeGenerationReceiverFile(ctx, receiver, true); err != nil {
		t.Fatal(err)
	}
	funding := filepath.Join(dir, "funding.sqlite")
	db, err := sql.Open("sqlite", funding)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := monthlysource.InitFundingPageStore(ctx, db); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED": "true", "MONITOR_PORTAL_MONTHLY_GENERATION_TARGETS": "1:2026-09",
		"MONITOR_PORTAL_MONTHLY_GENERATION_RECEIVER": receiver, "MONITOR_PORTAL_MONTHLY_GENERATION_FUNDING": funding,
		"MONITOR_PORTAL_MONTHLY_GENERATION_EVIDENCE": dir,
	}
	get := func(k string) string { return env[k] }
	for _, mode := range []string{"draft", "automatic", "draft"} {
		env["MONITOR_PORTAL_MONTHLY_GENERATION_MODE"] = mode
		env["MONITOR_PORTAL_MONTHLY_GENERATION_SCOPE_SOURCE"] = ""
		if mode == "automatic" {
			env["MONITOR_PORTAL_MONTHLY_GENERATION_SCOPE_SOURCE"] = "historical-scope.v1"
		}
		cfg, err := monthlyGenerationConfig(get, s)
		if err != nil {
			t.Fatal(err)
		}
		d, err := openMonthlyGeneration(ctx, cfg, m)
		if err != nil || d == nil {
			t.Fatal(mode, err)
		}
		// Opening alone must not admit work; Start remains explicit.
		if _, err := d.SubmitMonthly(ctx, 1, 1, "2026-09"); err == nil {
			t.Fatal("opener started admission")
		}
		if other, err := openMonthlyGeneration(ctx, cfg, m); other != nil || !errors.Is(err, monthlydraft.ErrGenerationBusy) {
			t.Fatal("service bypassed deployment lock", err)
		}
		if err := d.Start(ctx); err != nil {
			closeMonthlyGeneration(d)
			t.Fatal(err)
		}
		closeMonthlyGeneration(d)
		if err := d.Start(ctx); err == nil {
			t.Fatal("closed service restarted")
		}
		bad := cfg
		bad.ReceiverPath = filepath.Join(dir, "missing.sqlite")
		if other, err := openMonthlyGeneration(ctx, bad, m); other != nil || err == nil {
			t.Fatal("missing receiver accepted", err)
		}
		if _, err := os.Stat(bad.ReceiverPath); !os.IsNotExist(err) {
			t.Fatal("failed startup created receiver", err)
		}
	}
	// Failure after opening the base deployment must release its receiver lock.
	{
		index := `{"version":1,"ranges":[{"group_id":1,"source_identity":"` + strings.Repeat("a", 64) + `","from":"2026-09","through":"2026-09"}]}`
		if err := os.WriteFile(filepath.Join(dir, "startup-series.json"), []byte(index), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := monthlyGenerationConfig(get, s)
		if err != nil {
			t.Fatal(err)
		}
		cfg.mode, cfg.scopeSource, cfg.displaySeriesIndex = "automatic-series", "scope", "startup-series.json"
		cfg.preparationConfig, cfg.preparationSHA = "absent-preparation.json", strings.Repeat("a", 64)
		if other, err := openMonthlyGeneration(ctx, cfg, m); other != nil || err == nil {
			t.Fatal("missing preparation config started service", err)
		}
		cfg.preparationConfig, cfg.preparationSHA = "", ""
		recovered, err := openMonthlyGeneration(ctx, cfg, m)
		if err != nil {
			t.Fatal("failed preparation setup leaked base deployment resources", err)
		}
		closeMonthlyGeneration(recovered)
	}
	// Prove mode selection, not just successful construction: a draft service
	// does not require the automatic journal, but automatic startup must fail
	// without it and must never silently migrate or fall back to draft.
	rdb, err := sql.Open("sqlite", receiver)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	if _, err := rdb.ExecContext(ctx, "DROP TABLE portal_monthly_publication_control"); err != nil {
		t.Fatal(err)
	}
	env["MONITOR_PORTAL_MONTHLY_GENERATION_MODE"] = "automatic"
	env["MONITOR_PORTAL_MONTHLY_GENERATION_SCOPE_SOURCE"] = "historical-scope.v1"
	cfg, err := monthlyGenerationConfig(get, s)
	if err != nil {
		t.Fatal(err)
	}
	if d, err := openMonthlyGeneration(ctx, cfg, m); d != nil || err == nil {
		t.Fatal("automatic startup silently fell back or repaired schema", err)
	}
	var count int
	if err := rdb.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE name='portal_monthly_publication_control'").Scan(&count); err != nil || count != 0 {
		t.Fatal("startup initialized automatic schema", count, err)
	}
	cfg.mode, cfg.scopeSource = "draft", ""
	d, err := openMonthlyGeneration(ctx, cfg, m)
	if err != nil || d == nil {
		t.Fatal("failed automatic open leaked resources or broke draft fallback", err)
	}
	closeMonthlyGeneration(d)
}
