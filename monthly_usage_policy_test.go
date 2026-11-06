package main

import (
	"context"
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlydraft"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestMonthlyUsageRegisteredPolicyRequiresExplicitPreparedComposition(t *testing.T) {
	base := map[string]string{"MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED": "true", "MONITOR_PORTAL_MONTHLY_GENERATION_POLICY": "usage-registered", "MONITOR_PORTAL_MONTHLY_GENERATION_MODE": "automatic-series", "MONITOR_PORTAL_MONTHLY_GENERATION_SCOPE_SOURCE": "scope", "MONITOR_PORTAL_MONTHLY_GENERATION_DISPLAY_SERIES_INDEX": "ranges.json", "MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_CONFIG": "captured.json", "MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_SHA256": strings.Repeat("a", 64)}
	settings := monitor.Settings{PortalAddr: "127.0.0.1:8091", PortalMonthlyStatementEnabled: true, StorePath: "/private/tmp/policy.sqlite"}
	for _, scenario := range []string{"valid", "disabled", "unknown_policy", "mixed_targets", "draft", "automatic", "missing_capture", "missing_pin", "inbox", "snapshot"} {
		t.Run(scenario, func(t *testing.T) {
			env := map[string]string{}
			for k, v := range base {
				env[k] = v
			}
			s := settings
			switch scenario {
			case "disabled":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED"] = "false"
				env["MONITOR_PORTAL_MONTHLY_GENERATION_POLICY"] = "unknown"
			case "unknown_policy":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_POLICY"] = "all"
			case "mixed_targets":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_TARGETS"] = "1:2026-09"
			case "draft", "automatic":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_MODE"] = scenario
				env["MONITOR_PORTAL_MONTHLY_GENERATION_DISPLAY_SERIES_INDEX"] = ""
			case "missing_capture":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_CONFIG"] = ""
				env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_SHA256"] = ""
			case "missing_pin":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_SHA256"] = ""
			case "inbox":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_INBOX"] = "inbox.json"
			case "snapshot":
				s.LocalSnapshotOnly = true
			}
			cfg, err := monthlyGenerationConfig(func(k string) string { return env[k] }, s)
			if scenario == "valid" {
				if err != nil || !cfg.Rollout.Enabled || cfg.Rollout.Policy != monthlydraft.GenerationPolicyUsageRegistered || len(cfg.Rollout.Targets) != 0 {
					t.Fatal("explicit eligibility config rejected", cfg, err)
				}
			} else if scenario == "disabled" {
				if err != nil || cfg.Rollout.Enabled || cfg.Rollout.Policy != "" {
					t.Fatal("disabled parsed stale policy", err)
				}
			} else if err == nil {
				t.Fatal("unsafe/ambiguous registered policy accepted")
			}
		})
	}
	if d, err := openMonthlyGeneration(context.Background(), monthlyServiceConfig{DeploymentConfig: monthlydraft.DeploymentConfig{Rollout: monthlydraft.GenerationRollout{Enabled: true, Policy: monthlydraft.GenerationPolicyUsageRegistered}}, mode: "draft"}, nil); err == nil || d != nil {
		t.Fatal("direct opener bypassed captured eligibility requirement", err)
	}
}
