package monitor

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestFinanceGiftPreviewSettingsDefaultAndBounds(t *testing.T) {
	t.Setenv("MONITOR_FINANCE_GIFT_HANDOFF_PREVIEW_DIR", "")
	t.Setenv("MONITOR_FINANCE_GIFT_HANDOFF_PREVIEW_SHA256", "")
	t.Setenv("MONITOR_FINANCE_GIFT_HANDOFF_APPROVAL_ENABLED", "")
	t.Setenv("MONITOR_FINANCE_GIFT_HANDOFF_LOCAL_EXECUTION_ENABLED", "")
	s := LoadSettings()
	if s.FinanceGiftHandoffPreviewDir != "" || s.FinanceGiftHandoffPreviewSHA256 != "" || s.FinanceGiftHandoffApprovalEnabled || s.FinanceGiftHandoffLocalExecutionEnabled {
		t.Fatal("preview default enabled")
	}
	if err := validateFinanceGiftPreviewSettings(Settings{}); err != nil {
		t.Fatal(err)
	}
	valid := Settings{FinanceEnabled: true, FinanceFactsReadIsolationEnabled: true, UsageFactsHistorySourceEpoch: "v1", FinanceGiftHandoffPreviewDir: filepath.Join(t.TempDir(), "job"), FinanceGiftHandoffPreviewSHA256: strings.Repeat("a", 64)}
	if err := validateFinanceGiftPreviewSettings(valid); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Settings){
		func(s *Settings) { s.FinanceGiftHandoffPreviewDir = "" }, func(s *Settings) { s.FinanceGiftHandoffPreviewDir = "relative" },
		func(s *Settings) { s.FinanceGiftHandoffPreviewSHA256 = "" }, func(s *Settings) { s.FinanceGiftHandoffPreviewSHA256 = strings.Repeat("G", 64) },
		func(s *Settings) { s.FinanceEnabled = false }, func(s *Settings) { s.FinanceFactsReadIsolationEnabled = false }, func(s *Settings) { s.UsageFactsHistorySourceEpoch = "" },
	} {
		invalid := valid
		mutate(&invalid)
		if err := validateFinanceSettings(invalid); err == nil {
			t.Fatal("invalid preview accepted")
		}
	}
}

func TestFinanceGiftLocalExecutionSettings(t *testing.T) {
	valid := Settings{FinanceEnabled: true, FinanceFactsReadIsolationEnabled: true, UsageFactsHistorySourceEpoch: "test", FinanceGiftHandoffPreviewDir: filepath.Join(t.TempDir(), "job"), FinanceGiftHandoffPreviewSHA256: strings.Repeat("a", 64), FinanceGiftHandoffApprovalEnabled: true, FinanceGiftHandoffLocalExecutionEnabled: true, LocalSnapshotOnly: true, SessionSecret: "test", StorePath: "/private/main.db", UsageFactsStorePath: "/private/facts.db"}
	if err := validateFinanceGiftPreviewSettings(valid); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Settings){func(s *Settings) { s.LocalSnapshotOnly = false }, func(s *Settings) { s.FinanceGiftHandoffApprovalEnabled = false }, func(s *Settings) { s.ProdDSN = "test" }, func(s *Settings) { s.NewAPIBaseURL = "https://invalid.example" }} {
		s := valid
		change(&s)
		if validateFinanceGiftPreviewSettings(s) == nil {
			t.Fatal("unsafe local execution accepted")
		}
	}
}

func TestFinanceGiftApprovalSettings(t *testing.T) {
	valid := Settings{FinanceEnabled: true, FinanceFactsReadIsolationEnabled: true, UsageFactsHistorySourceEpoch: "v1",
		FinanceGiftHandoffPreviewDir: filepath.Join(t.TempDir(), "job"), FinanceGiftHandoffPreviewSHA256: strings.Repeat("a", 64),
		FinanceGiftHandoffApprovalEnabled: true, SessionSecret: "test-session", StorePath: "/private/main.db", UsageFactsStorePath: "/private/facts.db"}
	if err := validateFinanceGiftPreviewSettings(valid); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Settings){
		func(s *Settings) { s.FinanceGiftHandoffPreviewDir = "" }, func(s *Settings) { s.SessionSecret = "" },
		func(s *Settings) { s.LocalAuthBypass = true }, func(s *Settings) { s.StorePath = "" },
		func(s *Settings) { s.UsageFactsStorePath = s.StorePath }, func(s *Settings) { s.UsageFactsStorePath = "relative.db" },
	} {
		invalid := valid
		mutate(&invalid)
		if err := validateFinanceGiftPreviewSettings(invalid); err == nil {
			t.Fatal("unsafe approval config accepted")
		}
	}
}
