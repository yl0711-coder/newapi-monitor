package monitor

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

func TestChannelManagementPageIncludesBusinessDateShortcuts(t *testing.T) {
	for _, shortcut := range []string{
		`data-cm-preset="today">今天`,
		`data-cm-preset="yesterday">昨天`,
		`data-cm-preset="week">本周`,
	} {
		if !strings.Contains(pageHTML, shortcut) {
			t.Fatalf("渠道管理页缺少日期快捷项 %q", shortcut)
		}
	}
	if strings.Contains(portalHTML, "data-cm-preset") {
		t.Fatal("Usage Portal 不应接入渠道管理日期快捷项")
	}
}

func TestNormalizeChannelBaseDomain(t *testing.T) {
	tests := map[string]string{
		"https://temp.last-api.ai/v1":                   "last-api.ai",
		"https://www.last-api.ai":                       "last-api.ai",
		"api.service.example.co.uk/v1/chat/completions": "example.co.uk",
		"HTTP://API.EXAMPLE.COM.:8443/v1":               "example.com",
		"http://127.0.0.1:3000/v1":                      "127.0.0.1",
		"http://localhost:3000":                         "localhost",
		"":                                              "",
		"://broken":                                     "",
	}
	for raw, want := range tests {
		if got := normalizeChannelBaseDomain(raw); got != want {
			t.Errorf("normalizeChannelBaseDomain(%q)=%q, want %q", raw, got, want)
		}
	}
}

func TestNormalizeChannelBaseHost(t *testing.T) {
	tests := map[string]string{
		"https://temp.last-api.ai/v1?token=hidden": "temp.last-api.ai",
		"HTTP://API.EXAMPLE.COM.:8443/v1":          "api.example.com",
		"http://127.0.0.1:3000/v1":                 "127.0.0.1",
		"https://user:pass@example.com/private":    "example.com",
		"://broken":                                "",
	}
	for raw, want := range tests {
		if got := normalizeChannelBaseHost(raw); got != want {
			t.Errorf("normalizeChannelBaseHost(%q)=%q, want %q", raw, got, want)
		}
	}
}

func TestBuildChannelManagementReportGroupsDomainVendorChannelAndServiceGroup(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	snaps := []ChannelSnap{
		{ID: 1, Name: "last-openai", Type: 1, Vendor: "OpenAI", BaseDomain: "last-api.ai", BaseHost: "temp.last-api.ai", Status: 1, Groups: "codex-1.4x,codex-1.2x", Models: "gpt-5.6-sol", UpdatedAt: day + 60},
		{ID: 2, Name: "last-anthropic", Type: 14, Vendor: "Anthropic", BaseDomain: "last-api.ai", BaseHost: "www.last-api.ai", Status: 1, Groups: "codex-1.4x,claude-0.5x", Models: "gpt-5.6-sol,claude-sonnet", UpdatedAt: day + 60},
		{ID: 3, Name: "other", Type: 1, Vendor: "OpenAI", BaseDomain: "other.ai", Status: 2, Groups: "other", Models: "gpt-5.4", UpdatedAt: day + 60},
		{ID: 4, Name: "default-url", Type: 1, Vendor: "OpenAI", Status: 1, Groups: "default", Models: "gpt-5.4", UpdatedAt: day + 60},
	}
	if err := m.storeDB.Create(&snaps).Error; err != nil {
		t.Fatal(err)
	}
	rows := []StabilityHourSample{
		{HourTs: day, ChannelID: 1, ModelName: "gpt-5.6-sol", Grp: "codex-1.4x", Success: 10, Tokens: 1000, Quota: 500000},
		{HourTs: day, ChannelID: 2, ModelName: "gpt-5.6-sol", Grp: "codex-1.4x", Success: 5, Tokens: 500, Quota: 250000},
		{HourTs: day, ChannelID: 2, ModelName: "claude-sonnet", Grp: "claude-0.5x", Anomaly: 2, Tokens: 100, Quota: 50000},
		{HourTs: day, ChannelID: 99, ModelName: "removed-model", Grp: "legacy", Failed: 3},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildChannelManagementReport(context.Background(), stabilityScope{FromTs: day, ToTs: day + 86400}, day+7200)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.ConfiguredDomains != 2 || report.Summary.CurrentChannels != 4 || report.Summary.EnabledChannels != 3 || report.Summary.Unconfigured != 1 || report.Summary.HistoricalChannels != 1 {
		t.Fatalf("summary=%+v", report.Summary)
	}
	if report.Summary.Usage.Requests != 20 || report.Summary.Usage.Tokens != 1600 || math.Abs(report.Summary.Usage.CostUSD-1.6) > 1e-9 {
		t.Fatalf("usage=%+v", report.Summary.Usage)
	}

	var last *ChannelManagementDomain
	var hasUnconfigured, hasHistorical bool
	for i := range report.Domains {
		switch report.Domains[i].Domain {
		case "last-api.ai":
			last = &report.Domains[i]
		case "未配置主地址":
			hasUnconfigured = true
		case "历史未知渠道":
			hasHistorical = true
		}
	}
	if last == nil || !hasUnconfigured || !hasHistorical {
		t.Fatalf("domains=%+v", report.Domains)
	}
	if last.Usage.Requests != 17 || len(last.Vendors) != 2 || len(last.Groups) != 2 {
		t.Fatalf("last domain=%+v", last)
	}
	var openAI *ChannelManagementVendor
	for i := range last.Vendors {
		if last.Vendors[i].Name == "OpenAI" {
			openAI = &last.Vendors[i]
		}
	}
	if openAI == nil || len(openAI.Channels) != 1 || openAI.Channels[0].ID != 1 || len(openAI.Channels[0].Groups) != 1 {
		t.Fatalf("OpenAI vendor=%+v", openAI)
	}
	if openAI.Channels[0].Host != "temp.last-api.ai" {
		t.Fatalf("channel host=%q", openAI.Channels[0].Host)
	}
	if report.Meta.DataUntil != day+3600 || report.Meta.LatestDataUntil != day+3600 || report.Meta.ChannelConfigUpdatedAt != day+60 {
		t.Fatalf("meta=%+v", report.Meta)
	}
}

func TestChannelManagementReportRejectsPartialDimensionResult(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	rows := make([]StabilityHourSample, 0, maxChannelManagementRows+1)
	for i := 0; i <= maxChannelManagementRows; i++ {
		rows = append(rows, StabilityHourSample{HourTs: day, ChannelID: i + 1, ModelName: "m", Grp: "g", Success: 1})
	}
	if err := m.storeDB.CreateInBatches(rows, 200).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.buildChannelManagementReport(context.Background(), stabilityScope{FromTs: day, ToTs: day + 3600}, day+3600); err == nil {
		t.Fatal("维度超限时应拒绝返回部分结果")
	}
}

func TestChannelManagementFreshnessSeparatesHistoricalRangeFromLatestData(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := time.Date(2026, 8, 4, 0, 0, 0, 0, cstLocation).Unix()
	rows := []StabilityHourSample{
		{HourTs: day, ChannelID: 1, ModelName: "m", Grp: "g", Success: 1},
		{HourTs: day + 2*86400, ChannelID: 1, ModelName: "m", Grp: "g", Success: 2},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildChannelManagementReport(context.Background(), stabilityScope{FromTs: day, ToTs: day + 86400}, day+3*86400)
	if err != nil {
		t.Fatal(err)
	}
	if report.Meta.DataUntil != day+3600 {
		t.Fatalf("selected range data_until=%d", report.Meta.DataUntil)
	}
	if report.Meta.LatestDataUntil != day+2*86400+3600 {
		t.Fatalf("latest_data_until=%d", report.Meta.LatestDataUntil)
	}
}

func TestChannelManagementReportSyncsRenameAndKeepsDeletedSnapshot(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	if err := m.replaceChannelSnapsAuthoritative([]ChannelSnap{{
		ID: 43, Name: "old-name", Vendor: "Anthropic", BaseDomain: "codeyu.shop",
		BaseHost: "api.codeyu.shop", Status: 1, Groups: "claude-0.5x", Models: "claude-sonnet", UpdatedAt: day,
	}}, day); err != nil {
		t.Fatal(err)
	}
	// 同一渠道改名时同步当前快照；这不是一条新的历史渠道。
	if err := m.replaceChannelSnapsAuthoritative([]ChannelSnap{{
		ID: 43, Name: "new-name", Vendor: "Anthropic", BaseDomain: "codeyu.shop",
		BaseHost: "api.codeyu.shop", Status: 1, Groups: "claude-0.5x", Models: "claude-sonnet", UpdatedAt: day + 60,
	}}, day+60); err != nil {
		t.Fatal(err)
	}
	// 渠道随后从 NewAPI 删除；Monitor 只打墓碑，不应丢失最后名称和归并主域名。
	if err := m.replaceChannelSnapsAuthoritative(nil, day+120); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{
		HourTs: day, ChannelID: 43, ModelName: "claude-sonnet", Grp: "claude-0.5x", Success: 4,
	}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildChannelManagementReport(context.Background(), stabilityScope{FromTs: day, ToTs: day + 86400}, day+3600)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.CurrentChannels != 0 || report.Summary.HistoricalChannels != 1 {
		t.Fatalf("summary=%+v", report.Summary)
	}
	var found *ChannelManagementChannel
	for _, domain := range report.Domains {
		if domain.Domain != "codeyu.shop" {
			continue
		}
		for _, vendor := range domain.Vendors {
			for i := range vendor.Channels {
				if vendor.Channels[i].ID == 43 {
					found = &vendor.Channels[i]
				}
			}
		}
	}
	if found == nil || found.Current || found.Name != "new-name" || found.Host != "api.codeyu.shop" || found.Usage.Requests != 4 {
		t.Fatalf("deleted channel did not retain its last visible snapshot: %+v", found)
	}
}
