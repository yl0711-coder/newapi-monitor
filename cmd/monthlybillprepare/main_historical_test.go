package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func historicalCLIInput(t *testing.T) (string, string, string, []string) {
	t.Helper()
	source, input, output := preparationCLIInput(t)
	var data monitor.MonthlyPreparationInput
	if err := json.Unmarshal(reviewCLIBytes(t, input), &data); err != nil {
		t.Fatal(err)
	}
	a := &data.Accounts[0]
	a.Baseline.TimeBasis, a.Baseline.CutoffSource = "source_log_cutoff", a.Baseline.Source
	a.Snapshots[0].TimeBasis, a.Snapshots[0].CutoffSource = "source_log_cutoff", a.Snapshots[0].Source
	a.Spans[0].ConsumeQuota = 1000
	a.Snapshots[0].BalanceQuota, a.Snapshots[0].UsedQuota = -900, 900
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return source, input, output, []string{"-input", input, "-out", output, "-historical-preview", "-history-input-sha256", reviewCLIDigest(raw), "-history-user", "1", "-history-anchor", "1782835200", "-history-month", "2026-06", "-history-gap-quota", "100"}
}

func TestMonthlyHistoricalCLIExclusiveLocalPreview(t *testing.T) {
	source, input, output, args := historicalCLIInput(t)
	before, sourceBefore := reviewCLIBytes(t, input), reviewCLIBytes(t, source)
	now := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	var message bytes.Buffer
	if err := run(args, &message, now); err != nil {
		t.Fatal(err)
	}
	raw := reviewCLIBytes(t, output)
	var p monitor.MonthlyHistoricalSettlementPreview
	if err := json.Unmarshal(raw, &p); err != nil || p.Status != "proposal_requires_internal_review" || len(p.Months) != 1 || p.Months[0].Amounts.Consume != "0.002000" || p.Months[0].BalanceAdjustment != "0.000200" || !strings.Contains(p.SourceIntegrity, "sha256_verified") {
		t.Fatal(p, err)
	}
	if !strings.Contains(message.String(), "不可直接发布") {
		t.Fatal(message.String())
	}
	st, err := os.Stat(output)
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("permissions", err)
	}
	if err := run(args, &message, now); err == nil {
		t.Fatal("overwrote report")
	}
	if !bytes.Equal(before, reviewCLIBytes(t, input)) || !bytes.Equal(sourceBefore, reviewCLIBytes(t, source)) || !bytes.Equal(raw, reviewCLIBytes(t, output)) {
		t.Fatal("files changed")
	}
}

func TestMonthlyHistoricalCLIRejectsModeMixingAndSourceDrift(t *testing.T) {
	for _, kind := range []string{"source-drift", "bundle", "review", "missing-opt-in", "missing-hash"} {
		t.Run(kind, func(t *testing.T) {
			source, _, output, args := historicalCLIInput(t)
			switch kind {
			case "source-drift":
				if err := os.WriteFile(source, []byte("changed synthetic source"), 0600); err != nil {
					t.Fatal(err)
				}
			case "bundle":
				args = append(args, "-bundle-context", "/unused")
			case "review":
				args = append(args, "-review-request", "/unused")
			case "missing-opt-in":
				args[4] = "-historical-preview=false"
			case "missing-hash":
				args[6] = ""
			}
			if err := run(args, &bytes.Buffer{}, time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)); err == nil {
				t.Fatal("invalid preview accepted")
			}
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Fatal("failure left output", err)
			}
		})
	}
}
