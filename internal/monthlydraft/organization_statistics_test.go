package monthlydraft

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestOrganizationHistoryStatisticsSourceIsRetained(t *testing.T) {
	a := monitor.MonthlyPreparationAccount{MonthStatistics: []monitor.MonthlyPreparationStatistics{{UserID: 1, Month: "2026-09", Requests: 0, Tokens: 0, Source: "independent-month-counts#original-zero-query"}}}
	refs, _ := organizationHistoryRefs([]monitor.MonthlyPreparationAccount{a}, AutomaticScope{})
	if !refs["independent-month-counts"] {
		t.Fatal("complete monthly counts lost their independent source")
	}
}

func TestOrganizationHistoryStatisticsThroughActualPreparationAndGeneration(t *testing.T) {
	f, cfg, r, _ := observationMaterialPreparationFixture(t)
	catalogue := useOrganizationHistoryFixture(t, f, cfg, &r)
	var rows []monitor.MonthlyPreparationStatistics
	for j := range catalogue.Accounts {
		a := &catalogue.Accounts[j]
		if a.GroupID != f.plan.Owner.GroupID {
			continue
		}
		a.MonthStatistics = []monitor.MonthlyPreparationStatistics{
			{UserID: a.UserID, Month: "2026-08", Requests: 99, Tokens: 999, Source: "independent-counts#original-august"},
			{UserID: a.UserID, Month: f.plan.Funding.Month, Requests: 1, Tokens: 37 + a.UserID, Source: "independent-counts#original-september"},
		}
		rows = append(rows, a.MonthStatistics...)
	}
	statsRaw := materialPreparationJSON(t, rows)
	statsPath := filepath.Join(f.root, "independent-month-counts.json")
	if err := os.WriteFile(statsPath, statsRaw, 0600); err != nil {
		t.Fatal(err)
	}
	catalogue.Sources = append(catalogue.Sources, monitor.MonthlyPreparationSource{ID: "independent-counts", Path: statsPath, SHA256: generationHash(statsRaw)})
	historyRaw := draftJSON(t, catalogue)
	if err := os.WriteFile(r.History.Path, historyRaw, 0600); err != nil {
		t.Fatal(err)
	}
	r.History.SHA256, r.Write = generationHash(historyRaw), true
	receipt, err := PrepareObservationMaterialFiles(t.Context(), cfg, r)
	if err != nil || !receipt.Complete {
		t.Fatal("source-backed counts cannot enter actual history preparation", err)
	}
	plan, err := os.ReadFile(filepath.Join(r.OutputRoot, "plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(r.OutputRoot, "input.json"))
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range input.Accounts {
		if len(a.MonthStatistics) != 1 || a.MonthStatistics[0].Month != f.plan.Funding.Month || a.MonthStatistics[0].Tokens != 37+a.UserID {
			t.Fatal("wrong-month counts leaked or original counts lost")
		}
	}
	if _, err := PrepareGenerationInput(t.Context(), cfg, plan, raw, "", true); err != nil {
		t.Fatal("local activation failed", err)
	}
	useWalletPreparation(t, &f, WalletPreparation{Plan: plan, Preparation: raw})
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(t.Context(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal("actual source-audited generation failed", err)
	}
	for _, a := range artifact.Reconciliation.Accounts {
		row := a.Months[0]
		if row.Tokens == nil || *row.Tokens != 37+a.UserID || row.Requests == nil || *row.Requests != 1 || row.Amounts == nil || row.Amounts.Consume != "3.000000" || row.Amounts.Refund != "1.000000" {
			t.Fatal("actual generation lost counts or changed money", a.UserID)
		}
	}
	if current, err := os.ReadFile(statsPath); err != nil || generationHash(current) != generationHash(statsRaw) {
		t.Fatal("original counts source changed", err)
	}
}

func TestObservedUsageCannotBypassMonthlyRequestCount(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.FixedZone("Usage-UTC+8", 8*3600))
	from, to := start.Unix(), start.AddDate(0, 1, 0).Unix()
	a := monitor.MonthlyPreparationAccount{UserID: 1,
		Spans:           []monitor.MonthlyPreparationSpan{{UserID: 1, From: from, To: to, ConsumeQuota: 3}},
		MonthStatistics: []monitor.MonthlyPreparationStatistics{{UserID: 1, Month: "2026-09", Requests: 7, Tokens: 300, Source: "independent-counts#month"}}}
	observed := monthlysource.UsageMonthAccountReport{UserID: 1, Totals: &monthlysource.UsageMonthTotals{}, MatchingSeconds: to - from, ObservedSeconds: to - from,
		Windows: []monthlysource.UsageMonthWindowReport{{From: from, To: to, Matching: true,
			Rows: []monthlysource.UsageWindowRow{{UserID: 1, Type: 2, BillingSource: "wallet", Quota: 3, Records: 7}}}}}
	if !sameUsageSpans(context.Background(), a, observed, from, to) {
		t.Fatal("matching whole-month request count rejected")
	}
	a.MonthStatistics[0].Requests++
	if sameUsageSpans(context.Background(), a, observed, from, to) {
		t.Fatal("an unknown interval count bypassed the original observed request count")
	}
}
