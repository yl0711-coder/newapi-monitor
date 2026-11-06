package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Original retained month records come from an independently pinned synthetic
// source, not from the normalizer being tested. Amounts remain original ints.
func fillRetainedHistoryMonth(t *testing.T, f generationFixture, r *ObservationMaterialPreparationRequest, keepUnknownTokens ...bool) monitor.MonthlyPreparationInput {
	t.Helper()
	raw, err := os.ReadFile(f.input.Sources[0].Path)
	var original []monitor.MonthlyPreparationAccount
	if err != nil || json.Unmarshal(raw, &original) != nil {
		t.Fatal("independent synthetic month unavailable", err)
	}
	byID := map[int64]monitor.MonthlyPreparationAccount{}
	for _, a := range original {
		byID[a.UserID] = a
	}
	raw, err = os.ReadFile(r.History.Path)
	if err != nil {
		t.Fatal(err)
	}
	catalogue, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := range catalogue.Accounts {
		a := &catalogue.Accounts[i]
		old, selected := byID[a.UserID]
		if !selected {
			continue
		}
		a.Spans, a.Movements = old.Spans, old.Movements
		for i := range a.Spans {
			requests, tokens := int64(1), int64(123456)
			a.Spans[i].Requests, a.Spans[i].Tokens = &requests, &tokens
			if len(keepUnknownTokens) != 0 && keepUnknownTokens[0] {
				a.Spans[i].Tokens = nil
			}
		}
	}
	raw = draftJSON(t, catalogue)
	if err := os.WriteFile(r.History.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	r.History.SHA256 = generationHash(raw)
	return catalogue
}

func TestObservedHistoryPreviouslyFilledMonthRechecksWithoutChangingMoney(t *testing.T) {
	f, cfg, r, _ := observationMaterialPreparationFixture(t)
	useOrganizationHistoryFixture(t, f, cfg, &r)
	catalogue := fillRetainedHistoryMonth(t, f, &r)
	before, err := os.ReadFile(r.History.Path)
	if err != nil {
		t.Fatal(err)
	}
	preview, err := PrepareObservationMaterialFiles(t.Context(), cfg, r)
	if err != nil || preview.Complete {
		t.Fatal("previously retained month cannot be inspected", err)
	}
	preparationCounts(t, f, 0, 0)
	r.Write = true
	receipt, err := PrepareObservationMaterialFiles(t.Context(), cfg, r)
	if err != nil || !receipt.Complete || receipt.ManifestSHA256 != preview.ManifestSHA256 {
		t.Fatal("checked history failed prepared output", err)
	}
	inputRaw, err := os.ReadFile(filepath.Join(r.OutputRoot, "input.json"))
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(inputRaw)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[int64]monitor.MonthlyPreparationAccount{}
	for _, a := range catalogue.Accounts {
		byID[a.UserID] = a
	}
	for _, a := range input.Accounts {
		old := byID[a.UserID]
		if !reflect.DeepEqual(a.Baseline, old.Baseline) || !reflect.DeepEqual(a.Spans, old.Spans) ||
			!reflect.DeepEqual(a.MovementCoverage, old.MovementCoverage) || len(a.Movements) != len(old.Movements) {
			t.Fatal("retained money/history/statistics changed", a.UserID)
		}
		for i, m := range a.Movements {
			old.Movements[i].Source = m.Source // Only the funding source reference changes to the newly audited original row.
			if m != old.Movements[i] {
				t.Fatal("funding amount, owner, time or identity changed")
			}
		}
	}
	after, err := os.ReadFile(r.History.Path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("original catalogue rewritten", err)
	}
	plan, err := os.ReadFile(filepath.Join(r.OutputRoot, "plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareGenerationInput(t.Context(), cfg, plan, inputRaw, "", false); err != nil {
		t.Fatal("formal inspector rejected retained history", err)
	}
	if _, err := PrepareGenerationInput(t.Context(), cfg, plan, inputRaw, "", true); err != nil {
		t.Fatal(err)
	}
	useWalletPreparation(t, &f, WalletPreparation{Plan: plan, Preparation: inputRaw})
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(t.Context(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := []monitor.MonthlyPreparationAmounts{
		{Opening: "1000.000000", Increase: "10.000000", Decrease: "-800.000000", Consume: "3.000000", Refund: "1.000000", Closing: "208.000000"},
		{Opening: "1000.000000", Increase: "800.000000", Decrease: "0.000000", Consume: "3.000000", Refund: "1.000000", Closing: "1798.000000"},
	}
	for i, a := range artifact.Reconciliation.Accounts {
		row := a.Months[0]
		if row.Amounts == nil || *row.Amounts != want[i] || row.Requests == nil || *row.Requests != 1 {
			t.Fatal("original per-account money changed")
		}
	}
	for _, a := range artifact.Reconciliation.Accounts {
		if a.Months[0].Tokens == nil || *a.Months[0].Tokens != 123456 {
			t.Fatal("known retained tokens lost")
		}
	}
	again, err := f.g.Run(t.Context(), job.Request.Owner, job.ID)
	if err != nil || !reflect.DeepEqual(again, artifact) {
		t.Fatal("repeated run changed money", err)
	}
}

func TestObservedHistoryConflictsNeverOverwriteOrCancel(t *testing.T) {
	for _, kind := range []string{"consume", "refund", "net_only", "offsetting_accounts", "requests", "partial_month", "split_capture", "missing_funding", "funding_amount", "funding_time", "duplicate_funding", "foreign_member", "missing_zero_day", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg, r, _ := observationMaterialPreparationFixture(t)
			useOrganizationHistoryFixture(t, f, cfg, &r)
			catalogue := fillRetainedHistoryMonth(t, f, &r)
			// Reverse fixture order: 1 is member 202, 2 is member 201.
			a, b := &catalogue.Accounts[1], &catalogue.Accounts[2]
			ctx := context.Background()
			switch kind {
			case "consume":
				a.Spans[0].ConsumeQuota++
			case "refund":
				a.Spans[0].RefundQuota++
			case "net_only":
				a.Spans[0].ConsumeQuota++
				a.Spans[0].RefundQuota++
			case "offsetting_accounts":
				a.Spans[0].ConsumeQuota++
				b.Spans[0].ConsumeQuota--
			case "requests":
				*a.Spans[0].Requests++
			case "partial_month":
				a.Spans[0].To -= 86400
			case "split_capture":
				a.Spans[0].From += 1
			case "missing_funding":
				a.Movements = []monitor.MonthlyPreparationMovement{}
			case "funding_amount":
				a.Movements[0].DeltaQuota++
			case "funding_time":
				a.Movements[0].At++
			case "duplicate_funding":
				a.Movements = append(a.Movements, a.Movements[0])
			case "foreign_member":
				a.Spans[0].UserID = b.UserID
			case "missing_zero_day":
				a.Spans[0].To -= 86400 // A zero-activity day remains required coverage.
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			raw := draftJSON(t, catalogue)
			if err := os.WriteFile(r.History.Path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			r.History.SHA256 = generationHash(raw)
			r.Write = true
			receipt, err := PrepareObservationMaterialFiles(ctx, cfg, r)
			if err == nil || receipt.Complete || receipt.Directory != "" {
				t.Fatal("conflicting history overwritten", kind, receipt, err)
			}
			entries, err := os.ReadDir(r.OutputRoot)
			if err != nil || len(entries) != 0 {
				t.Fatal("conflict produced prepared output", err)
			}
			after, err := os.ReadFile(r.History.Path)
			if err != nil || !bytes.Equal(raw, after) {
				t.Fatal("conflict rewrote original", err)
			}
			preparationCounts(t, f, 0, 0)
		})
	}
}

func TestObservedHistoryManualTemplateStillRejectsPreviouslyFilledMonth(t *testing.T) {
	f, cfg, r, _ := observationMaterialPreparationFixture(t)
	useOrganizationHistoryFixture(t, f, cfg, &r)
	fillRetainedHistoryMonth(t, f, &r)
	body, err := os.ReadFile(r.History.Path)
	if err != nil {
		t.Fatal(err)
	}
	catalogue, err := monitor.DecodeMonthlyPreparationInput(body)
	if err != nil {
		t.Fatal(err)
	}
	catalogue.Accounts = catalogue.Accounts[1:]
	for i := range catalogue.Accounts {
		catalogue.Accounts[i].Months = []string{f.plan.Funding.Month}
	}
	catalogue.Sources = slices.DeleteFunc(catalogue.Sources, func(s monitor.MonthlyPreparationSource) bool { return s.ID == "unrelated" })
	r.Template, r.History, r.Organization = draftJSON(t, catalogue), nil, nil
	r.Write = true
	if result, err := PrepareObservationMaterialFiles(t.Context(), cfg, r); !errors.Is(err, ErrGenerationInput) || result.Complete {
		t.Fatal("manual no-overwrite semantics relaxed")
	}
	if _, err := os.Stat(filepath.Join(r.OutputRoot, "manifest.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("manual conflict completed", err)
	}
	preparationCounts(t, f, 0, 0)
}

func TestObservedHistoryMixedAccountsAndFundingOnly(t *testing.T) {
	for _, kind := range []string{"mixed", "funding_only"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg, r, _ := observationMaterialPreparationFixture(t)
			useOrganizationHistoryFixture(t, f, cfg, &r)
			catalogue := fillRetainedHistoryMonth(t, f, &r, true)
			for i := range catalogue.Accounts {
				a := &catalogue.Accounts[i]
				if a.GroupID != f.plan.Owner.GroupID {
					continue
				}
				if kind == "funding_only" || a.UserID == f.plan.Funding.UserIDs[1] {
					a.Spans = []monitor.MonthlyPreparationSpan{}
				}
				if kind == "mixed" && a.UserID == f.plan.Funding.UserIDs[1] {
					a.Movements = []monitor.MonthlyPreparationMovement{}
				}
			}
			raw := draftJSON(t, catalogue)
			if err := os.WriteFile(r.History.Path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			r.History.SHA256 = generationHash(raw)
			r.Write = true
			receipt, err := PrepareObservationMaterialFiles(t.Context(), cfg, r)
			if err != nil || !receipt.Complete {
				t.Fatal("mixed history cannot prepare", kind, err)
			}
			plan, err := os.ReadFile(filepath.Join(r.OutputRoot, "plan.json"))
			if err != nil {
				t.Fatal(err)
			}
			body, err := os.ReadFile(filepath.Join(r.OutputRoot, "input.json"))
			if err != nil {
				t.Fatal(err)
			}
			if checked, err := PrepareGenerationInput(t.Context(), cfg, plan, body, "", false); err != nil || checked.Applied {
				t.Fatal("mixed history failed existing audit", err)
			}
			preparationCounts(t, f, 0, 0)
		})
	}
}
