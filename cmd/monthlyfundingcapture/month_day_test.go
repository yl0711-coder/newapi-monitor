package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func TestDailyFundingCalendarExplicitOptInAndNoSource(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		month string
		days  int
	}{
		{"2024-02", 29}, {"2026-02", 28}, {"2026-09", 30}, {"2026-08", 31},
	} {
		f := auditPlanFixture(t).Funding
		f.Month = tc.month
		p, err := newMonthBatch(ctx, 1, f, 86400)
		if err != nil || p.Version != 2 || len(p.Windows) != tc.days || batchReceipt(p).MinimumPageReads != 2*tc.days {
			t.Fatal("daily calendar/reads", tc.month, err)
		}
		for _, w := range p.Windows {
			m := batchManifest(p, w)
			if m.Older.Version != 2 || m.Newer.Version != 2 || m.validate(time.Now()) != nil {
				t.Fatal("day manifest rejected")
			}
		}
		bad := p
		bad.Version = 1
		if validateMonthBatch(ctx, bad) == nil {
			t.Fatal("daily batch downgraded to hour protocol")
		}
	}
	root := auditPrivateDir(t)
	f := auditPlanFixture(t).Funding
	args := []string{"init-month", "-workspace", root, "-batch", "days", "-group-id", "1", "-month", "2026-09", "-source-identity", f.SourceIdentity, "-user-ids", "201,202", "-window-seconds", "86400"}
	var out bytes.Buffer
	if err := run(ctx, args, &out); err != nil {
		t.Fatal(err)
	}
	var r monthBatchReceipt
	if json.Unmarshal(out.Bytes(), &r) != nil || r.Windows != 30 || r.MinimumPageReads != 60 || r.Mode != "checked_month_schedule_no_source_or_files" {
		t.Fatal("daily default unexpectedly executed", out.String())
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatal("dry run wrote local jobs", err)
	}
	for _, size := range []int64{3601, 7200, 43200, 86399, 86401} {
		if _, err := newMonthBatch(ctx, 1, f, size); err == nil {
			t.Fatal("unreviewed intermediate window accepted", size)
		}
	}
	// Existing flags still select the legacy hour; days need explicit opt-in.
	single := []string{"init", "-workspace", root, "-job", "day", "-group-id", "1", "-month", "2026-09", "-source-identity", f.SourceIdentity, "-user-ids", "201,202", "-from", strconv.FormatInt(1788192000, 10), "-to", strconv.FormatInt(1788278400, 10)}
	out.Reset()
	if err := run(ctx, single, &out); err == nil || out.Len() != 0 {
		t.Fatal("legacy init silently accepted daily bounds")
	}
	if err := run(ctx, append(single, "-daily"), &out); err != nil {
		t.Fatal("explicit daily init", err)
	}
}

func TestDailyFundingAssemblyGenerationAndMismatch(t *testing.T) {
	ctx := context.Background()
	root := auditPrivateDir(t)
	batch, err := newMonthBatch(ctx, 1, auditPlanFixture(t).Funding, 86400)
	if err != nil {
		t.Fatal(err)
	}
	p := monthAuditPlan{Version: batch.Version, GroupID: batch.GroupID, Funding: batch.Funding}
	for _, w := range batch.Windows {
		p.Windows = append(p.Windows, monthAuditWindow{Job: w.Job, ManifestSHA256: w.ManifestSHA256, Version: monthlysource.FundingMonthWindowVersion{
			From: w.From, To: w.To, OlderScanID: w.OlderScanID, NewerScanID: w.NewerScanID,
		}})
	}
	before := map[string]string{}
	for i := range p.Windows {
		p.Windows[i] = writeAuditWindow(t, root, p, i, nil)
		for _, name := range []string{"funding.sqlite", "manifest.json"} {
			path := filepath.Join(root, p.Windows[i].Job, name)
			before[path] = fileHash(t, path)
		}
	}
	bad := p
	bad.Version = 1
	if _, err := validateMonthAuditPlan(ctx, bad); err == nil {
		t.Fatal("day audit downgraded to v1")
	}
	bad = p
	bad.Windows = append([]monthAuditWindow(nil), p.Windows...)
	bad.Windows = bad.Windows[1:]
	if _, err := validateMonthAuditPlan(ctx, bad); err == nil {
		t.Fatal("missing empty day counted as complete")
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := assembleMonth(ctx, root, "daily-assembly", p, raw, true)
	if err != nil || receipt.Pages != 60 || receipt.Rows != 4 || receipt.Mode != "assembled_local_evidence_not_bill_approval" {
		t.Fatal("daily assembly", err, receipt)
	}
	db, err := openReceiver(ctx, filepath.Join(root, "daily-assembly", "funding.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	review, err := reviewAssembledMonth(ctx, db, p, p.Windows, receipt.Pages, receipt.Rows)
	closeErr := db.Close()
	if err != nil || closeErr != nil || len(review.Windows.MissingWindows) != 0 || len(review.Accounts) != 2 ||
		review.Accounts[0].Totals == nil || review.Accounts[1].Totals == nil ||
		review.Accounts[0].Totals.IncreaseQuota != 10 || review.Accounts[1].Totals.DecreaseQuota != -20 {
		t.Fatal("daily exact per-account money", err, closeErr)
	}
	// Uses the same real generation runner as the 720-hour test, including a
	// one-quota mismatch that must fail; no customer bill is published.
	testAssembledGeneration(t, root, filepath.Join(root, "daily-assembly"), p, review)
	for path, want := range before {
		if fileHash(t, path) != want {
			t.Fatal("original daily evidence changed")
		}
	}
}

func TestDailyFundingSealPreservesProtocolAndNoSourceInspection(t *testing.T) {
	ctx := context.Background()
	root := auditPrivateDir(t)
	p, err := newMonthBatch(ctx, 1, auditPlanFixture(t).Funding, 86400)
	if err != nil {
		t.Fatal(err)
	}
	r, err := initializeMonthBatch(ctx, root, "days", p, true)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "days")
	for _, phase := range []string{"older", "newer"} {
		for _, w := range p.Windows {
			saveMonthStepPage(t, dir, p, w, phase)
		}
	}
	var out bytes.Buffer
	if err := run(ctx, []string{"run-month", "-workspace", root, "-batch", "days", "-expected-batch", r.BatchSHA256,
		"-connection-file", "/must-not-read"}, &out); err != nil {
		t.Fatal("dry inspection read source or rejected daily checkpoints", err)
	}
	var step monthStepReceipt
	if json.Unmarshal(out.Bytes(), &step) != nil || step.Mode != "checked_batch_no_source_connection" || step.Step != nil {
		t.Fatal("dry daily inspection executed work")
	}
	sealed, err := sealMonthPlan(ctx, root, "days", r.BatchSHA256, true)
	if err != nil {
		t.Fatal("seal daily month", err)
	}
	plan, _, err := readMonthPlan(filepath.Join(dir, "month-plan.json"), sealed.PlanSHA256)
	if err != nil || plan.Version != 2 || len(plan.Windows) != 30 {
		t.Fatal("daily protocol lost between batch and audited plan", err)
	}
	if _, err := auditMonth(ctx, dir, plan); err != nil {
		t.Fatal("sealed daily plan cannot be reaudited", err)
	}
}
