package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func TestMonthStepAndPlanLifecycle(t *testing.T) {
	ctx := context.Background()
	root := auditPrivateDir(t)
	p, err := newMonthBatch(ctx, 1, auditPlanFixture(t).Funding, 3600)
	if err != nil {
		t.Fatal(err)
	}
	created, err := initializeMonthBatch(ctx, root, "sep", p, true)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "sep")
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	sha := created.BatchSHA256
	limits := monthlysource.FundingCaptureLimits{MaxPages: 2, Interval: time.Second, Timeout: 3 * time.Second}
	args := []string{"run-month", "-workspace", root, "-batch", "sep", "-expected-batch", sha}
	var out bytes.Buffer
	if err := run(ctx, append(args, "-connection-file", "/never-read"), &out); err != nil {
		t.Fatal(err)
	}
	var checked monthStepReceipt
	if json.Unmarshal(out.Bytes(), &checked) != nil || checked.Step != nil || checked.Before.Next.Job != p.Windows[0].Job {
		t.Fatal("default not inspect only")
	}
	for _, extra := range [][]string{{"-max-pages", "11"}, {"-interval", "0s"}, {"-budget", "31s"}, {"-phase", "newer"}, {"-job", "window-0001"}, {"-capture"}, {"-expected-batch", strings.Repeat("b", 64)}} {
		out.Reset()
		if err := run(ctx, append(append([]string(nil), args...), extra...), &out); err == nil || out.Len() != 0 {
			t.Fatal("invalid batch control", extra)
		}
	}
	if _, err := sealMonthPlan(ctx, root, "sep", sha, true); err == nil {
		t.Fatal("unfinished plan accepted")
	}
	if _, err := os.Stat(filepath.Join(dir, "month-plan.pending.json")); !os.IsNotExist(err) {
		t.Fatal("unfinished plan wrote a file")
	}
	calls := 0
	first := p.Windows[0]
	step := func(c context.Context, workspace, job, expected, phase, connection string, got monthlysource.FundingCaptureLimits, capture bool) (receipt, error) {
		calls++
		if workspace != dir || job != first.Job || expected != first.ManifestSHA256 || phase != "older" || connection != "synthetic" || got != limits || !capture {
			t.Fatal("selection or budget changed")
		}
		if _, err := execute(c, dir, job, expected, phase, "", limits, false); !errors.Is(err, errBusy) {
			t.Fatal("selection/execution lock not held", err)
		}
		saveMonthStepPage(t, dir, p, first, phase)
		return receipt{Mode: "synthetic_saved_before_failure", EndReached: true}, context.Canceled
	}
	r, err := advanceMonth(ctx, root, "sep", sha, "synthetic", limits, true, step)
	if !errors.Is(err, context.Canceled) || calls != 1 || !r.NeedsCheckpointCheck || r.After != nil {
		t.Fatal("failure retried or lost uncertainty", err)
	}
	r, err = advanceMonth(ctx, root, "sep", sha, "", limits, false, step)
	if err != nil || calls != 1 || r.Before.Next.Job != p.Windows[1].Job || r.Before.OlderCompleted != 1 {
		t.Fatal("saved checkpoint not recovered", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = advanceMonth(cancelled, root, "sep", sha, "synthetic", limits, true, step); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal("cancelled work executed", err)
	}
	for _, w := range p.Windows[1 : len(p.Windows)-1] {
		saveMonthStepPage(t, dir, p, w, "older")
	}
	last := p.Windows[len(p.Windows)-1]
	finish := func(c context.Context, workspace, job, expected, phase, connection string, got monthlysource.FundingCaptureLimits, capture bool) (receipt, error) {
		calls++
		if job != last.Job || expected != last.ManifestSHA256 {
			t.Fatal("wrong final window")
		}
		saveMonthStepPage(t, workspace, p, last, phase)
		return receipt{Mode: "synthetic_completed_window", EndReached: true}, nil
	}
	r, err = advanceMonth(ctx, root, "sep", sha, "synthetic", limits, true, finish)
	if err != nil || calls != 2 || r.Before.Phase != "older" || r.After == nil || r.After.Phase != "newer" || r.After.NewerCompleted != 0 {
		t.Fatal("sweep boundary crossed in one invocation", err)
	}
	for _, w := range p.Windows[:len(p.Windows)-1] {
		saveMonthStepPage(t, dir, p, w, "newer")
	}
	r, err = advanceMonth(ctx, root, "sep", sha, "synthetic", limits, true, finish)
	if err != nil || calls != 3 || r.After == nil || r.After.Phase != "checkpoints_finished_needs_full_audit" {
		t.Fatal("second sweep not complete", err)
	}
	r, err = advanceMonth(ctx, root, "sep", sha, "missing", limits, true, finish)
	if err != nil || calls != 3 || r.Step != nil {
		t.Fatal("completed batch recaptured", err)
	}
	before := map[string]string{}
	for _, w := range p.Windows {
		for _, name := range []string{"manifest.json", "funding.sqlite"} {
			path := filepath.Join(dir, w.Job, name)
			before[path] = fileHash(t, path)
		}
	}
	planArgs := []string{"plan-month", "-workspace", root, "-batch", "sep", "-expected-batch", sha}
	out.Reset()
	if err := run(ctx, planArgs, &out); err != nil {
		t.Fatal(err)
	}
	var preview monthAuditReceipt
	if json.Unmarshal(out.Bytes(), &preview) != nil || !validSHA(preview.PlanSHA256) {
		t.Fatal("missing audit preview")
	}
	a := preview.Review.Accounts
	if len(a) != 2 || a[0].Totals == nil || a[1].Totals == nil || a[0].Totals.IncreaseQuota != 10 || a[1].Totals.DecreaseQuota != -20 {
		t.Fatal("money duplicated or mixed", a)
	}
	path := filepath.Join(dir, "month-plan.json")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("preview wrote plan")
	}
	if executable := os.Getenv("MONTHLY_CAPTURE_TEST_BINARY"); executable != "" {
		b, err := exec.CommandContext(ctx, executable, planArgs...).CombinedOutput()
		if err != nil || !bytes.Equal(b, out.Bytes()) {
			t.Fatal("actual plan preview differs", err)
		}
		b, err = exec.CommandContext(ctx, executable, append(args, "-capture", "-connection-file", "/not-read-on-completion")...).CombinedOutput()
		var result monthStepReceipt
		if err != nil || json.Unmarshal(b, &result) != nil || result.Step != nil || result.Before.Phase != "checkpoints_finished_needs_full_audit" {
			t.Fatal("completed actual command connected", err)
		}
	}
	out.Reset()
	if err := run(ctx, append(planArgs, "-create-plan"), &out); err != nil {
		t.Fatal(err)
	}
	plan, _, err := readMonthPlan(path, preview.PlanSHA256)
	if err != nil || len(plan.Windows) != 720 {
		t.Fatal("plan handoff invalid", err)
	}
	review, err := auditMonth(ctx, dir, plan)
	if err != nil || !reflect.DeepEqual(review, preview.Review) {
		t.Fatal("handoff changed observations", err)
	}
	if _, err := sealMonthPlan(ctx, root, "sep", sha, true); err == nil || fileHash(t, path) != preview.PlanSHA256 {
		t.Fatal("existing plan overwritten")
	}
	for path, sha := range before {
		if fileHash(t, path) != sha {
			t.Fatal("source modified during handoff", path)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "month-plan.pending.json")); !os.IsNotExist(err) {
		t.Fatal("pending plan remained after success")
	}
	// A syntactically valid checkpoint is not enough: corrupt an index and
	// ensure plan creation still goes through full original chain/index audit.
	db, err := openReceiver(ctx, filepath.Join(dir, first.Job, "funding.sqlite"), true)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately bypass append-only protection in this disposable synthetic
	// database only, to model a damaged copy; production keeps its trigger.
	_, err = db.ExecContext(ctx, "DROP TRIGGER monthly_funding_records_no_delete")
	if err == nil {
		_, err = db.ExecContext(ctx, "DELETE FROM portal_monthly_funding_records")
	}
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	if _, err := sealMonthPlan(ctx, root, "sep", sha, false); err == nil {
		t.Fatal("corrupted index passed full audit")
	}
	t.Log("one-window budget, persisted error recovery, sweep barrier, immutable audited plan, exact +10/-20 quota and original file hashes verified")
}

func saveMonthStepPage(t *testing.T, root string, p monthCaptureBatch, w monthCaptureWindow, phase string) {
	t.Helper()
	m := batchManifest(p, w)
	scan, at := m.Older, int64(1790870400000)
	if phase == "newer" {
		scan, at = m.Newer, at+1000000
	}
	page := monthlysource.FundingPage{Version: 1, Scan: scan, FenceID: 100, EndReached: true, StartedAtMS: at, CompletedAtMS: at + 10, Rows: []monthlysource.FundingLog{}}
	if w.Job == p.Windows[0].Job {
		page.Rows = []monthlysource.FundingLog{
			{ID: 1, UserID: 9, CreatedAt: w.From, Type: 3, Other: `{"op":{"action":"user.quota_add","params":{"target_user_id":201,"quota":"10 点额度"}},"admin_info":{"admin_id":9}}`},
			{ID: 2, UserID: 9, CreatedAt: w.From + 1, Type: 3, Other: `{"op":{"action":"user.quota_subtract","params":{"target_user_id":202,"quota":"20 点额度"}},"admin_info":{"admin_id":9}}`},
		}
		page.Next = monthlysource.FundingCursor{At: w.From + 1, ID: 2}
	}
	db, err := openReceiver(context.Background(), filepath.Join(root, w.Job, "funding.sqlite"), true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = monthlysource.SaveFundingPage(context.Background(), db, page)
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
}

func TestMonthStepBlocksInterleavingAndBusyWorkspace(t *testing.T) {
	ctx := context.Background()
	root := auditPrivateDir(t)
	p, err := newMonthBatch(ctx, 1, auditPlanFixture(t).Funding, 3600)
	if err != nil {
		t.Fatal(err)
	}
	created, err := initializeMonthBatch(ctx, root, "bad-order", p, true)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "bad-order")
	saveMonthStepPage(t, dir, p, p.Windows[0], "older")
	saveMonthStepPage(t, dir, p, p.Windows[0], "newer")
	called := false
	step := func(context.Context, string, string, string, string, string, monthlysource.FundingCaptureLimits, bool) (receipt, error) {
		called = true
		return receipt{}, nil
	}
	r, err := advanceMonth(ctx, root, "bad-order", created.BatchSHA256, "must-not-read", monthlysource.FundingCaptureLimits{}, true, step)
	if err == nil || called || r.Before.Phase != "blocked" || r.Before.Next != nil {
		t.Fatal("interleaved batch executed", err)
	}
	lock, err := workspaceLock(dir, true, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = advanceMonth(ctx, root, "bad-order", created.BatchSHA256, "", monthlysource.FundingCaptureLimits{}, false, step)
	_ = lock.Close()
	if !errors.Is(err, errBusy) || called {
		t.Fatal("busy batch executed", err)
	}
	if _, err := sealMonthPlan(ctx, root, "bad-order", created.BatchSHA256, true); err == nil {
		t.Fatal("interleaved plan exported")
	}
}
