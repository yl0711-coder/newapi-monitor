package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

type monthStepReceipt struct {
	Mode                 string             `json:"mode"`
	Before               monthBatchReceipt  `json:"before"`
	Step                 *receipt           `json:"step,omitempty"`
	After                *monthBatchReceipt `json:"after,omitempty"`
	NeedsCheckpointCheck bool               `json:"needs_checkpoint_check"`
}

func runMonthStep(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workspace := fs.String("workspace", "", "private parent workspace")
	name := fs.String("batch", "", "existing batch directory name")
	expected := fs.String("expected-batch", "", "exact immutable batch SHA256")
	if args[0] == "plan-month" {
		create := fs.Bool("create-plan", false, "write a new audited month-plan.json; default audit only")
		if fs.Parse(args[1:]) != nil || fs.NArg() != 0 || !validSHA(*expected) {
			return errInput
		}
		r, err := sealMonthPlan(ctx, *workspace, *name, *expected, *create)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(r)
	}
	connection := fs.String("connection-file", "", "0600 local source connection; read only with -capture")
	capture := fs.Bool("capture", false, "capture at most one unfinished window; default inspect only")
	pages := fs.Int("max-pages", 1, "1..10 pages total this invocation")
	interval := fs.Duration("interval", 2*time.Second, "1s..10s before each page")
	budget := fs.Duration("budget", 15*time.Second, "1s..30s total source runner budget")
	if fs.Parse(args[1:]) != nil || fs.NArg() != 0 || !validSHA(*expected) || *pages < 1 || *pages > 10 || *interval < time.Second || *interval > 10*time.Second || *budget < time.Second || *budget > 30*time.Second || (*capture && *connection == "") {
		return errInput
	}
	r, err := advanceMonth(ctx, *workspace, *name, *expected, *connection, monthlysource.FundingCaptureLimits{MaxPages: *pages, Interval: *interval, Timeout: *budget}, *capture, executeLocked)
	if r.Mode == "" {
		return err
	}
	return errors.Join(err, json.NewEncoder(out).Encode(r))
}

type monthStepExecutor func(context.Context, string, string, string, string, string, monthlysource.FundingCaptureLimits, bool) (receipt, error)

// A single invocation never loops over source windows or crosses a sweep.
// The same workspace lock covers inspection, selection, capture and recheck.
func advanceMonth(ctx context.Context, workspace, name, expected, connection string, limits monthlysource.FundingCaptureLimits, capture bool, step monthStepExecutor) (result monthStepReceipt, err error) {
	_, root, err := jobPath(workspace, name)
	if err != nil {
		return result, err
	}
	lock, err := workspaceLock(root, capture, false)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	before, err := inspectMonthBatchLocked(ctx, root, expected)
	if err != nil {
		return result, err
	}
	result.Mode, result.Before = "checked_batch_no_source_connection", before
	if before.Phase == "blocked" {
		return result, errInput
	}
	if !capture || before.Next == nil {
		return result, nil // completed checkpoints still need the full audit
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.Mode = "one_bounded_window_not_bill_approval"
	n := before.Next
	r, runErr := step(ctx, root, n.Job, n.ManifestSHA256, n.Phase, connection, limits, true)
	result.Step = &r
	result.NeedsCheckpointCheck = true // retain this on any execution/recheck error
	if runErr != nil {
		return result, runErr // never retry or move to another window on failure
	}
	after, err := inspectMonthBatchLocked(ctx, root, expected)
	if err != nil {
		return result, err
	}
	result.After = &after
	if after.Phase == "blocked" {
		return result, errInput
	}
	result.NeedsCheckpointCheck = false
	return result, nil
}

// Generates the existing audit input from retained heads, rather than asking
// operators to transcribe hundreds of hashes. No copied/fabricated scan heads.
func sealMonthPlan(ctx context.Context, workspace, name, expected string, create bool) (result monthAuditReceipt, err error) {
	_, root, err := jobPath(workspace, name)
	if err != nil {
		return result, err
	}
	lock, err := workspaceLock(root, create, false)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	progress, err := inspectMonthBatchLocked(ctx, root, expected)
	if err != nil || progress.Phase != "checkpoints_finished_needs_full_audit" {
		return result, errors.Join(errInput, err)
	}
	p, err := readMonthBatch(ctx, root, expected)
	if err != nil {
		return result, err
	}
	plan := monthAuditPlan{Version: p.Version, GroupID: p.GroupID, Funding: p.Funding}
	for _, w := range p.Windows {
		m := batchManifest(p, w)
		db, err := openReceiver(ctx, filepath.Join(root, w.Job, "funding.sqlite"), false)
		if err != nil {
			return result, err
		}
		a, aErr := checkpoint(ctx, db, m.Older)
		b, bErr := checkpoint(ctx, db, m.Newer)
		if err := errors.Join(aErr, bErr, db.Close()); err != nil {
			return result, err
		}
		if a == nil || b == nil || !a.EndReached || !b.EndReached {
			return result, errInput
		}
		ar, _ := json.Marshal(a)
		br, _ := json.Marshal(b)
		plan.Windows = append(plan.Windows, monthAuditWindow{Job: w.Job, ManifestSHA256: w.ManifestSHA256, Version: monthlysource.FundingMonthWindowVersion{From: w.From, To: w.To, OlderScanID: w.OlderScanID, NewerScanID: w.NewerScanID, OlderHeadID: hash(ar), NewerHeadID: hash(br)}})
	}
	windows, err := validateMonthAuditPlan(ctx, plan)
	if err != nil {
		return result, err
	}
	review, err := auditMonthLocked(ctx, root, plan, windows)
	if err != nil {
		return result, err
	}
	raw, err := json.Marshal(plan)
	if err != nil || len(raw) > 2<<20 {
		return result, errInput
	}
	rr, err := json.Marshal(review)
	if err != nil || len(rr) > 8<<20 {
		return result, errInput
	}
	result = monthAuditReceipt{Mode: "checked_plan_observations_not_bill_approval", PlanSHA256: hash(raw), GroupID: p.GroupID, Review: review}
	if !create {
		return result, nil
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	path := filepath.Join(root, "month-plan.json")
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return result, errInput
	}
	pending := filepath.Join(root, "month-plan.pending.json")
	if err := exclusiveFile(pending, raw); err != nil {
		return result, err
	}
	if err := publishNewFile(pending, path); err != nil {
		return result, err
	}
	if err := syncPath(root); err != nil {
		return result, err
	}
	result.Mode = "created_audit_plan_not_bill_approval"
	return result, nil
}
