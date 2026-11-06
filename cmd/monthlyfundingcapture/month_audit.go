package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// This is an operator audit plan, not publication authority. It pins both the
// immutable job manifest and the completed heads; a newer scan is never silently
// substituted. All jobs must be in the same locked private workspace.
type monthAuditPlan struct {
	Version int                               `json:"version"`
	GroupID int64                             `json:"group_id"`
	Funding monthlysource.FundingMonthRequest `json:"funding"`
	Windows []monthAuditWindow                `json:"windows"`
}

type monthAuditWindow struct {
	Job            string                                  `json:"job"`
	ManifestSHA256 string                                  `json:"manifest_sha256"`
	Version        monthlysource.FundingMonthWindowVersion `json:"version"`
}

type monthAuditReceipt struct {
	Mode       string                           `json:"mode"`
	PlanSHA256 string                           `json:"plan_sha256"`
	GroupID    int64                            `json:"group_id"`
	Review     monthlysource.FundingMoneyReview `json:"review"`
}

func runMonthAudit(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("audit-month", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workspace := fs.String("workspace", "", "existing private capture workspace")
	planPath := fs.String("plan", "", "private canonical month plan JSON")
	expected := fs.String("expected-plan", "", "exact month plan SHA256")
	if fs.Parse(args) != nil || fs.NArg() != 0 || !validSHA(*expected) {
		return errInput
	}
	plan, _, err := readMonthPlan(*planPath, *expected)
	if err != nil {
		return err
	}
	result, err := auditMonth(ctx, *workspace, plan)
	if err != nil {
		return err // never print a partial successful result
	}
	return json.NewEncoder(out).Encode(monthAuditReceipt{
		Mode: "local_observations_only_not_bill_approval", PlanSHA256: *expected,
		GroupID: plan.GroupID, Review: result,
	})
}

func readMonthPlan(path, expected string) (monthAuditPlan, []byte, error) {
	raw, err := privateRead(path, 2<<20)
	var plan monthAuditPlan
	if err != nil || !validSHA(expected) || hash(raw) != expected || json.Unmarshal(raw, &plan) != nil {
		return plan, nil, errInput
	}
	canonical, err := json.Marshal(plan)
	if err != nil || string(raw) != string(canonical) {
		return monthAuditPlan{}, nil, errInput
	}
	return plan, raw, nil
}

func validateMonthAuditPlan(ctx context.Context, p monthAuditPlan) ([]monthAuditWindow, error) {
	if (p.Version != 1 && p.Version != 2) || p.GroupID <= 0 || len(p.Windows) == 0 || len(p.Windows) > 4096 || len(p.Windows)*len(p.Funding.UserIDs) > 150000 {
		return nil, errInput
	}
	// Reuse the existing month/rules/account validator, including closed UTC+8
	// boundaries. No independent calendar or amount interpretation lives here.
	base, err := monthlysource.ReviewFundingMonth(ctx, p.Funding, nil)
	if err != nil {
		return nil, err
	}
	for i, account := range base.Accounts {
		if p.Funding.UserIDs[i] != account.UserID {
			return nil, errInput // fixed canonical account order
		}
	}
	windows := append([]monthAuditWindow(nil), p.Windows...)
	sort.Slice(windows, func(i, j int) bool { return windows[i].Version.From < windows[j].Version.From })
	jobs, scans := map[string]bool{}, map[string]bool{}
	cursor := base.From
	for _, w := range windows {
		v := w.Version
		if !jobName.MatchString(w.Job) || jobs[w.Job] || !validSHA(w.ManifestSHA256) || v.From != cursor || v.To <= v.From || v.To > base.To || !validSHA(v.OlderHeadID) || !validSHA(v.NewerHeadID) {
			return nil, errInput
		}
		jobs[w.Job] = true
		for _, id := range []string{v.OlderScanID, v.NewerScanID} {
			s := monthlysource.FundingScan{Version: p.Version, ID: id, SourceIdentity: p.Funding.SourceIdentity, Rules: p.Funding.Rules, UserIDs: p.Funding.UserIDs, From: v.From, To: v.To}
			if scans[id] || monthlysource.ValidateFundingScan(s) != nil {
				return nil, errInput
			}
			scans[id] = true
		}
		cursor = v.To
	}
	if cursor != base.To {
		return nil, errInput // a missing hour must not become an empty hour
	}
	return windows, nil
}

func auditMonth(ctx context.Context, workspace string, plan monthAuditPlan) (monthlysource.FundingMoneyReview, error) {
	fail := monthlysource.FundingMoneyReview{}
	windows, err := validateMonthAuditPlan(ctx, plan)
	if err != nil {
		return fail, err
	}
	root, _, err := jobPath(workspace, windows[0].Job)
	if err != nil {
		return fail, err
	}
	lock, err := workspaceLock(root, false, false)
	if err != nil {
		return fail, err
	}
	defer lock.Close()
	return auditMonthLocked(ctx, root, plan, windows)
}

// Caller holds the capture-workspace lock for the entire audit/copy operation.
func auditMonthLocked(ctx context.Context, root string, plan monthAuditPlan, windows []monthAuditWindow) (monthlysource.FundingMoneyReview, error) {
	fail := monthlysource.FundingMoneyReview{}
	observations := make([]monthlysource.FundingWindowObservation, 0, len(windows))
	records, identities := 0, 0
	for _, w := range windows {
		if err := ctx.Err(); err != nil {
			return fail, err
		}
		o, err := auditMonthWindow(ctx, root, plan, w)
		if err != nil {
			return fail, err
		}
		c := o.Comparison()
		records += c.OlderRows + c.NewerRows
		identities += c.NewerRows + len(c.Changes)
		if records > 100000 || identities > 100000 {
			return fail, errors.New("month observation budget exceeded; narrow the account scope")
		}
		observations = append(observations, o)
	}
	// Existing rules enforce month-wide ordered sweeps, fences, cross-window
	// IDs and exact integer amounts. Changed/ambiguous accounts retain explicit
	// incomplete status and nil totals, never zero or another account's money.
	return monthlysource.ReviewFundingMoney(ctx, plan.Funding, observations)
}

func auditMonthWindow(ctx context.Context, root string, plan monthAuditPlan, w monthAuditWindow) (monthlysource.FundingWindowObservation, error) {
	fail := monthlysource.FundingWindowObservation{}
	_, dir, err := jobPath(root, w.Job)
	if err != nil {
		return fail, err
	}
	m, _, err := readManifest(dir, w.ManifestSHA256)
	if err != nil {
		return fail, err
	}
	f, v := plan.Funding, w.Version
	if m.Older.Version != plan.Version || m.Newer.Version != plan.Version || m.GroupID != plan.GroupID || m.Month != f.Month || m.Older.SourceIdentity != f.SourceIdentity || m.Older.Rules != f.Rules || !reflect.DeepEqual(m.Older.UserIDs, f.UserIDs) || m.Older.ID != v.OlderScanID || m.Newer.ID != v.NewerScanID || m.Older.From != v.From || m.Older.To != v.To {
		return fail, errInput
	}
	db, err := openReceiver(ctx, filepath.Join(dir, "funding.sqlite"), false)
	if err != nil {
		return fail, err
	}
	o, err := monthlysource.ReviewFundingWindow(ctx, db, v.OlderScanID, v.NewerScanID)
	err = errors.Join(err, db.Close()) // at most one window DB open at a time
	if err != nil {
		return fail, err
	}
	c := o.Comparison()
	if c.OlderHeadID != v.OlderHeadID || c.NewerHeadID != v.NewerHeadID || c.From != v.From || c.To != v.To {
		return fail, errInput
	}
	return o, nil
}
