package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// A capture schedule is not an audited month plan: it has no fabricated page
// heads, money, coverage flags or authorization to contact a source.
type monthCaptureBatch struct {
	Version       int                               `json:"version"`
	GroupID       int64                             `json:"group_id"`
	Funding       monthlysource.FundingMonthRequest `json:"funding"`
	WindowSeconds int64                             `json:"window_seconds"`
	Windows       []monthCaptureWindow              `json:"windows"`
}

type monthCaptureWindow struct {
	Job            string `json:"job"`
	From           int64  `json:"from"`
	To             int64  `json:"to"`
	OlderScanID    string `json:"older_scan_id"`
	NewerScanID    string `json:"newer_scan_id"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

type monthBatchReceipt struct {
	Mode             string          `json:"mode"`
	BatchSHA256      string          `json:"batch_sha256,omitempty"`
	GroupID          int64           `json:"group_id"`
	Month            string          `json:"month"`
	Accounts         int             `json:"accounts"`
	Windows          int             `json:"windows"`
	MinimumPageReads int             `json:"minimum_page_reads"` // lower bound, not a load authorization
	OlderCompleted   int             `json:"older_checkpoints_completed"`
	NewerCompleted   int             `json:"newer_checkpoints_completed"`
	Phase            string          `json:"phase,omitempty"`
	BlockedReason    string          `json:"blocked_reason,omitempty"`
	Next             *monthBatchNext `json:"next,omitempty"`
}

type monthBatchNext struct {
	Job            string `json:"job"`
	ManifestSHA256 string `json:"manifest_sha256"`
	Phase          string `json:"phase"`
}

func runMonthBatch(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workspace := fs.String("workspace", "", "existing private parent workspace")
	name := fs.String("batch", "", "one private batch directory name")
	if args[0] == "inspect-month" {
		expected := fs.String("expected-batch", "", "exact retained batch SHA256")
		if fs.Parse(args[1:]) != nil || fs.NArg() != 0 || !validSHA(*expected) {
			return errInput
		}
		r, err := inspectMonthBatch(ctx, *workspace, *name, *expected)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(r)
	}
	group := fs.Int64("group-id", 0, "one organization")
	month := fs.String("month", "", "one closed Beijing month")
	source := fs.String("source-identity", "", "independently verified source SHA256")
	users := fs.String("user-ids", "", "exact comma-separated account IDs")
	organization := fs.String("organization-scope", "", "optional sealed current roster instead of manual group/month/source/users")
	organizationSHA := fs.String("expected-organization", "", "exact current roster file SHA256")
	seconds := fs.Int64("window-seconds", 3600, "v1: 300..3600 dividing one hour; explicit 86400 selects v2 UTC+8 days")
	create := fs.Bool("create", false, "create a new empty batch; default check only")
	if fs.Parse(args[1:]) != nil || fs.NArg() != 0 {
		return errInput
	}
	gid, funding, err := organizationInitScope(*organization, *organizationSHA, *group, *month, *source, *users)
	if err != nil {
		return err
	}
	p, err := newMonthBatch(ctx, gid, funding, *seconds)
	if err != nil {
		return err
	}
	r, err := initializeMonthBatch(ctx, *workspace, *name, p, *create)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(r)
}

func newMonthBatch(ctx context.Context, group int64, funding monthlysource.FundingMonthRequest, seconds int64) (monthCaptureBatch, error) {
	p := monthCaptureBatch{Version: 1, GroupID: group, Funding: funding, WindowSeconds: seconds}
	newScan := monthlysource.NewFundingScan
	if seconds == 86400 {
		p.Version, newScan = 2, monthlysource.NewFundingDayScan
	}
	base, err := monthlysource.ReviewFundingMonth(ctx, funding, nil)
	if err != nil {
		return p, err
	}
	if group <= 0 || !validBatchWindow(p.Version, seconds) {
		return p, errInput
	}
	n := (base.To - base.From) / seconds
	if n > 4096 || n*int64(len(funding.UserIDs)) > 150000 {
		return p, errInput
	}
	p.Funding.UserIDs = make([]int64, len(base.Accounts))
	for i, a := range base.Accounts {
		p.Funding.UserIDs[i] = a.UserID
	}
	for at := base.From; at < base.To; at += seconds {
		if err := ctx.Err(); err != nil {
			return monthCaptureBatch{}, err
		}
		a, err := newScan(funding.SourceIdentity, funding.Rules, p.Funding.UserIDs, at, at+seconds)
		if err != nil {
			return monthCaptureBatch{}, err
		}
		b, err := newScan(funding.SourceIdentity, funding.Rules, p.Funding.UserIDs, at, at+seconds)
		if err != nil {
			return monthCaptureBatch{}, err
		}
		w := monthCaptureWindow{Job: fmt.Sprintf("window-%04d", len(p.Windows)), From: at, To: at + seconds, OlderScanID: a.ID, NewerScanID: b.ID}
		raw, err := json.Marshal(batchManifest(p, w))
		if err != nil {
			return monthCaptureBatch{}, err
		}
		w.ManifestSHA256 = hash(raw)
		p.Windows = append(p.Windows, w)
	}
	return p, validateMonthBatch(ctx, p)
}

func batchManifest(p monthCaptureBatch, w monthCaptureWindow) manifest {
	a := monthlysource.FundingScan{Version: p.Version, ID: w.OlderScanID, SourceIdentity: p.Funding.SourceIdentity, Rules: p.Funding.Rules, UserIDs: p.Funding.UserIDs, From: w.From, To: w.To}
	b := a
	b.ID = w.NewerScanID
	return manifest{Version: 1, GroupID: p.GroupID, Month: p.Funding.Month, Older: a, Newer: b}
}

func validBatchWindow(version int, seconds int64) bool {
	return (version == 1 && seconds >= 300 && seconds <= 3600 && 3600%seconds == 0) ||
		(version == 2 && seconds == 86400)
}

func validateMonthBatch(ctx context.Context, p monthCaptureBatch) error {
	base, err := monthlysource.ReviewFundingMonth(ctx, p.Funding, nil)
	if err != nil {
		return err
	}
	if !validBatchWindow(p.Version, p.WindowSeconds) || p.GroupID <= 0 || len(p.Windows) == 0 || len(p.Windows) > 4096 || len(p.Windows)*len(p.Funding.UserIDs) > 150000 {
		return errInput
	}
	for i, a := range base.Accounts {
		if p.Funding.UserIDs[i] != a.UserID {
			return errInput
		}
	}
	cursor := base.From
	seen := map[string]bool{}
	for i, w := range p.Windows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if w.Job != fmt.Sprintf("window-%04d", i) || w.From != cursor || w.To-w.From != p.WindowSeconds || w.To > base.To {
			return errInput
		}
		m := batchManifest(p, w)
		if err := m.validate(time.Now()); err != nil {
			return err
		}
		for _, id := range []string{w.OlderScanID, w.NewerScanID} {
			if seen[id] {
				return errInput
			}
			seen[id] = true
		}
		raw, err := json.Marshal(m)
		if err != nil || hash(raw) != w.ManifestSHA256 {
			return errInput
		}
		cursor = w.To
	}
	if cursor != base.To {
		return errInput
	}
	return nil
}

func batchReceipt(p monthCaptureBatch) monthBatchReceipt {
	return monthBatchReceipt{GroupID: p.GroupID, Month: p.Funding.Month, Accounts: len(p.Funding.UserIDs), Windows: len(p.Windows), MinimumPageReads: 2 * len(p.Windows)}
}

func initializeMonthBatch(ctx context.Context, workspace, name string, p monthCaptureBatch, create bool) (monthBatchReceipt, error) {
	fail := monthBatchReceipt{}
	if err := validateMonthBatch(ctx, p); err != nil {
		return fail, err
	}
	root, path, err := jobPath(workspace, name)
	if err != nil {
		return fail, err
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return fail, errInput
	}
	raw, err := json.Marshal(p)
	if err != nil || len(raw) > 2<<20 {
		return fail, errInput
	}
	r := batchReceipt(p)
	r.Mode = "checked_month_schedule_no_source_or_files"
	if !create {
		return r, nil
	}
	lock, err := workspaceLock(root, true, true)
	if err != nil {
		return fail, err
	}
	defer lock.Close()
	if err := ctx.Err(); err != nil {
		return fail, err
	}
	if err := os.Mkdir(path, 0700); err != nil {
		return fail, err
	}
	pending := filepath.Join(path, "batch.pending.json")
	if err := exclusiveFile(pending, raw); err != nil {
		return fail, err
	}
	// Persist all random scan IDs BEFORE creating jobs. Interrupted creation
	// stays visibly incomplete; never regenerate a half-created batch in place.
	var seed emptyFundingSchema
	for _, w := range p.Windows {
		got, err := initializeWithEmptySchema(ctx, path, w.Job, batchManifest(p, w), true, &seed)
		if err != nil || got.ManifestSHA256 != w.ManifestSHA256 {
			return fail, errors.Join(errInput, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return fail, err
	}
	if err := syncPath(path); err != nil {
		return fail, err
	}
	if err := publishNewFile(pending, filepath.Join(path, "batch.json")); err != nil {
		return fail, err
	}
	if err := errors.Join(syncPath(path), syncPath(root)); err != nil {
		return fail, err
	}
	r.Mode, r.BatchSHA256 = "created_empty_month_batch_not_captured", hash(raw)
	return r, nil
}

func readMonthBatch(ctx context.Context, root, expected string) (monthCaptureBatch, error) {
	var p monthCaptureBatch
	if _, err := privateInfo(root, true); err != nil {
		return p, err
	}
	if _, err := os.Lstat(filepath.Join(root, "batch.pending.json")); !os.IsNotExist(err) {
		return p, errInput
	}
	raw, err := privateRead(filepath.Join(root, "batch.json"), 2<<20)
	if err != nil || !validSHA(expected) || hash(raw) != expected || json.Unmarshal(raw, &p) != nil {
		return monthCaptureBatch{}, errInput
	}
	canonical, err := json.Marshal(p)
	if err != nil || string(raw) != string(canonical) {
		return monthCaptureBatch{}, errInput
	}
	return p, validateMonthBatch(ctx, p)
}

type batchWindowCheckpoint struct {
	olderDone    bool
	newerDone    bool
	newerStarted bool
	olderEndMS   int64
	olderFence   int64
	newerStartMS int64
	newerFence   int64
	latestEndMS  int64
}

// Inspection reports persisted checkpoints only, not full scan integrity or
// financial readiness. The existing full monthly audit remains mandatory.
func inspectMonthBatch(ctx context.Context, workspace, name, expected string) (monthBatchReceipt, error) {
	fail := monthBatchReceipt{}
	_, root, err := jobPath(workspace, name)
	if err != nil {
		return fail, err
	}
	if _, err := privateInfo(root, true); err != nil {
		return fail, err
	}
	lock, err := workspaceLock(root, false, false)
	if err != nil {
		return fail, err
	}
	defer lock.Close()
	return inspectMonthBatchLocked(ctx, root, expected)
}

// Caller holds the batch workspace lock; also used by one bounded batch step.
func inspectMonthBatchLocked(ctx context.Context, root, expected string) (monthBatchReceipt, error) {
	fail := monthBatchReceipt{}
	p, err := readMonthBatch(ctx, root, expected)
	if err != nil {
		return fail, err
	}
	checks := make([]batchWindowCheckpoint, 0, len(p.Windows))
	for _, w := range p.Windows {
		if err := ctx.Err(); err != nil {
			return fail, err
		}
		c, err := inspectBatchWindow(ctx, root, w)
		if err != nil {
			return fail, err
		}
		checks = append(checks, c)
	}
	return monthBatchProgress(p, checks, expected, time.Now()), nil
}

// Internal reducer; checks are supplied only by the locked local inspection.
func monthBatchProgress(p monthCaptureBatch, checks []batchWindowCheckpoint, expected string, now time.Time) monthBatchReceipt {
	r := batchReceipt(p)
	r.Mode, r.BatchSHA256 = "local_checkpoint_progress_not_full_audit", expected
	var oldEnd, oldFence, newStart, newFence, latestEnd int64
	var nextOld, nextNew *monthBatchNext
	newStarted := false
	for i, w := range p.Windows {
		c := checks[i]
		if c.olderDone {
			r.OlderCompleted++
		} else if nextOld == nil {
			nextOld = &monthBatchNext{w.Job, w.ManifestSHA256, "older"}
		}
		if c.newerDone {
			r.NewerCompleted++
		} else if nextNew == nil {
			nextNew = &monthBatchNext{w.Job, w.ManifestSHA256, "newer"}
		}
		oldEnd, oldFence = max(oldEnd, c.olderEndMS), max(oldFence, c.olderFence)
		latestEnd = max(latestEnd, c.latestEndMS)
		if c.newerStarted {
			if !newStarted || c.newerStartMS < newStart {
				newStart = c.newerStartMS
			}
			if !newStarted || c.newerFence < newFence {
				newFence = c.newerFence
			}
			newStarted = true
		}
	}
	r.Phase = "blocked"
	if newStarted && (nextOld != nil || newStart <= oldEnd || newFence < oldFence) {
		r.BlockedReason = "two_month_sweeps_interleaved_or_fence_regressed"
		return r
	}
	if latestEnd > now.UnixMilli() {
		r.BlockedReason = "local_clock_before_retained_capture"
		return r
	}
	switch {
	case nextOld != nil:
		r.Phase, r.Next = "older", nextOld
	case nextNew != nil:
		r.Phase, r.Next = "newer", nextNew
	default:
		r.Phase = "checkpoints_finished_needs_full_audit"
	}
	return r
}

func inspectBatchWindow(ctx context.Context, root string, w monthCaptureWindow) (batchWindowCheckpoint, error) {
	fail := batchWindowCheckpoint{}
	dir := filepath.Join(root, w.Job)
	m, _, err := readManifest(dir, w.ManifestSHA256)
	if err != nil {
		return fail, err
	}
	db, err := openReceiver(ctx, filepath.Join(dir, "funding.sqlite"), false)
	if err != nil {
		return fail, err
	}
	defer db.Close()
	a, err := checkpoint(ctx, db, m.Older)
	if err != nil {
		return fail, err
	}
	b, err := checkpoint(ctx, db, m.Newer)
	if err != nil {
		return fail, err
	}
	r := batchWindowCheckpoint{}
	if a != nil {
		r.olderDone, r.olderEndMS, r.olderFence = a.EndReached, a.CompletedAtMS, a.FenceID
		r.latestEndMS = a.CompletedAtMS
	}
	if b != nil {
		// The LAST page's timestamp cannot stand in for this sweep's start.
		var raw []byte
		var id string
		if err := db.QueryRowContext(ctx, `SELECT id,CASE WHEN length(payload)<=2097152 THEN payload ELSE NULL END FROM portal_monthly_funding_pages WHERE scan_id=? AND sequence=1`, m.Newer.ID).Scan(&id, &raw); err != nil {
			return fail, err
		}
		var first monthlysource.FundingPage
		if json.Unmarshal(raw, &first) != nil || hash(raw) != id || !sameScan(first.Scan, m.Newer) || first.PreviousID != "" || first.After != (monthlysource.FundingCursor{}) || first.StartedAtMS <= 0 || first.CompletedAtMS < first.StartedAtMS || first.CompletedAtMS > b.CompletedAtMS || first.FenceID != b.FenceID {
			return fail, errInput
		}
		canonical, err := json.Marshal(first)
		if err != nil || string(raw) != string(canonical) {
			return fail, errInput
		}
		r.newerStarted, r.newerDone, r.newerStartMS, r.newerFence = true, b.EndReached, first.StartedAtMS, first.FenceID
		r.latestEndMS = max(r.latestEndMS, b.CompletedAtMS)
	}
	return r, nil
}
