package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

const fundingCapturePlanLimit = 1 << 20

var ErrFundingMonthCaptureIntegrity = errors.New("monthly funding capture plan or checkpoint differs")
var ErrFundingMonthCaptureCapacity = errors.New("monthly funding capture plan capacity reached")

// FundingMonthCapturePlan pins the current collection roster and independent
// scans BEFORE source reads. It is neither historical membership nor a bill.
type FundingMonthCapturePlan struct {
	Version int                    `json:"version"`
	Key     string                 `json:"key"`
	Scope   OrganizationMonthScope `json:"scope"`
	Scans   []FundingScan          `json:"scans"` // whole older sweep, then whole newer sweep
}

type FundingMonthCaptureProgress struct {
	Mode               string `json:"mode"`
	FinishedScans      int    `json:"finished_scans"`
	TotalScans         int    `json:"total_scans"`
	NextScanID         string `json:"next_scan_id,omitempty"`
	NextEligibleMS     int64  `json:"next_eligible_unix_ms,omitempty"`
	TraversalsFinished bool   `json:"traversals_finished"`
	Captured           bool   `json:"captured"`
}

// InitFundingMonthCaptureStore is explicit setup on a PRIVATE SQLite capture
// receiver. It is not invoked by construction, HTTP, or production startup.
func InitFundingMonthCaptureStore(parent context.Context, db *sql.DB) error {
	if parent == nil || db == nil {
		return ErrFundingMonthCaptureIntegrity
	}
	if err := InitFundingPageStore(parent, db); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, query := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_funding_capture_plans (
key TEXT PRIMARY KEY CHECK(length(key)=64),sha256 TEXT NOT NULL CHECK(length(sha256)=64),
payload BLOB NOT NULL CHECK(length(payload)<=1048576))`,
		`CREATE TRIGGER IF NOT EXISTS monthly_funding_capture_plan_no_update BEFORE UPDATE ON portal_monthly_funding_capture_plans BEGIN SELECT RAISE(ABORT,'capture plan is immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_funding_capture_plan_no_delete BEFORE DELETE ON portal_monthly_funding_capture_plans BEGIN SELECT RAISE(ABORT,'capture plan is immutable'); END`,
	} {
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func canonicalFundingCaptureScope(scope OrganizationMonthScope, now time.Time) ([]byte, error) {
	raw, err := json.Marshal(scope)
	if err != nil {
		return nil, err
	}
	if _, err := DecodeOrganizationMonthScope(raw, now); err != nil {
		return nil, err
	}
	return raw, nil
}

func decodeFundingMonthCapturePlan(raw []byte, key string, scope OrganizationMonthScope, now time.Time) (FundingMonthCapturePlan, error) {
	var plan FundingMonthCapturePlan
	if !validDigest(key) || len(raw) == 0 || len(raw) > fundingCapturePlanLimit || json.Unmarshal(raw, &plan) != nil {
		return plan, ErrFundingMonthCaptureIntegrity
	}
	canonical, _ := json.Marshal(plan)
	want, err := canonicalFundingCaptureScope(scope, now)
	got, scopeErr := canonicalFundingCaptureScope(plan.Scope, now)
	start, monthErr := time.ParseInLocation("2006-01", scope.Funding.Month, fundingMonthZone)
	if err != nil || scopeErr != nil || monthErr != nil || !bytes.Equal(canonical, raw) || !bytes.Equal(want, got) || plan.Key != key || plan.Version != 1 {
		return FundingMonthCapturePlan{}, ErrFundingMonthCaptureIntegrity
	}
	days := int(start.AddDate(0, 1, 0).Sub(start) / (24 * time.Hour))
	if len(plan.Scans) != 2*days {
		return FundingMonthCapturePlan{}, ErrFundingMonthCaptureIntegrity
	}
	seen := map[string]bool{}
	for i, scan := range plan.Scans {
		from := start.Unix() + int64(i%days)*86400
		ids, _ := json.Marshal(scan.UserIDs)
		expectedIDs, _ := json.Marshal(scope.Funding.UserIDs)
		if ValidateFundingScan(scan) != nil || scan.Version != 2 || scan.From != from || scan.To != from+86400 || scan.SourceIdentity != scope.Funding.SourceIdentity || scan.Rules != scope.Funding.Rules || !bytes.Equal(ids, expectedIDs) || seen[scan.ID] {
			return FundingMonthCapturePlan{}, ErrFundingMonthCaptureIntegrity
		}
		seen[scan.ID] = true
	}
	return plan, nil
}

type fundingCapturePlanReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadFundingMonthCapturePlan(ctx context.Context, db fundingCapturePlanReader, key string, scope OrganizationMonthScope, now time.Time) (FundingMonthCapturePlan, error) {
	var raw []byte
	var sha string
	err := db.QueryRowContext(ctx, `SELECT sha256,CASE WHEN length(payload)<=1048576 THEN payload END FROM portal_monthly_funding_capture_plans WHERE key=?`, key).Scan(&sha, &raw)
	if err != nil {
		return FundingMonthCapturePlan{}, err
	}
	if digest(raw) != sha {
		return FundingMonthCapturePlan{}, ErrFundingMonthCaptureIntegrity
	}
	return decodeFundingMonthCapturePlan(raw, key, scope, now)
}

// EnsureFundingMonthCapture reuses the original plan after restarts. It never
// replaces scan IDs to escape a bad checkpoint or contacts the MySQL source.
// Caller must already have verified current Usage authority and own this job.
func EnsureFundingMonthCapture(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope) (FundingMonthCapturePlan, error) {
	if parent == nil || receiver == nil || !validDigest(key) {
		return FundingMonthCapturePlan{}, ErrFundingMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	if _, err := canonicalFundingCaptureScope(scope, time.Now()); err != nil {
		return FundingMonthCapturePlan{}, err
	}
	plan, err := loadFundingMonthCapturePlan(ctx, receiver, key, scope, time.Now())
	if err == nil || !errors.Is(err, sql.ErrNoRows) {
		return plan, err
	}
	start, err := time.ParseInLocation("2006-01", scope.Funding.Month, fundingMonthZone)
	if err != nil {
		return plan, err
	}
	plan = FundingMonthCapturePlan{Version: 1, Key: key, Scope: scope}
	for range 2 {
		for at := start; at.Before(start.AddDate(0, 1, 0)); at = at.AddDate(0, 0, 1) {
			scan, err := NewFundingDayScan(scope.Funding.SourceIdentity, scope.Funding.Rules, scope.Funding.UserIDs, at.Unix(), at.AddDate(0, 0, 1).Unix())
			if err != nil {
				return FundingMonthCapturePlan{}, err
			}
			plan.Scans = append(plan.Scans, scan)
		}
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return FundingMonthCapturePlan{}, err
	}
	if _, err := decodeFundingMonthCapturePlan(raw, key, scope, time.Now()); err != nil {
		return FundingMonthCapturePlan{}, err
	}
	tx, err := receiver.BeginTx(ctx, nil)
	if err != nil {
		return FundingMonthCapturePlan{}, err
	}
	defer func() { _ = tx.Rollback() }()
	// This first statement reserves the writer, performs the capacity check
	// atomically, and never overwrites a concurrent creator's durable plan.
	_, err = tx.ExecContext(ctx, `INSERT INTO portal_monthly_funding_capture_plans(key,sha256,payload) SELECT ?,?,? WHERE (SELECT count(*) FROM portal_monthly_funding_capture_plans)<256 ON CONFLICT(key) DO NOTHING`, key, digest(raw), raw)
	if err != nil {
		return FundingMonthCapturePlan{}, err
	}
	plan, err = loadFundingMonthCapturePlan(ctx, tx, key, scope, time.Now())
	if errors.Is(err, sql.ErrNoRows) {
		return FundingMonthCapturePlan{}, ErrFundingMonthCaptureCapacity
	}
	if err != nil {
		return FundingMonthCapturePlan{}, err
	}
	if err := tx.Commit(); err != nil {
		return FundingMonthCapturePlan{}, err
	}
	return plan, nil
}

// InspectFundingMonthCapture is metadata-only and opens no source connection.
// Finished traversals still require the EXISTING full chain/month audit; a
// latest-page checkpoint is deliberately not called complete financial evidence.
func InspectFundingMonthCapture(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope) (FundingMonthCaptureProgress, error) {
	return inspectFundingMonthCapture(parent, receiver, key, scope, time.Now())
}

func inspectFundingMonthCapture(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope, now time.Time) (FundingMonthCaptureProgress, error) {
	var out FundingMonthCaptureProgress
	if parent == nil || receiver == nil {
		return out, ErrFundingMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	plan, err := loadFundingMonthCapturePlan(ctx, receiver, key, scope, now)
	if err != nil {
		return out, err
	}
	out.Mode, out.TotalScans = "funding_traversals_not_bill_approval", len(plan.Scans)
	var oldEnd int64
	missing := false
	for i, scan := range plan.Scans {
		page, err := LoadLatestFundingPage(ctx, receiver, scan.ID)
		if errors.Is(err, sql.ErrNoRows) {
			missing = true
			if out.NextScanID == "" {
				out.NextScanID = scan.ID
			}
			continue
		}
		if err != nil {
			return FundingMonthCaptureProgress{}, err
		}
		if !sameFundingScan(page.Scan, scan) || page.CompletedAtMS > now.UnixMilli() {
			return FundingMonthCaptureProgress{}, ErrFundingMonthCaptureIntegrity
		}
		// No scan may start before its predecessors finished. This detects a
		// mixed sweep or a missing earlier checkpoint instead of filling holes.
		if missing {
			return FundingMonthCaptureProgress{}, ErrFundingMonthCaptureIntegrity
		}
		if i < len(plan.Scans)/2 {
			if page.CompletedAtMS > oldEnd {
				oldEnd = page.CompletedAtMS
			}
		} else if page.StartedAtMS <= oldEnd {
			return FundingMonthCaptureProgress{}, ErrFundingMonthCaptureIntegrity
		}
		if !page.EndReached {
			missing = true
			out.NextScanID = scan.ID
		} else {
			out.FinishedScans++
		}
	}
	if out.FinishedScans >= len(plan.Scans)/2 && now.UnixMilli() <= oldEnd {
		out.NextEligibleMS = oldEnd + 1
	}
	out.TraversalsFinished = out.FinishedScans == len(plan.Scans)
	return out, nil
}

// CaptureNextFundingMonthPage performs at most ONE existing bounded read-only
// source page and saves its original cursor/fence atomically. Caller owns the
// on-demand queue lease; source and receiver handles are caller-owned. No retry,
// audit shortcut, collection-schema initialization or bill publication occurs.
func CaptureNextFundingMonthPage(parent context.Context, source, receiver *sql.DB, key string, scope OrganizationMonthScope) (FundingMonthCaptureProgress, error) {
	if parent == nil || source == nil || receiver == nil || source == receiver {
		return FundingMonthCaptureProgress{}, ErrFundingMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	progress, err := InspectFundingMonthCapture(ctx, receiver, key, scope)
	if err != nil || progress.TraversalsFinished || progress.NextEligibleMS > time.Now().UnixMilli() {
		return progress, err
	}
	plan, err := loadFundingMonthCapturePlan(ctx, receiver, key, scope, time.Now())
	if err != nil {
		return FundingMonthCaptureProgress{}, err
	}
	var selected *FundingScan
	for i := range plan.Scans {
		if plan.Scans[i].ID == progress.NextScanID {
			selected = &plan.Scans[i]
			break
		}
	}
	if selected == nil {
		return FundingMonthCaptureProgress{}, ErrFundingMonthCaptureIntegrity
	}
	// Cold connection preparation has its own unchanged four-second ceiling;
	// each source transaction remains capped at five seconds, each SQL at two.
	connectCtx, connectCancel := context.WithTimeout(ctx, 4*time.Second)
	err = source.PingContext(connectCtx)
	connectCancel()
	if err != nil {
		return progress, err
	}
	_, err = RunFundingCapture(ctx, source, receiver, *selected, FundingCaptureLimits{MaxPages: 1, Interval: time.Second, Timeout: 10 * time.Second})
	if err != nil {
		return progress, err
	}
	after, err := InspectFundingMonthCapture(ctx, receiver, key, scope)
	if err == nil {
		after.Captured = true
	}
	return after, err
}
