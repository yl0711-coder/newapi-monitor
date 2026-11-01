package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

var ErrTokenMonthCaptureIntegrity = errors.New("token capture slot or projection differs from pinned month")
var ErrTokenMonthCaptureBudget = errors.New("token capture retained byte budget reached")

// Distinguish an absent original SLOT from a lost user-total projection.
// Both may wrap sql.ErrNoRows, but only the former permits explicit recovery.
var errTokenCaptureSlotMissing = errors.New("original token capture slot absent")

const tokenMonthCaptureBytes = 64 << 20

type TokenMonthCaptureProgress struct {
	Mode               string `json:"mode"`
	Completed          int    `json:"completed"`
	Total              int    `json:"total"`
	NextEligibleMS     int64  `json:"next_eligible_unix_ms,omitempty"`
	Captured           bool   `json:"captured"`
	TraversalsFinished bool   `json:"traversals_finished"`
}

// InitTokenMonthCaptureStore initializes only the private capture SQLite. The
// existing funding plan fixes current scope; no second roster/plan is invented.
func InitTokenMonthCaptureStore(parent context.Context, db *sql.DB) error {
	if parent == nil || db == nil {
		return ErrTokenMonthCaptureIntegrity
	}
	if err := InitUsageWindowStore(parent, db); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_token_capture_slots (
job_key TEXT NOT NULL CHECK(length(job_key)=64),ordinal INTEGER NOT NULL CHECK(ordinal>=0 AND ordinal<12400),
user_id INTEGER NOT NULL CHECK(user_id>0),sha256 TEXT NOT NULL CHECK(length(sha256)=64),usage_id TEXT NOT NULL CHECK(length(usage_id)=64),
started_at_ms INTEGER NOT NULL,completed_at_ms INTEGER NOT NULL,payload BLOB NOT NULL CHECK(length(payload)<=2097152),
PRIMARY KEY(job_key,ordinal))`,
		`CREATE TRIGGER IF NOT EXISTS monthly_token_capture_no_update BEFORE UPDATE ON portal_monthly_token_capture_slots BEGIN SELECT RAISE(ABORT,'token captures are immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_token_capture_no_delete BEFORE DELETE ON portal_monthly_token_capture_slots BEGIN SELECT RAISE(ABORT,'token captures are immutable'); END`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func tokenCaptureRequest(plan FundingMonthCapturePlan, ordinal int) (UsageWindowRequest, error) {
	n := len(plan.Scope.Funding.UserIDs)
	total := len(plan.Scans) * n
	if n == 0 || ordinal < 0 || ordinal >= total || total > 12400 {
		return UsageWindowRequest{}, ErrTokenMonthCaptureIntegrity
	}
	index := ordinal % (total / 2)
	scan := plan.Scans[index/n]
	return UsageWindowRequest{SourceIdentity: plan.Scope.Funding.SourceIdentity, Rules: plan.Scope.Funding.Rules,
		UserID: plan.Scope.Funding.UserIDs[index%n], From: scan.From, To: scan.To}, nil
}

// The inventory is only a local scheduling hint. It never asserts financial
// completeness. Original bytes and their atomic user-total projection are
// revalidated when loaded for the existing pair/month audit.
func tokenCaptureInventory(ctx context.Context, db walletRowReader, key string, total int, now time.Time) (TokenMonthCaptureProgress, error) {
	out := TokenMonthCaptureProgress{Mode: "token_and_user_observations_not_bill_approval", Total: total}
	var maxOrdinal, oldEnd, newStart, lastEnd, usedBytes int64
	err := db.QueryRowContext(ctx, `SELECT count(*),COALESCE(max(ordinal),-1),COALESCE(max(CASE WHEN ordinal<? THEN completed_at_ms END),0),COALESCE(min(CASE WHEN ordinal>=? THEN started_at_ms END),0),COALESCE(max(completed_at_ms),0),COALESCE(sum(length(payload)),0)
FROM portal_monthly_token_capture_slots WHERE job_key=?`, total/2, total/2, key).Scan(&out.Completed, &maxOrdinal, &oldEnd, &newStart, &lastEnd, &usedBytes)
	if err != nil {
		return out, err
	}
	if out.Completed > total || maxOrdinal != int64(out.Completed)-1 || lastEnd > now.UnixMilli() || (out.Completed > total/2 && newStart-oldEnd < 300000) {
		return TokenMonthCaptureProgress{}, ErrTokenMonthCaptureIntegrity
	}
	if usedBytes > tokenMonthCaptureBytes || (usedBytes == tokenMonthCaptureBytes && out.Completed < total) {
		return TokenMonthCaptureProgress{}, ErrTokenMonthCaptureBudget
	}
	if out.Completed >= total/2 && out.Completed < total && now.UnixMilli() < oldEnd+300000 {
		out.NextEligibleMS = oldEnd + 300000
	}
	out.TraversalsFinished = out.Completed == total
	return out, nil
}

func InspectTokenMonthCapture(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope) (TokenMonthCaptureProgress, error) {
	if parent == nil || receiver == nil {
		return TokenMonthCaptureProgress{}, ErrTokenMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	if err := rejectBoundTokenRecovery(ctx, receiver, key); err != nil {
		return TokenMonthCaptureProgress{}, err
	}
	plan, err := loadFundingMonthCapturePlan(ctx, receiver, key, scope, time.Now())
	if err != nil {
		return TokenMonthCaptureProgress{}, err
	}
	progress, err := tokenCaptureInventory(ctx, receiver, key, len(plan.Scans)*len(scope.Funding.UserIDs), time.Now())
	if err != nil {
		return TokenMonthCaptureProgress{}, err
	}
	if progress.Completed > progress.Total/2 {
		older, err := loadTokenCaptureSlot(ctx, receiver, plan, progress.Completed-1-progress.Total/2)
		if err != nil {
			return TokenMonthCaptureProgress{}, err
		}
		newer, err := loadTokenCaptureSlot(ctx, receiver, plan, progress.Completed-1)
		if err != nil || !TokenUsageWindowsMatch(older, newer) {
			return TokenMonthCaptureProgress{}, errors.Join(ErrTokenMonthCaptureIntegrity, err)
		}
	}
	return progress, nil
}

func loadTokenCaptureSlot(ctx context.Context, db walletRowReader, plan FundingMonthCapturePlan, ordinal int) (TokenUsageWindowObservation, error) {
	var o TokenUsageWindowObservation
	var raw []byte
	var sha, usageID string
	var user, started, completed int64
	err := db.QueryRowContext(ctx, `SELECT user_id,sha256,usage_id,started_at_ms,completed_at_ms,CASE WHEN length(payload)<=2097152 THEN payload END FROM portal_monthly_token_capture_slots WHERE job_key=? AND ordinal=?`, plan.Key, ordinal).Scan(&user, &sha, &usageID, &started, &completed, &raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return o, errors.Join(errTokenCaptureSlotMissing, err)
		}
		return o, err
	}
	want, err := tokenCaptureRequest(plan, ordinal)
	if err != nil || digest(raw) != sha || json.Unmarshal(raw, &o) != nil || ValidateTokenUsageWindowObservation(o) != nil || o.Request != want || o.Request.UserID != user || o.StartedAtMS != started || o.CompletedAtMS != completed {
		return TokenUsageWindowObservation{}, ErrTokenMonthCaptureIntegrity
	}
	canonical, _ := json.Marshal(o)
	projection, err := UsageObservationFromToken(o)
	if err != nil || !bytes.Equal(raw, canonical) {
		return TokenUsageWindowObservation{}, ErrTokenMonthCaptureIntegrity
	}
	projectedRaw, err := usageWindowBytes(projection)
	if err != nil || digest(projectedRaw) != usageID {
		return TokenUsageWindowObservation{}, ErrTokenMonthCaptureIntegrity
	}
	stored, err := loadUsageWindow(ctx, db, usageID)
	if err != nil || !bytes.Equal(stored, projectedRaw) {
		return TokenUsageWindowObservation{}, errors.Join(ErrTokenMonthCaptureIntegrity, err)
	}
	return o, nil
}

// Save both the ORIGINAL token capture and its exact SAME-TRANSACTION user
// totals projection atomically. An already-saved slot cannot be overwritten or
// silently repaired; changed bytes, lost projection or holes remain failures.
func saveTokenCaptureSlot(parent context.Context, receiver *sql.DB, plan FundingMonthCapturePlan, ordinal int, o TokenUsageWindowObservation) error {
	want, err := tokenCaptureRequest(plan, ordinal)
	if err != nil || o.Request != want || ValidateTokenUsageWindowObservation(o) != nil {
		return ErrTokenMonthCaptureIntegrity
	}
	u, err := UsageObservationFromToken(o)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(o)
	if err != nil {
		return err
	}
	projection, err := usageWindowBytes(u)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	tx, err := receiver.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_token_capture_slots(job_key,ordinal,user_id,sha256,usage_id,started_at_ms,completed_at_ms,payload)
SELECT ?,?,?,?,?,?,?,? WHERE (SELECT count(*) FROM portal_monthly_token_capture_slots WHERE job_key=?)=? AND (SELECT COALESCE(sum(length(payload)),0) FROM portal_monthly_token_capture_slots WHERE job_key=?)+?<=? ON CONFLICT(job_key,ordinal) DO NOTHING`,
		plan.Key, ordinal, o.Request.UserID, digest(raw), digest(projection), o.StartedAtMS, o.CompletedAtMS, raw, plan.Key, ordinal, plan.Key, len(raw), tokenMonthCaptureBytes)
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 1 {
		_, err = tx.ExecContext(ctx, `INSERT INTO portal_monthly_usage_observations(id,source_identity,user_id,window_from,window_to,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING`,
			digest(projection), u.Request.SourceIdentity, u.Request.UserID, u.Request.From, u.Request.To, u.StartedAtMS, u.CompletedAtMS, projection)
		if err != nil {
			return err
		}
	}
	stored, err := loadTokenCaptureSlot(ctx, tx, plan, ordinal)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			var used int64
			if readErr := tx.QueryRowContext(ctx, `SELECT COALESCE(sum(length(payload)),0) FROM portal_monthly_token_capture_slots WHERE job_key=?`, plan.Key).Scan(&used); readErr != nil {
				return readErr
			}
			if used+int64(len(raw)) > tokenMonthCaptureBytes {
				return ErrTokenMonthCaptureBudget
			}
		}
		return errors.Join(ErrTokenMonthCaptureIntegrity, err)
	}
	storedRaw, _ := json.Marshal(stored)
	if !bytes.Equal(storedRaw, raw) {
		return ErrTokenMonthCaptureIntegrity
	}
	return tx.Commit()
}

// CaptureNextTokenMonthSlot performs one existing one-user/day read-only
// capture, never a whole month in HTTP. A waiting second sweep does no source
// I/O and does not sleep with a database connection held.
func CaptureNextTokenMonthSlot(parent context.Context, source, receiver *sql.DB, key string, scope OrganizationMonthScope) (TokenMonthCaptureProgress, error) {
	if parent == nil || source == nil || receiver == nil || source == receiver {
		return TokenMonthCaptureProgress{}, ErrTokenMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	progress, err := InspectTokenMonthCapture(ctx, receiver, key, scope)
	if err != nil || progress.TraversalsFinished || progress.NextEligibleMS > time.Now().UnixMilli() {
		return progress, err
	}
	plan, err := loadFundingMonthCapturePlan(ctx, receiver, key, scope, time.Now())
	if err != nil {
		return progress, err
	}
	req, err := tokenCaptureRequest(plan, progress.Completed)
	if err != nil {
		return progress, err
	}
	// Pace the first/only capture as well. No transaction is held while waiting.
	if err := waitFundingInterval(ctx, time.Second); err != nil {
		return progress, err
	}
	connectCtx, connectCancel := context.WithTimeout(ctx, 4*time.Second)
	err = source.PingContext(connectCtx)
	connectCancel()
	if err != nil {
		return progress, err
	}
	o, err := CaptureTokenUsageWindow(ctx, source, req)
	if err != nil {
		return progress, err
	}
	if err := saveTokenCaptureSlot(ctx, receiver, plan, progress.Completed, o); err != nil {
		return progress, err
	}
	after, err := InspectTokenMonthCapture(ctx, receiver, key, scope)
	if err == nil {
		after.Captured = true
	}
	return after, err
}

// LoadCapturedTokenMonth validates one account's original pairs AND user
// totals projections before invoking the existing exact integer normalizer.
// It is local/read-only; it creates no archive, balances, approval or bill.
func LoadCapturedTokenMonth(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope, userID int64) ([]TokenUsageWindowPair, TokenUsageMonthReport, error) {
	var empty TokenUsageMonthReport
	if parent == nil || receiver == nil {
		return nil, empty, ErrTokenMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	plan, err := loadFundingMonthCapturePlan(ctx, receiver, key, scope, time.Now())
	if err != nil {
		return nil, empty, err
	}
	member := -1
	for i, id := range scope.Funding.UserIDs {
		if id == userID {
			member = i
			break
		}
	}
	if member < 0 {
		return nil, empty, ErrTokenMonthCaptureIntegrity
	}
	var count int
	var size int64
	if err := receiver.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(length(payload)),0) FROM portal_monthly_token_capture_slots WHERE job_key=? AND user_id=?`, key, userID).Scan(&count, &size); err != nil {
		return nil, empty, err
	}
	if count != len(plan.Scans) || size > 64<<20 {
		return nil, empty, ErrTokenMonthCaptureIntegrity
	}
	pairs := make([]TokenUsageWindowPair, 0, len(plan.Scans)/2)
	for day := 0; day < len(plan.Scans)/2; day++ {
		ordinal := day*len(scope.Funding.UserIDs) + member
		a, err := loadTokenCaptureSlot(ctx, receiver, plan, ordinal)
		if err != nil {
			return nil, empty, err
		}
		b, err := loadTokenCaptureSlot(ctx, receiver, plan, ordinal+len(plan.Scans)*len(scope.Funding.UserIDs)/2)
		if err != nil {
			return nil, empty, err
		}
		pairs = append(pairs, TokenUsageWindowPair{Older: a, Newer: b})
	}
	req := scope.Funding
	req.UserIDs = []int64{userID}
	report, err := SummarizeTokenUsageMonth(ctx, req, pairs, time.Now())
	if err != nil {
		return nil, empty, err
	}
	return pairs, report, nil
}
