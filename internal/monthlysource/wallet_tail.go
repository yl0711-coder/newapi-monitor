package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"time"
)

const walletTailBasis = "same_read_view_wallet_and_visible_tail_not_settled_boundary"
const walletTailRowsLimit = 2000
const walletTailBytesLimit = 2 << 20

// WalletTailRequest deliberately supports only the start of the current
// Beijing month. Older boundaries need independently collected closed-month
// bridges, not an unbounded lifetime scan against the production database.
type WalletTailRequest struct {
	SourceIdentity string       `json:"source_identity"`
	Rules          FundingRules `json:"rules"`
	UserID         int64        `json:"user_id"`
	Boundary       int64        `json:"boundary"`
}

// WalletTailObservation retains original visible rows, aggregates and physical
// capture times in ONE read-only repeatable-read view. Neither matching views
// nor a calculated boundary proves atomic application writes or log retention.
// This format grants no permission to publish and is not a WalletObservation.
type WalletTailObservation struct {
	Version       int                  `json:"version"`
	Basis         string               `json:"basis"`
	Request       WalletTailRequest    `json:"request"`
	StartedAtMS   int64                `json:"started_at_ms"`
	CompletedAtMS int64                `json:"completed_at_ms"`
	User          RegisteredWalletUser `json:"user"`
	Usage         []UsageWindowRow     `json:"usage"`
	Funding       []FundingLog         `json:"funding"`
}

func walletTailMonthStart(ms int64) int64 {
	t := time.UnixMilli(ms).In(time.FixedZone("UTC+8", 8*3600))
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).Unix()
}

func validWalletTailRequest(r WalletTailRequest, ms int64) bool {
	return ms > 0 && validDigest(r.SourceIdentity) && r.Rules.Producer == FundingProducerRC4RC26 &&
		r.Rules.QuotaPerUnit == 500000 && r.UserID > 0 && r.Boundary > 0 &&
		r.Boundary == walletTailMonthStart(ms) && r.Boundary <= ms/1000-60
}

// No wall-clock upper bound: a row in the capture's current second may already
// be visible alongside the wallet. Cutting it off would change the equation.
// Indexed access and the whole five-second transaction still bound the read.
const walletTailUsageQuery = `SELECT /*+ MAX_EXECUTION_TIME(2000) */ user_id,type,
` + usageBillingSourceSQL + ` AS billing_source,
COUNT(*),COUNT(quota),CAST(SUM(quota) AS CHAR),MIN(quota),MAX(quota),MIN(id),MAX(id),MIN(created_at),MAX(created_at)
FROM logs FORCE INDEX(idx_user_created_type)
WHERE user_id=? AND created_at>=? AND type IN (2,6)
GROUP BY user_id,type,billing_source ORDER BY user_id,type,billing_source LIMIT 11`

// Management rows include all actors: their target may be a different user.
// Each disjoint branch finds only IDs using the reviewed existing indexes.
// Keyset pages remain in the SAME snapshot; no fence from another transaction.
const walletTailFundingQuery = `SELECT /*+ MAX_EXECUTION_TIME(2000) */ l.id,l.user_id,l.created_at,l.type,
CASE WHEN OCTET_LENGTH(l.content)<=4096 THEN l.content ELSE NULL END,
CASE WHEN COALESCE(OCTET_LENGTH(l.other),0)<=65536 THEN COALESCE(l.other,'') ELSE NULL END
FROM (
SELECT id,created_at FROM (
(SELECT id,created_at FROM logs FORCE INDEX(idx_created_at_type)
WHERE created_at>=? AND (created_at>? OR (created_at=? AND id>?)) AND type=3
ORDER BY created_at,id LIMIT 201)
UNION ALL
(SELECT id,created_at FROM logs FORCE INDEX(idx_user_created_type)
WHERE created_at>=? AND (created_at>? OR (created_at=? AND id>?)) AND type IN (1,4) AND user_id=?
ORDER BY created_at,id LIMIT 201)
) AS branch_ids ORDER BY created_at,id LIMIT 201
) AS page_ids JOIN logs AS l ON l.id=page_ids.id
ORDER BY page_ids.created_at,page_ids.id`

// CaptureWalletTail starts no worker, retries nothing and writes no database.
// It shares the existing wallet lane. Every SQL is capped at two seconds and
// the entire view at five; a missing index/error/limit yields no partial data.
func CaptureWalletTail(parent context.Context, db *sql.DB, r WalletTailRequest) (WalletTailObservation, error) {
	return captureWalletTail(parent, db, r, time.Now)
}

func captureWalletTail(parent context.Context, db *sql.DB, r WalletTailRequest, clock func() time.Time) (_ WalletTailObservation, err error) {
	stage := "input"
	defer func() {
		if err != nil {
			err = &walletTailCaptureFailure{stage: stage, cause: err}
		}
	}()
	var empty WalletTailObservation
	if parent == nil || db == nil || clock == nil {
		return empty, errors.New("invalid wallet tail capture source")
	}
	started := clock().UnixMilli()
	if !validWalletTailRequest(r, started) {
		return empty, errors.New("invalid wallet tail source, unit or current-month boundary")
	}
	if err := parent.Err(); err != nil {
		return empty, err
	}
	select {
	case walletCaptureSlots <- struct{}{}:
		defer func() { <-walletCaptureSlots }()
	default:
		return empty, ErrWalletCaptureBusy
	}
	ctx, cancel := context.WithTimeout(parent, usageWindowTimeout)
	defer cancel()
	stage = "transaction"
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return empty, err
	}
	defer func() { _ = tx.Rollback() }()
	stage = "identity"
	var uuid, database string
	if err := tx.QueryRowContext(ctx, "SELECT @@server_uuid, DATABASE()").Scan(&uuid, &database); err != nil {
		return empty, err
	}
	identity, err := WalletSourceIdentity(uuid, database)
	if err != nil || identity != r.SourceIdentity {
		return empty, errors.New("wallet tail source identity changed")
	}
	stage = "registered_wallet"
	rows, err := tx.QueryContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,created_at,quota,used_quota FROM users WHERE id IN (?) ORDER BY id LIMIT 2", r.UserID)
	if err != nil {
		return empty, err
	}
	users, err := readRegisteredWalletRows(rows, []int64{r.UserID})
	if err != nil {
		return empty, err
	}
	stage = "tail_usage"
	rows, err = tx.QueryContext(ctx, walletTailUsageQuery, r.UserID, r.Boundary)
	if err != nil {
		return empty, err
	}
	// This is only a transport validator, not a fabricated observation cutoff.
	usage, err := readUsageWindowRows(rows, UsageWindowRequest{UserID: r.UserID, From: r.Boundary, To: math.MaxInt64})
	if err != nil {
		return empty, err
	}
	stage = "tail_funding"
	funding, err := readWalletTailFunding(ctx, tx, r)
	if err != nil {
		return empty, err
	}
	o := WalletTailObservation{Version: 1, Basis: walletTailBasis, Request: r, StartedAtMS: started,
		CompletedAtMS: clock().UnixMilli(), User: users[0], Usage: usage, Funding: funding}
	stage = "validation"
	if err := ValidateWalletTailObservation(o); err != nil {
		return empty, err
	}
	stage = "commit"
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return o, nil
}

type walletTailCaptureFailure struct {
	stage string
	cause error
}

func (e *walletTailCaptureFailure) Error() string { return "wallet tail capture failed at " + e.stage }
func (e *walletTailCaptureFailure) Unwrap() error { return e.cause }

// Categorical diagnostics only. Preserve timeout/driver classification through
// Unwrap without exposing credentials, source row contents or SQL to callers.
func WalletTailFailureStage(err error) string {
	var failure *walletTailCaptureFailure
	if !errors.As(err, &failure) {
		return ""
	}
	switch failure.stage {
	case "input", "transaction", "identity", "registered_wallet", "tail_index", "tail_usage", "tail_funding", "validation", "commit":
		return failure.stage
	default:
		return ""
	}
}

func readWalletTailFunding(ctx context.Context, tx *sql.Tx, r WalletTailRequest) ([]FundingLog, error) {
	result := []FundingLog{}
	after, size := FundingCursor{}, 0
	for {
		args := []any{r.Boundary, after.At, after.At, after.ID, r.Boundary, after.At, after.At, after.ID, r.UserID}
		rows, err := tx.QueryContext(ctx, walletTailFundingQuery, args...)
		if err != nil {
			return nil, err
		}
		page, err := readFundingRows(rows, FundingPage{Scan: FundingScan{UserIDs: []int64{r.UserID}, From: r.Boundary, To: math.MaxInt64}, FenceID: math.MaxInt64, After: after})
		if err != nil {
			return nil, err
		}
		n := len(page)
		if n > FundingPageLimit {
			page = page[:FundingPageLimit]
		}
		for _, row := range page {
			size += len(row.Content) + len(row.Other)
		}
		if len(result)+len(page) > walletTailRowsLimit || size > walletTailBytesLimit {
			return nil, errors.New("wallet tail funding exceeds observation budget")
		}
		result = append(result, page...)
		if n <= FundingPageLimit {
			return result, nil
		}
		after = FundingCursor{At: page[len(page)-1].CreatedAt, ID: page[len(page)-1].ID}
	}
}

func ValidateWalletTailObservation(o WalletTailObservation) error {
	if o.Version != 1 || o.Basis != walletTailBasis || !validWalletTailRequest(o.Request, o.StartedAtMS) ||
		o.CompletedAtMS < o.StartedAtMS || o.CompletedAtMS-o.StartedAtMS > usageWindowTimeout.Milliseconds() ||
		walletTailMonthStart(o.CompletedAtMS) != o.Request.Boundary || o.User.UserID != o.Request.UserID ||
		o.User.RegisteredAt <= 0 || o.User.RegisteredAt > o.CompletedAtMS/1000 || o.User.BalanceQuota == nil ||
		o.User.UsedQuota == nil || *o.User.UsedQuota < 0 || o.Usage == nil || len(o.Usage) > usageWindowRowsLimit ||
		o.Funding == nil || len(o.Funding) > walletTailRowsLimit {
		return errors.New("invalid wallet tail source, scope, time or wallet")
	}
	u := UsageWindowRequest{UserID: o.Request.UserID, From: o.Request.Boundary, To: math.MaxInt64}
	for i, row := range o.Usage {
		if !validUsageWindowRow(row, u) || !validUsageWindowAmount(row) || row.LastAt > o.CompletedAtMS/1000 ||
			row.FirstAt < o.User.RegisteredAt || (i > 0 && !usageRowBefore(o.Usage[i-1], row)) {
			return errors.New("invalid wallet tail usage source rows")
		}
	}
	scan := FundingScan{UserIDs: []int64{o.Request.UserID}, From: o.Request.Boundary, To: math.MaxInt64}
	after, size := FundingCursor{}, 0
	seen := make(map[int64]bool)
	for _, row := range o.Funding {
		if !validFundingRow(row, scan, math.MaxInt64, after) || seen[row.ID] || row.CreatedAt > o.CompletedAtMS/1000 {
			return errors.New("invalid wallet tail funding source rows")
		}
		seen[row.ID], after = true, FundingCursor{At: row.CreatedAt, ID: row.ID}
		size += len(row.Content) + len(row.Other)
		if size > walletTailBytesLimit {
			return errors.New("wallet tail funding exceeds byte budget")
		}
	}
	return nil
}
