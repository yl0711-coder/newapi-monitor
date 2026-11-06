package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/big"
	"strconv"
	"time"
)

const usageWindowBasis = "database_view_wallet_and_usage_not_month_boundary"
const usageWindowTimeout = 5 * time.Second
const usageWindowRowsLimit = 10 // two log types, five explicit source classes
const usageWindowBytesLimit = 32768

// UsageWindowRequest selects one account and at most one day, not an unbounded
// monthly SUM. The source must have the reviewed user/time/type index. A source
// without that index fails; it is never retried with a table-scan fallback.
type UsageWindowRequest struct {
	SourceIdentity string       `json:"source_identity"`
	Rules          FundingRules `json:"rules"`
	UserID         int64        `json:"user_id"`
	From           int64        `json:"from"`
	To             int64        `json:"to"`
}

// UsageWindowRow retains source aggregates in exact integer quota. Refunds
// stay separate from consumption; no payment/FX calculation is performed.
// Unknown legacy billing sources stay explicit, never assumed to be wallet.
type UsageWindowRow struct {
	UserID        int64  `json:"user_id"`
	Type          int    `json:"type"`
	BillingSource string `json:"billing_source"`
	Records       int64  `json:"records"`
	Quota         int64  `json:"quota"`
	MinQuota      int64  `json:"min_quota"`
	MaxQuota      int64  `json:"max_quota"`
	MinID         int64  `json:"min_id"`
	MaxID         int64  `json:"max_id"`
	FirstAt       int64  `json:"first_at"`
	LastAt        int64  `json:"last_at"`
}

// UsageWindowObservation is a retained database view, NOT settled wallet
// boundaries or a proof of successful application wallet/refund writes. Even
// matching repeat captures cannot rule out unlogged writes or log retention.
// No Complete, WalletOnly, Reviewed, Month or publication flag is provided.
type UsageWindowObservation struct {
	Version       int                `json:"version"`
	Basis         string             `json:"basis"`
	Request       UsageWindowRequest `json:"request"`
	StartedAtMS   int64              `json:"started_at_ms"`
	CompletedAtMS int64              `json:"completed_at_ms"`
	Wallet        WalletUser         `json:"wallet"`
	Rows          []UsageWindowRow   `json:"rows"`
}

func validUsageWindowRequest(r UsageWindowRequest) bool {
	return validDigest(r.SourceIdentity) && r.Rules.Producer == FundingProducerRC4RC26 && r.Rules.QuotaPerUnit == 500000 &&
		r.UserID > 0 && r.From > 0 && r.To > r.From && r.To-r.From <= 86400
}

const usageBillingSourceSQL = `CASE
 WHEN other IS NULL OR TRIM(other)='' THEN 'unspecified'
 WHEN NOT JSON_VALID(other) THEN 'invalid_json'
 WHEN JSON_TYPE(other)<>'OBJECT' THEN 'invalid_source'
 WHEN BINARY JSON_EXTRACT(other,'$.billing_source')=BINARY '"wallet"' THEN 'wallet'
 WHEN BINARY JSON_EXTRACT(other,'$.billing_source')=BINARY '"subscription"' THEN 'subscription'
 WHEN JSON_EXTRACT(other,'$.billing_source') IS NULL THEN 'unspecified'
 ELSE 'invalid_source' END`

const usageWindowQuery = `SELECT /*+ MAX_EXECUTION_TIME(2000) */ user_id,type,
` + usageBillingSourceSQL + ` AS billing_source,
COUNT(*),COUNT(quota),CAST(SUM(quota) AS CHAR),MIN(quota),MAX(quota),MIN(id),MAX(id),MIN(created_at),MAX(created_at)
FROM logs FORCE INDEX(idx_user_created_type)
WHERE user_id=? AND created_at>=? AND created_at<? AND type IN (2,6)
GROUP BY user_id,type,billing_source ORDER BY user_id,type,billing_source LIMIT 11`

// CaptureUsageWindow reads the wallet and usage aggregates in the SAME bounded
// READ ONLY / REPEATABLE READ transaction. It shares the wallet capture slot,
// performs three SELECTs, never retries, and returns no partial observation.
// Construction/scheduling/persistence are deliberately separate and explicit.
func CaptureUsageWindow(parent context.Context, db *sql.DB, request UsageWindowRequest) (UsageWindowObservation, error) {
	return captureUsageWindow(parent, db, request, time.Now)
}

func captureUsageWindow(parent context.Context, db *sql.DB, request UsageWindowRequest, clock func() time.Time) (UsageWindowObservation, error) {
	var empty UsageWindowObservation
	if parent == nil || db == nil || !validUsageWindowRequest(request) {
		return empty, errors.New("invalid usage observation source, unit or one-account/day scope")
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
	started := clock().UnixMilli()
	if request.To > started/1000-60 {
		return empty, errors.New("usage window must have ended at least 60 seconds before capture")
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return empty, err
	}
	defer func() { _ = tx.Rollback() }()
	var uuid, database string
	if err := tx.QueryRowContext(ctx, "SELECT @@server_uuid, DATABASE()").Scan(&uuid, &database); err != nil {
		return empty, err
	}
	identity, err := WalletSourceIdentity(uuid, database)
	if err != nil || identity != request.SourceIdentity {
		return empty, errors.New("usage source differs from pinned identity")
	}
	walletRows, err := tx.QueryContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,quota,used_quota FROM users WHERE id=? ORDER BY id LIMIT 2", request.UserID)
	if err != nil {
		return empty, err
	}
	users, err := readWalletRows(walletRows, []int64{request.UserID})
	if err != nil {
		return empty, err
	}
	rows, err := tx.QueryContext(ctx, usageWindowQuery, request.UserID, request.From, request.To)
	if err != nil {
		return empty, err
	}
	observed, err := readUsageWindowRows(rows, request)
	if err != nil {
		return empty, err
	}
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	o := UsageWindowObservation{Version: 1, Basis: usageWindowBasis, Request: request, StartedAtMS: started,
		CompletedAtMS: clock().UnixMilli(), Wallet: users[0], Rows: observed}
	if err := ValidateUsageWindowObservation(o); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return o, nil
}

func readUsageWindowRows(rows *sql.Rows, request UsageWindowRequest) ([]UsageWindowRow, error) {
	defer rows.Close()
	result := []UsageWindowRow{}
	for rows.Next() {
		var r UsageWindowRow
		var known int64
		var quota string // SUM may exceed int64: the string is parsed exactly.
		if err := rows.Scan(&r.UserID, &r.Type, &r.BillingSource, &r.Records, &known, &quota, &r.MinQuota, &r.MaxQuota, &r.MinID, &r.MaxID, &r.FirstAt, &r.LastAt); err != nil {
			return nil, err
		}
		q, err := strconv.ParseInt(quota, 10, 64)
		if err != nil || strconv.FormatInt(q, 10) != quota || known != r.Records || !validUsageWindowRow(r, request) || len(result) >= usageWindowRowsLimit {
			return nil, errors.New("usage aggregate has null, invalid, oversized or out-of-scope records")
		}
		r.Quota = q
		if !validUsageWindowAmount(r) || (len(result) > 0 && !usageRowBefore(result[len(result)-1], r)) {
			return nil, errors.New("usage aggregate amount or ordering is inconsistent")
		}
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

func validUsageWindowRow(r UsageWindowRow, request UsageWindowRequest) bool {
	if r.UserID != request.UserID || (r.Type != 2 && r.Type != 6) || r.Records <= 0 || r.Records > 9007199254740991 ||
		r.MinQuota < 0 || r.MaxQuota < r.MinQuota || r.MinID <= 0 || r.MaxID < r.MinID || r.Records > r.MaxID-r.MinID+1 ||
		r.FirstAt < request.From || r.LastAt < r.FirstAt || r.LastAt >= request.To {
		return false
	}
	switch r.BillingSource {
	case "wallet", "subscription", "unspecified", "invalid_json", "invalid_source":
		return true
	default:
		return false
	}
}

func usageRowBefore(a, b UsageWindowRow) bool {
	return a.Type < b.Type || (a.Type == b.Type && a.BillingSource < b.BillingSource)
}

func validUsageWindowAmount(r UsageWindowRow) bool {
	q, count := big.NewInt(r.Quota), big.NewInt(r.Records)
	return r.Quota >= 0 && q.Cmp(new(big.Int).Mul(big.NewInt(r.MinQuota), count)) >= 0 &&
		q.Cmp(new(big.Int).Mul(big.NewInt(r.MaxQuota), count)) <= 0
}

// ValidateUsageWindowObservation checks structural integrity, not financial
// completeness, atomic wallet updates, historic directory or refund success.
func ValidateUsageWindowObservation(o UsageWindowObservation) error {
	if o.Version != 1 || o.Basis != usageWindowBasis || !validUsageWindowRequest(o.Request) ||
		o.StartedAtMS <= 0 || o.CompletedAtMS < o.StartedAtMS || o.CompletedAtMS-o.StartedAtMS > usageWindowTimeout.Milliseconds() ||
		o.Request.To > o.StartedAtMS/1000-60 || o.Wallet.UserID != o.Request.UserID || o.Rows == nil || len(o.Rows) > usageWindowRowsLimit ||
		(o.Wallet.Exists && (o.Wallet.BalanceQuota == nil || o.Wallet.UsedQuota == nil || *o.Wallet.UsedQuota < 0)) ||
		(!o.Wallet.Exists && (o.Wallet.BalanceQuota != nil || o.Wallet.UsedQuota != nil)) {
		return errors.New("invalid usage observation version, basis, scope, time or wallet")
	}
	for i, r := range o.Rows {
		if !validUsageWindowRow(r, o.Request) || !validUsageWindowAmount(r) || (i > 0 && !usageRowBefore(o.Rows[i-1], r)) {
			return errors.New("invalid or duplicate usage observation aggregate")
		}
	}
	return nil
}

func usageWindowBytes(o UsageWindowObservation) ([]byte, error) {
	if err := ValidateUsageWindowObservation(o); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(o)
	if err != nil || len(raw) > usageWindowBytesLimit {
		return nil, errors.New("usage observation exceeds storage limit")
	}
	return raw, nil
}
