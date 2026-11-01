package monthlysource

// Separate opt-in observations: no change to existing usage capture payloads,
// receivers, schedulers or bill publication. Names are display metadata from
// the largest retained log ID in each group, never from the live tokens table.
import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"
)

const TokenUsageObservationLimit = 2 << 20
const tokenUsageRowsLimit = 2000
const tokenUsageBasis = "database_view_wallet_usage_and_token_groups_not_month_boundary"

var (
	errTokenObservationScope     = errors.New("invalid token observation scope, timing or totals")
	errTokenObservationTiming    = errors.New("token observation exceeds time limit")
	errTokenObservationRow       = errors.New("invalid or duplicate token observation row")
	errTokenObservationEndpoint  = errors.New("one retained log ID belongs to multiple token groups")
	errTokenObservationOverflow  = errors.New("token observation total overflow")
	errTokenObservationMismatch  = errors.New("token groups differ from independently queried user totals")
	errTokenObservationByteLimit = errors.New("token observation exceeds byte limit")
)

type TokenUsageWindowRow struct {
	UsageWindowRow
	TokenID int64  `json:"token_id"`
	Name    string `json:"name"`
}

type TokenUsageWindowObservation struct {
	Version       int                   `json:"version"`
	Basis         string                `json:"basis"`
	Request       UsageWindowRequest    `json:"request"`
	StartedAtMS   int64                 `json:"started_at_ms"`
	CompletedAtMS int64                 `json:"completed_at_ms"`
	Wallet        WalletUser            `json:"wallet"`
	Totals        []UsageWindowRow      `json:"totals"`
	Rows          []TokenUsageWindowRow `json:"rows"`
}

// Diagnostic stages contain no source values or SQL. Preserve the underlying
// error for cancellation/driver classification without exposing it to users.
type tokenUsageCaptureFailure struct {
	stage string
	cause error
}

func (e *tokenUsageCaptureFailure) Error() string { return "token usage capture failed at " + e.stage }
func (e *tokenUsageCaptureFailure) Unwrap() error { return e.cause }

func TokenUsageFailureStage(err error) string {
	var failure *tokenUsageCaptureFailure
	if !errors.As(err, &failure) {
		return ""
	}
	switch failure.stage {
	case "input", "transaction", "identity", "wallet", "user_totals", "token_groups", "validation", "commit":
		return failure.stage
	default:
		return ""
	}
}

// Fixed diagnostic codes only: never expose SQL, credentials, token names,
// user IDs or financial values. A timing rejection is not a money mismatch.
func TokenUsageFailureReason(err error) string {
	for _, r := range []struct {
		err  error
		code string
	}{
		{errTokenObservationScope, "scope_or_totals"},
		{errTokenObservationTiming, "observation_timing"},
		{errTokenObservationRow, "token_row"},
		{errTokenObservationEndpoint, "overlapping_log_groups"},
		{errTokenObservationOverflow, "aggregate_overflow"},
		{errTokenObservationMismatch, "group_totals_mismatch"},
		{errTokenObservationByteLimit, "observation_byte_limit"},
	} {
		if errors.Is(err, r.err) {
			return r.code
		}
	}
	return ""
}

// Aggregate first inside an indexed one-user/day range, then primary-key join
// to the retained name. No GROUP_CONCAT truncation, name-based grouping,
// current-token lookup or request-body/key field is used. Null legacy IDs are
// explicitly bucketed as 0; negative IDs are not silently normalized.
const tokenUsageWindowQuery = `SELECT /*+ MAX_EXECUTION_TIME(2000) */
g.user_id,g.type,g.billing_source,g.records,g.known,g.quota,g.min_quota,g.max_quota,g.min_id,g.max_id,g.first_at,g.last_at,g.token_id,COALESCE(l.token_name,'')
FROM (
 SELECT user_id,type,` + usageBillingSourceSQL + ` AS billing_source,
 COUNT(*) AS records,COUNT(quota) AS known,CAST(SUM(quota) AS CHAR) AS quota,
 MIN(quota) AS min_quota,MAX(quota) AS max_quota,MIN(id) AS min_id,MAX(id) AS max_id,
 MIN(created_at) AS first_at,MAX(created_at) AS last_at,COALESCE(token_id,0) AS token_id
 FROM logs FORCE INDEX(idx_user_created_type)
 WHERE user_id=? AND created_at>=? AND created_at<? AND type IN (2,6)
 GROUP BY user_id,COALESCE(token_id,0),type,billing_source
 ORDER BY token_id,type,billing_source LIMIT 2001
) g JOIN logs l ON l.id=g.max_id AND l.user_id=g.user_id AND COALESCE(l.token_id,0)=g.token_id AND l.type=g.type
ORDER BY g.token_id,g.type,g.billing_source`

func CaptureTokenUsageWindow(parent context.Context, db *sql.DB, request UsageWindowRequest) (TokenUsageWindowObservation, error) {
	return captureTokenUsageWindow(parent, db, request, time.Now)
}

func captureTokenUsageWindow(parent context.Context, db *sql.DB, request UsageWindowRequest, clock func() time.Time) (_ TokenUsageWindowObservation, captureErr error) {
	stage := "input"
	defer func() {
		if captureErr != nil {
			captureErr = &tokenUsageCaptureFailure{stage: stage, cause: captureErr}
		}
	}()
	var empty TokenUsageWindowObservation
	if parent == nil || db == nil || !validUsageWindowRequest(request) || clock == nil {
		return empty, errors.New("invalid token usage source or one-account/day scope")
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
		return empty, errors.New("token usage window is not closed")
	}
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
	if err != nil || identity != request.SourceIdentity {
		return empty, errors.New("token usage source differs from pinned identity")
	}
	stage = "wallet"
	walletRows, err := tx.QueryContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,quota,used_quota FROM users WHERE id=? ORDER BY id LIMIT 2", request.UserID)
	if err != nil {
		return empty, err
	}
	users, err := readWalletRows(walletRows, []int64{request.UserID})
	if err != nil {
		return empty, err
	}
	stage = "user_totals"
	rows, err := tx.QueryContext(ctx, usageWindowQuery, request.UserID, request.From, request.To)
	if err != nil {
		return empty, err
	}
	totals, err := readUsageWindowRows(rows, request)
	if err != nil {
		return empty, err
	}
	stage = "token_groups"
	rows, err = tx.QueryContext(ctx, tokenUsageWindowQuery, request.UserID, request.From, request.To)
	if err != nil {
		return empty, err
	}
	tokens, err := readTokenUsageWindowRows(rows, request)
	if err != nil {
		return empty, err
	}
	o := TokenUsageWindowObservation{Version: 1, Basis: tokenUsageBasis, Request: request, StartedAtMS: started, CompletedAtMS: clock().UnixMilli(), Wallet: users[0], Totals: totals, Rows: tokens}
	stage = "validation"
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := ValidateTokenUsageWindowObservation(o); err != nil {
		return empty, err
	}
	stage = "commit"
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	o.CompletedAtMS = clock().UnixMilli()
	stage = "validation"
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := ValidateTokenUsageWindowObservation(o); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return o, nil
}

func readTokenUsageWindowRows(rows *sql.Rows, request UsageWindowRequest) ([]TokenUsageWindowRow, error) {
	defer rows.Close()
	result := []TokenUsageWindowRow{}
	for rows.Next() {
		var r TokenUsageWindowRow
		var known int64
		var quota string
		if err := rows.Scan(&r.UserID, &r.Type, &r.BillingSource, &r.Records, &known, &quota, &r.MinQuota, &r.MaxQuota, &r.MinID, &r.MaxID, &r.FirstAt, &r.LastAt, &r.TokenID, &r.Name); err != nil {
			return nil, err
		}
		q, err := strconv.ParseInt(quota, 10, 64)
		if err != nil || strconv.FormatInt(q, 10) != quota || known != r.Records || len(result) >= tokenUsageRowsLimit {
			return nil, errors.New("invalid or oversized token aggregates")
		}
		r.Quota = q
		if !validTokenUsageRow(r, request) || (len(result) > 0 && !tokenUsageRowBefore(result[len(result)-1], r)) {
			return nil, errors.New("invalid or unordered token aggregates")
		}
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func validTokenUsageRow(r TokenUsageWindowRow, request UsageWindowRequest) bool {
	return validUsageWindowRow(r.UsageWindowRow, request) && validUsageWindowAmount(r.UsageWindowRow) && r.TokenID >= 0 && r.TokenID <= 9007199254740991 && utf8.ValidString(r.Name) && len(r.Name) <= 512
}

func tokenUsageRowBefore(a, b TokenUsageWindowRow) bool {
	return a.TokenID < b.TokenID || (a.TokenID == b.TokenID && usageRowBefore(a.UsageWindowRow, b.UsageWindowRow))
}

func ValidateTokenUsageWindowObservation(o TokenUsageWindowObservation) error {
	if o.StartedAtMS > 0 && o.CompletedAtMS >= o.StartedAtMS && o.CompletedAtMS-o.StartedAtMS > usageWindowTimeout.Milliseconds() {
		return errTokenObservationTiming
	}
	base := UsageWindowObservation{Version: 1, Basis: usageWindowBasis, Request: o.Request, StartedAtMS: o.StartedAtMS, CompletedAtMS: o.CompletedAtMS, Wallet: o.Wallet, Rows: o.Totals}
	if o.Version != 1 || o.Basis != tokenUsageBasis || o.Rows == nil || len(o.Rows) > tokenUsageRowsLimit || ValidateUsageWindowObservation(base) != nil {
		return errTokenObservationScope
	}
	bySource := map[string]UsageWindowRow{}
	endpointOwners := map[int64]string{}
	for i, r := range o.Rows {
		if !validTokenUsageRow(r, o.Request) || (i > 0 && !tokenUsageRowBefore(o.Rows[i-1], r)) {
			return errTokenObservationRow
		}
		key := strconv.Itoa(r.Type) + ":" + r.BillingSource
		owner := strconv.FormatInt(r.TokenID, 10) + ":" + key
		for _, id := range []int64{r.MinID, r.MaxID} {
			if previous, ok := endpointOwners[id]; ok && previous != owner {
				return errTokenObservationEndpoint
			}
			endpointOwners[id] = owner
		}
		v, exists := bySource[key]
		if !exists {
			bySource[key] = r.UsageWindowRow
			continue
		}
		if r.Records > 9007199254740991-v.Records || r.Quota > 9223372036854775807-v.Quota {
			return errTokenObservationOverflow
		}
		v.Records += r.Records
		v.Quota += r.Quota
		v.MinQuota = min(v.MinQuota, r.MinQuota)
		v.MaxQuota = max(v.MaxQuota, r.MaxQuota)
		v.MinID = min(v.MinID, r.MinID)
		v.MaxID = max(v.MaxID, r.MaxID)
		v.FirstAt = min(v.FirstAt, r.FirstAt)
		v.LastAt = max(v.LastAt, r.LastAt)
		bySource[key] = v
	}
	combined := make([]UsageWindowRow, 0, len(bySource))
	for _, r := range bySource {
		combined = append(combined, r)
	}
	sort.Slice(combined, func(i, j int) bool { return usageRowBefore(combined[i], combined[j]) })
	if !reflect.DeepEqual(combined, o.Totals) {
		return errTokenObservationMismatch
	}
	raw, err := json.Marshal(o)
	if err != nil || len(raw) > TokenUsageObservationLimit {
		return errTokenObservationByteLimit
	}
	return nil
}

// Compare retained log groups, not the moving live wallet. Exact repeat reads
// establish stability of those observations, not log-retention completeness,
// successful wallet flushes, historical membership, or publication approval.
func TokenUsageWindowsMatch(older, newer TokenUsageWindowObservation) bool {
	return ValidateTokenUsageWindowObservation(older) == nil && ValidateTokenUsageWindowObservation(newer) == nil && older.Request == newer.Request && newer.StartedAtMS-older.CompletedAtMS >= 300000 && reflect.DeepEqual(older.Totals, newer.Totals) && reflect.DeepEqual(older.Rows, newer.Rows)
}
