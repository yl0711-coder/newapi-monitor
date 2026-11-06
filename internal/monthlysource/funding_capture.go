package monthlysource

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const FundingPageLimit = 200
const fundingPageBytesLimit = 1 << 20

// Bounds the WHOLE read-only transaction, including its network round trips
// and result transfer. Each business SELECT remains separately capped at 2s;
// this does not grant a five-second SQL or change row/page limits.
const fundingCaptureTimeout = 5 * time.Second

var fundingCaptureSlots = make(chan struct{}, 1)
var ErrFundingCaptureBusy = errors.New("monthly funding capture already running")

type FundingScan struct {
	Version        int          `json:"version"`
	ID             string       `json:"id"`
	SourceIdentity string       `json:"source_identity"`
	Rules          FundingRules `json:"rules"`
	UserIDs        []int64      `json:"user_ids"`
	From           int64        `json:"from"`
	To             int64        `json:"to"`
}

type FundingCursor struct {
	At int64 `json:"at"`
	ID int64 `json:"id"`
}

// EndReached means only this traversal reached its fence. Separate transactions
// do NOT make a month-wide snapshot: late inserts and corrections need a fresh
// scan and reconciliation. No Complete, Reviewed or publishable field exists.
type FundingPage struct {
	Version       int           `json:"version"`
	Scan          FundingScan   `json:"scan"`
	PreviousID    string        `json:"previous_id"`
	FenceID       int64         `json:"fence_id"`
	After         FundingCursor `json:"after"`
	Next          FundingCursor `json:"next"`
	EndReached    bool          `json:"end_reached"`
	StartedAtMS   int64         `json:"started_at_ms"`
	CompletedAtMS int64         `json:"completed_at_ms"`
	Rows          []FundingLog  `json:"rows"`
}

func NewFundingScan(source string, rules FundingRules, ids []int64, from, to int64) (FundingScan, error) {
	return newFundingScan(1, source, rules, ids, from, to)
}

// NewFundingDayScan explicitly opts into v2: exactly one closed UTC+8 day.
// V1 retains its one-hour limit and byte format. Pagination, source timeout,
// row/byte limits and independent reconciliation are unchanged for both.
func NewFundingDayScan(source string, rules FundingRules, ids []int64, from, to int64) (FundingScan, error) {
	return newFundingScan(2, source, rules, ids, from, to)
}

func newFundingScan(version int, source string, rules FundingRules, ids []int64, from, to int64) (FundingScan, error) {
	ordered, err := walletIDs(ids)
	if err != nil {
		return FundingScan{}, err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return FundingScan{}, err
	}
	s := FundingScan{Version: version, ID: hex.EncodeToString(random[:]), SourceIdentity: source, Rules: rules, UserIDs: ordered, From: from, To: to}
	if err := validateFundingScan(s); err != nil {
		return FundingScan{}, err
	}
	return s, nil
}

func validFundingScanID(id string) bool {
	_, err := hex.DecodeString(id)
	return len(id) == 32 && strings.ToLower(id) == id && err == nil
}

// ValidateFundingScan checks an already retained scan without creating another
// identity or performing I/O. Operator manifests must use the same boundaries
// as capture, not a separate, more permissive interpretation of the scope.
func ValidateFundingScan(s FundingScan) error { return validateFundingScan(s) }

func validateFundingScan(s FundingScan) error {
	ids, err := walletIDs(s.UserIDs)
	if ValidateFundingWindow(s.Version, s.From, s.To) != nil || !validFundingScanID(s.ID) || !validDigest(s.SourceIdentity) || s.Rules.Producer != FundingProducerRC4RC26 || s.Rules.QuotaPerUnit != 500000 || err != nil {
		return errors.New("invalid funding scan identity, rules, scope or versioned window")
	}
	for i, id := range ids {
		if id != s.UserIDs[i] {
			return errors.New("funding account scope must be canonical")
		}
	}
	return nil
}

// ValidateFundingWindow checks only versioned interval semantics, not source,
// identity, account scope or collection completeness. Plan validators that
// already verified those once can reuse these bounds without sorting the same
// 200 accounts again for every window in a retained monthly plan.
func ValidateFundingWindow(version int, from, to int64) error {
	if from > 0 && to > from && ((version == 1 && to-from <= 3600) ||
		(version == 2 && to-from == 86400 && from%86400 == 57600)) {
		return nil
	}
	return errors.New("invalid versioned funding interval")
}

func sameFundingScan(a, b FundingScan) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

func fundingCursorBefore(a, b FundingCursor) bool {
	return a.At < b.At || (a.At == b.At && a.ID < b.ID)
}

func fundingPageQuery(ids []int64) string {
	// Management records must not be filtered by the operator's user_id.
	// Conditional projections bound each field without silently truncating it.
	return fmt.Sprintf(`SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,user_id,created_at,type,
CASE WHEN OCTET_LENGTH(content)<=4096 THEN content ELSE NULL END,
CASE WHEN COALESCE(OCTET_LENGTH(other),0)<=65536 THEN COALESCE(other,'') ELSE NULL END
FROM logs WHERE created_at>=? AND created_at<? AND id<=?
AND (created_at>? OR (created_at=? AND id>?))
AND (type=3 OR (type IN (1,4) AND user_id IN (%s)))
ORDER BY created_at,id LIMIT 201`, strings.TrimSuffix(strings.Repeat("?,", len(ids)), ","))
}

func fundingDayPageQuery(ids []int64) string {
	// These disjoint, covering-index branches locate only IDs. At most the
	// globally earliest 201 bodies are fetched by primary key. A branch's 202nd
	// ID cannot enter the first 201 of their union. Do not filter type 3 by actor.
	// V2 requires these existing indexes; absence fails, never a broad fallback
	// scan or source schema change. The V1 query stays unchanged.
	return fmt.Sprintf(`SELECT /*+ MAX_EXECUTION_TIME(2000) */ l.id,l.user_id,l.created_at,l.type,
CASE WHEN OCTET_LENGTH(l.content)<=4096 THEN l.content ELSE NULL END,
CASE WHEN COALESCE(OCTET_LENGTH(l.other),0)<=65536 THEN COALESCE(l.other,'') ELSE NULL END
FROM (
SELECT id,created_at FROM (
(SELECT id,created_at FROM logs FORCE INDEX(idx_created_at_type)
WHERE created_at>=? AND created_at<? AND id<=?
AND (created_at>? OR (created_at=? AND id>?)) AND type=3
ORDER BY created_at,id LIMIT 201)
UNION ALL
(SELECT id,created_at FROM logs FORCE INDEX(idx_user_created_type)
WHERE created_at>=? AND created_at<? AND id<=?
AND (created_at>? OR (created_at=? AND id>?)) AND type IN (1,4) AND user_id IN (%s)
ORDER BY created_at,id LIMIT 201)
) AS branch_ids ORDER BY created_at,id LIMIT 201
) AS page_ids JOIN logs AS l ON l.id=page_ids.id
ORDER BY page_ids.created_at,page_ids.id`, strings.TrimSuffix(strings.Repeat("?,", len(ids)), ","))
}

func fundingQuery(scan FundingScan, fence int64, after FundingCursor) (string, []any) {
	args := []any{scan.From, scan.To, fence, after.At, after.At, after.ID}
	query := fundingPageQuery(scan.UserIDs)
	if scan.Version == 2 {
		args = append(args, args...)
		query = fundingDayPageQuery(scan.UserIDs)
	}
	for _, id := range scan.UserIDs {
		args = append(args, id)
	}
	return query, args
}

// CaptureFundingPage does bounded SELECTs on a pinned source, no retries or
// source writes. A previous page must have been durably saved before resuming;
// SaveFundingPage independently enforces that chain. Nothing schedules this.
func CaptureFundingPage(parent context.Context, db *sql.DB, scan FundingScan, previous *FundingPage) (FundingPage, error) {
	return captureFundingPage(parent, db, scan, previous, time.Now)
}

func captureFundingPage(parent context.Context, db *sql.DB, scan FundingScan, previous *FundingPage, clock func() time.Time) (_ FundingPage, captureErr error) {
	stage := "input"
	defer func() {
		if captureErr != nil {
			captureErr = &fundingCaptureFailure{stage: stage, cause: captureErr}
		}
	}()
	var empty FundingPage
	if db == nil || validateFundingScan(scan) != nil {
		return empty, errors.New("invalid funding source or scan")
	}
	scan.UserIDs = append([]int64(nil), scan.UserIDs...)
	p := FundingPage{Version: 1, Scan: scan, Rows: make([]FundingLog, 0)}
	if previous != nil {
		raw, err := fundingPageBytes(*previous)
		if err != nil || previous.EndReached || !sameFundingScan(scan, previous.Scan) {
			return empty, errors.New("invalid, terminal or different funding continuation")
		}
		p.PreviousID, p.FenceID, p.After = digest(raw), previous.FenceID, previous.Next
	}
	if err := parent.Err(); err != nil {
		return empty, err
	}
	select {
	case fundingCaptureSlots <- struct{}{}:
		defer func() { <-fundingCaptureSlots }()
	default:
		return empty, ErrFundingCaptureBusy
	}
	ctx, cancel := context.WithTimeout(parent, fundingCaptureTimeout)
	defer cancel()
	p.StartedAtMS = clock().UnixMilli()
	if scan.To > p.StartedAtMS/1000-60 {
		return empty, errors.New("funding window must be closed for at least 60 seconds")
	}
	stage = "transaction"
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return empty, err
	}
	defer func() { _ = tx.Rollback() }()
	var uuid, database string
	stage = "identity"
	// The first V2 page pins identity and its primary-key fence in one bounded
	// metadata SELECT. This saves an SSH/MySQL round trip, not safety budget.
	// Body rows are still unread until the identity is verified. Continuations
	// retain their original fence; V1 retains its exact metadata query sequence.
	if scan.Version == 2 && previous == nil {
		err = tx.QueryRowContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(2000) */ @@server_uuid, DATABASE(), (SELECT COALESCE(MAX(id),0) FROM logs)").Scan(&uuid, &database, &p.FenceID)
	} else {
		err = tx.QueryRowContext(ctx, "SELECT @@server_uuid, DATABASE()").Scan(&uuid, &database)
	}
	if err != nil {
		return empty, err
	}
	identity, err := WalletSourceIdentity(uuid, database)
	if err != nil || identity != scan.SourceIdentity {
		return empty, errors.New("funding source differs from pinned identity")
	}
	if previous == nil && scan.Version != 2 {
		// MAX(primary key) is an index lookup, not a monthly COUNT or offset scan.
		if err := tx.QueryRowContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(2000) */ COALESCE(MAX(id),0) FROM logs").Scan(&p.FenceID); err != nil {
			return empty, err
		}
	}
	if p.FenceID < 0 {
		return empty, errors.New("invalid funding log fence")
	}
	query, args := fundingQuery(scan, p.FenceID, p.After)
	stage = "source_rows"
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return empty, err
	}
	stage = "row_validation"
	observed, err := readFundingRows(rows, p)
	if err != nil {
		return empty, err
	}
	p.EndReached = len(observed) <= FundingPageLimit
	if !p.EndReached {
		observed = observed[:FundingPageLimit] // lookahead is re-read, not skipped
	}
	p.Rows, p.Next = observed, p.After
	if len(observed) > 0 {
		last := observed[len(observed)-1]
		p.Next = FundingCursor{At: last.CreatedAt, ID: last.ID}
	}
	stage = "commit"
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	p.CompletedAtMS = clock().UnixMilli()
	stage = "validation"
	if _, err := fundingPageBytes(p); err != nil {
		return empty, err
	}
	return p, nil
}

type fundingCaptureFailure struct {
	stage string
	cause error
}

func (e *fundingCaptureFailure) Error() string { return "funding capture failed at " + e.stage }
func (e *fundingCaptureFailure) Unwrap() error { return e.cause }

// FundingFailureStage is categorical only: never source row values or SQL.
func FundingFailureStage(err error) string {
	var failure *fundingCaptureFailure
	if !errors.As(err, &failure) {
		return ""
	}
	switch failure.stage {
	case "input", "transaction", "identity", "source_rows", "row_validation", "commit", "validation":
		return failure.stage
	default:
		return ""
	}
}

func readFundingRows(rows *sql.Rows, p FundingPage) ([]FundingLog, error) {
	defer rows.Close()
	result := make([]FundingLog, 0)
	last, size := p.After, 0
	seen := make(map[int64]bool)
	for rows.Next() {
		var row FundingLog
		var content, other sql.NullString
		if err := rows.Scan(&row.ID, &row.UserID, &row.CreatedAt, &row.Type, &content, &other); err != nil {
			return nil, err
		}
		row.Content, row.Other = content.String, other.String
		if !content.Valid || !other.Valid || !validFundingRow(row, p.Scan, p.FenceID, last) || seen[row.ID] || len(result) >= FundingPageLimit+1 {
			return nil, errors.New("invalid, duplicated, oversized or out-of-window funding row")
		}
		seen[row.ID] = true
		last = FundingCursor{At: row.CreatedAt, ID: row.ID}
		size += len(row.Content) + len(row.Other)
		if size > fundingPageBytesLimit {
			return nil, errors.New("funding page exceeds byte budget")
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

func fundingScopeHas(ids []int64, id int64) bool {
	for _, allowed := range ids {
		if id == allowed {
			return true
		}
	}
	return false
}

func validFundingRow(r FundingLog, s FundingScan, fence int64, after FundingCursor) bool {
	return r.ID > 0 && r.ID <= fence && r.UserID > 0 && r.CreatedAt >= s.From && r.CreatedAt < s.To &&
		fundingCursorBefore(after, FundingCursor{At: r.CreatedAt, ID: r.ID}) &&
		(r.Type == 3 || ((r.Type == 1 || r.Type == 4) && fundingScopeHas(s.UserIDs, r.UserID))) &&
		len(r.Content) <= 4096 && len(r.Other) <= 65536 && utf8.ValidString(r.Content) && utf8.ValidString(r.Other)
}

func fundingPageBytes(p FundingPage) ([]byte, error) {
	if validateFundingScan(p.Scan) != nil || p.Version != 1 || p.FenceID < 0 || p.StartedAtMS <= 0 || p.CompletedAtMS < p.StartedAtMS || p.CompletedAtMS-p.StartedAtMS > fundingCaptureTimeout.Milliseconds() || p.Scan.To > p.StartedAtMS/1000-60 || len(p.Rows) > FundingPageLimit || p.Rows == nil || (!p.EndReached && len(p.Rows) != FundingPageLimit) {
		return nil, errors.New("invalid funding page metadata or bounds")
	}
	if p.PreviousID == "" {
		if p.After != (FundingCursor{}) {
			return nil, errors.New("first funding page cannot skip a prefix")
		}
	} else if !validDigest(p.PreviousID) || p.After.ID <= 0 || p.After.ID > p.FenceID || p.After.At < p.Scan.From || p.After.At >= p.Scan.To {
		return nil, errors.New("invalid funding continuation cursor")
	}
	last, size := p.After, 0
	seen := make(map[int64]bool)
	for _, row := range p.Rows {
		if !validFundingRow(row, p.Scan, p.FenceID, last) || seen[row.ID] {
			return nil, errors.New("invalid funding page record")
		}
		seen[row.ID] = true
		last = FundingCursor{At: row.CreatedAt, ID: row.ID}
		size += len(row.Content) + len(row.Other)
	}
	if last != p.Next || size > fundingPageBytesLimit {
		return nil, errors.New("funding continuation or byte budget mismatch")
	}
	raw, err := json.Marshal(p)
	if err != nil || len(raw) > 2*fundingPageBytesLimit {
		return nil, errors.New("funding serialized page exceeds budget")
	}
	return raw, nil
}

type FundingProjection struct {
	Movements []FundingResult
	Review    []FundingResult
	Separate  []FundingResult
}

// ProjectFundingPage filters proven movements by beneficiary, NOT operator.
// Unknown targets remain internal review items, never auto-assigned or zeroed.
// It is not a bill and does not sum multiple observations/scans together.
func ProjectFundingPage(p FundingPage) (FundingProjection, error) {
	if _, err := fundingPageBytes(p); err != nil {
		return FundingProjection{}, err
	}
	var projection FundingProjection
	for _, row := range p.Rows {
		r := NormalizeFundingLog(row, p.Scan.Rules)
		switch r.Status {
		case FundingMovement:
			if fundingScopeHas(p.Scan.UserIDs, r.TargetUserID) {
				projection.Movements = append(projection.Movements, r)
			}
		case FundingReview:
			projection.Review = append(projection.Review, r)
		case FundingSeparate:
			projection.Separate = append(projection.Separate, r)
		}
	}
	return projection, nil
}
