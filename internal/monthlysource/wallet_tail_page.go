package monthlysource

// Bounded source primitive only. A page is NOT a complete wallet tail, a
// month-boundary balance, or permission to publish. No runtime installs it yet.
import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

const walletTailPageBasis = "same_read_view_wallet_and_bounded_index_page_not_complete_tail"
const walletTailPageRows = 8192

type WalletTailPageCursor struct {
	At   int64 `json:"at"`
	Type int64 `json:"type"`
	ID   int64 `json:"id"`
}

type WalletTailPageRequest struct {
	Wallet WalletTailRequest    `json:"wallet"`
	Kind   string               `json:"kind"` // user_usage or global_funding
	After  WalletTailPageCursor `json:"after"`
}

type WalletTailPageKindCount struct {
	Type    int64 `json:"type"`
	Records int   `json:"records"`
}

// The digest identifies the index rows actually read by the collector, not a
// monetary approval. Compact summaries avoid retaining millions of index IDs.
type WalletTailPageIndex struct {
	Records int                       `json:"records"`
	First   WalletTailPageCursor      `json:"first"`
	Next    WalletTailPageCursor      `json:"next"`
	Kinds   []WalletTailPageKindCount `json:"kinds"`
	SHA256  string                    `json:"sha256"`
}

type WalletTailPageObservation struct {
	Version        int                   `json:"version"`
	Basis          string                `json:"basis"`
	Request        WalletTailPageRequest `json:"request"`
	StartedAtMS    int64                 `json:"started_at_ms"`
	CompletedAtMS  int64                 `json:"completed_at_ms"`
	User           RegisteredWalletUser  `json:"user"`
	Index          WalletTailPageIndex   `json:"index"`
	ViewEndReached bool                  `json:"view_end_reached"` // THIS view only
	Usage          []UsageWindowRow      `json:"usage"`
	Funding        []FundingLog          `json:"funding"`
	// Candidate metadata may be reused for discovery, never as another user's money.
	FundingKeys             []WalletTailPageCursor `json:"funding_keys"`
	OtherUserFundingRecords int                    `json:"other_user_funding_records"`
}

func tailPageCursorBefore(a, b WalletTailPageCursor) bool {
	return a.At < b.At || a.At == b.At && (a.Type < b.Type || a.Type == b.Type && a.ID < b.ID)
}

func validTailPageRequest(r WalletTailPageRequest, ms int64) bool {
	a := r.After
	return validWalletTailRequest(r.Wallet, ms) && (r.Kind == "user_usage" || r.Kind == "global_funding") &&
		(a == (WalletTailPageCursor{}) || a.At >= r.Wallet.Boundary && a.At <= ms/1000 && a.Type >= 0 && a.ID > 0)
}

// Each disjoint range uses the actual index order (including InnoDB's appended
// primary key). Limit EACH branch before the small union sort. Never aggregate
// a whole month or apply LIMIT only after doing a month-wide SUM/filesort.
func tailPageIndexQuery(r WalletTailPageRequest) (string, []any) {
	index, prefix, args := "idx_created_at_type", "", []any{}
	if r.Kind == "user_usage" {
		index, prefix = "idx_user_created_type", "user_id=? AND "
		args = append(args, r.Wallet.UserID)
	}
	head := "SELECT id,created_at,type FROM logs FORCE INDEX(" + index + ") WHERE " + prefix
	order := fmt.Sprintf(" ORDER BY created_at,type,id LIMIT %d", walletTailPageRows+1)
	if r.After == (WalletTailPageCursor{}) {
		return "SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,created_at,type FROM (" + head + "created_at>=?" + order + ") AS index_page" + order, append(args, r.Wallet.Boundary)
	}
	a := r.After
	allArgs := append([]any{}, args...)
	allArgs = append(allArgs, a.At, a.Type, a.ID)
	allArgs = append(allArgs, args...)
	allArgs = append(allArgs, a.At, a.Type)
	allArgs = append(allArgs, args...)
	allArgs = append(allArgs, a.At)
	q := "SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,created_at,type FROM (" +
		"(" + head + "created_at=? AND type=? AND id>?" + order + ") UNION ALL " +
		"(" + head + "created_at=? AND type>?" + order + ") UNION ALL " +
		"(" + head + "created_at>?" + order + ")" +
		") AS index_page" + order
	return q, allArgs
}

func tailPageRelevant(kind string, typ int64) bool {
	return kind == "user_usage" && (typ == 2 || typ == 6) || kind == "global_funding" && (typ == 1 || typ == 3 || typ == 4)
}

func readTailPageIndex(rows *sql.Rows, r WalletTailPageRequest) ([]WalletTailPageCursor, bool, error) {
	defer rows.Close()
	index := []WalletTailPageCursor{}
	last := r.After
	seenIDs := map[int64]bool{}
	for rows.Next() {
		var id, at, typ registeredWalletInteger
		if err := rows.Scan(&id, &at, &typ); err != nil {
			return nil, false, err
		}
		next := WalletTailPageCursor{At: at.value, Type: typ.value, ID: id.value}
		if id.value <= 0 || seenIDs[id.value] || at.value < r.Wallet.Boundary || typ.value < 0 || !tailPageCursorBefore(last, next) || len(index) >= walletTailPageRows+1 {
			return nil, false, errors.New("invalid bounded tail index order, range or length")
		}
		index, last = append(index, next), next
		seenIDs[id.value] = true
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if err := rows.Close(); err != nil {
		return nil, false, err
	}
	keep := len(index)
	if keep > walletTailPageRows {
		keep = walletTailPageRows
	}
	// Existing funding decoder/body budgets are smaller. Do not advance past
	// unfetched relevant rows: stop at its first 200 candidates, not the 201st.
	if r.Kind == "global_funding" {
		seen := 0
		for i := 0; i < keep; i++ {
			if tailPageRelevant(r.Kind, index[i].Type) {
				seen++
				if seen > FundingPageLimit {
					keep = i
					break
				}
			}
		}
	}
	return index[:keep], keep == len(index), nil
}

func tailPageIndexSummary(index []WalletTailPageCursor, after WalletTailPageCursor) (WalletTailPageIndex, error) {
	counts := map[int64]int{}
	for _, row := range index {
		counts[row.Type]++
	}
	if len(counts) > 128 {
		return WalletTailPageIndex{}, errors.New("tail index type budget exceeded")
	}
	raw, err := json.Marshal(index)
	if err != nil {
		return WalletTailPageIndex{}, err
	}
	s := WalletTailPageIndex{Records: len(index), Next: after, Kinds: []WalletTailPageKindCount{}, SHA256: digest(raw)}
	if len(index) > 0 {
		s.First, s.Next = index[0], index[len(index)-1]
	}
	for typ, count := range counts {
		s.Kinds = append(s.Kinds, WalletTailPageKindCount{Type: typ, Records: count})
	}
	sort.Slice(s.Kinds, func(i, j int) bool { return s.Kinds[i].Type < s.Kinds[j].Type })
	return s, nil
}

// CaptureWalletTailPage performs at most one bounded index read and one
// primary-key data read, alongside the source identity and wallet in the SAME
// RR/READ ONLY view. It neither persists a cursor nor creates a monthly proof.
func CaptureWalletTailPage(parent context.Context, db *sql.DB, r WalletTailPageRequest) (WalletTailPageObservation, error) {
	return captureWalletTailPage(parent, db, r, time.Now)
}

func captureWalletTailPage(parent context.Context, db *sql.DB, r WalletTailPageRequest, clock func() time.Time) (_ WalletTailPageObservation, err error) {
	stage := "input"
	defer func() {
		if err != nil {
			err = &walletTailCaptureFailure{stage: stage, cause: err}
		}
	}()
	var empty WalletTailPageObservation
	if parent == nil || db == nil || clock == nil {
		return empty, errors.New("invalid bounded tail source")
	}
	started := clock().UnixMilli()
	if !validTailPageRequest(r, started) {
		return empty, errors.New("invalid bounded tail request")
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
	pin, err := WalletSourceIdentity(uuid, database)
	if err != nil || pin != r.Wallet.SourceIdentity {
		return empty, errors.New("bounded tail source identity changed")
	}
	stage = "registered_wallet"
	rows, err := tx.QueryContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,created_at,quota,used_quota FROM users WHERE id IN (?) ORDER BY id LIMIT 2", r.Wallet.UserID)
	if err != nil {
		return empty, err
	}
	users, err := readRegisteredWalletRows(rows, []int64{r.Wallet.UserID})
	if err != nil {
		return empty, err
	}
	o, err := readTailPageInView(ctx, tx, r, started, users[0], clock, &stage)
	if err != nil {
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

// Does not start/commit transactions, reacquire a wallet, reset the timeout or
// borrow a checkpoint's view. The owner supplies ONE already pinned read view.
func readTailPageInView(ctx context.Context, tx *sql.Tx, r WalletTailPageRequest, started int64, user RegisteredWalletUser, clock func() time.Time, stage *string) (WalletTailPageObservation, error) {
	var empty WalletTailPageObservation
	*stage = "tail_index"
	query, args := tailPageIndexQuery(r)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return empty, err
	}
	index, end, err := readTailPageIndex(rows, r)
	if err != nil {
		return empty, err
	}
	summary, err := tailPageIndexSummary(index, r.After)
	if err != nil {
		return empty, err
	}
	o := WalletTailPageObservation{Version: 1, Basis: walletTailPageBasis, Request: r, StartedAtMS: started, User: user, Index: summary, ViewEndReached: end,
		Usage: []UsageWindowRow{}, Funding: []FundingLog{}, FundingKeys: []WalletTailPageCursor{}}
	ids := []any{}
	selected := map[int64]WalletTailPageCursor{}
	for _, row := range index {
		if tailPageRelevant(r.Kind, row.Type) {
			ids = append(ids, row.ID)
			selected[row.ID] = row
			if r.Kind == "global_funding" {
				o.FundingKeys = append(o.FundingKeys, row)
			}
		}
	}
	if len(ids) > 0 {
		*stage = "tail_usage"
		if r.Kind == "global_funding" {
			*stage = "tail_funding"
		}
		query, args, err = tailPageBodyQuery(r, ids)
		if err != nil {
			return empty, err
		}
		rows, err = tx.QueryContext(ctx, query, args...)
		if err != nil {
			return empty, err
		}
		if r.Kind == "user_usage" {
			o.Usage, err = readUsageWindowRows(rows, UsageWindowRequest{UserID: r.Wallet.UserID, From: r.Wallet.Boundary, To: math.MaxInt64})
		} else {
			o.Funding, o.OtherUserFundingRecords, err = readTailPageFunding(rows, r, selected)
		}
		if err != nil {
			return empty, err
		}
	}
	*stage = "validation"
	o.CompletedAtMS = clock().UnixMilli()
	if err := validateCapturedTailPage(o, selected); err != nil {
		return empty, err
	}
	encoded, err := json.Marshal(o)
	if err != nil || len(encoded) > walletTailBytesLimit {
		return empty, errors.New("bounded tail page exceeds byte budget")
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return o, nil
}

func tailPageBodyQuery(r WalletTailPageRequest, ids []any) (string, []any, error) {
	if len(ids) == 0 || len(ids) > walletTailPageRows || r.Kind == "global_funding" && len(ids) > FundingPageLimit {
		return "", nil, errors.New("invalid bounded tail key count")
	}
	seen := map[int64]bool{}
	for _, id := range ids {
		n, ok := id.(int64)
		if !ok || n <= 0 || seen[n] {
			return "", nil, errors.New("invalid bounded tail primary key")
		}
		seen[n] = true
	}
	raw, err := json.Marshal(ids)
	if err != nil {
		return "", nil, err
	}
	// The bounded key table drives PRIMARY eq_ref lookups. Unlike a large IN
	// list, this cannot fall back to a month-wide scan when the range optimizer
	// has a low memory limit. This new primitive requires MySQL 8; the existing
	// source path is unchanged and no unsupported-engine fallback is attempted.
	from := `FROM JSON_TABLE(?, '$[*]' COLUMNS(page_id BIGINT PATH '$' ERROR ON EMPTY ERROR ON ERROR)) AS page_keys
STRAIGHT_JOIN logs FORCE INDEX(PRIMARY) ON logs.id=page_keys.page_id`
	if r.Kind == "user_usage" {
		return `SELECT /*+ MAX_EXECUTION_TIME(2000) */ user_id,type,` + usageBillingSourceSQL + ` AS billing_source,
COUNT(*),COUNT(quota),CAST(SUM(quota) AS CHAR),MIN(quota),MAX(quota),MIN(id),MAX(id),MIN(created_at),MAX(created_at)
` + from + ` WHERE user_id=? AND type IN (2,6)
GROUP BY user_id,type,billing_source ORDER BY user_id,type,billing_source LIMIT 11`, []any{string(raw), r.Wallet.UserID}, nil
	}
	// Return a presence-only header for unrelated topups: no other account's
	// identifier, content, amount or other JSON is returned. Every selected ID
	// must still be accounted for, rather than silently skipping a missing row.
	return `SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,
CASE WHEN type=3 OR user_id=? THEN user_id ELSE 0 END,created_at,type,
CASE WHEN type=3 OR user_id=? THEN CASE WHEN OCTET_LENGTH(content)<=4096 THEN content ELSE NULL END ELSE '' END,
CASE WHEN type=3 OR user_id=? THEN CASE WHEN COALESCE(OCTET_LENGTH(other),0)<=65536 THEN COALESCE(other,'') ELSE NULL END ELSE '' END
` + from + ` WHERE type IN (1,3,4)
ORDER BY created_at,id LIMIT 201`, []any{r.Wallet.UserID, r.Wallet.UserID, r.Wallet.UserID, string(raw)}, nil
}

func readTailPageFunding(rows *sql.Rows, r WalletTailPageRequest, selected map[int64]WalletTailPageCursor) ([]FundingLog, int, error) {
	defer rows.Close()
	out, seen, skipped, bytes := []FundingLog{}, map[int64]bool{}, 0, 0
	last := FundingCursor{}
	scan := FundingScan{UserIDs: []int64{r.Wallet.UserID}, From: r.Wallet.Boundary, To: math.MaxInt64}
	for rows.Next() {
		var row FundingLog
		var content, other sql.NullString
		if err := rows.Scan(&row.ID, &row.UserID, &row.CreatedAt, &row.Type, &content, &other); err != nil {
			return nil, 0, err
		}
		meta, ok := selected[row.ID]
		if !ok || seen[row.ID] || len(seen) >= FundingPageLimit || meta.At != row.CreatedAt || meta.Type != int64(row.Type) || !content.Valid || !other.Valid || !fundingCursorBefore(last, FundingCursor{At: row.CreatedAt, ID: row.ID}) {
			return nil, 0, errors.New("bounded funding body differs from selected index")
		}
		seen[row.ID], last = true, FundingCursor{At: row.CreatedAt, ID: row.ID}
		if row.UserID == 0 {
			if row.Type != 1 && row.Type != 4 || content.String != "" || other.String != "" {
				return nil, 0, errors.New("invalid unrelated-account header")
			}
			skipped++
			continue
		}
		row.Content, row.Other = content.String, other.String
		if !validFundingRow(row, scan, math.MaxInt64, FundingCursor{}) {
			return nil, 0, errors.New("invalid bounded funding body")
		}
		bytes += len(row.Content) + len(row.Other)
		if bytes > fundingPageBytesLimit {
			return nil, 0, errors.New("bounded funding body exceeds byte budget")
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if err := rows.Close(); err != nil {
		return nil, 0, err
	}
	if len(seen) != len(selected) {
		return nil, 0, errors.New("bounded funding body omitted selected index rows")
	}
	return out, skipped, nil
}

func validateCapturedTailPage(o WalletTailPageObservation, selected map[int64]WalletTailPageCursor) error {
	if o.CompletedAtMS < o.StartedAtMS || o.CompletedAtMS-o.StartedAtMS > usageWindowTimeout.Milliseconds() || walletTailMonthStart(o.CompletedAtMS) != o.Request.Wallet.Boundary ||
		o.Index.Next.At > o.CompletedAtMS/1000 || o.User.RegisteredAt > o.CompletedAtMS/1000 {
		return errors.New("bounded tail observation time is invalid")
	}
	counts := map[int64]int64{}
	dates := map[int64]map[int64]bool{}
	for _, row := range selected {
		counts[row.Type]++
		if dates[row.Type] == nil {
			dates[row.Type] = map[int64]bool{}
		}
		dates[row.Type][row.At] = true
	}
	for _, row := range o.Usage {
		lo, lok := selected[row.MinID]
		hi, hik := selected[row.MaxID]
		if !lok || !hik || lo.Type != int64(row.Type) || hi.Type != int64(row.Type) || !dates[int64(row.Type)][row.FirstAt] || !dates[int64(row.Type)][row.LastAt] || row.LastAt > o.CompletedAtMS/1000 || row.FirstAt < o.User.RegisteredAt {
			return errors.New("bounded tail usage does not match selected IDs")
		}
		if row.Records > counts[int64(row.Type)] {
			return errors.New("bounded usage exceeds its index record count")
		}
		counts[int64(row.Type)] -= row.Records
	}
	if o.Request.Kind == "user_usage" && (counts[2] != 0 || counts[6] != 0) {
		return errors.New("bounded tail usage count differs from index page")
	}
	for _, row := range o.Funding {
		meta, ok := selected[row.ID]
		if !ok || meta.At != row.CreatedAt || meta.Type != int64(row.Type) || row.CreatedAt > o.CompletedAtMS/1000 {
			return errors.New("bounded tail funding does not match selected IDs")
		}
		delete(selected, row.ID)
	}
	// All type-3 rows must be returned, irrespective of actor. Other users'
	// type-1/4 rows may legitimately be absent from this account's body result.
	if o.Request.Kind == "global_funding" {
		for _, row := range selected {
			if row.Type == 3 {
				return errors.New("bounded tail omitted management row")
			}
		}
	}
	return nil
}
