package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"
)

const fundingAuditRowsLimit = 20000
const fundingAuditBytesLimit = 32 << 20

var fundingAuditSlots = make(chan struct{}, 1)
var ErrFundingAuditBusy = errors.New("monthly funding audit already running")

type fundingObservedRecord struct {
	digest, pageID, status string
	target                 int64
	targetKnown            bool
	at, delta              int64
	hasDelta               bool
	kind, method           string
}

type fundingAuditedScan struct {
	scan            FundingScan
	firstMS, lastMS int64
	fence           int64
	headID          string
	pages           int
	records         map[int64]fundingObservedRecord
}

// FundingScanComparison describes observations, NOT complete financial evidence.
// Two matching traversals cannot prove retained history, atomic wallet writes,
// unlogged operations, or exact month-boundary balances. No source text escapes.
type FundingScanComparison struct {
	OlderScanID           string                     `json:"older_scan_id"`
	NewerScanID           string                     `json:"newer_scan_id"`
	OlderHeadID           string                     `json:"older_head_id"`
	NewerHeadID           string                     `json:"newer_head_id"`
	SourceIdentity        string                     `json:"source_identity"`
	From                  int64                      `json:"from"`
	To                    int64                      `json:"to"`
	OlderRows             int                        `json:"older_rows"`
	NewerRows             int                        `json:"newer_rows"`
	ObservationsMatch     bool                       `json:"observations_match"`
	Changes               []FundingObservedChange    `json:"changes"`
	Accounts              []FundingAccountComparison `json:"accounts"`
	UnscopedChangedLogIDs []int64                    `json:"unscoped_changed_log_ids"`
	UnscopedReviewLogIDs  []int64                    `json:"unscoped_review_log_ids"`
}

type FundingObservedChange struct {
	LogID           int64   `json:"log_id"`
	Kind            string  `json:"kind"` // inserted, changed, absent_in_new_scan
	AffectedUserIDs []int64 `json:"affected_user_ids"`
	ScopeUncertain  bool    `json:"scope_uncertain"`
}

type FundingAccountComparison struct {
	UserID               int64   `json:"user_id"`
	ObservationsMatch    bool    `json:"observations_match"`
	ChangedLogIDs        []int64 `json:"changed_log_ids"`
	ReviewLogIDs         []int64 `json:"review_log_ids"`
	SeparateLedgerLogIDs []int64 `json:"separate_ledger_log_ids"`
}

// CompareFundingScans uses only the local observation store, never a source DB.
// Each scan is revalidated in a short read-only transaction before comparison.
// All errors discard the result; callers cannot use partial coverage. One audit
// per process, 10 seconds total, <=20,000 rows / 32 MiB per scan. Larger windows
// must be re-collected with narrower bounds, not silently truncated.
func CompareFundingScans(parent context.Context, db *sql.DB, olderID, newerID string) (FundingScanComparison, error) {
	observation, err := ReviewFundingWindow(parent, db, olderID, newerID)
	if err != nil {
		return FundingScanComparison{}, err
	}
	return observation.Comparison(), nil
}

// ReviewFundingWindow retains an opaque, in-memory observation for subsequent
// month assembly. Unlike a JSON comparison report, it cannot be fabricated by
// setting ObservationsMatch=true. It is still NOT financial completeness proof.
func ReviewFundingWindow(parent context.Context, db *sql.DB, olderID, newerID string) (FundingWindowObservation, error) {
	if db == nil || !validFundingScanID(olderID) || !validFundingScanID(newerID) || olderID == newerID {
		return FundingWindowObservation{}, errors.New("two independent funding scans and receiver required")
	}
	if err := parent.Err(); err != nil {
		return FundingWindowObservation{}, err
	}
	select {
	case fundingAuditSlots <- struct{}{}:
		defer func() { <-fundingAuditSlots }()
	default:
		return FundingWindowObservation{}, ErrFundingAuditBusy
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	a, err := auditFundingScan(ctx, db, olderID)
	if err != nil {
		return FundingWindowObservation{}, err
	}
	b, err := auditFundingScan(ctx, db, newerID)
	if err != nil {
		return FundingWindowObservation{}, err
	}
	result, err := compareFundingAudits(ctx, a, b)
	if err != nil {
		return FundingWindowObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return FundingWindowObservation{}, err
	}
	return newFundingWindowObservation(a, b, result), nil
}

func auditFundingScan(parent context.Context, db *sql.DB, scanID string) (fundingAuditedScan, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fundingAuditedScan{}, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `SELECT id,sequence,CASE WHEN length(payload)<=2097152 THEN payload ELSE NULL END
FROM portal_monthly_funding_pages WHERE scan_id=? ORDER BY sequence LIMIT 1001`, scanID)
	if err != nil {
		return fundingAuditedScan{}, err
	}
	a, err := readFundingChain(ctx, rows, scanID)
	if err != nil {
		return fundingAuditedScan{}, err
	}
	// Check the append-only dedup index too; missing/stale/extra identities must
	// not be hidden by valid page hashes. Both reads share this local snapshot.
	index, err := tx.QueryContext(ctx, `SELECT log_id,page_id,row_digest FROM portal_monthly_funding_records WHERE scan_id=? ORDER BY log_id LIMIT 20001`, scanID)
	if err != nil {
		return fundingAuditedScan{}, err
	}
	if err := checkFundingRecordIndex(ctx, index, a.records); err != nil {
		return fundingAuditedScan{}, err
	}
	if err := tx.Commit(); err != nil {
		return fundingAuditedScan{}, err
	}
	return a, nil
}

func readFundingChain(ctx context.Context, rows *sql.Rows, scanID string) (fundingAuditedScan, error) {
	defer rows.Close()
	a := fundingAuditedScan{records: make(map[int64]fundingObservedRecord)}
	var previous FundingPage
	var size int
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return fundingAuditedScan{}, err
		}
		var id string
		var sequence int
		var raw []byte
		if err := rows.Scan(&id, &sequence, &raw); err != nil {
			return fundingAuditedScan{}, err
		}
		size += len(raw)
		if sequence != a.pages+1 || sequence > 1000 || size > fundingAuditBytesLimit || digest(raw) != id {
			return fundingAuditedScan{}, errors.New("funding chain sequence, hash or byte budget failure")
		}
		var p FundingPage
		if json.Unmarshal(raw, &p) != nil || p.Scan.ID != scanID {
			return fundingAuditedScan{}, errors.New("funding chain source identity mismatch")
		}
		canonical, err := fundingPageBytes(p)
		if err != nil || !bytes.Equal(canonical, raw) {
			return fundingAuditedScan{}, errors.New("funding chain noncanonical page")
		}
		if sequence == 1 {
			if p.PreviousID != "" || p.After != (FundingCursor{}) {
				return fundingAuditedScan{}, errors.New("funding chain prefix missing")
			}
			a.scan, a.firstMS, a.fence = p.Scan, p.StartedAtMS, p.FenceID
		} else if previous.EndReached || p.PreviousID != a.headID || !sameFundingScan(a.scan, p.Scan) || p.FenceID != a.fence || p.After != previous.Next || p.StartedAtMS < a.lastMS {
			return fundingAuditedScan{}, errors.New("funding chain fork, skipped page or changed scope")
		}
		for _, row := range p.Rows {
			if _, duplicate := a.records[row.ID]; duplicate || len(a.records) >= fundingAuditRowsLimit {
				return fundingAuditedScan{}, errors.New("funding chain duplicate identity or row budget failure")
			}
			r := NormalizeFundingLog(row, a.scan.Rules)
			target, known := fundingAffectedTarget(row, r)
			record := fundingObservedRecord{digest: r.SourceDigest, pageID: id, status: r.Status, target: target, targetKnown: known, at: r.At, kind: r.Kind, method: r.Method}
			if r.DeltaQuota != nil {
				record.delta, record.hasDelta = *r.DeltaQuota, true
			}
			a.records[row.ID] = record
		}
		a.pages, a.headID, a.lastMS, previous = sequence, id, p.CompletedAtMS, p
	}
	if err := rows.Err(); err != nil {
		return fundingAuditedScan{}, err
	}
	if err := rows.Close(); err != nil {
		return fundingAuditedScan{}, err
	}
	if a.pages == 0 || !previous.EndReached {
		return fundingAuditedScan{}, errors.New("funding traversal absent or unfinished")
	}
	return a, nil
}

func checkFundingRecordIndex(ctx context.Context, rows *sql.Rows, records map[int64]fundingObservedRecord) error {
	defer rows.Close()
	count, lastID := 0, int64(0)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var id int64
		var pageID, rowDigest string
		if err := rows.Scan(&id, &pageID, &rowDigest); err != nil {
			return err
		}
		record, found := records[id]
		if !found || id <= lastID || record.pageID != pageID || record.digest != rowDigest || count >= fundingAuditRowsLimit {
			return errors.New("funding record index differs from page chain")
		}
		count, lastID = count+1, id
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if count != len(records) {
		return errors.New("funding record index missing identities")
	}
	return nil
}

// Resolve ONLY the account affected by a review item, not its amount. Known
// quota actions with an invalid amount can still have an unambiguous target.
// Unknown actions / ambiguous JSON remain unscoped; never fall back to actor.
func fundingAffectedTarget(row FundingLog, r FundingResult) (int64, bool) {
	if r.TargetUserID > 0 {
		return r.TargetUserID, true
	}
	if row.Type == 1 || row.Type == 4 {
		return row.UserID, true
	}
	if row.Type != 3 {
		return 0, false
	}
	envelope, ok := fundingObject(row.Other, true)
	if !ok {
		return 0, false
	}
	for key := range envelope {
		if strings.EqualFold(key, "op") && key != "op" {
			return 0, false
		}
	}
	raw, structured := envelope["op"]
	if !structured {
		if strings.HasPrefix(row.Content, "管理员增加用户额度 ") || strings.HasPrefix(row.Content, "管理员减少用户额度 ") || fundingOverride.MatchString(row.Content) {
			return row.UserID, true
		}
		return 0, false
	}
	op, ok := fundingObject(string(raw), false)
	if !ok || len(op) != 2 {
		return 0, false
	}
	action, ok := fundingString(op["action"])
	if !ok || (action != "user.quota_add" && action != "user.quota_subtract" && action != "user.quota_override") {
		return 0, false
	}
	params, ok := fundingObject(string(op["params"]), false)
	if !ok {
		return 0, false
	}
	for key := range params {
		if key != "target_user_id" && !((action == "user.quota_override" && (key == "from" || key == "to")) || (action != "user.quota_override" && key == "quota")) {
			return 0, false
		}
	}
	return fundingTarget(row.UserID, envelope, params)
}

func compareFundingAudits(ctx context.Context, a, b fundingAuditedScan) (FundingScanComparison, error) {
	comparable := b.scan
	comparable.ID = a.scan.ID
	if a.scan.ID == b.scan.ID || !sameFundingScan(a.scan, comparable) || b.firstMS <= a.lastMS || b.fence < a.fence {
		return FundingScanComparison{}, errors.New("funding observations require same source/scope/window, sequential independent scans and nonregressing fence")
	}
	r := FundingScanComparison{OlderScanID: a.scan.ID, NewerScanID: b.scan.ID, OlderHeadID: a.headID, NewerHeadID: b.headID, SourceIdentity: a.scan.SourceIdentity, From: a.scan.From, To: a.scan.To, OlderRows: len(a.records), NewerRows: len(b.records), ObservationsMatch: true}
	accounts := make(map[int64]*FundingAccountComparison)
	for _, id := range a.scan.UserIDs {
		r.Accounts = append(r.Accounts, FundingAccountComparison{UserID: id, ObservationsMatch: true})
	}
	for i := range r.Accounts {
		accounts[r.Accounts[i].UserID] = &r.Accounts[i]
	}
	ids := make([]int64, 0, len(a.records)+len(b.records))
	for id := range a.records {
		ids = append(ids, id)
	}
	for id := range b.records {
		if _, exists := a.records[id]; !exists {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return FundingScanComparison{}, err
		}
		old, oldExists := a.records[id]
		newer, newExists := b.records[id]
		if newExists {
			account := accounts[newer.target]
			if newer.status == FundingReview {
				if !newer.targetKnown {
					r.UnscopedReviewLogIDs = append(r.UnscopedReviewLogIDs, id)
				} else if account != nil {
					account.ReviewLogIDs = append(account.ReviewLogIDs, id)
				}
			} else if newer.status == FundingSeparate && account != nil {
				account.SeparateLedgerLogIDs = append(account.SeparateLedgerLogIDs, id)
			}
		}
		if oldExists && newExists && old.digest == newer.digest {
			continue
		}
		r.ObservationsMatch = false
		change := FundingObservedChange{LogID: id, Kind: "changed"}
		if !oldExists {
			change.Kind = "inserted"
		} else if !newExists {
			// Absence could mean deletion, moved timestamp or retention cleanup;
			// do not claim a DELETE operation without source evidence.
			change.Kind = "absent_in_new_scan"
		}
		affected := make(map[int64]bool)
		for _, v := range []struct {
			record fundingObservedRecord
			exists bool
		}{{old, oldExists}, {newer, newExists}} {
			if !v.exists {
				continue
			}
			if v.record.targetKnown {
				if accounts[v.record.target] != nil {
					affected[v.record.target] = true
				}
			} else if v.record.status != FundingNonWallet {
				change.ScopeUncertain = true
			}
		}
		for _, accountID := range a.scan.UserIDs {
			if affected[accountID] {
				change.AffectedUserIDs = append(change.AffectedUserIDs, accountID)
				accounts[accountID].ChangedLogIDs = append(accounts[accountID].ChangedLogIDs, id)
				accounts[accountID].ObservationsMatch = false
			}
		}
		if change.ScopeUncertain {
			r.UnscopedChangedLogIDs = append(r.UnscopedChangedLogIDs, id)
		}
		r.Changes = append(r.Changes, change)
	}
	if len(r.UnscopedChangedLogIDs) > 0 {
		for i := range r.Accounts {
			// Do not assert account stability while a changed record's target
			// is unknown. Keep it unscoped rather than inventing attribution.
			r.Accounts[i].ObservationsMatch = false
		}
	}
	return r, nil
}
