package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"sort"
	"time"
)

// CaptureWalletTailPaged is the bounded source reader used by opt-in monthly
// generation. Unlike joining retained page views, ALL reads here belong to one
// fresh source transaction, including the wallet and both log streams. It does
// not use a retained prefix as current source evidence or extend any timeout.
func CaptureWalletTailPaged(parent context.Context, source *sql.DB, r WalletTailRequest) (WalletTailObservation, error) {
	return captureWalletTailPaged(parent, source, r, time.Now, walletTailJournalPages)
}

type pagedTailUsageTotal struct {
	row   UsageWindowRow
	quota *big.Int
}

func foldPagedTailUsage(totals map[string]*pagedTailUsageTotal, rows []UsageWindowRow) error {
	for _, row := range rows {
		key := row.BillingSource
		if row.Type == 6 {
			key = "refund:" + key
		}
		t, exists := totals[key]
		if !exists {
			if len(totals) >= usageWindowRowsLimit {
				return errors.New("paged tail exceeds aggregate class budget")
			}
			totals[key] = &pagedTailUsageTotal{row: row, quota: big.NewInt(row.Quota)}
			continue
		}
		if t.row.UserID != row.UserID || t.row.Type != row.Type || t.row.BillingSource != row.BillingSource || row.Records > 9007199254740991-t.row.Records {
			return errors.New("paged tail aggregate scope or count differs")
		}
		t.row.Records += row.Records
		t.row.MinQuota, t.row.MaxQuota = min(t.row.MinQuota, row.MinQuota), max(t.row.MaxQuota, row.MaxQuota)
		t.row.MinID, t.row.MaxID = min(t.row.MinID, row.MinID), max(t.row.MaxID, row.MaxID)
		t.row.FirstAt, t.row.LastAt = min(t.row.FirstAt, row.FirstAt), max(t.row.LastAt, row.LastAt)
		t.quota.Add(t.quota, big.NewInt(row.Quota))
	}
	return nil
}

func finishPagedTailUsage(totals map[string]*pagedTailUsageTotal) ([]UsageWindowRow, error) {
	out := make([]UsageWindowRow, 0, len(totals))
	for _, total := range totals {
		if !total.quota.IsInt64() {
			return nil, errors.New("paged tail gross amount exceeds exact quota bounds")
		}
		row := total.row
		row.Quota = total.quota.Int64()
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return usageRowBefore(out[i], out[j]) })
	return out, nil
}

func captureWalletTailPaged(parent context.Context, source *sql.DB, r WalletTailRequest, clock func() time.Time, pageBudget int) (_ WalletTailObservation, err error) {
	stage := "input"
	defer func() {
		if err != nil {
			err = &walletTailCaptureFailure{stage: stage, cause: err}
		}
	}()
	var empty WalletTailObservation
	if parent == nil || source == nil || clock == nil || pageBudget < 1 || pageBudget > walletTailJournalPages {
		return empty, errors.New("invalid paged wallet tail source or work budget")
	}
	started := clock().UnixMilli()
	if !validWalletTailRequest(r, started) {
		return empty, errors.New("invalid paged wallet tail scope or boundary")
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
	tx, err := source.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
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
	if err != nil || pin != r.SourceIdentity {
		return empty, errors.New("paged tail source identity changed")
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
	o := WalletTailObservation{Version: 1, Basis: walletTailBasis, Request: r, StartedAtMS: started, User: users[0], Funding: []FundingLog{}}
	totals := map[string]*pagedTailUsageTotal{}
	pages := 0
	request := WalletTailPageRequest{Wallet: r, Kind: "user_usage"}
	for {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		if pages >= pageBudget {
			return empty, errors.New("paged tail read work budget reached")
		}
		stage = "tail_usage"
		q, args, err := serverTailUsageQuery(request)
		if err != nil {
			return empty, err
		}
		rows, err := tx.QueryContext(ctx, q, args...)
		if err != nil {
			return empty, err
		}
		usage, next, ended, err := readServerTailUsage(rows, request)
		if err != nil {
			return empty, err
		}
		pages++
		if err := foldPagedTailUsage(totals, usage); err != nil {
			return empty, err
		}
		if ended {
			break
		}
		if !tailPageCursorBefore(request.After, next) {
			return empty, errors.New("paged tail cursor did not advance")
		}
		request.After = next
	}
	stage = "tail_funding"
	o.Funding, err = readSelectiveTailFunding(ctx, tx, r, &pages, pageBudget)
	if err != nil {
		return empty, err
	}
	stage = "validation"
	o.Usage, err = finishPagedTailUsage(totals)
	if err != nil {
		return empty, err
	}
	// The index traversal order is (time,type,id), while the existing raw
	// funding format is (time,id). Restore that order without changing actors.
	sort.Slice(o.Funding, func(i, j int) bool {
		return fundingCursorBefore(FundingCursor{At: o.Funding[i].CreatedAt, ID: o.Funding[i].ID}, FundingCursor{At: o.Funding[j].CreatedAt, ID: o.Funding[j].ID})
	})
	o.CompletedAtMS = clock().UnixMilli()
	if _, err := walletTailBytes(o); err != nil {
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
