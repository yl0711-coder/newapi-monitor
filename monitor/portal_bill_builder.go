package monitor

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"math/big"
)

var errPortalBillSourceChanged = errors.New("账单来源或金额校验不一致，未生成完整账单")

// This builder is not registered in a route. The caller must provide an
// immutable, authorized source with independently obtained count/quota totals.
// An ID upper bound alone does NOT freeze updates/deletions of existing rows.
// verify must check source identity and authorization before and after reading.
// The destination is private staging, never an HTTP response: errors discard it.
type portalBillBuildPlan struct {
	Rows     int64
	Quota    string // exact nonnegative integer, not a floating point SUM
	UpperID  int64
	PageSize int
}

type portalBillPageReader func(context.Context, logPageCursor, int) ([]LogRow, error)

func portalBuildVerifiedBill(ctx context.Context, dst io.Writer, plan portalBillBuildPlan,
	page portalBillPageReader, verify func(context.Context) error, progress func(int64) error,
) (int64, error) {
	if len(plan.Quota) == 0 || len(plan.Quota) > 32 {
		return 0, errPortalBillSourceChanged
	}
	want, ok := new(big.Int).SetString(plan.Quota, 10)
	if !ok || want.Sign() < 0 || want.String() != plan.Quota ||
		plan.Rows < 0 || plan.Rows > 1000000 || plan.UpperID < 0 ||
		(plan.Rows > 0 && plan.UpperID == 0) || plan.PageSize < 1 || plan.PageSize > 1000 ||
		dst == nil || page == nil || verify == nil || progress == nil {
		return 0, errPortalBillSourceChanged
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := verify(ctx); err != nil {
		return 0, err
	}
	n, err := io.WriteString(dst, "\xEF\xBB\xBF")
	if err != nil {
		return 0, err
	}
	if n != 3 {
		return 0, io.ErrShortWrite
	}
	w := csv.NewWriter(dst)
	if err := w.Write(portalBillHeader()); err != nil {
		return 0, err
	}
	var count int64
	sum := new(big.Int)
	// Order validation catches repeats at page boundaries; the ID set also
	// catches the same source ID resurfacing under a changed timestamp.
	seen := make(map[int64]struct{})
	cursor := logPageCursor{HasUpper: true, UpperID: plan.UpperID}
	for {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		limit := plan.PageSize
		// Fetch one extra record at the end to prove exhaustion, even when
		// the expected count is an exact multiple of a page size (or zero).
		if remaining := plan.Rows - count + 1; int64(limit) > remaining {
			limit = int(remaining)
		}
		rows, err := page(ctx, cursor, limit)
		if err != nil {
			return count, err
		}
		if len(rows) > limit {
			return count, errPortalBillSourceChanged
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if err := ctx.Err(); err != nil {
				return count, err
			}
			_, duplicate := seen[row.ID]
			if count >= plan.Rows || row.ID <= 0 || row.ID > plan.UpperID || row.Type != 2 || duplicate ||
				(cursor.HasBefore && (row.CreatedAt > cursor.BeforeCreatedAt ||
					(row.CreatedAt == cursor.BeforeCreatedAt && row.ID >= cursor.BeforeID))) {
				return count, errPortalBillSourceChanged
			}
			record, err := portalBillRecord(row)
			if err != nil {
				return count, err
			}
			if err := w.Write(record); err != nil {
				return count, err
			}
			seen[row.ID] = struct{}{}
			sum.Add(sum, big.NewInt(row.billingQuota))
			count++
			cursor = logCursorAfterRow(row, cursor)
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return count, err
		}
		if err := progress(count); err != nil {
			return count, err
		}
	}
	if count != plan.Rows || sum.Cmp(want) != 0 {
		return count, errPortalBillSourceChanged
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return count, err
	}
	if err := ctx.Err(); err != nil {
		return count, err
	}
	if err := verify(ctx); err != nil {
		return count, err
	}
	return count, ctx.Err()
}
