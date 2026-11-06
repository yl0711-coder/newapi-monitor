package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Limits must be explicit; zero values do not enable an unlimited traversal.
type FundingCaptureLimits struct {
	MaxPages int
	Interval time.Duration
	Timeout  time.Duration
}

// Receipt describes durable local progress, not financial completeness. On an
// error only these saved pages may be used as a continuation. No rows or money
// are returned, and reaching the fence never grants review/publication rights.
type FundingCaptureReceipt struct {
	ScanID      string `json:"scan_id"`
	HeadID      string `json:"head_id"`
	PagesSaved  int    `json:"pages_saved"`
	RowsSaved   int    `json:"rows_saved"`
	EndReached  bool   `json:"end_reached"`
	BudgetEnded bool   `json:"budget_ended"`
}

var fundingRunSlots = make(chan struct{}, 1)
var ErrFundingRunBusy = errors.New("monthly funding capture run already active")

// RunFundingCapture is an explicitly invoked, bounded single-window operation.
// It neither initializes databases nor generates scan IDs: the caller must
// durably retain the authorized scan before starting. Source operations use
// CaptureFundingPage's read-only transactions. Only the separate receiver is
// appended. There is no scheduler, retry, rescan or month expansion here.
//
// The process-wide guard is not a distributed lock. An operational caller must
// additionally enforce a single operator/process and a SELECT-only source user.
func RunFundingCapture(ctx context.Context, source, receiver *sql.DB, scan FundingScan, limits FundingCaptureLimits) (FundingCaptureReceipt, error) {
	return runFundingCapture(ctx, source, receiver, scan, limits, waitFundingInterval)
}

func waitFundingInterval(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func runFundingCapture(parent context.Context, source, receiver *sql.DB, scan FundingScan, limits FundingCaptureLimits, wait func(context.Context, time.Duration) error) (FundingCaptureReceipt, error) {
	result := FundingCaptureReceipt{}
	if source == nil || receiver == nil || source == receiver || validateFundingScan(scan) != nil || limits.MaxPages < 1 || limits.MaxPages > 10 || limits.Interval < time.Second || limits.Interval > 10*time.Second || limits.Timeout < time.Second || limits.Timeout > 30*time.Second {
		return result, errors.New("explicit funding scope, separate databases and bounded capture limits required")
	}
	if err := parent.Err(); err != nil {
		return result, err
	}
	select {
	case fundingRunSlots <- struct{}{}:
		defer func() { <-fundingRunSlots }()
	default:
		return result, ErrFundingRunBusy
	}
	ctx, cancel := context.WithTimeout(parent, limits.Timeout)
	defer cancel()
	scan.UserIDs = append([]int64(nil), scan.UserIDs...)
	result.ScanID = scan.ID
	previous, err := LoadLatestFundingPage(ctx, receiver, scan.ID)
	var cursor *FundingPage
	if err == nil {
		if !sameFundingScan(previous.Scan, scan) {
			return result, errors.New("funding resume scope differs from persisted scan")
		}
		raw, err := fundingPageBytes(previous)
		if err != nil {
			return result, err
		}
		result.HeadID = digest(raw)
		result.EndReached = previous.EndReached
		if previous.EndReached {
			return result, nil
		}
		cursor = &previous
	} else if !errors.Is(err, sql.ErrNoRows) {
		return result, fmt.Errorf("load funding checkpoint: %w", err)
	}
	for range limits.MaxPages {
		// Also pace the first page: repeated invocations cannot bypass the gap.
		// No source transaction or SQLite transaction is held during this wait.
		if err := wait(ctx, limits.Interval); err != nil {
			return result, err
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		page, err := CaptureFundingPage(ctx, source, scan, cursor)
		if err != nil {
			return result, fmt.Errorf("capture funding page: %w", err)
		}
		head, err := SaveFundingPage(ctx, receiver, page)
		if err != nil {
			return result, fmt.Errorf("persist funding page: %w", err)
		}
		result.HeadID = head
		result.PagesSaved++
		result.RowsSaved += len(page.Rows)
		result.EndReached = page.EndReached
		if page.EndReached {
			return result, nil
		}
		cursor = &page
	}
	result.BudgetEnded = true
	return result, nil
}
