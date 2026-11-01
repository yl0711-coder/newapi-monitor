package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var walletTailMonthSlots = make(chan struct{}, 1)

type WalletTailCaptureProgress struct {
	Boundary       int64 `json:"boundary"`
	FirstViews     int   `json:"first_views"`
	Completed      int   `json:"completed"`
	TotalViews     int   `json:"total_views"`
	NextEligibleMS int64 `json:"next_eligible_unix_ms,omitempty"`
	Captured       bool  `json:"captured"`
}

func walletTailProgress(set walletTailSet, now time.Time) WalletTailCaptureProgress {
	out := WalletTailCaptureProgress{Boundary: set.boundary, TotalViews: len(set.pairs) * 2}
	for _, pair := range set.pairs {
		if pair.Older.Version == 0 {
			continue
		}
		out.FirstViews++
		out.Completed++
		if pair.Newer.Version != 0 {
			out.Completed++
		} else {
			eligible := pair.Older.CompletedAtMS + 300000
			if eligible > now.UnixMilli() && (out.NextEligibleMS == 0 || eligible < out.NextEligibleMS) {
				out.NextEligibleMS = eligible
			}
		}
	}
	return out
}

func inspectWalletTailSet(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope, now time.Time) (FundingMonthCapturePlan, walletTailSet, error) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	if err := ValidateWalletTailCaptureStore(ctx, receiver); err != nil {
		return FundingMonthCapturePlan{}, walletTailSet{}, err
	}
	plan, err := loadFundingMonthCapturePlan(ctx, receiver, key, scope, now)
	if err != nil {
		return plan, walletTailSet{}, err
	}
	boundary, err := chooseWalletTailRound(ctx, receiver, key, registeredCapturePlanSHA(plan), scope, now)
	if err != nil {
		return plan, walletTailSet{}, err
	}
	set, err := loadWalletTailRound(ctx, receiver, key, registeredCapturePlanSHA(plan), scope, boundary, now)
	return plan, set, err
}

// LOCAL read-only inspection. It re-audits original bytes and both independent
// calculations; a checkpoint count alone is never financial completeness.
func InspectWalletTailMonthCapture(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope) (WalletTailCaptureProgress, error) {
	if parent == nil || receiver == nil {
		return WalletTailCaptureProgress{}, ErrWalletMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	now := time.Now()
	_, set, err := inspectWalletTailSet(ctx, receiver, key, scope, now)
	if err != nil {
		return WalletTailCaptureProgress{}, err
	}
	return walletTailProgress(set, now), ctx.Err()
}

// At most ONE account/view per step. warmOnly captures missing first views but
// leaves the independent-read wait to the existing background queue. No source
// connection/transaction is held while waiting; committed originals are reused.
func CaptureNextWalletTailMonthView(parent context.Context, source, receiver *sql.DB, key string, scope OrganizationMonthScope, warmOnly bool) (WalletTailCaptureProgress, error) {
	return captureNextWalletTailMonthView(parent, source, receiver, key, scope, warmOnly, time.Now)
}

func captureNextWalletTailMonthView(parent context.Context, source, receiver *sql.DB, key string, scope OrganizationMonthScope, warmOnly bool, clock func() time.Time) (WalletTailCaptureProgress, error) {
	var empty WalletTailCaptureProgress
	if parent == nil || source == nil || receiver == nil || source == receiver || clock == nil {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	select {
	case walletTailMonthSlots <- struct{}{}:
		defer func() { <-walletTailMonthSlots }()
	default:
		return empty, ErrWalletCaptureBusy
	}
	ctx, cancel := context.WithTimeout(parent, 12*time.Second)
	defer cancel()
	now := clock()
	plan, set, err := inspectWalletTailSet(ctx, receiver, key, scope, now)
	if err != nil {
		return empty, err
	}
	progress := walletTailProgress(set, now)
	if progress.Completed == progress.TotalViews || (warmOnly && progress.FirstViews == len(set.pairs)) {
		return progress, nil
	}
	// The first minute of a new month is not a closed enough tail boundary.
	// Return a scheduling hint, never sleep through it on a source connection.
	if set.boundary*1000+60000 > now.UnixMilli() {
		progress.NextEligibleMS = set.boundary*1000 + 60000
		return progress, nil
	}
	index, ordinal := -1, 0
	for i, pair := range set.pairs {
		if pair.Older.Version == 0 {
			index = i
			break
		}
	}
	if index < 0 && !warmOnly {
		for i, pair := range set.pairs {
			if pair.Newer.Version == 0 && pair.Older.CompletedAtMS+300000 <= now.UnixMilli() {
				index, ordinal = i, 1
				break
			}
		}
	}
	if index < 0 {
		return progress, nil
	}
	if err := waitFundingInterval(ctx, time.Second); err != nil {
		return empty, err
	}
	connectCtx, connectCancel := context.WithTimeout(ctx, 4*time.Second)
	err = source.PingContext(connectCtx)
	connectCancel()
	if err != nil {
		return empty, err
	}
	request := WalletTailRequest{SourceIdentity: scope.Funding.SourceIdentity, Rules: scope.Funding.Rules,
		UserID: scope.Funding.UserIDs[index], Boundary: set.boundary}
	// Preserve the native observation and immutable receiver protocol. Only the
	// source read changes: bounded key pages, all inside ONE fresh read view.
	// There is no fallback to the unbounded collector after a timeout.
	o, err := captureWalletTailPaged(ctx, source, request, clock, walletTailJournalPages)
	if err != nil {
		return empty, err
	}
	writeCtx, writeCancel := context.WithTimeout(ctx, 2*time.Second)
	err = saveWalletTailCapture(writeCtx, receiver, key, registeredCapturePlanSHA(plan), scope, ordinal, o, clock())
	writeCancel()
	if err != nil {
		var coded interface{ Code() int }
		if errors.As(err, &coded) && coded.Code()&255 == 13 {
			err = errors.Join(ErrWalletTailCaptureCapacity, err)
		}
		return empty, err
	}
	_, after, err := inspectWalletTailSet(ctx, receiver, key, scope, clock())
	if err != nil {
		return empty, err
	}
	out := walletTailProgress(after, clock())
	out.Captured = true
	return out, ctx.Err()
}
