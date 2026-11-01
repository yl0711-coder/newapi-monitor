package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

// ReviewPartitionedCapturedUsage audits ALL original user-total projections,
// not caller-supplied sums. The expected original digest and complete pinned
// scope are supplied by private composition, never a customer request. This is
// not wallet-boundary, funding, raw-token or publication approval. Existing
// ReviewUsageMonth callers retain their original 4096-window/three-second cap.
func ReviewPartitionedCapturedUsage(parent context.Context, db *sql.DB, originalSHA256 string, scope OrganizationMonthScope, archive PartitionedCapturedUsage) (UsageMonthObservation, error) {
	var empty UsageMonthObservation
	if parent == nil || db == nil || !validDigest(originalSHA256) {
		return empty, ErrTokenMonthCaptureIntegrity
	}
	now := time.Now()
	raw, err := ReassembleCapturedUsageIndex(parent, archive, now)
	if err != nil {
		return empty, err
	}
	if digest(raw) != originalSHA256 {
		return empty, ErrTokenMonthCaptureIntegrity
	}
	var s CapturedMonthSelection
	var index capturedUsagePartIndex
	if json.Unmarshal(raw, &s) != nil || json.Unmarshal(archive.Index, &index) != nil {
		return empty, ErrTokenMonthCaptureIntegrity
	}
	want, err := canonicalFundingCaptureScope(scope, now)
	if err != nil {
		return empty, err
	}
	got, err := canonicalFundingCaptureScope(s.Scope, now)
	if err != nil || !bytes.Equal(want, got) {
		return empty, ErrTokenMonthCaptureIntegrity
	}
	scope = s.Scope // detached canonical scope, never the caller's backing slice
	start, _ := time.ParseInLocation("2006-01", scope.Funding.Month, fundingMonthZone)
	days := int(start.AddDate(0, 1, 0).Sub(start) / (24 * time.Hour))
	// Derived from the ORIGINAL 200-account/day plan plus at most 128 roots
	// with 16 selected leaves each. This is a lifetime bound, not per part.
	pairs := 0
	for _, a := range s.Tokens {
		pairs += len(a.Usage)
	}
	if pairs > len(scope.Funding.UserIDs)*days+adaptiveTokenJobs*15 {
		return empty, ErrTokenMonthCaptureBudget
	}
	select {
	case usageMonthSlots <- struct{}{}:
		defer func() { <-usageMonthSlots }()
	default:
		return empty, ErrUsageMonthBusy
	}
	// Smaller read batches retain the old three-second audit budget. The
	// ORIGINAL captured-selection audit's 30-second outer bound covers ALL
	// parts in one transaction; neither global bytes nor deadline reset.
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return empty, err
	}
	defer func() { _ = tx.Rollback() }()
	r := UsageMonthReport{Month: scope.Funding.Month, Timezone: "UTC+08:00", Basis: usageMonthBasis, SourceIdentity: scope.Funding.SourceIdentity,
		From: start.Unix(), To: start.AddDate(0, 1, 0).Unix(), Accounts: make([]UsageMonthAccountReport, len(scope.Funding.UserIDs))}
	for i, id := range scope.Funding.UserIDs {
		r.Accounts[i] = UsageMonthAccountReport{UserID: id, MissingWindows: []FundingInterval{}, Windows: []UsageMonthWindowReport{}}
	}
	usedBytes := 0
	for _, ref := range index.Parts {
		versions := make([]UsageWindowVersion, 0, ref.Pairs)
		for _, a := range s.Tokens[ref.FromMember:ref.ToMember] {
			versions = append(versions, a.Usage...)
		}
		for from := 0; from < len(versions); from += 512 {
			partCtx, partCancel := context.WithTimeout(ctx, 3*time.Second)
			err := appendUsageMonthWindows(partCtx, tx, scope.Funding, versions[from:min(from+512, len(versions))], now, &r, &usedBytes)
			partCancel()
			if err != nil {
				return empty, err
			}
		}
	}
	for i := range r.Accounts {
		a := &r.Accounts[i]
		if err := assembleUsageAccount(ctx, a, r.From, r.To); err != nil {
			return empty, err
		}
		if a.Totals == nil || len(a.MissingWindows) != 0 || a.ObservedSeconds != r.To-r.From || a.MatchingSeconds != r.To-r.From {
			return empty, ErrTokenMonthCaptureIntegrity
		}
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	return UsageMonthObservation{report: &r}, nil
}
