package monitor

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

type usageFactVerificationResult struct {
	state       UsageFactMemberState
	err         error
	waitForLive bool
	useLiveTail bool
}

// Only the local-only verifier may classify a query deadline as resource wait.
// Source-query deadlines retain their original bounded failure policy.
func usageFactLocalAuditResourceWait(err error) bool {
	return localResourceTemporarilyUnavailable(err) || errors.Is(err, context.DeadlineExceeded)
}

func (m *Monitor) reconcileUsageFactJobProgress(tx *gorm.DB, state UsageFactMemberState, existing *UsageFactJob, target, tail int64, resetRequired bool) (bool, error) {
	changed := false
	id := state.UserID
	// One durable job owns a member revision for its whole lifetime.
	// Hourly target movement extends that job instead of creating a
	// second active cursor for the same user.
	if target > existing.ThroughTs {
		existing.ThroughTs = target
		if existing.FromTs > 0 {
			existing.TotalHours = max(target-existing.FromTs, 0) / usageFactHourSeconds
		}
		if existing.Status == usageFactHistoryJobComplete {
			existing.Status = usageFactHistoryJobQueued
			existing.CompletedAt = 0
			existing.Kind = usageFactHistoryKindTail
			existing.NextHour = max(existing.NextHour, tail)
			if usageFactDayStart(existing.NextHour) == existing.NextHour && existing.NextHour < usageFactDayStart(target) {
				existing.Kind = usageFactHistoryKindBackfill
			}
		}
		changed = true
	}
	// Recover only the known pre-fix transient pause, after the raw
	// collector has filled the exact verification window. Selection
	// is not an integrity decision; the verifier still checks hashes.
	if m.usageFactsRawPageImportEnabled() && !resetRequired &&
		existing.Kind == usageFactHistoryKindVerify && existing.Status == usageFactHistoryJobPaused &&
		existing.SourceEpoch == state.SourceEpoch && existing.TrackedRevision == state.TrackedRevision &&
		existing.LeaseUntil == 0 && existing.LeaseOwner == "" && strings.HasPrefix(existing.LastError, usageFactMissingEpochHoursPrefix) &&
		usageFactRawVerifyCanBorrowLiveTail(state, *existing) && usageFactRawLiveCoversVerify(state, *existing) {
		var complete int64
		if err := tx.Model(&UsageFactMemberHourState{}).
			Where("user_id = ? AND hour_ts >= ? AND hour_ts < ? AND status = ? AND content_hash <> '' AND source_epoch = ?",
				id, existing.VerifyNextHour, existing.ThroughTs, "complete", existing.SourceEpoch).Count(&complete).Error; err != nil {
			return false, err
		}
		if complete == (existing.ThroughTs-existing.VerifyNextHour)/usageFactHourSeconds {
			existing.Status = usageFactHistoryJobQueued
			existing.NextRetryAt = 0
			changed = true
		}
	}
	// A pause spanning midnight cannot borrow today's live suffix
	// to certify whole missing days. Re-enter the ordinary Tail at
	// its unchanged durable cursor; it must finalize and verify each
	// intervening day before advancing coverage/publication.
	if m.usageFactsRawPageImportEnabled() && !resetRequired &&
		existing.Kind == usageFactHistoryKindVerify && existing.Status == usageFactHistoryJobPaused &&
		existing.SourceEpoch == state.SourceEpoch && existing.TrackedRevision == state.TrackedRevision &&
		existing.LeaseUntil == 0 && existing.LeaseOwner == "" && strings.HasPrefix(existing.LastError, usageFactMissingEpochHoursPrefix) &&
		state.SourceFloorHour != nil && state.TailThroughHour != nil &&
		existing.NextHour >= *state.SourceFloorHour && existing.NextHour == *state.TailThroughHour &&
		existing.NextHour < usageFactDayStart(existing.ThroughTs) && existing.NextHour%usageFactHourSeconds == 0 {
		existing.Kind, existing.Status = usageFactHistoryKindTail, usageFactHistoryJobQueued
		existing.NextRetryAt = 0
		changed = true
	}

	return changed, nil
}

func (m *Monitor) auditUsageFactClaimedMember(ctx context.Context, db *gorm.DB, claim usageFactHistoryClaim, claimed UsageFactJob, revision int64, epoch string) usageFactVerificationResult {
	id := *claimed.UserID
	if currentErr := m.usageFactJobRevisionCurrent(ctx, claimed); currentErr != nil {
		return usageFactVerificationResult{err: currentErr}
	}
	var state UsageFactMemberState
	if loadErr := db.First(&state, "user_id = ?", id).Error; loadErr != nil {
		return usageFactVerificationResult{err: loadErr}
	}
	if !state.Active || state.TrackedRevision != revision || state.SourceEpoch != epoch ||
		state.ClassificationVersion != userTrafficClassificationVersion ||
		state.QuerySemanticsVersion != usageFactQuerySemanticsVersion {
		return usageFactVerificationResult{state: state, err: fmt.Errorf("%w: verify state changed user_id=%d", errUsageMemberControlIntegrity, id)}
	}
	verifyFrom := claim.From
	verifyThrough := min(claim.Through, usageFactDayStart(claimed.ThroughTs))
	useLiveTail := m.usageFactsRawPageImportEnabled() && usageFactRawVerifyCanBorrowLiveTail(state, claimed)
	if useLiveTail && !usageFactRawLiveCoversVerify(state, claimed) {
		return usageFactVerificationResult{state: state, waitForLive: true}
	}
	verifyErr := error(nil)
	if state.SourceFloorHour == nil {
		verifyErr = fmt.Errorf("full-history source floor missing user=%d", id)
	} else if state.SourceHistoryStatus == "no_history" {
		verifyThrough = usageFactDayStart(claimed.ThroughTs)
		verifyErr = auditUsageFactKnownEmptyRange(db, id, *state.SourceFloorHour, claimed.ThroughTs)
	} else if state.SourceFirstLogHour == nil {
		verifyErr = fmt.Errorf("full-history first-log boundary missing user=%d", id)
	} else {
		if verifyFrom < *state.SourceFirstLogHour {
			verifyFrom = *state.SourceFirstLogHour
		}
		if claimed.VerifyNextHour == *state.SourceFirstLogHour {
			verifyErr = auditUsageFactKnownEmptyRange(db, id, *state.SourceFloorHour, *state.SourceFirstLogHour)
		}
		if verifyErr == nil {
			verifyErr = auditUsageFactFullHistoryDayRange(db, state, verifyFrom, verifyThrough)
		}
	}
	if verifyErr == nil && (state.SourceHistoryStatus != "no_history" || useLiveTail) &&
		verifyThrough >= usageFactDayStart(claimed.ThroughTs) && usageFactDayStart(claimed.ThroughTs) < claimed.ThroughTs {
		verifyErr = auditUsageFactTrailingHoursForEpoch(db, usageFactDayStart(claimed.ThroughTs), claimed.ThroughTs, []int64{id}, epoch)
	}
	return usageFactVerificationResult{state: state, err: verifyErr, useLiveTail: useLiveTail}
}
