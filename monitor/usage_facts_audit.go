package monitor

// This file closes the full-history recovery loop. Coverage jobs prove that a
// member was imported once; maintenance jobs continuously prove that the
// published SQLite copy still matches its proof and, at a lower cadence, the
// authoritative source. A detected member-day mismatch is never papered over:
// it becomes one idempotent exact repair and closes the facts read gate until
// the controlled source replacement has passed the local audit again.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

// One group probe plus one isolated-member probe keeps the worst-case
// 5s-query+20%-duty wall time inside the two-minute durable lease. A failed
// group still makes persistent progress: the isolated member either advances
// or becomes an exact repair, then leaves the next claim's cursor group.
const usageFactSourceAuditFetchBudget = 2

const usageFactAuditRepairHoldPendingPrefix = "repair_hold_pending: "

var (
	errUsageFactHistoryRepairRequestConflict = errors.New("全历史精确修复幂等键已用于其他目标")
	errUsageFactHistoryManualRepairInvalid   = errors.New("全历史精确修复目标无效")
)

func (m *Monitor) beginUsageFactRepairHold(cause error, now time.Time) {
	m.usageFactsRepairHoldPending.Add(1)
	m.recordUsageFactsSemanticAudit(now, cause)
}

func (m *Monitor) finishUsageFactRepairHoldDurable() {
	if remaining := m.usageFactsRepairHoldPending.Add(-1); remaining < 0 {
		// Defensive clamp: callers pair begin/finish, but a negative fence would
		// be fail-open and is therefore never allowed to persist.
		m.usageFactsRepairHoldPending.Store(0)
	}
}

func usageFactAuditDurableContext() (context.Context, context.CancelFunc) {
	// A source/local mismatch has already been proven. Epoch cancellation must
	// stop further source work, but it must not abort the small SQLite revoke and
	// repair-hold commit that makes that proof safe across restart.
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func usageFactMaintenanceJobID(prefix string, userID, revision, day int64) string {
	id := prefix + "-" + strconv.FormatInt(userID, 36) + "-r" + strconv.FormatInt(revision, 36)
	if day > 0 {
		id += "-d" + strconv.FormatInt(day, 36)
	}
	return id
}

func usageFactMaintenanceJobKey(id string) *string {
	key := id
	return &key
}

func usageFactRepairJobID(userID, revision, day int64) string {
	return usageFactMaintenanceJobID("fhr", userID, revision, day)
}

func usageFactLocalAuditJobID(userID, revision int64) string {
	return usageFactMaintenanceJobID("fhl", userID, revision, 0)
}

func usageFactSourceAuditJobID(userID, revision int64) string {
	return usageFactMaintenanceJobID("fhs", userID, revision, 0)
}

func usageFactRecentAuditJobID(userID, revision int64) string {
	return usageFactMaintenanceJobID("fhsr", userID, revision, 0)
}

func usageFactHistoryMaintenanceKind(kind string) bool {
	return kind == usageFactHistoryKindRepair || kind == usageFactHistoryKindRepairHour ||
		kind == usageFactHistoryKindLocalAudit || kind == usageFactHistoryKindSourceAudit ||
		kind == usageFactHistoryKindRecentAudit || kind == usageFactHistoryKindAuditHour
}

func usageFactSourceAuditBaseKind(job UsageFactJob) (string, error) {
	switch {
	case strings.HasPrefix(job.ID, "fhsr-"):
		return usageFactHistoryKindRecentAudit, nil
	case strings.HasPrefix(job.ID, "fhs-"):
		return usageFactHistoryKindSourceAudit, nil
	default:
		return "", fmt.Errorf("%w: hourly source audit has unknown job id %q", errUsageMemberControlIntegrity, job.ID)
	}
}

func usageFactAuditKindsCompatible(current, desired string) bool {
	if current == desired {
		return true
	}
	return current == usageFactHistoryKindAuditHour &&
		(desired == usageFactHistoryKindSourceAudit || desired == usageFactHistoryKindRecentAudit)
}

func loadUsageFactActiveRepairUsers(db *gorm.DB) (map[int64]bool, error) {
	var jobs []UsageFactJob
	if err := db.Select("user_id").Where("kind IN ? AND status NOT IN ?",
		[]string{usageFactHistoryKindRepair, usageFactHistoryKindRepairHour},
		[]string{usageFactHistoryJobComplete, usageFactHistoryJobCancelled}).Find(&jobs).Error; err != nil {
		return nil, err
	}
	out := make(map[int64]bool, len(jobs))
	for _, job := range jobs {
		if job.UserID != nil && *job.UserID > 0 {
			out[*job.UserID] = true
		}
	}
	return out, nil
}

// reconcileUsageFactHistoryAuditJobs creates one durable local and source
// audit cursor per currently signed member revision. Partial/admin-only members
// are deliberately absent: they are already covered by their import verifier.
func (m *Monitor) reconcileUsageFactHistoryAuditJobs(ctx context.Context, now time.Time) error {
	if !m.usageFactsFullHistoryEnabled() {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	snapshot, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return err
	}
	epoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
	nowUnix := now.Unix()
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var global UsageFactSyncState
		if err := tx.First(&global, 1).Error; err != nil {
			return err
		}
		dayTarget := usageFactDayStart(global.PublishedThrough)
		var published []UsageFactPublishedMember
		if err := tx.Order("user_id").Find(&published).Error; err != nil {
			return err
		}
		keep := make(map[string]bool, len(published)*2)
		for _, row := range published {
			control, ok := snapshot.Controls[row.UserID]
			if !ok || !usageFactPublishedMemberCurrent(row, control) ||
				!usageFactPublishedMemberHistorySignatureCurrent(row, epoch) {
				continue
			}
			var state UsageFactMemberState
			if err := tx.First(&state, "user_id = ?", row.UserID).Error; err != nil {
				return err
			}
			// The registration/source floor -> first-log prefix is represented by
			// one indexed boundary proof rather than synthetic empty day rows. It
			// still needs a rolling local anti-fact audit; otherwise an accidental
			// stale SQLite insert in that prefix could be served forever.
			from := row.SourceFloorHour
			if from <= 0 {
				continue
			}

			localID := usageFactLocalAuditJobID(row.UserID, row.TrackedRevision)
			keep[localID] = true
			localThrough := dayTarget
			if state.SourceHistoryStatus == "no_history" {
				localThrough = global.PublishedThrough
			}
			if localThrough > from {
				if err := upsertUsageFactAuditJob(tx, UsageFactJob{
					ID: localID, IdempotencyKey: usageFactMaintenanceJobKey(localID),
					Kind: usageFactHistoryKindLocalAudit, Priority: 20, UserID: int64Ptr(row.UserID),
					TrackedRevision: row.TrackedRevision, SourceEpoch: epoch,
					FromTs: from, ThroughTs: localThrough, NextHour: from,
					TotalHours: max(localThrough-from, 0) / usageFactHourSeconds,
					Reason:     "rolling local fact/proof audit", RequestedBy: "system", ApprovedBy: "system",
				}, usageFactHistoryLocalAuditCycle, now); err != nil {
					return err
				}
			}

			// A no-history member is re-probed by the 24-hour boundary discovery.
			// It intentionally has no per-day source audit cursor.
			if state.SourceHistoryStatus != "complete_hot" || state.SourceFirstLogHour == nil || dayTarget <= *state.SourceFirstLogHour {
				continue
			}
			sourceID := usageFactSourceAuditJobID(row.UserID, row.TrackedRevision)
			keep[sourceID] = true
			if err := upsertUsageFactAuditJob(tx, UsageFactJob{
				ID: sourceID, IdempotencyKey: usageFactMaintenanceJobKey(sourceID),
				Kind: usageFactHistoryKindSourceAudit, Priority: 5, UserID: int64Ptr(row.UserID),
				TrackedRevision: row.TrackedRevision, SourceEpoch: epoch,
				FromTs: *state.SourceFirstLogHour, ThroughTs: dayTarget, NextHour: *state.SourceFirstLogHour,
				TotalHours: max(dayTarget-*state.SourceFirstLogHour, 0) / usageFactHourSeconds,
				Reason:     "rolling authoritative source audit", RequestedBy: "system", ApprovedBy: "system",
			}, usageFactHistorySourceAuditCycle, now); err != nil {
				return err
			}

			recentFrom := dayTarget - int64(usageFactHistoryRecentAuditDays)*usageFactDaySeconds
			if recentFrom < *state.SourceFirstLogHour {
				recentFrom = *state.SourceFirstLogHour
			}
			if recentFrom < dayTarget {
				recentID := usageFactRecentAuditJobID(row.UserID, row.TrackedRevision)
				keep[recentID] = true
				if err := upsertUsageFactAuditJob(tx, UsageFactJob{
					ID: recentID, IdempotencyKey: usageFactMaintenanceJobKey(recentID),
					Kind: usageFactHistoryKindRecentAudit, Priority: 30, UserID: int64Ptr(row.UserID),
					TrackedRevision: row.TrackedRevision, SourceEpoch: epoch,
					FromTs: recentFrom, ThroughTs: dayTarget, NextHour: recentFrom,
					TotalHours: max(dayTarget-recentFrom, 0) / usageFactHourSeconds,
					Reason:     "recent authoritative source audit", RequestedBy: "system", ApprovedBy: "system",
				}, usageFactHistoryRecentAuditCycle, now); err != nil {
					return err
				}
			}
		}

		var existing []UsageFactJob
		if err := tx.Where("kind IN ?", []string{usageFactHistoryKindLocalAudit, usageFactHistoryKindSourceAudit,
			usageFactHistoryKindRecentAudit, usageFactHistoryKindAuditHour}).Find(&existing).Error; err != nil {
			return err
		}
		for _, job := range existing {
			if keep[job.ID] || job.Status == usageFactHistoryJobCancelled {
				continue
			}
			if err := tx.Model(&UsageFactJob{}).Where("id = ?", job.ID).Updates(map[string]any{
				"status": usageFactHistoryJobCancelled, "lease_owner": "", "lease_until": 0,
				"completed_at": nowUnix, "updated_at": nowUnix, "last_error": "publication signature removed",
			}).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	after, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return err
	}
	if !usageMemberControlSnapshotsEqual(snapshot, after) {
		return fmt.Errorf("%w: member manifest changed while scheduling history audits", errUsageMemberControlIntegrity)
	}
	return nil
}

func upsertUsageFactAuditJob(tx *gorm.DB, desired UsageFactJob, cycle time.Duration, now time.Time) error {
	var current UsageFactJob
	err := tx.First(&current, "id = ?", desired.ID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		desired.Status = usageFactHistoryJobQueued
		desired.CreatedAt, desired.UpdatedAt = now.Unix(), now.Unix()
		return tx.Create(&desired).Error
	}
	if err != nil {
		return err
	}
	fromChanged := current.FromTs != desired.FromTs
	if current.Kind == usageFactHistoryKindAuditHour && usageFactAuditKindsCompatible(current.Kind, desired.Kind) {
		// A recent-14-day cursor moves its desired left edge every midnight. Do
		// not erase an in-flight 24-hour fallback; after it returns to the base
		// kind, the next reconciliation can start the new cycle normally.
		fromChanged = false
	}
	reset := current.TrackedRevision != desired.TrackedRevision || current.SourceEpoch != desired.SourceEpoch ||
		fromChanged || !usageFactAuditKindsCompatible(current.Kind, desired.Kind) ||
		current.Status == usageFactHistoryJobCancelled
	if reset {
		current = desired
		current.Status = usageFactHistoryJobQueued
		current.CreatedAt, current.UpdatedAt = now.Unix(), now.Unix()
		return tx.Save(&current).Error
	}
	changed := false
	cycleDue := current.Status == usageFactHistoryJobComplete && current.CompletedAt > 0 &&
		now.Unix()-current.CompletedAt >= int64(cycle/time.Second)
	if desired.ThroughTs > current.ThroughTs {
		current.ThroughTs = desired.ThroughTs
		current.TotalHours = desired.TotalHours
		if current.Status == usageFactHistoryJobComplete && !cycleDue {
			// Target movement is not a new rolling cycle. Keep the completed
			// checkpoint quiet until its cadence is due; otherwise a daily right
			// edge extension makes both 24h/30d cursors audit only the newest day
			// forever and the old history is never revisited.
			current.NextHour = current.ThroughTs
			current.CompletedHours = current.TotalHours
		}
		changed = true
	}
	if cycleDue {
		current.Status = usageFactHistoryJobQueued
		current.NextHour = desired.FromTs
		current.CompletedHours = 0
		current.CompletedAt = 0
		current.Attempts, current.NextRetryAt = 0, 0
		current.LastError = ""
		changed = true
	}
	if changed {
		current.UpdatedAt = now.Unix()
		return tx.Save(&current).Error
	}
	return nil
}

func (m *Monitor) enqueueUsageFactHistoryRepair(ctx context.Context, userID, dayTs int64, reason, requestedBy string, now time.Time) (UsageFactJob, error) {
	return m.enqueueUsageFactHistoryRepairWithRequest(ctx, userID, dayTs, reason, requestedBy, now, nil)
}

func usageFactRepairRequestMatches(a, b UsageFactRepairRequest) bool {
	return a.RequestID == b.RequestID && a.UserID == b.UserID && a.DayTs == b.DayTs &&
		a.Reason == b.Reason && a.RequestedBy == b.RequestedBy
}

func (m *Monitor) loadUsageFactRepairRequestJob(ctx context.Context, requested UsageFactRepairRequest) (UsageFactJob, bool, error) {
	var prior UsageFactRepairRequest
	err := m.usageFactsStore().WithContext(ctx).First(&prior, "request_id = ?", requested.RequestID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return UsageFactJob{}, false, nil
	}
	if err != nil {
		return UsageFactJob{}, false, err
	}
	if !usageFactRepairRequestMatches(prior, requested) {
		return UsageFactJob{}, true, errUsageFactHistoryRepairRequestConflict
	}
	var job UsageFactJob
	if err := m.usageFactsStore().WithContext(ctx).First(&job, "id = ?", prior.JobID).Error; err != nil {
		return UsageFactJob{}, true, err
	}
	return job, true, nil
}

// requestUsageFactHistoryDayRepair validates the deliberately narrow manual
// repair surface: one already-published member, one fully closed CST day, one
// stable request ID. It never accepts a range and never creates source
// concurrency; the durable worker reuses the normal low-priority exact repair.
func (m *Monitor) requestUsageFactHistoryDayRepair(
	ctx context.Context,
	userID, dayTs int64,
	reason, requestID, requestedBy string,
	now time.Time,
) (UsageFactJob, error) {
	if !m.usageFactsFullHistoryEnabled() {
		return UsageFactJob{}, fmt.Errorf("%w: full-history worker is disabled", errUsageFactHistoryManualRepairInvalid)
	}
	if now.IsZero() {
		now = time.Now()
	}
	reason = strings.TrimSpace(reason)
	requestID = strings.TrimSpace(requestID)
	requestedBy = strings.TrimSpace(requestedBy)
	if userID <= 0 || dayTs <= 0 || usageFactDayStart(dayTs) != dayTs ||
		dayTs+usageFactDaySeconds > usageFactDayStart(now.Unix()) || reason == "" ||
		!usageMemberIdempotencyKeyPattern.MatchString(requestID) {
		return UsageFactJob{}, errUsageFactHistoryManualRepairInvalid
	}
	if len(reason) > 500 {
		reason = reason[:500]
	}
	if requestedBy == "" {
		requestedBy = "root"
	}
	if len(requestedBy) > 64 {
		requestedBy = requestedBy[:64]
	}
	requested := UsageFactRepairRequest{
		RequestID: requestID, UserID: userID, DayTs: dayTs, Reason: reason, RequestedBy: requestedBy,
	}
	if prior, found, err := m.loadUsageFactRepairRequestJob(ctx, requested); found || err != nil {
		return prior, err
	}
	return m.enqueueUsageFactHistoryRepairWithRequest(ctx, userID, dayTs, reason, requestedBy, now, &requested)
}

func (m *Monitor) enqueueUsageFactHistoryRepairWithRequest(
	ctx context.Context,
	userID, dayTs int64,
	reason, requestedBy string,
	now time.Time,
	request *UsageFactRepairRequest,
) (UsageFactJob, error) {
	if !m.usageFactsFullHistoryEnabled() || userID <= 0 || dayTs <= 0 || usageFactDayStart(dayTs) != dayTs {
		return UsageFactJob{}, errors.New("full-history repair target is invalid")
	}
	if now.IsZero() {
		now = time.Now()
	}
	snapshot, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return UsageFactJob{}, err
	}
	control, ok := snapshot.Controls[userID]
	if !ok || !control.Active || control.TrackedRevision < 1 {
		return UsageFactJob{}, fmt.Errorf("%w: repair member is not active", errUsageMemberControlIntegrity)
	}
	epoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
	id := usageFactRepairJobID(userID, control.TrackedRevision, dayTs)
	requestedBy = strings.TrimSpace(requestedBy)
	if requestedBy == "" {
		requestedBy = "system"
	}
	reason = truncateUsageFactError(reason)
	var result UsageFactJob
	var publicationChanged bool
	var publishedState UsageFactSyncState
	// All facts publication writers share this mutex.  In particular, a
	// publisher must not take its candidate snapshot before this repair exists
	// and then reinsert the revoked member after the hold transaction commits.
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if request != nil {
			request.UserID = userID
			request.TrackedRevision = control.TrackedRevision
			request.SourceEpoch = epoch
			request.DayTs = dayTs
			request.Reason = reason
			request.RequestedBy = requestedBy
			var prior UsageFactRepairRequest
			requestErr := tx.First(&prior, "request_id = ?", request.RequestID).Error
			if requestErr == nil {
				if !usageFactRepairRequestMatches(prior, *request) {
					return errUsageFactHistoryRepairRequestConflict
				}
				return tx.First(&result, "id = ?", prior.JobID).Error
			}
			if !errors.Is(requestErr, gorm.ErrRecordNotFound) {
				return requestErr
			}
		}
		jobErr := tx.First(&result, "id = ?", id).Error
		jobFound := jobErr == nil
		if request != nil && jobFound && result.Status != usageFactHistoryJobComplete && result.Status != usageFactHistoryJobCancelled {
			// A rolling audit or another administrator already created the same
			// exact member-day repair. Attach this request receipt to that one job.
			// An explicit request also safely resumes a paused copy.
			if result.Status == usageFactHistoryJobPaused {
				result.Status = usageFactHistoryJobQueued
				result.Attempts, result.NextRetryAt = 0, 0
				result.LeaseOwner, result.LeaseUntil = "", 0
				result.LastError = ""
				result.UpdatedAt = now.Unix()
				if err := tx.Save(&result).Error; err != nil {
					return err
				}
			}
			request.JobID = result.ID
			request.CreatedAt = now.Unix()
			return tx.Create(request).Error
		}
		if request != nil {
			// New/re-opened manual work is allowed only for a day that was already
			// source-signed and published. Partial members stay admin-only and are
			// completed by their coverage job instead of this escape hatch.
			var published UsageFactPublishedMember
			if err := tx.First(&published, "user_id = ?", userID).Error; err != nil {
				return fmt.Errorf("%w: member-day is not currently published", errUsageFactHistoryManualRepairInvalid)
			}
			if published.TrackedRevision != control.TrackedRevision || published.SourceEpoch != epoch ||
				published.ClassificationVersion != userTrafficClassificationVersion ||
				published.QuerySemanticsVersion != usageFactQuerySemanticsVersion ||
				dayTs < published.SourceFloorHour || dayTs+usageFactDaySeconds > published.VerifiedThroughHour {
				return fmt.Errorf("%w: member-day is outside the signed publication", errUsageFactHistoryManualRepairInvalid)
			}
		}
		err := jobErr
		if errors.Is(err, gorm.ErrRecordNotFound) {
			memberID := userID
			result = UsageFactJob{
				ID: id, IdempotencyKey: usageFactMaintenanceJobKey(id), Kind: usageFactHistoryKindRepair,
				Priority: 100, UserID: &memberID, TrackedRevision: control.TrackedRevision, SourceEpoch: epoch,
				FromTs: dayTs, ThroughTs: dayTs + usageFactDaySeconds, NextHour: dayTs,
				TotalHours: 24, Status: usageFactHistoryJobQueued, Reason: reason,
				RequestedBy: requestedBy, ApprovedBy: requestedBy, CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
			}
			if err := tx.Create(&result).Error; err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else if result.TrackedRevision != control.TrackedRevision || result.SourceEpoch != epoch {
			return fmt.Errorf("%w: repair signature changed", errUsageMemberControlIntegrity)
		} else if result.Status == usageFactHistoryJobComplete || result.Status == usageFactHistoryJobCancelled {
			result.Kind = usageFactHistoryKindRepair
			result.Status = usageFactHistoryJobQueued
			result.NextHour = dayTs
			result.CompletedHours = 0
			result.Attempts, result.NextRetryAt = 0, 0
			result.LeaseOwner, result.LeaseUntil = "", 0
			result.CompletedAt = 0
			result.LastError = ""
			result.Reason, result.RequestedBy, result.ApprovedBy = reason, requestedBy, requestedBy
			result.UpdatedAt = now.Unix()
			if err := tx.Save(&result).Error; err != nil {
				return err
			}
		}
		stateUpdate := tx.Model(&UsageFactMemberState{}).
			Where("user_id = ? AND active = ? AND tracked_revision = ? AND source_epoch = ?", userID, true, control.TrackedRevision, epoch).
			Updates(map[string]any{"coverage_status": "repairing", "verification_status": "repair_pending",
				"last_error": reason, "updated_at": now.Unix()})
		if stateUpdate.Error != nil {
			return stateUpdate.Error
		}
		if stateUpdate.RowsAffected != 1 {
			return fmt.Errorf("%w: repair member state signature changed", errUsageMemberControlIntegrity)
		}

		// Revoke only the affected member in the same durable transaction that
		// creates the repair hold. This closes the crash window where an in-memory
		// semantic flag had been the sole guard and keeps unrelated companies on
		// their last signed data.
		deleted := tx.Where("user_id = ?", userID).Delete(&UsageFactPublishedMember{})
		if deleted.Error != nil {
			return deleted.Error
		}
		if deleted.RowsAffected == 0 {
			if request != nil {
				request.JobID = result.ID
				request.CreatedAt = now.Unix()
				return tx.Create(request).Error
			}
			return nil
		}
		publicationChanged = true
		var kept []UsageFactPublishedMember
		if err := tx.Order("user_id").Find(&kept).Error; err != nil {
			return err
		}
		if err := tx.First(&publishedState, 1).Error; err != nil {
			return err
		}
		if len(kept) == 0 {
			publishedState.PublishedFingerprint = ""
			publishedState.PublishedRangeStart = 0
			publishedState.PublishedThrough = 0
			publishedState.PublishedWindowDays = 0
			publishedState.PublishedAt = 0
		} else {
			ids := make([]int64, 0, len(kept))
			for _, row := range kept {
				ids = append(ids, row.UserID)
			}
			publishedState.PublishedFingerprint = portalMemberFingerprintFromIDs(ids)
			if err := normalizeUsageFactPublishedRange(&publishedState, kept); err != nil {
				return err
			}
		}
		publishedState.Generation++
		publishedState.ServingGeneration++
		if err := tx.Save(&publishedState).Error; err != nil {
			return err
		}
		if request != nil {
			request.JobID = result.ID
			request.CreatedAt = now.Unix()
			return tx.Create(request).Error
		}
		return nil
	})
	if err != nil {
		return UsageFactJob{}, err
	}
	if publicationChanged {
		m.publishUsageFactGenerations(publishedState.Generation, publishedState.ServingGeneration)
		m.publishUsageFactReadBoundsAfterMutation(publishedState)
		// Never restore a previously sampled true gate here. Another concurrent
		// semantic audit may have closed it while this transaction ran. Existing
		// kept members remain on their already-open safe range; a closed gate is
		// reopened only by the durable checkpoint publisher/refresh path.
	}
	return result, nil
}

func (m *Monitor) persistProvenUsageFactMismatch(
	job UsageFactJob,
	leaseOwner string,
	userID, dayTs int64,
	cause error,
	repairReason, requestedBy string,
	now time.Time,
) error {
	// Close the in-memory gate before waiting for the publication mutex. A
	// mismatch is already authoritative at this point; continuing to serve it
	// for up to the publisher's 60s lock deadline would be fail-open.
	m.beginUsageFactRepairHold(cause, now)
	intentCtx, intentCancel := usageFactAuditDurableContext()
	intentErr := m.persistUsageFactAuditRepairHoldIntent(intentCtx, job, leaseOwner, cause, now)
	intentCancel()
	durableSafe := intentErr == nil
	if durableSafe {
		// From this point a restart sees the pending marker even if enqueue waits
		// behind a long publisher lock or the process exits.
		m.finishUsageFactRepairHoldDurable()
	}
	durableCtx, cancel := usageFactAuditDurableContext()
	_, enqueueErr := m.enqueueUsageFactHistoryRepair(durableCtx, userID, dayTs, repairReason, requestedBy, now)
	if enqueueErr == nil {
		if !durableSafe {
			durableSafe = true
			m.finishUsageFactRepairHoldDurable()
		}
		enqueueErr = m.pauseUsageFactAuditJob(durableCtx, job, leaseOwner, cause, now)
	} else if !durableSafe {
		markerErr := m.deferUsageFactAuditForRepairHoldFresh(job, leaseOwner, cause, now)
		if markerErr == nil {
			durableSafe = true
			m.finishUsageFactRepairHoldDurable()
		}
		enqueueErr = errors.Join(enqueueErr, markerErr)
	}
	cancel()
	enqueueErr = errors.Join(intentErr, enqueueErr)
	if durableSafe {
		// The initial close is deliberately global so no request can escape while
		// the durable hold is being serialized. Once SQLite contains either the
		// member-scoped repair/revocation or a repair-hold marker, recompute the
		// checkpoint so unrelated published companies can resume immediately.
		refreshCtx, refreshCancel := usageFactAuditDurableContext()
		m.refreshUsageFactsReadiness(refreshCtx, now)
		refreshCancel()
	}
	// If both the repair and marker writes failed, the pending counter remains
	// non-zero for this process. Restart remains protected when the marker later
	// succeeds; an unrecoverable SQLite write failure must not reopen reads.
	return enqueueErr
}

func (m *Monitor) persistUsageFactAuditRepairHoldIntent(ctx context.Context, job UsageFactJob, owner string, cause error, now time.Time) error {
	message := usageFactAuditRepairHoldPendingPrefix + truncateUsageFactError(cause.Error())
	result := m.usageFactsStore().WithContext(ctx).Model(&UsageFactJob{}).
		Where("id = ? AND lease_owner = ? AND status = ?", job.ID, owner, usageFactHistoryJobRunning).
		Updates(map[string]any{"last_error": message, "heartbeat_at": now.Unix(), "updated_at": now.Unix()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: audit repair hold intent lease changed", errUsageMemberControlIntegrity)
	}
	return nil
}

func (m *Monitor) executeUsageFactHistoryRepair(ctx context.Context, claim usageFactHistoryClaim, now time.Time) error {
	ids, revisions, err := usageFactHistoryClaimIDs(claim)
	if err != nil {
		_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
		return err
	}
	if claim.From <= 0 || claim.Through != claim.From+usageFactDaySeconds {
		err = errors.New("full-history repair must cover one natural day")
		_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
		return err
	}
	for _, job := range claim.Jobs {
		if err := m.usageFactJobRevisionCurrent(ctx, job); err != nil {
			_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
			return err
		}
	}
	result, err := m.fetchUsageFactHistoryRange(ctx, claim.From, claim.Through, ids)
	if err != nil {
		if usageFactHistoryRangeShouldFallback(err) && len(claim.Jobs) > 1 {
			mid := len(claim.Jobs) / 2
			left := usageFactHistoryClaim{Jobs: claim.Jobs[:mid], LeaseOwner: claim.LeaseOwner, From: claim.From, Through: claim.Through}
			right := usageFactHistoryClaim{Jobs: claim.Jobs[mid:], LeaseOwner: claim.LeaseOwner, From: claim.From, Through: claim.Through}
			return errors.Join(m.executeUsageFactHistoryRepair(ctx, left, now), m.executeUsageFactHistoryRepair(ctx, right, now))
		}
		if usageFactHistoryRangeShouldFallback(err) && len(claim.Jobs) == 1 {
			return m.downgradeUsageFactRepairToHourly(context.Background(), claim, err, now)
		}
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, usageFactHistoryFailureIsSourceGlobal(err))
		return err
	}
	jobID := "repair-day-" + strconv.FormatInt(claim.From, 36)
	if err := m.commitUsageFactHistoryRange(ctx, result, jobID, revisions); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	return m.completeUsageFactRepairJobs(ctx, claim, now)
}

func (m *Monitor) downgradeUsageFactRepairToHourly(ctx context.Context, claim usageFactHistoryClaim, cause error, now time.Time) error {
	if len(claim.Jobs) != 1 || claim.Through != claim.From+usageFactDaySeconds {
		return errors.New("full-history repair hourly downgrade is invalid")
	}
	return m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job UsageFactJob
		if err := tx.First(&job, "id = ? AND lease_owner = ? AND status = ?", claim.Jobs[0].ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; err != nil {
			return err
		}
		job.Kind = usageFactHistoryKindRepairHour
		job.NextHour = job.FromTs
		job.Status = usageFactHistoryJobQueued
		job.Attempts, job.NextRetryAt = 0, 0
		job.LeaseOwner, job.LeaseUntil = "", 0
		job.LastError = "daily repair uses bounded raw-page hourly protocol"
		if cause != nil {
			job.LastError += ": " + truncateUsageFactError(cause.Error())
		}
		job.UpdatedAt = now.Unix()
		return tx.Save(&job).Error
	})
}

// downgradeUsageFactSourceAuditToHourly persists a workload-local fallback for
// one already-published member-day. A dimensional row cap or a source control
// race means the daily audit did not finish; it does not prove that the served
// fact is wrong. Keep that signed fact visible while 24 bounded hour reads are
// staged. The last hour performs the independent day control and atomically
// replaces the daily row if the source really changed.
func (m *Monitor) downgradeUsageFactSourceAuditToHourly(ctx context.Context, claim usageFactHistoryClaim, cause error, now time.Time) error {
	if len(claim.Jobs) != 1 || claim.From <= 0 || usageFactDayStart(claim.From) != claim.From ||
		claim.Through != claim.From+usageFactDaySeconds {
		return errors.New("full-history source audit hourly downgrade is invalid")
	}
	return m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job UsageFactJob
		if err := tx.First(&job, "id = ? AND lease_owner = ? AND status = ?",
			claim.Jobs[0].ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; err != nil {
			return err
		}
		if job.Kind != usageFactHistoryKindSourceAudit && job.Kind != usageFactHistoryKindRecentAudit {
			return fmt.Errorf("%w: source audit kind changed before hourly downgrade", errUsageMemberControlIntegrity)
		}
		job.Kind = usageFactHistoryKindAuditHour
		job.NextHour = claim.From
		job.VerifyNextHour = claim.Through // durable end of this one fallback day
		job.Status = usageFactHistoryJobQueued
		job.Attempts, job.NextRetryAt = 0, 0
		job.LeaseOwner, job.LeaseUntil = "", 0
		job.LastError = "daily source audit uses bounded raw-page hourly verification"
		if cause != nil {
			job.LastError += ": " + truncateUsageFactError(cause.Error())
		}
		job.UpdatedAt = now.Unix()
		return tx.Save(&job).Error
	})
}

func (m *Monitor) executeUsageFactHistoryRepairHour(ctx context.Context, claim usageFactHistoryClaim, now time.Time) error {
	ids, _, err := usageFactHistoryClaimIDs(claim)
	if err != nil {
		_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
		return err
	}
	for _, job := range claim.Jobs {
		if err := m.usageFactJobRevisionCurrent(ctx, job); err != nil {
			_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
			return err
		}
	}
	result, syncErr := m.syncUsageFactHourBatchedWithOptions(ctx, claim.From, ids, usageFactHourSyncOptions{
		updateLastFactSync: false, recordFailure: false, lowPrioritySource: true,
	})
	succeeded := make(map[int64]bool, len(result.SucceededUserIDs))
	for _, id := range result.SucceededUserIDs {
		succeeded[id] = true
	}
	failed := result.FailedByUser
	if syncErr != nil && len(succeeded) == 0 {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, syncErr, now, usageFactHistoryFailureIsSourceGlobal(syncErr))
		return syncErr
	}
	if finalizeErr := m.maybeFinalizeUsageFactHistoryDayFromHours(ctx, claim.From, result.SucceededUserIDs, true); finalizeErr != nil {
		var memberErr *usageFactHistoryFinalizeError
		if errors.As(finalizeErr, &memberErr) {
			for id, oneErr := range memberErr.Failures {
				succeeded[id] = false
				if failed == nil {
					failed = make(map[int64]error)
				}
				failed[id] = oneErr
			}
		} else {
			for _, id := range result.SucceededUserIDs {
				succeeded[id] = false
				if failed == nil {
					failed = make(map[int64]error)
				}
				failed[id] = finalizeErr
			}
		}
	}
	next := claim.From + usageFactHourSeconds
	completed := make([]UsageFactJob, 0)
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, claimed := range claim.Jobs {
			if claimed.UserID == nil || !succeeded[*claimed.UserID] {
				continue
			}
			var job UsageFactJob
			if err := tx.First(&job, "id = ? AND lease_owner = ? AND status = ?", claimed.ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; err != nil {
				return err
			}
			job.NextHour = min(next, job.ThroughTs)
			job.CompletedHours = max(job.NextHour-job.FromTs, 0) / usageFactHourSeconds
			job.LeaseOwner, job.LeaseUntil = "", 0
			job.Attempts, job.NextRetryAt = 0, 0
			job.LastError = ""
			job.Status = usageFactHistoryJobQueued
			job.HeartbeatAt, job.UpdatedAt = now.Unix(), now.Unix()
			if job.NextHour >= job.ThroughTs {
				job.Status = usageFactHistoryJobComplete
				job.CompletedAt = now.Unix()
				completed = append(completed, job)
			}
			if err := tx.Save(&job).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	var failures error
	for _, claimed := range claim.Jobs {
		if claimed.UserID == nil || succeeded[*claimed.UserID] {
			continue
		}
		cause := failed[*claimed.UserID]
		if cause == nil {
			cause = syncErr
		}
		one := usageFactHistoryClaim{Jobs: []UsageFactJob{claimed}, LeaseOwner: claim.LeaseOwner, From: claim.From, Through: claim.Through}
		if releaseErr := m.releaseUsageFactHistoryClaim(context.Background(), one, cause, now, usageFactHistoryFailureIsSourceGlobal(cause)); releaseErr != nil {
			failures = errors.Join(failures, cause, releaseErr)
		} else {
			failures = errors.Join(failures, cause)
		}
	}
	if len(completed) > 0 {
		if completeErr := m.afterUsageFactRepairsCompleted(ctx, completed, now); completeErr != nil {
			failures = errors.Join(failures, completeErr)
		}
	}
	return failures
}

func (m *Monitor) executeUsageFactHistorySourceAuditHour(ctx context.Context, claim usageFactHistoryClaim, now time.Time) error {
	ids, _, err := usageFactHistoryClaimIDs(claim)
	if err != nil {
		_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
		return err
	}
	for _, job := range claim.Jobs {
		if err := m.usageFactJobRevisionCurrent(ctx, job); err != nil {
			_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
			return err
		}
	}
	result, syncErr := m.syncUsageFactHourBatchedWithOptions(ctx, claim.From, ids, usageFactHourSyncOptions{
		updateLastFactSync: false, recordFailure: false, lowPrioritySource: true,
	})
	succeeded := make(map[int64]bool, len(result.SucceededUserIDs))
	for _, id := range result.SucceededUserIDs {
		succeeded[id] = true
	}
	failed := result.FailedByUser
	if syncErr != nil && len(succeeded) == 0 {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, syncErr, now,
			usageFactHistoryFailureIsSourceGlobal(syncErr))
		return syncErr
	}
	if finalizeErr := m.maybeFinalizeUsageFactHistoryDayFromHours(ctx, claim.From, result.SucceededUserIDs, true); finalizeErr != nil {
		var memberErr *usageFactHistoryFinalizeError
		if errors.As(finalizeErr, &memberErr) {
			for id, oneErr := range memberErr.Failures {
				succeeded[id] = false
				if failed == nil {
					failed = make(map[int64]error)
				}
				failed[id] = oneErr
			}
		} else {
			for _, id := range result.SucceededUserIDs {
				succeeded[id] = false
				if failed == nil {
					failed = make(map[int64]error)
				}
				failed[id] = finalizeErr
			}
		}
	}
	next := claim.From + usageFactHourSeconds
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, claimed := range claim.Jobs {
			if claimed.UserID == nil || !succeeded[*claimed.UserID] {
				continue
			}
			var job UsageFactJob
			if err := tx.First(&job, "id = ? AND lease_owner = ? AND status = ?",
				claimed.ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; err != nil {
				return err
			}
			if job.Kind != usageFactHistoryKindAuditHour || job.VerifyNextHour <= job.NextHour ||
				usageFactDayStart(job.VerifyNextHour-1)+usageFactDaySeconds != job.VerifyNextHour {
				return fmt.Errorf("%w: hourly source audit checkpoint is invalid", errUsageMemberControlIntegrity)
			}
			job.NextHour = min(next, job.VerifyNextHour)
			job.CompletedHours = max(job.NextHour-job.FromTs, 0) / usageFactHourSeconds
			job.LeaseOwner, job.LeaseUntil = "", 0
			job.Attempts, job.NextRetryAt = 0, 0
			job.LastError = ""
			job.Status = usageFactHistoryJobQueued
			job.HeartbeatAt, job.UpdatedAt = now.Unix(), now.Unix()
			if job.NextHour >= job.VerifyNextHour {
				dayFrom := job.VerifyNextHour - usageFactDaySeconds
				// The finalizer has already replaced the strict daily fact. The hour
				// rows were hidden staging and can now be removed without changing
				// the serving namespace.
				if err := tx.Where("user_id = ? AND hour_ts >= ? AND hour_ts < ?", *job.UserID, dayFrom, job.VerifyNextHour).
					Delete(&UsageHourFact{}).Error; err != nil {
					return err
				}
				if err := tx.Where("user_id = ? AND hour_ts >= ? AND hour_ts < ?", *job.UserID, dayFrom, job.VerifyNextHour).
					Delete(&UsageFactMemberHourState{}).Error; err != nil {
					return err
				}
				baseKind, baseErr := usageFactSourceAuditBaseKind(job)
				if baseErr != nil {
					return baseErr
				}
				job.Kind = baseKind
				job.VerifyNextHour = 0
				if job.NextHour >= job.ThroughTs {
					job.Status = usageFactHistoryJobComplete
					job.CompletedAt = now.Unix()
				}
			}
			if err := tx.Save(&job).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	var failures error
	for _, claimed := range claim.Jobs {
		if claimed.UserID == nil || succeeded[*claimed.UserID] {
			continue
		}
		cause := failed[*claimed.UserID]
		if cause == nil {
			cause = syncErr
		}
		one := usageFactHistoryClaim{Jobs: []UsageFactJob{claimed}, LeaseOwner: claim.LeaseOwner,
			From: claim.From, Through: claim.Through}
		if releaseErr := m.releaseUsageFactHistoryClaim(context.Background(), one, cause, now,
			usageFactHistoryFailureIsSourceGlobal(cause)); releaseErr != nil {
			failures = errors.Join(failures, cause, releaseErr)
		} else {
			failures = errors.Join(failures, cause)
		}
	}
	return failures
}

func (m *Monitor) completeUsageFactRepairJobs(ctx context.Context, claim usageFactHistoryClaim, now time.Time) error {
	completed := make([]UsageFactJob, 0, len(claim.Jobs))
	err := m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, claimed := range claim.Jobs {
			var job UsageFactJob
			if err := tx.First(&job, "id = ? AND lease_owner = ? AND status = ?", claimed.ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; err != nil {
				return err
			}
			job.Status = usageFactHistoryJobComplete
			job.NextHour = job.ThroughTs
			job.CompletedHours = job.TotalHours
			job.Attempts, job.NextRetryAt = 0, 0
			job.LeaseOwner, job.LeaseUntil = "", 0
			job.LastError = ""
			job.CompletedAt, job.HeartbeatAt, job.UpdatedAt = now.Unix(), now.Unix(), now.Unix()
			if err := tx.Save(&job).Error; err != nil {
				return err
			}
			completed = append(completed, job)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return m.afterUsageFactRepairsCompleted(ctx, completed, now)
}

func (m *Monitor) afterUsageFactRepairsCompleted(ctx context.Context, completed []UsageFactJob, now time.Time) error {
	if len(completed) == 0 {
		return nil
	}
	err := m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, job := range completed {
			if job.UserID == nil {
				continue
			}
			// A strict closed-day source result is now authoritative. Hour staging
			// for that exact member-day is no longer needed and may itself be the
			// stale row that triggered a known-empty repair. Remove it before the
			// repair hold can be released so the next local audit converges.
			if err := tx.Where("user_id = ? AND hour_ts >= ? AND hour_ts < ?", *job.UserID, job.FromTs, job.ThroughTs).
				Delete(&UsageHourFact{}).Error; err != nil {
				return err
			}
			if err := tx.Where("user_id = ? AND hour_ts >= ? AND hour_ts < ?", *job.UserID, job.FromTs, job.ThroughTs).
				Delete(&UsageFactMemberHourState{}).Error; err != nil {
				return err
			}
			if err := tx.Model(&UsageFactJob{}).
				Where("user_id = ? AND source_epoch = ? AND kind IN ? AND status = ? AND next_hour <= ? AND through_ts > ?",
					*job.UserID, job.SourceEpoch, []string{usageFactHistoryKindLocalAudit, usageFactHistoryKindSourceAudit,
						usageFactHistoryKindRecentAudit},
					usageFactHistoryJobPaused, job.FromTs, job.FromTs).
				Updates(map[string]any{"status": usageFactHistoryJobQueued, "attempts": 0, "next_retry_at": 0,
					"lease_owner": "", "lease_until": 0, "last_error": "", "updated_at": now.Unix()}).Error; err != nil {
				return err
			}
			var remaining int64
			if err := tx.Model(&UsageFactJob{}).Where("user_id = ? AND tracked_revision = ? AND source_epoch = ? AND kind IN ? AND status NOT IN ?",
				*job.UserID, job.TrackedRevision, job.SourceEpoch,
				[]string{usageFactHistoryKindRepair, usageFactHistoryKindRepairHour},
				[]string{usageFactHistoryJobComplete, usageFactHistoryJobCancelled}).Count(&remaining).Error; err != nil {
				return err
			}
			if remaining == 0 {
				if err := tx.Model(&UsageFactMemberState{}).
					Where("user_id = ? AND active = ? AND tracked_revision = ? AND source_epoch = ?",
						*job.UserID, true, job.TrackedRevision, job.SourceEpoch).
					Updates(map[string]any{"coverage_status": "ready", "verification_status": "complete",
						"last_success_at": now.Unix(), "last_failure_at": 0, "last_error": "", "updated_at": now.Unix()}).Error; err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Publication is member-scoped: one company with a paused repair must not
	// hold every already-healed company behind a global "all repairs complete"
	// barrier. The publisher independently excludes members that still have an
	// active repair signature.
	if _, err := m.publishUsageFactFullHistorySnapshot(ctx, now); err != nil {
		return err
	}
	m.refreshUsageFactsReadiness(ctx, now)
	return nil
}

func (m *Monitor) executeUsageFactHistoryLocalAudit(ctx context.Context, claim usageFactHistoryClaim, now time.Time) error {
	if len(claim.Jobs) == 0 {
		return nil
	}
	type auditResult struct {
		job UsageFactJob
		err error
	}
	results := make([]auditResult, 0, len(claim.Jobs))
	m.usageFactsSyncMu.Lock()
	db := m.usageFactsStore().WithContext(ctx)
	for _, job := range claim.Jobs {
		var state UsageFactMemberState
		if job.UserID == nil {
			results = append(results, auditResult{job: job, err: errors.New("local audit member missing")})
			continue
		}
		err := db.First(&state, "user_id = ?", *job.UserID).Error
		if err == nil && (state.TrackedRevision != job.TrackedRevision || state.SourceEpoch != job.SourceEpoch) {
			err = fmt.Errorf("%w: local audit signature changed", errUsageMemberControlIntegrity)
		}
		if err == nil {
			auditThrough := min(claim.Through, job.ThroughTs)
			if state.SourceHistoryStatus == "no_history" {
				err = auditUsageFactKnownEmptyRange(db, *job.UserID, claim.From, auditThrough)
			} else {
				factFrom := claim.From
				if state.SourceFirstLogHour == nil {
					err = errors.New("local audit first-log boundary missing")
				} else {
					if factFrom < *state.SourceFirstLogHour {
						prefixThrough := min(auditThrough, *state.SourceFirstLogHour)
						err = auditUsageFactKnownEmptyRange(db, *job.UserID, factFrom, prefixThrough)
						factFrom = prefixThrough
					}
					if err == nil && factFrom < auditThrough {
						err = auditUsageFactFullHistoryDayRange(db, state, factFrom, auditThrough)
					}
				}
			}
		}
		results = append(results, auditResult{job: job, err: err})
	}
	m.usageFactsSyncMu.Unlock()

	var combined error
	for _, result := range results {
		if result.err != nil {
			var dayErr *usageFactMemberDayAuditError
			if result.job.UserID != nil && errors.As(result.err, &dayErr) &&
				dayErr.UserID == *result.job.UserID && usageFactDayStart(dayErr.DayTs) == dayErr.DayTs &&
				dayErr.DayTs >= claim.From && dayErr.DayTs < min(claim.Through, result.job.ThroughTs) {
				enqueueErr := m.persistProvenUsageFactMismatch(result.job, claim.LeaseOwner, dayErr.UserID, dayErr.DayTs,
					result.err, "rolling local audit: "+result.err.Error(), "system-local-audit", now)
				combined = errors.Join(combined, result.err, enqueueErr)
				continue
			}
			// A database/lease/manifest failure does not prove which day is bad.
			// Retrying the audit cursor is safe; manufacturing a repair for
			// claim.From would repeatedly rewrite a healthy day and never heal the
			// actual defect.
			one := usageFactHistoryClaim{Jobs: []UsageFactJob{result.job}, LeaseOwner: claim.LeaseOwner,
				From: claim.From, Through: claim.Through}
			releaseErr := m.releaseUsageFactHistoryClaim(ctx, one, result.err, now,
				usageFactHistoryFailureIsSourceGlobal(result.err))
			combined = errors.Join(combined, result.err, releaseErr)
			continue
		}
		if err := m.advanceUsageFactAuditJob(ctx, result.job, claim.LeaseOwner, claim.Through, now); err != nil {
			combined = errors.Join(combined, err)
		}
	}
	return combined
}

func (m *Monitor) executeUsageFactHistorySourceAudit(ctx context.Context, claim usageFactHistoryClaim, now time.Time) error {
	ids, _, err := usageFactHistoryClaimIDs(claim)
	if err != nil {
		_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
		return err
	}
	for _, job := range claim.Jobs {
		if err := m.usageFactJobRevisionCurrent(ctx, job); err != nil {
			_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
			return err
		}
	}
	ranges := make(map[int64]usageFactHistoryRange, len(ids))
	failures := make(map[int64]error)
	budget := usageFactSourceAuditFetchBudget
	m.fetchUsageFactSourceAuditAdaptive(ctx, claim.From, claim.Through, ids, &budget, ranges, failures)
	byID := make(map[int64]UsageFactJob, len(claim.Jobs))
	for _, job := range claim.Jobs {
		byID[*job.UserID] = job
	}
	var combined error
	// A successful adaptive fetch is also a half-open bulk probe. Every member
	// in a shared fetch carries the same aggregate timing, so record it once.
	for _, observed := range ranges {
		if recordErr := m.recordUsageFactHistoryBulkSuccess(context.Background(), observed, now); recordErr != nil {
			combined = errors.Join(combined, recordErr)
		}
		break
	}
	circuitFailureRecorded := false
	for _, id := range ids {
		job := byID[id]
		if failure := failures[id]; failure != nil {
			one := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: claim.LeaseOwner,
				From: claim.From, Through: claim.Through}
			if usageFactHistoryBulkTimeout(failure) {
				if !circuitFailureRecorded {
					combined = errors.Join(combined, m.recordUsageFactHistoryBulkFailure(context.Background(), failure, now))
					circuitFailureRecorded = true
				}
				combined = errors.Join(combined, failure,
					m.releaseUsageFactHistoryClaim(context.Background(), one, failure, now, true))
				continue
			}
			if errors.Is(failure, errUsageFactAdaptiveBudget) || usageFactHistoryFailureIsSourceGlobal(failure) {
				combined = errors.Join(combined, failure,
					m.releaseUsageFactHistoryClaim(context.Background(), one, failure, now, true))
				continue
			}
			if usageFactHistoryRangeShouldFallback(failure) {
				// A row cap/control race is an incomplete audit, not proof that the
				// currently signed daily fact is wrong. Persist a non-revoking hourly
				// verification. Its finalizer compares an independent day control and
				// atomically repairs only if the 24 source hours really changed.
				cause := fmt.Errorf("authoritative source audit requires hourly verification user=%d day=%d: %w", id, claim.From, failure)
				downgradeErr := m.downgradeUsageFactSourceAuditToHourly(context.Background(), one, cause, now)
				if downgradeErr != nil {
					// A failed local state transition must not leave the source lease
					// occupied for two minutes. It is still not a proven data mismatch, so
					// release without consuming a member attempt and keep serving the
					// last signed fact.
					downgradeErr = errors.Join(downgradeErr,
						m.releaseUsageFactHistoryClaim(context.Background(), one, downgradeErr, now, true))
				}
				combined = errors.Join(combined, cause, downgradeErr)
				continue
			}
			combined = errors.Join(combined, failure,
				m.releaseUsageFactHistoryClaim(context.Background(), one, failure, now, false))
			continue
		}
		result, ok := ranges[id]
		if !ok {
			failure := errors.New("source audit adaptive result missing")
			one := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: claim.LeaseOwner,
				From: claim.From, Through: claim.Through}
			combined = errors.Join(combined, failure,
				m.releaseUsageFactHistoryClaim(context.Background(), one, failure, now, true))
			continue
		}
		key := usageFactMemberDayKey{userID: id, dayTs: claim.From}
		rows := result.Facts[key]
		control := result.Controls[key]
		var proof UsageFactMemberDayState
		proofErr := m.usageFactsStore().WithContext(ctx).First(&proof, "user_id = ? AND date_ts = ?", id, claim.From).Error
		matches := proofErr == nil && usageFactMemberDayHistoryReady(proof) && proof.SourceEpoch == job.SourceEpoch &&
			proof.SourceResultHash == usageFactHistoryControlHash(control) && proof.FactContentHash == usageDailyFactContentHash(rows) &&
			usageFactMemberDayStrictMetricsMatchState(dailyFactsMetrics(rows), proof)
		if proofErr != nil && !errors.Is(proofErr, gorm.ErrRecordNotFound) {
			one := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: claim.LeaseOwner,
				From: claim.From, Through: claim.Through}
			combined = errors.Join(combined, proofErr,
				m.releaseUsageFactHistoryClaim(context.Background(), one, proofErr, now, false))
			continue
		}
		if !matches {
			cause := fmt.Errorf("authoritative source audit mismatch user=%d day=%d", id, claim.From)
			enqueueErr := m.persistProvenUsageFactMismatch(job, claim.LeaseOwner, id, claim.From,
				cause, cause.Error(), "system-source-audit", now)
			combined = errors.Join(combined, cause, enqueueErr)
			continue
		}
		if err := m.advanceUsageFactAuditJob(ctx, job, claim.LeaseOwner, claim.Through, now); err != nil {
			combined = errors.Join(combined, err)
		}
	}
	return combined
}

// fetchUsageFactSourceAuditAdaptive isolates workload-local failures without
// allowing one worker turn to walk an unbounded binary tree.  Unattempted
// members receive a no-penalty budget sentinel and are claimed again; isolated
// bad members become exact repair jobs while healthy peers advance.
func (m *Monitor) fetchUsageFactSourceAuditAdaptive(
	ctx context.Context,
	from, through int64,
	ids []int64,
	budget *int,
	success map[int64]usageFactHistoryRange,
	failed map[int64]error,
) {
	fetchUsageFactSourceAuditAdaptiveWith(ctx, from, through, ids, budget, success, failed, m.fetchUsageFactHistoryRange)
}

type usageFactSourceAuditFetcher func(context.Context, int64, int64, []int64) (usageFactHistoryRange, error)

func fetchUsageFactSourceAuditAdaptiveWith(
	ctx context.Context,
	from, through int64,
	ids []int64,
	budget *int,
	success map[int64]usageFactHistoryRange,
	failed map[int64]error,
	fetch usageFactSourceAuditFetcher,
) {
	if len(ids) == 0 {
		return
	}
	if budget == nil || *budget <= 0 || fetch == nil {
		for _, id := range ids {
			failed[id] = errUsageFactAdaptiveBudget
		}
		return
	}
	*budget--
	result, err := fetch(ctx, from, through, ids)
	if err == nil {
		for _, id := range ids {
			success[id] = result
		}
		return
	}
	if len(ids) == 1 || usageFactHistoryFailureIsSourceGlobal(err) || !usageFactHistoryRangeShouldFallback(err) {
		for _, id := range ids {
			failed[id] = err
		}
		return
	}
	// Do not restart a depth-first binary tree on every turn: with 50 members
	// and a four-query cap it can never reach a leaf, so the same lowest-ID bad
	// member strands all 49 peers forever. Probe one concrete member now and
	// release the remainder without penalty. Once this member advances/repairs,
	// the durable ordered claim naturally rotates to the next member.
	fetchUsageFactSourceAuditAdaptiveWith(ctx, from, through, ids[:1], budget, success, failed, fetch)
	for _, id := range ids[1:] {
		failed[id] = errUsageFactAdaptiveBudget
	}
}

// deferUsageFactAuditForRepairHold persists the fact that a proven mismatch
// could not yet create its revoke+repair transaction.  Readiness treats this
// marker as fail-closed across restart, while the queued audit retries without
// burning five attempts merely because the local store was temporarily busy.
func (m *Monitor) deferUsageFactAuditForRepairHold(ctx context.Context, job UsageFactJob, owner string, cause error, now time.Time) error {
	message := usageFactAuditRepairHoldPendingPrefix + truncateUsageFactError(cause.Error())
	result := m.usageFactsStore().WithContext(ctx).Model(&UsageFactJob{}).
		Where("id = ? AND lease_owner = ? AND status = ?", job.ID, owner, usageFactHistoryJobRunning).
		Updates(map[string]any{"status": usageFactHistoryJobQueued, "lease_owner": "", "lease_until": 0,
			"next_retry_at": now.Add(time.Minute).Unix(), "last_error": message,
			"heartbeat_at": now.Unix(), "updated_at": now.Unix()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: audit repair hold lease changed", errUsageMemberControlIntegrity)
	}
	return nil
}

// deferUsageFactAuditForRepairHoldFresh deliberately starts a new bounded
// context after enqueue returned. enqueue serializes with the publisher and
// can therefore spend its original deadline waiting for usageFactsSyncMu;
// reusing that expired context would lose the only durable fail-closed marker
// for a mismatch that has already been proven.
func (m *Monitor) deferUsageFactAuditForRepairHoldFresh(job UsageFactJob, owner string, cause error, now time.Time) error {
	ctx, cancel := usageFactAuditDurableContext()
	defer cancel()
	return m.deferUsageFactAuditForRepairHold(ctx, job, owner, cause, now)
}

func (m *Monitor) pauseUsageFactAuditJob(ctx context.Context, claimed UsageFactJob, owner string, cause error, now time.Time) error {
	return m.usageFactsStore().WithContext(ctx).Model(&UsageFactJob{}).
		Where("id = ? AND lease_owner = ? AND status = ?", claimed.ID, owner, usageFactHistoryJobRunning).
		Updates(map[string]any{"status": usageFactHistoryJobPaused, "lease_owner": "", "lease_until": 0,
			"attempts": gorm.Expr("attempts + 1"), "last_error": truncateUsageFactError(cause.Error()),
			"heartbeat_at": now.Unix(), "updated_at": now.Unix()}).Error
}

func (m *Monitor) advanceUsageFactAuditJob(ctx context.Context, claimed UsageFactJob, owner string, through int64, now time.Time) error {
	next := min(through, claimed.ThroughTs)
	updates := map[string]any{"next_hour": next, "completed_hours": max(next-claimed.FromTs, 0) / usageFactHourSeconds,
		"status": usageFactHistoryJobQueued, "attempts": 0, "next_retry_at": 0, "lease_owner": "", "lease_until": 0,
		"last_error": "", "heartbeat_at": now.Unix(), "updated_at": now.Unix()}
	if next >= claimed.ThroughTs {
		updates["status"] = usageFactHistoryJobComplete
		updates["completed_at"] = now.Unix()
	}
	result := m.usageFactsStore().WithContext(ctx).Model(&UsageFactJob{}).
		Where("id = ? AND lease_owner = ? AND status = ?", claimed.ID, owner, usageFactHistoryJobRunning).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: audit lease changed job=%s", errUsageMemberControlIntegrity, claimed.ID)
	}
	return nil
}
