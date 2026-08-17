package monitor

// usage_facts_jobs.go is the durable control loop for per-member full-history
// coverage. Cold source reads remain in usage_facts_boundary/history.go; this
// file only schedules, fences and advances those operations. The legacy signed
// snapshot keeps serving until a member reaches a source-controlled day range
// and a current-hour Tail watermark.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

const (
	usageFactHistoryKindDiscover    = "full_history_discover"
	usageFactHistoryKindBackfill    = "full_history_backfill"
	usageFactHistoryKindTail        = "full_history_tail"
	usageFactHistoryKindVerify      = "full_history_verify"
	usageFactHistoryKindRepair      = "full_history_repair"
	usageFactHistoryKindRepairHour  = "full_history_repair_hour"
	usageFactHistoryKindLocalAudit  = "full_history_local_audit"
	usageFactHistoryKindSourceAudit = "full_history_source_audit"
	usageFactHistoryKindRecentAudit = "full_history_recent_source_audit"
	// A source audit that cannot fit one controlled natural-day detail query
	// is verified hour by hour without revoking the last signed member. The
	// final hour performs the independent day control and atomically replaces
	// the daily fact only when all 24 source reads agree.
	usageFactHistoryKindAuditHour = "full_history_source_audit_hour"

	usageFactHistoryJobQueued    = "queued"
	usageFactHistoryJobRunning   = "running"
	usageFactHistoryJobComplete  = "complete"
	usageFactHistoryJobPaused    = "paused"
	usageFactHistoryJobCancelled = "cancelled"

	usageFactHistoryLeaseDuration    = 2 * time.Minute
	usageFactHistoryRetryBase        = 15 * time.Minute
	usageFactBoundaryRecheck         = 24 * time.Hour
	usageFactHistoryHealthyToGrow    = 10
	usageFactFullPublishTimeout      = 60 * time.Second
	usageFactHistoryVerifyDays       = 14
	usageFactHistoryLocalAuditDays   = 14
	usageFactHistoryLocalAuditCycle  = 24 * time.Hour
	usageFactHistorySourceAuditCycle = 30 * 24 * time.Hour
	usageFactHistoryRecentAuditCycle = 24 * time.Hour
	usageFactHistoryRecentAuditDays  = 14
)

var (
	errUsageFactHistoryRetryConflict = errors.New("全历史任务当前不可重试")
	errUsageFactHistoryJobNotFound   = errors.New("全历史任务不存在")
	errUsageFactLegacyPublication    = errors.New("旧版有限窗口发布签名")
)

type UsageFactHistoryMemberProgress struct {
	UserID              int64   `json:"user_id"`
	TrackedRevision     int64   `json:"tracked_revision"`
	JobID               string  `json:"job_id"`
	Stage               string  `json:"stage"`
	JobStatus           string  `json:"job_status"`
	CoverageStatus      string  `json:"coverage_status"`
	SourceHistoryStatus string  `json:"source_history_status"`
	SourceFloorHour     *int64  `json:"source_floor_hour,omitempty"`
	SourceFirstLogHour  *int64  `json:"source_first_log_hour,omitempty"`
	CoverageThroughHour *int64  `json:"coverage_through_hour,omitempty"`
	TailThroughHour     *int64  `json:"tail_through_hour,omitempty"`
	TargetThroughHour   int64   `json:"target_through_hour"`
	CompletedHours      int64   `json:"completed_hours"`
	TotalHours          int64   `json:"total_hours"`
	VerifiedHours       int64   `json:"verified_hours"`
	VerificationStatus  string  `json:"verification_status"`
	CoveragePercent     float64 `json:"coverage_percent"`
	Attempts            int     `json:"attempts"`
	NextRetryAt         int64   `json:"next_retry_at"`
	LastSuccessAt       int64   `json:"last_success_at"`
	LastFailureAt       int64   `json:"last_failure_at"`
	LastError           string  `json:"last_error,omitempty"`
	Published           bool    `json:"published"`
}

type UsageFactHistoryProgress struct {
	Enabled           bool                             `json:"enabled"`
	SourceMode        string                           `json:"source_mode"`
	SourceEpoch       string                           `json:"source_epoch"`
	TotalKnown        bool                             `json:"total_known"`
	TotalMembers      int                              `json:"total_members"`
	ReadyMembers      int                              `json:"ready_members"`
	PublishedMembers  int                              `json:"published_members"`
	PendingMembers    int                              `json:"pending_members"`
	PausedMembers     int                              `json:"paused_members"`
	FailedMembers     int                              `json:"failed_members"`
	CompletedHours    int64                            `json:"completed_hours"`
	VerifiedHours     int64                            `json:"verified_hours"`
	TotalHours        int64                            `json:"total_hours"`
	CoveragePercent   float64                          `json:"coverage_percent"`
	EstimatedSeconds  *int64                           `json:"estimated_seconds"`
	EstimateStatus    string                           `json:"estimate_status"`
	ThroughputHoursPS float64                          `json:"throughput_hours_per_second"`
	EstimateSampleSec int64                            `json:"estimate_sample_seconds"`
	DiskBlocked       bool                             `json:"disk_blocked"`
	DiskPressure      string                           `json:"disk_pressure"`
	DiskFreeBytes     int64                            `json:"disk_free_bytes"`
	DiskUsedPercent   float64                          `json:"disk_used_percent"`
	AsOf              int64                            `json:"as_of"`
	Members           []UsageFactHistoryMemberProgress `json:"members"`
	Maintenance       []UsageFactMaintenanceProgress   `json:"maintenance"`
	ActiveRepairs     int                              `json:"active_repairs"`
	PausedMaintenance int                              `json:"paused_maintenance"`
	BulkCircuitState  string                           `json:"bulk_circuit_state"`
	BulkOpenedUntil   int64                            `json:"bulk_opened_until"`
	BulkSlowStreak    int                              `json:"bulk_slow_streak"`
	BulkFailureStreak int                              `json:"bulk_failure_streak"`
	BulkLastQueryMS   int64                            `json:"bulk_last_query_ms"`
	BulkLastQueryAt   int64                            `json:"bulk_last_query_at"`
	BulkLastError     string                           `json:"bulk_last_error,omitempty"`
}

type UsageFactMaintenanceProgress struct {
	JobID           string `json:"job_id"`
	Kind            string `json:"kind"`
	UserID          int64  `json:"user_id"`
	TrackedRevision int64  `json:"tracked_revision"`
	Status          string `json:"status"`
	FromTs          int64  `json:"from_ts"`
	ThroughTs       int64  `json:"through_ts"`
	NextHour        int64  `json:"next_hour"`
	CompletedHours  int64  `json:"completed_hours"`
	TotalHours      int64  `json:"total_hours"`
	Attempts        int    `json:"attempts"`
	NextRetryAt     int64  `json:"next_retry_at"`
	LastError       string `json:"last_error,omitempty"`
	Reason          string `json:"reason,omitempty"`
}

type usageFactHistoryTuning struct {
	chunkDays   int
	memberLimit int
	healthy     int
	localWork   bool
	coldTurns   int
	auditCursor int
}

func defaultUsageFactHistoryTuning() usageFactHistoryTuning {
	return usageFactHistoryTuning{chunkDays: usageFactHistoryInitialMaxDays, memberLimit: usageFactHistoryMaxMembers}
}

// usageFactsFullHistoryMode describes the on-disk/read semantics. A restored
// immutable snapshot must retain this mode even though it has no source worker.
func (m *Monitor) usageFactsFullHistoryMode() bool {
	return m.cfg.UsageFactsFullHistoryEnabled
}

// usageFactsFullHistoryEnabled is intentionally the worker/mutation gate.
// LocalSnapshotOnly may validate and serve a full-history signature, but must
// never claim a job, query MySQL, retry repair work, or mutate the snapshot.
func (m *Monitor) usageFactsFullHistoryEnabled() bool {
	return m.cfg.UsageFactsEnabled && m.cfg.UsageFactsFullHistoryEnabled && !m.cfg.LocalSnapshotOnly
}

func (m *Monitor) usageFactHistoryDelay() time.Duration {
	ms := m.cfg.UsageFactsHistoryDelayMS
	if ms < 0 { // deterministic tests
		return 0
	}
	if ms < 15_000 {
		ms = 30_000
	}
	if ms > 60_000 {
		ms = 60_000
	}
	return time.Duration(ms) * time.Millisecond
}

func (m *Monitor) usageFactHistorySourceCooldown(queryDuration time.Duration) time.Duration {
	pct := m.cfg.UsageFactsHistoryDutyPercent
	if pct <= 0 || pct > 100 {
		pct = 20
	}
	if pct >= 100 || queryDuration <= 0 {
		return 0
	}
	return queryDuration * time.Duration(100-pct) / time.Duration(pct)
}

func usageFactHistoryJobID(userID, revision int64) string {
	return "fh-" + strconv.FormatInt(userID, 36) + "-r" + strconv.FormatInt(revision, 36)
}

func usageFactHistoryJobKey(userID, revision int64) *string {
	key := usageFactHistoryJobID(userID, revision)
	return &key
}

func usageFactHistoryRetryAt(now time.Time, attempts int) int64 {
	if attempts < 1 {
		attempts = 1
	}
	shift := attempts - 1
	if shift > 2 {
		shift = 2
	}
	return now.Add(usageFactHistoryRetryBase * time.Duration(1<<shift)).Unix()
}

// usageFactHistoryObservedETA deliberately uses only durable progress and
// timestamps. It remains useful after restart without pretending that a short
// or stale sample is a reliable promise to an administrator.
func usageFactHistoryObservedETA(done, total, startedAt, lastProgressAt int64, now time.Time) (*int64, string, float64, int64) {
	if total <= 0 || done >= total {
		return nil, "warming", 0, 0
	}
	nowUnix := now.Unix()
	if startedAt <= 0 || startedAt >= nowUnix || done <= 0 {
		return nil, "warming", 0, 0
	}
	elapsed := nowUnix - startedAt
	if elapsed < 5*60 {
		return nil, "warming", 0, elapsed
	}
	if lastProgressAt <= 0 || nowUnix-lastProgressAt > int64(time.Hour/time.Second) {
		return nil, "stalled", 0, elapsed
	}
	rate := float64(done) / float64(elapsed)
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return nil, "warming", 0, elapsed
	}
	seconds := int64(math.Ceil(float64(total-done) / rate))
	if seconds < 1 {
		seconds = 1
	}
	return &seconds, "observed", rate, elapsed
}

func (m *Monitor) usageFactHistoryProgress(ctx context.Context, now time.Time) (UsageFactHistoryProgress, error) {
	if now.IsZero() {
		now = time.Now()
	}
	out := UsageFactHistoryProgress{
		Enabled: m.usageFactsFullHistoryMode(), AsOf: now.Unix(),
		SourceMode:  strings.TrimSpace(m.cfg.UsageFactsHistorySourceMode),
		SourceEpoch: strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch),
		TotalKnown:  true, EstimateStatus: "warming",
	}
	out.DiskBlocked = m.usageFactsHistoryDiskBlocked.Load()
	out.DiskPressure = usageFactDiskPressureLevel(m.usageFactsHistoryDiskLevel.Load()).String()
	out.DiskFreeBytes = m.usageFactsHistoryDiskFreeBytes.Load()
	out.DiskUsedPercent = float64(m.usageFactsHistoryDiskUsedBPS.Load()) / 100
	if !out.Enabled {
		return out, nil
	}
	var syncState UsageFactSyncState
	if err := m.usageFactsStore().WithContext(ctx).First(&syncState, 1).Error; err != nil {
		return out, err
	}
	out.BulkCircuitState = normalizedUsageFactBulkCircuitState(syncState.HistoryBulkCircuitState)
	out.BulkOpenedUntil = syncState.HistoryBulkOpenedUntil
	out.BulkSlowStreak = syncState.HistoryBulkSlowStreak
	out.BulkFailureStreak = syncState.HistoryBulkFailureStreak
	out.BulkLastQueryMS = syncState.HistoryBulkLastQueryMS
	out.BulkLastQueryAt = syncState.HistoryBulkLastQueryAt
	out.BulkLastError = truncateUsageFactError(syncState.HistoryBulkLastError)
	snapshot, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return out, err
	}
	var states []UsageFactMemberState
	if err := m.usageFactsStore().WithContext(ctx).Order("user_id").Find(&states).Error; err != nil {
		return out, err
	}
	stateByID := make(map[int64]UsageFactMemberState, len(states))
	for _, state := range states {
		stateByID[state.UserID] = state
	}
	var jobs []UsageFactJob
	allKinds := []string{usageFactHistoryKindDiscover, usageFactHistoryKindBackfill, usageFactHistoryKindTail,
		usageFactHistoryKindVerify, usageFactHistoryKindRepair, usageFactHistoryKindRepairHour,
		usageFactHistoryKindLocalAudit, usageFactHistoryKindSourceAudit, usageFactHistoryKindRecentAudit,
		usageFactHistoryKindAuditHour}
	if err := m.usageFactsStore().WithContext(ctx).Where("kind IN ?", allKinds).Order("priority DESC, created_at, id").Find(&jobs).Error; err != nil {
		return out, err
	}
	jobByID := make(map[string]UsageFactJob, len(jobs))
	for _, job := range jobs {
		if usageFactHistoryMaintenanceKind(job.Kind) {
			userID := int64(0)
			if job.UserID != nil {
				userID = *job.UserID
			}
			out.Maintenance = append(out.Maintenance, UsageFactMaintenanceProgress{
				JobID: job.ID, Kind: job.Kind, UserID: userID, TrackedRevision: job.TrackedRevision,
				Status: job.Status, FromTs: job.FromTs, ThroughTs: job.ThroughTs, NextHour: job.NextHour,
				CompletedHours: job.CompletedHours, TotalHours: job.TotalHours, Attempts: job.Attempts,
				NextRetryAt: job.NextRetryAt, LastError: truncateUsageFactError(job.LastError), Reason: job.Reason,
			})
			if (job.Kind == usageFactHistoryKindRepair || job.Kind == usageFactHistoryKindRepairHour) &&
				!usageFactHistoryTerminal(job.Status) {
				out.ActiveRepairs++
			}
			if job.Status == usageFactHistoryJobPaused {
				out.PausedMaintenance++
			}
			continue
		}
		jobByID[job.ID] = job
	}
	var published []UsageFactPublishedMember
	if err := m.usageFactsStore().WithContext(ctx).Find(&published).Error; err != nil {
		return out, err
	}
	publishedByID := make(map[int64]UsageFactPublishedMember, len(published))
	for _, row := range published {
		publishedByID[row.UserID] = row
	}
	target := m.usageFactFinalizedHour(now)
	workDone, workTotal := int64(0), int64(0)
	startedAt, lastProgressAt := int64(0), int64(0)
	out.Members = make([]UsageFactHistoryMemberProgress, 0, len(snapshot.Tracked))
	for _, tracked := range snapshot.Tracked {
		control := snapshot.Controls[tracked.UserID]
		state := stateByID[tracked.UserID]
		job := jobByID[usageFactHistoryJobID(tracked.UserID, control.TrackedRevision)]
		item := UsageFactHistoryMemberProgress{
			UserID: tracked.UserID, TrackedRevision: control.TrackedRevision,
			JobID: job.ID,
			Stage: job.Kind, JobStatus: job.Status, CoverageStatus: state.CoverageStatus,
			SourceHistoryStatus: state.SourceHistoryStatus, SourceFloorHour: state.SourceFloorHour,
			SourceFirstLogHour: state.SourceFirstLogHour, CoverageThroughHour: state.CoverageThroughHour,
			TailThroughHour: state.TailThroughHour, TargetThroughHour: job.ThroughTs,
			CompletedHours: job.CompletedHours, TotalHours: job.TotalHours, Attempts: job.Attempts,
			VerifiedHours: job.VerifiedHours, VerificationStatus: state.VerificationStatus,
			NextRetryAt: job.NextRetryAt, LastSuccessAt: state.LastSuccessAt,
			LastFailureAt: state.LastFailureAt, LastError: truncateUsageFactError(firstNonEmpty(job.LastError, state.LastError)),
		}
		if item.TargetThroughHour == 0 {
			item.TargetThroughHour = target
		}
		if item.TotalHours > 0 {
			item.CoveragePercent = float64(min(item.CompletedHours, item.TotalHours)) * 100 / float64(item.TotalHours)
		}
		if row, ok := publishedByID[tracked.UserID]; ok && usageFactPublishedMemberCompatible(row, control) {
			item.Published = true
			out.PublishedMembers++
		}
		ready := job.Status == usageFactHistoryJobComplete && job.ThroughTs >= target &&
			job.SourceEpoch == strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch) &&
			usageFactMemberFullHistoryReady(state, control.TrackedRevision, target, strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch))
		if ready {
			item.VerifiedHours = item.TotalHours
			out.ReadyMembers++
		} else {
			out.PendingMembers++
		}
		if job.Status == usageFactHistoryJobPaused {
			out.PausedMembers++
		}
		if state.CoverageStatus == "failed" {
			out.FailedMembers++
		}
		out.CompletedHours += min(item.CompletedHours, item.TotalHours)
		out.VerifiedHours += min(item.VerifiedHours, item.TotalHours)
		out.TotalHours += item.TotalHours
		workDone += min(item.CompletedHours, item.TotalHours) + min(item.VerifiedHours, item.TotalHours)
		workTotal += item.TotalHours * 2
		jobStartedAt := job.StartedAt
		if jobStartedAt <= 0 {
			jobStartedAt = job.CreatedAt
		}
		if jobStartedAt > 0 && (startedAt == 0 || jobStartedAt < startedAt) {
			startedAt = jobStartedAt
		}
		if state.LastSuccessAt > lastProgressAt {
			lastProgressAt = state.LastSuccessAt
		}
		if job.FromTs <= 0 || job.TotalHours <= 0 {
			out.TotalKnown = false
		}
		out.Members = append(out.Members, item)
	}
	out.TotalMembers = len(out.Members)
	if out.TotalHours > 0 {
		out.CoveragePercent = float64(out.CompletedHours) * 100 / float64(out.TotalHours)
	}
	if out.PendingMembers == 0 && out.TotalKnown {
		zero := int64(0)
		out.EstimatedSeconds = &zero
		out.EstimateStatus = "complete"
	} else if !out.TotalKnown {
		out.EstimateStatus = "boundary_discovery"
	} else if out.PausedMembers > 0 || out.FailedMembers > 0 || out.PausedMaintenance > 0 {
		out.EstimateStatus = "blocked"
	} else {
		out.EstimatedSeconds, out.EstimateStatus, out.ThroughputHoursPS, out.EstimateSampleSec =
			usageFactHistoryObservedETA(workDone, workTotal, startedAt, lastProgressAt, now)
	}
	if out.DiskBlocked {
		out.EstimatedSeconds = nil
		out.EstimateStatus = "paused_disk"
	} else if out.BulkCircuitState == usageFactBulkCircuitOpen {
		out.EstimatedSeconds = nil
		out.EstimateStatus = "source_circuit_open"
	}
	return out, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func (m *Monitor) retryUsageFactHistoryJobTarget(ctx context.Context, userID int64, jobID string, now time.Time) (UsageFactJob, error) {
	if !m.usageFactsFullHistoryEnabled() || userID <= 0 {
		return UsageFactJob{}, errUsageFactHistoryRetryConflict
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
		return UsageFactJob{}, errUsageFactHistoryRetryConflict
	}
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		jobID = usageFactHistoryJobID(userID, control.TrackedRevision)
	}
	var result UsageFactJob
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&result, "id = ?", jobID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errUsageFactHistoryJobNotFound
			}
			return err
		}
		if result.UserID == nil || *result.UserID != userID || result.TrackedRevision != control.TrackedRevision ||
			result.SourceEpoch != strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch) ||
			result.Status == usageFactHistoryJobCancelled || result.Status == usageFactHistoryJobComplete {
			return errUsageFactHistoryRetryConflict
		}
		// Repeated retry is idempotent: once queued/running, never rewind the
		// cursor or reset a live lease a second time.
		if result.Status == usageFactHistoryJobQueued || result.Status == usageFactHistoryJobRunning {
			return nil
		}
		if result.Status != usageFactHistoryJobPaused {
			return errUsageFactHistoryRetryConflict
		}
		result.Status = usageFactHistoryJobQueued
		result.Attempts = 0
		result.NextRetryAt = 0
		result.LeaseOwner, result.LeaseUntil = "", 0
		result.LastError = ""
		result.UpdatedAt = now.Unix()
		if err := tx.Save(&result).Error; err != nil {
			return err
		}
		if usageFactHistoryMaintenanceKind(result.Kind) {
			if result.Kind == usageFactHistoryKindRepair || result.Kind == usageFactHistoryKindRepairHour {
				return tx.Model(&UsageFactMemberState{}).
					Where("user_id = ? AND active = ? AND tracked_revision = ? AND source_epoch = ?",
						userID, true, control.TrackedRevision, result.SourceEpoch).
					Updates(map[string]any{"coverage_status": "repairing", "verification_status": "repair_pending",
						"last_error": result.Reason, "updated_at": now.Unix()}).Error
			}
			return nil
		}
		stage := "backfilling"
		if result.Kind == usageFactHistoryKindDiscover {
			stage = "discovering"
		}
		return tx.Model(&UsageFactMemberState{}).
			Where("user_id = ? AND active = ? AND tracked_revision = ?", userID, true, control.TrackedRevision).
			Updates(map[string]any{"coverage_status": stage, "last_error": "", "updated_at": now.Unix()}).Error
	})
	if err != nil {
		return UsageFactJob{}, err
	}
	after, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return UsageFactJob{}, err
	}
	if !usageMemberControlSnapshotsEqual(snapshot, after) {
		return UsageFactJob{}, fmt.Errorf("%w: member manifest changed during history retry", errUsageMemberControlIntegrity)
	}
	return result, nil
}

func usageFactHistoryTerminal(status string) bool {
	return status == usageFactHistoryJobComplete || status == usageFactHistoryJobCancelled
}

// reconcileUsageFactHistoryJobs mirrors the authoritative active revisions and
// creates at most one target-watermark job per member. FromTs/ThroughTs never
// move after discovery; NextHour is the crash-resumable cursor.
func (m *Monitor) reconcileUsageFactHistoryJobs(ctx context.Context, now time.Time) error {
	if !m.usageFactsFullHistoryEnabled() {
		return nil
	}
	before, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return err
	}
	target := m.usageFactFinalizedHour(now)
	if target <= 0 {
		return errors.New("全历史目标水位无效")
	}
	nowUnix := now.Unix()
	sourceEpoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
	if strings.ToLower(strings.TrimSpace(m.cfg.UsageFactsHistorySourceMode)) != "complete" || sourceEpoch == "" {
		return errors.New("full-history source completeness has not been signed")
	}
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var syncState UsageFactSyncState
		if err := tx.First(&syncState, 1).Error; err != nil {
			return err
		}
		if usageFactRepairActive(syncState) {
			return errors.New("legacy usage-facts repair must finish before enabling full-history mode")
		}
		active := make(map[int64]UsageMemberControl, len(before.Controls))
		for id, control := range before.Controls {
			active[id] = control
		}
		// Source epoch is part of every durable work signature. A queued,
		// paused, or expired-running repair from the previous epoch must never be
		// claimed against the new source and commit under its old audit identity.
		if err := tx.Model(&UsageFactJob{}).
			Where("COALESCE(source_epoch,'') <> ? AND status NOT IN ?", sourceEpoch,
				[]string{usageFactHistoryJobComplete, usageFactHistoryJobCancelled}).
			Updates(map[string]any{"status": usageFactHistoryJobCancelled, "lease_owner": "", "lease_until": 0,
				"completed_at": nowUnix, "updated_at": nowUnix, "last_error": "source epoch changed"}).Error; err != nil {
			return err
		}

		var states []UsageFactMemberState
		if err := tx.Find(&states).Error; err != nil {
			return err
		}
		stateByID := make(map[int64]UsageFactMemberState, len(states))
		for _, state := range states {
			stateByID[state.UserID] = state
		}

		// A stale revision can finish a source query but can never publish or
		// advance coverage. Cancel its durable work before creating the new job.
		for id, control := range active {
			if err := tx.Model(&UsageFactJob{}).
				Where("user_id = ? AND tracked_revision <> ? AND status NOT IN ?", id, control.TrackedRevision,
					[]string{usageFactHistoryJobComplete, usageFactHistoryJobCancelled}).
				Updates(map[string]any{"status": usageFactHistoryJobCancelled, "lease_owner": "", "lease_until": 0,
					"completed_at": nowUnix, "updated_at": nowUnix, "last_error": "member revision changed"}).Error; err != nil {
				return err
			}
		}

		for id, control := range active {
			state, found := stateByID[id]
			if !found {
				state = UsageFactMemberState{UserID: id}
			}
			primaryJobID := usageFactHistoryJobID(id, control.TrackedRevision)
			revisionChanged := found && state.TrackedRevision != control.TrackedRevision
			epochChanged := state.SourceEpoch != sourceEpoch
			semanticChanged := state.ClassificationVersion != userTrafficClassificationVersion ||
				state.QuerySemanticsVersion != usageFactQuerySemanticsVersion
			resetRequired := revisionChanged || epochChanged || semanticChanged
			if resetRequired {
				// Removal does not delete facts, but the source may receive refunds or
				// historical corrections while the member is inactive. A rejoin must
				// re-audit every retained day before the new revision can publish.
				reason := "member revision changed; source recheck required"
				if epochChanged {
					reason = "source epoch changed; source recheck required"
				} else if semanticChanged {
					reason = "fact semantics changed; source recheck required"
				}
				// Repair and rolling-audit jobs do not own the member's semantic
				// signature. Cancel every ancillary checkpoint in the same
				// transaction that resets the member; otherwise a queued/running
				// job from an older classification/query contract can be claimed
				// after restart and make an old proof look current. The primary
				// coverage job is reset below and keeps its stable idempotency key.
				if err := tx.Model(&UsageFactJob{}).
					Where("user_id = ? AND id <> ? AND status <> ?", id, primaryJobID, usageFactHistoryJobCancelled).
					Updates(map[string]any{"status": usageFactHistoryJobCancelled, "lease_owner": "", "lease_until": 0,
						"completed_at": nowUnix, "updated_at": nowUnix, "last_error": reason}).Error; err != nil {
					return err
				}
				if err := tx.Model(&UsageFactMemberDayState{}).Where("user_id = ?", id).
					Updates(map[string]any{"status": "pending", "source_checked_at": 0,
						"last_error": reason, "updated_at": nowUnix}).Error; err != nil {
					return err
				}
				state.CoverageThroughHour = nil
				state.TailThroughHour = nil
				state.VerifyNextHour = nil
				state.VerifiedThroughHour = nil
				state.VerificationStatus = "pending"
				state.VerifiedAt = 0
				state.SourceFloorCheckedAt = 0
				// Preserve the last signed boundary as a monotonicity fence. Discovery
				// must reject a later MIN/earlier MAX instead of silently treating an
				// archive or source truncation as a legitimate empty prefix.
				state.SourceHistoryStatus = "discovering"
				state.CoverageStatus = "discovering"
			}
			state.Active = true
			state.TrackedRevision = control.TrackedRevision
			state.ClassificationVersion = userTrafficClassificationVersion
			state.QuerySemanticsVersion = usageFactQuerySemanticsVersion
			state.SourceEpoch = sourceEpoch
			state.UpdatedAt = nowUnix
			if !found || resetRequired || state.CoverageStatus == "" || state.CoverageStatus == "inactive" {
				state.CoverageStatus = "discovering"
			}
			if !found || resetRequired || state.SourceFloorHour == nil || state.SourceFloorCheckedAt <= 0 {
				state.SourceHistoryStatus = "discovering"
			}
			if err := tx.Save(&state).Error; err != nil {
				return err
			}

			coverage := int64(0)
			if state.CoverageThroughHour != nil {
				coverage = *state.CoverageThroughHour
			}
			tail := int64(0)
			if state.TailThroughHour != nil {
				tail = *state.TailThroughHour
			}
			boundaryStale := state.SourceFloorHour == nil || state.SourceFloorCheckedAt < now.Add(-usageFactBoundaryRecheck).Unix()
			verified := int64(0)
			if state.VerifiedThroughHour != nil {
				verified = *state.VerifiedThroughHour
			}
			if state.CoverageStatus == "ready" && state.VerificationStatus == "complete" && !boundaryStale &&
				coverage >= usageFactDayStart(target) && verified >= usageFactDayStart(target) && tail >= target {
				continue
			}

			jobID := primaryJobID
			var existing UsageFactJob
			err := tx.First(&existing, "id = ?", jobID).Error
			if err == nil {
				existingChanged := false
				if existing.SourceEpoch != sourceEpoch || resetRequired {
					existing.SourceEpoch = sourceEpoch
					existing.Kind = usageFactHistoryKindDiscover
					existing.Status = usageFactHistoryJobQueued
					existing.FromTs, existing.NextHour, existing.VerifyNextHour = 0, 0, 0
					existing.CompletedHours, existing.VerifiedHours = 0, 0
					existing.Attempts, existing.NextRetryAt = 0, 0
					existing.LeaseOwner, existing.LeaseUntil = "", 0
					existing.CompletedAt = 0
					existing.LastError = ""
					existingChanged = true
				}
				if boundaryStale && existing.Status == usageFactHistoryJobComplete {
					existing.Kind = usageFactHistoryKindDiscover
					existing.Status = usageFactHistoryJobQueued
					existing.CompletedAt = 0
					existing.NextRetryAt = 0
					existing.LastError = ""
					existingChanged = true
				}
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
					existingChanged = true
				}
				if existingChanged {
					existing.UpdatedAt = nowUnix
					if err := tx.Save(&existing).Error; err != nil {
						return err
					}
				}
				continue
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			memberID := id
			kind := usageFactHistoryKindDiscover
			from, next := int64(0), int64(0)
			if !boundaryStale && state.SourceFloorHour != nil {
				kind, from = usageFactHistoryKindBackfill, *state.SourceFloorHour
				next = coverage
				if next < from {
					next = from
				}
				if next >= usageFactDayStart(target) {
					kind = usageFactHistoryKindTail
					next = max(next, usageFactDayStart(target))
				}
			}
			job := UsageFactJob{
				ID: jobID, IdempotencyKey: usageFactHistoryJobKey(id, control.TrackedRevision),
				Kind: kind, Priority: 50, UserID: &memberID, TrackedRevision: control.TrackedRevision,
				SourceEpoch: sourceEpoch,
				FromTs:      from, ThroughTs: target, NextHour: next,
				Status: usageFactHistoryJobQueued, Reason: "full history coverage",
				RequestedBy: "system", ApprovedBy: "system", CreatedAt: nowUnix, UpdatedAt: nowUnix,
			}
			if from > 0 {
				job.TotalHours = max(target-from, 0) / usageFactHourSeconds
				job.CompletedHours = max(next-from, 0) / usageFactHourSeconds
			}
			if err := tx.Create(&job).Error; err != nil {
				return err
			}
		}

		for _, state := range states {
			if _, keep := active[state.UserID]; keep || !state.Active {
				continue
			}
			if err := tx.Model(&UsageFactMemberState{}).Where("user_id = ?", state.UserID).
				Updates(map[string]any{"active": false, "coverage_status": "inactive", "updated_at": nowUnix}).Error; err != nil {
				return err
			}
			if err := tx.Model(&UsageFactJob{}).Where("user_id = ? AND status NOT IN ?", state.UserID,
				[]string{usageFactHistoryJobComplete, usageFactHistoryJobCancelled}).
				Updates(map[string]any{"status": usageFactHistoryJobCancelled, "lease_owner": "", "lease_until": 0,
					"completed_at": nowUnix, "updated_at": nowUnix, "last_error": "member removed"}).Error; err != nil {
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
	if !usageMemberControlSnapshotsEqual(before, after) {
		return fmt.Errorf("%w: member manifest changed while scheduling full history", errUsageMemberControlIntegrity)
	}
	if _, _, localErr := m.reconcileUsageFactPublishedMembersLocal(ctx); localErr != nil {
		return localErr
	}
	// This also removes a deleted/stale revision from the publication table.
	// A partial new member is ignored, so reconciliation never blocks the
	// previously signed service snapshot.
	if _, publishErr := m.publishUsageFactFullHistorySnapshot(ctx, now); publishErr != nil && ctx.Err() == nil {
		// Scheduling and current Tail must continue even if the candidate cannot
		// be published. Existing rows remain fail-closed behind the revision
		// intersection and the next idle reconciliation retries publication.
		slog.Warn("full-history publication deferred after reconciliation", "err", publishErr)
	}
	return m.reconcileUsageFactHistoryAuditJobs(ctx, now)
}

type usageFactHistoryClaim struct {
	Jobs       []UsageFactJob
	LeaseOwner string
	From       int64
	Through    int64
}

func (m *Monitor) claimUsageFactHistoryJobs(ctx context.Context, kind, owner string, limit, chunkDays int, now time.Time) (usageFactHistoryClaim, error) {
	var claim usageFactHistoryClaim
	if limit < 1 {
		limit = 1
	}
	if limit > usageFactHistoryMaxMembers {
		limit = usageFactHistoryMaxMembers
	}
	if chunkDays < 1 {
		chunkDays = 1
	}
	if chunkDays > usageFactHistoryMaxDays {
		chunkDays = usageFactHistoryMaxDays
	}
	nowUnix := now.Unix()
	err := m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var first UsageFactJob
		orderColumn := "next_hour"
		if kind == usageFactHistoryKindVerify {
			orderColumn = "verify_next_hour"
		}
		query := tx.Where("kind = ? AND status IN ? AND next_retry_at <= ? AND (lease_until = 0 OR lease_until < ?)",
			kind, []string{usageFactHistoryJobQueued, usageFactHistoryJobRunning}, nowUnix, nowUnix).
			Order("priority DESC, " + orderColumn + ", created_at, id")
		if err := query.First(&first).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		q := tx.Where("kind = ? AND status IN ? AND next_retry_at <= ? AND (lease_until = 0 OR lease_until < ?)",
			kind, []string{usageFactHistoryJobQueued, usageFactHistoryJobRunning}, nowUnix, nowUnix)
		if kind == usageFactHistoryKindVerify {
			q = q.Where("verify_next_hour = ?", first.VerifyNextHour)
		} else if kind != usageFactHistoryKindDiscover {
			q = q.Where("next_hour = ?", first.NextHour)
		}
		if err := q.Order("priority DESC, created_at, id").Limit(limit).Find(&claim.Jobs).Error; err != nil {
			return err
		}
		if len(claim.Jobs) == 0 {
			return nil
		}
		ids := make([]string, 0, len(claim.Jobs))
		for _, job := range claim.Jobs {
			ids = append(ids, job.ID)
		}
		leaseUntil := now.Add(usageFactHistoryLeaseDuration).Unix()
		updates := map[string]any{"status": usageFactHistoryJobRunning, "lease_owner": owner,
			"lease_until": leaseUntil, "heartbeat_at": nowUnix, "updated_at": nowUnix,
			"started_at": gorm.Expr("CASE WHEN started_at = 0 THEN ? ELSE started_at END", nowUnix)}
		if err := tx.Model(&UsageFactJob{}).Where("id IN ?", ids).Updates(updates).Error; err != nil {
			return err
		}
		claim.LeaseOwner = owner
		claim.From = first.NextHour
		if kind == usageFactHistoryKindVerify {
			claim.From = first.VerifyNextHour
		}
		if kind == usageFactHistoryKindBackfill {
			claim.Through = first.NextHour + int64(chunkDays)*usageFactDaySeconds
			for _, job := range claim.Jobs {
				dayTarget := usageFactDayStart(job.ThroughTs)
				if dayTarget < claim.Through {
					claim.Through = dayTarget
				}
			}
		} else if kind == usageFactHistoryKindTail || kind == usageFactHistoryKindRepairHour ||
			kind == usageFactHistoryKindAuditHour {
			claim.Through = first.NextHour + usageFactHourSeconds
		} else if kind == usageFactHistoryKindVerify {
			claim.Through = claim.From + int64(usageFactHistoryVerifyDays)*usageFactDaySeconds
			for _, job := range claim.Jobs {
				dayTarget := usageFactDayStart(job.ThroughTs)
				if dayTarget < claim.Through {
					claim.Through = dayTarget
				}
			}
		} else if kind == usageFactHistoryKindRepair || kind == usageFactHistoryKindSourceAudit || kind == usageFactHistoryKindRecentAudit {
			claim.Through = first.NextHour + usageFactDaySeconds
			for _, job := range claim.Jobs {
				if job.ThroughTs < claim.Through {
					claim.Through = job.ThroughTs
				}
			}
		} else if kind == usageFactHistoryKindLocalAudit {
			claim.Through = first.NextHour + int64(usageFactHistoryLocalAuditDays)*usageFactDaySeconds
			for _, job := range claim.Jobs {
				if job.ThroughTs < claim.Through {
					claim.Through = job.ThroughTs
				}
			}
		}
		return nil
	})
	return claim, err
}

func (m *Monitor) releaseUsageFactHistoryClaim(ctx context.Context, claim usageFactHistoryClaim, cause error, now time.Time, immediate bool) error {
	if len(claim.Jobs) == 0 {
		return nil
	}
	ids := make([]string, 0, len(claim.Jobs))
	for _, job := range claim.Jobs {
		ids = append(ids, job.ID)
	}
	message := ""
	if cause != nil {
		message = truncateUsageFactError(cause.Error())
	}
	return m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current []UsageFactJob
		if err := tx.Where("id IN ? AND lease_owner = ?", ids, claim.LeaseOwner).Find(&current).Error; err != nil {
			return err
		}
		for _, job := range current {
			job.Status = usageFactHistoryJobQueued
			job.LeaseOwner, job.LeaseUntil = "", 0
			if !immediate {
				job.Attempts++
				job.LastError = message
			}
			job.UpdatedAt = now.Unix()
			job.HeartbeatAt = now.Unix()
			if !immediate {
				job.NextRetryAt = usageFactHistoryRetryAt(now, job.Attempts)
			}
			if job.Attempts >= 5 && !immediate {
				job.Status = usageFactHistoryJobPaused
			}
			if err := tx.Save(&job).Error; err != nil {
				return err
			}
			maintenanceJob := job.Kind == usageFactHistoryKindRepair || job.Kind == usageFactHistoryKindRepairHour ||
				job.Kind == usageFactHistoryKindLocalAudit || job.Kind == usageFactHistoryKindSourceAudit ||
				job.Kind == usageFactHistoryKindRecentAudit || job.Kind == usageFactHistoryKindAuditHour
			if job.UserID != nil && !immediate && !maintenanceJob {
				updates := map[string]any{"last_failure_at": now.Unix(), "last_error": message, "updated_at": now.Unix()}
				if job.Status == usageFactHistoryJobPaused {
					updates["coverage_status"] = "failed"
				}
				if err := tx.Model(&UsageFactMemberState{}).Where("user_id = ? AND tracked_revision = ?", *job.UserID, job.TrackedRevision).
					Updates(updates).Error; err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// usageFactHistoryFailureIsSourceGlobal separates a source/lifecycle outage
// from a member-specific bad range. A network break, read-only permission
// failure or source epoch drain must never consume five per-member attempts and
// strand every durable cursor in paused. The source lifecycle owns the global
// retry/blocked state; jobs remain queued and resume from the same cursor when
// that lifecycle becomes ready again.
func usageFactHistoryFailureIsSourceGlobal(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, errSourceNotReady) ||
		errors.Is(err, errUsageFactSourceBusy) || errors.Is(err, errUsageFactLeaseBusy) ||
		errors.Is(err, errUsageFactAdaptiveBudget) {
		return true
	}
	return usageFactHistoryShouldReportSourceError(err)
}

func usageFactHistoryClaimIDs(claim usageFactHistoryClaim) ([]int64, map[int64]int64, error) {
	ids := make([]int64, 0, len(claim.Jobs))
	revisions := make(map[int64]int64, len(claim.Jobs))
	for _, job := range claim.Jobs {
		if job.UserID == nil || *job.UserID <= 0 || job.TrackedRevision < 1 {
			return nil, nil, errors.New("全历史任务缺少成员 revision")
		}
		id := *job.UserID
		ids = append(ids, id)
		revisions[id] = job.TrackedRevision
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, revisions, nil
}

func usageFactHistoryWorkerOwner() string {
	return "history-" + strconv.Itoa(os.Getpid()) + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func (m *Monitor) runUsageFactHistoryWorker(ctx context.Context) {
	owner := usageFactHistoryWorkerOwner()
	tuning := defaultUsageFactHistoryTuning()
	if err := m.reconcileUsageFactHistoryJobs(ctx, time.Now()); err != nil && ctx.Err() == nil {
		slog.Warn("全历史任务初始化失败", "err", err)
	}
	for ctx.Err() == nil {
		tuning.localWork = false
		worked, err := m.syncNextUsageFactHistoryWork(ctx, owner, &tuning, time.Now())
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errSourceNotReady) {
			slog.Warn("全历史任务执行失败", "err", err)
		}
		if worked {
			delay := m.usageFactHistoryDelay()
			if tuning.localWork {
				delay = 10 * time.Millisecond // yield without applying source-QPS pacing to SQLite-only verification
			}
			if !waitUsageFact(ctx, delay) {
				return
			}
			continue
		}
		if errors.Is(err, errUsageFactHistoryDiskPressure) {
			// At 80% the cold queue is intentionally paused; at 85% every
			// derived writer is stopped. Do not run the otherwise-idle reconcile
			// transaction behind the capacity gate.
			if !waitUsageFact(ctx, time.Minute) {
				return
			}
			continue
		}
		if err := m.reconcileUsageFactHistoryJobs(ctx, time.Now()); err != nil && ctx.Err() == nil {
			slog.Warn("全历史任务对账失败", "err", err)
		}
		if !waitUsageFact(ctx, time.Minute) {
			return
		}
	}
}

func (m *Monitor) superviseUsageFactHistoryWorker(ctx context.Context) {
	for ctx.Err() == nil {
		exit := m.runUsageFactHistoryWorkerSafely(ctx)
		if ctx.Err() != nil {
			return
		}
		m.usageFactsHistoryRestarts.Add(1)
		if exit.panicValue != nil {
			slog.Error("全历史事实 worker 异常退出，将自动重启", "panic", fmt.Sprint(exit.panicValue), "stack", string(exit.panicStack))
		} else {
			slog.Error("全历史事实 worker 意外退出，将自动重启")
		}
		if !waitUsageFact(ctx, 5*time.Second) {
			return
		}
	}
}

func (m *Monitor) runUsageFactHistoryWorkerSafely(ctx context.Context) (exit usageFactsLoopExit) {
	defer func() {
		if panicValue := recover(); panicValue != nil {
			exit.panicValue = panicValue
			exit.panicStack = debug.Stack()
		}
	}()
	m.runUsageFactHistoryWorker(ctx)
	return exit
}

func (m *Monitor) syncNextUsageFactHistoryWork(ctx context.Context, owner string, tuning *usageFactHistoryTuning, now time.Time) (bool, error) {
	if !m.usageFactsFullHistoryEnabled() {
		return false, nil
	}
	if ok, capacityErr := m.usageFactHistoryCapacityOK(); !ok {
		// Disk protection pauses cold/import/verify/repair work, not the bounded
		// current Tail. Keeping the latest hour fresh consumes little reserve and
		// avoids turning a capacity warning into a customer-visible freshness
		// outage. A Tail write failure still leaves its durable cursor untouched.
		if usageFactDiskPressureLevel(m.usageFactsHistoryDiskLevel.Load()) >= usageFactDiskCritical {
			return false, capacityErr
		}
		claim, err := m.claimUsageFactHistoryJobs(ctx, usageFactHistoryKindTail, owner, usageFactHistoryMaxMembers, 1, now)
		if err != nil {
			return false, err
		}
		if len(claim.Jobs) > 0 {
			return true, m.executeUsageFactHistoryTail(ctx, claim, now)
		}
		return false, capacityErr
	}
	if usageFactDiskPressureLevel(m.usageFactsHistoryDiskLevel.Load()) >= usageFactDiskThrottled {
		// At the 70% tier keep cold progress but prevent adaptive widening from
		// multiplying WAL/temp growth. Tail remains on its normal bounded batch.
		tuning.chunkDays = 1
		if tuning.memberLimit > 10 {
			tuning.memberLimit = 10
		}
	}
	// Local semantic verification has a 24-hour SLO but performs no source SQL.
	// Drain it in small bounded SQLite batches on an independent lane instead of
	// making 500 published members wait behind 12 source-paced cold turns each
	// (which previously had a theoretical 50-hour floor). The 10ms local yield in
	// the worker prevents a tight loop while preserving Tail/source fairness.
	claim, err := m.claimUsageFactHistoryJobs(ctx, usageFactHistoryKindLocalAudit, owner, 8, usageFactHistoryLocalAuditDays, now)
	if err != nil || len(claim.Jobs) > 0 {
		if err != nil {
			return false, err
		}
		tuning.localWork = true
		return true, m.executeUsageFactHistoryLocalAudit(ctx, claim, now)
	}
	claim, err = m.claimUsageFactHistoryJobs(ctx, usageFactHistoryKindRepairHour, owner, usageFactHistoryMaxMembers, 1, now)
	if err != nil || len(claim.Jobs) > 0 {
		if err != nil {
			return false, err
		}
		return true, m.executeUsageFactHistoryRepairHour(ctx, claim, now)
	}
	claim, err = m.claimUsageFactHistoryJobs(ctx, usageFactHistoryKindRepair, owner, usageFactHistoryMaxMembers, 1, now)
	if err != nil || len(claim.Jobs) > 0 {
		if err != nil {
			return false, err
		}
		return true, m.executeUsageFactHistoryRepair(ctx, claim, now)
	}
	claim, err = m.claimUsageFactHistoryJobs(ctx, usageFactHistoryKindDiscover, owner, usageFactHistoryBoundaryBatch, 1, now)
	if err != nil || len(claim.Jobs) > 0 {
		if err != nil {
			return false, err
		}
		return true, m.executeUsageFactHistoryDiscovery(ctx, claim, now)
	}
	claim, err = m.claimUsageFactHistoryJobs(ctx, usageFactHistoryKindTail, owner, usageFactHistoryMaxMembers, 1, now)
	if err != nil || len(claim.Jobs) > 0 {
		if err != nil {
			return false, err
		}
		return true, m.executeUsageFactHistoryTail(ctx, claim, now)
	}
	claim, err = m.claimUsageFactHistoryJobs(ctx, usageFactHistoryKindVerify, owner, usageFactHistoryMaxMembers, usageFactHistoryVerifyDays, now)
	if err != nil || len(claim.Jobs) > 0 {
		if err != nil {
			return false, err
		}
		tuning.localWork = true
		return true, m.executeUsageFactHistoryVerify(ctx, claim, now)
	}
	bulkAllowed, halfOpen, err := m.usageFactHistoryBulkCircuitAllowed(ctx, now)
	if err != nil {
		return false, err
	}
	if !bulkAllowed {
		return false, nil
	}
	if halfOpen {
		tuning.chunkDays = 1
		tuning.memberLimit = 1
		tuning.healthy = 0
	}
	// Reserve every fourth cold slot for a round-robin maintenance class. This
	// prevents a years-long new-member import from starving already-published
	// members' local/recent/cold audits, while three of four slots remain
	// available to make deterministic backfill progress.
	if tuning.coldTurns%4 == 3 {
		worked, auditErr := m.syncNextUsageFactHistoryAuditWork(ctx, owner, tuning, now)
		if auditErr != nil || worked {
			tuning.coldTurns++
			return worked, auditErr
		}
	}
	claim, err = m.claimUsageFactHistoryJobs(ctx, usageFactHistoryKindBackfill, owner, tuning.memberLimit, tuning.chunkDays, now)
	if err != nil || len(claim.Jobs) > 0 {
		if err != nil {
			return false, err
		}
		tuning.coldTurns++
		return true, m.executeUsageFactHistoryBackfill(ctx, claim, tuning, now)
	}
	worked, err := m.syncNextUsageFactHistoryAuditWork(ctx, owner, tuning, now)
	if worked || err != nil {
		tuning.coldTurns++
	}
	return worked, err
}

func (m *Monitor) syncNextUsageFactHistoryAuditWork(ctx context.Context, owner string, tuning *usageFactHistoryTuning, now time.Time) (bool, error) {
	// Finish a previously downgraded member-day before starting another daily
	// source audit. This remains on the cold maintenance lane, so 24 hourly
	// reads cannot pre-empt current Tail or exact repairs.
	claim, err := m.claimUsageFactHistoryJobs(ctx, usageFactHistoryKindAuditHour, owner, usageFactHistoryMaxMembers, 1, now)
	if err != nil {
		return false, err
	}
	if len(claim.Jobs) > 0 {
		return true, m.executeUsageFactHistorySourceAuditHour(ctx, claim, now)
	}
	kinds := []string{usageFactHistoryKindRecentAudit, usageFactHistoryKindSourceAudit}
	for offset := 0; offset < len(kinds); offset++ {
		index := (tuning.auditCursor + offset) % len(kinds)
		kind := kinds[index]
		limit, days := usageFactHistoryMaxMembers, 1
		claim, err := m.claimUsageFactHistoryJobs(ctx, kind, owner, limit, days, now)
		if err != nil {
			return false, err
		}
		if len(claim.Jobs) == 0 {
			continue
		}
		tuning.auditCursor = (index + 1) % len(kinds)
		return true, m.executeUsageFactHistorySourceAudit(ctx, claim, now)
	}
	return false, nil
}

// The execution methods below keep every revision/lease check next to its
// corresponding local commit; they are intentionally not HTTP handlers.

func (m *Monitor) executeUsageFactHistoryDiscovery(ctx context.Context, claim usageFactHistoryClaim, now time.Time) error {
	ids, revisions, err := usageFactHistoryClaimIDs(claim)
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
	boundaries, err := m.discoverUsageFactSourceBoundaries(ctx, ids)
	if err != nil {
		if len(claim.Jobs) > 1 && usageFactHistoryRangeShouldFallback(err) {
			// Boundary SQL contains two indexed seeks per member. Recursing through
			// both halves in one turn can otherwise expand one overloaded 50-member
			// query into 99 calls and outlive the durable lease. Spend at most one
			// additional query on the first member; release every unattempted sibling
			// without attempts/backoff. Once the first job advances or is isolated,
			// the durable queue naturally rotates the next member to the front.
			attempted := usageFactHistoryClaim{Jobs: claim.Jobs[:1], LeaseOwner: claim.LeaseOwner, From: claim.From, Through: claim.Through}
			deferred := usageFactHistoryClaim{Jobs: claim.Jobs[1:], LeaseOwner: claim.LeaseOwner, From: claim.From, Through: claim.Through}
			attemptErr := m.executeUsageFactHistoryDiscovery(ctx, attempted, now)
			deferErr := m.releaseUsageFactHistoryClaim(context.Background(), deferred, errUsageFactAdaptiveBudget, now, true)
			return errors.Join(attemptErr, deferErr)
		}
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, usageFactHistoryFailureIsSourceGlobal(err))
		return err
	}
	byID := make(map[int64]usageFactSourceBoundary, len(boundaries))
	for _, boundary := range boundaries {
		byID[boundary.UserID] = boundary
	}

	var revokedGeneration, revokedServingGeneration int64
	var revokedState UsageFactSyncState
	m.usageFactsSyncMu.Lock()
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		revokePublished := make(map[int64]bool)
		for _, claimed := range claim.Jobs {
			id := *claimed.UserID
			var job UsageFactJob
			if err := tx.First(&job, "id = ? AND lease_owner = ? AND status = ?", claimed.ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; err != nil {
				return err
			}
			var state UsageFactMemberState
			if err := tx.First(&state, "user_id = ?", id).Error; err != nil {
				return err
			}
			if !state.Active || state.TrackedRevision != revisions[id] {
				return fmt.Errorf("%w: stale discovery user_id=%d", errUsageMemberControlIntegrity, id)
			}
			boundary := byID[id]
			floor, ok := boundary.sourceFloorHour()
			if !ok {
				// A previously signed member whose authoritative user/boundary has
				// disappeared is no longer safe to serve. Revoke only that member in
				// this transaction; a first-time partial member simply has no row to
				// delete and remains admin-only.
				revokePublished[id] = true
				job.Status = usageFactHistoryJobPaused
				job.LastError = "source boundary unknown"
				job.LeaseOwner, job.LeaseUntil = "", 0
				job.UpdatedAt = now.Unix()
				state.SourceHistoryStatus = "boundary_unknown"
				state.CoverageStatus = "failed"
				state.LastFailureAt = now.Unix()
				state.LastError = job.LastError
				state.UpdatedAt = now.Unix()
				if err := tx.Save(&job).Error; err != nil {
					return err
				}
				if err := tx.Save(&state).Error; err != nil {
					return err
				}
				continue
			}
			priorHistoryStatus := state.SourceHistoryStatus
			priorFloor := state.SourceFloorHour
			priorFirst := state.SourceFirstLogHour
			priorCeiling := state.SourceCeilingHour
			newFirst, hasNewFirst := boundary.historyStartHour()
			newCeiling, hasNewCeiling := boundary.sourceCeilingHour()
			regressionReason := ""
			if priorFirst != nil {
				switch {
				case !hasNewFirst:
					regressionReason = "source boundary regressed: previously visible history disappeared"
				case newFirst > *priorFirst:
					regressionReason = "source boundary regressed: earliest retained log moved later"
				}
			}
			if regressionReason == "" && priorCeiling != nil && (!hasNewCeiling || newCeiling < *priorCeiling) {
				regressionReason = "source boundary regressed: latest retained log moved earlier"
			}
			if regressionReason != "" {
				revokePublished[id] = true
				job.Status = usageFactHistoryJobPaused
				job.LastError = regressionReason
				job.LeaseOwner, job.LeaseUntil = "", 0
				job.NextRetryAt = usageFactHistoryRetryAt(now, max(job.Attempts, 1))
				job.UpdatedAt = now.Unix()
				state.SourceFloorCheckedAt = now.Unix()
				state.SourceHistoryStatus = "source_incomplete"
				state.CoverageStatus = "failed"
				state.VerificationStatus = "failed"
				state.LastFailureAt = now.Unix()
				state.LastError = regressionReason
				state.UpdatedAt = now.Unix()
				if err := tx.Save(&job).Error; err != nil {
					return err
				}
				if err := tx.Save(&state).Error; err != nil {
					return err
				}
				continue
			}
			if state.SourceFloorHour != nil && *state.SourceFloorHour < floor {
				floor = *state.SourceFloorHour // a later source view may never shrink signed history
			}
			floorExpanded := priorFloor != nil && floor < *priorFloor
			state.SourceFloorHour = int64Ptr(floor)
			state.SourceEpoch = strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
			if hasNewCeiling {
				state.SourceCeilingHour = int64Ptr(newCeiling)
			} else {
				state.SourceCeilingHour = nil
			}
			state.SourceFloorCheckedAt = now.Unix()
			state.SourceHistoryStatus = "complete_hot"

			dayTarget := usageFactDayStart(job.ThroughTs)
			next := floor
			hasSourceLogs := false
			if historyStart, hasLogs := boundary.historyStartHour(); hasLogs {
				hasSourceLogs = true
				// Keep the first source row separate from the conservative account
				// floor.  Registration -> first-log is proven empty by the same
				// boundary query and must not require one synthetic proof per day;
				// first-log -> coverage is the range that needs signed day proofs.
				state.SourceFirstLogHour = int64Ptr(historyStart)
				verifyNext := historyStart
				boundaryExpanded := priorFirst != nil && historyStart < *priorFirst
				historyExpanded := boundaryExpanded || floorExpanded
				if !historyExpanded && state.VerificationStatus == "complete" &&
					state.VerifiedThroughHour != nil && *state.VerifiedThroughHour > verifyNext {
					verifyNext = *state.VerifiedThroughHour
				}
				state.VerifyNextHour = int64Ptr(verifyNext)
				if historyExpanded {
					state.VerificationStatus = "pending"
				}
				if historyStart > next {
					next = historyStart // MIN proves the prefix is known-empty
				}
				next, err = contiguousUsageFactHistoryCoverage(tx, id, next, dayTarget)
				if err != nil {
					return err
				}
			} else {
				// A successful no-row MIN/MAX query proves the complete requested
				// source interval empty; do not manufacture one proof row per day.
				state.SourceFirstLogHour = nil
				next = job.ThroughTs
				state.SourceHistoryStatus = "no_history"
				state.VerifyNextHour = int64Ptr(floor)
				state.VerifiedThroughHour = nil
				state.VerificationStatus = "pending"
				state.VerifiedAt = 0
			}
			if (priorHistoryStatus == "no_history" && hasNewFirst) ||
				(priorFirst != nil && hasNewFirst && newFirst < *priorFirst) ||
				floorExpanded {
				revokePublished[id] = true
			}
			if next > job.ThroughTs {
				next = job.ThroughTs
			}
			coverageThrough := min(next, dayTarget)
			state.CoverageThroughHour = int64Ptr(coverageThrough)
			// A monotonically increasing MAX(created_at) is the normal live Tail,
			// not a historical-boundary mutation. Keep the last signed service
			// checkpoint ready while the durable job extends its candidate watermark;
			// only a newly discovered left-side interval invalidates that signature.
			if revokePublished[id] || state.CoverageStatus != "ready" {
				state.CoverageStatus = "backfilling"
			}
			state.LastSuccessAt = now.Unix()
			state.LastFailureAt, state.LastError = 0, ""
			state.UpdatedAt = now.Unix()

			job.FromTs = floor
			job.SourceEpoch = state.SourceEpoch
			job.NextHour = next
			if state.VerifyNextHour != nil {
				job.VerifyNextHour = *state.VerifyNextHour
			}
			job.TotalHours = max(job.ThroughTs-floor, 0) / usageFactHourSeconds
			job.CompletedHours = max(next-floor, 0) / usageFactHourSeconds
			job.Status = usageFactHistoryJobQueued
			job.Kind = usageFactHistoryKindBackfill
			if next >= dayTarget {
				job.Kind = usageFactHistoryKindTail
			}
			if !hasSourceLogs && next >= job.ThroughTs {
				job.Status = usageFactHistoryJobQueued
				job.Kind = usageFactHistoryKindVerify
				job.VerifyNextHour = floor
				job.CompletedAt = 0
				state.TailThroughHour = int64Ptr(job.ThroughTs)
				state.CoverageStatus = "verifying"
			} else if next >= job.ThroughTs {
				job.Status = usageFactHistoryJobQueued
				job.Kind = usageFactHistoryKindVerify
				state.TailThroughHour = int64Ptr(job.ThroughTs)
				state.CoverageStatus = "verifying"
			}
			job.LeaseOwner, job.LeaseUntil = "", 0
			job.NextRetryAt = 0
			job.LastError = ""
			job.HeartbeatAt = now.Unix()
			job.UpdatedAt = now.Unix()
			if err := tx.Save(&job).Error; err != nil {
				return err
			}
			if err := tx.Save(&state).Error; err != nil {
				return err
			}
		}
		if len(revokePublished) > 0 {
			ids := make([]int64, 0, len(revokePublished))
			for id := range revokePublished {
				ids = append(ids, id)
			}
			if err := tx.Where("user_id IN ?", ids).Delete(&UsageFactPublishedMember{}).Error; err != nil {
				return err
			}
			var kept []UsageFactPublishedMember
			if err := tx.Order("user_id").Find(&kept).Error; err != nil {
				return err
			}
			var global UsageFactSyncState
			if err := tx.First(&global, 1).Error; err != nil {
				return err
			}
			if len(kept) == 0 {
				global.PublishedFingerprint = ""
				global.PublishedRangeStart = 0
				global.PublishedThrough = 0
				global.PublishedWindowDays = 0
				global.PublishedAt = 0
			} else {
				keptIDs := make([]int64, 0, len(kept))
				for _, row := range kept {
					keptIDs = append(keptIDs, row.UserID)
				}
				global.PublishedFingerprint = portalMemberFingerprintFromIDs(keptIDs)
				if err := normalizeUsageFactPublishedRange(&global, kept); err != nil {
					return err
				}
			}
			global.Generation++
			global.ServingGeneration++
			revokedGeneration, revokedServingGeneration = global.Generation, global.ServingGeneration
			revokedState = global
			if err := tx.Save(&global).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil && revokedGeneration > 0 {
		m.publishUsageFactGenerations(revokedGeneration, revokedServingGeneration)
		m.publishUsageFactReadBoundsAfterMutation(revokedState)
	}
	m.usageFactsSyncMu.Unlock()
	if err != nil {
		return err
	}
	_, err = m.publishUsageFactFullHistorySnapshot(ctx, now)
	return err
}

func contiguousUsageFactHistoryCoverage(tx *gorm.DB, userID, from, through int64) (int64, error) {
	if from >= through {
		return through, nil
	}
	var proofs []UsageFactMemberDayState
	if err := tx.Where("user_id = ? AND date_ts >= ? AND date_ts < ?", userID, from, through).
		Order("date_ts").Find(&proofs).Error; err != nil {
		return from, err
	}
	next := from
	for _, proof := range proofs {
		if proof.DateTs < next {
			continue
		}
		if proof.DateTs != next || !usageFactMemberDayHistoryReady(proof) {
			break
		}
		next += usageFactDaySeconds
	}
	return next, nil
}

func int64Ptr(value int64) *int64 { return &value }

func usageFactMemberFullHistoryReady(state UsageFactMemberState, revision, through int64, sourceEpoch string) bool {
	if !state.Active || revision < 1 || state.TrackedRevision != revision ||
		state.SourceFloorHour == nil || *state.SourceFloorHour <= 0 || state.SourceFloorCheckedAt <= 0 ||
		state.CoverageStatus != "ready" || state.CoverageThroughHour == nil ||
		*state.CoverageThroughHour < usageFactDayStart(through) || state.TailThroughHour == nil ||
		*state.TailThroughHour < through || state.VerificationStatus != "complete" ||
		state.VerifiedThroughHour == nil || *state.VerifiedThroughHour < usageFactDayStart(through) ||
		state.ClassificationVersion != userTrafficClassificationVersion ||
		state.QuerySemanticsVersion != usageFactQuerySemanticsVersion || sourceEpoch == "" || state.SourceEpoch != sourceEpoch {
		return false
	}
	if state.SourceHistoryStatus == "no_history" {
		return state.SourceFirstLogHour == nil
	}
	return state.SourceHistoryStatus == "complete_hot" && state.SourceFirstLogHour != nil &&
		*state.SourceFirstLogHour >= *state.SourceFloorHour
}

func auditUsageFactFullHistoryDayRange(db *gorm.DB, state UsageFactMemberState, start, end int64) error {
	if start >= end {
		return nil
	}
	if state.SourceEpoch == "" {
		return fmt.Errorf("full-history source epoch missing user=%d", state.UserID)
	}
	for batchStart := start; batchStart < end; {
		batchEnd := batchStart + int64(usageFactSemanticAuditDays)*usageFactDaySeconds
		if batchEnd > end {
			batchEnd = end
		}
		var proofs []UsageFactMemberDayState
		if err := db.Where("user_id = ? AND date_ts >= ? AND date_ts < ?", state.UserID, batchStart, batchEnd).
			Order("date_ts").Find(&proofs).Error; err != nil {
			return err
		}
		proofByDay := make(map[int64]UsageFactMemberDayState, len(proofs))
		for _, proof := range proofs {
			proofByDay[proof.DateTs] = proof
		}
		rows, err := loadUsageDailyFacts(db, batchStart, batchEnd, []int64{state.UserID})
		if err != nil {
			return err
		}
		rowsByDay := usageDailyFactsByMemberDay(rows)
		for day := batchStart; day < batchEnd; day += usageFactDaySeconds {
			proof, ok := proofByDay[day]
			memberRows := rowsByDay[usageFactMemberDayKey{userID: state.UserID, dayTs: day}]
			factHash := usageDailyFactContentHash(memberRows)
			if !ok || !usageFactMemberDayHistoryReady(proof) || proof.SourceEpoch != state.SourceEpoch ||
				!usageFactMemberDayStrictMetricsMatchState(dailyFactsMetrics(memberRows), proof) ||
				proof.ContentHash != factHash || proof.FactContentHash != factHash {
				return &usageFactMemberDayAuditError{UserID: state.UserID, DayTs: day, Kind: "full-history day proof mismatch"}
			}
		}
		batchStart = batchEnd
	}
	return nil
}

func (m *Monitor) executeUsageFactHistoryVerify(ctx context.Context, claim usageFactHistoryClaim, now time.Time) error {
	_, revisions, err := usageFactHistoryClaimIDs(claim)
	if err != nil {
		_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
		return err
	}
	if claim.From <= 0 || claim.Through < claim.From || usageFactDayStart(claim.From) != claim.From || usageFactDayStart(claim.Through) != claim.Through {
		err = errors.New("全历史本地校验范围无效")
		_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
		return err
	}
	epoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
	type verificationResult struct {
		state UsageFactMemberState
		err   error
	}
	results := make(map[int64]verificationResult, len(claim.Jobs))
	// Keep the local audit and checkpoint commit in one write-serialization
	// epoch. Tail/repair cannot mutate a verified day between the hash read and
	// the durable cursor update.
	m.usageFactsSyncMu.Lock()
	locked := true
	defer func() {
		if locked {
			m.usageFactsSyncMu.Unlock()
		}
	}()
	db := m.usageFactsStore().WithContext(ctx)
	for _, claimed := range claim.Jobs {
		id := *claimed.UserID
		if currentErr := m.usageFactJobRevisionCurrent(ctx, claimed); currentErr != nil {
			results[id] = verificationResult{err: currentErr}
			continue
		}
		var state UsageFactMemberState
		if loadErr := db.First(&state, "user_id = ?", id).Error; loadErr != nil {
			results[id] = verificationResult{err: loadErr}
			continue
		}
		if !state.Active || state.TrackedRevision != revisions[id] || state.SourceEpoch != epoch ||
			state.ClassificationVersion != userTrafficClassificationVersion ||
			state.QuerySemanticsVersion != usageFactQuerySemanticsVersion {
			results[id] = verificationResult{state: state, err: fmt.Errorf("%w: verify state changed user_id=%d", errUsageMemberControlIntegrity, id)}
			continue
		}
		verifyFrom := claim.From
		verifyThrough := min(claim.Through, usageFactDayStart(claimed.ThroughTs))
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
		if verifyErr == nil && state.SourceHistoryStatus != "no_history" &&
			verifyThrough >= usageFactDayStart(claimed.ThroughTs) && usageFactDayStart(claimed.ThroughTs) < claimed.ThroughTs {
			verifyErr = auditUsageFactTrailingHoursForEpoch(db, usageFactDayStart(claimed.ThroughTs), claimed.ThroughTs, []int64{id}, epoch)
		}
		results[id] = verificationResult{state: state, err: verifyErr}
	}

	nowUnix := time.Now().Unix()
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, claimed := range claim.Jobs {
			id := *claimed.UserID
			result := results[id]
			var job UsageFactJob
			if loadErr := tx.First(&job, "id = ? AND lease_owner = ? AND status = ?", claimed.ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; loadErr != nil {
				return loadErr
			}
			if job.Kind != usageFactHistoryKindVerify || job.VerifyNextHour != claim.From ||
				job.TrackedRevision != revisions[id] || job.SourceEpoch != epoch {
				return fmt.Errorf("%w: verify cursor changed user_id=%d", errUsageMemberControlIntegrity, id)
			}
			if result.err != nil {
				job.Attempts++
				job.Status = usageFactHistoryJobPaused
				job.LastError = truncateUsageFactError(result.err.Error())
				job.NextRetryAt = usageFactHistoryRetryAt(now, job.Attempts)
				job.LeaseOwner, job.LeaseUntil = "", 0
				job.HeartbeatAt, job.UpdatedAt = nowUnix, nowUnix
				if saveErr := tx.Save(&job).Error; saveErr != nil {
					return saveErr
				}
				if updateErr := tx.Model(&UsageFactMemberState{}).
					Where("user_id = ? AND active = ? AND tracked_revision = ? AND source_epoch = ?", id, true, job.TrackedRevision, epoch).
					Updates(map[string]any{"coverage_status": "failed", "verification_status": "failed",
						"last_failure_at": nowUnix, "last_error": job.LastError, "updated_at": nowUnix}).Error; updateErr != nil {
					return updateErr
				}
				continue
			}
			dayTarget := usageFactDayStart(job.ThroughTs)
			next := min(claim.Through, dayTarget)
			job.VerifyNextHour = next
			job.VerifiedHours = max(next-job.FromTs, 0) / usageFactHourSeconds
			job.Status = usageFactHistoryJobQueued
			job.Attempts, job.NextRetryAt = 0, 0
			job.LastError = ""
			job.LeaseOwner, job.LeaseUntil = "", 0
			job.HeartbeatAt, job.UpdatedAt = nowUnix, nowUnix
			updates := map[string]any{"verify_next_hour": next, "verified_through_hour": next,
				"verification_status": "running", "coverage_status": "verifying", "last_error": "", "updated_at": nowUnix}
			if next >= dayTarget && result.state.TailThroughHour != nil && *result.state.TailThroughHour >= job.ThroughTs {
				job.Status = usageFactHistoryJobComplete
				job.CompletedAt = nowUnix
				updates["verification_status"] = "complete"
				updates["coverage_status"] = "ready"
				updates["verified_at"] = nowUnix
				updates["last_success_at"] = nowUnix
			}
			if saveErr := tx.Save(&job).Error; saveErr != nil {
				return saveErr
			}
			resultDB := tx.Model(&UsageFactMemberState{}).
				Where("user_id = ? AND active = ? AND tracked_revision = ? AND source_epoch = ?", id, true, job.TrackedRevision, epoch).
				Updates(updates)
			if resultDB.Error != nil {
				return resultDB.Error
			}
			if resultDB.RowsAffected != 1 {
				return fmt.Errorf("%w: verify member state changed user_id=%d", errUsageMemberControlIntegrity, id)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	m.usageFactsSyncMu.Unlock()
	locked = false
	_, publishErr := m.publishUsageFactFullHistorySnapshot(ctx, now)
	return publishErr
}

func usageFactPublishedRowsEqual(a, b []UsageFactPublishedMember) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].UserID != b[i].UserID || a[i].TrackedRevision != b[i].TrackedRevision ||
			a[i].SourceEpoch != b[i].SourceEpoch || a[i].ClassificationVersion != b[i].ClassificationVersion ||
			a[i].QuerySemanticsVersion != b[i].QuerySemanticsVersion ||
			a[i].SourceFloorHour != b[i].SourceFloorHour || a[i].VerifiedThroughHour != b[i].VerifiedThroughHour {
			return false
		}
	}
	return true
}

func usageFactPublishedMemberLegacy(row UsageFactPublishedMember) bool {
	return row.SourceEpoch == "" && row.ClassificationVersion == 0 &&
		row.QuerySemanticsVersion == 0 && row.SourceFloorHour == 0 && row.VerifiedThroughHour == 0
}

func usageFactPublishedMemberHistorySignatureCurrent(row UsageFactPublishedMember, sourceEpoch string) bool {
	if usageFactPublishedMemberLegacy(row) {
		return true
	}
	return sourceEpoch != "" && row.SourceEpoch == sourceEpoch &&
		row.ClassificationVersion == userTrafficClassificationVersion &&
		row.QuerySemanticsVersion == usageFactQuerySemanticsVersion &&
		row.SourceFloorHour > 0 && row.VerifiedThroughHour > 0
}

func normalizeUsageFactPublishedRange(state *UsageFactSyncState, rows []UsageFactPublishedMember) error {
	if state == nil || len(rows) == 0 {
		return nil
	}
	minFloor := int64(0)
	for _, row := range rows {
		if usageFactPublishedMemberLegacy(row) {
			// Legacy rows share the bounded range already stored in sync state.
			return nil
		}
		if row.SourceFloorHour <= 0 {
			return fmt.Errorf("full-history published floor missing user_id=%d", row.UserID)
		}
		if minFloor == 0 || row.SourceFloorHour < minFloor {
			minFloor = row.SourceFloorHour
		}
	}
	if state.PublishedThrough <= minFloor {
		return errors.New("full-history published range is empty after local reconcile")
	}
	state.PublishedRangeStart = minFloor
	state.PublishedWindowDays = int((state.PublishedThrough - minFloor + usageFactDaySeconds - 1) / usageFactDaySeconds)
	if state.PublishedWindowDays < 1 {
		state.PublishedWindowDays = 1
	}
	return nil
}

// reconcileUsageFactPublishedMembersLocal removes authority/signature rows that
// can no longer be served without touching the source database.  It is safe to
// run while MySQL is unavailable, so remove/rejoin and a source-epoch change do
// not take unrelated customers down until a source worker happens to recover.
// A mixed legacy/full-history set from an interrupted upgrade is collapsed back
// to the bounded legacy service set; the publisher switches all legacy rows to
// signed full-history rows only after every active member is ready.
func (m *Monitor) reconcileUsageFactPublishedMembersLocal(ctx context.Context) (UsageFactSyncState, bool, error) {
	before, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return UsageFactSyncState{}, false, err
	}
	epoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	db := m.usageFactsStore().WithContext(ctx)
	var state UsageFactSyncState
	var kept []UsageFactPublishedMember
	changed := false
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&state, 1).Error; err != nil {
			return err
		}
		var current []UsageFactPublishedMember
		if err := tx.Order("user_id").Find(&current).Error; err != nil {
			return err
		}
		kept = make([]UsageFactPublishedMember, 0, len(current))
		hasLegacy, hasSigned := false, false
		for _, row := range current {
			control, ok := before.Controls[row.UserID]
			if !ok || !usageFactPublishedMemberCompatible(row, control) ||
				!usageFactPublishedMemberHistorySignatureCurrent(row, epoch) {
				changed = true
				continue
			}
			kept = append(kept, row)
			if usageFactPublishedMemberLegacy(row) {
				hasLegacy = true
			} else {
				hasSigned = true
			}
		}
		if hasLegacy && hasSigned {
			legacy := kept[:0]
			for _, row := range kept {
				if usageFactPublishedMemberLegacy(row) {
					legacy = append(legacy, row)
				}
			}
			kept = legacy
			changed = true
		}
		if !changed && len(kept) == len(current) {
			return nil
		}
		if err := tx.Where("1 = 1").Delete(&UsageFactPublishedMember{}).Error; err != nil {
			return err
		}
		if len(kept) > 0 {
			if err := tx.CreateInBatches(kept, usageFactProfileBatch).Error; err != nil {
				return err
			}
			ids := make([]int64, 0, len(kept))
			for _, row := range kept {
				ids = append(ids, row.UserID)
			}
			state.PublishedFingerprint = portalMemberFingerprintFromIDs(ids)
			if err := normalizeUsageFactPublishedRange(&state, kept); err != nil {
				return err
			}
		} else {
			state.PublishedFingerprint = ""
			state.PublishedRangeStart = 0
			state.PublishedThrough = 0
			state.PublishedWindowDays = 0
			state.PublishedAt = 0
		}
		state.Generation++
		state.ServingGeneration++
		return tx.Save(&state).Error
	})
	if err != nil {
		return UsageFactSyncState{}, false, err
	}
	if changed {
		m.publishUsageFactGenerations(state.Generation, state.ServingGeneration)
		m.publishUsageFactReadBoundsAfterMutation(state)
	}
	after, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return UsageFactSyncState{}, changed, err
	}
	if !usageMemberControlSnapshotsEqual(before, after) {
		return UsageFactSyncState{}, changed, fmt.Errorf("%w: member manifest changed during local publication reconcile", errUsageMemberControlIntegrity)
	}
	return state, changed, nil
}

// validateUsageFactFullHistoryCheckpoint is the restart/readiness signature.
// The expensive content verification was already committed incrementally by
// executeUsageFactHistoryVerify while holding usageFactsSyncMu. Startup checks
// only the immutable member/job watermarks and current authority manifest, so
// years of history cannot make every restart time out.
func (m *Monitor) validateUsageFactFullHistoryCheckpoint(ctx context.Context, through int64) error {
	if !m.usageFactsFullHistoryMode() || through <= 0 {
		return errors.New("full-history checkpoint is unavailable")
	}
	before, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return err
	}
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	db := m.usageFactsStore().WithContext(ctx)
	var current UsageFactSyncState
	if err := db.First(&current, 1).Error; err != nil {
		return err
	}
	if current.PublishedThrough != through || current.PublishedRangeStart <= 0 ||
		current.PublishedThrough <= current.PublishedRangeStart || current.TrafficClassVersion != userTrafficClassificationVersion {
		return errors.New("full-history published metadata is not current")
	}
	var published []UsageFactPublishedMember
	if err := db.Order("user_id").Find(&published).Error; err != nil {
		return err
	}
	activeRepairs, err := loadUsageFactActiveRepairUsers(db)
	if err != nil {
		return err
	}
	var pendingRepairHolds []UsageFactJob
	if err := db.Select("user_id").Where("kind IN ? AND status NOT IN ? AND last_error LIKE ?",
		[]string{usageFactHistoryKindLocalAudit, usageFactHistoryKindSourceAudit, usageFactHistoryKindRecentAudit},
		[]string{usageFactHistoryJobComplete, usageFactHistoryJobCancelled},
		usageFactAuditRepairHoldPendingPrefix+"%").Find(&pendingRepairHolds).Error; err != nil {
		return err
	}
	pendingRepairByUser := make(map[int64]bool, len(pendingRepairHolds))
	for _, job := range pendingRepairHolds {
		if job.UserID != nil {
			pendingRepairByUser[*job.UserID] = true
		}
	}
	if len(published) == 0 {
		return errors.New("full-history published membership is empty")
	}
	ids := make([]int64, 0, len(published))
	trailingIDs := make([]int64, 0, len(published))
	epoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
	minFloor := int64(0)
	for _, row := range published {
		if activeRepairs[row.UserID] {
			return fmt.Errorf("full-history repair hold is active user_id=%d", row.UserID)
		}
		if pendingRepairByUser[row.UserID] {
			return fmt.Errorf("full-history repair hold is pending user_id=%d", row.UserID)
		}
		control, ok := before.Controls[row.UserID]
		if !ok || !usageFactPublishedMemberCurrent(row, control) {
			return fmt.Errorf("%w: published member user_id=%d is stale", errUsageMemberControlIntegrity, row.UserID)
		}
		if row.SourceEpoch == "" || row.ClassificationVersion == 0 || row.QuerySemanticsVersion == 0 ||
			row.SourceFloorHour <= 0 || row.VerifiedThroughHour <= 0 {
			return errUsageFactLegacyPublication
		}
		if row.SourceEpoch != epoch || row.ClassificationVersion != userTrafficClassificationVersion ||
			row.QuerySemanticsVersion != usageFactQuerySemanticsVersion || row.VerifiedThroughHour < usageFactDayStart(through) {
			return fmt.Errorf("full-history published signature is stale user_id=%d", row.UserID)
		}
		var state UsageFactMemberState
		if err := db.First(&state, "user_id = ?", row.UserID).Error; err != nil {
			return err
		}
		if !state.Active || state.TrackedRevision != control.TrackedRevision || state.SourceEpoch != row.SourceEpoch ||
			state.ClassificationVersion != row.ClassificationVersion ||
			state.QuerySemanticsVersion != row.QuerySemanticsVersion || state.SourceFloorHour == nil ||
			*state.SourceFloorHour != row.SourceFloorHour || state.VerifiedThroughHour == nil ||
			*state.VerifiedThroughHour < row.VerifiedThroughHour || state.CoverageStatus != "ready" ||
			state.VerificationStatus != "complete" || (state.SourceHistoryStatus != "complete_hot" && state.SourceHistoryStatus != "no_history") {
			return fmt.Errorf("full-history verification checkpoint incomplete user_id=%d", row.UserID)
		}
		if state.SourceHistoryStatus == "complete_hot" {
			trailingIDs = append(trailingIDs, row.UserID)
		}
		if minFloor == 0 || row.SourceFloorHour < minFloor {
			minFloor = row.SourceFloorHour
		}
		ids = append(ids, row.UserID)
	}
	if minFloor != current.PublishedRangeStart || current.PublishedFingerprint != portalMemberFingerprintFromIDs(ids) {
		return errors.New("full-history published boundary or fingerprint mismatch")
	}
	trailingStart := usageFactDayStart(through)
	if trailingStart < through && len(trailingIDs) > 0 {
		if err := auditUsageFactTrailingHoursForEpoch(db, trailingStart, through, trailingIDs, epoch); err != nil {
			return fmt.Errorf("full-history published trailing-hour checkpoint failed: %w", err)
		}
	}
	after, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return err
	}
	if !usageMemberControlSnapshotsEqual(before, after) {
		return fmt.Errorf("%w: member manifest changed during full-history readiness", errUsageMemberControlIntegrity)
	}
	return nil
}

// publishUsageFactFullHistorySnapshot incrementally upgrades the service
// membership. Existing signed members keep serving while cold history runs;
// a new/rejoined revision is added only after its complete source-controlled
// history and current Tail have passed a local semantic audit. The global left
// edge expands to the earliest member floor only after every serving member is
// full-history ready, so a legacy 90-day member can never be exposed as zero
// before its own history has completed.
func (m *Monitor) publishUsageFactFullHistorySnapshot(ctx context.Context, now time.Time) (UsageFactSyncState, error) {
	if !m.usageFactsFullHistoryEnabled() {
		return UsageFactSyncState{}, errors.New("full-history usage facts are disabled")
	}
	through := m.usageFactFinalizedHour(now)
	if through <= 0 {
		return UsageFactSyncState{}, errors.New("full-history published watermark is invalid")
	}
	qctx, cancel := context.WithTimeout(ctx, usageFactFullPublishTimeout)
	defer cancel()
	controlBefore, err := m.loadUsageMemberControlSnapshot(qctx)
	if err != nil {
		return UsageFactSyncState{}, err
	}

	m.usageFactsSyncMu.Lock()
	locked := true
	refreshAfterUnlock := false
	defer func() {
		if locked {
			m.usageFactsSyncMu.Unlock()
		}
		if refreshAfterUnlock && m.usageFactsReadRequested() && ctx.Err() == nil {
			// Re-open a gate that was closed by an epoch/semantic reset without
			// requiring a process restart. This is SQLite-only and revalidates the
			// just-published durable signatures before exposing the new range.
			m.refreshUsageFactsReadiness(ctx, now)
		}
	}()
	db := m.usageFactsStore().WithContext(qctx)
	var current UsageFactSyncState
	if err := db.First(&current, 1).Error; err != nil {
		return UsageFactSyncState{}, err
	}
	var prior []UsageFactPublishedMember
	if err := db.Order("user_id").Find(&prior).Error; err != nil {
		return UsageFactSyncState{}, err
	}
	priorByID := make(map[int64]UsageFactPublishedMember, len(prior))
	for _, row := range prior {
		priorByID[row.UserID] = row
	}
	var states []UsageFactMemberState
	if err := db.Where("active = ?", true).Order("user_id").Find(&states).Error; err != nil {
		return UsageFactSyncState{}, err
	}
	stateByID := make(map[int64]UsageFactMemberState, len(states))
	for _, state := range states {
		stateByID[state.UserID] = state
	}
	var jobs []UsageFactJob
	if err := db.Where("kind IN ?", []string{usageFactHistoryKindDiscover, usageFactHistoryKindBackfill, usageFactHistoryKindTail, usageFactHistoryKindVerify}).Find(&jobs).Error; err != nil {
		return UsageFactSyncState{}, err
	}
	jobByID := make(map[string]UsageFactJob, len(jobs))
	for _, job := range jobs {
		jobByID[job.ID] = job
	}
	activeRepairs, err := loadUsageFactActiveRepairUsers(db)
	if err != nil {
		return UsageFactSyncState{}, err
	}

	signedCandidate := make([]UsageFactPublishedMember, 0, len(controlBefore.Controls))
	retainedCandidate := make([]UsageFactPublishedMember, 0, len(prior))
	readyByID := make(map[int64]bool, len(controlBefore.Controls))
	allTrackedReady := true
	priorHasLegacy := false
	for _, row := range prior {
		priorHasLegacy = priorHasLegacy || usageFactPublishedMemberLegacy(row)
	}
	for _, tracked := range controlBefore.Tracked {
		control, ok := controlBefore.Controls[tracked.UserID]
		if !ok || !control.Active {
			continue
		}
		state, hasState := stateByID[tracked.UserID]
		job, hasJob := jobByID[usageFactHistoryJobID(tracked.UserID, control.TrackedRevision)]
		ready := !activeRepairs[tracked.UserID] && hasState && hasJob && job.Status == usageFactHistoryJobComplete && job.ThroughTs >= through &&
			state.SourceFloorCheckedAt >= now.Add(-usageFactBoundaryRecheck).Unix() &&
			job.SourceEpoch == strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch) &&
			usageFactMemberFullHistoryReady(state, control.TrackedRevision, through, strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch))
		if ready {
			// The durable verify cursor is the publication signature. Its local
			// hash audit and checkpoint are serialized with every facts writer, so
			// publication is O(member) and survives large-history restarts without
			// rescanning years of rows under this 60-second lock.
			readyByID[tracked.UserID] = true
			signedCandidate = append(signedCandidate, UsageFactPublishedMember{
				UserID: tracked.UserID, TrackedRevision: control.TrackedRevision,
				SourceEpoch: state.SourceEpoch, ClassificationVersion: state.ClassificationVersion,
				QuerySemanticsVersion: state.QuerySemanticsVersion, SourceFloorHour: *state.SourceFloorHour,
				VerifiedThroughHour: *state.VerifiedThroughHour, PublishedAt: now.Unix(),
			})
		} else {
			allTrackedReady = false
		}
		// Retain only a row whose authority and source/semantic signature are
		// still current. A global source-epoch or query-semantics change therefore
		// fails closed instead of serving a day-by-day mixture of two meanings.
		if row, existed := priorByID[tracked.UserID]; existed &&
			!activeRepairs[tracked.UserID] &&
			usageFactPublishedMemberCompatible(row, control) &&
			usageFactPublishedMemberHistorySignatureCurrent(row, strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)) {
			row.PublishedAt = now.Unix()
			retainedCandidate = append(retainedCandidate, row)
		}
	}
	// Do not create a mixed legacy/full-history publication. During the first
	// upgrade the bounded legacy set remains authoritative until every active
	// member has a durable full-history signature, then one transaction switches
	// the complete set. Subsequent all-signed publications may add ready members
	// incrementally while another new member remains admin-only.
	candidate := signedCandidate
	if len(prior) == 0 && !allTrackedReady {
		// Clean install / classification re-sign has no complete serving
		// baseline to retain. Publishing whichever member happens to finish first
		// would silently undercount an organisation until its peers catch up.
		// First publication is therefore an all-active-member atomic cutover.
		candidate = nil
	} else if priorHasLegacy && !allTrackedReady {
		candidate = retainedCandidate[:0]
		for _, row := range retainedCandidate {
			if usageFactPublishedMemberLegacy(row) {
				candidate = append(candidate, row)
			}
		}
	} else if !allTrackedReady && !priorHasLegacy {
		byID := make(map[int64]UsageFactPublishedMember, len(signedCandidate)+len(retainedCandidate))
		for _, row := range retainedCandidate {
			byID[row.UserID] = row
		}
		for _, row := range signedCandidate {
			byID[row.UserID] = row
		}
		candidate = candidate[:0]
		for _, row := range byID {
			candidate = append(candidate, row)
		}
	}
	sort.Slice(candidate, func(i, j int) bool { return candidate[i].UserID < candidate[j].UserID })
	hasPublishedMetadata := current.PublishedAt > 0 || current.PublishedFingerprint != "" ||
		current.PublishedRangeStart > 0 || current.PublishedThrough > 0 || current.PublishedWindowDays > 0
	clearPublication := len(candidate) == 0 && (len(prior) > 0 || hasPublishedMetadata)
	if len(candidate) == 0 {
		// No complete revision is publishable yet. Keep metadata empty, but do
		// remove stale rows so a damaged caller cannot bypass the main-store
		// membership intersection.
		if !clearPublication {
			return current, nil
		}
	}

	publishedThrough := current.PublishedThrough
	publishedStart := current.PublishedRangeStart
	priorUsable := current.PublishedAt > 0 && publishedStart > 0 && publishedThrough > publishedStart && len(prior) > 0
	if clearPublication || !priorUsable {
		publishedThrough = 0
		publishedStart = 0
	}
	// Tail freshness is independent of cold history. If every candidate has
	// complete hour proofs after the old watermark, advance the right edge even
	// while legacy members are still being upgraded; never move the left edge.
	if len(candidate) > 0 {
		canAdvance := true
		from := publishedThrough
		ids := make([]int64, 0, len(candidate))
		targetDay := usageFactDayStart(through)
		if from <= 0 {
			from = usageFactDayStart(through)
		}
		if from < through {
			for _, row := range candidate {
				// Crossing CST midnight requires the durable per-member day
				// finalizer signature, not merely 24 candidate hour proofs. The
				// high-priority Tail may have staged/finalized the day before the
				// history job advances its checkpoint; keep the old global right
				// edge readable until every published row has caught up.
				if row.VerifiedThroughHour < targetDay {
					canAdvance = false
					break
				}
				state, ready := stateByID[row.UserID]
				// A just-discovered no-history member has one indexed boundary
				// proof for the complete target interval instead of one synthetic
				// empty hour row. Retained legacy members and members with any
				// history still require the normal per-hour Tail proof.
				if readyByID[row.UserID] && ready && state.SourceHistoryStatus == "no_history" {
					continue
				}
				ids = append(ids, row.UserID)
			}
			if canAdvance && len(ids) > 0 {
				inSQL, inArgs := usageIn("user_id", ids)
				args := append([]any{from, through}, inArgs...)
				args = append(args, "complete", strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch))
				var complete int64
				if err := db.Model(&UsageFactMemberHourState{}).
					Where("hour_ts >= ? AND hour_ts < ? AND "+inSQL+" AND status = ? AND content_hash <> '' AND source_epoch = ?", args...).Count(&complete).Error; err != nil {
					return UsageFactSyncState{}, err
				}
				canAdvance = complete == ((through-from)/usageFactHourSeconds)*int64(len(ids))
			}
		}
		if canAdvance && len(ids) > 0 {
			// Count proves completeness; the content/hash audit proves those rows
			// were not logically damaged between the Tail transaction and publish.
			if err := auditUsageFactTrailingHoursForEpoch(db, from, through, ids, strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)); err != nil {
				return UsageFactSyncState{}, fmt.Errorf("full-history tail audit failed: %w", err)
			}
		}
		if canAdvance {
			publishedThrough = through
		}
	}
	if len(candidate) > 0 && publishedThrough > 0 && !usageFactPublishedMemberLegacy(candidate[0]) {
		rangeState := UsageFactSyncState{PublishedRangeStart: publishedStart, PublishedThrough: publishedThrough}
		if err := normalizeUsageFactPublishedRange(&rangeState, candidate); err != nil {
			return UsageFactSyncState{}, err
		}
		publishedStart = rangeState.PublishedRangeStart
	}
	if !clearPublication && (publishedStart <= 0 || publishedThrough <= publishedStart) {
		// A first publication requires at least one complete natural range.
		if !priorUsable {
			return current, nil
		}
		publishedStart, publishedThrough = current.PublishedRangeStart, current.PublishedThrough
	}

	controlAtCommit, err := m.loadUsageMemberControlSnapshot(qctx)
	if err != nil {
		return UsageFactSyncState{}, err
	}
	if !usageMemberControlSnapshotsEqual(controlBefore, controlAtCommit) {
		return UsageFactSyncState{}, fmt.Errorf("%w: member manifest changed during full-history publish", errUsageMemberControlIntegrity)
	}
	fingerprint := ""
	windowDays := 0
	if !clearPublication {
		fingerprintIDs := make([]int64, 0, len(candidate))
		for _, row := range candidate {
			fingerprintIDs = append(fingerprintIDs, row.UserID)
		}
		fingerprint = portalMemberFingerprintFromIDs(fingerprintIDs)
		windowDays = int((publishedThrough - publishedStart + usageFactDaySeconds - 1) / usageFactDaySeconds)
		if windowDays < 1 {
			windowDays = 1
		}
	}
	rowsSame := usageFactPublishedRowsEqual(prior, candidate)
	metadataSame := current.PublishedFingerprint == fingerprint && current.PublishedRangeStart == publishedStart &&
		current.PublishedThrough == publishedThrough && current.PublishedWindowDays == windowDays
	if rowsSame && metadataSame {
		refreshAfterUnlock = true
		return current, nil
	}

	publishedAt := int64(0)
	if !clearPublication {
		publishedAt = time.Now().Unix()
	}
	for i := range candidate {
		candidate[i].PublishedAt = publishedAt
	}
	var published UsageFactSyncState
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&published, 1).Error; err != nil {
			return err
		}
		if err := tx.Where("1 = 1").Delete(&UsageFactPublishedMember{}).Error; err != nil {
			return err
		}
		if len(candidate) > 0 {
			if err := tx.CreateInBatches(candidate, usageFactProfileBatch).Error; err != nil {
				return err
			}
		}
		published.PublishedFingerprint = fingerprint
		published.PublishedRangeStart = publishedStart
		published.PublishedThrough = publishedThrough
		published.PublishedWindowDays = windowDays
		published.PublishedAt = publishedAt
		published.TrafficClassVersion = userTrafficClassificationVersion
		published.Generation++
		published.ServingGeneration++
		return tx.Save(&published).Error
	})
	if err != nil {
		return UsageFactSyncState{}, err
	}
	m.publishUsageFactGenerations(published.Generation, published.ServingGeneration)
	controlAfter, err := m.loadUsageMemberControlSnapshot(qctx)
	if err != nil {
		return UsageFactSyncState{}, err
	}
	if !usageMemberControlSnapshotsEqual(controlAtCommit, controlAfter) {
		return UsageFactSyncState{}, fmt.Errorf("%w: member manifest changed after full-history publish", errUsageMemberControlIntegrity)
	}
	refreshAfterUnlock = true
	return published, nil
}

func (m *Monitor) executeUsageFactHistoryBackfill(ctx context.Context, claim usageFactHistoryClaim, tuning *usageFactHistoryTuning, now time.Time) error {
	ids, revisions, err := usageFactHistoryClaimIDs(claim)
	if err != nil {
		_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
		return err
	}
	if claim.From <= 0 || claim.Through <= claim.From {
		err = errors.New("全历史日任务范围无效")
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
		if usageFactHistoryBulkTimeout(err) {
			circuitErr := m.recordUsageFactHistoryBulkFailure(context.Background(), err, now)
			_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, true)
			return errors.Join(err, circuitErr)
		}
		fallback := usageFactHistoryRangeShouldFallback(err)
		immediate := fallback || usageFactHistoryFailureIsSourceGlobal(err)
		if fallback {
			if tuning.chunkDays > 1 {
				tuning.chunkDays = previousUsageFactHistoryChunkDays(tuning.chunkDays)
				tuning.healthy = 0
			} else if len(claim.Jobs) > 1 {
				tuning.memberLimit = max(1, len(claim.Jobs)/2)
				tuning.healthy = 0
			} else {
				if errors.Is(err, errUsageFactHistoryControl) {
					immediate = false
				} else {
					// A single user-day that cannot satisfy the 5s/20k guard is
					// deterministically degraded to the same low-priority hourly path.
					// The cursor is durable, so a crash resumes at the last signed hour;
					// day control/finalization remains independent and atomic.
					if downgradeErr := m.downgradeUsageFactHistoryClaimToHourly(context.Background(), claim, err, now); downgradeErr != nil {
						return errors.Join(err, downgradeErr)
					}
					tuning.healthy = 0
					return nil
				}
			}
		}
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, immediate)
		return err
	}
	if err := m.commitUsageFactHistoryRange(ctx, result, claim.Jobs[0].ID, revisions); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}

	nowUnix := time.Now().Unix()
	becameReady := false
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, claimed := range claim.Jobs {
			var job UsageFactJob
			if err := tx.First(&job, "id = ? AND lease_owner = ? AND status = ?", claimed.ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; err != nil {
				return err
			}
			if job.TrackedRevision != revisions[*job.UserID] || job.NextHour != claim.From {
				return fmt.Errorf("%w: history cursor changed user_id=%d", errUsageMemberControlIntegrity, *job.UserID)
			}
			job.NextHour = claim.Through
			job.CompletedHours = max(job.NextHour-job.FromTs, 0) / usageFactHourSeconds
			job.Status = usageFactHistoryJobQueued
			job.Kind = usageFactHistoryKindBackfill
			completed := job.NextHour >= job.ThroughTs
			if completed {
				job.Kind = usageFactHistoryKindVerify
				job.Status = usageFactHistoryJobQueued
				job.CompletedAt = 0
			} else if job.NextHour >= usageFactDayStart(job.ThroughTs) {
				job.Kind = usageFactHistoryKindTail
			}
			job.LeaseOwner, job.LeaseUntil = "", 0
			job.NextRetryAt = 0
			job.LastError = ""
			job.HeartbeatAt = nowUnix
			job.UpdatedAt = nowUnix
			if err := tx.Save(&job).Error; err != nil {
				return err
			}
			memberUpdates := map[string]any{"coverage_through_hour": claim.Through, "coverage_status": "backfilling",
				"last_success_at": nowUnix, "last_failure_at": 0, "last_error": "", "updated_at": nowUnix}
			if completed {
				memberUpdates["tail_through_hour"] = job.ThroughTs
				memberUpdates["coverage_status"] = "verifying"
			}
			if err := tx.Model(&UsageFactMemberState{}).
				Where("user_id = ? AND active = ? AND tracked_revision = ?", *job.UserID, true, job.TrackedRevision).
				Updates(memberUpdates).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// Facts are already committed and controlled; leaving the lease to expire
		// causes an idempotent retry instead of fabricating cursor progress.
		return err
	}
	updateUsageFactHistoryTuning(tuning, result)
	if err := m.recordUsageFactHistoryBulkSuccess(context.Background(), result, now); err != nil {
		return err
	}
	_ = becameReady // retained for source-compatible migrations; verify owns readiness.
	return nil
}

func (m *Monitor) downgradeUsageFactHistoryClaimToHourly(ctx context.Context, claim usageFactHistoryClaim, cause error, now time.Time) error {
	if len(claim.Jobs) != 1 || claim.From <= 0 || claim.Through != claim.From+usageFactDaySeconds {
		return errors.New("全历史逐小时降级范围无效")
	}
	claimed := claim.Jobs[0]
	message := "daily range exceeded guard; hourly fallback"
	if cause != nil {
		message += ": " + truncateUsageFactError(cause.Error())
	}
	return m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job UsageFactJob
		if err := tx.First(&job, "id = ? AND lease_owner = ? AND status = ?", claimed.ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; err != nil {
			return err
		}
		job.Kind = usageFactHistoryKindTail
		job.NextHour = claim.From
		job.Status = usageFactHistoryJobQueued
		job.Attempts = 0
		job.NextRetryAt = 0
		job.LeaseOwner, job.LeaseUntil = "", 0
		job.LastError = message
		job.HeartbeatAt, job.UpdatedAt = now.Unix(), now.Unix()
		if err := tx.Save(&job).Error; err != nil {
			return err
		}
		if job.UserID == nil {
			return errors.New("全历史逐小时降级缺少成员")
		}
		return tx.Model(&UsageFactMemberState{}).
			Where("user_id = ? AND active = ? AND tracked_revision = ? AND source_epoch = ?",
				*job.UserID, true, job.TrackedRevision, job.SourceEpoch).
			Updates(map[string]any{"coverage_status": "hourly_backfill", "last_error": message, "updated_at": now.Unix()}).Error
	})
}

func (m *Monitor) executeUsageFactHistoryTail(ctx context.Context, claim usageFactHistoryClaim, now time.Time) error {
	ids, revisions, err := usageFactHistoryClaimIDs(claim)
	if err != nil {
		_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
		return err
	}
	if claim.From <= 0 {
		err = errors.New("全历史 Tail 游标无效")
		_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
		return err
	}
	for _, job := range claim.Jobs {
		if err := m.usageFactJobRevisionCurrent(ctx, job); err != nil {
			_ = m.releaseUsageFactHistoryClaim(ctx, claim, err, now, false)
			return err
		}
	}
	epoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
	localComplete, err := m.completedUsageFactHourUsersForEpoch(claim.From, ids, epoch)
	if err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	missing := make([]int64, 0, len(ids))
	syncResult := usageFactHourSyncResult{SucceededUserIDs: make([]int64, 0, len(ids))}
	for _, id := range ids {
		if localComplete[id] {
			syncResult.SucceededUserIDs = append(syncResult.SucceededUserIDs, id)
		} else {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		remoteResult, syncErr := m.syncUsageFactHourBatchedWithOptions(ctx, claim.From, missing, usageFactHourSyncOptions{
			updateLastFactSync:  false,
			recordFailure:       false,
			lowPrioritySource:   true,
			invalidateNoHistory: true,
		})
		mergeUsageFactHourSyncResult(&syncResult, remoteResult)
		err = syncErr
	}
	succeeded := make(map[int64]bool, len(syncResult.SucceededUserIDs))
	for _, id := range syncResult.SucceededUserIDs {
		succeeded[id] = true
	}
	handled := make(map[int64]bool, len(syncResult.InvalidatedNoHistoryUserIDs))
	for _, id := range syncResult.InvalidatedNoHistoryUserIDs {
		// The hour transaction already revoked this member's published signature
		// and converted its durable job back to discovery under the same lock.
		// Do not advance or release the stale Tail lease afterward.
		delete(succeeded, id)
		handled[id] = true
	}
	failed := make(map[int64]error, len(syncResult.FailedByUser))
	for id, failure := range syncResult.FailedByUser {
		failed[id] = failure
	}
	if err != nil && len(succeeded) == 0 {
		immediate := usageFactHistoryFailureIsSourceGlobal(err)
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, immediate)
		return err
	}
	successIDs := make([]int64, 0, len(succeeded))
	for _, id := range ids {
		if succeeded[id] {
			successIDs = append(successIDs, id)
		}
	}
	if finalizeErr := m.maybeFinalizeUsageFactHistoryDayFromHours(ctx, claim.From, successIDs, true); finalizeErr != nil {
		var memberErr *usageFactHistoryFinalizeError
		if errors.As(finalizeErr, &memberErr) {
			for id, oneErr := range memberErr.Failures {
				failed[id] = oneErr
				delete(succeeded, id)
			}
		} else {
			// A corrupt/pathological member must not hold up every other member at
			// the same midnight cursor. Retry individually only after the bounded
			// batch failed; healthy members keep their signed day and advance.
			if len(successIDs) == 1 || !usageFactHistoryRangeShouldFallback(finalizeErr) {
				for _, id := range successIDs {
					failed[id] = finalizeErr
					delete(succeeded, id)
				}
			} else {
				for _, id := range successIDs {
					if oneErr := m.maybeFinalizeUsageFactHistoryDayFromHours(ctx, claim.From, []int64{id}, true); oneErr != nil {
						failed[id] = oneErr
						delete(succeeded, id)
					}
				}
			}
		}
	}
	changed := make(map[int64]bool, len(syncResult.ChangedUserIDs))
	for _, id := range syncResult.ChangedUserIDs {
		changed[id] = true
	}
	next := claim.From + usageFactHourSeconds
	nowUnix := time.Now().Unix()
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, claimed := range claim.Jobs {
			if claimed.UserID == nil || !succeeded[*claimed.UserID] {
				continue
			}
			var job UsageFactJob
			if err := tx.First(&job, "id = ? AND lease_owner = ? AND status = ?", claimed.ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; err != nil {
				return err
			}
			if job.TrackedRevision != revisions[*job.UserID] || job.NextHour != claim.From {
				return fmt.Errorf("%w: tail cursor changed user_id=%d", errUsageMemberControlIntegrity, *job.UserID)
			}
			var memberState UsageFactMemberState
			if err := tx.First(&memberState, "user_id = ?", *job.UserID).Error; err != nil {
				return err
			}
			job.NextHour = min(next, job.ThroughTs)
			job.CompletedHours = max(job.NextHour-job.FromTs, 0) / usageFactHourSeconds
			job.Status = usageFactHistoryJobQueued
			job.LeaseOwner, job.LeaseUntil = "", 0
			job.NextRetryAt = 0
			job.LastError = ""
			job.HeartbeatAt = nowUnix
			job.UpdatedAt = nowUnix
			memberUpdates := map[string]any{"tail_through_hour": job.NextHour,
				"last_success_at": nowUnix, "last_failure_at": 0, "last_error": "", "updated_at": nowUnix}
			if usageFactDayStart(job.NextHour) == job.NextHour && job.NextHour > claim.From {
				memberUpdates["coverage_through_hour"] = job.NextHour
				verifiedThrough := job.NextHour
				if memberState.VerifiedThroughHour != nil && *memberState.VerifiedThroughHour > verifiedThrough {
					verifiedThrough = *memberState.VerifiedThroughHour
				}
				memberUpdates["verified_through_hour"] = verifiedThrough
				memberUpdates["verify_next_hour"] = verifiedThrough
				job.VerifyNextHour = verifiedThrough
				job.VerifiedHours = max(verifiedThrough-job.FromTs, 0) / usageFactHourSeconds
				if job.NextHour < usageFactDayStart(job.ThroughTs) {
					job.Kind = usageFactHistoryKindBackfill
				}
			}
			memberChanged := changed[*job.UserID]
			if memberState.SourceHistoryStatus == "no_history" && memberChanged {
				// New data appeared after a no-history boundary. Force indexed
				// rediscovery before this revision can be re-signed.
				job.Kind = usageFactHistoryKindDiscover
				memberUpdates["source_floor_checked_at"] = int64(0)
				memberUpdates["source_history_status"] = "discovering"
				memberUpdates["coverage_status"] = "discovering"
			}
			if job.NextHour >= job.ThroughTs {
				if memberState.SourceHistoryStatus == "no_history" && !memberChanged {
					job.Status = usageFactHistoryJobComplete
					job.CompletedAt = nowUnix
					memberUpdates["coverage_status"] = "ready"
					memberUpdates["verification_status"] = "complete"
					memberUpdates["verified_at"] = nowUnix
				} else {
					job.Kind = usageFactHistoryKindVerify
					job.Status = usageFactHistoryJobQueued
					job.CompletedAt = 0
					memberUpdates["coverage_status"] = "verifying"
					memberUpdates["verification_status"] = "running"
				}
			}
			if err := tx.Save(&job).Error; err != nil {
				return err
			}
			if err := tx.Model(&UsageFactMemberState{}).
				Where("user_id = ? AND active = ? AND tracked_revision = ?", *job.UserID, true, job.TrackedRevision).
				Updates(memberUpdates).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	var failureErr error
	for _, claimed := range claim.Jobs {
		if claimed.UserID == nil || succeeded[*claimed.UserID] || handled[*claimed.UserID] {
			continue
		}
		cause := failed[*claimed.UserID]
		if cause == nil {
			cause = err
		}
		if cause == nil {
			cause = errors.New("全历史逐小时成员未完成")
		}
		immediate := usageFactHistoryFailureIsSourceGlobal(cause)
		one := usageFactHistoryClaim{Jobs: []UsageFactJob{claimed}, LeaseOwner: claim.LeaseOwner, From: claim.From, Through: claim.Through}
		if releaseErr := m.releaseUsageFactHistoryClaim(context.Background(), one, cause, now, immediate); releaseErr != nil {
			failureErr = errors.Join(failureErr, cause, releaseErr)
		} else {
			failureErr = errors.Join(failureErr, cause)
		}
	}
	return failureErr
}

func previousUsageFactHistoryChunkDays(days int) int {
	switch {
	case days > 4:
		return 4
	case days > 2:
		return 2
	default:
		return 1
	}
}

func nextUsageFactHistoryChunkDays(days int) int {
	switch days {
	case 1:
		return 2
	case 2:
		return 4
	default:
		return 7
	}
}

func updateUsageFactHistoryTuning(tuning *usageFactHistoryTuning, result usageFactHistoryRange) {
	if tuning == nil {
		return
	}
	perQuery := time.Duration(0)
	if result.SourceQueries > 0 {
		perQuery = result.QueryDuration / time.Duration(result.SourceQueries)
	}
	if perQuery > 2*time.Second || result.Rows >= usageFactHistoryMaxRows*8/10 {
		tuning.chunkDays = previousUsageFactHistoryChunkDays(tuning.chunkDays)
		tuning.healthy = 0
		return
	}
	if perQuery <= 750*time.Millisecond && result.Rows < usageFactHistoryMaxRows/2 {
		tuning.healthy++
		if tuning.healthy >= usageFactHistoryHealthyToGrow {
			tuning.chunkDays = nextUsageFactHistoryChunkDays(tuning.chunkDays)
			tuning.memberLimit = usageFactHistoryMaxMembers
			tuning.healthy = 0
		}
		return
	}
	tuning.healthy = 0
}
