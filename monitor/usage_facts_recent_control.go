package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Recent source audit: control pre-check (raw-page mode).
//
// The rolling recent audit re-reads each member's last three closed days every
// cycle to catch late logs. In raw-page mode every member-day used to be
// downgraded straight to 24 hourly raw-page reads (two cursor passes per hour,
// about 38s per audited hour on production). 48 members x 72 hours did not fit
// in the 24-hour cycle (about 36 hours of work, 2026-11-17).
//
// Almost every re-read day is unchanged. The day is first compared through its
// independent source control (rows, requests, refunds, tokens, quotas) against
// the signed day proof. The control is read in hour-aligned slices so no single
// statement scans a heavy member's whole day (one member's day took 15.7s
// against the 5s cap; a six-hour slice of it up to 4.7s, so slices start at
// three hours and halve on a timeout). On a production baseline the sliced
// control reproduced the signed hash for all eight member-days checked
// (1 to 142,860 source rows, 2026-11-18). An equal
// control advances the audit without raw pages. A different control is recorded
// as an audit finding and the day goes down the unchanged hourly raw-page path,
// which re-reads, compares and repairs exactly as before.
//
// Scope and limits:
//   - only the recent audit uses the pre-check; the rolling full-history source
//     audit keeps re-reading raw pages, so a change that leaves every control
//     sum equal (new-api does not rewrite logs) is still caught there;
//   - any doubt (no signed proof, other epoch or semantics, source error, time
//     budget) falls back to the hourly raw-page path, never to "verified".

const (
	usageFactRecentControlSliceHours    = 3
	usageFactRecentControlBudget        = 90 * time.Second
	usageFactRecentControlFindingSource = "recent_audit_control"
)

// UsageFactAuditFinding records a member-day whose source changed after it was
// signed, as seen by a later audit. It is evidence for how late corrections
// arrive; the repair itself is done by the normal audit path. One row per
// member-day; repeated detections update the latest figures.
type UsageFactAuditFinding struct {
	UserID int64 `gorm:"primaryKey;autoIncrement:false;column:user_id"`
	DateTs int64 `gorm:"primaryKey;autoIncrement:false;column:date_ts"`

	SignedSourceRows   int64 `gorm:"column:signed_source_rows"`
	SignedConsumeQuota int64 `gorm:"column:signed_consume_quota"`
	SignedRefundQuota  int64 `gorm:"column:signed_refund_quota"`
	SourceRows         int64 `gorm:"column:source_rows"`
	SourceConsumeQuota int64 `gorm:"column:source_consume_quota"`
	SourceRefundQuota  int64 `gorm:"column:source_refund_quota"`

	// SignedAt is when the compared proof was last checked against the source;
	// FirstDetectedAt - (DateTs + one day) is how late the change was seen.
	SignedAt        int64  `gorm:"column:signed_at"`
	Source          string `gorm:"size:40;column:source"`
	JobID           string `gorm:"size:80;column:job_id"`
	FirstDetectedAt int64  `gorm:"column:first_detected_at;index"`
	LastDetectedAt  int64  `gorm:"column:last_detected_at"`
	Occurrences     int64  `gorm:"column:occurrences"`
}

func (UsageFactAuditFinding) TableName() string { return "usage_fact_audit_findings" }

var errUsageFactRecentControlBudget = errors.New("recent audit control pre-check exceeded its time budget")

// usageFactControlSliceFetcher returns the source control of one member over
// [from, through), a range inside one CST day.
type usageFactControlSliceFetcher func(ctx context.Context, userID, from, through int64) (usageFactHistoryControl, error)

// sumUsageFactDayControlInSlices folds hour-aligned slice controls into one
// day control. A slice that hits the statement cap is split in half down to a
// single hour; the result is identical because every row belongs to exactly
// one slice and the control is a plain sum.
func sumUsageFactDayControlInSlices(ctx context.Context, userID, dayTs int64, fetch usageFactControlSliceFetcher,
	deadline time.Time) (usageFactHistoryControl, error) {
	total := usageFactHistoryControl{UserID: userID, DateTs: dayTs}
	type span struct{ from, through int64 }
	step := int64(usageFactRecentControlSliceHours) * usageFactHourSeconds
	var pending []span
	for from := dayTs; from < dayTs+usageFactDaySeconds; from += step {
		pending = append(pending, span{from, min(from+step, dayTs+usageFactDaySeconds)})
	}
	for len(pending) > 0 {
		if !deadline.IsZero() && time.Now().After(deadline) {
			return total, errUsageFactRecentControlBudget
		}
		s := pending[0]
		pending = pending[1:]
		c, err := fetch(ctx, userID, s.from, s.through)
		if err != nil {
			hours := (s.through - s.from) / usageFactHourSeconds
			if usageFactHistoryBulkTimeout(err) && ctx.Err() == nil && hours > 1 {
				mid := s.from + hours/2*usageFactHourSeconds
				pending = append([]span{{s.from, mid}, {mid, s.through}}, pending...)
				continue
			}
			return total, err
		}
		if c.SourceRows != c.Requests+c.RefundRecords {
			return total, &usageFactHistoryMemberControlError{UserID: userID, DayTs: dayTs,
				Detail: fmt.Sprintf("source_rows=%d records=%d", c.SourceRows, c.Requests+c.RefundRecords)}
		}
		total.SourceRows += c.SourceRows
		total.Requests += c.Requests
		total.RefundRecords += c.RefundRecords
		total.PromptTokens += c.PromptTokens
		total.CompletionTokens += c.CompletionTokens
		total.ConsumeQuota += c.ConsumeQuota
		total.RefundQuota += c.RefundQuota
	}
	return total, nil
}

// usageFactDayControlMatchesProof reports whether a fresh day control equals
// the one the signed day proof was built from.
func usageFactDayControlMatchesProof(control usageFactHistoryControl, proof UsageFactMemberDayState, epoch string) bool {
	// proof.Rows counts local fact rows, not source rows; the source side of the
	// proof is its control hash plus the source counters below.
	return usageFactMemberDayHistoryReady(proof) && proof.SourceEpoch == epoch &&
		proof.SourceResultHash == usageFactHistoryControlHash(control) &&
		proof.SourceRows == control.SourceRows && proof.Requests == control.Requests &&
		proof.RefundRecords == control.RefundRecords && proof.PromptTokens == control.PromptTokens &&
		proof.CompletionTokens == control.CompletionTokens && proof.ConsumeQuota == control.ConsumeQuota &&
		proof.RefundQuota == control.RefundQuota
}

// fetchUsageFactControlSlice reads one member's source control over a range
// inside one CST day, on the low-priority history lane.
func (m *Monitor) fetchUsageFactControlSlice(ctx context.Context, userID, from, through int64) (usageFactHistoryControl, error) {
	control := usageFactHistoryControl{UserID: userID, DateTs: usageFactDayStart(from)}
	if userID <= 0 || from <= 0 || through <= from || usageFactDayStart(from) != usageFactDayStart(through-1) {
		return control, errors.New("近期审计控制数分段无效")
	}
	query := usageFactHistoryControlSQL(m.dayExpr(), "user_id = ?", m.channelTestSourcePredicateSQL())
	err := m.withUsageFactHistorySourceQuery(ctx, func(qctx context.Context) error {
		rows, err := m.prodDB.QueryContext(qctx, query, from, through, userID, 2)
		if err != nil {
			return fmt.Errorf("读取近期审计控制数失败: %w", err)
		}
		defer rows.Close()
		seen := 0
		for rows.Next() {
			var dayIndex, uid int64
			var c usageFactHistoryControl
			if err := rows.Scan(&dayIndex, &uid, &c.SourceRows, &c.Requests, &c.RefundRecords,
				&c.PromptTokens, &c.CompletionTokens, &c.ConsumeQuota, &c.RefundQuota); err != nil {
				return err
			}
			if seen++; seen > 1 || uid != userID || usageFactHistoryDayIndexToTs(dayIndex) != control.DateTs {
				return errors.New("近期审计控制数返回越界行")
			}
			c.UserID, c.DateTs = userID, control.DateTs
			control = c
		}
		return rows.Err()
	})
	return control, err
}

// verifyRecentUsageFactAuditByControl runs the control pre-check for one
// claimed recent-audit member-day. handled=false hands the claim, untouched,
// to the hourly raw-page path.
func (m *Monitor) verifyRecentUsageFactAuditByControl(ctx context.Context, claim usageFactHistoryClaim, now time.Time) (handled bool, err error) {
	return m.verifyRecentUsageFactAuditByControlWith(ctx, claim, now, m.fetchUsageFactControlSlice)
}

func (m *Monitor) verifyRecentUsageFactAuditByControlWith(ctx context.Context, claim usageFactHistoryClaim, now time.Time,
	fetch usageFactControlSliceFetcher) (handled bool, err error) {
	if len(claim.Jobs) != 1 || claim.Through != claim.From+usageFactDaySeconds || usageFactDayStart(claim.From) != claim.From {
		return false, nil
	}
	job := claim.Jobs[0]
	if job.Kind != usageFactHistoryKindRecentAudit || job.UserID == nil || *job.UserID <= 0 {
		return false, nil
	}
	userID := *job.UserID
	epoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
	if epoch == "" || job.SourceEpoch != epoch {
		return false, nil
	}
	if err := m.usageFactJobRevisionCurrent(ctx, job); err != nil {
		return true, errors.Join(err, m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false))
	}
	var proof UsageFactMemberDayState
	if err := m.usageFactsStore().WithContext(ctx).First(&proof, "user_id = ? AND date_ts = ?", userID, claim.From).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return true, errors.Join(err, m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, true))
	}
	if !usageFactMemberDayHistoryReady(proof) || proof.SourceEpoch != epoch {
		return false, nil
	}
	started := time.Now()
	control, err := sumUsageFactDayControlInSlices(ctx, userID, claim.From, fetch, started.Add(usageFactRecentControlBudget))
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, errUsageFactSourceBusy) || errors.Is(err, errSourceNotReady) {
			// No result either way: give the slot back without an attempt.
			return true, errors.Join(err, m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, true))
		}
		slog.Info("近期审计控制数预检未完成，转逐小时原始分页核对", "user_id", userID, "day", claim.From,
			"elapsed_ms", time.Since(started).Milliseconds(), "err", err)
		return false, nil
	}
	if !usageFactDayControlMatchesProof(control, proof, epoch) {
		if recordErr := m.recordUsageFactAuditFinding(proof, control, job.ID, now); recordErr != nil {
			slog.Warn("近期审计修正记录写入失败(不影响核对)", "user_id", userID, "day", claim.From, "err", recordErr)
		}
		slog.Info("近期审计发现来源变化，转逐小时原始分页核对", "user_id", userID, "day", claim.From,
			"signed_rows", proof.SourceRows, "source_rows", control.SourceRows,
			"signed_quota", proof.ConsumeQuota, "source_quota", control.ConsumeQuota)
		return false, nil
	}
	m.usageFactRecentControlVerified.Add(1)
	return true, m.advanceUsageFactAuditJob(ctx, job, claim.LeaseOwner, claim.Through, now)
}

func (m *Monitor) recordUsageFactAuditFinding(proof UsageFactMemberDayState, control usageFactHistoryControl, jobID string, now time.Time) error {
	finding := UsageFactAuditFinding{
		UserID: proof.UserID, DateTs: proof.DateTs,
		SignedSourceRows: proof.SourceRows, SignedConsumeQuota: proof.ConsumeQuota, SignedRefundQuota: proof.RefundQuota,
		SourceRows: control.SourceRows, SourceConsumeQuota: control.ConsumeQuota, SourceRefundQuota: control.RefundQuota,
		SignedAt: proof.SourceCheckedAt, Source: usageFactRecentControlFindingSource, JobID: jobID,
		FirstDetectedAt: now.Unix(), LastDetectedAt: now.Unix(), Occurrences: 1,
	}
	return m.usageFactsStore().Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "user_id"}, {Name: "date_ts"}},
		DoUpdates: clause.Assignments(map[string]any{
			"signed_source_rows": finding.SignedSourceRows, "signed_consume_quota": finding.SignedConsumeQuota,
			"signed_refund_quota": finding.SignedRefundQuota, "source_rows": finding.SourceRows,
			"source_consume_quota": finding.SourceConsumeQuota, "source_refund_quota": finding.SourceRefundQuota,
			"signed_at": finding.SignedAt, "job_id": finding.JobID, "last_detected_at": finding.LastDetectedAt,
			"occurrences": gorm.Expr("occurrences + 1"),
		}),
	}).Create(&finding).Error
}
