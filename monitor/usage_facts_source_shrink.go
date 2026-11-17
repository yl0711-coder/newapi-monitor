package monitor

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Source shrink protection.
//
// Re-reading the source (audits, repairs) normally replaces a member-day with
// what new-api reports now. Late logs only ever add rows, and a late refund is
// an added refund row, so a correct re-read never makes any per-day counter
// smaller than an intact, previously verified day. A smaller source means the
// logs were removed or rewritten upstream (e.g. new-api "clear history logs",
// which deletes every log before a timestamp). Monitor's figures are the
// record those logs were billed from, so the verified day is kept, the
// difference is recorded, and an alert is raised for a person to decide.
//
// The day is only protected when the local copy is provably the one that was
// verified (fact hash matches its proof) and was produced by the same
// classification, query semantics and source epoch. A local corruption or a
// deliberate rule change is still repaired from the source as before.

const usageFactSourceShrinkAlertKind = "infra_usage_source_shrink"

// UsageFactSourceShrinkFinding records a member-day whose source became smaller
// than the verified local copy. One row per member-day; repeated audits update
// the latest source figures and the occurrence count.
type UsageFactSourceShrinkFinding struct {
	UserID int64 `gorm:"primaryKey;autoIncrement:false;column:user_id"`
	DateTs int64 `gorm:"primaryKey;autoIncrement:false;column:date_ts"`

	KeptRequests         int64 `gorm:"column:kept_requests"`
	KeptRefundRecords    int64 `gorm:"column:kept_refund_records"`
	KeptPromptTokens     int64 `gorm:"column:kept_prompt_tokens"`
	KeptCompletionTokens int64 `gorm:"column:kept_completion_tokens"`
	KeptConsumeQuota     int64 `gorm:"column:kept_consume_quota"`
	KeptRefundQuota      int64 `gorm:"column:kept_refund_quota"`
	KeptSourceRows       int64 `gorm:"column:kept_source_rows"`

	SourceRequests         int64 `gorm:"column:source_requests"`
	SourceRefundRecords    int64 `gorm:"column:source_refund_records"`
	SourcePromptTokens     int64 `gorm:"column:source_prompt_tokens"`
	SourceCompletionTokens int64 `gorm:"column:source_completion_tokens"`
	SourceConsumeQuota     int64 `gorm:"column:source_consume_quota"`
	SourceRefundQuota      int64 `gorm:"column:source_refund_quota"`
	SourceRows             int64 `gorm:"column:source_rows"`

	JobID           string `gorm:"size:80;column:job_id"`
	FirstDetectedAt int64  `gorm:"column:first_detected_at;index"`
	LastDetectedAt  int64  `gorm:"column:last_detected_at"`
	Occurrences     int64  `gorm:"column:occurrences"`
}

func (UsageFactSourceShrinkFinding) TableName() string { return "usage_fact_source_shrink_findings" }

// usageFactSourceShrank reports whether a fresh source control is smaller than
// an intact, verified local member-day produced under the current rules.
func usageFactSourceShrank(prior UsageFactMemberDayState, priorRowsHash string, fresh usageFactHistoryControl, epoch string) bool {
	if prior.Status != "complete" || prior.FactContentHash == "" || priorRowsHash != prior.FactContentHash {
		return false
	}
	if prior.ClassificationVersion != userTrafficClassificationVersion ||
		prior.QuerySemanticsVersion != usageFactQuerySemanticsVersion ||
		strings.TrimSpace(prior.SourceEpoch) != strings.TrimSpace(epoch) {
		return false
	}
	return fresh.SourceRows < prior.SourceRows ||
		fresh.Requests < prior.Requests ||
		fresh.RefundRecords < prior.RefundRecords ||
		fresh.PromptTokens < prior.PromptTokens ||
		fresh.CompletionTokens < prior.CompletionTokens ||
		fresh.ConsumeQuota < prior.ConsumeQuota ||
		fresh.RefundQuota < prior.RefundQuota
}

// recordUsageFactSourceShrinkTx upserts the finding inside the facts
// transaction. It reports whether this member-day is newly found.
func recordUsageFactSourceShrinkTx(tx *gorm.DB, prior UsageFactMemberDayState, fresh usageFactHistoryControl, jobID string, now int64) (bool, error) {
	var existing UsageFactSourceShrinkFinding
	err := tx.Where("user_id = ? AND date_ts = ?", prior.UserID, prior.DateTs).Limit(1).Find(&existing).Error
	if err != nil {
		return false, err
	}
	first := existing.UserID == 0
	row := UsageFactSourceShrinkFinding{
		UserID: prior.UserID, DateTs: prior.DateTs,
		KeptRequests: prior.Requests, KeptRefundRecords: prior.RefundRecords,
		KeptPromptTokens: prior.PromptTokens, KeptCompletionTokens: prior.CompletionTokens,
		KeptConsumeQuota: prior.ConsumeQuota, KeptRefundQuota: prior.RefundQuota, KeptSourceRows: prior.SourceRows,
		SourceRequests: fresh.Requests, SourceRefundRecords: fresh.RefundRecords,
		SourcePromptTokens: fresh.PromptTokens, SourceCompletionTokens: fresh.CompletionTokens,
		SourceConsumeQuota: fresh.ConsumeQuota, SourceRefundQuota: fresh.RefundQuota, SourceRows: fresh.SourceRows,
		JobID: jobID, FirstDetectedAt: now, LastDetectedAt: now, Occurrences: 1,
	}
	if !first {
		row.FirstDetectedAt = existing.FirstDetectedAt
		row.Occurrences = existing.Occurrences + 1
	}
	if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&row).Error; err != nil {
		return false, err
	}
	return first, nil
}

func usageFactSourceShrinkFindingOf(prior UsageFactMemberDayState, fresh usageFactHistoryControl) UsageFactSourceShrinkFinding {
	return UsageFactSourceShrinkFinding{UserID: prior.UserID, DateTs: prior.DateTs,
		KeptRequests: prior.Requests, KeptConsumeQuota: prior.ConsumeQuota,
		SourceRequests: fresh.Requests, SourceConsumeQuota: fresh.ConsumeQuota}
}

// alertUsageFactSourceShrink is called after the facts transaction commits.
// Only newly found member-days alert; the alert cooldown dedupes bursts.
func (m *Monitor) alertUsageFactSourceShrink(found []UsageFactSourceShrinkFinding) {
	if len(found) == 0 {
		return
	}
	lines := make([]string, 0, len(found))
	for _, f := range found {
		day := time.Unix(f.DateTs, 0).In(cstLocation).Format("2006-01-02")
		slog.Warn("用量来源比已核验记录变少，已保留原数据", "user_id", f.UserID, "day", day,
			"kept_requests", f.KeptRequests, "source_requests", f.SourceRequests,
			"kept_quota", f.KeptConsumeQuota, "source_quota", f.SourceConsumeQuota)
		lines = append(lines, fmt.Sprintf("用户 #%d %s：已核验 %d 次请求 / 消费额度 %d；new-api 现在只有 %d 次 / %d",
			f.UserID, day, f.KeptRequests, f.KeptConsumeQuota, f.SourceRequests, f.SourceConsumeQuota))
	}
	if m.cfg.AlertsDisabled {
		return
	}
	c := m.loadAlertConfig()
	subject := fmt.Sprintf("new-api 使用日志比已核验记录变少（%d 个用户日），monitor 已保留原数据", len(found))
	body := subject + "\n\n" + strings.Join(lines, "\n") +
		"\n\n可能原因：new-api 后台清除了历史日志，或数据库中的日志被删改。monitor 的用户用量和账单仍按原核验数据显示，未跟随变少。请确认是否为有意操作。"
	m.fire(c, usageFactSourceShrinkAlertKind, "usage_facts", subject, body, time.Now().Unix())
}
