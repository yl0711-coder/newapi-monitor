//go:build unix

package monitor

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// A cheap projection of durable receipts, NOT evidence revalidation or worker
// liveness. No source files, boundary-event scans, migrations or writes here.
// Counts are cumulative for this authorization, not this process/invocation.
type financeGiftCommitProgress struct {
	Status            string `json:"status"`
	AuthorizedTargets int    `json:"authorized_targets"`
	CommittedTargets  int    `json:"committed_targets"`
	RowsUpdated       int    `json:"rows_updated"`
	LastCommittedAt   int64  `json:"last_committed_at"`
}

var errFinanceGiftLedgerMissing = errors.New("gift commit ledger missing")

func readFinanceGiftCommitProgress(parent context.Context, db *gorm.DB, id string, p financeGiftAuthorizationPayload) (financeGiftCommitProgress, error) {
	result := financeGiftCommitProgress{Status: "no_commits_recorded", AuthorizedTargets: len(p.Targets)}
	ctx, cancel := context.WithTimeout(parent, financeGiftAuthorizationDBTimeout)
	defer cancel()
	// One bounded indexed query is a consistent view. Missing ledger or timeout
	// must return unavailable, never a fabricated zero or an empty success.
	var commits []financeGiftHandoffCommit
	if err := db.WithContext(ctx).Where("job=?", id).Order("\"index\"").Limit(len(p.Targets) + 1).Find(&commits).Error; err != nil {
		// Distinguish an absent ledger without parsing driver error text or
		// treating a failed catalog read as absence. Keep the same time budget.
		var exists int
		if ctx.Err() == nil && db.WithContext(ctx).Raw("SELECT EXISTS(SELECT 1 FROM main.sqlite_master WHERE name=?)", (financeGiftHandoffCommit{}).TableName()).Scan(&exists).Error == nil && exists == 0 {
			return financeGiftCommitProgress{}, errFinanceGiftLedgerMissing
		}
		return financeGiftCommitProgress{}, err
	}
	if len(commits) > len(p.Targets) {
		return financeGiftCommitProgress{}, errors.New("commit count exceeds authorization")
	}
	for _, commit := range commits {
		entry, err := decodeFinanceGiftAuthorizedCommit(commit, p)
		if err != nil {
			return financeGiftCommitProgress{}, err
		}
		result.CommittedTargets++
		result.RowsUpdated += entry.RowsUpdated
		result.LastCommittedAt = max(result.LastCommittedAt, entry.FinishedAt)
	}
	if result.CommittedTargets == result.AuthorizedTargets {
		result.Status = "all_commits_recorded"
	} else if result.CommittedTargets > 0 {
		result.Status = "partial_commits_recorded"
	}
	return result, nil
}

func decodeFinanceGiftAuthorizedCommit(commit financeGiftHandoffCommit, p financeGiftAuthorizationPayload) (financeGiftScopeBatchEntry, error) {
	var entry financeGiftScopeBatchEntry
	if commit.Index < 0 || commit.Index >= len(p.Targets) || giftLocalDecodeJSON([]byte(commit.Entry), &entry) != nil {
		return entry, errors.New("invalid authorized commit")
	}
	target := p.Targets[commit.Index]
	if entry.financeGiftScopeTarget != target.financeGiftScopeTarget || entry.Status != "repaired" || entry.ErrorCode != "" ||
		entry.RowsChecked != target.EvidenceRows || entry.RowsUpdated <= 0 || entry.RowsUpdated > target.RowsToUpdate ||
		entry.FinishedAt < p.ApprovedAt || !giftSeriesDigestValid(entry.ContentHash) {
		return entry, errors.New("commit outside authorized scope")
	}
	return entry, nil
}

func (m *Monitor) serveFinanceGiftHandoffProgress(c *gin.Context) {
	// Local acceptance only, as with execution. Do not expose a production
	// progress API for a ledger that production does not yet maintain.
	if !m.cfg.LocalSnapshotOnly || m.cfg.ProdDSN != "" || m.cfg.NewAPIBaseURL != "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "本地交接进度未启用"})
		return
	}
	row, p, ok := m.giftAuthorizationReadRequest(c)
	if !ok {
		return
	}
	reply := gin.H{"task_id": row.ID, "authorization_status": giftAuthorizationState(row, p, m.cfg, time.Now().Unix()),
		"execution_enabled": false, "requires_revalidation": true, "execution_state": "not_observed", "progress": nil}
	if reply["authorization_status"] == "awaiting_execution" {
		reply["authorization_status"] = "valid"
	}
	// A changed source plan does not erase historical receipts. A different
	// receiver does: never look up the same ID in a different facts database.
	if p.ReceiverBinding != financeGiftReceiverBinding(m.cfg) {
		reply["error"] = "接收库配置已变化，未查询其他事实库"
		c.JSON(http.StatusConflict, reply)
		return
	}
	reader := m.financeFactsReadStore()
	if reader == nil || reader == m.usageFactsStore() {
		reply["error"] = "独立只读事实连接未就绪，提交进度暂不可用"
		c.JSON(http.StatusServiceUnavailable, reply)
		return
	}
	progress, err := readFinanceGiftCommitProgress(c.Request.Context(), reader, row.ID, p)
	if err != nil {
		reply["error_code"] = "progress_unavailable"
		if errors.Is(err, errFinanceGiftLedgerMissing) {
			reply["error_code"] = "ledger_missing"
		}
		reply["error"] = "提交记录暂不可核验；不能据此判断未执行或已完成"
		c.JSON(http.StatusServiceUnavailable, reply)
		return
	}
	reply["progress"] = progress
	c.JSON(http.StatusOK, reply)
}
