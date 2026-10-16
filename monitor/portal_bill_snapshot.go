package monitor

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
)

const portalBillSnapshotTTL = 15 * time.Second

// Server-validated filters only. No SQL fragments or client-provided member
// scopes are accepted. The API layer must bind these to a verified session.
type portalBillScope struct {
	IDs                                    []int64
	From, To, Member                       int64
	Model, Group, Token, Detail, RequestID string
}

// Used only by the opt-in task service. dst MUST be the task's bounded private
// staging writer, never a customer socket. On any error discard the whole file.
// All source queries share one bounded READ ONLY / REPEATABLE READ transaction.
// No writes, locking reads, schema changes, or session-global settings are used.
func (m *Monitor) buildPortalBillSnapshot(ctx context.Context, dst io.Writer, scope portalBillScope, expectedRows int64,
	authorize func(context.Context) error, progress func(int64) error,
) (int64, error) {
	if m.prodDB == nil || authorize == nil || progress == nil || dst == nil ||
		len(scope.IDs) == 0 || len(scope.IDs) > 10000 || scope.From < 0 || scope.To <= scope.From ||
		scope.To-scope.From > 31*86400 || expectedRows < 0 || expectedRows > 1000000 || scope.Member < 0 {
		return 0, errPortalBillSourceChanged
	}
	if _, ok := m.prodDB.Driver().(*mysql.MySQLDriver); !ok {
		return 0, errors.New("账单快照只支持已验证的 MySQL 来源")
	}
	ids := append([]int64(nil), scope.IDs...)
	memberFound := scope.Member == 0
	for _, id := range ids {
		if id <= 0 {
			return 0, errPortalBillSourceChanged
		}
		if id == scope.Member {
			memberFound = true
		}
	}
	if !memberFound {
		return 0, errPortalBillSourceChanged
	}
	if err := validatePortalLogFuzzyRange(scope.From, scope.To, scope.Token, scope.Detail, true); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, portalBillSnapshotTTL)
	defer cancel()
	if err := authorize(ctx); err != nil {
		return 0, err
	}
	if err := m.acquireInteractiveUsageExportGate(ctx); err != nil {
		return 0, err
	}
	defer m.releaseUsageExportGate()
	tx, err := m.prodDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }() // Safe after Commit or a canceled read-only transaction.
	where, args := m.logFilterWhere(ids, scope.From, scope.To, scope.Member, 2, scope.Model, scope.Group, scope.Token, scope.Detail, scope.RequestID)
	var count, upper, invalid int64
	var quota string
	// First consistent SELECT establishes the view used by every later page.
	// DECIMAL avoids float loss and aggregate int64 overflow. Invalid original
	// quotas cannot disappear through SUM's NULL omission.
	err = tx.QueryRowContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(8000) */ COUNT(*),COALESCE(MAX(id),0),COALESCE(SUM(CAST(quota AS DECIMAL(65,0))),0),COALESCE(SUM(CASE WHEN quota IS NULL OR quota<0 THEN 1 ELSE 0 END),0) FROM logs WHERE "+where, args...).Scan(&count, &upper, &quota, &invalid)
	if err != nil {
		return 0, err
	}
	// Check after accessing logs: its transaction metadata lock now prevents
	// an engine switch between validation and paging. No fallback to MyISAM.
	var engine string
	if err := tx.QueryRowContext(ctx, "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='logs'").Scan(&engine); err != nil {
		return 0, err
	}
	if engine != "InnoDB" {
		return 0, errors.New("日志来源不支持一致性快照")
	}
	if invalid != 0 || count != expectedRows {
		return 0, errPortalBillSourceChanged
	}
	page := func(ctx context.Context, cursor logPageCursor, limit int) ([]LogRow, error) {
		pageWhere, pageArgs := appendCompositeLogCursor(where, append([]any(nil), args...), cursor)
		rows, err := tx.QueryContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(8000) */ id,created_at,COALESCE(username,''),COALESCE(token_name,''),COALESCE(model_name,''),COALESCE(`group`,''),prompt_tokens,completion_tokens,quota,COALESCE(request_id,''),COALESCE(other,'') FROM logs WHERE "+pageWhere+" ORDER BY created_at DESC,type DESC,id DESC LIMIT "+strconv.Itoa(limit), pageArgs...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]LogRow, 0, limit)
		for rows.Next() {
			r := LogRow{Type: 2, sortTypeValid: true}
			var prompt, completion, charge sql.NullInt64
			var other string
			if err := rows.Scan(&r.ID, &r.CreatedAt, &r.Member, &r.TokenName, &r.ModelName, &r.Group, &prompt, &completion, &charge, &r.RequestID, &other); err != nil {
				return nil, err
			}
			r.PromptTokens, r.CompletionTokens, r.billingQuota = prompt.Int64, completion.Int64, charge.Int64
			r.SourceFacts = &portalLogSourceFacts{PromptPresent: prompt.Valid, CompletionPresent: completion.Valid, QuotaPresent: charge.Valid}
			r.billingFactsValid = prompt.Valid && completion.Valid && charge.Valid
			populatePortalLogEvidence(&r, other)
			out = append(out, r)
		}
		return out, rows.Err()
	}
	// Authorization lives in Monitor's separate store. This callback must not
	// acquire another source connection while the snapshot owns this one.
	n, err := portalBuildVerifiedBill(ctx, dst, portalBillBuildPlan{count, quota, upper, 1000}, page, authorize, progress)
	if err != nil {
		return n, err
	}
	// Explicitly release the source connection before returning the file to
	// the task manager. A failed rollback/commit must never publish the file.
	if err := tx.Rollback(); err != nil {
		return n, err
	}
	return n, ctx.Err()
}
