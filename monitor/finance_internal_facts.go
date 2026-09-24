package monitor

// 内部账号的财务归属事实与全站用量事实分开：只保存小范围的
// 小时×用户×渠道×分组整数汇总，不保存 prompt、request_id、token
// 或其他请求内容。配置变更后按新哈希从核算起点重建派生事实。

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

const (
	financeInternalFactStateID  = 1
	financeInternalFactMaxRows  = 50_000
	financeInternalFactBatchHrs = 24
)

type FinanceInternalAccountHourFact struct {
	HourTs        int64  `gorm:"primaryKey;autoIncrement:false;column:hour_ts"`
	UserID        int64  `gorm:"primaryKey;autoIncrement:false;column:user_id"`
	ChannelID     int    `gorm:"primaryKey;autoIncrement:false;column:channel_id"`
	Grp           string `gorm:"primaryKey;size:128;column:grp"`
	Requests      int64  `gorm:"column:requests"`
	Tokens        int64  `gorm:"column:tokens"`
	RefundRecords int64  `gorm:"column:refund_records"`
	ConsumeQuota  int64  `gorm:"column:consume_quota"`
	RefundQuota   int64  `gorm:"column:refund_quota"`
}

type FinanceInternalAccountFactState struct {
	ID                int64  `gorm:"primaryKey;autoIncrement:false;column:id"`
	ConfigHash        string `gorm:"size:64;column:config_hash"`
	SourceEpoch       string `gorm:"size:64;column:source_epoch"`
	StartHourTs       int64  `gorm:"column:start_hour_ts"`
	NextHourTs        int64  `gorm:"column:next_hour_ts"`
	LastCompletedHour int64  `gorm:"column:last_completed_hour"`
	Status            string `gorm:"size:16;index;column:status"`
	FailureStreak     int64  `gorm:"column:failure_streak"`
	LastError         string `gorm:"size:512;column:last_error"`
	LastAttemptAt     int64  `gorm:"column:last_attempt_at"`
	LastSuccessAt     int64  `gorm:"column:last_success_at"`
	UpdatedAt         int64  `gorm:"column:updated_at"`
}

type financeInternalFactStatus struct {
	Enabled           bool    `json:"enabled"`
	Status            string  `json:"status"`
	StartHour         int64   `json:"start_hour"`
	FinalizedThrough  int64   `json:"finalized_through"`
	NextHour          int64   `json:"next_hour"`
	LastCompletedHour int64   `json:"last_completed_hour"`
	ExpectedHours     int64   `json:"expected_hours"`
	CompletedHours    int64   `json:"completed_hours"`
	ProgressPercent   float64 `json:"progress_percent"`
	FailureStreak     int64   `json:"failure_streak"`
	LastError         string  `json:"last_error,omitempty"`
	UpdatedAt         int64   `json:"updated_at"`
}

type financeConfiguredInternalEvidence struct {
	Rows     []FinanceInternalAccountHourFact
	Complete bool
	// VerifiedScope bounds the rows actually loaded under the current account
	// configuration. An incomplete tail must not invalidate a closed subrange.
	VerifiedScope stabilityScope
	Accounts      int64
	AccountIDs    []int64
	Requests      int64
	Tokens        int64
	NetQuota      int64
	SyncState     financeInternalFactStatus
}

func (m *Monitor) loadFinanceConfiguredInternalEvidence(ctx context.Context, scope stabilityScope, policies map[string]bool) (financeConfiguredInternalEvidence, error) {
	return m.loadFinanceInternalEvidence(ctx, scope, policies, false)
}

// Finance may retain a verified prefix for independent monthly/daily reports.
// Other consumers keep the existing all-or-nothing loading contract.
func (m *Monitor) loadFinanceInternalEvidence(ctx context.Context, scope stabilityScope, policies map[string]bool, allowPartial bool) (financeConfiguredInternalEvidence, error) {
	var result financeConfiguredInternalEvidence
	accounts, err := m.loadFinanceInternalAccounts(ctx)
	if err != nil {
		return result, err
	}
	result.Accounts = int64(len(accounts))
	for _, account := range accounts {
		result.AccountIDs = append(result.AccountIDs, account.UserID)
	}
	if len(accounts) == 0 {
		result.Complete = true
		result.SyncState = financeInternalFactStatus{Status: "not_configured"}
		return result, nil
	}
	status, err := m.financeInternalFactSyncStatus(ctx)
	if err != nil {
		return result, err
	}
	result.SyncState = status
	result.Complete = (status.Status == "caught_up" || status.Status == "backfilling") && status.StartHour <= scope.FromTs && status.NextHour >= scope.ToTs
	// 配置哈希已变更时，旧表中的行属于上一份账号名单。在后台
	// 重置派生表前必须完全忽略，不能把旧账号误扣到新口径。
	if status.Status == "configuration_changed" {
		return result, nil
	}
	if !result.Complete && (!allowPartial || (status.Status != "caught_up" && status.Status != "backfilling")) {
		return result, nil
	}
	result.VerifiedScope = stabilityScope{FromTs: max(scope.FromTs, status.StartHour), ToTs: min(scope.ToTs, status.NextHour)}
	if result.VerifiedScope.ToTs <= result.VerifiedScope.FromTs {
		result.VerifiedScope = stabilityScope{}
		return result, nil
	}
	db := m.financeFactsReadStore()
	if db == nil {
		return result, errors.New("finance facts store unavailable")
	}
	var rows []FinanceInternalAccountHourFact
	if err := db.WithContext(ctx).Where("hour_ts>=? AND hour_ts<?", result.VerifiedScope.FromTs, result.VerifiedScope.ToTs).
		Order("hour_ts,user_id,channel_id,grp").Find(&rows).Error; err != nil {
		return result, err
	}
	result.Rows = make([]FinanceInternalAccountHourFact, 0, len(rows))
	for _, row := range rows {
		if !channelBusinessGroupIncluded(policies, row.Grp) {
			continue
		}
		result.Rows = append(result.Rows, row)
		if err := addEconomicsInt64(&result.Requests, row.Requests); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&result.Tokens, row.Tokens); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&result.NetQuota, row.ConsumeQuota); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&result.NetQuota, -row.RefundQuota); err != nil {
			return result, err
		}
	}
	return result, nil
}

func financeConfiguredInternalSubrange(source financeConfiguredInternalEvidence, scope stabilityScope) (financeConfiguredInternalEvidence, error) {
	result := financeConfiguredInternalEvidence{
		Complete:      financeEvidenceScopeComplete(source.Complete, source.VerifiedScope, scope),
		VerifiedScope: financeEvidenceScopeIntersection(source.VerifiedScope, scope),
		Accounts:      source.Accounts, AccountIDs: append([]int64(nil), source.AccountIDs...), SyncState: source.SyncState,
	}
	for _, row := range source.Rows {
		if row.HourTs < scope.FromTs || row.HourTs >= scope.ToTs {
			continue
		}
		result.Rows = append(result.Rows, row)
		if err := addEconomicsInt64(&result.Requests, row.Requests); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&result.Tokens, row.Tokens); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&result.NetQuota, row.ConsumeQuota); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&result.NetQuota, -row.RefundQuota); err != nil {
			return result, err
		}
	}
	return result, nil
}

func financeEvidenceScopeComplete(complete bool, verified, requested stabilityScope) bool {
	if requested.ToTs <= requested.FromTs {
		return false
	}
	if verified.ToTs > verified.FromTs {
		return verified.FromTs <= requested.FromTs && verified.ToTs >= requested.ToTs
	}
	return complete
}

func financeEvidenceScopeIntersection(verified, requested stabilityScope) stabilityScope {
	result := stabilityScope{FromTs: max(verified.FromTs, requested.FromTs), ToTs: min(verified.ToTs, requested.ToTs)}
	if result.ToTs <= result.FromTs {
		return stabilityScope{}
	}
	return result
}

func financeInternalAccountHourSQL(ids []int64) (string, []any) {
	inSQL, args := usageIn("user_id", ids)
	query := fmt.Sprintf(`
SELECT /*+ MAX_EXECUTION_TIME(12000) */ FLOOR(created_at/3600)*3600 hour_ts,
  user_id,COALESCE(channel_id,0),COALESCE(NULLIF(TRIM(`+"`group`"+`),''),''),
  CAST(COALESCE(SUM(type=2),0) AS SIGNED) requests,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN COALESCE(prompt_tokens,0)+COALESCE(completion_tokens,0) ELSE 0 END),0) AS SIGNED) tokens,
  CAST(COALESCE(SUM(type=6),0) AS SIGNED) refund_records,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN quota ELSE 0 END),0) AS SIGNED) consume_quota,
  CAST(COALESCE(SUM(CASE WHEN type=6 THEN quota ELSE 0 END),0) AS SIGNED) refund_quota
FROM logs
WHERE created_at>=? AND created_at<? AND type IN (2,6) AND %s
  AND NOT (%s)
GROUP BY FLOOR(created_at/3600),user_id,channel_id,COALESCE(NULLIF(TRIM(`+"`group`"+`),''),'')
LIMIT %d`, inSQL, channelTestLogPredicateSQL(), financeInternalFactMaxRows+1)
	return query, args
}

func fetchFinanceInternalAccountHourFacts(ctx context.Context, source financeSourceQuerier, from, to int64, ids []int64) ([]FinanceInternalAccountHourFact, error) {
	if source == nil || from < 0 || from%3600 != 0 || to <= from || to%3600 != 0 || len(ids) == 0 || to-from > financeInternalFactBatchHrs*3600 {
		return nil, errors.New("invalid internal-account finance fact request")
	}
	query, idArgs := financeInternalAccountHourSQL(ids)
	// SQL 先出现时间占位符，然后才是 user_id IN (...)。
	args := append([]any{from, to}, idArgs...)
	rows, err := source.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	facts := make([]FinanceInternalAccountHourFact, 0, 64)
	for rows.Next() {
		var row FinanceInternalAccountHourFact
		if err := rows.Scan(&row.HourTs, &row.UserID, &row.ChannelID, &row.Grp, &row.Requests, &row.Tokens, &row.RefundRecords, &row.ConsumeQuota, &row.RefundQuota); err != nil {
			return nil, err
		}
		if len(facts) >= financeInternalFactMaxRows {
			return nil, fmt.Errorf("内部账号事实超过安全上限 %d", financeInternalFactMaxRows)
		}
		if row.HourTs < from || row.HourTs >= to || row.HourTs%3600 != 0 || row.UserID <= 0 || row.ChannelID < 0 || row.Requests < 0 || row.Tokens < 0 || row.RefundRecords < 0 || row.ConsumeQuota < 0 || row.RefundQuota < 0 {
			return nil, errors.New("invalid internal-account finance fact row")
		}
		row.Grp = clip(strings.TrimSpace(row.Grp), 128)
		facts = append(facts, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(facts, func(i, j int) bool {
		if facts[i].HourTs != facts[j].HourTs {
			return facts[i].HourTs < facts[j].HourTs
		}
		if facts[i].UserID != facts[j].UserID {
			return facts[i].UserID < facts[j].UserID
		}
		if facts[i].ChannelID != facts[j].ChannelID {
			return facts[i].ChannelID < facts[j].ChannelID
		}
		return facts[i].Grp < facts[j].Grp
	})
	return facts, nil
}

func (m *Monitor) financeInternalFactState(ctx context.Context, accounts []FinanceInternalAccount, sourceEpoch string) (FinanceInternalAccountFactState, error) {
	db := m.usageFactsStore()
	if db == nil {
		return FinanceInternalAccountFactState{}, errors.New("finance facts store unavailable")
	}
	start, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		return FinanceInternalAccountFactState{}, err
	}
	hash := financeInternalAccountHash(accounts)
	var state FinanceInternalAccountFactState
	err = db.WithContext(ctx).First(&state, financeInternalFactStateID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		state = FinanceInternalAccountFactState{ID: financeInternalFactStateID, ConfigHash: hash, SourceEpoch: sourceEpoch, StartHourTs: start, NextHourTs: start, Status: "pending", UpdatedAt: time.Now().Unix()}
		return state, db.WithContext(ctx).Create(&state).Error
	}
	if err != nil {
		return state, err
	}
	if state.ConfigHash == hash && state.SourceEpoch == sourceEpoch && state.StartHourTs == start {
		return state, nil
	}
	now := time.Now().Unix()
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 保留仍在名单中的派生事实，重建时再按时间桶原子替换。
		// 这样配置变更或进程异常不会先清空全部历史。
		ids := make([]int64, 0, len(accounts))
		for _, account := range accounts {
			ids = append(ids, account.UserID)
		}
		query := tx.Model(&FinanceInternalAccountHourFact{})
		if len(ids) == 0 {
			if err := query.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&FinanceInternalAccountHourFact{}).Error; err != nil {
				return err
			}
		} else if err := query.Where("user_id NOT IN ?", ids).Delete(&FinanceInternalAccountHourFact{}).Error; err != nil {
			return err
		}
		state = FinanceInternalAccountFactState{ID: financeInternalFactStateID, ConfigHash: hash, SourceEpoch: sourceEpoch, StartHourTs: start, NextHourTs: start, Status: "pending", UpdatedAt: now}
		return tx.Save(&state).Error
	})
	return state, err
}

func (m *Monitor) syncNextFinanceInternalAccountBatch(ctx context.Context) (bool, error) {
	if !m.financeFactsSyncEnabled() {
		return false, nil
	}
	accounts, err := m.loadFinanceInternalAccounts(ctx)
	if err != nil {
		return false, err
	}
	epoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
	state, err := m.financeInternalFactState(ctx, accounts, epoch)
	if err != nil {
		return false, err
	}
	finalized := m.usageFactFinalizedHour(time.Now())
	if len(accounts) == 0 {
		if state.NextHourTs != finalized || state.Status != "caught_up" {
			state.NextHourTs, state.LastCompletedHour, state.Status, state.UpdatedAt = finalized, max(state.StartHourTs, finalized-3600), "caught_up", time.Now().Unix()
			return true, m.usageFactsStore().WithContext(ctx).Save(&state).Error
		}
		return false, nil
	}
	if state.NextHourTs >= finalized {
		return false, nil
	}
	to := min(state.NextHourTs+financeInternalFactBatchHrs*3600, finalized)
	ids := make([]int64, 0, len(accounts))
	for _, row := range accounts {
		ids = append(ids, row.UserID)
	}
	var facts []FinanceInternalAccountHourFact
	err = m.withUsageFactSourceQuery(ctx, func(queryCtx context.Context) error {
		tx, beginErr := m.prodDB.BeginTx(queryCtx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		if beginErr != nil {
			return beginErr
		}
		defer func() { _ = tx.Rollback() }()
		facts, beginErr = fetchFinanceInternalAccountHourFacts(queryCtx, tx, state.NextHourTs, to, ids)
		if beginErr != nil {
			return beginErr
		}
		return tx.Commit()
	})
	now := time.Now().Unix()
	if err != nil {
		state.Status, state.FailureStreak, state.LastAttemptAt, state.UpdatedAt = "error", state.FailureStreak+1, now, now
		state.LastError = clip(err.Error(), 512)
		_ = m.usageFactsStore().WithContext(ctx).Save(&state).Error
		return false, err
	}
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("hour_ts>=? AND hour_ts<?", state.NextHourTs, to).Delete(&FinanceInternalAccountHourFact{}).Error; err != nil {
			return err
		}
		if len(facts) > 0 {
			if err := tx.CreateInBatches(facts, 200).Error; err != nil {
				return err
			}
		}
		state.NextHourTs, state.LastCompletedHour = to, to-3600
		state.Status, state.FailureStreak, state.LastError = "running", 0, ""
		state.LastAttemptAt, state.LastSuccessAt, state.UpdatedAt = now, now, now
		if to >= finalized {
			state.Status = "caught_up"
		}
		return tx.Save(&state).Error
	})
	return err == nil, err
}

func (m *Monitor) financeInternalFactSyncStatus(ctx context.Context) (financeInternalFactStatus, error) {
	accounts, err := m.loadFinanceInternalAccounts(ctx)
	if err != nil {
		return financeInternalFactStatus{}, err
	}
	start, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		return financeInternalFactStatus{}, err
	}
	finalized := m.usageFactFinalizedHour(time.Now())
	result := financeInternalFactStatus{Enabled: len(accounts) > 0, Status: "not_configured", StartHour: start, FinalizedThrough: finalized}
	if finalized > start {
		result.ExpectedHours = (finalized - start) / 3600
	}
	var state FinanceInternalAccountFactState
	err = m.financeFactsReadStore().WithContext(ctx).First(&state, financeInternalFactStateID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	result.NextHour, result.LastCompletedHour, result.FailureStreak, result.LastError, result.UpdatedAt = state.NextHourTs, state.LastCompletedHour, state.FailureStreak, state.LastError, state.UpdatedAt
	if state.ConfigHash != financeInternalAccountHash(accounts) || state.SourceEpoch != strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch) || state.StartHourTs != start {
		result.Status = "configuration_changed"
		return result, nil
	}
	completed := min(max(state.NextHourTs, start), finalized)
	if completed > start {
		result.CompletedHours = (completed - start) / 3600
	}
	if result.ExpectedHours > 0 {
		result.ProgressPercent = float64(result.CompletedHours) * 100 / float64(result.ExpectedHours)
	}
	if len(accounts) == 0 {
		result.Status = "not_configured"
	} else if state.FailureStreak > 0 || state.Status == "error" {
		result.Status = "error"
	} else if state.NextHourTs >= finalized {
		result.Status = "caught_up"
	} else {
		result.Status = "backfilling"
	}
	return result, nil
}
