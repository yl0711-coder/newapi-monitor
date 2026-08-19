package monitor

// usage_facts_hour_sync.go 封装“单小时来源聚合 → 本地事实原子替换”。
// Tail、首次回填和历史复核共用同一条写入路径，避免三套逻辑对
// 空小时、晚到日志和旧版状态作出不同判断。

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

type usageFactHourSyncOptions struct {
	recordFailure       bool
	updateLastFactSync  bool
	lowPrioritySource   bool
	invalidateNoHistory bool
}

type usageFactHourSyncResult struct {
	Changed                     bool
	HadPriorFingerprint         bool
	SucceededUserIDs            []int64
	ChangedUserIDs              []int64
	FailedByUser                map[int64]error
	InvalidatedNoHistoryUserIDs []int64
}

// usageFactContentHash 对持久化字段做稳定排序和定长编码。它既能
// 区分真实零流量和未采集，也能识别行数未变但 quota/tokens 被事后修订。
func usageFactContentHash(rows []UsageHourFact) string {
	ordered := append([]UsageHourFact(nil), rows...)
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if a.HourTs != b.HourTs {
			return a.HourTs < b.HourTs
		}
		if a.UserID != b.UserID {
			return a.UserID < b.UserID
		}
		if a.ChannelID != b.ChannelID {
			return a.ChannelID < b.ChannelID
		}
		if a.Grp != b.Grp {
			return a.Grp < b.Grp
		}
		if a.ModelName != b.ModelName {
			return a.ModelName < b.ModelName
		}
		if a.TokenID != b.TokenID {
			return a.TokenID < b.TokenID
		}
		return a.TokenName < b.TokenName
	})
	h := sha256.New()
	var buf [8]byte
	writeInt := func(v int64) {
		binary.BigEndian.PutUint64(buf[:], uint64(v))
		_, _ = h.Write(buf[:])
	}
	writeString := func(v string) {
		writeInt(int64(len(v)))
		_, _ = h.Write([]byte(v))
	}
	for _, row := range ordered {
		writeInt(row.HourTs)
		writeInt(row.DayTs)
		writeInt(row.UserID)
		writeInt(row.ChannelID)
		writeString(row.Grp)
		writeString(row.ModelName)
		writeInt(row.TokenID)
		writeString(row.TokenName)
		writeInt(row.Requests)
		writeInt(row.RefundRecords)
		writeInt(row.PromptTokens)
		writeInt(row.CompletionTokens)
		writeInt(row.ConsumeQuota)
		writeInt(row.RefundQuota)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func loadUsageFactHour(db *gorm.DB, hourTs int64, ids []int64) ([]UsageHourFact, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	inSQL, args := usageIn("user_id", ids)
	queryArgs := append([]any{hourTs}, args...)
	var rows []UsageHourFact
	err := db.Where("hour_ts = ? AND "+inSQL, queryArgs...).
		Order("user_id, channel_id, grp, model_name, token_id").Find(&rows).Error
	return rows, err
}

func usageFactRowsByUser(rows []UsageHourFact) map[int64][]UsageHourFact {
	out := make(map[int64][]UsageHourFact)
	for _, row := range rows {
		out[row.UserID] = append(out[row.UserID], row)
	}
	return out
}

func usageFactMemberMetricsMatchState(metrics usageFactMetrics, state UsageFactMemberHourState) bool {
	return metrics.Rows == int64(state.Rows) && metrics.Requests == state.Requests && metrics.tokens() == state.Tokens
}

// completedUsageFactHourUsersForEpoch lets the durable full-history Tail
// consume a current-epoch hour already written by the high-priority recent
// Tail.  The local proof and rows are checked under the common writer mutex;
// only missing/invalid members need another source query.  This is the normal
// single-owner hand-off, while a concurrent in-flight owner is still protected
// by claimUsageFactHour's lease.
func (m *Monitor) completedUsageFactHourUsersForEpoch(hourTs int64, ids []int64, epoch string) (map[int64]bool, error) {
	ready := make(map[int64]bool, len(ids))
	if hourTs <= 0 || len(ids) == 0 || strings.TrimSpace(epoch) == "" {
		return ready, nil
	}
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	db := m.usageFactsStore()
	inSQL, inArgs := usageIn("user_id", ids)
	stateArgs := append([]any{hourTs}, inArgs...)
	stateArgs = append(stateArgs, "complete", epoch)
	var states []UsageFactMemberHourState
	if err := db.Where("hour_ts = ? AND "+inSQL+" AND status = ? AND source_epoch = ? AND content_hash <> ''", stateArgs...).
		Find(&states).Error; err != nil {
		return nil, err
	}
	rows, err := loadUsageFactHour(db, hourTs, ids)
	if err != nil {
		return nil, err
	}
	rowsByUser := usageFactRowsByUser(rows)
	for _, state := range states {
		memberRows := rowsByUser[state.UserID]
		if usageFactMemberMetricsMatchState(factsMetrics(memberRows), state) &&
			usageFactContentHash(memberRows) == state.ContentHash {
			ready[state.UserID] = true
		}
	}
	return ready, nil
}

// usageFactDayHourReadiness 一次读取整批小时事实，但按成员独立给出
// 24 小时完整性结论。日终签收可以在触发任何来源控制查询前排除
// 少证明/坏指纹的成员，避免一个本地坏行将同批健康成员拆成 N 次查询。
// requiredEpoch 为空时仅校验事实/指纹，非空时还要求当前来源签名。
func usageFactDayHourReadiness(db *gorm.DB, dayTs int64, ids []int64, requiredEpoch string) (map[int64]bool, error) {
	ready := make(map[int64]bool, len(ids))
	if dayTs <= 0 || len(ids) == 0 {
		return ready, nil
	}
	inSQL, inArgs := usageIn("user_id", ids)
	stateArgs := append([]any{dayTs, dayTs + usageFactDaySeconds}, inArgs...)
	var memberStates []UsageFactMemberHourState
	if err := db.Where("hour_ts >= ? AND hour_ts < ? AND "+inSQL+" AND status = ? AND content_hash <> ''",
		append(stateArgs, "complete")...).Order("hour_ts, user_id").Find(&memberStates).Error; err != nil {
		return nil, err
	}
	queryArgs := append([]any{dayTs, dayTs + usageFactDaySeconds}, inArgs...)
	var rows []UsageHourFact
	if err := db.Where("hour_ts >= ? AND hour_ts < ? AND "+inSQL, queryArgs...).
		Order("hour_ts, user_id, channel_id, grp, model_name, token_id").Find(&rows).Error; err != nil {
		return nil, err
	}
	type memberHourKey struct{ userID, hourTs int64 }
	rowsByMemberHour := make(map[memberHourKey][]UsageHourFact, len(memberStates))
	for _, row := range rows {
		key := memberHourKey{userID: row.UserID, hourTs: row.HourTs}
		rowsByMemberHour[key] = append(rowsByMemberHour[key], row)
	}
	statesByMemberHour := make(map[memberHourKey]UsageFactMemberHourState, len(memberStates))
	for _, state := range memberStates {
		statesByMemberHour[memberHourKey{userID: state.UserID, hourTs: state.HourTs}] = state
	}
	requiredEpoch = strings.TrimSpace(requiredEpoch)
	for _, id := range ids {
		memberReady := true
		for hour := dayTs; hour < dayTs+usageFactDaySeconds; hour += usageFactHourSeconds {
			key := memberHourKey{userID: id, hourTs: hour}
			state, ok := statesByMemberHour[key]
			if !ok || (requiredEpoch != "" && state.SourceEpoch != requiredEpoch) {
				memberReady = false
				break
			}
			hourRows := rowsByMemberHour[key]
			if !usageFactMemberMetricsMatchState(factsMetrics(hourRows), state) || usageFactContentHash(hourRows) != state.ContentHash {
				memberReady = false
				break
			}
		}
		ready[id] = memberReady
	}
	return ready, nil
}

// verifyUsageFactDayHours 在重建日事实前按“成员 × 24 小时”同时核对完成证明和
// 实际明细。每个成员都有独立的空小时指纹，新增用户可以单独补历史，不需要
// 使已有成员的整段窗口失效。
func verifyUsageFactDayHours(db *gorm.DB, dayTs int64, ids []int64) (bool, error) {
	ready, err := usageFactDayHourReadiness(db, dayTs, ids, "")
	if err != nil {
		return false, err
	}
	if dayTs <= 0 || len(ids) == 0 {
		return false, nil
	}
	for _, id := range ids {
		if !ready[id] {
			return false, nil
		}
	}
	return true, nil
}

func usageFactMetricsMatchState(metrics usageFactMetrics, state UsageHourIngestState) bool {
	return metrics.Rows == int64(state.Rows) && metrics.Requests == state.Requests && metrics.tokens() == state.Tokens
}

// verifyCompletedUsageFactHour 只读 Monitor SQLite，用来防止“状态说已完成，
// 事实行却不完整”被回填游标跳过。旧版状态没有指纹时，先依据已
// 保存的行数/请求/Tokens 回填指纹；之后再由低频历史复核与来源对照。
func (m *Monitor) verifyCompletedUsageFactHour(hourTs int64, ids []int64, state UsageHourIngestState) (bool, error) {
	if state.Status != "complete" {
		return false, nil
	}
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	rows, err := loadUsageFactHour(m.usageFactsStore(), hourTs, ids)
	if err != nil {
		return false, err
	}
	metrics := factsMetrics(rows)
	if !usageFactMetricsMatchState(metrics, state) {
		return false, nil
	}
	hash := usageFactContentHash(rows)
	if state.ContentHash != "" {
		return state.ContentHash == hash, nil
	}
	// 这是旧版台账迁移，本地事实未改变，不递增 Generation。
	result := m.usageFactsStore().Model(&UsageHourIngestState{}).
		Where("hour_ts = ? AND status = ? AND content_hash = ''", hourTs, "complete").
		Updates(map[string]any{"content_hash": hash, "updated_at": time.Now().Unix()})
	return result.Error == nil, result.Error
}

type usageFactHourClaim struct {
	leaseToken string
	previous   map[int64]UsageFactMemberHourState
	localRows  []UsageHourFact
}

func (m *Monitor) nextUsageFactLeaseToken(hourTs int64) string {
	return fmt.Sprintf("%d-%d-%d", time.Now().UnixNano(), hourTs, m.usageFactsLeaseSeq.Add(1))
}

// claimUsageFactHour 只在本地 SQLite 内领取任务并立即释放进程锁。真正的远程
// MySQL 聚合发生在锁外；慢查询不会阻塞状态页、资料快照或其他本地维护。
func (m *Monitor) claimUsageFactHour(hourTs int64, ids []int64) (usageFactHourClaim, error) {
	claim := usageFactHourClaim{
		leaseToken: m.nextUsageFactLeaseToken(hourTs),
		previous:   make(map[int64]UsageFactMemberHourState, len(ids)),
	}
	now := time.Now()
	leaseUntil := now.Add(m.usageFactQueryTimeout() + 30*time.Second).Unix()
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	err := m.usageFactsStore().Transaction(func(tx *gorm.DB) error {
		rows, err := loadUsageFactHour(tx, hourTs, ids)
		if err != nil {
			return err
		}
		claim.localRows = rows
		for _, id := range ids {
			var state UsageFactMemberHourState
			err := tx.First(&state, "user_id = ? AND hour_ts = ?", id, hourTs).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if err == nil {
				claim.previous[id] = state
				if state.Status == "running" && state.LeaseUntil > now.Unix() {
					return errUsageFactLeaseBusy
				}
			} else {
				state = UsageFactMemberHourState{UserID: id, HourTs: hourTs}
			}
			state.Status = "running"
			state.LeaseToken = claim.leaseToken
			state.LeaseUntil = leaseUntil
			state.UpdatedAt = now.Unix()
			if err := tx.Save(&state).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return claim, err
}

func (m *Monitor) failUsageFactHourClaim(hourTs int64, ids []int64, claim usageFactHourClaim, cause error, recordFailure bool) error {
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	now := time.Now().Unix()
	return m.usageFactsStore().Transaction(func(tx *gorm.DB) error {
		for _, id := range ids {
			var current UsageFactMemberHourState
			if err := tx.First(&current, "user_id = ? AND hour_ts = ?", id, hourTs).Error; err != nil {
				return err
			}
			if current.LeaseToken != claim.leaseToken || current.Status != "running" {
				continue
			}
			previous, found := claim.previous[id]
			if errors.Is(cause, errUsageFactSourceBusy) || errors.Is(cause, context.Canceled) || errors.Is(cause, errUsageFactLeaseBusy) {
				if found && previous.Status != "running" {
					previous.LeaseToken, previous.LeaseUntil = "", 0
					if err := tx.Save(&previous).Error; err != nil {
						return err
					}
				} else if err := tx.Delete(&current).Error; err != nil {
					return err
				}
				continue
			}
			if found && previous.Status == "complete" && previous.ContentHash != "" {
				current = previous
			} else {
				current = UsageFactMemberHourState{UserID: id, HourTs: hourTs, Status: "failed"}
			}
			current.Attempts++
			current.UpdatedAt = now
			current.LastError = truncateUsageFactError(cause.Error())
			current.LeaseToken, current.LeaseUntil = "", 0
			if err := tx.Save(&current).Error; err != nil {
				return err
			}
			if err := tx.Model(&UsageFactMemberState{}).Where("user_id = ?", id).Updates(map[string]any{
				"last_failure_at": now, "last_error": current.LastError, "updated_at": now,
			}).Error; err != nil {
				return err
			}
		}
		if recordFailure && !errors.Is(cause, errUsageFactSourceBusy) && !errors.Is(cause, context.Canceled) {
			return tx.Model(&UsageFactSyncState{}).Where("id = 1").Update("last_fact_failure_at", now).Error
		}
		return nil
	})
}

func (m *Monitor) saveAggregateUsageHourState(tx *gorm.DB, hourTs int64, fallbackIDs []int64, nowUnix int64) error {
	var activeIDs []int64
	if err := tx.Model(&UsageFactMemberState{}).Where("active = ?", true).Order("user_id").Pluck("user_id", &activeIDs).Error; err != nil {
		return err
	}
	if len(activeIDs) == 0 {
		activeIDs = append(activeIDs, fallbackIDs...)
	}
	var complete int64
	inSQL, inArgs := usageIn("user_id", activeIDs)
	countArgs := append([]any{hourTs}, inArgs...)
	countArgs = append(countArgs, "complete")
	if err := tx.Model(&UsageFactMemberHourState{}).
		Where("hour_ts = ? AND "+inSQL+" AND status = ? AND content_hash <> ''", countArgs...).Count(&complete).Error; err != nil {
		return err
	}
	state := UsageHourIngestState{HourTs: hourTs, Status: "partial", UpdatedAt: nowUnix}
	var previous UsageHourIngestState
	if err := tx.First(&previous, "hour_ts = ?", hourTs).Error; err == nil {
		state.Attempts = previous.Attempts
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	if complete == int64(len(activeIDs)) {
		rows, err := loadUsageFactHour(tx, hourTs, activeIDs)
		if err != nil {
			return err
		}
		metrics := factsMetrics(rows)
		state.Status = "complete"
		state.Rows = int(metrics.Rows)
		state.Requests = metrics.Requests
		state.Tokens = metrics.tokens()
		state.ContentHash = usageFactContentHash(rows)
		state.CompletedAt = nowUnix
		state.LastError = ""
	}
	state.Attempts++
	return tx.Save(&state).Error
}

func (m *Monitor) invalidateNoHistoryAfterHourTx(tx *gorm.DB, changedIDs []int64, global *UsageFactSyncState, nowUnix int64) ([]int64, bool, error) {
	if !m.usageFactsFullHistoryEnabled() || len(changedIDs) == 0 || global == nil {
		return nil, false, nil
	}
	var states []UsageFactMemberState
	if err := tx.Where("user_id IN ? AND active = ? AND source_history_status = ?", changedIDs, true, "no_history").Find(&states).Error; err != nil {
		return nil, false, err
	}
	if len(states) == 0 {
		return nil, false, nil
	}
	invalidated := make([]int64, 0, len(states))
	for _, state := range states {
		jobID := usageFactHistoryJobID(state.UserID, state.TrackedRevision)
		jobUpdates := map[string]any{
			"kind": usageFactHistoryKindDiscover, "status": usageFactHistoryJobQueued,
			"attempts": 0, "next_retry_at": 0, "lease_owner": "", "lease_until": 0,
			"completed_at": 0, "last_error": "new source activity invalidated no-history boundary",
			"updated_at": nowUnix,
		}
		result := tx.Model(&UsageFactJob{}).Where("id = ? AND tracked_revision = ?", jobID, state.TrackedRevision).Updates(jobUpdates)
		if result.Error != nil {
			return nil, false, result.Error
		}
		if result.RowsAffected != 1 {
			return nil, false, fmt.Errorf("%w: no-history job missing user_id=%d", errUsageMemberControlIntegrity, state.UserID)
		}
		if err := tx.Model(&UsageFactMemberState{}).
			Where("user_id = ? AND active = ? AND tracked_revision = ?", state.UserID, true, state.TrackedRevision).
			Updates(map[string]any{
				"source_floor_checked_at": int64(0), "source_history_status": "discovering",
				"coverage_status": "discovering", "verification_status": "pending", "verified_at": int64(0),
				"last_error": "new source activity invalidated no-history boundary", "updated_at": nowUnix,
			}).Error; err != nil {
			return nil, false, err
		}
		invalidated = append(invalidated, state.UserID)
	}
	var publishedBefore int64
	if err := tx.Model(&UsageFactPublishedMember{}).Where("user_id IN ?", invalidated).Count(&publishedBefore).Error; err != nil {
		return nil, false, err
	}
	if publishedBefore == 0 {
		return invalidated, false, nil
	}
	if err := tx.Where("user_id IN ?", invalidated).Delete(&UsageFactPublishedMember{}).Error; err != nil {
		return nil, false, err
	}
	var kept []UsageFactPublishedMember
	if err := tx.Order("user_id").Find(&kept).Error; err != nil {
		return nil, false, err
	}
	if len(kept) == 0 {
		global.PublishedFingerprint = ""
		global.PublishedRangeStart = 0
		global.PublishedThrough = 0
		global.PublishedWindowDays = 0
		global.PublishedAt = 0
	} else {
		ids := make([]int64, 0, len(kept))
		for _, row := range kept {
			ids = append(ids, row.UserID)
		}
		global.PublishedFingerprint = portalMemberFingerprintFromIDs(ids)
		if err := normalizeUsageFactPublishedRange(global, kept); err != nil {
			return nil, false, err
		}
	}
	global.ServingGeneration++
	return invalidated, true, nil
}

// syncUsageFactHourWithOptions 是唯一会替换单小时事实的函数。来源查询
// 成功且本地事务内二次校验一致后，才把状态标记为 complete。
func (m *Monitor) syncUsageFactHourWithOptions(ctx context.Context, hourTs int64, ids []int64, options usageFactHourSyncOptions) (usageFactHourSyncResult, error) {
	var result usageFactHourSyncResult
	if hourTs <= 0 || len(ids) == 0 {
		return result, nil
	}
	claim, err := m.claimUsageFactHour(hourTs, ids)
	if err != nil {
		return result, err
	}
	for _, previous := range claim.previous {
		if previous.Status == "complete" && previous.ContentHash != "" {
			result.HadPriorFingerprint = true
			break
		}
	}
	sourceRows, err := m.fetchUsageFactHourWithPriority(ctx, hourTs, ids, options.lowPrioritySource)
	if err != nil {
		if cleanupErr := m.failUsageFactHourClaim(hourTs, ids, claim, err, options.recordFailure); cleanupErr != nil {
			return result, errors.Join(err, fmt.Errorf("清理用量事实小时租约失败: %w", cleanupErr))
		}
		return result, err
	}
	nowUnix := time.Now().Unix()
	sourceMetrics := factsMetrics(sourceRows)
	sourceHash := usageFactContentHash(sourceRows)
	localHash := usageFactContentHash(claim.localRows)
	changed := localHash != sourceHash || !usageFactMetricsEqual(factsMetrics(claim.localRows), sourceMetrics)
	sourceByUser := usageFactRowsByUser(sourceRows)
	localByUser := usageFactRowsByUser(claim.localRows)
	changedIDs := make([]int64, 0, len(ids))
	for _, id := range ids {
		localRows, currentRows := localByUser[id], sourceByUser[id]
		if usageFactContentHash(localRows) != usageFactContentHash(currentRows) ||
			!usageFactMetricsEqual(factsMetrics(localRows), factsMetrics(currentRows)) {
			changedIDs = append(changedIDs, id)
		}
	}

	var generation int64
	var servingGeneration int64
	var servingChanged bool
	var publicationBoundsChanged bool
	var publishedState UsageFactSyncState
	m.usageFactsSyncMu.Lock()
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, id := range ids {
			var leased UsageFactMemberHourState
			if err := tx.First(&leased, "user_id = ? AND hour_ts = ?", id, hourTs).Error; err != nil {
				return err
			}
			if leased.Status != "running" || leased.LeaseToken != claim.leaseToken || leased.LeaseUntil < nowUnix {
				return errUsageFactLeaseBusy
			}
		}
		if changed {
			inSQL, inArgs := usageIn("user_id", ids)
			deleteArgs := append([]any{hourTs}, inArgs...)
			if err := tx.Where("hour_ts = ? AND "+inSQL, deleteArgs...).Delete(&UsageHourFact{}).Error; err != nil {
				return err
			}
			if len(sourceRows) > 0 {
				if err := tx.CreateInBatches(sourceRows, 500).Error; err != nil {
					return err
				}
			}
		}
		verifiedRows, err := loadUsageFactHour(tx, hourTs, ids)
		if err != nil {
			return err
		}
		verifiedMetrics := factsMetrics(verifiedRows)
		if usageFactContentHash(verifiedRows) != sourceHash || verifiedMetrics.Rows != sourceMetrics.Rows || !usageFactMetricsEqual(verifiedMetrics, sourceMetrics) {
			return errors.New("本地小时事实写入后校验不一致")
		}
		for _, id := range ids {
			memberRows := sourceByUser[id]
			memberMetrics := factsMetrics(memberRows)
			state := UsageFactMemberHourState{
				UserID: id, HourTs: hourTs, Status: "complete",
				Rows: int(memberMetrics.Rows), Requests: memberMetrics.Requests, Tokens: memberMetrics.tokens(),
				ContentHash: usageFactContentHash(memberRows), Attempts: 1,
				UpdatedAt: nowUnix, CompletedAt: nowUnix,
			}
			if m.usageFactsFullHistoryEnabled() {
				state.SourceEpoch = strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
			}
			if previous, found := claim.previous[id]; found {
				state.Attempts = previous.Attempts + 1
			}
			if err := tx.Save(&state).Error; err != nil {
				return err
			}
			if err := tx.Model(&UsageFactMemberState{}).Where("user_id = ?", id).Updates(map[string]any{
				"last_sync_at": nowUnix, "last_failure_at": int64(0), "last_error": "", "updated_at": nowUnix,
			}).Error; err != nil {
				return err
			}
		}
		if !m.usageFactsFullHistoryEnabled() {
			if err := m.rebuildUsageDailyFact(tx, usageFactDayStart(hourTs), ids); err != nil {
				return err
			}
		}
		if err := m.saveAggregateUsageHourState(tx, hourTs, ids, nowUnix); err != nil {
			return err
		}
		var global UsageFactSyncState
		if err := tx.First(&global, 1).Error; err != nil {
			return err
		}
		if changed {
			global.Generation++
			if options.invalidateNoHistory {
				invalidated, revoked, invalidateErr := m.invalidateNoHistoryAfterHourTx(tx, changedIDs, &global, nowUnix)
				if invalidateErr != nil {
					return invalidateErr
				}
				result.InvalidatedNoHistoryUserIDs = append(result.InvalidatedNoHistoryUserIDs, invalidated...)
				if revoked {
					servingChanged = true
					publicationBoundsChanged = true
				}
			}
			// 候选新成员/扩窗历史不得冲掉当前服务版缓存。
			// 只有变化小时处在已发布窗口，且至少一个变化成员已发布时，
			// 才切换 serving generation。
			deferServingGeneration := usageFactRepairActive(global) &&
				hourTs >= global.RepairFrom && hourTs < global.RepairThrough
			if !deferServingGeneration && len(changedIDs) > 0 && hourTs >= global.PublishedRangeStart && hourTs < global.PublishedThrough {
				inSQL, inArgs := usageIn("user_id", changedIDs)
				var publishedChanged int64
				if m.usageFactsFullHistoryEnabled() {
					// A signed daily row/proof masks staging hourly rows in the read
					// CTE. Do not rotate serving cache until the independent day
					// control and daily replacement commit atomically in finalizer.
					args := append([]any{usageFactDayStart(hourTs), usageFactDayStart(hourTs)}, inArgs...)
					if err := tx.Raw(`SELECT COUNT(*) FROM usage_fact_published_members p
 WHERE NOT EXISTS (SELECT 1 FROM usage_daily_facts d WHERE d.user_id=p.user_id AND d.date_ts=?)
   AND NOT EXISTS (SELECT 1 FROM usage_fact_member_day_states s WHERE s.user_id=p.user_id AND s.date_ts=? AND s.content_hash<>'')
   AND `+strings.Replace(inSQL, "user_id", "p.user_id", 1), args...).Scan(&publishedChanged).Error; err != nil {
						return err
					}
				} else if err := tx.Model(&UsageFactPublishedMember{}).Where(inSQL, inArgs...).Count(&publishedChanged).Error; err != nil {
					return err
				}
				if publishedChanged > 0 {
					if !servingChanged {
						global.ServingGeneration++
					}
					servingChanged = true
				}
			}
		}
		if options.updateLastFactSync {
			global.LastFactSyncAt = nowUnix
			// 该字段表示当前事实来源链路的最近失败状态，而不是永久历史。
			// 成功后必须清零，否则运维会把数小时前已恢复的故障误判为仍在失败。
			global.LastFactFailureAt = 0
		}
		generation = global.Generation
		servingGeneration = global.ServingGeneration
		if publicationBoundsChanged {
			publishedState = global
		}
		return tx.Save(&global).Error
	})
	if err == nil {
		if changed {
			m.publishUsageFactGenerations(generation, 0)
		}
		if servingChanged {
			m.publishUsageFactGenerations(0, servingGeneration)
		}
		if publicationBoundsChanged {
			m.publishUsageFactReadBoundsAfterMutation(publishedState)
		}
	}
	m.usageFactsSyncMu.Unlock()
	if err != nil {
		if cleanupErr := m.failUsageFactHourClaim(hourTs, ids, claim, err, options.recordFailure); cleanupErr != nil {
			return result, errors.Join(err, fmt.Errorf("清理用量事实小时租约失败: %w", cleanupErr))
		}
		return result, err
	}
	result.Changed = changed
	result.SucceededUserIDs = append(result.SucceededUserIDs, ids...)
	result.ChangedUserIDs = append(result.ChangedUserIDs, changedIDs...)
	return result, nil
}
