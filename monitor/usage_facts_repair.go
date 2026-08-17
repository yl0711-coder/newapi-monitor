package monitor

// usage_facts_repair.go 提供一条对 8 天以外晚到日志和旧候选库缺失
// 成员日 proof 的受控修复路径。它不新建任何来源并发：只会把指定完整
// CST 自然日的本地完成证明置为待重建，随后复用已有的每 15 秒一小时、
// 每批最多 200 成员的串行回填。来源 NewAPI 始终只读。

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const (
	usageFactManualRepairMaxDays    = 31
	usageFactMigrationRepairMaxDays = 366
)

type usageFactRepairRequest struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Mode    string `json:"mode"`
	Confirm string `json:"confirm"`
}

func usageFactRepairActive(state UsageFactSyncState) bool {
	return state.RepairRequestedAt > 0 && state.RepairCompletedAt < state.RepairRequestedAt
}

func usageFactRepairMaxDays(mode string) int {
	if mode == "proof_migration" {
		return usageFactMigrationRepairMaxDays
	}
	return usageFactManualRepairMaxDays
}

func (m *Monitor) requestUsageFactsRepairHandler(c *gin.Context) {
	var in usageFactRepairRequest
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请填写待补数的开始和结束日期"})
		return
	}
	if strings.TrimSpace(in.Confirm) != "REPAIR_LOCAL_FACTS" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请明确确认受控补数"})
		return
	}
	state, err := m.requestUsageFactsRepair(c.Request.Context(), in, time.Now())
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errUsageFactRepairConflict) {
			status = http.StatusConflict
		} else if errors.Is(err, errUsageFactRepairUnavailable) {
			status = http.StatusServiceUnavailable
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"ok": true,
		"repair": gin.H{
			"from": state.RepairFrom, "through": state.RepairThrough,
			"mode":               state.RepairMode,
			"target_members":     state.RepairTargetMembers,
			"requested_at":       state.RepairRequestedAt,
			"total_member_hours": state.RepairTotalMemberHours,
		},
	})
}

var (
	errUsageFactRepairConflict    = errors.New("已有用量事实补数任务正在进行")
	errUsageFactRepairUnavailable = errors.New("用量事实受控补数当前不可用")
	errUsageFactRepairNotNeeded   = errors.New("候选用量事实没有可修复缺口")
)

func beginUsageFactRepairTx(tx *gorm.DB, state *UsageFactSyncState, mode string, from, through int64, ids []int64, membershipFingerprint string, nowUnix int64) error {
	if state == nil || len(ids) == 0 || from <= 0 || through <= from {
		return fmt.Errorf("%w：补数目标无效", errUsageFactRepairUnavailable)
	}
	ordered := append([]int64(nil), ids...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	unique := ordered[:0]
	for _, id := range ordered {
		if id <= 0 || (len(unique) > 0 && unique[len(unique)-1] == id) {
			continue
		}
		unique = append(unique, id)
	}
	ordered = unique
	if len(ordered) == 0 {
		return fmt.Errorf("%w：补数成员为空", errUsageFactRepairUnavailable)
	}
	inSQL, inArgs := usageIn("user_id", ordered)
	var members []UsageFactMemberState
	if err := tx.Where("active = ? AND "+inSQL, append([]any{true}, inArgs...)...).Order("user_id").Find(&members).Error; err != nil {
		return err
	}
	if len(members) != len(ordered) {
		return fmt.Errorf("%w：补数成员已发生变化", errUsageFactRepairConflict)
	}
	targets := make([]UsageFactRepairMember, 0, len(members))
	for _, member := range members {
		targets = append(targets, UsageFactRepairMember{
			UserID: member.UserID, RequestedAt: nowUnix, ResumeBackfillHour: member.NextBackfillHour,
		})
	}
	if err := tx.Where("1 = 1").Delete(&UsageFactRepairMember{}).Error; err != nil {
		return err
	}
	if err := tx.CreateInBatches(targets, usageFactProfileBatch).Error; err != nil {
		return err
	}
	deleteArgs := append([]any{from, through}, inArgs...)
	if err := tx.Where("hour_ts >= ? AND hour_ts < ? AND "+inSQL, deleteArgs...).Delete(&UsageFactMemberHourState{}).Error; err != nil {
		return err
	}
	updateArgs := append([]any{from, from, nowUnix}, inArgs...)
	if err := tx.Exec("UPDATE usage_fact_member_states SET next_backfill_hour = CASE WHEN next_backfill_hour > ? THEN ? ELSE next_backfill_hour END, last_failure_at = 0, last_error = '', updated_at = ? WHERE active = 1 AND "+inSQL,
		updateArgs...).Error; err != nil {
		return err
	}
	var next sql.NullInt64
	if err := tx.Model(&UsageFactMemberState{}).Select("MIN(next_backfill_hour)").Where("active = ?", true).Scan(&next).Error; err != nil {
		return err
	}
	state.NextBackfillHour = 0
	if next.Valid {
		state.NextBackfillHour = next.Int64
	}
	days := (through - from) / usageFactDaySeconds
	state.RepairFrom = from
	state.RepairThrough = through
	state.RepairMode = mode
	state.RepairMembershipFingerprint = membershipFingerprint
	state.RepairTargetMembers = int64(len(ordered))
	state.RepairRequestedAt = nowUnix
	state.RepairCompletedAt = 0
	state.RepairLastFailureAt = 0
	state.RepairLastError = ""
	state.RepairTotalMemberHours = days * 24 * int64(len(ordered))
	state.RepairCompletedMemberHours = 0
	state.Generation++
	return tx.Save(state).Error
}

func (m *Monitor) requestUsageFactsRepair(ctx context.Context, in usageFactRepairRequest, now time.Time) (UsageFactSyncState, error) {
	if !m.usageFactsEnabled() || m.prodDB == nil || m.usageFactsStore() == nil {
		return UsageFactSyncState{}, fmt.Errorf("%w：需要已启用且连接本地验收来源的 facts 同步器", errUsageFactRepairUnavailable)
	}
	if m.usageFactsFullHistoryEnabled() {
		return UsageFactSyncState{}, fmt.Errorf("%w：全历史模式禁止旧游标修复，请使用全历史成员日修复任务", errUsageFactRepairUnavailable)
	}
	mode := strings.TrimSpace(in.Mode)
	if mode == "" {
		mode = "manual"
	}
	if mode != "manual" && mode != "proof_migration" {
		return UsageFactSyncState{}, errors.New("补数 mode 只能是 manual 或 proof_migration")
	}
	from, through, err := parseUsageRange(in.From, in.To, now)
	if err != nil {
		return UsageFactSyncState{}, err
	}
	days := int((through - from) / usageFactDaySeconds)
	if days <= 0 || days > usageFactRepairMaxDays(mode) {
		return UsageFactSyncState{}, fmt.Errorf("单次补数最多 %d 个完整自然日", usageFactRepairMaxDays(mode))
	}
	// 只修复已完全闭合的自然日。当天尾部由 Tail 处理，不允许人工
	// 修复与它交叉，也不允许借未闭合日期绕过语义 proof。
	lastFullDayThrough := usageFactDayStart(m.usageFactFinalizedHour(now))
	if through > lastFullDayThrough {
		return UsageFactSyncState{}, errors.New("只能补数已完全闭合的 CST 自然日")
	}

	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()

	var requested UsageFactSyncState
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&requested, 1).Error; err != nil {
			return err
		}
		if usageFactRepairActive(requested) {
			return errUsageFactRepairConflict
		}
		if requested.PublishedAt <= 0 || requested.PublishedFingerprint == "" ||
			requested.PublishedThrough <= requested.PublishedRangeStart {
			return fmt.Errorf("%w：尚无完整已发布快照", errUsageFactRepairUnavailable)
		}
		firstFullDay := usageFactDayStart(requested.PublishedRangeStart)
		if firstFullDay < requested.PublishedRangeStart {
			firstFullDay += usageFactDaySeconds
		}
		lastPublishedFullDay := usageFactDayStart(requested.PublishedThrough)
		if from < firstFullDay || through > lastPublishedFullDay {
			return errors.New("补数范围必须完全位于已发布快照的完整自然日内")
		}
		// 候选名单变化或扩窗时先让既有回填完成，避免两种游标相互覆盖。
		if requested.MemberFingerprint != requested.PublishedFingerprint ||
			requested.BackfillWindowDays != requested.PublishedWindowDays {
			return fmt.Errorf("%w：候选名单或窗口正在回填", errUsageFactRepairConflict)
		}
		var publishedIDs []int64
		if err := tx.Model(&UsageFactPublishedMember{}).Order("user_id").Pluck("user_id", &publishedIDs).Error; err != nil {
			return err
		}
		if len(publishedIDs) == 0 || portalMemberFingerprintFromIDs(publishedIDs) != requested.PublishedFingerprint {
			return fmt.Errorf("%w：已发布成员快照不完整", errUsageFactRepairUnavailable)
		}
		var pending int64
		if err := tx.Model(&UsageFactMemberState{}).Where("active = ? AND next_backfill_hour < ?", true, requested.PublishedThrough).Count(&pending).Error; err != nil {
			return err
		}
		if pending > 0 {
			return fmt.Errorf("%w：历史候选回填尚未结束", errUsageFactRepairConflict)
		}

		return beginUsageFactRepairTx(tx, &requested, mode, from, through, publishedIDs, requested.MemberFingerprint, now.Unix())
	})
	if err != nil {
		return UsageFactSyncState{}, err
	}
	m.publishUsageFactGenerations(requested.Generation, 0)
	// 旧日事实和旧日 proof 在 24 小时重建完成前仍是自洽的服务版；
	// 不关闭 read_active，也不会回扫页面来源库。
	return requested, nil
}

// requestUsageFactsCandidateGapRepair 把候选发布审计发现的首个坏自然日转换为
// 持久修复任务。它只扫描该日本地 proof/facts，精确选择坏成员；来源读取仍由
// 原有串行小时 worker 执行，因此不会因自动修复增加并发或绕过节流。
func (m *Monitor) requestUsageFactsCandidateGapRepair(ctx context.Context, dayTs int64, now time.Time) (UsageFactSyncState, error) {
	if !m.usageFactsEnabled() || m.prodDB == nil || m.usageFactsStore() == nil {
		return UsageFactSyncState{}, fmt.Errorf("%w：candidate-gap 需要已启用的 facts 同步器", errUsageFactRepairUnavailable)
	}
	tracked, err := m.listTracked()
	if err != nil {
		return UsageFactSyncState{}, err
	}
	ids := idsOf(tracked)
	if len(ids) == 0 {
		return UsageFactSyncState{}, fmt.Errorf("%w：当前没有关注成员", errUsageFactRepairUnavailable)
	}
	fingerprint := portalMemberFingerprintFromIDs(ids)
	through := dayTs + usageFactDaySeconds
	end := m.usageFactFinalizedHour(now)
	start := end - int64(m.usageFactBackfillDays())*usageFactDaySeconds
	firstFullDay, lastFullDay := usageFactSemanticFullDayRange(start, end)
	if dayTs < firstFullDay || through > lastFullDay || usageFactDayStart(dayTs) != dayTs {
		return UsageFactSyncState{}, fmt.Errorf("%w：candidate-gap 不在候选完整自然日内", errUsageFactRepairUnavailable)
	}

	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	var requested UsageFactSyncState
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&requested, 1).Error; err != nil {
			return err
		}
		if usageFactRepairActive(requested) {
			return errUsageFactRepairConflict
		}
		if requested.MemberFingerprint != fingerprint || requested.BackfillWindowDays != m.usageFactBackfillDays() {
			return fmt.Errorf("%w：候选成员或窗口已变化", errUsageFactRepairConflict)
		}
		var pending int64
		if err := tx.Model(&UsageFactMemberState{}).Where("active = ? AND next_backfill_hour < ?", true, end).Count(&pending).Error; err != nil {
			return err
		}
		if pending > 0 {
			return fmt.Errorf("%w：候选历史仍在回填", errUsageFactRepairConflict)
		}
		invalidIDs, err := invalidUsageFactMembersForDay(tx, dayTs, ids)
		if err != nil {
			return err
		}
		if len(invalidIDs) == 0 {
			return errUsageFactRepairNotNeeded
		}
		return beginUsageFactRepairTx(tx, &requested, "candidate_gap", dayTs, through, invalidIDs, fingerprint, now.Unix())
	})
	if err != nil {
		return UsageFactSyncState{}, err
	}
	m.publishUsageFactGenerations(requested.Generation, 0)
	return requested, nil
}

// refreshUsageFactRepairProgressTx 只读最多 500 条成员游标，不扫描小时 proof。
// 游标只在对应成员小时已落盘后前移，因此可作为补数进度的持久证明。
func refreshUsageFactRepairProgressTx(tx *gorm.DB) error {
	var state UsageFactSyncState
	if err := tx.First(&state, 1).Error; err != nil {
		return err
	}
	if !usageFactRepairActive(state) || state.RepairThrough <= state.RepairFrom {
		return nil
	}
	var targets []UsageFactRepairMember
	if err := tx.Where("requested_at = ?", state.RepairRequestedAt).Order("user_id").Find(&targets).Error; err != nil {
		return err
	}
	// 兼容升级前已经在运行、尚无目标表的旧 repair。
	if len(targets) == 0 {
		var ids []int64
		if err := tx.Model(&UsageFactPublishedMember{}).Order("user_id").Pluck("user_id", &ids).Error; err != nil {
			return err
		}
		for _, id := range ids {
			targets = append(targets, UsageFactRepairMember{UserID: id, RequestedAt: state.RepairRequestedAt})
		}
	}
	if len(targets) == 0 {
		return nil
	}
	if state.RepairMembershipFingerprint != "" && state.MemberFingerprint != state.RepairMembershipFingerprint {
		nowUnix := time.Now().Unix()
		state.RepairCompletedAt = nowUnix
		state.RepairLastFailureAt = nowUnix
		state.RepairLastError = "候选成员在修复期间发生变化，任务已安全取消并等待重新审计"
		state.Generation++
		return tx.Save(&state).Error
	}
	ids := make([]int64, 0, len(targets))
	for _, target := range targets {
		ids = append(ids, target.UserID)
	}
	inSQL, inArgs := usageIn("user_id", ids)
	var members []UsageFactMemberState
	if err := tx.Where("active = ? AND "+inSQL, append([]any{true}, inArgs...)...).Order("user_id").Find(&members).Error; err != nil {
		return err
	}
	if len(members) != len(ids) {
		return nil
	}
	cursorByID := make(map[int64]int64, len(members))
	for _, member := range members {
		cursorByID[member.UserID] = member.NextBackfillHour
	}
	completed := int64(0)
	allDone := true
	perMember := (state.RepairThrough - state.RepairFrom) / usageFactHourSeconds
	for _, target := range targets {
		cursor := cursorByID[target.UserID]
		coveredThrough := cursor
		if coveredThrough < state.RepairFrom {
			coveredThrough = state.RepairFrom
		}
		if coveredThrough > state.RepairThrough {
			coveredThrough = state.RepairThrough
		}
		completed += (coveredThrough - state.RepairFrom) / usageFactHourSeconds
		if cursor < state.RepairThrough {
			allDone = false
		}
	}
	if completed > perMember*int64(len(ids)) {
		completed = perMember * int64(len(ids))
	}
	state.RepairCompletedMemberHours = completed
	if allDone {
		for _, target := range targets {
			if target.ResumeBackfillHour <= 0 {
				continue
			}
			if err := tx.Model(&UsageFactMemberState{}).
				Where("user_id = ? AND next_backfill_hour < ?", target.UserID, target.ResumeBackfillHour).
				Updates(map[string]any{"next_backfill_hour": target.ResumeBackfillHour, "updated_at": time.Now().Unix()}).Error; err != nil {
				return err
			}
		}
		state.RepairCompletedAt = time.Now().Unix()
		state.RepairLastError = ""
		state.Generation++
		if state.PublishedAt > 0 && state.RepairFrom < state.PublishedThrough && state.RepairThrough > state.PublishedRangeStart {
			var publishedTargets int64
			if err := tx.Model(&UsageFactPublishedMember{}).Where(inSQL, inArgs...).Count(&publishedTargets).Error; err != nil {
				return err
			}
			if publishedTargets > 0 {
				state.ServingGeneration++
			}
		}
	}
	return tx.Save(&state).Error
}

func (m *Monitor) recordUsageFactRepairFailure(hour int64, cause error) {
	if hour <= 0 || m.usageFactsStore() == nil {
		return
	}
	updates := map[string]any{"repair_last_failure_at": time.Now().Unix()}
	if cause != nil {
		updates["repair_last_error"] = truncateUsageFactError(cause.Error())
	}
	_ = m.usageFactsStore().Model(&UsageFactSyncState{}).
		Where("id = 1 AND repair_requested_at > repair_completed_at AND repair_from <= ? AND repair_through > ?", hour, hour).
		Updates(updates).Error
}
