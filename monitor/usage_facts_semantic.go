package monitor

// usage_facts_semantic.go 为已经压成日事实、且小时明细可能已按留存策略清理的
// 历史数据提供业务语义校验。SQLite quick_check 只能证明页结构正常，无法发现
// 合法 SQL 误删一行业务数据；这里用“重建时原子保存的成员×日指纹”重新比对。

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"gorm.io/gorm"
)

const (
	usageFactSemanticAuditInterval = time.Hour
	usageFactSemanticAuditDays     = 14 // 单批控制内存；366 天约 27 批
)

type usageFactMemberDayKey struct {
	userID int64
	dayTs  int64
}

// usageFactMemberDayAuditError 保留首个可精确修复的成员×自然日。
// 发布候选失败后，readiness 会据此扫描同一天的全部坏成员并创建持久
// candidate-gap repair；普通结构/查询错误仍保持 fail-closed，不自动猜测修复范围。
type usageFactMemberDayAuditError struct {
	UserID int64
	DayTs  int64
	Kind   string
}

func (e *usageFactMemberDayAuditError) Error() string {
	return fmt.Sprintf("%s: user=%d day=%d", e.Kind, e.UserID, e.DayTs)
}

func usageDailyFactContentHash(rows []UsageDailyFact) string {
	ordered := append([]UsageDailyFact(nil), rows...)
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if a.DateTs != b.DateTs {
			return a.DateTs < b.DateTs
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
		writeInt(row.DateTs)
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

func dailyFactsMetrics(rows []UsageDailyFact) usageFactMetrics {
	var out usageFactMetrics
	out.Rows = int64(len(rows))
	for _, row := range rows {
		out.Requests += row.Requests
		out.RefundRecords += row.RefundRecords
		out.PromptTokens += row.PromptTokens
		out.CompletionTokens += row.CompletionTokens
		out.ConsumeQuota += row.ConsumeQuota
		out.RefundQuota += row.RefundQuota
	}
	return out
}

func usageFactMemberDayMetricsMatchState(metrics usageFactMetrics, state UsageFactMemberDayState) bool {
	// Legacy finite-window proofs only persisted this subset plus ContentHash.
	// Keep the compatibility predicate deliberately narrow: callers accepting a
	// legacy publication must not reinterpret zero-valued v5 columns as a strict
	// source signature. Full-history paths use the strict predicate below after
	// first proving status/epoch/semantic-version readiness.
	return metrics.Rows == int64(state.Rows) && metrics.Requests == state.Requests &&
		metrics.tokens() == state.Tokens
}

func usageFactMemberDayStrictMetricsMatchState(metrics usageFactMetrics, state UsageFactMemberDayState) bool {
	return usageFactMemberDayMetricsMatchState(metrics, state) &&
		metrics.RefundRecords == state.RefundRecords && metrics.PromptTokens == state.PromptTokens &&
		metrics.CompletionTokens == state.CompletionTokens &&
		metrics.ConsumeQuota == state.ConsumeQuota && metrics.RefundQuota == state.RefundQuota
}

func loadUsageDailyFacts(db *gorm.DB, start, end int64, ids []int64) ([]UsageDailyFact, error) {
	if start <= 0 || end <= start || len(ids) == 0 {
		return nil, nil
	}
	inSQL, inArgs := usageIn("user_id", ids)
	args := make([]any, 0, 2+len(inArgs))
	args = append(args, start, end)
	args = append(args, inArgs...)
	var rows []UsageDailyFact
	err := db.Where("date_ts >= ? AND date_ts < ? AND "+inSQL, args...).
		Order("date_ts, user_id, channel_id, grp, model_name, token_id").Find(&rows).Error
	return rows, err
}

func usageDailyFactsByMemberDay(rows []UsageDailyFact) map[usageFactMemberDayKey][]UsageDailyFact {
	out := make(map[usageFactMemberDayKey][]UsageDailyFact)
	for _, row := range rows {
		key := usageFactMemberDayKey{userID: row.UserID, dayTs: row.DateTs}
		out[key] = append(out[key], row)
	}
	return out
}

func auditUsageFactTrailingHours(db *gorm.DB, start, end int64, ids []int64) error {
	if start >= end {
		return nil
	}
	expectedHours := (end - start) / usageFactHourSeconds
	inSQL, inArgs := usageIn("user_id", ids)
	countArgs := make([]any, 0, 3+len(inArgs))
	countArgs = append(countArgs, start, end)
	countArgs = append(countArgs, inArgs...)
	countArgs = append(countArgs, "complete")
	var memberProofs int64
	if err := db.Model(&UsageFactMemberHourState{}).
		Where("hour_ts >= ? AND hour_ts < ? AND "+inSQL+" AND status = ? AND content_hash <> ''", countArgs...).
		Count(&memberProofs).Error; err != nil {
		return err
	}
	if memberProofs == expectedHours*int64(len(ids)) {
		// 调用方已持有同步锁；这里直接分片复用无锁的本地 proof+内容校验。
		for cursor := start; cursor < end; {
			limit := cursor + int64(usageFactBackfillSkipBatch)*usageFactHourSeconds
			if limit > end {
				limit = end
			}
			stateArgs := make([]any, 0, 3+len(inArgs))
			stateArgs = append(stateArgs, cursor, limit)
			stateArgs = append(stateArgs, inArgs...)
			stateArgs = append(stateArgs, "complete")
			var states []UsageFactMemberHourState
			if err := db.Where("hour_ts >= ? AND hour_ts < ? AND "+inSQL+" AND status = ?",
				stateArgs...).Order("hour_ts, user_id").Find(&states).Error; err != nil {
				return err
			}
			factArgs := make([]any, 0, 2+len(inArgs))
			factArgs = append(factArgs, cursor, limit)
			factArgs = append(factArgs, inArgs...)
			var facts []UsageHourFact
			if err := db.Where("hour_ts >= ? AND hour_ts < ? AND "+inSQL, factArgs...).
				Order("hour_ts, user_id, channel_id, grp, model_name, token_id").Find(&facts).Error; err != nil {
				return err
			}
			type memberHourKey struct{ userID, hourTs int64 }
			statesByKey := make(map[memberHourKey]UsageFactMemberHourState, len(states))
			for _, state := range states {
				statesByKey[memberHourKey{userID: state.UserID, hourTs: state.HourTs}] = state
			}
			factsByKey := make(map[memberHourKey][]UsageHourFact)
			for _, fact := range facts {
				key := memberHourKey{userID: fact.UserID, hourTs: fact.HourTs}
				factsByKey[key] = append(factsByKey[key], fact)
			}
			for hour := cursor; hour < limit; hour += usageFactHourSeconds {
				for _, id := range ids {
					key := memberHourKey{userID: id, hourTs: hour}
					state, ok := statesByKey[key]
					rows := factsByKey[key]
					if !ok || !usageFactMemberMetricsMatchState(factsMetrics(rows), state) ||
						usageFactContentHash(rows) != state.ContentHash {
						return fmt.Errorf("当前日成员小时事实语义不一致: user=%d hour=%d", id, hour)
					}
				}
			}
			cursor = limit
		}
		return nil
	}
	if memberProofs != 0 {
		return fmt.Errorf("当前日成员小时证明不完整: got=%d want=%d", memberProofs, expectedHours*int64(len(ids)))
	}

	// 兼容早期本地快照：没有成员级 proof 时，必须由每小时聚合证明和实际
	// 小时事实共同通过，不能因为兼容分支而只信水位或把缺行当成零。
	var legacy []UsageHourIngestState
	if err := db.Where("hour_ts >= ? AND hour_ts < ? AND status = ? AND content_hash <> ''", start, end, "complete").
		Order("hour_ts").Find(&legacy).Error; err != nil {
		return err
	}
	if int64(len(legacy)) != expectedHours {
		return fmt.Errorf("当前日小时事实语义证明不完整: got=%d want=%d", len(legacy), expectedHours)
	}
	factArgs := make([]any, 0, 2+len(inArgs))
	factArgs = append(factArgs, start, end)
	factArgs = append(factArgs, inArgs...)
	var facts []UsageHourFact
	if err := db.Where("hour_ts >= ? AND hour_ts < ? AND "+inSQL, factArgs...).
		Order("hour_ts, user_id, channel_id, grp, model_name, token_id").Find(&facts).Error; err != nil {
		return err
	}
	factsByHour := make(map[int64][]UsageHourFact)
	for _, fact := range facts {
		factsByHour[fact.HourTs] = append(factsByHour[fact.HourTs], fact)
	}
	for _, state := range legacy {
		rows := factsByHour[state.HourTs]
		if !usageFactMetricsMatchState(factsMetrics(rows), state) || usageFactContentHash(rows) != state.ContentHash {
			return fmt.Errorf("当前日旧版小时事实语义不一致: hour=%d", state.HourTs)
		}
	}
	return nil
}

// auditUsageFactTrailingHoursForEpoch forbids a source-epoch migration from
// combining old hourly proofs with a new control result. The ordinary wrapper
// remains available to the legacy finite-window reader.
func auditUsageFactTrailingHoursForEpoch(db *gorm.DB, start, end int64, ids []int64, epoch string) error {
	if start >= end {
		return nil
	}
	if epoch == "" || len(ids) == 0 {
		return errors.New("当前来源 epoch 不能为空")
	}
	inSQL, inArgs := usageIn("user_id", ids)
	args := make([]any, 0, 4+len(inArgs))
	args = append(args, start, end)
	args = append(args, inArgs...)
	args = append(args, "complete", epoch)
	var complete int64
	if err := db.Model(&UsageFactMemberHourState{}).
		Where("hour_ts >= ? AND hour_ts < ? AND "+inSQL+" AND status = ? AND content_hash <> '' AND source_epoch = ?", args...).
		Count(&complete).Error; err != nil {
		return err
	}
	want := ((end - start) / usageFactHourSeconds) * int64(len(ids))
	if complete != want {
		return fmt.Errorf("当前来源 epoch 小时证明不完整: got=%d want=%d", complete, want)
	}
	return auditUsageFactTrailingHours(db, start, end, ids)
}

func auditUsageFactKnownEmptyRange(db *gorm.DB, userID, start, end int64) error {
	if userID <= 0 || start <= 0 || end <= start {
		return nil
	}
	// Locate the first contradictory partition instead of COUNTing an entire
	// multi-year range. Besides keeping the rolling audit index-bounded, the
	// typed error gives the repair scheduler the exact member-day; it must never
	// guess claim.From and repeatedly repair the wrong day.
	var daily UsageDailyFact
	dailyErr := db.Select("date_ts").
		Where("user_id = ? AND date_ts >= ? AND date_ts < ?", userID, usageFactDayStart(start), end).
		Order("date_ts").Take(&daily).Error
	if dailyErr != nil && !errors.Is(dailyErr, gorm.ErrRecordNotFound) {
		return dailyErr
	}
	var hourly UsageHourFact
	hourErr := db.Select("hour_ts").
		Where("user_id = ? AND hour_ts >= ? AND hour_ts < ?", userID, start, end).
		Order("hour_ts").Take(&hourly).Error
	if hourErr != nil && !errors.Is(hourErr, gorm.ErrRecordNotFound) {
		return hourErr
	}
	badDay := int64(0)
	if dailyErr == nil {
		badDay = usageFactDayStart(daily.DateTs)
	}
	if hourErr == nil {
		hourDay := usageFactDayStart(hourly.HourTs)
		if badDay == 0 || hourDay < badDay {
			badDay = hourDay
		}
	}
	if badDay > 0 {
		return &usageFactMemberDayAuditError{UserID: userID, DayTs: badDay, Kind: "已证明空区间仍有本地事实"}
	}
	return nil
}

// auditUsageFactDailyRange 对完整自然日分批校验，避免一次把整个 366 天矩阵
// 和证明同时载入内存。每个空流量成员日也必须有一条空集合证明。
func auditUsageFactDailyRange(db *gorm.DB, start, end int64, ids []int64) error {
	firstDay := usageFactDayStart(start)
	if firstDay < start {
		firstDay += usageFactDaySeconds
	}
	lastDay := usageFactDayStart(end)
	if firstDay >= lastDay || len(ids) == 0 {
		return nil
	}
	for batchStart := firstDay; batchStart < lastDay; {
		batchEnd := batchStart + int64(usageFactSemanticAuditDays)*usageFactDaySeconds
		if batchEnd > lastDay {
			batchEnd = lastDay
		}
		inSQL, inArgs := usageIn("user_id", ids)
		stateArgs := make([]any, 0, 3+len(inArgs))
		stateArgs = append(stateArgs, batchStart, batchEnd)
		stateArgs = append(stateArgs, inArgs...)
		stateArgs = append(stateArgs, "")
		var states []UsageFactMemberDayState
		if err := db.Where("date_ts >= ? AND date_ts < ? AND "+inSQL+" AND content_hash <> ?", stateArgs...).
			Order("date_ts, user_id").Find(&states).Error; err != nil {
			return fmt.Errorf("读取日事实语义证明失败: %w", err)
		}
		stateByKey := make(map[usageFactMemberDayKey]UsageFactMemberDayState, len(states))
		for _, state := range states {
			stateByKey[usageFactMemberDayKey{userID: state.UserID, dayTs: state.DateTs}] = state
		}
		rows, err := loadUsageDailyFacts(db, batchStart, batchEnd, ids)
		if err != nil {
			return fmt.Errorf("读取日事实语义内容失败: %w", err)
		}
		rowsByKey := usageDailyFactsByMemberDay(rows)
		for day := batchStart; day < batchEnd; day += usageFactDaySeconds {
			for _, id := range ids {
				key := usageFactMemberDayKey{userID: id, dayTs: day}
				state, ok := stateByKey[key]
				if !ok {
					return &usageFactMemberDayAuditError{UserID: id, DayTs: day, Kind: "缺少日事实语义证明"}
				}
				memberRows := rowsByKey[key]
				if !usageFactMemberDayMetricsMatchState(dailyFactsMetrics(memberRows), state) ||
					usageDailyFactContentHash(memberRows) != state.ContentHash {
					return &usageFactMemberDayAuditError{UserID: id, DayTs: day, Kind: "日事实语义指纹不一致"}
				}
			}
		}
		batchStart = batchEnd
	}
	return nil
}

// invalidUsageFactMembersForDay 返回同一完整自然日中所有缺 proof 或内容不一致的
// 目标成员。它只读本地 facts，不访问来源库；单次最多受 tracked-user 上限约束。
func invalidUsageFactMembersForDay(db *gorm.DB, dayTs int64, ids []int64) ([]int64, error) {
	if dayTs <= 0 || usageFactDayStart(dayTs) != dayTs || len(ids) == 0 {
		return nil, fmt.Errorf("候选缺口扫描范围无效")
	}
	inSQL, inArgs := usageIn("user_id", ids)
	proofArgs := append([]any{dayTs, dayTs + usageFactDaySeconds}, inArgs...)
	proofArgs = append(proofArgs, "")
	var states []UsageFactMemberDayState
	if err := db.Where("date_ts >= ? AND date_ts < ? AND "+inSQL+" AND content_hash <> ?", proofArgs...).
		Order("user_id").Find(&states).Error; err != nil {
		return nil, fmt.Errorf("读取候选日事实证明失败: %w", err)
	}
	stateByUser := make(map[int64]UsageFactMemberDayState, len(states))
	for _, state := range states {
		stateByUser[state.UserID] = state
	}
	rows, err := loadUsageDailyFacts(db, dayTs, dayTs+usageFactDaySeconds, ids)
	if err != nil {
		return nil, fmt.Errorf("读取候选日事实内容失败: %w", err)
	}
	rowsByKey := usageDailyFactsByMemberDay(rows)
	invalid := make([]int64, 0)
	for _, id := range ids {
		state, ok := stateByUser[id]
		memberRows := rowsByKey[usageFactMemberDayKey{userID: id, dayTs: dayTs}]
		if !ok || !usageFactMemberDayMetricsMatchState(dailyFactsMetrics(memberRows), state) ||
			usageDailyFactContentHash(memberRows) != state.ContentHash {
			invalid = append(invalid, id)
		}
	}
	return invalid, nil
}

// auditUsageFactSnapshot 校验页面真正可读取的数据层：完整自然日核对日事实
// 证明；当前未闭合自然日仍由小时事实提供，复用成员小时 proof+hash 校验。
// 回填窗口最左侧若从日中开始，UI 的自然日查询会从下一个完整日开始，因此
// 不把随后按小时留存清理的半日当作可服务历史。
func (m *Monitor) auditUsageFactSnapshot(ctx context.Context, start, end int64, ids []int64) error {
	if start <= 0 || end <= start || len(ids) == 0 {
		return fmt.Errorf("用量事实语义审计范围无效")
	}
	db := m.usageFactsStore().WithContext(ctx)
	if err := auditUsageFactDailyRange(db, start, end, ids); err != nil {
		return err
	}
	trailingStart := usageFactDayStart(end)
	if trailingStart < start {
		trailingStart = start
	}
	if err := auditUsageFactTrailingHours(db, trailingStart, end, ids); err != nil {
		return fmt.Errorf("核验当前日小时事实失败: %w", err)
	}
	return nil
}

func (m *Monitor) recordUsageFactsSemanticAudit(now time.Time, err error) {
	if now.IsZero() {
		now = time.Now()
	}
	m.usageFactsSemanticAuditAt.Store(now.Unix())
	if err == nil {
		m.usageFactsSemanticAuditOK.Store(true)
		return
	}
	m.usageFactsSemanticAuditFailureAt.Store(now.Unix())
	m.usageFactsSemanticAuditOK.Store(false)
	// 先关闭读取许可；包装器随后只会返回 facts not ready，绝不回扫来源库。
	m.setUsageFactsPublishedReadiness(false, 0, 0)
}

func usageFactSemanticFullDayRange(start, end int64) (int64, int64) {
	firstDay := usageFactDayStart(start)
	if firstDay < start {
		firstDay += usageFactDaySeconds
	}
	lastDay := usageFactDayStart(end)
	return firstDay, lastDay
}

// ensureUsageFactsSemanticAudit 在首次启用/发布前做全量审计，稳态按小时滚动
// 分片。这里的节流同时避免 Tail 每 5 分钟重复扫描长历史。
func (m *Monitor) ensureUsageFactsSemanticAudit(ctx context.Context, start, end int64, ids []int64, now time.Time, force bool) error {
	if now.IsZero() {
		now = time.Now()
	}
	last := m.usageFactsSemanticAuditAt.Load()
	if !force && last > 0 && now.Unix()-last < int64(usageFactSemanticAuditInterval/time.Second) {
		if m.usageFactsSemanticAuditOK.Load() {
			return nil
		}
		return fmt.Errorf("上次用量事实语义审计失败，等待下一轮复核")
	}
	qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	m.usageFactsSyncMu.Lock()
	var err error
	if force || !m.usageFactsSemanticAuditOK.Load() {
		// 首次启动和新候选发布前必须覆盖完整服务范围；未经全量审计
		// 绝不打开读取许可。
		err = m.auditUsageFactSnapshot(qctx, start, end, ids)
		if err == nil {
			firstDay, _ := usageFactSemanticFullDayRange(start, end)
			m.usageFactsSemanticAuditNextDay.Store(firstDay)
		}
	} else {
		// 稳态每小时只滚动审计最多 14 个完整自然日，避免 366 天全量
		// 复核与页面冷查询争用 SQLite。当前未闭合日和最近完整日每轮
		// 都复核，历史逻辑损坏最迟约 ceil(366/14)=27 小时被发现。
		firstDay, lastDay := usageFactSemanticFullDayRange(start, end)
		nextDay := m.usageFactsSemanticAuditNextDay.Load()
		if nextDay < firstDay || nextDay >= lastDay {
			nextDay = firstDay
		}
		batchEnd := nextDay + int64(usageFactSemanticAuditDays)*usageFactDaySeconds
		if batchEnd > lastDay {
			batchEnd = lastDay
		}
		if nextDay < batchEnd {
			err = auditUsageFactDailyRange(m.usageFactsStore().WithContext(qctx), nextDay, batchEnd, ids)
		}
		if err == nil && lastDay-firstDay >= usageFactDaySeconds {
			recentStart := lastDay - usageFactDaySeconds
			if recentStart < nextDay || recentStart >= batchEnd {
				err = auditUsageFactDailyRange(m.usageFactsStore().WithContext(qctx), recentStart, lastDay, ids)
			}
		}
		trailingStart := usageFactDayStart(end)
		if trailingStart < start {
			trailingStart = start
		}
		if err == nil {
			err = auditUsageFactTrailingHours(m.usageFactsStore().WithContext(qctx), trailingStart, end, ids)
		}
		if err == nil {
			if batchEnd >= lastDay {
				batchEnd = firstDay
			}
			m.usageFactsSemanticAuditNextDay.Store(batchEnd)
		}
	}
	m.usageFactsSyncMu.Unlock()
	m.recordUsageFactsSemanticAudit(now, err)
	return err
}
