package monitor

// 今日用量采用双水位：小时/日事实仍是唯一权威历史；用户资料快照中的
// users.used_quota 只提供一个可丢弃的实时净额投影。这样页面无需为了当前
// 小时扫描生产 logs，同时小时封口、审计、修复和回滚语义完全不变。

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

	"gorm.io/gorm"
)

const usageLiveProjectionMaxAge = 30 * time.Minute

const (
	usageLiveProjectionAnchorMaxGap       = 15 * time.Minute
	usageLiveProjectionWatermarkRetention = 49 * time.Hour
)

const usageLiveProjectionDimension = "实时待归类"

type usageLiveProjection struct {
	DayTs            int64
	Through          int64
	FinalizedThrough int64
	TodayNetByUser   map[int64]int64
}

// loadUsageLiveProjection 从本地 SQLite 计算“今日已封口小时事实 + 当前累计
// used_quota - 封口后最近累计锚点”。它不要求终身累计与全部历史事实永久相等；
// 只有所有所选成员都满足完整性、锚点和新鲜度条件时才返回，避免公司合计混入
// 不同覆盖口径。
func (m *Monitor) loadUsageLiveProjection(ctx context.Context, ids []int64, fromTs, toTs int64, now time.Time) (*usageLiveProjection, error) {
	if m == nil || !m.usageFactsReadEnabled() || len(ids) == 0 || toTs <= fromTs {
		return nil, nil
	}
	dayTs := usageFactDayStart(now.Unix())
	if fromTs > dayTs || toTs <= dayTs {
		return nil, nil
	}
	finalizedThrough := m.usageFactsReadyThrough.Load()
	if finalizedThrough < dayTs || finalizedThrough > now.Unix()+usageFactHourSeconds {
		return nil, nil
	}

	unique := make(map[int64]struct{}, len(ids))
	ordered := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, exists := unique[id]; exists {
			continue
		}
		unique[id] = struct{}{}
		ordered = append(ordered, id)
	}
	if len(ordered) == 0 {
		return nil, nil
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })

	qctx, cancel := usageFactQueryContext(ctx)
	defer cancel()
	var projection *usageLiveProjection
	err := m.usageFactsStore().WithContext(qctx).Transaction(func(tx *gorm.DB) error {
		var err error
		projection, err = m.loadUsageLiveProjectionTx(tx, ordered, dayTs, finalizedThrough, now)
		return err
	})
	return projection, err
}

// loadUsageLiveProjectionTx deliberately reads publication, member proof,
// profile watermarks and the historical baseline from one SQLite snapshot.
// Without this transaction a fact publication between two SELECTs could pair
// a new baseline with an old cumulative snapshot (or the reverse).
func (m *Monitor) loadUsageLiveProjectionTx(db *gorm.DB, ordered []int64, dayTs, finalizedThrough int64, now time.Time) (*usageLiveProjection, error) {
	var published []UsageFactPublishedMember
	if err := db.Where("user_id IN ?", ordered).Find(&published).Error; err != nil {
		return nil, err
	}
	var states []UsageFactMemberState
	if err := db.Where("user_id IN ?", ordered).Find(&states).Error; err != nil {
		return nil, err
	}
	if len(published) != len(ordered) || len(states) != len(ordered) {
		return nil, nil
	}
	publishedByID := make(map[int64]UsageFactPublishedMember, len(published))
	for _, row := range published {
		publishedByID[row.UserID] = row
	}
	stateByID := make(map[int64]UsageFactMemberState, len(states))
	for _, row := range states {
		stateByID[row.UserID] = row
	}
	for _, id := range ordered {
		publishedRow, publishedOK := publishedByID[id]
		state, stateOK := stateByID[id]
		revisionCompatible := publishedRow.TrackedRevision == state.TrackedRevision ||
			(publishedRow.TrackedRevision == 0 && state.TrackedRevision == 1)
		if !publishedOK || !stateOK || !state.Active || publishedRow.PublishedAt <= 0 || publishedRow.SourceFloorHour <= 0 ||
			publishedRow.SourceEpoch != m.cfg.UsageFactsHistorySourceEpoch || state.SourceEpoch != publishedRow.SourceEpoch ||
			publishedRow.ClassificationVersion != userTrafficClassificationVersion || state.ClassificationVersion != publishedRow.ClassificationVersion ||
			publishedRow.QuerySemanticsVersion != usageFactQuerySemanticsVersion || state.QuerySemanticsVersion != publishedRow.QuerySemanticsVersion ||
			!revisionCompatible || state.SourceFloorHour == nil || *state.SourceFloorHour != publishedRow.SourceFloorHour ||
			(state.SourceHistoryStatus != "complete_hot" && state.SourceHistoryStatus != "no_history") || state.CoverageStatus != "ready" ||
			state.VerificationStatus != "complete" || state.CoverageThroughHour == nil || *state.CoverageThroughHour < dayTs {
			return nil, nil
		}
	}

	var snapshots []UsageUserSnapshot
	if err := db.Where("user_id IN ?", ordered).Find(&snapshots).Error; err != nil {
		return nil, err
	}
	if len(snapshots) != len(ordered) {
		return nil, nil
	}
	snapshotByID := make(map[int64]UsageUserSnapshot, len(snapshots))
	through := now.Unix()
	oldestAllowed := now.Add(-usageLiveProjectionMaxAge).Unix()
	for _, snap := range snapshots {
		if !snap.Exists || snap.CapturedAt < oldestAllowed || snap.CapturedAt > now.Unix()+60 {
			return nil, nil
		}
		snapshotByID[snap.UserID] = snap
		if snap.CapturedAt < through {
			through = snap.CapturedAt
		}
	}
	if len(snapshotByID) != len(ordered) || through <= finalizedThrough {
		return nil, nil
	}

	finalizedToday := make(map[int64]int64, len(ordered))
	if finalizedThrough > dayTs {
		hourIn, hourArgs := usageIn("user_id", ordered)
		query := `SELECT user_id,COALESCE(SUM(consume_quota),0)-COALESCE(SUM(refund_quota),0)
FROM usage_hour_facts
WHERE hour_ts >= ? AND hour_ts < ? AND ` + hourIn + ` GROUP BY user_id`
		args := append([]any{dayTs, finalizedThrough}, hourArgs...)
		rows, err := db.Raw(query, args...).Rows()
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, net int64
			if err := rows.Scan(&id, &net); err != nil {
				_ = rows.Close()
				return nil, err
			}
			finalizedToday[id] = net
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}

	type quotaAnchor struct {
		UserID     int64
		UsedQuota  int64
		CapturedAt int64
	}
	anchorIn, anchorArgs := usageIn("user_id", ordered)
	anchorQuery := `WITH picked AS (
 SELECT user_id,MIN(captured_at) AS captured_at
 FROM usage_user_quota_watermarks
 WHERE captured_at >= ? AND captured_at <= ? AND ` + anchorIn + ` AND [exists] = ?
 GROUP BY user_id
)
SELECT w.user_id,w.used_quota,w.captured_at
FROM usage_user_quota_watermarks w
JOIN picked p ON p.user_id=w.user_id AND p.captured_at=w.captured_at`
	anchorParams := []any{finalizedThrough, time.Unix(finalizedThrough, 0).Add(usageLiveProjectionAnchorMaxGap).Unix()}
	anchorParams = append(anchorParams, anchorArgs...)
	anchorParams = append(anchorParams, true)
	var anchorRows []quotaAnchor
	if err := db.Raw(anchorQuery, anchorParams...).Scan(&anchorRows).Error; err != nil {
		return nil, err
	}
	anchors := make(map[int64]quotaAnchor, len(anchorRows))
	for _, anchor := range anchorRows {
		anchors[anchor.UserID] = anchor
	}
	if len(anchors) != len(ordered) {
		return nil, nil
	}

	today := make(map[int64]int64, len(ordered))
	for _, id := range ordered {
		snap, ok := snapshotByID[id]
		anchor, anchorOK := anchors[id]
		if !ok || !anchorOK || anchor.CapturedAt < finalizedThrough ||
			anchor.CapturedAt > finalizedThrough+int64(usageLiveProjectionAnchorMaxGap/time.Second) ||
			snap.CapturedAt < anchor.CapturedAt {
			return nil, nil
		}
		// A published no-history member is safe only while its cumulative source
		// watermark is still zero. The first non-zero quota may precede the Tail
		// invalidation by one scheduling turn, so suppress the whole projection
		// until that member is rediscovered and verified.
		if stateByID[id].SourceHistoryStatus == "no_history" && snap.UsedQuota != 0 {
			return nil, nil
		}
		delta := snap.UsedQuota - anchor.UsedQuota
		// 负值说明累计水位在锚点之后发生了回退（例如人工重置）。此时宁可
		// 继续显示已封口事实，也不能把差异伪装成当前退款。
		if delta < 0 {
			return nil, nil
		}
		today[id] = finalizedToday[id] + delta
	}
	return &usageLiveProjection{DayTs: dayTs, Through: through, FinalizedThrough: finalizedThrough, TodayNetByUser: today}, nil
}

func applyUsageBillingNetFloor(b *UsageBilling, targetNet int64) bool {
	if b == nil {
		return false
	}
	b.finalize()
	if targetNet < b.NetQuota {
		return false
	}
	delta := targetNet - b.NetQuota
	if delta > 0 {
		// used_quota only proves净额，无法证明当前窗口的请求数、Token 或退款
		// 归属。把正向补差记入“待归类消费”，小时事实接管后自然消失。
		b.ConsumeQuota += delta
	}
	b.finalize()
	return true
}

func applyUsageMatrixLiveProjection(mx *UsageMatrix, projection *usageLiveProjection) bool {
	if mx == nil || projection == nil || len(projection.TodayNetByUser) == 0 {
		return false
	}
	date := time.Unix(projection.DayTs, 0).In(usageCST).Format("2006-01-02")
	byUser := make(map[int64]int, len(mx.Cells))
	for i := range mx.Cells {
		if mx.Cells[i].Date == date {
			byUser[mx.Cells[i].UserID] = i
		}
	}
	// 先验证所有成员均只会向前补差，再修改结果，保持聚合原子性。
	for id, target := range projection.TodayNetByUser {
		if i, ok := byUser[id]; ok {
			cell := mx.Cells[i]
			cell.finalize()
			if target < cell.NetQuota {
				return false
			}
		}
	}
	for id, target := range projection.TodayNetByUser {
		i, ok := byUser[id]
		if !ok {
			if target == 0 {
				continue
			}
			mx.Cells = append(mx.Cells, UsageMatrixCell{UserID: id, Date: date})
			i = len(mx.Cells) - 1
		}
		_ = applyUsageBillingNetFloor(&mx.Cells[i].UsageBilling, target)
	}
	hasDay := false
	for _, day := range mx.Days {
		if day == date {
			hasDay = true
			break
		}
	}
	if !hasDay {
		mx.Days = append(mx.Days, date)
		sort.Sort(sort.Reverse(sort.StringSlice(mx.Days)))
	}
	mx.LiveProjectionApplied = true
	mx.LiveProjectionThrough = projection.Through
	mx.FinalizedThrough = projection.FinalizedThrough
	return true
}

func applyUsageStatsLiveProjection(st *UsageStats, projection *usageLiveProjection) bool {
	if st == nil || projection == nil || len(projection.TodayNetByUser) == 0 {
		return false
	}
	target := int64(0)
	for _, net := range projection.TodayNetByUser {
		target += net
	}
	date := time.Unix(projection.DayTs, 0).In(usageCST).Format("2006-01-02")
	dailyIndex := -1
	for i := range st.Daily {
		if st.Daily[i].Date == date {
			dailyIndex = i
			break
		}
	}
	current := UsageDaily{Date: date}
	if dailyIndex >= 0 {
		current = st.Daily[dailyIndex]
	}
	current.finalize()
	if target < current.NetQuota {
		return false
	}
	delta := target - current.NetQuota
	if delta == 0 {
		st.LiveProjectionApplied = true
		st.LiveProjectionThrough = projection.Through
		st.FinalizedThrough = projection.FinalizedThrough
		return true
	}
	current.ConsumeQuota += delta
	current.finalize()
	if dailyIndex >= 0 {
		st.Daily[dailyIndex] = current
	} else {
		st.Daily = append(st.Daily, current)
		sort.Slice(st.Daily, func(i, j int) bool { return st.Daily[i].Date < st.Daily[j].Date })
	}
	st.Summary.ConsumeQuota += delta
	st.Summary.finalize()
	projectionDim := UsageDim{Key: usageLiveProjectionDimension}
	projectionDim.ConsumeQuota = delta
	projectionDim.finalize()
	st.ByGroup = append(st.ByGroup, projectionDim)
	st.ByModel = append(st.ByModel, projectionDim)
	projectionModel := UsageDailyModel{Date: date, Model: usageLiveProjectionDimension}
	projectionModel.ConsumeQuota = delta
	projectionModel.finalize()
	st.DailyByModel = append(st.DailyByModel, projectionModel)
	st.LiveProjectionApplied = true
	st.LiveProjectionThrough = projection.Through
	st.FinalizedThrough = projection.FinalizedThrough
	return true
}

func (m *Monitor) projectUsageMatrixIfSafe(ctx context.Context, mx *UsageMatrix, ids []int64, fromTs, toTs int64, now time.Time) {
	projection, err := m.loadUsageLiveProjection(ctx, ids, fromTs, toTs, now)
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("读取今日用量暂定水位失败，继续使用已封口事实", "err", err)
		return
	}
	if projection != nil {
		_ = applyUsageMatrixLiveProjection(mx, projection)
	}
}

func (m *Monitor) projectUsageStatsIfSafe(ctx context.Context, st *UsageStats, ids []int64, fromTs, toTs int64, tokenID int64, now time.Time) {
	if tokenID > 0 { // 累计用户水位无法安全拆到单个 Token。
		return
	}
	projection, err := m.loadUsageLiveProjection(ctx, ids, fromTs, toTs, now)
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("读取今日用量暂定水位失败，继续使用已封口事实", "err", err)
		return
	}
	if projection != nil && !applyUsageStatsLiveProjection(st, projection) {
		slog.Warn("今日用量暂定水位落后于已封口事实，已拒绝覆盖", "finalized_through", projection.FinalizedThrough)
	}
}

func usageLiveProjectionMessage(through, finalizedThrough int64) string {
	if through <= 0 || finalizedThrough <= 0 {
		return ""
	}
	live := time.Unix(through, 0).In(usageCST).Format("15:04")
	finalized := time.Unix(finalizedThrough, 0).In(usageCST).Format("15:04")
	return "今日总消费已更新至 " + live + "；模型、渠道和 Token 明细已封口至 " + finalized
}
