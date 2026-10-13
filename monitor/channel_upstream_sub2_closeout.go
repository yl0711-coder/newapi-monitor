package monitor

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Durable, human-readable task state in the existing progress column. Keep
// this prefix stable: it is a storage format, not translatable UI copy.
const sub2CloseoutProgressPrefix = "Sub2API 日账单待核验："

func hasSub2PendingCloseout(row ChannelUpstreamAccount) bool {
	return row.Provider == upstreamProviderSub2API && strings.HasPrefix(row.UsageBackfillProgress, sub2CloseoutProgressPrefix)
}

func sub2PendingCloseoutDay(row ChannelUpstreamAccount, now int64) (int64, error) {
	day := strings.TrimPrefix(row.UsageBackfillProgress, sub2CloseoutProgressPrefix)
	t, err := time.ParseInLocation("2006-01-02", day, cstLocation)
	if !hasSub2PendingCloseout(row) || err != nil || t.Format("2006-01-02") != day || t.Unix() <= 0 || t.Unix() >= cstDayStart(now) {
		return 0, fmt.Errorf("Sub2API 待核验日期状态无效，保留原账单，请检查同步状态")
	}
	return t.Unix(), nil
}

// Save BEFORE network IO, including before cancellation, request budget
// exhaustion or an authentication failure. Only this metadata column changes.
func (m *Monitor) pinSub2Closeout(ctx context.Context, row *ChannelUpstreamAccount, plan upstreamUsageSyncPlan) error {
	progress := sub2CloseoutProgressPrefix + time.Unix(plan.backfillFrom, 0).In(cstLocation).Format("2006-01-02")
	if row.UsageBackfillProgress == progress {
		return nil
	}
	if hasSub2PendingCloseout(*row) {
		return fmt.Errorf("Sub2API 原待核验日期尚未完成，不能切换窗口")
	}
	return m.setSub2CloseoutProgress(ctx, row, progress)
}

func (m *Monitor) setSub2CloseoutProgress(ctx context.Context, row *ChannelUpstreamAccount, progress string) error {
	result := m.storeDB.WithContext(ctx).Model(&ChannelUpstreamAccount{}).
		Where("domain=? AND provider=? AND COALESCE(usage_backfill_progress,'')=?", row.Domain, upstreamProviderSub2API, row.UsageBackfillProgress).
		UpdateColumn("usage_backfill_progress", progress)
	if result.Error != nil {
		return fmt.Errorf("保存 Sub2API 待核验日期失败: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("Sub2API 待核验日期已变化，请重试")
	}
	row.UsageBackfillProgress = progress
	return nil
}
