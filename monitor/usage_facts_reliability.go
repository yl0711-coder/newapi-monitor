package monitor

// usage_facts_reliability.go 只负责后台同步的守护与低频复核。它不参与
// 页面查询，不改变前台读源，也不在一次周期中查询多个历史小时。

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"
)

type usageFactsLoopExit struct {
	panicValue any
	panicStack []byte
}

func (m *Monitor) superviseUsageFactsSync(ctx context.Context) {
	for ctx.Err() == nil {
		exit := m.runUsageFactsSyncSafely(ctx)
		if ctx.Err() != nil {
			return
		}
		m.usageFactsRestarts.Add(1)
		if exit.panicValue != nil {
			slog.Error("用量事实同步异常退出，将自动重启", "panic", fmt.Sprint(exit.panicValue), "stack", string(exit.panicStack))
		} else {
			slog.Error("用量事实同步意外退出，将自动重启")
		}
		if !waitUsageFact(ctx, 5*time.Second) {
			return
		}
	}
}

func (m *Monitor) runUsageFactsSyncSafely(ctx context.Context) (exit usageFactsLoopExit) {
	defer func() {
		if panicValue := recover(); panicValue != nil {
			exit.panicValue = panicValue
			exit.panicStack = debug.Stack()
		}
	}()
	m.runUsageFactsSync(ctx)
	return exit
}

// reconcileNextUsageFactHour 只在首次回填已追上尾部后运行；每次只复核
// 一个距当前至少 3 小时、且仍保留小时事实的近期小时。更早历史已经压缩
// 为日事实，不做无边界回扫；这样既能发现常见的晚到/事后修订，也不会
// 让 366 天历史复核长时间占用来源 logs。
func (m *Monitor) reconcileNextUsageFactHour(ctx context.Context, now time.Time) (bool, error) {
	ids, err := m.trackedIDsForUsageFacts()
	if err != nil || len(ids) == 0 {
		return false, err
	}
	end := m.usageFactFinalizedHour(now)
	candidateStart := end - int64(m.usageFactBackfillDays())*usageFactDaySeconds
	if err := m.ensureUsageFactMembershipAt(ids, candidateStart); err != nil {
		return false, err
	}
	var state UsageFactSyncState
	if err := m.usageFactsStore().First(&state, 1).Error; err != nil {
		return false, err
	}
	if state.NextBackfillHour < end || state.PublishedAt <= 0 || state.PublishedThrough <= state.PublishedRangeStart {
		return false, nil
	}
	start := end - int64(m.usageFactHourRetentionDays())*usageFactDaySeconds
	if state.PublishedRangeStart > start {
		start = state.PublishedRangeStart
	}
	safeEnd := end - 3*usageFactHourSeconds
	if safeEnd <= start {
		return false, nil
	}
	hour := state.NextReconcileHour
	if hour < start || hour >= safeEnd {
		hour = start
	}
	next := hour + usageFactHourSeconds
	if next >= safeEnd {
		next = start
	}
	result, syncErr := m.syncUsageFactHourBatchedWithOptions(ctx, hour, ids, usageFactHourSyncOptions{
		recordFailure:      false,
		updateLastFactSync: false,
	})
	if errors.Is(syncErr, context.Canceled) || errors.Is(syncErr, errUsageFactSourceBusy) {
		return false, syncErr
	}
	nowUnix := now.Unix()
	updates := map[string]any{"next_reconcile_hour": next}
	if syncErr != nil {
		updates["last_reconcile_failure_at"] = nowUnix
		if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Updates(updates).Error; err != nil {
			return true, err
		}
		return true, syncErr
	}
	updates["last_reconciled_hour"] = hour
	updates["last_reconcile_at"] = nowUnix
	updates["last_reconcile_failure_at"] = int64(0)
	if result.Changed && result.HadPriorFingerprint {
		updates["reconcile_corrections"] = state.ReconcileCorrections + 1
	}
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Updates(updates).Error; err != nil {
		return true, err
	}
	return true, nil
}
