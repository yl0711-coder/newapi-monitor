package monitor

// A manual check can be valid without doing any work. Keep that distinct from
// a successful upstream read, without clearing authentication isolation or
// bypassing retry deadlines. This notice never changes stored health counters.
type upstreamUsageDeferredError struct {
	message string
	retryAt int64
	warning bool
}

func (e *upstreamUsageDeferredError) Error() string { return e.message }

func deferredUpstreamUsageSync(row ChannelUpstreamAccount, now int64) error {
	notice := &upstreamUsageDeferredError{
		message: "本次未执行消费同步：尚未到下次同步时间，当前仍展示已采集账单。",
	}
	if row.UsageStatus == upstreamStatusReconnect || row.UsageNextSyncAt == upstreamAccountIsolatedUntil {
		notice.message = "本次未执行消费同步：上游日志认证或权限异常，自动同步已暂停。请先检测日志访问权限，必要时更新凭据。"
		notice.warning = true
		return notice
	}
	if row.UsageStatus == upstreamStatusError {
		notice.message = "本次未执行消费同步：上次同步失败，正在等待重试时间。"
		notice.warning = true
	}
	nextTimes := []int64{row.UsageNextSyncAt}
	if !row.UsageBackfillDone && row.UsageStatus != upstreamStatusError {
		nextTimes = append(nextTimes, row.UsageBackfillNextSyncAt)
	}
	for _, next := range nextTimes {
		if next > now && next < upstreamAccountIsolatedUntil && (notice.retryAt == 0 || next < notice.retryAt) {
			notice.retryAt = next
		}
	}
	return notice
}
