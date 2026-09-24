package monitor

// customer_health_source.go：8204/logchain-only 的客户维护只读采集通道。
//
// 完整来源 worker 需要 logs/channels/users/tokens/options 五张表；8204 的最小权限
// 账号只有 logs/channels。为了让客户维护能看到真实请求、稳定率、归因和金额，
// 本通道只读 logs(type=2/5/6)，并把分钟聚合写进 Monitor 自己的 SQLite。
// 它不读取用户/令牌表、不启动 Usage Facts、不调用 NewAPI 写接口。

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

const (
	// 单次最多读一个小时；SQL 自带 MAX_EXECUTION_TIME(8000)，并走后台低优先级
	// 单槽与 duty 限速，不能因为本地演示挤占客户排障的交互查询。
	customerHealthSourceSliceSeconds = int64(3600)
	// 每轮重读最近 20 分钟，接住长请求结束后才落库及短暂写入延迟。upsert 使重读幂等。
	customerHealthSourceReplaySeconds = int64(20 * 60)
	// 当前未结束的分钟不宣称完整；右水位固定落后两分钟。
	customerHealthSourceFinalizeDelay = 2 * time.Minute
	// 标准完整来源部署新口径时需要回算当天。启动瞬间的低优先级闸门繁忙或
	// 来源短抖动不能让页面一直 fail-closed 到次日；失败后在同一来源 epoch
	// 内低频重试，成功即退出。
	customerHealthPolicyBackfillRetryDelay = time.Minute
)

func customerHealthSourceRange(now time.Time) (int64, int64) {
	local := now.In(cstLocation)
	from := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, cstLocation).Unix()
	through := now.Add(-customerHealthSourceFinalizeDelay).Unix() / 60 * 60
	if through < from {
		through = from
	}
	return from, through
}

func customerHealthSourceNext(from, through int64) int64 {
	next := from + customerHealthSourceSliceSeconds
	if next > through {
		return through
	}
	return next
}

// customerHealthSourceDayTransition decides whether a source loop may switch
// to the next CST day.  The previous day remains active until its finalized
// watermark reaches the natural day end; this is what closes the delayed tail
// that becomes visible just after midnight.
func customerHealthSourceDayTransition(dayStart, currentDay, currentTarget, cursor int64) (switchDay bool, target int64) {
	if currentDay == dayStart {
		return false, currentTarget
	}
	dayEnd := dayStart + 24*3600
	if cursor >= dayEnd && currentTarget >= dayEnd {
		return true, currentTarget
	}
	if currentTarget > dayEnd {
		currentTarget = dayEnd
	}
	return false, currentTarget
}

func (m *Monitor) customerHealthSourcePollEvery() time.Duration {
	d := time.Duration(m.cfg.SampleSeconds) * time.Second
	if d < 30*time.Second {
		return 30 * time.Second
	}
	return d
}

func (m *Monitor) saveCustomerHealthSourceCursor(dayStart, through int64) error {
	// 同一天的可信持久水位只增不减。系统时间短暂回拨时，页面可以把本轮可见
	// 右界夹到当前 target，但不能让随后较小的分片覆盖已经落盘的更高水位。
	// 本 cursor 只有独立客户维护 lane 一个写者，无需为这次本地比较额外占写事务。
	var current CustomerHealthSourceCursor
	tx := m.storeDB.Where("id = ?", 1).Limit(1).Find(&current)
	if tx.Error != nil {
		return tx.Error
	}
	if tx.RowsAffected > 0 && current.DayTs == dayStart &&
		current.SemanticsVersion == customerHealthStabilityPolicyVersion && current.ThroughTs >= through {
		return nil
	}
	return m.storeDB.Save(&CustomerHealthSourceCursor{
		ID: 1, DayTs: dayStart, ThroughTs: through,
		SemanticsVersion: customerHealthStabilityPolicyVersion, UpdatedAt: time.Now().Unix(),
	}).Error
}

func (m *Monitor) loadCustomerHealthSourceCursor(dayStart, target int64) (cursor, coveredThrough int64, err error) {
	var state CustomerHealthSourceCursor
	tx := m.storeDB.Where("id = ?", 1).Limit(1).Find(&state)
	if tx.Error != nil {
		return 0, 0, tx.Error
	}
	if tx.RowsAffected == 0 || state.DayTs != dayStart || state.ThroughTs < dayStart ||
		state.SemanticsVersion != customerHealthStabilityPolicyVersion {
		if err := m.saveCustomerHealthSourceCursor(dayStart, dayStart); err != nil {
			return 0, 0, err
		}
		return dayStart, dayStart, nil
	}
	coveredThrough = state.ThroughTs
	if coveredThrough > target {
		// 防系统时钟短暂回拨；不能把已持久化的可信水位写小。
		coveredThrough = target
	}
	cursor = coveredThrough - customerHealthSourceReplaySeconds
	if cursor < dayStart {
		cursor = dayStart
	}
	return cursor, coveredThrough, nil
}

// finishPreviousCustomerHealthDay closes a previous-day cursor before a
// process that starts after midnight replaces it with the new day's cursor.
// Without this startup path, a restart between 00:00 and the normal two-minute
// finalization point could permanently lose the previous day's tail.
func (m *Monitor) finishPreviousCustomerHealthDay(ctx context.Context, currentDay int64) error {
	if currentDay <= 0 {
		return nil
	}
	var state CustomerHealthSourceCursor
	tx := m.storeDB.Where("id = ?", 1).Limit(1).Find(&state)
	if tx.Error != nil || tx.RowsAffected == 0 || state.DayTs != currentDay-24*3600 {
		return tx.Error
	}
	settleAt := time.Unix(currentDay, 0).Add(customerHealthSourceFinalizeDelay)
	if wait := time.Until(settleAt); wait > 0 {
		if !waitSourceLifecycle(ctx, wait) {
			return ctx.Err()
		}
	}
	dayStart := currentDay - 24*3600
	cursor := state.ThroughTs - customerHealthSourceReplaySeconds
	if cursor < dayStart || state.SemanticsVersion != customerHealthStabilityPolicyVersion {
		cursor = dayStart
	}
	for cursor < currentDay {
		to := customerHealthSourceNext(cursor, currentDay)
		if _, err := m.sampleCustomerHealthRangeLow(ctx, cursor, to); err != nil {
			return err
		}
		if err := m.saveCustomerHealthSourceCursor(dayStart, to); err != nil {
			return err
		}
		cursor = to
	}
	return nil
}

func (m *Monitor) customerHealthCollectionStatus(dayStart int64) CustomerHealthCollectionStatus {
	if m.cfg.CustomerHealthSourceEnabled {
		from := m.customerHealthSourceFrom.Load()
		through := m.customerHealthSourceThrough.Load()
		status := CustomerHealthCollectionStatus{
			Mode: "logchain_only", Enabled: true,
			Running: m.customerHealthSourceRunning.Load(),
			FromTs:  from, ThroughTs: through,
			LastSuccessAt: m.customerHealthSourceLastSuccess.Load(),
			LastFailureAt: m.customerHealthSourceLastFailure.Load(),
		}
		status.Ready = from == dayStart && through > dayStart
		switch {
		case status.Ready:
			status.Note = "本地只读采集已连续覆盖至 " +
				time.Unix(through, 0).In(cstLocation).Format("15:04") + "（CST）"
		case status.Running:
			status.Note = "本地只读采集正在从今日 00:00 追赶，未覆盖区间不补零"
		default:
			status.Note = "本地只读采集已开启但当前未运行"
		}
		return status
	}
	if m.cfg.sourceWorkerIsEnabled() && m.cfg.CapacityEnabled {
		last := m.LastSampleRun()
		return CustomerHealthCollectionStatus{
			Mode: "source_worker", Enabled: true, Running: m.sourceWorkerRunning.Load(),
			Ready: last > dayStart, ThroughTs: last, LastSuccessAt: last,
			Note: "使用标准来源采样器的本地分钟事实",
		}
	}
	return CustomerHealthCollectionStatus{Mode: "disabled", Note: "客户维护本地采集未开启"}
}

// backfillCustomerHealthPolicyToday 是普通完整来源 worker 的一次性口径回算。
// logchain-only 有自己的连续 worker；普通 worker 的实时采样只覆盖短尾窗，若不
// 单独回算，新版本部署当天更早的分钟会一直保留旧口径，整页会 fail-closed 到次日。
func (m *Monitor) backfillCustomerHealthPolicyToday(ctx context.Context) {
	for {
		err := m.backfillCustomerHealthPolicyTodayWith(ctx, time.Now(), m.sampleCustomerHealthRangeLow)
		if err == nil || errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return
		}
		slog.Warn("客户维护新责任方口径回算未完成，将在当前来源周期重试",
			"retry_in", customerHealthPolicyBackfillRetryDelay, "err", err)
		timer := time.NewTimer(customerHealthPolicyBackfillRetryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
	}
}

func (m *Monitor) backfillCustomerHealthPolicyTodayWith(ctx context.Context, now time.Time,
	sample metricRangeSampler) error {
	dayStart, target := customerHealthSourceRange(now)
	cursor, coveredThrough, err := m.loadCustomerHealthSourceCursor(dayStart, target)
	if err != nil {
		return err
	}
	for cursor < target {
		to := customerHealthSourceNext(cursor, target)
		if _, err := sample(ctx, cursor, to); err != nil {
			return err
		}
		if to > coveredThrough {
			coveredThrough = to
			if err := m.saveCustomerHealthSourceCursor(dayStart, coveredThrough); err != nil {
				return err
			}
		}
		cursor = to
	}
	return nil
}

// startCustomerHealthSource 只会由 logchain-only source epoch 启动。
//
// 水位只在连续区间成功后向右推进。查询失败保留原水位重试；页面因此可以明确
// 区分“已证明覆盖到几点”和“采集仍在追赶”，不会把缺口悄悄补成零。
func (m *Monitor) startCustomerHealthSource(ctx context.Context) {
	if !m.cfg.CustomerHealthSourceEnabled || m.prodDB == nil {
		return
	}
	m.customerHealthSourceRunning.Store(true)
	defer m.customerHealthSourceRunning.Store(false)

	dayStart, target := customerHealthSourceRange(time.Now())
	if err := m.finishPreviousCustomerHealthDay(ctx, dayStart); err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return
		}
		m.customerHealthSourceLastFailure.Store(time.Now().Unix())
		slog.Warn("启动时封口上一自然日客户维护采集失败", "day_start", dayStart, "err", err)
		return
	}
	cursor, coveredThrough, err := m.loadCustomerHealthSourceCursor(dayStart, target)
	if err != nil {
		m.customerHealthSourceLastFailure.Store(time.Now().Unix())
		slog.Error("初始化客户维护独立采集水位失败", "err", err)
		return
	}
	m.customerHealthSourceFrom.Store(dayStart)
	m.customerHealthSourceThrough.Store(coveredThrough)

	for {
		if err := ctx.Err(); err != nil {
			return
		}
		currentDay, currentTarget := customerHealthSourceRange(time.Now())
		switchDay, previousDayTarget := customerHealthSourceDayTransition(dayStart, currentDay, currentTarget, cursor)
		if switchDay {
			if err := m.saveCustomerHealthSourceCursor(currentDay, currentDay); err != nil {
				m.customerHealthSourceLastFailure.Store(time.Now().Unix())
				slog.Warn("切换客户维护独立采集自然日失败", "day_start", currentDay, "err", err)
				if !waitSourceLifecycle(ctx, 15*time.Second) {
					return
				}
				continue
			}
			dayStart, target = currentDay, currentTarget
			cursor, coveredThrough = dayStart, dayStart
			m.customerHealthSourceFrom.Store(dayStart)
			m.customerHealthSourceThrough.Store(dayStart)
		} else if currentDay != dayStart {
			// Do not discard the old-day cursor at midnight.  The normal two-minute
			// finalization delay means the last minutes of the previous day become
			// queryable only after midnight.  Keep collecting that day until its
			// natural end, then start the new day on the next loop.  Otherwise a
			// restart or a slow poll at 00:00 would leave a permanent tail gap.
			// Keep the old day active.  The helper clamps the target to its end
			// so the source never reads a new-day minute into the old cursor.
			target = previousDayTarget
		} else {
			target = currentTarget
		}

		if cursor >= target {
			m.customerHealthSourceLastSuccess.Store(time.Now().Unix())
			if !waitSourceLifecycle(ctx, m.customerHealthSourcePollEvery()) {
				return
			}
			// 重放最近窗口以接住迟到日志；覆盖水位不会因此倒退。
			cursor = coveredThrough - customerHealthSourceReplaySeconds
			if cursor < dayStart {
				cursor = dayStart
			}
			continue
		}

		sliceFrom := cursor
		to := customerHealthSourceNext(sliceFrom, target)
		rows, err := m.sampleCustomerHealthRangeLow(ctx, sliceFrom, to)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, errSourceNotReady) {
				return
			}
			m.customerHealthSourceLastFailure.Store(time.Now().Unix())
			slog.Warn("客户维护独立只读采集失败(保留水位重试)",
				"from", sliceFrom, "to", to, "err", err)
			if !waitSourceLifecycle(ctx, 15*time.Second) {
				return
			}
			continue
		}

		if to > coveredThrough {
			// 水位必须在事实已落库之后持久化。若这里失败，保留旧水位并重读该片；
			// upsert 使重复读取不会重复累计。
			if err := m.saveCustomerHealthSourceCursor(dayStart, to); err != nil {
				m.customerHealthSourceLastFailure.Store(time.Now().Unix())
				slog.Warn("客户维护独立采集水位持久化失败(保留旧水位重试)",
					"from", sliceFrom, "to", to, "err", err)
				if !waitSourceLifecycle(ctx, 15*time.Second) {
					return
				}
				continue
			}
			coveredThrough = to
			m.customerHealthSourceFrom.Store(dayStart)
			m.customerHealthSourceThrough.Store(coveredThrough)
		}
		cursor = to
		m.customerHealthSourceLastSuccess.Store(time.Now().Unix())
		slog.Info("客户维护独立只读采集完成分片", "from", sliceFrom, "to", to, "rows", rows)
	}
}
