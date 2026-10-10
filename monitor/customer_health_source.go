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

	"gorm.io/gorm"
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
	// 内低频重试，成功追平后仍按采样周期重放最近窗口，持续推进水位。
	customerHealthPolicyBackfillRetryDelay = time.Minute
)

func customerHealthSourceRange(now time.Time) (int64, int64) {
	local := now.In(cstLocation)
	from := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, cstLocation).Unix()
	through := customerHealthSourceFinalizedThrough(now)
	if through < from {
		through = from
	}
	return from, through
}

func customerHealthSourceFinalizedThrough(now time.Time) int64 {
	return now.Add(-customerHealthSourceFinalizeDelay).Unix() / 60 * 60
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
	m.customerHealthCursorMu.Lock()
	defer m.customerHealthCursorMu.Unlock()
	return m.saveCustomerHealthSourceCursorLocked(dayStart, through)
}

func (m *Monitor) saveCustomerHealthSourceCursorLocked(dayStart, through int64) error {
	// 同一天的可信持久水位只增不减。系统时间短暂回拨时，页面可以把本轮可见
	// 右界夹到当前 target，但不能让随后较小的分片覆盖已经落盘的更高水位。
	// The standard source worker has both realtime and policy-backfill writers.
	// The caller holds customerHealthCursorMu across this read/merge/write.
	var current CustomerHealthSourceCursor
	tx := m.storeDB.Where("id = ?", 1).Limit(1).Find(&current)
	if tx.Error != nil {
		return tx.Error
	}
	hadCurrent := tx.RowsAffected > 0
	// A delayed callback from the previous CST day must never replace a newer
	// day's durable cursor.  This can happen around midnight when the realtime
	// sampler and the policy backfill finish overlapping work in different
	// goroutines.  Same-day writes are merged below; a genuinely newer day is
	// the only writer allowed to replace an older day.
	if tx.RowsAffected > 0 && current.DayTs > dayStart {
		return nil
	}
	currentMainSemantics := tx.RowsAffected > 0 && current.DayTs == dayStart &&
		current.SemanticsVersion == customerHealthStabilityPolicyVersion
	currentTTFTSemantics := currentMainSemantics && current.TTFTSemanticsVersion == ttftCoverageSemanticsVersion
	if currentTTFTSemantics && current.TTFTCoverageFromTs == dayStart &&
		current.ThroughTs >= through &&
		current.TTFTCoverageThroughTs >= through {
		// Existing databases may predate the per-day ledger.  Even a no-op
		// cursor update must materialize its already proven day certificate.
		return m.storeDB.Transaction(func(tx *gorm.DB) error {
			return m.persistCustomerHealthDayCoverageTx(tx, current)
		})
	}
	mainThrough := through
	// Request-fact coverage is independent of the FRT/TTFT projection.  A
	// legacy TTFT version must trigger an FRT replay, but it must not rewind a
	// request watermark that was already proven complete.
	if currentMainSemantics && current.ThroughTs > mainThrough {
		mainThrough = current.ThroughTs
	}
	ttftFrom, ttftThrough := dayStart, through
	if currentTTFTSemantics {
		validCurrentTTFTRange := current.TTFTCoverageFromTs == dayStart &&
			current.TTFTCoverageThroughTs >= dayStart
		if validCurrentTTFTRange && current.TTFTCoverageFromTs <= through {
			ttftFrom = current.TTFTCoverageFromTs
		}
		if validCurrentTTFTRange && current.TTFTCoverageThroughTs > ttftThrough {
			ttftThrough = current.TTFTCoverageThroughTs
		}
	}
	next := CustomerHealthSourceCursor{
		ID: 1, DayTs: dayStart, ThroughTs: mainThrough,
		SemanticsVersion:     customerHealthStabilityPolicyVersion,
		TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
		TTFTCoverageFromTs:   ttftFrom, TTFTCoverageThroughTs: ttftThrough,
		UpdatedAt: time.Now().Unix(),
	}
	return m.storeDB.Transaction(func(tx *gorm.DB) error {
		// A single-row cursor is about to move to a newer day.  Preserve the
		// outgoing day's certified interval before replacing that row, in the
		// same SQLite transaction as the incoming day's certificate.
		if hadCurrent && current.DayTs > 0 && current.DayTs < dayStart {
			if err := m.persistCustomerHealthDayCoverageTx(tx, current); err != nil {
				return err
			}
		}
		if err := tx.Save(&next).Error; err != nil {
			return err
		}
		return m.persistCustomerHealthDayCoverageTx(tx, next)
	})
}

// publishCustomerHealthSourceThrough advances the in-memory watermark
// monotonically within one day.  The policy backfill and realtime sampler can
// both publish the same durable cursor; a late completion of an older slice
// must not overwrite the higher value already published by the other lane.
// The day transition is explicit through customerHealthSourceFrom, so a new
// CST day is allowed to reset the right edge to that day's start.
func (m *Monitor) publishCustomerHealthSourceThrough(dayStart, through int64) {
	if m == nil {
		return
	}
	if m.customerHealthSourceFrom.Load() != dayStart {
		// Publish the new day's right edge first.  A status reader that races the
		// transition will still see the old FromTs and therefore remain
		// incomplete; publishing FromTs first could pair the new day with the
		// previous day's larger ThroughTs for one read.
		m.customerHealthSourceThrough.Store(through)
		m.customerHealthSourceFrom.Store(dayStart)
		return
	}
	for {
		current := m.customerHealthSourceThrough.Load()
		if through <= current {
			return
		}
		if m.customerHealthSourceThrough.CompareAndSwap(current, through) {
			return
		}
	}
}

func (m *Monitor) resetCustomerHealthSourceWatermark(dayStart, through int64) {
	if m == nil {
		return
	}
	// See publishCustomerHealthSourceThrough: right edge first prevents a
	// transient cross-day pair from being reported as complete.
	m.customerHealthSourceThrough.Store(through)
	m.customerHealthSourceFrom.Store(dayStart)
}

// advanceCustomerHealthSourceCursorFromRealtime publishes the standard
// source_worker's successful live sample into the customer-maintenance cursor.
// The policy backfill owns large historical gaps; this small overlapping update
// keeps the durable [dayStart, through) proof moving after the backfill catches
// up, without ever bridging a disjoint outage window.
func (m *Monitor) advanceCustomerHealthSourceCursorFromRealtime(from, to int64) error {
	if m == nil || m.storeDB == nil || to <= from || !m.cfg.CapacityEnabled || m.cfg.CustomerHealthSourceEnabled {
		return nil
	}
	now := time.Now()
	dayStart, target := customerHealthSourceRange(now)
	through := to
	if through > target {
		through = target
	}
	if through <= dayStart {
		return nil
	}
	m.customerHealthCursorMu.Lock()
	defer m.customerHealthCursorMu.Unlock()
	var state CustomerHealthSourceCursor
	tx := m.storeDB.Where("id = ?", 1).Limit(1).Find(&state)
	if tx.Error != nil {
		return tx.Error
	}
	// A missing/legacy cursor is deliberately left to the policy backfill.  In
	// particular, a live window must not create a false day-start proof.
	if tx.RowsAffected == 0 || state.DayTs != dayStart ||
		state.SemanticsVersion != customerHealthStabilityPolicyVersion ||
		state.ThroughTs < dayStart {
		return nil
	}
	// The successful range must overlap the proven prefix.  If the worker was
	// offline long enough to leave a gap, retain the old watermark until the
	// contiguous backfill repairs that gap.
	if from > state.ThroughTs || through <= state.ThroughTs {
		return nil
	}
	// When FRT history is behind, the same successful live slice still proves
	// *request* facts. Publish request through_ts alone; extending the FRT
	// watermark here would bridge an unsampled historical FRT gap.
	if state.TTFTSemanticsVersion != ttftCoverageSemanticsVersion ||
		state.TTFTCoverageFromTs != dayStart || state.TTFTCoverageThroughTs < state.ThroughTs {
		state.ThroughTs = through
		state.UpdatedAt = time.Now().Unix()
		return m.storeDB.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&CustomerHealthSourceCursor{}).Where("id = ?", 1).Updates(map[string]any{
				"through_ts": through, "updated_at": state.UpdatedAt,
			}).Error; err != nil {
				return err
			}
			return m.persistCustomerHealthDayCoverageTx(tx, state)
		})
	}
	return m.saveCustomerHealthSourceCursorLocked(dayStart, through)
}

func (m *Monitor) loadCustomerHealthSourceCursor(dayStart, target int64) (cursor, coveredThrough int64, err error) {
	m.customerHealthCursorMu.Lock()
	defer m.customerHealthCursorMu.Unlock()
	return m.loadCustomerHealthSourceCursorLocked(dayStart, target)
}

func (m *Monitor) loadCustomerHealthSourceCursorLocked(dayStart, target int64) (cursor, coveredThrough int64, err error) {
	var state CustomerHealthSourceCursor
	tx := m.storeDB.Where("id = ?", 1).Limit(1).Find(&state)
	if tx.Error != nil {
		return 0, 0, tx.Error
	}
	if tx.RowsAffected == 0 || state.DayTs != dayStart || state.ThroughTs < dayStart ||
		state.SemanticsVersion != customerHealthStabilityPolicyVersion {
		if err := m.saveCustomerHealthSourceCursorLocked(dayStart, dayStart); err != nil {
			return 0, 0, err
		}
		return dayStart, dayStart, nil
	}
	coveredThrough = state.ThroughTs
	// A pre-TTFT cursor, a cursor with an empty TTFT watermark, or any legacy
	// capacity row in this day invalidates TTFT coverage. Rewind the same
	// source lane so the historical rows are replaced from logs.
	var legacyTTFTRowsInProof int64
	legacyProofEnd := min(target, state.TTFTCoverageThroughTs)
	if legacyProofEnd < dayStart {
		legacyProofEnd = dayStart
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).
		Where("bucket_ts >= ? AND bucket_ts < ? AND COALESCE(ttft_semantics_version,0) <> ?", dayStart, legacyProofEnd, ttftCoverageSemanticsVersion).
		Count(&legacyTTFTRowsInProof).Error; err != nil {
		return 0, 0, err
	}
	ttftNeedsReplay := state.TTFTSemanticsVersion != ttftCoverageSemanticsVersion ||
		state.TTFTCoverageFromTs != dayStart || state.TTFTCoverageThroughTs < dayStart ||
		legacyTTFTRowsInProof > 0
	if ttftNeedsReplay {
		state.TTFTSemanticsVersion = ttftCoverageSemanticsVersion
		state.TTFTCoverageFromTs = dayStart
		state.TTFTCoverageThroughTs = dayStart
		state.UpdatedAt = time.Now().Unix()
		coveredThrough = dayStart
		if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&CustomerHealthSourceCursor{}).Where("id = ?", 1).Updates(map[string]any{
				"ttft_semantics_version":   ttftCoverageSemanticsVersion,
				"ttft_coverage_from_ts":    dayStart,
				"ttft_coverage_through_ts": dayStart,
				"updated_at":               state.UpdatedAt,
			}).Error; err != nil {
				return err
			}
			return m.persistCustomerHealthDayCoverageTx(tx, state)
		}); err != nil {
			return 0, 0, err
		}
	} else if state.TTFTCoverageThroughTs < coveredThrough {
		// A partially replayed FRT prefix is still valid.  Resume from its own
		// durable through_ts (with the normal small overlap), not from midnight
		// on every retry/restart merely because request facts are further ahead.
		coveredThrough = state.TTFTCoverageThroughTs
	}
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
	cursor, _, err := m.loadCustomerHealthSourceCursor(dayStart, currentDay)
	if err != nil {
		return err
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

// customerHealthCollectionStatus reports the *proven* local coverage for the
// customer-maintenance report.  A sampler heartbeat only says that one recent
// query succeeded; it cannot prove that every minute from midnight through
// the report target was sampled.  The customer-health cursor is advanced only
// after each contiguous slice has been written, so it is the source of truth
// for both the standard source worker and the logchain-only lane.
//
// targetTs is passed by the caller (rather than calculated again here) so the
// readiness decision and the query window use the same instant. Ready only
// means caught up to target; MetricsAvailable separately certifies a nonempty
// continuous prefix. Falling behind by a minute never erases that prefix.
func (m *Monitor) customerHealthCollectionStatus(dayStart, targetTs int64) CustomerHealthCollectionStatus {
	status := CustomerHealthCollectionStatus{TargetTs: targetTs}
	if m.cfg.CustomerHealthSourceEnabled {
		status.Mode, status.Enabled = "logchain_only", true
		status.Running = m.customerHealthSourceRunning.Load()
		status.LastSuccessAt = m.customerHealthSourceLastSuccess.Load()
		status.LastFailureAt = m.customerHealthSourceLastFailure.Load()
	} else if m.cfg.sourceWorkerIsEnabled() && m.cfg.CapacityEnabled {
		status.Mode, status.Enabled = "source_worker", true
		status.Running = m.sourceWorkerRunning.Load()
		status.LastSuccessAt = m.LastSampleRun()
		status.LastFailureAt = m.sourceLastFailureAt.Load()
	} else {
		status.Mode, status.State = "disabled", "source_unavailable"
		status.Note = "客户维护本地采集未开启，指标暂不可用"
		return status
	}
	// Both lanes use the durable certificate. In-memory watermarks can be
	// empty just after restart, or be reset by an independent FRT replay. A
	// recent heartbeat and sparse fact rows cannot replace this proof.
	status.FromTs = dayStart
	var cursor CustomerHealthSourceCursor
	if m.storeDB == nil {
		status.State, status.Note = "source_unavailable", "本地事实库不可用，无法验证指标覆盖"
		return status
	}
	tx := m.storeDB.Where("id = ?", 1).Limit(1).Find(&cursor)
	switch {
	case tx.Error != nil:
		status.State, status.Note = "source_unavailable", "本地覆盖证明读取失败，指标暂不可用"
		return status
	case tx.RowsAffected == 0 || cursor.DayTs < dayStart:
		status.State, status.Note = "initial_backfill", "今日首次回算中，尚未建立从 00:00 起的连续覆盖"
		if status.LastFailureAt > status.LastSuccessAt {
			status.State, status.Note = "source_unavailable", "来源采集失败，尚无今日连续数据可展示"
		}
		return status
	case cursor.SemanticsVersion != customerHealthStabilityPolicyVersion:
		status.State, status.Note = "policy_backfill", "统计口径正在更新，旧口径覆盖证明不能用于当前指标"
		return status
	case cursor.DayTs != dayStart || cursor.ThroughTs < dayStart || cursor.ThroughTs > dayStart+24*3600:
		status.State, status.Note = "coverage_gap", "今日连续覆盖存在缺口，指标暂不可用"
		return status
	}
	status.ThroughTs = min(cursor.ThroughTs, targetTs)
	status.LastSuccessAt = max(status.LastSuccessAt, cursor.UpdatedAt)
	if status.ThroughTs <= dayStart {
		status.State, status.Note = "initial_backfill", "今日首次回算中，尚无已确认的完整分钟"
		if status.LastFailureAt > status.LastSuccessAt {
			status.State, status.Note = "source_unavailable", "来源采集失败，尚无今日连续数据可展示"
		}
		return status
	}
	status.MetricsAvailable, status.CoverageComplete = true, true
	status.Ready = status.ThroughTs >= targetTs
	status.State = "syncing"
	status.Note = "截至 " + time.Unix(status.ThroughTs, 0).In(cstLocation).Format("15:04") +
		"（CST），尾部同步中；仅统计今日 00:00 起已连续覆盖的数据"
	if status.Ready {
		status.State = "ready"
		status.Note += "（已追平定稿目标）"
	}
	if status.LastFailureAt > status.LastSuccessAt {
		status.State = "source_failed"
		status.Note = "采集失败；截至 " + time.Unix(status.ThroughTs, 0).In(cstLocation).Format("15:04") +
			"（CST），仅展示今日 00:00 起已确认的连续数据，不代表实时完整数据"
	} else if !status.Running {
		status.State = "source_paused"
		status.Note = "采集器已暂停；截至 " + time.Unix(status.ThroughTs, 0).In(cstLocation).Format("15:04") +
			"（CST），仅展示今日 00:00 起已确认的连续数据，不代表实时完整数据"
	}
	return status
}

// backfillCustomerHealthPolicyToday 是普通完整来源 worker 的低优先级连续
// 客户维护 lane。普通 worker 的实时采样只覆盖短尾窗；如果这里只在 epoch
// 启动时回算一次，启动后新封口的分钟不会推进 CustomerHealthSourceCursor，
// 客户维护会永远停在启动时的 through_ts。每轮完成当天连续回算后等待一个
// 采样周期，再从持久化水位回放最近窗口并继续推进；source epoch 取消时随之
// 退出，下一次 lease/epoch 会从 durable cursor 无缝续跑。
func (m *Monitor) backfillCustomerHealthPolicyToday(ctx context.Context) {
	for {
		// 午夜时 durable cursor 仍指向上一个 CST 自然日。先等待两分钟延迟
		// 封口，再让下一轮切换到今天；否则 00:00 附近重启或慢轮询可能让
		// 上一个自然日最后几分钟永久没有覆盖证明。
		currentDay, _ := customerHealthSourceRange(time.Now())
		if err := m.finishPreviousCustomerHealthDay(ctx, currentDay); err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			slog.Warn("客户维护上一自然日封口未完成，将在当前来源周期重试",
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
			continue
		}
		err := m.backfillCustomerHealthPolicyTodayWith(ctx, time.Now(), m.sampleCustomerHealthRangeLow)
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return
		}
		wait := m.customerHealthSourcePollEvery()
		if err != nil {
			// 查询失败时保留现有连续水位，并使用较慢的重试节奏；成功
			// 追平后只需按正常采样周期重读最近窗口接住迟到日志。
			slog.Warn("客户维护新责任方口径回算未完成，将在当前来源周期重试",
				"retry_in", customerHealthPolicyBackfillRetryDelay, "err", err)
			wait = customerHealthPolicyBackfillRetryDelay
		}
		timer := time.NewTimer(wait)
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
	m.customerHealthSourceTarget.Store(target)
	cursor, coveredThrough, err := m.loadCustomerHealthSourceCursor(dayStart, target)
	if err != nil {
		return err
	}
	m.resetCustomerHealthSourceWatermark(dayStart, coveredThrough)
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
			m.publishCustomerHealthSourceThrough(dayStart, coveredThrough)
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
	m.resetCustomerHealthSourceWatermark(dayStart, coveredThrough)
	m.customerHealthSourceTarget.Store(target)

	for {
		if err := ctx.Err(); err != nil {
			return
		}
		iterationNow := time.Now()
		currentDay, currentTarget := customerHealthSourceRange(iterationNow)
		// The current-day target is clamped to today's midnight during the
		// first two minutes. Use the raw finalized target while closing the
		// previous day, or its unfinalized tail would be signed prematurely.
		transitionTarget := currentTarget
		if currentDay != dayStart {
			transitionTarget = customerHealthSourceFinalizedThrough(iterationNow)
		}
		switchDay, previousDayTarget := customerHealthSourceDayTransition(dayStart, currentDay, transitionTarget, cursor)
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
			m.resetCustomerHealthSourceWatermark(dayStart, dayStart)
			m.customerHealthSourceTarget.Store(target)
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
		m.customerHealthSourceTarget.Store(target)

		if cursor >= target {
			m.customerHealthSourceLastSuccess.Store(time.Now().Unix())
			// Today's closed minutes always have priority.  Once caught up,
			// process at most one historical hour, then recheck the moving today
			// target before taking another historical slice.
			worked, historyErr := m.runCustomerHealthHistoryTurnWith(ctx, time.Now(), m.sampleCustomerHealthRangeLow)
			if historyErr != nil {
				if errors.Is(historyErr, context.Canceled) || ctx.Err() != nil {
					return
				}
				slog.Warn("客户维护历史日回放失败(保留水位重试)", "err", historyErr)
				if !waitSourceLifecycle(ctx, 15*time.Second) {
					return
				}
				continue
			}
			if worked {
				if !waitSourceLifecycle(ctx, 2*time.Second) {
					return
				}
				continue
			}
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
			m.publishCustomerHealthSourceThrough(dayStart, coveredThrough)
		}
		cursor = to
		m.customerHealthSourceLastSuccess.Store(time.Now().Unix())
		slog.Info("客户维护独立只读采集完成分片", "from", sliceFrom, "to", to, "rows", rows)
	}
}
