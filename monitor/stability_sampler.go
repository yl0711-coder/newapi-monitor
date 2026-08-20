package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	// 正常窗口仍维持一次有界查询；超过后转为本地游标续采，而不是丢弃整窗。
	maxStabilityProblemRowsPerRun = 5000
	stabilityProblemPageSize      = 500
	// 原始问题分钟延迟确认，覆盖 360 秒长请求及日志落库抖动。
	// 稳定性主报表不受此延迟影响；只有原始错误签名晚约 10 分钟定稿。
	stabilityProblemFinalizeDelaySec = int64(600)
	// Polling is intentionally faster than the ordinary sampler tick. Every
	// actual cold query still passes through the shared low-priority source gate,
	// global start spacing and duty-cycle cooldown, so this only removes idle
	// scheduler time and cannot create a query burst.
	stabilityProblemMigrationPollEvery = 2 * time.Second
	// The live lane uses finalized one-minute source windows.  Processing a
	// small bounded number per sampler turn lets it catch up after an outage
	// without ever retrying the old 12-minute aggregate that could repeatedly
	// hit MAX_EXECUTION_TIME.  Every window reacquires the shared source gate, so
	// global spacing and higher-priority work remain authoritative.
	stabilityProblemLiveWindowsPerTurn = 3
)

var errStabilityProblemClassificationMigrationRequired = errors.New("stability problem classification migration required")

// Gate wait exhaustion means the low lane correctly yielded to fresher work;
// it is not evidence that the selected history window is too expensive. Keep
// it distinct from a QueryContext deadline so migration attempts/span are not
// consumed before any SQL starts.
var errStabilityProblemSourceGateWait = errors.New("stability problem source gate wait exhausted")

// A live minute that already consumed a real source query is retried from its
// durable cursor after a bounded backoff.  This keeps one pathological minute
// from issuing the same 8-second query on every sampler tick or forgetting the
// protection after a process restart.
var errStabilityProblemLiveBackoff = errors.New("stability problem live retry backoff")

type stabilityProblemRawRow struct {
	ID, CreatedAt, ChannelID int64
	Model, Group, Raw        string
}

type stabilityProblemKey struct {
	bucket, channel int64
	model, group    string
	hash            string
}

// resetStaleStabilityProblemClassification is now a non-destructive startup
// gate. A classification change can affect the whole retained problem history;
// silently deleting it would create a long WAL/write lock and the ordinary Tail
// would only repopulate recent minutes. Explicit migration creates one small
// durable cursor and replaces bounded minute windows. Each replaced minute
// removes the prior version, so rollback relies on the pinned pre-migration
// SQLite snapshot rather than pretending old rows remain online.
func (m *Monitor) resetStaleStabilityProblemClassification() error {
	stale := false
	for _, table := range []string{"stability_problem_samples", "stability_problem_stages", "stability_problem_ingest_states"} {
		var exists int
		if err := m.storeDB.Raw("SELECT EXISTS(SELECT 1 FROM "+table+" WHERE COALESCE(traffic_class_version,0) <> ? LIMIT 1)",
			userTrafficClassificationVersion).Scan(&exists).Error; err != nil {
			return err
		}
		stale = stale || exists == 1
	}
	if !stale {
		return nil
	}
	now := time.Now().Unix()
	// Raw problem minutes are authoritative only after the same ten-minute
	// finalization delay used by the live lane.  Letting the cold migration end
	// at the current minute could mark a still-changing minute complete; the
	// live cursor would then skip late rows because both lanes share the durable
	// per-minute completion table.
	to := (now - stabilityProblemFinalizeDelaySec) / 60 * 60
	from := (to - int64(m.cfg.stabilityStorageDays())*86400) / 60 * 60
	if from < 0 {
		from = 0
	}
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		var state StabilityProblemClassificationMigration
		err := tx.First(&state, 1).Error
		if err == nil && state.TrafficClassVersion == userTrafficClassificationVersion {
			return nil
		}
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		state = StabilityProblemClassificationMigration{
			ID: 1, TrafficClassVersion: userTrafficClassificationVersion,
			FromTs: from, ThroughTs: to, NextTs: from, Status: "queued", CurrentSpanMinutes: 12,
			CreatedAt: now, UpdatedAt: now,
		}
		return tx.Save(&state).Error
	})
	if err != nil {
		return err
	}
	if !m.cfg.StabilityClassificationMigrationEnabled {
		// Persist the required cursor even while execution is disabled. Health and
		// status then remain honestly degraded/paused_disabled instead of hiding a
		// 181-day raw-history gap behind a successful recent Tail.
		return fmt.Errorf("%w: set MONITOR_STABILITY_CLASSIFICATION_MIGRATION_ENABLED=true only after the source pilot",
			errStabilityProblemClassificationMigrationRequired)
	}
	return nil
}

func aggregateStabilityProblemRows(rows []stabilityProblemRawRow) []StabilityProblemSample {
	agg := make(map[stabilityProblemKey]*StabilityProblemSample)
	for _, row := range rows {
		message, truncated := stabilityProblemText(row.Raw)
		hash := stabilityProblemHash("newapi", message)
		key := stabilityProblemKey{bucket: row.CreatedAt / 60 * 60, channel: row.ChannelID, model: row.Model, group: row.Group, hash: hash}
		p := agg[key]
		if p == nil {
			p = &StabilityProblemSample{
				BucketTs: key.bucket, TrafficClassVersion: userTrafficClassificationVersion,
				Source: "newapi", SignatureHash: hash,
				ChannelID: int(row.ChannelID), ModelName: row.Model, Grp: row.Group,
				Code: stabilityProblemCode(row.Raw), Message: message, Truncated: truncated,
				FirstTs: row.CreatedAt, LastTs: row.CreatedAt,
			}
			agg[key] = p
		}
		p.Count++
		if row.CreatedAt < p.FirstTs {
			p.FirstTs = row.CreatedAt
		}
		if row.CreatedAt > p.LastTs {
			p.LastTs = row.CreatedAt
		}
	}
	out := make([]StabilityProblemSample, 0, len(agg))
	for _, row := range agg {
		out = append(out, *row)
	}
	return out
}

func problemStages(rows []StabilityProblemSample) []StabilityProblemStage {
	out := make([]StabilityProblemStage, 0, len(rows))
	for _, row := range rows {
		out = append(out, StabilityProblemStage(row))
	}
	return out
}

func scanStabilityProblemRows(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]stabilityProblemRawRow, error) {
	var out []stabilityProblemRawRow
	for rows.Next() {
		var row stabilityProblemRawRow
		if err := rows.Scan(&row.ID, &row.CreatedAt, &row.ChannelID, &row.Model, &row.Group, &row.Raw); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (m *Monitor) pendingStabilityProblemStateInRange(fromTs, toTs int64) (*StabilityProblemIngestState, error) {
	var state StabilityProblemIngestState
	query := m.storeDB.Where("complete = ? AND traffic_class_version = ?", false, userTrafficClassificationVersion)
	if fromTs > 0 {
		query = query.Where("bucket_ts >= ?", fromTs)
	}
	if toTs > 0 {
		query = query.Where("bucket_ts < ?", toTs)
	}
	err := query.Order("bucket_ts ASC").First(&state).Error
	if err == nil {
		return &state, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return nil, err
}

func (m *Monitor) stabilityProblemPendingCountInRange(fromTs, toTs int64) int64 {
	var count int64
	query := m.storeDB.Model(&StabilityProblemIngestState{}).
		Where("complete = ? AND traffic_class_version = ?", false, userTrafficClassificationVersion)
	if fromTs > 0 {
		query = query.Where("bucket_ts >= ?", fromTs)
	}
	if toTs > 0 {
		query = query.Where("bucket_ts < ?", toTs)
	}
	warnReadErr("stability problem pending range", query.Count(&count))
	return count
}

func (m *Monitor) stabilityProblemPendingCount() int64 {
	var count int64
	warnReadErr("stability problem pending", m.storeDB.Model(&StabilityProblemIngestState{}).
		Where("complete = ? AND traffic_class_version = ?", false, userTrafficClassificationVersion).Count(&count))
	return count
}

func (m *Monitor) stabilityProblemNeedsCatchup(targetTo int64) bool {
	var cursor StabilityProblemLiveCursor
	if err := m.storeDB.First(&cursor, "id = ? AND traffic_class_version = ?", 1, userTrafficClassificationVersion).Error; err == nil {
		return cursor.NextTs < max(cursor.TargetThroughTs, targetTo/60*60)
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return true
	}
	var last struct{ Max int64 }
	if err := m.storeDB.Raw("SELECT COALESCE(MAX(bucket_ts),0) max FROM stability_problem_ingest_states WHERE traffic_class_version = ?",
		userTrafficClassificationVersion).Scan(&last).Error; err != nil {
		return true
	}
	return last.Max > 0 && last.Max+60 < targetTo/60*60
}

// nextUncoveredProblemWindow 返回给定范围内第一段尚未完整采集的连续分钟。
func (m *Monitor) nextUncoveredProblemWindow(fromTs, toTs int64) (int64, int64, error) {
	var rows []StabilityProblemIngestState
	if err := m.storeDB.Select("bucket_ts", "complete").Where(
		"bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ?", fromTs, toTs, userTrafficClassificationVersion).Find(&rows).Error; err != nil {
		return 0, 0, err
	}
	complete := make(map[int64]bool, len(rows))
	for _, row := range rows {
		complete[row.BucketTs] = row.Complete
	}
	start := int64(0)
	for bucket := fromTs; bucket < toTs; bucket += 60 {
		if !complete[bucket] {
			start = bucket
			break
		}
	}
	if start == 0 {
		return 0, 0, nil
	}
	end := start
	for end < toTs && !complete[end] {
		end += 60
	}
	return start, end, nil
}

func (m *Monitor) markProblemWindowComplete(tx *gorm.DB, fromTs, toTs, now int64, rows []stabilityProblemRawRow) error {
	if err := tx.Where("source = ? AND bucket_ts >= ? AND bucket_ts < ?", "newapi", fromTs, toTs).Delete(&StabilityProblemSample{}).Error; err != nil {
		return err
	}
	samples := aggregateStabilityProblemRows(rows)
	if len(samples) > 0 {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "bucket_ts"}, {Name: "source"}, {Name: "signature_hash"}, {Name: "channel_id"}, {Name: "model_name"}, {Name: "grp"}, {Name: "node"}, {Name: "path"}},
			UpdateAll: true,
		}).CreateInBatches(samples, 200).Error; err != nil {
			return err
		}
	}
	byMinute := map[int64]*StabilityProblemIngestState{}
	for bucket := fromTs; bucket < toTs; bucket += 60 {
		byMinute[bucket] = &StabilityProblemIngestState{BucketTs: bucket, TrafficClassVersion: userTrafficClassificationVersion,
			Complete: true, UpdatedAt: now, CompletedAt: now}
	}
	for _, row := range rows {
		state := byMinute[row.CreatedAt/60*60]
		if state == nil {
			continue
		}
		state.RowsScanned++
		if row.CreatedAt > state.LastCreatedAt || row.CreatedAt == state.LastCreatedAt && row.ID > state.LastID {
			state.LastCreatedAt, state.LastID = row.CreatedAt, row.ID
		}
	}
	states := make([]StabilityProblemIngestState, 0, len(byMinute))
	for _, state := range byMinute {
		states = append(states, *state)
	}
	return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "bucket_ts"}}, UpdateAll: true}).CreateInBatches(states, 200).Error
}

func (m *Monitor) ensureProblemWindowPending(fromTs, toTs, now int64) error {
	states := make([]StabilityProblemIngestState, 0, (toTs-fromTs)/60)
	for bucket := fromTs; bucket < toTs; bucket += 60 {
		states = append(states, StabilityProblemIngestState{BucketTs: bucket,
			TrafficClassVersion: userTrafficClassificationVersion, UpdatedAt: now})
	}
	if len(states) == 0 {
		return nil
	}
	return m.storeDB.Transaction(func(tx *gorm.DB) error {
		// bucket_ts is the durable cursor primary key. A pre-v5 complete row at
		// the same minute must be replaced, not ignored, otherwise a >5000-row
		// v5 probe can never create its paging cursor and repeats LIMIT 5001
		// forever. Old stages are likewise not a rollback copy; rollback uses the
		// pinned pre-migration SQLite snapshot.
		if err := tx.Where("bucket_ts >= ? AND bucket_ts < ? AND COALESCE(traffic_class_version,0) <> ?",
			fromTs, toTs, userTrafficClassificationVersion).Delete(&StabilityProblemStage{}).Error; err != nil {
			return err
		}
		return tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "bucket_ts"}},
			UpdateAll: true,
		}).CreateInBatches(states, 200).Error
	})
}

func (m *Monitor) finishProblemMinute(tx *gorm.DB, state StabilityProblemIngestState, now int64) error {
	if err := tx.Where("source = ? AND bucket_ts = ?", "newapi", state.BucketTs).Delete(&StabilityProblemSample{}).Error; err != nil {
		return err
	}
	var staged []StabilityProblemStage
	if err := tx.Where("bucket_ts = ? AND traffic_class_version = ?", state.BucketTs,
		userTrafficClassificationVersion).Find(&staged).Error; err != nil {
		return err
	}
	if len(staged) > 0 {
		final := make([]StabilityProblemSample, 0, len(staged))
		for _, row := range staged {
			final = append(final, StabilityProblemSample(row))
		}
		if err := tx.CreateInBatches(final, 200).Error; err != nil {
			return err
		}
	}
	if err := tx.Where("bucket_ts = ?", state.BucketTs).Delete(&StabilityProblemStage{}).Error; err != nil {
		return err
	}
	return tx.Model(&StabilityProblemIngestState{}).Where("bucket_ts = ?", state.BucketTs).Updates(map[string]any{
		"complete": true, "completed_at": now, "updated_at": now,
		"traffic_class_version": userTrafficClassificationVersion,
	}).Error
}

func (m *Monitor) continuePagedProblemIngest(ctx context.Context, state *StabilityProblemIngestState) (int, error) {
	if state == nil {
		return 0, nil
	}
	// Exactly one page per source-gate claim. Ten back-to-back SELECTs used to
	// bypass global start spacing and could monopolize the production source.
	limit := stabilityProblemPageSize
	rows, err := m.prodDB.QueryContext(ctx, `/*+ MAX_EXECUTION_TIME(8000) */ SELECT id, created_at, channel_id,
			COALESCE(model_name,''), COALESCE(`+"`group`"+`,''), COALESCE(content,'')
			FROM logs
			WHERE created_at >= ? AND created_at < ? AND type = 5
			  AND NOT (`+m.channelTestSourcePredicateSQL()+`)
			  AND (created_at > ? OR (created_at = ? AND id > ?))
			ORDER BY created_at, id LIMIT ?`, state.BucketTs, state.BucketTs+60,
		state.LastCreatedAt, state.LastCreatedAt, state.LastID, limit)
	if err != nil {
		m.reportSourceQueryError(err)
		return 0, err
	}
	batch, scanErr := scanStabilityProblemRows(rows)
	rows.Close()
	if scanErr != nil {
		m.reportSourceQueryError(scanErr)
		return 0, scanErr
	}
	now := time.Now().Unix()
	err = m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := upsertStabilityProblemStages(tx, problemStages(aggregateStabilityProblemRows(batch))); err != nil {
			return err
		}
		if len(batch) > 0 {
			last := batch[len(batch)-1]
			state.LastCreatedAt, state.LastID = last.CreatedAt, last.ID
			state.RowsScanned += int64(len(batch))
			state.UpdatedAt = now
			if err := tx.Save(state).Error; err != nil {
				return err
			}
		}
		if len(batch) < limit {
			return m.finishProblemMinute(tx, *state, now)
		}
		return nil
	})
	return len(batch), err
}

func (m *Monitor) stabilityProblemClassificationMigrationActive() bool {
	if !m.cfg.StabilityClassificationMigrationEnabled {
		return false
	}
	var count int64
	if err := m.storeDB.Model(&StabilityProblemClassificationMigration{}).
		Where("id = ? AND traffic_class_version = ? AND status <> ?", 1, userTrafficClassificationVersion, "complete").
		Count(&count).Error; err != nil {
		return true // fail toward the protected low-priority lane
	}
	return count > 0
}

// sampleStabilityProblemWindow executes one already-selected source window.
// Live and cold migration deliberately share the same local commit semantics,
// but not the source priority, pending cursor range or health watermark.
func (m *Monitor) sampleStabilityProblemWindow(ctx context.Context, fromTs, toTs int64, lowPriority bool) (int, error) {
	if m.prodDB == nil || !m.cfg.StabilityEnabled {
		return 0, nil
	}
	if toTs <= fromTs {
		return 0, nil
	}
	gateCtx, gateCancel := context.WithTimeout(ctx, 12*time.Second)
	var release func()
	var err error
	if lowPriority {
		release, err = m.acquireBackgroundSourceLow(gateCtx)
	} else {
		release, err = m.acquireBackgroundSource(gateCtx)
	}
	gateCancel()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return 0, fmt.Errorf("%w: %w", errStabilityProblemSourceGateWait, err)
		}
		return 0, err
	}
	defer release()
	// The server-side hint is 8s; begin a fresh client deadline only after the
	// source slot is acquired so scheduler wait cannot steal query runtime.
	cctx, cancel := context.WithTimeout(ctx, 9*time.Second)
	defer cancel()
	queryStarted := time.Now()
	if lowPriority {
		defer func() { m.deferBackgroundSourceStart(m.stabilitySourceCooldown(time.Since(queryStarted))) }()
	}

	if pending, err := m.pendingStabilityProblemStateInRange(fromTs, toTs); err != nil {
		return 0, err
	} else if pending != nil {
		return m.continuePagedProblemIngest(cctx, pending)
	}
	windowFrom, windowTo, err := m.nextUncoveredProblemWindow(fromTs, toTs)
	if err != nil || windowFrom == 0 {
		return 0, err
	}
	rows, err := m.prodDB.QueryContext(cctx, `/*+ MAX_EXECUTION_TIME(8000) */ SELECT id, created_at, channel_id,
		COALESCE(model_name,''), COALESCE(`+"`group`"+`,''), COALESCE(content,'')
		FROM logs
		WHERE created_at >= ? AND created_at < ? AND type = 5
		  AND NOT (`+m.channelTestSourcePredicateSQL()+`)
		ORDER BY created_at, id LIMIT ?`, windowFrom, windowTo, maxStabilityProblemRowsPerRun+1)
	if err != nil {
		m.reportSourceQueryError(err)
		return 0, err
	}
	batch, scanErr := scanStabilityProblemRows(rows)
	rows.Close()
	if scanErr != nil {
		m.reportSourceQueryError(scanErr)
		return 0, scanErr
	}
	now := time.Now().Unix()
	if len(batch) > maxStabilityProblemRowsPerRun {
		if err := m.ensureProblemWindowPending(windowFrom, windowTo, now); err != nil {
			return 0, err
		}
		// 本轮探测已经达到读取预算；下一轮从最老分钟开始分页，不额外压生产库。
		return 0, nil
	}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.markProblemWindowComplete(tx, windowFrom, windowTo, now, batch)
	}); err != nil {
		return 0, fmt.Errorf("保存稳定性问题窗口: %w", err)
	}
	return len(aggregateStabilityProblemRows(batch)), nil
}

func (m *Monitor) loadOrExtendStabilityProblemLiveCursor(requestedFrom, requestedTo, now int64) (*StabilityProblemLiveCursor, error) {
	var result StabilityProblemLiveCursor
	migrationActive := m.stabilityProblemClassificationMigrationActive()
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		err := tx.First(&result, 1).Error
		if errors.Is(err, gorm.ErrRecordNotFound) || err == nil && result.TrafficClassVersion != userTrafficClassificationVersion {
			next := requestedFrom
			// Before a classification migration existed, the minute table was the
			// only live cursor. Import that one legacy watermark once. During a raw
			// migration it is unsafe because cold work writes the same table.
			if !migrationActive {
				var last struct{ Max int64 }
				if err := tx.Raw("SELECT COALESCE(MAX(bucket_ts),0) max FROM stability_problem_ingest_states WHERE complete = ? AND traffic_class_version = ?",
					true, userTrafficClassificationVersion).Scan(&last).Error; err != nil {
					return err
				}
				if last.Max > 0 && last.Max+60 < requestedFrom {
					next = last.Max + 60
				}
			}
			result = StabilityProblemLiveCursor{ID: 1, TrafficClassVersion: userTrafficClassificationVersion,
				NextTs: next, TargetThroughTs: requestedTo, Status: "running", UpdatedAt: now}
			return tx.Save(&result).Error
		}
		if err != nil {
			return err
		}
		changed := false
		if result.NextTs <= 0 {
			result.NextTs = requestedFrom
			changed = true
		}
		if requestedTo > result.TargetThroughTs {
			result.TargetThroughTs = requestedTo
			changed = true
		}
		if result.NextTs < result.TargetThroughTs && result.Status != "running" {
			result.Status = "running"
			changed = true
		}
		if changed {
			result.UpdatedAt = now
			return tx.Save(&result).Error
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	m.problemLiveThrough.Store(result.NextTs)
	if result.LastSuccessAt > 0 {
		m.problemLastSuccess.Store(result.LastSuccessAt)
	}
	if result.LastFailureAt > 0 {
		m.problemLastFailure.Store(result.LastFailureAt)
	}
	return &result, nil
}

func (m *Monitor) advanceStabilityProblemLiveCursor(cursor *StabilityProblemLiveCursor, span, now int64, markSuccess bool) error {
	if cursor == nil {
		return errors.New("stability problem live cursor is missing")
	}
	if span < 60 {
		span = 60
	}
	next := cursor.NextTs
	probeTo := min(next+span, cursor.TargetThroughTs)
	var complete []int64
	if probeTo > next {
		if err := m.storeDB.Model(&StabilityProblemIngestState{}).
			Where("bucket_ts >= ? AND bucket_ts < ? AND complete = ? AND traffic_class_version = ?",
				next, probeTo, true, userTrafficClassificationVersion).
			Order("bucket_ts").Pluck("bucket_ts", &complete).Error; err != nil {
			return err
		}
	}
	known := make(map[int64]bool, len(complete))
	for _, bucket := range complete {
		known[bucket] = true
	}
	for next < probeTo && known[next] {
		next += 60
	}
	updates := map[string]any{"next_ts": next, "target_through_ts": cursor.TargetThroughTs,
		"status": "running", "updated_at": now}
	if next >= cursor.TargetThroughTs {
		updates["status"] = "caught_up"
	}
	if markSuccess {
		updates["last_success_at"] = now
		updates["last_error"] = ""
		updates["last_failure_at"] = 0
		updates["attempts"] = 0
		updates["next_retry_at"] = 0
	}
	if err := m.storeDB.Model(&StabilityProblemLiveCursor{}).
		Where("id = ? AND traffic_class_version = ?", cursor.ID, cursor.TrafficClassVersion).Updates(updates).Error; err != nil {
		return err
	}
	cursor.NextTs = next
	cursor.Status = updates["status"].(string)
	cursor.UpdatedAt = now
	if markSuccess {
		cursor.LastSuccessAt = now
		cursor.LastFailureAt = 0
		cursor.Attempts = 0
		cursor.NextRetryAt = 0
		cursor.LastError = ""
		m.problemLastSuccess.Store(now)
		m.problemLastFailure.Store(0)
	}
	m.problemLiveThrough.Store(next)
	return nil
}

func (m *Monitor) recordStabilityProblemLiveFailure(cursor *StabilityProblemLiveCursor, err error, now int64) {
	if cursor == nil || err == nil {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, errSourceNotReady) ||
		errors.Is(err, errStabilityProblemSourceGateWait) {
		return
	}
	attempts := cursor.Attempts + 1
	var delay time.Duration
	switch attempts {
	case 1:
		delay = time.Minute
	case 2:
		delay = 2 * time.Minute
	case 3:
		delay = 5 * time.Minute
	case 4:
		delay = 15 * time.Minute
	default:
		delay = 30 * time.Minute
	}
	nextRetryAt := now + int64(delay/time.Second)
	message := truncateStabilityProblemMigrationError(err)
	_ = m.storeDB.Model(&StabilityProblemLiveCursor{}).
		Where("id = ? AND traffic_class_version = ?", cursor.ID, cursor.TrafficClassVersion).
		Updates(map[string]any{"status": "running", "attempts": attempts, "next_retry_at": nextRetryAt,
			"last_error": message, "last_failure_at": now, "updated_at": now}).Error
	cursor.Attempts = attempts
	cursor.NextRetryAt = nextRetryAt
	cursor.LastError = message
	cursor.LastFailureAt = now
	m.problemLastFailure.Store(now)
}

// sampleStabilityProblems is the recent/live lane. A classification migration
// must never redirect this call to a 181-day-old cursor or lower its source
// priority. Its own durable cursor extends the target but never skips a gap.
func (m *Monitor) sampleStabilityProblems(ctx context.Context, fromTs, toTs int64) (int, error) {
	if m.prodDB == nil || !m.cfg.StabilityEnabled {
		return 0, nil
	}
	requestedFrom := fromTs / 60 * 60
	requestedTo := toTs / 60 * 60
	if requestedTo <= requestedFrom {
		return 0, nil
	}
	now := time.Now().Unix()
	cursor, err := m.loadOrExtendStabilityProblemLiveCursor(requestedFrom, requestedTo, now)
	if err != nil {
		return 0, err
	}
	if cursor.NextRetryAt > now {
		return 0, fmt.Errorf("%w: retry_at=%d", errStabilityProblemLiveBackoff, cursor.NextRetryAt)
	}
	const liveSpan = int64(60)
	total := 0
	for turn := 0; turn < stabilityProblemLiveWindowsPerTurn; turn++ {
		turnNow := time.Now().Unix()
		if err := m.advanceStabilityProblemLiveCursor(cursor, liveSpan, turnNow, false); err != nil {
			m.recordStabilityProblemLiveFailure(cursor, err, turnNow)
			return total, err
		}
		if cursor.NextTs >= cursor.TargetThroughTs {
			if err := m.advanceStabilityProblemLiveCursor(cursor, liveSpan, turnNow, true); err != nil {
				return total, err
			}
			return total, nil
		}

		windowTo := min(cursor.NextTs+liveSpan, cursor.TargetThroughTs)
		count, err := m.sampleStabilityProblemWindow(ctx, cursor.NextTs, windowTo, false)
		total += count
		if err != nil {
			m.recordStabilityProblemLiveFailure(cursor, err, time.Now().Unix())
			return total, err
		}
		if err := m.advanceStabilityProblemLiveCursor(cursor, liveSpan, time.Now().Unix(), true); err != nil {
			return total, err
		}
	}
	return total, nil
}

func truncateStabilityProblemMigrationError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if len(message) > 512 {
		message = message[:512]
	}
	return message
}

func (m *Monitor) loadStabilityProblemMigration() (*StabilityProblemClassificationMigration, error) {
	var state StabilityProblemClassificationMigration
	err := m.storeDB.First(&state, "id = ? AND traffic_class_version = ?", 1, userTrafficClassificationVersion).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &state, nil
}

type stabilityProblemMigrationProgress struct {
	Enabled             bool    `json:"enabled"`
	Status              string  `json:"status"`
	FromTs              int64   `json:"from_ts"`
	ThroughTs           int64   `json:"through_ts"`
	NextTs              int64   `json:"next_ts"`
	Percent             float64 `json:"percent"`
	CurrentSpanMinutes  int     `json:"current_span_minutes"`
	Attempts            int     `json:"attempts"`
	NextRetryAt         int64   `json:"next_retry_at,omitempty"`
	LastError           string  `json:"last_error,omitempty"`
	LastSuccessAt       int64   `json:"last_success_at,omitempty"`
	LastFailureAt       int64   `json:"last_failure_at,omitempty"`
	CreatedAt           int64   `json:"created_at,omitempty"`
	UpdatedAt           int64   `json:"updated_at"`
	CompletedAt         int64   `json:"completed_at,omitempty"`
	EstimatedSeconds    *int64  `json:"estimated_seconds"`
	EstimatedFinishAt   int64   `json:"estimated_finish_at,omitempty"`
	EstimateStatus      string  `json:"estimate_status"`
	ThroughputMinutesPS float64 `json:"throughput_minutes_per_second"`
	EstimateSampleSec   int64   `json:"estimate_sample_seconds"`
}

func stabilityProblemMigrationEstimate(state StabilityProblemClassificationMigration, status string, now int64) (*int64, string, float64, int64) {
	if status == "complete" || state.NextTs >= state.ThroughTs && state.ThroughTs > state.FromTs {
		zero := int64(0)
		return &zero, "complete", 0, max(now-state.CreatedAt, int64(0))
	}
	if status == "paused" || status == "paused_disabled" {
		return nil, "blocked", 0, max(now-state.CreatedAt, int64(0))
	}
	if state.NextRetryAt > now {
		return nil, "backoff", 0, max(now-state.CreatedAt, int64(0))
	}
	done := max(min(state.NextTs, state.ThroughTs)-state.FromTs, int64(0))
	elapsed := now - state.CreatedAt
	if elapsed < 5*60 || done < 60 {
		return nil, "warming", 0, max(elapsed, int64(0))
	}
	if state.LastSuccessAt <= 0 || now-state.LastSuccessAt > int64(time.Hour/time.Second) {
		return nil, "stalled", 0, elapsed
	}
	rateSecondsPerSecond := float64(done) / float64(elapsed)
	if rateSecondsPerSecond <= 0 || math.IsNaN(rateSecondsPerSecond) || math.IsInf(rateSecondsPerSecond, 0) {
		return nil, "warming", 0, elapsed
	}
	remaining := max(state.ThroughTs-max(state.NextTs, state.FromTs), int64(0))
	seconds := int64(math.Ceil(float64(remaining) / rateSecondsPerSecond))
	if seconds < 1 {
		seconds = 1
	}
	return &seconds, "observed", rateSecondsPerSecond / 60, elapsed
}

func (m *Monitor) stabilityProblemMigrationProgress() stabilityProblemMigrationProgress {
	result := stabilityProblemMigrationProgress{Enabled: m.cfg.StabilityClassificationMigrationEnabled, Status: "disabled"}
	state, err := m.loadStabilityProblemMigration()
	if err != nil {
		result.Status = "error"
		result.LastError = truncateStabilityProblemMigrationError(err)
		return result
	}
	if state == nil {
		if result.Enabled {
			result.Status = "not_required"
		}
		return result
	}
	result.Status = state.Status
	if !result.Enabled && state.Status != "complete" {
		result.Status = "paused_disabled"
	}
	result.FromTs, result.ThroughTs, result.NextTs = state.FromTs, state.ThroughTs, state.NextTs
	result.LastError, result.CreatedAt, result.UpdatedAt, result.CompletedAt = state.LastError, state.CreatedAt, state.UpdatedAt, state.CompletedAt
	result.CurrentSpanMinutes, result.Attempts, result.NextRetryAt = state.CurrentSpanMinutes, state.Attempts, state.NextRetryAt
	result.LastSuccessAt, result.LastFailureAt = state.LastSuccessAt, state.LastFailureAt
	if total := state.ThroughTs - state.FromTs; total > 0 {
		done := max(min(state.NextTs, state.ThroughTs)-state.FromTs, int64(0))
		result.Percent = float64(done) / float64(total) * 100
		if result.Percent > 100 {
			result.Percent = 100
		}
	} else if state.Status == "complete" {
		result.Percent = 100
	}
	now := time.Now().Unix()
	result.EstimatedSeconds, result.EstimateStatus, result.ThroughputMinutesPS, result.EstimateSampleSec =
		stabilityProblemMigrationEstimate(*state, result.Status, now)
	if result.EstimatedSeconds != nil && *result.EstimatedSeconds > 0 {
		result.EstimatedFinishAt = now + *result.EstimatedSeconds
	}
	return result
}

// advanceStabilityProblemMigration moves only across a bounded run of durable
// complete minute states. It is independent of the live watermark and may be
// resumed after a crash without replaying the whole retained history.
func (m *Monitor) advanceStabilityProblemMigration(state *StabilityProblemClassificationMigration, span int64, now int64) error {
	if state == nil || state.Status == "complete" {
		return nil
	}
	if span < 60 {
		span = 60
	}
	next := max(state.NextTs, state.FromTs)
	probeTo := min(next+span, state.ThroughTs)
	var complete []int64
	if probeTo > next {
		if err := m.storeDB.Model(&StabilityProblemIngestState{}).
			Where("bucket_ts >= ? AND bucket_ts < ? AND complete = ? AND traffic_class_version = ?",
				next, probeTo, true, userTrafficClassificationVersion).
			Order("bucket_ts").Pluck("bucket_ts", &complete).Error; err != nil {
			return err
		}
	}
	known := make(map[int64]bool, len(complete))
	for _, bucket := range complete {
		known[bucket] = true
	}
	for next < probeTo && known[next] {
		next += 60
	}
	updates := map[string]any{"next_ts": next, "status": "running", "updated_at": now, "last_error": ""}
	if next >= state.ThroughTs {
		updates["status"] = "complete"
		updates["completed_at"] = now
	}
	if err := m.storeDB.Model(&StabilityProblemClassificationMigration{}).
		Where("id = ? AND traffic_class_version = ?", state.ID, state.TrafficClassVersion).Updates(updates).Error; err != nil {
		return err
	}
	state.NextTs = next
	state.Status = updates["status"].(string)
	state.UpdatedAt = now
	state.LastError = ""
	if state.Status == "complete" {
		state.CompletedAt = now
	}
	return nil
}

func stabilityProblemMigrationSpanMinutes(current int, requestedSpan int64) int {
	if current == 1 || current == 3 || current == 6 || current == 12 {
		return current
	}
	minutes := int(requestedSpan / 60)
	if minutes >= 12 {
		return 12
	}
	if minutes >= 6 {
		return 6
	}
	if minutes >= 3 {
		return 3
	}
	return 1
}

func smallerStabilityProblemMigrationSpan(current int) int {
	switch {
	case current >= 12:
		return 6
	case current >= 6:
		return 3
	default:
		return 1
	}
}

func largerStabilityProblemMigrationSpan(current int) int {
	switch current {
	case 1:
		return 3
	case 3:
		return 6
	default:
		return 12
	}
}

func stabilityProblemMigrationRetryDelay(attempts int) time.Duration {
	switch attempts {
	case 1:
		return 15 * time.Minute
	case 2:
		return 30 * time.Minute
	default:
		return time.Hour
	}
}

func (m *Monitor) recordStabilityProblemMigrationFailure(state *StabilityProblemClassificationMigration, cause error, now int64) error {
	if state == nil || cause == nil {
		return cause
	}
	// Epoch cancellation/source-not-ready means no cold query should run. It is
	// a protected scheduler interruption, not a bad history window and must not
	// consume one of the five manual-intervention attempts.
	interrupted := errors.Is(cause, context.Canceled) || errors.Is(cause, errSourceNotReady) ||
		errors.Is(cause, errStabilityProblemSourceGateWait)
	attempts := state.Attempts
	span := stabilityProblemMigrationSpanMinutes(state.CurrentSpanMinutes, 12*60)
	status := "running"
	nextRetry := now + 60
	if !interrupted {
		attempts++
		span = smallerStabilityProblemMigrationSpan(span)
		nextRetry = now + int64(stabilityProblemMigrationRetryDelay(attempts)/time.Second)
		if attempts >= 5 {
			status = "paused"
			nextRetry = 0
		}
	}
	updates := map[string]any{"status": status, "attempts": attempts, "current_span_minutes": span,
		"healthy_windows": 0, "next_retry_at": nextRetry, "last_error": truncateStabilityProblemMigrationError(cause),
		"last_failure_at": now, "updated_at": now}
	if err := m.storeDB.Model(&StabilityProblemClassificationMigration{}).
		Where("id = ? AND traffic_class_version = ?", state.ID, state.TrafficClassVersion).Updates(updates).Error; err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (m *Monitor) recordStabilityProblemMigrationSuccess(state *StabilityProblemClassificationMigration, requestedSpan int64, now int64) error {
	if state == nil {
		return nil
	}
	span := stabilityProblemMigrationSpanMinutes(state.CurrentSpanMinutes, requestedSpan)
	healthy := state.HealthyWindows + 1
	if healthy >= 10 {
		span = largerStabilityProblemMigrationSpan(span)
		healthy = 0
	}
	updates := map[string]any{"attempts": 0, "next_retry_at": 0, "healthy_windows": healthy,
		"current_span_minutes": span, "last_error": "", "last_success_at": now, "updated_at": now}
	return m.storeDB.Model(&StabilityProblemClassificationMigration{}).
		Where("id = ? AND traffic_class_version = ?", state.ID, state.TrafficClassVersion).Updates(updates).Error
}

// sampleStabilityProblemMigration runs at most one bounded cold window on the
// protected low lane. It never updates problemLastSuccess/problemLiveThrough;
// operators see its durable cursor separately from live freshness.
func (m *Monitor) sampleStabilityProblemMigration(ctx context.Context, span int64) (int, error) {
	state, err := m.loadStabilityProblemMigration()
	if err != nil || state == nil || state.Status == "complete" || m.prodDB == nil || !m.cfg.StabilityEnabled ||
		!m.cfg.StabilityClassificationMigrationEnabled {
		return 0, err
	}
	now := time.Now().Unix()
	if state.Status == "paused" || state.NextRetryAt > now {
		return 0, nil
	}
	spanMinutes := stabilityProblemMigrationSpanMinutes(state.CurrentSpanMinutes, span)
	effectiveSpan := int64(spanMinutes) * 60
	if err := m.advanceStabilityProblemMigration(state, effectiveSpan, now); err != nil {
		return 0, err
	}
	if state.Status == "complete" {
		_ = m.recordStabilityProblemMigrationSuccess(state, span, now)
		return 0, nil
	}
	windowTo := min(state.NextTs+effectiveSpan, state.ThroughTs)
	count, sampleErr := m.sampleStabilityProblemWindow(ctx, state.NextTs, windowTo, true)
	if sampleErr != nil {
		return count, m.recordStabilityProblemMigrationFailure(state, sampleErr, time.Now().Unix())
	}
	finishedAt := time.Now().Unix()
	if err := m.advanceStabilityProblemMigration(state, effectiveSpan, finishedAt); err != nil {
		return count, err
	}
	if err := m.recordStabilityProblemMigrationSuccess(state, span, finishedAt); err != nil {
		return count, err
	}
	return count, nil
}

// runStabilityProblemMigrationLoop removes the old one-window-per-main-tick
// ceiling. A source epoch must first complete a recent/live sample; afterwards
// this worker continuously offers one bounded cold window to the protected low
// lane. The shared scheduler remains the authority for high-priority
// preemption, minimum start spacing and source-duty cooldown.
func (m *Monitor) runStabilityProblemMigrationLoop(ctx context.Context) {
	timer := time.NewTimer(stabilityProblemMigrationPollEvery)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		delay := stabilityProblemMigrationPollEvery
		state, err := m.loadStabilityProblemMigration()
		switch {
		case err != nil:
			slog.Warn("读取稳定性原始错误历史迁移游标失败", "err", err)
			delay = 30 * time.Second
		case state == nil || state.Status == "complete" || !m.cfg.StabilityClassificationMigrationEnabled:
			return
		case m.problemLastSuccess.Load() == 0:
			// Do not let cold history take the first source slot after a reconnect.
			delay = 5 * time.Second
		case state.Status == "paused":
			// Stay alive so the root retry endpoint can resume without a restart,
			// but do not spin against SQLite while operator action is required.
			delay = 30 * time.Second
		case state.NextRetryAt > time.Now().Unix():
			delay = min(time.Until(time.Unix(state.NextRetryAt, 0)), 30*time.Second)
			if delay < stabilityProblemMigrationPollEvery {
				delay = stabilityProblemMigrationPollEvery
			}
		default:
			_, sampleErr := m.sampleStabilityProblemMigration(ctx, 12*60)
			if sampleErr != nil && !errors.Is(sampleErr, context.Canceled) &&
				!errors.Is(sampleErr, errSourceNotReady) &&
				!errors.Is(sampleErr, errStabilityProblemSourceGateWait) {
				slog.Warn("稳定性原始错误历史分类迁移失败(低优先级重试)", "err", sampleErr)
			}
			if sampleErr == nil && !m.stabilityProblemClassificationMigrationActive() {
				return
			}
		}
		timer.Reset(delay)
	}
}

func (m *Monitor) retryStabilityProblemMigration(now time.Time) (*StabilityProblemClassificationMigration, error) {
	if !m.cfg.StabilityClassificationMigrationEnabled {
		return nil, errors.New("raw problem classification migration is disabled")
	}
	if now.IsZero() {
		now = time.Now()
	}
	state, err := m.loadStabilityProblemMigration()
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, errors.New("raw problem classification migration is not required")
	}
	if state.Status == "complete" {
		return nil, errors.New("raw problem classification migration is already complete")
	}
	updates := map[string]any{"status": "queued", "attempts": 0, "next_retry_at": 0,
		"current_span_minutes": 1, "healthy_windows": 0, "last_error": "", "updated_at": now.Unix()}
	if err := m.storeDB.Model(&StabilityProblemClassificationMigration{}).
		Where("id = ? AND traffic_class_version = ?", state.ID, state.TrafficClassVersion).Updates(updates).Error; err != nil {
		return nil, err
	}
	if err := m.storeDB.First(state, state.ID).Error; err != nil {
		return nil, err
	}
	return state, nil
}

func stabilityProblemIntervalSeconds(v int) int64 {
	if v < 60 {
		v = 300
	}
	if v > 3600 {
		v = 3600
	}
	return int64(v)
}
