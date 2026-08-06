package monitor

import (
	"context"
	"errors"
	"fmt"
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
)

type stabilityProblemRawRow struct {
	ID, CreatedAt, ChannelID int64
	Model, Group, Raw        string
}

type stabilityProblemKey struct {
	bucket, channel int64
	model, group    string
	hash            string
}

func aggregateStabilityProblemRows(rows []stabilityProblemRawRow) []StabilityProblemSample {
	agg := make(map[stabilityProblemKey]*StabilityProblemSample)
	for _, row := range rows {
		message, truncated := stabilityProblemText(row.Raw)
		hash := stabilityProblemHash("newapi", row.Raw)
		key := stabilityProblemKey{bucket: row.CreatedAt / 60 * 60, channel: row.ChannelID, model: row.Model, group: row.Group, hash: hash}
		p := agg[key]
		if p == nil {
			p = &StabilityProblemSample{
				BucketTs: key.bucket, Source: "newapi", SignatureHash: hash,
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

func (m *Monitor) pendingStabilityProblemState() (*StabilityProblemIngestState, error) {
	var state StabilityProblemIngestState
	err := m.storeDB.Where("complete = ?", false).Order("bucket_ts ASC").First(&state).Error
	if err == nil {
		return &state, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return nil, err
}

func (m *Monitor) stabilityProblemPendingCount() int64 {
	var count int64
	warnReadErr("stability problem pending", m.storeDB.Model(&StabilityProblemIngestState{}).Where("complete = ?", false).Count(&count))
	return count
}

// stabilityProblemCatchupWindow 保证高峰分页追赶期间不会跳过新产生的分钟。
// 每轮最多只向前推进一个正常查询窗口，保持生产库的时间范围与平时一致。
func (m *Monitor) stabilityProblemCatchupWindow(fromTs, toTs int64) (int64, int64, error) {
	var last struct{ Max int64 }
	if err := m.storeDB.Raw("SELECT COALESCE(MAX(bucket_ts),0) max FROM stability_problem_ingest_states").Scan(&last).Error; err != nil {
		return 0, 0, err
	}
	resumeFrom := last.Max + 60
	if last.Max == 0 || resumeFrom >= fromTs || resumeFrom >= toTs {
		return fromTs, toTs, nil
	}
	span := toTs - fromTs
	if span < 60 {
		span = 60
	}
	resumeTo := resumeFrom + span
	if resumeTo > toTs {
		resumeTo = toTs
	}
	return resumeFrom, resumeTo, nil
}

func (m *Monitor) stabilityProblemNeedsCatchup(targetTo int64) bool {
	var last struct{ Max int64 }
	if err := m.storeDB.Raw("SELECT COALESCE(MAX(bucket_ts),0) max FROM stability_problem_ingest_states").Scan(&last).Error; err != nil {
		return true
	}
	return last.Max > 0 && last.Max+60 < targetTo/60*60
}

// nextUncoveredProblemWindow 返回给定范围内第一段尚未完整采集的连续分钟。
func (m *Monitor) nextUncoveredProblemWindow(fromTs, toTs int64) (int64, int64, error) {
	var rows []StabilityProblemIngestState
	if err := m.storeDB.Select("bucket_ts", "complete").Where("bucket_ts >= ? AND bucket_ts < ?", fromTs, toTs).Find(&rows).Error; err != nil {
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
		byMinute[bucket] = &StabilityProblemIngestState{BucketTs: bucket, Complete: true, UpdatedAt: now, CompletedAt: now}
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
		states = append(states, StabilityProblemIngestState{BucketTs: bucket, UpdatedAt: now})
	}
	if len(states) == 0 {
		return nil
	}
	return m.storeDB.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(states, 200).Error
}

func (m *Monitor) finishProblemMinute(tx *gorm.DB, state StabilityProblemIngestState, now int64) error {
	if err := tx.Where("source = ? AND bucket_ts = ?", "newapi", state.BucketTs).Delete(&StabilityProblemSample{}).Error; err != nil {
		return err
	}
	var staged []StabilityProblemStage
	if err := tx.Where("bucket_ts = ?", state.BucketTs).Find(&staged).Error; err != nil {
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
	}).Error
}

func (m *Monitor) continuePagedProblemIngest(ctx context.Context) (int, error) {
	processed := 0
	for processed < maxStabilityProblemRowsPerRun {
		state, err := m.pendingStabilityProblemState()
		if err != nil || state == nil {
			return processed, err
		}
		limit := stabilityProblemPageSize
		if remaining := maxStabilityProblemRowsPerRun - processed; remaining < limit {
			limit = remaining
		}
		rows, err := m.prodDB.QueryContext(ctx, `SELECT id, created_at, channel_id,
			COALESCE(model_name,''), COALESCE(`+"`group`"+`,''), COALESCE(content,'')
			FROM logs
			WHERE created_at >= ? AND created_at < ? AND type = 5
			  AND (created_at > ? OR (created_at = ? AND id > ?))
			ORDER BY created_at, id LIMIT ?`, state.BucketTs, state.BucketTs+60,
			state.LastCreatedAt, state.LastCreatedAt, state.LastID, limit)
		if err != nil {
			return processed, err
		}
		batch, scanErr := scanStabilityProblemRows(rows)
		rows.Close()
		if scanErr != nil {
			return processed, scanErr
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
		if err != nil {
			return processed, err
		}
		processed += len(batch)
		if len(batch) == 0 {
			continue
		}
	}
	return processed, nil
}

// sampleStabilityProblems 增量读取完整分钟内的 type=5 原始错误。
// 正常流量仍是一条小范围查询；超过安全预算时切换为可恢复的本地游标分页。
func (m *Monitor) sampleStabilityProblems(ctx context.Context, fromTs, toTs int64) (int, error) {
	if m.prodDB == nil || !m.cfg.StabilityEnabled {
		return 0, nil
	}
	fromTs = fromTs / 60 * 60
	toTs = toTs / 60 * 60
	if toTs <= fromTs {
		return 0, nil
	}
	cctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()

	if pending, err := m.pendingStabilityProblemState(); err != nil {
		return 0, err
	} else if pending != nil {
		return m.continuePagedProblemIngest(cctx)
	}
	var err error
	fromTs, toTs, err = m.stabilityProblemCatchupWindow(fromTs, toTs)
	if err != nil {
		return 0, err
	}
	windowFrom, windowTo, err := m.nextUncoveredProblemWindow(fromTs, toTs)
	if err != nil || windowFrom == 0 {
		return 0, err
	}
	rows, err := m.prodDB.QueryContext(cctx, `SELECT id, created_at, channel_id,
		COALESCE(model_name,''), COALESCE(`+"`group`"+`,''), COALESCE(content,'')
		FROM logs
		WHERE created_at >= ? AND created_at < ? AND type = 5
		ORDER BY created_at, id LIMIT ?`, windowFrom, windowTo, maxStabilityProblemRowsPerRun+1)
	if err != nil {
		return 0, err
	}
	batch, scanErr := scanStabilityProblemRows(rows)
	rows.Close()
	if scanErr != nil {
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

func stabilityProblemIntervalSeconds(v int) int64 {
	if v < 60 {
		v = 300
	}
	if v > 3600 {
		v = 3600
	}
	return int64(v)
}
