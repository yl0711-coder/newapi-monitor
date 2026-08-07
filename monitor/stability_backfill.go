package monitor

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const (
	stabilityHourFinalizeDelaySec = int64(10 * 60)
	maxStabilityRowsPerHour       = 10000
	maxStabilityBackfillAttempts  = 3
)

var errStabilityBackfillDisabled = errors.New("稳定性历史补数已禁用")

// StabilityHourIngestState 是长期小时汇总的完整性台账。
// 即使某小时没有任何请求，也会写一条 complete 记录；因此页面能区分
// “确实零流量”和“这个小时尚未采集”，不能再靠数据表 MIN/MAX 猜覆盖率。
type StabilityHourIngestState struct {
	HourTs      int64  `gorm:"primaryKey;autoIncrement:false" json:"hour_ts"`
	Status      string `gorm:"size:16;index" json:"status"`
	Rows        int64  `json:"rows"`
	Requests    int64  `json:"requests"`
	Tokens      int64  `json:"tokens"`
	Quota       int64  `json:"quota"`
	Attempts    int    `json:"attempts"`
	JobID       string `gorm:"size:40;column:job_id;index" json:"job_id,omitempty"`
	UpdatedAt   int64  `gorm:"index" json:"updated_at"`
	CompletedAt int64  `gorm:"column:completed_at" json:"completed_at,omitempty"`
	LastError   string `gorm:"size:512;column:last_error" json:"last_error,omitempty"`
}

// StabilityBackfillJob 是可恢复的后台补数任务。任务和浏览器请求解耦；进程重启后
// queued/running 任务会重新扫描缺口并继续，已 complete 的小时自动跳过。
type StabilityBackfillJob struct {
	ID             string `gorm:"primaryKey;size:40" json:"id"`
	FromTs         int64  `gorm:"index" json:"from_ts"`
	ToTs           int64  `gorm:"index" json:"to_ts"`
	Status         string `gorm:"size:16;index" json:"status"`
	TotalHours     int    `json:"total_hours"`
	CompletedHours int    `json:"completed_hours"`
	FailedHours    int    `json:"failed_hours"`
	CurrentHourTs  int64  `json:"current_hour_ts,omitempty"`
	StartedAt      int64  `json:"started_at,omitempty"`
	UpdatedAt      int64  `gorm:"index" json:"updated_at"`
	FinishedAt     int64  `json:"finished_at,omitempty"`
	LastError      string `gorm:"size:512;column:last_error" json:"last_error,omitempty"`
}

// StabilityDataCoverage 是报表口径的数据完整率，不是“筛选结果占全量”的比例。
type StabilityDataCoverage struct {
	FromTs         int64   `json:"from_ts"`
	ToTs           int64   `json:"to_ts"`
	ExpectedHours  int64   `json:"expected_hours"`
	CompletedHours int64   `json:"completed_hours"`
	MissingHours   int64   `json:"missing_hours"`
	Percent        float64 `json:"percent"`
	Complete       bool    `json:"complete"`
}

func finalizedStabilityHourTo(now int64) int64 {
	to := now - stabilityHourFinalizeDelaySec
	if to < 0 {
		return 0
	}
	return to / 3600 * 3600
}

func (m *Monitor) stabilityDataCoverage(ctx context.Context, fromTs, toTs, now int64) StabilityDataCoverage {
	fromTs = fromTs / 3600 * 3600
	finalizedTo := finalizedStabilityHourTo(now)
	if toTs > finalizedTo {
		toTs = finalizedTo
	}
	toTs = toTs / 3600 * 3600
	result := StabilityDataCoverage{FromTs: fromTs, ToTs: toTs}
	if toTs <= fromTs {
		result.Complete = true
		result.Percent = 100
		return result
	}
	result.ExpectedHours = (toTs - fromTs) / 3600
	var count int64
	if tx := m.storeDB.WithContext(ctx).Model(&StabilityHourIngestState{}).
		Where("hour_ts >= ? AND hour_ts < ? AND status = ?", fromTs, toTs, "complete").Count(&count); tx.Error != nil {
		slog.Warn("读取稳定性小时覆盖台账失败", "err", tx.Error)
		return result
	}
	result.CompletedHours = count
	result.MissingHours = result.ExpectedHours - count
	if result.MissingHours < 0 {
		result.MissingHours = 0
	}
	if result.ExpectedHours > 0 {
		result.Percent = float64(result.CompletedHours) / float64(result.ExpectedHours) * 100
	}
	result.Complete = result.MissingHours == 0
	return result
}

func stabilityHourSQL() string {
	q := `
SELECT channel_id, model_name, ` + "`group`" + ` AS grp,
  CAST(COALESCE(SUM(type=2 AND NOT {{ANOM}}),0) AS SIGNED) AS success,
  CAST(COALESCE(SUM(type=2 AND {{ANOM}}),0) AS SIGNED) AS anomaly,
  CAST(COALESCE(SUM(type=5),0) AS SIGNED) AS failed,
  CAST(COALESCE(SUM(type=2 AND {{ZERO}} AND prompt_tokens > 0),0) AS SIGNED) AS anomaly_billed,
  CAST(COALESCE(SUM(type=2 AND {{ZERO}} AND prompt_tokens = 0),0) AS SIGNED) AS anomaly_free,
  CAST(COALESCE(SUM(type=2 AND {{STREAMBAD}} AND NOT {{ZERO}}),0) AS SIGNED) AS anomaly_stream,
  CAST(COALESCE(SUM(CASE WHEN type=2 AND {{ZERO}} AND prompt_tokens > 0 THEN quota END),0) AS SIGNED) AS anomaly_quota,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN use_time END),0) AS SIGNED) AS sum_use_time,
  CAST(COALESCE(MAX(CASE WHEN type=2 THEN use_time END),0) AS SIGNED) AS max_use_time,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN prompt_tokens+completion_tokens END),0) AS SIGNED) AS tokens,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN quota END),0) AS SIGNED) AS quota,
  CAST(COALESCE(SUM(type=5 AND content REGEXP 'status_code=4'
        AND content NOT LIKE '%timeout%' AND content NOT LIKE '%deadline%'),0) AS SIGNED) AS err_4xx,
  CAST(COALESCE(SUM(type=5 AND content REGEXP 'status_code=5'
        AND content NOT LIKE '%timeout%' AND content NOT LIKE '%deadline%'),0) AS SIGNED) AS err_5xx,
  CAST(COALESCE(SUM(type=5 AND (content LIKE '%timeout%' OR content LIKE '%deadline%')),0) AS SIGNED) AS err_timeout
FROM logs
WHERE created_at >= ? AND created_at < ? AND type IN (2,5)
GROUP BY channel_id, model_name, grp`
	return expandAnomalyPredicates(q)
}

func (m *Monitor) stabilityQueryTimeout() time.Duration {
	seconds := m.cfg.StabilityBackfillTimeoutSec
	if seconds < 5 {
		seconds = 20
	}
	if seconds > 60 {
		seconds = 60
	}
	return time.Duration(seconds) * time.Second
}

// fetchStabilityHour 只读生产库的一个完整小时。它不写分钟表，返回的数据量受
// 渠道×模型×分组基数限制，避免 90 天补数把本地 SQLite 放大 60 倍。
func (m *Monitor) fetchStabilityHour(ctx context.Context, hourTs int64) ([]StabilityHourSample, error) {
	cctx, cancel := context.WithTimeout(ctx, m.stabilityQueryTimeout())
	defer cancel()
	rows, err := m.prodDB.QueryContext(cctx, stabilityHourSQL(), hourTs, hourTs+3600)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]StabilityHourSample, 0, 128)
	for rows.Next() {
		var row StabilityHourSample
		var group sql.NullString
		var err4xx, err5xx, errTimeout int64
		if err := rows.Scan(&row.ChannelID, &row.ModelName, &group,
			&row.Success, &row.Anomaly, &row.Failed,
			&row.AnomalyBilled, &row.AnomalyFree, &row.AnomalyStream, &row.AnomalyQuota,
			&row.SumUseTime, &row.MaxUseTime, &row.Tokens, &row.Quota,
			&err4xx, &err5xx, &errTimeout); err != nil {
			return nil, err
		}
		row.HourTs, row.Grp = hourTs, group.String
		row.Err4xx, row.Err5xx, row.ErrTimeout = err4xx, err5xx, errTimeout
		if other := row.Failed - err4xx - err5xx - errTimeout; other > 0 {
			row.ErrOther = other
		}
		out = append(out, row)
		if len(out) > maxStabilityRowsPerHour {
			return nil, fmt.Errorf("单小时维度超过安全上限 %d", maxStabilityRowsPerHour)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func stabilityHourTotals(rows []StabilityHourSample) (requests, tokens, quota int64) {
	for _, row := range rows {
		requests += row.Success + row.Anomaly + row.Failed
		tokens += row.Tokens
		quota += row.Quota
	}
	return
}

// replaceStabilityHour 在一个本地事务里完成“删旧→写全量→控制总数复核→标记完成”。
// 任一步失败都会回滚，页面永远不会把半个小时的数据当成完整结果。
func (m *Monitor) replaceStabilityHour(hourTs int64, rows []StabilityHourSample, state StabilityHourIngestState) error {
	expectedRequests, expectedTokens, expectedQuota := stabilityHourTotals(rows)
	return m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("hour_ts = ?", hourTs).Delete(&StabilityHourSample{}).Error; err != nil {
			return err
		}
		if len(rows) > 0 {
			if err := tx.CreateInBatches(rows, 200).Error; err != nil {
				return err
			}
		}
		var local struct{ Requests, Tokens, Quota int64 }
		if err := tx.Raw(`SELECT COALESCE(SUM(success+anomaly+failed),0) requests,
			COALESCE(SUM(tokens),0) tokens,COALESCE(SUM(quota),0) quota
			FROM stability_hour_samples WHERE hour_ts=?`, hourTs).Scan(&local).Error; err != nil {
			return err
		}
		if local.Requests != expectedRequests || local.Tokens != expectedTokens || local.Quota != expectedQuota {
			return fmt.Errorf("本地控制总数不一致: got=%d/%d/%d want=%d/%d/%d",
				local.Requests, local.Tokens, local.Quota, expectedRequests, expectedTokens, expectedQuota)
		}
		state.HourTs = hourTs
		state.Status = "complete"
		state.Rows = int64(len(rows))
		state.Requests, state.Tokens, state.Quota = expectedRequests, expectedTokens, expectedQuota
		state.CompletedAt, state.UpdatedAt, state.LastError = time.Now().Unix(), time.Now().Unix(), ""
		return tx.Save(&state).Error
	})
}

func (m *Monitor) markStabilityHourAttempt(hourTs int64, jobID, status, lastError string) StabilityHourIngestState {
	var state StabilityHourIngestState
	m.storeDB.First(&state, "hour_ts = ?", hourTs)
	state.HourTs = hourTs
	state.Status = status
	state.JobID = jobID
	if status == "running" {
		state.Attempts++
	}
	state.UpdatedAt = time.Now().Unix()
	state.LastError = clip(lastError, 512)
	if status != "complete" {
		state.CompletedAt = 0
	}
	if err := m.storeDB.Save(&state).Error; err != nil {
		slog.Warn("写稳定性小时采集状态失败", "hour", hourTs, "err", err)
	}
	return state
}

func (m *Monitor) backfillOneStabilityHour(ctx context.Context, hourTs int64, jobID string) error {
	var lastErr error
	for attempt := 1; attempt <= maxStabilityBackfillAttempts; attempt++ {
		state := m.markStabilityHourAttempt(hourTs, jobID, "running", "")
		rows, err := m.fetchStabilityHour(ctx, hourTs)
		if err == nil {
			return m.replaceStabilityHour(hourTs, rows, state)
		}
		lastErr = err
		if stabilityBackfillInterrupted(ctx, err) {
			m.markStabilityHourAttempt(hourTs, jobID, "queued", "服务停止，等待续跑")
			return err
		}
		m.markStabilityHourAttempt(hourTs, jobID, "failed", err.Error())
		if attempt < maxStabilityBackfillAttempts {
			wait := time.Duration(1<<(attempt-1)) * time.Second
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}
	}
	return lastErr
}

// 只有服务生命周期取消才进入 queued 等待重启续跑。单小时自己的查询超时
// (context deadline exceeded，但父 ctx 仍正常)仍属于真实失败，按退避重试并暂停。
func stabilityBackfillInterrupted(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled)
}

func newStabilityBackfillID() (string, error) {
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func (m *Monitor) backfillDelay() time.Duration {
	ms := m.cfg.StabilityBackfillDelayMS
	if ms < 250 {
		ms = 2000
	}
	if ms > 30000 {
		ms = 30000
	}
	return time.Duration(ms) * time.Millisecond
}

func (m *Monitor) startStabilityBackfill(fromTs, toTs int64) (*StabilityBackfillJob, error) {
	if !m.cfg.StabilityBackfillEnabled {
		return nil, errStabilityBackfillDisabled
	}
	if m.prodDB == nil {
		return nil, fmt.Errorf("未配置生产库只读连接")
	}
	fromTs, toTs = fromTs/3600*3600, toTs/3600*3600
	if toTs <= fromTs {
		return nil, fmt.Errorf("补数范围为空")
	}
	retention := m.cfg.stabilityStorageDays()
	if toTs-fromTs > int64(retention)*86400 {
		return nil, fmt.Errorf("补数范围不能超过稳定性留存 %d 天", retention)
	}
	if !m.stabilityBackfillRunning.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("已有稳定性历史补数或自动修洞正在执行")
	}
	id, err := newStabilityBackfillID()
	if err != nil {
		m.stabilityBackfillRunning.Store(false)
		return nil, err
	}
	now := time.Now().Unix()
	job := &StabilityBackfillJob{ID: id, FromTs: fromTs, ToTs: toTs, Status: "queued", TotalHours: int((toTs - fromTs) / 3600), UpdatedAt: now}
	if err := m.storeDB.Create(job).Error; err != nil {
		m.stabilityBackfillRunning.Store(false)
		return nil, err
	}
	go m.runStabilityBackfill(m.taskContext(), job.ID)
	return job, nil
}

func (m *Monitor) runStabilityBackfill(ctx context.Context, jobID string) {
	defer m.stabilityBackfillRunning.Store(false)
	var job StabilityBackfillJob
	if err := m.storeDB.First(&job, "id = ?", jobID).Error; err != nil {
		slog.Warn("读取稳定性补数任务失败", "job_id", jobID, "err", err)
		return
	}
	now := time.Now().Unix()
	job.Status, job.StartedAt, job.UpdatedAt = "running", now, now
	job.CompletedHours, job.FailedHours = 0, 0
	job.LastError, job.FinishedAt = "", 0
	m.storeDB.Save(&job)
	delay := m.backfillDelay()
	for hour := job.ToTs - 3600; hour >= job.FromTs; hour -= 3600 {
		select {
		case <-ctx.Done():
			job.Status, job.LastError, job.UpdatedAt = "queued", "服务停止，等待下次启动续跑", time.Now().Unix()
			m.storeDB.Save(&job)
			return
		default:
		}
		job.CurrentHourTs, job.UpdatedAt = hour, time.Now().Unix()
		m.storeDB.Model(&job).Updates(map[string]any{"current_hour_ts": hour, "updated_at": job.UpdatedAt})
		var state StabilityHourIngestState
		if err := m.storeDB.First(&state, "hour_ts = ? AND status = ?", hour, "complete").Error; err == nil {
			job.CompletedHours++
			m.storeDB.Model(&job).Updates(map[string]any{"completed_hours": job.CompletedHours, "updated_at": time.Now().Unix()})
			continue
		}
		if err := m.backfillOneStabilityHour(ctx, hour, job.ID); err != nil {
			if stabilityBackfillInterrupted(ctx, err) {
				job.Status, job.LastError, job.UpdatedAt = "queued", "服务停止，等待下次启动续跑", time.Now().Unix()
				m.storeDB.Save(&job)
				return
			}
			job.FailedHours++
			job.Status, job.LastError, job.UpdatedAt = "paused", clip(err.Error(), 512), time.Now().Unix()
			m.storeDB.Save(&job)
			slog.Warn("稳定性历史补数暂停", "job_id", job.ID, "hour", hour, "err", err)
			return
		}
		job.CompletedHours++
		m.storeDB.Model(&job).Updates(map[string]any{"completed_hours": job.CompletedHours, "updated_at": time.Now().Unix()})
		if hour > job.FromTs {
			select {
			case <-ctx.Done():
				job.Status, job.LastError, job.UpdatedAt = "queued", "服务停止，等待下次启动续跑", time.Now().Unix()
				m.storeDB.Save(&job)
				return
			case <-time.After(delay):
			}
		}
	}
	job.Status, job.CurrentHourTs, job.UpdatedAt, job.FinishedAt = "complete", 0, time.Now().Unix(), time.Now().Unix()
	job.LastError = ""
	m.storeDB.Save(&job)
	slog.Info("稳定性历史补数完成", "job_id", job.ID, "hours", job.TotalHours)
}

func (m *Monitor) resumeStabilityBackfill() {
	if !m.stabilityBackfillRunning.CompareAndSwap(false, true) {
		return
	}
	var job StabilityBackfillJob
	err := m.storeDB.Where("status IN ? OR (status = ? AND last_error = ?)", []string{"queued", "running"}, "paused", context.Canceled.Error()).Order("updated_at DESC").First(&job).Error
	if err != nil {
		m.stabilityBackfillRunning.Store(false)
		return
	}
	job.Status, job.UpdatedAt = "queued", time.Now().Unix()
	m.storeDB.Save(&job)
	go m.runStabilityBackfill(m.taskContext(), job.ID)
}

// repairOneStabilityHour 每次只修一个缺口。它只在没有人工补数时运行，查询失败
// 不影响主采样、不循环重压生产库；下一轮会继续尝试。
func (m *Monitor) repairOneStabilityHour(ctx context.Context) {
	if !m.stabilityBackfillRunning.CompareAndSwap(false, true) {
		return
	}
	defer m.stabilityBackfillRunning.Store(false)
	retention := m.cfg.stabilityStorageDays()
	to := finalizedStabilityHourTo(time.Now().Unix())
	from := to - int64(retention)*86400
	var complete []int64
	if err := m.storeDB.Model(&StabilityHourIngestState{}).Where("hour_ts >= ? AND hour_ts < ? AND status = ?", from, to, "complete").Pluck("hour_ts", &complete).Error; err != nil {
		return
	}
	known := make(map[int64]bool, len(complete))
	for _, hour := range complete {
		known[hour] = true
	}
	for hour := to - 3600; hour >= from; hour -= 3600 {
		if known[hour] {
			continue
		}
		if err := m.backfillOneStabilityHour(ctx, hour, "auto-repair"); err != nil {
			slog.Warn("稳定性小时自动修洞失败(等待下轮)", "hour", hour, "err", err)
		}
		return
	}
}

func (m *Monitor) startStabilityBackfillMaintenance(ctx context.Context) {
	if !m.cfg.StabilityBackfillEnabled {
		return
	}
	m.resumeStabilityBackfill()
	if !m.cfg.StabilityAutoRepair {
		return
	}
	go func() {
		timer := time.NewTimer(45 * time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			m.repairOneStabilityHour(ctx)
		}
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.repairOneStabilityHour(ctx)
			}
		}
	}()
}

func (m *Monitor) startStabilityBackfillHandler(c *gin.Context) {
	retention := m.cfg.stabilityStorageDays()
	days, _ := strconv.Atoi(c.DefaultQuery("days", strconv.Itoa(retention)))
	if days <= 0 || days > retention {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("days 必须在 1～%d 之间", retention)})
		return
	}
	to := finalizedStabilityHourTo(time.Now().Unix())
	job, err := m.startStabilityBackfill(to-int64(days)*86400, to)
	if err != nil {
		if errors.Is(err, errStabilityBackfillDisabled) {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, job)
}

func (m *Monitor) stabilityBackfillStatusHandler(c *gin.Context) {
	var job StabilityBackfillJob
	if err := m.storeDB.Order("updated_at DESC").First(&job).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	retention := m.cfg.stabilityStorageDays()
	to := finalizedStabilityHourTo(time.Now().Unix())
	coverage := m.stabilityDataCoverage(c.Request.Context(), to-int64(retention)*86400, to, time.Now().Unix())
	c.JSON(http.StatusOK, gin.H{"job": job, "running": m.stabilityBackfillRunning.Load(), "coverage": coverage})
}
