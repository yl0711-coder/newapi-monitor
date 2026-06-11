package monitor

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// sampler.go:唯一访问生产库的组件。每周期对 logs 表做【一条】小窗口聚合查询,写本地 sqlite。
// 采样心跳(m.lastRun)与渠道名缓存(m.chNames)都挂在 Monitor 上。

// LastSampleRun 返回采样器最近一次成功运行时刻(0=从未)。
func (m *Monitor) LastSampleRun() int64 { return m.lastRun.Load() }

func (m *Monitor) channelNames() map[string]string {
	m.chMu.RLock()
	defer m.chMu.RUnlock()
	cp := make(map[string]string, len(m.chNames))
	for k, v := range m.chNames {
		cp[k] = v
	}
	return cp
}

// startSampler 启动后台采样(prodDB 未配置则不启动)。
func (m *Monitor) startSampler(ctx context.Context) {
	if m.prodDB == nil {
		return
	}
	m.refreshChannels()

	if h := m.cfg.BackfillHours; h > 0 {
		if n, err := m.sampleWindow(ctx, int64(h)*3600); err != nil {
			slog.Warn("历史回填失败(忽略)", "err", err)
		} else {
			slog.Info("历史回填完成", "hours", h, "rows", n)
		}
		if err := m.sampleTokens(ctx, int64(h)*3600); err != nil {
			slog.Warn("token 维度回填失败(忽略,不影响主监控)", "err", err)
		}
		if err := m.rollupHours(time.Now().Unix() - int64(m.cfg.RetentionDays)*86400); err != nil {
			slog.Warn("启动小时汇总失败(忽略)", "err", err)
		}
	}

	interval := time.Duration(m.cfg.SampleSeconds) * time.Second
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}
	m.lastRun.Store(time.Now().Unix()) // 初始化心跳,避免启动初期误报"采样异常"
	m.heartbeat()                      // 启动即对外打一次心跳,让 dead-man 立刻知道"活着"
	go m.loop(ctx, interval)
	slog.Info("采样器已启动", "interval", interval.String(), "note", "生产库仅每周期一条小查询")
}

func (m *Monitor) loop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lookback := int64(interval.Seconds())*3 + 60
	var ticks int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := m.sampleWindow(ctx, lookback); err != nil {
				slog.Error("采样失败(下周期重试)", "err", err)
				// 采样失败也评估报警:lastRun 不更新 → 超过 3 周期会触发"采样掉线"
				m.evaluateAlerts(time.Now().Unix())
				continue
			}
			m.lastRun.Store(time.Now().Unix())
			m.heartbeat() // 成功采样后向外部 dead-man 服务打心跳
			if err := m.sampleTokens(ctx, lookback); err != nil {
				slog.Warn("token 维度采样失败(忽略,不影响主监控)", "err", err)
			}
			m.refreshChannels() // 每周期同步渠道开关(小查询),禁用/重启用近乎实时反映到稳定性
			m.evaluateAlerts(time.Now().Unix())
			ticks++
			if ticks%(int(600/interval.Seconds())+1) == 0 {
				if d := m.cfg.RetentionDays; d > 0 {
					cutoff := time.Now().Unix() - int64(d)*86400
					if n, err := m.pruneOlderThan(cutoff); err == nil && n > 0 {
						slog.Info("清理过期采样", "rows", n)
					}
					if err := m.rollupHours(cutoff); err != nil { // 分钟数据被清前,先滚动汇总进小时表
						slog.Warn("小时汇总失败(忽略)", "err", err)
					}
					if n, err := m.pruneRejectionsOlderThan(cutoff); err == nil && n > 0 {
						slog.Info("清理过期被拒采样", "rows", n)
					}
				}
				if hd := m.cfg.HourRetentionDays; hd > 0 {
					if n, err := m.pruneHoursOlderThan(time.Now().Unix() - int64(hd)*86400); err == nil && n > 0 {
						slog.Info("清理过期小时汇总", "rows", n)
					}
				}
			}
		}
	}
}

// sampleWindow 查询生产库最近 lookbackSec 秒日志,按"分钟桶×渠道×模型×分组"聚合并写本地。
// 这是全程唯一打到生产库的查询。
func (m *Monitor) sampleWindow(ctx context.Context, lookbackSec int64) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	// MySQL SUM/布尔聚合返回 DECIMAL,需 CAST 成 SIGNED 才能 Scan 进 int64。
	// 错误分类互斥(优先级:超时 > 5xx > 4xx),四类之和不超过失败数。
	// FRT = 首字延迟(ms),取自 other JSON 的 frt;非法 JSON 或缺失则计 0(被 frt>0 过滤掉)。
	const frt = "(CASE WHEN JSON_VALID(other) THEN CAST(JSON_EXTRACT(other,'$.frt') AS SIGNED) ELSE 0 END)"
	q := `
SELECT
  (created_at DIV 60)*60 AS bucket,
  channel_id, model_name, ` + "`group`" + ` AS grp,
  CAST(COALESCE(SUM(type=2 AND COALESCE(other,'') NOT REGEXP 'client_gone|scanner_error|panic|ping_fail'),0) AS SIGNED) AS success,
  CAST(COALESCE(SUM(type=2 AND COALESCE(other,'')     REGEXP 'client_gone|scanner_error|panic|ping_fail'),0) AS SIGNED) AS anomaly,
  CAST(COALESCE(SUM(type=5),0) AS SIGNED) AS failed,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN use_time END),0) AS SIGNED) AS sum_use_time,
  CAST(COALESCE(MAX(CASE WHEN type=2 THEN use_time END),0) AS SIGNED) AS max_use_time,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN prompt_tokens+completion_tokens END),0) AS SIGNED) AS tokens,
  CAST(COALESCE(SUM(quota),0) AS SIGNED) AS quota,
  CAST(COALESCE(SUM(type=5 AND content REGEXP 'status_code=4'
        AND content NOT LIKE '%timeout%' AND content NOT LIKE '%deadline%'),0) AS SIGNED) AS err_4xx,
  CAST(COALESCE(SUM(type=5 AND content REGEXP 'status_code=5'
        AND content NOT LIKE '%timeout%' AND content NOT LIKE '%deadline%'),0) AS SIGNED) AS err_5xx,
  CAST(COALESCE(SUM(type=5 AND (content LIKE '%timeout%' OR content LIKE '%deadline%')),0) AS SIGNED) AS err_timeout,
  CAST(COALESCE(SUM(type=2 AND use_time<=1),0) AS SIGNED)                 AS lat_1,
  CAST(COALESCE(SUM(type=2 AND use_time>1  AND use_time<=2),0) AS SIGNED) AS lat_2,
  CAST(COALESCE(SUM(type=2 AND use_time>2  AND use_time<=5),0) AS SIGNED) AS lat_5,
  CAST(COALESCE(SUM(type=2 AND use_time>5  AND use_time<=10),0) AS SIGNED) AS lat_10,
  CAST(COALESCE(SUM(type=2 AND use_time>10 AND use_time<=30),0) AS SIGNED) AS lat_30,
  CAST(COALESCE(SUM(type=2 AND use_time>30 AND use_time<=60),0) AS SIGNED) AS lat_60,
  CAST(COALESCE(SUM(type=2 AND use_time>60),0) AS SIGNED)                 AS lat_inf,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN completion_tokens END),0) AS SIGNED) AS completion_tokens,
  CAST(COALESCE(SUM(type=2 AND FRT>0    AND FRT<=500),0)   AS SIGNED) AS ttft_500,
  CAST(COALESCE(SUM(type=2 AND FRT>500  AND FRT<=1000),0)  AS SIGNED) AS ttft_1k,
  CAST(COALESCE(SUM(type=2 AND FRT>1000 AND FRT<=2000),0)  AS SIGNED) AS ttft_2k,
  CAST(COALESCE(SUM(type=2 AND FRT>2000 AND FRT<=5000),0)  AS SIGNED) AS ttft_5k,
  CAST(COALESCE(SUM(type=2 AND FRT>5000 AND FRT<=10000),0) AS SIGNED) AS ttft_10k,
  CAST(COALESCE(SUM(type=2 AND FRT>10000),0)               AS SIGNED) AS ttft_inf,
  CAST(COALESCE(MAX(CASE WHEN type=2 AND FRT>0 THEN FRT END),0) AS SIGNED) AS ttft_max_ms
FROM logs
WHERE created_at >= UNIX_TIMESTAMP() - ? AND type IN (2,5)
GROUP BY bucket, channel_id, model_name, grp`
	q = strings.ReplaceAll(q, "FRT", frt)

	rows, err := m.prodDB.QueryContext(cctx, q, lookbackSec)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var batch []MetricSample
	for rows.Next() {
		var (
			s           MetricSample
			grp         sql.NullString
			e4, e5, eto int64
		)
		if err := rows.Scan(&s.BucketTs, &s.ChannelID, &s.ModelName, &grp,
			&s.Success, &s.Anomaly, &s.Failed, &s.SumUseTime, &s.MaxUseTime, &s.Tokens, &s.Quota,
			&e4, &e5, &eto,
			&s.Lat1, &s.Lat2, &s.Lat5, &s.Lat10, &s.Lat30, &s.Lat60, &s.LatInf,
			&s.CompletionTokens,
			&s.Ttft500, &s.Ttft1k, &s.Ttft2k, &s.Ttft5k, &s.Ttft10k, &s.TtftInf, &s.TtftMaxMs); err != nil {
			return 0, err
		}
		s.Grp = grp.String
		s.Err4xx, s.Err5xx, s.ErrTimeout = e4, e5, eto
		if other := s.Failed - e4 - e5 - eto; other > 0 {
			s.ErrOther = other
		}
		batch = append(batch, s)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if err := m.upsertSamples(batch); err != nil {
		return 0, err
	}
	return len(batch), nil
}

// sampleTokens 按【分钟桶 × 令牌】聚合最近 lookbackSec 秒日志,写本地 token_samples。
// 与主采样隔离:它失败由调用方记日志后继续,绝不影响主监控。
func (m *Monitor) sampleTokens(ctx context.Context, lookbackSec int64) error {
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	q := `
SELECT (created_at DIV 60)*60 AS bucket, token_name,
  CAST(COALESCE(SUM(type=2 AND COALESCE(other,'') NOT REGEXP 'client_gone|scanner_error|panic|ping_fail'),0) AS SIGNED) AS success,
  CAST(COALESCE(SUM(type=2 AND COALESCE(other,'')     REGEXP 'client_gone|scanner_error|panic|ping_fail'),0) AS SIGNED) AS anomaly,
  CAST(COALESCE(SUM(type=5),0) AS SIGNED) AS failed,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN prompt_tokens+completion_tokens END),0) AS SIGNED) AS tokens,
  CAST(COALESCE(SUM(quota),0) AS SIGNED) AS quota
FROM logs
WHERE created_at >= UNIX_TIMESTAMP() - ? AND type IN (2,5)
GROUP BY bucket, token_name`
	rows, err := m.prodDB.QueryContext(cctx, q, lookbackSec)
	if err != nil {
		return err
	}
	defer rows.Close()
	var batch []TokenSample
	for rows.Next() {
		var s TokenSample
		var tn sql.NullString
		if err := rows.Scan(&s.BucketTs, &tn, &s.Success, &s.Anomaly, &s.Failed, &s.Tokens, &s.Quota); err != nil {
			return err
		}
		s.TokenName = tn.String
		batch = append(batch, s)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return m.upsertTokenSamples(batch)
}

// refreshChannels 刷新渠道 id->name 映射,并把渠道健康快照(状态/分组/模型)写入本地库,
// 供对外看板派生"无可用渠道"。低频、失败保留旧值。仅读非密字段(无 key/凭证)。
func (m *Monitor) refreshChannels() {
	if m.prodDB == nil {
		return
	}
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := m.prodDB.QueryContext(cctx, "SELECT id, name, status, `group`, models FROM channels")
	if err != nil {
		return
	}
	defer rows.Close()
	names := map[string]string{}
	var snaps []ChannelSnap
	now := time.Now().Unix()
	prev := m.channelEnabledState() // 上一轮各渠道 (status, enabled_since)
	for rows.Next() {
		var id, status int
		var name, grp, models sql.NullString
		if err := rows.Scan(&id, &name, &status, &grp, &models); err != nil {
			return
		}
		names[strconv.Itoa(id)] = name.String
		p := prev[id] // 不存在则零值(status 0 / since 0),nextEnabledSince 按"新建即启用"处理
		enabledSince := nextEnabledSince(status, p.status, p.since, now)
		snaps = append(snaps, ChannelSnap{ID: id, Status: status, Groups: grp.String, Models: models.String, EnabledSince: enabledSince, UpdatedAt: now})
	}
	if err := rows.Err(); err != nil {
		return
	}
	if len(names) > 0 {
		m.chMu.Lock()
		m.chNames = names
		m.chMu.Unlock()
	}
	if len(snaps) > 0 {
		if err := m.replaceChannelSnaps(snaps, now); err != nil {
			slog.Warn("渠道健康快照写入失败(忽略,不影响监控)", "err", err)
		}
	}
}

// heartbeat 向外部 dead-man 服务(如 healthchecks.io)打一次心跳。
// fire-and-forget:5 秒超时、失败忽略,绝不影响采样。未配置 MONITOR_HEARTBEAT_URL 则空操作。
func (m *Monitor) heartbeat() {
	if m.cfg.HeartbeatURL == "" {
		return
	}
	cl := &http.Client{Timeout: 5 * time.Second}
	resp, err := cl.Get(m.cfg.HeartbeatURL)
	if err != nil {
		return // 失败忽略,绝不影响监控主流程
	}
	resp.Body.Close()
}
