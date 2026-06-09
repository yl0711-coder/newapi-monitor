package monitor

import (
	"context"
	"database/sql"
	"log"
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
	m.refreshChannelNames()

	if h := m.cfg.BackfillHours; h > 0 {
		if n, err := m.sampleWindow(ctx, int64(h)*3600); err != nil {
			log.Printf("[Monitor] 历史回填失败(忽略): %v", err)
		} else {
			log.Printf("[Monitor] 历史回填完成:近 %dh,写入 %d 行采样", h, n)
		}
	}

	interval := time.Duration(m.cfg.SampleSeconds) * time.Second
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}
	m.lastRun.Store(time.Now().Unix()) // 初始化心跳,避免启动初期误报"采样异常"
	m.heartbeat()                      // 启动即对外打一次心跳,让 dead-man 立刻知道"活着"
	go m.loop(ctx, interval)
	log.Printf("[Monitor] 采样器已启动,间隔 %s(生产库仅每周期一条小查询)", interval)
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
				log.Printf("[Monitor] 采样失败(下周期重试): %v", err)
				// 采样失败也评估报警:lastRun 不更新 → 超过 3 周期会触发"采样掉线"
				m.evaluateAlerts(time.Now().Unix())
				continue
			}
			m.lastRun.Store(time.Now().Unix())
			m.heartbeat() // 成功采样后向外部 dead-man 服务打心跳
			m.evaluateAlerts(time.Now().Unix())
			ticks++
			if ticks%(int(600/interval.Seconds())+1) == 0 {
				m.refreshChannelNames()
				if d := m.cfg.RetentionDays; d > 0 {
					cutoff := time.Now().Unix() - int64(d)*86400
					if n, err := m.pruneOlderThan(cutoff); err == nil && n > 0 {
						log.Printf("[Monitor] 清理过期采样 %d 行", n)
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
		if err := rows.Scan(&s.BucketTs, &s.ChannelId, &s.ModelName, &grp,
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

// refreshChannelNames 刷新渠道 id->name 映射(低频,失败则保留旧值)。
func (m *Monitor) refreshChannelNames() {
	if m.prodDB == nil {
		return
	}
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := m.prodDB.QueryContext(cctx, "SELECT id, name FROM channels")
	if err != nil {
		return
	}
	defer rows.Close()
	names := map[string]string{}
	for rows.Next() {
		var id int
		var name sql.NullString
		if err := rows.Scan(&id, &name); err != nil {
			return
		}
		names[strconv.Itoa(id)] = name.String
	}
	if len(names) > 0 {
		m.chMu.Lock()
		m.chNames = names
		m.chMu.Unlock()
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
