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
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

const (
	stabilityHourFinalizeDelaySec  = int64(10 * 60)
	maxStabilityRowsPerHour        = 10000
	maxStabilityRowsPerRange       = 20000
	maxStabilityBackfillAttempts   = 3
	stabilitySourceQueriesPerRange = 2 // detail aggregate + independent source control totals
	stabilityMigrationJobKind      = "classification_migration"
	stabilityManualJobKind         = "manual"
)

var (
	errStabilityBackfillDisabled = errors.New("稳定性历史补数已禁用")
	errStabilityRangeTooLarge    = errors.New("稳定性分段结果超过安全上限")
	errStabilityControlMismatch  = errors.New("稳定性来源控制总数不一致")
)

// StabilityHourIngestState 是长期小时汇总的完整性台账。
// 即使某小时没有任何请求，也会写一条 complete 记录；因此页面能区分
// “确实零流量”和“这个小时尚未采集”，不能再靠数据表 MIN/MAX 猜覆盖率。
type StabilityHourIngestState struct {
	HourTs               int64  `gorm:"primaryKey;autoIncrement:false" json:"hour_ts"`
	Status               string `gorm:"size:16;index" json:"status"`
	Rows                 int64  `json:"rows"`
	Requests             int64  `json:"requests"`
	Tokens               int64  `json:"tokens"`
	Quota                int64  `json:"quota"`
	InternalTestRows     int64  `gorm:"column:internal_test_rows" json:"internal_test_rows"`
	InternalTestRequests int64  `gorm:"column:internal_test_requests" json:"internal_test_requests"`
	InternalTestTokens   int64  `gorm:"column:internal_test_tokens" json:"internal_test_tokens"`
	InternalTestQuota    int64  `gorm:"column:internal_test_quota" json:"internal_test_quota"`
	TrafficClassVersion  int    `gorm:"column:traffic_class_version;index" json:"traffic_class_version"`
	Attempts             int    `json:"attempts"`
	JobID                string `gorm:"size:40;column:job_id;index" json:"job_id,omitempty"`
	UpdatedAt            int64  `gorm:"index" json:"updated_at"`
	CompletedAt          int64  `gorm:"column:completed_at" json:"completed_at,omitempty"`
	LastError            string `gorm:"size:512;column:last_error" json:"last_error,omitempty"`
}

func (s *StabilityHourIngestState) BeforeCreate(_ *gorm.DB) error {
	if s.TrafficClassVersion == 0 {
		s.TrafficClassVersion = userTrafficClassificationVersion
	}
	return nil
}

// StabilityBackfillJob 是可恢复的后台补数任务。任务和浏览器请求解耦；进程重启后
// queued/running 任务会重新扫描缺口并继续，已 complete 的小时自动跳过。
type StabilityBackfillJob struct {
	ID                        string  `gorm:"primaryKey;size:40" json:"id"`
	Kind                      string  `gorm:"size:32;index" json:"kind"`
	FromTs                    int64   `gorm:"index" json:"from_ts"`
	ToTs                      int64   `gorm:"index" json:"to_ts"`
	Status                    string  `gorm:"size:16;index" json:"status"`
	TotalHours                int     `json:"total_hours"`
	CompletedHours            int     `json:"completed_hours"`
	FailedHours               int     `json:"failed_hours"`
	FailedHourTs              []int64 `gorm:"serializer:json;type:text" json:"failed_hour_ts,omitempty"`
	CurrentHourTs             int64   `json:"current_hour_ts,omitempty"`
	CurrentBatchHours         int     `json:"current_batch_hours,omitempty"`
	HealthyChunks             int     `json:"healthy_chunks"`
	SourceQueries             int     `json:"source_queries"`
	AverageQueryMS            int64   `json:"average_query_ms,omitempty"`
	ProgressPercent           float64 `json:"progress_percent"`
	FailedPercent             float64 `json:"failed_percent"`
	ProcessedPercent          float64 `json:"processed_percent"`
	RemainingHours            int     `json:"remaining_hours"`
	EstimatedRemainingSeconds int64   `json:"estimated_remaining_seconds,omitempty"`
	EstimatedFinishAt         int64   `json:"estimated_finish_at,omitempty"`
	StartedAt                 int64   `json:"started_at,omitempty"`
	UpdatedAt                 int64   `gorm:"index" json:"updated_at"`
	FinishedAt                int64   `json:"finished_at,omitempty"`
	LastError                 string  `gorm:"size:512;column:last_error" json:"last_error,omitempty"`
}

// StabilityDataCoverage 是报表口径的数据完整率，不是“筛选结果占全量”的比例。
type StabilityDataCoverage struct {
	FromTs                int64   `json:"from_ts"`
	ToTs                  int64   `json:"to_ts"`
	ExpectedHours         int64   `json:"expected_hours"`
	CompletedHours        int64   `json:"completed_hours"`
	MissingHours          int64   `json:"missing_hours"`
	Percent               float64 `json:"percent"`
	Complete              bool    `json:"complete"`
	EffectiveHours        int64   `json:"effective_hours"`
	EffectiveMissingHours int64   `json:"effective_missing_hours"`
	EffectivePercent      float64 `json:"effective_percent"`
	EffectiveComplete     bool    `json:"effective_complete"`
	LegacyFallbackHours   int64   `json:"legacy_fallback_hours"`
	LatestHourPending     bool    `json:"latest_hour_pending"`
	PendingHourTs         int64   `json:"pending_hour_ts,omitempty"`
}

func finalizedStabilityHourTo(now int64) int64 {
	to := now - stabilityHourFinalizeDelaySec
	if to < 0 {
		return 0
	}
	return to / 3600 * 3600
}

// stabilityRetentionCutoff uses the same finalized-hour boundary as readiness
// and coverage checks. Using raw wall-clock time here would delete the oldest
// retained hour before the coverage window advances, leaving readiness
// permanently one hour short even though every finalized hour was collected.
func stabilityRetentionCutoff(now int64, days int) int64 {
	if days <= 0 {
		return 0
	}
	cutoff := finalizedStabilityHourTo(now) - int64(days)*86400
	if cutoff < 0 {
		return 0
	}
	return cutoff
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
		result.EffectiveComplete = true
		result.EffectivePercent = 100
		return result
	}
	result.ExpectedHours = (toTs - fromTs) / 3600
	var count int64
	if tx := m.storeDB.WithContext(ctx).Model(&StabilityHourIngestState{}).
		Where("hour_ts >= ? AND hour_ts < ? AND status = ? AND traffic_class_version = ?",
			fromTs, toTs, "complete", userTrafficClassificationVersion).Count(&count); tx.Error != nil {
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
	// 报表读取可在 v5 尚未覆盖的小时回退到旧口径，但同一小时一旦有
	// v5 事实或 v5 零流量签收就立即停止回退。这里单独暴露“可展示覆盖”
	// 与严格 v5 覆盖；前者服务页面连续性，后者继续作为迁移/就绪门禁。
	var effective struct {
		Hours       int64
		LegacyHours int64
	}
	effectiveSQL := `SELECT
		COUNT(DISTINCT hs.hour_ts) AS hours,
		COUNT(DISTINCT CASE WHEN COALESCE(hs.traffic_class_version,0) <> ? THEN hs.hour_ts END) AS legacy_hours
	FROM stability_hour_ingest_states hs
	WHERE hs.hour_ts >= ? AND hs.hour_ts < ? AND hs.status = 'complete'
		AND (hs.traffic_class_version = ? OR (
			COALESCE(hs.traffic_class_version,0) <> ?
			AND NOT EXISTS (SELECT 1 FROM stability_hour_ingest_states v5hs
				WHERE v5hs.hour_ts = hs.hour_ts AND v5hs.status = 'complete' AND v5hs.traffic_class_version = ?)
		))`
	v := userTrafficClassificationVersion
	if tx := m.storeDB.WithContext(ctx).Raw(effectiveSQL,
		v, fromTs, toTs, v, v, v).Scan(&effective); tx.Error != nil {
		slog.Warn("读取稳定性兼容覆盖失败", "err", tx.Error)
	} else {
		result.EffectiveHours = effective.Hours
		result.LegacyFallbackHours = effective.LegacyHours
	}
	result.EffectiveMissingHours = result.ExpectedHours - result.EffectiveHours
	if result.EffectiveMissingHours < 0 {
		result.EffectiveMissingHours = 0
	}
	if result.ExpectedHours > 0 {
		result.EffectivePercent = float64(result.EffectiveHours) / float64(result.ExpectedHours) * 100
	}
	result.EffectiveComplete = result.EffectiveMissingHours == 0
	// 仅当查询范围追到当前最新可归档小时，且唯一缺口正好是
	// 最后一小时时，才标记为正常的尾部汇总延迟。历史中间缺口或已失败
	// 的最新小时仍是真实的数据完整性问题，不能被页面降级隐藏。
	if !result.Complete && result.MissingHours == 1 && toTs == finalizedTo {
		latestHourTs := toTs - 3600
		var latest StabilityHourIngestState
		tx := m.storeDB.WithContext(ctx).First(&latest, "hour_ts = ?", latestHourTs)
		if errors.Is(tx.Error, gorm.ErrRecordNotFound) ||
			(tx.Error == nil && (latest.Status == "queued" || latest.Status == "running")) {
			result.LatestHourPending = true
			result.PendingHourTs = latestHourTs
		} else if tx.Error != nil {
			slog.Warn("读取最新稳定性小时状态失败", "hour", latestHourTs, "err", tx.Error)
		}
	}
	return result
}

func stabilityHourSQL() string {
	testPredicate := channelTestLogPredicateSQL()
	testOrigin := channelTestSeriesSQL(testPredicate)
	testScope := channelTestScopeSQL(testPredicate)
	testResult := channelTestResultSQL(testPredicate)
	testCostBasis := channelTestCostBasisSQL(testPredicate)
	q := `
SELECT channel_id, model_name, ` + "`group`" + ` AS grp,
  CASE WHEN ` + testPredicate + ` THEN 1 ELSE 0 END AS is_channel_test,
  ` + testOrigin + ` AS channel_test_origin,
  ` + testScope + ` AS channel_test_scope,
  ` + testCostBasis + ` AS channel_test_cost_basis,
  CAST(COALESCE(SUM(CASE WHEN ` + testPredicate + ` THEN ` + testResult + `='success' ELSE type=2 AND NOT {{ANOM}} END),0) AS SIGNED) AS success,
  CAST(COALESCE(SUM(CASE WHEN ` + testPredicate + ` THEN ` + testResult + `='anomaly' ELSE type=2 AND {{ANOM}} END),0) AS SIGNED) AS anomaly,
  CAST(COALESCE(SUM(CASE WHEN ` + testPredicate + ` THEN ` + testResult + `='failed' ELSE type=5 END),0) AS SIGNED) AS failed,
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
GROUP BY channel_id, model_name, grp, is_channel_test, channel_test_origin, channel_test_scope, channel_test_cost_basis`
	return expandAnomalyPredicates(q)
}

// stabilityRangeSQL keeps exactly the same business dimensions as the
// single-hour query, but lets MySQL aggregate several complete hours in one
// round trip.  LIMIT is deliberately cap+1: seeing the sentinel row makes the
// caller split the range instead of accepting a truncated chunk.
func stabilityRangeSQL() string {
	return stabilityRangeSQLWithMaxExecution(8000)
}

func stabilityRangeSQLWithMaxExecution(maxExecutionMS int) string {
	maxExecutionMS = normalizeStabilityMaxExecutionMS(maxExecutionMS)
	q := stabilityHourSQL()
	q = strings.Replace(q, "SELECT channel_id,", fmt.Sprintf("SELECT /*+ MAX_EXECUTION_TIME(%d) */ (created_at DIV 3600) * 3600 AS hour_ts, channel_id,", maxExecutionMS), 1)
	q = strings.Replace(q, "GROUP BY channel_id,", "GROUP BY hour_ts, channel_id,", 1)
	return q + fmt.Sprintf("\nLIMIT %d", maxStabilityRowsPerRange+1)
}

func stabilityRangeControlSQL() string {
	return stabilityRangeControlSQLWithMaxExecution(8000)
}

func stabilityRangeControlSQLWithMaxExecution(maxExecutionMS int) string {
	maxExecutionMS = normalizeStabilityMaxExecutionMS(maxExecutionMS)
	testPredicate := channelTestLogPredicateSQL()
	testExpr := "COALESCE((" + testPredicate + "),0)"
	q := fmt.Sprintf(`
SELECT /*+ MAX_EXECUTION_TIME(%d) */ (created_at DIV 3600) * 3600 AS hour_ts,
  CAST(COALESCE(SUM(CASE WHEN NOT (`+testExpr+`) THEN 1 ELSE 0 END),0) AS SIGNED) AS user_requests,
  CAST(COALESCE(SUM(CASE WHEN NOT (`+testExpr+`) AND type=2 THEN prompt_tokens+completion_tokens ELSE 0 END),0) AS SIGNED) AS user_tokens,
  CAST(COALESCE(SUM(CASE WHEN NOT (`+testExpr+`) AND type=2 THEN quota ELSE 0 END),0) AS SIGNED) AS user_quota,
  CAST(COALESCE(SUM(CASE WHEN (`+testExpr+`) THEN 1 ELSE 0 END),0) AS SIGNED) AS internal_requests,
  CAST(COALESCE(SUM(CASE WHEN (`+testExpr+`) AND type=2 THEN prompt_tokens+completion_tokens ELSE 0 END),0) AS SIGNED) AS internal_tokens,
  CAST(COALESCE(SUM(CASE WHEN (`+testExpr+`) AND type=2 THEN quota ELSE 0 END),0) AS SIGNED) AS internal_quota
FROM logs
WHERE created_at >= ? AND created_at < ? AND type IN (2,5)
GROUP BY hour_ts
LIMIT 13`, maxExecutionMS)
	return q
}

func normalizeStabilityMaxExecutionMS(ms int) int {
	if ms < 1000 {
		return 8000
	}
	if ms > 8000 {
		return 8000
	}
	return ms
}

// The SQLite variants keep fake-production acceptance semantically equivalent
// without relying on MySQL's REGEXP operator. The JSON expressions themselves
// are shared with production and therefore still receive execution coverage.
func stabilityRangeSQLiteSQLWithMaxExecution(maxExecutionMS int) string {
	q := stabilityRangeSQLWithMaxExecution(maxExecutionMS)
	q = strings.ReplaceAll(q, "(created_at DIV 3600) * 3600", "CAST(created_at / 3600 AS INTEGER) * 3600")
	return stabilitySQLiteSQL(q)
}

func stabilityRangeControlSQLiteSQLWithMaxExecution(maxExecutionMS int) string {
	q := stabilityRangeControlSQLWithMaxExecution(maxExecutionMS)
	q = strings.ReplaceAll(q, "(created_at DIV 3600) * 3600", "CAST(created_at / 3600 AS INTEGER) * 3600")
	return stabilitySQLiteSQL(q)
}

func stabilitySQLiteSQL(q string) string {
	sqliteModelClass := `(LOWER(COALESCE(model_name,'')) NOT LIKE '%embed%' AND ` +
		`LOWER(COALESCE(model_name,'')) NOT LIKE '%rerank%' AND ` +
		`LOWER(COALESCE(model_name,'')) NOT LIKE '%bge-%' AND ` +
		`LOWER(COALESCE(model_name,'')) NOT LIKE '%m3e%' AND ` +
		`LOWER(COALESCE(model_name,'')) NOT LIKE '%image%' AND ` +
		`LOWER(COALESCE(model_name,'')) NOT LIKE '%seedream%' AND ` +
		`LOWER(COALESCE(model_name,'')) NOT LIKE '%seedance%')`
	q = strings.ReplaceAll(q,
		`model_name NOT REGEXP 'embed|rerank|bge-|m3e|image|seedream|seedance'`, sqliteModelClass)
	q = strings.ReplaceAll(q, `content REGEXP 'status_code=4'`, `content LIKE '%status_code=4%'`)
	q = strings.ReplaceAll(q, `content REGEXP 'status_code=5'`, `content LIKE '%status_code=5%'`)
	return q
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

func (m *Monitor) stabilityServerMaxExecutionMS() int {
	return normalizeStabilityMaxExecutionMS(m.cfg.StabilityBackfillServerMaxExecutionMS)
}

// fetchStabilityHour 只读生产库的一个完整小时。它不写分钟表，返回的数据量受
// 渠道×模型×分组基数限制，避免 90 天补数把本地 SQLite 放大 60 倍。
type stabilityHourTraffic struct {
	Users         []StabilityHourSample
	InternalTests []ChannelTestHourSample
}

type stabilityRangeResult struct {
	Hours         map[int64]stabilityHourTraffic
	Rows          int
	SourceQueries int
	QueryDuration time.Duration
}

type stabilityRangeFetcher func(context.Context, int64, int64) (stabilityRangeResult, error)

type stabilitySourceControl struct {
	UserRequests, UserTokens, UserQuota             int64
	InternalRequests, InternalTokens, InternalQuota int64
}

func (m *Monitor) queryStabilitySource(ctx context.Context, query string, args []any, scan func(*sql.Rows) error) (duration time.Duration, retErr error) {
	release, err := m.acquireBackgroundSourceLow(ctx)
	if err != nil {
		return 0, err
	}
	queryStarted := time.Now()
	defer func() {
		duration = time.Since(queryStarted)
		m.deferBackgroundSourceStart(m.stabilitySourceCooldown(duration))
		release()
	}()

	cctx, cancel := context.WithTimeout(ctx, m.stabilityQueryTimeout())
	defer cancel()
	rows, err := m.prodDB.QueryContext(cctx, query, args...)
	if err != nil {
		m.reportSourceQueryError(err)
		if cctx.Err() != nil && ctx.Err() == nil {
			return 0, cctx.Err()
		}
		return 0, err
	}
	defer rows.Close()
	if err := scan(rows); err != nil {
		if cctx.Err() != nil && ctx.Err() == nil {
			return 0, cctx.Err()
		}
		return 0, err
	}
	if err := rows.Err(); err != nil {
		m.reportSourceQueryError(err)
		if cctx.Err() != nil && ctx.Err() == nil {
			return 0, cctx.Err()
		}
		return 0, err
	}
	return 0, nil
}

func (m *Monitor) fetchStabilityRange(ctx context.Context, fromTs, toTs int64) (out stabilityRangeResult, retErr error) {
	out.Hours = make(map[int64]stabilityHourTraffic)
	if fromTs%3600 != 0 || toTs%3600 != 0 || toTs <= fromTs || toTs-fromTs > 12*3600 {
		return out, fmt.Errorf("稳定性分段范围非法: [%d,%d)", fromTs, toTs)
	}
	q := stabilityRangeSQLWithMaxExecution(m.stabilityServerMaxExecutionMS())
	if m.usageDayExpr != "" {
		q = stabilityRangeSQLiteSQLWithMaxExecution(m.stabilityServerMaxExecutionMS())
	}
	perHourRows := make(map[int64]int)
	out.SourceQueries++
	duration, err := m.queryStabilitySource(ctx, q, []any{fromTs, toTs}, func(rows *sql.Rows) error {
		for rows.Next() {
			var row StabilityHourSample
			var hourTs int64
			var group sql.NullString
			var testOrigin sql.NullString
			var testScope sql.NullString
			var testCostBasis sql.NullString
			var isChannelTest int64
			var err4xx, err5xx, errTimeout int64
			if err := rows.Scan(&hourTs, &row.ChannelID, &row.ModelName, &group, &isChannelTest, &testOrigin, &testScope, &testCostBasis,
				&row.Success, &row.Anomaly, &row.Failed,
				&row.AnomalyBilled, &row.AnomalyFree, &row.AnomalyStream, &row.AnomalyQuota,
				&row.SumUseTime, &row.MaxUseTime, &row.Tokens, &row.Quota,
				&err4xx, &err5xx, &errTimeout); err != nil {
				return err
			}
			if hourTs < fromTs || hourTs >= toTs || hourTs%3600 != 0 {
				return fmt.Errorf("来源返回越界小时 %d，不在 [%d,%d)", hourTs, fromTs, toTs)
			}
			traffic := out.Hours[hourTs]
			if traffic.Users == nil {
				traffic.Users = make([]StabilityHourSample, 0, 128)
				traffic.InternalTests = make([]ChannelTestHourSample, 0, 16)
			}
			row.HourTs, row.Grp, row.TrafficClassVersion = hourTs, group.String, userTrafficClassificationVersion
			row.Err4xx, row.Err5xx, row.ErrTimeout = err4xx, err5xx, errTimeout
			if other := row.Failed - err4xx - err5xx - errTimeout; other > 0 {
				row.ErrOther = other
			}
			if isChannelTest != 0 {
				origin := testOrigin.String
				if origin != "legacy_base" && origin != "legacy_tiered" {
					origin = "legacy_base"
				}
				scope := testScope.String
				if scope != "single" && scope != "all" {
					scope = "legacy"
				}
				costBasis := testCostBasis.String
				switch costBasis {
				case "legacy_assumed_base", "legacy_after_group":
				case "":
					costBasis = "legacy_assumed_base"
				default:
					costBasis = "unsupported"
				}
				traffic.InternalTests = append(traffic.InternalTests, ChannelTestHourSample{
					HourTs: hourTs, ChannelID: row.ChannelID, ModelName: row.ModelName, Grp: row.Grp, Origin: origin,
					Scope: scope, CostBasis: costBasis, TrafficClassVersion: userTrafficClassificationVersion,
					Requests: row.Success + row.Anomaly + row.Failed, Success: row.Success, Anomaly: row.Anomaly, Failed: row.Failed,
					Tokens: row.Tokens, Quota: row.Quota, SumUseTime: row.SumUseTime, MaxUseTime: row.MaxUseTime,
				})
			} else {
				traffic.Users = append(traffic.Users, row)
			}
			out.Hours[hourTs] = traffic
			out.Rows++
			perHourRows[hourTs]++
			if out.Rows > maxStabilityRowsPerRange {
				return fmt.Errorf("%w %d", errStabilityRangeTooLarge, maxStabilityRowsPerRange)
			}
			if perHourRows[hourTs] > maxStabilityRowsPerHour {
				return fmt.Errorf("%w: 单小时 %d 维度超过 %d", errStabilityRangeTooLarge, hourTs, maxStabilityRowsPerHour)
			}
		}
		return nil
	})
	out.QueryDuration += duration
	if err != nil {
		return out, err
	}

	controlSQL := stabilityRangeControlSQLWithMaxExecution(m.stabilityServerMaxExecutionMS())
	if m.usageDayExpr != "" {
		controlSQL = stabilityRangeControlSQLiteSQLWithMaxExecution(m.stabilityServerMaxExecutionMS())
	}
	controls := make(map[int64]stabilitySourceControl)
	out.SourceQueries++
	duration, err = m.queryStabilitySource(ctx, controlSQL, []any{fromTs, toTs}, func(rows *sql.Rows) error {
		for rows.Next() {
			var hourTs int64
			var control stabilitySourceControl
			if err := rows.Scan(&hourTs, &control.UserRequests, &control.UserTokens, &control.UserQuota,
				&control.InternalRequests, &control.InternalTokens, &control.InternalQuota); err != nil {
				return err
			}
			if hourTs < fromTs || hourTs >= toTs || hourTs%3600 != 0 {
				return fmt.Errorf("来源控制查询返回越界小时 %d", hourTs)
			}
			controls[hourTs] = control
			if len(controls) > 12 {
				return fmt.Errorf("%w: 控制查询小时数超过 12", errStabilityRangeTooLarge)
			}
		}
		return nil
	})
	out.QueryDuration += duration
	if err != nil {
		return out, err
	}
	if err := verifyStabilitySourceControls(fromTs, toTs, out.Hours, controls); err != nil {
		return out, err
	}
	return out, nil
}

func verifyStabilitySourceControls(fromTs, toTs int64, hours map[int64]stabilityHourTraffic, controls map[int64]stabilitySourceControl) error {
	for hour := fromTs; hour < toTs; hour += 3600 {
		traffic := hours[hour]
		userRequests, userTokens, userQuota := stabilityHourTotals(traffic.Users)
		internalRequests, internalTokens, internalQuota := channelTestHourTotals(traffic.InternalTests)
		control := controls[hour]
		if userRequests != control.UserRequests || userTokens != control.UserTokens || userQuota != control.UserQuota ||
			internalRequests != control.InternalRequests || internalTokens != control.InternalTokens || internalQuota != control.InternalQuota {
			return fmt.Errorf("%w: hour=%d detail=%d/%d/%d+%d/%d/%d control=%d/%d/%d+%d/%d/%d",
				errStabilityControlMismatch, hour,
				userRequests, userTokens, userQuota, internalRequests, internalTokens, internalQuota,
				control.UserRequests, control.UserTokens, control.UserQuota,
				control.InternalRequests, control.InternalTokens, control.InternalQuota)
		}
	}
	return nil
}

func (m *Monitor) fetchStabilityHour(ctx context.Context, hourTs int64) (stabilityHourTraffic, error) {
	result, err := m.fetchStabilityRange(ctx, hourTs, hourTs+3600)
	if err != nil {
		return stabilityHourTraffic{}, err
	}
	return result.Hours[hourTs], nil
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
func channelTestHourTotals(rows []ChannelTestHourSample) (requests, tokens, quota int64) {
	for _, row := range rows {
		requests += row.Requests
		tokens += row.Tokens
		quota += row.Quota
	}
	return
}

func (m *Monitor) replaceStabilityHour(hourTs int64, rows []StabilityHourSample, state StabilityHourIngestState) error {
	return m.replaceStabilityHourTraffic(hourTs, rows, nil, state)
}

func (m *Monitor) replaceStabilityHourTraffic(hourTs int64, rows []StabilityHourSample, testRows []ChannelTestHourSample, state StabilityHourIngestState) error {
	for i := range testRows {
		// Compatibility for callers/imported snapshots created before explicit
		// success/failed columns existed. Fresh source rows always carry all three
		// counters; legacy rows can only recover requests-anomaly as success.
		if testRows[i].Requests > 0 && testRows[i].Success+testRows[i].Anomaly+testRows[i].Failed == 0 {
			testRows[i].Success = testRows[i].Requests - testRows[i].Anomaly
		}
		if testRows[i].CostBasis == "" {
			testRows[i].CostBasis = "legacy_assumed_base"
		}
		testRows[i].Origin = "legacy_base"
		if testRows[i].CostBasis == "legacy_after_group" {
			testRows[i].Origin = "legacy_tiered"
		}
		testRows[i].Scope = "legacy"
		if testRows[i].Requests != testRows[i].Success+testRows[i].Anomaly+testRows[i].Failed {
			return fmt.Errorf("渠道测试结果分类不完整: origin=%s requests=%d success=%d anomaly=%d failed=%d",
				testRows[i].Origin, testRows[i].Requests, testRows[i].Success, testRows[i].Anomaly, testRows[i].Failed)
		}
		testRows[i].TrafficClassVersion = userTrafficClassificationVersion
	}
	expectedRequests, expectedTokens, expectedQuota := stabilityHourTotals(rows)
	expectedTestRequests, expectedTestTokens, expectedTestQuota := channelTestHourTotals(testRows)
	for i := range rows {
		rows[i].TrafficClassVersion = userTrafficClassificationVersion
	}
	return m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("hour_ts = ?", hourTs).Delete(&StabilityHourSample{}).Error; err != nil {
			return err
		}
		if len(rows) > 0 {
			if err := tx.CreateInBatches(rows, 200).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("hour_ts = ?", hourTs).Delete(&ChannelTestHourSample{}).Error; err != nil {
			return err
		}
		if len(testRows) > 0 {
			if err := tx.CreateInBatches(testRows, 200).Error; err != nil {
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
		var localTests struct{ Requests, Tokens, Quota int64 }
		if err := tx.Raw(`SELECT COALESCE(SUM(requests),0) requests,
			COALESCE(SUM(tokens),0) tokens,COALESCE(SUM(quota),0) quota
			FROM channel_test_hour_samples WHERE hour_ts=?`, hourTs).Scan(&localTests).Error; err != nil {
			return err
		}
		if localTests.Requests != expectedTestRequests || localTests.Tokens != expectedTestTokens || localTests.Quota != expectedTestQuota {
			return fmt.Errorf("本地渠道测试控制总数不一致: got=%d/%d/%d want=%d/%d/%d",
				localTests.Requests, localTests.Tokens, localTests.Quota,
				expectedTestRequests, expectedTestTokens, expectedTestQuota)
		}
		state.HourTs = hourTs
		state.Status = "complete"
		state.Rows = int64(len(rows))
		state.Requests, state.Tokens, state.Quota = expectedRequests, expectedTokens, expectedQuota
		state.InternalTestRows = int64(len(testRows))
		state.InternalTestRequests, state.InternalTestTokens, state.InternalTestQuota = expectedTestRequests, expectedTestTokens, expectedTestQuota
		state.TrafficClassVersion = userTrafficClassificationVersion
		state.CompletedAt, state.UpdatedAt, state.LastError = time.Now().Unix(), time.Now().Unix(), ""
		return tx.Save(&state).Error
	})
}

func (m *Monitor) markStabilityHourAttempt(hourTs int64, jobID, status, lastError string) (StabilityHourIngestState, error) {
	var state StabilityHourIngestState
	if err := m.storeDB.First(&state, "hour_ts = ?", hourTs).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return state, err
	}
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
		return state, err
	}
	return state, nil
}

func (m *Monitor) backfillOneStabilityHour(ctx context.Context, hourTs int64, jobID string) error {
	var lastErr error
	for attempt := 1; attempt <= maxStabilityBackfillAttempts; attempt++ {
		state, err := m.markStabilityHourAttempt(hourTs, jobID, "running", "")
		if err != nil {
			return err
		}
		traffic, err := m.fetchStabilityHour(ctx, hourTs)
		if err == nil {
			return m.replaceStabilityHourTraffic(hourTs, traffic.Users, traffic.InternalTests, state)
		}
		lastErr = err
		if stabilityBackfillInterrupted(ctx, err) {
			_, _ = m.markStabilityHourAttempt(hourTs, jobID, "queued", "服务停止，等待续跑")
			return err
		}
		if _, stateErr := m.markStabilityHourAttempt(hourTs, jobID, "failed", err.Error()); stateErr != nil {
			return stateErr
		}
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
	if ms < 0 { // tests may explicitly disable sleeping; LoadSettings never emits a negative default.
		return 0
	}
	if ms < 250 {
		ms = 2000
	}
	if ms > 30000 {
		ms = 30000
	}
	return time.Duration(ms) * time.Millisecond
}

func (m *Monitor) stabilitySourceDutyPercent() int {
	pct := m.cfg.StabilityBackfillSourceDutyPercent
	if pct <= 0 || pct > 100 {
		pct = 20
	}
	return pct
}

func (m *Monitor) stabilitySourceCooldown(queryDuration time.Duration) time.Duration {
	pct := m.stabilitySourceDutyPercent()
	delay := m.backfillDelay()
	if pct < 100 && queryDuration > 0 {
		dutyDelay := queryDuration * time.Duration(100-pct) / time.Duration(pct)
		if dutyDelay > delay {
			delay = dutyDelay
		}
	}
	return delay
}

func nextStabilityBatchHours(current int) int {
	switch current {
	case 1:
		return 2
	case 2:
		return 4
	case 4:
		return 6
	default:
		return 12
	}
}

func previousStabilityBatchHours(current int) int {
	switch {
	case current >= 12:
		return 6
	case current >= 6:
		return 4
	case current >= 4:
		return 2
	default:
		return 1
	}
}

func stabilityRangeShouldFallback(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errStabilityRangeTooLarge) {
		return true
	}
	// MAX_EXECUTION_TIME is enforced by MySQL itself. go-sql-driver/mysql
	// returns server error 3024 instead of context.DeadlineExceeded, so it must
	// take the same adaptive split path; otherwise one slow 12-hour chunk would
	// pause the whole migration instead of degrading to a smaller range.
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 3024
}

func adaptStabilityBatchAfterSuccess(batchHours, healthyChunks int, result stabilityRangeResult) (int, int) {
	queryCount := result.SourceQueries
	if queryCount < 1 {
		queryCount = 1
	}
	querySlow := result.QueryDuration > time.Duration(queryCount)*2*time.Second
	rowsNearLimit := result.Rows >= maxStabilityRowsPerRange*8/10
	if querySlow || rowsNearLimit {
		return previousStabilityBatchHours(batchHours), 0
	}
	healthyChunks++
	if healthyChunks >= 3 {
		return nextStabilityBatchHours(batchHours), 0
	}
	return batchHours, healthyChunks
}

func (m *Monitor) startStabilityBackfill(fromTs, toTs int64) (*StabilityBackfillJob, error) {
	return m.startStabilityBackfillKind(fromTs, toTs, stabilityManualJobKind)
}

func (m *Monitor) startStabilityBackfillKind(fromTs, toTs int64, kind string) (*StabilityBackfillJob, error) {
	if !m.cfg.StabilityBackfillEnabled {
		return nil, errStabilityBackfillDisabled
	}
	if m.prodDB == nil {
		return nil, fmt.Errorf("未配置生产库只读连接")
	}
	if !m.sourceAccessAllowed() {
		return nil, errSourceNotReady
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
	job := &StabilityBackfillJob{
		ID: id, Kind: kind, FromTs: fromTs, ToTs: toTs, Status: "queued",
		TotalHours: int((toTs - fromTs) / 3600), CurrentBatchHours: 2, UpdatedAt: now,
	}
	m.refreshStabilityJobProgress(job, 2, now)
	if err := m.storeDB.Create(job).Error; err != nil {
		m.stabilityBackfillRunning.Store(false)
		return nil, err
	}
	sourceCtx := m.sourceTaskContext()
	if !goSourceEpoch(sourceCtx, func(ctx context.Context) { m.runStabilityBackfill(ctx, job.ID) }) {
		m.stabilityBackfillRunning.Store(false)
		return nil, errSourceNotReady
	}
	return job, nil
}

func (m *Monitor) runStabilityBackfill(ctx context.Context, jobID string) {
	m.runStabilityBackfillWithFetcher(ctx, jobID, m.fetchStabilityRange)
}

func (m *Monitor) runStabilityBackfillWithFetcher(ctx context.Context, jobID string, fetch stabilityRangeFetcher) {
	defer m.stabilityBackfillRunning.Store(false)
	var job StabilityBackfillJob
	if err := m.storeDB.First(&job, "id = ?", jobID).Error; err != nil {
		slog.Warn("读取稳定性补数任务失败", "job_id", jobID, "err", err)
		return
	}
	if job.Kind == "" {
		job.Kind = stabilityManualJobKind
	}
	if job.TotalHours <= 0 {
		job.TotalHours = int((job.ToTs - job.FromTs) / 3600)
	}
	known, err := m.completeStabilityHours(job.FromTs, job.ToTs)
	if err != nil {
		job.Status, job.LastError, job.UpdatedAt = "paused", clip(err.Error(), 512), time.Now().Unix()
		m.saveStabilityBackfillJob(&job, "读取已完成小时失败")
		return
	}
	failed := make(map[int64]bool, len(job.FailedHourTs))
	cleanFailed := job.FailedHourTs[:0]
	for _, hour := range job.FailedHourTs {
		if hour < job.FromTs || hour >= job.ToTs || known[hour] || failed[hour] {
			continue
		}
		failed[hour] = true
		cleanFailed = append(cleanFailed, hour)
	}
	job.FailedHourTs = cleanFailed
	now := time.Now().Unix()
	job.Status, job.UpdatedAt = "running", now
	if job.StartedAt == 0 {
		job.StartedAt = now
	}
	job.CompletedHours, job.FailedHours = len(known), len(failed)
	job.LastError, job.FinishedAt = "", 0
	batchHours := job.CurrentBatchHours
	if batchHours != 1 && batchHours != 2 && batchHours != 4 && batchHours != 6 && batchHours != 12 {
		batchHours = 2
	}
	m.refreshStabilityJobProgress(&job, batchHours, now)
	if !m.saveStabilityBackfillJob(&job, "标记运行") {
		return
	}
	singleHourAttempts := make(map[int64]int)
	for {
		select {
		case <-ctx.Done():
			job.Status, job.LastError, job.UpdatedAt = "queued", "服务停止，等待下次启动续跑", time.Now().Unix()
			m.saveStabilityBackfillJob(&job, "保存停止进度")
			return
		default:
		}
		fromTs, toTs, ok := nextMissingStabilityRange(job.FromTs, job.ToTs, batchHours, known, failed)
		if !ok {
			break
		}
		job.CurrentHourTs = toTs - 3600
		job.CurrentBatchHours = int((toTs - fromTs) / 3600)
		job.UpdatedAt = time.Now().Unix()
		m.refreshStabilityJobProgress(&job, batchHours, job.UpdatedAt)
		if !m.saveStabilityBackfillJob(&job, "保存当前分段") {
			return
		}

		result, fetchErr := fetch(ctx, fromTs, toTs)
		m.recordStabilitySourceQuery(&job, result.QueryDuration, result.SourceQueries)
		if fetchErr != nil {
			if stabilityBackfillInterrupted(ctx, fetchErr) {
				job.Status, job.LastError, job.UpdatedAt = "queued", "服务停止，等待下次启动续跑", time.Now().Unix()
				m.refreshStabilityJobProgress(&job, batchHours, job.UpdatedAt)
				m.saveStabilityBackfillJob(&job, "保存中断进度")
				return
			}
			if stabilityRangeShouldFallback(fetchErr) {
				if toTs-fromTs > 3600 {
					batchHours = previousStabilityBatchHours(int((toTs - fromTs) / 3600))
					job.CurrentBatchHours = batchHours
					job.HealthyChunks = 0
					job.LastError = clip(fmt.Sprintf("分段已降级为 %d 小时: %v", batchHours, fetchErr), 512)
					job.UpdatedAt = time.Now().Unix()
					m.refreshStabilityJobProgress(&job, batchHours, job.UpdatedAt)
					if !m.saveStabilityBackfillJob(&job, "保存分段降级") {
						return
					}
					continue
				}
				// A pathological hour must not hold thousands of healthy hours
				// hostage.  Persist it as isolated and continue; the final job is
				// partial and exposes the exact retry list.
				hour := fromTs
				singleHourAttempts[hour]++
				if errors.Is(fetchErr, context.DeadlineExceeded) && singleHourAttempts[hour] < maxStabilityBackfillAttempts {
					job.LastError = clip(fmt.Sprintf("单小时查询第 %d 次超时，继续受控重试: %v", singleHourAttempts[hour], fetchErr), 512)
					job.UpdatedAt = time.Now().Unix()
					m.refreshStabilityJobProgress(&job, 1, job.UpdatedAt)
					if !m.saveStabilityBackfillJob(&job, "保存单小时重试") {
						return
					}
					continue
				}
				if _, stateErr := m.markStabilityHourAttempt(hour, job.ID, "failed", fetchErr.Error()); stateErr != nil {
					job.Status, job.LastError, job.UpdatedAt = "paused", clip(stateErr.Error(), 512), time.Now().Unix()
					m.saveStabilityBackfillJob(&job, "保存隔离小时失败")
					return
				}
				if !failed[hour] {
					failed[hour] = true
					job.FailedHourTs = append(job.FailedHourTs, hour)
				}
				job.FailedHours = len(failed)
				job.LastError = clip(fetchErr.Error(), 512)
				batchHours = 2
				job.HealthyChunks = 0
				job.UpdatedAt = time.Now().Unix()
				m.refreshStabilityJobProgress(&job, batchHours, job.UpdatedAt)
				if !m.saveStabilityBackfillJob(&job, "保存隔离小时") {
					return
				}
				continue
			}
			job.Status, job.LastError, job.UpdatedAt = "paused", clip(fetchErr.Error(), 512), time.Now().Unix()
			m.refreshStabilityJobProgress(&job, batchHours, job.UpdatedAt)
			m.saveStabilityBackfillJob(&job, "保存来源异常暂停状态")
			slog.Warn("稳定性历史补数暂停", "job_id", job.ID, "from", fromTs, "to", toTs, "err", fetchErr)
			return
		}

		for hour := toTs - 3600; hour >= fromTs; hour -= 3600 {
			state, stateErr := m.markStabilityHourAttempt(hour, job.ID, "running", "")
			if stateErr != nil {
				job.Status, job.LastError, job.UpdatedAt = "paused", clip(stateErr.Error(), 512), time.Now().Unix()
				m.saveStabilityBackfillJob(&job, "保存小时运行状态失败")
				return
			}
			traffic := result.Hours[hour]
			if err := m.replaceStabilityHourTraffic(hour, traffic.Users, traffic.InternalTests, state); err != nil {
				_, _ = m.markStabilityHourAttempt(hour, job.ID, "failed", err.Error())
				job.Status, job.LastError, job.UpdatedAt = "paused", clip(err.Error(), 512), time.Now().Unix()
				m.saveStabilityBackfillJob(&job, "本地原子写入失败")
				return
			}
			known[hour] = true
			delete(singleHourAttempts, hour)
		}
		job.CompletedHours = len(known)
		job.LastError = ""
		batchHours, job.HealthyChunks = adaptStabilityBatchAfterSuccess(batchHours, job.HealthyChunks, result)
		job.CurrentBatchHours = batchHours
		job.UpdatedAt = time.Now().Unix()
		m.refreshStabilityJobProgress(&job, batchHours, job.UpdatedAt)
		if !m.saveStabilityBackfillJob(&job, "保存分段完成进度") {
			return
		}
	}
	// Give isolated pathological hours one final pass after all healthy ranges
	// have been published.  A transient timeout therefore cannot become a
	// permanent hole merely because it happened during the first sweep.
	if len(failed) > 0 {
		retryHours := append([]int64(nil), job.FailedHourTs...)
		for _, hour := range retryHours {
			if !failed[hour] {
				continue
			}
			result, retryErr := fetch(ctx, hour, hour+3600)
			m.recordStabilitySourceQuery(&job, result.QueryDuration, result.SourceQueries)
			if retryErr != nil {
				if stabilityBackfillInterrupted(ctx, retryErr) {
					job.Status, job.LastError, job.UpdatedAt = "queued", "服务停止，等待下次启动续跑", time.Now().Unix()
					m.saveStabilityBackfillJob(&job, "保存隔离小时复核中断")
					return
				}
				_, _ = m.markStabilityHourAttempt(hour, job.ID, "failed", retryErr.Error())
				job.LastError = clip(retryErr.Error(), 512)
				continue
			}
			state, stateErr := m.markStabilityHourAttempt(hour, job.ID, "running", "")
			if stateErr != nil {
				job.Status, job.LastError, job.UpdatedAt = "paused", clip(stateErr.Error(), 512), time.Now().Unix()
				m.saveStabilityBackfillJob(&job, "保存隔离小时复核状态失败")
				return
			}
			traffic := result.Hours[hour]
			if err := m.replaceStabilityHourTraffic(hour, traffic.Users, traffic.InternalTests, state); err != nil {
				_, _ = m.markStabilityHourAttempt(hour, job.ID, "failed", err.Error())
				job.Status, job.LastError, job.UpdatedAt = "paused", clip(err.Error(), 512), time.Now().Unix()
				m.saveStabilityBackfillJob(&job, "隔离小时复核写入失败")
				return
			}
			known[hour] = true
			delete(failed, hour)
			job.CompletedHours, job.FailedHours = len(known), len(failed)
			job.UpdatedAt = time.Now().Unix()
			m.refreshStabilityJobProgress(&job, 1, job.UpdatedAt)
			if !m.saveStabilityBackfillJob(&job, "保存隔离小时复核进度") {
				return
			}
		}
		job.FailedHourTs = job.FailedHourTs[:0]
		for _, hour := range retryHours {
			if failed[hour] {
				job.FailedHourTs = append(job.FailedHourTs, hour)
			}
		}
		job.FailedHours = len(failed)
	}

	job.Status = "complete"
	if len(failed) > 0 {
		job.Status = "partial"
	}
	job.CurrentHourTs, job.CurrentBatchHours = 0, 0
	job.UpdatedAt, job.FinishedAt = time.Now().Unix(), time.Now().Unix()
	if len(failed) == 0 {
		job.LastError = ""
	}
	m.refreshStabilityJobProgress(&job, 1, job.UpdatedAt)
	if !m.saveStabilityBackfillJob(&job, "保存完成状态") {
		return
	}
	slog.Info("稳定性历史补数结束", "job_id", job.ID, "status", job.Status,
		"complete_hours", job.CompletedHours, "failed_hours", job.FailedHours, "source_queries", job.SourceQueries)
}

func (m *Monitor) completeStabilityHours(fromTs, toTs int64) (map[int64]bool, error) {
	var hours []int64
	if err := m.storeDB.Model(&StabilityHourIngestState{}).
		Where("hour_ts >= ? AND hour_ts < ? AND status = ? AND traffic_class_version = ?",
			fromTs, toTs, "complete", userTrafficClassificationVersion).Pluck("hour_ts", &hours).Error; err != nil {
		return nil, err
	}
	known := make(map[int64]bool, len(hours))
	for _, hour := range hours {
		known[hour] = true
	}
	return known, nil
}

func nextMissingStabilityRange(fromTs, toTs int64, batchHours int, complete, failed map[int64]bool) (int64, int64, bool) {
	if batchHours < 1 {
		batchHours = 1
	}
	if batchHours > 12 {
		batchHours = 12
	}
	for hour := toTs - 3600; hour >= fromTs; hour -= 3600 {
		if complete[hour] || failed[hour] {
			continue
		}
		start := hour
		for n := 1; n < batchHours; n++ {
			candidate := start - 3600
			if candidate < fromTs || complete[candidate] || failed[candidate] {
				break
			}
			start = candidate
		}
		return start, hour + 3600, true
	}
	return 0, 0, false
}

func (m *Monitor) recordStabilitySourceQuery(job *StabilityBackfillJob, duration time.Duration, queries int) {
	if queries < 1 {
		queries = 1
	}
	ms := duration.Milliseconds() / int64(queries)
	if ms < 1 {
		ms = 1
	}
	previous := job.SourceQueries
	job.SourceQueries += queries
	if job.AverageQueryMS <= 0 {
		job.AverageQueryMS = ms
	} else {
		job.AverageQueryMS = (job.AverageQueryMS*int64(previous) + ms*int64(queries)) / int64(job.SourceQueries)
	}
}

func (m *Monitor) refreshStabilityJobProgress(job *StabilityBackfillJob, batchHours int, now int64) {
	job.CompletedHours = max(0, min(job.CompletedHours, job.TotalHours))
	job.FailedHours = max(0, min(job.FailedHours, job.TotalHours-job.CompletedHours))
	job.RemainingHours = max(0, job.TotalHours-job.CompletedHours-job.FailedHours)
	if job.TotalHours > 0 {
		job.ProgressPercent = float64(job.CompletedHours) / float64(job.TotalHours) * 100
		job.FailedPercent = float64(job.FailedHours) / float64(job.TotalHours) * 100
		job.ProcessedPercent = float64(job.CompletedHours+job.FailedHours) / float64(job.TotalHours) * 100
	}
	if job.RemainingHours == 0 || job.Status == "complete" || job.Status == "partial" {
		job.EstimatedRemainingSeconds, job.EstimatedFinishAt = 0, 0
		return
	}
	if batchHours < 1 {
		batchHours = 1
	}
	queryDuration := time.Duration(job.AverageQueryMS) * time.Millisecond
	if queryDuration <= 0 {
		queryDuration = time.Second
	}
	perQuery := queryDuration + m.stabilitySourceCooldown(queryDuration)
	queries := (job.RemainingHours + batchHours - 1) / batchHours
	seconds := int64((time.Duration(queries*stabilitySourceQueriesPerRange)*perQuery + time.Second - 1) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	job.EstimatedRemainingSeconds = seconds
	job.EstimatedFinishAt = now + seconds
}

func (m *Monitor) saveStabilityBackfillJob(job *StabilityBackfillJob, operation string) bool {
	if err := m.storeDB.Save(job).Error; err != nil {
		slog.Error("稳定性补数任务状态持久化失败", "operation", operation, "job_id", job.ID, "err", err)
		return false
	}
	return true
}

func (m *Monitor) resumeStabilityBackfill() bool {
	if !m.stabilityBackfillRunning.CompareAndSwap(false, true) {
		return false
	}
	var job StabilityBackfillJob
	query := m.storeDB.Where("status IN ? OR (status = ? AND last_error = ?)",
		[]string{"queued", "running"}, "paused", context.Canceled.Error())
	if !m.cfg.StabilityClassificationMigrationEnabled {
		query = query.Where("kind IS NULL OR kind = '' OR kind <> ?", stabilityMigrationJobKind)
	}
	err := query.Order("updated_at DESC").First(&job).Error
	if err != nil {
		m.stabilityBackfillRunning.Store(false)
		return false
	}
	job.Status, job.UpdatedAt = "queued", time.Now().Unix()
	if !m.saveStabilityBackfillJob(&job, "恢复排队状态") {
		m.stabilityBackfillRunning.Store(false)
		return false
	}
	sourceCtx := m.sourceTaskContext()
	if !goSourceEpoch(sourceCtx, func(ctx context.Context) { m.runStabilityBackfill(ctx, job.ID) }) {
		m.stabilityBackfillRunning.Store(false)
		return false
	}
	return true
}

func (m *Monitor) startStabilityClassificationMigration() bool {
	if !m.cfg.StabilityClassificationMigrationEnabled {
		return false
	}
	// A paused/partial migration is an explicit operator decision point.  Do
	// not create a second job merely because coverage is still incomplete:
	// that would bypass the root-only retry endpoint, hit the source again on
	// every restart, and split one migration's audit trail across job IDs.
	var existing StabilityBackfillJob
	err := m.storeDB.Where("kind = ? AND status IN ?", stabilityMigrationJobKind,
		[]string{"queued", "running", "paused", "partial"}).
		Order("updated_at DESC").First(&existing).Error
	if err == nil {
		if existing.Status == "paused" || existing.Status == "partial" {
			slog.Warn("稳定性分类迁移等待管理员显式重试",
				"job_id", existing.ID, "status", existing.Status,
				"failed_hours", existing.FailedHours)
		}
		return false
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		slog.Warn("检查现有稳定性分类迁移任务失败", "err", err)
		return false
	}
	to := finalizedStabilityHourTo(time.Now().Unix())
	from := to - int64(m.cfg.stabilityStorageDays())*86400
	coverage := m.stabilityDataCoverage(context.Background(), from, to, time.Now().Unix())
	if coverage.Complete {
		return false
	}
	job, err := m.startStabilityBackfillKind(from, to, stabilityMigrationJobKind)
	if err != nil {
		if !strings.Contains(err.Error(), "已有稳定性") {
			slog.Warn("自动创建稳定性分类迁移任务失败", "err", err)
		}
		return false
	}
	slog.Info("已显式启动稳定性分类迁移任务", "job_id", job.ID,
		"missing_hours", coverage.MissingHours, "source_duty_percent", m.stabilitySourceDutyPercent())
	return true
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
	if err := m.storeDB.Model(&StabilityHourIngestState{}).
		Where("hour_ts >= ? AND hour_ts < ? AND status = ? AND traffic_class_version = ?",
			from, to, "complete", userTrafficClassificationVersion).Pluck("hour_ts", &complete).Error; err != nil {
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
	resumed := m.resumeStabilityBackfill()
	if !resumed {
		m.startStabilityClassificationMigration()
	}
	if !m.cfg.StabilityAutoRepair {
		return
	}
	goSourceEpoch(ctx, func(ctx context.Context) {
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
	})
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

func (m *Monitor) retryStabilityBackfill(jobID string) (*StabilityBackfillJob, error) {
	if !m.cfg.StabilityBackfillEnabled {
		return nil, errStabilityBackfillDisabled
	}
	if m.prodDB == nil {
		return nil, fmt.Errorf("未配置生产库只读连接")
	}
	if !m.sourceAccessAllowed() {
		return nil, errSourceNotReady
	}
	if !m.stabilityBackfillRunning.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("已有稳定性历史补数或自动修洞正在执行")
	}
	var job StabilityBackfillJob
	query := m.storeDB.Where("status IN ?", []string{"partial", "paused"})
	if strings.TrimSpace(jobID) != "" {
		query = query.Where("id = ?", strings.TrimSpace(jobID))
	}
	if err := query.Order("updated_at DESC").First(&job).Error; err != nil {
		m.stabilityBackfillRunning.Store(false)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("没有可重试的 partial/paused 稳定性任务")
		}
		return nil, err
	}
	job.Status, job.LastError, job.FinishedAt = "queued", "", 0
	job.FailedHours, job.FailedHourTs = 0, nil
	job.CurrentBatchHours, job.HealthyChunks = 1, 0
	job.UpdatedAt = time.Now().Unix()
	m.refreshStabilityJobProgress(&job, 1, job.UpdatedAt)
	if !m.saveStabilityBackfillJob(&job, "显式重试排队") {
		m.stabilityBackfillRunning.Store(false)
		return nil, fmt.Errorf("持久化重试任务失败")
	}
	sourceCtx := m.sourceTaskContext()
	if !goSourceEpoch(sourceCtx, func(ctx context.Context) { m.runStabilityBackfill(ctx, job.ID) }) {
		m.stabilityBackfillRunning.Store(false)
		return nil, errSourceNotReady
	}
	return &job, nil
}

func (m *Monitor) retryStabilityBackfillHandler(c *gin.Context) {
	if strings.EqualFold(strings.TrimSpace(c.Query("domain")), "problems") {
		state, err := m.retryStabilityProblemMigration(time.Now())
		if err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{"problem_migration": state})
		return
	}
	job, err := m.retryStabilityBackfill(c.Query("id"))
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
	m.backgroundSourceScheduleMu.Lock()
	notBefore := int64(0)
	if !m.stabilitySourceNotBefore.IsZero() {
		notBefore = m.stabilitySourceNotBefore.UnixMilli()
	}
	m.backgroundSourceScheduleMu.Unlock()
	problemMigration := m.stabilityProblemMigrationProgress()
	hourlyMigrationStatus := "not_required"
	var hourlyMigration StabilityBackfillJob
	if err := m.storeDB.Where("kind = ?", stabilityMigrationJobKind).Order("updated_at DESC").First(&hourlyMigration).Error; err == nil {
		hourlyMigrationStatus = hourlyMigration.Status
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		hourlyMigrationStatus = "error"
	}
	problemDone := problemMigration.Status == "complete" || problemMigration.Status == "not_required"
	hourlyDone := hourlyMigrationStatus == "complete" || hourlyMigrationStatus == "not_required"
	c.JSON(http.StatusOK, gin.H{
		"job": job, "running": m.stabilityBackfillRunning.Load(), "coverage": coverage,
		"migration_enabled":          m.cfg.StabilityClassificationMigrationEnabled,
		"problem_migration":          problemMigration,
		"hourly_migration_status":    hourlyMigrationStatus,
		"migration_ready_to_disable": problemDone && hourlyDone,
		"source_throttle": gin.H{
			"min_start_interval_ms":   m.backgroundSourceMinStartInterval().Milliseconds(),
			"server_max_execution_ms": m.stabilityServerMaxExecutionMS(),
			"source_duty_percent":     m.stabilitySourceDutyPercent(),
			"query_starts":            m.backgroundSourceStarts.Load(),
			"last_start_at_ms":        m.backgroundSourceLastStart.Load() / int64(time.Millisecond),
			"not_before_ms":           notBefore, // legacy key: Stability low-lane duty waterline
			"low_not_before_ms":       notBefore,
		},
	})
}
