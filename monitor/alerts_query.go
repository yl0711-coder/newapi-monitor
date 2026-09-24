package monitor

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const (
	alertRejectDefaultLimit = 100
	alertRejectMaxLimit     = 200
	alertRejectCursorMaxLen = 2048
)

type alertRejectFilter struct {
	Reason string
	UserID *int64
	Limit  int
	Cursor *alertRejectCursor
}

type alertRejectCursor struct {
	Version      int    `json:"v"`
	FromTs       int64  `json:"from"`
	ToTs         int64  `json:"to"`
	FilterReason string `json:"fr,omitempty"`
	FilterUserID *int64 `json:"fu,omitempty"`
	Ts           int64  `json:"ts"`
	Reason       string `json:"r"`
	Model        string `json:"m"`
	Group        string `json:"g"`
	UserID       int64  `json:"u"`
}

func parseAlertRejectFilter(c *gin.Context) (alertRejectFilter, error) {
	f := alertRejectFilter{Limit: alertRejectDefaultLimit}
	if raw, exists := c.GetQuery("reason"); exists {
		f.Reason = strings.TrimSpace(raw)
		if f.Reason == "" {
			return f, errors.New("reason 不能为空")
		}
		if !utf8.ValidString(f.Reason) || len(f.Reason) > 64 {
			return f, errors.New("reason 必须是 1～64 字节的 UTF-8 文本")
		}
	}
	if raw, exists := c.GetQuery("user_id"); exists {
		raw = strings.TrimSpace(raw)
		uid, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || uid < 0 {
			return f, errors.New("user_id 必须是非负整数")
		}
		f.UserID = &uid
	}
	if raw, exists := c.GetQuery("limit"); exists {
		limit, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || limit < 1 || limit > alertRejectMaxLimit {
			return f, fmt.Errorf("limit 必须在 1～%d 之间", alertRejectMaxLimit)
		}
		f.Limit = limit
	}
	if raw, exists := c.GetQuery("cursor"); exists {
		cursor, err := decodeAlertRejectCursor(strings.TrimSpace(raw))
		if err != nil {
			return f, err
		}
		f.Cursor = &cursor
	}
	return f, nil
}

func validateAlertRejectCursor(scope stabilityScope, filter alertRejectFilter) error {
	cursor := filter.Cursor
	if cursor == nil {
		return nil
	}
	if cursor.FromTs != scope.FromTs || cursor.ToTs != scope.ToTs || cursor.Ts < scope.FromTs || cursor.Ts >= scope.ToTs {
		return errors.New("cursor 不属于当前日期范围")
	}
	if cursor.FilterReason != filter.Reason {
		return errors.New("cursor 与当前 reason 筛选不一致")
	}
	if (cursor.FilterUserID == nil) != (filter.UserID == nil) ||
		(cursor.FilterUserID != nil && *cursor.FilterUserID != *filter.UserID) {
		return errors.New("cursor 与当前 user_id 筛选不一致")
	}
	return nil
}

// validateAlertRejectCursorForContinuation keeps a cursor usable while the
// current-day upper bound advances between page requests.  stabilityRange
// intentionally ends today's range at time.Now(), so requiring an exact
// ToTs match makes a perfectly normal "加载更多" request fail a few seconds
// later.  The cursor's own range remains the snapshot used for the query;
// only an upper bound that is no longer ahead of the current request is
// accepted.  The lower bound and filters still have to match exactly.
func validateAlertRejectCursorForContinuation(scope stabilityScope, filter alertRejectFilter) error {
	cursor := filter.Cursor
	if cursor == nil {
		return nil
	}
	if cursor.FromTs != scope.FromTs || cursor.ToTs > scope.ToTs || cursor.ToTs <= cursor.FromTs ||
		cursor.Ts < cursor.FromTs || cursor.Ts >= cursor.ToTs {
		return errors.New("cursor 不属于当前日期范围")
	}
	if cursor.FilterReason != filter.Reason {
		return errors.New("cursor 与当前 reason 筛选不一致")
	}
	if (cursor.FilterUserID == nil) != (filter.UserID == nil) ||
		(cursor.FilterUserID != nil && *cursor.FilterUserID != *filter.UserID) {
		return errors.New("cursor 与当前 user_id 筛选不一致")
	}
	return nil
}

func encodeAlertRejectCursor(scope stabilityScope, filter alertRejectFilter, row AlertRejectRow) string {
	payload, _ := json.Marshal(alertRejectCursor{
		Version: 1, FromTs: scope.FromTs, ToTs: scope.ToTs,
		FilterReason: filter.Reason, FilterUserID: filter.UserID,
		Ts: row.Ts, Reason: row.Reason, Model: row.Model,
		Group: row.Grp, UserID: row.UserID,
	})
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeAlertRejectCursor(raw string) (alertRejectCursor, error) {
	var cursor alertRejectCursor
	if raw == "" || len(raw) > alertRejectCursorMaxLen {
		return cursor, errors.New("cursor 无效")
	}
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || json.Unmarshal(payload, &cursor) != nil {
		return cursor, errors.New("cursor 无效")
	}
	if cursor.Version != 1 || cursor.FromTs <= 0 || cursor.ToTs <= cursor.FromTs ||
		cursor.Ts <= 0 || cursor.UserID < 0 || (cursor.FilterUserID != nil && *cursor.FilterUserID < 0) ||
		!utf8.ValidString(cursor.FilterReason) || !utf8.ValidString(cursor.Reason) || !utf8.ValidString(cursor.Model) || !utf8.ValidString(cursor.Group) ||
		len(cursor.FilterReason) > 64 || len(cursor.Reason) > 64 || len(cursor.Model) > 128 || len(cursor.Group) > 128 {
		return alertRejectCursor{}, errors.New("cursor 无效")
	}
	return cursor, nil
}

// alertRejectBaseWhere is kept as a small compatibility helper for tests and
// callers that do not have a Monitor settings object.  The HTTP path uses the
// coverage-aware variant below so a stale CloudWatch cursor cannot hide legacy
// rows after the direct lane is disabled.
func alertRejectBaseWhere(scope stabilityScope, filter alertRejectFilter, includeReason bool) (string, []any) {
	// Preserve the pre-CloudWatch helper's contract: callers that do not have a
	// Monitor/config context get only the time/identity/reason predicates.  The
	// production page opts into overlap suppression explicitly through
	// alertRejectBaseWhereWithCoverage below; making this compatibility helper
	// implicitly reference the cursor table would make old callers fail on a
	// database that predates the direct-lane migration.
	return alertRejectBaseWhereWithCoverage(scope, filter, includeReason, false)
}

// cloudWatchPreRouteCoverageTableAvailable keeps compatibility snapshots
// usable while a schema migration is pending.  The direct lane is enabled
// only after its cursor table is created, but an older local database can be
// opened with the new setting before that migration has run.  Referencing a
// missing table inside NOT EXISTS would fail the whole page query instead of
// falling back to the legacy rejection facts.
func cloudWatchPreRouteCoverageTableAvailable(db *gorm.DB) bool {
	return db != nil && db.Migrator().HasTable(&CloudWatchPreRouteCursor{})
}

// alertRejectCanonicalReasonSQL mirrors canonicalShadowRejectionReason for the
// closed reason set used by the direct lane.  Unknown legacy reasons are not
// silently hidden merely because a CloudWatch cursor covers their minute.
const alertRejectCanonicalReasonSQL = `CASE lower(replace(replace(replace(trim(rejection_samples.reason), '-', '_'), ' ', '_'), '.', '_'))
  WHEN 'no_channel' THEN 'route_no_channel'
  WHEN 'no_available_channel' THEN 'route_no_channel'
  WHEN 'route_no_channel' THEN 'route_no_channel'
  WHEN 'noavailablechannel' THEN 'route_no_channel'
  WHEN 'invalid_token' THEN 'invalid_token'
  WHEN 'token_invalid' THEN 'invalid_token'
  WHEN 'token_disabled' THEN 'token_disabled'
  WHEN 'disabled_token' THEN 'token_disabled'
  WHEN 'model_forbidden' THEN 'model_forbidden'
  WHEN 'token_model_forbidden' THEN 'model_forbidden'
  WHEN 'model_not_found' THEN 'model_not_found'
  WHEN 'unknown_model' THEN 'model_not_found'
  WHEN 'quota_account' THEN 'quota_account'
  WHEN 'quota_insufficient' THEN 'quota_account'
  WHEN 'insufficient_quota' THEN 'quota_account'
  WHEN 'user_quota_insufficient' THEN 'quota_account'
  WHEN 'token_quota_insufficient' THEN 'quota_account'
  WHEN 'pre_consume_failed' THEN 'quota_account'
  WHEN 'rate_limited' THEN 'rate_limited'
  WHEN 'rate_limit' THEN 'rate_limited'
  WHEN 'too_many_requests' THEN 'rate_limited'
  ELSE lower(replace(replace(replace(trim(rejection_samples.reason), '-', '_'), ' ', '_'), '.', '_'))
END`

// alertRejectCanonicalReasonSQLForColumn returns the same canonical reason
// expression with an explicit table alias.  The alert query compares an
// outer legacy row with a correlated CloudWatch row; model statistics reuses
// that exact comparison but uses different aliases.  Keeping the expression
// in one place prevents the two pages from drifting when another historical
// reason spelling is added.
func alertRejectCanonicalReasonSQLForColumn(column string) string {
	column = strings.TrimSpace(column)
	if column == "" {
		return alertRejectCanonicalReasonSQL
	}
	// The base expression is intentionally written against the concrete
	// `rejection_samples` table because it is also used in the outer WHERE.
	// Aggregation CTEs, however, expose only a bare `reason` column, so a
	// caller passing "reason" must replace the table-qualified reference too;
	// otherwise SQLite tries to resolve `rejection_samples.reason` in the CTE
	// scope and reports "no such column" at runtime.
	return strings.ReplaceAll(alertRejectCanonicalReasonSQL, "rejection_samples.reason", column)
}

// alertRejectNormalizedReasonSQLForColumn is the pre-alias normalization used
// for the three legacy quota subcategories.  canonicalShadowRejectionReason
// intentionally folds those categories into quota_account for overlap
// reconciliation, but the alert summary still needs to explain which legacy
// field was missing the user identity.
func alertRejectNormalizedReasonSQLForColumn(column string) string {
	column = strings.TrimSpace(column)
	if column == "" {
		column = "reason"
	}
	return "lower(replace(replace(replace(trim(" + column + "), '-', '_'), ' ', '_'), '.', '_'))"
}

// alertRejectReasonFilterPredicate keeps the legacy quota subcategories
// selectable even though overlap reconciliation intentionally folds them into
// the CloudWatch canonical quota_account class.  The direct lane cannot tell
// user-vs-token quota apart, so a fine-grained legacy filter must stay on the
// normalized raw reason; selecting quota_account itself still returns the
// whole canonical class from both sources.
func normalizeAlertRejectReason(reason string) string {
	return strings.ToLower(strings.NewReplacer("-", "_", " ", "_", ".", "_").Replace(strings.TrimSpace(reason)))
}

func isAlertRejectFineQuotaReason(reason string) bool {
	switch normalizeAlertRejectReason(reason) {
	case "user_quota_insufficient", "token_quota_insufficient", "pre_consume_failed":
		return true
	default:
		return false
	}
}

func alertRejectReasonFilterPredicate(reason string) (string, any) {
	normalized := normalizeAlertRejectReason(reason)
	switch normalized {
	case "user_quota_insufficient", "token_quota_insufficient", "pre_consume_failed":
		return alertRejectNormalizedReasonSQLForColumn("rejection_samples.reason") + " = ?", normalized
	default:
		return "(" + alertRejectCanonicalReasonSQL + ") = ?", canonicalShadowRejectionReason(reason)
	}
}

func alertRejectBaseWhereWithCoverage(scope stabilityScope, filter alertRejectFilter, includeReason bool, cloudWatchCoverageEnabled bool) (string, []any) {
	// Negative IDs were accepted by an early legacy ingest path.  They are not
	// customer identities; hide any stale rows defensively instead of rendering
	// a fabricated customer such as #-1.  New ingest paths reject them at the
	// boundary, but old local snapshots can outlive that validation.
	// New ingest rejects non-positive counts.  Keep the read path fail-closed
	// for stale snapshots created before that validation; otherwise a zero or
	// negative row would appear as a real alert and distort the summary.
	where := "bucket_ts >= ? AND bucket_ts < ? AND user_id >= 0 AND count > 0"
	args := []any{scope.FromTs, scope.ToTs}
	if filter.UserID != nil {
		where += " AND user_id = ?"
		args = append(args, *filter.UserID)
	}
	if includeReason && filter.Reason != "" {
		// The direct lane stores canonical reason codes while the legacy
		// collector may have stored one of several historical aliases (for
		// example no.channel/no_available_channel).  Filtering on the raw
		// value would make an alias option return zero rows after its legacy
		// row is correctly suppressed as a duplicate of the direct row.  Use
		// the same canonical expression as the overlap reconciliation, while
		// keeping the raw option in the cursor/API for backwards compatibility.
		// The three historical quota subcategories are the exception: their
		// fine-grained filters use normalized raw values because canonical
		// quota_account cannot distinguish user-vs-token quota.
		predicate, value := alertRejectReasonFilterPredicate(filter.Reason)
		where += " AND " + predicate
		args = append(args, value)
	}
	if !cloudWatchCoverageEnabled {
		return where, args
	}
	// A fine-grained legacy quota filter cannot be reconciled against a direct
	// quota_account row: the latter has no user-vs-token/pre-consume subtype.
	// Keep the legacy evidence visible for that filter (the direct row does not
	// match its raw-reason predicate); the broad quota_account filter below
	// still applies normal cross-source precedence.
	if isAlertRejectFineQuotaReason(filter.Reason) {
		return where, args
	}
	// CloudWatch 直采追平的分钟由它作为唯一事实来源；旧 collector 在同一
	// 覆盖窗口内、且已有同一 canonical 维度的直采行时排除，避免迁移/重叠
	// 双算。若直采没有对应维度（例如解析失败、身份缺失）或旧行是未知
	// reason，则保留旧行，不能把“覆盖”误当成“未知错误已被观察到”。
	// user_id=0 表示日志没有可确认的客户身份；即使两条来源都为 0，
	// 也不能据此断言是同一请求，必须保留旧行，避免覆盖水位把未知客户的
	// 事实静默吞掉。调用方在覆盖表缺失时会选择不带 cursor 的兼容分支，
	// 不让旧快照因为新表尚未迁移而整页失败。
	where += ` AND (
  node = ? OR user_id <= 0 OR NOT EXISTS (
    SELECT 1 FROM cloud_watch_pre_route_cursors AS coverage
    WHERE coverage.id = ?
      AND coverage.semantics_version = ?
      AND rejection_samples.bucket_ts >= coverage.coverage_from_ts
      AND rejection_samples.bucket_ts < coverage.through_ts
      AND EXISTS (
        SELECT 1 FROM rejection_samples AS direct
        WHERE direct.node = ?
          AND direct.bucket_ts = rejection_samples.bucket_ts
          AND direct.model = rejection_samples.model
          AND direct.grp = rejection_samples.grp
          AND direct.user_id = rejection_samples.user_id
          AND direct.count > 0
          AND ` + alertRejectCanonicalReasonSQLForColumn("direct.reason") + ` = ` + alertRejectCanonicalReasonSQLForColumn("rejection_samples.reason") + `
      )
  )
)`
	args = append(args, cloudWatchPreRouteNode, cloudWatchPreRouteCursorID, cloudWatchPreRouteVersion, cloudWatchPreRouteNode)
	return where, args
}

// alertRejectBaseWhereWithCloudWatchOverlap extends the coverage-aware
// predicate with a small transition safeguard for the case where the direct
// lane has been switched off.  A disabled lane can leave already-published
// cloudwatch-direct rows in the local fact store while the legacy collector
// continues to write the same minutes.  We still must not use the stale
// cursor as an authority in that mode: only an explicit matching direct row
// is enough to suppress the legacy copy.  This keeps unmatched legacy facts
// visible and avoids requiring the cursor table on old local snapshots.
func alertRejectBaseWhereWithCloudWatchOverlap(scope stabilityScope, filter alertRejectFilter, includeReason bool, cloudWatchCoverageEnabled bool) (string, []any) {
	where, args := alertRejectBaseWhereWithCoverage(scope, filter, includeReason, cloudWatchCoverageEnabled)
	if cloudWatchCoverageEnabled || isAlertRejectFineQuotaReason(filter.Reason) {
		return where, args
	}
	where += ` AND (
  node = ? OR user_id <= 0 OR NOT EXISTS (
    SELECT 1 FROM rejection_samples AS direct
    WHERE direct.node = ?
      AND direct.bucket_ts = rejection_samples.bucket_ts
      AND direct.model = rejection_samples.model
      AND direct.grp = rejection_samples.grp
      AND direct.user_id = rejection_samples.user_id
      AND direct.count > 0
      AND ` + alertRejectCanonicalReasonSQLForColumn("direct.reason") + ` = ` + alertRejectCanonicalReasonSQLForColumn("rejection_samples.reason") + `
  )
)`
	args = append(args, cloudWatchPreRouteNode, cloudWatchPreRouteNode)
	return where, args
}

func (m *Monitor) queryRejectPage(scope stabilityScope, filter alertRejectFilter) ([]AlertRejectRow, bool, string, AlertRejectStats, error) {
	coverageEnabled := m.cfg.CloudWatchPreRouteEnabled && cloudWatchPreRouteCoverageTableAvailable(m.storeDB)
	baseWhere, baseArgs := alertRejectBaseWhereWithCloudWatchOverlap(scope, filter, true, coverageEnabled)
	seek := ""
	args := append([]any(nil), baseArgs...)
	if cursor := filter.Cursor; cursor != nil {
		seek = ` AND (
  ts < ? OR
  (ts = ? AND reason > ?) OR
  (ts = ? AND reason = ? AND model > ?) OR
  (ts = ? AND reason = ? AND model = ? AND grp > ?) OR
  (ts = ? AND reason = ? AND model = ? AND grp = ? AND user_id > ?)
)`
		args = append(args,
			cursor.Ts,
			cursor.Ts, cursor.Reason,
			cursor.Ts, cursor.Reason, cursor.Model,
			cursor.Ts, cursor.Reason, cursor.Model, cursor.Group,
			cursor.Ts, cursor.Reason, cursor.Model, cursor.Group, cursor.UserID,
		)
	}
	q := `
WITH agg AS (
  SELECT bucket_ts AS ts, reason, model, grp, user_id, SUM(count) AS count
  FROM rejection_samples
  WHERE ` + baseWhere + `
  GROUP BY bucket_ts, reason, model, grp, user_id
), stats AS (
  SELECT COUNT(*) AS row_total,
         COALESCE(SUM(count),0) AS total,
         -- user_id=0 只有 invalid_token 才能确定为未鉴权。其余 reason
         --（包括 CloudWatch canonical reason）只是日志没有提供用户段，
         -- 必须统一计入客户未知；trim/lower 兼容旧采集器偶发的大小写/空格。
         -- CloudWatch 的 quota_account 单独统计，避免顶部说明把它误报成“其他”。
		 COALESCE(SUM(CASE WHEN user_id=0 AND ` + alertRejectCanonicalReasonSQLForColumn("reason") + `='invalid_token' THEN count ELSE 0 END),0) AS unauth_count,
		 COALESCE(SUM(CASE WHEN user_id=0 AND ` + alertRejectCanonicalReasonSQLForColumn("reason") + `<>'invalid_token' THEN count ELSE 0 END),0) AS unknown_customer_count,
		 COALESCE(SUM(CASE WHEN user_id=0 AND ` + alertRejectNormalizedReasonSQLForColumn("reason") + `='user_quota_insufficient' THEN count ELSE 0 END),0) AS unknown_user_quota_count,
		 COALESCE(SUM(CASE WHEN user_id=0 AND ` + alertRejectNormalizedReasonSQLForColumn("reason") + `='pre_consume_failed' THEN count ELSE 0 END),0) AS unknown_pre_consume_count,
		 COALESCE(SUM(CASE WHEN user_id=0 AND ` + alertRejectNormalizedReasonSQLForColumn("reason") + `='token_quota_insufficient' THEN count ELSE 0 END),0) AS unknown_token_quota_count,
		 COALESCE(SUM(CASE WHEN user_id=0 AND ` + alertRejectCanonicalReasonSQLForColumn("reason") + `='quota_account' AND ` + alertRejectNormalizedReasonSQLForColumn("reason") + ` NOT IN ('user_quota_insufficient','pre_consume_failed','token_quota_insufficient') THEN count ELSE 0 END),0) AS unknown_quota_account_count,
		 COALESCE(SUM(CASE WHEN user_id=0 AND ` + alertRejectCanonicalReasonSQLForColumn("reason") + ` NOT IN ('invalid_token','quota_account') THEN count ELSE 0 END),0) AS unknown_customer_other_count
  FROM agg
), page AS (
  SELECT * FROM agg WHERE 1=1` + seek + `
  ORDER BY ts DESC, reason ASC, model ASC, grp ASC, user_id ASC
  LIMIT ?
)
SELECT 0 AS row_kind, 0 AS ts, '' AS reason, '' AS model, '' AS grp, 0 AS user_id, 0 AS count,
       row_total, total, unauth_count, unknown_customer_count, unknown_user_quota_count,
       unknown_pre_consume_count, unknown_token_quota_count, unknown_quota_account_count, unknown_customer_other_count
FROM stats
UNION ALL
SELECT 1 AS row_kind, ts, reason, model, grp, user_id, count,
       0, 0, 0, 0, 0, 0, 0, 0, 0
FROM page
ORDER BY row_kind ASC, ts DESC, reason ASC, model ASC, grp ASC, user_id ASC`
	args = append(args, filter.Limit+1)
	type dbRow struct {
		RowKind int `gorm:"column:row_kind"`
		AlertRejectRow
		RowTotal                  int64 `gorm:"column:row_total"`
		Total                     int64 `gorm:"column:total"`
		UnauthCount               int64 `gorm:"column:unauth_count"`
		UnknownCustomerCount      int64 `gorm:"column:unknown_customer_count"`
		UnknownUserQuotaCount     int64 `gorm:"column:unknown_user_quota_count"`
		UnknownPreConsumeCount    int64 `gorm:"column:unknown_pre_consume_count"`
		UnknownTokenQuotaCount    int64 `gorm:"column:unknown_token_quota_count"`
		UnknownQuotaAccountCount  int64 `gorm:"column:unknown_quota_account_count"`
		UnknownCustomerOtherCount int64 `gorm:"column:unknown_customer_other_count"`
	}
	var dbRows []dbRow
	if err := m.storeDB.Raw(q, args...).Scan(&dbRows).Error; err != nil {
		return nil, false, "", AlertRejectStats{}, err
	}
	var stats AlertRejectStats
	rows := make([]AlertRejectRow, 0, filter.Limit+1)
	for _, row := range dbRows {
		if row.RowKind == 0 {
			stats = AlertRejectStats{
				RowTotal: row.RowTotal, Total: row.Total, UnauthCount: row.UnauthCount,
				UnknownCustomerCount:      row.UnknownCustomerCount,
				UnknownUserQuotaCount:     row.UnknownUserQuotaCount,
				UnknownPreConsumeCount:    row.UnknownPreConsumeCount,
				UnknownTokenQuotaCount:    row.UnknownTokenQuotaCount,
				UnknownQuotaAccountCount:  row.UnknownQuotaAccountCount,
				UnknownCustomerOtherCount: row.UnknownCustomerOtherCount,
			}
			continue
		}
		rows = append(rows, row.AlertRejectRow)
	}
	hasMore := len(rows) > filter.Limit
	if hasMore {
		rows = rows[:filter.Limit]
	}
	ids := make([]int64, 0, len(rows))
	seen := map[int64]bool{}
	for _, row := range rows {
		if row.UserID != 0 && !seen[row.UserID] {
			seen[row.UserID] = true
			ids = append(ids, row.UserID)
		}
	}
	names := lookupUserNames(m.storeDB, ids)
	for i := range rows {
		rows[i].Username = names[rows[i].UserID]
	}
	var next string
	if hasMore && len(rows) > 0 {
		next = encodeAlertRejectCursor(scope, filter, rows[len(rows)-1])
	}
	return rows, hasMore, next, stats, nil
}

func (m *Monitor) queryRejectReasonOptions(scope stabilityScope, filter alertRejectFilter) ([]string, error) {
	coverageEnabled := m.cfg.CloudWatchPreRouteEnabled && cloudWatchPreRouteCoverageTableAvailable(m.storeDB)
	where, args := alertRejectBaseWhereWithCloudWatchOverlap(scope, filter, false, coverageEnabled)
	var reasons []string
	err := m.storeDB.Raw(`SELECT DISTINCT reason FROM rejection_samples WHERE `+where+` ORDER BY reason`, args...).Scan(&reasons).Error
	return reasons, err
}
