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

func alertRejectBaseWhere(scope stabilityScope, filter alertRejectFilter, includeReason bool) (string, []any) {
	where := "bucket_ts >= ? AND bucket_ts < ?"
	args := []any{scope.FromTs, scope.ToTs}
	if filter.UserID != nil {
		where += " AND user_id = ?"
		args = append(args, *filter.UserID)
	}
	if includeReason && filter.Reason != "" {
		where += " AND reason = ?"
		args = append(args, filter.Reason)
	}
	return where, args
}

func (m *Monitor) queryRejectPage(scope stabilityScope, filter alertRejectFilter) ([]AlertRejectRow, bool, string, AlertRejectStats, error) {
	baseWhere, baseArgs := alertRejectBaseWhere(scope, filter, true)
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
         COALESCE(SUM(CASE WHEN user_id=0 AND reason='invalid_token' THEN count ELSE 0 END),0) AS unauth_count,
         COALESCE(SUM(CASE WHEN user_id=0 AND reason!='invalid_token' THEN count ELSE 0 END),0) AS unknown_customer_count,
         COALESCE(SUM(CASE WHEN user_id=0 AND reason='user_quota_insufficient' THEN count ELSE 0 END),0) AS unknown_user_quota_count,
         COALESCE(SUM(CASE WHEN user_id=0 AND reason='pre_consume_failed' THEN count ELSE 0 END),0) AS unknown_pre_consume_count,
         COALESCE(SUM(CASE WHEN user_id=0 AND reason='token_quota_insufficient' THEN count ELSE 0 END),0) AS unknown_token_quota_count,
         COALESCE(SUM(CASE WHEN user_id=0 AND reason NOT IN ('invalid_token','user_quota_insufficient','pre_consume_failed','token_quota_insufficient') THEN count ELSE 0 END),0) AS unknown_customer_other_count
  FROM agg
), page AS (
  SELECT * FROM agg WHERE 1=1` + seek + `
  ORDER BY ts DESC, reason ASC, model ASC, grp ASC, user_id ASC
  LIMIT ?
)
SELECT 0 AS row_kind, 0 AS ts, '' AS reason, '' AS model, '' AS grp, 0 AS user_id, 0 AS count,
       row_total, total, unauth_count, unknown_customer_count, unknown_user_quota_count,
       unknown_pre_consume_count, unknown_token_quota_count, unknown_customer_other_count
FROM stats
UNION ALL
SELECT 1 AS row_kind, ts, reason, model, grp, user_id, count,
       0, 0, 0, 0, 0, 0, 0, 0
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
	where, args := alertRejectBaseWhere(scope, filter, false)
	var reasons []string
	err := m.storeDB.Raw(`SELECT DISTINCT reason FROM rejection_samples WHERE `+where+` ORDER BY reason`, args...).Scan(&reasons).Error
	return reasons, err
}
