package monitor

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
)

const usageLogsDiagnosticCandidates = 5000

// HTTP requests amortize network/pool overhead while each statement still has
// a bounded candidate set. A partial response is a checkpoint, never a page.
func queryUsageLogsDiagnosticPage(ctx context.Context, db usageLogsQuerier, f usageLogFilter, where *usageLogWhere, cursor *usageLogCursor, page, size, cap, maxBatches int, mysql bool) (usageLogsDiagnosticBatch, error) {
	r := usageLogsDiagnosticBatch{Items: []usageLogItem{}, Bounded: true, Page: page, PageSize: size, TotalPending: true}
	start := time.Now()
	current := cursor
	for batch := 0; batch < maxBatches; batch++ {
		remaining := size - len(r.Items)
		probing := remaining == 0
		if probing {
			remaining = 1
		}
		b, err := queryUsageLogsDiagnosticBatch(ctx, db, f, where, current, page, remaining, cap, mysql)
		if err != nil {
			return r, err
		}
		r.Scanned += b.Scanned
		if probing && len(b.Items) > 0 {
			last := r.Items[len(r.Items)-1]
			r.HasMore = true
			r.Next = &usageLogCursor{CreatedAt: last.CreatedAt, ID: last.ID}
			return r, nil
		}
		r.Items = append(r.Items, b.Items...)
		if b.HasMore {
			r.HasMore = true
			r.Next = b.Next
			return r, nil
		}
		if !b.ScanPending {
			if batch == 0 && cursor == nil && page == 1 {
				r.Total = b.Total
				r.TotalPending = b.TotalPending
			}
			return r, nil
		}
		current = b.ScanCursor
		r.ScanPending = true
		r.ScanCursor = current
		if batch+1 < maxBatches && time.Since(start) < 5*time.Second {
			r.ScanPending = false
			r.ScanCursor = nil
			continue
		}
		return r, nil
	}
	return r, nil
}

// LIMIT and window functions materialize the candidate keys. Only that bounded
// set drives primary-key lookups/JSON filtering; counts below are of the small
// CTEs, never the complete search. Cursor order is (created_at,id), not ID alone.
func usageLogsDiagnosticBatchSQL(f usageLogFilter, where *usageLogWhere, cursor *usageLogCursor, size, cap int, mysql bool) (string, []any) {
	keys := &usageLogWhere{}
	keys.add("created_at >= ?", f.Start)
	keys.add("created_at <= ?", f.End)
	if cursor != nil {
		keys.add("(created_at < ? OR (created_at = ? AND id < ?))", cursor.CreatedAt, cursor.CreatedAt, cursor.ID)
	}
	join := "CROSS JOIN"
	source := "logs"
	if mysql {
		join = "STRAIGHT_JOIN"
		// This source index was observed in the retained replica schema. It is
		// mandatory for this opt-in path: fail closed if absent rather than
		// filesort every key in the whole window. Ordinary queries are untouched.
		source = "logs FORCE INDEX (idx_created_at_id_rc26)"
	}
	q := `WITH diagnostic_keys AS (
 SELECT id AS candidate_id,created_at AS candidate_at FROM ` + source + keys.sql() + `
 ORDER BY created_at DESC,id DESC LIMIT ?
), diagnostic_ranked AS (
 SELECT candidate_id,candidate_at,ROW_NUMBER() OVER(ORDER BY candidate_at DESC,candidate_id DESC) AS candidate_no FROM diagnostic_keys
), diagnostic_matches AS (
 SELECT logs.id AS match_id,logs.created_at AS match_at,candidate_no FROM diagnostic_ranked ` + join + ` logs ON logs.id=candidate_id` + where.sql() + `
 AND candidate_no<=? ORDER BY logs.created_at DESC,logs.id DESC LIMIT ?
), diagnostic_meta AS (
 SELECT COUNT(*) AS candidate_count,
 COALESCE(MAX(CASE WHEN candidate_no=? THEN candidate_at END),0) AS tail_at,
 COALESCE(MAX(CASE WHEN candidate_no=? THEN candidate_id END),0) AS tail_id,
 (SELECT COUNT(*) FROM diagnostic_matches) AS match_count FROM diagnostic_ranked
), diagnostic_visible AS (
 SELECT match_id,match_at,candidate_no FROM diagnostic_matches ORDER BY match_at DESC,match_id DESC LIMIT ?
) ` + usageLogsSelect + `candidate_count,tail_at,tail_id,match_count,COALESCE(candidate_no,0),` + usageLogColumns + `
 FROM diagnostic_meta LEFT JOIN diagnostic_visible ON 1=1 LEFT JOIN logs ON logs.id=match_id
 ORDER BY match_at DESC,match_id DESC`
	args := append([]any{}, keys.args...)
	args = append(args, cap+1)
	args = append(args, where.args...)
	args = append(args, cap, size+1, cap, cap, size)
	return q, args
}

type usageLogsDiagnosticBatch struct {
	Items        []usageLogItem  `json:"items"`
	HasMore      bool            `json:"has_more"`
	Next         *usageLogCursor `json:"next_cursor"`
	ScanPending  bool            `json:"scan_pending"`
	ScanCursor   *usageLogCursor `json:"scan_cursor"`
	Scanned      int64           `json:"scanned"`
	Total        any             `json:"total"`
	TotalPending bool            `json:"total_pending"`
	Bounded      bool            `json:"bounded_search"`
	Page         int             `json:"page"`
	PageSize     int             `json:"page_size"`
}

func queryUsageLogsDiagnosticBatch(ctx context.Context, db usageLogsQuerier, f usageLogFilter, where *usageLogWhere, cursor *usageLogCursor, page, size, cap int, mysql bool) (usageLogsDiagnosticBatch, error) {
	r := usageLogsDiagnosticBatch{Items: []usageLogItem{}, TotalPending: true, Bounded: true, Page: page, PageSize: size}
	q, args := usageLogsDiagnosticBatchSQL(f, where, cursor, size, cap, mysql)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	var candidates, tailAt, tailID, matches, lastOrdinal int64
	for rows.Next() {
		var it usageLogItem
		var id sql.NullInt64
		var stream any
		if err := rows.Scan(&candidates, &tailAt, &tailID, &matches, &lastOrdinal,
			&id, &it.UserID, &it.CreatedAt, &it.Type, &it.Content, &it.Username, &it.TokenName, &it.ModelName, &it.Quota,
			&it.PromptTokens, &it.CompletionTokens, &it.UseTime, &stream, &it.Channel, &it.TokenID, &it.Group, &it.IP, &it.RequestID, &it.UpstreamRequestID, &it.Other); err != nil {
			return r, err
		}
		if !id.Valid {
			continue
		}
		it.ID = id.Int64
		it.IsStream = usageLogTruthy(stream)
		it.Other = projectUsageLogOtherForRoot(it.Other)
		r.Items = append(r.Items, it)
	}
	if err := rows.Err(); err != nil {
		return r, err
	}
	r.HasMore = matches > int64(size)
	r.Scanned = candidates
	if r.Scanned > int64(cap) {
		r.Scanned = int64(cap)
	}
	if r.HasMore {
		last := r.Items[len(r.Items)-1]
		r.Next = &usageLogCursor{CreatedAt: last.CreatedAt, ID: last.ID}
		r.Scanned = lastOrdinal
	} else if candidates > int64(cap) {
		r.ScanPending = true
		r.ScanCursor = &usageLogCursor{CreatedAt: tailAt, ID: tailID}
	}
	if cursor == nil && page == 1 && !r.HasMore && !r.ScanPending {
		r.Total = len(r.Items)
		r.TotalPending = false
	}
	return r, nil
}

func (m *Monitor) serveUsageLogsDiagnosticBatch(ctx context.Context, c *gin.Context, db usageLogsQuerier, f usageLogFilter, where *usageLogWhere, cursor *usageLogCursor, page, size int) {
	r, err := queryUsageLogsDiagnosticPage(ctx, db, f, where, cursor, page, size, usageLogsDiagnosticCandidates, 8, m.usageLogsMySQL())
	if err == nil {
		err = fillUsageLogChannelNames(ctx, db, r.Items, map[int64]string{})
	}
	if err != nil {
		usageLogsFail(c, fmt.Errorf("查询排障日志失败: %w", usageLogsQueryErr(ctx, err)))
		return
	}
	c.JSON(200, gin.H{"success": true, "message": "", "data": r})
}
