package usagelogs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// ListInput describes the page independent of its HTTP representation.
// The cursor is parsed after the filters, preserving validation precedence.
type ListInput struct {
	Filter          Filter
	Page, PageSize  int
	BeforeCreatedAt string
	BeforeID        string
	DeferTotal      bool
}

// Page is the result of one paginated administrator log query.
type Page struct {
	Number  int     `json:"page"`
	Size    int     `json:"page_size"`
	Total   any     `json:"total"`
	Pending *bool   `json:"total_pending,omitempty"`
	Items   []Item  `json:"items"`
	Next    *Cursor `json:"next_cursor"`
	HasMore *bool   `json:"has_more,omitempty"`
}

// Item mirrors the JSON shape of a new-api log row.
type Item struct {
	ID                int64  `json:"id"`
	UserID            int64  `json:"user_id"`
	CreatedAt         int64  `json:"created_at"`
	Type              int    `json:"type"`
	Content           string `json:"content"`
	Username          string `json:"username"`
	TokenName         string `json:"token_name"`
	ModelName         string `json:"model_name"`
	Quota             int64  `json:"quota"`
	PromptTokens      int64  `json:"prompt_tokens"`
	CompletionTokens  int64  `json:"completion_tokens"`
	UseTime           int64  `json:"use_time"`
	IsStream          bool   `json:"is_stream"`
	Channel           int64  `json:"channel"`
	ChannelName       string `json:"channel_name"`
	TokenID           int64  `json:"token_id"`
	Group             string `json:"group"`
	IP                string `json:"ip"`
	RequestID         string `json:"request_id,omitempty"`
	UpstreamRequestID string `json:"upstream_request_id,omitempty"`
	Other             string `json:"other"`
}

const usageLogColumns = "id, COALESCE(user_id,0), COALESCE(created_at,0), COALESCE(type,0), COALESCE(content,''), " +
	"COALESCE(username,''), COALESCE(token_name,''), COALESCE(model_name,''), COALESCE(quota,0), " +
	"COALESCE(prompt_tokens,0), COALESCE(completion_tokens,0), COALESCE(use_time,0), COALESCE(is_stream,0), " +
	"COALESCE(channel_id,0), COALESCE(token_id,0), COALESCE(`group`,''), COALESCE(ip,''), " +
	"COALESCE(request_id,''), COALESCE(upstream_request_id,''), COALESCE(other,'')"

func scanUsageLogRows(rows *sql.Rows) ([]Item, error) {
	return scanUsageLogRowsAndTotal(rows, nil)
}

func scanUsageLogRowsAndTotal(rows *sql.Rows, total *int64) ([]Item, error) {
	items := []Item{}
	for rows.Next() {
		var it Item
		var stream any
		values := []any{&it.ID, &it.UserID, &it.CreatedAt, &it.Type, &it.Content, &it.Username, &it.TokenName,
			&it.ModelName, &it.Quota, &it.PromptTokens, &it.CompletionTokens, &it.UseTime, &stream, &it.Channel,
			&it.TokenID, &it.Group, &it.IP, &it.RequestID, &it.UpstreamRequestID, &it.Other}
		if total != nil {
			values = append(values, total)
		}
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}
		it.IsStream = usageLogTruthy(stream)
		it.Other = projectUsageLogOtherForRoot(it.Other)
		items = append(items, it)
	}
	return items, rows.Err()
}

func usageLogTruthy(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int64:
		return x != 0
	case []byte:
		s := strings.TrimSpace(string(x))
		return s != "" && s != "0" && !strings.EqualFold(s, "false")
	case string:
		s := strings.TrimSpace(x)
		return s != "" && s != "0" && !strings.EqualFold(s, "false")
	}
	return false
}

// projectUsageLogOtherForRoot applies new-api's root projection: everything is
// kept, raw values untouched, except a legacy top-level reject_reason, which is
// moved into admin_info. Invalid JSON is returned as is (root view).
func projectUsageLogOtherForRoot(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || !strings.Contains(trimmed, `"reject_reason"`) {
		return raw
	}
	var top map[string]json.RawMessage
	if json.Unmarshal([]byte(trimmed), &top) != nil {
		return raw
	}
	reason, ok := top["reject_reason"]
	if !ok {
		return raw
	}
	admin := map[string]json.RawMessage{}
	if existing, ok := top["admin_info"]; ok && json.Unmarshal(existing, &admin) != nil {
		return raw
	}
	if admin == nil { // JSON null decodes successfully to a nil map.
		admin = map[string]json.RawMessage{}
	}
	if _, exists := admin["reject_reason"]; !exists {
		admin["reject_reason"] = reason
	}
	delete(top, "reject_reason")
	encodedAdmin, err := json.Marshal(admin)
	if err != nil {
		return raw
	}
	top["admin_info"] = encodedAdmin
	out, err := json.Marshal(top)
	if err != nil {
		return raw
	}
	return string(out)
}

// fillUsageLogChannelNames looks up channel names on the replica; a missing or
// deleted channel keeps an empty name, as in new-api.
func fillUsageLogChannelNames(ctx context.Context, db usageLogsQuerier, items []Item, cache map[int64]string) error {
	var missing []any
	seen := map[int64]bool{}
	for _, it := range items {
		if it.Channel == 0 || seen[it.Channel] {
			continue
		}
		seen[it.Channel] = true
		if _, ok := cache[it.Channel]; !ok {
			missing = append(missing, it.Channel)
		}
	}
	if len(missing) > 0 {
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(missing)), ",")
		rows, err := db.QueryContext(ctx, usageLogsSelect+"id, COALESCE(name,'') FROM channels WHERE id IN ("+placeholders+")", missing...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for _, id := range missing {
			cache[id.(int64)] = ""
		}
		for rows.Next() {
			var id int64
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				return err
			}
			cache[id] = name
		}
		if err := rows.Err(); err != nil {
			return err
		}
	}
	for i := range items {
		items[i].ChannelName = cache[items[i].Channel]
	}
	return nil
}

// Cursor accelerates sequential paging without removing arbitrary page jumps.
// It bounds the page query only, never the complete filtered count or statistics.
type Cursor struct {
	CreatedAt int64 `json:"created_at"`
	ID        int64 `json:"id"`
}

func usageLogsCursor(at, id string) (*Cursor, error) {
	if at == "" && id == "" {
		return nil, nil
	}
	created, errAt := strconv.ParseInt(at, 10, 64)
	key, errID := strconv.ParseInt(id, 10, 64)
	if errAt != nil || errID != nil || created < 0 || key <= 0 {
		return nil, errors.New("日志分页位置无效，请重新搜索")
	}
	return &Cursor{CreatedAt: created, ID: key}, nil
}

// Look ahead using only bounded ID/time metadata, then fetch exactly this
// page's full rows. The window count covers at most size+1 IDs, never the
// complete filtered result. In particular, a large content/other value on
// the next page must not be read or transferred just to determine has_more.
func usageLogsPageSQL(where *usageLogWhere, source string, page, size int, cursor *Cursor, deferTotal bool) (string, []any) {
	pageWhere := &usageLogWhere{clauses: append([]string{}, where.clauses...), args: append([]any{}, where.args...)}
	offset := (page - 1) * size
	if page > 1 && cursor != nil {
		pageWhere.add("(created_at < ? OR (created_at = ? AND id < ?))", cursor.CreatedAt, cursor.CreatedAt, cursor.ID)
		offset = 0
	}
	args := append([]any{}, pageWhere.args...)
	if !deferTotal {
		return usageLogsSelect + usageLogColumns + " FROM logs JOIN (SELECT id AS page_id FROM " + source + pageWhere.sql() +
			" ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?) page ON logs.id = page.page_id ORDER BY created_at DESC, id DESC", append(args, size, offset)
	}
	return usageLogsSelect + usageLogColumns + ", page.page_matches FROM logs JOIN (" +
		"SELECT page_id, COUNT(*) OVER() AS page_matches FROM (SELECT id AS page_id, created_at AS page_created_at FROM " + source + pageWhere.sql() +
		" ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?) lookahead ORDER BY page_created_at DESC, page_id DESC LIMIT ?) page ON logs.id = page.page_id ORDER BY created_at DESC, id DESC", append(args, size+1, offset, size)
}

// List returns a page of logs matching the requested filters.
func (r *Request) List(in ListInput) (Page, error) {
	f := in.Filter
	where, err := buildUsageLogWhere(f, usageLogWhereList)
	if err != nil {
		return Page{}, err
	}
	page, size := pageBounds(in.Page, in.PageSize)
	cursor, err := usageLogsCursor(in.BeforeCreatedAt, in.BeforeID)
	if err != nil {
		return Page{}, err
	}
	db, session, err := r.open(false)
	if err != nil {
		return Page{}, err
	}
	ctx, cancel := context.WithTimeout(session, usageLogsQueryTimeout)
	defer cancel()
	if f.RequestID != "" || f.UpstreamRequestID != "" {
		return usageLogsIDPage(ctx, db, f, where, cursor, page, size)
	}
	deferTotal := in.DeferTotal
	// The page rows take the same user_id switch as COUNT/SUM: it is made only
	// when it selects exactly the username's rows (usageLogsUsernameUserID).
	// Without it, username + channel intersected two whole-history indexes on
	// production and hit the 18s cap. A failed check keeps the username plan.
	pageFilter, indexErr := usageLogsWithUserIndex(ctx, db, f)
	if indexErr == nil && pageFilter.userID > 0 {
		if userWhere, werr := buildUsageLogWhere(pageFilter, usageLogWhereList); werr == nil {
			where = userWhere
		} else {
			pageFilter = f
		}
	} else if indexErr != nil {
		pageFilter = f
	}
	var total int64
	if !deferTotal {
		countStarted := time.Now()
		countFilter, err := pageFilter, indexErr
		var countWhere *usageLogWhere
		if err == nil {
			countWhere, err = buildUsageLogWhere(countFilter, usageLogWhereList)
		}
		countSource := "logs"
		skipCount := false
		if err == nil {
			var empty bool
			countWhere, countSource, empty, _ = r.service.usageLogsWithIDSpan(ctx, db, countFilter, countWhere)
			skipCount = empty
		}
		if err == nil && !skipCount {
			err = db.QueryRowContext(ctx, usageLogsSelect+"COUNT(*) FROM "+countSource+countWhere.sql(), countWhere.args...).Scan(&total)
		}
		usageLogsObserve("list_count", countStarted, countFilter)
		if err != nil {
			err = usageLogsQueryErr(ctx, err)
			usageLogsWarnFailure("list_count", countStarted, countFilter, err)
			return Page{}, fmt.Errorf("查询使用日志失败: %w", err)
		}
		if total == 0 {
			// The count already establishes an empty result at this observation.
			// Do not run the derived-page lookup (or fetch content/other) only to
			// return the same empty page. No count or amount cache is introduced.
			return Page{Number: page, Size: size, Total: total, Items: []Item{}}, nil
		}
	}
	query, args := usageLogsPageSQL(where, r.service.usageLogsPageSource(pageFilter), page, size, cursor, deferTotal)
	// Late row lookup: page through the (created_at, id) order using only the
	// index (a created_at/id index covers a time-only filter), then read the
	// full rows, including the large content/other columns, for this page only.
	// Without it a deep OFFSET reads every skipped row in full.
	pageStarted := time.Now()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		usageLogsObserve("list_page", pageStarted, pageFilter)
		return Page{}, fmt.Errorf("查询使用日志失败: %w", usageLogsQueryErr(ctx, err))
	}
	var items []Item
	var pageMatches int64
	if deferTotal {
		items, err = scanUsageLogRowsAndTotal(rows, &pageMatches)
	} else {
		items, err = scanUsageLogRows(rows)
	}
	rows.Close()
	usageLogsObserve("list_page", pageStarted, pageFilter)
	if err != nil {
		return Page{}, fmt.Errorf("查询使用日志失败: %w", usageLogsQueryErr(ctx, err))
	}
	if deferTotal {
		hasMore := pageMatches > int64(size)
		if err := fillUsageLogChannelNames(ctx, db, items, map[int64]string{}); err != nil {
			return Page{}, fmt.Errorf("查询渠道名称失败: %w", usageLogsQueryErr(ctx, err))
		}
		var next *Cursor
		if hasMore {
			last := items[len(items)-1]
			next = &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
		}
		// The first page and its look-ahead prove the exact count when all
		// matches fit on it, including zero matches. No COUNT scan is needed.
		var knownTotal any
		pending := page != 1 || hasMore
		if !pending {
			knownTotal = len(items)
		}
		return Page{Number: page, Size: size, Total: knownTotal, Pending: &pending,
			Items: items, Next: next, HasMore: &hasMore}, nil
	}
	return finishUsageLogsPage(ctx, db, page, size, total, items)
}

func pageBounds(page, size int) (int, int) {
	if size <= 0 {
		size = usageLogsDefaultPageSize
	}
	if size > usageLogsMaxPageSize {
		size = usageLogsMaxPageSize
	}
	if page < 1 || page-1 > math.MaxInt/size {
		page = 1
	}
	return page, size
}

// Count over only the matching ID metadata; do not materialize content/other
// for every matching log. This is deliberately restricted to indexed exact
// request-ID lookups, not general time/name queries with large result sets.
func usageLogsIDPageSQL(where *usageLogWhere, cursor *Cursor, page, size int) (string, []any) {
	args := append([]any{}, where.args...)
	after := ""
	offset := (page - 1) * size
	if page > 1 && cursor != nil {
		after = " WHERE (page_created_at < ? OR (page_created_at = ? AND page_id < ?))"
		args = append(args, cursor.CreatedAt, cursor.CreatedAt, cursor.ID)
		offset = 0
	}
	args = append(args, size, offset)
	query := usageLogsSelect + usageLogColumns + ", page.matched_total FROM logs JOIN (" +
		"SELECT page_id, matched_total FROM (SELECT id AS page_id, created_at AS page_created_at, COUNT(*) OVER() AS matched_total FROM logs" + where.sql() +
		") counted" + after + " ORDER BY page_created_at DESC, page_id DESC LIMIT ? OFFSET ?) page ON logs.id = page.page_id ORDER BY created_at DESC, id DESC"
	return query, args
}

func usageLogsIDPage(ctx context.Context, db usageLogsQuerier, f Filter, where *usageLogWhere, cursor *Cursor, page, size int) (Page, error) {
	query, args := usageLogsIDPageSQL(where, cursor, page, size)
	started := time.Now()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		usageLogsObserve("list_id_page", started, f)
		return Page{}, fmt.Errorf("查询使用日志失败: %w", usageLogsQueryErr(ctx, err))
	}
	var total int64
	items, err := scanUsageLogRowsAndTotal(rows, &total)
	rows.Close()
	usageLogsObserve("list_id_page", started, f)
	if err != nil {
		return Page{}, fmt.Errorf("查询使用日志失败: %w", usageLogsQueryErr(ctx, err))
	}
	// An empty first page proves zero matches. An empty later/cursor page
	// carries no window count: obtain the full count for the existing pager.
	if len(items) == 0 && page > 1 {
		err = db.QueryRowContext(ctx, usageLogsSelect+"COUNT(*) FROM logs"+where.sql(), where.args...).Scan(&total)
		if err != nil {
			return Page{}, fmt.Errorf("查询使用日志失败: %w", usageLogsQueryErr(ctx, err))
		}
	}
	return finishUsageLogsPage(ctx, db, page, size, total, items)
}

func finishUsageLogsPage(ctx context.Context, db usageLogsQuerier, page, size int, total int64, items []Item) (Page, error) {
	if err := fillUsageLogChannelNames(ctx, db, items, map[int64]string{}); err != nil {
		return Page{}, fmt.Errorf("查询渠道名称失败: %w", usageLogsQueryErr(ctx, err))
	}
	var next *Cursor
	if len(items) == size {
		last := items[len(items)-1]
		next = &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	return Page{Number: page, Size: size, Total: total, Items: items, Next: next}, nil
}
