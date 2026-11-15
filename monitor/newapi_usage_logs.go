package monitor

// 使用日志（通用日志）：按 new-api v1.0.0-rc.48 管理员“使用日志 → 通用日志”的行为
// 重新实现（未拷贝其源码），只读查询 new-api 只读从库，供 monitor 管理员排查。
// 口径要点（与 new-api 保持一致）：
//   - 列表与统计保留完整查询范围；本模块不提供导出。
//   - 只有用户名、模型名支持 % 通配（最多 2 个、不能连续、关键词至少 2 字节，_ 按字面）；
//     其余筛选全部精确匹配。
//   - 统计忽略类型、请求 ID、上游请求 ID：用量 = 所选时间内消耗（type=2）的额度之和；
//     RPM/TPM = 服务器最近 60 秒的消耗记录，与所选时间无关。
//   - 排序 created_at DESC, id DESC；每页最多 100 条；总数不封顶。
//   - other 按超级管理员口径返回（旧的顶层 reject_reason 移入 admin_info）。
// 与 monitor 其他页面隔离：独立的并发闸门（容量 2），不占客户门户/客户排障共用的
// usageDetailGate；其中总数/金额统计最多占 1 个名额，给列表留出容量。
// 原列表接口默认仍返回精确总数；页面可用 defer_total=1 先取记录，再按相同
// 列表筛选独立精确计数。每条查询带 MAX_EXECUTION_TIME，不使用近似金额或计数。

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	mysqlDriver "github.com/go-sql-driver/mysql"
)

const (
	usageLogsMaxPageSize      = 100
	usageLogsDefaultPageSize  = 10
	usageLogsGateCapacity     = 2
	usageLogsGateWait         = 15 * time.Second
	usageLogsQueryTimeout     = 25 * time.Second
	usageLogsRPMWindowSeconds = 60
	// A cancelled request kills its own replica statement within this budget,
	// so an abandoned aggregation does not keep running until MAX_EXECUTION_TIME.
	usageLogsKillTimeout = 3 * time.Second
	// MAX_EXECUTION_TIME hint (ms) keeps a slow replica query from running past
	// the HTTP timeout; SQLite (tests) treats it as a comment.
	// MySQL only recognizes optimizer hints AFTER SELECT, including on joins.
	// 18s, deliberately below the production DSN readTimeout (20s): the server
	// then ends the statement with a clear 3024 timeout before the driver's
	// socket read times out and reports a bare "invalid connection".
	usageLogsSelect = "SELECT /*+ MAX_EXECUTION_TIME(18000) */ "
)

var (
	errUsageLogsSourceUnavailable = errors.New("未连接 new-api 只读从库，无法查询使用日志")
	errUsageLogsBusy              = errors.New("使用日志查询繁忙，请稍后再试")
)

// usageLogsQuerier is satisfied by *sql.DB and by the per-request *sql.Conn.
type usageLogsQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type usageLogFilter struct {
	Type              int
	Start             int64
	End               int64
	ModelName         string
	Username          string
	TokenName         string
	Group             string
	RequestID         string
	UpstreamRequestID string
	Channel           int
}

// parseUsageLogFilter reads the admin filter. Unparseable numbers silently turn
// the filter off (0), as in new-api.
func parseUsageLogFilter(c *gin.Context) usageLogFilter {
	atoi := func(k string) int { v, _ := strconv.Atoi(strings.TrimSpace(c.Query(k))); return v }
	atoi64 := func(k string) int64 { v, _ := strconv.ParseInt(strings.TrimSpace(c.Query(k)), 10, 64); return v }
	return usageLogFilter{
		Type:              atoi("type"),
		Start:             atoi64("start_timestamp"),
		End:               atoi64("end_timestamp"),
		ModelName:         c.Query("model_name"),
		Username:          c.Query("username"),
		TokenName:         c.Query("token_name"),
		Group:             c.Query("group"),
		RequestID:         c.Query("request_id"),
		UpstreamRequestID: c.Query("upstream_request_id"),
		Channel:           atoi("channel"),
	}
}

// usageLogMatch returns an exact-match clause, or a LIKE clause when the value
// carries explicit % wildcards (username / model name only).
func usageLogMatch(column, value string) (string, any, error) {
	if !strings.Contains(value, "%") {
		return column + " = ?", value, nil
	}
	if strings.Contains(value, "%%") {
		return "", nil, errors.New("搜索模式中不允许包含连续的 % 通配符")
	}
	if strings.Count(value, "%") > 2 {
		return "", nil, errors.New("搜索模式中最多允许包含 2 个 % 通配符")
	}
	if len(strings.ReplaceAll(value, "%", "")) < 2 {
		return "", nil, errors.New("使用模糊搜索时，关键词长度至少为 2 个字符")
	}
	escaped := strings.NewReplacer("!", "!!", "_", "!_").Replace(value)
	return column + " LIKE ? ESCAPE '!'", escaped, nil
}

type usageLogWhere struct {
	clauses []string
	args    []any
}

func (w *usageLogWhere) add(clause string, args ...any) {
	w.clauses = append(w.clauses, clause)
	w.args = append(w.args, args...)
}

func (w *usageLogWhere) sql() string {
	if len(w.clauses) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(w.clauses, " AND ")
}

type usageLogWhereScope int

const (
	usageLogWhereList usageLogWhereScope = iota // every filter
	usageLogWhereStat                           // type forced to consume; ids ignored
	usageLogWhereRate                           // as stat, but the caller adds its own time window
)

func buildUsageLogWhere(f usageLogFilter, scope usageLogWhereScope) (*usageLogWhere, error) {
	w := &usageLogWhere{}
	switch scope {
	case usageLogWhereList:
		if f.Type != 0 {
			w.add("type = ?", f.Type)
		}
	default:
		w.add("type = ?", 2)
	}
	if f.ModelName != "" {
		clause, arg, err := usageLogMatch("model_name", f.ModelName)
		if err != nil {
			return nil, err
		}
		w.add(clause, arg)
	}
	if f.Username != "" {
		clause, arg, err := usageLogMatch("username", f.Username)
		if err != nil {
			return nil, err
		}
		w.add(clause, arg)
	}
	if f.TokenName != "" {
		w.add("token_name = ?", f.TokenName)
	}
	if scope != usageLogWhereRate {
		if f.Start != 0 {
			w.add("created_at >= ?", f.Start)
		}
		if f.End != 0 {
			w.add("created_at <= ?", f.End)
		}
	}
	if f.Channel != 0 {
		w.add("channel_id = ?", f.Channel)
	}
	if f.Group != "" {
		w.add("`group` = ?", f.Group)
	}
	if scope == usageLogWhereList {
		if f.RequestID != "" {
			w.add("request_id = ?", f.RequestID)
		}
		if f.UpstreamRequestID != "" {
			w.add("upstream_request_id = ?", f.UpstreamRequestID)
		}
	}
	return w, nil
}

// usageLogItem mirrors the JSON shape of a new-api log row.
type usageLogItem struct {
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

func scanUsageLogRows(rows *sql.Rows) ([]usageLogItem, error) {
	return scanUsageLogRowsAndTotal(rows, nil)
}

func scanUsageLogRowsAndTotal(rows *sql.Rows, total *int64) ([]usageLogItem, error) {
	items := []usageLogItem{}
	for rows.Next() {
		var it usageLogItem
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
func fillUsageLogChannelNames(ctx context.Context, db usageLogsQuerier, items []usageLogItem, cache map[int64]string) error {
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

// usageLogsGate bounds concurrent replica queries from this page only.
func (m *Monitor) acquireUsageLogsGate(ctx context.Context) (func(), error) {
	m.initUsageLogsGates()
	return acquireUsageLogsSlot(ctx, m.usageLogsGate)
}

func (m *Monitor) initUsageLogsGates() {
	m.usageLogsGateOnce.Do(func() {
		m.usageLogsGate = make(chan struct{}, usageLogsGateCapacity)
		m.usageLogsBackgroundGate = make(chan struct{}, 1)
	})
}

func acquireUsageLogsSlot(ctx context.Context, gate chan struct{}) (func(), error) {
	waitCtx, cancel := context.WithTimeout(ctx, usageLogsGateWait)
	defer cancel()
	select {
	case gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-gate
			return nil, err
		}
		return func() { <-gate }, nil
	case <-waitCtx.Done():
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, ctx.Err()
		}
		return nil, errUsageLogsBusy
	}
}

func usageLogsFail(c *gin.Context, err error) {
	if errors.Is(err, context.Canceled) {
		c.Status(statusClientClosedRequest)
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": false, "message": err.Error()})
}

func (m *Monitor) usageLogsSource(c *gin.Context, background bool) (usageLogsQuerier, func(), bool) {
	if m.prodDB == nil {
		usageLogsFail(c, errUsageLogsSourceUnavailable)
		return nil, nil, false
	}
	// One deadline includes admission, connection-pool wait and all SQL.
	ctx, cancel := context.WithTimeout(c.Request.Context(), usageLogsQueryTimeout)
	c.Request = c.Request.WithContext(ctx)
	backgroundRelease := func() {}
	if background {
		m.initUsageLogsGates()
		var err error
		backgroundRelease, err = acquireUsageLogsSlot(ctx, m.usageLogsBackgroundGate)
		if err != nil {
			usageLogsFail(c, usageLogsQueryErr(ctx, err))
			cancel()
			return nil, nil, false
		}
	}
	release, err := m.acquireUsageLogsGate(ctx)
	if err != nil {
		backgroundRelease()
		usageLogsFail(c, usageLogsQueryErr(ctx, err))
		cancel()
		return nil, nil, false
	}
	conn, err := m.prodDB.Conn(ctx)
	if err != nil {
		release()
		backgroundRelease()
		usageLogsFail(c, fmt.Errorf("连接 new-api 只读从库失败: %w", usageLogsQueryErr(ctx, err)))
		cancel()
		return nil, nil, false
	}
	threadID := m.usageLogsThreadID(ctx, conn)
	done := func() {
		// A cancelled or timed-out request may leave its statement running on
		// the replica: closing the client socket is not noticed by MySQL until
		// the statement ends. Kill it on our own thread id (MySQL thread ids
		// are never reused while the server runs) and only then free the
		// admission slot, so the gate bounds the replica.
		//
		// Discard our (already aborted) connection before the kill so it is not
		// handed back to other features. The replica statement keeps running
		// regardless of the socket, so the thread id stays valid; the kill goes
		// through its own one-connection pool (usageLogsKillPool).
		kill := ctx.Err() != nil && threadID > 0
		if kill {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
		cancel()
		if !kill {
			release()
			backgroundRelease()
			return
		}
		// The response is already written; do not hold it for up to the kill
		// timeout. The admission slots stay taken until the kill was sent, so
		// the gate still bounds what runs on the replica.
		m.usageLogsKillAsync(threadID, func() { release(); backgroundRelease() })
	}
	return conn, done, true
}

func (m *Monitor) usageLogsKillAsync(threadID int64, releaseSlots func()) {
	go func() {
		defer releaseSlots()
		killCtx, killCancel := context.WithTimeout(context.Background(), usageLogsKillTimeout)
		defer killCancel()
		// 1094 (unknown thread): the statement already ended on its own.
		var killErr *mysqlDriver.MySQLError
		if err := m.usageLogsKill(killCtx, threadID); err != nil && !(errors.As(err, &killErr) && killErr.Number == 1094) {
			slog.Warn("使用日志取消后终止从库查询失败", "err", err)
		}
	}()
}

// usageLogsThreadID returns the replica thread id of conn, or 0 when it is
// unknown (non-MySQL test source, or the lookup failed). 0 disables the kill.
func (m *Monitor) usageLogsThreadID(ctx context.Context, conn *sql.Conn) int64 {
	if m.usageLogsThreadIDFn != nil {
		return m.usageLogsThreadIDFn(ctx, conn)
	}
	if _, ok := m.prodDB.Driver().(*mysqlDriver.MySQLDriver); !ok {
		if _, ok := m.prodDB.Driver().(mysqlDriver.MySQLDriver); !ok {
			return 0
		}
	}
	var id int64
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id); err != nil {
		slog.Warn("使用日志读取从库连接编号失败", "err", err)
		return 0
	}
	return id
}

func (m *Monitor) usageLogsKill(ctx context.Context, threadID int64) error {
	if m.usageLogsKillFn != nil {
		return m.usageLogsKillFn(ctx, threadID)
	}
	// threadID is an int64 from CONNECTION_ID(); formatting it is not an
	// injection surface, and KILL does not need a prepared statement.
	_, err := m.usageLogsKillPool().ExecContext(ctx, fmt.Sprintf("KILL QUERY %d", threadID))
	return err
}

// usageLogsKillPool is a dedicated single-connection pool for KILL QUERY.
// The shared source pool (4, one held by the lease) hands freed capacity to
// callers already queued, so a kill sent through it could wait behind other
// pages exactly when the replica is busiest. This pool holds no idle
// connection: it opens one only while a kill is being sent.
func (m *Monitor) usageLogsKillPool() *sql.DB {
	m.usageLogsKillOnce.Do(func() {
		dsn := strings.TrimSpace(m.cfg.ProdDSN)
		if dsn == "" {
			return
		}
		open := m.cfg.sourceOpen
		if open == nil {
			open = func(dsn string) (*sql.DB, error) { return sql.Open("mysql", dsn) }
		}
		db, err := open(dsn)
		if err != nil {
			slog.Warn("使用日志终止查询专用连接创建失败，改用共享连接池", "err", err)
			return
		}
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(0)
		m.usageLogsKillDB = db
	})
	if m.usageLogsKillDB != nil {
		return m.usageLogsKillDB
	}
	return m.prodDB
}

func usageLogsPage(c *gin.Context) (page, size int) {
	page, _ = strconv.Atoi(c.Query("p"))
	if page < 1 {
		page = 1
	}
	size = 0
	for _, k := range []string{"page_size", "ps", "size"} {
		if v, err := strconv.Atoi(c.Query(k)); err == nil && v > 0 {
			size = v
			break
		}
	}
	if size <= 0 {
		size = usageLogsDefaultPageSize
	}
	if size > usageLogsMaxPageSize {
		size = usageLogsMaxPageSize
	}
	if page-1 > math.MaxInt/size {
		page = 1 // Do not let a malformed page overflow to a negative OFFSET.
	}
	return page, size
}

// A cursor accelerates sequential paging without removing arbitrary page jumps.
// It bounds the page query only, never the complete filtered count or statistics.
type usageLogCursor struct {
	CreatedAt int64 `json:"created_at"`
	ID        int64 `json:"id"`
}

func usageLogsCursor(c *gin.Context) (*usageLogCursor, error) {
	at, id := c.Query("before_created_at"), c.Query("before_id")
	if at == "" && id == "" {
		return nil, nil
	}
	created, errAt := strconv.ParseInt(at, 10, 64)
	key, errID := strconv.ParseInt(id, 10, 64)
	if errAt != nil || errID != nil || created < 0 || key <= 0 {
		return nil, errors.New("日志分页位置无效，请重新搜索")
	}
	return &usageLogCursor{CreatedAt: created, ID: key}, nil
}

// Look ahead using only bounded ID/time metadata, then fetch exactly this
// page's full rows. The window count covers at most size+1 IDs, never the
// complete filtered result. In particular, a large content/other value on
// the next page must not be read or transferred just to determine has_more.
func usageLogsPageSQL(where *usageLogWhere, page, size int, cursor *usageLogCursor, deferTotal bool) (string, []any) {
	pageWhere := &usageLogWhere{clauses: append([]string{}, where.clauses...), args: append([]any{}, where.args...)}
	offset := (page - 1) * size
	if page > 1 && cursor != nil {
		pageWhere.add("(created_at < ? OR (created_at = ? AND id < ?))", cursor.CreatedAt, cursor.CreatedAt, cursor.ID)
		offset = 0
	}
	args := append([]any{}, pageWhere.args...)
	if !deferTotal {
		return usageLogsSelect + usageLogColumns + " FROM logs JOIN (SELECT id AS page_id FROM logs" + pageWhere.sql() +
			" ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?) page ON logs.id = page.page_id ORDER BY created_at DESC, id DESC", append(args, size, offset)
	}
	return usageLogsSelect + usageLogColumns + ", page.page_matches FROM logs JOIN (" +
		"SELECT page_id, COUNT(*) OVER() AS page_matches FROM (SELECT id AS page_id, created_at AS page_created_at FROM logs" + pageWhere.sql() +
		" ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?) lookahead ORDER BY page_created_at DESC, page_id DESC LIMIT ?) page ON logs.id = page.page_id ORDER BY created_at DESC, id DESC", append(args, size+1, offset, size)
}

// GET /usage-logs/list
func (m *Monitor) serveUsageLogsList(c *gin.Context) {
	f := parseUsageLogFilter(c)
	where, err := buildUsageLogWhere(f, usageLogWhereList)
	if err != nil {
		usageLogsFail(c, err)
		return
	}
	page, size := usageLogsPage(c)
	cursor, err := usageLogsCursor(c)
	if err != nil {
		usageLogsFail(c, err)
		return
	}
	db, release, ok := m.usageLogsSource(c, false)
	if !ok {
		return
	}
	defer release()
	ctx, cancel := context.WithTimeout(c.Request.Context(), usageLogsQueryTimeout)
	defer cancel()
	if f.RequestID != "" || f.UpstreamRequestID != "" {
		m.serveUsageLogsIDPage(ctx, c, db, f, where, cursor, page, size)
		return
	}
	deferTotal := c.Query("defer_total") == "1"
	var total int64
	if !deferTotal {
		countStarted := time.Now()
		err = db.QueryRowContext(ctx, usageLogsSelect+"COUNT(*) FROM logs"+where.sql(), where.args...).Scan(&total)
		usageLogsObserve("list_count", countStarted, f)
		if err != nil {
			usageLogsFail(c, fmt.Errorf("查询使用日志失败: %w", usageLogsQueryErr(ctx, err)))
			return
		}
		if total == 0 {
			// The count already establishes an empty result at this observation.
			// Do not run the derived-page lookup (or fetch content/other) only to
			// return the same empty page. No count or amount cache is introduced.
			c.JSON(http.StatusOK, gin.H{"success": true, "message": "", "data": gin.H{
				"page": page, "page_size": size, "total": total, "items": []usageLogItem{}, "next_cursor": nil,
			}})
			return
		}
	}
	query, args := usageLogsPageSQL(where, page, size, cursor, deferTotal)
	// Late row lookup: page through the (created_at, id) order using only the
	// index (a created_at/id index covers a time-only filter), then read the
	// full rows, including the large content/other columns, for this page only.
	// Without it a deep OFFSET reads every skipped row in full.
	pageStarted := time.Now()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		usageLogsObserve("list_page", pageStarted, f)
		usageLogsFail(c, fmt.Errorf("查询使用日志失败: %w", usageLogsQueryErr(ctx, err)))
		return
	}
	var items []usageLogItem
	var pageMatches int64
	if deferTotal {
		items, err = scanUsageLogRowsAndTotal(rows, &pageMatches)
	} else {
		items, err = scanUsageLogRows(rows)
	}
	rows.Close()
	usageLogsObserve("list_page", pageStarted, f)
	if err != nil {
		usageLogsFail(c, fmt.Errorf("查询使用日志失败: %w", usageLogsQueryErr(ctx, err)))
		return
	}
	if deferTotal {
		hasMore := pageMatches > int64(size)
		if err := fillUsageLogChannelNames(ctx, db, items, map[int64]string{}); err != nil {
			usageLogsFail(c, fmt.Errorf("查询渠道名称失败: %w", usageLogsQueryErr(ctx, err)))
			return
		}
		var next *usageLogCursor
		if hasMore {
			last := items[len(items)-1]
			next = &usageLogCursor{CreatedAt: last.CreatedAt, ID: last.ID}
		}
		// The first page and its look-ahead prove the exact count when all
		// matches fit on it, including zero matches. No COUNT scan is needed.
		var knownTotal any
		pending := page != 1 || hasMore
		if !pending {
			knownTotal = len(items)
		}
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "", "data": gin.H{
			"page": page, "page_size": size, "total": knownTotal, "total_pending": pending,
			"items": items, "next_cursor": next, "has_more": hasMore,
		}})
		return
	}
	writeUsageLogsPage(ctx, c, db, page, size, total, items)
}

// Exact LIST count, not the consume-only statistics count. Never apply a
// pagination cursor here or substitute the rate/count from /stat.
func (m *Monitor) serveUsageLogsCount(c *gin.Context) {
	f := parseUsageLogFilter(c)
	where, err := buildUsageLogWhere(f, usageLogWhereList)
	if err != nil {
		usageLogsFail(c, err)
		return
	}
	db, release, ok := m.usageLogsSource(c, true)
	if !ok {
		return
	}
	defer release()
	ctx := c.Request.Context()
	var total int64
	started := time.Now()
	err = db.QueryRowContext(ctx, usageLogsSelect+"COUNT(*) FROM logs"+where.sql(), where.args...).Scan(&total)
	usageLogsObserve("list_count", started, f)
	if err != nil {
		usageLogsFail(c, fmt.Errorf("查询日志总数失败: %w", usageLogsQueryErr(ctx, err)))
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "", "data": gin.H{"total": total}})
}

// Count over only the matching ID metadata; do not materialize content/other
// for every matching log. This is deliberately restricted to indexed exact
// request-ID lookups, not general time/name queries with large result sets.
func usageLogsIDPageSQL(where *usageLogWhere, cursor *usageLogCursor, page, size int) (string, []any) {
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

func (m *Monitor) serveUsageLogsIDPage(ctx context.Context, c *gin.Context, db usageLogsQuerier, f usageLogFilter, where *usageLogWhere, cursor *usageLogCursor, page, size int) {
	query, args := usageLogsIDPageSQL(where, cursor, page, size)
	started := time.Now()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		usageLogsObserve("list_id_page", started, f)
		usageLogsFail(c, fmt.Errorf("查询使用日志失败: %w", usageLogsQueryErr(ctx, err)))
		return
	}
	var total int64
	items, err := scanUsageLogRowsAndTotal(rows, &total)
	rows.Close()
	usageLogsObserve("list_id_page", started, f)
	if err != nil {
		usageLogsFail(c, fmt.Errorf("查询使用日志失败: %w", usageLogsQueryErr(ctx, err)))
		return
	}
	// An empty first page proves zero matches. An empty later/cursor page
	// carries no window count: obtain the full count for the existing pager.
	if len(items) == 0 && page > 1 {
		err = db.QueryRowContext(ctx, usageLogsSelect+"COUNT(*) FROM logs"+where.sql(), where.args...).Scan(&total)
		if err != nil {
			usageLogsFail(c, fmt.Errorf("查询使用日志失败: %w", usageLogsQueryErr(ctx, err)))
			return
		}
	}
	writeUsageLogsPage(ctx, c, db, page, size, total, items)
}

func writeUsageLogsPage(ctx context.Context, c *gin.Context, db usageLogsQuerier, page, size int, total int64, items []usageLogItem) {
	if err := fillUsageLogChannelNames(ctx, db, items, map[int64]string{}); err != nil {
		usageLogsFail(c, fmt.Errorf("查询渠道名称失败: %w", usageLogsQueryErr(ctx, err)))
		return
	}
	var next *usageLogCursor
	if len(items) == size {
		last := items[len(items)-1]
		next = &usageLogCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "", "data": gin.H{
		"page": page, "page_size": size, "total": total, "items": items, "next_cursor": next,
	}})
}

// usageLogsSlowQuery is the duration above which a replica query from this
// page is logged, with the shape of the filter (never its values), so slow
// patterns can be found without exposing user data.
const usageLogsSlowQuery = 2 * time.Second

func usageLogsObserve(phase string, started time.Time, f usageLogFilter) {
	elapsed := time.Since(started)
	if elapsed < usageLogsSlowQuery {
		return
	}
	span := int64(0)
	if f.Start > 0 && f.End > f.Start {
		span = (f.End - f.Start) / 3600
	}
	slog.Warn("使用日志从库查询较慢", "phase", phase, "elapsed_ms", elapsed.Milliseconds(), "range_hours", span,
		"type", f.Type != 0, "model", f.ModelName != "", "username", f.Username != "", "token", f.TokenName != "",
		"channel", f.Channel != 0, "group", f.Group != "", "request_id", f.RequestID != "", "upstream_request_id", f.UpstreamRequestID != "")
}

func usageLogsQueryErr(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	var serverErr *mysqlDriver.MySQLError
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || (errors.As(err, &serverErr) && serverErr.Number == 3024) {
		return errors.New("查询超时，请缩小时间范围或增加筛选条件")
	}
	// A socket read timeout (DSN readTimeout) or dropped tunnel surfaces from
	// the driver as an invalid connection; say what it means to the admin.
	var netErr net.Error
	if errors.Is(err, mysqlDriver.ErrInvalidConn) || errors.Is(err, driver.ErrBadConn) || (errors.As(err, &netErr) && netErr.Timeout()) {
		return errors.New("查询超时或从库连接中断，请缩小时间范围后重试")
	}
	return err
}

// GET /usage-logs/stat
func (m *Monitor) serveUsageLogsStat(c *gin.Context) {
	f := parseUsageLogFilter(c)
	quotaWhere, err := buildUsageLogWhere(f, usageLogWhereStat)
	if err != nil {
		usageLogsFail(c, err)
		return
	}
	rateWhere, _ := buildUsageLogWhere(f, usageLogWhereRate)
	rateWhere.add("created_at >= ?", time.Now().Unix()-usageLogsRPMWindowSeconds)
	// skip_quota=1: the page defers the range SUM (the costly part over a wide
	// range) until the admin asks for it. The remaining last-minute rates are
	// cheap, so they take a list slot instead of queueing behind heavy work.
	skipQuota := c.Query("skip_quota") == "1"
	db, release, ok := m.usageLogsSource(c, !skipQuota)
	if !ok {
		return
	}
	defer release()
	ctx, cancel := context.WithTimeout(c.Request.Context(), usageLogsQueryTimeout)
	defer cancel()
	var quota, rpm, prompt, completion int64
	if !skipQuota {
		quotaStarted := time.Now()
		err = db.QueryRowContext(ctx, usageLogsSelect+"COALESCE(SUM(quota),0) FROM logs"+quotaWhere.sql(), quotaWhere.args...).Scan(&quota)
		usageLogsObserve("stat_quota", quotaStarted, f)
		if err != nil {
			usageLogsFail(c, fmt.Errorf("查询统计数据失败: %w", usageLogsQueryErr(ctx, err)))
			return
		}
	}
	if err := db.QueryRowContext(ctx, usageLogsSelect+"COUNT(*), COALESCE(SUM(prompt_tokens),0), COALESCE(SUM(completion_tokens),0) FROM logs"+rateWhere.sql(), rateWhere.args...).Scan(&rpm, &prompt, &completion); err != nil {
		usageLogsFail(c, fmt.Errorf("查询统计数据失败: %w", usageLogsQueryErr(ctx, err)))
		return
	}
	var quotaOut any = quota
	if skipQuota {
		quotaOut = nil
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "", "data": gin.H{"quota": quotaOut, "quota_skipped": skipQuota, "rpm": rpm, "tpm": prompt + completion}})
}

// usageLogsOptions holds the display settings read from the
// replica's options table, with new-api's defaults.
type usageLogsOptions struct {
	QuotaPerUnit        float64  `json:"quota_per_unit"`
	QuotaDisplayType    string   `json:"quota_display_type"`
	USDExchangeRate     float64  `json:"usd_exchange_rate"`
	CustomSymbol        string   `json:"custom_currency_symbol"`
	CustomExchangeRate  float64  `json:"custom_currency_exchange_rate"`
	Groups              []string `json:"groups"`
	HasEnabledSubscribe bool     `json:"has_enabled_subscription_plans"`
}

func (m *Monitor) loadUsageLogsOptions(ctx context.Context, db usageLogsQuerier) (usageLogsOptions, error) {
	o := usageLogsOptions{QuotaPerUnit: 500000, QuotaDisplayType: "USD", USDExchangeRate: 7.3, CustomSymbol: "¤",
		CustomExchangeRate: 1, Groups: []string{}}
	keys := []any{"QuotaPerUnit", "USDExchangeRate", "GroupRatio", "general_setting.quota_display_type",
		"general_setting.custom_currency_symbol", "general_setting.custom_currency_exchange_rate"}
	rows, err := db.QueryContext(ctx, usageLogsSelect+"`key`, COALESCE(`value`,'') FROM options WHERE `key` IN (?,?,?,?,?,?)", keys...)
	if err != nil {
		return o, err
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return o, err
		}
		v = strings.TrimSpace(v)
		num, numErr := strconv.ParseFloat(v, 64)
		finite := numErr == nil && !math.IsNaN(num) && !math.IsInf(num, 0)
		switch k {
		case "QuotaPerUnit":
			if finite && num > 0 {
				o.QuotaPerUnit = num
			}
		case "USDExchangeRate":
			if finite && num > 0 {
				o.USDExchangeRate = num
			}
		case "general_setting.quota_display_type":
			if v != "" {
				o.QuotaDisplayType = strings.ToUpper(v)
			}
		case "general_setting.custom_currency_symbol":
			if v != "" {
				o.CustomSymbol = v
			}
		case "general_setting.custom_currency_exchange_rate":
			if finite && num > 0 {
				o.CustomExchangeRate = num
			}
		case "GroupRatio":
			var ratios map[string]json.RawMessage
			if json.Unmarshal([]byte(v), &ratios) == nil {
				for name := range ratios {
					if name != "auto" {
						o.Groups = append(o.Groups, name)
					}
				}
				sort.Strings(o.Groups)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return o, err
	}
	var enabled int64
	// The wallet icon on cost cells only shows when a plan is enabled. A
	// missing table (older schema) just means no plans.
	if err := db.QueryRowContext(ctx, usageLogsSelect+"COUNT(*) FROM subscription_plans WHERE enabled = 1").Scan(&enabled); err == nil {
		o.HasEnabledSubscribe = enabled > 0
	}
	return o, nil
}

// GET /usage-logs/meta
func (m *Monitor) serveUsageLogsMeta(c *gin.Context) {
	db, release, ok := m.usageLogsSource(c, false)
	if !ok {
		return
	}
	defer release()
	ctx, cancel := context.WithTimeout(c.Request.Context(), usageLogsQueryTimeout)
	defer cancel()
	o, err := m.loadUsageLogsOptions(ctx, db)
	if err != nil {
		usageLogsFail(c, fmt.Errorf("读取站点设置失败: %w", usageLogsQueryErr(ctx, err)))
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "", "data": o})
}

// GET /usage-logs/user/:id
func (m *Monitor) serveUsageLogsUser(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		usageLogsFail(c, errors.New("用户 ID 无效"))
		return
	}
	db, release, ok := m.usageLogsSource(c, false)
	if !ok {
		return
	}
	defer release()
	ctx, cancel := context.WithTimeout(c.Request.Context(), usageLogsQueryTimeout)
	defer cancel()
	var u struct {
		ID           int64  `json:"id"`
		Username     string `json:"username"`
		DisplayName  string `json:"display_name"`
		Quota        int64  `json:"quota"`
		UsedQuota    int64  `json:"used_quota"`
		RequestCount int64  `json:"request_count"`
		Group        string `json:"group"`
		AffCode      string `json:"aff_code"`
		AffCount     int64  `json:"aff_count"`
		AffQuota     int64  `json:"aff_quota"`
		Remark       string `json:"remark"`
	}
	err = db.QueryRowContext(ctx, usageLogsSelect+"id, COALESCE(username,''), COALESCE(display_name,''), COALESCE(quota,0), "+
		"COALESCE(used_quota,0), COALESCE(request_count,0), COALESCE(`group`,''), COALESCE(aff_code,''), COALESCE(aff_count,0), "+
		"COALESCE(aff_quota,0), COALESCE(remark,'') FROM users WHERE id = ? AND deleted_at IS NULL", id).
		Scan(&u.ID, &u.Username, &u.DisplayName, &u.Quota, &u.UsedQuota, &u.RequestCount, &u.Group, &u.AffCode, &u.AffCount, &u.AffQuota, &u.Remark)
	if errors.Is(err, sql.ErrNoRows) {
		usageLogsFail(c, errors.New("用户不存在"))
		return
	}
	if err != nil {
		usageLogsFail(c, fmt.Errorf("获取用户信息失败: %w", usageLogsQueryErr(ctx, err)))
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "", "data": u})
}
