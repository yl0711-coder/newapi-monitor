package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	mysqlDriver "github.com/go-sql-driver/mysql"
)

func newUsageLogsFakeProd(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/prod.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, s := range []string{
		"CREATE TABLE logs (id INTEGER PRIMARY KEY, user_id INTEGER, created_at INTEGER, type INTEGER, content TEXT, username TEXT, token_name TEXT, model_name TEXT, quota INTEGER, prompt_tokens INTEGER, completion_tokens INTEGER, use_time INTEGER, is_stream INTEGER, channel_id INTEGER, token_id INTEGER, `group` TEXT, ip TEXT, request_id TEXT, upstream_request_id TEXT, other TEXT)",
		"CREATE TABLE channels (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, display_name TEXT, quota INTEGER, used_quota INTEGER, request_count INTEGER, `group` TEXT, aff_code TEXT, aff_count INTEGER, aff_quota INTEGER, remark TEXT, deleted_at TIMESTAMP, password TEXT, access_token TEXT)",
		"CREATE TABLE options (`key` TEXT PRIMARY KEY, `value` TEXT)",
		"CREATE TABLE subscription_plans (id INTEGER PRIMARY KEY, enabled INTEGER)",
		"INSERT INTO channels VALUES (7, 'gateai-codex'), (9, 'jikesoft')",
		"INSERT INTO users (id, username, display_name, quota, used_quota, request_count, `group`, aff_code, aff_count, aff_quota, remark, password, access_token) VALUES (31, 'kjl_Dev', 'KJL', 1000000, 250000, 42, 'codex', 'AFF1', 2, 0, 'vip', 'secret-hash', 'secret-token')",
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	return db
}

type usageLogRow struct {
	id                      int64
	userID                  int64
	createdAt               int64
	typ                     int
	username, token, model  string
	quota, prompt, complete int64
	channel                 int64
	group, requestID        string
	other                   string
}

func insertUsageLogs(t *testing.T, db *sql.DB, rows ...usageLogRow) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if _, err := tx.Exec("INSERT INTO logs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", r.id, r.userID, r.createdAt, r.typ, "content", r.username,
			r.token, r.model, r.quota, r.prompt, r.complete, 3, 1, r.channel, 5, r.group, "1.2.3.4", r.requestID, "up-"+r.requestID, r.other); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func usageLogsGET(t *testing.T, m *Monitor, handler gin.HandlerFunc, target string, params gin.Params) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, target, nil)
	c.Params = params
	handler(c)
	return w
}

type usageLogsListResp struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    struct {
		Page     int             `json:"page"`
		PageSize int             `json:"page_size"`
		Total    int64           `json:"total"`
		Pending  bool            `json:"total_pending"`
		HasMore  bool            `json:"has_more"`
		Items    []usageLogItem  `json:"items"`
		Next     *usageLogCursor `json:"next_cursor"`
	} `json:"data"`
}

func TestUsageLogMatchRules(t *testing.T) {
	for _, c := range []struct{ in, clause, arg, err string }{
		{"gpt-5", "model_name = ?", "gpt-5", ""},
		{"gpt%", "model_name LIKE ? ESCAPE '!'", "gpt%", ""},
		{"%a_b!%", "model_name LIKE ? ESCAPE '!'", "%a!_b!!%", ""},
		{"g%%", "", "", "连续"},
		{"%a%b%", "", "", "最多"},
		{"%a%", "", "", "至少为 2"},
	} {
		clause, arg, err := usageLogMatch("model_name", c.in)
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Fatalf("%q: want error %q, got %v", c.in, c.err, err)
			}
			continue
		}
		if err != nil || clause != c.clause || arg != c.arg {
			t.Fatalf("%q: clause=%q arg=%v err=%v", c.in, clause, arg, err)
		}
	}
}

func TestUsageLogsListFiltersOrderPagingAndChannelNames(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	base := int64(1794500000)
	insertUsageLogs(t, db,
		usageLogRow{1, 31, base, 2, "kjl_Dev", "model-link-codex", "gpt-6-astra", 31248, 5043, 777, 7, "codex", "r1", `{"model_ratio":1}`},
		usageLogRow{2, 31, base + 10, 5, "kjl_Dev", "model-link-codex", "gpt-6-astra", 0, 0, 0, 9, "codex", "r2", `{"reject_reason":"blocked","admin_info":{"use_channel":[9,7]}}`},
		usageLogRow{3, 32, base + 10, 2, "other_user", "t2", "claude-x", 100, 1, 1, 0, "default", "r3", ""},
		usageLogRow{4, 31, base - 5000, 2, "kjl_Dev", "model-link-codex", "gpt_6", 1, 1, 1, 42, "codex", "r4", "not json"},
	)
	m := newChannelUpstreamTestMonitor(t)
	m.prodDB = db

	w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?p=1&page_size=500", nil)
	var resp usageLogsListResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || !resp.Success {
		t.Fatalf("list: %s", w.Body.String())
	}
	if resp.Data.PageSize != 100 || resp.Data.Total != 4 || len(resp.Data.Items) != 4 {
		t.Fatalf("page size capped at 100 and total uncapped: %+v", resp.Data)
	}
	if got := []int64{resp.Data.Items[0].ID, resp.Data.Items[1].ID, resp.Data.Items[2].ID, resp.Data.Items[3].ID}; got[0] != 3 || got[1] != 2 || got[2] != 1 || got[3] != 4 {
		t.Fatalf("order must be created_at DESC, id DESC: %v", got)
	}
	byID := map[int64]usageLogItem{}
	for _, it := range resp.Data.Items {
		byID[it.ID] = it
	}
	if byID[1].ChannelName != "gateai-codex" || byID[3].ChannelName != "" || byID[4].ChannelName != "" || !byID[1].IsStream || byID[1].IP != "1.2.3.4" || byID[1].UpstreamRequestID != "up-r1" {
		t.Fatalf("channel names / fields: %+v", byID)
	}
	var other map[string]any
	if json.Unmarshal([]byte(byID[2].Other), &other) != nil || other["reject_reason"] != nil ||
		other["admin_info"].(map[string]any)["reject_reason"] != "blocked" {
		t.Fatalf("legacy reject_reason must move into admin_info: %s", byID[2].Other)
	}
	if byID[4].Other != "not json" {
		t.Fatalf("root view keeps invalid other raw: %q", byID[4].Other)
	}

	for _, c := range []struct {
		query string
		want  []int64
	}{
		{"type=5", []int64{2}},
		{"username=kjl_Dev&start_timestamp=" + fmt.Sprint(base) + "&end_timestamp=" + fmt.Sprint(base+10), []int64{2, 1}},
		{"username=kjl%25", []int64{2, 1, 4}},
		{"model_name=gpt_6", []int64{4}},
		{"model_name=gpt%25", []int64{2, 1, 4}},
		{"channel=9", []int64{2}},
		{"channel=abc", []int64{3, 2, 1, 4}},
		{"group=default", []int64{3}},
		{"token_name=t2", []int64{3}},
		{"token_name=t", nil},
		{"request_id=r1", []int64{1}},
		{"request_id=r1&upstream_request_id=up-r1&type=2&username=kjl_Dev&model_name=gpt-6-astra&token_name=model-link-codex&channel=7&group=codex&start_timestamp=" + fmt.Sprint(base) + "&end_timestamp=" + fmt.Sprint(base), []int64{1}},
		{"request_id=r1&upstream_request_id=up-r2", nil},
		{"request_id=r1&start_timestamp=" + fmt.Sprint(base+1), nil},
		{"upstream_request_id=up-r4", []int64{4}},
		{"p=2&page_size=2", []int64{1, 4}},
	} {
		w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?"+c.query, nil)
		var r usageLogsListResp
		if json.Unmarshal(w.Body.Bytes(), &r) != nil || !r.Success {
			t.Fatalf("%s: %s", c.query, w.Body.String())
		}
		var got []int64
		for _, it := range r.Data.Items {
			got = append(got, it.ID)
		}
		if fmt.Sprint(got) != fmt.Sprint(c.want) {
			t.Fatalf("%s: got %v want %v", c.query, got, c.want)
		}
	}
	w = usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?username=%25a%25", nil)
	if !strings.Contains(w.Body.String(), "至少为 2") {
		t.Fatalf("wildcard validation error must be reported: %s", w.Body.String())
	}
}

func TestUsageLogsStatIgnoresTypeAndIDsAndUsesLastMinuteForRates(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	now := time.Now().Unix()
	insertUsageLogs(t, db,
		usageLogRow{1, 31, now - 30, 2, "kjl_Dev", "a", "m", 100, 10, 5, 7, "codex", "r1", ""},
		usageLogRow{2, 31, now - 3600, 2, "kjl_Dev", "a", "m", 200, 1, 1, 7, "codex", "r2", ""},
		usageLogRow{3, 31, now - 20, 5, "kjl_Dev", "a", "m", 999, 50, 50, 7, "codex", "r3", ""},
		usageLogRow{4, 32, now - 10, 2, "other", "b", "m", 400, 3, 3, 9, "default", "r4", ""},
	)
	m := newChannelUpstreamTestMonitor(t)
	m.prodDB = db
	w := usageLogsGET(t, m, m.serveUsageLogsStat, fmt.Sprintf("/usage-logs/stat?type=5&request_id=nope&username=kjl_Dev&start_timestamp=%d", now-7200), nil)
	var resp struct {
		Success bool
		Data    struct{ Quota, RPM, TPM int64 }
	}
	if json.Unmarshal(w.Body.Bytes(), &resp) != nil || !resp.Success {
		t.Fatalf("stat: %s", w.Body.String())
	}
	if resp.Data.Quota != 300 || resp.Data.RPM != 1 || resp.Data.TPM != 15 {
		t.Fatalf("quota sums consume rows in range; rpm/tpm last 60s consume only: %+v", resp.Data)
	}
}

func TestUsageLogsCursorPagesPreserveCompleteResults(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	var rows []usageLogRow
	for i := int64(1); i <= 2005; i++ {
		rows = append(rows, usageLogRow{i, 31, 1794500000 - i/3, 2, "fixture", "t", "model", 1, 1, 1, 7, "codex", fmt.Sprint(i), "{}"})
	}
	insertUsageLogs(t, db, rows...)
	m := newChannelUpstreamTestMonitor(t)
	m.prodDB = db
	seen := map[int64]bool{}
	var cursor *usageLogCursor
	for page := 1; ; page++ {
		q := fmt.Sprintf("/usage-logs/list?p=%d&page_size=100&username=fixture", page)
		if cursor != nil {
			q += fmt.Sprintf("&before_created_at=%d&before_id=%d", cursor.CreatedAt, cursor.ID)
		}
		w := usageLogsGET(t, m, m.serveUsageLogsList, q, nil)
		var r usageLogsListResp
		if json.Unmarshal(w.Body.Bytes(), &r) != nil || !r.Success || r.Data.Total != 2005 {
			t.Fatalf("page %d: %s", page, w.Body.String())
		}
		for _, it := range r.Data.Items {
			if seen[it.ID] {
				t.Fatalf("duplicate %d on page %d", it.ID, page)
			}
			seen[it.ID] = true
		}
		cursor = r.Data.Next
		if cursor == nil {
			break
		}
		if page > 21 {
			t.Fatal("cursor did not terminate")
		}
	}
	if len(seen) != 2005 {
		t.Fatalf("missing rows: %d of 2005", len(seen))
	}
	for _, q := range []string{"before_created_at=1", "before_id=2", "before_created_at=-1&before_id=2", "before_created_at=1&before_id=bad"} {
		w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?p=2&"+q, nil)
		if !strings.Contains(w.Body.String(), "分页位置无效") {
			t.Fatalf("bad cursor accepted: %s", w.Body.String())
		}
	}
}

func TestUsageLogsEmptyResultNeedsOnlyOneSourceQuery(t *testing.T) {
	db, counts := newCountingFakeProdDB(t)
	m := newChannelUpstreamTestMonitor(t)
	m.prodDB = db
	for _, filter := range []string{"model_name=not-found", "start_timestamp=9999999999"} {
		counts.reset()
		w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?p=2&page_size=100&"+filter, nil)
		var r usageLogsListResp
		if json.Unmarshal(w.Body.Bytes(), &r) != nil || !r.Success || r.Data.Total != 0 || r.Data.Page != 2 || r.Data.PageSize != 100 || r.Data.Items == nil || len(r.Data.Items) != 0 || r.Data.Next != nil {
			t.Fatalf("empty result changed: %s", w.Body.String())
		}
		if got := counts.logs.Load(); got != 1 {
			t.Fatalf("%s: empty query made %d source queries, want count only", filter, got)
		}
	}
}

func newCountedUsageLogsDB(t *testing.T) (*sql.DB, *usageQueryCounts) {
	t.Helper()
	db, counts := newCountingFakeProdDB(t)
	if _, err := db.Exec("ALTER TABLE logs ADD COLUMN upstream_request_id TEXT DEFAULT ''"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("ALTER TABLE logs ADD COLUMN ip TEXT DEFAULT ''"); err != nil {
		t.Fatal(err)
	}
	return db, counts
}

func TestUsageLogsDeferredTotalPreservesRecordsAndExactCount(t *testing.T) {
	db, counts := newCountedUsageLogsDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 205; i++ {
		model, typ := "large", 2
		if i <= 100 {
			model = "small"
		}
		if i%2 != 0 {
			typ = 5
		}
		if _, err := tx.Exec("INSERT INTO logs (id,user_id,created_at,type,username,model_name,request_id,upstream_request_id,quota) VALUES (?,?,?,?,?,?,?,?,?)", i, 31, 1794500000+i/3, typ, "fixture", model, "same", "up-same", i); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	m := newChannelUpstreamTestMonitor(t)
	m.prodDB = db
	read := func(query string, wantQueries int64) usageLogsListResp {
		t.Helper()
		counts.reset()
		w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?"+query, nil)
		var r usageLogsListResp
		if json.Unmarshal(w.Body.Bytes(), &r) != nil || !r.Success || counts.logs.Load() != wantQueries {
			t.Fatalf("%s queries=%d: %s", query, counts.logs.Load(), w.Body.String())
		}
		if r.Data.Pending && !strings.Contains(w.Body.String(), `"total":null`) {
			t.Fatal("unknown exact total must not be represented as zero")
		}
		return r
	}
	var cursor *usageLogCursor
	for page := 1; page <= 3; page++ {
		q := fmt.Sprintf("p=%d&page_size=100", page)
		legacy := read(q, 2)
		if cursor != nil {
			q += fmt.Sprintf("&before_created_at=%d&before_id=%d", cursor.CreatedAt, cursor.ID)
		}
		fast := read(q+"&defer_total=1", 1)
		if !fast.Data.Pending || fast.Data.HasMore != (page < 3) || len(fast.Data.Items) != len(legacy.Data.Items) {
			t.Fatalf("fast page metadata changed: %+v", fast.Data)
		}
		for i, row := range fast.Data.Items {
			a, _ := json.Marshal(row)
			b, _ := json.Marshal(legacy.Data.Items[i])
			if string(a) != string(b) {
				t.Fatal("fast page changed a complete record")
			}
		}
		cursor = fast.Data.Next
	}
	for _, c := range []struct {
		filter string
		total  int64
	}{{"model_name=small", 100}, {"model_name=absent", 0}} {
		r := read(c.filter+"&page_size=100&defer_total=1", 1)
		if r.Data.Pending || r.Data.HasMore || r.Data.Total != c.total || int64(len(r.Data.Items)) != c.total {
			t.Fatalf("complete first page count must need no COUNT query: %+v", r.Data)
		}
	}
	for _, filter := range []string{"", "type=5", "request_id=same&upstream_request_id=up-same&type=5", "request_id=absent", "username=fixture&model_name=small&end_timestamp=1794500020"} {
		wantQueries := int64(2)
		if strings.HasPrefix(filter, "request_id=") {
			wantQueries = 1
		}
		legacy := read(filter, wantQueries)
		counts.reset()
		w := usageLogsGET(t, m, m.serveUsageLogsCount, "/usage-logs/count?"+filter+"&p=999&before_created_at=1&before_id=1", nil)
		var r struct {
			Success bool
			Data    struct{ Total int64 }
		}
		if json.Unmarshal(w.Body.Bytes(), &r) != nil || !r.Success || r.Data.Total != legacy.Data.Total || counts.logs.Load() != 1 {
			t.Fatalf("list count scope changed for %s: %s", filter, w.Body.String())
		}
	}
}

func TestUsageLogsPageStrategiesRemainEquivalentAcrossScopes(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	rows := make([]usageLogRow, 1307)
	for i := range rows {
		rows[i] = usageLogRow{int64(i + 1), int64(i%4 + 1), 1794500000 + int64(i/4),
			[]int{1, 2, 5, 6}[i%4], []string{"alpha_01", "alphaA01", "用户乙", "beta"}[i%4],
			fmt.Sprintf("token-%d", i%3), []string{"gpt_6", "gpt-6", "claude"}[i%3],
			int64(i + 1), int64(i * 3), int64(i % 21), int64(i%2 + 1), fmt.Sprintf("g%d", i%2),
			fmt.Sprintf("dup-%d", i%5), `{"admin_info":null,"reject_reason":"legacy","big":9007199254740993}`}
	}
	insertUsageLogs(t, db, rows...)
	// Large complete fields remain available. They are not shortened to make
	// the page faster; the look-ahead must only materialize IDs and time.
	if _, err := db.Exec("UPDATE logs SET content=?,other=? WHERE id=1207", strings.Repeat("大字段", 20000), `{"padding":"`+strings.Repeat("x", 65536)+`"}`); err != nil {
		t.Fatal(err)
	}
	m := newChannelUpstreamTestMonitor(t)
	m.prodDB = db
	filters := []string{"", "type=2", "type=5", "username=alpha_01", "username=alpha%25", "username=%25alpha_01%25", "model_name=gpt_6", "model_name=gpt%25", "group=g1", "channel=2", "token_name=token-1", "request_id=dup-1", "upstream_request_id=up-dup-1", "username=" + url.QueryEscape("用户乙"), "start_timestamp=1794500010&end_timestamp=1794500021", "type=6&group=g1&model_name=claude", "request_id=absent"}
	for _, filter := range filters {
		for _, size := range []int{1, 7, 100} {
			for _, page := range []int{1, 2, 4} {
				q := fmt.Sprintf("/usage-logs/list?%s&p=%d&page_size=%d", filter, page, size)
				var legacy, fast usageLogsListResp
				a := usageLogsGET(t, m, m.serveUsageLogsList, q, nil)
				b := usageLogsGET(t, m, m.serveUsageLogsList, q+"&defer_total=1", nil)
				if json.Unmarshal(a.Body.Bytes(), &legacy) != nil || json.Unmarshal(b.Body.Bytes(), &fast) != nil || !legacy.Success || !fast.Success {
					t.Fatalf("%s: legacy=%s fast=%s", q, a.Body.String(), b.Body.String())
				}
				if !reflect.DeepEqual(legacy.Data.Items, fast.Data.Items) || (!fast.Data.Pending && fast.Data.Total != legacy.Data.Total) {
					t.Fatalf("record/order/known count changed for %s", q)
				}
				if !strings.Contains(filter, "request_id=") && fast.Data.HasMore != (int64(page*size) < legacy.Data.Total) {
					t.Fatalf("look-ahead changed for %s", q)
				}
			}
		}
	}
	where, _ := buildUsageLogWhere(usageLogFilter{Group: "g1"}, usageLogWhereList)
	beforeSQL, beforeArg := where.sql(), append([]any{}, where.args...)
	for _, cursor := range []*usageLogCursor{nil, {CreatedAt: 1794500300, ID: 1202}} {
		query, args := usageLogsPageSQL(where, 2, 100, cursor, true)
		if !strings.HasPrefix(query, usageLogsSelect) {
			t.Fatal("page query lost its server time limit")
		}
		result, err := db.Query(query, args...)
		if err != nil {
			t.Fatal(err)
		}
		var matched int64
		items, err := scanUsageLogRowsAndTotal(result, &matched)
		result.Close()
		if err != nil || len(items) != 100 || matched != 101 {
			t.Fatalf("look-ahead must return 100 full rows and count only 101 IDs: %d %d %v", len(items), matched, err)
		}
	}
	if where.sql() != beforeSQL || !reflect.DeepEqual(where.args, beforeArg) {
		t.Fatal("pagination must not mutate full count/stat filters")
	}
}

func TestUsageLogsQueryErrorsKeepCancellationAndServerTimeoutDistinct(t *testing.T) {
	serverTimeout := &mysqlDriver.MySQLError{Number: 3024, Message: "maximum statement execution time exceeded"}
	for _, err := range []error{serverTimeout, fmt.Errorf("wrapped: %w", serverTimeout)} {
		if got := usageLogsQueryErr(context.Background(), err); got == nil || !strings.Contains(got.Error(), "查询超时") {
			t.Fatal("server-side timeout must be a clear retryable query failure")
		}
	}
	other := &mysqlDriver.MySQLError{Number: 1142, Message: "SELECT denied"}
	if !errors.Is(usageLogsQueryErr(context.Background(), other), other) {
		t.Fatal("authorization errors must not be disguised as timeouts")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(usageLogsQueryErr(ctx, serverTimeout), context.Canceled) {
		t.Fatal("client cancellation must win over a coincident server error")
	}
	// The driver's DSN readTimeout reports a bare invalid connection.
	readTimeout := &net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}
	for _, err := range []error{mysqlDriver.ErrInvalidConn, fmt.Errorf("x: %w", driver.ErrBadConn), readTimeout} {
		if got := usageLogsQueryErr(context.Background(), err); got == nil || !strings.Contains(got.Error(), "查询超时或从库连接中断") {
			t.Fatalf("driver timeout must read as a timeout, got %v", got)
		}
	}
	// The hint must stay below the production DSN readTimeout (20s).
	if !strings.Contains(usageLogsSelect, "MAX_EXECUTION_TIME(18000)") {
		t.Fatal("replica statement limit must stay below the 20s DSN readTimeout")
	}
}

func TestUsageLogsExactIDQueryCountsAndPagesStayEquivalent(t *testing.T) {
	db, counts := newCountedUsageLogsDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 1005; i++ {
		typ := 2
		if i%2 != 0 {
			typ = 5
		}
		_, err := tx.Exec("INSERT INTO logs (id,user_id,created_at,type,username,model_name,request_id,upstream_request_id,quota,other) VALUES (?,?,?,?,?,?,?,?,?,?)", i, 31, 1794500000+i/3, typ, "fixture-user", "model", "shared-id", "shared-upstream", i, `{"extra":"kept"}`)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	m := newChannelUpstreamTestMonitor(t)
	m.prodDB = db
	read := func(query string, wantQueries int64) usageLogsListResp {
		t.Helper()
		counts.reset()
		w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?"+query, nil)
		var r usageLogsListResp
		if json.Unmarshal(w.Body.Bytes(), &r) != nil || !r.Success {
			t.Fatalf("%s: %s", query, w.Body.String())
		}
		if got := counts.logs.Load(); got != wantQueries {
			t.Fatalf("%s: source queries %d, want %d", query, got, wantQueries)
		}
		return r
	}
	seen := map[int64]bool{}
	var cursor *usageLogCursor
	for page := 1; ; page++ {
		query := fmt.Sprintf("request_id=shared-id&p=%d&page_size=100", page)
		if cursor != nil {
			query += fmt.Sprintf("&before_created_at=%d&before_id=%d", cursor.CreatedAt, cursor.ID)
		}
		r := read(query, 1)
		if r.Data.Total != 1005 {
			t.Fatalf("cursor changed total: %d", r.Data.Total)
		}
		for _, it := range r.Data.Items {
			if seen[it.ID] || it.ID != int64(1005-len(seen)) || it.Other != `{"extra":"kept"}` {
				t.Fatalf("duplicate or changed record: %+v", it)
			}
			seen[it.ID] = true
		}
		cursor = r.Data.Next
		if cursor == nil {
			break
		}
		if page > 11 {
			t.Fatal("pagination did not finish")
		}
	}
	if len(seen) != 1005 {
		t.Fatalf("missing records: %d", len(seen))
	}
	for _, query := range []string{"upstream_request_id=shared-upstream", "request_id=shared-id&upstream_request_id=shared-upstream"} {
		r := read(query, 1)
		if r.Data.Total != 1005 || len(r.Data.Items) != 10 {
			t.Fatalf("ID semantics changed: %+v", r.Data)
		}
	}
	r := read("request_id=shared-id&type=5&username=fixture-user&p=4&page_size=100", 1)
	if r.Data.Total != 503 || len(r.Data.Items) != 100 || r.Data.Items[0].ID != 405 || r.Data.Items[99].ID != 207 {
		t.Fatalf("combined filters/jump changed: %+v", r.Data)
	}
	r = read("request_id=absent", 1)
	if r.Data.Total != 0 || r.Data.Items == nil || len(r.Data.Items) != 0 {
		t.Fatalf("empty first page: %+v", r.Data)
	}
	r = read("request_id=shared-id&p=999&page_size=100", 2)
	if r.Data.Total != 1005 || len(r.Data.Items) != 0 {
		t.Fatalf("empty later page lost total: %+v", r.Data)
	}
}

func TestUsageLogsUserAndMeta(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	m := newChannelUpstreamTestMonitor(t)
	m.prodDB = db
	w := usageLogsGET(t, m, m.serveUsageLogsUser, "/usage-logs/user/31", gin.Params{{Key: "id", Value: "31"}})
	body := w.Body.String()
	if !strings.Contains(body, `"username":"kjl_Dev"`) || !strings.Contains(body, `"request_count":42`) || strings.Contains(body, "secret") {
		t.Fatalf("user info must include dialog fields only: %s", body)
	}
	w = usageLogsGET(t, m, m.serveUsageLogsUser, "/usage-logs/user/99", gin.Params{{Key: "id", Value: "99"}})
	if !strings.Contains(w.Body.String(), "用户不存在") {
		t.Fatalf("missing user: %s", w.Body.String())
	}

	w = usageLogsGET(t, m, m.serveUsageLogsMeta, "/usage-logs/meta", nil)
	var meta struct {
		Success bool
		Data    usageLogsOptions
	}
	if json.Unmarshal(w.Body.Bytes(), &meta) != nil || !meta.Success || meta.Data.QuotaPerUnit != 500000 || meta.Data.QuotaDisplayType != "USD" || len(meta.Data.Groups) != 0 || meta.Data.HasEnabledSubscribe {
		t.Fatalf("defaults: %s", w.Body.String())
	}
	for _, s := range []string{
		`INSERT INTO options VALUES ('QuotaPerUnit','1000'), ('general_setting.quota_display_type','cny'), ('USDExchangeRate','7.1'), ('GroupRatio','{"default":1,"auto":1,"codex":0.7}')`,
		"INSERT INTO subscription_plans VALUES (1, 1)",
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	w = usageLogsGET(t, m, m.serveUsageLogsMeta, "/usage-logs/meta", nil)
	if json.Unmarshal(w.Body.Bytes(), &meta) != nil || meta.Data.QuotaPerUnit != 1000 || meta.Data.QuotaDisplayType != "CNY" || meta.Data.USDExchangeRate != 7.1 ||
		fmt.Sprint(meta.Data.Groups) != "[codex default]" || !meta.Data.HasEnabledSubscribe {
		t.Fatalf("options: %s", w.Body.String())
	}
}

func TestUsageLogsWithoutSourceReportsUnavailable(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	m.prodDB = nil
	w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list", nil)
	if !strings.Contains(w.Body.String(), "未连接 new-api 只读从库") {
		t.Fatalf("no source: %s", w.Body.String())
	}
}

func TestUsageLogsHistoricalOtherNullAndMalformedAdmin(t *testing.T) {
	for _, raw := range []string{
		`{"reject_reason":"blocked","admin_info":null,"quota":9007199254740993}`,
		`{"reject_reason":"legacy","admin_info":{"reject_reason":"new"}}`,
	} {
		got := projectUsageLogOtherForRoot(raw)
		var v map[string]json.RawMessage
		if err := json.Unmarshal([]byte(got), &v); err != nil || v["reject_reason"] != nil {
			t.Fatalf("projection failed: %s", got)
		}
		if strings.Contains(raw, "9007199254740993") && !strings.Contains(got, "9007199254740993") {
			t.Fatalf("large quota was rounded: %s", got)
		}
		if strings.Contains(raw, `"new"`) && !strings.Contains(got, `"new"`) {
			t.Fatalf("current admin reason overwritten: %s", got)
		}
	}
	for _, raw := range []string{`null`, `[]`, `invalid`, `{"reject_reason":"x","admin_info":"legacy"}`} {
		if got := projectUsageLogOtherForRoot(raw); got != raw {
			t.Fatalf("unsupported legacy content changed: %s", got)
		}
	}
}

func TestUsageLogsBoundsAndInvalidSettings(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	m.prodDB = newUsageLogsFakeProd(t)
	for _, query := range []string{fmt.Sprintf("p=%d&page_size=100", math.MaxInt), "p=-10&page_size=100"} {
		w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?"+query, nil)
		var r usageLogsListResp
		if json.Unmarshal(w.Body.Bytes(), &r) != nil || !r.Success || r.Data.Page != 1 {
			t.Fatalf("page overflow: %s", w.Body.String())
		}
	}
	if _, err := m.prodDB.Exec(`INSERT INTO options VALUES ('QuotaPerUnit','NaN'), ('USDExchangeRate','+Inf'), ('general_setting.custom_currency_exchange_rate','NaN')`); err != nil {
		t.Fatal(err)
	}
	w := usageLogsGET(t, m, m.serveUsageLogsMeta, "/usage-logs/meta", nil)
	var r struct {
		Success bool
		Data    usageLogsOptions
	}
	if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil || !r.Success || r.Data.QuotaPerUnit != 500000 || r.Data.USDExchangeRate != 7.3 {
		t.Fatalf("non-finite settings broke metadata: %s", w.Body.String())
	}
}

func TestUsageLogsGateCancellationIsIndependent(t *testing.T) {
	m := &Monitor{}
	var releases []func()
	for i := 0; i < usageLogsGateCapacity; i++ {
		release, err := m.acquireUsageLogsGate(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.acquireUsageLogsGate(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter: %v", err)
	}
	for _, release := range releases {
		release()
	}
	if len(m.usageLogsGate) != 0 {
		t.Fatal("query slots leaked")
	}
}

func TestUsageLogsBackgroundAdmissionLeavesListCapacityAndHonorsDeadline(t *testing.T) {
	m := &Monitor{prodDB: newUsageLogsFakeProd(t)}
	newContext := func(ctx context.Context) *gin.Context {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodGet, "/usage-logs/count", nil).WithContext(ctx)
		return c
	}
	started := time.Now()
	c := newContext(context.Background())
	_, release, ok := m.usageLogsSource(c, true)
	if !ok {
		t.Fatal("first background query rejected")
	}
	defer release()
	deadline, exists := c.Request.Context().Deadline()
	if !exists || deadline.Before(started.Add(usageLogsQueryTimeout)) || deadline.After(time.Now().Add(usageLogsQueryTimeout)) {
		t.Fatal("admission must be included in the request deadline")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	waiting := make(chan bool, 1)
	go func() {
		_, free, accepted := m.usageLogsSource(newContext(ctx), true)
		if accepted {
			free()
		}
		waiting <- accepted
	}()
	_, freeList, accepted := m.usageLogsSource(newContext(context.Background()), false)
	if !accepted {
		t.Fatal("background work consumed the list slot")
	}
	if len(m.usageLogsGate) != 2 || len(m.usageLogsBackgroundGate) != 1 {
		t.Fatal("source admission limits changed")
	}
	freeList()
	if <-waiting {
		t.Fatal("second background request must time out without executing")
	}
	if len(m.usageLogsGate) != 1 || len(m.usageLogsBackgroundGate) != 1 {
		t.Fatal("cancelled background waiter leaked query slots")
	}
}

func TestUsageLogsQueuedCancellationStormKeepsListCapacity(t *testing.T) {
	m := &Monitor{prodDB: newUsageLogsFakeProd(t)}
	makeContext := func(ctx context.Context) *gin.Context {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodGet, "/usage-logs/count", nil).WithContext(ctx)
		return c
	}
	_, release, ok := m.usageLogsSource(makeContext(context.Background()), true)
	if !ok {
		t.Fatal("cannot hold background slot")
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, finished := make(chan struct{}, 32), make(chan bool, 32)
	for i := 0; i < 32; i++ {
		go func() {
			started <- struct{}{}
			_, free, accepted := m.usageLogsSource(makeContext(ctx), true)
			if accepted {
				free()
			}
			finished <- accepted
		}()
	}
	for i := 0; i < 32; i++ {
		<-started
	}
	for i := 0; i < 64; i++ {
		_, free, accepted := m.usageLogsSource(makeContext(context.Background()), false)
		if !accepted {
			t.Fatal("queued statistics starved a foreground list")
		}
		if len(m.usageLogsGate) > 2 || len(m.usageLogsBackgroundGate) > 1 {
			t.Fatal("query concurrency exceeded the existing budget")
		}
		free()
	}
	cancel()
	for i := 0; i < 32; i++ {
		if <-finished {
			t.Fatal("cancelled background waiter executed")
		}
	}
	if len(m.usageLogsGate) != 1 || len(m.usageLogsBackgroundGate) != 1 {
		t.Fatal("cancel storm leaked slots")
	}
}

func TestUsageLogsCountCancellationReleasesAdmission(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	m := &Monitor{prodDB: db}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/usage-logs/count", nil).WithContext(ctx)
	started := time.Now()
	m.serveUsageLogsCount(c)
	if time.Since(started) > time.Second || !strings.Contains(w.Body.String(), "查询超时") || len(m.usageLogsGate) != 0 || len(m.usageLogsBackgroundGate) != 0 {
		t.Fatalf("cancelled pool waiter leaked capacity or failed to finish: %s", w.Body.String())
	}
}

func TestUsageLogsRoutesRequireMonitorAdministrator(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	m.prodDB = newUsageLogsFakeProd(t)
	r := gin.New()
	m.RegisterRoutes(r)
	for _, endpoint := range []string{"meta", "list", "count", "stat", "user/31"} {
		for _, role := range []int{0, 1, roleAdmin, roleRoot} {
			req := httptest.NewRequest(http.MethodGet, "/usage-logs/"+endpoint, nil)
			req.Header.Set("Accept", "application/json")
			if role > 0 {
				req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("fixture-admin", role, time.Now().Unix())})
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			want := http.StatusOK
			if role == 0 {
				want = http.StatusUnauthorized
			} else if role < roleAdmin {
				want = http.StatusForbidden
			}
			if w.Code != want {
				t.Fatalf("%s role %d: %d: %s", endpoint, role, w.Code, w.Body.String())
			}
			if role >= roleAdmin && !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
				t.Fatalf("sensitive response can be cached: %s", endpoint)
			}
		}
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/usage-logs/export", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("fixture-admin", roleRoot, time.Now().Unix())})
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("removed export is still available: %d", w.Code)
	}
}

// Opt-in real-browser test: actual handlers and embedded assets, synthetic
// databases only. Normal CI skips the browser, not the handler unit tests.
func TestUsageLogsBrowserAcceptance(t *testing.T) {
	browser, out := os.Getenv("MONITOR_USAGE_LOGS_BROWSER"), os.Getenv("MONITOR_USAGE_LOGS_REPORT")
	if browser == "" || out == "" {
		t.Skip("set MONITOR_USAGE_LOGS_BROWSER and MONITOR_USAGE_LOGS_REPORT for local browser acceptance")
	}
	gin.SetMode(gin.TestMode)
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.LocalAuthBypass = true
	m.prodDB = newUsageLogsFakeProd(t)
	now := time.Now().Unix()
	var rows []usageLogRow
	for i := 1; i <= 1105; i++ {
		rows = append(rows, usageLogRow{int64(i), 31, now - 600 - int64(i/3), 2, "fixture-user", "fixture-token", "fixture-model", 500000, 100, 20, 7, "codex", fmt.Sprintf("fixture-request-%d", i), `{"frt":500,"model_ratio":1,"completion_ratio":2,"group_ratio":0.7,"cache_tokens":30}`})
	}
	rows = append(rows,
		usageLogRow{2001, 31, now - 60, 5, "fixture-user", "fixture-token", "fixture-model", 0, 0, 0, 7, "codex", "fixture-error", `{"reject_reason":"synthetic rejection","admin_info":null}`},
		usageLogRow{2002, 31, now - 59, 6, "fixture-user", "fixture-token", "fixture-model", 1000, 0, 0, 7, "codex", "fixture-refund", `{"task_id":"synthetic-task","reason":"synthetic refund"}`},
		usageLogRow{2003, 31, now - 58, 3, "fixture-user", "", "", 0, 0, 0, 0, "", "fixture-audit", `{"op":{"action":"synthetic update","params":{"fields":["models"]}},"admin_info":{"admin_username":"fixture-admin"}}`},
		usageLogRow{2004, 31, now - 57, 2, "fixture-user", "fixture-token", "fixture-model", 500000, 100, 20, 7, "codex", "fixture-tier", `{"billing_source":"subscription","subscription_consumed":500000,"subscription_plan_title":"测试套餐","frt":600,"billing_mode":"tiered_expr","matched_tier":"base","expr_b64":"dGllcigiYmFzZSIsIHAgKiAyICsgYyAqIDgp"}`})
	insertUsageLogs(t, m.prodDB, rows...)
	if _, err := m.prodDB.Exec(`UPDATE users SET username='fixture-user', display_name='合成用户' WHERE id=31`); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec(`INSERT INTO options VALUES ('general_setting.quota_display_type','CNY'), ('USDExchangeRate','7'), ('GroupRatio','{"codex":0.7}')`); err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	m.RegisterRoutes(r)
	r.GET("/usage-logs-acceptance", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(`<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Monitor 使用日志 · 本机合成验收</title><link rel="stylesheet" href="/usage-logs.css"><style>:root{--bg:#151720;--surface:#1b1e2a;--border:#303549;--text:#e6e9f0;--text-muted:#94a3b8;--accent:#6366f1}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,sans-serif}main{padding:20px;max-width:1800px;margin:auto}h1{font-size:22px}p{color:var(--text-muted);font-size:13px}@media(max-width:640px){main{padding:10px}}</style><main><h1>使用日志</h1><p>本机合成数据验收 · 无线上连接</p><div class="usage-logs-page" id="tab-usage-logs"></div></main><script src="/usage-logs.js"></script><script src="/usage-logs-acceptance.js"></script></html>`))
	})
	r.GET("/usage-logs-acceptance.js", func(c *gin.Context) {
		c.Data(http.StatusOK, "application/javascript", []byte(`window.usageLogsActivate();`))
	})
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &httptest.Server{Listener: listener, Config: &http.Server{Handler: r}}
	srv.Start()
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "../dev/verify-usage-logs-browser.mjs", "--browser", browser, "--out", out, "--origin", srv.URL)
	if result, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("browser acceptance: %v\n%s", err, result)
	} else {
		t.Log(string(result))
	}
}

// A request cancelled mid-query must kill its own replica statement (closing
// the socket is not noticed by MySQL until the statement ends) and keep its
// admission slot until the kill was sent, so the gate really bounds the replica.
func TestUsageLogsCancelledRequestKillsReplicaThreadBeforeReleasingAdmission(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	insertUsageLogs(t, db, usageLogRow{1, 31, time.Now().Unix(), 2, "u", "t", "m", 1, 1, 1, 7, "g", "r1", ""})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &Monitor{prodDB: db}
	var killMu sync.Mutex
	var killed []int64
	var slotsHeldAtKill, bgHeldAtKill int
	m.usageLogsThreadIDFn = func(context.Context, *sql.Conn) int64 {
		cancel() // the admin cancels while the statement would be running
		return 4242
	}
	m.usageLogsKillFn = func(killCtx context.Context, id int64) error {
		if killCtx.Err() != nil {
			t.Error("kill must not reuse the cancelled request context")
		}
		killMu.Lock()
		defer killMu.Unlock()
		killed = append(killed, id)
		slotsHeldAtKill, bgHeldAtKill = len(m.usageLogsGate), len(m.usageLogsBackgroundGate)
		if inUse := db.Stats().InUse; inUse != 0 {
			t.Errorf("the aborted connection must be discarded before the kill needs a pool slot: in use=%d", inUse)
		}
		return nil
	}
	idleBefore := db.Stats().Idle
	if idleBefore == 0 {
		t.Fatal("fixture should leave an idle pooled connection")
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/usage-logs/stat", nil).WithContext(ctx)
	m.serveUsageLogsStat(c)
	waitUsageLogsSlotsFree(t, m)
	killMu.Lock()
	defer killMu.Unlock()
	if len(killed) != 1 || killed[0] != 4242 {
		t.Fatalf("cancelled request must kill exactly its own thread once: %v", killed)
	}
	if slotsHeldAtKill != 1 || bgHeldAtKill != 1 {
		t.Fatalf("admission must be held until the kill is sent: gate=%d background=%d", slotsHeldAtKill, bgHeldAtKill)
	}
	if len(m.usageLogsGate) != 0 || len(m.usageLogsBackgroundGate) != 0 {
		t.Fatal("admission leaked after kill")
	}
	// gin only buffers a body-less status; read it from the writer.
	if got := c.Writer.Status(); got != statusClientClosedRequest {
		t.Fatalf("cancelled request status = %d body=%s", got, w.Body.String())
	}
	// The killed connection is discarded, not handed back to other features.
	if after := db.Stats().Idle; after >= idleBefore {
		t.Fatalf("killed connection returned to the pool: idle before=%d after=%d", idleBefore, after)
	}
}

func TestUsageLogsCompletedRequestDoesNotKillAndKillFailureStillReleases(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	insertUsageLogs(t, db, usageLogRow{1, 31, time.Now().Unix(), 2, "u", "t", "m", 1, 1, 1, 7, "g", "r1", ""})
	m := &Monitor{prodDB: db}
	var killCount atomic.Int64
	m.usageLogsThreadIDFn = func(context.Context, *sql.Conn) int64 { return 77 }
	m.usageLogsKillFn = func(context.Context, int64) error { killCount.Add(1); return errors.New("kill refused") }
	if w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?defer_total=1", nil); !strings.Contains(w.Body.String(), `"success":true`) {
		t.Fatalf("list: %s", w.Body.String())
	}
	if kills := killCount.Load(); kills != 0 {
		t.Fatalf("a completed request must not kill its thread: %d", kills)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.usageLogsThreadIDFn = func(context.Context, *sql.Conn) int64 { cancel(); return 78 }
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/usage-logs/count", nil).WithContext(ctx)
	m.serveUsageLogsCount(c)
	waitUsageLogsSlotsFree(t, m)
	if kills := killCount.Load(); kills != 1 || len(m.usageLogsGate) != 0 || len(m.usageLogsBackgroundGate) != 0 {
		t.Fatalf("a failed kill must still release admission: kills=%d gate=%d bg=%d", kills, len(m.usageLogsGate), len(m.usageLogsBackgroundGate))
	}
	// Non-MySQL sources have no thread id: nothing is killed.
	m.usageLogsThreadIDFn = nil
	if id := m.usageLogsThreadID(context.Background(), nil); id != 0 {
		t.Fatalf("non-MySQL source thread id = %d", id)
	}
}

func TestUsageLogsStatSkipQuotaKeepsRates(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	now := time.Now().Unix()
	insertUsageLogs(t, db,
		usageLogRow{1, 31, now - 30, 2, "u", "a", "m", 100, 10, 5, 7, "g", "r1", ""},
		usageLogRow{2, 31, now - 3600, 2, "u", "a", "m", 200, 1, 1, 7, "g", "r2", ""},
	)
	m := &Monitor{prodDB: db}
	type statResp struct {
		Success bool
		Data    struct {
			Quota        *int64 `json:"quota"`
			QuotaSkipped bool   `json:"quota_skipped"`
			RPM, TPM     int64
		}
	}
	get := func(q string) statResp {
		var r statResp
		w := usageLogsGET(t, m, m.serveUsageLogsStat, "/usage-logs/stat?start_timestamp="+strconv.FormatInt(now-7200, 10)+q, nil)
		if json.Unmarshal(w.Body.Bytes(), &r) != nil || !r.Success {
			t.Fatalf("stat%s: %s", q, w.Body.String())
		}
		return r
	}
	full, skipped := get(""), get("&skip_quota=1")
	if full.Data.Quota == nil || *full.Data.Quota != 300 || full.Data.QuotaSkipped {
		t.Fatalf("full stat: %+v", full.Data)
	}
	if skipped.Data.Quota != nil || !skipped.Data.QuotaSkipped {
		t.Fatalf("skip_quota must not report an amount: %+v", skipped.Data)
	}
	// The cheap rates must not queue behind heavy background work.
	m.initUsageLogsGates()
	m.usageLogsBackgroundGate <- struct{}{}
	busy := get("&skip_quota=1")
	<-m.usageLogsBackgroundGate
	if busy.Data.RPM != 1 {
		t.Fatalf("skip_quota stat waited for the background slot: %+v", busy.Data)
	}
	if skipped.Data.RPM != full.Data.RPM || skipped.Data.TPM != full.Data.TPM || skipped.Data.RPM != 1 || skipped.Data.TPM != 15 {
		t.Fatalf("rates must not change with skip_quota: full=%+v skipped=%+v", full.Data, skipped.Data)
	}
}

// KILL QUERY must not queue behind other features in the small shared source
// pool: it uses its own one-connection pool built from the same DSN.
func TestUsageLogsKillUsesDedicatedPool(t *testing.T) {
	shared := newUsageLogsFakeProd(t)
	m := &Monitor{prodDB: shared}
	if m.usageLogsKillPool() != shared {
		t.Fatal("without a DSN the kill falls back to the shared pool")
	}
	opened := 0
	dedicated := newUsageLogsFakeProd(t)
	m2 := &Monitor{prodDB: shared}
	m2.cfg.ProdDSN = "user:pw@tcp(replica:3306)/nexusapi"
	m2.cfg.sourceOpen = func(string) (*sql.DB, error) { opened++; return dedicated, nil }
	first, second := m2.usageLogsKillPool(), m2.usageLogsKillPool()
	if first != dedicated || second != dedicated || opened != 1 {
		t.Fatalf("kill pool must be dedicated and created once: opened=%d", opened)
	}
	if got := dedicated.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("kill pool max open = %d", got)
	}
}

// The kill runs after the response; slots are freed once it was sent.
func waitUsageLogsSlotsFree(t *testing.T, m *Monitor) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(m.usageLogsGate) != 0 || len(m.usageLogsBackgroundGate) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("admission slots were never released after the kill")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestUsageLogsResponseDoesNotWaitForSlowKill(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	m := &Monitor{prodDB: db}
	ctx, cancel := context.WithCancel(context.Background())
	m.usageLogsThreadIDFn = func(context.Context, *sql.Conn) int64 { cancel(); return 9 }
	unblock := make(chan struct{})
	var held atomic.Int64
	m.usageLogsKillFn = func(context.Context, int64) error {
		held.Store(int64(len(m.usageLogsGate) + len(m.usageLogsBackgroundGate)))
		<-unblock
		return nil
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/usage-logs/count", nil).WithContext(ctx)
	returned := make(chan struct{})
	go func() { m.serveUsageLogsCount(c); close(returned) }()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("the response waited for the kill")
	}
	deadline := time.Now().Add(2 * time.Second)
	for held.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if held.Load() != 2 || len(m.usageLogsGate) != 1 || len(m.usageLogsBackgroundGate) != 1 {
		t.Fatalf("slots must stay held while the kill is pending: during=%d gate=%d bg=%d", held.Load(), len(m.usageLogsGate), len(m.usageLogsBackgroundGate))
	}
	close(unblock)
	waitUsageLogsSlotsFree(t, m)
}
