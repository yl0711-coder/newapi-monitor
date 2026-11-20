package usagelogs

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
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
		"CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)",
		"INSERT INTO channels VALUES (7, 'gateai-codex'), (9, 'jikesoft')",
		"INSERT INTO users VALUES (31, 'kjl_Dev')",
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

func addUsageLogsUser(t *testing.T, db *sql.DB, id int64, username string) {
	t.Helper()
	if _, err := db.Exec("INSERT INTO users (id, username) VALUES (?, ?)", id, username); err != nil {
		t.Fatal(err)
	}
}

func acquireForTest(ctx context.Context, s *Service, background bool) (func(), bool) {
	_, _, finish, err := s.acquire(ctx, background)
	return finish, err == nil
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

func TestUsageLogsGateCancellationIsIndependent(t *testing.T) {
	s := New(nil, "", nil)
	var releases []func()
	for i := 0; i < usageLogsGateCapacity; i++ {
		release, err := s.acquireUsageLogsGate(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.acquireUsageLogsGate(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter: %v", err)
	}
	for _, release := range releases {
		release()
	}
	if len(s.gate) != 0 {
		t.Fatal("query slots leaked")
	}
}

func TestUsageLogsBackgroundAdmissionLeavesListCapacityAndHonorsDeadline(t *testing.T) {
	s := New(newUsageLogsFakeProd(t), "", nil)
	started := time.Now()
	_, queryContext, release, acquireErr := s.acquire(context.Background(), true)
	if acquireErr != nil {
		t.Fatal("first background query rejected")
	}
	defer release()
	deadline, exists := queryContext.Deadline()
	if !exists || deadline.Before(started.Add(usageLogsQueryTimeout)) || deadline.After(time.Now().Add(usageLogsQueryTimeout)) {
		t.Fatal("admission must be included in the request deadline")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	waiting := make(chan bool, 1)
	go func() {
		free, accepted := acquireForTest(ctx, s, true)
		if accepted {
			free()
		}
		waiting <- accepted
	}()
	freeList, accepted := acquireForTest(context.Background(), s, false)
	if !accepted {
		t.Fatal("background work consumed the list slot")
	}
	if len(s.gate) != 2 || len(s.backgroundGate) != 1 {
		t.Fatal("source admission limits changed")
	}
	freeList()
	if <-waiting {
		t.Fatal("second background request must time out without executing")
	}
	if len(s.gate) != 1 || len(s.backgroundGate) != 1 {
		t.Fatal("cancelled background waiter leaked query slots")
	}
}

func TestUsageLogsQueuedCancellationStormKeepsListCapacity(t *testing.T) {
	s := New(newUsageLogsFakeProd(t), "", nil)
	release, ok := acquireForTest(context.Background(), s, true)
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
			free, accepted := acquireForTest(ctx, s, true)
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
		free, accepted := acquireForTest(context.Background(), s, false)
		if !accepted {
			t.Fatal("queued statistics starved a foreground list")
		}
		if len(s.gate) > 2 || len(s.backgroundGate) > 1 {
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
	if len(s.gate) != 1 || len(s.backgroundGate) != 1 {
		t.Fatal("cancel storm leaked slots")
	}
}

func TestUsageLogsCancelledRequestKillsReplicaThreadBeforeReleasingAdmission(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	insertUsageLogs(t, db, usageLogRow{1, 31, time.Now().Unix(), 2, "u", "t", "s", 1, 1, 1, 7, "g", "r1", ""})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := New(db, "", nil)
	var killMu sync.Mutex
	var killed []int64
	var slotsHeldAtKill, bgHeldAtKill int
	s.threadIDFn = func(context.Context, *sql.Conn) int64 {
		cancel() // the admin cancels while the statement would be running
		return 4242
	}
	s.killFn = func(killCtx context.Context, id int64) error {
		if killCtx.Err() != nil {
			t.Error("kill must not reuse the cancelled request context")
		}
		killMu.Lock()
		defer killMu.Unlock()
		killed = append(killed, id)
		slotsHeldAtKill, bgHeldAtKill = len(s.gate), len(s.backgroundGate)
		if inUse := db.Stats().InUse; inUse != 0 {
			t.Errorf("the aborted connection must be discarded before the kill needs a pool slot: in use=%d", inUse)
		}
		return nil
	}
	idleBefore := db.Stats().Idle
	if idleBefore == 0 {
		t.Fatal("fixture should leave an idle pooled connection")
	}
	req := s.Request(ctx)
	_, requestErr := req.Stat(Filter{}, false)
	req.Finish()
	waitUsageLogsSlotsFree(t, s)
	killMu.Lock()
	defer killMu.Unlock()
	if len(killed) != 1 || killed[0] != 4242 {
		t.Fatalf("cancelled request must kill exactly its own thread once: %v", killed)
	}
	if slotsHeldAtKill != 1 || bgHeldAtKill != 1 {
		t.Fatalf("admission must be held until the kill is sent: gate=%d background=%d", slotsHeldAtKill, bgHeldAtKill)
	}
	if len(s.gate) != 0 || len(s.backgroundGate) != 0 {
		t.Fatal("admission leaked after kill")
	}
	if !errors.Is(requestErr, context.Canceled) {
		t.Fatalf("cancelled query returned %v", requestErr)
	}
	// The killed connection is discarded, not handed back to other features.
	if after := db.Stats().Idle; after >= idleBefore {
		t.Fatalf("killed connection returned to the pool: idle before=%d after=%d", idleBefore, after)
	}
}

func TestUsageLogsCompletedRequestDoesNotKillAndKillFailureStillReleases(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	insertUsageLogs(t, db, usageLogRow{1, 31, time.Now().Unix(), 2, "u", "t", "s", 1, 1, 1, 7, "g", "r1", ""})
	s := New(db, "", nil)
	var killCount atomic.Int64
	s.threadIDFn = func(context.Context, *sql.Conn) int64 { return 77 }
	s.killFn = func(context.Context, int64) error { killCount.Add(1); return errors.New("kill refused") }
	req := s.Request(context.Background())
	_, listErr := req.List(ListInput{DeferTotal: true})
	req.Finish()
	if listErr != nil {
		t.Fatal(listErr)
	}
	if kills := killCount.Load(); kills != 0 {
		t.Fatalf("a completed request must not kill its thread: %d", kills)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.threadIDFn = func(context.Context, *sql.Conn) int64 { cancel(); return 78 }
	req = s.Request(ctx)
	_, _ = req.Count(Filter{})
	req.Finish()
	waitUsageLogsSlotsFree(t, s)
	if kills := killCount.Load(); kills != 1 || len(s.gate) != 0 || len(s.backgroundGate) != 0 {
		t.Fatalf("a failed kill must still release admission: kills=%d gate=%d bg=%d", kills, len(s.gate), len(s.backgroundGate))
	}
	// Non-MySQL sources have no thread id: nothing is killed.
	s.threadIDFn = nil
	if id := s.threadID(context.Background(), nil); id != 0 {
		t.Fatalf("non-MySQL source thread id = %d", id)
	}
}

func TestUsageLogsKillUsesDedicatedPool(t *testing.T) {
	shared := newUsageLogsFakeProd(t)
	s := New(shared, "", nil)
	if s.killPool() != shared {
		t.Fatal("without a DSN the kill falls back to the shared pool")
	}
	opened := 0
	dedicated := newUsageLogsFakeProd(t)
	s2 := New(shared, "fixture@tcp(replica:3306)/nexusapi", func(string) (*sql.DB, error) {
		opened++
		return dedicated, nil
	})
	first, second := s2.killPool(), s2.killPool()
	if first != dedicated || second != dedicated || opened != 1 {
		t.Fatalf("kill pool must be dedicated and created once: opened=%d", opened)
	}
	if got := dedicated.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("kill pool max open = %d", got)
	}
}

func waitUsageLogsSlotsFree(t *testing.T, s *Service) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(s.gate) != 0 || len(s.backgroundGate) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("admission slots were never released after the kill")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestUsageLogsResponseDoesNotWaitForSlowKill(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	s := New(db, "", nil)
	ctx, cancel := context.WithCancel(context.Background())
	s.threadIDFn = func(context.Context, *sql.Conn) int64 { cancel(); return 9 }
	unblock := make(chan struct{})
	var held atomic.Int64
	s.killFn = func(context.Context, int64) error {
		held.Store(int64(len(s.gate) + len(s.backgroundGate)))
		<-unblock
		return nil
	}
	returned := make(chan struct{})
	go func() {
		req := s.Request(ctx)
		_, _ = req.Count(Filter{})
		req.Finish()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("the response waited for the kill")
	}
	deadline := time.Now().Add(2 * time.Second)
	for held.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if held.Load() != 2 || len(s.gate) != 1 || len(s.backgroundGate) != 1 {
		t.Fatalf("slots must stay held while the kill is pending: during=%d gate=%d bg=%d", held.Load(), len(s.gate), len(s.backgroundGate))
	}
	close(unblock)
	waitUsageLogsSlotsFree(t, s)
}

func TestUsageLogsUsernameUserIDRequiresProvenEquivalence(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name     string
		users    map[int64]string
		logs     []usageLogRow
		username string
		want     int64
	}{
		{name: "stable user", users: map[int64]string{31: "kjl_Dev"},
			logs:     []usageLogRow{{id: 1, userID: 31, typ: 2, username: "kjl_Dev"}, {id: 5, userID: 31, typ: 2, username: "kjl_Dev"}},
			username: "kjl_Dev", want: 31},
		{name: "wildcard keeps username", users: map[int64]string{31: "kjl_Dev"},
			logs:     []usageLogRow{{id: 1, userID: 31, typ: 2, username: "kjl_Dev"}},
			username: "kjl%", want: 0},
		{name: "unknown username", users: map[int64]string{31: "kjl_Dev"},
			logs:     []usageLogRow{{id: 1, userID: 31, typ: 2, username: "kjl_Dev"}},
			username: "nobody", want: 0},
		{name: "renamed user: earliest row has the old name", users: map[int64]string{40: "newname"},
			logs:     []usageLogRow{{id: 1, userID: 40, typ: 2, username: "oldname"}, {id: 2, userID: 40, typ: 2, username: "newname"}},
			username: "newname", want: 0},
		{name: "name used earlier by another user", users: map[int64]string{41: "reused", 42: "moved"},
			logs:     []usageLogRow{{id: 1, userID: 42, typ: 2, username: "reused"}, {id: 2, userID: 41, typ: 2, username: "reused"}},
			username: "reused", want: 0},
		{name: "no rows at all", users: map[int64]string{43: "quiet"},
			logs:     []usageLogRow{{id: 1, userID: 31, typ: 2, username: "kjl_Dev"}},
			username: "quiet", want: 43},
		{name: "no own rows but another user's rows carry the name", users: map[int64]string{44: "taken"},
			logs:     []usageLogRow{{id: 1, userID: 45, typ: 2, username: "taken"}},
			username: "taken", want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := newUsageLogsFakeProd(t)
			if _, err := db.Exec("DELETE FROM users"); err != nil {
				t.Fatal(err)
			}
			for id, name := range tc.users {
				addUsageLogsUser(t, db, id, name)
			}
			insertUsageLogs(t, db, tc.logs...)
			got, err := usageLogsUsernameUserID(ctx, db, tc.username)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("user_id = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestUsageLogsWantsIDSpanOnlyForUnindexedColumnFilters(t *testing.T) {
	base := Filter{Start: 100, End: 200}
	for name, tc := range map[string]struct {
		f    Filter
		want bool
	}{
		"channel":           {Filter{Start: 100, End: 200, Channel: 7}, true},
		"model":             {Filter{Start: 100, End: 200, ModelName: "gpt-6"}, true},
		"group":             {Filter{Start: 100, End: 200, Group: "codex"}, true},
		"time only":         {base, false},
		"user id":           {Filter{Start: 100, End: 200, Channel: 7, userID: 3}, false},
		"username":          {Filter{Start: 100, End: 200, Channel: 7, Username: "u"}, false},
		"token":             {Filter{Start: 100, End: 200, Channel: 7, TokenName: "t"}, false},
		"request id":        {Filter{Start: 100, End: 200, Channel: 7, RequestID: "r"}, false},
		"upstream request":  {Filter{Start: 100, End: 200, Channel: 7, UpstreamRequestID: "r"}, false},
		"open-ended window": {Filter{Start: 100, Channel: 7}, false},
		"inverted window":   {Filter{Start: 300, End: 200, Channel: 7}, false},
		"no window at all":  {Filter{Channel: 7}, false},
	} {
		if got := usageLogsWantsIDSpan(tc.f); got != tc.want {
			t.Errorf("%s: got %v want %v", name, got, tc.want)
		}
	}
}

func TestUsageLogsIDSpanCountsAndSumsMatchOriginalPredicates(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	rows := []usageLogRow{
		{id: 1, createdAt: 90, typ: 2, channel: 7, quota: 1},
		{id: 2, createdAt: 101, typ: 2, channel: 7, quota: 10},
		{id: 3, createdAt: 99, typ: 2, channel: 7, quota: 100}, // inside the span, outside the window
		{id: 4, createdAt: 150, typ: 5, channel: 7, quota: 0},
		{id: 5, createdAt: 150, typ: 2, channel: 9, quota: 1000},
		{id: 6, createdAt: 210, typ: 2, channel: 7, quota: 10000}, // inside the span, outside the window
		{id: 7, createdAt: 199, typ: 2, channel: 7, quota: 100000},
		{id: 8, createdAt: 200, typ: 2, channel: 7, quota: 1000000},
		{id: 9, createdAt: 201, typ: 2, channel: 7, quota: 7},
	}
	for i := range rows {
		rows[i].username, rows[i].model, rows[i].group = "u", "m", "g"
	}
	insertUsageLogs(t, db, rows...)
	ctx := context.Background()
	for _, f := range []Filter{
		{Start: 100, End: 200, Channel: 7},
		{Start: 100, End: 200, Channel: 9},
		{Start: 100, End: 200, Channel: 7, Type: 5},
		{Start: 100, End: 200, ModelName: "m"},
		{Start: 100, End: 200, Group: "g"},
	} {
		for _, scope := range []usageLogWhereScope{usageLogWhereList, usageLogWhereStat} {
			where, err := buildUsageLogWhere(f, scope)
			if err != nil {
				t.Fatal(err)
			}
			narrowed, source, empty, ok := usageLogsIDSpan(ctx, db, true, f, where)
			if !ok || empty || source != "logs FORCE INDEX (PRIMARY)" {
				t.Fatalf("%+v: ok=%v empty=%v source=%q", f, ok, empty, source)
			}
			for _, agg := range []string{"COUNT(*)", "COALESCE(SUM(quota),0)"} {
				var want, got int64
				if err := db.QueryRow("SELECT "+agg+" FROM logs"+where.sql(), where.args...).Scan(&want); err != nil {
					t.Fatal(err)
				}
				// SQLite has no FORCE INDEX; the narrowed predicates are what is checked.
				if err := db.QueryRow("SELECT "+agg+" FROM logs"+narrowed.sql(), narrowed.args...).Scan(&got); err != nil {
					t.Fatal(err)
				}
				if got != want {
					t.Fatalf("%+v scope=%d %s: span=%d original=%d", f, scope, agg, got, want)
				}
			}
		}
	}
	// A window without rows is answered without the main scan.
	f := Filter{Start: 500, End: 600, Channel: 7}
	where, _ := buildUsageLogWhere(f, usageLogWhereList)
	if _, _, empty, ok := usageLogsIDSpan(ctx, db, true, f, where); !ok || !empty {
		t.Fatalf("empty window: ok=%v empty=%v", ok, empty)
	}
	// Not MySQL (or not eligible): the original plan is kept untouched.
	if got, source, _, ok := usageLogsIDSpan(ctx, db, false, Filter{Start: 100, End: 200, Channel: 7}, where); ok || source != "logs" || got != where {
		t.Fatalf("sqlite must keep the original plan: ok=%v source=%q", ok, source)
	}
}

func TestUsageLogsDeferredLookaheadAndFilterIsolation(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	if _, err := db.Exec("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<1307) " +
		"INSERT INTO logs(id,created_at,`group`,content) SELECT i,1794500000+i/4,'g1','fixture' FROM n"); err != nil {
		t.Fatal(err)
	}

	where, _ := buildUsageLogWhere(Filter{Group: "g1"}, usageLogWhereList)
	beforeSQL, beforeArg := where.sql(), append([]any{}, where.args...)
	for _, cursor := range []*Cursor{nil, {CreatedAt: 1794500300, ID: 1202}} {
		query, args := usageLogsPageSQL(where, "logs", 2, 100, cursor, true)
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
