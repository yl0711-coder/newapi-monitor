package monitor

// usage_test.go:「用户用量」单元/集成测试。
// 名单 CRUD 走真 sqlite 本地库;聚合链路用 sqlite 假生产库端到端验证(建 logs/users 表塞已知行),
// 仅日桶表达式按方言覆盖(MySQL DIV → sqlite 整除 /),SQL 其余部分两边通用。

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	glebarezsqlite "github.com/glebarez/go-sqlite" // 注册 database/sql 驱动 "sqlite"(纯 Go,免 cgo)
	"gorm.io/gorm"
)

const usageDayExprSQLite = "(created_at + 28800) / 86400" // sqlite 整型相除即整除

// usageCountingDriver 只用于测试查询往返数：透传真实 SQLite 驱动，
// 不伪造 SQL 结果，仅统计 users/tokens 表的 SELECT。
type usageQueryCounts struct {
	users  atomic.Int64
	tokens atomic.Int64
}

func (c *usageQueryCounts) reset() {
	c.users.Store(0)
	c.tokens.Store(0)
}

type usageCountingDriver struct {
	inner  driver.Driver
	counts *usageQueryCounts
}

func (d *usageCountingDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &usageCountingConn{Conn: conn, counts: d.counts}, nil
}

type usageCountingConn struct {
	driver.Conn
	counts *usageQueryCounts
}

func (c *usageCountingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	lower := strings.ToLower(query)
	if strings.Contains(lower, " from users") {
		c.counts.users.Add(1)
	}
	if strings.Contains(lower, " from tokens") {
		c.counts.tokens.Add(1)
	}
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return queryer.QueryContext(ctx, query, args)
}

var usageCountingDriverID atomic.Int64

func TestParseUsageRange(t *testing.T) {
	// 固定“现在”:2026-07-07 15:00 CST
	now := time.Date(2026, 7, 7, 15, 0, 0, 0, usageCST)

	// 默认:近 7 天(含今天)→ [7-01 00:00, 7-08 00:00)
	from, to, err := parseUsageRange("", "", now)
	if err != nil {
		t.Fatalf("默认范围: %v", err)
	}
	wantFrom := time.Date(2026, 7, 1, 0, 0, 0, 0, usageCST).Unix()
	wantTo := time.Date(2026, 7, 8, 0, 0, 0, 0, usageCST).Unix()
	if from != wantFrom || to != wantTo {
		t.Fatalf("默认范围 = [%d,%d), want [%d,%d)", from, to, wantFrom, wantTo)
	}

	// 显式区间含端点;from>to 自动交换
	f2, t2, err := parseUsageRange("2026-07-05", "2026-07-03", now)
	if err != nil {
		t.Fatalf("交换区间: %v", err)
	}
	if f2 != time.Date(2026, 7, 3, 0, 0, 0, 0, usageCST).Unix() || t2 != time.Date(2026, 7, 6, 0, 0, 0, 0, usageCST).Unix() {
		t.Fatalf("交换后 = [%d,%d) 不符", f2, t2)
	}

	// 超上限拒绝
	if _, _, err := parseUsageRange("2025-01-01", "2026-07-07", now); err == nil {
		t.Fatal("超长范围应报错")
	}
	// 坏格式拒绝
	if _, _, err := parseUsageRange("07/01", "", now); err == nil {
		t.Fatal("坏日期格式应报错")
	}
}

func TestUsageGateCanceledBeforeAcquireDoesNotTakeFreeSlot(t *testing.T) {
	m := &Monitor{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// 旧实现中空闲槽位与 Done 同时就绪，select 约一半概率误放行；重复验证避免回归。
	for i := 0; i < 1000; i++ {
		err := m.acquireUsageGate(ctx)
		if err == nil {
			m.releaseUsageGate()
			t.Fatalf("第 %d 次：已取消请求不应取得空闲槽位", i+1)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("第 %d 次：取消错误=%v", i+1, err)
		}
	}

	// 取消请求不能污染闸门；随后的正常请求仍须立即成功。
	if err := m.acquireUsageGate(context.Background()); err != nil {
		t.Fatalf("取消请求后正常请求应能取得槽位: %v", err)
	}
	m.releaseUsageGate()
}

func TestUsageGateCanceledWaiterDoesNotReleaseOwner(t *testing.T) {
	m := &Monitor{}
	if err := m.acquireUsageGate(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- m.acquireUsageGate(ctx) }()
	cancel()
	if err := <-waiterDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("已取消的排队查询应立即退出，got %v", err)
	}

	// 已取消等待者没有取得槽位，因此绝不能释放当前 owner 的槽位。
	normalDone := make(chan error, 1)
	go func() { normalDone <- m.acquireUsageGate(context.Background()) }()
	select {
	case err := <-normalDone:
		t.Fatalf("owner 释放前正常等待者不应取得槽位: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	m.releaseUsageGate()
	if err := <-normalDone; err != nil {
		t.Fatalf("owner 释放后正常等待者应取得槽位: %v", err)
	}
	m.releaseUsageGate()
}

func TestUsageGateDeadlineWaiterDoesNotReleaseOwner(t *testing.T) {
	m := &Monitor{}
	if err := m.acquireUsageGate(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := m.acquireUsageGate(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("等待超时应返回 DeadlineExceeded，got %v", err)
	}

	// 超时等待者同样不能释放 owner 的槽位。
	normalDone := make(chan error, 1)
	go func() { normalDone <- m.acquireUsageGate(context.Background()) }()
	select {
	case err := <-normalDone:
		t.Fatalf("owner 释放前正常等待者不应取得槽位: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	m.releaseUsageGate()
	if err := <-normalDone; err != nil {
		t.Fatalf("owner 释放后正常等待者应取得槽位: %v", err)
	}
	m.releaseUsageGate()
}

func TestUsageGateDoesNotCancelNormalWaiter(t *testing.T) {
	m := &Monitor{}
	if err := m.acquireUsageGate(context.Background()); err != nil {
		t.Fatal(err)
	}
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- m.acquireUsageGate(context.Background()) }()
	select {
	case err := <-waiterDone:
		t.Fatalf("正常等待者不应被提前放行或误取消: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	m.releaseUsageGate()
	if err := <-waiterDone; err != nil {
		t.Fatalf("正常等待者在槽位释放后应成功: %v", err)
	}
	m.releaseUsageGate()
}

func TestUsageGateSerializesNormalRequests(t *testing.T) {
	m := &Monitor{}
	const workers = 24
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	var active atomic.Int32
	var maxActive atomic.Int32

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := m.acquireUsageGate(context.Background()); err != nil {
				errs <- err
				return
			}
			n := active.Add(1)
			for old := maxActive.Load(); n > old && !maxActive.CompareAndSwap(old, n); old = maxActive.Load() {
			}
			time.Sleep(time.Millisecond)
			active.Add(-1)
			m.releaseUsageGate()
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("正常并发请求不应失败: %v", err)
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("同一时刻只应放行 1 个请求，实际最大=%d", got)
	}
}

func TestUsageGateCancelReleaseRaceNeverLeaksOrDoubleReleases(t *testing.T) {
	// 模拟 owner 释放与等待者取消同时发生。等待者可以在取消前合法取得槽位，
	// 也可以返回 Canceled；无论哪种时序，都不能泄漏槽位或释放两次。
	for i := 0; i < 1000; i++ {
		m := &Monitor{}
		if err := m.acquireUsageGate(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		waiterDone := make(chan error, 1)
		go func() { waiterDone <- m.acquireUsageGate(ctx) }()

		start := make(chan struct{})
		ownerReleased := make(chan struct{})
		go func() {
			<-start
			m.releaseUsageGate()
			close(ownerReleased)
		}()
		go func() {
			<-start
			cancel()
		}()
		close(start)

		err := <-waiterDone
		<-ownerReleased
		if err == nil {
			// 等待者在 cancel 生效前取得槽位是合法结果，由它归还自己的槽位。
			m.releaseUsageGate()
		} else if !errors.Is(err, context.Canceled) {
			t.Fatalf("第 %d 次：竞争结果错误=%v", i+1, err)
		}

		// 每轮都验证闸门最终处于可用且空闲状态；泄漏会在这里超时。
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		if err := m.acquireUsageGate(probeCtx); err != nil {
			probeCancel()
			t.Fatalf("第 %d 次：取消/释放竞争后槽位不可用: %v", i+1, err)
		}
		probeCancel()
		m.releaseUsageGate()
	}
}

func TestCanceledUsageRequestClassification(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name       string
		requestCtx context.Context
		err        error
		wantAbort  bool
	}{
		{name: "wrapped query cancellation", requestCtx: context.Background(), err: fmt.Errorf("wrapped: %w", context.Canceled), wantAbort: true},
		{name: "request canceled with driver error", requestCtx: canceledContext(), err: errors.New("driver: bad connection"), wantAbort: true},
		{name: "query deadline is a real timeout", requestCtx: context.Background(), err: context.DeadlineExceeded, wantAbort: false},
		{name: "request deadline is a real timeout", requestCtx: expiredContext(), err: context.DeadlineExceeded, wantAbort: false},
		{name: "ordinary database error", requestCtx: context.Background(), err: errors.New("database unavailable"), wantAbort: false},
		{name: "nil error", requestCtx: context.Background(), err: nil, wantAbort: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("GET", "/usage/matrix", nil).WithContext(tt.requestCtx)
			got := abortCanceledUsageRequest(c, tt.err)
			if got != tt.wantAbort {
				t.Fatalf("abort=%v want=%v err=%v requestErr=%v", got, tt.wantAbort, tt.err, tt.requestCtx.Err())
			}
			if tt.wantAbort {
				if w.Code != statusClientClosedRequest {
					t.Fatalf("取消状态=%d want=%d", w.Code, statusClientClosedRequest)
				}
			} else if c.Writer.Written() {
				t.Fatalf("非取消错误不应被 helper 提前写响应，status=%d", w.Code)
			}
		})
	}
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func expiredContext() context.Context {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	return ctx
}

func TestParseUsageRangeRejectsPreciseTime(t *testing.T) {
	now := time.Date(2026, 7, 29, 8, 0, 0, 0, usageCST)
	if _, _, err := parseUsageRange("2026-07-28 08:00:00", "2026-07-29 09:00:00", now); err == nil {
		t.Fatal("纯日期接口不应继续接受精确时间")
	}
}

func TestTrackedUserCRUD(t *testing.T) {
	m := newTestMonitor(t)
	u := &TrackedUser{UserID: 7, Username: "alice", Email: "a@b.com", AddedAt: 100}
	if err := m.storeDB.Save(u).Error; err != nil {
		t.Fatalf("save: %v", err)
	}
	// 重复添加 = 幂等更新(主键 user_id)
	u2 := &TrackedUser{UserID: 7, Username: "alice2", Email: "a@b.com", AddedAt: 200}
	if err := m.storeDB.Save(u2).Error; err != nil {
		t.Fatalf("save again: %v", err)
	}
	rows, err := m.listTracked()
	if err != nil || len(rows) != 1 || rows[0].Username != "alice2" {
		t.Fatalf("listTracked = %+v, %v; want 1 行且已更新", rows, err)
	}
	if err := m.storeDB.Delete(&TrackedUser{}, "user_id = ?", int64(7)).Error; err != nil {
		t.Fatalf("delete: %v", err)
	}
	if rows, _ := m.listTracked(); len(rows) != 0 {
		t.Fatalf("删除后应为空,得到 %+v", rows)
	}
}

// newFakeProdDB 建一个 sqlite 假生产库,带最小化的 users/logs 表(列名与 new-api rc.4 对齐)。
func newFakeProdDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/prod.db")
	if err != nil {
		t.Fatalf("open fake prod: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	stmts := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, email TEXT, quota INTEGER, used_quota INTEGER)",
		"CREATE TABLE logs (id INTEGER PRIMARY KEY, user_id INTEGER, created_at INTEGER, type INTEGER, model_name TEXT, quota INTEGER, prompt_tokens INTEGER, completion_tokens INTEGER, `group` TEXT, token_id INTEGER DEFAULT 0, token_name TEXT DEFAULT '', username TEXT DEFAULT '', use_time INTEGER DEFAULT 0, is_stream INTEGER DEFAULT 0, content TEXT DEFAULT '', other TEXT DEFAULT '', request_id TEXT DEFAULT '')",
		"CREATE TABLE tokens (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT, `key` TEXT, `group` TEXT, used_quota INTEGER DEFAULT 0, deleted_at TIMESTAMP)",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("create schema: %v", err)
		}
	}
	return db
}

func newCountingFakeProdDB(t *testing.T) (*sql.DB, *usageQueryCounts) {
	t.Helper()
	counts := &usageQueryCounts{}
	driverName := fmt.Sprintf("usage-counting-sqlite-%d", usageCountingDriverID.Add(1))
	sql.Register(driverName, &usageCountingDriver{inner: &glebarezsqlite.Driver{}, counts: counts})
	db, err := sql.Open(driverName, t.TempDir()+"/prod.db")
	if err != nil {
		t.Fatalf("open counting fake prod: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, stmt := range []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, email TEXT, quota INTEGER, used_quota INTEGER)",
		"CREATE TABLE logs (id INTEGER PRIMARY KEY, user_id INTEGER, created_at INTEGER, type INTEGER, model_name TEXT, quota INTEGER, prompt_tokens INTEGER, completion_tokens INTEGER, `group` TEXT, token_id INTEGER DEFAULT 0, token_name TEXT DEFAULT '', username TEXT DEFAULT '', use_time INTEGER DEFAULT 0, is_stream INTEGER DEFAULT 0, content TEXT DEFAULT '', other TEXT DEFAULT '', request_id TEXT DEFAULT '')",
		"CREATE TABLE tokens (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT, `key` TEXT, `group` TEXT, used_quota INTEGER DEFAULT 0, deleted_at TIMESTAMP)",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create counting schema: %v", err)
		}
	}
	counts.reset()
	return db, counts
}

func TestResolveNewAPIUser(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	seed := []string{
		"INSERT INTO users (id,username,email) VALUES (1,'alice','a@b.com')",
		"INSERT INTO users (id,username,email) VALUES (2,'bob','dup@x.com')",
		"INSERT INTO users (id,username,email) VALUES (3,'bob2','dup@x.com')",
	}
	for _, s := range seed {
		if _, err := m.prodDB.Exec(s); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	ctx := context.Background()

	if u, err := m.resolveNewAPIUser(ctx, "1"); err != nil || u.UserID != 1 || u.Username != "alice" {
		t.Fatalf("按ID解析 = %+v, %v", u, err)
	}
	if u, err := m.resolveNewAPIUser(ctx, "alice"); err != nil || u.UserID != 1 {
		t.Fatalf("按用户名解析 = %+v, %v", u, err)
	}
	if u, err := m.resolveNewAPIUser(ctx, "a@b.com"); err != nil || u.UserID != 1 {
		t.Fatalf("按邮箱解析 = %+v, %v", u, err)
	}
	if _, err := m.resolveNewAPIUser(ctx, "dup@x.com"); err == nil {
		t.Fatal("重复邮箱应报错,提示改用ID")
	}
	if _, err := m.resolveNewAPIUser(ctx, "999"); err == nil {
		t.Fatal("不存在的ID应报错")
	}
	if _, err := m.resolveNewAPIUser(ctx, "  "); err == nil {
		t.Fatal("空输入应报错")
	}
	// 数字撞车:用户名"7"的人 vs ID=7 的人 → 数字输入按 ID 优先
	if _, err := m.prodDB.Exec("INSERT INTO users (id,username,email) VALUES (7,'seven','s@x.com'),(8,'7','collide@x.com')"); err != nil {
		t.Fatalf("seed collide: %v", err)
	}
	if u, err := m.resolveNewAPIUser(ctx, "7"); err != nil || u.UserID != 7 || u.Username != "seven" {
		t.Fatalf("数字撞车应 ID 优先 = %+v, %v", u, err)
	}
	if u, err := m.resolveNewAPIUser(ctx, "seven"); err != nil || u.UserID != 7 {
		t.Fatalf("按用户名 seven = %+v, %v", u, err)
	}
}

func TestCustomerGroups(t *testing.T) {
	m := newTestMonitor(t)
	// 建组
	g := CustomerGroup{Name: "AcmeCorp", Note: "重点客户", CreatedAt: 100}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatalf("create group: %v", err)
	}
	// 组名唯一
	if err := m.storeDB.Create(&CustomerGroup{Name: "AcmeCorp"}).Error; err == nil {
		t.Fatal("重名分组应被唯一索引拒绝")
	}
	// 成员归组
	for _, u := range []TrackedUser{{UserID: 1, Username: "a", GroupID: g.ID}, {UserID: 2, Username: "b", GroupID: g.ID}, {UserID: 3, Username: "c"}} {
		uu := u
		if err := m.storeDB.Save(&uu).Error; err != nil {
			t.Fatalf("save user: %v", err)
		}
	}
	var n int64
	m.storeDB.Model(&TrackedUser{}).Where("group_id = ?", g.ID).Count(&n)
	if n != 2 {
		t.Fatalf("组内人数 = %d", n)
	}
	// 解散:成员回未分组,用户仍在
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&TrackedUser{}).Where("group_id = ?", g.ID).Update("group_id", 0).Error; err != nil {
			return err
		}
		return tx.Delete(&CustomerGroup{}, g.ID).Error
	})
	if err != nil {
		t.Fatalf("dissolve: %v", err)
	}
	var users []TrackedUser
	m.storeDB.Find(&users)
	if len(users) != 3 {
		t.Fatalf("解散不应删用户,got %d", len(users))
	}
	for _, u := range users {
		if u.GroupID != 0 {
			t.Fatalf("解散后应回未分组 %+v", u)
		}
	}
	var gs []CustomerGroup
	m.storeDB.Find(&gs)
	if len(gs) != 0 {
		t.Fatalf("分组应已删除 %+v", gs)
	}
}

func TestComputeUsageStats(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite

	day1 := time.Date(2026, 7, 1, 10, 0, 0, 0, usageCST).Unix() // 7-01 白天
	day1b := time.Date(2026, 7, 1, 23, 59, 0, 0, usageCST).Unix()
	day2 := time.Date(2026, 7, 2, 1, 0, 0, 0, usageCST).Unix() // 7-02 凌晨(考验 CST 切日:UTC 里仍是 7-01)
	outside := time.Date(2026, 6, 20, 10, 0, 0, 0, usageCST).Unix()

	type row struct {
		uid, ts, typ, quota, pt, ct int64
		model, grp                  string
	}
	rows := []row{
		{1, day1, 2, 500000, 100, 50, "gpt-4o", "default"},   // $1
		{1, day1b, 2, 250000, 40, 10, "claude-x", "vip"},     // $0.5
		{2, day2, 2, 1000000, 300, 200, "gpt-4o", "default"}, // $2
		{2, day2, 5, 0, 0, 0, "gpt-4o", "default"},           // 失败行(type=5):不计
		{3, day1, 2, 9000000, 999, 999, "gpt-4o", "default"}, // 未被盯用户:不计
		{1, outside, 2, 700000, 1, 1, "gpt-4o", "default"},   // 范围外:不计
	}
	for _, r := range rows {
		if _, err := m.prodDB.Exec("INSERT INTO logs (user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`) VALUES (?,?,?,?,?,?,?,?)",
			r.uid, r.ts, r.typ, r.model, r.quota, r.pt, r.ct, r.grp); err != nil {
			t.Fatalf("seed logs: %v", err)
		}
	}

	fromTs := time.Date(2026, 7, 1, 0, 0, 0, 0, usageCST).Unix()
	toTs := time.Date(2026, 7, 3, 0, 0, 0, 0, usageCST).Unix()
	st, err := m.computeUsageStats(context.Background(), []int64{1, 2}, fromTs, toTs, 0)
	if err != nil {
		t.Fatalf("computeUsageStats: %v", err)
	}

	// 每日:两天,CST 切日正确(day2 的 UTC 日期仍是 7-01,必须归到 7-02)
	if len(st.Daily) != 2 || st.Daily[0].Date != "2026-07-01" || st.Daily[1].Date != "2026-07-02" {
		t.Fatalf("Daily = %+v", st.Daily)
	}
	if st.Daily[0].Requests != 2 || st.Daily[0].CostUSD != 1.5 || st.Daily[0].Tokens != 200 {
		t.Fatalf("7-01 = %+v", st.Daily[0])
	}
	if st.Daily[1].Requests != 1 || st.Daily[1].CostUSD != 2 || st.Daily[1].Tokens != 500 {
		t.Fatalf("7-02 = %+v", st.Daily[1])
	}
	// 汇总
	if st.Summary.Requests != 3 || st.Summary.CostUSD != 3.5 || st.Summary.Tokens != 700 {
		t.Fatalf("Summary = %+v", st.Summary)
	}
	// 按分组:default($3) > vip($0.5),按费用降序
	if len(st.ByGroup) != 2 || st.ByGroup[0].Key != "default" || st.ByGroup[0].CostUSD != 3 || st.ByGroup[1].Key != "vip" {
		t.Fatalf("ByGroup = %+v", st.ByGroup)
	}
	// 按模型:gpt-4o($3) > claude-x($0.5)
	if len(st.ByModel) != 2 || st.ByModel[0].Key != "gpt-4o" || st.ByModel[0].Requests != 2 {
		t.Fatalf("ByModel = %+v", st.ByModel)
	}
	// 起止日期回显
	if st.From != "2026-07-01" || st.To != "2026-07-02" {
		t.Fatalf("From/To = %s/%s", st.From, st.To)
	}

	// 空名单:不出 SQL,直接空结果
	if empty, err := m.computeUsageStats(context.Background(), nil, fromTs, toTs, 0); err != nil || len(empty.Daily) != 0 {
		t.Fatalf("空名单 = %+v, %v", empty, err)
	}

	// —— 令牌下钻:tokenID>0 只聚合该令牌的日志(user_id+token_id 双条件) ——
	if _, err := m.prodDB.Exec("UPDATE logs SET token_id = 77 WHERE user_id = 1 AND quota = 500000"); err != nil {
		t.Fatal(err)
	}
	ts, err := m.computeUsageStats(context.Background(), []int64{1}, fromTs, toTs, 77)
	if err != nil {
		t.Fatalf("token 过滤聚合: %v", err)
	}
	if ts.Summary.Requests != 1 || ts.Summary.CostUSD != 1 || len(ts.Daily) != 1 || ts.Daily[0].Date != "2026-07-01" {
		t.Fatalf("token 过滤结果 = %+v", ts)
	}
	// 越权探测:token 77 属 user1,拿 user2 查必须为空(隔离靠双条件,不靠归属校验)
	if cross, err := m.computeUsageStats(context.Background(), []int64{2}, fromTs, toTs, 77); err != nil || cross.Summary.Requests != 0 {
		t.Fatalf("跨用户令牌查询应为空 = %+v, %v", cross, err)
	}

	// —— 矩阵数据(列表页,前端渲染为 行=用户×列=日期):days 连续新→旧,格=当日费用 ——
	mx, err := m.computeUsageMatrix(context.Background(), []int64{1, 2}, fromTs, toTs)
	if err != nil {
		t.Fatalf("computeUsageMatrix: %v", err)
	}
	if len(mx.Days) != 2 || mx.Days[0] != "2026-07-02" || mx.Days[1] != "2026-07-01" {
		t.Fatalf("Days 应连续且新→旧 = %+v", mx.Days)
	}
	// 稀疏格:user1 只 7-01 一格($1.5,两笔合并),user2 只 7-02 一格($2);没消费的天不出格
	cell := map[string]float64{}
	for _, c := range mx.Cells {
		cell[c.Date+"#"+strconv.FormatInt(c.UserID, 10)] = c.CostUSD
	}
	if len(mx.Cells) != 2 || cell["2026-07-01#1"] != 1.5 || cell["2026-07-02#2"] != 2 {
		t.Fatalf("Cells = %+v", mx.Cells)
	}
	// 空名单矩阵:仍出日期轴,零格
	mx0, err := m.computeUsageMatrix(context.Background(), nil, fromTs, toTs)
	if err != nil || len(mx0.Days) != 2 || len(mx0.Cells) != 0 {
		t.Fatalf("空名单矩阵 = %+v, %v", mx0, err)
	}
}

func TestUsageRefundAndNetQuota(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite

	day1 := time.Date(2026, 7, 1, 10, 0, 0, 0, usageCST).Unix()
	day2 := time.Date(2026, 7, 2, 10, 0, 0, 0, usageCST).Unix()
	seed := []string{
		"INSERT INTO users (id,username,email) VALUES (1,'u1','u1@example.com')",
		"INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota) VALUES (10,1,'t1','abcdefghijk','g1',500000)",
		fmt.Sprintf("INSERT INTO logs (user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,%d,2,'m1',500000,100,50,'g1',10,'t1')", day1),
		fmt.Sprintf("INSERT INTO logs (user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,%d,6,'m1',200000,0,0,'g1',10,'t1')", day1),
		fmt.Sprintf("INSERT INTO logs (user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,%d,6,'m1',700000,0,0,'g1',10,'t1')", day2),
		fmt.Sprintf("INSERT INTO logs (user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,%d,5,'m1',900000,999,999,'g1',10,'t1')", day1),
	}
	for _, q := range seed {
		if _, err := m.prodDB.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	fromTs := time.Date(2026, 7, 1, 0, 0, 0, 0, usageCST).Unix()
	toTs := time.Date(2026, 7, 3, 0, 0, 0, 0, usageCST).Unix()

	st, err := m.computeUsageStats(context.Background(), []int64{1}, fromTs, toTs, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Daily) != 2 {
		t.Fatalf("每日聚合应包含仅退款日期: %+v", st.Daily)
	}
	if d := st.Daily[0]; d.Requests != 1 || d.RefundRecords != 1 || d.ConsumeQuota != 500000 || d.RefundQuota != 200000 || d.NetQuota != 300000 || d.Tokens != 150 {
		t.Fatalf("首日消费/退款口径错误: %+v", d)
	}
	if d := st.Daily[1]; d.Requests != 0 || d.RefundRecords != 1 || d.ConsumeQuota != 0 || d.RefundQuota != 700000 || d.NetQuota != -700000 || d.Tokens != 0 {
		t.Fatalf("仅退款日应允许负净消费: %+v", d)
	}
	if s := st.Summary; s.Requests != 1 || s.RefundRecords != 2 || s.ConsumeQuota != 500000 || s.RefundQuota != 900000 || s.NetQuota != -400000 || s.Tokens != 150 || s.CostUSD != 1 {
		t.Fatalf("汇总口径错误(兼容 cost_usd 必须仍是消费毛额): %+v", s)
	}
	if len(st.ByGroup) != 1 || st.ByGroup[0].NetQuota != -400000 || len(st.ByModel) != 1 || st.ByModel[0].RefundQuota != 900000 {
		t.Fatalf("维度退款口径错误: group=%+v model=%+v", st.ByGroup, st.ByModel)
	}

	mx, err := m.computeUsageMatrix(context.Background(), []int64{1}, fromTs, toTs)
	if err != nil {
		t.Fatal(err)
	}
	if len(mx.Cells) != 2 || mx.Cells[0].NetQuota != 300000 || mx.Cells[1].NetQuota != -700000 {
		t.Fatalf("矩阵应包含消费/退款/净消费: %+v", mx.Cells)
	}
	toks, err := m.computeUserTokenUsage(context.Background(), 1, fromTs, toTs)
	if err != nil {
		t.Fatal(err)
	}
	if len(toks) != 1 || toks[0].Requests != 1 || toks[0].RefundRecords != 2 || toks[0].ConsumeQuota != 500000 || toks[0].RefundQuota != 900000 || toks[0].NetQuota != -400000 {
		t.Fatalf("令牌退款口径错误: %+v", toks)
	}
}

func TestUsageDimensionTruncationAndDailyOtherCompleteness(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	day := time.Date(2026, 7, 1, 10, 0, 0, 0, usageCST).Unix()
	var wantQuota int64
	for i := 0; i < maxUsageDimRows+2; i++ {
		quota := int64(i + 1)
		wantQuota += quota
		if _, err := m.prodDB.Exec("INSERT INTO logs (user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`) VALUES (1,?,2,?,?,1,1,?)",
			day, fmt.Sprintf("model-%03d", i), quota, fmt.Sprintf("group-%03d", i)); err != nil {
			t.Fatal(err)
		}
	}
	fromTs := time.Date(2026, 7, 1, 0, 0, 0, 0, usageCST).Unix()
	toTs := time.Date(2026, 7, 2, 0, 0, 0, 0, usageCST).Unix()
	st, err := m.computeUsageStats(context.Background(), []int64{1}, fromTs, toTs, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !st.ByGroupTruncated || !st.ByModelTruncated || len(st.ByGroup) != maxUsageDimRows || len(st.ByModel) != maxUsageDimRows {
		t.Fatalf("维度截断必须显式标记: groups=%d/%v models=%d/%v", len(st.ByGroup), st.ByGroupTruncated, len(st.ByModel), st.ByModelTruncated)
	}
	if len(st.DailyByModel) != 7 {
		t.Fatalf("每日模型应固定为 Top6+其他，而不是被硬 LIMIT 截断: %+v", st.DailyByModel)
	}
	var gotQuota int64
	var otherCount int
	for _, r := range st.DailyByModel {
		gotQuota += r.ConsumeQuota
		if r.Other {
			otherCount++
		}
	}
	if gotQuota != wantQuota || otherCount != 1 || st.Summary.ConsumeQuota != wantQuota {
		t.Fatalf("每日趋势必须覆盖完整消费: got=%d want=%d other=%d summary=%d", gotQuota, wantQuota, otherCount, st.Summary.ConsumeQuota)
	}
}

func TestServeUsageStatsDoesNotReturnPartialTokenData(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	if err := m.storeDB.Save(&TrackedUser{UserID: 1, Username: "u1", AddedAt: 1}).Error; err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 7, 1, 10, 0, 0, 0, usageCST).Unix()
	for _, q := range []string{
		"INSERT INTO users (id,username,email) VALUES (1,'u1','')",
		fmt.Sprintf("INSERT INTO logs (user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id) VALUES (1,%d,2,'m',500000,1,1,'g',10)", day),
		"DROP TABLE tokens",
	} {
		if _, err := m.prodDB.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/usage/stats?user_id=1&from=2026-07-01&to=2026-07-01", nil)
	m.serveUsageStats(c)
	if w.Code != 500 || strings.Contains(w.Body.String(), `"stats"`) || !strings.Contains(w.Body.String(), "令牌统计查询失败") {
		t.Fatalf("令牌子查询失败不得返回统计半成品: status=%d body=%s", w.Code, w.Body.String())
	}
}

type usageMatrixHTTPResponse struct {
	Enabled bool        `json:"enabled"`
	Matrix  UsageMatrix `json:"matrix"`
}

func requestUsageMatrixForTest(t *testing.T, m *Monitor, path string) usageMatrixHTTPResponse {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, path, nil)
	m.serveUsageMatrix(c)
	if w.Code != http.StatusOK {
		t.Fatalf("用量矩阵请求失败: status=%d body=%s", w.Code, w.Body.String())
	}
	var out usageMatrixHTTPResponse
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestServeUsageMatrixBalancesPerformanceAccuracyAndFreshness(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	remote := newMemoryByteCacheStore()
	m.usageCache = newUsageResultCacheForTest(remote, 32, 1<<20)

	g := CustomerGroup{Name: "cache-company"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, GroupID: g.ID, Username: "snapshot", Email: "snapshot@example.test"}).Error; err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 7, 1, 10, 0, 0, 0, usageCST).Unix()
	seed := []string{
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (1,'live-v1','live-v1@example.test',500000,1000000)",
		fmt.Sprintf("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`) VALUES (1,1,%d,2,'m',500000,10,2,'g')", day),
	}
	for _, q := range seed {
		if _, err := m.prodDB.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	path := "/usage/matrix?from=2026-07-01&to=2026-07-01"
	first := requestUsageMatrixForTest(t, m, path)
	if len(first.Matrix.Cells) != 1 || first.Matrix.Cells[0].Requests != 1 ||
		len(first.Matrix.Users) != 1 || first.Matrix.Users[0].BalanceQuota == nil || *first.Matrix.Users[0].BalanceQuota != 500000 ||
		first.Matrix.Users[0].Username != "live-v1" || m.usageCache.fills.Load() != 1 {
		t.Fatalf("首次矩阵结果错误: matrix=%+v fills=%d", first.Matrix, m.usageCache.fills.Load())
	}

	if _, err := m.prodDB.Exec("UPDATE users SET username='live-v2',email='live-v2@example.test',quota=750000,used_quota=1250000 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`) VALUES (2,1,?,2,'m',250000,4,1,'g')", day+60); err != nil {
		t.Fatal(err)
	}

	// TTL 内聚合保持上次成功结果，但身份/余额/累计消耗每次实时取回。
	cached := requestUsageMatrixForTest(t, m, path)
	if cached.Matrix.Cells[0].Requests != 1 || cached.Matrix.Users[0].Username != "live-v2" ||
		cached.Matrix.Users[0].BalanceQuota == nil || *cached.Matrix.Users[0].BalanceQuota != 750000 ||
		m.usageCache.fills.Load() != 1 {
		t.Fatalf("缓存命中时的聚合/实时字段边界错误: matrix=%+v fills=%d", cached.Matrix, m.usageCache.fills.Load())
	}

	// 现有日期重选动作会带 refresh=1：立即重算并覆盖缓存。
	refreshed := requestUsageMatrixForTest(t, m, path+"&refresh=1")
	if refreshed.Matrix.Cells[0].Requests != 2 || refreshed.Matrix.Cells[0].ConsumeQuota != 750000 || m.usageCache.fills.Load() != 2 {
		t.Fatalf("主动刷新未取到最新聚合: matrix=%+v fills=%d", refreshed.Matrix, m.usageCache.fills.Load())
	}
	after := requestUsageMatrixForTest(t, m, path)
	if after.Matrix.Cells[0].Requests != 2 || m.usageCache.fills.Load() != 2 {
		t.Fatalf("刷新结果未覆盖缓存: matrix=%+v fills=%d", after.Matrix, m.usageCache.fills.Load())
	}

	remote.mu.Lock()
	defer remote.mu.Unlock()
	for key, item := range remote.items {
		payload := string(item.value)
		if strings.Contains(payload, "live-v2") || strings.Contains(payload, "live-v2@example.test") {
			t.Fatalf("Redis 矩阵键 %q 不得包含身份资料: %s", key, payload)
		}
	}
}

type usageStatsHTTPResponse struct {
	Stats          UsageStats   `json:"stats"`
	ByToken        []TokenUsage `json:"by_token"`
	BalanceQuota   *int64       `json:"balance_quota"`
	TotalUsedQuota *int64       `json:"total_used_quota"`
	Members        int          `json:"members"`
}

func requestUsageStatsForTest(t *testing.T, m *Monitor, path string) usageStatsHTTPResponse {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, path, nil)
	m.serveUsageStats(c)
	if w.Code != http.StatusOK {
		t.Fatalf("用量详情请求失败: status=%d body=%s", w.Code, w.Body.String())
	}
	var out usageStatsHTTPResponse
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestServeUsageStatsCachesOnlyAggregatesAndRefreshesExactly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	remote := newMemoryByteCacheStore()
	m.usageCache = newUsageResultCacheForTest(remote, 32, 1<<20)

	g := CustomerGroup{Name: "detail-company"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, GroupID: g.ID, Username: "snapshot", Email: "snapshot@example.test"}).Error; err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 7, 1, 10, 0, 0, 0, usageCST).Unix()
	seed := []string{
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (1,'owner-v1','owner-v1@example.test',500000,1000000)",
		"INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota) VALUES (10,1,'token-a','abcdefghijklmnop','g',750000)",
		fmt.Sprintf("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,1,%d,2,'m',500000,10,2,'g',10,'token-a')", day),
	}
	for _, q := range seed {
		if _, err := m.prodDB.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	path := "/usage/stats?user_id=1&from=2026-07-01&to=2026-07-01"
	first := requestUsageStatsForTest(t, m, path)
	if first.Stats.Summary.Requests != 1 || len(first.ByToken) != 1 || first.ByToken[0].Owner != "owner-v1" ||
		first.BalanceQuota == nil || *first.BalanceQuota != 500000 || m.usageCache.fills.Load() != 2 {
		t.Fatalf("首次详情结果错误: %+v fills=%d", first, m.usageCache.fills.Load())
	}

	if _, err := m.prodDB.Exec("UPDATE users SET username='owner-v2',email='owner-v2@example.test',quota=800000,used_quota=1300000 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("UPDATE tokens SET name='token-b',`key`='qrstuvwxyzabcdef',`group`='g2',used_quota=900000,deleted_at='2026-07-02 00:00:00' WHERE id=10"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (2,1,?,2,'m',250000,3,1,'g',10,'token-a')", day+60); err != nil {
		t.Fatal(err)
	}
	cached := requestUsageStatsForTest(t, m, path)
	if cached.Stats.Summary.Requests != 1 || cached.ByToken[0].Owner != "owner-v2" ||
		cached.ByToken[0].Name != "token-b" || cached.ByToken[0].Group != "g2" || !cached.ByToken[0].Deleted ||
		cached.ByToken[0].MaskedKey == "qrstuvwxyzabcdef" || cached.ByToken[0].TotalCostQuota == nil || *cached.ByToken[0].TotalCostQuota != 900000 ||
		cached.BalanceQuota == nil || *cached.BalanceQuota != 800000 ||
		cached.TotalUsedQuota == nil || *cached.TotalUsedQuota != 1300000 || m.usageCache.fills.Load() != 2 {
		t.Fatalf("详情缓存命中后实时字段错误: %+v fills=%d", cached, m.usageCache.fills.Load())
	}
	refreshed := requestUsageStatsForTest(t, m, path+"&refresh=1")
	if refreshed.Stats.Summary.Requests != 2 || refreshed.Stats.Summary.ConsumeQuota != 750000 || m.usageCache.fills.Load() != 4 {
		t.Fatalf("详情主动刷新错误: %+v fills=%d", refreshed, m.usageCache.fills.Load())
	}

	remote.mu.Lock()
	defer remote.mu.Unlock()
	for key, item := range remote.items {
		payload := string(item.value)
		if strings.Contains(payload, "owner-v1") || strings.Contains(payload, "owner-v2") ||
			strings.Contains(payload, "example.test") || strings.Contains(payload, "abcdefghijklmnop") ||
			strings.Contains(payload, "token-b") || strings.Contains(payload, "qrstuvwxyzabcdef") {
			t.Fatalf("Redis 详情键 %q 泄漏实时/完整敏感字段: %s", key, payload)
		}
	}
}

// 锁定实时资料查询合并前后的兼容语义：邮箱回退、负余额、NULL、零值、主站已删用户，
// 以及组织内部分/全部用户缺失时的合计都不能因减少 SQL 往返而改变。
func TestServeUsageStatsLiveFieldCompatibility(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	m.usageCache = newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, 1<<20)

	g := CustomerGroup{Name: "live-fields"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	for _, u := range []TrackedUser{
		{UserID: 1, GroupID: g.ID, Username: "snapshot-one", Email: "snapshot-one@example.test"},
		{UserID: 2, GroupID: g.ID, Username: "snapshot-two", Email: "snapshot-two@example.test"},
	} {
		if err := m.storeDB.Create(&u).Error; err != nil {
			t.Fatal(err)
		}
	}
	day := time.Date(2026, 7, 1, 10, 0, 0, 0, usageCST).Unix()
	seed := []string{
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (1,'','email-only@example.test',-250000,0)",
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (2,'second','',NULL,1500000)",
		"INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota) VALUES (10,1,'token-one','abcdefghijklmnop','g',0)",
		fmt.Sprintf("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,1,%d,2,'m',500000,1,1,'g',10,'token-one')", day),
		fmt.Sprintf("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`) VALUES (2,2,%d,2,'m',250000,1,1,'g')", day),
	}
	for _, q := range seed {
		if _, err := m.prodDB.Exec(q); err != nil {
			t.Fatal(err)
		}
	}

	userPath := "/usage/stats?user_id=1&from=2026-07-01&to=2026-07-01"
	user := requestUsageStatsForTest(t, m, userPath)
	if user.BalanceQuota == nil || *user.BalanceQuota != -250000 ||
		user.TotalUsedQuota == nil || *user.TotalUsedQuota != 0 ||
		len(user.ByToken) != 1 || user.ByToken[0].Owner != "email-only@example.test" {
		t.Fatalf("单用户实时字段兼容性错误: %+v", user)
	}

	groupPath := fmt.Sprintf("/usage/stats?group_id=%d&from=2026-07-01&to=2026-07-01", g.ID)
	group := requestUsageStatsForTest(t, m, groupPath)
	if group.Members != 2 || group.BalanceQuota == nil || *group.BalanceQuota != -250000 ||
		group.TotalUsedQuota == nil || *group.TotalUsedQuota != 1500000 {
		t.Fatalf("组织实时合计兼容性错误: %+v", group)
	}
	// 名单里仍有成员、但其中一人已从主站删除时，只合计仍存在的主站行。
	if _, err := m.prodDB.Exec("DELETE FROM users WHERE id=2"); err != nil {
		t.Fatal(err)
	}
	partialGroup := requestUsageStatsForTest(t, m, groupPath)
	if partialGroup.BalanceQuota == nil || *partialGroup.BalanceQuota != -250000 ||
		partialGroup.TotalUsedQuota == nil || *partialGroup.TotalUsedQuota != 0 {
		t.Fatalf("组织成员部分从主站删除后的合计错误: %+v", partialGroup)
	}

	// 主站删掉用户后，日志聚合仍可从缓存读取；单用户金额为 null、owner 回退 #ID。
	if _, err := m.prodDB.Exec("DELETE FROM users"); err != nil {
		t.Fatal(err)
	}
	deletedUser := requestUsageStatsForTest(t, m, userPath)
	if deletedUser.BalanceQuota != nil || deletedUser.TotalUsedQuota != nil ||
		len(deletedUser.ByToken) != 1 || deletedUser.ByToken[0].Owner != "#1" {
		t.Fatalf("已删用户降级语义错误: %+v", deletedUser)
	}
	// 组织聚合 SQL 对零匹配行仍返回 COALESCE 后的 0；这与既有页面语义一致。
	deletedGroup := requestUsageStatsForTest(t, m, groupPath)
	if deletedGroup.BalanceQuota == nil || *deletedGroup.BalanceQuota != 0 ||
		deletedGroup.TotalUsedQuota == nil || *deletedGroup.TotalUsedQuota != 0 {
		t.Fatalf("组织成员全部从主站删除后的合计语义错误: %+v", deletedGroup)
	}
}

func TestUsageBlankLiveIdentityFallsBackToID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	m.usageCache = newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, 1<<20)

	g := CustomerGroup{Name: "blank-live-identity"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	member := TrackedUser{UserID: 1, GroupID: g.ID, Username: "old-snapshot", Email: "old@example.test"}
	if err := m.storeDB.Create(&member).Error; err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 7, 1, 10, 0, 0, 0, usageCST).Unix()
	for _, stmt := range []string{
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (1,'','',500000,750000)",
		"INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota) VALUES (10,1,'token','abcdefghijklmnop','g',750000)",
		fmt.Sprintf("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,1,%d,2,'m',250000,1,1,'g',10,'token')", createdAt),
	} {
		if _, err := m.prodDB.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}

	admin := requestUsageStatsForTest(t, m, "/usage/stats?user_id=1&from=2026-07-01&to=2026-07-01")
	if len(admin.ByToken) != 1 || admin.ByToken[0].Owner != "#1" ||
		admin.BalanceQuota == nil || *admin.BalanceQuota != 500000 ||
		admin.TotalUsedQuota == nil || *admin.TotalUsedQuota != 750000 {
		t.Fatalf("管理端空用户名/邮箱回退错误: %+v", admin)
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/user?uid=1&from=2026-07-01&to=2026-07-01", nil)
	c.Set("portalGID", g.ID)
	m.portalUserDetail(c)
	if w.Code != http.StatusOK {
		t.Fatalf("Portal 空身份详情失败: %d %s", w.Code, w.Body.String())
	}
	var portal struct {
		Data struct {
			ByToken        []TokenUsage `json:"by_token"`
			BalanceQuota   *int64       `json:"balance_quota"`
			TotalUsedQuota *int64       `json:"total_used_quota"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &portal); err != nil {
		t.Fatal(err)
	}
	if len(portal.Data.ByToken) != 1 || portal.Data.ByToken[0].Owner != "#1" ||
		portal.Data.BalanceQuota == nil || *portal.Data.BalanceQuota != 500000 ||
		portal.Data.TotalUsedQuota == nil || *portal.Data.TotalUsedQuota != 750000 {
		t.Fatalf("Portal 空用户名/邮箱回退错误: %+v", portal.Data)
	}
}

func TestUsageUsersQueryFailureKeepsHistoricalDataAvailable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	m.usageCache = newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, 1<<20)

	g := CustomerGroup{Name: "users-query-failure"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	member := TrackedUser{UserID: 1, GroupID: g.ID, Username: "snapshot-owner", Email: "snapshot@example.test"}
	if err := m.storeDB.Create(&member).Error; err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 7, 1, 10, 0, 0, 0, usageCST).Unix()
	for _, stmt := range []string{
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (1,'live-owner','live@example.test',500000,750000)",
		"INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota) VALUES (10,1,'token','abcdefghijklmnop','g',750000)",
		fmt.Sprintf("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,1,%d,2,'m',250000,1,1,'g',10,'token')", createdAt),
		"DROP TABLE users",
	} {
		if _, err := m.prodDB.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}

	adminPath := "/usage/stats?user_id=1&from=2026-07-01&to=2026-07-01"
	admin := requestUsageStatsForTest(t, m, adminPath)
	if admin.Stats.Summary.Requests != 1 || len(admin.ByToken) != 1 || admin.ByToken[0].Owner != "#1" ||
		admin.BalanceQuota != nil || admin.TotalUsedQuota != nil {
		t.Fatalf("users 查询失败时管理端降级错误: %+v", admin)
	}

	groupPath := fmt.Sprintf("/usage/stats?group_id=%d&from=2026-07-01&to=2026-07-01", g.ID)
	group := requestUsageStatsForTest(t, m, groupPath)
	if group.Stats.Summary.Requests != 1 || group.BalanceQuota != nil || group.TotalUsedQuota != nil {
		t.Fatalf("users 查询失败时组织汇总降级错误: %+v", group)
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/user?uid=1&from=2026-07-01&to=2026-07-01", nil)
	c.Set("portalGID", g.ID)
	m.portalUserDetail(c)
	if w.Code != http.StatusOK {
		t.Fatalf("users 查询失败时 Portal 不应中断: %d %s", w.Code, w.Body.String())
	}
	var portal struct {
		Data struct {
			Stats          UsageStats   `json:"stats"`
			ByToken        []TokenUsage `json:"by_token"`
			BalanceQuota   *int64       `json:"balance_quota"`
			TotalUsedQuota *int64       `json:"total_used_quota"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &portal); err != nil {
		t.Fatal(err)
	}
	if portal.Data.Stats.Summary.Requests != 1 || len(portal.Data.ByToken) != 1 ||
		portal.Data.ByToken[0].Owner != "snapshot-owner" ||
		portal.Data.BalanceQuota != nil || portal.Data.TotalUsedQuota != nil {
		t.Fatalf("users 查询失败时 Portal 降级错误: %+v", portal.Data)
	}
}

func TestUsageLiveQueriesAreActuallyConsolidated(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageDayExpr = usageDayExprSQLite
	m.usageCache = newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, 1<<20)

	g := CustomerGroup{Name: "query-count"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	member := TrackedUser{UserID: 1, GroupID: g.ID, Username: "snapshot", Email: "snapshot@example.test"}
	if err := m.storeDB.Create(&member).Error; err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 7, 1, 10, 0, 0, 0, usageCST).Unix()
	for _, stmt := range []string{
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (1,'live','live@example.test',500000,750000)",
		"INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota) VALUES (10,1,'token','abcdefghijklmnop','g',750000)",
		fmt.Sprintf("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,1,%d,2,'m',250000,1,1,'g',10,'token')", createdAt),
	} {
		if _, err := m.prodDB.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}

	counts.reset()
	user := requestUsageStatsForTest(t, m, "/usage/stats?user_id=1&from=2026-07-01&to=2026-07-01")
	if len(user.ByToken) != 1 || user.ByToken[0].Owner != "live" {
		t.Fatalf("管理端单用户结果错误: %+v", user)
	}
	if got := counts.users.Load(); got != 1 {
		t.Fatalf("管理端单用户 users SELECT=%d, want 1", got)
	}
	if got := counts.tokens.Load(); got != 1 {
		t.Fatalf("管理端单用户 tokens SELECT=%d, want 1", got)
	}

	counts.reset()
	group := requestUsageStatsForTest(t, m, fmt.Sprintf("/usage/stats?group_id=%d&from=2026-07-01&to=2026-07-01", g.ID))
	if group.Members != 1 || group.BalanceQuota == nil || *group.BalanceQuota != 500000 {
		t.Fatalf("管理端组织结果错误: %+v", group)
	}
	if got := counts.users.Load(); got != 1 {
		t.Fatalf("管理端组织 users SELECT=%d, want 1", got)
	}
	if got := counts.tokens.Load(); got != 0 {
		t.Fatalf("管理端组织 tokens SELECT=%d, want 0", got)
	}

	counts.reset()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/user?uid=1&from=2026-07-01&to=2026-07-01", nil)
	c.Set("portalGID", g.ID)
	m.portalUserDetail(c)
	if w.Code != http.StatusOK {
		t.Fatalf("客户端单用户详情失败: %d %s", w.Code, w.Body.String())
	}
	if got := counts.users.Load(); got != 1 {
		t.Fatalf("客户端单用户 users SELECT=%d, want 1", got)
	}
	if got := counts.tokens.Load(); got != 1 {
		t.Fatalf("客户端单用户 tokens SELECT=%d, want 1", got)
	}
}

func TestParseUsageRangeBoundary(t *testing.T) {
	now := time.Date(2026, 7, 7, 15, 0, 0, 0, usageCST)
	// 含两端点恰 366 天(覆盖闰年):2026-01-01 + 365 天 = 2027-01-01 → 应通过
	if _, _, err := parseUsageRange("2026-01-01", "2027-01-01", now); err != nil {
		t.Fatalf("恰 366 天应通过: %v", err)
	}
	// 367 天 → 应拒绝(差值恰 366*24h,>= 判定)
	if _, _, err := parseUsageRange("2026-01-01", "2027-01-02", now); err == nil {
		t.Fatal("367 天应被拒绝")
	}
	from, to, err := parseUsageRange("2026-07-02", "2026-07-02", now)
	if err != nil {
		t.Fatal(err)
	}
	if from != time.Date(2026, 7, 2, 0, 0, 0, 0, usageCST).Unix() || to != time.Date(2026, 7, 3, 0, 0, 0, 0, usageCST).Unix() {
		t.Fatalf("单日范围应为左闭右开自然日: %d ~ %d", from, to)
	}
	if _, _, err := parseUsageRange("2026-07-02 08:00:00", "2026-07-02", now); err == nil {
		t.Fatal("带时分秒的旧格式应被拒绝")
	}
}

func TestEmbeddedRangePickerIsDateOnly(t *testing.T) {
	js := string(rangePickerJS)
	if !strings.Contains(js, "type: 'dateRange'") || !strings.Contains(js, "format: 'yyyy-MM-dd'") {
		t.Fatal("范围控件没有配置为纯日期模式")
	}
	if strings.Contains(js, "dateTimeRange") || strings.Contains(js, "HH:mm:ss") {
		t.Fatal("范围控件仍残留时分秒模式")
	}
	if !strings.Contains(pageHTML, "usageRefresh(true)") || !strings.Contains(pageHTML, "q.set('refresh','1')") {
		t.Fatal("管理端重新选择日期未连接到强制取新语义")
	}
	if !strings.Contains(pageHTML, "showClear:false") || strings.Contains(pageHTML, "showClear:true") || strings.Contains(pageHTML, "onClear:") {
		t.Fatal("管理端日期控件仍允许清空为无效范围")
	}
}

// 主站用户名允许引号和反斜杠；它只能作为显示数据使用，不能进入内联事件源码。
// 这里锁住待跟进两个入口（普通提醒、长期沉默），防以后为图省事重新拼 onclick。
func TestFollowUpActionsKeepUserTextOutOfInlineJavaScript(t *testing.T) {
	html := pageHTML
	for _, forbidden := range []string{
		"const nm=(m.username||m.email",
		"onclick=\"usageOpenUserFrom(${m.user_id}",
		"onclick=\"openFuDrawer(${m.user_id}",
	} {
		if strings.Contains(html, forbidden) {
			t.Fatalf("待跟进操作仍把用户文本拼进内联 JavaScript: %q", forbidden)
		}
	}
	for _, required := range []string{
		`data-fu-action="usage" data-user-id="${m.user_id}"`,
		`data-fu-action="follow" data-user-id="${m.user_id}"`,
		"function followUpMemberName(uid)",
		"document.getElementById('followList').addEventListener('click'",
		"Number.isSafeInteger(uid)",
	} {
		if !strings.Contains(html, required) {
			t.Fatalf("待跟进操作缺少安全事件委托: %q", required)
		}
	}
}

func TestRefreshTrackedLabels(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	seed := []string{
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (1,'alice','new-alice@b.com',1000000,1500000)", // 主站已改邮箱;余额 $2、累计消耗 $3
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (2,'bob','bob@x.com',250000,0)",                // 未变;余额 $0.5、累计消耗 $0
	}
	for _, s := range seed {
		if _, err := m.prodDB.Exec(s); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	tracked := []TrackedUser{
		{UserID: 1, Username: "alice", Email: "old-alice@b.com", AddedAt: 1}, // 快照过期
		{UserID: 2, Username: "bob", Email: "bob@x.com", AddedAt: 2},         // 快照仍准
		{UserID: 9, Username: "ghost", Email: "ghost@x.com", AddedAt: 3},     // 主站已删:保留快照
	}
	for i := range tracked {
		u := tracked[i]
		if err := m.storeDB.Save(&u).Error; err != nil {
			t.Fatalf("save tracked: %v", err)
		}
	}
	out, balances, used := m.refreshTrackedLabels(context.Background(), tracked)
	if out[0].Email != "new-alice@b.com" {
		t.Fatalf("过期快照应被刷新 = %+v", out[0])
	}
	if out[1].Email != "bob@x.com" || out[2].Email != "ghost@x.com" {
		t.Fatalf("未变/已删用户处理不对 = %+v", out[1:])
	}
	// 余额顺路取回原始 quota:alice 1000000、bob 250000;已删用户(9)不在表中 → 前端显 —
	if balances[1] != 1000000 || balances[2] != 250000 {
		t.Fatalf("余额 = %+v", balances)
	}
	if _, ok := balances[9]; ok {
		t.Fatal("已删用户不应有余额")
	}
	// 累计总消耗顺路取回原始 quota:alice 1500000、bob 0;已删用户(9)不在表中 → 前端显 —
	if used[1] != 1500000 || used[2] != 0 {
		t.Fatalf("累计总消耗 = %+v", used)
	}
	if _, ok := used[9]; ok {
		t.Fatal("已删用户不应有累计消耗")
	}
	// 刷新应回写本地库(自愈缓存)
	var persisted TrackedUser
	if err := m.storeDB.First(&persisted, "user_id = ?", int64(1)).Error; err != nil || persisted.Email != "new-alice@b.com" {
		t.Fatalf("回写本地库失败 = %+v, %v", persisted, err)
	}
	// 标签取值:用户名优先 → 邮箱 → #id(需求:显示用户名)
	if trackedLabel(out[0]) != "alice" || trackedLabel(TrackedUser{UserID: 5, Email: "e@x.com"}) != "e@x.com" || trackedLabel(TrackedUser{UserID: 6}) != "#6" {
		t.Fatal("trackedLabel 优先级不对")
	}
}

func TestUserNotePreservedOnLabelRefresh(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	// 主站改了 alice 的邮箱 → 触发标签回写;备注(本地字段)必须保住
	if _, err := m.prodDB.Exec("INSERT INTO users (id,username,email) VALUES (1,'alice','new@b.com')"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	u := TrackedUser{UserID: 1, Username: "alice", Email: "old@b.com", Note: "合同7月到期", AddedAt: 1}
	if err := m.storeDB.Save(&u).Error; err != nil {
		t.Fatalf("save: %v", err)
	}
	out, _, _ := m.refreshTrackedLabels(context.Background(), []TrackedUser{u})
	if out[0].Email != "new@b.com" || out[0].Note != "合同7月到期" {
		t.Fatalf("邮箱应刷新且备注应保留 = %+v", out[0])
	}
	// 回写本地库后备注仍在
	var p TrackedUser
	m.storeDB.First(&p, "user_id = ?", int64(1))
	if p.Email != "new@b.com" || p.Note != "合同7月到期" {
		t.Fatalf("本地库 = %+v", p)
	}
}

func TestComputeFollowUps(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	m.usageCache = newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, 1<<20)

	// 固定"现在"= 2026-07-09 12:00 CST
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, usageCST).Unix()
	dayTs := func(y int, mo time.Month, d int) int64 { return time.Date(y, mo, d, 10, 0, 0, 0, usageCST).Unix() }

	// 三个客户:
	// g1 正式,成员1,连续无消费(最后消费在30天前边界外)+ 低余额 → 命中"流失"+"低余额"
	// g2 试用,成员2,近7天消费高($25)→ 命中"转化时机"
	// g3 正式,成员3,近期正常消费、余额充足 → 不命中(不上榜)
	for _, g := range []CustomerGroup{
		{ID: 1, Name: "沉睡正式", Stage: "active", CreatedAt: 1},
		{ID: 2, Name: "活跃试用", Stage: "trial", TrialEnd: now + 20*86400, CreatedAt: 2},
		{ID: 3, Name: "健康正式", Stage: "active", CreatedAt: 3},
	} {
		gg := g
		if err := m.storeDB.Create(&gg).Error; err != nil {
			t.Fatalf("group: %v", err)
		}
	}
	users := []TrackedUser{{UserID: 1, GroupID: 1}, {UserID: 2, GroupID: 2}, {UserID: 3, GroupID: 2}, {UserID: 4, GroupID: 3}}
	for _, u := range users {
		uu := u
		m.storeDB.Save(&uu)
	}
	// 主站 users:余额 g1 低($1)、g2 各$50、g3 高
	seed := []string{
		"INSERT INTO users (id,username,email,quota) VALUES (1,'u1','',500000)",   // $1 低余额
		"INSERT INTO users (id,username,email,quota) VALUES (2,'u2','',25000000)", // $50
		"INSERT INTO users (id,username,email,quota) VALUES (3,'u3','',25000000)",
		"INSERT INTO users (id,username,email,quota) VALUES (4,'u4','',50000000)",
	}
	for _, q := range seed {
		if _, err := m.prodDB.Exec(q); err != nil {
			t.Fatalf("seed users: %v", err)
		}
	}
	ins := func(uid int64, ts, quota int64) {
		if _, err := m.prodDB.Exec("INSERT INTO logs (user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`) VALUES (?,?,2,'m',?,1,1,'default')", uid, ts, quota); err != nil {
			t.Fatalf("ins log: %v", err)
		}
	}
	// g1(uid1):只有 40 天前有消费 → 30天窗口内全无 → 流失
	ins(1, now-40*86400, 100000)
	// g2(uid2/3):试用期两人近7天各自消费都高(各 >= $20 阈值)→ 各命中转化时机
	ins(2, dayTs(2026, 7, 8), 12500000) // $25
	ins(3, dayTs(2026, 7, 7), 11000000) // $22
	// g3(uid4):近期天天有,余额高 → 不命中
	ins(4, dayTs(2026, 7, 8), 200000)
	ins(4, dayTs(2026, 7, 6), 200000)

	items, err := m.computeFollowUps(context.Background(), now)
	if err != nil {
		t.Fatalf("computeFollowUps: %v", err)
	}
	byName := map[string]FollowUpCompany{}
	for _, co := range items {
		byName[co.GroupName] = co
	}
	// 健康正式:成员消费正常,不该上榜
	if _, ok := byName["健康正式"]; ok {
		t.Fatalf("健康客户不该进待跟进: %+v", items)
	}
	// 沉睡正式:成员(uid1)命中 流失 + 低余额
	g1 := byName["沉睡正式"]
	if g1.GroupID != 1 || len(g1.Members) != 1 || g1.Members[0].UserID != 1 {
		t.Fatalf("沉睡正式应有1个需跟进成员uid1: %+v", g1)
	}
	joined := strings.Join(g1.Members[0].Reasons, ";")
	if !strings.Contains(joined, "无消费") || !strings.Contains(joined, "余额低") {
		t.Fatalf("g1成员原因 = %v", g1.Members[0].Reasons)
	}
	// 活跃试用:两个成员都消费高(各命中转化时机)
	g2 := byName["活跃试用"]
	if len(g2.Members) != 2 {
		t.Fatalf("活跃试用应有2个成员: %+v", g2)
	}
	if !strings.Contains(strings.Join(g2.Members[0].Reasons, ";"), "试用消耗高") {
		t.Fatalf("g2成员原因 = %v", g2.Members[0].Reasons)
	}
	// member_total 汇总口径
	if s := m.loadUsageSettings(); s.DormantDays != 7 || s.TrialHighUSD != 20 {
		t.Fatalf("默认阈值 = %+v", s)
	}
	if got := m.usageCache.fills.Load(); got != 1 {
		t.Fatalf("待跟进首次只应填充一份 30 天矩阵: fills=%d", got)
	}
	if _, err := m.computeFollowUps(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if got := m.usageCache.fills.Load(); got != 1 {
		t.Fatalf("同窗口待跟进重算应复用矩阵缓存: fills=%d", got)
	}
}

// 按令牌聚合:全部现存令牌都列出(零用量补0)+ 累计总消耗列;已删令牌(软删/硬删)范围内有消费才显示、
// 标 deleted 且沉底;硬删回退日志名、key 空、累计为 null;区内按范围费用降序。
func TestComputeUserTokenUsage(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	if _, err := m.prodDB.Exec("INSERT INTO users (id,username,email) VALUES (5,'fiveuser','five@x.com')"); err != nil {
		t.Fatal(err)
	}
	// token 10 = 现存令牌(累计 $10);token 20 = 硬删除(logs 有记录但 tokens 表无);
	// token 30 = 现存但范围内零用量(累计 $2,必须仍显示);token 40 = 软删除且范围内有消费(累计 $6,须显示并标 deleted);
	// token 50 = 软删除且范围内无消费(必须不显示);均属 user 5
	tokSeed := []string{
		"INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota) VALUES (10,5,'生产key','abcd1234567890wxyz','claude-1.6x',5000000)",
		"INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota) VALUES (30,5,'闲置key','zzzz1234567890yyyy','',1000000)",
		"INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota,deleted_at) VALUES (40,5,'软删有量key','dddd1234567890eeee','',3000000,'2026-01-01 00:00:00')",
		"INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota,deleted_at) VALUES (50,5,'软删无量key','ffff1234567890gggg','',300000,'2026-01-01 00:00:00')",
	}
	for _, s := range tokSeed {
		if _, err := m.prodDB.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	seed := [][]any{
		// user_id, created_at, type, model, quota, pt, ct, group, token_id, token_name
		{5, 1000, 2, "gpt", 500000, 10, 10, "default", 10, "生产key"}, // $1.0
		{5, 1100, 2, "gpt", 500000, 10, 10, "default", 10, "生产key"}, // 再 $1.0 → token10 合计 $2
		{5, 1200, 2, "gpt", 2500000, 5, 5, "default", 20, "旧key"},   // $5.0,令牌已硬删
		{5, 1250, 2, "gpt", 100000, 3, 3, "default", 40, "软删有量key"}, // $0.2,令牌已软删
		{6, 1300, 2, "gpt", 999999, 1, 1, "default", 10, "别人的"},     // 别的用户,不该计入
	}
	for _, r := range seed {
		if _, err := m.prodDB.Exec("INSERT INTO logs (user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (?,?,?,?,?,?,?,?,?,?)", r...); err != nil {
			t.Fatal(err)
		}
	}
	out, err := m.computeUserTokenUsage(context.Background(), 5, 0, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 4 {
		t.Fatalf("应有 4 个令牌(现存2+硬删1+软删有量1;软删零用量不显示), 实得 %d: %+v", len(out), out)
	}
	// 分区排序:现存在前(区内费用降序:生产$2>闲置$0),已删沉底(硬删$5>软删$0.2)
	names := []string{out[0].Name, out[1].Name, out[2].Name, out[3].Name}
	want := []string{"生产key", "闲置key", "旧key", "软删有量key"}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("分区排序不对: %v (want %v)", names, want)
		}
	}
	if out[0].Deleted || out[1].Deleted || !out[2].Deleted || !out[3].Deleted {
		t.Fatalf("deleted 标记不对: %+v", out)
	}
	// 现存令牌:名称/分组来自 tokens 表,key 脱敏且不含完整明文;累计总消耗 = used_quota 折美元
	if out[0].Group != "claude-1.6x" || out[0].CostUSD != 2 || out[0].Requests != 2 || out[0].Tokens != 40 {
		t.Fatalf("现存令牌数据不对: %+v", out[0])
	}
	if out[0].TotalCostUSD == nil || *out[0].TotalCostUSD != 10 {
		t.Fatalf("现存令牌累计总消耗不对: %+v", out[0])
	}
	// 零用量令牌:必须显示,范围指标全 0,累计 $2
	if out[1].Requests != 0 || out[1].Tokens != 0 || out[1].CostUSD != 0 {
		t.Fatalf("零用量令牌应显示且范围指标为0: %+v", out[1])
	}
	if out[1].TotalCostUSD == nil || *out[1].TotalCostUSD != 2 {
		t.Fatalf("零用量令牌累计总消耗不对: %+v", out[1])
	}
	// 硬删令牌:回退日志名,key 空(前端显示 —),分组空(前端显示"默认"),累计 null
	if out[2].CostUSD != 5 || out[2].MaskedKey != "" || out[2].Group != "" || out[2].TotalCostUSD != nil {
		t.Fatalf("硬删令牌回退不对: %+v", out[2])
	}
	// 软删有量令牌:名称/key/累计仍可回查
	if out[3].CostUSD != 0.2 || out[3].MaskedKey == "" || out[3].TotalCostUSD == nil || *out[3].TotalCostUSD != 6 {
		t.Fatalf("软删有量令牌数据不对: %+v", out[3])
	}
	// 所属用户:各行都标 user 5 的展示名(username=fiveuser)
	for i, r := range out {
		if r.Owner != "fiveuser" {
			t.Fatalf("第%d行所属用户标注不对: %q", i, r.Owner)
		}
	}
	mk := out[0].MaskedKey
	if !strings.HasPrefix(mk, "sk-abcd") || !strings.HasSuffix(mk, "wxyz") || strings.Contains(mk, "567890") {
		t.Fatalf("脱敏 key 不合规(泄露或格式错): %q", mk)
	}
}

// 即使日志里出现了错误/伪造的 token_id，也只能回退显示该用户日志中的名称，
// 绝不能借 token_id 读取另一个用户的令牌名称、分组、累计用量或脱敏 key。
func TestComputeUserTokenUsageDoesNotLeakForeignToken(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	seed := []string{
		"INSERT INTO users (id,username,email) VALUES (5,'owner','owner@x.com')",
		"INSERT INTO users (id,username,email) VALUES (6,'other','other@x.com')",
		"INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota) VALUES (60,6,'其他用户密钥','foreign-secret-value','private',9000000)",
		"INSERT INTO logs (user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (5,1000,2,'gpt',500000,1,1,'default',60,'日志侧名称')",
	}
	for _, q := range seed {
		if _, err := m.prodDB.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	out, err := m.computeUserTokenUsage(context.Background(), 5, 0, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("应只有日志回退行: %+v", out)
	}
	got := out[0]
	if got.Name != "日志侧名称" || got.MaskedKey != "" || got.Group != "" || got.TotalCostUSD != nil || !got.Deleted {
		t.Fatalf("发生跨用户令牌元数据泄漏: %+v", got)
	}
}

// tokenMetaOf:归属校验(id+user_id 双条件)、脱敏、累计折美元、软删标记;查不到返回 nil 不报错。
func TestTokenMetaOf(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	if _, err := m.prodDB.Exec("INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota,deleted_at) VALUES (10,5,'生产key','abcd1234567890wxyz','vip',5000000,NULL),(11,5,'删了的','bbbb1234567890cccc','',1000000,'2026-01-01 00:00:00')"); err != nil {
		t.Fatal(err)
	}
	meta := m.tokenMetaOf(context.Background(), 5, 10)
	if meta == nil || meta.Name != "生产key" || meta.Group != "vip" || meta.Deleted {
		t.Fatalf("meta = %+v", meta)
	}
	if meta.TotalCostUSD == nil || *meta.TotalCostUSD != 10 {
		t.Fatalf("累计折美元不对: %+v", meta)
	}
	if !strings.HasPrefix(meta.MaskedKey, "sk-abcd") || strings.Contains(meta.MaskedKey, "567890") {
		t.Fatalf("脱敏不合规: %q", meta.MaskedKey)
	}
	// 软删令牌:仍可取元数据且标 deleted(令牌详情页要展示"已删除")
	if del := m.tokenMetaOf(context.Background(), 5, 11); del == nil || !del.Deleted {
		t.Fatalf("软删 meta = %+v", del)
	}
	// 越权:别人的 uid 拿这个 token id → nil(不泄露存在性)
	if cross := m.tokenMetaOf(context.Background(), 6, 10); cross != nil {
		t.Fatalf("越权应返回 nil, got %+v", cross)
	}
	// 不存在的 token → nil
	if none := m.tokenMetaOf(context.Background(), 5, 999); none != nil {
		t.Fatalf("不存在应返回 nil, got %+v", none)
	}
}

// maskTokenKey 边界:空/极短/中等/长。
func TestMaskTokenKey(t *testing.T) {
	cases := map[string]string{
		"":                   "",
		"ab":                 "**",
		"abcdef":             "sk-ab****ef",
		"abcd1234567890wxyz": "sk-abcd**********wxyz",
	}
	for in, want := range cases {
		if got := maskTokenKey(in); got != want {
			t.Fatalf("maskTokenKey(%q)=%q want %q", in, got, want)
		}
	}
}

// 日志逐条查询:组隔离(只本组成员)+ 时间窗口 + 模型/成员筛选 + 游标倒序分页。
func TestQueryGroupLogs(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	// group A = uid 10,11;别的组 uid 20。id 升序=时间升序;含多种 type + 流/首字 + content/other
	seed := [][]any{
		// id,user_id,created_at,type,model,quota,pt,ct,group,token_name,username,use_time,is_stream,content,other,request_id
		{1, 10, 1000, 2, "gpt", 500000, 100, 20, "default", "tkA", "u10", 3, 0, "", "", ""},
		{2, 11, 1100, 2, "claude", 250000, 50, 10, "default", "tkB", "u11", 5, 1, "", `{"frt":3400,"model_ratio":2.5,"group_ratio":1.4,"cache_tokens":100,"cache_ratio":0.1,"channel_id":9,"channel_name":"secret-up"}`, "req-abc"}, // 流式+首字;倍率+输入价+缓存读;other含渠道(必须不外传);带 request_id
		{3, 10, 1200, 2, "gpt", 1000000, 200, 40, "default", "tkA", "u10", 8, 0, "", `{"group_ratio":1.2}`, ""},
		{4, 20, 1300, 2, "gpt", 999999, 1, 1, "default", "tkX", "u20", 1, 0, "", "", ""},                                                    // 别的组,不该出现
		{5, 11, 1400, 2, "gpt", 300000, 30, 6, "vip", "tkB", "u11", 2, 0, "", "", ""},                                                       // 分组=vip
		{6, 10, 1500, 5, "gpt", 0, 0, 0, "default", "tkA", "u10", 120, 0, "上游返回 429 限流", `{"channel_id":9,"channel_name":"secret-up"}`, ""}, // 错误(type=5),content=错误信息(对齐 new-api:已脱敏后写入,普通用户可见)
		{7, 11, 1600, 1, "", 5000000, 0, 0, "", "", "u11", 0, 0, "充值 $10", "", ""},                                                          // 充值(type=1),content=充值说明
		{8, 11, 1700, 6, "", 1000000, 0, 0, "", "", "u11", 0, 0, "", "", ""},                                                                // 退款(type=6):对齐 new-api 官方客户端,普通用户可见
	}
	for _, r := range seed {
		if _, err := m.prodDB.Exec("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_name,username,use_time,is_stream,content,other,request_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", r...); err != nil {
			t.Fatal(err)
		}
	}
	ids := []int64{10, 11}
	// 全部(logType=0)对齐 new-api 官方客户端使用日志:全 6 种类型都可见,含错误(5)/退款(6);
	// 本组应 7 条(id 1,2,3,5,6,7,8),倒序 → 8,7,6,5,3,2,1;绝无 uid20 的 id4(越权)
	all, err := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 0, "", "", "", "", "", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 7 {
		t.Fatalf("全部(含错误/退款)应 7 条,实得 %d: %+v", len(all), all)
	}
	if all[0].ID != 8 || all[6].ID != 1 {
		t.Fatalf("倒序不对: %d..%d", all[0].ID, all[6].ID)
	}
	for _, r := range all {
		if r.ID == 4 {
			t.Fatalf("不该出现的行(越权其它组): %+v", r)
		}
	}
	// 类型筛选 消费(2):id 1,2,3,5
	if cs, _ := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 2, "", "", "", "", "", 0, 100); len(cs) != 4 {
		t.Fatalf("消费类型筛选应 4 条,得 %d", len(cs))
	}
	// 类型筛选 错误(5):只 id6;退款(6):只 id8——对齐 new-api,两者都能单独筛出来
	if es, _ := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 5, "", "", "", "", "", 0, 100); len(es) != 1 || es[0].ID != 6 {
		t.Fatalf("错误类型筛选不对: %+v", es)
	}
	if rs, _ := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 6, "", "", "", "", "", 0, 100); len(rs) != 1 || rs[0].ID != 8 {
		t.Fatalf("退款类型筛选不对: %+v", rs)
	}
	// Request ID 精确匹配(同 new-api GetUserLogs):只 id2;不匹配任何行时返回空
	if ri, _ := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 0, "", "", "", "", "req-abc", 0, 100); len(ri) != 1 || ri[0].ID != 2 {
		t.Fatalf("Request ID 筛选不对: %+v", ri)
	}
	if ri, _ := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 0, "", "", "", "", "req-nonexist", 0, 100); len(ri) != 0 {
		t.Fatalf("Request ID 不匹配应 0 条,得 %d", len(ri))
	}
	// 流式+首字:id2 应 IsStream=true、FirstByteMs=3400,且【绝不】泄露 other 里的渠道
	var r2 LogRow
	for _, r := range all {
		if r.ID == 2 {
			r2 = r
		}
	}
	if !r2.IsStream || r2.FirstByteMs != 3400 {
		t.Fatalf("流式/首字不对: %+v", r2)
	}
	// 校验字段(id=3):消费、非流、有花费
	var r3 LogRow
	for _, r := range all {
		if r.ID == 3 {
			r3 = r
		}
	}
	if r3.Member != "u10" || r3.Type != 2 || r3.ModelName != "gpt" || r3.PromptTokens != 200 || r3.UseTime != 8 || r3.CostUSD != 2 || r3.IsStream {
		t.Fatalf("字段不对: %+v", r3)
	}
	// 费用仅消费(type=2)有值:充值 id7(type=1)quota 非0 但语义是金额,CostUSD 必须为 0(前端/CSV 留空)
	for _, r := range all {
		if r.ID == 7 && r.CostUSD != 0 {
			t.Fatalf("充值行费用应为 0(不当消费费用), 得 %v", r.CostUSD)
		}
	}
	// 模型筛选 claude:只 id2
	cl, _ := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 0, "claude", "", "", "", "", 0, 100)
	if len(cl) != 1 || cl[0].ID != 2 {
		t.Fatalf("模型筛选不对: %+v", cl)
	}
	// 分组筛选 vip:只 id5
	vg, _ := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 0, "", "vip", "", "", "", 0, 100)
	if len(vg) != 1 || vg[0].ID != 5 {
		t.Fatalf("分组筛选不对: %+v", vg)
	}
	// 计数:全部(含错误/退款)=7;消费=4;成员 uid11=id 2,5,7,8=4
	if n, err := m.countGroupLogs(context.Background(), ids, 0, 2000, 0, 0, "", "", "", "", ""); err != nil || n != 7 {
		t.Fatalf("总计数 = %d, %v; want 7", n, err)
	}
	if n, _ := m.countGroupLogs(context.Background(), ids, 0, 2000, 0, 2, "", "", "", "", ""); n != 4 {
		t.Fatalf("消费计数 = %d; want 4", n)
	}
	if n, _ := m.countGroupLogs(context.Background(), ids, 0, 2000, 11, 0, "", "", "", "", ""); n != 4 {
		t.Fatalf("成员计数 = %d; want 4", n)
	}
	// 游标分页(全部,含错误/退款):limit 2 → 8,7;再传 cursor=7 → 6,5
	p1, _ := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 0, "", "", "", "", "", 0, 2)
	if len(p1) != 2 || p1[0].ID != 8 || p1[1].ID != 7 {
		t.Fatalf("第一页不对: %+v", p1)
	}
	p2, _ := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 0, "", "", "", "", "", p1[1].ID, 2)
	if len(p2) != 2 || p2[0].ID != 6 || p2[1].ID != 5 {
		t.Fatalf("第二页不对: %+v", p2)
	}
	// 时间窗口 [0,1150):只 id 1,2
	win, _ := m.queryGroupLogs(context.Background(), ids, 0, 1150, 0, 0, "", "", "", "", "", 0, 100)
	if len(win) != 2 {
		t.Fatalf("时间窗口不对: %+v", win)
	}
	// 令牌搜索:通配符按字面匹配(%/_ 已转义),"%"搜不到任何行;正常子串仍可搜到
	if tw, _ := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 0, "", "", "%", "", "", 0, 100); len(tw) != 0 {
		t.Fatalf("通配符应按字面匹配,搜'%%'应 0 条,得 %d", len(tw))
	}
	if tw, _ := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 0, "", "", "kA", "", "", 0, 100); len(tw) != 3 {
		t.Fatalf("子串搜索 kA 应 3 条(tkA,含错误行 id6),得 %d", len(tw))
	}
	// 详情关键字搜索:普通词只匹配 content 字面;id6(错误类型,content="上游返回 429 限流")现在全局可见,搜"限流"应命中
	if dk, _ := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 0, "", "", "", "限流", "", 0, 100); len(dk) != 1 || dk[0].ID != 6 {
		t.Fatalf("详情关键字搜索'限流'应命中错误行 id6,得 %+v", dk)
	}
	if dk, _ := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 0, "", "", "", "充值", "", 0, 100); len(dk) != 1 || dk[0].ID != 7 {
		t.Fatalf("详情关键字搜索'充值'应命中 id7,得 %+v", dk)
	}
	// "违规费"额外命中 other.violation_fee_code(即使 content 里没有这几个字);id2 的 other 不含该标记,不应误中
	if _, err := m.prodDB.Exec("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_name,username,use_time,is_stream,content,other,request_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		9, 11, 1650, 2, "grok", 400000, 10, 10, "default", "tkB", "u11", 1, 0, "", `{"violation_fee_code":"grok-safety"}`, ""); err != nil {
		t.Fatal(err)
	}
	if dk, _ := m.queryGroupLogs(context.Background(), ids, 0, 2000, 0, 0, "", "", "", "违规费", "", 0, 100); len(dk) != 1 || dk[0].ID != 9 {
		t.Fatalf("详情关键字搜索'违规费'应只命中带 violation_fee_code 标记的 id9,得 %+v", dk)
	}
	// 详情摘要口径(对齐 new-api):消费按价/倍率,退款固定文案,其余回退 content
	byID := map[int64]LogRow{}
	for _, r := range all {
		byID[r.ID] = r
	}
	// 多行详情,对齐 new-api 线上:首行倍率,再 输入价、缓存读(model_ratio 2.5→$5;cache_ratio 0.1→$0.5)
	if d := byID[2].Detail; d != "分组倍率 1.4x\n输入 $5 / 1M tokens\n缓存读 $0.5 / 1M tokens" {
		t.Fatalf("id2 计价详情 = %q", d)
	}
	if d := byID[3].Detail; d != "分组倍率 1.2x" {
		t.Fatalf("id3 倍率详情 = %q", d)
	}
	if d := byID[7].Detail; d != "充值 $10" { // 充值 → content
		t.Fatalf("id7 充值详情 = %q", d)
	}
	if d := byID[8].Detail; d != "异步任务退款" { // 退款 → 固定文案
		t.Fatalf("id8 退款详情 = %q", d)
	}
	// 错误详情必须原样保留 content；不可按关键词归类/改写，否则会丢失上游排障证据。
	if d := byID[6].Detail; d != "上游返回 429 限流" {
		t.Fatalf("id6 错误详情 = %q", d)
	}
	if rid := byID[2].RequestID; rid != "req-abc" { // request_id 原样带出,供筛选/CSV
		t.Fatalf("id2 request_id = %q", rid)
	}
	if rid := byID[7].RequestID; rid != "" {
		t.Fatalf("id7 request_id 应为空, 得 %q", rid)
	}
	// 渠道零泄露:id2/id6 的 other 里有 channel_name,任何字段都不该带出
	for _, r := range all {
		blob := r.Detail + "|" + r.TokenName + "|" + r.ModelName + "|" + r.Group
		if strings.Contains(blob, "secret-up") || strings.Contains(blob, "channel") {
			t.Fatalf("渠道泄露: %+v", r)
		}
	}
}

// 导出限流:每组织账号窗口内 1 次;reserve 原子预占(并发只有一个能过),rollback 撤销(探测/失败不计次)。
func TestExportLimiter(t *testing.T) {
	l := &exportLimiter{last: map[int64]int64{}}
	now := int64(100000)
	win := int64(300) // 5min
	prev, ok := l.reserve(1, now, win)
	if !ok {
		t.Fatal("首次应预占成功")
	}
	// 并发第二个请求(占位未释放)必须被挡——check-then-act 竞态的回归防线
	if _, ok2 := l.reserve(1, now+1, win); ok2 {
		t.Fatal("预占期间并发请求应被拒")
	}
	// 探测/失败:回退后立刻可再占(不计次)
	l.rollback(1, prev, now)
	if _, ok3 := l.reserve(1, now+2, win); !ok3 {
		t.Fatal("回退后应放行")
	}
	// 本次视为成功下载(不回退):窗口内拒绝、满窗放行
	if _, bad := l.reserve(1, now+2+win-1, win); bad {
		t.Fatal("窗口内应拒绝")
	}
	if _, ok4 := l.reserve(1, now+2+win, win); !ok4 {
		t.Fatal("满窗应放行")
	}
	// 迟到的 rollback(占位已被新预占覆盖)不得误撤别人的占位
	l.rollback(1, prev, now) // reservedAt=now 已不是当前占位
	if _, bad := l.reserve(1, now+2+win+1, win); bad {
		t.Fatal("误撤保护失败:新占位被旧 rollback 清掉了")
	}
	if _, ok5 := l.reserve(2, now+10, win); !ok5 {
		t.Fatal("别的组织不受影响")
	}
	// prune 清理过期
	l.prune(now+9000, win)
	if len(l.last) != 0 {
		t.Fatalf("prune 应清空过期: %d", len(l.last))
	}
}

// 详情文案各分支 + 内部信息剔除(纵深防御)+ 阶梯计费不显错误单价。
func TestBuildLogDetail(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	cases := []struct {
		name    string
		logType int
		o       *logOther
		content string
		want    string
	}{
		{"退款固定文案", 6, nil, "", "异步任务退款"},
		{"充值回退content", 1, nil, "充值 $10.00", "充值 $10.00"},
		{"错误原文直出", 5, nil, "status_code=524, origin timeout", "status_code=524, origin timeout"},
		{"消费标准价+缓存读", 2, &logOther{GroupRatio: f(1.4), ModelRatio: f(2.5), CacheTokens: 100, CacheRatio: f(0.1)},
			"", "分组倍率 1.4x\n输入 $5 / 1M tokens\n缓存读 $0.5 / 1M tokens"},
		{"按次计费", 2, &logOther{ModelPrice: f(0.03)}, "", "模型价格 $0.03"},
		{"专属倍率优先", 2, &logOther{UserGroupRatio: f(0.8), GroupRatio: f(1.4), ModelRatio: f(1)}, "",
			"专属倍率 0.8x\n输入 $2 / 1M tokens"},
		// 阶梯计费:model_ratio/price 为0,绝不能显 "$0/1M",回退 content
		{"阶梯计费回退content", 2, &logOther{BillingMode: "tiered_expr", ModelRatio: f(0)}, "阶梯: 见计费表", "阶梯: 见计费表"},
		{"阶梯计费无content标注", 2, &logOther{BillingMode: "tiered_expr", GroupRatio: f(1.2)}, "", "阶梯计费 · 分组倍率 1.2x"},
		// 纵深防御:含"渠道"的系统日志 content 一律隐去(如管理员账号误入客户组)
		{"系统日志渠道信息剔除", 4, nil, "查看渠道密钥信息 (渠道ID: 5)", ""},
		{"管理日志正常保留", 3, nil, "管理员增加用户额度 $50", "管理员增加用户额度 $50"},
	}
	for _, c := range cases {
		if got := buildLogDetail(c.logType, c.o, c.content); got != c.want {
			t.Errorf("%s: buildLogDetail = %q, want %q", c.name, got, c.want)
		}
	}
}
