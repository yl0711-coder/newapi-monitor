package monitor

// usage_test.go:「用户用量」单元/集成测试。
// 名单 CRUD 走真 sqlite 本地库;聚合链路用 sqlite 假生产库端到端验证(建 logs/users 表塞已知行),
// 仅日桶表达式按方言覆盖(MySQL DIV → sqlite 整除 /),SQL 其余部分两边通用。

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"gorm.io/gorm" // 注册 database/sql 驱动 "sqlite"(纯 Go,免 cgo)
)

const usageDayExprSQLite = "(created_at + 28800) / 86400" // sqlite 整型相除即整除

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
		"CREATE TABLE logs (id INTEGER PRIMARY KEY, user_id INTEGER, created_at INTEGER, type INTEGER, model_name TEXT, quota INTEGER, prompt_tokens INTEGER, completion_tokens INTEGER, `group` TEXT, token_id INTEGER DEFAULT 0, token_name TEXT DEFAULT '')",
		"CREATE TABLE tokens (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT, `key` TEXT, `group` TEXT)",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("create schema: %v", err)
		}
	}
	return db
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
	st, err := m.computeUsageStats(context.Background(), []int64{1, 2}, fromTs, toTs)
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
	if empty, err := m.computeUsageStats(context.Background(), nil, fromTs, toTs); err != nil || len(empty.Daily) != 0 {
		t.Fatalf("空名单 = %+v, %v", empty, err)
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

func TestParseUsageRangeBoundary(t *testing.T) {
	now := time.Date(2026, 7, 7, 15, 0, 0, 0, usageCST)
	// 含两端点恰 90 天:2026-01-01 + 89 天 = 2026-03-31 → 应通过
	if _, _, err := parseUsageRange("2026-01-01", "2026-03-31", now); err != nil {
		t.Fatalf("恰 90 天应通过: %v", err)
	}
	// 91 天 → 应拒绝(差值恰 90*24h,>= 判定)
	if _, _, err := parseUsageRange("2026-01-01", "2026-04-01", now); err == nil {
		t.Fatal("91 天应被拒绝")
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
	// 余额顺路取回:alice $2、bob $0.5;已删用户(9)不在表中 → 前端显 —
	if balances[1] != 2 || balances[2] != 0.5 {
		t.Fatalf("余额 = %+v", balances)
	}
	if _, ok := balances[9]; ok {
		t.Fatal("已删用户不应有余额")
	}
	// 累计总消耗顺路取回:alice $3、bob $0;已删用户(9)不在表中 → 前端显 —
	if used[1] != 3 || used[2] != 0 {
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
}

// 按令牌聚合:key 脱敏(带 sk- 前缀、只留首尾);令牌已删则回退日志名、key 空;按费用降序。
func TestComputeUserTokenUsage(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	if _, err := m.prodDB.Exec("INSERT INTO users (id,username,email) VALUES (5,'fiveuser','five@x.com')"); err != nil {
		t.Fatal(err)
	}
	// token 10 = 现存令牌(分组 claude-1.6x);token 20 = 已删除(logs 有记录但 tokens 表无);均属 user 5
	if _, err := m.prodDB.Exec("INSERT INTO tokens (id,user_id,name,`key`,`group`) VALUES (10,5,'生产key','abcd1234567890wxyz','claude-1.6x')"); err != nil {
		t.Fatal(err)
	}
	seed := [][]any{
		// user_id, created_at, type, model, quota, pt, ct, group, token_id, token_name
		{5, 1000, 2, "gpt", 500000, 10, 10, "default", 10, "生产key"}, // $1.0
		{5, 1100, 2, "gpt", 500000, 10, 10, "default", 10, "生产key"}, // 再 $1.0 → token10 合计 $2
		{5, 1200, 2, "gpt", 2500000, 5, 5, "default", 20, "旧key"},   // $5.0,令牌已删
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
	if len(out) != 2 {
		t.Fatalf("应有 2 个令牌, 实得 %d: %+v", len(out), out)
	}
	// 降序:已删令牌 $5 在前
	if out[0].CostUSD != 5 || out[1].CostUSD != 2 {
		t.Fatalf("费用降序不对: %+v", out)
	}
	// 已删令牌:回退日志名,key 空(前端显示 —),分组空(前端显示"默认")
	if out[0].Name != "旧key" || out[0].MaskedKey != "" || out[0].Group != "" {
		t.Fatalf("已删令牌回退不对: %+v", out[0])
	}
	// 现存令牌:名称/分组来自 tokens 表,key 脱敏且不含完整明文
	if out[1].Name != "生产key" || out[1].Group != "claude-1.6x" {
		t.Fatalf("现存令牌名/分组不对: %+v", out[1])
	}
	// 所属用户:两行都标 user 5 的展示名(username=fiveuser)
	if out[0].Owner != "fiveuser" || out[1].Owner != "fiveuser" {
		t.Fatalf("所属用户标注不对: %q / %q", out[0].Owner, out[1].Owner)
	}
	mk := out[1].MaskedKey
	if !strings.HasPrefix(mk, "sk-abcd") || !strings.HasSuffix(mk, "wxyz") || strings.Contains(mk, "567890") {
		t.Fatalf("脱敏 key 不合规(泄露或格式错): %q", mk)
	}
	if out[1].Requests != 2 || out[1].Tokens != 40 {
		t.Fatalf("现存令牌请求/tokens 不对: %+v", out[1])
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
