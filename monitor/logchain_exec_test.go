package monitor

// logchain_exec_test.go：排障链路的**真执行**测试。
//
// 与 logchain_test.go 的分工（这个区别很重要，别把两边混着写）：
//   logchain_test.go       字符串断言——SQL 长什么样、参数化了没、冲突拒不拒
//   logchain_exec_test.go  真跑 SQL——塞已知数据进假生产库，断言"筛出来的是哪几行"
//
// 为什么必须有这一份：异常判据写了两遍（SQL 在库里筛、Go 侧给捞回的行打标签），
// 两份实现表达同一套口径。字符串断言只能各自验"自己长得对"，**验不了两者是否一致**。
// 曾经的 ORDER BY id DESC 就是通过了全部字符串断言、靠真数据才露馅的
// （id 序 ≠ 发生时间序，因为 new-api 在请求完成时才写日志）。
//
// 假生产源用 SQLite（newFakeProdDB），因此 logchain 的 SQL 必须是 MySQL/SQLite 通用写法。
// 这也是把 model_name NOT REGEXP 改成 LOWER(...) NOT LIKE 链的原因：只有一份 SQL，
// 测的就是生产跑的那一句，不存在"翻译过的版本测过了、生产那句没测"。
//
// **本层管不了什么**（连真库时要重点看这些，SQLite 结构上验不出来）：
//   - MySQL 方言与 collation 差异、MAX_EXECUTION_TIME hint 是否真生效（SQLite 当注释吃掉）
//   - 真实数据形态：other 里 NULL 的分布、没见过的 end_reason 取值、错误原文真实格式
//   - 真实数据量下的查询耗时与索引命中

import (
	"context"
	"testing"
)

// logChainSeedRow 一条待塞入假生产库的 logs 行。字段名与 logs 表列名对齐，
// 只列测试关心的；其余走 DDL 默认值。
type logChainSeedRow struct {
	ID               int64
	CreatedAt        int64
	Type             int
	UserID           int64
	Username         string
	Group            string
	TokenName        string
	ChannelID        int64
	ModelName        string
	Quota            int64
	PromptTokens     int64
	CompletionTokens int64
	UseTime          int64
	IsStream         int
	Content          string
	Other            string
	RequestID        string
}

// newLogChainExecMonitor 造一个能真跑排障查询的 Monitor：本地 store + SQLite 假生产库。
func newLogChainExecMonitor(t *testing.T, rows []logChainSeedRow) *Monitor {
	t.Helper()
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	for _, r := range rows {
		if _, err := m.prodDB.Exec(
			"INSERT INTO logs (id,user_id,channel_id,created_at,type,model_name,quota,"+
				"prompt_tokens,completion_tokens,`group`,token_name,username,use_time,"+
				"is_stream,content,other,request_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			r.ID, r.UserID, r.ChannelID, r.CreatedAt, r.Type, r.ModelName, r.Quota,
			r.PromptTokens, r.CompletionTokens, r.Group, r.TokenName, r.Username,
			r.UseTime, r.IsStream, r.Content, r.Other, r.RequestID); err != nil {
			t.Fatalf("塞入 logs 行 id=%d 失败: %v", r.ID, err)
		}
	}
	return m
}

// TestLogChainQueryExecutesOnFakeSource 冒烟：整条查询必须能在假生产源上真的跑起来。
// 这是本文件其余测试的前提——SQL 里任何 MySQL 专有语法（如 NOT REGEXP）都会在这里报错，
// 而不是留到连真库时才发现"这批判据从来没被执行过"。
func TestLogChainQueryExecutesOnFakeSource(t *testing.T) {
	m := newLogChainExecMonitor(t, []logChainSeedRow{
		{ID: 1, CreatedAt: 1000, Type: 5, UserID: 7, Username: "alice", ChannelID: 3,
			ModelName: "gpt-4o", Content: "渠道 relay-a (#3) 返回错误：status_code=429"},
		{ID: 2, CreatedAt: 1100, Type: 2, UserID: 7, Username: "alice", ChannelID: 3,
			ModelName: "gpt-4o", Quota: 500, PromptTokens: 10, CompletionTokens: 20,
			Other: `{"stream_status":{"end_reason":"eof"}}`},
	})

	scope := logChainScope{FromTs: 0, ToTs: 9999, Limit: 50}
	rows, hasMore, err := m.queryLogChain(context.Background(), scope, nil)
	if err != nil {
		t.Fatalf("查询在假生产源上执行失败（SQL 可能含 MySQL 专有语法）: %v", err)
	}
	if hasMore {
		t.Error("两行数据不该报 has_more")
	}
	if len(rows) != 2 {
		t.Fatalf("应返回 2 行，got=%d", len(rows))
	}
	// 默认倒序：最新在上。
	if rows[0].ID != 2 || rows[1].ID != 1 {
		t.Errorf("默认应按时间倒序: got=[%d %d]", rows[0].ID, rows[1].ID)
	}
	// content 必须原样返回，不过 scrubContent（那个函数见"渠道"二字就整段清空）。
	if rows[1].Content == "" {
		t.Error("含“渠道”的上游错误原文被清空了：content 不得过 scrubContent")
	}
}

// logChainAnomalyFixture 覆盖全部异常判据分支的数据集。每行注释写明"期望被判成什么"，
// 便于后续改判据时直接看出哪一类的预期变了。
//
// 时间戳递增且互不相同：排序与游标测试要靠它区分行，同秒多条另有专门用例。
func logChainAnomalyFixture() []logChainSeedRow {
	// 带 request_path：生产实测每条 type=2 都有该字段（近 5 天 197371 行填充率 100%），
	// 而「扣费未交付」以端点白名单为主判据（RB-02），fixture 不带路径就测不到那条判据。
	const eof = `{"stream_status":{"end_reason":"eof"},"request_path":"/v1/chat/completions"}`
	return []logChainSeedRow{
		// —— 正常，不该有任何标签 ——
		{ID: 1, CreatedAt: 1001, Type: 2, UserID: 7, Username: "alice", ChannelID: 3,
			ModelName: "gpt-4o", Quota: 500, CompletionTokens: 20, Other: eof},
		// 非流式请求：other 里根本没有 stream_status，取不到值。这类不得被判成流异常
		// （排除法必须同时排除空串，只排 eof 会让全部非流式请求变成"异常"）。
		{ID: 2, CreatedAt: 1002, Type: 2, UserID: 7, Username: "alice", ChannelID: 3,
			ModelName: "gpt-4o", Quota: 500, CompletionTokens: 20, Other: `{}`},

		// done 也是正常结束，不得判为异常。
		// 2026-08-21 生产实测：20 条 done 全部真交付（平均 741 输出 token、31 秒）。
		// 此前被误判成"流未正常结束"，是本仓库唯一靠真实数据才发现的误报。
		{ID: 13, CreatedAt: 1013, Type: 2, UserID: 7, Username: "alice", ChannelID: 3,
			ModelName: "gpt-4o", Quota: 500, CompletionTokens: 741, UseTime: 31,
			Other: `{"stream_status":{"end_reason":"done"}}`},

		// —— 流异常 ——
		{ID: 3, CreatedAt: 1003, Type: 2, UserID: 7, Username: "alice", ChannelID: 3,
			ModelName: "gpt-4o", Quota: 500, CompletionTokens: 5, UseTime: 47,
			Other: `{"stream_status":{"end_reason":"client_gone"}}`},
		// 没见过的新取值必须也算异常——排除法的全部意义就在这里。
		// new-api 升级新增 end_reason 时，枚举写法会假装它不存在。
		{ID: 4, CreatedAt: 1004, Type: 2, UserID: 7, Username: "alice", ChannelID: 3,
			ModelName: "gpt-4o", Quota: 500, CompletionTokens: 5,
			Other: `{"stream_status":{"end_reason":"brand_new_reason_v2"}}`},
		// 正常结束但流内出过错：error_count 那一半判据。
		{ID: 5, CreatedAt: 1005, Type: 2, UserID: 7, Username: "alice", ChannelID: 3,
			ModelName: "gpt-4o", Quota: 500, CompletionTokens: 5,
			Other: `{"stream_status":{"end_reason":"eof","error_count":2}}`},

		// —— 消费异常：扣费未交付（客户亏）——
		{ID: 6, CreatedAt: 1006, Type: 2, UserID: 8, Username: "bob", ChannelID: 4,
			ModelName: "gpt-4o", Quota: 300, CompletionTokens: 0, Other: eof},
		// —— 消费异常：交付未扣费（我方亏）——
		{ID: 7, CreatedAt: 1007, Type: 2, UserID: 8, Username: "bob", ChannelID: 4,
			ModelName: "gpt-4o", Quota: 0, CompletionTokens: 12, Other: eof},

		// —— 必须排除的：订阅计费 quota 恒为 0 属正常 ——
		// 不排除会把所有订阅客户的请求整批误报成漏计费。
		{ID: 8, CreatedAt: 1008, Type: 2, UserID: 9, Username: "carol", ChannelID: 4,
			ModelName: "gpt-4o", Quota: 0, CompletionTokens: 12,
			Other: `{"stream_status":{"end_reason":"eof"},"billing_source":"subscription"}`},
		// —— 必须排除的：天然无输出模型 ——
		// 不排除的话每条 embedding 请求都会被判成"扣费未交付"，整类误报。
		{ID: 9, CreatedAt: 1009, Type: 2, UserID: 9, Username: "carol", ChannelID: 4,
			ModelName: "text-embedding-3-small", Quota: 200, CompletionTokens: 0, Other: eof},
		// 大写模型名同样要被排除：SQL 侧靠 LOWER()，不能依赖库的 collation 恰好是 _ci。
		{ID: 10, CreatedAt: 1010, Type: 2, UserID: 9, Username: "carol", ChannelID: 4,
			ModelName: "DALL-E-3-IMAGE", Quota: 200, CompletionTokens: 0, Other: eof},

		// —— 错误行：不打异常标签（异常判据都限定 type=2），但 err_anom 要能捞到 ——
		{ID: 11, CreatedAt: 1011, Type: 5, UserID: 7, Username: "alice", ChannelID: 3,
			ModelName: "gpt-4o", Content: "渠道 relay-a (#3) 返回错误：status_code=429 rate_limit_error"},

		// —— 一行同时命中两类：client_gone 且扣费未交付 ——
		{ID: 12, CreatedAt: 1012, Type: 2, UserID: 8, Username: "bob", ChannelID: 4,
			ModelName: "gpt-4o", Quota: 400, CompletionTokens: 0, UseTime: 61,
			Other: `{"stream_status":{"end_reason":"client_gone"},"request_path":"/v1/chat/completions"}`},
	}
}

// anomalyKindAcceptsTags 把 anomaly 取值翻译成"这一类应当命中哪些标签"。
// 这是交叉验证的判定函数：SQL 说某行该出现，标签侧就必须认它；反之亦然。
func anomalyKindAcceptsTags(kind string, row LogChainRow) bool {
	has := func(want string) bool {
		for _, tag := range row.AnomalyTags {
			if tag == want {
				return true
			}
		}
		return false
	}
	switch kind {
	case anomalyStream:
		return has("stream")
	case anomalyClientGone:
		return has(logChainClientGoneEndReason)
	case anomalyBillingUnpaid:
		return has("billing_unpaid")
	case anomalyBillingFree:
		return has("billing_free")
	case anomalyBilling:
		return has("billing_unpaid") || has("billing_free")
	case anomalyAll:
		return len(row.AnomalyTags) > 0
	case anomalyErrAnom:
		// 唯一跨 type 的取值：错误(type=5)本身不打异常标签，但属于"本页能查到的问题"。
		return len(row.AnomalyTags) > 0 || row.Type == 5
	}
	return false
}

// TestLogChainAnomalySQLMatchesTagsOnRealRows **本文件最核心的测试**。
//
// 异常判据写了两遍且必须写两遍：SQL 在库里筛（不能把全表捞回来再过滤），
// Go 侧给已捞回的行打标签。两份实现表达同一套口径，一旦漂移就会出现
// "筛出来了但没标签"或"有标签却筛不出来"这种自相矛盾的结果。
//
// 字符串断言在结构上做不到这个验证——它只能各自检查"自己长得对"。
// 这里的做法是：先不带筛选把全部行捞回来（标签由后端逐行算好），
// 再对每种 anomaly 取值跑一次真查询，断言两侧给出的行集合**完全一致**。
func TestLogChainAnomalySQLMatchesTagsOnRealRows(t *testing.T) {
	m := newLogChainExecMonitor(t, logChainAnomalyFixture())
	ctx := context.Background()
	all := logChainScope{FromTs: 0, ToTs: 9999, Limit: 200}

	// 基准：全部行（默认 type IN (2,5)）及其标签。
	baseRows, _, err := m.queryLogChain(ctx, all, nil)
	if err != nil {
		t.Fatalf("基准查询失败: %v", err)
	}
	if len(baseRows) != 13 {
		t.Fatalf("fixture 应有 13 行落在默认 type IN (2,5) 内，got=%d", len(baseRows))
	}

	for _, kind := range []string{
		anomalyStream, anomalyClientGone, anomalyBillingUnpaid, anomalyBillingFree,
		anomalyBilling, anomalyAll, anomalyErrAnom,
	} {
		t.Run(kind, func(t *testing.T) {
			scope := all
			scope.Anomaly = kind
			got, _, err := m.queryLogChain(ctx, scope, nil)
			if err != nil {
				t.Fatalf("anomaly=%s 查询失败: %v", kind, err)
			}
			sqlPicked := make(map[int64]bool, len(got))
			for _, r := range got {
				sqlPicked[r.ID] = true
			}

			for _, r := range baseRows {
				tagSaysYes := anomalyKindAcceptsTags(kind, r)
				switch {
				case sqlPicked[r.ID] && !tagSaysYes:
					// 最危险的一种：页面上有这一行，却没有任何标签说明它为什么在这里。
					t.Errorf("id=%d 被 SQL 筛出但标签侧不认（会出现“筛出来了但没标签”）: type=%d tags=%v model=%q",
						r.ID, r.Type, r.AnomalyTags, r.ModelName)
				case !sqlPicked[r.ID] && tagSaysYes:
					// 反向：标签侧认它是这类异常，SQL 却没捞它 —— 漏报。
					t.Errorf("id=%d 标签侧判为 %s 但 SQL 没筛出（漏报）: type=%d tags=%v model=%q",
						r.ID, kind, r.Type, r.AnomalyTags, r.ModelName)
				}
			}
		})
	}
}

// TestLogChainAnomalyExclusionsHoldOnRealRows 三类必须排除的行，逐条钉住它们**不出现**。
//
// 与上面那个交叉验证的区别：那个保证"两侧一致"，这个保证"一致在正确的位置上"。
// 两侧同时判错时交叉验证是绿的（都说不是异常/都说是），所以必须另外断言期望值。
func TestLogChainAnomalyExclusionsHoldOnRealRows(t *testing.T) {
	m := newLogChainExecMonitor(t, logChainAnomalyFixture())
	ctx := context.Background()

	pick := func(t *testing.T, kind string) map[int64]bool {
		t.Helper()
		scope := logChainScope{FromTs: 0, ToTs: 9999, Limit: 200, Anomaly: kind}
		rows, _, err := m.queryLogChain(ctx, scope, nil)
		if err != nil {
			t.Fatalf("anomaly=%s 查询失败: %v", kind, err)
		}
		got := make(map[int64]bool, len(rows))
		for _, r := range rows {
			got[r.ID] = true
		}
		return got
	}

	t.Run("流故障用排除法_未见过的取值也要捞到", func(t *testing.T) {
		got := pick(t, anomalyStream)
		// id=4 未知取值、id=5 流内有错误计数 → 都是流故障。
		for _, want := range []int64{4, 5} {
			if !got[want] {
				t.Errorf("id=%d 应判为流故障但没捞到", want)
			}
		}
		// id=4 是 brand_new_reason_v2：枚举法会把它吞掉，排除法必须捞到。
		// **拆出 client_gone 后这条性质必须仍然成立**——排除法不能被削弱。
		if !got[4] {
			t.Error("未见过的 end_reason 被静默吞掉了——退回枚举法了？排障最怕这个")
		}
		// client_gone 已独立成档，不再算流故障（id=3 纯断连、id=12 断连+扣费未交付）。
		for _, notWant := range []int64{3, 12} {
			if got[notWant] {
				t.Errorf("id=%d 是 client_gone，已独立成档，不该出现在流故障里——"+
					"混在一起会让真故障被大量客户断连淹掉（实测 1594:25）", notWant)
			}
		}
		// 非流式(id=2)、eof(id=1)、done(id=13) 都是正常结束，不得入选。
		for _, notWant := range []int64{1, 2, 13} {
			if got[notWant] {
				t.Errorf("id=%d 不该判为流故障（1=eof，2=非流式无 stream_status，13=done）", notWant)
			}
		}
		// done 单独再断言一次：它是靠生产真实数据才发现的误报，值得防回退。
		if got[13] {
			t.Error("id=13 的 end_reason=done 是正常结束（生产实测 20/20 真交付），" +
				"不得判为流故障——这个误报只能靠真数据发现，回退了就再难察觉")
		}
	})

	t.Run("客户断连独立成档", func(t *testing.T) {
		got := pick(t, anomalyClientGone)
		// id=3 纯断连、id=12 断连且扣费未交付 → 都该在这一档。
		for _, want := range []int64{3, 12} {
			if !got[want] {
				t.Errorf("id=%d 是 client_gone，应出现在该档", want)
			}
		}
		// 真流故障不该混进来。
		for _, notWant := range []int64{4, 5} {
			if got[notWant] {
				t.Errorf("id=%d 是流故障（4=未知取值，5=流内错误计数），不该出现在客户断连档", notWant)
			}
		}
		// 正常结束更不该进来。
		for _, notWant := range []int64{1, 2, 13} {
			if got[notWant] {
				t.Errorf("id=%d 是正常结束，不该出现在客户断连档", notWant)
			}
		}
	})

	t.Run("全部异常仍须含客户断连", func(t *testing.T) {
		got := pick(t, anomalyAll)
		// 拆档后 all 若漏掉 client_gone，"全部异常"就名不副实。
		for _, want := range []int64{3, 4, 5, 12} {
			if !got[want] {
				t.Errorf("id=%d 应出现在「全部异常」里（拆档后 all 必须是三档并集）", want)
			}
		}
	})

	t.Run("订阅计费不算漏收", func(t *testing.T) {
		if pick(t, anomalyBillingFree)[8] {
			t.Error("id=8 是订阅计费(quota 恒为 0 属正常)，不得判为交付未扣费——" +
				"不排除会把所有订阅客户整批误报")
		}
		if !pick(t, anomalyBillingFree)[7] {
			t.Error("id=7 是真的交付未扣费，应被筛出")
		}
	})

	t.Run("天然无输出模型不算扣费未交付", func(t *testing.T) {
		got := pick(t, anomalyBillingUnpaid)
		if got[9] {
			t.Error("id=9 是 embedding，completion_tokens=0 属正常，不得判为扣费未交付")
		}
		// 大写模型名：SQL 侧靠 LOWER()，若依赖 collation 会在这里露馅。
		if got[10] {
			t.Error("id=10 模型名是大写的 DALL-E-3-IMAGE，同样该被排除——" +
				"SQL 侧不得依赖库的 collation 恰好大小写不敏感")
		}
		if !got[6] || !got[12] {
			t.Error("id=6/12 是真的扣费未交付，应被筛出")
		}
	})

	t.Run("错误行不打异常标签但 err_anom 要捞到", func(t *testing.T) {
		if pick(t, anomalyAll)[11] {
			t.Error("id=11 是 type=5，异常判据都限定 type=2，不该出现在 anomaly=all")
		}
		if !pick(t, anomalyErrAnom)[11] {
			t.Error("id=11 应出现在 err_anom（错误+异常）")
		}
	})

	t.Run("一行可同时命中两类", func(t *testing.T) {
		// id=12 是 client_gone 且扣费未交付：分别属「客户断连」与「扣费未交付」两档。
		// 拆档后它不再属于「流故障」——那一档只放真的传输故障。
		if !pick(t, anomalyClientGone)[12] || !pick(t, anomalyBillingUnpaid)[12] {
			t.Error("id=12 是 client_gone 且扣费未交付，这两档都该命中")
		}
	})
}

// TestLogChainOrdersByOccurrenceTimeOnRealRows 排序必须按发生时间，不按 id。
//
// **这个 bug 当初是 fixture 实测才抓出来的，读代码发现不了**：原实现写 ORDER BY id DESC，
// 注释还断言"id 近似时间序"。根因是 new-api 在请求**完成时**才写日志——一个耗时 60 秒的
// 超时请求，会比后发起、快速失败的请求更晚拿到 id。所以 id 序 ≠ 发生时间序，生产上同样会乱。
//
// fixture 特意让 id 序与时间序**完全相反**：id 越大发生时间越早。
// 若有人改回按 id 排，这里立刻红。
func TestLogChainOrdersByOccurrenceTimeOnRealRows(t *testing.T) {
	m := newLogChainExecMonitor(t, []logChainSeedRow{
		// id=1 是那个耗时 60s 的慢请求：先发起、最晚完成，所以 id 最小但时间最晚。
		{ID: 1, CreatedAt: 5000, Type: 5, ModelName: "gpt-4o", UseTime: 60, Content: "timeout"},
		{ID: 2, CreatedAt: 3000, Type: 5, ModelName: "gpt-4o", Content: "err-b"},
		{ID: 3, CreatedAt: 1000, Type: 5, ModelName: "gpt-4o", Content: "err-c"},
	})
	ctx := context.Background()

	desc, _, err := m.queryLogChain(ctx, logChainScope{FromTs: 0, ToTs: 9999, Limit: 50}, nil)
	if err != nil {
		t.Fatalf("倒序查询失败: %v", err)
	}
	wantDesc := []int64{1, 2, 3} // 时间 5000 → 3000 → 1000
	for i, want := range wantDesc {
		if desc[i].ID != want {
			t.Fatalf("倒序应按发生时间(最新在上)=%v，got=%v —— 退回按 id 排了？", wantDesc, logChainIDsOf(desc))
		}
	}
	// 若按 id DESC 排，结果会是 3,2,1（时间 1000→3000→5000，完全反了）。
	if desc[0].CreatedAt < desc[len(desc)-1].CreatedAt {
		t.Errorf("倒序结果的时间在变大，说明没按 created_at 排: %v", createdAtsOf(desc))
	}

	asc, _, err := m.queryLogChain(ctx, logChainScope{FromTs: 0, ToTs: 9999, Limit: 50, Asc: true}, nil)
	if err != nil {
		t.Fatalf("正序查询失败: %v", err)
	}
	wantAsc := []int64{3, 2, 1}
	for i, want := range wantAsc {
		if asc[i].ID != want {
			t.Fatalf("正序应按发生时间(最早在上)=%v，got=%v", wantAsc, logChainIDsOf(asc))
		}
	}
}

func logChainIDsOf(rows []LogChainRow) []int64 {
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.ID)
	}
	return out
}

func createdAtsOf(rows []LogChainRow) []int64 {
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.CreatedAt)
	}
	return out
}

// TestLogChainCursorPagesWithoutGapOrDuplicateOnRealRows 翻页必须不漏不重，两个方向都要。
//
// 复合游标 (created_at, id) 是这个功能里最容易错的一段，而字符串断言只能验
// "WHERE 里出现了 < 或 >"。**翻页行为对不对，只有真跑才知道。**
//
// 已知的两个坑都在这里钉住：
//   - 比较方向写死：会让"加载更多"在正序下往回翻、重复吐出已看过的行，
//     **首页看不出来，只在翻第二页时才暴露**
//   - 同秒多条不用 id 破平：翻页会漏行或重复
func TestLogChainCursorPagesWithoutGapOrDuplicateOnRealRows(t *testing.T) {
	// 25 行，其中特意让若干行落在**同一秒**：同秒多条是复合游标 id 破平的用途所在。
	rows := make([]logChainSeedRow, 0, 25)
	for i := 1; i <= 25; i++ {
		rows = append(rows, logChainSeedRow{
			ID: int64(i), CreatedAt: int64(1000 + i/3), // 每 3 行共享一个秒
			Type: 5, UserID: 7, ModelName: "gpt-4o", Content: "err",
		})
	}
	m := newLogChainExecMonitor(t, rows)
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		asc  bool
	}{{"倒序", false}, {"正序", true}} {
		t.Run(tc.name, func(t *testing.T) {
			seen := map[int64]int{}
			var order []int64
			scope := logChainScope{FromTs: 0, ToTs: 9999, Limit: 7, Asc: tc.asc}
			// 25 行 / 每页 7 → 4 页。多留几轮上限防死循环。
			for page := 1; page <= 10; page++ {
				got, hasMore, err := m.queryLogChain(ctx, scope, nil)
				if err != nil {
					t.Fatalf("第 %d 页查询失败: %v", page, err)
				}
				if len(got) == 0 {
					t.Fatalf("第 %d 页返回空，但上一页说还有更多", page)
				}
				for _, r := range got {
					seen[r.ID]++
					order = append(order, r.ID)
				}
				if !hasMore {
					break
				}
				// 按接口约定推进游标：取本页最后一行的 (created_at, id)。
				last := got[len(got)-1]
				scope.BeforeTs, scope.BeforeID = last.CreatedAt, last.ID
			}

			// 不重
			for id, n := range seen {
				if n > 1 {
					t.Errorf("id=%d 出现 %d 次（翻页重复吐出已看过的行）", id, n)
				}
			}
			// 不漏
			if len(seen) != 25 {
				t.Errorf("翻完应覆盖全部 25 行，实际 %d 行；顺序=%v", len(seen), order)
			}
			// 全程单调：跨页拼接后时间序不得回头。
			for i := 1; i < len(order); i++ {
				prev, cur := findRow(rows, order[i-1]), findRow(rows, order[i])
				if tc.asc && (cur.CreatedAt < prev.CreatedAt) {
					t.Errorf("正序翻页出现时间回退: id=%d(ts=%d) 在 id=%d(ts=%d) 之后",
						cur.ID, cur.CreatedAt, prev.ID, prev.CreatedAt)
				}
				if !tc.asc && (cur.CreatedAt > prev.CreatedAt) {
					t.Errorf("倒序翻页出现时间前进: id=%d(ts=%d) 在 id=%d(ts=%d) 之后",
						cur.ID, cur.CreatedAt, prev.ID, prev.CreatedAt)
				}
			}
		})
	}
}

func findRow(rows []logChainSeedRow, id int64) logChainSeedRow {
	for _, r := range rows {
		if r.ID == id {
			return r
		}
	}
	return logChainSeedRow{}
}

// TestLogChainFiltersSelectExpectedRowsOnRealRows 各筛选条件真跑一遍，断言"筛出来的正是哪几行"。
//
// 字符串断言只能验"WHERE 里有 user_id = ?"，验不出条件是否真的生效
// （比如参数顺序错位、LIKE 转义写坏，都能通过字符串断言）。
func TestLogChainFiltersSelectExpectedRowsOnRealRows(t *testing.T) {
	m := newLogChainExecMonitor(t, []logChainSeedRow{
		{ID: 1, CreatedAt: 1001, Type: 5, UserID: 7, Username: "alice", Group: "vip",
			TokenName: "alice-prod", ChannelID: 3, ModelName: "gpt-4o", Content: "err-a", RequestID: "req-aaa"},
		{ID: 2, CreatedAt: 1002, Type: 5, UserID: 8, Username: "bob", Group: "default",
			TokenName: "bob-test", ChannelID: 4, ModelName: "claude-sonnet-4", Content: "err-b", RequestID: "req-bbb"},
		{ID: 3, CreatedAt: 1003, Type: 5, UserID: 7, Username: "alice", Group: "vip",
			TokenName: "alice-dev", ChannelID: 4, ModelName: "gpt-4o", Content: "err-c", RequestID: "req-ccc"},
	})
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		scope logChainScope
		want  []int64
	}{
		{"按客户 ID", logChainScope{UserID: 7}, []int64{3, 1}},
		{"按渠道", logChainScope{ChannelID: 4}, []int64{3, 2}},
		{"按模型(精确)", logChainScope{Model: "gpt-4o"}, []int64{3, 1}},
		{"按分组", logChainScope{Group: "vip"}, []int64{3, 1}},
		{"按令牌名(模糊)", logChainScope{TokenName: "alice"}, []int64{3, 1}},
		{"按请求 ID(精确)", logChainScope{RequestID: "req-bbb"}, []int64{2}},
		{"按错误原文关键词", logChainScope{Keyword: "err-c"}, []int64{3}},
		{"只看错误", logChainScope{ErrorOnly: true}, []int64{3, 2, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope := tc.scope
			scope.FromTs, scope.ToTs, scope.Limit = 0, 9999, 50
			got, _, err := m.queryLogChain(ctx, scope, nil)
			if err != nil {
				t.Fatalf("查询失败: %v", err)
			}
			gotIDs := logChainIDsOf(got)
			if len(gotIDs) != len(tc.want) {
				t.Fatalf("行数不符: got=%v want=%v", gotIDs, tc.want)
			}
			for i := range gotIDs {
				if gotIDs[i] != tc.want[i] {
					t.Fatalf("结果不符: got=%v want=%v", gotIDs, tc.want)
				}
			}
		})
	}
}

// TestLogChainDefaultTypeFilterOnRealRows 缺省只看消费与错误(type IN (2,5))。
// 充值/管理/系统日志与"请求走了哪个上游"无关，混进来会把错误行挤出首页。
func TestLogChainDefaultTypeFilterOnRealRows(t *testing.T) {
	m := newLogChainExecMonitor(t, []logChainSeedRow{
		// type=1 充值、type=4 管理操作：与"请求走了哪个上游"无关，缺省不该出现。
		{ID: 1, CreatedAt: 1001, Type: 1, UserID: 7, Content: "充值 100"},
		{ID: 2, CreatedAt: 1002, Type: 2, UserID: 7, ModelName: "gpt-4o", Quota: 5},
		{ID: 3, CreatedAt: 1003, Type: 4, UserID: 7, Content: "管理操作"},
		{ID: 4, CreatedAt: 1004, Type: 5, UserID: 7, Content: "上游报错"},
	})
	got, _, err := m.queryLogChain(context.Background(),
		logChainScope{FromTs: 0, ToTs: 9999, Limit: 50}, nil)
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	for _, r := range got {
		if r.Type != 2 && r.Type != 5 {
			t.Errorf("缺省不该返回 type=%d 的日志(id=%d)", r.Type, r.ID)
		}
	}
	if len(got) != 2 {
		t.Errorf("应只返回 type=2/5 两行，got=%v", logChainIDsOf(got))
	}
	// 显式传 type 时要能查到别的类型（缺省收窄不等于查不了）。
	recharge, _, err := m.queryLogChain(context.Background(),
		logChainScope{FromTs: 0, ToTs: 9999, Limit: 50, LogType: 1}, nil)
	if err != nil {
		t.Fatalf("按 type=1 查询失败: %v", err)
	}
	if len(recharge) != 1 || recharge[0].ID != 1 {
		t.Errorf("显式传 type=1 应能查到充值日志，got=%v", logChainIDsOf(recharge))
	}
}

// TestLogChainExcludesChannelTestTrafficOnRealRows 渠道测试流量必须排除，
// 但**不得误伤真实客户请求**。
//
// 排除谓词(trafficclass.SourceExclusionPredicateSQL)对 type=5 要求四项同时命中：
// user_id 为 1、token_id 为 0、token_name 为空串、request_id 为空串。
// 这里正反两面各造一条：少一项就不该被排除，否则真实错误会被静默吞掉。
func TestLogChainExcludesChannelTestTrafficOnRealRows(t *testing.T) {
	m := newLogChainExecMonitor(t, []logChainSeedRow{
		// 渠道测试(旧版失败测试的合成上下文)：四项特征全中 → 排除
		{ID: 1, CreatedAt: 1001, Type: 5, UserID: 1, ModelName: "gpt-4o", Content: "测试失败"},
		// 渠道测试(成功)：type=2 且 token_name 与 content 均为"模型测试" → 排除
		{ID: 2, CreatedAt: 1002, Type: 2, UserID: 1, TokenName: "模型测试", Content: "模型测试",
			ModelName: "gpt-4o", Quota: 1},
		// 真实的管理员请求：user_id=1 但有 request_id → **不得排除**
		{ID: 3, CreatedAt: 1003, Type: 5, UserID: 1, ModelName: "gpt-4o",
			Content: "真实错误", RequestID: "req-real"},
		// 普通客户错误 → 不得排除
		{ID: 4, CreatedAt: 1004, Type: 5, UserID: 7, TokenName: "alice-prod",
			ModelName: "gpt-4o", Content: "客户遇到的错误", RequestID: "req-x"},
	})
	got, _, err := m.queryLogChain(context.Background(),
		logChainScope{FromTs: 0, ToTs: 9999, Limit: 50}, nil)
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	ids := map[int64]bool{}
	for _, r := range got {
		ids[r.ID] = true
	}
	if ids[1] || ids[2] {
		t.Errorf("渠道测试流量未被排除: got=%v", logChainIDsOf(got))
	}
	if !ids[3] {
		t.Error("id=3 有 request_id，不是合成测试上下文，不得被排除——" +
			"排除谓词收得太宽会静默吞掉真实错误")
	}
	if !ids[4] {
		t.Error("id=4 是普通客户错误，必须能查到")
	}
}

// TestLogChainTimeWindowIsHalfOpenOnRealRows 时间窗左闭右开，边界行的取舍必须确定。
// 边界差一秒在排障里会表现成"客户说的那条查不到"，而且很难自查。
func TestLogChainTimeWindowIsHalfOpenOnRealRows(t *testing.T) {
	m := newLogChainExecMonitor(t, []logChainSeedRow{
		{ID: 1, CreatedAt: 999, Type: 5, Content: "窗前一秒"},
		{ID: 2, CreatedAt: 1000, Type: 5, Content: "窗起始(含)"},
		{ID: 3, CreatedAt: 1999, Type: 5, Content: "窗末尾(含)"},
		{ID: 4, CreatedAt: 2000, Type: 5, Content: "窗上界(不含)"},
	})
	got, _, err := m.queryLogChain(context.Background(),
		logChainScope{FromTs: 1000, ToTs: 2000, Limit: 50}, nil)
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	want := []int64{3, 2} // 倒序
	gotIDs := logChainIDsOf(got)
	if len(gotIDs) != len(want) {
		t.Fatalf("时间窗应为 [from,to) 左闭右开: got=%v want=%v", gotIDs, want)
	}
	for i := range want {
		if gotIDs[i] != want[i] {
			t.Fatalf("时间窗边界不符: got=%v want=%v", gotIDs, want)
		}
	}
}

// TestLogChainAttachChannelSnapsOnRealRows 渠道补全走完整链路：查生产 → 补本地快照。
// 三态必须分开：正常 / 快照缺失 / 未打到渠道——留空会被读成"这条请求没有上游域名"，
// 而三者的排障动作完全不同。
func TestLogChainAttachChannelSnapsOnRealRows(t *testing.T) {
	m := newLogChainExecMonitor(t, []logChainSeedRow{
		{ID: 1, CreatedAt: 1001, Type: 5, ChannelID: 3, Content: "有快照"},
		{ID: 2, CreatedAt: 1002, Type: 5, ChannelID: 99, Content: "快照缺失"},
		{ID: 3, CreatedAt: 1003, Type: 5, ChannelID: 0, Content: "未打到渠道"},
	})
	if err := m.replaceChannelSnaps([]ChannelSnap{
		{ID: 3, Name: "relay-a", Vendor: "OpenAI", BaseDomain: "a.example", Status: 1, UpdatedAt: 100},
	}, 100); err != nil {
		t.Fatalf("replaceChannelSnaps: %v", err)
	}
	ctx := context.Background()
	rows, _, err := m.queryLogChain(ctx, logChainScope{FromTs: 0, ToTs: 9999, Limit: 50}, nil)
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if err := m.attachChannelSnaps(ctx, rows); err != nil {
		t.Fatalf("attachChannelSnaps: %v", err)
	}
	byID := map[int64]LogChainRow{}
	for _, r := range rows {
		byID[r.ID] = r
	}
	if got := byID[1]; got.UpstreamDomain != "a.example" || got.ChannelUnresolved {
		t.Errorf("id=1 应补到域名: domain=%q unresolved=%v", got.UpstreamDomain, got.ChannelUnresolved)
	}
	if got := byID[2]; !got.ChannelUnresolved || got.UpstreamDomain != "" {
		t.Errorf("id=2 快照查不到应标 unresolved（不是留空冒充“没有上游”）: %+v",
			struct {
				Unresolved bool
				Domain     string
			}{got.ChannelUnresolved, got.UpstreamDomain})
	}
	if got := byID[3]; got.ChannelUnresolved {
		t.Error("id=3 是 channel_id=0（未打到渠道），不该标成“快照缺失”——两者含义不同")
	}
}

// TestLogChainAnomalySQLRunsForEveryKind 每一种 anomaly 取值的 SQL 都必须能执行。
// 逐个跑而不是只跑一个：billing_free 额外用了 channelTestJSONEnumSQL 取 other.billing_source，
// err_anom 是唯一跨 type 的组合，任一分支单独写错都不会被别的分支覆盖到。
func TestLogChainAnomalySQLRunsForEveryKind(t *testing.T) {
	m := newLogChainExecMonitor(t, []logChainSeedRow{
		{ID: 1, CreatedAt: 1000, Type: 2, ModelName: "gpt-4o", Quota: 100, CompletionTokens: 5,
			Other: `{"stream_status":{"end_reason":"eof"}}`},
	})
	for _, kind := range []string{
		anomalyStream, anomalyClientGone, anomalyBilling, anomalyBillingUnpaid,
		anomalyBillingFree, anomalyAll, anomalyErrAnom,
	} {
		t.Run(kind, func(t *testing.T) {
			scope := logChainScope{FromTs: 0, ToTs: 9999, Limit: 50, Anomaly: kind}
			if _, _, err := m.queryLogChain(context.Background(), scope, nil); err != nil {
				t.Fatalf("anomaly=%s 的 SQL 无法执行: %v", kind, err)
			}
		})
	}
}
