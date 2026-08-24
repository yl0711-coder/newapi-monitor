package monitor

// logchain_boundary_test.go：排障链路的边界与异常执行测试（SOP §5.5）。
//
// 与另两份的分工：
//   logchain_test.go       字符串断言：SQL 长什么样
//   logchain_exec_test.go  真执行：判据筛出哪几行、排序与翻页
//   logchain_boundary_test.go  真执行：0 条 / 1 条 / 分页上限 / 特殊字符 / 超长值 / 生产库不可用
//
// 这些用例上线前必须过：它们对应的是"客户报障时你正在用这个页面"的真实场景，
// 一旦在这些边界上出错，表现是页面 500 或静默给出错误结果，而不是明显的功能缺失。

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestLogChainZeroAndSingleRowOnRealRows 0 条与 1 条必须都正常，且不谎报 has_more。
// 0 条最关键：排障页查不到是常态（当天没错误就该是 0 条），不能因此 500。
func TestLogChainZeroAndSingleRowOnRealRows(t *testing.T) {
	t.Run("零条", func(t *testing.T) {
		m := newLogChainExecMonitor(t, nil)
		rows, hasMore, err := m.queryLogChain(context.Background(),
			logChainScope{FromTs: 0, ToTs: 9999, Limit: 50}, nil)
		if err != nil {
			t.Fatalf("空库查询不该报错（当天无错误是常态）: %v", err)
		}
		if len(rows) != 0 || hasMore {
			t.Errorf("空库应返回 0 行且 has_more=false: rows=%d hasMore=%v", len(rows), hasMore)
		}
		// 渠道补全对空集合必须是 no-op，不能因为没有行就报错。
		if err := m.attachChannelSnaps(context.Background(), rows); err != nil {
			t.Errorf("空集合补全渠道不该报错: %v", err)
		}
	})

	t.Run("一条", func(t *testing.T) {
		m := newLogChainExecMonitor(t, []logChainSeedRow{
			{ID: 1, CreatedAt: 1000, Type: 5, UserID: 7, ModelName: "gpt-4o", Content: "唯一一条"},
		})
		rows, hasMore, err := m.queryLogChain(context.Background(),
			logChainScope{FromTs: 0, ToTs: 9999, Limit: 50}, nil)
		if err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		if len(rows) != 1 || hasMore {
			t.Errorf("一条数据应返回 1 行且 has_more=false: rows=%d hasMore=%v", len(rows), hasMore)
		}
	})
}

// TestLogChainHasMoreAtLimitBoundaryOnRealRows has_more 在"正好等于 limit"处不得谎报。
//
// 实现是多取一行判断（LIMIT n+1）。恰好 n 条时若报 has_more=true，
// 前端会显示"加载更多"按钮，点下去返回空 —— 用户会以为功能坏了。
func TestLogChainHasMoreAtLimitBoundaryOnRealRows(t *testing.T) {
	rows := make([]logChainSeedRow, 0, 11)
	for i := 1; i <= 11; i++ {
		rows = append(rows, logChainSeedRow{
			ID: int64(i), CreatedAt: int64(1000 + i), Type: 5, ModelName: "gpt-4o", Content: "err",
		})
	}
	m := newLogChainExecMonitor(t, rows)
	ctx := context.Background()

	for _, tc := range []struct {
		name        string
		limit       int
		wantRows    int
		wantHasMore bool
	}{
		{"少于 limit", 20, 11, false},
		{"正好等于 limit", 11, 11, false}, // 关键：不得谎报还有更多
		{"少一条", 10, 10, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, hasMore, err := m.queryLogChain(ctx,
				logChainScope{FromTs: 0, ToTs: 9999, Limit: tc.limit}, nil)
			if err != nil {
				t.Fatalf("查询失败: %v", err)
			}
			if len(got) != tc.wantRows || hasMore != tc.wantHasMore {
				t.Errorf("limit=%d: got rows=%d hasMore=%v, want rows=%d hasMore=%v",
					tc.limit, len(got), hasMore, tc.wantRows, tc.wantHasMore)
			}
		})
	}
}

// TestLogChainSpecialCharactersOnRealRows LIKE 通配符与特殊字符不得被当成通配符解释。
//
// keyword 与 token_name 走 LIKE ... ESCAPE '!'。若转义写坏，用户搜 "100%" 会匹配到
// 所有含 "100" 的行 —— 结果看起来正常，实际是错的，属最难自查的一类 bug。
func TestLogChainSpecialCharactersOnRealRows(t *testing.T) {
	m := newLogChainExecMonitor(t, []logChainSeedRow{
		{ID: 1, CreatedAt: 1001, Type: 5, TokenName: "rate-100%-limit", Content: "配额用尽 100% 已达上限"},
		{ID: 2, CreatedAt: 1002, Type: 5, TokenName: "rate-100X-limit", Content: "配额用尽 100X 冒充通配符命中"},
		{ID: 3, CreatedAt: 1003, Type: 5, TokenName: "under_score", Content: "下划线 a_b"},
		{ID: 4, CreatedAt: 1004, Type: 5, TokenName: "underXscore", Content: "下划线 aXb"},
		{ID: 5, CreatedAt: 1005, Type: 5, TokenName: "bang!name", Content: "感叹号 !转义符本身"},
	})
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		scope logChainScope
		want  []int64
	}{
		// % 必须当字面量：只该命中 id=1，不该把 100X 也算进来。
		{"百分号当字面量", logChainScope{Keyword: "100%"}, []int64{1}},
		{"百分号在令牌名里", logChainScope{TokenName: "100%"}, []int64{1}},
		// _ 是单字符通配符，必须转义：只该命中 id=3。
		{"下划线当字面量", logChainScope{Keyword: "a_b"}, []int64{3}},
		{"下划线在令牌名里", logChainScope{TokenName: "under_score"}, []int64{3}},
		// 转义符本身要能搜。
		{"感叹号(转义符本身)", logChainScope{TokenName: "bang!name"}, []int64{5}},
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
				t.Fatalf("特殊字符被当成通配符解释了？got=%v want=%v", gotIDs, tc.want)
			}
			for i := range gotIDs {
				if gotIDs[i] != tc.want[i] {
					t.Fatalf("结果不符: got=%v want=%v", gotIDs, tc.want)
				}
			}
		})
	}
}

// TestLogChainLongAndUnicodeValuesOnRealRows 超长值与 Unicode 必须原样透传，不截断不乱码。
//
// 上游错误原文有时很长（含堆栈或 JSON），而排障要的就是能原样拿去问上游客服。
// 后端不得截断——截断后的错误原文拿去问上游，对方会说"没有这条记录"。
func TestLogChainLongAndUnicodeValuesOnRealRows(t *testing.T) {
	longErr := "渠道 LA-claude-max (#31) 返回错误：" + strings.Repeat("上游堆栈信息很长很长 ", 500)
	unicodeName := "客户-张三-🔥-模型测试用-Ünïcödé"
	m := newLogChainExecMonitor(t, []logChainSeedRow{
		{ID: 1, CreatedAt: 1001, Type: 5, TokenName: unicodeName,
			Username: unicodeName, ModelName: "claude-sonnet-4", Content: longErr},
	})
	rows, _, err := m.queryLogChain(context.Background(),
		logChainScope{FromTs: 0, ToTs: 9999, Limit: 50}, nil)
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("应返回 1 行，got=%d", len(rows))
	}
	if rows[0].Content != longErr {
		t.Errorf("超长错误原文被改写或截断了：原文 %d 字节，返回 %d 字节——"+
			"排障要能原样拿去问上游客服", len(longErr), len(rows[0].Content))
	}
	if rows[0].TokenName != unicodeName || rows[0].Member != unicodeName {
		t.Errorf("Unicode 值未原样返回: token=%q member=%q", rows[0].TokenName, rows[0].Member)
	}
	// Unicode 也要能作为筛选条件命中（emoji 与变音符号不得破坏 LIKE）。
	got, _, err := m.queryLogChain(context.Background(),
		logChainScope{FromTs: 0, ToTs: 9999, Limit: 50, TokenName: "张三-🔥"}, nil)
	if err != nil {
		t.Fatalf("按 Unicode 令牌名查询失败: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("Unicode 子串应能命中，got=%d 行", len(got))
	}
}

// TestLogChainNullColumnsOnRealRows 历史数据留 NULL 时不得整页 500。
//
// 实现对全部列做了 COALESCE。这个测试证明那不是多余的防御：
// 直接 Scan NULL 进 int64/string 会让整页返回 500，而排障页正是在出问题时才被打开。
func TestLogChainNullColumnsOnRealRows(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	// 绕过 helper 直接插 NULL：模拟迁移遗留数据。
	if _, err := m.prodDB.Exec(
		"INSERT INTO logs (id,user_id,channel_id,created_at,type,model_name,quota," +
			"prompt_tokens,completion_tokens,`group`,token_name,username,use_time," +
			"is_stream,content,other,request_id) VALUES " +
			"(1,7,NULL,1000,5,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)"); err != nil {
		t.Fatalf("插入含 NULL 的行失败: %v", err)
	}
	rows, _, err := m.queryLogChain(context.Background(),
		logChainScope{FromTs: 0, ToTs: 9999, Limit: 50}, nil)
	if err != nil {
		t.Fatalf("含 NULL 的历史数据让查询报错了（COALESCE 没兜住）: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("应返回 1 行，got=%d", len(rows))
	}
	// NULL 的 channel_id 应落成 0，语义是"未打到渠道"，不得标成"快照缺失"。
	if rows[0].ChannelID != 0 {
		t.Errorf("NULL channel_id 应落成 0，got=%d", rows[0].ChannelID)
	}
	if err := m.attachChannelSnaps(context.Background(), rows); err != nil {
		t.Fatalf("attachChannelSnaps 对 NULL 行报错: %v", err)
	}
	if rows[0].ChannelUnresolved {
		t.Error("channel_id 为 NULL/0 是“未打到渠道”，不是“快照缺失”——两者排障动作不同")
	}
}

// TestLogChainReleasesGateOnQueryFailure 查询失败后必须释放闸门，否则一次失败会永久堵死客户 Portal。
//
// **这是上线前最关键的安全性质。** usageDetailGate 容量只有 1，且客户 Portal 查自己日志
// （countGroupLogs / queryGroupLogs）走同一条泳道。若排障查询在超时或报错路径上漏掉释放，
// 后果不是"排障页坏了"，而是**客户从此再也查不了自己的日志** —— 一次失败换来永久故障。
//
// 用一个已关闭的 DB 制造真实的查询失败，然后连续跑多次：
// 若闸门泄漏，第二次就会卡在等待槽位上直到超时（15 秒），测试会明显变慢并失败。
func TestLogChainReleasesGateOnQueryFailure(t *testing.T) {
	m := newTestMonitor(t)
	db := newFakeProdDB(t)
	m.prodDB = db
	// 关掉连接：后续查询必然失败，走的是"已拿到闸门后才出错"的路径。
	if err := db.Close(); err != nil {
		t.Fatalf("关闭假生产库失败: %v", err)
	}

	scope := logChainScope{FromTs: 0, ToTs: 9999, Limit: 50}
	// 跑 3 次。闸门容量 1，若第一次没释放，第二次会阻塞到 15 秒超时。
	for i := 1; i <= 3; i++ {
		started := time.Now()
		_, _, err := m.queryLogChain(context.Background(), scope, nil)
		elapsed := time.Since(started)
		if err == nil {
			t.Fatalf("第 %d 次：连接已关闭，查询应该失败", i)
		}
		// 失败必须是查询本身的错，不能是"等待槽位失败"——后者说明闸门被前一次占着没还。
		if strings.Contains(err.Error(), "等待日志查询槽位失败") {
			t.Fatalf("第 %d 次：闸门未被释放，新请求拿不到槽位。"+
				"这会让客户 Portal 查自己日志也一起卡死（同一条容量 1 的泳道）", i)
		}
		// 正常路径应当立即返回；接近闸门超时说明在排队。
		if elapsed > 2*time.Second {
			t.Errorf("第 %d 次耗时 %v，接近闸门超时(%v)——疑似在等一个没被释放的槽位",
				i, elapsed, logChainGateTimeout)
		}
	}
}

// TestLogChainProductionDBUnavailableOnRealRows 生产库不可用时必须明确报错，不得伪装成"没有数据"。
//
// 这是 docs/aimustkonw.md 的核心原则之一：缺失、延迟、覆盖不全绝不能显示为零。
// 排障页若把"查不到"和"库连不上"混为一谈，会让人得出"客户在瞎说"的结论。
func TestLogChainProductionDBUnavailableOnRealRows(t *testing.T) {
	m := newTestMonitor(t)
	// prodDB 为 nil：本地快照只读模式。
	rows, hasMore, err := m.queryLogChain(context.Background(),
		logChainScope{FromTs: 0, ToTs: 9999, Limit: 50}, nil)
	if err == nil {
		t.Fatal("生产库未连接时必须报错，不得返回空集冒充“没有数据”")
	}
	if !strings.Contains(err.Error(), "生产库未连接") {
		t.Errorf("错误信息应说清是库未连接而非无数据: %v", err)
	}
	if len(rows) != 0 || hasMore {
		t.Error("报错路径不该同时返回数据")
	}
}
