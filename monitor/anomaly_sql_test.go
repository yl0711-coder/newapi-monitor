package monitor

import "strings"

import "testing"

// TestExpandAnomalyPredicates 守住三件事:
//  1. 占位符全部被展开(SQL 里不留 {{}},否则打到生产库直接语法错);
//  2. 列别名 anomaly_billed 等不被误伤(裸 ANOM 是它们的前缀);
//  3. 判据用 completion_tokens 且排除天然无输出模型。
func TestExpandAnomalyPredicates(t *testing.T) {
	in := `SUM(type=2 AND {{ANOM}}) AS anomaly,
SUM(type=2 AND {{ZERO}} AND prompt_tokens > 0) AS anomaly_billed,
SUM(type=2 AND {{STREAMBAD}} AND NOT {{ZERO}}) AS anomaly_stream`
	got := expandAnomalyPredicates(in)

	if strings.Contains(got, "{{") || strings.Contains(got, "}}") {
		t.Fatalf("占位符未全部展开:\n%s", got)
	}
	for _, alias := range []string{"AS anomaly,", "AS anomaly_billed", "AS anomaly_stream"} {
		if !strings.Contains(got, alias) {
			t.Errorf("列别名被误伤,应保留 %q:\n%s", alias, got)
		}
	}
	if !strings.Contains(got, "completion_tokens = 0") {
		t.Error("交付判据必须基于 completion_tokens")
	}
	// frt 只证明上游开口(任何 data: 行都会置位),不能当交付凭证。
	if strings.Contains(got, "$.frt") {
		t.Error("不得用 frt 判断是否交付")
	}
	if !strings.Contains(got, "embed|rerank") {
		t.Error("必须排除天然无输出模型,否则 embedding 类会被整类误判成 B1")
	}
	// end_reason 必须走 JSON_EXTRACT:other 里的 end_error 自由文本可能含 panic 等词。
	if !strings.Contains(got, "JSON_EXTRACT(other,'$.stream_status.end_reason')") {
		t.Error("end_reason 必须走 JSON_EXTRACT,不能对 other 整串做正则")
	}
}

// TestSampleWindowSQLPlaceholderCount 守住 SQL 的 ? 个数与 sampleRange 传参个数一致。
// 曾经把 WHERE 从 "created_at >= UNIX_TIMESTAMP() - ?" 改成区间式时漏改,
// SQL 只有 1 个 ? 而调用方传 2 个,结果每次采样都直接报错——这类错 Go 编译器不管,
// 只有打到库才炸,所以必须有断言兜住。
func TestSampleWindowSQLPlaceholderCount(t *testing.T) {
	q := sampleWindowSQL()
	if !strings.Contains(q, "MAX_EXECUTION_TIME(8000)") {
		t.Fatal("常规来源聚合必须有 8 秒 MySQL 服务端硬上限")
	}
	if n := strings.Count(q, "?"); n != 2 {
		t.Fatalf("sampleWindowSQL 应有 2 个 ?(fromTs, toTs),实际 %d 个", n)
	}
	// 必须是半开区间:上界用 < 而非 <=,否则相邻分片会把边界那一秒算两次。
	if !strings.Contains(q, "created_at >= ? AND created_at < ?") {
		t.Error("WHERE 必须是 created_at >= ? AND created_at < ? 的半开区间形式")
	}
	// 不能再残留相对时间写法,否则回填传进来的历史区间会被忽略、永远只采最近的数据。
	if strings.Contains(q, "UNIX_TIMESTAMP()") {
		t.Error("WHERE 不得用 UNIX_TIMESTAMP() 算相对窗口,回填需要显式区间")
	}
	if !strings.Contains(q, "type IN (2,5)") {
		t.Error("只取消费日志与错误日志两类")
	}
	for _, marker := range []string{
		"COALESCE(token_name,'')='模型测试' AND COALESCE(content,'')='模型测试'",
		"AND NOT (",
	} {
		if !strings.Contains(q, marker) {
			t.Errorf("用户流量 SQL 必须排除渠道测试标记 %q", marker)
		}
	}
}

func TestSourceEpochStartupLookbackIsDurablyBounded(t *testing.T) {
	now := int64(10_000)
	if got := boundedSourceEpochStartupLookback(24, now, 0); got != 3600 {
		t.Fatalf("fresh start replayed an operator-sized window: got %ds", got)
	}
	if got := boundedSourceEpochStartupLookback(24, now, now-600); got != 720 {
		t.Fatalf("durable watermark was ignored: got %ds want 720s", got)
	}
	if got := boundedSourceEpochStartupLookback(24, now, now-30); got != 180 {
		t.Fatalf("small restart overlap=%ds want 180s", got)
	}
	if got := boundedSourceEpochStartupLookback(0, now, 0); got != 0 {
		t.Fatalf("disabled startup catchup=%ds", got)
	}
}

func TestTokenSampleSQLHasServerExecutionLimit(t *testing.T) {
	q := sampleTokenSQL()
	if !strings.Contains(q, "MAX_EXECUTION_TIME(8000)") {
		t.Fatal("token 来源聚合必须有 8 秒 MySQL 服务端硬上限")
	}
	if n := strings.Count(q, "?"); n != 2 {
		t.Fatalf("token SQL placeholders=%d want 2", n)
	}
	if !strings.Contains(q, "created_at >= ? AND created_at < ?") {
		t.Fatal("token 来源聚合必须使用显式半开区间")
	}
}

func TestChannelTestPredicateUsesOnlyUnmodifiedNewAPILegacyShapes(t *testing.T) {
	predicate := channelTestLogPredicateSQL()
	if !strings.Contains(predicate, "COALESCE(token_name,'')='模型测试' AND COALESCE(content,'')='模型测试'") {
		t.Fatalf("旧日志必须同时匹配 token_name/content，避免误伤普通用户令牌: %s", predicate)
	}
	for _, marker := range []string{
		"type=5 AND user_id=1",
		"COALESCE(token_id,0)=0",
		"COALESCE(request_id,'')=''",
	} {
		if !strings.Contains(predicate, marker) {
			t.Fatalf("旧版批量测试失败必须用完整合成请求特征识别，缺少 %q: %s", marker, predicate)
		}
	}
	if strings.Contains(predicate, "channel_test_audit") || strings.Contains(predicate, "$.is_channel_test") {
		t.Fatalf("Monitor 不得依赖需要修改 NewAPI 才会出现的字段: %s", predicate)
	}
	origin := channelTestOriginSQL(predicate)
	if strings.Contains(origin, "channel_test_origin") || !strings.Contains(origin, "'legacy'") {
		t.Fatalf("旧日志无法证明手动/定时来源，只能标记 legacy: %s", origin)
	}
}
