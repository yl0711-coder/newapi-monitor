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
}
