package monitor

// logchain_dumpsql_test.go：把排障链路的成品 SQL 导出到文件，供人工拿到**真 MySQL** 上
// 只读验证语法、collation 与 hint 行为。
//
// 为什么需要这个：Go 侧字符串断言与 SQLite 假源都盖不住"只在 MySQL 上才报错"的问题——
// MAX_EXECUTION_TIME hint、JSON 函数细节、LOWER/LIKE 的 collation 行为都是 MySQL 特有的。
// 与 dumpsql_test.go（导出采样器 SQL）同一套做法，设 DUMP_SQL=1 才写文件。

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestDumpLogChainSQL(t *testing.T) {
	if os.Getenv("DUMP_SQL") == "" {
		t.Skip("设 DUMP_SQL=1 才导出")
	}
	out := os.Getenv("DUMP_SQL_PATH")
	if out == "" {
		out = "/tmp/logchain_sql.txt"
	}

	var b strings.Builder
	b.WriteString("-- 客户排障链路成品 SQL（由 TestDumpLogChainSQL 生成）\n")
	b.WriteString("-- 用途：拿到真 MySQL 上只读验证语法/collation/hint，SQLite 假源验不出这些。\n\n")

	// 逐个 anomaly 取值导出完整查询：判据组合各不相同，任一分支单独写错都不会被别的覆盖。
	for _, kind := range []string{
		"", anomalyStream, anomalyBilling, anomalyBillingUnpaid,
		anomalyBillingFree, anomalyAll, anomalyErrAnom,
	} {
		scope := logChainScope{FromTs: 1000, ToTs: 100000, Limit: 50, Anomaly: kind}
		where, args := logChainWhere(scope, nil)
		label := kind
		if label == "" {
			label = "(无异常筛选，缺省 type IN (2,5))"
		}
		q := "SELECT /*+ MAX_EXECUTION_TIME(" + strconv.Itoa(logChainQueryTimeoutMS) + ") */" +
			" id, created_at, COALESCE(type,0), COALESCE(user_id,0), COALESCE(username,'')," +
			" COALESCE(`group`,''), COALESCE(token_name,''), COALESCE(channel_id,0)," +
			" COALESCE(model_name,''), COALESCE(prompt_tokens,0), COALESCE(completion_tokens,0)," +
			" COALESCE(use_time,0), COALESCE(is_stream,0), COALESCE(quota,0)," +
			" COALESCE(content,''), COALESCE(other,''), COALESCE(request_id,'')" +
			" FROM logs WHERE " + where +
			" " + logChainOrderBySQL(scope.Asc) + " LIMIT " + strconv.Itoa(scope.Limit+1)

		fmt.Fprintf(&b, "-- ===== anomaly=%s  （%d 个参数）=====\n", label, len(args))
		b.WriteString(q)
		b.WriteString(";\n\n")
	}

	if err := os.WriteFile(out, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("已写 %s", out)
}
