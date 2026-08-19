package monitor

import (
	"os"
	"testing"
)

// TestDumpSampleWindowSQL 把成品 SQL 写到 /tmp,供人工拿到真实 MySQL 上只读验证
// 语法与 collation。判据含 REGEXP + JSON_EXTRACT,Go 侧字符串断言盖不住
// "打到生产库才报错"这类问题。设 DUMP_SQL=1 时才写文件。
func TestDumpSampleWindowSQL(t *testing.T) {
	if os.Getenv("DUMP_SQL") == "" {
		t.Skip("设 DUMP_SQL=1 才导出")
	}
	if err := os.WriteFile("/tmp/sample_window.sql", []byte(sampleWindowSQL()+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("/tmp/stability_hour.sql", []byte(stabilityHourSQL()+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Log("已写 /tmp/sample_window.sql 与 /tmp/stability_hour.sql")
}
