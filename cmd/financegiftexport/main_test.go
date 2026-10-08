package main

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestReadonlyConfigRejectsUnsafeSource(t *testing.T) {
	for _, dsn := range []string{"", "root:secret@tcp(127.0.0.1:3306)/fixture", "monitor_ro:secret@tcp(10.0.0.1:3306)/fixture", "monitor_ro:secret@tcp(localhost:3306)/fixture", "monitor_ro:secret@unix(/tmp/mysql.sock)/fixture", "monitor_ro:secret@tcp(127.0.0.1:3306)/other", "monitor_ro:secret@tcp(127.0.0.1:3306)/fixture?multiStatements=true", "monitor_ro:secret@tcp(127.0.0.1:3306)/fixture?allowAllFiles=true", "monitor_ro:secret@tcp(127.0.0.1:3306)/fixture?sql_mode=x"} {
		if _, err := readonlyConfig(dsn, "fixture"); err == nil {
			t.Fatal("unsafe source accepted")
		}
	}
	cfg, err := readonlyConfig("monitor_ro:secret@tcp(127.0.0.1:13326)/fixture?readTimeout=1h", "fixture")
	if err != nil || cfg.ReadTimeout != 8*time.Second || cfg.Timeout != 3*time.Second {
		t.Fatal("safe source rejected or timeout not capped", err)
	}
}

func TestExportCLIRequiresExplicitScopeBeforeCredentials(t *testing.T) {
	for _, args := range [][]string{nil, {"-plan", "missing"}, {"-unknown"}, {"-allow-source-read", "extra"}} {
		var output bytes.Buffer
		err := run(context.Background(), args, &output, func(string) string { t.Fatal("incomplete command reached credentials"); return "" })
		if err == nil || output.Len() != 0 {
			t.Fatal("incomplete command produced output")
		}
	}
}
