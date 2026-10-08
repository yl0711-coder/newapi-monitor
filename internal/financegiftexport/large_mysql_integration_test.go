package financegiftexport

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Run only inside a network=none container with a synthetic 4833-row fixture.
// The test user must have SELECT only. No production DSN is accepted.
func TestLargeHourIsolatedMySQL(t *testing.T) {
	dsn := os.Getenv("FINANCE_GIFT_SYNTHETIC_MYSQL_DSN")
	if dsn == "" {
		t.Skip("requires isolated synthetic MySQL fixture")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || cfg.User != "monitor_ro" || cfg.Net != "tcp" || cfg.Addr != "127.0.0.1:3306" || cfg.DBName != "gift_export_test" {
		t.Fatal("only explicitly named local synthetic fixture accepted")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	plan, _ := largeExportFixture(4833)
	digest, err := LargeHourConfirmation(plan)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "large.json")
	start := time.Now()
	if err := ExportLargeHour(context.Background(), db, plan, digest, path); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 10*batchInterval {
		t.Fatal("public export bypassed page cooldown")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var result LargeHourEvidence
	if err := json.Unmarshal(data, &result); err != nil || len(result.Rows) != 4833 || result.PlanSHA256 != digest {
		t.Fatal("incorrect complete proof", err)
	}
	for i, row := range result.Rows {
		if row.ID != int64(i+1) || row.Quota != int64(i) || row.Group != "business" {
			t.Fatal("wrong source identity or group")
		}
	}
	// A count-preserving monetary change in the approved plan must still fail;
	// no partial file is published after the first page succeeded.
	plan.Rows[500].Quota++
	digest, _ = LargeHourConfirmation(plan)
	path = filepath.Join(t.TempDir(), "mismatch.json")
	if err := exportLargeHour(context.Background(), db, plan, digest, path, func(ctx context.Context, _ time.Duration) error { return ctx.Err() }); err == nil {
		t.Fatal("monetary mismatch accepted")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("partial mismatch evidence published")
	}
	t.Log("isolated MySQL: 4833 synthetic records, 10 SELECT-only page transactions, real 10-second cooldowns, mismatch fails closed")
}
