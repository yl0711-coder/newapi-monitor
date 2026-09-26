package financegiftexport

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Opt-in synthetic fixture only. No production DSN or default connection.
// Run in a network=none MySQL container using its loopback interface.
func TestBatchIsolatedMySQL(t *testing.T) {
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
	plan := BatchPlan{SourceEpoch: "synthetic-only"}
	for i, n := range []int{2498, 17, 3} {
		plan.Targets = append(plan.Targets, BatchTarget{UserID: int64(7 + i), HourTs: 3600, ExpectedRows: n, LocalContentHash: strings.Repeat("a", 64)})
	}
	digest, err := BatchConfirmation(plan)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "complete")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	start := time.Now()
	got, err := ExportBatch(ctx, db, plan, digest, dir)
	if err != nil || got.Status != "complete" || got.Remaining != 0 || len(got.Entries) != 3 {
		t.Fatalf("batch=%+v err=%v", got, err)
	}
	if time.Since(start) < 3*batchInterval {
		t.Fatal("public entry skipped cooldown")
	}
	for index, entry := range got.Entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.File))
		if err != nil {
			t.Fatal(err)
		}
		var e struct {
			UserID int64                `json:"user_id"`
			Hour   int64                `json:"hour_ts"`
			Rows   []giftScopeExportRow `json:"rows"`
		}
		if err = json.Unmarshal(data, &e); err != nil {
			t.Fatal(err)
		}
		if len(e.Rows) != plan.Targets[index].ExpectedRows || e.UserID != int64(index+7) || e.Hour != 3600 {
			t.Fatal("wrong complete export")
		}
		for i, r := range e.Rows {
			if r.UserID != e.UserID || r.CreatedAt < 3600 || r.CreatedAt >= 7200 || r.ID == 25001 {
				t.Fatal("wrong identity or leaked automatic test")
			}
			want := int64(i)
			if index > 0 {
				want = int64(index*10 + i)
			}
			if r.Quota != want {
				t.Fatal("fixture quota mismatch")
			}
		}
	}
	// A second-target count mismatch must preserve the first file and never
	// read/export the third target or publish a complete manifest.
	plan.Targets[1].ExpectedRows++
	digest, err = BatchConfirmation(plan)
	if err != nil {
		t.Fatal(err)
	}
	dir = filepath.Join(t.TempDir(), "mismatch")
	got, err = exportBatch(ctx, db, plan, digest, dir, func(ctx context.Context, _ time.Duration) error { return ctx.Err() })
	if err == nil || got.Status != "failed" || len(got.Entries) != 1 || got.Remaining != 2 {
		t.Fatalf("failure not bounded: %+v %v", got, err)
	}
	if _, err = os.Stat(filepath.Join(dir, "evidence-manifest.json")); !os.IsNotExist(err) {
		t.Fatal("failed batch published manifest")
	}
	if _, err = os.Stat(filepath.Join(dir, "evidence-02.json")); !os.IsNotExist(err) {
		t.Fatal("failed batch continued to third target")
	}
	t.Log("isolated MySQL: 2518 rows, three targets, real cooldown; count mismatch stops batch; all data synthetic")
}
