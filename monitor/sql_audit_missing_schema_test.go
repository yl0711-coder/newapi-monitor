package monitor

import (
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"

	glebarez "github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// Resolve only the two explicitly missing Monitor-owned schemas from the
// earlier static report. Migrate actual models in memory, not invented tables.
func TestSQLAuditMissingMonitorSchemas(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	g, err := gorm.Open(glebarez.Dialector{Conn: db}, &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err = g.AutoMigrate(&PortalMonthlyBill{}, &UsageUserSnapshot{}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../docs/sql-audit-20261126/internal-static-plans-20261127.json")
	if err != nil {
		t.Fatal(err)
	}
	var old struct {
		Entries []struct {
			File string `json:"file"`
			SQL  string `json:"source_sql"`
		}
	}
	if err = json.Unmarshal(raw, &old); err != nil {
		t.Fatal(err)
	}
	type result struct {
		File, SQL, Status string
		Plan              []string
	}
	var results []result
	for _, item := range old.Entries {
		var args []any
		switch item.File {
		case "internal/monthlydraft/published_archive.go":
			args = []any{int64(7), "2026-10"}
		case "internal/monthlysource/snapshot_selection.go":
			if !strings.Contains(item.SQL, "FROM main.usage_user_snapshots") {
				continue
			}
		default:
			continue
		}
		source, err := os.ReadFile("../" + item.File)
		if err != nil || !strings.Contains(string(source), item.SQL) {
			t.Fatal("static statement no longer matches current source", item.File)
		}
		r := result{File: item.File, SQL: item.SQL, Status: "actual_model_empty_schema_explained"}
		rows, err := db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+item.SQL, args...)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var a, b, c int
			var detail string
			if err := rows.Scan(&a, &b, &c, &detail); err != nil {
				t.Fatal(err)
			}
			r.Plan = append(r.Plan, detail)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err = rows.Close(); err != nil {
			t.Fatal(err)
		}
		plan := strings.Join(r.Plan, "\n")
		if strings.Contains(item.SQL, "FROM portal_monthly_bills") && !strings.Contains(plan, "SEARCH portal_monthly_bills USING INDEX") {
			t.Fatal("publication no longer uses its exact key", plan)
		}
		if strings.Contains(item.SQL, "FROM main.usage_user_snapshots") && strings.Contains(plan, "TEMP B-TREE") {
			t.Fatal("snapshot introduced a redundant sort", plan)
		}
		results = append(results, r)
	}
	if len(results) != 2 {
		t.Fatalf("missing or duplicate schema cases: %d", len(results))
	}
	raw, err = json.MarshalIndent(struct {
		Warning string
		Results []result
	}{"Actual current Monitor models and exact current SQL, in-memory SQLite EXPLAIN only. No published bill read, business function execution or production latency claim.", results}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile("../docs/sql-audit-20261126/missing-monitor-schema-plans-20261127.json", append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
