package monitor

import (
	"database/sql"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"

	glebarez "github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// Resolve the two Monitor-owned schemas from actual current source statements.
// A clean checkout needs no private capture report or production-derived file.
func sqlAuditSourceStatement(t *testing.T, file, table string) string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "../"+file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var statements []string
	ast.Inspect(f, func(n ast.Node) bool {
		literal, ok := n.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		s, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(s, "FROM "+table+" ") || strings.Contains(s, "FROM "+table+"\n") {
			statements = append(statements, s)
		}
		return true
	})
	if len(statements) != 1 {
		t.Fatalf("expected one current statement for %s, got %d", table, len(statements))
	}
	return statements[0]
}

// Migrate actual models in memory, not invented tables. Keep the exact source
// SQL, key plan and no-redundant-sort checks from the original audit.
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
	items := []struct{ File, SQL string }{
		{File: "internal/monthlydraft/published_archive.go", SQL: sqlAuditSourceStatement(t, "internal/monthlydraft/published_archive.go", "portal_monthly_bills")},
		{File: "internal/monthlysource/snapshot_selection.go", SQL: sqlAuditSourceStatement(t, "internal/monthlysource/snapshot_selection.go", "main.usage_user_snapshots")},
	}
	type result struct {
		File, SQL, Status string
		Plan              []string
	}
	var results []result
	for _, item := range items {
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
	raw, err := json.MarshalIndent(struct {
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
