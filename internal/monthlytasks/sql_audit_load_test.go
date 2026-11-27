package monthlytasks

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

type auditLoadReader struct {
	t       *testing.T
	db      *sql.DB
	queries []string
	plans   [][]string
}

func (r *auditLoadReader) QueryRowContext(ctx context.Context, q string, a ...any) *sql.Row {
	r.t.Helper()
	rows, err := r.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+q, a...)
	if err != nil {
		r.t.Fatal(err)
	}
	var plan []string
	for rows.Next() {
		var x, y, z int
		var detail string
		if err := rows.Scan(&x, &y, &z, &detail); err != nil {
			r.t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err = rows.Err(); err != nil {
		r.t.Fatal(err)
	}
	if err = rows.Close(); err != nil {
		r.t.Fatal(err)
	}
	r.queries = append(r.queries, q)
	r.plans = append(r.plans, plan)
	return r.db.QueryRowContext(ctx, q, a...)
}
func TestSQLAuditTaskLoadAllowedColumns(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err = Init(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	s, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Submit(t.Context(), requestFixture())
	if err != nil {
		t.Fatal(err)
	}
	r := &auditLoadReader{t: t, db: db}
	byID, err := load(t.Context(), r, "id", job.ID)
	if err != nil {
		t.Fatal(err)
	}
	byKey, err := load(t.Context(), r, "task_key", job.Key)
	if err != nil || !reflect.DeepEqual(byID, byKey) {
		t.Fatal("exact key changed record", err)
	}
	if _, err = load(t.Context(), r, "arbitrary_column", job.ID); err != ErrInvalid || len(r.queries) != 2 {
		t.Fatal("invalid dynamic column queried")
	}
	for _, p := range r.plans {
		if !strings.Contains(strings.Join(p, "\n"), "SEARCH portal_monthly_draft_tasks USING INDEX sqlite_autoindex") {
			t.Fatal("task key no longer indexed", p)
		}
	}
	raw, err := json.MarshalIndent(struct {
		Warning string
		Queries []string
		Plans   [][]string
	}{"Actual private load() with both allowed columns, actual schema, complete record equality; invalid column fails before SQL. In-memory functional plan proof, not latency benchmark.", r.queries, r.plans}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile("../../docs/sql-audit-20261126/task-load-columns-20261127.json", append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
