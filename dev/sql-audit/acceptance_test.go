package main

import (
	"encoding/json"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestInventoryFileKeepsExecutionsAndSkipsDecoders(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "fixture.go")
	text := `package fixture
func (m *Monitor) read(ctx any) {
 rows,err:=m.prodDB.QueryContext(ctx,"SELECT id FROM logs WHERE id=?",1)
 rows.Scan(&err)
 m.storeDB.Raw("SELECT COUNT(*) FROM facts").Scan(&err)
 m.prodDB.QueryRowContext(ctx,"SELECT 1").Scan(&err)
 fmt.Scan(&err)
}`
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	a := inventoryFile(token.NewFileSet(), path, root, nil, nil)
	b := inventoryFile(token.NewFileSet(), path, root, nil, nil)
	if !reflect.DeepEqual(a, b) || len(a) != 4 {
		t.Fatal("nonstable inventory or decoder included", a)
	}
	var APIs []string
	for _, v := range a {
		APIs = append(APIs, v.API)
		if v.Function != "*Monitor.read" || v.File != "fixture.go" || v.ID == "" {
			t.Fatal(v)
		}
	}
	if !reflect.DeepEqual(APIs, []string{"QueryContext", "Scan", "Raw", "QueryRowContext"}) {
		t.Fatal(APIs)
	}
}

func TestSQLAuditPlanAndStoppedReportRemainSanitized(t *testing.T) {
	p := map[string]any{"table": map[string]any{"table_name": "logs", "key": "idx_created_at_id_rc26", "attached_condition": "secret-filter", "rows_examined_per_scan": float64(10)}}
	plan := sqlAuditPlan(p)
	if len(plan) != 1 || plan[0]["key"] != "idx_created_at_id_rc26" || plan[0]["attached_condition"] != nil {
		t.Fatal(plan)
	}
	out := filepath.Join(t.TempDir(), "report.json")
	if err := persistSQLAuditReport(out, 2, 3, 5, true, []explainResult{{ID: "fixture", Status: "explain_error"}}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Offset, End, Total int
		Analyze, Stopped   bool
		Entries            []explainResult
	}
	if json.Unmarshal(b, &report) != nil || !report.Stopped || report.Analyze || report.Offset != 2 || report.End != 3 || report.Total != 5 || len(report.Entries) != 1 {
		t.Fatal(string(b))
	}
}
