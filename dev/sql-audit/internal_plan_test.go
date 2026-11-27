package main

// Diagnostic only: prepare statically resolved SELECTs against source-derived,
// empty in-memory SQLite schemas. This is NOT runtime branch coverage, a
// MySQL plan, a trigger execution test, or a performance benchmark.
import (
	"context"
	"database/sql"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
)

type internalPlanRecord struct {
	File   string   `json:"file"`
	Line   int      `json:"line"`
	SQL    string   `json:"source_sql"`
	Status string   `json:"status"`
	Plan   []string `json:"plan,omitempty"`
	Error  string   `json:"error,omitempty"`
}

func TestInternalSQLStaticPlans(t *testing.T) {
	report := os.Getenv("SQL_AUDIT_INTERNAL_REPORT")
	if report == "" {
		t.Skip("explicit bounded local diagnostic report required")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	out, err := filepath.Abs(report)
	if err != nil || !strings.HasPrefix(out, filepath.Join(root, "docs/sql-audit-20261126")+string(os.PathSeparator)) {
		t.Fatal("report must remain in this task's evidence directory")
	}
	fs := token.NewFileSet()
	files := map[string]*ast.File{}
	constants := map[string]ast.Expr{}
	for _, dir := range []string{"monthlysource", "monthlydraft", "monthlytasks"} {
		paths, err := filepath.Glob(filepath.Join(root, "internal", dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(fs, path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			files[path] = f
			for _, d := range f.Decls {
				g, ok := d.(*ast.GenDecl)
				if !ok || g.Tok != token.CONST {
					continue
				}
				for _, s := range g.Specs {
					v := s.(*ast.ValueSpec)
					for i, n := range v.Names {
						if i < len(v.Values) {
							constants[f.Name.Name+"."+n.Name] = v.Values[i]
						}
					}
				}
			}
		}
	}
	var resolve func(ast.Expr, string, int) (string, bool)
	resolve = func(e ast.Expr, pkg string, depth int) (string, bool) {
		if depth > 20 {
			return "", false
		}
		switch x := e.(type) {
		case *ast.BasicLit:
			if x.Kind == token.STRING {
				s, err := strconv.Unquote(x.Value)
				return s, err == nil
			}
		case *ast.Ident:
			if v, ok := constants[pkg+"."+x.Name]; ok {
				return resolve(v, pkg, depth+1)
			}
		case *ast.BinaryExpr:
			if x.Op == token.ADD {
				a, ok := resolve(x.X, pkg, depth+1)
				b, other := resolve(x.Y, pkg, depth+1)
				return a + b, ok && other
			}
		case *ast.ParenExpr:
			return resolve(x.X, pkg, depth+1)
		}
		return "", false
	}
	var records []internalPlanRecord
	seen := map[string]bool{}
	for path, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			e, ok := n.(ast.Expr)
			if !ok {
				return true
			}
			// Identifiers are expanded as part of their enclosing expression;
			// counting every reference would inflate template coverage.
			if _, ok := e.(*ast.Ident); ok {
				return true
			}
			s, ok := resolve(e, f.Name.Name, 0)
			if !ok {
				return true
			}
			s = strings.TrimSpace(s)
			u := strings.ToUpper(s)
			if !strings.HasPrefix(u, "SELECT ") && !strings.HasPrefix(u, "WITH ") && !strings.HasPrefix(u, "CREATE TABLE ") && !strings.HasPrefix(u, "CREATE INDEX ") && !strings.HasPrefix(u, "CREATE UNIQUE INDEX ") {
				return true
			}
			pos := fs.Position(e.Pos())
			rel, _ := filepath.Rel(root, path)
			key := rel + ":" + strconv.Itoa(pos.Line) + ":" + s
			if !seen[key] {
				seen[key] = true
				records = append(records, internalPlanRecord{File: rel, Line: pos.Line, SQL: s})
			}
			return false // Keep complete const concatenation, not its fragments.
		})
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].File != records[j].File {
			return records[i].File < records[j].File
		}
		return records[i].Line < records[j].Line
	})
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	// Tables first, then indexes, exactly from source declarations. Triggers
	// are deliberately not installed: this inventory concerns SELECT plans.
	for _, phase := range []string{"table", "index"} {
		for i := range records {
			r := &records[i]
			u := strings.ToUpper(r.SQL)
			isTable := strings.HasPrefix(u, "CREATE TABLE ")
			if !strings.HasPrefix(u, "CREATE ") || (phase == "table") != isTable {
				continue
			}
			if strings.Contains(r.SQL, "%s") || strings.Contains(r.SQL, "%d") {
				r.Status = "dynamic_ddl_not_installed"
				continue
			}
			if _, err := db.ExecContext(ctx, r.SQL); err != nil {
				r.Status, r.Error = "schema_not_installed", err.Error()
			} else {
				r.Status = "empty_schema_installed"
			}
		}
	}
	for i := range records {
		r := &records[i]
		if r.Status != "" {
			continue
		}
		if strings.Contains(r.SQL, "@@") || strings.Contains(r.SQL, "FORCE INDEX") || strings.Contains(r.SQL, "OCTET_LENGTH") || strings.Contains(r.SQL, "DATABASE()") || strings.Contains(r.SQL, "JSON_VALID") {
			r.Status = "mysql_requires_separate_actual_plan"
			continue
		}
		if strings.Contains(r.SQL, "%s") || strings.Contains(r.SQL, "%d") {
			r.Status = "formatted_template_requires_rendering"
			continue
		}
		// Count unquoted positional placeholders; preparing the actual SQLite
		// plan then validates the statement. This is not a general SQL parser.
		n := internalSQLPlaceholders(r.SQL)
		args := make([]any, n)
		for i := range args {
			args[i] = int64(1)
		}
		rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+r.SQL, args...)
		if err != nil {
			r.Status, r.Error = "requires_manual_resolution", err.Error()
			continue
		}
		r.Status = "explained_empty_synthetic_schema"
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			r.Plan = append(r.Plan, detail)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	counts := map[string]int{}
	for _, r := range records {
		counts[r.Status]++
	}
	raw, err := json.MarshalIndent(struct {
		Warning string               `json:"warning"`
		Counts  map[string]int       `json:"counts"`
		Entries []internalPlanRecord `json:"entries"`
	}{"Static SQL expressions, not executed call-site coverage. Empty source-derived schemas; no triggers, production files, external connections or business SELECTs. Dynamic fragments, MySQL and GORM schemas remain explicit gaps. Error may mean an incomplete source fragment, not a product bug.", counts, records}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("local static plan counts=%v", counts)
}

func internalSQLPlaceholders(s string) int {
	n, quote := 0, byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				if i+1 < len(s) && s[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
		} else if c == '\'' || c == '"' || c == '`' {
			quote = c
		} else if c == '?' {
			n++
		}
	}
	return n
}
