// sql-audit inventories database call sites, not an invented count of fully
// rendered SQL variants. Unresolved builders remain uncovered until explained.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type entry struct {
	ID           string `json:"id"`
	File         string `json:"file"`
	Line         int    `json:"line"`
	Function     string `json:"function"`
	API          string `json:"api"`
	Receiver     string `json:"receiver"`
	Kind         string `json:"kind"`
	Operation    string `json:"operation"`
	Source       string `json:"source_hint"`
	Literal      string `json:"literal_template,omitempty"`
	Status       string `json:"status"`
	TypeIdentity string `json:"type_identity"`
}

func render(fs *token.FileSet, n ast.Node) string {
	var b bytes.Buffer
	_ = format.Node(&b, fs, n)
	return b.String()
}

func literal(n ast.Expr) string {
	switch v := n.(type) {
	case *ast.BasicLit:
		if v.Kind == token.STRING {
			s, _ := strconv.Unquote(v.Value)
			return s
		}
	case *ast.BinaryExpr:
		if v.Op == token.ADD {
			return literal(v.X) + " <dynamic> " + literal(v.Y)
		}
	case *ast.CallExpr:
		if len(v.Args) > 0 {
			return literal(v.Args[0])
		}
	}
	return ""
}

func main() {
	root := flag.String("root", ".", "repository root")
	out := flag.String("out", "", "generated inventory JSON")
	catalog := flag.String("mysql-catalog", "", "rendered SQL catalog; EXPLAIN only")
	offset := flag.Int("offset", 0, "first catalog case")
	limit := flag.Int("limit", 15, "maximum cases in one bounded batch")
	typed := flag.Bool("types", false, "resolve database methods using cached compiler type exports")
	flag.Parse()
	if *out == "" {
		panic("explicit output required")
	}
	if *catalog != "" {
		if err := explainMySQLCatalog(*catalog, *out, *offset, *limit); err != nil {
			fmt.Fprintln(os.Stderr, "SQL_EXPLAIN stopped; inspect retained report")
			os.Exit(1)
		}
		return
	}
	fs := token.NewFileSet()
	var knownTypes, dbTypes map[string]bool
	if *typed {
		var err error
		knownTypes, dbTypes, err = databaseMethodTypes(*root)
		if err != nil {
			panic(err)
		}
	}
	var entries []entry
	var files []string
	for _, dir := range []string{"monitor", "internal"} {
		err := filepath.WalkDir(filepath.Join(*root, dir), func(p string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if strings.HasSuffix(p, ".go") && !strings.HasSuffix(p, "_test.go") {
				files = append(files, p)
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	sort.Strings(files)
	for _, path := range files {
		entries = append(entries, inventoryFile(fs, path, *root, knownTypes, dbTypes)...)
	}
	counts := map[string]int{}
	for _, e := range entries {
		counts[e.Kind]++
	}
	payload := struct {
		Warning string         `json:"warning"`
		Files   int            `json:"files_scanned"`
		Counts  map[string]int `json:"candidate_counts"`
		Entries []entry        `json:"entries"`
	}{"Candidate call sites include unresolved receivers, builders and writes. Not a count of unique SQL; none is marked explained by inventory alone.", len(files), counts, entries}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0700); err != nil {
		panic(err)
	}
	if err := os.WriteFile(*out, append(data, '\n'), 0600); err != nil {
		panic(err)
	}
	fmt.Printf("files=%d candidates=%d counts=%v bytes=%d output=%s\n", len(files), len(entries), counts, len(data), *out)
}

func inventoryFile(fs *token.FileSet, path, root string, knownTypes, dbTypes map[string]bool) []entry {
	var entries []entry
	f, err := parser.ParseFile(fs, path, nil, 0)
	if err != nil {
		panic(err)
	}
	rel, _ := filepath.Rel(root, path)
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		name := fn.Name.Name
		if fn.Recv != nil && len(fn.Recv.List) > 0 {
			name = render(fs, fn.Recv.List[0].Type) + "." + name
		}
		decoders := inventoryDecoders(fn.Body)
		ordinal := map[string]int{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			s, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			api, kind, op := s.Sel.Name, "", "unknown"
			switch api {
			case "QueryContext", "QueryRowContext", "Query", "QueryRow", "PrepareContext", "Prepare":
				kind = "database_sql_candidate"
			case "ExecContext", "Exec":
				kind = "execute_candidate"
			case "Raw":
				kind = "raw_builder"
			case "Find", "First", "Take", "Last", "Count", "Scan", "Pluck", "Rows", "Row":
				if api == "Scan" {
					if parent, ok := s.X.(*ast.CallExpr); ok {
						if ps, ok := parent.Fun.(*ast.SelectorExpr); ok {
							switch ps.Sel.Name {
							case "QueryRowContext", "QueryRow", "Row":
								return true
							}
						}
					}
				}
				if id, ok := s.X.(*ast.Ident); ok && decoders[id.Name] {
					return true
				}
				kind, op = "orm_read_candidate", "SELECT"
			case "Create", "CreateInBatches", "Save", "Updates", "Update", "UpdateColumn", "UpdateColumns", "Delete":
				kind = "orm_write_candidate"
			default:
				return true
			}
			recv := render(fs, s.X)
			pos := fs.Position(s.Sel.Pos())
			typeKey := fmt.Sprintf("%s:%d:%d", rel, pos.Line, pos.Column)
			identity := "unresolved"
			if knownTypes[typeKey] {
				if !dbTypes[typeKey] {
					return true
				}
				identity = "database_method_confirmed"
			}
			if recv == "fmt" || recv == "json" || recv == "os" || recv == "io" {
				return true
			}
			if api == "Raw" && len(call.Args) > 0 {
				if _, ok := call.Args[0].(*ast.FuncLit); ok {
					kind, op = "connection_access", "METADATA"
				}
			}
			text := ""
			if len(call.Args) > 0 {
				at := 0
				if strings.HasSuffix(api, "Context") && len(call.Args) > 1 {
					at = 1
				}
				text = literal(call.Args[at])
			}
			fields := strings.Fields(strings.TrimSpace(text))
			if len(fields) > 0 {
				first := strings.ToUpper(fields[0])
				switch first {
				case "SELECT", "WITH", "EXPLAIN", "INSERT", "UPDATE", "DELETE", "REPLACE", "PRAGMA", "CREATE", "ALTER", "DROP", "ATTACH", "DETACH", "VACUUM":
					op = first
				}
			}
			source := "unresolved"
			if strings.Contains(recv, "prodDB") {
				source = "mysql_replica"
			} else if strings.Contains(recv, "storeDB") || strings.Contains(recv, "usageFacts") || strings.Contains(recv, "FactsReadStore") || strings.Contains(recv, "factsDB") || strings.Contains(recv, "tokenStore") {
				source = "local_sqlite"
			}
			key := rel + "|" + name + "|" + api + "|" + render(fs, call)
			ordinal[key]++
			h := sha256.Sum256([]byte(fmt.Sprintf("%s|%d", key, ordinal[key])))
			entries = append(entries, entry{ID: hex.EncodeToString(h[:12]), File: rel, Line: fs.Position(s.Sel.Pos()).Line, Function: name, API: api, Receiver: recv, Kind: kind, Operation: op, Source: source, Literal: text, Status: "unreviewed", TypeIdentity: identity})
			return true
		})
	}
	return entries
}

func inventoryDecoders(body *ast.BlockStmt) map[string]bool {
	decoders := map[string]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		a, ok := n.(*ast.AssignStmt)
		if !ok || len(a.Lhs) == 0 || len(a.Rhs) != 1 {
			return true
		}
		call, ok := a.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		s, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch s.Sel.Name {
		case "QueryContext", "Query", "QueryRowContext", "QueryRow", "Rows", "Row", "NewScanner":
			if id, ok := a.Lhs[0].(*ast.Ident); ok {
				decoders[id.Name] = true
			}
		}
		return true
	})
	return decoders
}
