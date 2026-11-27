package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Use compiler type identity, not just method spelling: gin.Context.Query and
// url.Values.Set must never be counted as database queries.
func databaseMethodTypes(root string) (map[string]bool, map[string]bool, error) {
	var err error
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, nil, err
	}
	cmd := exec.Command("go", "list", "-deps", "-export", "-json", "./monitor", "./internal/...")
	cmd.Dir = root
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, nil, fmt.Errorf("type inventory dependency export failed: %w", err)
	}
	type pkg struct {
		ImportPath, Dir, Export string
		GoFiles                 []string
	}
	var packages []pkg
	exports := map[string]string{}
	dec := json.NewDecoder(&stdout)
	for {
		var p pkg
		if err := dec.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, err
		}
		exports[p.ImportPath] = p.Export
		if strings.HasPrefix(p.ImportPath, "github.com/yl0711-coder/newapi-monitor/monitor") || strings.HasPrefix(p.ImportPath, "github.com/yl0711-coder/newapi-monitor/internal/") {
			packages = append(packages, p)
		}
	}
	known, database := map[string]bool{}, map[string]bool{}
	for _, p := range packages {
		fs := token.NewFileSet()
		var files []*ast.File
		for _, name := range p.GoFiles {
			f, err := parser.ParseFile(fs, filepath.Join(p.Dir, name), nil, 0)
			if err != nil {
				return nil, nil, err
			}
			files = append(files, f)
		}
		info := &types.Info{Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
		imp := importer.ForCompiler(fs, "gc", func(path string) (io.ReadCloser, error) { return os.Open(exports[path]) })
		cfg := types.Config{Importer: imp}
		if _, err := cfg.Check(p.ImportPath, fs, files, info); err != nil {
			return nil, nil, fmt.Errorf("type inventory failed for %s: %w", p.ImportPath, err)
		}
		for _, f := range files {
			ast.Inspect(f, func(n ast.Node) bool {
				s, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				obj := info.Uses[s.Sel]
				if obj == nil {
					return true
				}
				pos := fs.Position(s.Sel.Pos())
				rel, _ := filepath.Rel(root, pos.Filename)
				key := fmt.Sprintf("%s:%d:%d", rel, pos.Line, pos.Column)
				known[key] = true
				// Row.Scan/Rows.Scan decode an already executed result; they do
				// not issue another SQL statement. GORM Scan is different.
				if sqlResultDecoder(obj) {
					return true
				}
				if obj.Pkg() != nil && (obj.Pkg().Path() == "database/sql" || obj.Pkg().Path() == "gorm.io/gorm") {
					database[key] = true
					return true
				}
				// Application interfaces wrap sql.DB but retain its result types.
				if sig, ok := obj.Type().(*types.Signature); ok && sig.Results() != nil {
					for i := 0; i < sig.Results().Len(); i++ {
						typ := types.TypeString(sig.Results().At(i).Type(), func(p *types.Package) string { return p.Path() })
						if typ == "*database/sql.Rows" || typ == "*database/sql.Row" || typ == "database/sql.Result" || typ == "*database/sql.Stmt" {
							database[key] = true
						}
					}
				}
				return true
			})
		}
	}
	return known, database, nil
}

func sqlResultDecoder(obj types.Object) bool {
	return obj != nil && obj.Pkg() != nil && obj.Pkg().Path() == "database/sql" && obj.Name() == "Scan"
}
