// Command funcsize is a ratchet on function size: a function longer than
// maxLines lines or with more than maxBranches branch points must be listed in
// the baseline, and a listed function may not grow past its recorded size.
// Existing long functions can stay until they are refactored; no new ones
// can be added and none can get worse. Run from the repository root:
//
//	go run ./dev/funcsize                 # check (CI)
//	go run ./dev/funcsize -write          # rewrite the baseline after shrinking functions
//
// A function is keyed by package directory and Receiver.Name, so moving it to
// another file of the same package keeps its entry.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	maxLines    = 120
	maxBranches = 40
)

type size struct{ lines, branches int }

func main() {
	baseline := flag.String("baseline", "dev/function-size-baseline.txt", "baseline file")
	write := flag.Bool("write", false, "rewrite the baseline from the current code")
	flag.Parse()
	cur, err := measure(".")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *write {
		if err := writeBaseline(*baseline, cur); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Printf("baseline written: %d functions over %d lines or %d branches\n", len(oversized(cur)), maxLines, maxBranches)
		return
	}
	base, err := readBaseline(*baseline)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	failures, shrunk := check(base, cur)
	for _, s := range shrunk {
		fmt.Println("can shrink baseline:", s)
	}
	for _, f := range failures {
		fmt.Println("FAIL:", f)
	}
	if len(failures) > 0 {
		fmt.Printf("\n%d function(s) exceed the size ratchet (max %d lines / %d branches for new code; listed functions may not grow).\nSplit the function, or if it shrank elsewhere run: go run ./dev/funcsize -write\n", len(failures), maxLines, maxBranches)
		os.Exit(1)
	}
	fmt.Printf("function size ratchet ok: %d listed, %d can shrink\n", len(base), len(shrunk))
}

func over(s size) bool { return s.lines > maxLines || s.branches > maxBranches }

func oversized(cur map[string]size) map[string]size {
	out := map[string]size{}
	for k, s := range cur {
		if over(s) {
			out[k] = s
		}
	}
	return out
}

// check compares the current code with the baseline.
func check(base, cur map[string]size) (failures, shrunk []string) {
	for k, s := range cur {
		b, listed := base[k]
		switch {
		case !listed && over(s):
			failures = append(failures, fmt.Sprintf("%s is %d lines / %d branches (new code max %d / %d)", k, s.lines, s.branches, maxLines, maxBranches))
		case listed && (s.lines > b.lines || s.branches > b.branches):
			failures = append(failures, fmt.Sprintf("%s grew to %d lines / %d branches (baseline %d / %d)", k, s.lines, s.branches, b.lines, b.branches))
		case listed && (s.lines < b.lines || s.branches < b.branches):
			shrunk = append(shrunk, k)
		}
	}
	for k := range base {
		if _, ok := cur[k]; !ok {
			shrunk = append(shrunk, k+" (removed)")
		}
	}
	sort.Strings(failures)
	sort.Strings(shrunk)
	return failures, shrunk
}

func measure(root string) (map[string]size, error) {
	out := map[string]size{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); p != root && (strings.HasPrefix(n, ".") || n == "node_modules" || n == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			return err
		}
		dir := filepath.ToSlash(filepath.Dir(p))
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			key := dir + " " + funcName(fd)
			s := size{lines: fset.Position(fd.End()).Line - fset.Position(fd.Pos()).Line + 1}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch n.(type) {
				case *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt, *ast.CaseClause, *ast.CommClause:
					s.branches++
				}
				return true
			})
			if prev, dup := out[key]; dup { // e.g. build-tagged variants: keep the larger
				if prev.lines > s.lines {
					s.lines = prev.lines
				}
				if prev.branches > s.branches {
					s.branches = prev.branches
				}
			}
			out[key] = s
		}
		return nil
	})
	return out, err
}

func funcName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	t := fd.Recv.List[0].Type
	if s, ok := t.(*ast.StarExpr); ok {
		t = s.X
	}
	if ix, ok := t.(*ast.IndexExpr); ok {
		t = ix.X
	}
	if id, ok := t.(*ast.Ident); ok {
		return id.Name + "." + fd.Name.Name
	}
	return fd.Name.Name
}

func readBaseline(path string) (map[string]size, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string]size{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 4 {
			return nil, fmt.Errorf("bad baseline line: %q", line)
		}
		l, err1 := strconv.Atoi(parts[2])
		b, err2 := strconv.Atoi(parts[3])
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("bad baseline line: %q", line)
		}
		out[parts[0]+" "+parts[1]] = size{l, b}
	}
	return out, sc.Err()
}

func writeBaseline(path string, cur map[string]size) error {
	keys := make([]string, 0)
	for k := range oversized(cur) {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	fmt.Fprintf(&b, "# Function size ratchet baseline (dev/funcsize). Functions over %d lines or %d branch points.\n", maxLines, maxBranches)
	b.WriteString("# These may shrink or be removed but not grow; new code must stay under the limits.\n")
	b.WriteString("# package-dir function lines branches\n")
	for _, k := range keys {
		s := cur[k]
		fmt.Fprintf(&b, "%s %d %d\n", k, s.lines, s.branches)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
