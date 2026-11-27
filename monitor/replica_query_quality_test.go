package monitor

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// MySQL optimizer hints belong after SELECT, not before it. Check SQL literals
// across both the Monitor and delegated monthly source implementations. Plain
// hint fragments used by a builder are fine; a complete misplaced SELECT is not.
func TestReplicaSQLExecutionHintsAreAttachedToSelect(t *testing.T) {
	misplaced := regexp.MustCompile(`(?is)/\*\+\s*MAX_EXECUTION_TIME\([^)]*\)[^*]*\*/\s*SELECT\b`)
	for _, dir := range []string{".", "../internal/monthlysource"} {
		files, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".go") || strings.HasSuffix(file.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(dir, file.Name())
			positions := token.NewFileSet()
			parsed, err := parser.ParseFile(positions, path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			ast.Inspect(parsed, func(n ast.Node) bool {
				literal, ok := n.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatal(err)
				}
				if misplaced.MatchString(value) {
					t.Errorf("optimizer hint before SELECT at %s", positions.Position(literal.Pos()))
				}
				return true
			})
		}
	}
}

func TestReplicaFinanceLogReadsHaveServerBudgets(t *testing.T) {
	boundary, _ := financeGiftBoundarySQL(1)
	for name, query := range map[string]string{
		"credit hour":   financeCreditHourSQL(),
		"gift boundary": boundary,
		"user hour":     financeUserHourSQL(),
	} {
		if !strings.HasPrefix(strings.TrimSpace(query), "SELECT /*+ MAX_EXECUTION_TIME(8000) */") {
			t.Errorf("%s missing effective source statement budget", name)
		}
	}
}

// This remains test-only: the measured FRT candidate was not faster, so the
// production FRT query keeps its original shape. Do not re-enable by analogy.
func replicaFRTParseOnceCandidateSQL() string {
	return logsParseOtherOnceWithColumns(frtRecomputeWindowSQL(),
		"created_at, type, user_id, channel_id, model_name, `group`, is_stream")
}

func TestReplicaTokenAndFRTAggregatesParseOnce(t *testing.T) {
	_, end := modelFRTFixture(t, true)
	db := modelFRTSQLiteSource(t)
	insertParseOnceFuzzLogs(t, db, end, 600, sampleWindowAggregateSQL(true, false))
	for _, tc := range []struct {
		name, plain, optimized string
	}{
		{"token", sampleTokenAggregateSQL(), sampleTokenSQL()},
		{"FRT candidate disabled", frtRecomputeWindowSQL(), replicaFRTParseOnceCandidateSQL()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if strings.Count(tc.optimized, "?") != 2 || !strings.Contains(tc.optimized, "MAX_EXECUTION_TIME(8000) NO_MERGE(logs)") {
				t.Fatal("time parameters or effective timeout changed")
			}
			if strings.Count(tc.optimized, "JSON_VALID(other)") != 2 || strings.Count(tc.optimized, "JSON_EXTRACT(other,") != 1 {
				t.Fatal("raw JSON must only be parsed in the derived projection")
			}
			// The giant payload and columns unrelated to each aggregate should
			// not be copied into the materialized temporary table.
			inner := tc.optimized[strings.Index(tc.optimized, "\nFROM ("):]
			if strings.Contains(inner, "SELECT "+logsAggregateSourceColumns) || strings.Contains(inner, "SELECT *") {
				t.Fatal("aggregate projection widened")
			}
			plain := readSampleRows(t, db, tc.plain, end-600, end)
			once := readSampleRows(t, db, tc.optimized, end-600, end)
			if len(plain) == 0 || strings.Join(plain, "\n") != strings.Join(once, "\n") {
				t.Fatalf("plain vs rewritten result differs (%d vs %d groups)", len(plain), len(once))
			}
		})
	}
}

func TestReplicaHistorySortIsLocalAndControlsStayIndependent(t *testing.T) {
	_, end := modelFRTFixture(t, true)
	db := modelFRTSQLiteSource(t)
	insertParseOnceFuzzLogs(t, db, end, 600, sampleWindowAggregateSQL(true, false))
	for _, tc := range []struct{ name, query, order string }{
		{"detail", usageFactHistoryDetailSQL("CAST((created_at+28800)/86400 AS INTEGER)", "user_id IN (1,2,3,4)", channelTestLogPredicateSQL()), "day_idx, user_id, channel_id, `group`, model_name, token_id"},
		{"control", usageFactHistoryControlSQL("CAST((created_at+28800)/86400 AS INTEGER)", "user_id IN (1,2,3,4)", channelTestLogPredicateSQL()), "day_idx, user_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if strings.Contains(tc.query, "ORDER BY") || !strings.Contains(tc.query, "FROM logs") || !strings.Contains(tc.query, "GROUP BY") || !strings.HasSuffix(tc.query, "LIMIT ?") {
				t.Fatal("must retain an independent bounded raw-source aggregate, without redundant server sorting")
			}
			q := strings.Replace(tc.query, "LIMIT ?", "LIMIT 10000", 1)
			old := strings.Replace(q, "LIMIT 10000", "ORDER BY "+tc.order+" LIMIT 10000", 1)
			if strings.Join(readSampleRows(t, db, q, end-600, end), "\n") != strings.Join(readSampleRows(t, db, old, end-600, end), "\n") {
				t.Fatal("removing server sort changed complete source groups")
			}
		})
	}
}
