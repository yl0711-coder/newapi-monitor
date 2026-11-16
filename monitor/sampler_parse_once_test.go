package monitor

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var sampleWindowVariants = []struct {
	name                    string
	includeUser, healthOnly bool
	build                   func() string
}{
	{"plain", false, false, sampleWindowSQL},
	{"user", true, false, sampleWindowUserSQL},
	{"health", true, true, sampleCustomerHealthWindowSQL},
}

// The production aggregation must parse `other` once per row: every
// classification predicate reads the parsed value from the derived table.
func TestSampleWindowParsesOtherOnce(t *testing.T) {
	bare := regexp.MustCompile(`\bother\b`)
	for _, v := range sampleWindowVariants {
		q := v.build()
		if q != sampleWindowParseOtherOnce(sampleWindowAggregateSQL(v.includeUser, v.healthOnly)) {
			t.Fatalf("%s: production SQL must be the plain aggregation with only the parse-once rewrite", v.name)
		}
		if n := strings.Count(q, "?"); n != 2 {
			t.Fatalf("%s: placeholders = %d, want 2 (fromTs, toTs)", v.name, n)
		}
		for _, want := range []string{
			"SELECT /*+ MAX_EXECUTION_TIME(8000) NO_MERGE(logs) */",
			"JSON_VALID(other) AS other_valid",
			"CASE WHEN JSON_VALID(other) THEN JSON_EXTRACT(other,'$') END AS other_json",
			"WHERE created_at >= ? AND created_at < ? AND type IN (2,5,6)",
			"AND NOT (" + channelTestLogPredicateSQL() + ")",
			"\n) logs\nGROUP BY bucket, channel_id, model_name, grp",
		} {
			if !strings.Contains(q, want) {
				t.Fatalf("%s: missing %q", v.name, want)
			}
		}
		if strings.Count(q, "JSON_VALID(other)") != 2 || strings.Count(q, "JSON_EXTRACT(other,") != 1 {
			t.Fatalf("%s: `other` must be validated/parsed only inside the derived table", v.name)
		}
		from := strings.Index(q, "\nFROM (")
		if from < 0 {
			t.Fatalf("%s: derived table missing", v.name)
		}
		outer := q[:from]
		if refs := bare.FindAllString(outer, -1); len(refs) != 0 {
			t.Fatalf("%s: outer aggregation still reads raw other %d times", v.name, len(refs))
		}
		if !strings.Contains(outer, "other_valid") || !strings.Contains(outer, "JSON_EXTRACT(other_json,") {
			t.Fatalf("%s: predicates must read the parsed columns", v.name)
		}
		if strings.Contains(q, "\nWHERE created_at >= ? AND created_at < ? AND type IN (2,5,6)\n  AND NOT (") {
			t.Fatalf("%s: window filter must move into the derived table", v.name)
		}
	}
	if got := sampleWindowParseOtherOnce("SELECT 1"); got != "SELECT 1" {
		t.Fatalf("unknown shapes must be left untouched, got %q", got)
	}
}

// Rewritten and plain aggregations must return identical rows for arbitrary
// `other` / content / model values, including invalid JSON, JSON null,
// numbers stored as strings and missing keys.
func TestSampleWindowParseOnceMatchesPlainAggregation(t *testing.T) {
	_, end := modelFRTFixture(t, true)
	db := modelFRTSQLiteSource(t)

	insertParseOnceFuzzLogs(t, db, end, 600, sampleWindowAggregateSQL(true, false))
	for _, v := range sampleWindowVariants {
		plain := readSampleRows(t, db, sampleWindowAggregateSQL(v.includeUser, v.healthOnly), end-600, end)
		once := readSampleRows(t, db, v.build(), end-600, end)
		if len(plain) < 50 {
			t.Fatalf("%s: fixture produced only %d groups", v.name, len(plain))
		}
		if v.name == "user" {
			sums := map[string]int{}
			for _, row := range plain {
				for _, kv := range strings.Split(row, "|") {
					k, val, _ := strings.Cut(kv, "=")
					var n int
					if _, err := fmt.Sscan(val, &n); err == nil {
						sums[k] += n
					}
				}
			}
			for _, col := range []string{"success", "anomaly", "failed", "anomaly_stream", "customer_health_anomaly", "err_4xx", "err_5xx", "err_timeout", "ttft_observed", "refund_records"} {
				if sums[col] == 0 {
					t.Fatalf("fixture never exercises %s (sums=%v)", col, sums)
				}
			}
		}
		if strings.Join(plain, "\n") != strings.Join(once, "\n") {
			for i := range plain {
				if i >= len(once) || plain[i] != once[i] {
					t.Fatalf("%s: group %d differs\nplain: %s\nonce:  %s", v.name, i, plain[i], safeIndex(once, i))
				}
			}
			t.Fatalf("%s: %d plain groups vs %d rewritten", v.name, len(plain), len(once))
		}
	}
}

// insertParseOnceFuzzLogs fills the fake source with rows built from the JSON
// paths, literals and LIKE patterns of raw, so every classification branch of
// the aggregation is exercised with clean, dirty and missing values.
func insertParseOnceFuzzLogs(t *testing.T, db *sql.DB, end, span int64, raw string) {
	t.Helper()
	paths := uniqueMatches(raw, `JSON_EXTRACT\(other,'([^']+)'\)`)
	literals := uniqueMatches(raw, `'([^'%]{1,40})'`)
	likes := uniqueMatches(raw, `LIKE '([^']+)'`)
	if len(paths) < 3 || len(literals) < 5 {
		t.Fatalf("fixture extraction failed: paths=%v literals=%d", paths, len(literals))
	}
	r := rand.New(rand.NewPCG(20261116, 61))
	pick := func(xs []string) string { return xs[r.IntN(len(xs))] }
	likeText := func() string { return strings.ReplaceAll(pick(likes), "%", pick([]string{"", "x", " upstream "})) }
	jsonValue := func() any {
		switch r.IntN(9) {
		case 0:
			return nil
		case 1:
			return r.IntN(20000)
		case 2:
			return fmt.Sprint(r.IntN(20000))
		case 3:
			return -1
		case 4:
			return true
		default:
			return pick(literals)
		}
	}
	for i := 0; i < 1500; i++ {
		var other string
		switch r.IntN(12) {
		case 0:
			other = "not-json"
		case 1:
			other = "null"
		case 2:
			other = ""
		case 3:
			other = `"text"`
		default:
			obj := map[string]any{}
			for _, p := range paths {
				if r.IntN(3) == 0 {
					continue
				}
				setJSONPath(obj, p, jsonValue())
			}
			b, _ := json.Marshal(obj)
			other = string(b)
		}
		typ := []int{2, 2, 2, 5, 6}[r.IntN(5)]
		content := ""
		switch r.IntN(4) {
		case 0:
			content = likeText()
		case 1:
			content = pick([]string{"upstream status_code=429 rate limited", "bad response status_code=502", "status_code=400 invalid", "status_code=503 timeout"})
		}
		model := pick([]string{"gpt-5", "claude-opus", "gemini-pro", "deepseek"})
		if r.IntN(4) == 0 {
			model = likeText()
		}
		tokenName, requestID := "tok", fmt.Sprintf("req-%d", i)
		if i%97 == 0 {
			tokenName, content = "模型测试", "模型测试"
		}
		if _, err := db.Exec("INSERT INTO logs(id,user_id,username,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,use_time,`group`,is_stream,content,token_name,token_id,request_id,other) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			i+1, 1+r.IntN(4), fmt.Sprintf("u%d", r.IntN(4)), 1+r.IntN(5), end-span+int64(r.IntN(int(span))), typ, model,
			r.IntN(3)*1000, r.IntN(500), r.IntN(3)*r.IntN(300), r.IntN(90), fmt.Sprintf("g%d", r.IntN(3)), r.IntN(2), content, tokenName, r.IntN(3), requestID, other); err != nil {
			t.Fatal(err)
		}
	}
}

func stabilityRangePlainSQLite(maxExecutionMS int) string {
	q := stabilityHourSQL()
	q = strings.Replace(q, "SELECT channel_id,", fmt.Sprintf("SELECT /*+ MAX_EXECUTION_TIME(%d) */ (created_at DIV 3600) * 3600 AS hour_ts, channel_id,", maxExecutionMS), 1)
	q = strings.Replace(q, "GROUP BY channel_id,", "GROUP BY hour_ts, channel_id,", 1)
	q += fmt.Sprintf("\nLIMIT %d", maxStabilityRowsPerRange+1)
	q = strings.ReplaceAll(q, "(created_at DIV 3600) * 3600", "CAST(created_at / 3600 AS INTEGER) * 3600")
	return stabilitySQLiteSQL(q)
}

// The stability hour aggregation gets the same once-per-row parsing.
func TestStabilityRangeParsesOtherOnce(t *testing.T) {
	q := stabilityRangeSQL()
	for _, want := range []string{
		"SELECT /*+ MAX_EXECUTION_TIME(8000) NO_MERGE(logs) */ (created_at DIV 3600) * 3600 AS hour_ts",
		"WHERE created_at >= ? AND created_at < ? AND type IN (2,5,6)",
		"\n) logs\nGROUP BY hour_ts, channel_id, model_name, grp, is_channel_test",
		fmt.Sprintf("\nLIMIT %d", maxStabilityRowsPerRange+1),
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("missing %q", want)
		}
	}
	if strings.Count(q, "?") != 2 || strings.Count(q, "JSON_VALID(other)") != 2 || strings.Count(q, "JSON_EXTRACT(other,") != 1 {
		t.Fatal("stability range must keep two placeholders and parse other only in the derived table")
	}
	if !strings.Contains(stabilityRangeSQLWithMaxExecution(3000), "MAX_EXECUTION_TIME(3000) NO_MERGE(logs) */") {
		t.Fatal("configured server cap must survive the rewrite")
	}
	if strings.Contains(stabilityRangeSQLiteSQLWithMaxExecution(8000), "JSON_EXTRACT(other,'$')") {
		t.Fatal("SQLite variant must map whole-document extraction to json()")
	}
}

func TestStabilityRangeParseOnceMatchesPlainAggregation(t *testing.T) {
	_, end := modelFRTFixture(t, true)
	db := modelFRTSQLiteSource(t)
	end -= end % 3600
	insertParseOnceFuzzLogs(t, db, end, 7200, stabilityHourSQL())
	plain := readSampleRows(t, db, stabilityRangePlainSQLite(8000), end-7200, end)
	once := readSampleRows(t, db, stabilityRangeSQLiteSQLWithMaxExecution(8000), end-7200, end)
	if len(plain) < 20 {
		t.Fatalf("fixture produced only %d groups", len(plain))
	}
	if strings.Join(plain, "\n") != strings.Join(once, "\n") {
		for i := range plain {
			if i >= len(once) || plain[i] != once[i] {
				t.Fatalf("group %d differs\nplain: %s\nonce:  %s", i, plain[i], safeIndex(once, i))
			}
		}
		t.Fatalf("%d plain groups vs %d rewritten", len(plain), len(once))
	}
}

func modelFRTSQLiteSource(t *testing.T) *sql.DB {
	t.Helper()
	source := newFakeProdDB(t)
	var id int
	var name, path string
	if err := source.QueryRow("PRAGMA database_list").Scan(&id, &name, &path); err != nil {
		t.Fatal(err)
	}
	driverName := fmt.Sprintf("parse-once-%d", usageCountingDriverID.Add(1))
	sql.Register(driverName, modelFRTSQLDriver{source.Driver()})
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func readSampleRows(t *testing.T, db *sql.DB, q string, from, to int64) []string {
	t.Helper()
	rows, err := db.Query(q, from, to)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(cols))
		for i, v := range vals {
			parts[i] = cols[i] + "=" + v.String
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func uniqueMatches(s, pattern string) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range regexp.MustCompile(pattern).FindAllStringSubmatch(s, -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			out = append(out, m[1])
		}
	}
	sort.Strings(out)
	return out
}

// setJSONPath writes value at a simple MySQL path like $.a or $.a.b.
func setJSONPath(obj map[string]any, path string, value any) {
	keys := strings.Split(strings.TrimPrefix(path, "$."), ".")
	for i, k := range keys {
		k = strings.Trim(k, `"`)
		if i == len(keys)-1 {
			obj[k] = value
			return
		}
		next, ok := obj[k].(map[string]any)
		if !ok {
			next = map[string]any{}
			obj[k] = next
		}
		obj = next
	}
}

func safeIndex(xs []string, i int) string {
	if i < len(xs) {
		return xs[i]
	}
	return "<missing>"
}

// Raw-logs usage fallbacks must carry a server-side cap below their client
// deadline; otherwise an abandoned statement keeps running on the replica.
func TestUsageRawFallbackQueriesAreServerCapped(t *testing.T) {
	src, err := os.ReadFile("usage.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if n := strings.Count(code, "usageRawSelect20s +"); n != 4 {
		t.Fatalf("20 s raw usage aggregations using the cap = %d, want 4 (daily, dimensions, daily-model, matrix)", n)
	}
	if n := strings.Count(code, "usageRawSelect15s +"); n != 1 {
		t.Fatalf("token aggregation must use the 15 s cap, got %d", n)
	}
	for _, unguarded := range []string{`"SELECT " + m.dayExpr()`, `"SELECT user_id, " + m.dayExpr()`, `"SELECT token_id, COALESCE(MAX(token_name)`, `"SELECT " + dim.col`} {
		if strings.Contains(code, unguarded) {
			t.Fatalf("uncapped raw usage query remains: %s", unguarded)
		}
	}
	if !strings.Contains(usageRawSelect20s, "MAX_EXECUTION_TIME(18000)") || !strings.Contains(usageRawSelect15s, "MAX_EXECUTION_TIME(13000)") {
		t.Fatal("caps must stay below the 20 s / 15 s client deadlines and the 20 s driver readTimeout")
	}
}
