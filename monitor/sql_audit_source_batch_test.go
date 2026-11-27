package monitor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type auditXMLField struct {
	Name string `xml:"name,attr"`
	Nil  bool   `xml:"nil,attr"`
	Text string `xml:",chardata"`
}
type auditXMLResult struct {
	Rows []struct {
		Fields []auditXMLField `xml:"field"`
	} `xml:"row"`
}

// XML, unlike batch TSV, preserves SQL NULL versus the literal string NULL,
// and real tabs/newlines. Returned source rows are NOT persisted in reports.
func auditXMLRows(raw []byte) (*explainCatalogRows, error) {
	var x auditXMLResult
	if e := xml.Unmarshal(raw, &x); e != nil {
		return nil, e
	}
	r := &explainCatalogRows{}
	for i, row := range x.Rows {
		if i == 0 {
			for _, f := range row.Fields {
				r.columns = append(r.columns, f.Name)
			}
		}
		if len(row.Fields) != len(r.columns) {
			return nil, errors.New("inconsistent XML row width")
		}
		var values []driver.Value
		for j, f := range row.Fields {
			if f.Name != r.columns[j] {
				return nil, errors.New("inconsistent XML columns")
			}
			if f.Nil {
				values = append(values, nil)
			} else {
				values = append(values, f.Text)
			}
		}
		r.values = append(r.values, values)
	}
	return r, nil
}

func TestSQLAuditXMLRowsPreserveNullAndText(t *testing.T) {
	r, e := auditXMLRows([]byte(`<resultset xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><row><field name="a" xsi:nil="true"/><field name="b">NULL</field><field name="c">a&#10;b&#9;&amp;&lt;</field></row></resultset>`))
	if e != nil || r.values[0][0] != nil || r.values[0][1] != "NULL" || r.values[0][2] != "a\nb\t&<" {
		t.Fatal("XML lost source values", r, e)
	}
}

type sourceAuditMySQLDriver struct {
	t         *testing.T
	container string
	queries   *[]sourceAuditQuery
}
type sourceAuditQuery struct {
	SQL      string
	Args     []any
	Line     int
	Function string
}
type sourceAuditMySQLConn struct{ d sourceAuditMySQLDriver }

func (d sourceAuditMySQLDriver) Open(string) (driver.Conn, error) {
	return sourceAuditMySQLConn{d}, nil
}
func (c sourceAuditMySQLConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("audit only supports contextual reads")
}
func (c sourceAuditMySQLConn) Close() error { return nil }
func (c sourceAuditMySQLConn) Begin() (driver.Tx, error) {
	return nil, errors.New("CLI audit does not emulate transaction snapshots")
}

func auditDirectSourceCaller() (string, int, string) {
	pcs := make([]uintptr, 40)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.Contains(f.File, "/monitor/") {
			if filepath.Base(f.File) == "sql_audit_source_batch_test.go" { /* explicit recorder frames only */
			} else if strings.HasSuffix(f.File, "_test.go") {
				return "", 0, ""
			} else {
				return "monitor/" + filepath.Base(f.File), f.Line, f.Function
			}
		}
		if !more {
			break
		}
	}
	return "", 0, ""
}

func (c sourceAuditMySQLConn) readXML(ctx context.Context, q string) (*explainCatalogRows, error) {
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", c.d.container, "mysql", "--protocol=socket", "-uroot", "--xml", "--raw")
	cmd.Stdin = strings.NewReader("USE sql_source_audit;" + q + ";")
	var out, diagnostic bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &diagnostic
	if e := cmd.Run(); e != nil {
		return nil, fmt.Errorf("isolated source read: %w: %s", e, diagnostic.String())
	}
	if out.Len() > 8<<20 {
		return nil, errors.New("XML result exceeds test budget")
	}
	return auditXMLRows(out.Bytes())
}

func (c sourceAuditMySQLConn) QueryContext(ctx context.Context, q string, named []driver.NamedValue) (driver.Rows, error) {
	args := make([]any, len(named))
	for i, a := range named {
		args[i] = a.Value
	}
	bound := usageLogsSyntheticBind(c.d.t, q, args)
	file, line, fn := auditDirectSourceCaller()
	if file == "" {
		return nil, errors.New("source read without actual application caller")
	}
	p, e := c.readXML(ctx, "EXPLAIN FORMAT=JSON "+bound)
	if e != nil {
		return nil, e
	}
	if len(p.values) != 1 || len(p.values[0]) != 1 {
		return nil, errors.New("missing MySQL plan")
	}
	plan, ok := p.values[0][0].(string)
	if !ok || !json.Valid([]byte(plan)) {
		return nil, errors.New("invalid MySQL plan")
	}
	r, e := c.readXML(ctx, bound)
	if e != nil {
		return nil, e
	}
	if c.d.queries != nil {
		*c.d.queries = append(*c.d.queries, sourceAuditQuery{q, append([]any{}, args...), line, fn})
	}
	if activeSQLPlanCapture != nil {
		capture := activeSQLPlanCapture
		if len(capture.onlyFiles) == 0 || capture.onlyFiles[file] {
			h := sha256.Sum256([]byte(q))
			item := capturedSQLPlan{Fixture: c.d.t.Name(), Store: "mysql-synthetic", File: file, Line: line, Function: fn, Fingerprint: fmt.Sprintf("%x", h), Template: auditStringLiteral.ReplaceAllString(q, "'<redacted>'"), Parameters: len(args), Status: "explained", Plan: []string{plan}}
			for _, a := range args {
				item.Shape = append(item.Shape, fmt.Sprintf("%T", a))
			}
			key := fmt.Sprintf("%s:%d|%s|%v", file, line, item.Fingerprint, item.Shape)
			capture.mu.Lock()
			if !capture.seen[key] {
				capture.seen[key] = true
				capture.items = append(capture.items, item)
			}
			capture.mu.Unlock()
		}
	}
	return r, nil
}

func TestSQLAuditUsageGovernanceSourceCallsMySQL(t *testing.T) {
	if activeSQLPlanCapture == nil {
		t.Skip("requires bounded source audit report")
	}
	run := isolatedSQLOptimizationRunner(t)
	run(`DROP DATABASE IF EXISTS sql_source_audit;CREATE DATABASE sql_source_audit;USE sql_source_audit;
CREATE TABLE users(id BIGINT PRIMARY KEY,username VARCHAR(80),display_name VARCHAR(80),email VARCHAR(80),quota BIGINT,used_quota BIGINT,created_at BIGINT,status INT,role INT,` + "`group`" + ` VARCHAR(80),deleted_at DATETIME NULL);
INSERT INTO users VALUES(201,'one','One','one@example.invalid',1000,100,100,1,1,'g1',NULL),(202,'two','Two','two@example.invalid',2000,200,100,1,1,'g2',NULL),(203,'blank','','',0,0,100,1,1,' ',NULL);
CREATE TABLE tokens(id BIGINT PRIMARY KEY,user_id BIGINT,name VARCHAR(80),` + "`key`" + ` VARCHAR(80),` + "`group`" + ` VARCHAR(80),used_quota BIGINT,status INT,expired_time BIGINT,deleted_at DATETIME NULL,INDEX idx_tokens_user_id(user_id));
INSERT INTO tokens VALUES(1,201,'tok1','synthetic-only-abcdefghijkl','g1',10,1,-1,NULL),(2,201,'tok2','synthetic-only-abcdefghijkl','g2',20,2,1,NULL),(3,202,'tok3','synthetic-only-abcdefghijkl','g1',30,1,-1,NULL);
CREATE TABLE options(` + "`key`" + ` VARCHAR(80) PRIMARY KEY,` + "`value`" + ` TEXT);
INSERT INTO options VALUES('GroupRatio','{"g1":1,"g2":2}'),('UserUsableGroups','{"g1":"One"}'),('AutoGroups','["g1"]'),('GroupGroupRatio','{}'),('TopupGroupRatio','{}'),('ModelRequestRateLimitGroup','{}');
CREATE TABLE subscription_plans(id BIGINT PRIMARY KEY,title VARCHAR(80),enabled BOOL,upgrade_group VARCHAR(80));INSERT INTO subscription_plans VALUES(1,'Plan',1,'g1');
CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,user_id BIGINT,username VARCHAR(80),token_id BIGINT,token_name VARCHAR(80),model_name VARCHAR(80),` + "`group`" + ` VARCHAR(80),channel_id BIGINT,quota BIGINT,prompt_tokens BIGINT,completion_tokens BIGINT,use_time BIGINT,is_stream BOOL,content TEXT,other TEXT,request_id VARCHAR(80),upstream_request_id VARCHAR(80),ip VARCHAR(80),
INDEX idx_user_created_type(user_id,created_at,type),INDEX idx_logs_user_group_created_type(user_id,` + "`group`" + `,created_at,type),INDEX idx_created_at_type(created_at,type),INDEX idx_logs_token_id(token_id),INDEX idx_logs_request_id(request_id));`)
	from := time.Date(2026, 11, 20, 0, 0, 0, 0, usageCST).Unix()
	to := from + 2*86400
	for _, r := range []struct {
		id, at, uid, typ, quota, token, p, c int64
		model, group                         string
	}{
		{1, from, 201, 2, 10, 1, 3, 7, "m1", "g1"}, {2, from + 1, 201, 2, 20, 2, 2, 3, "m2", "g2"}, {3, from + 2, 201, 6, 5, 2, 0, 0, "m2", "g2"}, {4, from + 86401, 202, 2, 30, 3, 5, 4, "m1", "g1"}, {5, from + 3, 201, 5, 0, 1, 0, 0, "m1", "g1"}, {7, from - 1, 201, 2, 777, 1, 0, 0, "m1", "g1"}, {8, to, 201, 2, 888, 1, 0, 0, "m1", "g1"},
	} {
		run(fmt.Sprintf("USE sql_source_audit;INSERT INTO logs VALUES(%d,%d,%d,%d,'owner',%d,'tok','%s','%s',7,%d,%d,%d,1,0,'','{}','r%d','up%d','127.0.0.1');", r.id, r.at, r.typ, r.uid, r.token, r.model, r.group, r.quota, r.p, r.c, r.id, r.id))
	}
	name := fmt.Sprintf("source-audit-mysql-%d", usageQuotaDriverID.Add(1))
	run(fmt.Sprintf("USE sql_source_audit;INSERT INTO logs(id,created_at,type,user_id,username,token_id,token_name,model_name,`group`,quota,content,other) VALUES(6,%d,NULL,202,'two',3,'tok','m1','g1',NULL,'','{}');", from+3))
	run(`USE sql_source_audit;UPDATE logs SET content='literal 50%_!' WHERE id=2;UPDATE logs SET other='{"violation_fee_code":"synthetic"}' WHERE id=5;`)
	// Large unrelated traffic must not change selected customers' results.
	run(fmt.Sprintf("USE sql_source_audit;INSERT INTO logs(id,created_at,type,user_id,username,token_id,token_name,model_name,`group`,channel_id,quota,prompt_tokens,completion_tokens,use_time,is_stream,content,other,request_id,upstream_request_id,ip) "+syntheticDigitsCTE()+"SELECT n+1000,%d+MOD(n,86400),2,999,'noise',9,'noise','noise','noise',9,1,1,1,1,0,'','{}',CONCAT('noise',n),'noise','127.0.0.1' FROM seq WHERE n<=50000;ANALYZE TABLE logs;", from))
	var actualQueries []sourceAuditQuery
	sql.Register(name, sourceAuditMySQLDriver{t, os.Getenv("MONITOR_SQL_OPT_CONTAINER"), &actualQueries})
	db, e := sql.Open(name, "")
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	m := &Monitor{prodDB: db}
	m.portalLogColumns.db = db
	m.portalLogColumns.expires = time.Now().Add(time.Hour)
	m.portalLogColumns.columns = portalLogColumnSet{ip: true, upstreamID: true}
	ctx := context.Background()
	ids := []int64{201, 202}
	stats, e := m.computeUsageStats(ctx, ids, from, to, 0)
	if e != nil || stats.Summary.ConsumeQuota != 60 || stats.Summary.RefundQuota != 5 || stats.Summary.Requests != 3 || len(stats.Daily) != 2 || len(stats.DailyByModel) == 0 {
		t.Fatalf("stats actual result wrong %+v %v", stats, e)
	}
	tokenStats, e := m.computeUsageStats(ctx, ids, from, to, 1)
	if e != nil || tokenStats.Summary.ConsumeQuota != 10 {
		t.Fatal("token stats differ", tokenStats, e)
	}
	if _, e = m.computeUsageMatrix(ctx, ids, from, to); e != nil {
		t.Fatal(e)
	}
	owner, balance, used := m.userLiveUsage(ctx, 201)
	if owner != "one" || balance == nil || *balance != 1000 || used == nil || *used != 100 {
		t.Fatal("live profile differs")
	}
	if m.tokenMetaOf(ctx, 201, 1) == nil || m.tokenMetaOf(ctx, 202, 1) != nil {
		t.Fatal("token owner isolation differs")
	}
	a, e := m.computeUserTokenAggregates(ctx, 201, from, to)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = m.hydrateUserTokenUsage(ctx, 201, a, nil); e != nil {
		t.Fatal(e)
	}
	if m.usageOwnerLabel(ctx, 201) != "one" {
		t.Fatal("owner label differs")
	}
	b, u := m.sumUsageLiveQuotas(ctx, ids)
	if b == nil || u == nil || *b != 3000 || *u != 300 {
		t.Fatal("group live totals differ")
	}
	for _, filter := range []struct {
		name, model, group, token, request, detail, want string
		member                                           int64
		typ                                              int
	}{
		{name: "all", want: "[6 5 4 3 2 1]"},
		{name: "model", model: "m1", want: "[6 5 4 1]"},
		{name: "group", group: "g1", want: "[6 5 4 1]"},
		{name: "token", token: "tok", want: "[6 5 4 3 2 1]"},
		{name: "request", request: "r1", want: "[1]"},
		{name: "absent", request: "missing", want: "[]"},
		{name: "member", member: 201, want: "[5 3 2 1]"},
		{name: "outside member set", member: 999, want: "[]"},
		{name: "consume", typ: 2, want: "[4 2 1]"},
		{name: "error", typ: 5, want: "[5]"},
		{name: "refund", typ: 6, want: "[3]"},
		{name: "literal wildcards", detail: "50%_!", want: "[2]"},
		{name: "violation display keyword", detail: "违规费", want: "[5]"},
		{name: "combined", member: 201, typ: 2, model: "m2", group: "g2", token: "tok", detail: "50%_!", want: "[2]"},
	} {
		n, e := m.countGroupLogs(ctx, ids, from, to, filter.member, filter.typ, filter.model, filter.group, filter.token, filter.detail, filter.request)
		if e != nil {
			t.Fatal("group count failed", filter.name, e)
		}
		rows, e := m.queryGroupLogs(ctx, ids, from, to, filter.member, filter.typ, filter.model, filter.group, filter.token, filter.detail, filter.request, 0, 10)
		if e != nil || int64(len(rows)) != n {
			t.Fatal("group rows/count differ", filter.name, e)
		}
		var got []int64
		for _, row := range rows {
			got = append(got, row.ID)
		}
		if fmt.Sprint(got) != filter.want {
			t.Fatal("exact selected row IDs differ", filter.name, got, filter.want)
		}
	}
	if n, upper, e := m.countGroupLogsSnapshot(ctx, ids, from, to, 0, 0, "", "", "", "", ""); e != nil || n != 6 || upper != 7 {
		t.Fatal("export count or upper ID differs", n, upper, e)
	}
	if _, e = m.fetchGroupGovernanceOptions(ctx); e != nil {
		t.Fatal(e)
	}
	if x, e := m.fetchGroupGovernanceUsers(ctx); e != nil || len(x) != 2 {
		t.Fatal("governance users differ", x, e)
	}
	if x, e := m.fetchGroupGovernanceTokens(ctx, to); e != nil || x["g1"].Enabled != 2 || x["g2"].Disabled != 1 {
		t.Fatal("governance tokens differ", x, e)
	}
	if x, ok, e := m.fetchGroupGovernanceSubscriptions(ctx); e != nil || !ok || len(x["g1"]) != 1 {
		t.Fatal("governance subscriptions differ", x, e)
	}
	if db.Stats().InUse != 0 {
		t.Fatal("source audit leaked connection")
	}
	wideIDs := append([]int64{201, 202, 999}, make([]int64, 61)...)
	curRows, e := m.queryGroupLogsCursor(ctx, ids, from, to, 0, 0, "", "", "", "", "", logPageCursor{}, 10)
	if e != nil {
		t.Fatal(e)
	}
	var curIDs []int64
	for _, r := range curRows {
		curIDs = append(curIDs, r.ID)
	}
	if fmt.Sprint(curIDs) != "[4 5 6 3 2 1]" {
		t.Fatal("time/type/null ordering changed", curIDs)
	}
	for i := 3; i < len(wideIDs); i++ {
		wideIDs[i] = int64(700 + i)
	}
	if _, e = m.countGroupLogs(ctx, wideIDs, from, to, 0, 0, "", "", "", "", ""); e != nil {
		t.Fatal(e)
	}
	if _, e = m.queryGroupLogs(ctx, wideIDs, from, to, 0, 0, "", "", "", "", "", 0, 10); e != nil {
		t.Fatal(e)
	}
	if _, e = m.queryGroupLogsCursor(ctx, wideIDs, from, to, 0, 0, "", "", "", "", "", logPageCursor{}, 10); e != nil {
		t.Fatal(e)
	}
	// Compare whole actual SQL, not a hand-copied simplified predicate.
	type observation struct {
		Distribution, Operation, Window, Variant string
		US                                       []int64
		Plan                                     json.RawMessage
		Analyze                                  string
	}
	var performance []observation
	var targets []sourceAuditQuery
	for _, q := range actualQueries {
		// Legacy ID pagination already uses the optimizer in production. Do not
		// count a hypothetical forced legacy plan as a product speedup.
		if (strings.HasSuffix(q.Function, ".countGroupLogs") || strings.HasSuffix(q.Function, ".queryGroupLogsWithLane") && strings.Contains(q.SQL, "ORDER BY created_at DESC")) && !strings.Contains(q.SQL, "LIKE") && !strings.Contains(q.SQL, "request_id =") && !strings.Contains(q.SQL, " AND model_name = ?") && !strings.Contains(q.SQL, " AND `group` = ?") && !strings.Contains(q.SQL, " AND token_name = ?") && !strings.Contains(q.SQL, " AND user_id = ?") && !strings.Contains(q.SQL, " AND type = ?") {
			duplicate := false
			for _, seen := range targets {
				if seen.SQL == q.SQL {
					duplicate = true
				}
			}
			if !duplicate {
				targets = append(targets, q)
			}
		}
	}
	if len(targets) != 4 {
		t.Fatal("missing actual multi-member COUNT/list paths")
	}
	for _, export := range []bool{false, true} {
		cursor := logPageCursor{HasUpper: export, UpperID: 5}
		var got []int64
		for page := 0; page < 8; page++ {
			var rows []LogRow
			if export {
				rows, e = m.queryGroupLogsForExportCursor(ctx, ids, from, to, 0, 0, "", "", "", "", "", cursor, 1)
			} else {
				rows, e = m.queryGroupLogsCursor(ctx, ids, from, to, 0, 0, "", "", "", "", "", cursor, 1)
			}
			if e != nil {
				t.Fatal("actual MySQL cursor page failed", e)
			}
			if len(rows) == 0 {
				break
			}
			got = append(got, rows[0].ID)
			cursor = logCursorAfterRow(rows[0], cursor)
		}
		want := "[4 5 6 3 2 1]"
		if export {
			want = "[4 5 3 2 1]"
		}
		if fmt.Sprint(got) != want {
			t.Fatal("actual MySQL cursor duplicated/lost a row or crossed export upper ID", got, want)
		}
	}
	for _, dist := range []string{"selected_sparse", "selected_dense"} {
		if dist == "selected_dense" {
			run("USE sql_source_audit;UPDATE logs SET user_id=201+MOD(id,2) WHERE id>1000;ANALYZE TABLE logs;")
		}
		for i, target := range targets {
			for _, window := range []string{"wide", "short"} {
				args := append([]any{}, target.Args...)
				if window == "short" {
					for j, a := range args {
						if v, ok := a.(int64); ok && v == to {
							args[j] = from + 120
						}
					}
				}
				nextSQL := strings.Replace(target.SQL, "logs FORCE INDEX (idx_created_at_type)", "logs", 1)
				oldSQL := strings.Replace(nextSQL, "FROM logs", "FROM logs FORCE INDEX (idx_created_at_type)", 1)
				old := usageLogsSyntheticBind(t, oldSQL, args)
				next := usageLogsSyntheticBind(t, nextSQL, args)
				variants := []struct{ name, q string }{{"forced_time", old}, {"native", next}}
				start := len(performance)
				for _, v := range variants {
					operation := []string{"count_members2", "time_cursor_members2", "count_members64", "time_cursor_members64"}[i]
					performance = append(performance, observation{Distribution: dist, Operation: operation, Window: window, Variant: v.name})
				}
				for round := 0; round < 7; round++ {
					for j := range variants {
						n := j
						if round%2 != 0 {
							n = len(variants) - 1 - j
						}
						_, us := timedSyntheticSQL(t, run, "sql_source_audit", variants[n].q)
						performance[start+n].US = append(performance[start+n].US, us)
					}
				}
				for j, v := range variants {
					r := &performance[start+j]
					r.Plan = json.RawMessage(run("USE sql_source_audit;EXPLAIN FORMAT=JSON " + v.q + ";"))
					r.Analyze = run("USE sql_source_audit;EXPLAIN ANALYZE " + v.q + ";")
				}
				x, e := (sourceAuditMySQLConn{sourceAuditMySQLDriver{t, os.Getenv("MONITOR_SQL_OPT_CONTAINER"), nil}}).readXML(ctx, old)
				if e != nil {
					t.Fatal(e)
				}
				y, e := (sourceAuditMySQLConn{sourceAuditMySQLDriver{t, os.Getenv("MONITOR_SQL_OPT_CONTAINER"), nil}}).readXML(ctx, next)
				if e != nil {
					t.Fatal(e)
				}
				bx, _ := json.Marshal(x.values)
				by, _ := json.Marshal(y.values)
				if !bytes.Equal(bx, by) {
					t.Fatal("native choice changed ordered full row values")
				}
			}
		}
	}
	sqlScaleWrite(t, "source-multimember-plan-candidate-20261127.json", struct {
		Warning string
		Results []observation
	}{"Actual generated multi-member COUNT/time-cursor SQL; 50000 synthetic noise rows; historical forced-time policy versus native optimizer; full ordered values equal in sparse/dense membership. Legacy ID pagination already used native and is excluded from speedup claims. CLI session-wall only, not HTTP/p95.", performance})
}
