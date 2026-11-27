package monitor

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This socket-CLI adapter exercises Go row scanning and the actual handler
// against MySQL in a network-none container. CLI overhead is included in wall
// time; it is NOT a production persistent-driver or cancellation benchmark.
type diagnosticPageMySQLDriver struct {
	t       *testing.T
	run     func(string) string
	queries *int
}
type diagnosticPageMySQLConn struct{ d diagnosticPageMySQLDriver }

func (d diagnosticPageMySQLDriver) Open(string) (driver.Conn, error) {
	return diagnosticPageMySQLConn{d}, nil
}
func (c diagnosticPageMySQLConn) Close() error { return nil }
func (c diagnosticPageMySQLConn) Begin() (driver.Tx, error) {
	return nil, errors.New("synthetic transactions unsupported")
}
func (c diagnosticPageMySQLConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("synthetic prepare unsupported")
}
func (c diagnosticPageMySQLConn) QueryContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	args := make([]any, len(a))
	for i, v := range a {
		args[i] = v.Value
	}
	*c.d.queries++
	// The bounded CLI helper trims outer whitespace. Keep a nonempty sentinel
	// after all fields so an empty metadata row cannot lose trailing columns.
	marked := strings.Contains(q, usageLogColumns)
	if marked {
		q = strings.Replace(q, usageLogColumns, usageLogColumns+", '__row_end'", 1)
	}
	out := c.d.run("USE diagnostic_page_acceptance;" + usageLogsSyntheticBind(c.d.t, q, args) + ";")
	r := &explainCatalogRows{}
	if out == "" {
		return r, nil
	}
	for _, line := range strings.Split(out, "\n") {
		parts := strings.Split(line, "\t")
		if marked {
			if parts[len(parts)-1] != "__row_end" {
				return nil, errors.New("missing synthetic row sentinel")
			}
			parts = parts[:len(parts)-1]
		}
		if len(r.columns) == 0 {
			for i := range parts {
				r.columns = append(r.columns, fmt.Sprint(i))
			}
		}
		if len(parts) != len(r.columns) {
			return nil, errors.New("synthetic row width changed")
		}
		values := make([]driver.Value, len(parts))
		for i, p := range parts {
			if p != "NULL" {
				values[i] = p
			}
		}
		r.values = append(r.values, values)
	}
	return r, nil
}

func TestUsageLogsDiagnosticPageMySQLAcceptance(t *testing.T) {
	run := isolatedSQLOptimizationRunner(t)
	const database = "diagnostic_page_acceptance"
	if os.Getenv("MONITOR_DIAGNOSTIC_PAGE_REUSE") != "1" {
		run("CREATE DATABASE " + database + ";USE " + database + ";" +
			"CREATE TABLE channels(id BIGINT PRIMARY KEY,name VARCHAR(80));INSERT INTO channels VALUES(7,'synthetic-channel');" +
			"CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,content TEXT,other TEXT,username VARCHAR(80),channel_id INT,user_id BIGINT,token_name VARCHAR(80),model_name VARCHAR(80),quota BIGINT,prompt_tokens BIGINT,completion_tokens BIGINT,use_time INT,is_stream INT,token_id BIGINT,`group` VARCHAR(80),ip VARCHAR(80),request_id VARCHAR(80),upstream_request_id VARCHAR(80),INDEX idx_created_at_id_rc26(created_at,id),INDEX idx_created_at_type(created_at,type),INDEX idx_logs_username(username));")
		for first := 1; first <= 200000; first += 10000 {
			run("USE " + database + ";INSERT INTO logs " + syntheticDigitsCTE() + fmt.Sprintf("SELECT n,100000+FLOOR((n-1)*86400/200000),IF(MOD(n,100)=0,5,2),CASE WHEN n=1 THEN 'oldest-marker' WHEN MOD(n,100)=0 THEN 'rate_limit_exceeded' ELSE 'normal' END,JSON_OBJECT('padding',REPEAT('p',IF(n<=4000,4096,512)),'admin_info',IF(MOD(n,200)=1,JSON_OBJECT('reject_reason','claude_stop_reason=refusal'),JSON_OBJECT())),'fixture',7,31,'t','model',50000,1200,240,3,1,5,'g','127.0.0.1',CONCAT('diag-',n),CONCAT('up-',n) FROM seq WHERE n BETWEEN %d AND %d;", first, first+9999))
		}
		run("USE " + database + ";ANALYZE TABLE logs;")
	} else if run("USE "+database+";SELECT COUNT(*) FROM logs;") != "200000" {
		t.Fatal("cannot reuse incomplete synthetic fixture")
	}
	queries := 0
	name := fmt.Sprintf("diagnostic-page-mysql-%d", time.Now().UnixNano())
	sql.Register(name, diagnosticPageMySQLDriver{t, run, &queries})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	m := &Monitor{prodDB: db}
	type measurement struct {
		Case                        string
		Rows, HTTPUnits, Statements int
		WholePageUS                 int64
		More                        bool
		IDs                         []int64
	}
	var results []measurement
	for _, tc := range []struct {
		name, scope, message string
		want                 []int64
		more                 bool
	}{
		{"missing", "", "not-present", []int64{}, false},
		{"oldest", "", "oldest-marker", []int64{1}, false},
		{"rejected", "rejected", "", []int64{199801, 199601, 199401, 199201, 199001, 198801, 198601, 198401, 198201, 198001, 197801, 197601, 197401, 197201, 197001, 196801, 196601, 196401, 196201, 196001}, true},
		{"error-text", "", "rate_limit_exceeded", []int64{200000, 199900, 199800, 199700, 199600, 199500, 199400, 199300, 199200, 199100, 199000, 198900, 198800, 198700, 198600, 198500, 198400, 198300, 198200, 198100}, true},
	} {
		start, before := time.Now(), queries
		r := measurement{Case: tc.name, IDs: []int64{}}
		q := url.Values{"start_timestamp": {"100000"}, "end_timestamp": {"186399"}, "page_size": {"20"}, "defer_total": {"1"}, "bounded_search": {"1"}, "diagnostic_scope": {tc.scope}, "message": {tc.message}}
		for unit := 0; unit < 64; unit++ {
			q.Set("page_size", strconv.Itoa(20-len(r.IDs)))
			w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?"+q.Encode(), nil)
			var reply struct {
				Success bool
				Data    usageLogsDiagnosticBatch
				Message string
			}
			if json.Unmarshal(w.Body.Bytes(), &reply) != nil || !reply.Success {
				t.Fatal(w.Body.String())
			}
			r.HTTPUnits++
			for _, it := range reply.Data.Items {
				r.IDs = append(r.IDs, it.ID)
				if it.ChannelName != "synthetic-channel" {
					t.Fatal("channel lookup missing")
				}
			}
			if !reply.Data.ScanPending {
				r.More = reply.Data.HasMore
				break
			}
			if len(r.IDs) >= 20 {
				t.Fatal("fixture unexpectedly needs proof-only client request")
			}
			q.Set("before_id", fmt.Sprint(reply.Data.ScanCursor.ID))
			q.Set("before_created_at", fmt.Sprint(reply.Data.ScanCursor.CreatedAt))
		}
		r.WholePageUS = time.Since(start).Microseconds()
		r.Statements = queries - before
		r.Rows = len(r.IDs)
		if !reflect.DeepEqual(r.IDs, tc.want) || r.More != tc.more {
			t.Fatalf("%s %+v", tc.name, r)
		}
		results = append(results, r)
		t.Logf("%s complete_page_us=%d statements=%d HTTP_units=%d rows=%d", r.Case, r.WholePageUS, r.Statements, r.HTTPUnits, r.Rows)
	}
	sources := map[string]string{}
	for _, file := range []string{"usage_logs_diagnostic_batch.go", "usage_logs_diagnostics.go", "newapi_usage_logs.go", "usage_logs.js"} {
		b, e := os.ReadFile(file)
		if e != nil {
			t.Fatal(e)
		}
		sources[file] = fmt.Sprintf("%x", sha256.Sum256(b))
	}
	sqlScaleWrite(t, "usage-log-page-mysql-acceptance-20261127.json", map[string]any{"synthetic_only": true, "rows": 200000, "source_sha256": sources, "results": results, "notes": "Entire logical page via real Go handler/JSON, MySQL 8.4 network-none. Socket CLI overhead included, persistent driver/network/browser latency not measured; no production performance guarantee."})
}
