package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"
)

type explainCase struct {
	ID      string `json:"id"`
	Owner   string `json:"owner"`
	Variant string `json:"variant"`
	SQL     string `json:"sql"`
	Args    []any  `json:"args"`
}
type explainResult struct {
	ID         string           `json:"id"`
	Owner      string           `json:"owner"`
	Variant    string           `json:"variant"`
	Parameters string           `json:"parameter_profile"`
	SQLSHA     string           `json:"sql_sha256"`
	Status     string           `json:"status"`
	Millis     int64            `json:"elapsed_ms"`
	Plan       []map[string]any `json:"plan,omitempty"`
	MySQLError uint16           `json:"mysql_error,omitempty"`
	ErrorClass string           `json:"error_class,omitempty"`
}

var executionHint = regexp.MustCompile(`MAX_EXECUTION_TIME\(\d+\)`)

func explainMySQLCatalog(path, out string, offset, limit int) (retErr error) {
	if limit < 1 || limit > 20 || offset < 0 {
		return fmt.Errorf("invalid batch bounds")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var catalog struct {
		Cases []explainCase `json:"cases"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err = decoder.Decode(&catalog); err != nil {
		return err
	}
	if offset >= len(catalog.Cases) {
		return fmt.Errorf("catalog offset outside range")
	}
	end := min(offset+limit, len(catalog.Cases))
	results := []explainResult{}
	defer func() {
		if e := persistSQLAuditReport(out, offset, end, len(catalog.Cases), retErr != nil, results); e != nil {
			retErr = e
		}
		fmt.Printf("SQL_EXPLAIN offset=%d end=%d records=%d stopped=%t report=%s\n", offset, end, len(results), retErr != nil, out)
	}()
	db, err := openSQLAuditReplica()
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err = sqlAuditReplicaIdentity(ctx, db); err != nil {
		return err
	}

	if err = sqlAuditPressure(ctx, db); err != nil {
		return err
	}
	for i := offset; i < end; i++ {
		v := catalog.Cases[i]
		hash := sha256.Sum256([]byte(v.SQL))
		r := explainResult{ID: v.ID, Owner: v.Owner, Variant: v.Variant, SQLSHA: fmt.Sprintf("%x", hash[:]), Status: "unreviewed"}
		r.Parameters = "catalog_closed_range_and_audit_filter_values"
		if v.Owner == "serveUsageLogsStat/rate" && len(v.Args) > 0 {
			// Production rates ignore the selected date and use server-recent
			// 60s. A frozen old lower bound would explain hours, not this query.
			v.Args[len(v.Args)-1] = time.Now().Unix() - 60
			r.Parameters = "runtime_recent_60_seconds"
		}
		trim := strings.TrimSpace(strings.ToUpper(v.SQL))
		if !strings.HasPrefix(trim, "SELECT ") && !strings.HasPrefix(trim, "WITH ") {
			r.Status = "blocked_not_read_select"
			results = append(results, r)
			continue
		}
		if strings.Contains(v.SQL, ";") || strings.Contains(trim, "GET_LOCK(") || strings.Contains(trim, "SLEEP(") {
			r.Status = "blocked_side_effect_or_multiple_statement"
			results = append(results, r)
			continue
		}
		// MySQL may compute aggregate bounds while explaining a materialized
		// constant derived table. Do not disguise that as metadata-only work.
		if strings.Contains(v.SQL, "bounds.row_count") {
			r.Status = "requires_isolated_explain_derived_aggregate"
			results = append(results, r)
			continue
		}
		for j, arg := range v.Args {
			if n, ok := arg.(json.Number); ok {
				v.Args[j], err = n.Int64()
				if err != nil {
					return err
				}
			}
		}
		q := executionHint.ReplaceAllString(v.SQL, "MAX_EXECUTION_TIME(2000)")
		qctx, stop := context.WithTimeout(ctx, 3*time.Second)
		started := time.Now()
		var raw string
		err = db.QueryRowContext(qctx, "EXPLAIN FORMAT=JSON "+q, v.Args...).Scan(&raw)
		stop()
		r.Millis = time.Since(started).Milliseconds()
		if err != nil {
			r.Status = "explain_error"
			r.ErrorClass = fmt.Sprintf("%T", err)
			var my *mysql.MySQLError
			if errors.As(err, &my) {
				r.MySQLError = my.Number
			}
			results = append(results, r)
			return err
		}
		var plan any
		if err = json.Unmarshal([]byte(raw), &plan); err != nil {
			return err
		}
		r.Plan = sqlAuditPlan(plan)
		r.Status = "explained"
		results = append(results, r)
		if i < end-1 {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return sqlAuditPressure(ctx, db)
}

func sqlAuditReplicaIdentity(ctx context.Context, db *sql.DB) error {
	var ro int
	var database, user string
	if err := db.QueryRowContext(ctx, "SELECT @@read_only,DATABASE(),CURRENT_USER()").Scan(&ro, &database, &user); err != nil {
		return err
	}
	if ro != 1 || database != "nexusapi" || !strings.HasPrefix(user, "monitor_ro@") {
		return fmt.Errorf("not intended replica identity")
	}
	return nil
}

func openSQLAuditReplica() (*sql.DB, error) {
	env, err := os.ReadFile("/Users/yl/.config/newapi-monitor/local-acceptance.env")
	if err != nil {
		return nil, err
	}
	var dsn string
	for _, line := range strings.Split(string(env), "\n") {
		if strings.HasPrefix(line, "NEWAPI_LOG_DSN=") {
			dsn = strings.Trim(strings.TrimSpace(strings.TrimPrefix(line, "NEWAPI_LOG_DSN=")), "\"'")
			break
		}
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.User != "monitor_ro" || cfg.DBName != "nexusapi" || cfg.Addr != "host.docker.internal:13316" {
		return nil, fmt.Errorf("unexpected source")
	}
	const endpoint = "nexusapi-mysql-read-01.clisguyaiw4j.us-west-2.rds.amazonaws.com"
	response, err := (&http.Client{Timeout: 8 * time.Second}).Get("https://truststore.pki.rds.amazonaws.com/us-west-2/us-west-2-bundle.pem")
	if err != nil {
		return nil, err
	}
	bundle, err := io.ReadAll(io.LimitReader(response.Body, 65537))
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || len(bundle) > 65536 {
		return nil, fmt.Errorf("CA retrieval failed")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle) {
		return nil, fmt.Errorf("invalid CA")
	}
	if err := mysql.RegisterTLSConfig("sql-audit-explain", &tls.Config{ServerName: endpoint, RootCAs: roots, MinVersion: tls.VersionTLS12}); err != nil {
		return nil, err
	}
	cfg.Net = "tcp"
	cfg.Addr = "127.0.0.1:13326"
	cfg.TLSConfig = "sql-audit-explain"
	cfg.Timeout = 4 * time.Second
	cfg.ReadTimeout = 4 * time.Second
	cfg.WriteTimeout = 4 * time.Second
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	return db, nil
}

func sqlAuditPressure(ctx context.Context, db *sql.DB) error {
	rows, e := db.QueryContext(ctx, "SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_running','Threads_connected')")
	if e != nil {
		return e
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var value int
		if e = rows.Scan(&name, &value); e != nil {
			return e
		}
		if name == "Threads_running" && value > 4 || name == "Threads_connected" && value > 10 {
			return fmt.Errorf("source busy")
		}
	}
	return rows.Err()
}

func sqlAuditPlan(plan any) []map[string]any {
	var result []map[string]any
	var walk func(any, string)
	walk = func(node any, path string) {
		switch x := node.(type) {
		case map[string]any:
			clean := map[string]any{}
			for _, key := range []string{"table_name", "access_type", "possible_keys", "key", "used_key_parts", "rows_examined_per_scan", "rows_produced_per_join", "filtered", "using_index", "using_temporary_table", "using_filesort"} {
				if val, ok := x[key]; ok {
					clean[key] = val
				}
			}
			if len(clean) > 0 {
				clean["plan_path"] = path
				if _, ok := x["materialized_from_subquery"]; ok {
					clean["materialized"] = true
				}
				result = append(result, clean)
			}
			keys := make([]string, 0, len(x))
			for key := range x {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				walk(x[key], path+"/"+key)
			}
		case []any:
			for i, child := range x {
				walk(child, fmt.Sprintf("%s/%d", path, i))
			}
		}
	}
	walk(plan, "$")
	return result
}

func persistSQLAuditReport(out string, offset, end, total int, stopped bool, results []explainResult) error {
	payload := struct {
		Offset, End, Total int
		Analyze            bool
		Stopped            bool
		Entries            []explainResult
	}{offset, end, total, false, stopped, results}
	b, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(out, append(b, '\n'), 0600)
}
