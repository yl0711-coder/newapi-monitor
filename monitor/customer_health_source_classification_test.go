package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Explicit opt-in acceptance: two closed minutes, one read-only connection,
// original query budgets and capped rows. No local or production data writes.
func TestCustomerHealthSourceClassificationReadOnly(t *testing.T) {
	dsn, out := os.Getenv("CUSTOMER_HEALTH_AUDIT_DSN"), os.Getenv("CUSTOMER_HEALTH_AUDIT_OUT")
	if dsn == "" || out == "" {
		t.Skip("explicit source audit not supplied")
	}
	c, err := mysql.ParseDSN(dsn)
	if err != nil || c.DBName != "nexusapi" || (c.User != "nexus_ro" && c.User != "monitor_ro") {
		t.Fatal("read-only source identity rejected")
	}
	c.Timeout = 3 * time.Second
	c.ReadTimeout = 4 * time.Second
	db, err := sql.Open("mysql", c.FormatDSN())
	if err != nil {
		t.Fatal("source open failed")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	grants, err := db.QueryContext(ctx, "SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		t.Fatal("source grant check failed")
	}
	for grants.Next() {
		var grant string
		if err = grants.Scan(&grant); err != nil {
			t.Fatal("grant decode")
		}
		for _, p := range []string{"INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "ALL PRIVILEGES"} {
			if strings.Contains(strings.ToUpper(grant), p) {
				t.Fatal("source is not read only")
			}
		}
	}
	err = grants.Err()
	grants.Close()
	if err != nil {
		t.Fatal("grant read incomplete")
	}
	through := (time.Now().Unix() - 120) / 60 * 60
	from := through - 120
	if os.Getenv("CUSTOMER_HEALTH_AUDIT_RECENT_ERROR") == "1" {
		// Select a two-minute error-bearing window, without expanding the
		// aggregate scan. The timestamp seek has its own original 2s limit.
		var at int64
		q := `SELECT /*+ MAX_EXECUTION_TIME(2000) */ created_at FROM logs WHERE created_at>=? AND created_at<? AND type=5 AND NOT (` + channelTestLogPredicateSQL() + `) ORDER BY created_at DESC LIMIT 1`
		if err = db.QueryRowContext(ctx, q, through-3600, through).Scan(&at); err != nil {
			t.Fatal("no recent error window available within source budget")
		}
		through = at/60*60 + 60
		from = through - 120
	}
	evidence := map[string]any{"from": from, "through": through, "read_only": true, "source_connections": 1, "query_timeout_ms": 2000, "policy_version": customerHealthStabilityPolicyVersion}
	defer func() {
		b, _ := json.MarshalIndent(evidence, "", "  ")
		if err := os.WriteFile(out, b, 0600); err != nil {
			t.Error(err)
		}
	}()
	aggregate := `SELECT /*+ MAX_EXECUTION_TIME(2000) */ (created_at DIV 60)*60,user_id,COALESCE(channel_id,0),COALESCE(model_name,''),COALESCE(` + "`group`" + `,''),SUM(type=2),SUM(type=5),SUM(CASE WHEN type=2 AND ` + customerHealthAttributedAnomalySQL() + ` THEN 1 ELSE 0 END)
 FROM logs WHERE created_at>=? AND created_at<? AND type IN (2,5) AND NOT (` + channelTestLogPredicateSQL() + `) GROUP BY 1,2,3,4,5 LIMIT 2049`
	for _, q := range []string{aggregate, customerHealthClassificationSQL()} {
		rows, e := db.QueryContext(ctx, "EXPLAIN "+q, from, through)
		if e != nil {
			t.Fatal("source explain failed")
		}
		columns, _ := rows.Columns()
		var plan []map[string]string
		for rows.Next() {
			v := make([]sql.NullString, len(columns))
			dest := make([]any, len(v))
			for i := range v {
				dest[i] = &v[i]
			}
			if e = rows.Scan(dest...); e != nil {
				t.Fatal("explain decode failed")
			}
			r := map[string]string{}
			for i, k := range columns {
				r[k] = v[i].String
			}
			if r["type"] != "range" && r["type"] != "ref" {
				t.Fatal("unbounded source plan rejected")
			}
			plan = append(plan, r)
		}
		e = rows.Err()
		rows.Close()
		if e != nil || len(plan) == 0 {
			t.Fatal("source explain incomplete")
		}
		evidence[q] = plan
	}
	rows, err := db.QueryContext(ctx, aggregate, from, through)
	if err != nil {
		t.Fatal("aggregate source read failed")
	}
	var batch []CapacityUserMinuteSample
	for rows.Next() {
		var r CapacityUserMinuteSample
		if err = rows.Scan(&r.BucketTs, &r.UserID, &r.ChannelID, &r.ModelName, &r.Grp, &r.Success, &r.Failed, &r.CustomerHealthAnomaly); err != nil {
			t.Fatal("aggregate decode failed")
		}
		batch = append(batch, r)
		if len(batch) > 2048 {
			t.Fatal("aggregate row cap")
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal("aggregate incomplete")
	}
	if err = classifyCustomerHealthErrors(ctx, db, from, through, batch); err != nil {
		evidence["error"] = err.Error()
		t.Fatal("bounded diagnostic classification failed")
	}
	var total, rawFailed, counted, anomaly int64
	for _, r := range batch {
		total += r.Success + r.Failed
		rawFailed += r.Failed
		counted += r.CustomerHealthFailed
		anomaly += r.CustomerHealthAnomaly
	}
	evidence["requests"], evidence["raw_failed"], evidence["stability_failed"], evidence["stability_anomaly"] = total, rawFailed, counted, anomaly
	evidence["stability_percent"] = customerHealthStability(total, anomaly, counted)
	rows, err = db.QueryContext(ctx, customerHealthClassificationSQL(), from, through)
	if err != nil {
		t.Fatal("audit errors read failed")
	}
	var records []map[string]any
	var independentlyCounted int64
	for rows.Next() {
		var r LogChainRow
		var other string
		r.Type = 5
		if err = rows.Scan(&r.ID, &r.CreatedAt, &r.UserID, &r.ChannelID, &r.ModelName, &r.Group, &r.RequestID, &r.Content, &other); err != nil {
			t.Fatal("audit error decode failed")
		}
		r = customerHealthDiagnosticRow(r, other)
		included, got := customerHealthDiagnosticFailure(r)
		want := logChainAttributeFaultWithEvidence(r, nil)
		if got.Fault != want.Fault || got.Reason != want.Reason {
			t.Fatal("customer maintenance/troubleshooting disagreement")
		}
		if included {
			independentlyCounted++
		}
		records = append(records, map[string]any{"log_id": r.ID, "request_id": r.RequestID, "created_at": r.CreatedAt, "user_id": r.UserID, "channel_id": r.ChannelID, "model": r.ModelName, "error_code": r.UpstreamErrorCode, "message": logChainDiagnosticSafeText(r.Content, 4096), "party": got.Fault, "reason": got.Reason, "counts_toward_stability": included})
		if len(records) > 2048 {
			t.Fatal("audit row cap")
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal("audit error read incomplete")
	}
	evidence["errors"] = records
	evidence["aggregate_buckets"] = batch
	if int64(len(records)) != rawFailed || independentlyCounted != counted {
		t.Fatal("source changed during audit; no complete proof")
	}
	t.Logf("source requests=%d errors=%d stability_errors=%d anomalies=%d", total, rawFailed, counted, anomaly)
}
