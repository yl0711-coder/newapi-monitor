package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSQLAuditUsageMonthDynamicBatch(t *testing.T) {
	base, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	count := &sqlAuditReadCounter{}
	db := sql.OpenDB(sqlAuditCounterConnector{base: base.Driver(), count: count})
	base.Close()
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err = InitUsageWindowStore(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	versions := make([]UsageWindowVersion, 64)
	o := usageFixture(t)
	for n := 0; n < 128; n++ {
		next := o
		next.StartedAtMS -= int64(128-n) * 1000
		next.CompletedAtMS -= int64(128-n) * 1000
		id, err := SaveUsageWindowObservation(t.Context(), db, next)
		if err != nil {
			t.Fatal(err)
		}
		if n%2 == 0 {
			versions[n/2].OlderID = id
		} else {
			versions[n/2].NewerID = id
		}
	}
	type result struct {
		SQL                  string
		Parameters, Returned int
		Plan                 []string
	}
	var records []result
	count.observeQuery = func(ctx context.Context, c driver.Conn, q string, a []driver.NamedValue) (err error) {
		if !strings.Contains(q, "length(payload)<=32768 AND id IN (") {
			return nil
		}
		r := result{SQL: q, Parameters: len(a)}
		rows, err := c.(driver.QueryerContext).QueryContext(ctx, "EXPLAIN QUERY PLAN "+q, a)
		if err != nil {
			return err
		}
		// driver.Rows (unlike sql.Rows) need not allow double Close.
		defer func() { err = errors.Join(err, rows.Close()) }()
		for {
			values := make([]driver.Value, 4)
			err := rows.Next(values)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			detail, ok := values[3].(string)
			if !ok {
				return errors.New("unexpected plan value")
			}
			r.Plan = append(r.Plan, detail)
		}
		records = append(records, r)
		return nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, n := range []int{1, 64} {
		used := 0
		got, err := loadUsageMonthBatch(ctx, tx, versions[:n], &used)
		if err != nil || len(got) != n*2 {
			t.Fatal("complete dynamic batch", n, len(got), err)
		}
		for _, v := range versions[:n] {
			for _, id := range []string{v.OlderID, v.NewerID} {
				raw, err := loadUsageWindow(ctx, tx, id)
				if err != nil {
					t.Fatal(err)
				}
				var expected UsageWindowObservation
				if err = json.Unmarshal(raw, &expected); err != nil || !reflect.DeepEqual(got[id], expected) {
					t.Fatal("batch changed original", err)
				}
			}
		}
		records[len(records)-1].Returned = len(got)
	}
	if len(records) != 2 || records[0].Parameters != 2 || records[1].Parameters != 128 {
		t.Fatal("dynamic limits not exercised")
	}
	for _, r := range records {
		if !strings.Contains(strings.Join(r.Plan, "\n"), "SEARCH portal_monthly_usage_observations USING INDEX sqlite_autoindex") {
			t.Fatal("batch no longer seeks by primary key", r.Plan)
		}
	}
	raw, err := json.MarshalIndent(struct {
		Warning string
		Results []result
	}{"Actual loadUsageMonthBatch, 1/64 pairs, same read transaction, real in-memory schema and original observations. EXPLAIN plus full exact-value comparison, not production size/IO evidence.", records}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile("../../docs/sql-audit-20261126/usage-month-dynamic-batch-20261127.json", append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
