package monitor

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestUsageLogsAggregateProbeExactBoundaries(t *testing.T) {
	for _, n := range []int{0, 1, 63, 64, 255, 256, 257, 258} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			calls := &usageQuotaCalls{}
			name := fmt.Sprintf("fused-mysql-%d", usageQuotaDriverID.Add(1))
			sql.Register(name, &usageQuotaMySQLDriver{inner: usageTestSQLiteDriver(t), calls: calls})
			db, err := sql.Open(name, ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			_, err = db.Exec(`CREATE TABLE logs(id INTEGER PRIMARY KEY,created_at INTEGER,type INTEGER,quota INTEGER,username TEXT,channel_id INTEGER,model_name TEXT,` + "`group`" + ` TEXT);
INSERT INTO logs VALUES(0,90,2,0,'old',9,'old','old'),(2000000,300,2,0,'old',9,'old','old');`)
			if err != nil {
				t.Fatal(err)
			}
			for i := 1; i <= n; i++ {
				if _, err = db.Exec("INSERT INTO logs VALUES(?, ?, ?, ?, 'historical_name',7,'model','group')", i, 100+i/5, []int{2, 5}[i%2], i%7-3); err != nil {
					t.Fatal(err)
				}
			}
			for _, f := range []usageLogFilter{
				{Username: "historical_name", Channel: 7}, {Username: "missing", Channel: 7},
				{Channel: 7}, {Channel: 999}, {ModelName: "model"}, {Group: "group"},
				{Channel: 7, ModelName: "model", Group: "group"}, {},
			} {
				f.Start, f.End = 100, 220
				for _, typ := range []int{0, 2, 5, -1} {
					f.Type = typ
					for _, scope := range []usageLogWhereScope{usageLogWhereList, usageLogWhereStat} {
						w, err := buildUsageLogWhere(f, scope)
						if err != nil {
							t.Fatal(err)
						}
						expr := "COUNT(*)"
						planning := f
						if scope == usageLogWhereStat {
							expr = "COALESCE(SUM(quota),0)"
							planning.Type = 0
						}
						var want int64
						if err = db.QueryRow("SELECT "+expr+" FROM logs"+w.sql(), w.args...).Scan(&want); err != nil {
							t.Fatal(err)
						}
						p, err := usageLogsProbeAggregate(context.Background(), db, planning, w, expr)
						if err != nil {
							t.Fatal(err)
						}
						if p.exact && p.value != want {
							t.Fatalf("n=%d f=%+v scope=%v got=%d want=%d", n, f, scope, p.value, want)
						}
						selector := f.Username != "" || f.Channel != 0 || f.ModelName != "" || f.Group != ""
						if (n < 64 || selector && n <= 256) && !p.exact {
							t.Fatalf("small selector/time did not return exact: n=%d f=%+v", n, f)
						}
						if n >= 257 && f.Username == "historical_name" && (p.exact || typ == 0 && p.plan != usageLogPlanSpan) {
							t.Fatalf("dense selector plan changed: %+v", p)
						}
						if db.Stats().InUse != 0 {
							t.Fatal("probe leaked connection")
						}
					}
				}
			}
		})
	}
}

func TestUsageLogsAggregateProbeProtocol(t *testing.T) {
	f := usageLogFilter{Start: 100, End: 200, Username: "raw_name", Channel: 7}
	for _, n := range []int64{0, 9007199254740993, -9007199254740993, 9223372036854775807, -9223372036854775808} {
		v := strconv.FormatInt(n, 10)
		raw := []byte("exact|" + v)
		p, e := decodeUsageLogsAggregateProbe(f, raw)
		if e != nil || !p.exact || p.value != n {
			t.Fatalf("amount lost: %s %+v %v", v, p, e)
		}
	}
	for _, raw := range []string{
		"exact|", "exact|1.0", "exact|1e3", "exact|9223372036854775808", "exact|0|1|2|3|4|5|6", "plan|0",
		"plan|65|1|64|1|90|1000|210", "plan|64|null|64|1|90|1000|210", "plan|64|1|64|1|90|1000|210|0",
		``, `null`, `[]`, `{}`, `{"kind":"unknown"}`, `{"kind":"exact"}`, `{"kind":"exact","value":null}`,
		`{"kind":"exact","value":1}`, `{"kind":"exact","value":"1.0"}`, `{"kind":"exact","value":"9223372036854775808"}`,
		`{"kind":"exact","value":"0","endpoints":[1,64]}`, `{"kind":"exact","value":"0","extra":true}`,
		`{"kind":"exact","value":"0"} {}`, `{"kind":"plan"}`,
		`{"kind":"plan","endpoints":[1,64,65],"first":[1,90],"last":[1000,210]}`,
		`{"kind":"plan","endpoints":[1,64],"first":[1],"last":[1000,210]}`,
		`{"kind":"plan","endpoints":[null,64],"first":[1,90],"last":[1000,210]}`,
		`{"kind":"plan","value":"0","endpoints":[1,64],"first":[1,90],"last":[1000,210]}`,
		strings.Repeat(" ", 4097),
	} {
		if _, e := decodeUsageLogsAggregateProbe(f, []byte(raw)); e == nil {
			t.Fatalf("accepted invalid probe %q", raw)
		}
	}
	p, e := decodeUsageLogsAggregateProbe(f, []byte("plan|64|9007199254740993|9007199254741056|9007199254740992|90|9007199254742000|210"))
	if e != nil || p.exact || p.plan != usageLogPlanSpan {
		t.Fatalf("ID precision/decision lost: %+v %v", p, e)
	}
}
