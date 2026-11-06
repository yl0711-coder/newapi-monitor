package monthlysource

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
)

// Frozen previous expression is the independent compatibility oracle.
const usageBillingSourceLegacySQL = `CASE
 WHEN other IS NULL OR TRIM(other)='' THEN 'unspecified'
 WHEN NOT JSON_VALID(other) THEN 'invalid_json'
 WHEN JSON_TYPE(other)<>'OBJECT' THEN 'invalid_source'
 WHEN JSON_EXTRACT(other,'$.billing_source') IS NULL THEN 'unspecified'
 WHEN JSON_TYPE(JSON_EXTRACT(other,'$.billing_source'))='STRING'
  AND BINARY JSON_UNQUOTE(JSON_EXTRACT(other,'$.billing_source')) IN ('wallet','subscription')
 THEN JSON_UNQUOTE(JSON_EXTRACT(other,'$.billing_source'))
 ELSE 'invalid_source' END`

func TestUsageBillingSourceMySQLCandidateExactCompatibility(t *testing.T) {
	if os.Getenv("MONTHLY_USAGE_LOCAL_MYSQL") != "1" {
		t.Skip("isolated disposable MySQL socket only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", "root@unix(/var/run/mysqld/mysqld.sock)/?timeout=2s&readTimeout=4s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	cases := []struct {
		value any
		want  string
	}{
		{nil, "unspecified"}, {"", "unspecified"}, {"  ", "unspecified"},
		{`broken json`, "invalid_json"}, {`{`, "invalid_json"},
		{`[]`, "invalid_source"}, {`["wallet"]`, "invalid_source"}, {`"wallet"`, "invalid_source"}, {`null`, "invalid_source"}, {`true`, "invalid_source"}, {`1`, "invalid_source"},
		{`{}`, "unspecified"}, {`{"other":"wallet"}`, "unspecified"},
		{`{"billing_source":"wallet"}`, "wallet"}, {`{"billing_source":"subscription"}`, "subscription"},
		{`{"billing_source":"\u0077allet"}`, "wallet"}, {` { "billing_source" : "wallet" } `, "wallet"},
		{`{"billing_source":"Wallet"}`, "invalid_source"}, {`{"billing_source":"wallet "}`, "invalid_source"}, {`{"billing_source":"wallet\u0000"}`, "invalid_source"},
		{`{"billing_source":null}`, "invalid_source"}, {`{"billing_source":true}`, "invalid_source"}, {`{"billing_source":0}`, "invalid_source"},
		{`{"billing_source":{}}`, "invalid_source"}, {`{"billing_source":["wallet"]}`, "invalid_source"},
		{`{"billing_source":"wallet","billing_source":"subscription"}`, "subscription"},
		{`{"billing_source":"subscription","billing_source":"wallet"}`, "wallet"},
	}
	for i, c := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			var before, after string
			q := "SELECT " + usageBillingSourceLegacySQL + "," + usageBillingSourceSQL + " FROM (SELECT ? AS other) s"
			if err := db.QueryRowContext(ctx, q, c.value).Scan(&before, &after); err != nil {
				t.Fatal(err)
			}
			if before != c.want || after != before {
				t.Fatalf("classification changed: old=%q candidate=%q want=%q", before, after, c.want)
			}
		})
	}
}
