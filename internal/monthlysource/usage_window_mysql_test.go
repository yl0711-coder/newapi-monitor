package monthlysource

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Opt-in only inside a disposable --network none MySQL container. There is no
// caller-supplied host, password, DSN or production configuration lookup.
func TestUsageWindowMySQLReadOnlyScopeAndPersistence(t *testing.T) {
	if os.Getenv("MONTHLY_USAGE_LOCAL_MYSQL") != "1" {
		t.Skip("isolated local MySQL fixture only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const socket = "unix(/var/run/mysqld/mysqld.sock)"
	admin, err := sql.Open("mysql", "root@"+socket+"/?timeout=2s&readTimeout=5s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := fmt.Sprintf("usage_local_%d", time.Now().UnixNano())
	user := fmt.Sprintf("usage_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.ExecContext(context.Background(), "DROP DATABASE "+name) }()
	if _, err := admin.ExecContext(ctx, "CREATE USER '"+user+"'@'localhost'"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.ExecContext(context.Background(), "DROP USER '"+user+"'@'localhost'") }()
	writer, err := sql.Open("mysql", "root@"+socket+"/"+name+"?timeout=2s&readTimeout=5s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	for _, query := range []string{
		`CREATE TABLE users(id BIGINT PRIMARY KEY,quota BIGINT NULL,used_quota BIGINT NULL) ENGINE=InnoDB`,
		`CREATE TABLE logs(id BIGINT PRIMARY KEY,user_id BIGINT NOT NULL,created_at BIGINT NOT NULL,type INT NOT NULL,quota BIGINT NULL,other LONGTEXT NULL,token_id BIGINT NULL,token_name TEXT NULL,INDEX idx_user_created_type(user_id,created_at,type)) ENGINE=InnoDB`,
		`INSERT INTO users VALUES(201,-1,9007199254740993)`,
	} {
		if _, err := writer.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	const from = int64(1780000000)
	// 50,000 actual source rows; no giant customer response or Go row materialization.
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for batch := int64(0); batch < 100; batch++ {
		var values []string
		args := make([]any, 0, 3000)
		for offset := int64(1); offset <= 500; offset++ {
			id := batch*500 + offset
			values = append(values, "(?,?,?,?,?,?)")
			args = append(args, id, int64(201), from+id%3600, int64(2), int64(1), `{"billing_source":"wallet"}`)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO logs(id,user_id,created_at,type,quota,other) VALUES "+strings.Join(values, ","), args...); err != nil {
			t.Fatal(err)
		}
	}
	for i, event := range []struct {
		user, at int64
		kind     int
		quota    int64
		other    any
	}{
		{201, from, 2, 0, `{"billing_source":"wallet"}`},
		{201, from + 1, 6, 17, `{"billing_source":"wallet"}`},
		{201, from + 1, 2, 11, `{"billing_source":"subscription"}`},
		{201, from + 1, 2, 13, ""},
		{201, from + 1, 2, 9, "{"},
		{201, from + 1, 2, 7, `{"billing_source":"Wallet"}`},
		{201, from + 1, 2, 5, `{"billing_source":null}`},
		{201, from + 1, 2, 19, `[]`},
		{201, from + 1, 2, 23, `"wallet"`},
		{201, from + 1, 2, 29, nil},
		{201, from - 1, 2, 1000000, `{"billing_source":"wallet"}`},
		{201, from + 3600, 2, 1000000, `{"billing_source":"wallet"}`},
		{999, from + 1, 2, 1000000, `{"billing_source":"wallet"}`},
		{201, from + 1, 5, 1000000, nil},
	} {
		if _, err := tx.ExecContext(ctx, "INSERT INTO logs(id,user_id,created_at,type,quota,other) VALUES(?,?,?,?,?,?)", int64(50001+i), event.user, event.at, event.kind, event.quota, event.other); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Two tokens deliberately share the same name. There is no tokens table:
	// deleted tokens must still be attributable from their retained log IDs.
	if _, err := writer.ExecContext(ctx, `UPDATE logs SET token_id=IF(id<=25000,11,12),token_name='same name' WHERE id<=50000`); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, `UPDATE logs SET token_name='old name' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "GRANT SELECT ON "+name+".* TO '"+user+"'@'localhost'"); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("mysql", user+"@"+socket+"/"+name+"?timeout=2s&readTimeout=5s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader.SetMaxOpenConns(1)
	for _, q := range []string{"UPDATE users SET quota=0 WHERE id=201", "DELETE FROM logs WHERE id=1"} {
		if _, err := reader.ExecContext(ctx, q); err == nil {
			t.Fatal("fixture reader can write source")
		}
	}
	var uuid string
	if err := reader.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatal(err)
	}
	pin, err := WalletSourceIdentity(uuid, name)
	if err != nil {
		t.Fatal(err)
	}
	req := UsageWindowRequest{SourceIdentity: pin, Rules: fundingTestRules(), UserID: 201, From: from, To: from + 3600}
	started := time.Now()
	o, err := CaptureUsageWindow(ctx, reader, req)
	if err != nil || len(o.Rows) != 6 || *o.Wallet.BalanceQuota != -1 || *o.Wallet.UsedQuota != 9007199254740993 {
		t.Fatal("real MySQL observation failed", err)
	}
	elapsed := time.Since(started)
	want := []struct {
		kind           int
		source         string
		records, quota int64
	}{{2, "invalid_json", 1, 9}, {2, "invalid_source", 4, 54}, {2, "subscription", 1, 11}, {2, "unspecified", 2, 42}, {2, "wallet", 50001, 50000}, {6, "wallet", 1, 17}}
	for i, r := range o.Rows {
		x := want[i]
		if r.Type != x.kind || r.BillingSource != x.source || r.Records != x.records || r.Quota != x.quota || r.FirstAt < from || r.LastAt >= req.To {
			t.Fatal("boundary, source classification or aggregate changed", r)
		}
	}
	tokenStarted := time.Now()
	tokenView, err := CaptureTokenUsageWindow(ctx, reader, req)
	if err != nil || len(tokenView.Rows) != 8 || !reflect.DeepEqual(tokenView.Totals, o.Rows) {
		t.Fatal("real token SQL differs from independent user aggregates", err, tokenView)
	}
	var knownTokens int
	for _, row := range tokenView.Rows {
		if row.TokenID == 11 || row.TokenID == 12 {
			knownTokens++
			if row.Quota != 25000 || row.Name != "same name" || row.Records != 25000 {
				t.Fatal("same-name IDs merged or renamed history lost", row)
			}
		}
	}
	if knownTokens != 2 {
		t.Fatal("token groups missing")
	}
	t.Logf("50,000-row token capture: four SELECT-only queries with independent exact totals in %s", time.Since(tokenStarted))
	local, _ := usageTestStore(t)
	id, err := SaveUsageWindowObservation(ctx, local, o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, "UPDATE users SET quota=9 WHERE id=201"); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, "UPDATE logs SET quota=2 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	updated, err := CaptureUsageWindow(ctx, reader, req)
	if err != nil || updated.Rows[4].Quota != 50001 || *updated.Wallet.BalanceQuota != 9 {
		t.Fatal("new capture did not observe source changes", err)
	}
	tokenChanged, err := CaptureTokenUsageWindow(ctx, reader, req)
	if err != nil || TokenUsageWindowsMatch(tokenView, tokenChanged) {
		t.Fatal("changed source token capture incorrectly matched", err)
	}
	// An attribution-only change must be detected even when the user amount
	// does not move. Do not mistake organization-level equality for accuracy.
	if _, err := writer.ExecContext(ctx, "UPDATE logs SET token_id=12 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	tokenMoved, err := CaptureTokenUsageWindow(ctx, reader, req)
	if err != nil || !reflect.DeepEqual(tokenMoved.Totals, tokenChanged.Totals) || TokenUsageWindowsMatch(tokenChanged, tokenMoved) {
		t.Fatal("token reassignment hidden by equal user total", err)
	}
	if _, err := SaveUsageWindowObservation(ctx, local, updated); err != nil {
		t.Fatal(err)
	}
	old, err := LoadUsageWindowObservation(ctx, local, id)
	if err != nil || !reflect.DeepEqual(old, o) {
		t.Fatal("new capture changed original evidence", err)
	}
	for _, invalid := range []any{nil, int64(-1), int64(9223372036854775807)} {
		if _, err := writer.ExecContext(ctx, "UPDATE logs SET quota=? WHERE id=1", invalid); err != nil {
			t.Fatal(err)
		}
		if x, err := CaptureUsageWindow(ctx, reader, req); err == nil || x.Version != 0 {
			t.Fatal("NULL, negative or overflowing source total accepted", invalid, err)
		}
	}
	if _, err := writer.ExecContext(ctx, "ALTER TABLE logs DROP INDEX idx_user_created_type"); err != nil {
		t.Fatal(err)
	}
	if x, err := CaptureUsageWindow(ctx, reader, req); err == nil || x.Version != 0 {
		t.Fatal("missing reviewed index used unbounded fallback", err)
	}
	if x, err := CaptureTokenUsageWindow(ctx, reader, req); err == nil || x.Version != 0 {
		t.Fatal("token capture silently fell back without required index")
	}
	if reader.Stats().InUse != 0 {
		t.Fatal("MySQL connection retained")
	}
	t.Logf("50,000-row source: three SELECT-only queries, six exact aggregates in %s; current wallet remains an observation, not a historical balance", elapsed)
}
