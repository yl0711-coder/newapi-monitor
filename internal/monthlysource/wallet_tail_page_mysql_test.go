package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type pageSnapshotConnector struct {
	driver.Connector
	hook func() error
}

func (c pageSnapshotConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &pageSnapshotConn{Conn: conn, hook: c.hook}, nil
}

type pageSnapshotConn struct {
	driver.Conn
	hook func() error
}

func (c *pageSnapshotConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, o)
}
func (c *pageSnapshotConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(q, "AS index_page") || strings.Contains(q, "WITH usage_keys AS") {
		if err := c.hook(); err != nil {
			return nil, err
		}
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
}

func TestWalletTailPageMySQLReadOnlyBoundedRanges(t *testing.T) {
	if os.Getenv("MONTHLY_WALLET_LOCAL_MYSQL") != "1" {
		t.Skip("isolated synthetic MySQL socket fixture required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	const socket = "unix(/var/run/mysqld/mysqld.sock)"
	admin, err := sql.Open("mysql", "root@"+socket+"/?timeout=2s&readTimeout=5s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name, user := fmt.Sprintf("page_local_%d", time.Now().UnixNano()), fmt.Sprintf("page_%d", time.Now().UnixNano())
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
	for _, q := range []string{"CREATE TABLE users(id BIGINT PRIMARY KEY,created_at BIGINT,quota BIGINT,used_quota BIGINT) ENGINE=InnoDB", "CREATE TABLE logs(id BIGINT PRIMARY KEY,user_id BIGINT,created_at BIGINT,type INT,quota BIGINT,content TEXT,other TEXT,KEY idx_user_created_type(user_id,created_at,type),KEY idx_created_at_type(created_at,type)) ENGINE=InnoDB"} {
		if _, err := writer.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := admin.ExecContext(ctx, "GRANT SELECT ON "+name+".* TO '"+user+"'@'localhost'"); err != nil {
		t.Fatal(err)
	}
	boundary := walletTailMonthStart(time.Now().UnixMilli())
	now := time.Unix(boundary+14*86400, 37000000)
	if _, err := writer.ExecContext(ctx, "INSERT INTO users VALUES(?,?,?,?)", 201, boundary-1, int64(1000000000), int64(1000000)); err != nil {
		t.Fatal(err)
	}
	var wantC, wantR int64
	if _, err := writer.ExecContext(ctx, "INSERT INTO users VALUES(?,?,?,?)", 202, boundary-1, int64(123), int64(456)); err != nil {
		t.Fatal(err)
	}
	const count = 2*walletTailPageRows + 37
	for offset := 0; offset < count; offset += 500 {
		n := min(500, count-offset)
		args := []any{}
		for i := offset + 1; i <= offset+n; i++ {
			typ := int64(2)
			if i%7 == 0 {
				typ = 6
			}
			quota := int64(i % 5)
			if typ == 2 {
				wantC += quota
			} else {
				wantR += quota
			}
			args = append(args, int64(i), int64(201), boundary+10+int64(i/10000), typ, quota, "", `{"billing_source":"wallet"}`)
		}
		q := "INSERT INTO logs VALUES " + strings.TrimSuffix(strings.Repeat("(?,?,?,?,?,?,?),", n), ",")
		if _, err := writer.ExecContext(ctx, q, args...); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]any{{int64(90001), int64(999), boundary + 1, int64(3), int64(0), "admin-actor", `{"op":{"action":"user.quota_add","params":{"target_user_id":201,"quota":"1000 点额度"}},"admin_info":{"admin_id":999}}`}, {int64(90002), int64(202), boundary + 1, int64(1), int64(0), "foreign-private-topup", ""}, {int64(90003), int64(201), boundary + 1, int64(1), int64(0), "使用Creem充值成功，充值额度: 5，支付金额：0.01", ""}} {
		if _, err := writer.ExecContext(ctx, "INSERT INTO logs VALUES(?,?,?,?,?,?,?)", args...); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := mysql.ParseDSN(user + "@" + socket + "/" + name + "?timeout=2s&readTimeout=5s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	base, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	reader := sql.OpenDB(base)
	reader.SetMaxOpenConns(1)
	defer reader.Close()
	if _, err := reader.ExecContext(ctx, "UPDATE users SET quota=0"); err == nil {
		t.Fatal("source identity can write")
	}
	var uuid string
	if err := reader.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatal(err)
	}
	pin, err := WalletSourceIdentity(uuid, name)
	if err != nil {
		t.Fatal(err)
	}
	r := WalletTailPageRequest{Wallet: WalletTailRequest{SourceIdentity: pin, Rules: fundingTestRules(), UserID: 201, Boundary: boundary}, Kind: "user_usage"}
	var gotC, gotR, gotN int64
	pages := 0
	for {
		p, err := captureWalletTailPage(ctx, reader, r, func() time.Time { return now })
		if err != nil {
			t.Fatal("actual page SQL failed", err)
		}
		pages++
		gotN += int64(p.Index.Records)
		for _, v := range p.Usage {
			if v.Type == 2 {
				gotC += v.Quota
			} else {
				gotR += v.Quota
			}
		}
		if p.ViewEndReached {
			break
		}
		r.After = p.Index.Next
		if pages > 4 {
			t.Fatal("cursor did not terminate")
		}
	}
	if gotN != count+1 || gotC != wantC || gotR != wantR || pages != 3 {
		t.Fatal("actual engine lost/doubled dense-index records or changed money", gotN, gotC, gotR, pages)
	}
	r.Kind, r.After = "global_funding", WalletTailPageCursor{}
	var funding, other int
	seenActor := false
	var firstKeys []WalletTailPageCursor
	for pages = 0; pages < 5; pages++ {
		p, err := captureWalletTailPage(ctx, reader, r, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		funding += len(p.Funding)
		if pages == 0 {
			firstKeys = append(firstKeys, p.FundingKeys...)
		}
		other += p.OtherUserFundingRecords
		for _, row := range p.Funding {
			if row.UserID == 202 || strings.Contains(row.Content, "foreign-private") {
				t.Fatal("unrelated topup leaked")
			}
			if row.ID == 90001 && row.UserID == 999 {
				seenActor = true
			}
		}
		if p.ViewEndReached {
			break
		}
		r.After = p.Index.Next
	}
	if funding != 2 || other != 1 || !seenActor {
		t.Fatal("global source actor/beneficiary filtering failed", funding, other)
	}
	otherRequest := r
	otherRequest.Wallet.UserID, otherRequest.After = 202, WalletTailPageCursor{}
	otherPage, err := captureWalletTailPage(ctx, reader, otherRequest, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if otherPage.User.UserID != 202 || len(firstKeys) != 3 || !reflect.DeepEqual(firstKeys, otherPage.FundingKeys) {
		t.Fatal("global discovery metadata changed with the account")
	}
	for _, row := range otherPage.Funding {
		if row.Type != 3 && row.UserID != 202 {
			t.Fatal("another account's funding was reused as money")
		}
	}
	// A tiny bounded key table must drive PRIMARY eq_ref, not a range-optimizer
	// fallback that scans the full account/month. Check actual engine metadata.
	r.Kind = "user_usage"
	q, args, err := tailPageBodyQuery(r, []any{int64(1), int64(2)})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := reader.QueryContext(ctx, "EXPLAIN "+q, args...)
	if err != nil {
		t.Fatal(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	primary := false
	for rows.Next() {
		v := make([]sql.NullString, len(columns))
		dest := make([]any, len(columns))
		for i := range v {
			dest[i] = &v[i]
		}
		if rows.Scan(dest...) != nil {
			t.Fatal("plan row invalid")
		}
		m := map[string]string{}
		for i, c := range columns {
			m[c] = v[i].String
		}
		if m["table"] == "logs" && m["key"] == "PRIMARY" && m["type"] == "eq_ref" && m["rows"] == "1" {
			primary = true
		}
	}
	if rows.Err() != nil || rows.Close() != nil || !primary {
		t.Fatal("bounded key table did not use one-row PRIMARY lookups")
	}
	// Commit a concurrent writer AFTER the collector has read users. Both its
	// wallet and selected amount must remain from the first repeatable-read view.
	called := false
	hooked := sql.OpenDB(pageSnapshotConnector{Connector: base, hook: func() error {
		if called {
			return nil
		}
		called = true
		if _, err := writer.ExecContext(ctx, "UPDATE logs SET quota=quota+100 WHERE id=1"); err != nil {
			return err
		}
		_, err := writer.ExecContext(ctx, "UPDATE users SET quota=quota-100,used_quota=used_quota+100 WHERE id=201")
		return err
	}})
	hooked.SetMaxOpenConns(1)
	defer hooked.Close()
	r.After = WalletTailPageCursor{}
	old, err := captureWalletTailPage(ctx, hooked, r, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := captureWalletTailPage(ctx, reader, r, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	sum := func(p WalletTailPageObservation) int64 {
		var n int64
		for _, row := range p.Usage {
			if row.Type == 2 {
				n += row.Quota
			}
		}
		return n
	}
	if !called || *fresh.User.BalanceQuota != *old.User.BalanceQuota-100 || *fresh.User.UsedQuota != *old.User.UsedQuota+100 || sum(fresh) != sum(old)+100 {
		t.Fatal("source view mixed concurrent money")
	}
	// The full paged reader must remain byte-for-byte equivalent to the old
	// complete same-view format, not a join of earlier cached observations.
	baseline, err := captureWalletTail(ctx, reader, r.Wallet, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	paged, err := captureWalletTailPaged(ctx, reader, r.Wallet, func() time.Time { return now }, walletTailJournalPages)
	if err != nil || !reflect.DeepEqual(baseline, paged) {
		t.Fatal("full same-view paged output differs from legacy source semantics", err)
	}
	if partial, err := captureWalletTailPaged(ctx, reader, r.Wallet, func() time.Time { return now }, 1); err == nil || partial.Version != 0 {
		t.Fatal("actual page work limit returned partial full-tail evidence")
	}
	changed := false
	fullHook := sql.OpenDB(pageSnapshotConnector{Connector: base, hook: func() error {
		if changed {
			return nil
		}
		changed = true
		tx, err := writer.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, "UPDATE logs SET quota=quota+7 WHERE id=2"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO logs VALUES(?,?,?,?,?,?,?)", int64(999991), int64(201), boundary+1, int64(2), int64(11), "", `{"billing_source":"wallet"}`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE users SET quota=quota-18,used_quota=used_quota+18 WHERE id=201"); err != nil {
			return err
		}
		return tx.Commit()
	}})
	fullHook.SetMaxOpenConns(1)
	defer fullHook.Close()
	during, err := captureWalletTailPaged(ctx, fullHook, r.Wallet, func() time.Time { return now }, walletTailJournalPages)
	if err != nil || !changed || !reflect.DeepEqual(during, baseline) {
		t.Fatal("full paged view mixed an atomic concurrent writer/late insert", err)
	}
	later, err := captureWalletTailPaged(ctx, reader, r.Wallet, func() time.Time { return now.Add(5 * time.Minute) }, walletTailJournalPages)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DeriveWalletTailBoundary(during, later); err != nil {
		t.Fatal("existing independent boundary gate rejected consistent full source views", err)
	}
	for i := int64(0); i < 211; i++ {
		_, err := writer.ExecContext(ctx, "INSERT INTO logs VALUES(?,?,?,?,?,?,?)", 950000+i, int64(999), boundary+2, int64(3), int64(0), "other-target-admin", `{"op":{"action":"user.quota_add","params":{"target_user_id":202,"quota":"7 点额度"}},"admin_info":{"admin_id":999}}`)
		if err != nil {
			t.Fatal(err)
		}
	}
	denseLegacy, err := captureWalletTail(ctx, reader, r.Wallet, func() time.Time { return now.Add(6 * time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	denseSelective, err := captureWalletTailPaged(ctx, reader, r.Wallet, func() time.Time { return now.Add(6 * time.Minute) }, walletTailJournalPages)
	if err != nil || len(denseSelective.Funding) != 213 || !reflect.DeepEqual(denseLegacy, denseSelective) {
		t.Fatal("selective funding repeated/skipped dense same-second IDs or changed account scope", err)
	}
	for i, other := range []string{`{"billing_source":"wallet"}`, `{"billing_source":"subscription"}`, "", "invalid-json", `[]`} {
		for _, typ := range []int64{2, 6} {
			_, err := writer.ExecContext(ctx, "INSERT INTO logs VALUES(?,?,?,?,?,?,?)", int64(970000+i*10)+typ, int64(201), boundary+3, typ, int64(2), "", other)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	classesLegacy, err := captureWalletTail(ctx, reader, r.Wallet, func() time.Time { return now.Add(7 * time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	classesServer, err := captureWalletTailPaged(ctx, reader, r.Wallet, func() time.Time { return now.Add(7 * time.Minute) }, walletTailJournalPages)
	if err != nil || len(classesServer.Usage) != usageWindowRowsLimit || !reflect.DeepEqual(classesLegacy, classesServer) {
		t.Fatal("server aggregation changed real SQL billing-source class semantics", err)
	}
	if _, err := writer.ExecContext(ctx, "UPDATE logs SET quota=NULL WHERE id=970002"); err != nil {
		t.Fatal(err)
	}
	if partial, err := captureWalletTailPaged(ctx, reader, r.Wallet, func() time.Time { return now.Add(7 * time.Minute) }, walletTailJournalPages); err == nil || partial.Version != 0 {
		t.Fatal("NULL quota silently omitted by server SUM")
	}
	if _, err := writer.ExecContext(ctx, "UPDATE logs SET quota=2 WHERE id=970002"); err != nil {
		t.Fatal(err)
	}
	for _, after := range []FundingCursor{{}, {At: boundary + 2, ID: 950198}} {
		q, args, err := selectiveTailFundingQuery(r.Wallet, after)
		if err != nil {
			t.Fatal(err)
		}
		planRows, err := reader.QueryContext(ctx, "EXPLAIN "+q, args...)
		if err != nil {
			t.Fatal(err)
		}
		cols, err := planRows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		primaryBody := false
		for planRows.Next() {
			values := make([]sql.NullString, len(cols))
			dest := make([]any, len(cols))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := planRows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			m := map[string]string{}
			for i, c := range cols {
				m[c] = values[i].String
			}
			if m["table"] == "logs" && strings.Contains(m["Extra"], "Using filesort") {
				t.Fatal("funding branch sorts an unbounded month range")
			}
			if m["table"] == "l" && m["key"] == "PRIMARY" && m["type"] == "eq_ref" && m["rows"] == "1" {
				primaryBody = true
			}
		}
		if planRows.Err() != nil || planRows.Close() != nil || !primaryBody {
			t.Fatal("selective funding body does not use one-row PRIMARY lookup")
		}
	}
	for _, after := range []WalletTailPageCursor{{}, {At: boundary + 10, Type: 2, ID: 8192}} {
		q, args, err := serverTailUsageQuery(WalletTailPageRequest{Wallet: r.Wallet, Kind: "user_usage", After: after})
		if err != nil {
			t.Fatal(err)
		}
		planRows, err := reader.QueryContext(ctx, "EXPLAIN "+q, args...)
		if err != nil {
			t.Fatal(err)
		}
		cols, err := planRows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		primaryBody := false
		for planRows.Next() {
			values := make([]sql.NullString, len(cols))
			dest := make([]any, len(cols))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := planRows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			m := map[string]string{}
			for i, c := range cols {
				m[c] = values[i].String
			}
			if m["table"] == "l" && m["key"] == "PRIMARY" && m["type"] == "eq_ref" && m["rows"] == "1" {
				primaryBody = true
			}
		}
		if planRows.Err() != nil || planRows.Close() != nil || !primaryBody {
			t.Fatal("server-side bounded aggregation does not use one-row PRIMARY lookup")
		}
	}
	if _, err := writer.ExecContext(ctx, "ALTER TABLE logs DROP INDEX idx_user_created_type"); err != nil {
		t.Fatal(err)
	}
	failed, err := captureWalletTailPage(ctx, reader, r, func() time.Time { return now })
	if err == nil || failed.Version != 0 || WalletTailFailureStage(err) != "tail_index" {
		t.Fatal("missing index fell back or returned partial data")
	}
	failedFull, err := captureWalletTailPaged(ctx, reader, r.Wallet, func() time.Time { return now }, walletTailJournalPages)
	if err == nil || failedFull.Version != 0 || WalletTailFailureStage(err) != "tail_usage" {
		t.Fatal("full paged reader fell back after index failure")
	}
	t.Log("actual SELECT-only MySQL; page and complete-view parity, PRIMARY lookups, other-actor funding and atomic-writer/late-insert RR money consistency verified")
}
