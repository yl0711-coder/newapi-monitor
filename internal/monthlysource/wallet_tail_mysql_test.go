package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Test transport hook only: the actual server, transaction, SQL, indexes and
// SELECT-only identity are real. A separate writer commits BETWEEN reading
// users and logs, so a non-repeatable capture would produce incorrect money.
type walletTailHookConnector struct {
	driver.Connector
	hook func() error
}

func (c *walletTailHookConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &walletTailHookConn{Conn: conn, hook: c.hook}, nil
}

type walletTailHookConn struct {
	driver.Conn
	hook func() error
}

func (c *walletTailHookConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func (c *walletTailHookConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query == walletTailUsageQuery {
		if err := c.hook(); err != nil {
			return nil, err
		}
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func TestWalletTailMySQLReadOnlySnapshot(t *testing.T) {
	if os.Getenv("MONTHLY_WALLET_LOCAL_MYSQL") != "1" {
		t.Skip("isolated synthetic MySQL fixture only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const socket = "unix(/var/run/mysqld/mysqld.sock)"
	admin, err := sql.Open("mysql", "root@"+socket+"/?timeout=2s&readTimeout=5s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := fmt.Sprintf("tail_local_%d", time.Now().UnixNano())
	user := fmt.Sprintf("tail_%d", time.Now().UnixNano())
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
		"CREATE TABLE users(id BIGINT PRIMARY KEY,created_at BIGINT,quota BIGINT,used_quota BIGINT) ENGINE=InnoDB",
		"CREATE TABLE logs(id BIGINT PRIMARY KEY,user_id BIGINT,created_at BIGINT,type INT,quota BIGINT,content TEXT,other TEXT,INDEX idx_user_created_type(user_id,created_at,type),INDEX idx_created_at_type(created_at,type)) ENGINE=InnoDB",
	} {
		if _, err := writer.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := admin.ExecContext(ctx, "GRANT SELECT ON "+name+".* TO '"+user+"'@'localhost'"); err != nil {
		t.Fatal(err)
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
	defer reader.Close()
	reader.SetMaxOpenConns(1)
	if _, err := reader.ExecContext(ctx, "UPDATE users SET quota=0"); err == nil {
		t.Fatal("source fixture identity can write")
	}
	var uuid string
	if err := reader.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatal(err)
	}
	pin, err := WalletSourceIdentity(uuid, name)
	if err != nil {
		t.Fatal(err)
	}
	boundary := walletTailMonthStart(time.Now().UnixMilli())
	// Fixed physical clock for a synthetic CURRENT month; no waiting five
	// minutes and no representation that this fixture proves elapsed real time.
	now := time.Unix(boundary+14*86400, 123000000)
	request := WalletTailRequest{SourceIdentity: pin, Rules: fundingTestRules(), UserID: 201, Boundary: boundary}
	if _, err := writer.ExecContext(ctx, "INSERT INTO users VALUES(?,?,?,?)", 201, boundary-86400, 1720, 480); err != nil {
		t.Fatal(err)
	}
	for _, row := range []FundingLog{
		{ID: 31, UserID: 100, CreatedAt: boundary + 2, Type: 3, Content: "target not actor", Other: fundingTestAudit("user.quota_add", `{"target_user_id":201,"quota":"1000 点额度"}`)},
		{ID: 32, UserID: 100, CreatedAt: now.Unix(), Type: 3, Content: "unrelated beneficiary", Other: fundingTestAudit("user.quota_add", `{"target_user_id":999,"quota":"9007199254740993 点额度"}`)},
	} {
		if _, err := writer.ExecContext(ctx, "INSERT INTO logs VALUES(?,?,?,?,?,?,?)", row.ID, row.UserID, row.CreatedAt, row.Type, 0, row.Content, row.Other); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range []struct{ id, kind, quota, at int64 }{{11, 2, 300, boundary + 1}, {12, 6, 20, now.Unix()}} {
		if _, err := writer.ExecContext(ctx, "INSERT INTO logs VALUES(?,?,?,?,?,?,?)", row.id, 201, row.at, row.kind, row.quota, "", `{"billing_source":"wallet"}`); err != nil {
			t.Fatal(err)
		}
	}
	hooked := false
	hook := func() error {
		if hooked {
			return fmt.Errorf("unexpected second hook")
		}
		hooked = true
		tx, err := writer.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		// Writer is intentionally atomic in this one synthetic fixture. This
		// demonstrates read-view isolation, not NewAPI transaction guarantees.
		for _, query := range []string{
			"UPDATE users SET quota=1880,used_quota=520 WHERE id=201",
			fmt.Sprintf(`INSERT INTO logs VALUES(13,201,%d,2,50,'','{"billing_source":"wallet"}')`, now.Unix()),
			fmt.Sprintf(`INSERT INTO logs VALUES(14,201,%d,6,10,'','{"billing_source":"wallet"}')`, now.Unix()),
			fmt.Sprintf("INSERT INTO logs VALUES(33,201,%d,1,0,'使用Creem充值成功，充值额度: 200，支付金额：0.01','')", now.Unix()),
		} {
			if _, err := tx.ExecContext(ctx, query); err != nil {
				return err
			}
		}
		return tx.Commit()
	}
	snapshotReader := sql.OpenDB(&walletTailHookConnector{Connector: base, hook: hook})
	defer snapshotReader.Close()
	snapshotReader.SetMaxOpenConns(1)
	a, err := captureWalletTail(ctx, snapshotReader, request, func() time.Time { return now })
	if err != nil || !hooked || *a.User.BalanceQuota != 1720 || *a.User.UsedQuota != 480 || len(a.Usage) != 2 || a.Usage[0].Quota != 300 || a.Usage[1].Quota != 20 || len(a.Funding) != 2 {
		t.Fatal("concurrent commit contaminated source snapshot", a, err)
	}
	now = now.Add(5 * time.Minute)
	b, err := captureWalletTail(ctx, reader, request, func() time.Time { return now })
	if err != nil || *b.User.BalanceQuota != 1880 || *b.User.UsedQuota != 520 || b.Usage[0].Quota != 350 || b.Usage[1].Quota != 30 || len(b.Funding) != 3 {
		t.Fatal("second actual view missed committed rows", b, err)
	}
	result, err := DeriveWalletTailBoundary(a, b)
	if err != nil || result.BalanceQuota() != 1000 || result.UsedQuota() != 200 {
		t.Fatal("real MySQL pair changed closed boundary", result, err)
	}
	// SQL keyset pages, not merely a driver-supplied preassembled array.
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for i := 0; i < 401; i++ {
		if _, err := tx.ExecContext(ctx, "INSERT INTO logs VALUES(?,?,?,?,?,?,?)", 1000+i, 100, boundary+2, 3, 0, "管理员增加用户额度 1 点额度", ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	paged, err := captureWalletTail(ctx, reader, request, func() time.Time { return now })
	if err != nil || len(paged.Funding) != 404 {
		t.Fatal("real union/keyset pages lost rows", len(paged.Funding), err)
	}
	t.Run("actual_capture_checkpoint_restart_and_offline_reuse", func(t *testing.T) {
		// More than two usage pages, all in the SAME second. A balance/used
		// update offsets the exact charges; the closed boundary must not move.
		const extra = 16421
		if _, err := writer.ExecContext(ctx, "SET SESSION cte_max_recursion_depth=17000"); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.ExecContext(ctx, `INSERT INTO logs(id,user_id,created_at,type,quota,content,other)
WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM seq WHERE n+1<?)
SELECT 100000+n,201,?,2,1,'','{"billing_source":"wallet"}' FROM seq`, extra, now.Unix()-1); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.ExecContext(ctx, "UPDATE users SET quota=quota-?,used_quota=used_quota+? WHERE id=201", extra, extra); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.ExecContext(ctx, "INSERT INTO users VALUES(?,?,?,?)", 202, boundary-86400, 0, 0); err != nil {
			t.Fatal(err)
		}
		receiver, file, key, scope, _ := fundingMonthCaptureFixture(t, pin)
		if err := InitWalletTailCaptureStore(ctx, receiver); err != nil {
			t.Fatal(err)
		}
		if err := InitWalletTailCaptureStore(ctx, reader); err == nil {
			t.Fatal("source handle accepted private receiver setup")
		}
		want := make([]WalletTailObservation, len(scope.Funding.UserIDs))
		for i, id := range scope.Funding.UserIDs {
			r := request
			r.UserID = id
			want[i], err = captureWalletTail(ctx, reader, r, func() time.Time { return now })
			if err != nil {
				t.Fatal("independent legacy reference failed", err)
			}
		}
		for i := 1; i <= 2; i++ {
			p, err := captureNextWalletTailMonthView(ctx, reader, receiver, key, scope, true, func() time.Time { return now })
			if err != nil || p.Completed != i || !p.Captured {
				t.Fatal("real source original/checkpoint not committed together", p, err)
			}
		}
		if err := receiver.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := sql.Open("sqlite", "file:"+file+"?mode=rw&_pragma=busy_timeout(500)")
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		reopened.SetMaxOpenConns(1)
		now = now.Add(5 * time.Minute)
		for i := 3; i <= 4; i++ {
			p, err := captureNextWalletTailMonthView(ctx, reader, reopened, key, scope, false, func() time.Time { return now })
			if err != nil || p.Completed != i || !p.Captured {
				t.Fatal("real paired capture failed after receiver restart", p, err)
			}
		}
		_, set, err := inspectWalletTailSet(ctx, reopened, key, scope, now)
		if err != nil || len(set.pairs) != 2 {
			t.Fatal("saved source scope not restored", err)
		}
		for i, pair := range set.pairs {
			newReference := want[i]
			newReference.StartedAtMS, newReference.CompletedAtMS = now.UnixMilli(), now.UnixMilli()
			if !reflect.DeepEqual(pair.Older, want[i]) || !reflect.DeepEqual(pair.Newer, newReference) {
				t.Fatal("runtime paging or receiver restart changed the native original")
			}
			b, err := DeriveWalletTailBoundary(pair.Older, pair.Newer)
			balance, used := int64(0), int64(0)
			if i == 0 {
				balance, used = 1000, 200
			}
			if err != nil || b.BalanceQuota() != balance || b.UsedQuota() != used {
				t.Fatal("restored exact money differs from independent real source", i, b, err)
			}
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
		ro, err := sql.Open("sqlite", "file:"+file+"?mode=ro&_pragma=query_only(1)")
		if err != nil {
			t.Fatal(err)
		}
		defer ro.Close()
		offline := sql.OpenDB(base)
		if err := offline.Close(); err != nil {
			t.Fatal(err)
		}
		p, err := captureNextWalletTailMonthView(ctx, offline, ro, key, scope, false, func() time.Time { return now })
		if err != nil || p.Completed != 4 || p.Captured {
			t.Fatal("offline reuse contacted source or changed committed originals", p, err)
		}
	})
	// Missing the reviewed source index must fail, not fall back to a scan.
	if _, err := writer.ExecContext(ctx, "ALTER TABLE logs DROP INDEX idx_created_at_type"); err != nil {
		t.Fatal(err)
	}
	if partial, err := captureWalletTail(ctx, reader, request, func() time.Time { return now }); err == nil || partial.Version != 0 {
		t.Fatal("missing index returned a partial original", err)
	}
	if reader.Stats().InUse != 0 || snapshotReader.Stats().InUse != 0 {
		t.Fatal("source connection leaked")
	}
}
