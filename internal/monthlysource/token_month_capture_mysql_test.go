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

// Fixed disposable-container socket only: no production DSN or configuration.
func TestTokenMonthCaptureMySQLLargeDayAndResume(t *testing.T) {
	if os.Getenv("MONTHLY_USAGE_LOCAL_MYSQL") != "1" {
		t.Skip("isolated local MySQL fixture only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const socket = "unix(/var/run/mysqld/mysqld.sock)"
	admin, err := sql.Open("mysql", "root@"+socket+"/?timeout=2s&readTimeout=4s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := fmt.Sprintf("auto_token_%d", time.Now().UnixNano())
	user := fmt.Sprintf("token_read_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.ExecContext(context.Background(), "DROP DATABASE "+name) }()
	if _, err := admin.ExecContext(ctx, "CREATE USER '"+user+"'@'localhost'"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.ExecContext(context.Background(), "DROP USER '"+user+"'@'localhost'") }()
	writer, err := sql.Open("mysql", "root@"+socket+"/"+name+"?timeout=2s&readTimeout=4s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	for _, q := range []string{
		`CREATE TABLE users(id BIGINT PRIMARY KEY,quota BIGINT NULL,used_quota BIGINT NULL) ENGINE=InnoDB`,
		`CREATE TABLE logs(id BIGINT PRIMARY KEY,user_id BIGINT NOT NULL,created_at BIGINT NOT NULL,type INT NOT NULL,quota BIGINT NULL,other LONGTEXT NULL,token_id BIGINT NULL,token_name TEXT NULL,INDEX idx_user_created_type(user_id,created_at,type)) ENGINE=InnoDB`,
		`INSERT INTO users VALUES(201,-1,9007199254740993)`,
	} {
		if _, err := writer.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	const from int64 = 1788192000
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for batch := int64(0); batch < 100; batch++ {
		values := make([]string, 0, 500)
		args := make([]any, 0, 4000)
		for offset := int64(1); offset <= 500; offset++ {
			id := batch*500 + offset
			token := int64(11)
			if id > 25000 {
				token = 12
			}
			values = append(values, "(?,?,?,?,?,?,?,?)")
			args = append(args, id, int64(201), from+id%86400, 2, int64(1), `{"billing_source":"wallet"}`, token, "same name")
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO logs VALUES "+strings.Join(values, ","), args...); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		`UPDATE logs SET token_name='old name' WHERE id=1`,
		fmt.Sprintf(`INSERT INTO logs VALUES(50001,201,%d,2,5,'{"billing_source":"wallet"}',NULL,NULL)`, from+10),
		fmt.Sprintf(`INSERT INTO logs VALUES(50002,201,%d,6,17,'{"billing_source":"wallet"}',11,'same name')`, from+11),
		fmt.Sprintf(`INSERT INTO logs VALUES(50003,999,%d,2,100000,'{"billing_source":"wallet"}',99,'foreign')`, from+12),
		fmt.Sprintf(`INSERT INTO logs VALUES(50004,201,%d,2,900000,'{"billing_source":"wallet"}',11,'same name')`, from+86400),
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "GRANT SELECT ON "+name+".* TO '"+user+"'@'localhost'"); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("mysql", user+"@"+socket+"/"+name+"?timeout=2s&readTimeout=4s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader.SetMaxOpenConns(1)
	if _, err := reader.ExecContext(ctx, `UPDATE users SET quota=0 WHERE id=201`); err == nil {
		t.Fatal("source user can write")
	}
	var uuid string
	if err := reader.QueryRowContext(ctx, `SELECT @@server_uuid`).Scan(&uuid); err != nil {
		t.Fatal(err)
	}
	pin, err := WalletSourceIdentity(uuid, name)
	if err != nil {
		t.Fatal(err)
	}
	receiver, path, key, scope, plan := tokenCaptureFixture(t, pin)
	started := time.Now()
	progress, err := CaptureNextTokenMonthSlot(ctx, reader, receiver, key, scope)
	if err != nil || progress.Completed != 1 || !progress.Captured {
		t.Fatal(progress, err)
	}
	original, err := loadTokenCaptureSlot(ctx, receiver, plan, 0)
	if err != nil || len(original.Rows) != 4 || len(original.Totals) != 2 || original.Totals[0].Quota != 50005 || original.Totals[1].Quota != 17 || *original.Wallet.BalanceQuota != -1 || *original.Wallet.UsedQuota != 9007199254740993 {
		t.Fatal("large source/user/token money differs", original, err)
	}
	if original.Rows[0].TokenID != 0 || original.Rows[1].TokenID != 11 || original.Rows[1].Quota != 25000 || original.Rows[1].Name != "same name" || original.Rows[3].TokenID != 12 || original.Rows[3].Quota != 25000 {
		t.Fatal("same-name tokens/legacy token merged", original.Rows)
	}
	elapsed := time.Since(started)
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	receiver, err = sql.Open("sqlite", path+"?_pragma=busy_timeout(500)")
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	receiver.SetMaxOpenConns(1)
	progress, err = CaptureNextTokenMonthSlot(ctx, reader, receiver, key, scope)
	if err != nil || progress.Completed != 2 {
		t.Fatal("restart did not advance exact next day", progress, err)
	}
	retained, err := loadTokenCaptureSlot(ctx, receiver, plan, 0)
	if err != nil || !reflect.DeepEqual(retained, original) {
		t.Fatal("restart rewrote previous observation", err)
	}
	second, err := loadTokenCaptureSlot(ctx, receiver, plan, 1)
	if err != nil || len(second.Totals) != 1 || second.Totals[0].Quota != 900000 {
		t.Fatal("calendar end boundary wrong", second, err)
	}
	t.Logf("50,002 selected consumption/refund rows aggregated without row export; same-name IDs, unknown token, refund, negative wallet, >2^53 wallet counter and restart verified; first bounded step %s", elapsed)
}
