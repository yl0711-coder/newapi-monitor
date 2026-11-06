package monthlysource

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

// This opt-in test can reach ONLY a disposable container's fixed Unix socket.
// It accepts no host, production DSN, SSH settings or external credentials.
func TestFundingMonthCaptureMySQLReadOnlyPagesAndResume(t *testing.T) {
	if os.Getenv("MONTHLY_FUNDING_LOCAL_MYSQL") != "1" {
		t.Skip("isolated local MySQL fixture only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	const socket = "unix(/var/run/mysqld/mysqld.sock)"
	admin, err := sql.Open("mysql", "root@"+socket+"/?timeout=2s&readTimeout=4s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := fmt.Sprintf("auto_funding_%d", time.Now().UnixNano())
	user := fmt.Sprintf("auto_read_%d", time.Now().UnixNano())
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
	if _, err := writer.ExecContext(ctx, `CREATE TABLE logs(id BIGINT PRIMARY KEY,user_id BIGINT NOT NULL,created_at BIGINT NOT NULL,type INT NOT NULL,content TEXT NULL,other MEDIUMTEXT NULL,INDEX idx_created_at_id(created_at,id),INDEX idx_created_at_type(created_at,type),INDEX idx_user_created_type(user_id,created_at,type)) ENGINE=InnoDB`); err != nil {
		t.Fatal(err)
	}
	const from int64 = 1788192000
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	insert := func(id, uid, at int64, kind int, content, other string) {
		t.Helper()
		if _, err := tx.ExecContext(ctx, `INSERT INTO logs VALUES(?,?,?,?,?,?)`, id, uid, at, kind, content, other); err != nil {
			t.Fatal(err)
		}
	}
	for id := int64(1); id <= 205; id++ {
		insert(id, 201, from+10, 4, "新用户注册赠送 1 点额度", "")
	}
	insert(206, 999, from+10, 4, "新用户注册赠送 999 点额度", "")
	insert(207, 201, from+10, 2, "consume", "")
	insert(208, 9, from+10, 3, "", fundingTestAudit("user.quota_add", `{"quota":"500000 点额度","target_user_id":202}`))
	insert(209, 201, from+86400, 4, "新用户注册赠送 7 点额度", "")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "GRANT SELECT ON "+name+".logs TO '"+user+"'@'localhost'"); err != nil {
		t.Fatal(err)
	}
	dsn := user + "@" + socket + "/" + name + "?timeout=2s&readTimeout=4s&writeTimeout=2s"
	reader, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader.SetMaxOpenConns(1)
	if _, err := reader.ExecContext(ctx, `UPDATE logs SET content='forbidden' WHERE id=1`); err == nil {
		t.Fatal("test source user can write")
	}
	var uuid string
	if err := reader.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatal(err)
	}
	pin, err := WalletSourceIdentity(uuid, name)
	if err != nil {
		t.Fatal(err)
	}
	receiver, path, key, scope, plan := fundingMonthCaptureFixture(t, pin)
	first, err := CaptureNextFundingMonthPage(ctx, reader, receiver, key, scope)
	if err != nil || first.FinishedScans != 0 || first.NextScanID != plan.Scans[0].ID {
		t.Fatal(first, err)
	}
	head, err := LoadLatestFundingPage(ctx, receiver, plan.Scans[0].ID)
	if err != nil || head.EndReached || head.Next.ID != 200 || head.FenceID != 209 {
		t.Fatal(head, err)
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	// A late insert behind the cursor must NOT widen the retained first sweep.
	if _, err := writer.ExecContext(ctx, `INSERT INTO logs VALUES(?,?,?,?,?,?)`, 210, 201, from+5, 4, "新用户注册赠送 13 点额度", ""); err != nil {
		t.Fatal(err)
	}
	receiver, err = sql.Open("sqlite", path+"?_pragma=busy_timeout(500)")
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	receiver.SetMaxOpenConns(1)
	reader, err = sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader.SetMaxOpenConns(1)
	retained, err := EnsureFundingMonthCapture(ctx, receiver, key, scope)
	if err != nil || !reflect.DeepEqual(retained, plan) {
		t.Fatal("restart changed plan", err)
	}
	second, err := CaptureNextFundingMonthPage(ctx, reader, receiver, key, scope)
	if err != nil || second.FinishedScans != 1 || second.NextScanID != plan.Scans[1].ID {
		t.Fatal(second, err)
	}
	audit, err := auditFundingScan(ctx, receiver, plan.Scans[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, record := range audit.records {
		if record.hasDelta {
			total += record.delta
		}
	}
	if len(audit.records) != 206 || total != 500205 || audit.fence != 209 || audit.pages != 2 {
		t.Fatal("real money/cursor/fence mismatch", len(audit.records), total, audit.fence, audit.pages)
	}
	if _, ok := audit.records[210]; ok {
		t.Fatal("late row smuggled into old fence")
	}
	third, err := CaptureNextFundingMonthPage(ctx, reader, receiver, key, scope)
	if err != nil || third.FinishedScans != 2 || third.TraversalsFinished {
		t.Fatal("crossed more than one day", third, err)
	}
}
