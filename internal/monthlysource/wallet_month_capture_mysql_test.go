package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

// The only possible transport is this disposable container's Unix socket.
// No production hostname, credential file, DSN or environment is accepted.
func TestWalletMonthCaptureMySQLReadOnlyScopeAndRestartWait(t *testing.T) {
	if os.Getenv("MONTHLY_WALLET_LOCAL_MYSQL") != "1" {
		t.Skip("isolated local MySQL fixture only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	const socket = "unix(/var/run/mysqld/mysqld.sock)"
	admin, err := sql.Open("mysql", "root@"+socket+"/?timeout=2s&readTimeout=4s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := fmt.Sprintf("auto_wallet_%d", time.Now().UnixNano())
	user := fmt.Sprintf("wallet_read_%d", time.Now().UnixNano())
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
		`INSERT INTO users VALUES(201,-1,9007199254740993),(202,3750000,1),(999,100000000,9)`,
	} {
		if _, err := writer.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := admin.ExecContext(ctx, "GRANT SELECT ON "+name+".users TO '"+user+"'@'localhost'"); err != nil {
		t.Fatal(err)
	}
	dsn := user + "@" + socket + "/" + name + "?timeout=2s&readTimeout=4s&writeTimeout=2s"
	reader, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader.SetMaxOpenConns(1)
	if _, err := reader.ExecContext(ctx, `UPDATE users SET quota=0 WHERE id=201`); err == nil {
		t.Fatal("source reader can write")
	}
	var uuid string
	if err := reader.QueryRowContext(ctx, `SELECT @@server_uuid`).Scan(&uuid); err != nil {
		t.Fatal(err)
	}
	pin, err := WalletSourceIdentity(uuid, name)
	if err != nil {
		t.Fatal(err)
	}
	receiver, path, key, scope, _ := fundingMonthCaptureFixture(t, pin)
	if err := InitWalletMonthCaptureStore(ctx, receiver); err != nil {
		t.Fatal(err)
	}
	first, err := CaptureNextWalletMonthView(ctx, reader, receiver, key, scope)
	if err != nil || first.Completed != 1 || !first.Captured {
		t.Fatal(first, err)
	}
	original, err := LoadWalletObservation(ctx, receiver, first.IDs[0])
	if err != nil || len(original.Users) != 2 || original.Users[0].UserID != 201 || original.Users[1].UserID != 202 || *original.Users[0].BalanceQuota != -1 || *original.Users[0].UsedQuota != 9007199254740993 || *original.Users[1].BalanceQuota != 3750000 {
		t.Fatal(original, err)
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	receiver, err = sql.Open("sqlite", path+"?_pragma=busy_timeout(500)")
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	receiver.SetMaxOpenConns(1)
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	// A closed source handle proves that waiting/restart inspection makes no
	// Ping or query attempt. The original observation remains unchanged.
	waiting, err := CaptureNextWalletMonthView(ctx, reader, receiver, key, scope)
	if err != nil || waiting.Completed != 1 || waiting.Captured || waiting.NextEligibleMS <= time.Now().UnixMilli() {
		t.Fatal("wait contacted closed source", waiting, err)
	}
	retained, err := LoadWalletObservation(ctx, receiver, first.IDs[0])
	if err != nil || !reflect.DeepEqual(retained, original) {
		t.Fatal("restart changed original", err)
	}
	reader, err = sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader.SetMaxOpenConns(1)
	if _, err := writer.ExecContext(ctx, `DELETE FROM users WHERE id=202`); err != nil {
		t.Fatal(err)
	}
	missingKey := digest([]byte("synthetic missing account job"))
	if _, err := EnsureFundingMonthCapture(ctx, receiver, missingKey, scope); err != nil {
		t.Fatal(err)
	}
	if _, err := CaptureNextWalletMonthView(ctx, reader, receiver, missingKey, scope); !errors.Is(err, ErrWalletMonthCaptureIntegrity) {
		t.Fatal("missing account became zero", err)
	}
	t.Log("batch scope, negative balance, >2^53 counter, immutable restart and no-source wait verified; missing account remains explicit")
}
