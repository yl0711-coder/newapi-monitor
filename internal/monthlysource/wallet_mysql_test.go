package monthlysource

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Opt-in only in a throwaway --network none container. No supplied hostname,
// DSN or password; the fixture creates and removes its own synthetic database.
func TestWalletMySQLReadOnlyCaptureAndLocalPersistence(t *testing.T) {
	if os.Getenv("MONTHLY_WALLET_LOCAL_MYSQL") != "1" {
		t.Skip("isolated MySQL fixture only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const socket = "unix(/var/run/mysqld/mysqld.sock)"
	admin, err := sql.Open("mysql", "root@"+socket+"/?timeout=2s&readTimeout=5s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := fmt.Sprintf("wallet_local_%d", time.Now().UnixNano())
	user := fmt.Sprintf("wallet_%d", time.Now().UnixNano())
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
	if _, err := writer.ExecContext(ctx, "CREATE TABLE users(id BIGINT PRIMARY KEY,quota BIGINT NULL,used_quota BIGINT NULL,created_at BIGINT NULL) ENGINE=InnoDB"); err != nil {
		t.Fatal(err)
	}
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	registeredAt := time.Now().Add(-24 * time.Hour).Unix()
	for i := int64(1); i <= 200; i++ {
		if _, err := tx.ExecContext(ctx, "INSERT INTO users VALUES(?,?,?,?)", i, i-2, i*100, registeredAt); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "GRANT SELECT ON "+name+".users TO '"+user+"'@'localhost'"); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("mysql", user+"@"+socket+"/"+name+"?timeout=2s&readTimeout=4s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader.SetMaxOpenConns(1)
	if _, err := reader.ExecContext(ctx, "UPDATE users SET quota=0 WHERE id=1"); err == nil {
		t.Fatal("fixture source reader can write")
	}
	var uuid string
	if err := reader.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatal(err)
	}
	pin, err := WalletSourceIdentity(uuid, name)
	if err != nil {
		t.Fatal(err)
	}
	registered, err := ReadRegistrationTimes(ctx, reader, pin, []int64{2, 1})
	if err != nil || !reflect.DeepEqual(registered, []RegistrationTime{{1, registeredAt}, {2, registeredAt}}) {
		t.Fatal("real read-only registration metadata differs", registered, err)
	}
	if partial, err := ReadRegistrationTimes(ctx, reader, pin, []int64{1, 999}); err == nil || partial != nil {
		t.Fatal("missing real source user became a registration date")
	}
	first, err := CaptureWallet(ctx, reader, pin, []int64{999, 2, 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Users) != 3 || *first.Users[0].BalanceQuota != -1 || *first.Users[1].BalanceQuota != 0 || first.Users[2].Exists {
		t.Fatal("real MySQL capture lost zero/negative/missing distinction")
	}
	local, _ := walletTestStore(t)
	id, err := SaveWalletObservation(ctx, local, first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, "UPDATE users SET quota=123456789,used_quota=201 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	second, err := CaptureWallet(ctx, reader, pin, []int64{1, 2, 999})
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := SaveWalletObservation(ctx, local, second)
	if err != nil || secondID == id {
		t.Fatal("new snapshot replaced prior capture", err)
	}
	old, err := LoadWalletObservation(ctx, local, id)
	if err != nil || !reflect.DeepEqual(old, first) {
		t.Fatal("prior capture changed after source update", err)
	}
	ids := make([]int64, 200)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	started := time.Now()
	large, err := CaptureWallet(ctx, reader, pin, ids)
	if err != nil || len(large.Users) != 200 {
		t.Fatal("bounded maximum batch incomplete", err)
	}
	t.Run("registered_originals_same_select_only_view", func(t *testing.T) {
		// Original values from the throwaway fixture, not generated bill output.
		if _, err := writer.ExecContext(ctx, "UPDATE users SET quota=?,used_quota=? WHERE id=200", int64(9007199254740993), int64(math.MaxInt64)); err != nil {
			t.Fatal(err)
		}
		original, err := CaptureRegisteredWallet(ctx, reader, pin, ids)
		if err != nil || len(original.Users) != 200 || original.Users[0].RegisteredAt != registeredAt || *original.Users[0].BalanceQuota != 123456789 || *original.Users[0].UsedQuota != 201 || *original.Users[1].BalanceQuota != 0 || *original.Users[199].BalanceQuota != 9007199254740993 || *original.Users[199].UsedQuota != math.MaxInt64 {
			t.Fatal("real BIGINT registration/wallet source changed precision or account identity", err)
		}
		if bad, err := CaptureRegisteredWallet(ctx, reader, pin, []int64{1, 999}); err == nil || bad.Version != 0 || bad.Users != nil {
			t.Fatal("missing real account became a partial or zero source")
		}
		receiver, file := registeredWalletTestStore(t)
		id, err := SaveRegisteredWalletObservation(ctx, receiver, original)
		if err != nil {
			t.Fatal("original registered source was not retained", err)
		}
		if err := InitRegisteredWalletObservationStore(ctx, reader); err == nil {
			t.Fatal("receiver setup accepted a source MySQL handle")
		}
		if id, err := SaveRegisteredWalletObservation(ctx, reader, original); err == nil || id != "" {
			t.Fatal("source MySQL handle accepted a local retention write")
		}
		if _, err := writer.ExecContext(ctx, "UPDATE users SET quota=0,used_quota=1 WHERE id=200"); err != nil {
			t.Fatal(err)
		}
		later, err := CaptureRegisteredWallet(ctx, reader, pin, ids)
		if err != nil || *later.Users[199].BalanceQuota != 0 || *later.Users[199].UsedQuota != 1 {
			t.Fatal("new registered source did not capture new original", err)
		}
		if next, err := SaveRegisteredWalletObservation(ctx, receiver, later); err != nil || next == id {
			t.Fatal("later registered source replaced prior capture", err)
		}
		if err := receiver.Close(); err != nil {
			t.Fatal(err)
		}
		ro, err := sql.Open("sqlite", "file:"+file+"?mode=ro&_pragma=query_only(1)")
		if err != nil {
			t.Fatal(err)
		}
		defer ro.Close()
		got, err := LoadRegisteredWalletObservation(ctx, ro, id, pin, ids)
		if err != nil || !reflect.DeepEqual(got, original) {
			t.Fatal("reopen/source change altered immutable original fields", err)
		}
		// Actual task checkpoint: original+request reference commit together,
		// then recover without needing the live source connection at all.
		for _, id := range []int64{201, 202} {
			if _, err := writer.ExecContext(ctx, "INSERT INTO users VALUES(?,?,?,?)", id, int64(-1), int64(9007199254740993), registeredAt); err != nil {
				t.Fatal(err)
			}
		}
		capture, captureFile, key, scope, _ := fundingMonthCaptureFixture(t, pin)
		if err := InitRegisteredWalletMonthCaptureStore(ctx, capture); err != nil {
			t.Fatal(err)
		}
		checkpoint, err := CaptureNextRegisteredWalletMonthView(ctx, reader, capture, key, scope)
		if err != nil || checkpoint.Completed != 1 || !checkpoint.Captured {
			t.Fatal("real SELECT-only source did not commit request checkpoint", checkpoint, err)
		}
		if err := capture.Close(); err != nil {
			t.Fatal(err)
		}
		restored, err := sql.Open("sqlite", "file:"+captureFile+"?mode=ro&_pragma=query_only(1)")
		if err != nil {
			t.Fatal(err)
		}
		defer restored.Close()
		offline, err := sql.Open("mysql", user+"@"+socket+"/"+name)
		if err != nil {
			t.Fatal(err)
		}
		if err := offline.Close(); err != nil {
			t.Fatal(err)
		}
		reused, err := CaptureNextRegisteredWalletMonthView(ctx, offline, restored, key, scope)
		if err != nil || reused.Captured || reused.Completed != 1 || reused.ID != checkpoint.ID {
			t.Fatal("real-source restart did not reuse original while offline", reused, err)
		}
	})
	if reader.Stats().InUse != 0 {
		t.Fatal("source connection retained")
	}
	if _, err := writer.ExecContext(ctx, "UPDATE users SET quota=NULL WHERE id=2"); err != nil {
		t.Fatal(err)
	}
	if bad, err := CaptureWallet(ctx, reader, pin, []int64{1, 2}); err == nil || bad.Version != 0 {
		t.Fatal("NULL amount accepted as zero")
	}
	if bad, err := CaptureRegisteredWallet(ctx, reader, pin, []int64{1, 2}); err == nil || bad.Version != 0 || bad.Users != nil {
		t.Fatal("NULL registered-wallet amount accepted as zero or partial original")
	}
	var count int
	if err := local.QueryRowContext(ctx, "SELECT COUNT(*) FROM portal_monthly_wallet_observations").Scan(&count); err != nil || count != 2 {
		t.Fatal("failed capture affected local history", err)
	}
	t.Logf("MySQL SELECT-only source -> immutable SQLite: zero, negative, missing, NULL guard, 200-account capture (%s), prior history preserved", time.Since(started))
}
