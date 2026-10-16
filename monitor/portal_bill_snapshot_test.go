package monitor

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"
)

// Opt-in ONLY inside the throwaway --network none MySQL container. No DSN,
// hostname, password or production config can be supplied to this fixture.
func billSnapshotMySQLFixture(t *testing.T, count int) (*Monitor, *sql.DB) {
	t.Helper()
	if os.Getenv("BILL_SNAPSHOT_LOCAL_MYSQL") != "1" {
		t.Skip("isolated local MySQL fixture only")
	}
	const socket = "unix(/var/run/mysqld/mysqld.sock)"
	admin, err := sql.Open("mysql", "root@"+socket+"/?timeout=2s&readTimeout=5s&writeTimeout=5s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	if err := admin.Ping(); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("bill_local_%d", time.Now().UnixNano())
	user := fmt.Sprintf("bill_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP DATABASE " + name) })
	if _, err := admin.Exec("CREATE USER '" + user + "'@'localhost'"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP USER '" + user + "'@'localhost'") })
	writer, err := sql.Open("mysql", "root@"+socket+"/"+name+"?timeout=2s&readTimeout=5s&writeTimeout=5s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { writer.Close() })
	_, err = writer.Exec("CREATE TABLE logs (id BIGINT PRIMARY KEY,user_id BIGINT,created_at BIGINT,type INT,quota BIGINT NULL,prompt_tokens BIGINT,completion_tokens BIGINT,username VARCHAR(100),token_name VARCHAR(100),model_name VARCHAR(100),`group` VARCHAR(100),request_id VARCHAR(100),content TEXT,other TEXT,KEY idx_user_created_type(user_id,created_at,type,id)) ENGINE=InnoDB")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec("ALTER TABLE logs ADD COLUMN token_id BIGINT DEFAULT 1"); err != nil {
		t.Fatal(err)
	}
	tx, err := writer.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	s, err := tx.Prepare("INSERT INTO logs(id,user_id,created_at,type,quota,prompt_tokens,completion_tokens,username,token_name,model_name,`group`,request_id,content,other) VALUES(?,101,?,2,?,10,5,'local-user','token','model','group',?,'',?)")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 1; i <= count; i++ {
		_, err = s.Exec(i, 1700000000+i/3, i%31, fmt.Sprintf("local-%d", i), `{"billing_source":"wallet","usage_semantic":"openai","cache_tokens":2,"cache_creation_tokens":3,"request_path":"/v1/responses"}`)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec("GRANT SELECT ON " + name + ".logs TO '" + user + "'@'localhost'"); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("mysql", user+"@"+socket+"/"+name+"?timeout=2s&readTimeout=10s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })
	reader.SetMaxOpenConns(1)
	m := &Monitor{prodDB: reader}
	return m, writer
}

func snapshotTestScope() portalBillScope {
	return portalBillScope{IDs: []int64{101}, From: 1699999999, To: 1700086400}
}

func TestPortalBillSnapshotMySQLConcurrentChanges(t *testing.T) {
	m, writer := billSnapshotMySQLFixture(t, 2100)
	// The source connection cannot write even outside the READ ONLY transaction.
	if _, err := m.prodDB.Exec("UPDATE logs SET quota=0 WHERE id=1"); err == nil {
		t.Fatal("reader can write")
	}
	var expected int64
	if err := writer.QueryRow("SELECT SUM(quota) FROM logs").Scan(&expected); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	changed := false
	started := time.Now()
	n, err := m.buildPortalBillSnapshot(context.Background(), &out, snapshotTestScope(), 2100, func(context.Context) error { return nil }, func(int64) error {
		if changed {
			return nil
		}
		changed = true
		// Mutate the unvisited second/third pages after first page generation.
		// Equal-count delete/insert and a changed amount/text must not leak in.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		for _, q := range []string{"UPDATE logs SET quota=999999,request_id='changed' WHERE id=1", "DELETE FROM logs WHERE id=2", "INSERT INTO logs SELECT 3000,user_id,created_at,type,quota,prompt_tokens,completion_tokens,username,token_name,model_name,`group`,'new-request',content,other,token_id FROM logs WHERE id=3"} {
			if _, err := writer.ExecContext(ctx, q); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil || n != 2100 || !changed {
		t.Fatalf("rows=%d changed=%v err=%v", n, changed, err)
	}
	rows, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(out.String(), "\ufeff"))).ReadAll()
	if err != nil || len(rows) != 2101 {
		t.Fatal("incomplete CSV")
	}
	sum := new(big.Rat)
	for i, row := range rows[1:] {
		if len(row) != 15 || row[5] != fmt.Sprintf("local-%d", 2100-i) {
			t.Fatal("mixed source versions")
		}
		v, ok := new(big.Rat).SetString(strings.TrimPrefix(row[13], "¥"))
		if !ok {
			t.Fatal("invalid amount")
		}
		sum.Add(sum, v)
	}
	if sum.Cmp(big.NewRat(expected, 500000)) != 0 {
		t.Fatal("mixed money")
	}
	if m.prodDB.Stats().InUse != 0 {
		t.Fatal("connection retained for download")
	}
	var current int64
	if err := writer.QueryRow("SELECT quota FROM logs WHERE id=1").Scan(&current); err != nil || current != 999999 {
		t.Fatal("concurrent writes did not complete")
	}
	t.Logf("MySQL read-only snapshot rows=%d elapsed=%s; concurrent UPDATE/DELETE/INSERT completed, original rows and charges retained", n, time.Since(started))
}

func TestPortalBillSnapshotMySQLReleasesOnFailure(t *testing.T) {
	m, writer := billSnapshotMySQLFixture(t, 2100)
	for _, mode := range []string{"count", "auth", "cancel", "timeout", "null-quota", "engine"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "timeout" {
				var c context.CancelFunc
				ctx, c = context.WithTimeout(ctx, 30*time.Millisecond)
				defer c()
			}
			rows := int64(2100)
			if mode == "count" {
				rows++
			}
			if mode == "null-quota" {
				if _, err := writer.Exec("UPDATE logs SET quota=NULL WHERE id=1"); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if _, err := writer.Exec("UPDATE logs SET quota=1 WHERE id=1"); err != nil {
						t.Errorf("restore synthetic quota: %v", err)
					}
				}()
			}
			if mode == "engine" {
				if _, err := writer.Exec("ALTER TABLE logs ENGINE=MyISAM"); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if _, err := writer.Exec("ALTER TABLE logs ENGINE=InnoDB"); err != nil {
						t.Errorf("restore synthetic engine: %v", err)
					}
				}()
			}
			checks := 0
			_, err := m.buildPortalBillSnapshot(ctx, io.Discard, snapshotTestScope(), rows, func(context.Context) error {
				checks++
				if mode == "auth" && checks == 3 {
					return errors.New("revoked")
				}
				return nil
			}, func(int64) error {
				if mode == "cancel" {
					cancel()
				}
				if mode == "timeout" {
					<-ctx.Done()
				}
				return nil
			})
			if err == nil {
				t.Fatal("invalid snapshot succeeded")
			}
			switch mode {
			case "count", "null-quota":
				if !errors.Is(err, errPortalBillSourceChanged) {
					t.Fatalf("wrong failure: %v", err)
				}
			case "auth":
				if checks != 3 || err.Error() != "revoked" {
					t.Fatalf("authorization not exercised: checks=%d err=%v", checks, err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("wrong cancel: %v", err)
				}
			case "timeout":
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("wrong timeout: %v", err)
				}
			case "engine":
				if err.Error() != "日志来源不支持一致性快照" {
					t.Fatalf("wrong engine refusal: %v", err)
				}
			}
			deadline := time.Now().Add(time.Second)
			for m.prodDB.Stats().InUse != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if m.prodDB.Stats().InUse != 0 {
				t.Fatal("connection leaked")
			}
			if err := m.prodDB.Ping(); err != nil {
				t.Fatal("source pool did not recover")
			}
		})
	}
}

func TestPortalBillSnapshotMySQLLarge(t *testing.T) {
	if os.Getenv("BILL_SNAPSHOT_LARGE") != "1" {
		t.Skip("opt-in isolated 100k MySQL run")
	}
	const count = 100000
	m, _ := billSnapshotMySQLFixture(t, count)
	f, err := os.CreateTemp(t.TempDir(), "bill-*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	start := time.Now()
	n, err := m.buildPortalBillSnapshot(context.Background(), f, snapshotTestScope(), count, func(context.Context) error { return nil }, func(int64) error { return nil })
	if err != nil || n != count {
		t.Fatalf("rows=%d err=%v", n, err)
	}
	elapsed := time.Since(start)
	if _, err := f.Seek(3, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	r := csv.NewReader(f)
	if _, err := r.Read(); err != nil {
		t.Fatal(err)
	}
	for id := count; id > 0; id-- {
		row, err := r.Read()
		if err != nil {
			t.Fatal(err)
		}
		want := big.NewRat(int64(id%31), 500000).FloatString(6)
		if len(row) != 15 || row[5] != fmt.Sprintf("local-%d", id) || strings.TrimPrefix(row[13], "¥") != want {
			t.Fatalf("row %d not identical", id)
		}
	}
	if _, err := r.Read(); err != io.EOF {
		t.Fatal("extra rows")
	}
	if m.prodDB.Stats().InUse != 0 {
		t.Fatal("source connection retained")
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MySQL snapshot 100k: bytes=%d generation=%s; each ID and amount verified", info.Size(), elapsed)
}
