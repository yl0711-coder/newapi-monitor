package monthlysource

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"
)

// Only a disposable container's fixed socket, never a caller-supplied source.
func TestFundingDayMySQLUnionMatchesOriginalKeysetAndMissingIndexFails(t *testing.T) {
	if os.Getenv("MONTHLY_FUNDING_LOCAL_MYSQL") != "1" {
		t.Skip("isolated local MySQL fixture only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const socket = "unix(/var/run/mysqld/mysqld.sock)"
	admin, err := sql.Open("mysql", "root@"+socket+"/?timeout=2s&readTimeout=5s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := fmt.Sprintf("day_union_%d", time.Now().UnixNano())
	user := fmt.Sprintf("day_reader_%d", time.Now().UnixNano())
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
	if _, err := writer.ExecContext(ctx, `CREATE TABLE logs(id BIGINT PRIMARY KEY,user_id BIGINT NOT NULL,created_at BIGINT NOT NULL,type INT NOT NULL,content TEXT,other MEDIUMTEXT,INDEX idx_created_at_type(created_at,type),INDEX idx_user_created_type(user_id,created_at,type)) ENGINE=InnoDB`); err != nil {
		t.Fatal(err)
	}
	const from int64 = 1788192000
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var original []FundingLog
	for id := int64(1); id <= 1805; id++ {
		uid := int64(201) + id%2
		if id%5 == 0 {
			uid = 999
		}
		kind := int(id%6) + 1
		if kind == 3 {
			uid = 9
		}
		row := FundingLog{ID: id, UserID: uid, CreatedAt: from + (id-1)/900, Type: kind, Content: "synthetic original text", Other: ""}
		original = append(original, row)
		if _, err := tx.ExecContext(ctx, "INSERT INTO logs VALUES(?,?,?,?,?,?)", row.ID, row.UserID, row.CreatedAt, row.Type, row.Content, row.Other); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO logs VALUES(1806,9,?,3,'outside',''),(1807,201,?,4,'outside','')", from+86400, from-1); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "GRANT SELECT ON "+name+".logs TO '"+user+"'@'localhost'"); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("mysql", user+"@"+socket+"/"+name+"?timeout=2s&readTimeout=4s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader.SetMaxOpenConns(1)
	var uuid string
	if err := reader.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatal(err)
	}
	pin, err := WalletSourceIdentity(uuid, name)
	if err != nil {
		t.Fatal(err)
	}
	many := make([]int64, 200)
	for i := range many {
		many[i] = int64(i + 1)
	}
	for _, ids := range [][]int64{{201}, {201, 202}, many} {
		scan, err := NewFundingDayScan(pin, fundingTestRules(), ids, from, from+86400)
		if err != nil {
			t.Fatal(err)
		}
		var want, got []FundingLog
		for _, row := range original {
			if row.Type == 3 || ((row.Type == 1 || row.Type == 4) && slices.Contains(ids, row.UserID)) {
				want = append(want, row)
			}
		}
		local, _ := fundingTestStore(t)
		var previous *FundingPage
		for page := 0; page < 10; page++ {
			p, err := CaptureFundingPage(ctx, reader, scan, previous)
			if err != nil {
				t.Fatal("optimized capture", len(ids), err)
			}
			// This is the unchanged original predicate for comparison only, not
			// a forged legacy-day capture or a production fallback.
			reference := scan
			reference.Version = 1
			q, args := fundingQuery(reference, p.FenceID, p.After)
			rtx, err := reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := rtx.QueryContext(ctx, q, args...)
			if err != nil {
				_ = rtx.Rollback()
				t.Fatal(err)
			}
			raw, err := readFundingRows(rows, p)
			if err != nil {
				_ = rtx.Rollback()
				t.Fatal(err)
			}
			if err := rtx.Commit(); err != nil {
				t.Fatal(err)
			}
			end := len(raw) <= FundingPageLimit
			if !end {
				raw = raw[:FundingPageLimit]
			}
			if !reflect.DeepEqual(p.Rows, raw) || p.EndReached != end {
				t.Fatal("union changed original page order, scope or lookahead")
			}
			if _, err := SaveFundingPage(ctx, local, p); err != nil {
				t.Fatal(err)
			}
			got = append(got, p.Rows...)
			if p.EndReached {
				break
			}
			loaded, err := LoadLatestFundingPage(ctx, local, scan.ID)
			if err != nil {
				t.Fatal(err)
			}
			previous = &loaded
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatal("mixed actors, timestamps or types lost/duplicated records", len(ids), len(got), len(want))
		}
	}
	if _, err := writer.ExecContext(ctx, "ALTER TABLE logs DROP INDEX idx_user_created_type"); err != nil {
		t.Fatal(err)
	}
	scan, err := NewFundingDayScan(pin, fundingTestRules(), []int64{201}, from, from+86400)
	if err != nil {
		t.Fatal(err)
	}
	if p, err := CaptureFundingPage(ctx, reader, scan, nil); err == nil || p.Version != 0 {
		t.Fatal("missing index silently fell back or retained a partial page")
	}
	if reader.Stats().InUse != 0 {
		t.Fatal("failed indexed capture leaked connection")
	}
	t.Log("1/2/200-account union pages equal unchanged original predicate and independent Go filtering; same-second interleaving, branch overflow, midnight, resume and missing-index rejection verified")
}
