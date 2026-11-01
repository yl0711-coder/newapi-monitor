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

// This fixture can connect only to its disposable container's fixed Unix
// socket, never a caller-supplied host/DSN. No production settings are read.
func TestFundingMySQLPagesSQLiteResumeAndLateSourceChanges(t *testing.T) {
	testFundingMySQLPages(t, false)
}

func TestFundingDayMySQLPagesSQLiteResumeAndLateSourceChanges(t *testing.T) {
	testFundingMySQLPages(t, true)
}

func testFundingMySQLPages(t *testing.T, daily bool) {
	t.Helper()
	if os.Getenv("MONTHLY_FUNDING_LOCAL_MYSQL") != "1" {
		t.Skip("isolated local MySQL fixture only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	const socket = "unix(/var/run/mysqld/mysqld.sock)"
	admin, err := sql.Open("mysql", "root@"+socket+"/?timeout=2s&readTimeout=5s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := fmt.Sprintf("funding_local_%d", time.Now().UnixNano())
	user := fmt.Sprintf("funding_%d", time.Now().UnixNano())
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
	if _, err := writer.ExecContext(ctx, `CREATE TABLE logs(id BIGINT PRIMARY KEY,user_id BIGINT NOT NULL,created_at BIGINT NOT NULL,type INT NOT NULL,content TEXT NULL,other MEDIUMTEXT NULL,INDEX idx_created_at_id(created_at,id),INDEX idx_created_at_type(created_at,type),INDEX idx_user_created_type(user_id,created_at,type)) ENGINE=InnoDB`); err != nil {
		t.Fatal(err)
	}
	from, span := int64(1780000000), int64(3600)
	constructor := NewFundingScan
	if daily {
		from, span, constructor = 1788192000, 86400, NewFundingDayScan
	}
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	insert := func(id, userID, at int64, kind int, content, other string) {
		t.Helper()
		if _, err := tx.ExecContext(ctx, "INSERT INTO logs VALUES(?,?,?,?,?,?)", id, userID, at, kind, content, other); err != nil {
			t.Fatal(err)
		}
	}
	for i := int64(1); i <= 405; i++ {
		target := 201
		if i%2 == 0 {
			target = 999
		}
		insert(i, 9, from+10, 3, "", fundingTestAudit("user.quota_add", fmt.Sprintf(`{"quota":"1 点额度","target_user_id":%d}`, target)))
	}
	insert(406, 201, from+20, 4, "新用户注册赠送 1 点额度", "")
	insert(407, 202, from+20, 4, "新用户注册赠送 1 点额度", "")
	insert(408, 999, from+20, 4, "新用户注册赠送 1 点额度", "")
	insert(409, 201, from+20, 2, "consume", "")
	insert(410, 201, from+span, 4, "新用户注册赠送 1 点额度", "")
	insert(411, 201, from-1, 4, "新用户注册赠送 1 点额度", "")
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
	if _, err := reader.ExecContext(ctx, "UPDATE logs SET content='bad' WHERE id=1"); err == nil {
		t.Fatal("readonly fixture can write source")
	}
	var uuid string
	if err := reader.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatal(err)
	}
	pin, err := WalletSourceIdentity(uuid, name)
	if err != nil {
		t.Fatal(err)
	}
	scan, err := constructor(pin, fundingTestRules(), []int64{201, 202}, from, from+span)
	if err != nil {
		t.Fatal(err)
	}
	local, path := fundingTestStore(t)
	limits := FundingCaptureLimits{MaxPages: 1, Interval: time.Second, Timeout: 10 * time.Second}
	started := time.Now()
	receipt, err := RunFundingCapture(ctx, reader, local, scan, limits)
	if err != nil || receipt.PagesSaved != 1 || receipt.RowsSaved != 200 || !receipt.BudgetEnded || receipt.EndReached || time.Since(started) < time.Second {
		t.Fatal("real bounded runner/pacing contract failed", receipt, err)
	}
	first, err := LoadLatestFundingPage(ctx, local, scan.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Rows) != 200 || first.EndReached || first.Next.ID != 200 || first.FenceID != 411 {
		t.Fatal("real SQL pagination contract changed")
	}
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}
	local, err = sql.Open("sqlite", path+"?_pragma=busy_timeout(1000)")
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	local.SetMaxOpenConns(1)
	last, err := LoadLatestFundingPage(ctx, local, scan.ID)
	if err != nil || !reflect.DeepEqual(last, first) {
		t.Fatal("real capture not resumable after restart", err)
	}
	// Add a late row behind the cursor and change an already observed row.
	// The old scan is not relabeled as a financial snapshot or silently changed.
	if _, err := writer.ExecContext(ctx, "INSERT INTO logs VALUES(?,?,?,?,?,?)", 412, 201, from+5, 4, "新用户注册赠送 5 点额度", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, "UPDATE logs SET other=? WHERE id=1", fundingTestAudit("user.quota_add", `{"quota":"2 点额度","target_user_id":201}`)); err != nil {
		t.Fatal(err)
	}
	collect := func(s FundingScan, previous *FundingPage) (int, int64, FundingPage) {
		t.Helper()
		rows := 0
		sum := int64(0)
		var result FundingPage
		for range 4 {
			p, err := CaptureFundingPage(ctx, reader, s, previous)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := SaveFundingPage(ctx, local, p); err != nil {
				t.Fatal(err)
			}
			view, err := ProjectFundingPage(p)
			if err != nil || len(view.Review) != 0 {
				t.Fatal("synthetic account projection failed", err)
			}
			for _, m := range view.Movements {
				sum += *m.DeltaQuota
			}
			rows += len(p.Rows)
			result = p
			if p.EndReached {
				return rows, sum, result
			}
			loaded, err := LoadLatestFundingPage(ctx, local, s.ID)
			if err != nil {
				t.Fatal(err)
			}
			previous = &loaded
		}
		t.Fatal("bounded fixture failed to reach end")
		return 0, 0, result
	}
	remainingRows, remainingQuota, oldEnd := collect(scan, &last)
	initial, err := ProjectFundingPage(first)
	if err != nil {
		t.Fatal(err)
	}
	initialQuota := int64(0)
	for _, m := range initial.Movements {
		initialQuota += *m.DeltaQuota
	}
	if remainingRows+len(first.Rows) != 407 || remainingQuota+initialQuota != 205 || oldEnd.FenceID != 411 {
		t.Fatal("same-second records missed, non-scope money included, or fence widened")
	}
	// Audit requires provably non-overlapping capture timestamps, including
	// their millisecond precision. Give this tiny synthetic traversal a gap.
	time.Sleep(2 * time.Millisecond)
	newScan, err := constructor(pin, fundingTestRules(), []int64{201, 202}, from, from+span)
	if err != nil {
		t.Fatal(err)
	}
	newRows, newQuota, newEnd := collect(newScan, nil)
	if newRows != 408 || newQuota != 211 || newEnd.FenceID != 412 || newScan.ID == scan.ID {
		t.Fatal("fresh scan failed to retain late/corrected evidence independently")
	}
	prior, err := LoadLatestFundingPage(ctx, local, scan.ID)
	if err != nil || !reflect.DeepEqual(prior, oldEnd) {
		t.Fatal("new source observations altered prior traversal", err)
	}
	comparison, err := CompareFundingScans(ctx, local, scan.ID, newScan.ID)
	if err != nil || comparison.ObservationsMatch || len(comparison.Changes) != 2 || comparison.Changes[0].LogID != 1 || comparison.Changes[0].Kind != "changed" || comparison.Changes[1].LogID != 412 || comparison.Changes[1].Kind != "inserted" || comparison.Accounts[0].ObservationsMatch || !comparison.Accounts[1].ObservationsMatch {
		t.Fatal("real late insert/correction comparison failed", err, comparison)
	}
	// Delete a non-max source ID: same fence is not proof of unchanged data.
	if _, err := writer.ExecContext(ctx, "DELETE FROM logs WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	deletedScan, err := constructor(pin, fundingTestRules(), []int64{201, 202}, from, from+span)
	if err != nil {
		t.Fatal(err)
	}
	deletedRows, deletedQuota, _ := collect(deletedScan, nil)
	comparison, err = CompareFundingScans(ctx, local, newScan.ID, deletedScan.ID)
	if err != nil || deletedRows != 407 || deletedQuota != 209 || len(comparison.Changes) != 1 || comparison.Changes[0].LogID != 1 || comparison.Changes[0].Kind != "absent_in_new_scan" || comparison.Accounts[0].ObservationsMatch || !comparison.Accounts[1].ObservationsMatch {
		t.Fatal("real source disappearance not localized", err, comparison)
	}
	// Explicit independent runner invocation consumes all pages with real SQL,
	// real pacing and atomic checkpoints. It must agree with the prior scan,
	// while leaving that scan unchanged (observations are never summed).
	runnerScan, err := constructor(pin, fundingTestRules(), []int64{201, 202}, from, from+span)
	if err != nil {
		t.Fatal(err)
	}
	limits.MaxPages = 1
	paused, err := RunFundingCapture(ctx, reader, local, runnerScan, limits)
	if err != nil || !paused.BudgetEnded || paused.EndReached || paused.PagesSaved != 1 || paused.RowsSaved != 200 {
		t.Fatal("real runner did not pause at budget", paused, err)
	}
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}
	local, err = sql.Open("sqlite", path+"?_pragma=busy_timeout(1000)")
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	local.SetMaxOpenConns(1)
	limits.MaxPages = 3
	receipt, err = RunFundingCapture(ctx, reader, local, runnerScan, limits)
	if err != nil || !receipt.EndReached || receipt.BudgetEnded || receipt.PagesSaved+paused.PagesSaved != 3 || receipt.RowsSaved+paused.RowsSaved != 407 || receipt.HeadID == paused.HeadID {
		t.Fatal("real multi-page runner resume failed", receipt, err)
	}
	comparison, err = CompareFundingScans(ctx, local, deletedScan.ID, runnerScan.ID)
	if err != nil || !comparison.ObservationsMatch {
		t.Fatal("runner traversal changed observations", err)
	}
	// Oversize content is NULLed by SQL, not truncated and accepted as evidence.
	if _, err := writer.ExecContext(ctx, "UPDATE logs SET content=? WHERE id=412", strings.Repeat("x", 4097)); err != nil {
		t.Fatal(err)
	}
	oversized, err := constructor(pin, fundingTestRules(), []int64{201, 202}, from, from+span)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := RunFundingCapture(ctx, reader, local, oversized, limits); err == nil || result.PagesSaved != 0 || result.HeadID != "" || result.EndReached {
		t.Fatal("oversize source accepted or partial page leaked", result, err)
	}
	if reader.Stats().InUse != 0 {
		t.Fatal("source connection held after failure")
	}
	t.Log("SELECT-only MySQL -> SQLite: three-page scans, restart, late insert/correction/deletion comparison, independent accounts and immutable old observations verified")
}
