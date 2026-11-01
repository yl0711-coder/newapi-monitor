package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// Only the disposable container's fixed socket is accepted by this fixture.
// Credentials below are synthetic; no online settings or host DSN are loaded.
func TestCaptureCLIRealMySQLBinaryResumeAndIndependentPasses(t *testing.T) {
	testCaptureCLIRealMySQL(t, false)
}

func TestDailyFundingCLIRealMySQLBinaryResumeAndIndependentPasses(t *testing.T) {
	testCaptureCLIRealMySQL(t, true)
}

func testCaptureCLIRealMySQL(t *testing.T, daily bool) {
	t.Helper()
	bin := os.Getenv("MONTHLY_CAPTURE_TEST_BINARY")
	if os.Getenv("MONTHLY_CAPTURE_LOCAL_MYSQL") != "1" || !filepath.IsAbs(bin) {
		t.Skip("explicit isolated MySQL and compiled command required")
	}
	// The legacy batch creates 720 temporary SQLite files under a constrained
	// container. Allow fixture setup too; product command/query budgets stay fixed.
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	const socket = "/var/run/mysqld/mysqld.sock"
	admin, err := sql.Open("mysql", "root@unix("+socket+")/?timeout=2s&readTimeout=5s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := fmt.Sprintf("capture_%d", time.Now().UnixNano())
	user := fmt.Sprintf("reader_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.ExecContext(context.Background(), "DROP DATABASE "+name) }()
	if _, err := admin.ExecContext(ctx, "CREATE USER '"+user+"'@'localhost'"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.ExecContext(context.Background(), "DROP USER '"+user+"'@'localhost'") }()
	writer, err := sql.Open("mysql", "root@unix("+socket+")/"+name+"?timeout=2s&readTimeout=5s&writeTimeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.ExecContext(ctx, `CREATE TABLE logs(id BIGINT PRIMARY KEY,user_id BIGINT NOT NULL,created_at BIGINT NOT NULL,type INT NOT NULL,content TEXT,other MEDIUMTEXT,INDEX idx_created_at_id(created_at,id),INDEX idx_created_at_type(created_at,type),INDEX idx_user_created_type(user_id,created_at,type)) ENGINE=InnoDB`); err != nil {
		t.Fatal(err)
	}
	const from = int64(1788192000)
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for i := int64(1); i <= 405; i++ {
		if _, err := tx.ExecContext(ctx, "INSERT INTO logs VALUES(?,?,?,?,?,?)", i, 201, from+10, 4, "新用户注册赠送 1 点额度", ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO logs VALUES(406,999,?,4,'新用户注册赠送 1 点额度','')", from+10); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "GRANT SELECT ON "+name+".logs TO '"+user+"'@'localhost'"); err != nil {
		t.Fatal(err)
	}
	var uuid string
	if err := admin.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatal(err)
	}
	pin, err := monthlysource.WalletSourceIdentity(uuid, name)
	if err != nil {
		t.Fatal(err)
	}
	args := fixtureInitArgs(t)
	args = append(args, "-source-identity", pin)
	if daily {
		args = append(args, "-daily", "-to", strconv.FormatInt(from+86400, 10))
	}
	command := func(args []string, wantFailure bool) receipt {
		t.Helper()
		cmd := exec.CommandContext(ctx, bin, args...)
		var out, stderr bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		err := cmd.Run()
		if (err != nil) != wantFailure {
			t.Fatal("unexpected command result", out.String(), stderr.String(), err)
		}
		var r receipt
		if out.Len() != 0 && json.Unmarshal(out.Bytes(), &r) != nil {
			t.Fatal("invalid command receipt")
		}
		return r
	}
	created := command(append(args, "-create"), false)
	if !created.Created {
		t.Fatal("actual CLI did not create job")
	}
	conn := filepath.Join(args[2], "connection.json")
	raw, _ := json.Marshal(connectionConfig{Network: "unix", Address: socket, User: user, Database: name})
	if err := os.WriteFile(conn, raw, 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := readConnection(conn)
	if err != nil {
		t.Fatal(err)
	}
	connector, err := mysqlConnectorForTest(cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()
	if _, err := connector.ExecContext(ctx, "UPDATE logs SET content='bad' WHERE id=1"); err == nil {
		t.Fatal("fixture user can write")
	}
	job := filepath.Join(args[2], args[4])
	dbPath := filepath.Join(job, "funding.sqlite")
	before := fileHash(t, dbPath)
	defaultCheck := command(append(inspectArgs(args, created, "older"), "-connection-file", "/missing/credentials.json"), false)
	if !defaultCheck.LocalReady || defaultCheck.Started || before != fileHash(t, dbPath) {
		t.Fatal("default CLI queried or changed state")
	}
	control := func(phase string) []string {
		return append(inspectArgs(args, created, phase), "-capture", "-connection-file", conn, "-max-pages", "3", "-interval", "1s", "-budget", "15s")
	}
	blocked := command(control("newer"), true)
	if blocked.BlockedReason == "" || fileHash(t, dbPath) != before {
		t.Fatal("premature second pass changed state")
	}
	// Run an actual command, wait for its first durable page, then interrupt
	// while the page interval owns no database transaction. Resume in a new OS
	// process; the saved scan ID and cursor must survive that process exit.
	cmd := exec.CommandContext(ctx, bin, control("older")...)
	var interruptedOut, interruptedErr bytes.Buffer
	cmd.Stdout = &interruptedOut
	cmd.Stderr = &interruptedErr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	finished := false
	defer func() {
		if !finished {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	readOnly, err := openReceiver(ctx, dbPath, false)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	var first monthlysource.FundingPage
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		first, err = monthlysource.LoadLatestFundingPage(ctx, readOnly, created.OlderScanID)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || first.Next.ID != 200 || first.EndReached {
		t.Fatal("first durable page missing", err)
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	stopErr := cmd.Wait()
	finished = true
	var stopped receipt
	if stopErr == nil || json.Unmarshal(interruptedOut.Bytes(), &stopped) != nil || stopped.Capture == nil || stopped.Capture.PagesSaved > 1 || stopped.EndReached || !stopped.NeedsCheckpointCheck {
		t.Fatal("interrupted command lost durable receipt", interruptedOut.String(), interruptedErr.String(), stopErr)
	}
	// A cancellation at commit acknowledgement can leave a saved page without
	// its success receipt. Inspect the authoritative checkpoint, never infer
	// zero evidence from an error's acknowledged-page counter.
	inspected := command(inspectArgs(args, created, "older"), false)
	if !inspected.Started || inspected.EndReached || inspected.CheckpointHead == "" || inspected.NeedsCheckpointCheck {
		t.Fatal("durable checkpoint not inspectable after interruption", inspected)
	}
	resumed := command(control("older"), false)
	if resumed.Capture == nil || !resumed.EndReached || resumed.Capture.PagesSaved != 2 || resumed.Capture.RowsSaved != 205 {
		t.Fatal("real CLI resume lost or duplicated rows", resumed)
	}
	idempotent := command(append(inspectArgs(args, created, "older"), "-capture", "-connection-file", "/missing-after-completion"), false)
	if !idempotent.EndReached || idempotent.Capture != nil {
		t.Fatal("completed command connected again")
	}
	newer := command(control("newer"), false)
	if newer.Capture == nil || !newer.EndReached || newer.Capture.PagesSaved != 3 || newer.Capture.RowsSaved != 405 {
		t.Fatal("independent scan failed", newer)
	}
	comparison, err := monthlysource.CompareFundingScans(ctx, readOnly, created.OlderScanID, created.NewerScanID)
	if err != nil || !comparison.ObservationsMatch || comparison.OlderRows != 405 || comparison.NewerRows != 405 {
		t.Fatal("independent observations differ", err)
	}
	var pages, records int
	if err := readOnly.QueryRowContext(ctx, "SELECT count(*) FROM portal_monthly_funding_pages").Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if err := readOnly.QueryRowContext(ctx, "SELECT count(*) FROM portal_monthly_funding_records").Scan(&records); err != nil {
		t.Fatal(err)
	}
	if pages != 6 || records != 810 {
		t.Fatal("unexpected retained observations", pages, records)
	}
	// A wrong pinned source fails before the first observed row is saved.
	wrongArgs := append(append([]string{}, args...), "-job", "wrong-source", "-source-identity", strings.Repeat("b", 64))
	wrong := command(append(wrongArgs, "-create"), false)
	wrongRun := []string{"run", "-workspace", args[2], "-job", "wrong-source", "-expected-manifest", wrong.ManifestSHA256, "-phase", "older", "-capture", "-connection-file", conn, "-interval", "1s"}
	bad := command(wrongRun, true)
	if bad.Started || bad.EndReached || bad.Capture == nil || bad.Capture.PagesSaved != 0 {
		t.Fatal("wrong source saved data", bad)
	}
	t.Log("actual CLI: inspect-only, SIGINT/resume, immutable scope, SELECT-only source, two 405-row observations, six durable pages; no bill publication")
	testMonthStepRealMySQL(ctx, t, bin, args[2], pin, conn, daily)
}

func testMonthStepRealMySQL(ctx context.Context, t *testing.T, bin, root, pin, connection string, daily bool) {
	t.Helper()
	init := []string{"init-month", "-workspace", root, "-batch", "real-batch", "-group-id", "1", "-month", "2026-09", "-user-ids", "201,202", "-source-identity", pin, "-create"}
	if daily {
		init = append(init, "-window-seconds", "86400")
	}
	b, err := exec.CommandContext(ctx, bin, init...).CombinedOutput()
	var created monthBatchReceipt
	if err != nil || json.Unmarshal(b, &created) != nil || !validSHA(created.BatchSHA256) {
		t.Fatal("actual batch init failed", err, string(b))
	}
	if daily && (created.Windows != 30 || created.MinimumPageReads != 60) {
		t.Fatal("daily CLI accidentally exercised legacy hour batch")
	}
	args := []string{"run-month", "-workspace", root, "-batch", "real-batch", "-expected-batch", created.BatchSHA256, "-connection-file", connection, "-interval", "1s", "-max-pages", "1", "-capture"}
	runStep := func(extra ...string) monthStepReceipt {
		t.Helper()
		b, err := exec.CommandContext(ctx, bin, append(append([]string(nil), args...), extra...)...).CombinedOutput()
		var r monthStepReceipt
		if err != nil || json.Unmarshal(b, &r) != nil || r.Step == nil || r.Step.Capture == nil || r.After == nil {
			t.Fatal("actual batch step failed", err, string(b))
		}
		return r
	}
	// Exercise SIGINT in the actual batch wrapper, not only its underlying
	// single-window runner. The durable checkpoint outlives the process.
	cmd := exec.CommandContext(ctx, bin, append(append([]string(nil), args...), "-max-pages", "3")...)
	var stoppedOut bytes.Buffer
	cmd.Stdout = &stoppedOut
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	finished := false
	defer func() {
		if !finished {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	dir := filepath.Join(root, "real-batch")
	plan, err := readMonthBatch(ctx, dir, created.BatchSHA256)
	if err != nil {
		t.Fatal(err)
	}
	if daily && plan.Version != 2 {
		t.Fatal("actual daily CLI batch lost protocol version")
	}
	db, err := openReceiver(ctx, filepath.Join(dir, "window-0000", "funding.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var first monthlysource.FundingPage
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		first, err = monthlysource.LoadLatestFundingPage(ctx, db, plan.Windows[0].OlderScanID)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || first.Next.ID != 200 || first.EndReached {
		t.Fatal("batch first durable page missing", err)
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	stopErr := cmd.Wait()
	finished = true
	var stopped monthStepReceipt
	if stopErr == nil || json.Unmarshal(stoppedOut.Bytes(), &stopped) != nil || !stopped.NeedsCheckpointCheck || stopped.After != nil {
		t.Fatal("batch interruption not reported", stopErr)
	}
	bResult := runStep("-max-pages", "2")
	if bResult.Step.Capture.PagesSaved != 2 || bResult.Step.Capture.RowsSaved != 205 || !bResult.Step.EndReached || bResult.After.OlderCompleted != 1 || bResult.After.NewerCompleted != 0 || bResult.After.Next.Job != "window-0001" {
		t.Fatal("batch resume duplicated or skipped", bResult)
	}
	c := runStep("-max-pages", "10")
	if c.Before.Next.Job != "window-0001" || c.Step.Capture.PagesSaved != 1 || c.Step.Capture.RowsSaved != 0 || c.After.OlderCompleted != 2 || c.After.Next.Job != "window-0002" {
		t.Fatal("batch crossed multiple windows", c)
	}
	t.Log("actual batch CLI: SIGINT after 200 saved rows, resumed remaining 205 in a new process, next empty window once despite max-pages=10; no premature newer sweep")
}

func mysqlConnectorForTest(dsn string) (*sql.DB, error) { return sql.Open("mysql", dsn) }

// Keep timestamp interpretation explicit in this fixture; no current date or
// real account is substituted for its synthetic, closed September window.
func TestCaptureFixtureWindow(t *testing.T) {
	args := fixtureInitArgs(t)
	from, err := strconv.ParseInt(args[len(args)-3], 10, 64)
	if err != nil || time.Unix(from, 0).In(time.FixedZone("CST", 8*3600)).Format("2006-01") != "2026-09" {
		t.Fatal("fixture window moved")
	}
}
