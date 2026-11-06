package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func TestMonthAssemblyWholeMonthNoOverwriteAndReaudit(t *testing.T) {
	root, p := auditPrivateDir(t), auditPlanFixture(t)
	hashes := make(map[string]string)
	for i := range p.Windows {
		p.Windows[i] = writeAuditWindow(t, root, p, i, nil)
		for _, name := range []string{"manifest.json", "funding.sqlite"} {
			path := filepath.Join(root, p.Windows[i].Job, name)
			hashes[path] = fileHash(t, path)
		}
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	planPath := filepath.Join(root, "month-plan.json")
	if err := exclusiveFile(planPath, raw); err != nil {
		t.Fatal(err)
	}
	args := []string{"assemble-month", "-workspace", root, "-plan", planPath, "-expected-plan", hash(raw), "-output", "assembled"}
	var out bytes.Buffer
	if err := run(context.Background(), args, &out); err != nil {
		t.Fatal(err)
	}
	var preview monthAssemblyReceipt
	if err := json.Unmarshal(out.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	if preview.Mode != "checked_local_assembly_no_output" || preview.DatabaseSHA256 != "" {
		t.Fatal("preview pretended to create output", preview)
	}
	output := filepath.Join(root, "assembled")
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		t.Fatal("preview wrote output", err)
	}
	out.Reset()
	started := time.Now()
	if err := run(context.Background(), append(args, "-create-output"), &out); err != nil {
		t.Fatal(err)
	}
	t.Logf("720-window assembly and reopened audit: %s", time.Since(started))
	var receipt monthAssemblyReceipt
	if err := json.Unmarshal(out.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Pages != 1440 || receipt.Rows != 4 || receipt.Mode != "assembled_local_evidence_not_bill_approval" || receipt.ReviewSHA256 != preview.ReviewSHA256 {
		t.Fatal("assembly receipt mismatch", receipt)
	}
	marker, err := privateRead(filepath.Join(output, "assembly.json"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(receipt)
	if err != nil || !bytes.Equal(marker, canonical) {
		t.Fatal("completion marker differs", err)
	}
	sha, size, err := hashAssemblyDatabase(context.Background(), filepath.Join(output, "funding.sqlite"))
	if err != nil || sha != receipt.DatabaseSHA256 || size != receipt.DatabaseBytes {
		t.Fatal("database seal differs", err)
	}
	for _, name := range []string{"funding.pending.sqlite", "assembly.pending.json", "funding.sqlite-wal", "funding.sqlite-journal"} {
		if _, err := os.Lstat(filepath.Join(output, name)); !os.IsNotExist(err) {
			t.Fatal("published staging or sidecar remains", name, err)
		}
	}
	assembled, err := openReceiver(context.Background(), filepath.Join(output, "funding.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	review, err := reviewAssembledMonth(context.Background(), assembled, p, p.Windows, receipt.Pages, receipt.Rows)
	err = errors.Join(err, assembled.Close())
	if err != nil {
		t.Fatal(err)
	}
	if review.Accounts[0].Totals.IncreaseQuota != 10 || review.Accounts[1].Totals.DecreaseQuota != -20 {
		t.Fatal("copied amounts changed", review.Accounts)
	}
	// Existing generation consumes exactly these same preserved scan/head IDs.
	testAssembledGeneration(t, root, output, p, review)
	for _, extra := range [][]string{{"-create-output"}, {"-output", p.Windows[0].Job, "-create-output"}, {"-output", "../escape", "-create-output"}, {"-connection-file", "/never-read"}} {
		out.Reset()
		if err := run(context.Background(), append(append([]string(nil), args...), extra...), &out); err == nil || out.Len() != 0 {
			t.Fatal("overwrite or invalid command accepted", extra)
		}
	}
	if fileHash(t, filepath.Join(output, "funding.sqlite")) != receipt.DatabaseSHA256 {
		t.Fatal("failed retry changed assembled DB")
	}
	changed := p
	changed.Windows = append([]monthAuditWindow(nil), p.Windows...)
	changed.Windows[0].Job = "changed-observation"
	changed.Windows[0] = writeAuditWindow(t, root, changed, 0, func(page *monthlysource.FundingPage) {
		if page.Scan.ID == changed.Windows[0].Version.NewerScanID {
			page.Rows[0].Other = strings.Replace(page.Rows[0].Other, "10 点额度", "11 点额度", 1)
		}
	})
	changedRaw, err := json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := assembleMonth(context.Background(), root, "unresolved-output", changed, changedRaw, true); err == nil {
		t.Fatal("unresolved observation was assembled")
	}
	if _, err := os.Lstat(filepath.Join(root, "unresolved-output")); !os.IsNotExist(err) {
		t.Fatal("failed preflight left an output directory", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := assembleMonth(ctx, root, "cancelled", p, raw, true); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel ignored", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "cancelled")); !os.IsNotExist(err) {
		t.Fatal("canceled request created output")
	}
	if executable := os.Getenv("MONTHLY_CAPTURE_TEST_BINARY"); executable != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		result, err := exec.CommandContext(ctx, executable, append(args, "-output", "actual-cli", "-create-output")...).CombinedOutput()
		var actual monthAssemblyReceipt
		if err != nil || json.Unmarshal(result, &actual) != nil || actual.PlanSHA256 != receipt.PlanSHA256 || actual.ReviewSHA256 != receipt.ReviewSHA256 || actual.Pages != receipt.Pages || actual.Rows != receipt.Rows {
			t.Fatal("actual assembly command differs", err)
		}
		t.Log("actual assembly binary created and sealed identical observations")
		testAssemblyProcessInterruption(t, executable, args, root)
	}
	for path, sha := range hashes {
		if fileHash(t, path) != sha {
			t.Fatal("original changed", path)
		}
	}
}

func TestMonthAssemblyPageCopyFailurePreservesOriginal(t *testing.T) {
	root, p := auditPrivateDir(t), auditPlanFixture(t)
	w := writeAuditWindow(t, root, p, 0, func(page *monthlysource.FundingPage) {
		row := page.Rows[0]
		page.EndReached = false
		page.Rows = make([]monthlysource.FundingLog, 200)
		for i := range page.Rows {
			page.Rows[i] = row
			page.Rows[i].ID = int64(i + 1)
		}
	})
	sourcePath := filepath.Join(root, w.Job, "funding.sqlite")
	source, err := openReceiver(context.Background(), sourcePath, true)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	last, err := monthlysource.LoadLatestFundingPage(context.Background(), source, w.Version.OlderScanID)
	if err != nil {
		t.Fatal(err)
	}
	lastRaw, _ := json.Marshal(last)
	next := last
	next.PreviousID, next.After, next.EndReached = hash(lastRaw), last.Next, true
	next.StartedAtMS += 20
	next.CompletedAtMS += 20
	next.Rows = []monthlysource.FundingLog{last.Rows[0]}
	next.Rows[0].ID = 201
	next.Next.ID = 201
	head, err := monthlysource.SaveFundingPage(context.Background(), source, next)
	if err != nil {
		t.Fatal(err)
	}
	before := fileHash(t, sourcePath)
	for _, kind := range []string{"good", "wrong-head", "row-budget", "byte-budget", "write-failure"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(root, kind+".pending.sqlite")
			if err := exclusiveFile(path, nil); err != nil {
				t.Fatal(err)
			}
			dest, err := openReceiver(context.Background(), path, true)
			if err != nil {
				t.Fatal(err)
			}
			defer dest.Close()
			if err := monthlysource.InitFundingPageStore(context.Background(), dest); err != nil {
				t.Fatal(err)
			}
			if kind == "write-failure" {
				if _, err := dest.Exec(`CREATE TRIGGER fail_second_page BEFORE INSERT ON portal_monthly_funding_pages WHEN NEW.sequence=2 BEGIN SELECT RAISE(ABORT,'synthetic disk failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			expected, byteBudget, rowBudget := head, int64(96<<20), 100000
			if kind == "wrong-head" {
				expected = strings.Repeat("f", 64)
			}
			if kind == "row-budget" {
				rowBudget = 200
			}
			if kind == "byte-budget" {
				byteBudget = 1
			}
			n, count, _, err := copySelectedScan(context.Background(), source, dest, last.Scan.ID, expected, byteBudget, rowBudget)
			if kind == "good" {
				if err != nil || n != 2 || count != 201 {
					t.Fatal("copy failed", err, n, count)
				}
				got, err := monthlysource.LoadLatestFundingPage(context.Background(), dest, last.Scan.ID)
				if err != nil || !reflect.DeepEqual(got, next) {
					t.Fatal("original page changed during copy", err)
				}
			} else if err == nil {
				t.Fatal("bad copy accepted")
			}
			if kind == "write-failure" {
				var saved int
				if err := dest.QueryRow("SELECT count(*) FROM portal_monthly_funding_pages").Scan(&saved); err != nil || saved != 1 {
					t.Fatal("failure did not exercise partial staging", err, saved)
				}
			}
		})
	}
	if fileHash(t, sourcePath) != before {
		t.Fatal("copy changed original")
	}
}

func TestAssemblyPublishNeverReplacesAndHashBounds(t *testing.T) {
	dir := auditPrivateDir(t)
	pending, final := filepath.Join(dir, "pending"), filepath.Join(dir, "final")
	if err := exclusiveFile(pending, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := exclusiveFile(final, []byte("existing")); err != nil {
		t.Fatal(err)
	}
	if err := publishNewFile(pending, final); err == nil {
		t.Fatal("replaced final")
	}
	if fileHash(t, final) != hash([]byte("existing")) || fileHash(t, pending) != hash([]byte("new")) {
		t.Fatal("data lost")
	}
	if err := os.Truncate(pending, assemblyDatabaseLimit+1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := hashAssemblyDatabase(context.Background(), pending); err == nil {
		t.Fatal("accepted oversized output")
	}
}
