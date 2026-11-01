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

func TestMonthBatchCalendarAndCapacity(t *testing.T) {
	for _, tc := range []struct {
		month string
		days  int
	}{{"2024-02", 29}, {"2026-02", 28}, {"2026-09", 30}, {"2026-08", 31}} {
		f := auditPlanFixture(t).Funding
		f.Month = tc.month
		f.UserIDs = []int64{202, 201}
		p, err := newMonthBatch(context.Background(), 1, f, 3600)
		if err != nil || len(p.Windows) != tc.days*24 || !reflect.DeepEqual(p.Funding.UserIDs, []int64{201, 202}) || f.UserIDs[0] != 202 {
			t.Fatal("calendar/account normalization", err)
		}
		if time.Unix(p.Windows[0].From, 0).UTC().Hour() != 16 {
			t.Fatal("wrong month timezone")
		}
		for i, w := range p.Windows {
			if i > 0 && w.From != p.Windows[i-1].To {
				t.Fatal("hour gap")
			}
		}
	}
	for _, kind := range []string{"group", "future", "old-month", "too-many-windows", "nondivisor", "duplicate-user", "account-window-budget", "source"} {
		t.Run(kind, func(t *testing.T) {
			f, group, seconds := auditPlanFixture(t).Funding, int64(1), int64(3600)
			switch kind {
			case "group":
				group = 0
			case "future":
				f.Month = "2999-01"
			case "old-month":
				f.Month = "2019-01"
			case "too-many-windows":
				seconds = 300
			case "nondivisor":
				seconds = 1000
			case "duplicate-user":
				f.UserIDs = []int64{201, 201}
			case "source":
				f.SourceIdentity = "invalid"
			case "account-window-budget":
				seconds = 1800
				f.UserIDs = make([]int64, 200)
				for i := range f.UserIDs {
					f.UserIDs[i] = int64(i + 1)
				}
			}
			if _, err := newMonthBatch(context.Background(), group, f, seconds); err == nil {
				t.Fatal("invalid batch accepted")
			}
		})
	}
	p, err := newMonthBatch(context.Background(), 1, auditPlanFixture(t).Funding, 900)
	if err != nil || len(p.Windows) != 2880 {
		t.Fatal("valid smaller windows rejected", err)
	}
}

func TestMonthBatchValidationAndProgress(t *testing.T) {
	base, err := newMonthBatch(context.Background(), 1, auditPlanFixture(t).Funding, 3600)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"gap", "missing", "duplicate-scan", "changed-group", "changed-scope", "manifest", "job", "window-size"} {
		t.Run(kind, func(t *testing.T) {
			p := base
			p.Windows = append([]monthCaptureWindow(nil), base.Windows...)
			switch kind {
			case "gap":
				p.Windows[1].From++
			case "missing":
				p.Windows = p.Windows[:719]
			case "duplicate-scan":
				p.Windows[1].OlderScanID = p.Windows[0].OlderScanID
			case "changed-group":
				p.GroupID++
			case "changed-scope":
				p.Funding.UserIDs = []int64{201}
			case "manifest":
				p.Windows[0].ManifestSHA256 = strings.Repeat("f", 64)
			case "job":
				p.Windows[0].Job = "../escape"
			case "window-size":
				p.WindowSeconds = 1800
			}
			if validateMonthBatch(context.Background(), p) == nil {
				t.Fatal("invalid immutable batch accepted")
			}
		})
	}
	for _, kind := range []string{"empty", "resume-first-pass", "second-pass", "second-resume", "done", "premature-second", "interleaved", "fence", "clock"} {
		t.Run(kind, func(t *testing.T) {
			checks := make([]batchWindowCheckpoint, len(base.Windows))
			wantPhase, wantNext := "older", 0
			if kind != "empty" && kind != "resume-first-pass" {
				for i := range checks {
					checks[i] = batchWindowCheckpoint{olderDone: true, olderEndMS: 1000, olderFence: 100, latestEndMS: 1000}
				}
				wantPhase = "newer"
			}
			switch kind {
			case "resume-first-pass":
				checks[0].olderDone = true
				wantNext = 1
			case "second-resume":
				checks[0].newerStarted = true
				checks[0].newerDone = true
				checks[0].newerStartMS = 2000
				checks[0].newerFence = 100
				wantNext = 1
			case "done":
				for i := range checks {
					checks[i].newerStarted = true
					checks[i].newerDone = true
					checks[i].newerStartMS = 2000
					checks[i].newerFence = 100
				}
				wantPhase = "checkpoints_finished_needs_full_audit"
			case "premature-second", "interleaved", "fence":
				checks[0].newerStarted = true
				checks[0].newerStartMS = 2000
				checks[0].newerFence = 100
				if kind == "premature-second" {
					checks[719].olderDone = false
				}
				if kind == "interleaved" {
					checks[0].newerStartMS = 1000
				}
				if kind == "fence" {
					checks[0].newerFence = 99
				}
				wantPhase = "blocked"
			case "clock":
				checks[719].latestEndMS = 10001
				wantPhase = "blocked"
			}
			r := monthBatchProgress(base, checks, strings.Repeat("a", 64), time.UnixMilli(10000))
			if r.Phase != wantPhase {
				t.Fatal("wrong phase", kind, r)
			}
			if wantPhase == "older" || wantPhase == "newer" {
				if r.Next == nil || r.Next.Job != base.Windows[wantNext].Job || r.Next.Phase != wantPhase {
					t.Fatal("wrong resume job", r)
				}
			} else if r.Next != nil {
				t.Fatal("unexpected executable next job", r)
			}
		})
	}
}

func TestMonthBatchCreateInspectAndDurableScope(t *testing.T) {
	root := auditPrivateDir(t)
	args := []string{"init-month", "-workspace", root, "-batch", "sep", "-group-id", "1", "-month", "2026-09", "-source-identity", strings.Repeat("a", 64), "-user-ids", "202,201"}
	var out bytes.Buffer
	if err := run(context.Background(), args, &out); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatal("preview created files", err)
	}
	var preview monthBatchReceipt
	if json.Unmarshal(out.Bytes(), &preview) != nil || preview.Windows != 720 || preview.MinimumPageReads != 1440 || preview.BatchSHA256 != "" {
		t.Fatal("incorrect preview", preview)
	}
	out.Reset()
	if err := run(context.Background(), append(args, "-create"), &out); err != nil {
		t.Fatal(err)
	}
	var created monthBatchReceipt
	if json.Unmarshal(out.Bytes(), &created) != nil || !validSHA(created.BatchSHA256) {
		t.Fatal("invalid batch receipt")
	}
	dir := filepath.Join(root, "sep")
	p, err := readMonthBatch(context.Background(), dir, created.BatchSHA256)
	if err != nil {
		t.Fatal(err)
	}
	before := map[string]string{}
	for _, w := range p.Windows {
		for _, name := range []string{"manifest.json", "funding.sqlite"} {
			path := filepath.Join(dir, w.Job, name)
			before[path] = fileHash(t, path)
		}
	}
	inspect := []string{"inspect-month", "-workspace", root, "-batch", "sep", "-expected-batch", created.BatchSHA256}
	check := func() monthBatchReceipt {
		t.Helper()
		out.Reset()
		if err := run(context.Background(), inspect, &out); err != nil {
			t.Fatal(err)
		}
		var r monthBatchReceipt
		if json.Unmarshal(out.Bytes(), &r) != nil {
			t.Fatal("non-json progress")
		}
		return r
	}
	r := check()
	if r.Phase != "older" || r.OlderCompleted != 0 || r.NewerCompleted != 0 || r.Next == nil || r.Next.Job != p.Windows[0].Job {
		t.Fatal("empty batch misclassified", r)
	}
	for path, sha := range before {
		if fileHash(t, path) != sha {
			t.Fatal("inspection modified empty evidence")
		}
	}
	for _, extra := range [][]string{{"-expected-batch", strings.Repeat("f", 64)}, {"-capture"}, {"-connection-file", "/never-read"}} {
		out.Reset()
		if err := run(context.Background(), append(append([]string(nil), inspect...), extra...), &out); err == nil || out.Len() != 0 {
			t.Fatal("invalid control accepted", extra)
		}
	}
	if err := run(context.Background(), append(args, "-create"), &out); err == nil {
		t.Fatal("existing batch overwritten")
	}
	lock, err := workspaceLock(dir, true, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = inspectMonthBatch(context.Background(), root, "sep", created.BatchSHA256)
	_ = lock.Close()
	if !errors.Is(err, errBusy) {
		t.Fatal("inspection raced capture", err)
	}
	for _, w := range p.Windows {
		saveBatchTestPage(t, dir, p, w, "older")
	}
	r = check()
	if r.Phase != "newer" || r.OlderCompleted != 720 || r.NewerCompleted != 0 {
		t.Fatal("did not advance to second sweep", r)
	}
	saveBatchTestPage(t, dir, p, p.Windows[0], "newer")
	r = check()
	if r.Phase != "newer" || r.NewerCompleted != 1 || r.Next.Job != p.Windows[1].Job {
		t.Fatal("second sweep resume lost", r)
	}
	for _, w := range p.Windows[1:] {
		saveBatchTestPage(t, dir, p, w, "newer")
	}
	r = check()
	if r.Phase != "checkpoints_finished_needs_full_audit" || r.Next != nil || r.NewerCompleted != 720 {
		t.Fatal("completion claimed prematurely", r)
	}
	if fileHash(t, filepath.Join(dir, "batch.json")) != created.BatchSHA256 {
		t.Fatal("batch scope changed")
	}
	if executable := os.Getenv("MONTHLY_CAPTURE_TEST_BINARY"); executable != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		result, err := exec.CommandContext(ctx, executable, inspect...).CombinedOutput()
		if err != nil || !bytes.Equal(result, out.Bytes()) {
			t.Fatal("actual inspection command differs", err)
		}
		result, err = exec.CommandContext(ctx, executable, append(args, "-batch", "actual-empty", "-create")...).CombinedOutput()
		var actual monthBatchReceipt
		if err != nil || json.Unmarshal(result, &actual) != nil || actual.Windows != 720 || actual.Mode != "created_empty_month_batch_not_captured" {
			t.Fatal("actual batch creation failed", err)
		}
		got, err := inspectMonthBatch(ctx, root, "actual-empty", actual.BatchSHA256)
		if err != nil || got.Phase != "older" || got.OlderCompleted != 0 {
			t.Fatal("actual new batch not empty", err)
		}
		t.Log("actual command created a private empty 720-window batch and inspected retained completed checkpoints")
		testMonthBatchCreationInterrupted(t, executable, args, root)
	}
}

func testMonthBatchCreationInterrupted(t *testing.T, executable string, args []string, root string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	commandArgs := append(append([]string(nil), args...), "-batch", "interrupted-init", "-create")
	cmd := exec.CommandContext(ctx, executable, commandArgs...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() { _ = cmd.Process.Kill() }()
	dir := filepath.Join(root, "interrupted-init")
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			t.Fatal("batch creation completed before interrupt exercise", err)
		case <-ctx.Done():
			t.Fatal("no first durable window", ctx.Err())
		case <-ticker.C:
			if _, err := os.Stat(filepath.Join(dir, "window-0000", "manifest.json")); err != nil {
				continue
			}
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err == nil {
				t.Fatal("interrupted creation reported success")
			}
			pending := filepath.Join(dir, "batch.pending.json")
			before := fileHash(t, pending)
			if _, err := os.Lstat(filepath.Join(dir, "batch.json")); !os.IsNotExist(err) {
				t.Fatal("partial batch became ready")
			}
			if _, err := inspectMonthBatch(ctx, root, "interrupted-init", before); err == nil {
				t.Fatal("partial batch accepted for inspection")
			}
			if err := exec.CommandContext(ctx, executable, commandArgs...).Run(); err == nil {
				t.Fatal("partial batch overwritten")
			}
			if fileHash(t, pending) != before {
				t.Fatal("retry regenerated persisted scan identities")
			}
			t.Log("actual batch creation interrupted after first durable window; immutable pending plan retained and retry refused")
			return
		}
	}
}

func TestMonthBatchUsesFirstPageForSweepOrder(t *testing.T) {
	root := auditPrivateDir(t)
	p, err := newMonthBatch(context.Background(), 1, auditPlanFixture(t).Funding, 3600)
	if err != nil {
		t.Fatal(err)
	}
	w := p.Windows[0]
	m := batchManifest(p, w)
	if _, err := initialize(context.Background(), root, w.Job, m, true); err != nil {
		t.Fatal(err)
	}
	saveBatchTestPage(t, root, p, w, "older")
	db, err := openReceiver(context.Background(), filepath.Join(root, w.Job, "funding.sqlite"), true)
	if err != nil {
		t.Fatal(err)
	}
	base := int64(1790870400000)
	first := monthlysource.FundingPage{Version: 1, Scan: m.Newer, FenceID: 1000, StartedAtMS: base - 1000, CompletedAtMS: base - 990, Rows: make([]monthlysource.FundingLog, 200)}
	for i := range first.Rows {
		first.Rows[i] = monthlysource.FundingLog{ID: int64(i + 1), UserID: 201, CreatedAt: w.From + 1, Type: 4, Content: "用户签到，获得额度 1 点额度"}
	}
	first.Next = monthlysource.FundingCursor{At: w.From + 1, ID: 200}
	head, err := monthlysource.SaveFundingPage(context.Background(), db, first)
	if err != nil {
		t.Fatal(err)
	}
	last := first
	last.PreviousID = head
	last.After = first.Next
	last.StartedAtMS = base + 1000
	last.CompletedAtMS = base + 1010
	last.EndReached = true
	last.Rows = []monthlysource.FundingLog{first.Rows[0]}
	last.Rows[0].ID = 201
	last.Next.ID = 201
	_, err = monthlysource.SaveFundingPage(context.Background(), db, last)
	err = errors.Join(err, db.Close())
	if err != nil {
		t.Fatal(err)
	}
	c, err := inspectBatchWindow(context.Background(), root, w)
	if err != nil || c.newerStartMS != first.StartedAtMS || c.latestEndMS != last.CompletedAtMS {
		t.Fatal("latest page hid early sweep start", c, err)
	}
	checks := make([]batchWindowCheckpoint, len(p.Windows))
	for i := range checks {
		checks[i] = batchWindowCheckpoint{olderDone: true, olderEndMS: base + 10, olderFence: 100, latestEndMS: base + 10}
	}
	checks[0] = c
	r := monthBatchProgress(p, checks, strings.Repeat("a", 64), time.Now())
	if r.Phase != "blocked" || r.Next != nil {
		t.Fatal("interleaved multi-page sweep allowed", r)
	}
}

func saveBatchTestPage(t *testing.T, dir string, p monthCaptureBatch, w monthCaptureWindow, phase string) {
	t.Helper()
	m := batchManifest(p, w)
	scan := m.Older
	at := int64(1790870400000)
	if phase == "newer" {
		scan = m.Newer
		at += 1000000
	}
	db, err := openReceiver(context.Background(), filepath.Join(dir, w.Job, "funding.sqlite"), true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = monthlysource.SaveFundingPage(context.Background(), db, monthlysource.FundingPage{Version: 1, Scan: scan, FenceID: 100, EndReached: true, StartedAtMS: at, CompletedAtMS: at + 10, Rows: []monthlysource.FundingLog{}})
	err = errors.Join(err, db.Close())
	if err != nil {
		t.Fatal(err)
	}
}

func TestMonthBatchRejectsIncompleteAndMutatedMetadata(t *testing.T) {
	p, err := newMonthBatch(context.Background(), 1, auditPlanFixture(t).Funding, 3600)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"pending", "unknown", "duplicate", "scope", "linked", "permissions"} {
		t.Run(kind, func(t *testing.T) {
			dir := auditPrivateDir(t)
			data := append([]byte(nil), raw...)
			if kind == "unknown" {
				data = append([]byte(`{"extra":1,`), data[1:]...)
			}
			if kind == "duplicate" {
				data = append([]byte(`{"version":1,`), data[1:]...)
			}
			if kind == "scope" {
				q := p
				q.GroupID++
				data, err = json.Marshal(q)
				if err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(dir, "batch.json")
			if kind == "linked" {
				other := filepath.Join(dir, "original")
				if err := exclusiveFile(other, data); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(other, path); err != nil {
					t.Fatal(err)
				}
			} else if err := exclusiveFile(path, data); err != nil {
				t.Fatal(err)
			}
			if kind == "pending" {
				if err := exclusiveFile(filepath.Join(dir, "batch.pending.json"), raw); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "permissions" {
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := readMonthBatch(context.Background(), dir, hash(data)); err == nil {
				t.Fatal("unsafe batch read", kind)
			}
		})
	}
	root := auditPrivateDir(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := initializeMonthBatch(ctx, root, "cancelled", p, true); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled creation accepted")
	}
	if _, err := os.Stat(filepath.Join(root, "cancelled")); !os.IsNotExist(err) {
		t.Fatal("cancellation wrote batch")
	}
}
