package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func auditPlanFixture(t *testing.T) monthAuditPlan {
	t.Helper()
	f := monthlysource.FundingMonthRequest{Month: "2026-09", SourceIdentity: strings.Repeat("a", 64), Rules: monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}, UserIDs: []int64{201, 202}}
	p := monthAuditPlan{Version: 1, GroupID: 1, Funding: f}
	for i := 0; i < 720; i++ {
		from := int64(1788192000 + i*3600)
		p.Windows = append(p.Windows, monthAuditWindow{Job: fmt.Sprintf("window-%d", i), ManifestSHA256: strings.Repeat("b", 64), Version: monthlysource.FundingMonthWindowVersion{
			From: from, To: from + 3600, OlderScanID: fmt.Sprintf("%032x", i*2+1), NewerScanID: fmt.Sprintf("%032x", i*2+2), OlderHeadID: strings.Repeat("c", 64), NewerHeadID: strings.Repeat("d", 64),
		}})
	}
	return p
}

func TestMonthAuditPlanRejectsGapsAndScopeBeforeOpeningFiles(t *testing.T) {
	for _, kind := range []string{"missing-first", "missing-middle", "missing-last", "overlap", "duplicate-job", "duplicate-scan", "same-pass", "wrong-head", "wrong-manifest", "escape", "rules", "unsorted-accounts", "duplicate-account", "group", "too-many"} {
		t.Run(kind, func(t *testing.T) {
			p := auditPlanFixture(t)
			switch kind {
			case "missing-first":
				p.Windows = p.Windows[1:]
			case "missing-middle":
				p.Windows = append(p.Windows[:5], p.Windows[6:]...)
			case "missing-last":
				p.Windows = p.Windows[:719]
			case "overlap":
				p.Windows[1].Version.From--
			case "duplicate-job":
				p.Windows[1].Job = p.Windows[0].Job
			case "duplicate-scan":
				p.Windows[1].Version.OlderScanID = p.Windows[0].Version.OlderScanID
			case "same-pass":
				p.Windows[0].Version.NewerScanID = p.Windows[0].Version.OlderScanID
			case "wrong-head":
				p.Windows[0].Version.NewerHeadID = ""
			case "wrong-manifest":
				p.Windows[0].ManifestSHA256 = ""
			case "escape":
				p.Windows[0].Job = "../other"
			case "rules":
				p.Funding.Rules.QuotaPerUnit++
			case "unsorted-accounts":
				p.Funding.UserIDs = []int64{202, 201}
			case "duplicate-account":
				p.Funding.UserIDs = []int64{201, 201}
			case "group":
				p.GroupID = 0
			case "too-many":
				p.Windows = make([]monthAuditWindow, 4097)
			}
			if _, err := validateMonthAuditPlan(context.Background(), p); err == nil {
				t.Fatal("invalid plan accepted", kind)
			}
		})
	}
	p := auditPlanFixture(t)
	p.Windows[0], p.Windows[719] = p.Windows[719], p.Windows[0]
	got, err := validateMonthAuditPlan(context.Background(), p)
	if err != nil || got[0].Version.From != 1788192000 || p.Windows[0].Job != "window-719" {
		t.Fatal("detached ordering failed", err)
	}
}

// Creates only synthetic local observations. Each job has different scan IDs
// and timestamps; it does not duplicate a captured pass to fake independence.
func writeAuditWindow(t *testing.T, root string, p monthAuditPlan, index int, mutate func(*monthlysource.FundingPage)) monthAuditWindow {
	t.Helper()
	w := p.Windows[index]
	v := w.Version
	a := monthlysource.FundingScan{Version: p.Version, ID: v.OlderScanID, SourceIdentity: p.Funding.SourceIdentity, Rules: p.Funding.Rules, UserIDs: p.Funding.UserIDs, From: v.From, To: v.To}
	b := a
	b.ID = v.NewerScanID
	m := manifest{Version: 1, GroupID: p.GroupID, Month: p.Funding.Month, Older: a, Newer: b}
	r, err := initialize(context.Background(), root, w.Job, m, true)
	if err != nil {
		t.Fatal(err)
	}
	w.ManifestSHA256 = r.ManifestSHA256
	db, err := openReceiver(context.Background(), filepath.Join(root, w.Job, "funding.sqlite"), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for pass, scan := range []monthlysource.FundingScan{a, b} {
		at := int64(1790870400000) + int64(pass)*1000000 + int64(index)*100
		page := monthlysource.FundingPage{Version: 1, Scan: scan, FenceID: 100000, EndReached: true, StartedAtMS: at, CompletedAtMS: at + 10, Rows: []monthlysource.FundingLog{}}
		if index == 0 {
			page.Rows = []monthlysource.FundingLog{
				{ID: 1, UserID: 9, CreatedAt: v.From, Type: 3, Other: `{"op":{"action":"user.quota_add","params":{"target_user_id":201,"quota":"10 点额度"}},"admin_info":{"admin_id":9}}`},
				{ID: 2, UserID: 9, CreatedAt: v.To - 1, Type: 3, Other: `{"op":{"action":"user.quota_subtract","params":{"target_user_id":202,"quota":"20 点额度"}},"admin_info":{"admin_id":9}}`},
			}
		}
		if mutate != nil {
			mutate(&page)
		}
		if len(page.Rows) > 0 {
			last := page.Rows[len(page.Rows)-1]
			page.Next = monthlysource.FundingCursor{At: last.CreatedAt, ID: last.ID}
		}
		head, err := monthlysource.SaveFundingPage(context.Background(), db, page)
		if err != nil {
			t.Fatal(err)
		}
		if pass == 0 {
			w.Version.OlderHeadID = head
		} else {
			w.Version.NewerHeadID = head
		}
	}
	return w
}

func auditPrivateDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestMonthAuditWindowPinsAndTamper(t *testing.T) {
	for _, kind := range []string{"scope", "group", "head", "source", "manifest", "incomplete", "index", "pending", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			root, p := auditPrivateDir(t), auditPlanFixture(t)
			var mutate func(*monthlysource.FundingPage)
			if kind == "incomplete" {
				mutate = func(page *monthlysource.FundingPage) {
					row := page.Rows[0]
					page.EndReached = false
					page.Rows = make([]monthlysource.FundingLog, monthlysource.FundingPageLimit)
					for i := range page.Rows {
						page.Rows[i] = row
						page.Rows[i].ID = int64(i + 1)
					}
				}
			}
			w := writeAuditWindow(t, root, p, 0, mutate)
			dir := filepath.Join(root, w.Job)
			switch kind {
			case "scope":
				p.Funding.UserIDs = []int64{201}
			case "group":
				p.GroupID++
			case "head":
				w.Version.NewerHeadID = strings.Repeat("f", 64)
			case "source":
				p.Funding.SourceIdentity = strings.Repeat("f", 64)
			case "manifest":
				w.ManifestSHA256 = strings.Repeat("f", 64)
			case "pending":
				if err := exclusiveFile(filepath.Join(dir, "manifest.pending"), []byte("partial")); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(dir, filepath.Join(root, "linked")); err != nil {
					t.Fatal(err)
				}
				w.Job = "linked"
			case "index":
				db, err := openReceiver(context.Background(), filepath.Join(dir, "funding.sqlite"), true)
				if err != nil {
					t.Fatal(err)
				}
				_, err = db.Exec("DROP TRIGGER monthly_funding_records_no_delete")
				if err == nil {
					_, err = db.Exec("DELETE FROM portal_monthly_funding_records")
				}
				err = errors.Join(err, db.Close())
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := auditMonthWindow(context.Background(), root, p, w); err == nil {
				t.Fatal("invalid local evidence accepted", kind)
			}
		})
	}
}

func TestMonthAuditFullMonthReadOnlyAndExactAccountAmounts(t *testing.T) {
	root, p := auditPrivateDir(t), auditPlanFixture(t)
	before := make(map[string]string)
	for i := range p.Windows {
		p.Windows[i] = writeAuditWindow(t, root, p, i, nil)
		for _, name := range []string{"manifest.json", "funding.sqlite"} {
			path := filepath.Join(root, p.Windows[i].Job, name)
			before[path] = fileHash(t, path)
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
	args := []string{"audit-month", "-workspace", root, "-plan", planPath, "-expected-plan", hash(raw)}
	var out bytes.Buffer
	started := time.Now()
	if err := run(context.Background(), args, &out); err != nil {
		t.Fatal(err)
	}
	t.Logf("720 windows, 1440 independent scan heads audited in %s", time.Since(started))
	var got monthAuditReceipt
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	a := got.Review.Accounts
	if got.Mode != "local_observations_only_not_bill_approval" || got.PlanSHA256 != hash(raw) || len(got.Review.Windows.Windows) != 720 || len(got.Review.Windows.MissingWindows) != 0 || len(a) != 2 || a[0].UserID != 201 || a[1].UserID != 202 || a[0].Totals == nil || a[1].Totals == nil || a[0].Totals.IncreaseQuota != 10 || a[0].Totals.DecreaseQuota != 0 || a[1].Totals.IncreaseQuota != 0 || a[1].Totals.DecreaseQuota != -20 || len(a[0].Movements) != 1 || len(a[1].Movements) != 1 {
		t.Fatal("month coverage, exact amount or account isolation failed", got)
	}
	if executable := os.Getenv("MONTHLY_CAPTURE_TEST_BINARY"); executable != "" {
		if !filepath.IsAbs(executable) {
			t.Fatal("test binary must be explicit and absolute")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		actual, err := exec.CommandContext(ctx, executable, args...).CombinedOutput()
		if err != nil || !bytes.Equal(actual, out.Bytes()) {
			t.Fatal("actual CLI differs from reviewed result", err)
		}
		t.Log("actual command process returned identical complete-month result")
	}
	for path, sha := range before {
		if fileHash(t, path) != sha {
			t.Fatal("read-only audit modified original", path)
		}
	}
	for _, extra := range [][]string{{"-expected-plan", strings.Repeat("f", 64)}, {"-capture"}, {"-connection-file", "/never-read"}} {
		out.Reset()
		if err := run(context.Background(), append(append([]string(nil), args...), extra...), &out); err == nil || out.Len() != 0 {
			t.Fatal("unexpected accepted flags or partial output", extra)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out.Reset()
	if err := run(ctx, args, &out); !errors.Is(err, context.Canceled) || out.Len() != 0 {
		t.Fatal("canceled audit emitted success", err)
	}
	lock, err := workspaceLock(root, true, false)
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	err = run(context.Background(), args, &out)
	_ = lock.Close()
	if !errors.Is(err, errBusy) || out.Len() != 0 {
		t.Fatal("audit raced active capture", err)
	}
	for _, kind := range []string{"changed-account", "unknown-account", "cross-window-duplicate"} {
		t.Run(kind, func(t *testing.T) {
			q := p
			q.Windows = append([]monthAuditWindow(nil), p.Windows...)
			index := 0
			if kind == "cross-window-duplicate" {
				index = 1
			}
			q.Windows[index].Job = kind
			q.Windows[index] = writeAuditWindow(t, root, q, index, func(page *monthlysource.FundingPage) {
				switch kind {
				case "changed-account":
					if page.Scan.ID == q.Windows[index].Version.NewerScanID {
						page.Rows[0].Other = strings.Replace(page.Rows[0].Other, "10 点额度", "11 点额度", 1)
					}
				case "unknown-account":
					page.Rows[0].Other = `{"op":{"action":"user.quota_add","params":{"quota":"10 点额度"}},"admin_info":{"admin_id":9}}`
					// Explicit malformed target is never assigned to the actor.
					page.Rows[0].Other = strings.Replace(page.Rows[0].Other, `"params":{`, `"params":{"target_user_id":"invalid",`, 1)
				case "cross-window-duplicate":
					page.Rows = []monthlysource.FundingLog{{ID: 1, UserID: 201, CreatedAt: page.Scan.From, Type: 4, Content: "用户签到，获得额度 1 点额度"}}
				}
			})
			review, err := auditMonth(context.Background(), root, q)
			if err != nil {
				t.Fatal(err)
			}
			if review.Accounts[0].Totals != nil || review.Accounts[0].Status != "incomplete_observation" {
				t.Fatal("uncertain account exposed a money total", review.Accounts[0])
			}
			if kind == "unknown-account" {
				if review.Accounts[1].Totals != nil {
					t.Fatal("unscoped operation passed as known", review.Accounts[1])
				}
			} else if review.Accounts[1].Totals == nil || review.Accounts[1].Totals.DecreaseQuota != -20 {
				t.Fatal("unaffected account changed", review.Accounts[1])
			}
			if kind == "cross-window-duplicate" && len(review.Windows.Conflicts) != 1 {
				t.Fatal("duplicate not surfaced")
			}
		})
	}
	for _, kind := range []string{"unknown-field", "duplicate-field", "shared-file", "linked-file"} {
		t.Run(kind, func(t *testing.T) {
			data := append([]byte(nil), raw...)
			if kind == "unknown-field" {
				data = append([]byte(`{"unknown":true,`), raw[1:]...)
			}
			if kind == "duplicate-field" {
				data = append([]byte(`{"version":1,`), raw[1:]...)
			}
			path := filepath.Join(root, kind+".json")
			if kind == "linked-file" {
				if err := os.Symlink(planPath, path); err != nil {
					t.Fatal(err)
				}
			} else if err := exclusiveFile(path, data); err != nil {
				t.Fatal(err)
			}
			if kind == "shared-file" {
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
			}
			var result bytes.Buffer
			err := run(context.Background(), []string{"audit-month", "-workspace", root, "-plan", path, "-expected-plan", hash(data)}, &result)
			if err == nil || result.Len() != 0 {
				t.Fatal("invalid month plan exposed partial result", kind)
			}
		})
	}
	// All individual windows can be valid while the full-month sweep order is
	// wrong. Replace only the selected fixture job, never overwrite old evidence.
	q := p
	q.Windows = append([]monthAuditWindow(nil), p.Windows...)
	q.Windows[719].Job = "interleaved-last"
	q.Windows[719] = writeAuditWindow(t, root, q, 719, func(page *monthlysource.FundingPage) {
		page.StartedAtMS += 5000000
		page.CompletedAtMS += 5000000
	})
	bad, err := auditMonth(context.Background(), root, q)
	if err == nil || !reflect.DeepEqual(bad, monthlysource.FundingMoneyReview{}) {
		t.Fatal("interleaved sweeps accepted", err)
	}
}
