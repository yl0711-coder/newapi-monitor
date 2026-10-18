//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func rechargeTaskFixture(t *testing.T) (*gorm.DB, string, string, financeRechargeGuardPlan) {
	t.Helper()
	db, plan, _, _ := guardedRechargeFixture(t)
	dir := filepath.Join(t.TempDir(), "task")
	confirmation, err := createFinanceRechargeTask(context.Background(), db, dir, financeRechargeTaskPlan{Repair: &plan})
	if err != nil {
		t.Fatal(err)
	}
	return db, dir, confirmation, plan
}

func TestFinanceRechargeTaskApprovalExecutionRecovery(t *testing.T) {
	db, dir, confirmation, original := rechargeTaskFixture(t)
	ctx, now := context.Background(), time.Now().Unix()
	before := guardedVersionRows(t, db)
	if _, err := runFinanceRechargeTask(ctx, db, dir, confirmation, "root", now); err == nil {
		t.Fatal("ran without approval")
	}
	if err := approveFinanceRechargeTask(ctx, db, dir, confirmation, "root", now); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(filepath.Join(dir, "approval.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := approveFinanceRechargeTask(ctx, db, dir, confirmation, "root", now+1); err != nil {
		t.Fatal(err)
	}
	second, _ := os.ReadFile(filepath.Join(dir, "approval.json"))
	if string(first) != string(second) || !reflect.DeepEqual(before, guardedVersionRows(t, db)) {
		t.Fatal("approval extended or changed money")
	}
	if _, err := runFinanceRechargeTask(ctx, db, dir, confirmation, "other-root", now); err == nil {
		t.Fatal("used another actor approval")
	}
	result, err := runFinanceRechargeTask(ctx, db, dir, confirmation, "root", now)
	if err != nil || !result.Committed || !result.ReceiptAvailable {
		t.Fatal(result, err)
	}
	after := guardedVersionRows(t, db)
	if result, err := runFinanceRechargeTask(ctx, db, dir, confirmation, "root", original.ExpiresAt+1); err != nil || !result.Committed || !reflect.DeepEqual(after, guardedVersionRows(t, db)) {
		t.Fatal("lost-response replay after expiry", result, err)
	}
	if err := stopFinanceRechargeTask(ctx, db, dir, confirmation, "root", now); err == nil {
		t.Fatal("stop claims to undo committed task")
	}
	hash, _ := financeRechargeGuardHash(original)
	recovery, err := prepareFinanceRechargeRecovery(ctx, db, original, hash, now)
	if err != nil {
		t.Fatal(err)
	}
	recoveryDir := filepath.Join(t.TempDir(), "recovery")
	recoveryConfirmation, err := createFinanceRechargeTask(ctx, db, recoveryDir, financeRechargeTaskPlan{Recovery: &recovery})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runFinanceRechargeTask(ctx, db, recoveryDir, recoveryConfirmation, "root", now); err == nil {
		t.Fatal("recovery reused repair approval")
	}
	if err := approveFinanceRechargeTask(ctx, db, recoveryDir, recoveryConfirmation, "root", now); err != nil {
		t.Fatal(err)
	}
	if result, err := runFinanceRechargeTask(ctx, db, recoveryDir, recoveryConfirmation, "root", now); err != nil || !result.Committed {
		t.Fatal(result, err)
	}
}

func TestFinanceRechargeTaskReceiptFailureAndReceiverBinding(t *testing.T) {
	db, dir, confirmation, plan := rechargeTaskFixture(t)
	ctx, now := context.Background(), time.Now().Unix()
	if err := approveFinanceRechargeTask(ctx, db, dir, confirmation, "root", now); err != nil {
		t.Fatal(err)
	}
	// Simulate receipt persistence failure after the transaction, not DB failure.
	if err := os.Mkdir(filepath.Join(dir, "receipt.json"), 0700); err != nil {
		t.Fatal(err)
	}
	result, err := runFinanceRechargeTask(ctx, db, dir, confirmation, "root", now)
	if err != nil || !result.Committed || result.ReceiptAvailable || result.Status != "committed_receipt_pending" {
		t.Fatal(result, err)
	}
	after := guardedVersionRows(t, db)
	var database struct{ File string }
	if err := db.Raw("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&database).Error; err != nil {
		t.Fatal(err)
	}
	conn, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, closeDB, err := giftLocalDatabase(database.File)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	db = reopened
	if result, err := runFinanceRechargeTask(ctx, db, dir, confirmation, "root", plan.ExpiresAt+1); err != nil || !result.Committed || result.ReceiptAvailable || !reflect.DeepEqual(after, guardedVersionRows(t, db)) {
		t.Fatal("receipt failure retried money", result, err)
	}
	other, _, _, _ := guardedRechargeFixture(t)
	if _, err := runFinanceRechargeTask(ctx, other, dir, confirmation, "root", now); err == nil {
		t.Fatal("task transferred to another database")
	}
}

func TestFinanceRechargeTaskStopAndDrift(t *testing.T) {
	for _, mode := range []string{"stop", "drift", "approval_corrupt", "intent_corrupt", "expiry", "plan_changed"} {
		t.Run(mode, func(t *testing.T) {
			db, dir, confirmation, plan := rechargeTaskFixture(t)
			ctx, now := context.Background(), time.Now().Unix()
			if err := approveFinanceRechargeTask(ctx, db, dir, confirmation, "root", now); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "stop":
				if err := stopFinanceRechargeTask(ctx, db, dir, confirmation, "root", now); err != nil {
					t.Fatal(err)
				}
			case "drift":
				if err := db.Model(&ChannelUpstreamUsageHour{}).Where("domain=?", plan.Request.Corrections[0].Domain).Update("cost_usd", 99).Error; err != nil {
					t.Fatal(err)
				}
			case "approval_corrupt":
				if err := os.WriteFile(filepath.Join(dir, "approval.json"), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "intent_corrupt":
				if err := giftLocalWriteNew(filepath.Join(dir, "started.json"), []byte("{")); err != nil {
					t.Fatal(err)
				}
			case "expiry":
				now = plan.ExpiresAt + 1
			case "plan_changed":
				if err := os.WriteFile(filepath.Join(dir, "plan.json"), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			before := guardedVersionRows(t, db)
			if _, err := runFinanceRechargeTask(ctx, db, dir, confirmation, "root", now); err == nil {
				t.Fatal("unsafe execution accepted")
			}
			if !reflect.DeepEqual(before, guardedVersionRows(t, db)) {
				t.Fatal("rejected execution changed history")
			}
		})
	}
}

func rechargeTaskHTTP(t *testing.T, db *gorm.DB, dir, action, body string, role int, origin string) *httptest.ResponseRecorder {
	t.Helper()
	r := gin.New()
	r.POST("/task", func(c *gin.Context) {
		if role > 0 {
			c.Set("urole", role)
			c.Set("uname", "root")
		}
		serveFinanceRechargeTask(c, db, dir, action)
	})
	req := httptest.NewRequest("POST", "http://localhost/task", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestFinanceRechargeTaskHTTPBoundary(t *testing.T) {
	db, dir, confirmation, _ := rechargeTaskFixture(t)
	body := `{"confirmation":"` + confirmation + `"}`
	before := guardedVersionRows(t, db)
	for _, role := range []int{0, 1, 10} {
		if w := rechargeTaskHTTP(t, db, dir, "approve", body, role, ""); w.Code != 403 {
			t.Fatal(role, w.Code, w.Body.String())
		}
	}
	for _, tc := range []struct{ body, origin string }{{body, "https://other.example"}, {`{"confirmation":"` + confirmation + `","actor":"root"}`, ""}, {`{"confirmation":"` + confirmation + `","path":"/tmp/other.db"}`, ""}} {
		if w := rechargeTaskHTTP(t, db, dir, "approve", tc.body, roleRoot, tc.origin); w.Code < 400 {
			t.Fatal("unsafe HTTP accepted", w.Body.String())
		}
	}
	for _, action := range []string{"preview", "status", "approve"} {
		if w := rechargeTaskHTTP(t, db, dir, action, body, roleRoot, ""); w.Code != 200 {
			t.Fatal(action, w.Code, w.Body.String())
		} else if action == "preview" {
			if !strings.Contains(w.Body.String(), `"paid":1`) || !strings.Contains(w.Body.String(), `"credit":1`) || strings.Contains(w.Body.String(), "SnapshotJSON") || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("unclear/unsafe preview", w.Body.String())
			}
		}
	}
	if !reflect.DeepEqual(before, guardedVersionRows(t, db)) {
		t.Fatal("preview/approval modified money")
	}
	w := rechargeTaskHTTP(t, db, dir, "execute", body, roleRoot, "")
	var status financeRechargeTaskStatus
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil || !status.Committed {
		t.Fatal(w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), dir) {
		t.Fatal("response exposed filesystem path")
	}
}

func TestFinanceRechargeTaskRealSessionMiddleware(t *testing.T) {
	db, dir, confirmation, _ := rechargeTaskFixture(t)
	m := &Monitor{cfg: Settings{SessionSecret: "synthetic-only-task-session"}}
	r := gin.New()
	r.POST("/task", m.requireRole(roleRoot), func(c *gin.Context) { serveFinanceRechargeTask(c, db, dir, "approve") })
	for _, tc := range []struct {
		name, cookie string
		code         int
	}{
		{"anonymous", "", 401},
		{"forged", "not-a-valid-session", 401},
		{"admin", m.signSession("admin", roleAdmin, time.Now().Unix()), 403},
		{"root", m.signSession("root", roleRoot, time.Now().Unix()), 200},
	} {
		req := httptest.NewRequest("POST", "http://localhost/task", strings.NewReader(`{"confirmation":"`+confirmation+`"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		if tc.cookie != "" {
			req.AddCookie(&http.Cookie{Name: sessionCookie, Value: tc.cookie})
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != tc.code {
			t.Fatal(tc.name, w.Code, w.Body.String())
		}
	}
	var approval financeRechargeTaskApproval
	if _, err := giftLocalReadJSON(filepath.Join(dir, "approval.json"), &approval); err != nil || approval.Actor != "root" {
		t.Fatal("approval not bound to signed session", err, approval)
	}
}

func TestFinanceRechargeTaskRestartAndPrivateArtifacts(t *testing.T) {
	db, plan, _, path := guardedRechargeFixture(t)
	dir := filepath.Join(t.TempDir(), "task")
	ctx, now := context.Background(), time.Now().Unix()
	confirmation, err := createFinanceRechargeTask(ctx, db, dir, financeRechargeTaskPlan{Repair: &plan})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := createFinanceRechargeTask(ctx, db, dir, financeRechargeTaskPlan{Repair: &plan}); err == nil {
		t.Fatal("existing task replaced")
	}
	if err := approveFinanceRechargeTask(ctx, db, dir, confirmation, "root", now); err != nil {
		t.Fatal(err)
	}
	if result, err := runFinanceRechargeTask(ctx, db, dir, confirmation, "root", now); err != nil || !result.Committed {
		t.Fatal(result, err)
	}
	before := guardedVersionRows(t, db)
	conn, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, closeDB, err := giftLocalDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	if result, err := runFinanceRechargeTask(ctx, reopened, dir, confirmation, "root", now); err != nil || !result.Committed || !reflect.DeepEqual(before, guardedVersionRows(t, reopened)) {
		t.Fatal("restart replay", result, err)
	}
	for _, name := range []string{"job.lock", "plan.json", "approval.json", "started.json", "receipt.json"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatal(name, err)
		}
	}
}

func TestFinanceRechargeTaskLocksLinksAndCanceledContext(t *testing.T) {
	for _, mode := range []string{"lock", "plan_link", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			db, dir, confirmation, _ := rechargeTaskFixture(t)
			ctx, now := context.Background(), time.Now().Unix()
			if err := approveFinanceRechargeTask(ctx, db, dir, confirmation, "root", now); err != nil {
				t.Fatal(err)
			}
			before := guardedVersionRows(t, db)
			switch mode {
			case "lock":
				lock, err := giftLocalLock(dir)
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Close()
			case "plan_link":
				path := filepath.Join(dir, "plan.json")
				target := filepath.Join(dir, "original-plan.json")
				if err := os.Rename(path, target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if _, err := runFinanceRechargeTask(ctx, db, dir, confirmation, "root", now); err == nil {
				t.Fatal("unsafe execution accepted")
			}
			if !reflect.DeepEqual(before, guardedVersionRows(t, db)) {
				t.Fatal("rejected execution wrote history")
			}
		})
	}
}

func TestFinanceRechargeTaskPreviewAndApprovalRejectStaleEvidence(t *testing.T) {
	db, dir, confirmation, plan := rechargeTaskFixture(t)
	if err := db.Model(&ChannelUpstreamUsageHour{}).Where("domain=?", plan.Request.Corrections[0].Domain).Update("cost_usd", 99).Error; err != nil {
		t.Fatal(err)
	}
	before := guardedVersionRows(t, db)
	body := `{"confirmation":"` + confirmation + `"}`
	for _, action := range []string{"preview", "approve"} {
		w := rechargeTaskHTTP(t, db, dir, action, body, roleRoot, "")
		if w.Code != 409 {
			t.Fatal("stale evidence accepted", action, w.Code, w.Body.String())
		}
	}
	if _, err := os.Lstat(filepath.Join(dir, "approval.json")); !os.IsNotExist(err) {
		t.Fatal("stale task got approval", err)
	}
	if !reflect.DeepEqual(before, guardedVersionRows(t, db)) {
		t.Fatal("stale preview changed history")
	}
}

func TestFinanceRechargeTaskDuplicateDirectoriesCommitOnce(t *testing.T) {
	db, dir, confirmation, plan := rechargeTaskFixture(t)
	ctx, now := context.Background(), time.Now().Unix()
	otherDir := filepath.Join(t.TempDir(), "duplicate-task")
	otherHash, err := createFinanceRechargeTask(ctx, db, otherDir, financeRechargeTaskPlan{Repair: &plan})
	if err != nil || otherHash != confirmation {
		t.Fatal("identical task changed identity", err)
	}
	for _, job := range []string{dir, otherDir} {
		if err := approveFinanceRechargeTask(ctx, db, job, confirmation, "root", now); err != nil {
			t.Fatal(err)
		}
	}
	before := guardedVersionRows(t, db)
	var wg sync.WaitGroup
	errors := make(chan error, 2)
	results := make(chan financeRechargeTaskStatus, 2)
	for _, job := range []string{dir, otherDir} {
		wg.Add(1)
		go func(job string) {
			defer wg.Done()
			result, err := runFinanceRechargeTask(ctx, db, job, confirmation, "root", now)
			results <- result
			errors <- err
		}(job)
	}
	wg.Wait()
	close(errors)
	close(results)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	available := 0
	for result := range results {
		if !result.Committed {
			t.Fatal(result)
		}
		if result.ReceiptAvailable {
			available++
		}
	}
	after := guardedVersionRows(t, db)
	if available != 1 || len(after) != len(before)+len(plan.Rows) || !reflect.DeepEqual(before, after[:len(before)]) {
		t.Fatal("duplicate task duplicated commit or receipt", available)
	}
}

func TestFinanceRechargeTaskFailureDoesNotAutoResume(t *testing.T) {
	db, dir, confirmation, _ := rechargeTaskFixture(t)
	ctx, now := context.Background(), time.Now().Unix()
	if err := approveFinanceRechargeTask(ctx, db, dir, confirmation, "root", now); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TRIGGER reject_task BEFORE INSERT ON channel_finance_versions WHEN NEW.updated_by LIKE 'guarded-recharge:%' BEGIN SELECT RAISE(ABORT,'task injected failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	before := guardedVersionRows(t, db)
	if status, err := runFinanceRechargeTask(ctx, db, dir, confirmation, "root", now); err == nil || status.Committed {
		t.Fatal("failed transaction reported committed", status, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "started.json")); err != nil {
		t.Fatal("missing durable intent", err)
	}
	if err := db.Exec("DROP TRIGGER reject_task").Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		w := rechargeTaskHTTP(t, db, dir, "status", `{"confirmation":"`+confirmation+`"}`, roleRoot, "")
		var status financeRechargeTaskStatus
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil || status.Committed || !reflect.DeepEqual(before, guardedVersionRows(t, db)) {
			t.Fatal("status auto-resumed failed task", w.Code, w.Body.String())
		}
	}
	if status, err := runFinanceRechargeTask(ctx, db, dir, confirmation, "root", now); err != nil || !status.Committed {
		t.Fatal("explicit retry failed", status, err)
	}
}
