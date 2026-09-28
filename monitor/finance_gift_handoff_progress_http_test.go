//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type giftProgressResponse struct {
	ErrorCode           string                     `json:"error_code"`
	AuthorizationStatus string                     `json:"authorization_status"`
	ExecutionEnabled    bool                       `json:"execution_enabled"`
	ExecutionState      string                     `json:"execution_state"`
	Revalidate          bool                       `json:"requires_revalidation"`
	Progress            *financeGiftCommitProgress `json:"progress"`
}

func giftGetProgress(t *testing.T, m *Monitor, r *gin.Engine, id string, wantCode int) giftProgressResponse {
	t.Helper()
	w := giftAuthorizationRequestHTTP(m, r, "GET", giftAuthorizationURL+"/"+id+"/progress", "", "local-root", roleRoot)
	var result giftProgressResponse
	if w.Code != wantCode || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.ExecutionEnabled || !result.Revalidate || result.ExecutionState != "not_observed" || !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
		t.Fatalf("progress response: %d %s", w.Code, w.Body.String())
	}
	return result
}

func TestFinanceGiftProgressLifecycle(t *testing.T) {
	m, r, a := giftAuthorizedLocalFixture(t)
	initial := giftGetProgress(t, m, r, a.TaskID, 200)
	if initial.Progress == nil || initial.Progress.Status != "no_commits_recorded" || initial.Progress.CommittedTargets != 0 || initial.Progress.AuthorizedTargets != 2 {
		t.Fatal(initial)
	}
	ctx, cancel := context.WithCancel(context.Background())
	waits := 0
	_, err := m.runFinanceGiftAuthorizedLocal(ctx, a.TaskID, func(ctx context.Context, _ time.Duration) error {
		waits++
		if waits == 2 {
			cancel()
		}
		return ctx.Err()
	})
	cancel()
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	partial := giftGetProgress(t, m, r, a.TaskID, 200)
	if partial.Progress == nil || partial.Progress.Status != "partial_commits_recorded" || partial.Progress.CommittedTargets != 1 {
		t.Fatal(partial)
	}
	if _, err := m.runFinanceGiftAuthorizedLocal(context.Background(), a.TaskID, handoffNoWait); err != nil {
		t.Fatal(err)
	}
	complete := giftGetProgress(t, m, r, a.TaskID, 200)
	if complete.AuthorizationStatus != "valid" || complete.Progress.Status != "all_commits_recorded" || complete.Progress.CommittedTargets != 2 || complete.Progress.RowsUpdated <= partial.Progress.RowsUpdated {
		t.Fatal(complete)
	}
	// Authorization state is NOT a worker state. Revocation preserves receipts.
	giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL+"/"+a.TaskID+"/revoke", "{}", "local-root", roleRoot))
	revoked := giftGetProgress(t, m, r, a.TaskID, 200)
	if revoked.AuthorizationStatus != "revoked" || *revoked.Progress != *complete.Progress {
		t.Fatal(revoked)
	}
	// Query must neither reread nor hash the source DB, or require it to exist.
	if err := os.Rename(m.cfg.FinanceGiftHandoffPreviewDir, m.cfg.FinanceGiftHandoffPreviewDir+"-unavailable"); err != nil {
		t.Fatal(err)
	}
	before := giftBoundaryFingerprint(t, m.usageFactsDB)
	for i := 0; i < 3; i++ {
		got := giftGetProgress(t, m, r, a.TaskID, 200)
		if *got.Progress != *complete.Progress {
			t.Fatal(got)
		}
	}
	if giftBoundaryFingerprint(t, m.usageFactsDB) != before {
		t.Fatal("progress query changed facts")
	}
}

func TestFinanceGiftProgressUnavailableIsNotZero(t *testing.T) {
	for _, mode := range []string{"missing_ledger", "corrupt_json", "wrong_target", "invalid_hash", "excess_rows", "excess_targets", "receiver_changed", "reader_missing"} {
		t.Run(mode, func(t *testing.T) {
			m, r, a := giftAuthorizedLocalFixture(t)
			if _, err := m.runFinanceGiftAuthorizedLocal(context.Background(), a.TaskID, handoffNoWait); err != nil {
				t.Fatal(err)
			}
			code := 503
			var err error
			switch mode {
			case "missing_ledger":
				err = m.usageFactsDB.Migrator().DropTable(&financeGiftHandoffCommit{})
			case "reader_missing":
				pool, _ := m.financeFactsReadDB.Load().DB()
				pool.Close()
				m.financeFactsReadDB.Store(nil)
			case "receiver_changed":
				m.cfg.UsageFactsStorePath = filepath.Join(t.TempDir(), "other.db")
				code = 409
			case "corrupt_json":
				err = m.usageFactsDB.Model(&financeGiftHandoffCommit{}).Where("job=?", a.TaskID).Update("entry", "bad JSON").Error
			case "excess_targets":
				err = m.usageFactsDB.Create(&financeGiftHandoffCommit{Job: a.TaskID, Index: 99, Entry: "{}"}).Error
			default:
				var row financeGiftHandoffCommit
				if err := m.usageFactsDB.Where("job=? AND \"index\"=0", a.TaskID).First(&row).Error; err != nil {
					t.Fatal(err)
				}
				var entry financeGiftScopeBatchEntry
				if err := json.Unmarshal([]byte(row.Entry), &entry); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "wrong_target":
					entry.UserID++
				case "invalid_hash":
					entry.ContentHash = "invalid"
				case "excess_rows":
					entry.RowsUpdated = a.Authorization.Targets[0].RowsToUpdate + 1
				}
				data, _ := json.Marshal(entry)
				err = m.usageFactsDB.Model(&financeGiftHandoffCommit{}).Where("job=? AND \"index\"=0", a.TaskID).Update("entry", string(data)).Error
			}
			if err != nil {
				t.Fatal(err)
			}
			got := giftGetProgress(t, m, r, a.TaskID, code)
			if got.Progress != nil {
				t.Fatal("unavailable progress exposed misleading counts", got)
			}
			if (got.ErrorCode == "ledger_missing") != (mode == "missing_ledger") {
				t.Fatal("misclassified missing ledger", mode, got.ErrorCode)
			}
		})
	}
}

func TestFinanceGiftProgressFreshMissingLedger(t *testing.T) {
	m, r, a := giftAuthorizedLocalFixture(t)
	if err := m.usageFactsDB.Migrator().DropTable(&financeGiftHandoffCommit{}); err != nil {
		t.Fatal(err)
	}
	got := giftGetProgress(t, m, r, a.TaskID, 503)
	if got.ErrorCode != "ledger_missing" || got.Progress != nil || m.usageFactsDB.Migrator().HasTable(&financeGiftHandoffCommit{}) {
		t.Fatal("read must explain absence without initializing ledger or fabricating zero", got)
	}
	pool, _ := m.financeFactsReadStore().DB()
	pool.Close()
	got = giftGetProgress(t, m, r, a.TaskID, 503)
	if got.ErrorCode == "ledger_missing" {
		t.Fatal("failed catalog read misclassified as missing ledger")
	}
}

func TestFinanceGiftProgressRestartAndExpiry(t *testing.T) {
	m, _, a := giftAuthorizedLocalFixture(t)
	if _, err := m.runFinanceGiftAuthorizedLocal(context.Background(), a.TaskID, handoffNoWait); err != nil {
		t.Fatal(err)
	}
	// Expiry controls future admission, not visibility of previous commits.
	p := a.Authorization
	p.ApprovedAt -= int64(time.Hour / time.Second)
	p.ExpiresAt -= int64(time.Hour / time.Second)
	data, _ := json.Marshal(p)
	if err := m.storeDB.Model(&financeGiftHandoffAuthorization{}).Where("id=?", a.TaskID).Updates(map[string]any{"payload": string(data), "payload_hash": giftLocalDigest(data)}).Error; err != nil {
		t.Fatal(err)
	}
	main, closeMain, err := giftLocalDatabase(m.cfg.StorePath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeMain()
	facts, closeFacts, err := giftLocalDatabase(m.cfg.UsageFactsStorePath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFacts()
	restarted := &Monitor{cfg: m.cfg, storeDB: main, usageFactsDB: facts}
	defer func() {
		if db := restarted.financeFactsReadDB.Load(); db != nil {
			pool, _ := db.DB()
			pool.Close()
		}
	}()
	r := gin.New()
	restarted.RegisterRoutes(r)
	got := giftGetProgress(t, restarted, r, a.TaskID, 200)
	if got.AuthorizationStatus != "expired" || got.Progress == nil || got.Progress.CommittedTargets != 2 {
		t.Fatal(got)
	}
	// A busy isolated reader must respect the caller's shorter deadline.
	reader := restarted.financeFactsReadStore()
	pool, _ := reader.DB()
	conn, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = readFinanceGiftCommitProgress(ctx, reader, a.TaskID, p)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("pool wait escaped budget", err)
	}
}

func TestFinanceGiftProgressAccessAndNoExecutionRoute(t *testing.T) {
	m, r, a := giftAuthorizedLocalFixture(t)
	path := giftAuthorizationURL + "/" + a.TaskID + "/progress"
	for _, role := range []int{-1, 1, roleAdmin} {
		w := giftAuthorizationRequestHTTP(m, r, "GET", path, "", "viewer", role)
		if w.Code != 401 && w.Code != 403 {
			t.Fatal(role, w.Code)
		}
	}
	for _, mode := range []string{"nonlocal", "source_dsn", "source_api", "disabled"} {
		cfg := m.cfg
		switch mode {
		case "nonlocal":
			m.cfg.LocalSnapshotOnly = false
		case "source_dsn":
			m.cfg.ProdDSN = "must-not-connect"
		case "source_api":
			m.cfg.NewAPIBaseURL = "https://must-not-connect.invalid"
		case "disabled":
			m.cfg.FinanceGiftHandoffApprovalEnabled = false
		}
		w := giftAuthorizationRequestHTTP(m, r, "GET", path, "", "local-root", roleRoot)
		m.cfg = cfg
		if w.Code != 404 {
			t.Fatal(mode, w.Code)
		}
	}
	for _, suffix := range []string{"run", "apply"} {
		w := giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL+"/"+a.TaskID+"/"+suffix, "{}", "local-root", roleRoot)
		if w.Code != 404 {
			t.Fatal("unexpected execution endpoint", w.Code)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := readFinanceGiftCommitProgress(ctx, m.financeFactsReadStore(), a.TaskID, a.Authorization); err == nil {
		t.Fatal("ignored cancellation")
	}
}
