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
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func rechargeMaintenanceFixture(t *testing.T) (*Monitor, *gin.Engine, financeRechargeGuardPlan) {
	t.Helper()
	db, plan, _, path := guardedRechargeFixture(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	proposal := financeRechargeMaintenanceProposal{Version: 1, Repair: &plan.Request}
	b, err := json.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	if err := giftLocalWriteNew(filepath.Join(dir, "proposal.json"), b); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db, cfg: Settings{StorePath: path, FinanceEnabled: true, FinanceRechargeMaintenanceEnabled: true, FinanceRechargeTaskDir: dir, FinanceRechargeProposalSHA256: giftLocalDigest(b), SessionSecret: "synthetic-maintenance-session"}}
	if err := validateFinanceRechargeMaintenance(m.cfg); err != nil {
		info, _ := os.Lstat(dir)
		t.Fatalf("fixture validation: %v; directory=%v", err, info.Mode())
	}
	r := gin.New()
	m.RegisterRoutes(r)
	return m, r, plan
}

func rechargeMaintenanceHTTP(m *Monitor, r *gin.Engine, path, confirmation string, role int) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "http://localhost"+path, strings.NewReader(`{"confirmation":"`+confirmation+`"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if role > 0 {
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("test-root", role, time.Now().Unix())})
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func prepareRechargeMaintenanceHTTP(t *testing.T, m *Monitor, r *gin.Engine) string {
	t.Helper()
	w := rechargeMaintenanceHTTP(m, r, "/finance/recharge-maintenance/prepare", m.cfg.FinanceRechargeProposalSHA256, roleRoot)
	var response struct {
		TaskID  string `json:"task_id"`
		Started bool   `json:"execution_started"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil || !rechargeTaskDigestValid(response.TaskID) || response.Started {
		t.Fatal(w.Code, w.Body.String())
	}
	return response.TaskID
}

func TestFinanceRechargeMaintenanceDefaultOffAndConfiguration(t *testing.T) {
	for _, key := range []string{"MONITOR_FINANCE_RECHARGE_MAINTENANCE_ENABLED", "MONITOR_FINANCE_RECHARGE_EXECUTION_ENABLED", "MONITOR_FINANCE_RECHARGE_TASK_DIR", "MONITOR_FINANCE_RECHARGE_PROPOSAL_SHA256"} {
		t.Setenv(key, "")
	}
	s := LoadSettings()
	if s.FinanceRechargeMaintenanceEnabled || s.FinanceRechargeExecutionEnabled || s.FinanceRechargeTaskDir != "" || s.FinanceRechargeProposalSHA256 != "" {
		t.Fatal("maintenance not disabled by default")
	}
	m, _, _ := rechargeMaintenanceFixture(t)
	m.cfg.FinanceRechargeMaintenanceEnabled = false
	m.cfg.FinanceRechargeExecutionEnabled = true
	r := gin.New()
	m.RegisterRoutes(r)
	if w := rechargeMaintenanceHTTP(m, r, "/finance/recharge-maintenance/prepare", m.cfg.FinanceRechargeProposalSHA256, roleRoot); w.Code != 404 {
		t.Fatal("write flag exposed disabled maintenance", w.Code)
	}
	if _, err := os.Lstat(filepath.Join(m.cfg.FinanceRechargeTaskDir, "job.lock")); !os.IsNotExist(err) {
		t.Fatal("route registration started work")
	}
	for _, mode := range []string{"bypass", "no_secret", "public_directory", "relative_directory", "bad_digest", "finance_disabled"} {
		t.Run(mode, func(t *testing.T) {
			m, r, _ := rechargeMaintenanceFixture(t)
			switch mode {
			case "bypass":
				m.cfg.LocalAuthBypass = true
			case "no_secret":
				m.cfg.SessionSecret = ""
			case "public_directory":
				if err := os.Chmod(m.cfg.FinanceRechargeTaskDir, 0755); err != nil {
					t.Fatal(err)
				}
			case "relative_directory":
				m.cfg.FinanceRechargeTaskDir = "relative"
			case "bad_digest":
				m.cfg.FinanceRechargeProposalSHA256 = "bad"
			case "finance_disabled":
				m.cfg.FinanceEnabled = false
			}
			if w := rechargeMaintenanceHTTP(m, r, "/finance/recharge-maintenance/prepare", m.cfg.FinanceRechargeProposalSHA256, roleRoot); w.Code != 503 {
				t.Fatal(w.Code, w.Body.String())
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest("GET", "/live", nil))
			if w.Code != 200 {
				t.Fatal("maintenance config broke liveness", w.Code)
			}
		})
	}
}

func TestFinanceRechargeMaintenancePreparationExecutionAndStop(t *testing.T) {
	m, r, _ := rechargeMaintenanceFixture(t)
	before := guardedVersionRows(t, m.storeDB)
	for _, role := range []int{0, roleAdmin} {
		if w := rechargeMaintenanceHTTP(m, r, "/finance/recharge-maintenance/prepare", m.cfg.FinanceRechargeProposalSHA256, role); w.Code != 401 && w.Code != 403 {
			t.Fatal("unprivileged prepare", w.Code)
		}
	}
	id := prepareRechargeMaintenanceHTTP(t, m, r)
	if again := prepareRechargeMaintenanceHTTP(t, m, r); again != id {
		t.Fatal("prepare created a replacement task")
	}
	if !reflect.DeepEqual(before, guardedVersionRows(t, m.storeDB)) {
		t.Fatal("prepare wrote financial history")
	}
	base := "/finance/recharge-maintenance/tasks/" + id + "/"
	for _, action := range []string{"preview", "approve"} {
		if w := rechargeMaintenanceHTTP(m, r, base+action, id, roleRoot); w.Code != 200 {
			t.Fatal(action, w.Code, w.Body.String())
		}
	}
	if w := rechargeMaintenanceHTTP(m, r, base+"execute", id, roleRoot); w.Code != 403 {
		t.Fatal("default execution gate open", w.Code)
	}
	m.cfg.FinanceRechargeExecutionEnabled = true
	if w := rechargeMaintenanceHTTP(m, r, base+"execute", id, roleRoot); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	after := guardedVersionRows(t, m.storeDB)
	if len(after) <= len(before) {
		t.Fatal("explicit authorized task did not execute")
	}
	// Turning off execution leaves read-only status and does not auto-undo data.
	m.cfg.FinanceRechargeExecutionEnabled = false
	if w := rechargeMaintenanceHTTP(m, r, base+"status", id, roleRoot); w.Code != 200 || !strings.Contains(w.Body.String(), `"committed":true`) {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := rechargeMaintenanceHTTP(m, r, base+"execute", id, roleRoot); w.Code != 403 {
		t.Fatal("write gate ignored")
	}
	if !reflect.DeepEqual(after, guardedVersionRows(t, m.storeDB)) {
		t.Fatal("disable altered committed data")
	}
}

func TestFinanceRechargeMaintenanceChangedProposalAllowsStatusStopOnly(t *testing.T) {
	m, r, _ := rechargeMaintenanceFixture(t)
	id := prepareRechargeMaintenanceHTTP(t, m, r)
	base := "/finance/recharge-maintenance/tasks/" + id + "/"
	if err := os.WriteFile(filepath.Join(m.cfg.FinanceRechargeTaskDir, "proposal.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	m.cfg.FinanceRechargeExecutionEnabled = true
	for _, action := range []string{"preview", "approve", "execute"} {
		if w := rechargeMaintenanceHTTP(m, r, base+action, id, roleRoot); w.Code != 409 {
			t.Fatal(action, w.Code)
		}
	}
	for _, action := range []string{"status", "stop"} {
		if w := rechargeMaintenanceHTTP(m, r, base+action, id, roleRoot); w.Code != 200 {
			t.Fatal(action, w.Code, w.Body.String())
		}
	}
}

func TestFinanceRechargeMaintenanceRejectsClientSelectedScope(t *testing.T) {
	m, r, _ := rechargeMaintenanceFixture(t)
	before := guardedVersionRows(t, m.storeDB)
	path := "/finance/recharge-maintenance/prepare"
	body := `{"confirmation":"` + m.cfg.FinanceRechargeProposalSHA256 + `"}`
	for _, tc := range []struct {
		name, path, body, origin, fetchSite string
		status                              int
	}{
		{"query", path + "?path=/tmp/other", body, "", "", 400},
		{"scope", path, `{"confirmation":"` + m.cfg.FinanceRechargeProposalSHA256 + `","domain":"other.example"}`, "", "", 400},
		{"bad_confirmation", path, `{"confirmation":"` + strings.Repeat("0", 64) + `"}`, "", "", 409},
		{"cross_origin", path, body, "https://other.example", "", 403},
		{"cross_site", path, body, "", "cross-site", 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "http://localhost"+tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json")
			req.Header.Set("Origin", tc.origin)
			req.Header.Set("Sec-Fetch-Site", tc.fetchSite)
			req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("test-root", roleRoot, time.Now().Unix())})
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatal(w.Code, w.Body.String())
			}
			if _, err := os.Lstat(filepath.Join(m.cfg.FinanceRechargeTaskDir, "prepared.json")); !os.IsNotExist(err) {
				t.Fatal("rejected request prepared a task")
			}
		})
	}
	id := prepareRechargeMaintenanceHTTP(t, m, r)
	m.cfg.FinanceRechargeExecutionEnabled = true
	wrong := strings.Repeat("0", 64)
	if w := rechargeMaintenanceHTTP(m, r, "/finance/recharge-maintenance/tasks/"+wrong+"/execute", wrong, roleRoot); w.Code != 409 {
		t.Fatal("accepted task outside configured proposal", w.Code)
	}
	if w := rechargeMaintenanceHTTP(m, r, "/finance/recharge-maintenance/tasks/"+id+"/approve", wrong, roleRoot); w.Code != 409 {
		t.Fatal("accepted mismatched confirmation", w.Code)
	}
	if !reflect.DeepEqual(before, guardedVersionRows(t, m.storeDB)) {
		t.Fatal("invalid requests changed money history")
	}
}

func TestFinanceRechargeMaintenanceRecoveryNeedsNewProposal(t *testing.T) {
	m, r, _ := rechargeMaintenanceFixture(t)
	id := prepareRechargeMaintenanceHTTP(t, m, r)
	plan, err := loadFinanceRechargeTask(context.Background(), m.storeDB, filepath.Join(m.cfg.FinanceRechargeTaskDir, id), id)
	if err != nil {
		t.Fatal(err)
	}
	base := "/finance/recharge-maintenance/tasks/" + id + "/"
	m.cfg.FinanceRechargeExecutionEnabled = true
	for _, action := range []string{"approve", "execute"} {
		if w := rechargeMaintenanceHTTP(m, r, base+action, id, roleRoot); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	hash, _ := financeRechargeGuardHash(*plan.Repair)
	proposal := financeRechargeMaintenanceProposal{Version: 1, RecoverOriginal: plan.Repair, OriginalSHA256: hash}
	b, err := json.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := giftLocalWriteNew(filepath.Join(dir, "proposal.json"), b); err != nil {
		t.Fatal(err)
	}
	m.cfg.FinanceRechargeTaskDir, m.cfg.FinanceRechargeProposalSHA256 = dir, giftLocalDigest(b)
	recoveryID := prepareRechargeMaintenanceHTTP(t, m, r)
	base = "/finance/recharge-maintenance/tasks/" + recoveryID + "/"
	if w := rechargeMaintenanceHTTP(m, r, base+"execute", recoveryID, roleRoot); w.Code != 409 {
		t.Fatal("recovery reused old approval", w.Code)
	}
	for _, action := range []string{"preview", "approve", "execute"} {
		if w := rechargeMaintenanceHTTP(m, r, base+action, recoveryID, roleRoot); w.Code != 200 {
			t.Fatal(action, w.Code, w.Body.String())
		}
	}
}

func TestFinanceRechargeMaintenanceApplicationFixture(t *testing.T) {
	root := os.Getenv("MONITOR_RECHARGE_APP_FIXTURE_DIR")
	if root == "" {
		t.Skip("explicit private application acceptance output required")
	}
	source, request := rechargeRehearsalFixture(t)
	request.SourceSHA256 = ""
	request.RebuildEconomics = true
	for _, name := range []string{"data", "task"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	hash, err := giftLocalCopyBackup(source, filepath.Join(root, "data", "monitor.db"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(financeRechargeMaintenanceProposal{Version: 1, Repair: &request})
	if err != nil {
		t.Fatal(err)
	}
	if err := giftLocalWriteNew(filepath.Join(root, "task", "proposal.json"), b); err != nil {
		t.Fatal(err)
	}
	if err := writeFinanceRechargeTaskJSON(filepath.Join(root, "fixture.json"), struct{ SourceSHA256, ProposalSHA256 string }{hash, giftLocalDigest(b)}); err != nil {
		t.Fatal(err)
	}
	t.Log("synthetic closed SQLite and fixed proposal exported; no external source")
}
