//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func giftLiveFixture(t *testing.T) (*Monitor, *gin.Engine, giftAuthorizationResponse) {
	t.Helper()
	m, r, a := giftAuthorizedLocalFixture(t)
	m.cfg.LocalSnapshotOnly = false
	m.cfg.FinanceGiftHandoffLiveExecutionEnabled = true
	// Invalid external configuration must never be used by this local-evidence core.
	m.cfg.ProdDSN, m.cfg.NewAPIBaseURL = "must-not-connect", "https://invalid.example"
	if err := m.usageFactsDB.Migrator().DropTable(&financeGiftHandoffCommit{}); err != nil {
		t.Fatal(err)
	}
	return m, r, a
}

func giftLiveFacts(t *testing.T, m *Monitor) string {
	t.Helper()
	var events []FinanceGiftBoundaryEvent
	var states []FinanceGiftBoundaryState
	if err := m.usageFactsDB.Order("source_epoch,source_log_id").Find(&events).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsDB.Order("source_epoch,hour_ts,user_id").Find(&states).Error; err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal([]any{events, states})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestFinanceGiftLiveScopeAndRestart(t *testing.T) {
	m, _, a := giftLiveFixture(t)
	money := giftMixedMonetarySnapshot(t, m.usageFactsDB)
	source := giftLocalFileHash(t, filepath.Join(m.cfg.FinanceGiftHandoffPreviewDir, "usage-facts.db"))
	result, err := m.runFinanceGiftAuthorizedLive(context.Background(), a.TaskID, handoffNoWait)
	want := 0
	for _, target := range a.Authorization.Targets {
		want += target.RowsToUpdate
	}
	if err != nil || result.Status != "complete" || result.CommittedTargets != 2 || result.RowsUpdated != want {
		t.Fatal(result, err)
	}
	before := giftLiveFacts(t, m)
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
	restarted.financeFactsReadDB.Store(m.financeFactsReadDB.Load())
	repeat, err := restarted.runFinanceGiftAuthorizedLive(context.Background(), a.TaskID, handoffNoWait)
	if err != nil || !reflect.DeepEqual(result, repeat) || before != giftLiveFacts(t, m) {
		t.Fatal("restart changed facts or receipts", repeat, err)
	}
	if money != giftMixedMonetarySnapshot(t, m.usageFactsDB) || source != giftLocalFileHash(t, filepath.Join(m.cfg.FinanceGiftHandoffPreviewDir, "usage-facts.db")) {
		t.Fatal("money or source changed")
	}
}

func TestFinanceGiftLiveRejectsBeforeProvisioning(t *testing.T) {
	for _, mode := range []string{"disabled", "local", "mixed", "approval-off", "bypass", "shutdown", "wrong-receiver", "wrong-source", "expired", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			m, r, a := giftLiveFixture(t)
			before := giftLiveFacts(t, m)
			switch mode {
			case "disabled":
				m.cfg.FinanceGiftHandoffLiveExecutionEnabled = false
			case "local":
				m.cfg.LocalSnapshotOnly = true
			case "mixed":
				m.cfg.FinanceGiftHandoffLocalExecutionEnabled = true
			case "approval-off":
				m.cfg.FinanceGiftHandoffApprovalEnabled = false
			case "bypass":
				m.cfg.LocalAuthBypass = true
			case "shutdown":
				m.shuttingDown.Store(true)
			case "wrong-receiver":
				m.cfg.UsageFactsStorePath = m.cfg.StorePath
			case "wrong-source":
				m.cfg.FinanceGiftHandoffPreviewSHA256 = "invalid"
			case "revoked":
				giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL+"/"+a.TaskID+"/revoke", "{}", "root", roleRoot))
			case "expired":
				p := a.Authorization
				p.ApprovedAt, p.ExpiresAt = time.Now().Unix()-3600, time.Now().Unix()-1800
				b, err := json.Marshal(p)
				if err != nil {
					t.Fatal(err)
				}
				if err := m.storeDB.Model(&financeGiftHandoffAuthorization{}).Where("id=?", a.TaskID).Updates(map[string]any{"payload": string(b), "payload_hash": giftLocalDigest(b)}).Error; err != nil {
					t.Fatal(err)
				}
			}
			if _, err := m.runFinanceGiftAuthorizedLive(context.Background(), a.TaskID, handoffNoWait); err == nil {
				t.Fatal("unsafe execution accepted")
			}
			if before != giftLiveFacts(t, m) || m.usageFactsDB.Migrator().HasTable(&financeGiftHandoffCommit{}) {
				t.Fatal("rejected attempt changed facts or provisioned ledger")
			}
		})
	}
}

func TestFinanceGiftLiveCancelAndExplicitResume(t *testing.T) {
	m, _, a := giftLiveFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waits := 0
	result, err := m.runFinanceGiftAuthorizedLive(ctx, a.TaskID, func(ctx context.Context, _ time.Duration) error {
		waits++
		if waits == 2 {
			cancel()
		}
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) || result.Status != "progress_unavailable" {
		t.Fatal(result, err)
	}
	progress := giftAuthorizedLocalProgress(t, m, a.TaskID)
	if progress.CommittedTargets != 1 || progress.Status != "partial" {
		t.Fatal(progress)
	}
	result, err = m.runFinanceGiftAuthorizedLive(context.Background(), a.TaskID, handoffNoWait)
	if err != nil || result.Status != "complete" || result.CommittedTargets != 2 {
		t.Fatal(result, err)
	}
}

func TestFinanceGiftLiveHTTPDisabledByDefault(t *testing.T) {
	m, r, a := giftLiveFixture(t)
	m.cfg.FinanceGiftHandoffLiveExecutionEnabled = false
	before := giftLiveFacts(t, m)
	for _, request := range []struct{ method, path string }{
		{"GET", "/finance/gift-handoff/local/control"},
		{"GET", "/finance/gift-handoff/live/control"},
		{"GET", "/finance/gift-handoff/live"},
		{"GET", giftAuthorizationURL + "/" + a.TaskID + "/progress"},
		{"POST", giftAuthorizationURL + "/" + a.TaskID + "/start"},
	} {
		w := giftAuthorizationRequestHTTP(m, r, request.method, request.path, "{}", "root", roleRoot)
		if w.Code != 404 {
			t.Fatal(request.path, w.Code, w.Body.String())
		}
	}
	if before != giftLiveFacts(t, m) || m.usageFactsDB.Migrator().HasTable(&financeGiftHandoffCommit{}) {
		t.Fatal("HTTP activated live core")
	}
}
