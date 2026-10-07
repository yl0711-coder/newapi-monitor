package monitor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestUpstreamRecoveryAutomaticProbeDoesNotResetFailureBudget(t *testing.T) {
	for _, fullSuccess := range []bool{false, true} {
		t.Run(fmt.Sprint(fullSuccess), func(t *testing.T) {
			m, row, calls := newUpstreamRecoveryFixture(t)
			row.Status, row.ConsecutiveFails, row.NextSyncAt = upstreamStatusError, upstreamRecoveryProbeThreshold, 0
			if err := m.storeDB.Save(&row).Error; err != nil {
				t.Fatal(err)
			}
			setUpstreamRecoveryTransport(m, upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				n := calls.Add(1)
				if !fullSuccess && n > 2 {
					return recoveryHTTPResponse(503, `{"message":"temporary"}`), nil
				}
				if r.URL.Path == "/api/status" {
					return recoveryHTTPResponse(200, `{"success":true,"data":{"quota_per_unit":500000}}`), nil
				}
				return recoveryHTTPResponse(200, `{"success":true,"data":{"quota":1000000}}`), nil
			}))
			got, err := m.syncStoredUpstreamAccount(context.Background(), row.Domain)
			if fullSuccess {
				if err != nil || got.Status != upstreamStatusOK || got.ConsecutiveFails != 0 || got.BalanceUSD != 2 || calls.Load() != 4 {
					t.Fatalf("full recovery: status=%s fails=%d balance=%v calls=%d err=%v", got.Status, got.ConsecutiveFails, got.BalanceUSD, calls.Load(), err)
				}
				return
			}
			if err == nil || got.ConsecutiveFails != 7 || got.BalanceUSD != row.BalanceUSD || got.LastSuccessAt != row.LastSuccessAt || calls.Load() != 3 {
				t.Fatalf("probe incorrectly published success: fails=%d calls=%d err=%v", got.ConsecutiveFails, calls.Load(), err)
			}
			for count := 8; count <= 9; count++ {
				if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", row.Domain).UpdateColumn("next_sync_at", 0).Error; err != nil {
					t.Fatal(err)
				}
				before := calls.Load()
				got, err = m.syncStoredUpstreamAccount(context.Background(), row.Domain)
				if err == nil || got.ConsecutiveFails != count || calls.Load() != before+1 {
					t.Fatalf("failed probe fell through to bulk sync: fails=%d calls=%d err=%v", got.ConsecutiveFails, calls.Load()-before, err)
				}
			}
			restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, upstreamClient: m.upstreamClient}
			before := calls.Load()
			if _, err := restarted.syncStoredUpstreamAccount(context.Background(), row.Domain); err == nil || calls.Load() != before {
				t.Fatal("exhausted retry budget lost on restart")
			}
		})
	}
}

func TestUpstreamRecoveryDeadlinePersistsScheduleWithoutHTTP(t *testing.T) {
	m, row, calls := newUpstreamRecoveryFixture(t)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	now := time.Now().Unix()
	funds := UpstreamFundSyncState{Domain: row.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(row), TailSyncedUntil: 123}
	if err := m.failFundState(ctx, &funds, now, context.DeadlineExceeded); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	logs := UpstreamErrorLogSyncState{Domain: row.Domain, SyncedUntil: 123}
	if err := m.failUpstreamErrorLogState(ctx, &logs, now, context.DeadlineExceeded); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	for _, task := range []string{"funds", "error_logs"} {
		state, err := m.loadUpstreamRecoveryTarget(context.Background(), row, task)
		if err != nil || state.next <= now || state.next == upstreamAccountIsolatedUntil || state.failures != 1 {
			t.Fatalf("%s retry schedule lost after deadline: %+v %v", task, state, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("local finalization requested upstream")
	}
}

func TestUpstreamRecoveryRejectsMalformedSuccessAndRequiresRoot(t *testing.T) {
	m, row, calls := newUpstreamRecoveryFixture(t)
	setUpstreamRecoveryTransport(m, upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return recoveryHTTPResponse(200, `<html>please sign in</html>`), nil
	}))
	router := gin.New()
	router.POST("/channels/upstream/recover", m.requireRole(roleRoot), m.recoverChannelUpstreamHandler)
	body := map[string]string{"domain": row.Domain, "task": "usage"}
	if w := upstreamRouteRequest(t, m, router, roleAdmin, http.MethodPost, "/channels/upstream/recover", body); w.Code != 403 || calls.Load() != 0 {
		t.Fatal("non-root recovery contacted upstream")
	}
	if w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream/recover", body); w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"blocked"`) || calls.Load() != 1 {
		t.Fatalf("malformed HTTP 200 passed recovery: %s", w.Body.String())
	}
	var got ChannelUpstreamAccount
	if err := m.storeDB.First(&got, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if got.UsageNextSyncAt != upstreamAccountIsolatedUntil || got.UsageDataUntil != row.UsageDataUntil {
		t.Fatal("invalid probe modified data")
	}
	body["task"] = "not-a-task"
	if w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream/recover", body); w.Code != 409 || calls.Load() != 1 {
		t.Fatal("unknown task issued a request")
	}
	r := httptest.NewRequest(http.MethodPost, "/channels/upstream/recover", strings.NewReader(`{"domain":"`+strings.Repeat("x", 5000)+`","task":"usage"}`))
	r.Header.Set("Content-Type", "application/json")
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("tester", roleRoot, time.Now().Unix())})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	if w.Code != 400 || calls.Load() != 1 {
		t.Fatal("oversized input was accepted")
	}
}

func TestUpstreamRecoverySpringKeepsOtherKeysAndFrozenProtocol(t *testing.T) {
	m, row, calls := newUpstreamRecoveryFixture(t)
	m.cfg.UpstreamAICodeWithRecordsEnabled = true
	row.Provider, row.BalanceUnit = upstreamProviderAICodeWith, 1
	cred, err := normalizeAICodeWithCredential(aiCodeWithCredential{APIKeys: []string{"sk-acw-fixture-key-one", "sk-acw-fixture-key-two"}})
	if err != nil {
		t.Fatal(err)
	}
	version, err := aiCodeWithCredentialSetVersion(cred)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.sealUpstreamAccountCredential(&row, cred); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Save(&row).Error; err != nil {
		t.Fatal(err)
	}
	var original []AICodeWithKeySyncState
	for i, slot := range cred.Slots {
		state := AICodeWithKeySyncState{Domain: row.Domain, SlotID: slot.SlotID, CredentialSetVersion: version, Ordinal: i + 1, Status: upstreamStatusReconnect, LastAttemptAt: time.Now().Unix() - 7200, NextSyncAt: upstreamAccountIsolatedUntil, BackfillNextSyncAt: upstreamAccountIsolatedUntil, ConsecutiveFails: 2, BackfillConsecutiveFails: 2, TailRoundID: "old-round", BackfillCursor: 123}
		if err := m.storeDB.Create(&state).Error; err != nil {
			t.Fatal(err)
		}
		original = append(original, state)
	}
	round := AICodeWithUsageRound{Domain: row.Domain, Kind: "tail", Status: upstreamStatusPending, RecordMode: false, CredentialSetVersion: version, RoundID: "frozen-daily", TotalKeys: 2}
	if err := m.storeDB.Create(&round).Error; err != nil {
		t.Fatal(err)
	}
	setUpstreamRecoveryTransport(m, upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if r.URL.Query().Get("group_by") != "day" || r.Header.Get("Authorization") != "Bearer "+cred.Slots[0].Secret {
			t.Fatal("recovery used wrong key or changed frozen protocol")
		}
		body := fmt.Sprintf(`{"data":{"api_key_id":1,"group_by":"day","period":{"start":%q,"end":%q},"summary":{"cost":0,"total_tokens":0,"requests":0},"daily":[]}}`, r.URL.Query().Get("start"), r.URL.Query().Get("end"))
		return recoveryHTTPResponse(200, body), nil
	}))
	if result, err := m.recoverUpstreamTask(context.Background(), row.Domain, "usage_history"); err != nil || result.Status != "blocked" || calls.Load() != 0 {
		t.Fatalf("history bypassed key authentication pause: %+v %v", result, err)
	}
	if result, err := m.recoverUpstreamTask(context.Background(), row.Domain, "usage"); err != nil || result.Status != "queued" || calls.Load() != 1 {
		t.Fatalf("key recovery failed: %+v %v", result, err)
	}
	var states []AICodeWithKeySyncState
	if err := m.storeDB.Where("domain = ?", row.Domain).Order("ordinal ASC").Find(&states).Error; err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 || states[1] != original[1] {
		t.Fatal("unverified key state was changed")
	}
	if states[0].NextSyncAt == upstreamAccountIsolatedUntil || states[0].BackfillNextSyncAt == upstreamAccountIsolatedUntil || states[0].ConsecutiveFails != 2 || states[0].TailRoundID != "old-round" || states[0].BackfillCursor != 123 {
		t.Fatal("verified key lost checkpoint or remained isolated")
	}
}

func TestUpstreamRecoveryDiagnosticWrapped403IsNotExpiredCredential(t *testing.T) {
	if got := diagnosticFailure("test", &upstreamAuthError{err: &upstreamHTTPError{Status: 403}}); got.Code != "forbidden" {
		t.Fatalf("misleading auth advice: %+v", got)
	}
	if got := diagnosticFailure("test", &upstreamAuthError{err: &upstreamHTTPError{Status: 503, Message: "refresh_token unavailable"}}); got.Code != "upstream_error" {
		t.Fatalf("outage mislabeled as invalid credential: %+v", got)
	}
}
