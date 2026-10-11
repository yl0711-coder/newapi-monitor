package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestUpstreamRecoveryStatusReadOnlyAndCapabilities(t *testing.T) {
	m, row, calls := newUpstreamRecoveryFixture(t)
	before, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	views, err := m.upstreamRecoveryStatus(context.Background(), row, time.Now().Unix())
	if err != nil || len(views) != 6 {
		t.Fatalf("%+v %v", views, err)
	}
	for _, v := range views {
		if v.Task == "usage_history" {
			if v.CanRecover || v.State != "blocked" {
				t.Fatalf("history before tail: %+v", v)
			}
		} else if !v.CanRecover {
			t.Fatalf("missing recovery: %+v", v)
		}
	}
	m.cfg.UpstreamFundsDomains = []string{"other.example"}
	m.cfg.UpstreamPricingLedgerEnabled = false
	m.cfg.UpstreamSyncEnabled = false
	views, err = m.upstreamRecoveryStatus(context.Background(), row, time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 3, 4} {
		if views[i].State != "disabled" || views[i].CanRecover {
			t.Fatalf("feature gating: %+v", views[i])
		}
	}
	var after ChannelUpstreamAccount
	if err := m.storeDB.First(&after, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(after)
	if string(encoded) != string(before) || calls.Load() != 0 {
		t.Fatal("status read mutated account or contacted upstream")
	}
}

func TestUpstreamRecoveryStatusProjection(t *testing.T) {
	const now int64 = 10000
	for _, tc := range []struct {
		name    string
		target  upstreamRecoveryTarget
		state   string
		recover bool
		retry   int64
	}{
		{"empty", upstreamRecoveryTarget{}, "pending", false, 0},
		{"published", upstreamRecoveryTarget{status: upstreamStatusOK, last: 9000, success: 9010}, "synced", false, 0},
		{"new attempt", upstreamRecoveryTarget{status: upstreamStatusOK, last: 9900, success: 9010}, "pending", false, 0},
		{"probe only", upstreamRecoveryTarget{status: upstreamStatusPending, last: 9000, success: 9000}, "pending", false, 0},
		{"read not verified", upstreamRecoveryTarget{status: "pending_verification", last: 9000, success: 9000}, "verifying", false, 0},
		{"reconcile blocked", upstreamRecoveryTarget{status: "reconcile_blocked", last: 9000, success: 9000}, "mismatch", false, 0},
		{"backfilling", upstreamRecoveryTarget{status: "backfilling", last: 9000, success: 9000}, "collecting", false, 0},
		{"isolated", upstreamRecoveryTarget{status: upstreamStatusReconnect, next: upstreamAccountIsolatedUntil, last: 8000}, "failed", true, 0},
		{"cooldown", upstreamRecoveryTarget{status: upstreamStatusError, last: 9900}, "cooling", false, 10200},
		{"retry after", upstreamRecoveryTarget{status: upstreamStatusError, last: 9900, next: 12000}, "cooling", false, 12000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var v upstreamRecoveryTaskView
			projectUpstreamRecoveryStatus(&v, tc.target, false, now)
			if v.State != tc.state || v.CanRecover != tc.recover || v.RetryAt != tc.retry {
				t.Fatalf("%+v", v)
			}
		})
	}
}

func TestUpstreamRecoveryStatusExplainsRetryVersusPause(t *testing.T) {
	const now int64 = 10000
	for _, tc := range []struct {
		name   string
		target upstreamRecoveryTarget
		note   string
	}{
		{"scheduled", upstreamRecoveryTarget{status: upstreamStatusError, last: 8000, next: 12000, failures: 2}, "已安排自动重试"},
		{"probe", upstreamRecoveryTarget{status: upstreamStatusError, last: 8000, next: 12000, failures: upstreamRecoveryProbeThreshold}, "等待低频检测"},
		{"paused", upstreamRecoveryTarget{status: upstreamStatusError, last: 8000, next: upstreamAccountIsolatedUntil, failures: upstreamRecoveryFailureLimit}, "已达到自动重试上限"},
		{"pause cooldown", upstreamRecoveryTarget{status: upstreamStatusError, last: 9900, next: upstreamAccountIsolatedUntil, failures: upstreamRecoveryFailureLimit}, "自动同步已暂停"},
		{"invalid endpoint", upstreamRecoveryTarget{status: upstreamStatusError, last: 8000, next: upstreamAccountIsolatedUntil, failures: 1}, "自动重试已暂停"},
		{"due", upstreamRecoveryTarget{status: upstreamStatusError, last: 8000, next: 9000, failures: 2}, "等待调度"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var view upstreamRecoveryTaskView
			projectUpstreamRecoveryStatus(&view, tc.target, false, now)
			if !strings.Contains(view.Note, tc.note) {
				t.Fatalf("%+v", view)
			}
			if view.RetryAt >= upstreamAccountIsolatedUntil {
				t.Fatal("pause sentinel exposed as a retry date")
			}
		})
	}
}

func TestUpstreamRecoveryStatusMissingStateAndReadFailure(t *testing.T) {
	m, row, _ := newUpstreamRecoveryFixture(t)
	if err := m.storeDB.Where("domain = ?", row.Domain).Delete(&UpstreamFundSyncState{}).Error; err != nil {
		t.Fatal(err)
	}
	views, err := m.upstreamRecoveryStatus(context.Background(), row, time.Now().Unix())
	if err != nil || views[3].State != "pending" || views[3].CanRecover {
		t.Fatalf("%+v %v", views, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.upstreamRecoveryStatus(ctx, row, time.Now().Unix()); err == nil {
		t.Fatal("DB read failure must not look healthy")
	}
	m.cfg.LocalSnapshotOnly = true
	views, err = m.upstreamRecoveryStatus(context.Background(), row, time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range views {
		if v.State != "disabled" || v.CanRecover {
			t.Fatalf("offline action: %+v", v)
		}
	}
}

func TestUpstreamRecoveryStatusHistoryDoneDoesNotHideFailure(t *testing.T) {
	v := upstreamRecoveryTaskView{Task: "usage_history"}
	projectUpstreamRecoveryStatus(&v, upstreamRecoveryTarget{done: true}, false, 10000)
	if v.State != "complete" || v.CanRecover {
		t.Fatalf("%+v", v)
	}
	v = upstreamRecoveryTaskView{Task: "usage_history"}
	projectUpstreamRecoveryStatus(&v, upstreamRecoveryTarget{done: true, errorText: "failed", last: 1}, false, 10000)
	if v.State != "failed" || !v.CanRecover {
		t.Fatalf("old completion hid failure: %+v", v)
	}
}

func TestUpstreamRecoveryStatusHandlerNoCredentialLeak(t *testing.T) {
	m, row, calls := newUpstreamRecoveryFixture(t)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/channels/upstream/recovery-status?domain="+row.Domain, nil)
	m.upstreamRecoveryStatusHandler(c)
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	for _, secret := range []string{"fixture-access", row.Credential, "HTTP 403"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatal("raw credential/error leaked")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("read issued upstream request")
	}
}

func TestUpstreamRecoveryStatusRequiresRoot(t *testing.T) {
	m, row, calls := newUpstreamRecoveryFixture(t)
	router := gin.New()
	router.GET("/channels/upstream/recovery-status", m.requireRole(roleRoot), m.upstreamRecoveryStatusHandler)
	url := "/channels/upstream/recovery-status?domain=" + row.Domain
	for _, role := range []int{roleAdmin, roleRoot} {
		w := upstreamRouteRequest(t, m, router, role, http.MethodGet, url, nil)
		want := http.StatusForbidden
		if role == roleRoot {
			want = http.StatusOK
		}
		if w.Code != want {
			t.Fatalf("role %d got %d: %s", role, w.Code, w.Body.String())
		}
	}
	if calls.Load() != 0 {
		t.Fatal("status route contacted upstream")
	}
}

func TestUpstreamRecoveryStatusSpringUsesCurrentKeysAndPublishedSuccess(t *testing.T) {
	m, row, calls := newUpstreamRecoveryFixture(t)
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
	now := time.Now().Unix()
	row.UsageStatus = upstreamStatusPending
	for i, slot := range cred.Slots {
		state := AICodeWithKeySyncState{Domain: row.Domain, SlotID: slot.SlotID, CredentialSetVersion: version, Ordinal: i + 1, Status: upstreamStatusOK, LastAttemptAt: now - 1, LastSuccessAt: now}
		if i == 1 {
			state.Status, state.NextSyncAt = upstreamStatusReconnect, upstreamAccountIsolatedUntil
		}
		if err := m.storeDB.Create(&state).Error; err != nil {
			t.Fatal(err)
		}
	}
	old := AICodeWithKeySyncState{Domain: row.Domain, SlotID: "removed", CredentialSetVersion: "old", Status: upstreamStatusReconnect, NextSyncAt: upstreamAccountIsolatedUntil}
	if err := m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	views, err := m.upstreamRecoveryStatus(context.Background(), row, now)
	if err != nil {
		t.Fatal(err)
	}
	if views[1].State != "cooling" || views[1].CanRecover || views[1].LastSuccessAt != row.UsageLastSuccessAt {
		t.Fatalf("key success published early: %+v", views[1])
	}
	views, err = m.upstreamRecoveryStatus(context.Background(), row, now+600)
	if err != nil || !views[1].CanRecover {
		t.Fatalf("current failed key unavailable: %+v %v", views, err)
	}
	if err := m.storeDB.Model(&AICodeWithKeySyncState{}).Where("domain = ? AND credential_set_version = ?", row.Domain, version).Updates(map[string]any{"status": upstreamStatusOK, "next_sync_at": 0}).Error; err != nil {
		t.Fatal(err)
	}
	views, err = m.upstreamRecoveryStatus(context.Background(), row, now+600)
	if err != nil || views[1].CanRecover || views[1].State != "pending" {
		t.Fatalf("removed key or unpublished round leaked: %+v %v", views, err)
	}
	row.UsageStatus = upstreamStatusOK
	row.UsageLastSuccessAt = now
	row.UsageNextSyncAt = now + 1800
	views, err = m.upstreamRecoveryStatus(context.Background(), row, now+600)
	if err != nil || views[1].State != "synced" {
		t.Fatalf("published round hidden: %+v %v", views, err)
	}
	if calls.Load() != 0 {
		t.Fatal("projection contacted upstream")
	}
}
