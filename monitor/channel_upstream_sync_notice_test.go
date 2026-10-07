package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type usageSyncNoticeResponse struct {
	Account     ChannelUpstreamAccountView `json:"account"`
	SyncSkipped bool                       `json:"sync_skipped"`
	SyncWarning bool                       `json:"sync_warning"`
	SyncError   string                     `json:"sync_error"`
	RetryAt     int64                      `json:"retry_at"`
}

func runUsageSyncNoticeRequest(t *testing.T, m *Monitor, domain string) usageSyncNoticeResponse {
	t.Helper()
	router := gin.New()
	router.POST("/sync", m.syncChannelUpstreamUsageHandler)
	body, err := json.Marshal(map[string]string{"domain": domain})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/sync", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("sync status=%d body=%s", w.Code, w.Body.String())
	}
	var result usageSyncNoticeResponse
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestManualUsageSyncDeferredDoesNotClaimSuccessOrTouchState(t *testing.T) {
	now := time.Now().Unix()
	for _, tc := range []struct {
		name, state, lastError, message string
		next, retryAt                   int64
		warning                         bool
	}{
		{"auth_403", upstreamStatusReconnect, "上游返回 HTTP 403", "认证或权限异常", upstreamAccountIsolatedUntil, 0, true},
		{"isolated_sentinel", upstreamStatusOK, "", "认证或权限异常", upstreamAccountIsolatedUntil, 0, true},
		{"retry_429", upstreamStatusError, "上游返回 HTTP 429", "等待重试时间", now + 3600, now + 3600, true},
		{"normal_interval", upstreamStatusOK, "", "尚未到下次同步时间", now + 1800, now + 1800, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				http.Error(w, "unexpected upstream request", http.StatusInternalServerError)
			}))
			defer server.Close()
			m := newChannelUpstreamTestMonitor(t)
			m.cfg.UpstreamUsageSyncEnabled = true
			row := ChannelUpstreamAccount{
				Domain: "deferred.example", Provider: upstreamProviderNewAPI, BaseURL: server.URL,
				UserID: 31, Enabled: true, UsageSyncEnabled: true,
				UsageStatus: tc.state, UsageLastError: tc.lastError, UsageNextSyncAt: tc.next,
				UsageDataUntil: now - 9*86400, UsageLastSuccessAt: now - 9*86400,
				UsageLastAttemptAt: now - 8*86400, UsageConsecutiveFails: 2,
				UsageBackfillDone: true, UsageBackfillCursor: cstDayStart(now) - 9*86400,
			}
			if err := m.persistSyncedUpstreamAccount(context.Background(), &row, newAPICredential{AccessToken: "usage-token"}); err != nil {
				t.Fatal(err)
			}
			var before ChannelUpstreamAccount
			if err := m.storeDB.First(&before, "domain = ?", row.Domain).Error; err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				result := runUsageSyncNoticeRequest(t, m, row.Domain)
				if !result.SyncSkipped || result.SyncWarning != tc.warning || !strings.Contains(result.SyncError, tc.message) || result.RetryAt != tc.retryAt {
					t.Fatalf("incorrect deferred result: skipped=%v warning=%v message=%q retry=%d", result.SyncSkipped, result.SyncWarning, result.SyncError, result.RetryAt)
				}
				if result.Account.UsageDataUntil != before.UsageDataUntil || result.Account.UsageLastAttemptAt != before.UsageLastAttemptAt {
					t.Fatal("manual no-op claimed a new attempt or watermark")
				}
			}
			var after ChannelUpstreamAccount
			if err := m.storeDB.First(&after, "domain = ?", row.Domain).Error; err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 0 || !reflect.DeepEqual(before, after) {
				t.Fatalf("deferred check changed persisted account or made requests: calls=%d", calls.Load())
			}
		})
	}
}

func TestManualUsageSyncActual403RemainsFailure(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, `{"message":"forbidden"}`, http.StatusForbidden)
	}))
	defer server.Close()
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamUsageSyncEnabled = true
	row := ChannelUpstreamAccount{
		Domain: "actual-error.example", Provider: upstreamProviderNewAPI, BaseURL: server.URL,
		UserID: 31, Enabled: true, UsageSyncEnabled: true, UsageBackfillDone: true,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, newAPICredential{AccessToken: "usage-token"}); err != nil {
		t.Fatal(err)
	}
	result := runUsageSyncNoticeRequest(t, m, row.Domain)
	if result.SyncSkipped || !strings.Contains(result.SyncError, "403") || result.Account.UsageStatus != upstreamStatusError || calls.Load() != 1 {
		t.Fatalf("actual upstream failure was misclassified: skipped=%v error=%q state=%q calls=%d", result.SyncSkipped, result.SyncError, result.Account.UsageStatus, calls.Load())
	}
}

func TestManualUsageSyncDisabledAccountCannotReturnEmptySuccess(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamUsageSyncEnabled = true
	row := ChannelUpstreamAccount{Domain: "disabled.example", Provider: upstreamProviderNewAPI, Enabled: false}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	result := runUsageSyncNoticeRequest(t, m, row.Domain)
	if result.SyncError == "" || result.SyncSkipped {
		t.Fatal("local failure returned an empty sync_error, which old clients treat as success")
	}
}

func TestManualUsageSyncRealWorkStillRuns(t *testing.T) {
	for _, historyOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "tail", true: "history_only"}[historyOnly], func(t *testing.T) {
			server, calls := newUpstreamUsageFixtureServer(t, nil, nil)
			m := newChannelUpstreamTestMonitor(t)
			m.cfg.UpstreamUsageSyncEnabled = true
			now := time.Now().Unix()
			row := ChannelUpstreamAccount{
				Domain: "real-work.example", Provider: upstreamProviderNewAPI, BaseURL: server.URL,
				UserID: 31, Enabled: true, UsageSyncEnabled: true,
				UsageStatus: upstreamStatusOK, UsageDataUntil: now - 60,
				UsageBackfillDone: true, UsageBackfillCursor: cstDayStart(now), BalanceUnit: 500000,
			}
			if historyOnly {
				row.UsageNextSyncAt = now + 3600
				row.UsageBackfillCursor -= 3600
				row.UsageBackfillDone = false
			}
			if err := m.persistSyncedUpstreamAccount(context.Background(), &row, newAPICredential{AccessToken: "usage-token"}); err != nil {
				t.Fatal(err)
			}
			result := runUsageSyncNoticeRequest(t, m, row.Domain)
			if result.SyncSkipped || result.SyncError != "" || calls.Load() == 0 {
				t.Fatalf("eligible work was blocked: skipped=%v error=%q calls=%d", result.SyncSkipped, result.SyncError, calls.Load())
			}
			if historyOnly {
				if result.Account.UsageDataUntil != row.UsageDataUntil || !result.Account.UsageBackfillDone {
					t.Fatal("history-only success changed today's watermark or failed to finish history")
				}
			} else if result.Account.UsageDataUntil < now {
				t.Fatal("successful current-day sync did not advance its watermark")
			}
		})
	}
}
