package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestUpstreamBalanceHealthPreservesSnapshotsAndBoundsFreshness(t *testing.T) {
	const now int64 = 1800000000
	for _, tc := range []struct {
		name, status, want string
		age                int64
		known, fresh       bool
		next               int64
	}{
		{"fresh", upstreamStatusOK, upstreamStatusOK, 60, true, true, 0},
		{"boundary", upstreamStatusOK, upstreamStatusOK, 1800, true, true, 0},
		{"expired successful state", upstreamStatusOK, "stale", 1801, true, false, 0},
		{"future timestamp", upstreamStatusOK, "stale", -60, true, false, 0},
		{"no balance", upstreamStatusOK, "queued", 60, false, false, 0},
		{"failed after recent balance", upstreamStatusError, upstreamStatusError, 60, true, true, 0},
		{"paused", upstreamStatusError, "paused", 60, true, true, upstreamAccountIsolatedUntil},
		{"auth pause", upstreamStatusReconnect, upstreamStatusReconnect, 60, true, true, upstreamAccountIsolatedUntil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := ChannelUpstreamAccount{Enabled: true, BalanceKnown: tc.known, BalanceUSD: -2,
				Status: tc.status, LastSuccessAt: now - tc.age, NextSyncAt: tc.next}
			before := row
			view := upstreamAccountView(row)
			decorateUpstreamBalanceHealth(&view, row, Settings{UpstreamSyncEnabled: true}, now)
			if view.BalanceEffectiveStatus != tc.want || view.BalanceFresh != tc.fresh || view.BalanceFreshnessLimitSeconds != 1800 {
				t.Fatalf("health = %s fresh=%v limit=%d", view.BalanceEffectiveStatus, view.BalanceFresh, view.BalanceFreshnessLimitSeconds)
			}
			if row != before || (tc.known && (view.BalanceUSD == nil || *view.BalanceUSD != -2)) {
				t.Fatal("health decoration changed snapshot or persisted state")
			}
		})
	}
	for _, amount := range []float64{0, -1, 23} {
		row := ChannelUpstreamAccount{Enabled: true, BalanceKnown: true, BalanceUSD: amount, Status: upstreamStatusOK, LastSuccessAt: now}
		view := upstreamAccountView(row)
		decorateUpstreamBalanceHealth(&view, row, Settings{}, now)
		if view.BalanceEffectiveStatus != "global_off" || !view.BalanceFresh || view.BalanceWorkerEnabled {
			t.Fatal("disabled worker must not erase a recent balance or claim automatic collection")
		}
		row.Enabled = false
		decorateUpstreamBalanceHealth(&view, row, Settings{UpstreamSyncEnabled: true}, now)
		if view.BalanceEffectiveStatus != upstreamStatusDisabled || *view.BalanceUSD != amount {
			t.Fatal("disabled account must retain its actual balance")
		}
	}
}

func TestUpstreamBalanceFreshnessUsesAlertWindow(t *testing.T) {
	const now int64 = 1800000000
	for _, minutes := range []int{0, 5, 10, 20, 1440} {
		limit := int64(max(30, minutes*3) * 60)
		if upstreamBalanceFreshnessLimit(minutes) != limit || !upstreamBalanceFresh(now-limit, now, minutes) || upstreamBalanceFresh(now-limit-1, now, minutes) || upstreamBalanceFresh(0, now, minutes) {
			t.Fatalf("incorrect balance freshness window: %d minutes", minutes)
		}
	}
}

func TestUpstreamUsageHistoryPauseIsNotScheduledRetry(t *testing.T) {
	s := Settings{UpstreamUsageSyncEnabled: true}
	row := ChannelUpstreamAccount{Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK,
		UsageBackfillLastError: "HTTP 403", UsageBackfillNextSyncAt: upstreamAccountIsolatedUntil}
	if got := upstreamUsageHistoryPhase(row, s); got != "paused" {
		t.Fatalf("permanent stop presented as %q", got)
	}
	row.UsageBackfillNextSyncAt = 1800000100
	if got := upstreamUsageHistoryPhase(row, s); got != "retry" {
		t.Fatalf("scheduled retry presented as %q", got)
	}
	row.UsageBackfillDone = true
	if got := upstreamUsageHistoryPhase(row, s); got != "complete" {
		t.Fatalf("complete history presented as %q", got)
	}
	s.UpstreamUsageSyncEnabled = false
	if got := upstreamUsageHistoryPhase(row, s); got != "global_off" {
		t.Fatalf("disabled worker presented as %q", got)
	}
}

func TestUpstreamHealthRefreshReadsLocalStateWithoutSyncOrMutation(t *testing.T) {
	m, row, calls := newUpstreamRecoveryFixture(t)
	for range 3 {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/channels/upstream?domain="+row.Domain, nil)
		m.getChannelUpstreamHandler(c)
		var result channelUpstreamConfigView
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &result) != nil {
			t.Fatalf("local status read failed: %d", w.Code)
		}
		if result.Account.BalanceEffectiveStatus != upstreamStatusReconnect || result.Account.BalanceFresh || !result.Account.BalanceWorkerEnabled || result.Account.BalanceUSD == nil || *result.Account.BalanceUSD != row.BalanceUSD {
			t.Fatal("status read did not expose the preserved stale balance and paused authentication")
		}
		if strings.Contains(w.Body.String(), "fixture-access") {
			t.Fatal("local status read exposed credential")
		}
	}
	var after ChannelUpstreamAccount
	if err := m.storeDB.First(&after, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if after != row || calls.Load() != 0 {
		t.Fatal("reading status must not change schedules/facts or contact the upstream")
	}
}
