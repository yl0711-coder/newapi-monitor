package monitor

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestUpstreamRecoveryRefreshOutagePersistsPricingCooldown(t *testing.T) {
	m, row, calls := newUpstreamRecoveryFixture(t)
	row.Provider = upstreamProviderSub2API
	row.UsageAdapter = upstreamUsageAdapterSub2Trend
	if err := m.sealUpstreamAccountCredential(&row, sub2APICredential{RefreshToken: "fixture-refresh"}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Save(&row).Error; err != nil {
		t.Fatal(err)
	}
	setUpstreamRecoveryTransport(m, upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/auth/refresh" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		return recoveryHTTPResponse(503, `{"message":"temporarily unavailable"}`), nil
	}))
	state, err := m.syncStoredUpstreamPricing(context.Background(), row.Domain)
	if err == nil || calls.Load() != 1 || state.Status != upstreamStatusError || state.ConsecutiveFailures != 1 || state.TailNextSyncAt <= time.Now().Unix() || state.TailNextSyncAt == upstreamAccountIsolatedUntil {
		t.Fatalf("refresh outage not scheduled: status=%s failures=%d retry=%d calls=%d error=%v", state.Status, state.ConsecutiveFailures, state.TailNextSyncAt, calls.Load(), err)
	}
	if _, err := m.syncStoredUpstreamPricing(context.Background(), row.Domain); err == nil || calls.Load() != 1 {
		t.Fatal("second turn repeated refresh during cooldown")
	}
}

func TestUpstreamRecoveryConfigurationPreservesFinancialFacts(t *testing.T) {
	m, row, _ := newUpstreamRecoveryFixture(t)
	row.UpdatedAt = time.Now().Unix()
	if err := m.persistUpstreamAccountIdentityChange(context.Background(), &row, false, true); err != nil {
		t.Fatal(err)
	}
	for _, task := range []string{"funds", "pricing", "error_logs"} {
		state, err := m.loadUpstreamRecoveryTarget(context.Background(), row, task)
		if err != nil || state.next != 0 || state.failures != 0 || state.status != upstreamStatusPending {
			t.Fatalf("%s remained isolated: %+v %v", task, state, err)
		}
	}
	var funds UpstreamFundSyncState
	if err := m.storeDB.First(&funds, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if funds.TailSyncedUntil != row.UsageDataUntil {
		t.Fatal("credential recovery changed funds cursor")
	}
	var pricing ChannelUpstreamPricingSyncState
	if err := m.storeDB.First(&pricing, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if pricing.BackfillNextHour != row.UsageBackfillCursor {
		t.Fatal("credential recovery changed pricing cursor")
	}
	var got ChannelUpstreamAccount
	if err := m.storeDB.First(&got, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if got.BalanceUSD != row.BalanceUSD || got.UsageDataUntil != row.UsageDataUntil || got.UsageBackfillCursor != row.UsageBackfillCursor {
		t.Fatal("credential recovery changed published financial facts")
	}
}

func TestUpstreamRecoveryLongRetryAfterPersistsAcrossHostGuardRestart(t *testing.T) {
	m, row, calls := newUpstreamRecoveryFixture(t)
	transport := upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		response := recoveryHTTPResponse(429, `{"message":"rate limited"}`)
		response.Header.Set("Retry-After", "36000")
		return response, nil
	})
	setUpstreamRecoveryTransport(m, transport)
	result, err := m.recoverUpstreamTask(context.Background(), row.Domain, "usage")
	if err != nil || result.Status != "blocked" || result.RetryAt < time.Now().Unix()+35998 {
		t.Fatalf("long cooldown lost: %+v %v", result, err)
	}
	// Simulate expiry of only the per-task 5-minute click guard. Recreate the
	// host guard as on restart: its durable 10-hour Retry-After still wins.
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", row.Domain).UpdateColumn("usage_last_attempt_at", time.Now().Unix()-3600).Error; err != nil {
		t.Fatal(err)
	}
	setUpstreamRecoveryTransport(m, transport)
	result, err = m.recoverUpstreamTask(context.Background(), row.Domain, "usage")
	if err != nil || result.Status != "blocked" || calls.Load() != 1 || result.RetryAt < time.Now().Unix()+35998 {
		t.Fatalf("restart bypassed Retry-After: %+v %v requests=%d", result, err, calls.Load())
	}
}

func TestUpstreamRecoveryPricingDirtyWorkCannotBypassPause(t *testing.T) {
	m, row, calls := newUpstreamRecoveryFixture(t)
	m.cfg.ChannelCostClosureEnabled = true
	m.cfg.ChannelCostClosureDomains = []string{row.Domain}
	if err := m.storeDB.Create(&ChannelCostDirtyHour{Domain: row.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(row), HourTs: 3600, Status: "pending", NextAttemptAt: 1}).Error; err != nil {
		t.Fatal(err)
	}
	m.syncDueUpstreamPricing(context.Background())
	if calls.Load() != 0 {
		t.Fatal("dirty cost work bypassed paused upstream")
	}
	state, err := m.loadUpstreamRecoveryTarget(context.Background(), row, "pricing")
	if err != nil || state.next != upstreamAccountIsolatedUntil || state.last != row.LastAttemptAt {
		t.Fatalf("paused scheduler mutated state: %+v %v", state, err)
	}
}
