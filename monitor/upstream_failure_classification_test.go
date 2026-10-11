package monitor

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Exercise adapter errors through the common policy, rather than only testing
// a synthetic HTTP error which can hide an adapter's lost status code.
func TestUpstreamOpenOxForbiddenRemainsRecoverable(t *testing.T) {
	for _, status := range []int{401, 403, 429, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			client := &http.Client{Transport: upstreamRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return recoveryHTTPResponse(status, `{"message":"private-fixture-secret"}`), nil
			})}
			var target any
			err := openOxGET(context.Background(), client, ChannelUpstreamAccount{BaseURL: "https://fixture.example"}, openOxCredential{AccessToken: "private-fixture-secret"}, "/api/v1/profile", &target)
			var httpErr *upstreamHTTPError
			if !errors.As(err, &httpErr) || httpErr.Status != status || strings.Contains(err.Error(), "private-fixture-secret") {
				t.Fatalf("lost HTTP classification or secret leak: %v", err)
			}
			assertUpstreamFailureLanePolicy(t, err, status == 401)
		})
	}
}

func TestUpstreamOpenOxPauseRestartAndManualReactivation(t *testing.T) {
	m, row, calls := newUpstreamRecoveryFixture(t)
	row.Provider, row.BalanceUnit = upstreamProviderOpenOx, 1
	row.Status, row.ConsecutiveFails, row.NextSyncAt = upstreamStatusPending, 0, 0
	if err := m.sealUpstreamAccountCredential(&row, openOxCredential{AccessToken: "fixture-token"}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Save(&row).Error; err != nil {
		t.Fatal(err)
	}
	healthy := false
	setUpstreamRecoveryTransport(m, upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/profile" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if !healthy {
			return recoveryHTTPResponse(403, `{"message":"denied"}`), nil
		}
		return recoveryHTTPResponse(200, `{"success":true,"data":{"user":{"id":7},"spendable_balance":"41"}}`), nil
	}))
	for attempt := 1; attempt <= upstreamRecoveryFailureLimit; attempt++ {
		before := time.Now().Unix()
		got, err := m.syncStoredUpstreamAccount(context.Background(), row.Domain)
		if err == nil || got.ConsecutiveFails != attempt || calls.Load() != int32(attempt) {
			t.Fatalf("attempt=%d failures=%d calls=%d err=%v", attempt, got.ConsecutiveFails, calls.Load(), err)
		}
		if got.Status != upstreamStatusError || got.BalanceUSD != row.BalanceUSD || got.LastSuccessAt != row.LastSuccessAt {
			t.Fatal("403 changed financial facts or falsely required new credentials")
		}
		if attempt < upstreamRecoveryFailureLimit {
			if got.NextSyncAt == upstreamAccountIsolatedUntil || got.NextSyncAt < before+1800 {
				t.Fatal("403 permanently paused early or bypassed conservative cooldown")
			}
			if _, err := m.syncStoredUpstreamAccount(context.Background(), row.Domain); err == nil || calls.Load() != int32(attempt) {
				t.Fatal("ordinary sync bypassed cooldown")
			}
			// Advance only this fixture's due time; production intervals are unchanged.
			if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", row.Domain).UpdateColumn("next_sync_at", 0).Error; err != nil {
				t.Fatal(err)
			}
		} else if got.NextSyncAt != upstreamAccountIsolatedUntil {
			t.Fatal("exhausted incident budget did not pause")
		}
	}
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, upstreamClient: m.upstreamClient}
	if _, err := restarted.syncStoredUpstreamAccount(context.Background(), row.Domain); err == nil || calls.Load() != upstreamRecoveryFailureLimit {
		t.Fatal("restart bypassed pause")
	}
	healthy = true
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", row.Domain).UpdateColumn("last_attempt_at", time.Now().Unix()-3600).Error; err != nil {
		t.Fatal(err)
	}
	result, err := restarted.recoverUpstreamTask(context.Background(), row.Domain, "balance")
	if err != nil || result.Status != "queued" || calls.Load() != upstreamRecoveryFailureLimit+1 {
		t.Fatalf("reactivation: %+v %v", result, err)
	}
	var queued ChannelUpstreamAccount
	if err := m.storeDB.First(&queued, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if queued.ConsecutiveFails != upstreamRecoveryFailureLimit || queued.BalanceUSD != row.BalanceUSD || queued.LastSuccessAt != row.LastSuccessAt {
		t.Fatal("probe published facts or reset retry budget")
	}
	got, err := restarted.syncStoredUpstreamAccount(context.Background(), row.Domain)
	if err != nil || got.Status != upstreamStatusOK || got.ConsecutiveFails != 0 || got.BalanceUSD != 41 || calls.Load() != upstreamRecoveryFailureLimit+2 {
		t.Fatalf("actual sync did not recover: status=%s failures=%d calls=%d err=%v", got.Status, got.ConsecutiveFails, calls.Load(), err)
	}
}

func TestUpstreamSpringBalancePreservesMixedFailureClassification(t *testing.T) {
	for _, statuses := range [][]int{{401}, {403}, {401, 403}, {403, 401}, {503}} {
		name := ""
		for _, status := range statuses {
			name += http.StatusText(status) + "/"
		}
		t.Run(name, func(t *testing.T) {
			calls := 0
			keys := make([]string, len(statuses))
			for i := range keys {
				keys[i] = "sk-acw-fixture-key-" + strings.Repeat("x", i+1)
			}
			client := &http.Client{Transport: upstreamRoundTripFunc(func(*http.Request) (*http.Response, error) {
				status := statuses[calls]
				calls++
				return recoveryHTTPResponse(status, `{"message":"private-fixture-secret"}`), nil
			})}
			_, _, err := syncAICodeWithBalanceSnapshot(context.Background(), client, ChannelUpstreamAccount{BaseURL: "https://fixture.example"}, aiCodeWithCredential{APIKeys: keys})
			want := statuses[0]
			for _, status := range statuses {
				if status == 403 {
					want = 403
				}
			}
			var httpErr *upstreamHTTPError
			if !errors.As(err, &httpErr) || httpErr.Status != want || calls != len(statuses) {
				t.Fatalf("all-Key failure lost classification: calls=%d error=%v", calls, err)
			}
			assertUpstreamFailureLanePolicy(t, err, want == 401)
		})
	}
}

func assertUpstreamFailureLanePolicy(t *testing.T, err error, isolated bool) {
	t.Helper()
	const now int64 = 1800000000
	base := ChannelUpstreamAccount{Domain: "fixture.example", BalanceKnown: true, BalanceUSD: 71, LastSuccessAt: now - 100,
		UsageStatus: upstreamStatusOK, UsageNextSyncAt: now + 600, UsageDataUntil: now - 50, UsageBackfillCursor: now - 86400}
	balance, usage, history := base, base, base
	applyUpstreamSyncResult(&balance, upstreamBalanceResult{}, err, now, Settings{})
	applyUpstreamUsageResult(&usage, upstreamUsageResult{}, err, now, Settings{})
	applyUpstreamUsageBackfillResult(&history, err, now, Settings{})
	for _, next := range []int64{balance.NextSyncAt, usage.UsageNextSyncAt, history.UsageBackfillNextSyncAt} {
		if (next == upstreamAccountIsolatedUntil) != isolated || next <= now {
			t.Fatalf("incorrect retry/isolation: next=%d isolated=%v", next, isolated)
		}
	}
	if balance.BalanceUSD != base.BalanceUSD || balance.LastSuccessAt != base.LastSuccessAt || usage.UsageDataUntil != base.UsageDataUntil || history.UsageBackfillCursor != base.UsageBackfillCursor {
		t.Fatal("failure changed published facts or checkpoint")
	}
	if !isolated && (history.UsageStatus != base.UsageStatus || history.UsageNextSyncAt != base.UsageNextSyncAt) {
		t.Fatal("history-only temporary failure isolated recent usage")
	}
}
