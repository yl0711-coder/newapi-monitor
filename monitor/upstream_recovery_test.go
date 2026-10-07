package monitor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newUpstreamRecoveryFixture(t *testing.T) (*Monitor, ChannelUpstreamAccount, *atomic.Int32) {
	t.Helper()
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.LocalSnapshotOnly = false
	m.cfg.UpstreamSyncEnabled = true
	m.cfg.UpstreamUsageSyncEnabled = true
	m.cfg.UpstreamFundsSyncEnabled = true
	m.cfg.UpstreamPricingLedgerEnabled = true
	m.cfg.UpstreamErrorLogSyncEnabled = true
	m.cfg.UpstreamFundsDomains = []string{"fixture.example"}
	m.cfg.UpstreamPricingLedgerDomains = []string{"fixture.example"}
	m.cfg.UpstreamErrorLogDomains = []string{"fixture.example"}
	now := time.Now().Unix()
	row := ChannelUpstreamAccount{Domain: "fixture.example", BaseURL: "https://fixture.example", Provider: upstreamProviderNewAPI, UserID: 7, Enabled: true, UsageSyncEnabled: true, CredentialVersion: upstreamCredentialVersion,
		BalanceKnown: true, BalanceUSD: 71, BalanceUnit: 500000, Status: upstreamStatusReconnect, NextSyncAt: upstreamAccountIsolatedUntil, LastAttemptAt: now - 7200, LastSuccessAt: now - 8000, ConsecutiveFails: 2,
		UsageStatus: upstreamStatusReconnect, UsageNextSyncAt: upstreamAccountIsolatedUntil, UsageLastAttemptAt: now - 7200, UsageLastSuccessAt: now - 8000, UsageConsecutiveFails: 2, UsageLastError: "HTTP 403",
		UsageDataUntil: now - 9000, UsageBackfillCursor: now - 86400, UsageBackfillNextSyncAt: upstreamAccountIsolatedUntil, UsageBackfillLastError: "HTTP 403", UsageBackfillConsecutiveFails: 2, UsageBackfillLastAttemptAt: now - 7200}
	if err := m.sealUpstreamAccountCredential(&row, newAPICredential{AccessToken: "fixture-access"}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	for _, state := range []any{
		&UpstreamFundSyncState{Domain: row.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(row), Status: upstreamStatusReconnect, NextSyncAt: upstreamAccountIsolatedUntil, LastAttemptAt: now - 7200, ConsecutiveFails: 2, TailSyncedUntil: now - 9000},
		&UpstreamErrorLogSyncState{Domain: row.Domain, Status: upstreamStatusReconnect, NextSyncAt: upstreamAccountIsolatedUntil, LastAttemptAt: now - 7200, ConsecutiveFails: 2, SyncedUntil: now - 9000},
		&ChannelUpstreamPricingSyncState{Domain: row.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(row), SemanticsVersion: upstreamPricingSemanticsVersion, Status: upstreamStatusError, TailNextSyncAt: upstreamAccountIsolatedUntil, BackfillNextSyncAt: upstreamAccountIsolatedUntil, LastAttemptAt: now - 7200, ConsecutiveFailures: 9, BackfillNextHour: now - 86400},
	} {
		if err := m.storeDB.Create(state).Error; err != nil {
			t.Fatal(err)
		}
	}
	calls := new(atomic.Int32)
	setUpstreamRecoveryTransport(m, upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if r.Method != http.MethodGet {
			t.Fatal("probe wrote upstream")
		}
		body := `{"success":true,"data":{"items":[],"total":0}}`
		switch r.URL.Path {
		case "/api/user/self":
			body = `{"success":true,"data":{"quota":1000000}}`
		case "/api/status":
			body = `{"success":true,"data":{"quota_per_unit":500000}}`
		case "/api/log/self":
		default:
			t.Fatalf("unexpected endpoint %s", r.URL.Path)
		}
		return recoveryHTTPResponse(200, body), nil
	}))
	return m, row, calls
}

func setUpstreamRecoveryTransport(m *Monitor, transport http.RoundTripper) {
	guard := newUpstreamHostGuard(m.storeDB, upstreamHostGuardOptions{Clock: realUpstreamGuardClock{}, Jitter: func() time.Duration { return 0 }, MinInterval: 0})
	m.upstreamClient = installUpstreamHostGuardForTest(&http.Client{Transport: transport}, m.storeDB, guard)
}

func recoveryHTTPResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func TestUpstreamRecoveryAllTaskScopesPreserveFacts(t *testing.T) {
	for _, task := range []string{"balance", "usage", "usage_history", "funds", "pricing", "error_logs"} {
		t.Run(task, func(t *testing.T) {
			m, row, calls := newUpstreamRecoveryFixture(t)
			if task == "usage_history" {
				row.UsageStatus = upstreamStatusOK
				if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", row.Domain).UpdateColumn("usage_status", upstreamStatusOK).Error; err != nil {
					t.Fatal(err)
				}
			}
			before, err := m.loadUpstreamRecoveryTarget(context.Background(), row, task)
			if err != nil {
				t.Fatal(err)
			}
			result, err := m.recoverUpstreamTask(context.Background(), row.Domain, task)
			if err != nil || result.Status != "queued" {
				t.Fatalf("%+v %v", result, err)
			}
			if calls.Load() < 1 || calls.Load() > 4 {
				t.Fatalf("unbounded or missing probe: %d", calls.Load())
			}
			var got ChannelUpstreamAccount
			if err := m.storeDB.First(&got, "domain = ?", row.Domain).Error; err != nil {
				t.Fatal(err)
			}
			if got.Credential != row.Credential || got.BalanceUSD != row.BalanceUSD || got.BalanceKnown != row.BalanceKnown || got.UsageDataUntil != row.UsageDataUntil || got.UsageBackfillCursor != row.UsageBackfillCursor || got.LastSuccessAt != row.LastSuccessAt || got.UsageLastSuccessAt != row.UsageLastSuccessAt {
				t.Fatal("probe changed published facts, cursor or credentials")
			}
			after, err := m.loadUpstreamRecoveryTarget(context.Background(), got, task)
			if err != nil {
				t.Fatal(err)
			}
			if after.failures != before.failures || after.next == upstreamAccountIsolatedUntil {
				t.Fatal("probe reset failures or failed to resume")
			}
			count := calls.Load()
			if _, err := m.recoverUpstreamTask(context.Background(), row.Domain, task); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != count {
				t.Fatal("duplicate click repeated upstream request")
			}
		})
	}
}

func TestUpstreamRecoveryFailureCooldownSurvivesMonitorRestart(t *testing.T) {
	m, row, calls := newUpstreamRecoveryFixture(t)
	setUpstreamRecoveryTransport(m, upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return recoveryHTTPResponse(403, `{"message":"denied"}`), nil
	}))
	result, err := m.recoverUpstreamTask(context.Background(), row.Domain, "usage")
	if err != nil || result.Status != "blocked" || calls.Load() != 1 {
		t.Fatalf("%+v %v calls=%d", result, err, calls.Load())
	}
	// Fresh gate/monitor state, same durable database: no in-memory cooldown.
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, upstreamClient: m.upstreamClient}
	result, err = restarted.recoverUpstreamTask(context.Background(), row.Domain, "usage")
	if err != nil || result.Status != "waiting" || calls.Load() != 1 {
		t.Fatalf("restart lost cooldown: %+v %v calls=%d", result, err, calls.Load())
	}
	var got ChannelUpstreamAccount
	if err := m.storeDB.First(&got, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if got.UsageNextSyncAt != upstreamAccountIsolatedUntil || got.UsageDataUntil != row.UsageDataUntil {
		t.Fatal("failed probe activated task or overwrote data")
	}
}

func TestUpstreamRecoveryConfigRaceAndDisabledTasks(t *testing.T) {
	t.Run("configuration changed while probing", func(t *testing.T) {
		m, row, _ := newUpstreamRecoveryFixture(t)
		setUpstreamRecoveryTransport(m, upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", row.Domain).UpdateColumn("base_url", "https://changed.example").Error; err != nil {
				t.Fatal(err)
			}
			return recoveryHTTPResponse(200, `{"success":true,"data":{"items":[],"total":0}}`), nil
		}))
		_, err := m.recoverUpstreamTask(context.Background(), row.Domain, "usage")
		if err == nil {
			t.Fatal("stale probe activated changed configuration")
		}
		var got ChannelUpstreamAccount
		if err := m.storeDB.First(&got, "domain = ?", row.Domain).Error; err != nil {
			t.Fatal(err)
		}
		if got.UsageNextSyncAt != upstreamAccountIsolatedUntil {
			t.Fatal("stale probe resumed task")
		}
	})
	t.Run("disabled and busy", func(t *testing.T) {
		m, row, calls := newUpstreamRecoveryFixture(t)
		m.cfg.UpstreamUsageSyncEnabled = false
		if _, err := m.recoverUpstreamTask(context.Background(), row.Domain, "usage"); err == nil {
			t.Fatal("global-off task recovered")
		}
		m.cfg.UpstreamUsageSyncEnabled = true
		release, err := m.tryAcquireUpstreamAccountBackground(row.Domain)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		if _, err := m.recoverUpstreamTask(context.Background(), row.Domain, "usage"); !errors.Is(err, errUpstreamAccountBusy) {
			t.Fatal("busy task queued another probe")
		}
		if calls.Load() != 0 {
			t.Fatal("disabled/busy action contacted upstream")
		}
	})
}

func TestUpstreamRecoveryCapabilityMatrix(t *testing.T) {
	s := Settings{UpstreamSyncEnabled: true, UpstreamUsageSyncEnabled: true, UpstreamFundsSyncEnabled: true, UpstreamPricingLedgerEnabled: true, UpstreamErrorLogSyncEnabled: true, UpstreamFundsDomains: []string{"fixture.example"}, UpstreamPricingLedgerDomains: []string{"fixture.example"}, UpstreamErrorLogDomains: []string{"fixture.example"}}
	for _, provider := range []string{upstreamProviderNewAPI, upstreamProviderSub2API, upstreamProviderTokenForce, upstreamProviderAICodeWith, upstreamProviderOpenOx} {
		row := ChannelUpstreamAccount{Domain: "fixture.example", Provider: provider, Enabled: true, UsageSyncEnabled: true}
		for _, task := range []string{"balance", "usage", "usage_history"} {
			if !upstreamRecoveryTaskAllowed(s, row, task) {
				t.Fatalf("missing provider %s task %s", provider, task)
			}
		}
		row.Enabled = false
		if upstreamRecoveryTaskAllowed(s, row, "balance") {
			t.Fatal("re-enabled disabled account")
		}
	}
}
