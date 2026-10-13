package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
)

func tokenRecoveryStatusFixture(t *testing.T) (tokenReconcileFixture, int64) {
	t.Helper()
	f, now := tokenAdvanceFixture(t, upstreamProviderSub2API)
	tokenPipelineStore(t, f)
	f.m.cfg.UpstreamPricingLedgerEnabled = true
	f.m.cfg.UpstreamPricingLedgerDomains = []string{f.account.Domain}
	f.m.cfg.UpstreamCredentialSecret = "synthetic-recovery-status-only"
	f.account.Enabled, f.account.UsageSyncEnabled = true, true
	sealed, err := f.m.sealUpstreamCredential(f.account.Domain, f.account.Provider, sub2APICredential{AccessToken: "synthetic-status-access", ExpiresAt: now + 3600})
	if err != nil {
		t.Fatal(err)
	}
	f.account.Credential, f.account.CredentialVersion = sealed, upstreamCredentialVersion
	if err := f.m.storeDB.Save(&f.account).Error; err != nil {
		t.Fatal(err)
	}
	f.m.upstreamClient = &http.Client{Transport: upstreamRoundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("status made HTTP request")
		return nil, errors.New("forbidden")
	})}
	return f, now
}

func TestUpstreamTokenRecoveryStatusStatesReadOnly(t *testing.T) {
	for _, mode := range []string{"pending", "paging", "waiting_second_scan", "waiting_legacy", "retry", "running", "reconnect", "reauthorized", "observed", "unknown", "disabled", "other-epoch", "other-domain", "future-day", "other-kind"} {
		t.Run(mode, func(t *testing.T) {
			f, now := tokenRecoveryStatusFixture(t)
			w := ChannelUpstreamTokenWorkState{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), Kind: "scan_day", DayTs: f.day, Status: mode, InputRevision: f.m.sub2TokenRecoveryCredentialRevision(f.account), NextAttemptAt: now + 300, UpdatedAt: now - 1}
			want := mode
			switch mode {
			case "running":
				w.Status = "retry"
				w.LeaseUntil = now + 60
			case "reconnect":
				w.NextAttemptAt = upstreamAccountIsolatedUntil
			case "reauthorized":
				w.Status = "reconnect"
				w.InputRevision++
				w.NextAttemptAt = upstreamAccountIsolatedUntil
				want = "pending"
			case "unknown":
				w.Status = "<script>private-error</script>"
				want = "pending"
			case "disabled":
				f.m.cfg.UpstreamPricingLedgerEnabled = false
			case "other-epoch":
				w.AccountEpoch = strings.Repeat("a", 64)
				want = "pending"
			case "other-domain":
				w.Domain = "other.example"
				want = "pending"
			case "future-day":
				w.DayTs += 86400
				want = "pending"
			case "other-kind":
				w.Kind = "cost_day"
				w.ChannelID = 185
				want = "pending"
			}
			if mode != "pending" {
				if err := f.m.storeDB.Create(&w).Error; err != nil {
					t.Fatal(err)
				}
			}
			// The status GET must work with a write-prohibited connection and no
			// proof table, without triggering collection or financial recalculation.
			if err := f.m.storeDB.Exec("DROP TABLE channel_upstream_token_scan_checkpoints").Error; err != nil {
				t.Fatal(err)
			}
			db, err := f.m.storeDB.DB()
			if err != nil {
				t.Fatal(err)
			}
			db.SetMaxOpenConns(1)
			if err := f.m.storeDB.Exec("PRAGMA query_only=ON").Error; err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = f.m.storeDB.Exec("PRAGMA query_only=OFF").Error })
			v := tokenStatus(t, f, now, "pending")
			r := v.Processing.Recovery
			if r == nil || r.Status != want {
				t.Fatalf("want %s got %+v", want, r)
			}
			if mode == "reconnect" || mode == "reauthorized" || mode == "running" {
				if r.RetryAt != 0 {
					t.Fatal("misleading retry timestamp", r)
				}
			}
			if mode == "retry" && r.RetryAt != now+300 {
				t.Fatal(r)
			}
			encoded, err := json.Marshal(r)
			if err != nil || len(encoded) > 220 {
				t.Fatal("unbounded projection", err)
			}
			for _, secret := range []string{"synthetic-status-access", f.account.Credential, "input_revision", "private-error", "SELECT", f.m.cfg.ChannelCostHMACKey} {
				if secret != "" && strings.Contains(string(encoded), secret) {
					t.Fatal("projection leaked internal data")
				}
			}
		})
	}
}

func TestUpstreamTokenRecoveryStatusAccountPauseWinsAndBindingIndependent(t *testing.T) {
	f, now := tokenRecoveryStatusFixture(t)
	f.m.syncDueUpstreamTokenWorkAt(context.Background(), now)
	now += 3600
	rows := []ChannelUpstreamTokenWorkState{
		{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), Kind: "scan_day", DayTs: f.day, Status: "observed", UpdatedAt: now},
		{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), Kind: "scan_day", DayTs: f.day - 86400, Status: "reconnect", InputRevision: f.m.sub2TokenRecoveryCredentialRevision(f.account), NextAttemptAt: upstreamAccountIsolatedUntil, UpdatedAt: now - 100},
	}
	if err := f.m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	v := tokenStatus(t, f, now, "active")
	if !v.AttributionVerified || v.Processing.Recovery.Status != "reconnect" || v.Processing.Recovery.DayTs != f.day-86400 {
		t.Fatal(v.Processing)
	}
	// Same credentials with another encryption nonce must still show paused.
	cred, err := f.m.credentialForAccount(f.account)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := f.m.sealUpstreamCredential(f.account.Domain, f.account.Provider, cred)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", f.account.Domain).Update("credential", sealed).Error; err != nil {
		t.Fatal(err)
	}
	if v = tokenStatus(t, f, now, "active"); v.Processing.Recovery.Status != "reconnect" {
		t.Fatal(v.Processing)
	}
	// A changed credential is pending recheck, never an invented success.
	sealed, err = f.m.sealUpstreamCredential(f.account.Domain, f.account.Provider, sub2APICredential{AccessToken: "new-synthetic", ExpiresAt: now + 3600})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", f.account.Domain).Update("credential", sealed).Error; err != nil {
		t.Fatal(err)
	}
	if v = tokenStatus(t, f, now, "active"); v.Processing.Recovery.Status != "pending" {
		t.Fatal(v.Processing)
	}
}

func TestUpstreamTokenRecoveryStatusOptionalProviderIsolation(t *testing.T) {
	for _, mode := range []string{"newapi", "cleared", "unsupported", "bad-key"} {
		t.Run(mode, func(t *testing.T) {
			var f tokenReconcileFixture
			var now int64
			if mode == "newapi" {
				f, now = tokenAdvanceFixture(t, upstreamProviderNewAPI)
				tokenPipelineStore(t, f)
			} else {
				f, now = tokenRecoveryStatusFixture(t)
			}
			want := "pending"
			switch mode {
			case "cleared":
				if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 185, "", 1), "fixture", now-1); err != nil {
					t.Fatal(err)
				}
				want = "unconfigured"
			case "unsupported":
				if err := f.m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", f.account.Domain).Update("usage_adapter", upstreamUsageAdapterSub2Stats).Error; err != nil {
					t.Fatal(err)
				}
				want = "unsupported"
			case "bad-key":
				f.m.cfg.ChannelCostHMACKey = "invalid"
				want = "unavailable"
			}
			v := tokenStatus(t, f, now, want)
			if mode == "bad-key" {
				if v.Processing.Recovery == nil || v.Processing.Recovery.Status != "unavailable" {
					t.Fatal(v.Processing)
				}
			} else if v.Processing.Recovery != nil {
				t.Fatal("unrelated provider/config gained recovery status", v.Processing)
			}
		})
	}
}
