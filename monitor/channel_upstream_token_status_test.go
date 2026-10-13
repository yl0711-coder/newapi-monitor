package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func tokenStatus(t *testing.T, f tokenReconcileFixture, now int64, want string) upstreamTokenConfigView {
	t.Helper()
	before := tokenPublishedSnapshot(t, f)
	bindings := tokenBindingStoredRows(t, f)
	var configs []ChannelUpstreamTokenConfig
	var jobs []ChannelUpstreamTokenWorkState
	if err := f.m.storeDB.Order("rowid").Find(&configs).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.Order("rowid").Find(&jobs).Error; err != nil {
		t.Fatal(err)
	}
	v, err := f.m.loadUpstreamTokenConfigWithStatus(context.Background(), f.account.Domain, 185, now)
	if err != nil || v.Processing == nil || v.Processing.Status != want {
		t.Fatalf("want %s, got %+v (%+v): %v", want, v, v.Processing, err)
	}
	var afterConfigs []ChannelUpstreamTokenConfig
	var afterJobs []ChannelUpstreamTokenWorkState
	if err := f.m.storeDB.Order("rowid").Find(&afterConfigs).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.Order("rowid").Find(&afterJobs).Error; err != nil {
		t.Fatal(err)
	}
	if tokenPublishedSnapshot(t, f) != before || !reflect.DeepEqual(bindings, tokenBindingStoredRows(t, f)) ||
		!reflect.DeepEqual(configs, afterConfigs) || !reflect.DeepEqual(jobs, afterJobs) {
		t.Fatal("read-only projection changed persistent state")
	}
	return v
}

func TestUpstreamTokenStatusActivationBothProviders(t *testing.T) {
	for _, provider := range []string{upstreamProviderNewAPI, upstreamProviderSub2API} {
		t.Run(provider, func(t *testing.T) {
			f, now := tokenAdvanceFixture(t, provider)
			tokenPipelineStore(t, f)
			tokenStatus(t, f, now, "pending")
			f.m.syncDueUpstreamTokenWorkAt(context.Background(), now)
			v := tokenStatus(t, f, now, "scheduled")
			if v.Processing.EffectiveAt != (now/3600+1)*3600 || v.Processing.CheckStatus != "checked" {
				t.Fatal(v.Processing)
			}
			tokenStatus(t, f, v.Processing.EffectiveAt-1, "scheduled")
			// Status never needs proof envelopes or report computation.
			proofTable := "channel_upstream_token_scan_checkpoints"
			if provider == upstreamProviderNewAPI {
				proofTable = "channel_upstream_pricing_page_checkpoints"
			}
			if err := f.m.storeDB.Exec("DROP TABLE " + proofTable).Error; err != nil {
				t.Fatal(err)
			}
			f.m.cfg.LocalSnapshotOnly = true
			tokenStatus(t, f, v.Processing.EffectiveAt, "active")
		})
	}
}

func TestUpstreamTokenStatusSharedAndChangedIntent(t *testing.T) {
	f, now := tokenAdvanceFixture(t, upstreamProviderSub2API)
	tokenPipelineStore(t, f)
	if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 186, "575", 1), "tester", now-1); err != nil {
		t.Fatal(err)
	}
	f.m.syncDueUpstreamTokenWorkAt(context.Background(), now)
	v := tokenStatus(t, f, now+3600, "shared")
	if len(v.SharedChannelIDs) != 2 {
		t.Fatal(v)
	}
	if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 186, "574", 2), "tester", now+3601); err != nil {
		t.Fatal(err)
	}
	// Another owner's edit invalidates old shared intent even though this
	// channel's own configuration revision has not changed.
	v = tokenStatus(t, f, now+3602, "pending")
	if v.Processing.CheckStatus != "pending" || v.Processing.EffectiveAt != 0 {
		t.Fatal("stale task validated changed owner", v.Processing)
	}
	if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 185, "", 1), "tester", now+3603); err != nil {
		t.Fatal(err)
	}
	tokenStatus(t, f, now+3604, "unconfigured")
}

func TestUpstreamTokenStatusWorkerAndFailClosed(t *testing.T) {
	for _, mode := range []string{"retry", "running", "waiting_evidence", "stale-worker", "bad-key", "missing-table", "disabled", "wrong-domain", "unsupported", "future-config", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			f, now := tokenAdvanceFixture(t, upstreamProviderSub2API)
			tokenPipelineStore(t, f)
			var revision int64
			if err := f.m.storeDB.Raw("SELECT MAX(rowid) FROM channel_upstream_token_configs").Scan(&revision).Error; err != nil {
				t.Fatal(err)
			}
			work := ChannelUpstreamTokenWorkState{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), Kind: "binding", Revision: 1, InputRevision: revision, Status: "retry", NextAttemptAt: now + 60, UpdatedAt: now - 1}
			want, check := "pending", "retry"
			switch mode {
			case "running":
				work.LeaseUntil, check = now+60, "running"
			case "waiting_evidence":
				work.Status, check = "waiting_evidence", "waiting_evidence"
			case "stale-worker":
				work.InputRevision, check = revision-1, "pending"
			case "bad-key":
				f.m.cfg.ChannelCostHMACKey = "invalid"
				want = "unavailable"
			case "missing-table":
				if err := f.m.storeDB.Exec("DROP TABLE channel_cost_key_registry").Error; err != nil {
					t.Fatal(err)
				}
				want = "unavailable"
			case "disabled":
				f.m.cfg.ChannelCostClosureEnabled = false
				want = "disabled"
			case "wrong-domain":
				f.m.cfg.ChannelCostClosureDomains = []string{"other.example"}
				want = "disabled"
			case "unsupported":
				if err := f.m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", f.account.Domain).Update("usage_adapter", upstreamUsageAdapterSub2Stats).Error; err != nil {
					t.Fatal(err)
				}
				want = "unsupported"
			case "future-config":
				now = f.day - 100
				want = "unavailable"
			case "canceled":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := f.m.loadUpstreamTokenConfigWithStatus(ctx, f.account.Domain, 185, now); err == nil {
					t.Fatal("canceled read succeeded")
				}
				return
			}
			if err := f.m.storeDB.Create(&work).Error; err != nil {
				t.Fatal(err)
			}
			v := tokenStatus(t, f, now, want)
			if want == "pending" && v.Processing.CheckStatus != check {
				t.Fatal(v.Processing)
			}
			if mode == "retry" && v.Processing.RetryAt != now+60 {
				t.Fatal(v.Processing)
			}
			b, err := json.Marshal(v.Processing)
			if err != nil || len(b) > 300 {
				t.Fatal("unbounded status", err)
			}
		})
	}
}

func TestUpstreamTokenStatusHTTPReadOnlyAndSanitized(t *testing.T) {
	f, now := tokenAdvanceFixture(t, upstreamProviderSub2API)
	tokenPipelineStore(t, f)
	f.m.syncDueUpstreamTokenWorkAt(context.Background(), now)
	f.m.cfg.SessionSecret = "synthetic-status-session"
	sqlDB, err := f.m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	if err := f.m.storeDB.Exec("PRAGMA query_only=ON").Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.m.storeDB.Exec("PRAGMA query_only=OFF").Error })
	path := "/channels/upstream/token-config?domain=" + f.account.Domain + "&channel_id=185"
	r := tokenConfigRouter(f.m)
	w := tokenConfigRequest(f.m, r, http.MethodGet, path, "", roleRoot)
	var v upstreamTokenConfigView
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || v.Processing == nil || v.Processing.Status != "active" || !v.AttributionVerified {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, secret := range []string{f.m.cfg.ChannelCostHMACKey, "success_hash", "source_ref", "envelope", "SELECT "} {
		if secret != "" && strings.Contains(w.Body.String(), secret) {
			t.Fatal("status leaked internal detail")
		}
	}
	w = tokenConfigRequest(f.m, r, http.MethodGet, path, "", roleAdmin)
	if w.Code != http.StatusForbidden {
		t.Fatal("status escaped root authorization", w.Code)
	}
}
