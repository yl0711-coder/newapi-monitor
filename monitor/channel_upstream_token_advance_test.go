package monitor

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func tokenAdvanceFixture(t *testing.T, provider string) (tokenReconcileFixture, int64) {
	t.Helper()
	if provider == upstreamProviderNewAPI {
		f, _, now := newAPITokenStoreFixture(t)
		return f, now
	}
	f, _, now := tokenBindingStoreFixture(t)
	return f, now
}

func TestUpstreamTokenAdvanceSchedulesOnceAndRestartsWithoutProofReads(t *testing.T) {
	for _, provider := range []string{upstreamProviderSub2API, upstreamProviderNewAPI} {
		t.Run(provider, func(t *testing.T) {
			f, now := tokenAdvanceFixture(t, provider)
			before := tokenPublishedSnapshot(t, f)
			ctx := context.Background()
			got, err := f.m.advanceUpstreamTokenBindings(ctx, f.account.Domain, 185, now)
			if err != nil || got.Status != "scheduled" || got.ChangedSources != 2 || got.EffectiveFrom != (now/3600+1)*3600 {
				t.Fatal("initial account turn", got, err)
			}
			bindings := tokenBindingStoredRows(t, f)
			for _, b := range bindings {
				if b.ValidFrom <= now || b.CreatedBy != "monitor-token-config" || b.AllocationMode != "allocated" {
					t.Fatal("invalid scheduled ownership", b)
				}
			}
			if tokenPublishedSnapshot(t, f) != before {
				t.Fatal("binding turn changed money or queues")
			}
			// Remove a proof dependency. The unchanged path must not re-read it
			// or claim that a previously established mapping needs recreating.
			proofTable := "channel_upstream_token_scan_checkpoints"
			if provider == upstreamProviderNewAPI {
				proofTable = "channel_upstream_pricing_page_checkpoints"
			}
			if err := f.m.storeDB.Exec("DROP TABLE " + proofTable).Error; err != nil {
				t.Fatal(err)
			}
			f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
			for _, at := range []int64{now + 1, now + 3600, now + 86400} {
				got, err = f.m.advanceUpstreamTokenBindings(ctx, f.account.Domain, 186, at)
				if err != nil || got != (upstreamTokenAdvanceResult{Status: "unchanged"}) || !reflect.DeepEqual(bindings, tokenBindingStoredRows(t, f)) {
					t.Fatal("restart/retry changed ownership", got, err)
				}
			}
			if tokenPublishedSnapshot(t, f) != before {
				t.Fatal("repeat turn changed financial state")
			}
		})
	}
}

func TestUpstreamTokenAdvanceMissingProofDoesNotPartiallyBind(t *testing.T) {
	for _, provider := range []string{upstreamProviderSub2API, upstreamProviderNewAPI} {
		for _, mode := range []string{"missing", "one-scan", "unknown-token", "bad-key", "readonly", "disabled", "wrong-domain", "canceled"} {
			t.Run(provider+"/"+mode, func(t *testing.T) {
				f, now := tokenAdvanceFixture(t, provider)
				ctx := context.Background()
				sql := ""
				switch mode {
				case "missing":
					if provider == upstreamProviderSub2API {
						sql = "DELETE FROM channel_upstream_token_scan_checkpoints"
					} else {
						sql = "DELETE FROM channel_upstream_cost_hour_states"
					}
				case "one-scan":
					if provider == upstreamProviderSub2API {
						sql = "UPDATE channel_upstream_token_scan_checkpoints SET stable_scans=1"
					} else {
						sql = "UPDATE channel_upstream_cost_hour_states SET verified_scans=1"
					}
				case "unknown-token":
					if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, 186, "999", 1), "test", now); err != nil {
						t.Fatal(err)
					}
				case "bad-key":
					f.m.cfg.ChannelCostHMACKey = "different-test-key-0123456789abcdef0123456789"
				case "readonly":
					f.m.cfg.LocalSnapshotOnly = true
				case "disabled":
					f.m.cfg.ChannelCostClosureEnabled = false
				case "wrong-domain":
					f.m.cfg.ChannelCostClosureDomains = []string{"other.example"}
				case "canceled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				if sql != "" {
					if err := f.m.storeDB.Exec(sql).Error; err != nil {
						t.Fatal(err)
					}
				}
				before := tokenPublishedSnapshot(t, f)
				got, err := f.m.advanceUpstreamTokenBindings(ctx, f.account.Domain, 185, now)
				if err == nil && got.Status != "waiting_evidence" && got.Status != "disabled" {
					t.Fatal("invalid proof scheduled binding", got)
				}
				if mode == "readonly" && !errors.Is(err, errUpstreamTokenReadOnly) {
					t.Fatal("readonly guard missing", err)
				}
				if len(tokenBindingStoredRows(t, f)) != 0 || tokenPublishedSnapshot(t, f) != before {
					t.Fatal("failed account turn changed ownership or money")
				}
			})
		}
	}
}

func TestUpstreamTokenAdvanceSharedThenClearWithoutEvidence(t *testing.T) {
	for _, provider := range []string{upstreamProviderSub2API, upstreamProviderNewAPI} {
		t.Run(provider, func(t *testing.T) {
			f, now := tokenAdvanceFixture(t, provider)
			ctx := context.Background()
			if _, err := f.m.advanceUpstreamTokenBindings(ctx, f.account.Domain, 185, now); err != nil {
				t.Fatal(err)
			}
			old := tokenBindingStoredRows(t, f)
			if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, 186, "575", 1), "test", now+3600); err != nil {
				t.Fatal(err)
			}
			got, err := f.m.advanceUpstreamTokenBindings(ctx, f.account.Domain, 185, now+3600)
			if err != nil || got.Status != "scheduled" || got.ChangedSources != 2 {
				t.Fatal("shared activation", got, err)
			}
			var shared int
			for _, b := range tokenBindingStoredRows(t, f) {
				if b.ValidTo == 0 && b.AllocationMode == "shared" && b.LocalChannelID == 0 {
					shared++
				}
			}
			if shared != 1 {
				t.Fatal("shared source duplicated", shared)
			}
			for i, id := range []int{185, 186} {
				if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, id, "", int64(i+1)), "test", now+7200); err != nil {
					t.Fatal(err)
				}
			}
			// Clearing only needs the saved intent and existing mapping, not a
			// working upstream or a fresh evidence table.
			table := "channel_upstream_token_scan_checkpoints"
			if provider == upstreamProviderNewAPI {
				table = "channel_upstream_pricing_page_checkpoints"
			}
			if err := f.m.storeDB.Exec("DROP TABLE " + table).Error; err != nil {
				t.Fatal(err)
			}
			before := tokenPublishedSnapshot(t, f)
			got, err = f.m.advanceUpstreamTokenBindings(ctx, f.account.Domain, 185, now+7200)
			if err != nil || got.Status != "scheduled" || got.ChangedSources != 1 {
				t.Fatal("clear required unavailable proof", got, err)
			}
			for _, b := range tokenBindingStoredRows(t, f) {
				if b.ValidTo == 0 && b.AllocationMode != "unallocated" {
					t.Fatal("clear retained active ownership")
				}
				for _, earlier := range old {
					if b.SourceRef == earlier.SourceRef && b.ValidFrom == earlier.ValidFrom &&
						(b.LocalChannelID != earlier.LocalChannelID || b.ValidTo != (now/3600+2)*3600) {
						t.Fatal("old interval rewritten instead of closed in future")
					}
				}
			}
			if tokenPublishedSnapshot(t, f) != before {
				t.Fatal("clearing rewrote money or queued history")
			}
		})
	}
}

func TestUpstreamTokenAdvanceRollbackAndRetry(t *testing.T) {
	f, now := tokenAdvanceFixture(t, upstreamProviderSub2API)
	ctx := context.Background()
	before := tokenPublishedSnapshot(t, f)
	if err := f.m.storeDB.Exec(`CREATE TRIGGER reject_second_auto_binding BEFORE INSERT ON channel_cost_source_bindings WHEN (SELECT COUNT(*) FROM channel_cost_source_bindings)>0 BEGIN SELECT RAISE(ABORT,'synthetic interrupted turn'); END`).Error; err != nil {
		t.Fatal(err)
	}
	got, err := f.m.advanceUpstreamTokenBindings(ctx, f.account.Domain, 185, now)
	if err == nil || got != (upstreamTokenAdvanceResult{}) || len(tokenBindingStoredRows(t, f)) != 0 || tokenPublishedSnapshot(t, f) != before {
		t.Fatal("interrupted turn left partial bindings or costs", got, err)
	}
	if err := f.m.storeDB.Exec("DROP TRIGGER reject_second_auto_binding").Error; err != nil {
		t.Fatal(err)
	}
	f.m = &Monitor{storeDB: f.m.storeDB, cfg: f.m.cfg}
	got, err = f.m.advanceUpstreamTokenBindings(ctx, f.account.Domain, 185, now+1)
	if err != nil || got.Status != "scheduled" || got.ChangedSources != 2 || tokenPublishedSnapshot(t, f) != before {
		t.Fatal("restart retry did not converge safely", got, err)
	}
}

func TestUpstreamTokenAdvanceLegacyBlankAndUnsupportedNoKeyRequired(t *testing.T) {
	m, account := tokenConfigFixture(t)
	m.cfg.ChannelCostClosureEnabled = true
	m.cfg.ChannelCostClosureDomains = []string{account.Domain}
	now := cstDayStart(1787541630) + 86405
	ctx := context.Background()
	got, err := m.advanceUpstreamTokenBindings(ctx, account.Domain, 185, now)
	if err != nil || got.Status != "unconfigured" {
		t.Fatal("blank legacy setting required proof/key", got, err)
	}
	account.UsageAdapter = upstreamUsageAdapterSub2Stats
	if err := m.storeDB.Save(&account).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(account, 185, "575", 0), "test", now); err != nil {
		t.Fatal(err)
	}
	got, err = m.advanceUpstreamTokenBindings(ctx, account.Domain, 185, now)
	if err != nil || got.Status != "unsupported" {
		t.Fatal("unsupported provider changed legacy path", got, err)
	}
}
