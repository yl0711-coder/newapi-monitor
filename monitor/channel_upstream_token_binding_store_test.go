package monitor

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func tokenBindingStoreFixture(t *testing.T) (tokenReconcileFixture, upstreamTokenBindingCommitInput, int64) {
	t.Helper()
	f, _ := tokenCostStoreFixture(t, -1)
	if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 186, "574", 0), "fixture", f.day-1); err != nil {
		t.Fatal(err)
	}
	now := f.day + 86405
	return f, tokenBindingStoreInput(t, f, now), now
}

func tokenBindingStoreInput(t *testing.T, f tokenReconcileFixture, now int64) upstreamTokenBindingCommitInput {
	t.Helper()
	p, err := f.m.previewUpstreamTokenBindingPlan(context.Background(), f.account.Domain, 185, now)
	if err != nil {
		t.Fatal(err)
	}
	return upstreamTokenBindingCommitInput{Domain: f.account.Domain, ChannelID: 185, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), PlanDigest: p.Digest, ProofDayTs: f.day, ProofCursorRevision: f.cursor.Revision, ProofSuccessHash: f.cursor.SuccessHash}
}

func tokenBindingStoredRows(t *testing.T, f tokenReconcileFixture) []ChannelCostSourceBinding {
	t.Helper()
	var rows []ChannelCostSourceBinding
	if err := f.m.storeDB.Order("source_ref,valid_from").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

func tokenBindingAssertNoMoneyWrites(t *testing.T, f tokenReconcileFixture) {
	t.Helper()
	for _, model := range []any{&ChannelUpstreamCostHourEvidence{}, &ChannelUpstreamCostHourState{}, &ChannelEconomicsDirtyHour{}, &ChannelCostDirtyHour{}, &ChannelFinanceVersion{}, &ChannelEconomicsHourPublication{}} {
		tokenCostAssertCount(t, f, model, 0)
	}
}

func TestUpstreamTokenBindingStoreActivationRepeatAndNoFinancialWrites(t *testing.T) {
	f, in, now := tokenBindingStoreFixture(t)
	ctx := context.Background()
	got, err := f.m.commitSub2TokenBindingPlan(ctx, in, "local-tester", now)
	if err != nil || got.ChangedSources != 2 || got.EffectiveFrom != f.day+90000 {
		t.Fatalf("%+v %v", got, err)
	}
	before := tokenBindingStoredRows(t, f)
	if len(before) != 2 {
		t.Fatal("wrong source count")
	}
	for _, b := range before {
		if b.CreatedBy != "local-tester" || b.MappingSource != upstreamTokenBindingMappingSource || b.ValidFrom <= now || b.AllocationMode != "allocated" {
			t.Fatal("invalid ownership", b)
		}
	}
	// An ambiguous old submission is rejected, not replayed as another write.
	got, err = f.m.commitSub2TokenBindingPlan(ctx, in, "local-tester", now+1)
	if !errors.Is(err, errUpstreamTokenConflict) || got != (upstreamTokenBindingCommitResult{}) {
		t.Fatalf("stale retry %+v %v", got, err)
	}
	refresh := tokenBindingStoreInput(t, f, now+1)
	got, err = f.m.commitSub2TokenBindingPlan(ctx, refresh, "local-tester", now+1)
	if err != nil || got.ChangedSources != 0 {
		t.Fatalf("refreshed repeat %+v %v", got, err)
	}
	if !reflect.DeepEqual(before, tokenBindingStoredRows(t, f)) {
		t.Fatal("repeat churned ownership")
	}
	tokenBindingAssertNoMoneyWrites(t, f)
	// The complete pricing evidence remains present; binding is not billing.
	tokenCostAssertCount(t, f, &ChannelUpstreamPricingHourEvidence{}, int64(len(f.rows)))
}

func TestUpstreamTokenBindingStoreSharedThenClearPreservesHistoricalOwners(t *testing.T) {
	f, in, now := tokenBindingStoreFixture(t)
	ctx := context.Background()
	commit := func(at int64) {
		t.Helper()
		if _, err := f.m.commitSub2TokenBindingPlan(ctx, tokenBindingStoreInput(t, f, at), "tester", at); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.m.commitSub2TokenBindingPlan(ctx, in, "tester", now); err != nil {
		t.Fatal(err)
	}
	change := func(channel int, token string, rev, at int64) {
		t.Helper()
		if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, channel, token, rev), "tester", at); err != nil {
			t.Fatal(err)
		}
	}
	change(186, "575", 1, now+3600)
	commit(now + 3600)
	change(185, "", 1, now+7200)
	commit(now + 7200)
	change(186, "", 2, now+10800)
	clear := tokenBindingStoreInput(t, f, now+10800)
	clear.ProofDayTs, clear.ProofCursorRevision, clear.ProofSuccessHash = 0, 0, ""
	// No dependency on an available upstream scan for explicit clearing.
	if err := f.m.storeDB.Where("domain=?", f.account.Domain).Delete(&ChannelUpstreamTokenScanCheckpoint{}).Error; err != nil {
		t.Fatal(err)
	}
	if got, err := f.m.commitSub2TokenBindingPlan(ctx, clear, "tester", now+10800); err != nil || got.ChangedSources != 1 {
		t.Fatalf("clear %+v %v", got, err)
	}
	source, _ := channelCostSourceRef(f.key, f.account.Provider, in.AccountEpoch, sub2TokenSourceKind, "575")
	for _, want := range []struct {
		hour    int64
		mode    string
		channel int
	}{{f.day + 90000, "allocated", 185}, {f.day + 93600, "shared", 0}, {f.day + 97200, "allocated", 186}, {f.day + 100800, "unallocated", 0}} {
		b, err := f.m.costSourceBindingAt(ctx, f.account.Domain, in.AccountEpoch, source, want.hour)
		if err != nil || b.AllocationMode != want.mode || b.LocalChannelID != want.channel {
			t.Fatalf("hour %d: %+v %v", want.hour, b, err)
		}
	}
	rows := tokenBindingStoredRows(t, f)
	for i, b := range rows {
		if i > 0 && rows[i-1].SourceRef == b.SourceRef && rows[i-1].ValidTo != b.ValidFrom {
			t.Fatal("overlap or gap", rows)
		}
	}
	tokenBindingAssertNoMoneyWrites(t, f)
}

func TestUpstreamTokenBindingStorePendingSwapAndAtomicRollback(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprint(active), func(t *testing.T) {
			f, in, now := tokenBindingStoreFixture(t)
			ctx := context.Background()
			if _, err := f.m.commitSub2TokenBindingPlan(ctx, in, "tester", now); err != nil {
				t.Fatal(err)
			}
			before := tokenBindingStoredRows(t, f)
			at := now + 1
			if active {
				at += 3600
			}
			for _, s := range []struct {
				ch    int
				token string
			}{{185, "574"}, {186, "575"}} {
				if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, s.ch, s.token, 1), "tester", at); err != nil {
					t.Fatal(err)
				}
			}
			in = tokenBindingStoreInput(t, f, at)
			p, err := f.m.previewUpstreamTokenBindingPlan(ctx, f.account.Domain, 185, at)
			if err != nil || len(p.Changes) != 2 {
				t.Fatal("invalid swap", p, err)
			}
			trigger := fmt.Sprintf("CREATE TRIGGER reject_binding_swap BEFORE INSERT ON channel_cost_source_bindings WHEN NEW.source_ref='%s' BEGIN SELECT RAISE(ABORT,'injected second insert failure'); END", p.Changes[1].After.SourceRef)
			if err := f.m.storeDB.Exec(trigger).Error; err != nil {
				t.Fatal(err)
			}
			got, err := f.m.commitSub2TokenBindingPlan(ctx, in, "tester", at)
			if err == nil || !strings.Contains(err.Error(), "injected second insert failure") || got != (upstreamTokenBindingCommitResult{}) {
				t.Fatalf("%+v %v", got, err)
			}
			if !reflect.DeepEqual(before, tokenBindingStoredRows(t, f)) {
				t.Fatal("partial swap survived rollback")
			}
			if err := f.m.storeDB.Exec("DROP TRIGGER reject_binding_swap").Error; err != nil {
				t.Fatal(err)
			}
			got, err = f.m.commitSub2TokenBindingPlan(ctx, in, "tester", at)
			if err != nil || got.ChangedSources != 2 {
				t.Fatalf("retry after rollback %+v %v", got, err)
			}
			rows := tokenBindingStoredRows(t, f)
			want := 2
			if active {
				want = 4
			}
			if len(rows) != want {
				t.Fatal("pending interval duplicated", len(rows), want)
			}
			for _, old := range before {
				b, err := f.m.costSourceBindingAt(ctx, old.Domain, old.AccountEpoch, old.SourceRef, got.EffectiveFrom)
				if err != nil || b.LocalChannelID == old.LocalChannelID {
					t.Fatal("swap did not change owner", b, err)
				}
			}
			tokenBindingAssertNoMoneyWrites(t, f)
		})
	}
}

func TestUpstreamTokenBindingStoreRejectsStaleOrUnprovenInput(t *testing.T) {
	for _, name := range []string{"config", "another-channel", "account", "hour-boundary", "cursor", "hash", "key", "control", "missing-state", "one-scan", "unseen-second-token", "old-day", "readonly", "disabled", "cancelled", "actor"} {
		t.Run(name, func(t *testing.T) {
			f, in, now := tokenBindingStoreFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			actor := "tester"
			exec := func(sql string, args ...any) {
				t.Helper()
				if err := f.m.storeDB.Exec(sql, args...).Error; err != nil {
					t.Fatal(err)
				}
			}
			switch name {
			case "config":
				if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, 185, "574", 1), "tester", now); err != nil {
					t.Fatal(err)
				}
			case "another-channel":
				c := ChannelSnap{ID: 188, BaseDomain: f.account.Domain}
				if err := f.m.storeDB.Create(&c).Error; err != nil {
					t.Fatal(err)
				}
				if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, 188, "575", 0), "tester", now); err != nil {
					t.Fatal(err)
				}
			case "account":
				exec("UPDATE channel_upstream_accounts SET account='changed'")
			case "hour-boundary":
				now += 3600
			case "cursor":
				exec("UPDATE channel_upstream_token_scan_checkpoints SET success_revision=0")
			case "hash":
				in.ProofSuccessHash = strings.Repeat("a", 64)
			case "key":
				exec("UPDATE channel_cost_key_registry SET key_check='changed'")
			case "control":
				exec("UPDATE channel_upstream_usage_hours SET requests=requests+1")
			case "missing-state":
				if err := f.m.storeDB.Where("hour_ts=?", f.day).Delete(&ChannelUpstreamPricingHourState{}).Error; err != nil {
					t.Fatal(err)
				}
			case "one-scan":
				exec("UPDATE channel_upstream_token_scan_checkpoints SET stable_scans=1")
			case "unseen-second-token":
				if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, 186, "999", 1), "tester", now); err != nil {
					t.Fatal(err)
				}
				in = tokenBindingStoreInput(t, f, now)
			case "old-day":
				in.ProofDayTs -= 86400
			case "readonly":
				f.m.cfg.LocalSnapshotOnly = true
			case "disabled":
				f.m.cfg.ChannelCostClosureEnabled = false
			case "cancelled":
				cancel()
			case "actor":
				actor = ""
			}
			got, err := f.m.commitSub2TokenBindingPlan(ctx, in, actor, now)
			if err == nil || got != (upstreamTokenBindingCommitResult{}) {
				t.Fatalf("accepted %s: %+v %v", name, got, err)
			}
			tokenCostAssertCount(t, f, &ChannelCostSourceBinding{}, 0)
			tokenBindingAssertNoMoneyWrites(t, f)
		})
	}
}

func TestUpstreamTokenBindingStoreConcurrentSubmission(t *testing.T) {
	f, in, now := tokenBindingStoreFixture(t)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.m.commitSub2TokenBindingPlan(context.Background(), in, "tester", now)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	passed, conflicts := 0, 0
	for err := range errs {
		if err == nil {
			passed++
		} else if errors.Is(err, errUpstreamTokenConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if passed != 1 || conflicts != 1 {
		t.Fatal("unexpected concurrency", passed, conflicts)
	}
	tokenCostAssertCount(t, f, &ChannelCostSourceBinding{}, 2)
	tokenBindingAssertNoMoneyWrites(t, f)
}

func TestUpstreamTokenBindingStoreDoesNotAttributeEarlierCost(t *testing.T) {
	f, in, now := tokenBindingStoreFixture(t)
	ctx := context.Background()
	if _, err := f.m.commitSub2TokenBindingPlan(ctx, in, "tester", now); err != nil {
		t.Fatal(err)
	}
	costInput := sub2TokenCostCommitInput{Domain: in.Domain, ChannelID: 185, AccountEpoch: in.AccountEpoch, ConfigRevision: 1,
		DayTs: f.day, CursorRevision: f.cursor.Revision, SuccessHash: f.cursor.SuccessHash}
	got, err := f.m.commitSub2TokenCostCandidates(ctx, costInput, now)
	if err != nil || got.UpdatedHours != 24 || got.QueuedHours != 0 {
		t.Fatalf("historical cost queued by a future binding: %+v %v", got, err)
	}
	tokenCostAssertCount(t, f, &ChannelEconomicsDirtyHour{}, 0)
	tokenCostAssertCount(t, f, &ChannelEconomicsHourPublication{}, 0)
	tokenCostAssertCount(t, f, &ChannelFinanceVersion{}, 0)
	var rows []ChannelUpstreamCostHourEvidence
	if err := f.m.storeDB.Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	var amount int64
	for _, row := range rows {
		amount += row.ChargeUnits
		if _, err := f.m.costSourceBindingAt(ctx, row.Domain, row.AccountEpoch, row.SourceRef, row.HourTs); err == nil {
			t.Fatal("future binding applied to historical hour")
		}
	}
	if amount != 481479 {
		t.Fatal("binding changed account cost", amount)
	}
}

func TestUpstreamTokenBindingStoreUnchangedLegacyTokenDoesNotBlockAnotherSource(t *testing.T) {
	f, _, now := tokenBindingStoreFixture(t)
	ctx := context.Background()
	if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, 185, "999", 1), "tester", now-1); err != nil {
		t.Fatal(err)
	}
	source, err := channelCostSourceRef(f.key, f.account.Provider, newAPIUpstreamAccountEpoch(f.account), sub2TokenSourceKind, "999")
	if err != nil {
		t.Fatal(err)
	}
	old := ChannelCostSourceBinding{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), Provider: f.account.Provider,
		SourceRef: source, SourceRefKind: sub2TokenSourceKind, HMACKeyID: "v1", LocalChannelID: 185, ValidFrom: f.day, Status: "confirmed", AllocationMode: "allocated", MappingSource: "manual"}
	if err := f.m.saveCostSourceBinding(ctx, old); err != nil {
		t.Fatal(err)
	}
	// Compare with the actual persisted row, including GORM's creation time,
	// not the caller's value copy whose timestamp may still be zero.
	if err := f.m.storeDB.Where("source_ref=?", source).Take(&old).Error; err != nil {
		t.Fatal(err)
	}
	// 999 has no requests in the selected day, but its unchanged manual
	// mapping is not part of this write. Only the new 574 source needs proof.
	got, err := f.m.commitSub2TokenBindingPlan(ctx, tokenBindingStoreInput(t, f, now), "tester", now)
	if err != nil || got.ChangedSources != 1 {
		t.Fatalf("unrelated idle token blocked verified source: %+v %v", got, err)
	}
	var kept ChannelCostSourceBinding
	if err := f.m.storeDB.Where("source_ref=?", source).Take(&kept).Error; err != nil {
		t.Fatal(err)
	}
	if kept != old {
		t.Fatal("unrelated manual owner was changed")
	}
	tokenBindingAssertNoMoneyWrites(t, f)
}
