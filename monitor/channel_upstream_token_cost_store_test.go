package monitor

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func tokenCostStoreFixture(t *testing.T, offset int64) (tokenReconcileFixture, sub2TokenCostCommitInput) {
	t.Helper()
	f := newTokenReconcileFixture(t)
	f.m.cfg.ChannelCostClosureEnabled = true
	f.m.cfg.ChannelCostClosureDomains = []string{f.account.Domain}
	if err := f.m.validateChannelCostKeyRegistry(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 185, "575", 0), "fixture", f.day+offset); err != nil {
		t.Fatal(err)
	}
	for _, rows := range []any{&f.rows, &f.states, &f.controls} {
		if err := f.m.storeDB.Create(rows).Error; err != nil {
			t.Fatal(err)
		}
	}
	return f, sub2TokenCostCommitInput{Domain: f.account.Domain, ChannelID: 185, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), ConfigRevision: 1, DayTs: f.day, CursorRevision: f.cursor.Revision, SuccessHash: f.cursor.SuccessHash}
}

func tokenCostAssertCount(t *testing.T, f tokenReconcileFixture, model any, want int64) {
	t.Helper()
	var n int64
	if err := f.m.storeDB.Model(model).Count(&n).Error; err != nil || n != want {
		t.Fatalf("%T count=%d want=%d err=%v", model, n, want, err)
	}
}

func tokenCostTestBinding(t *testing.T, f tokenReconcileFixture) {
	t.Helper()
	ref, err := channelCostSourceRef(f.key, f.account.Provider, newAPIUpstreamAccountEpoch(f.account), sub2TokenSourceKind, "575")
	if err != nil {
		t.Fatal(err)
	}
	// Explicit test fixture, not automatic ownership inferred by the commit.
	b := ChannelCostSourceBinding{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), SourceRef: ref, ValidFrom: f.day, Provider: f.account.Provider, SourceRefKind: sub2TokenSourceKind, HMACKeyID: "v1", LocalChannelID: 185, Status: "confirmed", AllocationMode: "allocated"}
	if err := f.m.saveCostSourceBinding(context.Background(), b); err != nil {
		t.Fatal(err)
	}
}

func TestSub2TokenCostStoreIdempotentAndSharedWithoutActivation(t *testing.T) {
	f, in := tokenCostStoreFixture(t, -1)
	ctx := context.Background()
	got, err := f.m.commitSub2TokenCostCandidates(ctx, in, f.day+86400)
	if err != nil || got != (sub2TokenCostCommitResult{UpdatedHours: 24}) {
		t.Fatalf("%+v %v", got, err)
	}
	var before []ChannelUpstreamCostHourState
	if err := f.m.storeDB.Order("hour_ts").Find(&before).Error; err != nil {
		t.Fatal(err)
	}
	var rows []ChannelUpstreamCostHourEvidence
	if err := f.m.storeDB.Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	var amount int64
	for _, row := range rows {
		amount += row.ChargeUnits
	}
	if amount != 481479 || len(rows) != 3 {
		t.Fatalf("money=%d rows=%d", amount, len(rows))
	}
	if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, 186, "575", 0), "fixture", f.day-1); err != nil {
		t.Fatal(err)
	}
	for _, channel := range []int{185, 186} {
		in.ChannelID = channel
		got, err = f.m.commitSub2TokenCostCandidates(ctx, in, f.day+86401)
		if err != nil || got != (sub2TokenCostCommitResult{UnchangedHours: 24}) {
			t.Fatalf("repeat %+v %v", got, err)
		}
	}
	var after []ChannelUpstreamCostHourState
	if err := f.m.storeDB.Order("hour_ts").Find(&after).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("retry rewrote states/timestamps")
	}
	tokenCostAssertCount(t, f, &ChannelUpstreamCostHourEvidence{}, 3)
	for _, model := range []any{&ChannelEconomicsDirtyHour{}, &ChannelCostSourceBinding{}, &ChannelFinanceVersion{}, &ChannelEconomicsHourPublication{}} {
		tokenCostAssertCount(t, f, model, 0)
	}
	var pricing []ChannelUpstreamPricingHourEvidence
	if err := f.m.storeDB.Find(&pricing).Error; err != nil {
		t.Fatal(err)
	}
	if len(pricing) != len(f.rows) {
		t.Fatal("source evidence changed")
	}
	for _, old := range f.rows {
		found := false
		for _, now := range pricing {
			if reflect.DeepEqual(old, now) {
				found = true
			}
		}
		if !found {
			t.Fatal("source evidence mutated")
		}
	}
}

func TestSub2TokenCostStoreNeverBackdatesConfiguration(t *testing.T) {
	for _, tc := range []struct {
		offset int64
		skip   int
	}{{10*3600 + 5, 11}, {86400, 24}} {
		t.Run(fmt.Sprint(tc.offset), func(t *testing.T) {
			f, in := tokenCostStoreFixture(t, tc.offset)
			got, err := f.m.commitSub2TokenCostCandidates(context.Background(), in, f.day+86401)
			if err != nil || got.SkippedHours != tc.skip || got.UpdatedHours != 24-tc.skip {
				t.Fatalf("%+v %v", got, err)
			}
			var before int64
			if err := f.m.storeDB.Model(&ChannelUpstreamCostHourState{}).Where("hour_ts<?", f.day+int64(tc.skip)*3600).Count(&before).Error; err != nil || before != 0 {
				t.Fatal("backdated", before, err)
			}
		})
	}
}

func TestSub2TokenCostStoreRejectsChangedEvidenceAndConfiguration(t *testing.T) {
	for _, name := range []string{"config", "account", "cursor", "hash", "key", "missing-control", "changed-pricing", "readonly", "disabled", "cancelled", "rounding"} {
		t.Run(name, func(t *testing.T) {
			f, in := tokenCostStoreFixture(t, -1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			exec := func(sql string, args ...any) {
				t.Helper()
				if err := f.m.storeDB.Exec(sql, args...).Error; err != nil {
					t.Fatal(err)
				}
			}
			switch name {
			case "config":
				in.ConfigRevision++
			case "account":
				exec("UPDATE channel_upstream_accounts SET account='replaced' WHERE domain=?", f.account.Domain)
			case "cursor":
				in.CursorRevision++
			case "hash":
				in.SuccessHash = strings.Repeat("a", 64)
			case "key":
				exec("UPDATE channel_cost_key_registry SET key_check='bad'")
			case "missing-control":
				if err := f.m.storeDB.Where("hour_ts=?", f.day).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
					t.Fatal(err)
				}
			case "changed-pricing":
				exec("UPDATE channel_upstream_pricing_hour_evidence SET final_quota=final_quota+1")
			case "readonly":
				f.m.cfg.LocalSnapshotOnly = true
			case "disabled":
				f.m.cfg.ChannelCostClosureEnabled = false
			case "cancelled":
				cancel()
			case "rounding":
				for _, s := range f.states {
					if s.EligibleRequests == 2 {
						if err := f.m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("hour_ts=?", s.HourTs).Update("cost_usd", float64(s.FinalQuota-1)/1e6).Error; err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			got, err := f.m.commitSub2TokenCostCandidates(ctx, in, f.day+86400)
			if err == nil || got != (sub2TokenCostCommitResult{}) {
				t.Fatalf("accepted invalid state %+v %v", got, err)
			}
			for _, model := range []any{&ChannelUpstreamCostHourState{}, &ChannelUpstreamCostHourEvidence{}, &ChannelEconomicsDirtyHour{}} {
				tokenCostAssertCount(t, f, model, 0)
			}
		})
	}
}

func TestSub2TokenCostStoreAtomicRollback(t *testing.T) {
	for _, queue := range []bool{false, true} {
		t.Run(fmt.Sprint(queue), func(t *testing.T) {
			f, in := tokenCostStoreFixture(t, -1)
			tokenCostTestBinding(t, f)
			table := (ChannelUpstreamCostHourState{}).TableName()
			if queue {
				table = (ChannelEconomicsDirtyHour{}).TableName()
			}
			trigger := fmt.Sprintf("CREATE TRIGGER reject_cost_write BEFORE INSERT ON %s WHEN NEW.hour_ts=%d BEGIN SELECT RAISE(ABORT,'injected failure'); END", table, f.day+11*3600)
			if err := f.m.storeDB.Exec(trigger).Error; err != nil {
				t.Fatal(err)
			}
			got, err := f.m.commitSub2TokenCostCandidates(context.Background(), in, f.day+86400)
			if err == nil || !strings.Contains(err.Error(), "injected failure") || got != (sub2TokenCostCommitResult{}) {
				t.Fatalf("%+v %v", got, err)
			}
			for _, model := range []any{&ChannelUpstreamCostHourState{}, &ChannelUpstreamCostHourEvidence{}, &ChannelEconomicsDirtyHour{}} {
				tokenCostAssertCount(t, f, model, 0)
			}
			tokenCostAssertCount(t, f, &ChannelCostSourceBinding{}, 1)
		})
	}
}

func TestSub2TokenCostStoreBoundQueueAndCorruptionRepair(t *testing.T) {
	f, in := tokenCostStoreFixture(t, -1)
	tokenCostTestBinding(t, f)
	ctx := context.Background()
	got, err := f.m.commitSub2TokenCostCandidates(ctx, in, f.day+86400)
	if err != nil || got != (sub2TokenCostCommitResult{UpdatedHours: 24, QueuedHours: 24}) {
		t.Fatalf("%+v %v", got, err)
	}
	got, err = f.m.commitSub2TokenCostCandidates(ctx, in, f.day+86401)
	if err != nil || got != (sub2TokenCostCommitResult{UnchangedHours: 24}) {
		t.Fatalf("%+v %v", got, err)
	}
	if err := f.m.storeDB.Exec("UPDATE channel_upstream_cost_hour_evidence SET charge_units=charge_units+1").Error; err != nil {
		t.Fatal(err)
	}
	got, err = f.m.commitSub2TokenCostCandidates(ctx, in, f.day+86402)
	if err != nil || got != (sub2TokenCostCommitResult{UpdatedHours: 2, QueuedHours: 2, UnchangedHours: 22}) {
		t.Fatalf("corruption not repaired %+v %v", got, err)
	}
	var queues []ChannelEconomicsDirtyHour
	if err := f.m.storeDB.Find(&queues).Error; err != nil {
		t.Fatal(err)
	}
	var generations int64
	for _, q := range queues {
		generations += q.Generation
	}
	if len(queues) != 24 || generations != 26 {
		t.Fatalf("retry caused queue churn %d %d", len(queues), generations)
	}
	tokenCostAssertCount(t, f, &ChannelCostSourceBinding{}, 1)
}

func TestSub2TokenCostStoreConcurrentRepeat(t *testing.T) {
	f, in := tokenCostStoreFixture(t, -1)
	var wg sync.WaitGroup
	results := make(chan sub2TokenCostCommitResult, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := f.m.commitSub2TokenCostCandidates(context.Background(), in, f.day+86400)
			results <- r
			errs <- e
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var updates, unchanged int
	for r := range results {
		updates += r.UpdatedHours
		unchanged += r.UnchangedHours
	}
	if updates != 24 || unchanged != 24 {
		t.Fatal("concurrent double commit", updates, unchanged)
	}
	tokenCostAssertCount(t, f, &ChannelUpstreamCostHourEvidence{}, 3)
}

func TestSub2TokenCostStoreClearsPreviouslyNonzeroHourAndQueuesIt(t *testing.T) {
	f, in := tokenCostStoreFixture(t, -1)
	tokenCostTestBinding(t, f)
	candidates := tokenCostCandidates(t, f, "575")
	// Fabricate an old nonzero hour which the new complete scan says is empty.
	old := candidates[0].State
	old.EvidenceChargeUnits, old.EvidenceRows = 99, 1
	var row ChannelUpstreamCostHourEvidence
	for _, c := range candidates {
		if len(c.Rows) > 0 {
			row = c.Rows[0]
			break
		}
	}
	row.HourTs, row.ChargeUnits = f.day, 99
	row.ContentHash = channelCostEvidenceRowHash(row)
	old.ContentHash = channelCostEvidenceContentHash([]ChannelUpstreamCostHourEvidence{row})
	if err := f.m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	got, err := f.m.commitSub2TokenCostCandidates(context.Background(), in, f.day+86400)
	if err != nil || got.QueuedHours != 24 {
		t.Fatalf("%+v %v", got, err)
	}
	var current ChannelUpstreamCostHourState
	if err := f.m.storeDB.Where("hour_ts=?", f.day).Take(&current).Error; err != nil {
		t.Fatal(err)
	}
	if current.EvidenceChargeUnits != 0 || current.EvidenceRows != 0 {
		t.Fatal("old money survived", current)
	}
	var queue ChannelEconomicsDirtyHour
	if err := f.m.storeDB.Where("hour_ts=?", f.day).Take(&queue).Error; err != nil {
		t.Fatal("zero hour not queued", err)
	}
	tokenCostAssertCount(t, f, &ChannelUpstreamCostHourEvidence{}, 3)
}

func TestSub2TokenCostStoreRollbackPreservesExistingMoney(t *testing.T) {
	f, in := tokenCostStoreFixture(t, -1)
	ctx := context.Background()
	if _, err := f.m.commitSub2TokenCostCandidates(ctx, in, f.day+86400); err != nil {
		t.Fatal(err)
	}
	// Force replacement, then abort after an earlier hour has already changed.
	if err := f.m.storeDB.Exec("UPDATE channel_upstream_cost_hour_evidence SET charge_units=charge_units+1").Error; err != nil {
		t.Fatal(err)
	}
	var before []ChannelUpstreamCostHourEvidence
	if err := f.m.storeDB.Order("hour_ts,dimension_hash").Find(&before).Error; err != nil {
		t.Fatal(err)
	}
	var beforeStates []ChannelUpstreamCostHourState
	if err := f.m.storeDB.Order("hour_ts").Find(&beforeStates).Error; err != nil {
		t.Fatal(err)
	}
	trigger := fmt.Sprintf("CREATE TRIGGER reject_replacement BEFORE INSERT ON channel_upstream_cost_hour_evidence WHEN NEW.hour_ts=%d BEGIN SELECT RAISE(ABORT,'replacement failure'); END", f.day+11*3600)
	if err := f.m.storeDB.Exec(trigger).Error; err != nil {
		t.Fatal(err)
	}
	got, err := f.m.commitSub2TokenCostCandidates(ctx, in, f.day+86401)
	if err == nil || !strings.Contains(err.Error(), "replacement failure") || got != (sub2TokenCostCommitResult{}) {
		t.Fatalf("%+v %v", got, err)
	}
	var after []ChannelUpstreamCostHourEvidence
	if err := f.m.storeDB.Order("hour_ts,dimension_hash").Find(&after).Error; err != nil {
		t.Fatal(err)
	}
	var afterStates []ChannelUpstreamCostHourState
	if err := f.m.storeDB.Order("hour_ts").Find(&afterStates).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(beforeStates, afterStates) {
		t.Fatal("failed transaction modified previously stored money")
	}
}
