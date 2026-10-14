package monitor

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func tokenPipelineStore(t *testing.T, f tokenReconcileFixture) {
	t.Helper()
	if err := f.m.storeDB.AutoMigrate(&ChannelUpstreamTokenWorkState{}); err != nil {
		t.Fatal(err)
	}
}

func tokenPipelineState(t *testing.T, f tokenReconcileFixture, kind string, day int64, channel int) ChannelUpstreamTokenWorkState {
	t.Helper()
	var state ChannelUpstreamTokenWorkState
	if err := upstreamTokenWorkScope(f.m.storeDB, ChannelUpstreamTokenWorkState{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), Kind: kind, DayTs: day, ChannelID: channel}).Take(&state).Error; err != nil {
		t.Fatal(err)
	}
	return state
}

func TestUpstreamTokenPipelineActivationIsFutureOnlyAndBounded(t *testing.T) {
	for _, provider := range []string{upstreamProviderNewAPI, upstreamProviderSub2API} {
		t.Run(provider, func(t *testing.T) {
			f, now := tokenAdvanceFixture(t, provider)
			tokenPipelineStore(t, f)
			money := tokenPublishedSnapshot(t, f)
			f.m.syncDueUpstreamTokenWorkAt(context.Background(), now)
			bindings := tokenBindingStoredRows(t, f)
			if len(bindings) != 2 {
				t.Fatal("activation not dispatched", len(bindings))
			}
			for _, b := range bindings {
				if b.ValidFrom != (now/3600+1)*3600 {
					t.Fatal("backdated activation", b.ValidFrom)
				}
			}
			s := tokenPipelineState(t, f, "binding", 0, 0)
			if s.Status != "scheduled" || s.LeaseUntil != 0 || s.Attempts != 0 || s.NextAttemptAt != now+60 {
				t.Fatal(s)
			}
			f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
			f.m.syncDueUpstreamTokenWorkAt(context.Background(), now+1)
			if s != tokenPipelineState(t, f, "binding", 0, 0) || !reflect.DeepEqual(bindings, tokenBindingStoredRows(t, f)) {
				t.Fatal("restart bypassed cooldown or changed ownership")
			}
			if tokenPublishedSnapshot(t, f) != money {
				t.Fatal("activation rewrote preexisting costs or money")
			}
		})
	}
}

func TestSub2TokenPipelineLateCostAfterClearingAllIDs(t *testing.T) {
	f, now := sub2TokenDayFixture(t, false)
	tokenPipelineStore(t, f)
	for _, channel := range []int{185, 186} {
		if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, channel, "", 1), "test", now); err != nil {
			t.Fatal(err)
		}
	}
	// Historical proof day before activation is scanned but never billed.
	// Each turn prepares at most one channel-day and publishes at most 8 hours.
	for i := 0; i < 8; i++ {
		f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
		at := now + int64(i)*60
		f.m.syncDueUpstreamTokenWorkAt(context.Background(), at)
		f.m.syncDueChannelEconomicsAt(context.Background(), at)
	}
	tokenCostAssertCount(t, f, &ChannelUpstreamCostHourState{}, 23)
	tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 23)
	tokenCostAssertCount(t, f, &ChannelEconomicsDirtyHour{}, 0)
	for _, channel := range []int{185, 186} {
		s := tokenPipelineState(t, f, "cost_day", f.day, channel)
		if s.Status != "prepared" || s.Attempts != 0 || s.InputRevision != f.cursor.Revision {
			t.Fatal("historical task not completed", s)
		}
	}
	want := map[int]int64{185: 246912, 186: 234567}
	if got := tokenHistoryPublishAll(t, f, now+480, true); !reflect.DeepEqual(got, want) {
		t.Fatal("current clear destroyed historical allocation", got)
	}
	stable := tokenPublishedSnapshot(t, f)
	f.m.syncDueUpstreamTokenWorkAt(context.Background(), now+481)
	f.m.syncDueChannelEconomicsAt(context.Background(), now+481)
	if tokenPublishedSnapshot(t, f) != stable {
		t.Fatal("duplicate pipeline turn changed money")
	}
}

func TestSub2TokenPipelineFailureBackoffAndNewProofWake(t *testing.T) {
	f, now := sub2TokenDayFixture(t, false)
	tokenPipelineStore(t, f)
	// This fixture retains only the day being exercised, avoiding unrelated
	// pre-activation discovery turns. No production database is used.
	if err := f.m.storeDB.Where("day_ts<>?", f.day).Delete(&ChannelUpstreamTokenScanCheckpoint{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.Exec(`CREATE TRIGGER reject_pipeline_cost BEFORE INSERT ON channel_upstream_cost_hour_states BEGIN SELECT RAISE(ABORT,'synthetic failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	f.m.syncDueUpstreamTokenWorkAt(context.Background(), now)
	s := tokenPipelineState(t, f, "cost_day", f.day, 185)
	if s.Status != "retry" || s.Attempts != 1 || s.NextAttemptAt != now+60 || s.LeaseUntil != 0 {
		t.Fatal(s)
	}
	tokenCostAssertCount(t, f, &ChannelUpstreamCostHourState{}, 0)
	_, items, _, _ := sub2TokenFixture(t)
	for i := range items {
		items[i].CreatedAt += 86400
	}
	for i := 0; i < 2; i++ {
		f.cursor = tokenEconomicsRescan(t, f, items, now+1)
	}
	f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
	f.m.syncDueUpstreamTokenWorkAt(context.Background(), now+2)
	if s != tokenPipelineState(t, f, "cost_day", f.day, 185) {
		t.Fatal("new bad scan bypassed retry cooldown")
	}
	if err := f.m.storeDB.Exec("DROP TRIGGER reject_pipeline_cost").Error; err != nil {
		t.Fatal(err)
	}
	f.m.syncDueUpstreamTokenWorkAt(context.Background(), now+60)
	s = tokenPipelineState(t, f, "cost_day", f.day, 185)
	if s.Status != "prepared" || s.Attempts != 0 || s.NextAttemptAt != now+60+upstreamTokenCostRecheck {
		t.Fatal("did not recover", s)
	}
	tokenCostAssertCount(t, f, &ChannelUpstreamCostHourState{}, 23)
	// Successful jobs wake on a changed proof, without waiting six hours.
	for i := 0; i < 2; i++ {
		f.cursor = tokenEconomicsRescan(t, f, items, now+61)
	}
	for i := 0; i < 2; i++ {
		f.m.syncDueUpstreamTokenWorkAt(context.Background(), now+62+int64(i))
	}
	s = tokenPipelineState(t, f, "cost_day", f.day, 185)
	if s.Status != "prepared" || s.InputRevision != f.cursor.Revision || s.Revision <= 2 {
		t.Fatal("changed proof did not wake completed job", s)
	}
	tokenCostAssertCount(t, f, &ChannelUpstreamCostHourState{}, 23)
	tokenCostAssertCount(t, f, &ChannelEconomicsDirtyHour{}, 23)
}

func TestUpstreamTokenPipelineClaimRestartAndFencing(t *testing.T) {
	f, now := tokenAdvanceFixture(t, upstreamProviderSub2API)
	tokenPipelineStore(t, f)
	key := ChannelUpstreamTokenWorkState{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), Kind: "binding"}
	old, ok, err := f.m.claimUpstreamTokenWork(context.Background(), key, 0, now)
	if err != nil || !ok || old.Revision != 1 {
		t.Fatal(old, ok, err)
	}
	f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
	if _, ok, err := f.m.claimUpstreamTokenWork(context.Background(), key, 0, now+59); err != nil || ok {
		t.Fatal("unexpired task claimed twice", ok, err)
	}
	next, ok, err := f.m.claimUpstreamTokenWork(context.Background(), key, 0, now+60)
	if err != nil || !ok || next.Revision != 2 {
		t.Fatal("crashed job not reclaimed", next, ok, err)
	}
	if err := f.m.finishUpstreamTokenWork(context.Background(), old, 0, now+61, "unchanged", false); err == nil {
		t.Fatal("old worker acknowledged newer lease")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.m.finishUpstreamTokenWork(ctx, next, 0, now+61, "retry", true); err != nil {
		t.Fatal("cancelled work could not persist cooldown", err)
	}
	s := tokenPipelineState(t, f, "binding", 0, 0)
	if s.Revision != 2 || s.LeaseUntil != 0 || s.NextAttemptAt != now+121 || s.Attempts != 1 {
		t.Fatal(s)
	}
	tokenBindingAssertNoMoneyWrites(t, f)
}

func TestUpstreamTokenPipelineGuards(t *testing.T) {
	for _, kind := range []string{"readonly", "disabled", "not-allowed", "unsupported", "epoch", "cancelled", "unconfigured"} {
		t.Run(kind, func(t *testing.T) {
			f, now := tokenAdvanceFixture(t, upstreamProviderSub2API)
			tokenPipelineStore(t, f)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch kind {
			case "readonly":
				f.m.cfg.LocalSnapshotOnly = true
			case "disabled":
				f.m.cfg.ChannelCostClosureEnabled = false
			case "not-allowed":
				f.m.cfg.ChannelCostClosureDomains = []string{"other.example"}
			case "unsupported":
				if err := f.m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", f.account.Domain).Update("usage_adapter", upstreamUsageAdapterSub2Stats).Error; err != nil {
					t.Fatal(err)
				}
			case "epoch":
				if err := f.m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", f.account.Domain).Update("account", "new-account").Error; err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				cancel()
			case "unconfigured":
				if err := f.m.storeDB.Where("domain=?", f.account.Domain).Delete(&ChannelUpstreamTokenConfig{}).Error; err != nil {
					t.Fatal(err)
				}
			}
			f.m.syncDueUpstreamTokenWorkAt(ctx, now)
			tokenCostAssertCount(t, f, &ChannelUpstreamTokenWorkState{}, 0)
			tokenCostAssertCount(t, f, &ChannelCostSourceBinding{}, 0)
			tokenBindingAssertNoMoneyWrites(t, f)
		})
	}
}

func TestSub2TokenHistoricalShadowClosedWindowOnly(t *testing.T) {
	for _, kind := range []string{"historical", "cleared-before-day", "configured-too-late", "other-epoch"} {
		t.Run(kind, func(t *testing.T) {
			f, now := sub2TokenDayFixture(t, false)
			for _, channel := range []int{185, 186} {
				if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, channel, "", 1), "test", now); err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "cleared-before-day":
				if err := f.m.storeDB.Model(&ChannelUpstreamTokenConfig{}).Where("revision=2").Update("created_at", f.day-1).Error; err != nil {
					t.Fatal(err)
				}
			case "configured-too-late":
				if err := f.m.storeDB.Model(&ChannelUpstreamTokenConfig{}).Where("revision=1").Update("created_at", f.day+23*3600).Error; err != nil {
					t.Fatal(err)
				}
			case "other-epoch":
				if err := f.m.storeDB.Model(&ChannelUpstreamTokenConfig{}).Where("revision=1").Update("account_epoch", "old-account").Error; err != nil {
					t.Fatal(err)
				}
			}
			shadow := f.m.beginSub2TokenShadow(context.Background(), f.account, f.day, now, ChannelUpstreamPricingPageCheckpoint{}, false)
			if (shadow != nil) != (kind == "historical") || shadow != nil && shadow.failed {
				t.Fatal("historical observer eligibility mismatch", kind)
			}
			if kind == "historical" {
				_, items, _, _ := sub2TokenFixture(t)
				for i := range items {
					items[i].CreatedAt += 86400
				}
				page := sub2PricingPage{Page: 1, PageSize: 100, Pages: 1, Total: int64(len(items)), Items: items}
				shadow.observe(page)
				shadow.finish(sub2ObservationLegacy(t, f.account, items, f.day))
				if shadow.failed {
					t.Fatal("historical observation failed")
				}
				if later := f.m.beginSub2TokenShadow(context.Background(), f.account, f.day+2*86400, now+3*86400, ChannelUpstreamPricingPageCheckpoint{}, false); later != nil {
					t.Fatal("cleared setting kept future collection running")
				}
			}
			tokenCostAssertCount(t, f, &ChannelUpstreamCostHourState{}, 0)
			tokenCostAssertCount(t, f, &ChannelEconomicsDirtyHour{}, 0)
		})
	}
}

func TestUpstreamTokenPipelineStorageFailureDoesNotPublish(t *testing.T) {
	f, now := tokenAdvanceFixture(t, upstreamProviderSub2API)
	tokenPipelineStore(t, f)
	if err := f.m.storeDB.Exec(`CREATE TRIGGER reject_token_claim BEFORE UPDATE ON channel_upstream_token_work_states BEGIN SELECT RAISE(ABORT,'claim failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	key := ChannelUpstreamTokenWorkState{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), Kind: "binding"}
	called := false
	did, err := f.m.runUpstreamTokenWork(context.Background(), key, 0, now, func() (string, bool, error) {
		called = true
		return "scheduled", false, nil
	})
	if err == nil || did || called || errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("failed claim dispatched work", did, called, err)
	}
	tokenCostAssertCount(t, f, &ChannelUpstreamTokenWorkState{}, 0)
	tokenBindingAssertNoMoneyWrites(t, f)
}

func TestUpstreamTokenPipelineConfigChangeReactivatesCooldown(t *testing.T) {
	f, now := tokenAdvanceFixture(t, upstreamProviderSub2API)
	tokenPipelineStore(t, f)
	if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 186, "999", 1), "test", now); err != nil {
		t.Fatal(err)
	}
	f.m.syncDueUpstreamTokenWorkAt(context.Background(), now)
	old := tokenPipelineState(t, f, "binding", 0, 0)
	if old.Attempts != 1 || old.NextAttemptAt != now+60 {
		t.Fatal("missing source did not defer", old)
	}
	for channel, rev := range map[int]int64{185: 1, 186: 2} {
		if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, channel, "", rev), "test", now+1); err != nil {
			t.Fatal(err)
		}
	}
	f.m.syncDueUpstreamTokenWorkAt(context.Background(), now+1)
	next := tokenPipelineState(t, f, "binding", 0, 0)
	if next.Status != "unchanged" || next.Attempts != 0 || next.Revision != old.Revision+1 || next.InputRevision <= old.InputRevision {
		t.Fatal("explicit clear remained trapped in old cooldown", next)
	}
	tokenBindingAssertNoMoneyWrites(t, f)
}

func TestSub2TokenPipelinePreparationTimeoutDoesNotBlockPublication(t *testing.T) {
	f, now := tokenWorkerFixture(t, false)
	// Cancellation may discard a driver connection. A shared-memory SQLite DB
	// disappears when its last connection closes; production uses a file. Copy
	// this fixture to a private file before testing deadline recovery.
	path := filepath.Join(t.TempDir(), "pipeline.db")
	if err := f.m.storeDB.Exec("VACUUM main INTO ?", path).Error; err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	f.m.storeDB = db
	tokenPipelineStore(t, f)
	money := tokenPublishedSnapshot(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key := ChannelUpstreamTokenWorkState{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), Kind: "cost_day", DayTs: f.day, ChannelID: 185}
	did, err := f.m.runUpstreamTokenWork(ctx, key, 0, now, func() (string, bool, error) {
		// Enter the preparation deadline path only AFTER the work is claimed.
		// No 30 ms race against scheduler/fixture SQL on a busy CI runner.
		prepCtx, stop := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer stop()
		_, prepErr := f.m.prepareSub2TokenHistoricalCostDay(prepCtx, f.account.Domain, 185, f.day, now)
		if !errors.Is(prepErr, context.DeadlineExceeded) {
			t.Fatalf("expected preparation deadline, got %v", prepErr)
		}
		cancel() // finishing the claim must survive parent cancellation as well
		// Force connection retirement: a file-backed store must retain the claim.
		sqlDB.SetMaxIdleConns(0)
		return "retry", true, prepErr
	})
	if !did || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("deadline path was not exercised", did, err)
	}
	s := tokenPipelineState(t, f, "cost_day", f.day, 185)
	if s.Attempts != 1 || s.Status != "retry" || s.LeaseUntil != 0 || s.NextAttemptAt != now+60 {
		t.Fatal("timeout cooldown not durable", s)
	}
	if tokenPublishedSnapshot(t, f) != money {
		t.Fatal("failed preparation changed money or queued publications")
	}
	f.m.syncDueChannelEconomicsAt(context.Background(), now)
	tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 8)
	tokenCostAssertCount(t, f, &ChannelEconomicsDirtyHour{}, 15)
}

func TestSub2TokenPipelineChangedProofReplacesOldCostWithZero(t *testing.T) {
	f, now := sub2TokenDayFixture(t, false)
	tokenPipelineStore(t, f)
	for i := 0; i < 8; i++ {
		at := now + int64(i)*60
		f.m.syncDueUpstreamTokenWorkAt(context.Background(), at)
		f.m.syncDueChannelEconomicsAt(context.Background(), at)
	}
	tokenCostAssertCount(t, f, &ChannelEconomicsDirtyHour{}, 0)
	tokenHistoryReplaceProof(t, &f, nil, now+480)
	for i := 0; i < 3; i++ {
		at := now + 481 + int64(i)
		f.m.syncDueUpstreamTokenWorkAt(context.Background(), at)
		f.m.syncDueChannelEconomicsAt(context.Background(), at)
	}
	f.m.cfg.ChannelEconomicsReportEnabled, f.m.cfg.FinanceEnabled = true, true
	for _, finance := range []bool{false, true} {
		report, err := f.m.buildChannelEconomicsReportMode(context.Background(), stabilityScope{FromTs: f.day + 3600, ToTs: f.day + 86400}, f.account.Domain, finance)
		if err != nil || report.Totals.KnownUpstreamCost.MicroUSD != "0" || report.Totals.KnownCorrectedCost.MicroUSD != "0" || report.Totals.KnownRevenue.MicroUSD != "3000000" || report.Totals.ProfitKnown {
			t.Fatal("zero correction lost revenue, retained ghost cost or invented profit", finance, report, err)
		}
	}
	tokenCostAssertCount(t, f, &ChannelEconomicsDirtyHour{}, 0)
}
