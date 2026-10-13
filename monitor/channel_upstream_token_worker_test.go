package monitor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func tokenWorkerFixture(t *testing.T, shared bool) (tokenReconcileFixture, int64) {
	t.Helper()
	f, now := sub2TokenDayFixture(t, shared)
	got, err := f.m.prepareSub2TokenHistoricalCostDay(context.Background(), f.account.Domain, 185, f.day, now)
	if err != nil || got.UpdatedHours != 23 || got.QueuedHours != 23 {
		t.Fatal(got, err)
	}
	return f, now
}

func TestSub2TokenWorkerBoundedRestartAndConservation(t *testing.T) {
	for _, kind := range []string{"separate", "shared", "cleared", "replaced"} {
		t.Run(kind, func(t *testing.T) {
			f, now := tokenWorkerFixture(t, kind == "shared")
			if kind == "cleared" || kind == "replaced" {
				id := ""
				if kind == "replaced" {
					id = "999"
				}
				for _, channel := range []int{185, 186} {
					if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, channel, id, 1), "test", now); err != nil {
						t.Fatal(err)
					}
				}
			}
			legacy := tokenDayLegacySnapshot(t, f)
			for i, pending := range []int64{15, 7, 0} {
				f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
				f.m.syncDueChannelEconomicsAt(context.Background(), now+int64(i))
				tokenCostAssertCount(t, f, &ChannelEconomicsDirtyHour{}, pending)
				tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 23-pending)
				if i == 0 {
					var current []ChannelEconomicsHourManifestCurrent
					if err := f.m.storeDB.Order("hour_ts").Find(&current).Error; err != nil || len(current) != 8 || current[0].HourTs != f.day+3600 || current[7].HourTs != f.day+23*3600 {
						t.Fatal("newest and oldest work must both advance", current, err)
					}
				}
			}
			stable := tokenPublishedSnapshot(t, f)
			f.m.syncDueChannelEconomicsAt(context.Background(), now+3)
			if stable != tokenPublishedSnapshot(t, f) || legacy != tokenDayLegacySnapshot(t, f) {
				t.Fatal("worker changed source facts or duplicated publications")
			}
			want := map[int]int64{185: 246912, 186: 234567}
			wantCorrection := int64(48149)
			if kind == "shared" {
				want = map[int]int64{0: 481479, 185: 0, 186: 0}
				// Shared cost rounds once at channel 0; separated costs round
				// once per channel-hour (existing report semantics).
				wantCorrection = 48148
			}
			var rows []ChannelEconomicsHourPublication
			if err := f.m.storeDB.Find(&rows).Error; err != nil {
				t.Fatal(err)
			}
			got := map[int]int64{}
			var corrected, revenue int64
			for _, row := range rows {
				got[row.LocalChannelID] += row.UpstreamChargeUnits
				corrected += row.CorrectedCostMicroUSD
				revenue += row.RevenueMicroUSD
			}
			if !reflect.DeepEqual(got, want) || corrected != wantCorrection || revenue != 3000000 {
				t.Fatal("published amount/ownership mismatch", got, want, corrected, revenue)
			}
			f.m.cfg.ChannelEconomicsReportEnabled, f.m.cfg.FinanceEnabled = true, true
			for _, finance := range []bool{false, true} {
				report, err := f.m.buildChannelEconomicsReportMode(context.Background(), stabilityScope{FromTs: f.day + 3600, ToTs: f.day + 86400}, f.account.Domain, finance)
				if err != nil || report.Totals.KnownUpstreamCost.MicroUSD != "481479" || report.Totals.KnownCorrectedCost.MicroUSD != fmt.Sprint(wantCorrection) || report.Totals.KnownRevenue.MicroUSD != "3000000" || report.Totals.ProfitKnown == (kind == "shared") {
					t.Fatal("published report mismatch", finance, report, err)
				}
			}
		})
	}
}

func TestSub2TokenWorkerRetrySurvivesRestartAndRecovers(t *testing.T) {
	f, now := tokenWorkerFixture(t, false)
	// Keep one due hour, while retaining the other durable jobs for later.
	if err := f.m.storeDB.Model(&ChannelEconomicsDirtyHour{}).Where("hour_ts<>?", f.day+36000).Update("next_attempt_at", now+86400).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.Model(&ChannelUpstreamTokenScanCheckpoint{}).Where("day_ts=?", f.day).Update("stable_scans", 1).Error; err != nil {
		t.Fatal(err)
	}
	f.m.syncDueChannelEconomicsAt(context.Background(), now)
	var failed ChannelEconomicsDirtyHour
	if err := f.m.storeDB.Where("hour_ts=?", f.day+36000).Take(&failed).Error; err != nil || failed.Attempts != 1 || failed.NextAttemptAt != now+60 || failed.LastError != "令牌成本报表发布待重试" {
		t.Fatal(failed, err)
	}
	tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 0)
	f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
	f.m.syncDueChannelEconomicsAt(context.Background(), now+59)
	var unchanged ChannelEconomicsDirtyHour
	if err := f.m.storeDB.Where("hour_ts=?", failed.HourTs).Take(&unchanged).Error; err != nil || unchanged != failed {
		t.Fatal("backoff lost across restart", unchanged, err)
	}
	f.m.syncDueChannelEconomicsAt(context.Background(), now+60)
	if err := f.m.storeDB.Where("hour_ts=?", failed.HourTs).Take(&failed).Error; err != nil || failed.Attempts != 2 || failed.NextAttemptAt != now+180 {
		t.Fatal("backoff did not increase", failed, err)
	}
	_, items, _, _ := sub2TokenFixture(t)
	for i := range items {
		items[i].CreatedAt += 86400
	}
	for i := 0; i < 2; i++ {
		f.cursor = tokenEconomicsRescan(t, f, items, now+61)
	}
	f.m.syncDueChannelEconomicsAt(context.Background(), now+179)
	tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 0)
	f.m.syncDueChannelEconomicsAt(context.Background(), now+180)
	tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 1)
	tokenCostAssertCount(t, f, &ChannelEconomicsDirtyHour{}, 22)
}

func TestSub2TokenWorkerGuardsAndCancellation(t *testing.T) {
	for _, kind := range []string{"readonly", "disabled", "not-allowed", "unsupported", "changed-account", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			f, now := tokenWorkerFixture(t, false)
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
			case "changed-account":
				if err := f.m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", f.account.Domain).Update("account", "replacement-account").Error; err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				cancel()
			}
			f.m.syncDueChannelEconomicsAt(ctx, now)
			tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 0)
			tokenCostAssertCount(t, f, &ChannelEconomicsDirtyHour{}, 23)
		})
	}
}

func TestSub2TokenWorkerBackoffOverflowIsBounded(t *testing.T) {
	for _, attempts := range []int{-1, 6, math.MaxInt} {
		t.Run(fmt.Sprint(attempts), func(t *testing.T) {
			f, now := tokenWorkerFixture(t, false)
			if err := f.m.storeDB.Model(&ChannelEconomicsDirtyHour{}).Where("domain=?", f.account.Domain).Update("attempts", attempts).Error; err != nil {
				t.Fatal(err)
			}
			if err := f.m.storeDB.Model(&ChannelUpstreamTokenScanCheckpoint{}).Where("day_ts=?", f.day).Update("stable_scans", 1).Error; err != nil {
				t.Fatal(err)
			}
			f.m.syncDueChannelEconomicsAt(context.Background(), now)
			var failed ChannelEconomicsDirtyHour
			if err := f.m.storeDB.Where("hour_ts=?", f.day+23*3600).Take(&failed).Error; err != nil || failed.Attempts < 1 || failed.Attempts > 7 || failed.NextAttemptAt <= now || failed.NextAttemptAt > now+3840 {
				t.Fatal(failed, err)
			}
		})
	}
}

func TestSub2TokenWorkerDoesNotStarveNewAPI(t *testing.T) {
	f, now := tokenWorkerFixture(t, false)
	other := ChannelUpstreamAccount{Domain: "newapi.example", Provider: upstreamProviderNewAPI, BaseURL: "https://newapi.example", UserID: 5, BalanceUnit: quotaPerUSD}
	if err := f.m.storeDB.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	state := ChannelUpstreamCostHourState{Domain: other.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(other), HourTs: f.day + 3600,
		SemanticsVersion: channelCostEvidenceSemanticsVersion, Provider: other.Provider, Status: "verified", ReconcileStatus: "matched", ChargeUnitsPerUSD: "500000", ContentHash: strings.Repeat("a", 64)}
	if err := f.m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.Create(&ChannelEconomicsDirtyHour{Domain: other.Domain, AccountEpoch: state.AccountEpoch, HourTs: state.HourTs, Generation: 1, Status: "pending", NextAttemptAt: now}).Error; err != nil {
		t.Fatal(err)
	}
	f.m.cfg.ChannelCostClosureDomains = []string{f.account.Domain, other.Domain}
	if now/60%2 == 1 {
		now += 60
	}
	if err := f.m.storeDB.Model(&ChannelUpstreamTokenScanCheckpoint{}).Where("day_ts=?", f.day).Update("stable_scans", 1).Error; err != nil {
		t.Fatal(err)
	}
	f.m.syncDueChannelEconomicsAt(context.Background(), now)    // selected Sub2 fails
	f.m.syncDueChannelEconomicsAt(context.Background(), now+60) // rotation selects NewAPI
	var published int64
	if err := f.m.storeDB.Model(&ChannelEconomicsHourManifestCurrent{}).Where("domain=?", other.Domain).Count(&published).Error; err != nil || published != 1 {
		t.Fatal("Sub2 blocked original provider worker", published, err)
	}
	var dirty int64
	if err := f.m.storeDB.Model(&ChannelEconomicsDirtyHour{}).Where("domain=?", f.account.Domain).Count(&dirty).Error; err != nil || dirty != 23 {
		t.Fatal("failed Sub2 work disappeared", dirty, err)
	}
}

func TestUpstreamTokenWriteLockCancellationDoesNotLeakWaiter(t *testing.T) {
	m := &Monitor{}
	m.channelCostBindingMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- m.lockUpstreamTokenWrite(ctx) }()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		m.channelCostBindingMu.Unlock()
		t.Fatal("cancelled writer still blocked")
	}
	m.channelCostBindingMu.Unlock()
	for i := 0; i < 10; i++ {
		if err := m.lockUpstreamTokenWrite(context.Background()); err != nil {
			t.Fatal(err)
		}
		m.channelCostBindingMu.Unlock()
	}
	if err := m.lockUpstreamTokenWrite(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled context acquired idle lock", err)
	}
	if !m.channelCostBindingMu.TryLock() {
		t.Fatal("cancelled operation leaked ownership")
	}
	m.channelCostBindingMu.Unlock()
}

func TestUpstreamTokenWriteLockBoundsConfigAndCostWrites(t *testing.T) {
	for _, kind := range []string{"config", "cost", "cursor"} {
		t.Run(kind, func(t *testing.T) {
			f, in := tokenCostStoreFixture(t, -3600)
			before := tokenDayLegacySnapshot(t, f)
			f.m.channelCostBindingMu.Lock()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				var err error
				switch kind {
				case "config":
					_, err = f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, 185, "888", 1), "test", f.day+86405)
				case "cost":
					_, err = f.m.commitSub2TokenCostCandidates(ctx, in, f.day+86405)
				case "cursor":
					_, err = f.m.commitSub2TokenCursor(ctx, f.account, f.day, f.cursor, nil, f.day+86405, true)
				}
				result <- err
			}()
			select {
			case err := <-result:
				f.m.channelCostBindingMu.Unlock()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("write bypassed deadline", err)
				}
			case <-time.After(time.Second):
				f.m.channelCostBindingMu.Unlock()
				t.Fatal("write waited beyond budget")
			}
			if before != tokenDayLegacySnapshot(t, f) {
				t.Fatal("cancelled write changed source facts")
			}
			tokenBindingAssertNoMoneyWrites(t, f)
		})
	}
}
