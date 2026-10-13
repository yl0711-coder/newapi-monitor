package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func sub2TokenDayFixture(t *testing.T, shared bool) (tokenReconcileFixture, int64) {
	t.Helper()
	f, _, activationAt := tokenBindingStoreFixture(t)
	ctx := context.Background()
	if shared {
		if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, 186, "575", 1), "fixture", activationAt); err != nil {
			t.Fatal(err)
		}
	}
	activation, err := f.m.advanceUpstreamTokenBindings(ctx, f.account.Domain, 185, activationAt)
	if err != nil || activation.Status != "scheduled" {
		t.Fatal(activation, err)
	}
	// Yesterday's evidence establishes identity only. No old costs are minted.
	old, err := f.m.advanceSub2TokenCostDay(ctx, f.account.Domain, 185, f.day, activationAt)
	if err != nil || old.Status != "before_effective_window" {
		t.Fatal("proof day backdated into economics", old, err)
	}
	tokenBindingAssertNoMoneyWrites(t, f)
	f.day += 86400
	now := f.day + 86405
	_, items, _, _ := sub2TokenFixture(t)
	for i := range items {
		items[i].CreatedAt += 86400
	}
	legacy := sub2ObservationLegacy(t, f.account, items, f.day)
	f.rows, f.states, f.controls = nil, nil, nil
	for hour := f.day; hour < f.day+86400; hour += 3600 {
		state, err := pricingHourStateFromEvidence(f.account, hour, legacy[hour], now)
		if err != nil {
			t.Fatal(err)
		}
		state.Status, state.VerifiedScans, state.ReconcileStatus = "verified", 2, "matched"
		state.LegacyQuota, state.LegacyRequests, state.LegacyTokens = state.FinalQuota, state.EligibleRequests, state.Tokens
		f.rows = append(f.rows, legacy[hour]...)
		f.states = append(f.states, state)
		f.controls = append(f.controls, ChannelUpstreamUsageHour{Domain: f.account.Domain, Provider: f.account.Provider, HourTs: hour, BucketSeconds: 3600, Requests: state.EligibleRequests, Tokens: state.Tokens, CostUSD: float64(state.FinalQuota) / 1e6, FetchedAt: now})
	}
	for _, rows := range []any{&f.rows, &f.states, &f.controls} {
		if err := f.m.storeDB.Create(rows).Error; err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		f.cursor = tokenEconomicsRescan(t, f, items, now)
	}
	if err := f.m.storeDB.AutoMigrate(&StabilityHourSample{}, &StabilityHourIngestState{}); err != nil {
		t.Fatal(err)
	}
	for h := f.day + 3600; h < f.day+86400; h += 3600 {
		if err := f.m.storeDB.Create(&StabilityHourIngestState{HourTs: h, Status: "complete", TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, pair := range []struct {
		hour    int64
		channel int
	}{{f.day + 36000, 185}, {f.day + 36000, 186}, {f.day + 39600, 185}} {
		if err := f.m.storeDB.Create(&StabilityHourSample{HourTs: pair.hour, ChannelID: pair.channel, ModelName: "model-a", Grp: "local", TrafficClassVersion: stabilityTrafficClassificationVersion, Success: 1, Quota: 500000}).Error; err != nil {
			t.Fatal(err)
		}
	}
	snapshot, _ := json.Marshal(channelFinanceVersionSnapshot{Domain: f.account.Domain, UpstreamRechargePaid: 1, UpstreamRechargeCredit: 10})
	if err := f.m.storeDB.Create(&ChannelFinanceVersion{Domain: f.account.Domain, Version: 1, EffectiveAt: f.day, SnapshotJSON: string(snapshot)}).Error; err != nil {
		t.Fatal(err)
	}
	return f, now
}

func tokenDayLegacySnapshot(t *testing.T, f tokenReconcileFixture) string {
	t.Helper()
	var all [][]map[string]any
	for _, model := range []any{&ChannelUpstreamPricingHourEvidence{}, &ChannelUpstreamPricingHourState{}, &ChannelUpstreamUsageHour{}, &ChannelFinanceVersion{}, &ChannelUpstreamTokenConfig{}, &ChannelCostSourceBinding{}} {
		var rows []map[string]any
		if err := f.m.storeDB.Model(model).Order("rowid").Find(&rows).Error; err != nil {
			t.Fatal(err)
		}
		all = append(all, rows)
	}
	encoded, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func TestSub2TokenDayEndToEndBoundedRestartAndConservation(t *testing.T) {
	for _, shared := range []bool{false, true} {
		name := "separate"
		if shared {
			name = "shared"
		}
		t.Run(name, func(t *testing.T) {
			f, now := sub2TokenDayFixture(t, shared)
			before := tokenDayLegacySnapshot(t, f)
			for i, pending := range []int64{15, 7, 0} {
				f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
				got, err := f.m.advanceSub2TokenCostDay(context.Background(), f.account.Domain, 185, f.day, now+int64(i))
				if err != nil || !got.PendingKnown || got.PendingHours != pending || got.PublishedHours > 8 || got.Cost.SkippedHours != 1 {
					t.Fatal("bounded day turn", got, err)
				}
				if i == 0 && (got.Cost.UpdatedHours != 23 || got.Cost.QueuedHours != 23) || i > 0 && (got.Cost.UnchangedHours != 23 || got.Cost.QueuedHours != 0) {
					t.Fatal("retry changed costs or queues", got)
				}
				if pending == 0 && got.Status != "processed" {
					t.Fatal(got)
				}
			}
			tokenCostAssertCount(t, f, &ChannelUpstreamCostHourState{}, 23)
			tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 23)
			var rows []ChannelEconomicsHourPublication
			if err := f.m.storeDB.Find(&rows).Error; err != nil {
				t.Fatal(err)
			}
			var cost, corrected, revenue int64
			byChannel := map[int]int64{}
			for _, r := range rows {
				if r.HourTs < f.day+3600 {
					t.Fatal("published before effective boundary")
				}
				cost += r.UpstreamChargeUnits
				byChannel[r.LocalChannelID] += r.UpstreamChargeUnits
				corrected += r.CorrectedCostMicroUSD
				revenue += r.RevenueMicroUSD
				if shared && r.ProfitKnown || !shared && (!r.ProfitKnown || r.LocalChannelID == 0) {
					t.Fatal("incorrect profit claim", r)
				}
			}
			wantCorrection := int64(48149)
			wantChannels := map[int]int64{185: 246912, 186: 234567}
			if shared {
				// Preserve existing per-hour/destination rounding: shared costs
				// round once as unallocated, rather than once per channel.
				wantCorrection = 48148
				wantChannels = map[int]int64{0: 481479, 185: 0, 186: 0}
			}
			if cost != 481479 || corrected != wantCorrection || revenue != 3000000 || !reflect.DeepEqual(byChannel, wantChannels) {
				t.Fatal("money not conserved", cost, corrected, revenue)
			}
			stable := tokenPublishedSnapshot(t, f)
			f.m.cfg.ChannelEconomicsReportEnabled, f.m.cfg.FinanceEnabled = true, true
			for _, financeMode := range []bool{false, true} {
				report, err := f.m.buildChannelEconomicsReportMode(context.Background(), stabilityScope{FromTs: f.day + 3600, ToTs: f.day + 86400}, f.account.Domain, financeMode)
				if err != nil {
					t.Fatal("read published economics report", err)
				}
				if !report.Enabled || report.Coverage.PublishedHours != 23 || report.Coverage.ExpectedHours != 23 ||
					report.Totals.KnownUpstreamCost.MicroUSD != "481479" || report.Totals.KnownRevenue.MicroUSD != "3000000" ||
					report.Totals.KnownCorrectedCost.MicroUSD != fmt.Sprint(wantCorrection) || report.Totals.ProfitKnown == shared {
					t.Fatalf("wrong report mode=%t totals=%+v coverage=%+v", financeMode, report.Totals, report.Coverage)
				}
			}
			got, err := f.m.advanceSub2TokenCostDay(context.Background(), f.account.Domain, 185, f.day, now+4)
			if err != nil || got.PublishedHours != 0 || got.Status != "processed" || tokenPublishedSnapshot(t, f) != stable || tokenDayLegacySnapshot(t, f) != before {
				t.Fatal("idempotent day replay or legacy state changed", got, err)
			}
		})
	}
}

func TestSub2TokenDayPublicationFailureBackoffThenRecovery(t *testing.T) {
	f, now := sub2TokenDayFixture(t, false)
	before := tokenDayLegacySnapshot(t, f)
	if err := f.m.storeDB.Exec(`CREATE TRIGGER reject_day_publication BEFORE INSERT ON channel_economics_hour_manifest_publications BEGIN SELECT RAISE(ABORT,'synthetic publication failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	got, err := f.m.advanceSub2TokenCostDay(context.Background(), f.account.Domain, 185, f.day, now)
	if err == nil || got.Status != "publication_retry" || got.PublishedHours != 0 || got.Cost.UpdatedHours != 23 || got.PendingKnown {
		t.Fatal(got, err)
	}
	tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 0)
	tokenCostAssertCount(t, f, &ChannelEconomicsGlobalHourFact{}, 0)
	var first ChannelEconomicsDirtyHour
	if err := f.m.storeDB.Order("hour_ts").First(&first).Error; err != nil || first.Attempts != 1 || first.NextAttemptAt != now+60 {
		t.Fatal("no bounded retry", first, err)
	}
	if err := f.m.storeDB.Exec("DROP TRIGGER reject_day_publication").Error; err != nil {
		t.Fatal(err)
	}
	// Backed-off first hour does not starve other ready hours.
	got, err = f.m.advanceSub2TokenCostDay(context.Background(), f.account.Domain, 185, f.day, now+1)
	if err != nil || got.PublishedHours != 8 || got.PendingHours != 15 {
		t.Fatal(got, err)
	}
	for _, at := range []int64{now + 61, now + 62} {
		f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
		if _, err := f.m.advanceSub2TokenCostDay(context.Background(), f.account.Domain, 185, f.day, at); err != nil {
			t.Fatal(err)
		}
	}
	tokenCostAssertCount(t, f, &ChannelEconomicsDirtyHour{}, 0)
	tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 23)
	if tokenDayLegacySnapshot(t, f) != before {
		t.Fatal("recovery changed original bills/configuration")
	}
}

func TestSub2TokenDayPublicationBeforeQueueAckRetriesIdempotently(t *testing.T) {
	f, now := sub2TokenDayFixture(t, false)
	if err := f.m.storeDB.Exec(`CREATE TRIGGER reject_day_ack BEFORE DELETE ON channel_economics_dirty_hours BEGIN SELECT RAISE(ABORT,'synthetic ack failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := f.m.advanceSub2TokenCostDay(context.Background(), f.account.Domain, 185, f.day, now); err == nil {
		t.Fatal("ack failure hidden")
	}
	tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 1)
	tokenCostAssertCount(t, f, &ChannelEconomicsDirtyHour{}, 23)
	if err := f.m.storeDB.Exec("DROP TRIGGER reject_day_ack").Error; err != nil {
		t.Fatal(err)
	}
	f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
	got, err := f.m.advanceSub2TokenCostDay(context.Background(), f.account.Domain, 185, f.day, now+1)
	if err != nil || got.PublishedHours != 8 || got.PendingHours != 15 {
		t.Fatal(got, err)
	}
	tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 8)
}

func TestSub2TokenDayFailedPreparationAndMissingEvidencePreserveState(t *testing.T) {
	for _, kind := range []string{"write-failure", "waiting-proof", "readonly", "disabled", "no-binding", "cleared", "open-day"} {
		t.Run(kind, func(t *testing.T) {
			f, now := sub2TokenDayFixture(t, false)
			sql := ""
			switch kind {
			case "write-failure":
				sql = `CREATE TRIGGER reject_second_cost BEFORE INSERT ON channel_upstream_cost_hour_states WHEN (SELECT COUNT(*) FROM channel_upstream_cost_hour_states)>0 BEGIN SELECT RAISE(ABORT,'synthetic cost failure'); END`
			case "waiting-proof":
				sql = "UPDATE channel_upstream_token_scan_checkpoints SET stable_scans=1"
			case "readonly":
				f.m.cfg.LocalSnapshotOnly = true
			case "disabled":
				f.m.cfg.ChannelCostClosureEnabled = false
			case "no-binding":
				sql = "DELETE FROM channel_cost_source_bindings"
			case "cleared":
				if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 185, "", 1), "test", now); err != nil {
					t.Fatal(err)
				}
			case "open-day":
				now = f.day + 86399
			}
			if sql != "" {
				if err := f.m.storeDB.Exec(sql).Error; err != nil {
					t.Fatal(err)
				}
			}
			before := tokenPublishedSnapshot(t, f)
			got, err := f.m.advanceSub2TokenCostDay(context.Background(), f.account.Domain, 185, f.day, now)
			if kind == "write-failure" || kind == "readonly" || kind == "open-day" {
				if err == nil {
					t.Fatal("invalid turn succeeded", got)
				}
			} else if err != nil || got.Status == "processed" {
				t.Fatal(got, err)
			}
			if tokenPublishedSnapshot(t, f) != before {
				t.Fatal("failed/no-op turn changed money or queues")
			}
		})
	}
}

func TestSub2TokenDayNewerDirtyGenerationSurvivesPublication(t *testing.T) {
	f, now := sub2TokenDayFixture(t, false)
	hour := f.day + 3600
	// Simulate a producer committing a new wake-up after report insertion,
	// before the consumer acknowledges the generation it originally read.
	query := fmt.Sprintf(`CREATE TRIGGER new_generation_during_publication AFTER INSERT ON channel_economics_hour_manifest_publications WHEN NEW.hour_ts=%d BEGIN UPDATE channel_economics_dirty_hours SET generation=generation+1,next_attempt_at=%d WHERE hour_ts=NEW.hour_ts AND domain=NEW.domain; END`, hour, now+60)
	if err := f.m.storeDB.Exec(query).Error; err != nil {
		t.Fatal(err)
	}
	got, err := f.m.advanceSub2TokenCostDay(context.Background(), f.account.Domain, 185, f.day, now)
	if err != nil || got.PublishedHours != 8 || got.PendingHours != 16 {
		t.Fatal("newer job consumed by old ack", got, err)
	}
	var newer ChannelEconomicsDirtyHour
	if err := f.m.storeDB.Where("hour_ts=?", hour).First(&newer).Error; err != nil || newer.Generation != 2 || newer.NextAttemptAt != now+60 {
		t.Fatal("newer generation not preserved", newer, err)
	}
	if _, err := f.m.advanceSub2TokenCostDay(context.Background(), f.account.Domain, 185, f.day, now+61); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := f.m.storeDB.Model(&ChannelEconomicsHourManifestPublication{}).Where("hour_ts=?", hour).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("new wake-up duplicated report", count, err)
	}
}

func TestSub2TokenDayConfigChangesDoNotStrandCommittedHours(t *testing.T) {
	for _, kind := range []string{"clear-all", "replace-token", "share-token"} {
		t.Run(kind, func(t *testing.T) {
			f, now := sub2TokenDayFixture(t, false)
			ctx := context.Background()
			first, err := f.m.advanceSub2TokenCostDay(ctx, f.account.Domain, 185, f.day, now)
			if err != nil || first.PublishedHours != 8 || first.PendingHours != 15 {
				t.Fatal(first, err)
			}
			changes := map[int]string{185: "999"}
			wantPreparation := "before_effective_window"
			if kind == "clear-all" {
				changes = map[int]string{185: "", 186: ""}
				wantPreparation = "unconfigured"
			} else if kind == "share-token" {
				changes = map[int]string{185: "574"}
			}
			for channel, token := range changes {
				if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, channel, token, 1), "test", now+1); err != nil {
					t.Fatal(err)
				}
			}
			// Explicit clearing must close future ownership even when scans are
			// unavailable. Changing to an unseen token need not activate it.
			if kind == "clear-all" {
				activation, err := f.m.advanceUpstreamTokenBindings(ctx, f.account.Domain, 185, now+2)
				if err != nil || activation.Status != "scheduled" {
					t.Fatal(activation, err)
				}
			}
			legacy := tokenDayLegacySnapshot(t, f)
			var costs []ChannelUpstreamCostHourState
			if err := f.m.storeDB.Order("hour_ts").Find(&costs).Error; err != nil {
				t.Fatal(err)
			}
			for i, pending := range []int64{7, 0} {
				f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
				got, err := f.m.advanceSub2TokenCostDay(ctx, f.account.Domain, 185, f.day, now+3+int64(i))
				if err != nil || !got.PendingKnown || got.PendingHours != pending || got.PublishedHours > 8 || got.PreparationStatus != wantPreparation || got.Cost != (sub2TokenCostCommitResult{}) {
					t.Fatal("old queue stranded or old costs regenerated", got, err)
				}
			}
			tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 23)
			var rows []ChannelEconomicsHourPublication
			if err := f.m.storeDB.Find(&rows).Error; err != nil {
				t.Fatal(err)
			}
			byChannel := map[int]int64{}
			var revenue, corrected int64
			for _, row := range rows {
				if !row.ProfitKnown || row.LocalChannelID == 0 {
					t.Fatal("old ownership replaced by current config", row)
				}
				byChannel[row.LocalChannelID] += row.UpstreamChargeUnits
				revenue += row.RevenueMicroUSD
				corrected += row.CorrectedCostMicroUSD
			}
			var afterCosts []ChannelUpstreamCostHourState
			if err := f.m.storeDB.Order("hour_ts").Find(&afterCosts).Error; err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(byChannel, map[int]int64{185: 246912, 186: 234567}) || revenue != 3000000 || corrected != 48149 || !reflect.DeepEqual(costs, afterCosts) || tokenDayLegacySnapshot(t, f) != legacy {
				t.Fatal("old accounting or source records changed", byChannel, revenue, corrected)
			}
			stable := tokenPublishedSnapshot(t, f)
			got, err := f.m.advanceSub2TokenCostDay(ctx, f.account.Domain, 185, f.day, now+5)
			if err != nil || got.PublishedHours != 0 || got.PendingHours != 0 || got.Status != wantPreparation || tokenPublishedSnapshot(t, f) != stable {
				t.Fatal("replay regenerated jobs", got, err)
			}
		})
	}
}

func TestSub2TokenDayClearedConfigDoesNotBypassPublicationSafety(t *testing.T) {
	for _, kind := range []string{"invalid-proof", "read-only", "disabled", "wrong-epoch"} {
		t.Run(kind, func(t *testing.T) {
			f, now := sub2TokenDayFixture(t, false)
			ctx := context.Background()
			if _, err := f.m.advanceSub2TokenCostDay(ctx, f.account.Domain, 185, f.day, now); err != nil {
				t.Fatal(err)
			}
			if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, 185, "", 1), "test", now+1); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "invalid-proof":
				if err := f.m.storeDB.Model(&ChannelUpstreamTokenScanCheckpoint{}).Where("day_ts=?", f.day).Update("stable_scans", 1).Error; err != nil {
					t.Fatal(err)
				}
			case "read-only":
				f.m.cfg.LocalSnapshotOnly = true
			case "disabled":
				f.m.cfg.ChannelCostClosureEnabled = false
			case "wrong-epoch":
				if err := f.m.storeDB.Model(&ChannelEconomicsDirtyHour{}).Where("domain=?", f.account.Domain).Update("account_epoch", "other-account").Error; err != nil {
					t.Fatal(err)
				}
			}
			got, err := f.m.advanceSub2TokenCostDay(ctx, f.account.Domain, 185, f.day, now+2)
			if (kind == "invalid-proof" || kind == "read-only") && err == nil || got.PublishedHours != 0 {
				t.Fatal("guard bypassed", got, err)
			}
			tokenCostAssertCount(t, f, &ChannelEconomicsDirtyHour{}, 15)
			tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 8)
			if kind == "invalid-proof" && (got.Status != "publication_retry" || got.PreparationStatus != "unconfigured") {
				t.Fatal("invalid proof not visible", got, err)
			}
			if kind == "invalid-proof" {
				_, items, _, _ := sub2TokenFixture(t)
				for i := range items {
					items[i].CreatedAt += 86400
				}
				for i := 0; i < 2; i++ {
					tokenEconomicsRescan(t, f, items, now+3+int64(i))
				}
				for i, pending := range []int64{7, 0} {
					f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
					got, err := f.m.advanceSub2TokenCostDay(ctx, f.account.Domain, 185, f.day, now+63+int64(i))
					if err != nil || got.PendingHours != pending || got.Cost != (sub2TokenCostCommitResult{}) || got.PreparationStatus != "unconfigured" {
						t.Fatal("reverified old jobs did not recover independently of config", got, err)
					}
				}
				tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 23)
			}
		})
	}
}
