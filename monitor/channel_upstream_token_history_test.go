package monitor

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func tokenHistoryWindow(t *testing.T, f tokenReconcileFixture, now int64) sub2TokenHistoricalWindow {
	t.Helper()
	w, err := readSub2TokenHistoricalWindow(f.m.storeDB, f.account, 185, f.day, now, f.key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func tokenHistoryCommitInput(t *testing.T, f tokenReconcileFixture, now int64) sub2TokenCostCommitInput {
	t.Helper()
	w := tokenHistoryWindow(t, f, now)
	c, err := readUpstreamTokenConfig(f.m.storeDB, f.account, 185)
	if err != nil {
		t.Fatal(err)
	}
	return sub2TokenCostCommitInput{Domain: f.account.Domain, ChannelID: 185, AccountEpoch: c.AccountEpoch, ConfigRevision: c.Revision, DayTs: f.day, CursorRevision: f.cursor.Revision, SuccessHash: f.cursor.SuccessHash, HistoricalWindowDigest: w.Digest}
}

func tokenHistoryPublishAll(t *testing.T, f tokenReconcileFixture, now int64, fullProfit bool) map[int]int64 {
	t.Helper()
	var states []ChannelUpstreamCostHourState
	if err := f.m.storeDB.Order("hour_ts").Find(&states).Error; err != nil {
		t.Fatal(err)
	}
	for _, state := range states {
		if err := f.m.publishSub2TokenEconomicsHour(context.Background(), f.account, state.HourTs, "historical local test", now); err != nil {
			t.Fatal(err)
		}
	}
	var rows []ChannelEconomicsHourPublication
	if err := f.m.storeDB.Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	byChannel := map[int]int64{}
	var cost, corrected, revenue, profit int64
	allKnown := true
	for _, row := range rows {
		byChannel[row.LocalChannelID] += row.UpstreamChargeUnits
		cost += row.UpstreamChargeUnits
		corrected += row.CorrectedCostMicroUSD
		revenue += row.RevenueMicroUSD
		profit += row.ProfitMicroUSD
		if !row.ProfitKnown {
			allKnown = false
		}
		if row.ProfitKnown && row.ProfitMicroUSD != row.RevenueMicroUSD-row.CorrectedCostMicroUSD || !row.ProfitKnown && row.ProfitMicroUSD != 0 {
			t.Fatal("invalid profit certainty", row)
		}
	}
	if cost != 481479 || corrected != 48149 || revenue != 3000000 || fullProfit && (!allKnown || profit != 3000000-48149) || !fullProfit && allKnown {
		t.Fatal("historical money mismatch", cost, corrected, revenue, profit)
	}
	f.m.cfg.ChannelEconomicsReportEnabled, f.m.cfg.FinanceEnabled = true, true
	for _, finance := range []bool{false, true} {
		report, err := f.m.buildChannelEconomicsReportMode(context.Background(), stabilityScope{FromTs: f.day + 3600, ToTs: f.day + 86400}, f.account.Domain, finance)
		if err != nil {
			t.Fatal(err)
		}
		if report.Totals.KnownUpstreamCost.MicroUSD != "481479" || report.Totals.KnownCorrectedCost.MicroUSD != "48149" || report.Totals.KnownRevenue.MicroUSD != "3000000" || report.Totals.ProfitKnown != fullProfit {
			t.Fatal("historical report mismatch", finance, report.Totals)
		}
	}
	return byChannel
}

func TestSub2TokenHistoryLateDayAfterReplaceOrClear(t *testing.T) {
	for _, kind := range []string{"replace", "clear-all", "share-now", "manual-cleared"} {
		t.Run(kind, func(t *testing.T) {
			f, now := sub2TokenDayFixture(t, false)
			if kind == "manual-cleared" {
				if err := f.m.storeDB.Model(&ChannelCostSourceBinding{}).Where("domain=?", f.account.Domain).Update("mapping_source", "manual").Error; err != nil {
					t.Fatal(err)
				}
			}
			changes := map[int]string{185: "999"}
			if kind == "clear-all" || kind == "manual-cleared" {
				changes = map[int]string{185: "", 186: ""}
			}
			if kind == "share-now" {
				changes = map[int]string{185: "574"}
			}
			for channel, id := range changes {
				if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, channel, id, 1), "test", now); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "clear-all" {
				if _, err := f.m.advanceUpstreamTokenBindings(context.Background(), f.account.Domain, 185, now+1); err != nil {
					t.Fatal(err)
				}
			}
			legacy := tokenDayLegacySnapshot(t, f)
			got, err := f.m.prepareSub2TokenHistoricalCostDay(context.Background(), f.account.Domain, 185, f.day, now+2)
			if err != nil || got.UpdatedHours != 23 || got.QueuedHours != 23 || got.SkippedHours != 1 {
				t.Fatal("old unprepared costs lost", got, err)
			}
			want := map[int]int64{185: 246912, 186: 234567}
			if rows := tokenHistoryPublishAll(t, f, now+2, true); !reflect.DeepEqual(rows, want) {
				t.Fatal("latest config overwrote historical ownership", rows)
			}
			stable := tokenPublishedSnapshot(t, f)
			f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
			again, err := f.m.prepareSub2TokenHistoricalCostDay(context.Background(), f.account.Domain, 185, f.day, now+3)
			if err != nil || again.UnchangedHours != 23 || again.QueuedHours != 0 || tokenPublishedSnapshot(t, f) != stable || tokenDayLegacySnapshot(t, f) != legacy {
				t.Fatal("restart/retry changed old money or records", again, err)
			}
		})
	}
}

func TestSub2TokenHistoryIntradaySwapUsesHourOwnership(t *testing.T) {
	for _, offset := range []int64{35999, 36000} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			f, now := sub2TokenDayFixture(t, false)
			at := f.day + offset
			for ch, id := range map[int]string{185: "574", 186: "575"} {
				if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, ch, id, 1), "test", at); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.m.advanceUpstreamTokenBindings(context.Background(), f.account.Domain, 185, at); err != nil {
				t.Fatal(err)
			}
			legacy := tokenDayLegacySnapshot(t, f)
			got, err := f.m.prepareSub2TokenHistoricalCostDay(context.Background(), f.account.Domain, 185, f.day, now)
			if err != nil || got.UpdatedHours != 23 {
				t.Fatal(got, err)
			}
			want := map[int]int64{185: 234567, 186: 246912}
			if offset == 36000 {
				want = map[int]int64{185: 123456, 186: 358023}
			}
			// The fixture deliberately retains a local hour-11 request on
			// channel 185 while the upstream source now belongs to 186. Costs
			// remain conserved, but these unmatched rows must NOT claim profit.
			if rows := tokenHistoryPublishAll(t, f, now, false); !reflect.DeepEqual(rows, want) {
				t.Fatal("wrong half-open hour ownership", rows, want)
			}
			if tokenDayLegacySnapshot(t, f) != legacy {
				t.Fatal("rewrote historical versions/bindings")
			}
		})
	}
}

func TestSub2TokenHistoryNeverInventsAuthorization(t *testing.T) {
	for _, kind := range []string{"no-config-history", "no-binding", "in-day-clear", "other-owner-conflict", "fingerprint", "wrong-key", "overlap", "open-day", "stats-only"} {
		t.Run(kind, func(t *testing.T) {
			f, now := sub2TokenDayFixture(t, false)
			query := ""
			want := 0
			switch kind {
			case "no-config-history":
				query = "DELETE FROM channel_upstream_token_configs"
			case "no-binding":
				query = "DELETE FROM channel_cost_source_bindings"
			case "in-day-clear":
				if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 185, "", 1), "test", f.day+35999); err != nil {
					t.Fatal(err)
				}
				want = 9
			case "other-owner-conflict":
				query = "UPDATE channel_cost_source_bindings SET mapping_source='manual',local_channel_id=185 WHERE local_channel_id=186"
			case "fingerprint":
				query = "UPDATE channel_cost_source_bindings SET reason='missing fingerprint'"
			case "wrong-key":
				query = "UPDATE channel_cost_source_bindings SET hmac_key_id='other'"
			case "overlap":
				var b ChannelCostSourceBinding
				if err := f.m.storeDB.First(&b).Error; err != nil {
					t.Fatal(err)
				}
				b.ValidFrom += 3600
				if err := f.m.storeDB.Create(&b).Error; err != nil {
					t.Fatal(err)
				}
			case "open-day":
				now = f.day + 86399
			case "stats-only":
				f.account.UsageAdapter = upstreamUsageAdapterSub2Stats
			}
			if query != "" {
				if err := f.m.storeDB.Exec(query).Error; err != nil {
					t.Fatal(err)
				}
			}
			before, legacy := tokenPublishedSnapshot(t, f), tokenDayLegacySnapshot(t, f)
			w, err := readSub2TokenHistoricalWindow(f.m.storeDB, f.account, 185, f.day, now, f.key, "v1")
			mustError := kind == "wrong-key" || kind == "overlap" || kind == "open-day" || kind == "stats-only"
			if mustError && err == nil || !mustError && (err != nil || w.EligibleHours != want) {
				t.Fatal("invalid authorization window", kind, w.EligibleHours, err)
			}
			if tokenPublishedSnapshot(t, f) != before || tokenDayLegacySnapshot(t, f) != legacy {
				t.Fatal("read planning wrote data")
			}
		})
	}
}

func TestSub2TokenHistoryWriteFenceAndRollback(t *testing.T) {
	for _, kind := range []string{"historical-config", "binding", "current-config", "cursor", "control", "readonly", "disabled", "write-failure"} {
		t.Run(kind, func(t *testing.T) {
			f, now := sub2TokenDayFixture(t, false)
			in := tokenHistoryCommitInput(t, f, now)
			query := ""
			switch kind {
			case "historical-config":
				query = "UPDATE channel_upstream_token_configs SET created_by='changed' WHERE channel_id=186"
			case "binding":
				query = "UPDATE channel_cost_source_bindings SET reason='changed'"
			case "current-config":
				if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 185, "", 1), "test", now); err != nil {
					t.Fatal(err)
				}
			case "cursor":
				query = "UPDATE channel_upstream_token_scan_checkpoints SET stable_scans=1"
			case "control":
				query = fmt.Sprintf("UPDATE channel_upstream_usage_hours SET cost_usd=cost_usd+1 WHERE hour_ts=%d", f.day+36000)
			case "readonly":
				f.m.cfg.LocalSnapshotOnly = true
			case "disabled":
				f.m.cfg.ChannelCostClosureEnabled = false
			case "write-failure":
				query = `CREATE TRIGGER reject_second_history_cost BEFORE INSERT ON channel_upstream_cost_hour_states WHEN (SELECT COUNT(*) FROM channel_upstream_cost_hour_states)>0 BEGIN SELECT RAISE(ABORT,'synthetic cost failure'); END`
			}
			if query != "" {
				if err := f.m.storeDB.Exec(query).Error; err != nil {
					t.Fatal(err)
				}
			}
			before, legacy := tokenPublishedSnapshot(t, f), tokenDayLegacySnapshot(t, f)
			got, err := f.m.commitSub2TokenCostCandidates(context.Background(), in, now)
			if err == nil || got != (sub2TokenCostCommitResult{}) || tokenPublishedSnapshot(t, f) != before || tokenDayLegacySnapshot(t, f) != legacy {
				t.Fatal("failed historical write changed data", kind, got, err)
			}
		})
	}
}

func tokenHistoryReplaceProof(t *testing.T, f *tokenReconcileFixture, items []sub2PricingUsageItem, now int64) {
	t.Helper()
	legacy := sub2ObservationLegacy(t, f.account, items, f.day)
	for _, model := range []any{&ChannelUpstreamPricingHourEvidence{}, &ChannelUpstreamPricingHourState{}, &ChannelUpstreamUsageHour{}} {
		if err := f.m.storeDB.Where("domain=? AND hour_ts>=? AND hour_ts<?", f.account.Domain, f.day, f.day+86400).Delete(model).Error; err != nil {
			t.Fatal(err)
		}
	}
	f.rows, f.states, f.controls = nil, nil, nil
	for hour := f.day; hour < f.day+86400; hour += 3600 {
		s, err := pricingHourStateFromEvidence(f.account, hour, legacy[hour], now)
		if err != nil {
			t.Fatal(err)
		}
		s.Status, s.VerifiedScans, s.ReconcileStatus = "verified", 2, "matched"
		s.LegacyQuota, s.LegacyRequests, s.LegacyTokens = s.FinalQuota, s.EligibleRequests, s.Tokens
		f.rows = append(f.rows, legacy[hour]...)
		f.states = append(f.states, s)
		f.controls = append(f.controls, ChannelUpstreamUsageHour{Domain: f.account.Domain, Provider: f.account.Provider, HourTs: hour, BucketSeconds: 3600, Requests: s.EligibleRequests, Tokens: s.Tokens, CostUSD: float64(s.FinalQuota) / 1e6, FetchedAt: now})
	}
	if len(f.rows) > 0 {
		if err := f.m.storeDB.Create(&f.rows).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, rows := range []any{&f.states, &f.controls} {
		if err := f.m.storeDB.Create(rows).Error; err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		current, err := readSub2TokenStoredCursor(f.m.storeDB, f.account, f.day)
		if err != nil {
			t.Fatal(err)
		}
		current, err = f.m.resetSub2TokenScanCheckpoint(context.Background(), f.account, f.day, current.Revision, now, f.key, "v1")
		if err != nil {
			t.Fatal(err)
		}
		page := sub2PricingPage{Items: items, Total: int64(len(items)), Page: 1, PageSize: 100, Pages: 1}
		current, err = f.m.saveSub2TokenScanPage(context.Background(), f.account, f.day, page, current.Revision, now, f.key, "v1")
		if err != nil {
			t.Fatal(err)
		}
		f.cursor, err = f.m.recordSub2TokenObservation(context.Background(), f.account, f.day, current, legacy, now, f.key, "v1")
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestSub2TokenHistoryConfirmedTokenWithoutUsage(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			f, now := sub2TokenDayFixture(t, false)
			_, items, _, _ := sub2TokenFixture(t)
			items = []sub2PricingUsageItem{items[1]}
			items[0].CreatedAt += 86400
			want := int64(234567)
			if empty {
				items = nil
				want = 0
			}
			tokenHistoryReplaceProof(t, &f, items, now)
			// The normal first-activation path must remain strict: absence
			// on a bill does not establish an unverified token's identity.
			if _, err := buildSub2TokenCostCandidates(f.account, "575", f.day, now, f.cursor, f.rows, f.states, f.controls, f.key, "v1"); err == nil {
				t.Fatal("initial token check unexpectedly relaxed")
			}
			if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 185, "", 1), "test", now); err != nil {
				t.Fatal(err)
			}
			legacy := tokenDayLegacySnapshot(t, f)
			got, err := f.m.prepareSub2TokenHistoricalCostDay(context.Background(), f.account.Domain, 185, f.day, now+1)
			if err != nil || got.UpdatedHours != 23 {
				t.Fatal("confirmed inactive token blocked whole-account costs", got, err)
			}
			var rows []ChannelUpstreamCostHourEvidence
			if err := f.m.storeDB.Find(&rows).Error; err != nil {
				t.Fatal(err)
			}
			var total int64
			for _, r := range rows {
				total += r.ChargeUnits
			}
			if total != want || tokenDayLegacySnapshot(t, f) != legacy {
				t.Fatal("absent token invented/lost money", total, want)
			}
			// Verified empty hours are real ledger states, not missing facts.
			tokenCostAssertCount(t, f, &ChannelUpstreamCostHourState{}, 23)
		})
	}
}

func TestSub2TokenHistoryIntradaySharedCostsRemainSingle(t *testing.T) {
	f, now := sub2TokenDayFixture(t, false)
	at := f.day + 35999
	if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 186, "575", 1), "test", at); err != nil {
		t.Fatal(err)
	}
	if _, err := f.m.advanceUpstreamTokenBindings(context.Background(), f.account.Domain, 185, at); err != nil {
		t.Fatal(err)
	}
	legacy := tokenDayLegacySnapshot(t, f)
	got, err := f.m.prepareSub2TokenHistoricalCostDay(context.Background(), f.account.Domain, 185, f.day, now)
	if err != nil || got.UpdatedHours != 23 {
		t.Fatal(got, err)
	}
	var states []ChannelUpstreamCostHourState
	if err := f.m.storeDB.Find(&states).Error; err != nil {
		t.Fatal(err)
	}
	for _, s := range states {
		if err := f.m.publishSub2TokenEconomicsHour(context.Background(), f.account, s.HourTs, "test", now); err != nil {
			t.Fatal(err)
		}
	}
	var rows []ChannelEconomicsHourPublication
	if err := f.m.storeDB.Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	var cost, corrected int64
	for _, r := range rows {
		cost += r.UpstreamChargeUnits
		corrected += r.CorrectedCostMicroUSD
		if r.ProfitKnown || r.LocalChannelID != 0 && r.UpstreamChargeUnits != 0 {
			t.Fatal("shared money allocated or counted twice", r)
		}
	}
	if cost != 481479 || corrected != 48148 || tokenDayLegacySnapshot(t, f) != legacy {
		t.Fatal("shared historical cost not conserved", cost, corrected)
	}
}

func TestSub2TokenHistoryRevisedZeroSupersedesPublishedMoney(t *testing.T) {
	f, now := sub2TokenDayFixture(t, false)
	ctx := context.Background()
	if _, err := f.m.prepareSub2TokenHistoricalCostDay(ctx, f.account.Domain, 185, f.day, now); err != nil {
		t.Fatal(err)
	}
	tokenHistoryPublishAll(t, f, now, true)
	tokenHistoryReplaceProof(t, &f, nil, now+1)
	legacy := tokenDayLegacySnapshot(t, f)
	got, err := f.m.prepareSub2TokenHistoricalCostDay(ctx, f.account.Domain, 185, f.day, now+2)
	if err != nil || got.UpdatedHours != 2 || got.UnchangedHours != 21 || got.QueuedHours != 2 {
		t.Fatal("zero correction not queued exactly once", got, err)
	}
	var states []ChannelUpstreamCostHourState
	if err := f.m.storeDB.Find(&states).Error; err != nil {
		t.Fatal(err)
	}
	for _, s := range states {
		if err := f.m.publishSub2TokenEconomicsHour(ctx, f.account, s.HourTs, "zero correction", now+2); err != nil {
			t.Fatal(err)
		}
	}
	for _, finance := range []bool{false, true} {
		report, err := f.m.buildChannelEconomicsReportMode(ctx, stabilityScope{FromTs: f.day + 3600, ToTs: f.day + 86400}, f.account.Domain, finance)
		if err != nil {
			t.Fatal(err)
		}
		if report.Totals.KnownUpstreamCost.MicroUSD != "0" || report.Totals.KnownCorrectedCost.MicroUSD != "0" || report.Totals.KnownRevenue.MicroUSD != "3000000" || report.Totals.ProfitKnown {
			t.Fatal("old money survived zero correction or missing cost became profit", report.Totals)
		}
	}
	stable := tokenPublishedSnapshot(t, f)
	got, err = f.m.prepareSub2TokenHistoricalCostDay(ctx, f.account.Domain, 185, f.day, now+3)
	if err != nil || got.UnchangedHours != 23 || got.QueuedHours != 0 || tokenPublishedSnapshot(t, f) != stable || tokenDayLegacySnapshot(t, f) != legacy {
		t.Fatal("zero retry changed facts", got, err)
	}
}

func TestSub2TokenHistoryPreDayHeadAndMidnightBoundary(t *testing.T) {
	for _, offset := range []int64{-1, 0} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			f, now := sub2TokenDayFixture(t, false)
			// Synthetic earlier confirmed intervals expose hour zero as well.
			if err := f.m.storeDB.Model(&ChannelCostSourceBinding{}).Where("domain=?", f.account.Domain).Update("valid_from", f.day-3600).Error; err != nil {
				t.Fatal(err)
			}
			if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 185, "", 1), "test", f.day+offset); err != nil {
				t.Fatal(err)
			}
			w := tokenHistoryWindow(t, f, now)
			want := 0
			if offset == 0 {
				want = 1
			}
			if w.EligibleHours != want || w.Hours[0] != (want == 1) {
				t.Fatal("wrong last pre-day revision / exact midnight cutoff", offset, w.Hours)
			}
			for i := 1; i < 24; i++ {
				if w.Hours[i] {
					t.Fatal("clear did not take effect", i)
				}
			}
		})
	}
}
