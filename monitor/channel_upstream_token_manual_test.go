package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func sub2TokenManualDayFixture(t *testing.T, shared bool) (tokenReconcileFixture, int64) {
	t.Helper()
	f, now := sub2TokenDayFixture(t, shared)
	// Synthetic fixture only: represent an already confirmed manual mapping.
	if err := f.m.storeDB.Model(&ChannelCostSourceBinding{}).Where("domain=?", f.account.Domain).
		Updates(map[string]any{"mapping_source": "manual", "reason": "existing verified ownership", "created_by": "operator"}).Error; err != nil {
		t.Fatal(err)
	}
	return f, now
}

func TestSub2TokenManualDayMixedBindingsCannotBypassConflict(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint(conflict), func(t *testing.T) {
			f, now := sub2TokenDayFixture(t, false)
			updates := map[string]any{"mapping_source": "manual", "reason": "existing manual owner"}
			if conflict {
				updates["local_channel_id"] = 185
			}
			if err := f.m.storeDB.Model(&ChannelCostSourceBinding{}).Where("local_channel_id=186").Updates(updates).Error; err != nil {
				t.Fatal(err)
			}
			before, legacy := tokenPublishedSnapshot(t, f), tokenDayLegacySnapshot(t, f)
			got, err := f.m.advanceSub2TokenCostDay(context.Background(), f.account.Domain, 185, f.day, now)
			if conflict {
				if err == nil || tokenPublishedSnapshot(t, f) != before {
					t.Fatal("managed source bypassed manual conflict", got, err)
				}
			} else if err != nil || got.Cost.UpdatedHours != 23 || got.PublishedHours != 8 {
				t.Fatal("compatible mixed ownership rejected", got, err)
			}
			if tokenDayLegacySnapshot(t, f) != legacy {
				t.Fatal("existing ownership modified")
			}
		})
	}
}

func TestSub2TokenManualDayRechargeAndReportMoneyMatrix(t *testing.T) {
	for _, shared := range []bool{false, true} {
		for _, ratio := range []struct {
			paid, credit           float64
			numerator, denominator int64
		}{
			{1, 1, 1, 1}, {1, 10, 1, 10}, {7, 1, 7, 1}, {2.35, 9.7, 47, 194}, {7.14, 1, 357, 50},
		} {
			t.Run(fmt.Sprintf("shared=%t/%g:%g", shared, ratio.paid, ratio.credit), func(t *testing.T) {
				f, now := sub2TokenManualDayFixture(t, shared)
				snapshot, err := json.Marshal(channelFinanceVersionSnapshot{Domain: f.account.Domain, UpstreamRechargePaid: ratio.paid, UpstreamRechargeCredit: ratio.credit})
				if err != nil {
					t.Fatal(err)
				}
				if err := f.m.storeDB.Model(&ChannelFinanceVersion{}).Where("domain=?", f.account.Domain).Update("snapshot_json", string(snapshot)).Error; err != nil {
					t.Fatal(err)
				}
				legacy := tokenDayLegacySnapshot(t, f)
				for i := 0; i < 3; i++ {
					if _, err := f.m.advanceSub2TokenCostDay(context.Background(), f.account.Domain, 185, f.day, now+int64(i)); err != nil {
						t.Fatal(err)
					}
				}
				// Independent integer oracle, not the production money helper.
				// Rounding remains per hour and destination, not on the total.
				amounts := []int64{123456, 234567, 123456}
				if shared {
					amounts = []int64{358023, 123456}
				}
				var wantCorrected int64
				for _, amount := range amounts {
					wantCorrected += (2*amount*ratio.numerator + ratio.denominator) / (2 * ratio.denominator)
				}
				f.m.cfg.ChannelEconomicsReportEnabled, f.m.cfg.FinanceEnabled = true, true
				for _, finance := range []bool{false, true} {
					report, err := f.m.buildChannelEconomicsReportMode(context.Background(), stabilityScope{FromTs: f.day + 3600, ToTs: f.day + 86400}, f.account.Domain, finance)
					if err != nil {
						t.Fatal(err)
					}
					if report.Totals.KnownUpstreamCost.MicroUSD != "481479" || report.Totals.KnownRevenue.MicroUSD != "3000000" || report.Totals.KnownCorrectedCost.MicroUSD != fmt.Sprint(wantCorrected) || report.Totals.ProfitKnown == shared {
						t.Fatalf("wrong money mode=%t totals=%+v want_corrected=%d", finance, report.Totals, wantCorrected)
					}
				}
				if !shared {
					var rows []ChannelEconomicsHourPublication
					if err := f.m.storeDB.Find(&rows).Error; err != nil {
						t.Fatal(err)
					}
					var profit int64
					for _, row := range rows {
						profit += row.ProfitMicroUSD
					}
					if profit != 3000000-wantCorrected {
						t.Fatal("profit/loss not conserved", profit, wantCorrected)
					}
				}
				if tokenDayLegacySnapshot(t, f) != legacy {
					t.Fatal("source or recharge data changed")
				}
			})
		}
	}
}

func TestSub2TokenManualDayCompatibleOwnershipAndMoney(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprint(shared), func(t *testing.T) {
			f, now := sub2TokenManualDayFixture(t, shared)
			ctx := context.Background()
			before := tokenDayLegacySnapshot(t, f)
			activation, err := f.m.advanceUpstreamTokenBindings(ctx, f.account.Domain, 185, now)
			if err != nil || activation.Status != "unchanged" || activation.ChangedSources != 0 {
				t.Fatal("manual mapping taken over", activation, err)
			}
			for i, pending := range []int64{15, 7, 0} {
				f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
				got, err := f.m.advanceSub2TokenCostDay(ctx, f.account.Domain, 185+i%2, f.day, now+int64(i))
				if err != nil || got.PreparationStatus != "ready" || !got.PendingKnown || got.PendingHours != pending || got.Cost.SkippedHours != 1 {
					t.Fatal("matching manual ownership blocked", got, err)
				}
			}
			var rows []ChannelEconomicsHourPublication
			if err := f.m.storeDB.Find(&rows).Error; err != nil {
				t.Fatal(err)
			}
			byChannel := map[int]int64{}
			var revenue, corrected int64
			for _, row := range rows {
				byChannel[row.LocalChannelID] += row.UpstreamChargeUnits
				revenue += row.RevenueMicroUSD
				corrected += row.CorrectedCostMicroUSD
				if row.ProfitKnown == shared {
					t.Fatal("incorrect profit certainty", row)
				}
			}
			want := map[int]int64{185: 246912, 186: 234567}
			wantCorrected := int64(48149)
			if shared {
				want, wantCorrected = map[int]int64{0: 481479, 185: 0, 186: 0}, 48148
			}
			if !reflect.DeepEqual(byChannel, want) || revenue != 3000000 || corrected != wantCorrected || tokenDayLegacySnapshot(t, f) != before {
				t.Fatal("manual compatibility changed ownership, money or legacy facts", byChannel, revenue, corrected)
			}
			stable := tokenPublishedSnapshot(t, f)
			got, err := f.m.advanceSub2TokenCostDay(ctx, f.account.Domain, 186, f.day, now+3)
			if err != nil || got.PublishedHours != 0 || got.Cost.UpdatedHours != 0 || tokenPublishedSnapshot(t, f) != stable {
				t.Fatal("repeated channel turn duplicated money", got, err)
			}
		})
	}
}

func TestSub2TokenManualDayNeverBackdatesOptionalConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		configOffset, bindingOffset int64
		skipped                     int
		cost                        int64
	}{
		{"mid-hour", 9*3600 + 5, 3600, 10, 481479},
		{"exact-hour", 10 * 3600, 3600, 11, 123456},
		{"configured-after-day", 86400, 3600, 24, 0},
		{"future-manual", 1, 90000, 24, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, now := sub2TokenManualDayFixture(t, false)
			for _, update := range []struct {
				query string
				at    int64
			}{
				{"UPDATE channel_upstream_token_configs SET created_at=?", f.day + tc.configOffset},
				{"UPDATE channel_cost_source_bindings SET valid_from=?", f.day + tc.bindingOffset},
			} {
				if err := f.m.storeDB.Exec(update.query, update.at).Error; err != nil {
					t.Fatal(err)
				}
			}
			before := tokenDayLegacySnapshot(t, f)
			for i := 0; i < 3; i++ {
				got, err := f.m.advanceSub2TokenCostDay(context.Background(), f.account.Domain, 185, f.day, now+int64(i))
				if err != nil || (tc.skipped < 24 && got.Cost.SkippedHours != tc.skipped) || (tc.skipped == 24 && got.Status != "before_effective_window") {
					t.Fatal("wrong activation floor", got, err)
				}
			}
			var rows []ChannelUpstreamCostHourEvidence
			if err := f.m.storeDB.Find(&rows).Error; err != nil {
				t.Fatal(err)
			}
			var total int64
			for _, row := range rows {
				if row.HourTs < f.day+int64(tc.skipped)*3600 {
					t.Fatal("backdated cost", row)
				}
				total += row.ChargeUnits
			}
			if total != tc.cost || tokenDayLegacySnapshot(t, f) != before {
				t.Fatal("historical money changed", total)
			}
			tokenCostAssertCount(t, f, &ChannelUpstreamCostHourState{}, int64(24-tc.skipped))
		})
	}
}

func TestSub2TokenManualDayAmbiguousOwnershipCannotPrepareCosts(t *testing.T) {
	for _, kind := range []string{"conflict", "partial", "finite-history", "wrong-key", "deleted-channel", "missing-proof"} {
		t.Run(kind, func(t *testing.T) {
			f, now := sub2TokenManualDayFixture(t, false)
			query := ""
			switch kind {
			case "conflict":
				query = "UPDATE channel_cost_source_bindings SET local_channel_id=186 WHERE local_channel_id=185"
			case "partial":
				query = "DELETE FROM channel_cost_source_bindings WHERE local_channel_id=186"
			case "finite-history":
				query = fmt.Sprintf("UPDATE channel_cost_source_bindings SET valid_to=%d", f.day+86400)
			case "wrong-key":
				query = "UPDATE channel_cost_source_bindings SET hmac_key_id='another-key'"
			case "deleted-channel":
				query = "UPDATE channel_snaps SET deleted_at=1 WHERE id=186"
			case "missing-proof":
				query = "DELETE FROM channel_upstream_token_scan_checkpoints"
			}
			if err := f.m.storeDB.Exec(query).Error; err != nil {
				t.Fatal(err)
			}
			before, legacy := tokenPublishedSnapshot(t, f), tokenDayLegacySnapshot(t, f)
			got, err := f.m.advanceSub2TokenCostDay(context.Background(), f.account.Domain, 185, f.day, now)
			if err == nil && (got.PreparationStatus == "ready" || got.Status == "processed") {
				t.Fatal("ambiguous configuration accepted", got)
			}
			if tokenPublishedSnapshot(t, f) != before || tokenDayLegacySnapshot(t, f) != legacy {
				t.Fatal("failed compatibility check mutated money/ownership")
			}
		})
	}
}

func TestSub2TokenManualDayWriteFenceRejectsChangedSnapshot(t *testing.T) {
	for _, kind := range []string{"reason", "owner", "interval", "removed", "other-config", "invalid-digest"} {
		t.Run(kind, func(t *testing.T) {
			f, now := sub2TokenManualDayFixture(t, false)
			config, err := readUpstreamTokenConfig(f.m.storeDB, f.account, 185)
			if err != nil {
				t.Fatal(err)
			}
			from, digest, err := f.m.readSub2TokenManualBoundary(f.m.storeDB, f.account, config, now)
			if err != nil || from != f.day+3600 || !validSHA256Hex(digest) {
				t.Fatal(from, digest, err)
			}
			in := sub2TokenCostCommitInput{Domain: f.account.Domain, ChannelID: 185, AccountEpoch: config.AccountEpoch, ConfigRevision: config.Revision, DayTs: f.day, CursorRevision: f.cursor.Revision, SuccessHash: f.cursor.SuccessHash, NotBeforeHour: from, BindingPlanDigest: digest}
			query := ""
			switch kind {
			case "reason":
				query = "UPDATE channel_cost_source_bindings SET reason='concurrent operator change'"
			case "owner":
				query = "UPDATE channel_cost_source_bindings SET local_channel_id=186 WHERE local_channel_id=185"
			case "interval":
				query = "UPDATE channel_cost_source_bindings SET valid_from=valid_from+3600"
			case "removed":
				query = "DELETE FROM channel_cost_source_bindings WHERE local_channel_id=186"
			case "other-config":
				if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 186, "575", 1), "test", now); err != nil {
					t.Fatal(err)
				}
			case "invalid-digest":
				in.BindingPlanDigest = "invalid"
			}
			if query != "" {
				if err := f.m.storeDB.Exec(query).Error; err != nil {
					t.Fatal(err)
				}
			}
			before, legacy := tokenPublishedSnapshot(t, f), tokenDayLegacySnapshot(t, f)
			if _, err := f.m.commitSub2TokenCostCandidates(context.Background(), in, now); err == nil {
				t.Fatal("stale manual snapshot accepted")
			}
			if tokenPublishedSnapshot(t, f) != before || tokenDayLegacySnapshot(t, f) != legacy {
				t.Fatal("stale write mutated money/ownership")
			}
		})
	}
}
