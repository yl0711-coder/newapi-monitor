package monitor

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"gorm.io/gorm"
)

func rechargeRebuildFixture(t *testing.T) (*Monitor, ChannelUpstreamAccount, ChannelEconomicsHourPublication, FinanceRechargeCorrection) {
	t.Helper()
	db, rows := publishEconomicsCoverageFixture(t, economicsCoverageFixture{
		withCost: true, allocatedCost: true, withLocal: true, localVerified: true, withFinance: true,
	})
	if err := db.AutoMigrate(&ChannelUpstreamAccount{}); err != nil {
		t.Fatal(err)
	}
	a := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1, BalanceUnit: quotaPerUSD}
	if err := db.Create(&a).Error; err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{a.Domain}}}
	createChannelRechargeVersion(t, m, a.Domain, 2, 86400, 1, 7)
	return m, a, rows[0], FinanceRechargeCorrection{Domain: a.Domain, FromTs: 3600, ToTs: 7200, Paid: 2.5, Credit: 8, Reason: "local fixture"}
}

func appendRechargeTestCorrection(t *testing.T, db *gorm.DB, correction FinanceRechargeCorrection) {
	t.Helper()
	var history []ChannelFinanceVersion
	if err := db.Where("domain=?", correction.Domain).Find(&history).Error; err != nil {
		t.Fatal(err)
	}
	rows, err := buildFinanceRechargeCorrection(history, correction, 172800)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) > 0 {
		if err := db.Create(&rows).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func TestFinanceRechargeRebuildClosesInternalDeductionAndIsIdempotent(t *testing.T) {
	m, account, original, correction := rechargeRebuildFixture(t)
	ctx := context.Background()
	scope := stabilityScope{FromTs: 3600, ToTs: 7200}
	configured := financeConfiguredInternalEvidence{Complete: true, VerifiedScope: scope,
		Rows: []FinanceInternalAccountHourFact{{HourTs: 3600, ChannelID: 59, UserID: 1, Requests: 10, ConsumeQuota: 1_000_000}}}
	read := func() financeInternalTestCostEvidence {
		t.Helper()
		evidence, err := m.loadFinanceInternalCostEvidence(ctx, scope, configured, false)
		if err != nil || !evidence.Complete || evidence.StrictPairs != 1 {
			t.Fatal("invalid fixture deduction", evidence, err)
		}
		return evidence
	}
	before := read()
	appendRechargeTestCorrection(t, m.storeDB, correction)
	terms, err := m.loadChannelRechargeVersions(ctx, map[string]ChannelUpstreamAccountView{account.Domain: {Configured: true, UsageSyncEnabled: true}}, channelFinanceSnapshot{})
	if err != nil {
		t.Fatal(err)
	}
	if got := financeRechargeDeductionStatus(before.Events, before.Total, terms[account.Domain]); got != "cost_pricing_basis_mismatch" {
		t.Fatal("stale deduction was not blocked", got)
	}
	var first []FinanceRechargeEconomicsRebuild
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		var err error
		first, err = rebuildFinanceRechargeEconomics(ctx, tx, []FinanceRechargeCorrection{correction}, 172800)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].RebuiltHours != 1 || first[0].CompleteHours != 1 || first[0].WithoutVerifiedHours != 0 {
		t.Fatal("rebuild did not close the existing evidence", first)
	}
	after := read()
	if got := financeRechargeDeductionStatus(after.Events, after.Total, terms[account.Domain]); got != "" || after.Total.CorrectedCostMicroUSD != 312500 {
		t.Fatal("rebuilt internal deduction disagrees with account recharge price", got, after)
	}
	// A two-unit account bill less the one-unit verified internal request cost
	// must use the SAME arbitrary paid:credit ratio (2.5:8) on both sides.
	net, status := financeInternalCostDeduction(economicsMoney(625000), after, map[string]financeDeductionSource{
		account.Domain: {Cost: economicsMoney(625000), Included: true},
	})
	if status != "" || net.MicroUSD != "312500" {
		t.Fatal("net business cost not conserved", status, net)
	}
	var old ChannelEconomicsHourPublication
	if err := m.storeDB.First(&old, "publication_id=?", original.PublicationID).Error; err != nil || !reflect.DeepEqual(old, original) {
		t.Fatal("immutable old publication overwritten", err)
	}
	again, err := rebuildFinanceRechargeEconomics(ctx, m.storeDB, []FinanceRechargeCorrection{correction}, 172801)
	if err != nil || again[0].RebuiltHours != 0 || again[0].UnchangedHours != 1 {
		t.Fatal("retry republished duplicate costs", again, err)
	}
	var count int64
	if err := m.storeDB.Model(&ChannelEconomicsHourPublication{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatal("unexpected revision count", count, err)
	}
}

func TestFinanceRechargeRebuildPreservesScopeEpochAndMissingEvidence(t *testing.T) {
	m, account, original, correction := rechargeRebuildFixture(t)
	// Outside the repair window, another verified hour remains untouched.
	var state ChannelUpstreamCostHourState
	if err := m.storeDB.First(&state).Error; err != nil {
		t.Fatal(err)
	}
	state.HourTs = 7200
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	state.HourTs, state.AccountEpoch = 3600, "old-account"
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	appendRechargeTestCorrection(t, m.storeDB, correction)
	result, err := rebuildFinanceRechargeEconomics(context.Background(), m.storeDB, []FinanceRechargeCorrection{correction}, 172800)
	if err != nil || result[0].RebuiltHours != 1 || result[0].WindowHours != 1 {
		t.Fatal("rebuild crossed explicit hour/epoch", result, err)
	}
	var current []ChannelEconomicsHourCurrent
	if err := m.storeDB.Find(&current).Error; err != nil || len(current) != 1 || current[0].LogicalKey != original.LogicalKey {
		t.Fatal("rebuild added out-of-scope publications", current, err)
	}
	if err := m.storeDB.Model(&ChannelUpstreamCostHourState{}).Where("account_epoch=? AND hour_ts=3600", newAPIUpstreamAccountEpoch(account)).Update("status", "pending").Error; err != nil {
		t.Fatal(err)
	}
	result, err = rebuildFinanceRechargeEconomics(context.Background(), m.storeDB, []FinanceRechargeCorrection{correction}, 172800)
	if err == nil || !strings.Contains(err.Error(), "refusing mixed pricing") || result != nil {
		t.Fatal("existing ledger without verified evidence must not survive a pricing repair", result, err)
	}
	// An account switch must likewise not reuse an old epoch's publication.
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", account.Domain).Update("user_id", 2).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := rebuildFinanceRechargeEconomics(context.Background(), m.storeDB, []FinanceRechargeCorrection{correction}, 172800); err == nil {
		t.Fatal("account switch left the historical ledger on a mixed pricing basis")
	}
}

func TestFinanceRechargeRebuildRollbackIncludesCorrectionAndPointers(t *testing.T) {
	m, _, original, correction := rechargeRebuildFixture(t)
	if err := m.storeDB.Exec(`CREATE TRIGGER reject_recharge_manifest BEFORE INSERT ON channel_economics_hour_manifest_publications
		BEGIN SELECT RAISE(ABORT,'test manifest failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		appendRechargeTestCorrection(t, tx, correction)
		_, err := rebuildFinanceRechargeEconomics(context.Background(), tx, []FinanceRechargeCorrection{correction}, 172800)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "test manifest failure") {
		t.Fatal("injected failure returned success")
	}
	for _, check := range []struct {
		model any
		want  int64
	}{{&ChannelFinanceVersion{}, 2}, {&ChannelEconomicsHourPublication{}, 1}, {&ChannelEconomicsHourManifestPublication{}, 1}} {
		var count int64
		if err := m.storeDB.Model(check.model).Count(&count).Error; err != nil || count != check.want {
			t.Fatal("partial transaction survived", check.model, count, err)
		}
	}
	var current ChannelEconomicsHourCurrent
	if err := m.storeDB.First(&current).Error; err != nil || current.PublicationID != original.PublicationID {
		t.Fatal("failed transaction advanced current pointer", current, err)
	}
}

func TestFinanceRechargeWholeHourPricingGuardsTrueChanges(t *testing.T) {
	for _, mode := range []string{"mid_hour", "same_instant_replacement", "end_excluded"} {
		t.Run(mode, func(t *testing.T) {
			m, account, _, _ := rechargeRebuildFixture(t)
			at := int64(5400)
			if mode == "end_excluded" {
				at = 7200
			}
			createChannelRechargeVersion(t, m, account.Domain, 3, at, 1, 1)
			if mode == "same_instant_replacement" {
				createChannelRechargeVersion(t, m, account.Domain, 4, at, 2, 20)
			}
			if err := m.publishChannelEconomicsHour(context.Background(), account, 3600, "hour_guard", 172800); err != nil {
				t.Fatal(err)
			}
			var row ChannelEconomicsHourPublication
			if err := currentEconomicsPublicationQuery(m.storeDB).Select("p.*").Take(&row).Error; err != nil {
				t.Fatal(err)
			}
			wantKnown := mode != "mid_hour"
			if row.CorrectedCostKnown != wantKnown || row.ProfitKnown != wantKnown || (wantKnown && row.CorrectedCostMicroUSD != 100000) {
				t.Fatal("hourly publication guessed ambiguous pricing", row)
			}
		})
	}
}

func TestFinanceRechargeRebuildBudgetsAndCancelDoNotPublish(t *testing.T) {
	m, _, _, correction := rechargeRebuildFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := rebuildFinanceRechargeEconomics(ctx, m.storeDB, []FinanceRechargeCorrection{correction}, 172800); err == nil {
		t.Fatal("canceled rebuild succeeded")
	}
	correction.ToTs = correction.FromTs + (financeRechargeRebuildHourLimit+1)*3600
	if _, err := rebuildFinanceRechargeEconomics(context.Background(), m.storeDB, []FinanceRechargeCorrection{correction}, correction.ToTs+3600); err == nil {
		t.Fatal("unbounded rebuild accepted")
	}
	var count int64
	if err := m.storeDB.Model(&ChannelEconomicsHourPublication{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("invalid operation changed publications", count, err)
	}
}
