//go:build unix

package monitor

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gorm.io/gorm"
)

func mixedLocalPlans(ctx context.Context, t *testing.T, m *Monitor, account ChannelUpstreamAccount, scope stabilityScope) ([]ChannelCostSourceBinding, []int64) {
	t.Helper()
	var bindings []ChannelCostSourceBinding
	union := map[int64]bool{}
	for _, candidate := range mixedLocalCandidates {
		plan, status, err := m.planChannelCostHistoricalBinding(ctx, channelCostHistoricalBindingInput{
			Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), SourceRef: candidate.Source,
			LocalChannelID: candidate.Channel, Reason: "UNCONFIRMED LOCAL REHEARSAL ONLY; finite mixed-traffic acceptance",
			ValidFrom: &scope.FromTs, ValidTo: &scope.ToTs,
		}, "local-acceptance-only")
		if err != nil || status != 200 || plan.LocalTestRequests == 0 {
			t.Fatal("expected mixed candidate preview", status, err)
		}
		// Use the real bounded preview contract without changing its resulting
		// binding. Only the selected, observed evidence interval is attributed.
		binding := plan.Binding
		var rows []struct{ HourTs, Requests int64 }
		if err := m.storeDB.Model(&ChannelUpstreamCostHourEvidence{}).Select("hour_ts,SUM(requests) requests").
			Where("domain=? AND account_epoch=? AND source_ref=? AND semantics_version=? AND hour_ts>=? AND hour_ts<?",
				account.Domain, newAPIUpstreamAccountEpoch(account), candidate.Source, channelCostEvidenceSemanticsVersion, scope.FromTs, scope.ToTs).
			Group("hour_ts").Order("hour_ts").Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		var requests int64
		for _, row := range rows {
			union[row.HourTs] = true
			requests += row.Requests
		}
		if int64(len(rows)) != candidate.Hours || requests != candidate.Requests || plan.EvidenceHours != candidate.Hours || plan.EvidenceRequests != candidate.Requests ||
			binding.ValidFrom < scope.FromTs || binding.ValidTo > scope.ToTs {
			t.Fatal("bounded source control totals differ", candidate.Channel, len(rows), requests)
		}
		bindings = append(bindings, binding)
	}
	hours := make([]int64, 0, len(union))
	for hour := range union {
		hours = append(hours, hour)
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i] < hours[j] })
	return bindings, hours
}

func mixedLocalSaveAndRetry(ctx context.Context, t *testing.T, m *Monitor, bindings []ChannelCostSourceBinding, hours []int64) {
	t.Helper()
	for _, binding := range bindings {
		if err := m.saveCostSourceBinding(ctx, binding); err != nil {
			t.Fatal(err)
		}
	}
	var queued []int64
	if err := m.storeDB.Model(&ChannelEconomicsDirtyHour{}).Order("hour_ts").Pluck("hour_ts", &queued).Error; err != nil || !reflect.DeepEqual(queued, hours) {
		t.Fatal("queue must contain only the exact union of source hours", err)
	}
	var saved []ChannelCostSourceBinding
	if err := m.storeDB.Order("local_channel_id").Find(&saved).Error; err != nil || !reflect.DeepEqual(saved, bindings) {
		t.Fatal("saved finite bindings differ from the local-only plan", err)
	}
	queries := []attributionLocalQuery{
		{name: "bindings", sql: "SELECT * FROM channel_cost_source_bindings ORDER BY rowid"},
		{name: "queue", sql: "SELECT * FROM channel_economics_dirty_hours ORDER BY rowid"},
	}
	before := attributionQueryDigests(ctx, t, m.storeDB, queries)
	for _, binding := range bindings {
		if err := m.saveCostSourceBinding(ctx, binding); err == nil || !strings.Contains(err.Error(), "重叠") {
			t.Fatal("duplicate binding did not reject overlapping attribution", err)
		}
	}
	if !reflect.DeepEqual(before, attributionQueryDigests(ctx, t, m.storeDB, queries)) {
		t.Fatal("duplicate binding changed durable state")
	}
}

func mixedLocalRows(t *testing.T, db *gorm.DB, account ChannelUpstreamAccount, hours []int64) []ChannelEconomicsHourPublication {
	t.Helper()
	var rows []ChannelEconomicsHourPublication
	if err := currentEconomicsPublicationQuery(db).Select("p.*").Where(
		"p.domain=? AND p.account_epoch=? AND p.hour_ts IN ? AND p.local_channel_id IN ? AND p.semantics_version=?",
		account.Domain, newAPIUpstreamAccountEpoch(account), hours, []int{59, 60}, channelEconomicsSemanticsVersion).
		Order("p.hour_ts,p.local_channel_id").Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

func verifyMixedLocalRows(t *testing.T, db *gorm.DB, account ChannelUpstreamAccount, hours []int64, before, after []ChannelEconomicsHourPublication) {
	t.Helper()
	old := make(map[financeTestPairKey]ChannelEconomicsHourPublication)
	for _, row := range before {
		old[financeTestPairKey{HourTs: row.HourTs, ChannelID: row.LocalChannelID}] = row
	}
	type amount struct{ HourTs, Requests, Charges int64 }
	expected := make(map[financeTestPairKey]amount)
	for _, candidate := range mixedLocalCandidates {
		var rows []amount
		if err := db.Model(&ChannelUpstreamCostHourEvidence{}).Select("hour_ts,SUM(requests) requests,SUM(charge_units) charges").
			Where("domain=? AND account_epoch=? AND hour_ts IN ? AND source_ref=? AND semantics_version=?",
				account.Domain, newAPIUpstreamAccountEpoch(account), hours, candidate.Source, channelCostEvidenceSemanticsVersion).
			Group("hour_ts").Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			expected[financeTestPairKey{HourTs: row.HourTs, ChannelID: candidate.Channel}] = row
		}
	}
	for _, row := range after {
		key := financeTestPairKey{HourTs: row.HourTs, ChannelID: row.LocalChannelID}
		previous, existed := old[key]
		if existed && !reflect.DeepEqual(attributionLocalRevenue(previous), attributionLocalRevenue(row)) {
			t.Fatal("attribution changed local income or request facts")
		}
		evidence, hasCost := expected[key]
		if !hasCost {
			if !existed || row != previous {
				t.Fatal("channel hour without mapped source changed")
			}
			continue
		}
		delete(expected, key)
		if row.Revision != previous.Revision+1 || row.SupersedesPublicationID != previous.PublicationID {
			t.Fatal("mapped cost did not append exactly one revision")
		}
		// These sealed hours use 500,000 quota units and the earlier hypothetical
		// 1:1 recharge version. No production conversion function verifies itself.
		if row.UpstreamRequests != evidence.Requests || row.UpstreamChargeUnits != evidence.Charges || row.ChargeUnitsPerUSD != "500000" ||
			row.UpstreamCostMicroUSD != 2*evidence.Charges || !row.CorrectedCostKnown || row.CorrectedCostMicroUSD != row.UpstreamCostMicroUSD {
			t.Fatal("candidate cost differs from exact source evidence")
		}
	}
	if len(expected) != 0 {
		t.Fatal("mapped source evidence is missing from current publications")
	}
}

type mixedLocalFinanceViews struct {
	Statement financeStatementView
	Days      []financeDailyView
	Evidence  financeInternalTestCostEvidence
}

func mixedLocalViews(ctx context.Context, t *testing.T, m *Monitor, scope stabilityScope) mixedLocalFinanceViews {
	t.Helper()
	policies, err := loadChannelBusinessGroupPolicies(ctx, m.storeDB)
	if err != nil {
		t.Fatal(err)
	}
	configured, err := m.loadFinanceInternalEvidence(ctx, scope, policies, true)
	if err != nil || !configured.Complete || configured.Accounts != 8 {
		t.Fatal("configured internal-account facts are not verified for this window", err)
	}
	evidence, err := m.loadFinanceInternalTestCostEvidence(ctx, scope, configured)
	if err != nil {
		t.Fatal(err)
	}
	finance, err := m.loadChannelFinanceSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := m.loadChannelUpstreamViews(ctx)
	if err != nil {
		t.Fatal(err)
	}
	statement, _, _, _, err := m.buildFinancePeriod(ctx, scope, scope.ToTs+86400, accounts, finance, evidence, configured, policies)
	if err != nil {
		t.Fatal(err)
	}
	days, err := m.buildFinanceDailyViews(ctx, scope, scope.ToTs+86400, evidence, configured, policies)
	if err != nil {
		t.Fatal(err)
	}
	return mixedLocalFinanceViews{Statement: statement, Days: days, Evidence: evidence}
}

func verifyMixedLocalViews(t *testing.T, before, after mixedLocalFinanceViews) {
	t.Helper()
	// Independently inspected sealed facts: #60's three strict hours contain
	// 1/1/2 upstream requests and 2,194/2,194/9,478 quota units. All local
	// requests in each hour belong to the configured internal accounts.
	const strictExpected = int64((2194 + 2194 + 9478) * 2)
	if before.Evidence.TestPairs != 1441 || before.Evidence.StrictPairs != 0 || before.Evidence.MixedPairs != 0 ||
		after.Evidence.TestPairs != 1441 || after.Evidence.StrictPairs != 3 || after.Evidence.MixedPairs != 16 || after.Evidence.UnverifiedPairs != 1422 ||
		after.Evidence.Total.CorrectedCostMicroUSD != strictExpected {
		t.Fatal("closed-fixture strict/mixed control totals differ")
	}
	if before.Statement.KnownUserConsumption != after.Statement.KnownUserConsumption || before.Statement.InternalTestConsumption != after.Statement.InternalTestConsumption ||
		before.Statement.InternalTestRequests != after.Statement.InternalTestRequests || before.Statement.KnownRawCorrectedUpstreamCost != after.Statement.KnownRawCorrectedUpstreamCost {
		t.Fatal("attribution changed income classification or gross upstream expense")
	}
	var mixed int
	for _, event := range after.Evidence.Events {
		if event.Domain == attributionLocalDomain && event.State == "mixed" {
			mixed++
		}
	}
	if mixed == 0 || after.Evidence.Complete || after.Statement.CorrectedUpstreamCost != nil || after.Statement.InternalTestUpstreamCost != nil || after.Statement.ContributionProfit != nil {
		t.Fatal("mixed evidence was not exercised, or was falsely published as exact", mixed)
	}
	// A mixed row may be removed from the paired-customer subtotal, but its
	// cost is NOT a known internal expense to deduct from gross cost.
	var strictCost int64
	for _, event := range after.Evidence.Events {
		if event.State == "strict" {
			strictCost += event.Fact.CorrectedCostMicroUSD
		}
	}
	if after.Evidence.Total.CorrectedCostMicroUSD != strictCost || after.Statement.KnownInternalTestUpstreamCost != economicsMoney(strictCost) {
		t.Fatal("unresolved mixed cost was counted as a strict internal deduction")
	}
	gross, grossKnown := financeMoneyInt64(after.Statement.KnownRawCorrectedUpstreamCost)
	if !grossKnown || after.Statement.KnownCorrectedUpstreamCost != economicsMoney(gross-strictExpected) {
		t.Fatal("gross expense did not deduct exactly the proven internal cost")
	}
	if len(before.Days) != 3 || len(after.Days) != 3 {
		t.Fatal("expected exactly three finite daily projections")
	}
	for i, day := range after.Days {
		if day.Date != before.Days[i].Date || day.Statement.KnownUserConsumption != before.Days[i].Statement.KnownUserConsumption ||
			day.Statement.InternalTestConsumption != before.Days[i].Statement.InternalTestConsumption || day.Statement.InternalTestRequests != before.Days[i].Statement.InternalTestRequests {
			t.Fatal("daily customer/internal income changed")
		}
		if !day.InternalCostComplete && (day.Statement.CorrectedUpstreamCost != nil || day.Statement.InternalTestUpstreamCost != nil || day.Statement.ContributionProfit != nil) {
			t.Fatal("unresolved daily costs were promoted to exact profit")
		}
	}
}
