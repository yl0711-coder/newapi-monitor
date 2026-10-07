package monitor

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func financeContributionFixture(t *testing.T, pairedHours int, pairedRevenue int64) (*Monitor, stabilityScope, map[string]ChannelUpstreamAccountView) {
	t.Helper()
	const domain = "accuracy.example"
	m := newFinanceReportTestMonitor(t, domain)
	hour, err := financeStartHour("2026-05-01")
	if err != nil {
		t.Fatal(err)
	}
	epoch := strings.Repeat("1", 64)
	for i := 0; i < 2; i++ {
		h := hour + int64(i)*3600
		channel := 59 + i
		for _, row := range []any{
			&ChannelSnap{ID: channel, BaseDomain: domain, Status: 1},
			&StabilityHourSample{HourTs: h, ChannelID: channel, ModelName: "m", Grp: "business", Success: 20, Quota: 6_000_000, TrafficClassVersion: stabilityTrafficClassificationVersion},
			&StabilityHourIngestState{HourTs: h, Status: "complete", Requests: 20, TrafficClassVersion: stabilityTrafficClassificationVersion},
			&ChannelUpstreamUsageHour{Domain: domain, HourTs: h, BucketSeconds: 3600, Requests: 18, Quota: 2_500_000, CostUSD: 5, UnitPerUSD: quotaPerUSD, Provider: upstreamProviderNewAPI},
		} {
			if err := m.storeDB.Create(row).Error; err != nil {
				t.Fatal(err)
			}
		}
		if i < pairedHours {
			pub := insertEconomicsReportHour(t, m, domain, epoch, h, channel, pairedRevenue, 5_000_000, 4_000_000, pairedRevenue-4_000_000)
			insertEconomicsReportManifest(t, m, domain, epoch, h, pub)
		}
	}
	createChannelRechargeVersion(t, m, domain, 1, hour, 4, 5)
	accounts := map[string]ChannelUpstreamAccountView{
		domain: {Configured: true, UsageSyncEnabled: true, Provider: upstreamProviderNewAPI, UsageGranularity: "hour"},
	}
	return m, stabilityScope{FromTs: hour, ToTs: hour + 7200}, accounts
}

func TestFinancePeriodContributionRequiresCompletePairing(t *testing.T) {
	for _, tc := range []struct {
		name             string
		pairedHours      int
		pairedRevenue    int64
		internalComplete bool
		wantExact        bool
	}{
		{"complete", 2, 12_000_000, true, true},
		{"missing_hour", 1, 12_000_000, true, false},
		{"same_amount_missing_hour", 1, 24_000_000, true, false},
		{"complete_hours_wrong_revenue", 2, 6_000_000, true, false},
		{"internal_scope_incomplete", 2, 12_000_000, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, scope, accounts := financeContributionFixture(t, tc.pairedHours, tc.pairedRevenue)
			s, user, upstream, _, err := m.buildFinancePeriod(context.Background(), scope, scope.ToTs+86400, accounts,
				channelFinanceSnapshot{}, financeInternalTestCostEvidence{Complete: tc.internalComplete}, financeConfiguredInternalEvidence{Complete: true}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !user.Complete || !upstream.Complete || s.KnownUserConsumption.MicroUSD != "24000000" || s.RawCorrectedUpstreamCost == nil || s.RawCorrectedUpstreamCost.MicroUSD != "8000000" {
				t.Fatalf("independently complete user/bill facts were lost: %+v", s)
			}
			if (s.ContributionProfit != nil) != tc.wantExact || (s.ContributionMargin != nil) != tc.wantExact {
				t.Fatalf("exact contribution must require all pairing proofs: %+v", s)
			}
			wantKnown := economicsMoney(int64(tc.pairedHours) * (tc.pairedRevenue - 4_000_000))
			if s.KnownContributionProfit != wantKnown {
				t.Fatalf("verified partial contribution was discarded: got=%+v want=%+v", s.KnownContributionProfit, wantKnown)
			}
		})
	}
}

func TestFinanceBillRedistributionInvalidatesMonthlyDailyCache(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	for domain := range accounts {
		createChannelRechargeVersion(t, m, domain, 1, scope.FromTs, 1, 1)
	}
	build := func() (financePeriodComponent, bool) {
		t.Helper()
		v, hit, err := m.buildFinancePeriodComponent(context.Background(), scope, scope.ToTs+86400, "bill-redistribution", accounts,
			channelFinanceSnapshot{}, financeInternalTestCostEvidence{Complete: true}, financeConfiguredInternalEvidence{Complete: true}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return v, hit
	}
	before, hit := build()
	if hit || before.Days[0].Statement.KnownUpstreamBilledCost.MicroUSD != "11000000" {
		t.Fatal("invalid initial bill fixture")
	}
	if _, hit := build(); !hit {
		t.Fatal("unchanged month did not reuse cache")
	}
	// Amounts move between days, but all sums and fetched_at stay identical.
	for i, cost := range []float64{2, 1} {
		if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain=? AND hour_ts=?", "hour.example", scope.FromTs+int64(i)*86400).
			Updates(map[string]any{"cost_usd": cost, "quota": cost * quotaPerUSD}).Error; err != nil {
			t.Fatal(err)
		}
	}
	after, hit := build()
	if hit || after.Days[0].Statement.KnownUpstreamBilledCost.MicroUSD != "12000000" || after.Days[1].Statement.KnownUpstreamBilledCost.MicroUSD != "21000000" {
		t.Fatalf("same-total correction reused stale daily amounts: hit=%v days=%+v", hit, after.Days)
	}
	if after.Statement.KnownUpstreamBilledCost != before.Statement.KnownUpstreamBilledCost || after.Statement.KnownRawCorrectedUpstreamCost != before.Statement.KnownRawCorrectedUpstreamCost {
		t.Fatal("redistribution changed month totals")
	}
	again, hit := build()
	if !hit || !reflect.DeepEqual(again, after) {
		t.Fatal("corrected month did not remain cacheable")
	}
}

func TestFinanceBillFingerprintTracksBucketEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values map[string]any
	}{
		{"unit", map[string]any{"unit_per_usd": 1}},
		{"provider", map[string]any{"provider": upstreamProviderAICodeWith}},
		{"source_kind", map[string]any{"source_kind": "credits"}},
		{"source_units", map[string]any{"source_cost_units": 3}},
		{"provisional", map[string]any{"provisional": true}},
		{"duration", map[string]any{"bucket_seconds": 7200}},
		{"fetch_version", map[string]any{"fetched_at": 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, scope, _ := dailyBillFixture(t)
			for _, wholeReport := range []bool{false, true} {
				before, err := m.financeReportSourceFingerprintForScope(context.Background(), scope.FromTs, scope.ToTs, wholeReport)
				if err != nil {
					t.Fatal(err)
				}
				var original ChannelUpstreamUsageHour
				if err := m.storeDB.First(&original, "domain=? AND hour_ts=?", "hour.example", scope.FromTs).Error; err != nil {
					t.Fatal(err)
				}
				if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain=? AND hour_ts=?", original.Domain, original.HourTs).Updates(tc.values).Error; err != nil {
					t.Fatal(err)
				}
				after, err := m.financeReportSourceFingerprintForScope(context.Background(), scope.FromTs, scope.ToTs, wholeReport)
				if err != nil || before == after {
					t.Fatalf("bucket evidence change did not invalidate report=%v: %v", wholeReport, err)
				}
				if err := m.storeDB.Save(&original).Error; err != nil {
					t.Fatal(err)
				}
				restored, err := m.financeReportSourceFingerprintForScope(context.Background(), scope.FromTs, scope.ToTs, wholeReport)
				if err != nil || restored != before {
					t.Fatalf("identical restored facts changed version: %v", err)
				}
			}
		})
	}
}

func TestFinanceBillFingerprintHonorsScopeAndCancellation(t *testing.T) {
	m, scope, _ := dailyBillFixture(t)
	ctx := context.Background()
	fingerprint := func() string {
		t.Helper()
		v, err := m.financeReportPeriodSourceFingerprint(ctx, scope.FromTs, scope.ToTs)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	before := fingerprint()
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: "hour.example", HourTs: scope.ToTs, CostUSD: 99}).Error; err != nil {
		t.Fatal(err)
	}
	if fingerprint() != before {
		t.Fatal("future bill invalidated closed range")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := m.financeReportPeriodSourceFingerprint(canceled, scope.FromTs, scope.ToTs); !errors.Is(err, context.Canceled) {
		t.Fatalf("fingerprint ignored cancellation: %v", err)
	}
}

func TestFinanceBillFingerprintIncludesOverlappingNaturalDay(t *testing.T) {
	m, scope, _ := dailyBillFixture(t)
	// A daily bucket before the selected hour still affects window integrity.
	scope.FromTs += 3600
	before, err := m.financeReportPeriodSourceFingerprint(context.Background(), scope.FromTs, scope.ToTs)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain=? AND hour_ts=?", "day.example", cstDayStart(scope.FromTs)).
		Update("bucket_seconds", 3600).Error; err != nil {
		t.Fatal(err)
	}
	after, err := m.financeReportPeriodSourceFingerprint(context.Background(), scope.FromTs, scope.ToTs)
	if err != nil || before == after {
		t.Fatalf("overlapping daily proof change was ignored: %v", err)
	}
}

func TestFinanceBillFingerprintAcceptsLegacyNullColumns(t *testing.T) {
	m, scope, _ := dailyBillFixture(t)
	before, err := m.financeReportPeriodSourceFingerprint(context.Background(), scope.FromTs, scope.ToTs)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec(`UPDATE channel_upstream_usage_hours SET bucket_seconds=NULL,unit_per_usd=NULL,
		source_cost_units=NULL,source_kind=NULL,provisional=NULL,provider=NULL,fetched_at=NULL WHERE domain=? AND hour_ts=?`, "hour.example", scope.FromTs).Error; err != nil {
		t.Fatal(err)
	}
	after, err := m.financeReportPeriodSourceFingerprint(context.Background(), scope.FromTs, scope.ToTs)
	if err != nil || before == after {
		t.Fatalf("legacy NULL bucket proof was rejected or reused valid version: %v", err)
	}
	pool, err := m.storeDB.DB()
	if err != nil || pool.Stats().InUse != 0 {
		t.Fatalf("fingerprint leaked its read connection: %v", err)
	}
}
