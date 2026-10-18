package monitor

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"
)

type financeBoundedRechargeFixture struct {
	NotAnImportPlan bool  `json:"not_an_import_plan"`
	ObservedAt      int64 `json:"observed_at"`
	Domains         []struct {
		Domain                 string
		From, To               int64
		Rows                   []ChannelUpstreamUsageHour
		Versions               []ChannelFinanceVersion
		Corrections            []FinanceRechargeCorrection
		ExpectedRawMicro       string `json:"expected_raw_micro"`
		ExpectedAfterMicro     string `json:"expected_after_micro"`
		ExpectedHours          int64  `json:"expected_hours"`
		ExpectedNonzeroBuckets int    `json:"expected_nonzero_buckets"`
	}
}

// This opt-in test accepts a private, reduced evidence projection, never a DB
// import plan. It has no database or network client and persists no versions.
func TestFinanceBoundedRechargeLocalReplay(t *testing.T) {
	path := os.Getenv("MONITOR_FINANCE_BOUNDED_RECHARGE_FIXTURE")
	if path == "" {
		t.Skip("requires an explicitly reviewed private local replay fixture")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > 2<<20 {
		t.Fatal("replay fixture exceeds bounded evidence size")
	}
	var fixture financeBoundedRechargeFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if !fixture.NotAnImportPlan || fixture.ObservedAt <= 0 || len(fixture.Domains) == 0 || len(fixture.Domains) > 2 {
		t.Fatal("expected a bounded, non-import evidence projection")
	}
	t.Cleanup(func() {
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(raw, after) {
			t.Error("private source evidence changed", err)
		}
	})
	for _, domain := range fixture.Domains {
		t.Run(domain.Domain, func(t *testing.T) {
			if domain.From >= domain.To || domain.To > fixture.ObservedAt || domain.To-domain.From > 31*86400 ||
				len(domain.Rows) == 0 || len(domain.Rows) > 800 || len(domain.Versions) == 0 || len(domain.Versions) > 200 ||
				len(domain.Corrections) == 0 || len(domain.Corrections) > 4 {
				t.Fatal("unbounded replay scope")
			}
			originalRows := append([]ChannelUpstreamUsageHour(nil), domain.Rows...)
			originalVersions := append([]ChannelFinanceVersion(nil), domain.Versions...)
			scope := stabilityScope{FromTs: domain.From, ToTs: domain.To}
			accounts := map[string]ChannelUpstreamAccountView{domain.Domain: {
				Configured: true, UsageSyncEnabled: true, Provider: upstreamProviderSub2API,
				UsageGranularity: "hour", FinanceRequiredFrom: domain.From,
			}}
			beforeTerms := correctionTerms(t, domain.Versions)
			beforeMetrics, beforeMoney, err := projectFinanceBillWindow(domain.Rows, scope, fixture.ObservedAt, accounts,
				map[string][]channelRechargeVersion{domain.Domain: beforeTerms})
			if err != nil || !beforeMetrics[domain.Domain].Complete || beforeMetrics[domain.Domain].AdjustedCostAvailable ||
				beforeMoney[domain.Domain].Raw.MicroUSD != domain.ExpectedRawMicro {
				t.Fatalf("baseline must reproduce complete bills and unresolved correction: %+v %+v %v", beforeMetrics, beforeMoney, err)
			}
			history := append([]ChannelFinanceVersion(nil), domain.Versions...)
			for _, correction := range domain.Corrections {
				if correction.Domain != domain.Domain || correction.FromTs < domain.From || correction.ToTs > domain.To {
					t.Fatal("correction escaped reviewed domain/window")
				}
				added, err := buildFinanceRechargeCorrection(history, correction, fixture.ObservedAt)
				if err != nil || len(added) == 0 {
					t.Fatal("expected a finite, explicit correction", err)
				}
				history = append(history, added...)
			}
			afterTerms := correctionTerms(t, history)
			afterMetrics, afterMoney, err := projectFinanceBillWindow(domain.Rows, scope, fixture.ObservedAt, accounts,
				map[string][]channelRechargeVersion{domain.Domain: afterTerms})
			metric := afterMetrics[domain.Domain]
			if err != nil || !metric.Complete || !metric.AdjustedCostAvailable ||
				metric.ExpectedHours != domain.ExpectedHours || metric.CompletedHours != domain.ExpectedHours ||
				afterMoney[domain.Domain].Raw.MicroUSD != domain.ExpectedRawMicro ||
				afterMoney[domain.Domain].RechargeCorrected.MicroUSD != domain.ExpectedAfterMicro {
				t.Fatalf("repaired accounting did not reconcile: %+v %+v %v", metric, afterMoney, err)
			}
			// Check every constant-history segment, including one second outside
			// each boundary; a correction must not leak into other hours/current terms.
			points := []int64{0, fixture.ObservedAt, fixture.ObservedAt + 1}
			for _, term := range afterTerms {
				points = append(points, term.EffectiveAt)
			}
			for _, c := range domain.Corrections {
				points = append(points, c.FromTs-1, c.FromTs, c.ToTs-1, c.ToTs, c.ToTs+1)
			}
			sort.Slice(points, func(i, j int) bool { return points[i] < points[j] })
			for _, at := range points {
				inside := false
				for _, c := range domain.Corrections {
					inside = inside || (at >= c.FromTs && at < c.ToTs)
				}
				if inside {
					continue
				}
				bp, bc, bs := rechargeTermsForBucket(beforeTerms, at, at+1)
				ap, ac, as := rechargeTermsForBucket(afterTerms, at, at+1)
				if bp != ap || bc != ac || bs != as {
					t.Fatalf("terms changed outside confirmed intervals at %d", at)
				}
			}
			nonzero := 0
			for _, row := range domain.Rows {
				if row.Domain != domain.Domain {
					t.Fatal("mixed account evidence")
				}
				if row.HourTs >= domain.From && row.HourTs < domain.To && !upstreamBillHasZeroFaceValue(row) {
					nonzero++
				}
			}
			if nonzero != domain.ExpectedNonzeroBuckets {
				t.Fatal("unexpected nonzero bill count", nonzero)
			}
			for _, correction := range domain.Corrections {
				added, err := buildFinanceRechargeCorrection(history, correction, fixture.ObservedAt)
				if err != nil || len(added) != 0 {
					t.Fatal("repeat repair was not a no-op", err)
				}
			}
			if !reflect.DeepEqual(domain.Rows, originalRows) || !reflect.DeepEqual(domain.Versions, originalVersions) ||
				!reflect.DeepEqual(history[:len(originalVersions)], originalVersions) {
				t.Fatal("raw bills or existing version history mutated")
			}
			t.Logf("raw_micro=%s corrected_micro=%s verified_hours=%d nonzero_buckets=%d; outside terms unchanged; repeat=no-op; no database writes",
				domain.ExpectedRawMicro, domain.ExpectedAfterMicro, metric.CompletedHours, nonzero)
		})
	}
}
