package monitor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

// Explicit opt-in; never downloads data, opens a database, or enables a sync.
func TestFinanceCURSourceLocalEvidenceIntegration(t *testing.T) {
	dir := os.Getenv("MONITOR_CUR_LOCAL_EVIDENCE_DIR")
	if dir == "" {
		t.Skip("requires retained local CUR evidence directory")
	}
	policyName := financeCURLocalPolicyName(t)
	raw, err := os.ReadFile(filepath.Join(dir, policyName))
	if err != nil {
		t.Fatal(err)
	}
	var rules []financecur.AllocationRule
	if err := json.Unmarshal(raw, &rules); err != nil {
		t.Fatal(err)
	}
	policyHash, err := financecur.AllocationPolicyHash(rules)
	if err != nil {
		t.Fatal(err)
	}
	artifacts := []financecur.Artifact{}
	var originalKnown int64
	var originalExcluded int64
	for _, name := range []string{"2026-09.parquet", "2026-10.parquet"} {
		file, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.New()
		if _, err := io.Copy(h, file); err != nil {
			file.Close()
			t.Fatal(err)
		}
		if _, err := file.Seek(0, 0); err != nil {
			file.Close()
			t.Fatal(err)
		}
		audit, err := financecur.ReadParquet(file, financecur.Limits{})
		file.Close()
		if err != nil {
			t.Fatal(err)
		}
		r, err := financecur.Allocate(audit.CostEntries, rules)
		if err != nil {
			t.Fatal(err)
		}
		if policyName == "confirmed-account-wide-policy.json" {
			// Allocate preserves zero entries in explicit counts, not in the
			// nonzero EntryAllocations slice. Verify both, rather than dropping zeros.
			nonzero := []financecur.CostEntry{}
			var zeroEntries, zeroRows int64
			for _, e := range audit.CostEntries {
				if e.NanoUSD == 0 {
					zeroEntries++
					zeroRows += e.Rows
				} else {
					nonzero = append(nonzero, e)
				}
			}
			if !r.Publishable || r.NexusAPINanoUSD != audit.TotalNanoUSD || r.ExcludedNanoUSD != 0 || r.UnallocatedNanoUSD != 0 || r.ConflictNanoUSD != 0 || len(r.EntryAllocations) != len(nonzero) || r.ZeroCostEntries != zeroEntries || r.ZeroCostRows != zeroRows {
				t.Fatal("all-account policy lost, excluded, or duplicated source costs")
			}
			var positive, negative, videoCost int64
			for i, a := range r.EntryAllocations {
				if a.Entry != nonzero[i] || a.NexusAPINanoUSD != a.Entry.NanoUSD || a.ExcludedNanoUSD != 0 || a.UnallocatedNanoUSD != 0 {
					t.Fatal("account-wide policy changed a signed source entry")
				}
				if a.Entry.NanoUSD > 0 {
					positive += a.Entry.NanoUSD
				} else {
					negative += a.Entry.NanoUSD
				}
				if a.Entry.ResourceID == "arn:aws:lightsail:us-west-2:842806122225:Instance/6ec32c21-5ada-479d-93d6-03a6fa73588f" {
					videoCost += a.NexusAPINanoUSD
				}
			}
			wantNegative, wantVideo := int64(-20_000_000_000), int64(23_220_000_000)
			if name == "2026-10.parquet" {
				wantNegative, wantVideo = 0, 12_319_500_000
			}
			if positive+negative != r.TotalNanoUSD || negative != wantNegative || videoCost != wantVideo {
				t.Fatal("account-wide policy dropped video-demo or deducted credits twice")
			}
		}
		if policyName == "confirmed-resource-with-source-exclusions-v2-policy.json" {
			baseRaw, err := os.ReadFile(filepath.Join(dir, "confirmed-resource-policy.json"))
			if err != nil {
				t.Fatal(err)
			}
			var baseRules []financecur.AllocationRule
			if err := json.Unmarshal(baseRaw, &baseRules); err != nil {
				t.Fatal(err)
			}
			base, err := financecur.Allocate(audit.CostEntries, baseRules)
			if err != nil {
				t.Fatal(err)
			}
			if base.TotalNanoUSD != r.TotalNanoUSD || base.NexusAPINanoUSD != r.NexusAPINanoUSD || r.ConflictNanoUSD != 0 || r.Publishable {
				t.Fatal("exclusions changed nexus cost, source, or closure")
			}
			if len(base.EntryAllocations) != len(r.EntryAllocations) {
				t.Fatal("entry count changed")
			}
			for i, after := range r.EntryAllocations {
				before := base.EntryAllocations[i]
				if after.Entry.ProductCode == "AmazonRegistrar" && after.Entry.Description == "[OP-NJ24] Registration of modelapi.link" {
					if after.Entry.FromUnix != 1788242885 || after.Entry.ToUnix != 1819778885 || after.ExcludedNanoUSD != 5_000_000_000 || after.Entry.Rows != 1 {
						t.Fatal("annual registration source interval or exclusion changed")
					}
				}
				if before.Entry != after.Entry || before.NexusAPINanoUSD != after.NexusAPINanoUSD {
					t.Fatal("source or nexus entry changed")
				}
				if before.Status == "allocated" && (after.Status != "allocated" || before.ExcludedNanoUSD != after.ExcludedNanoUSD) {
					t.Fatal("existing allocation changed")
				}
				if after.Entry.LineItemType == "Credit" && after.Entry.ResourceID == "" && (after.Status != "unallocated" || after.UnallocatedNanoUSD != after.Entry.NanoUSD) {
					t.Fatal("anonymous credit assigned")
				}
				if after.ExcludedNanoUSD != 0 && (before.Status != "unallocated" || after.ExcludedNanoUSD != after.Entry.NanoUSD || after.UnallocatedNanoUSD != 0) {
					t.Fatal("exclusion is not an exact transfer from unknown")
				}
			}
			if base.UnallocatedNanoUSD-r.UnallocatedNanoUSD != r.ExcludedNanoUSD {
				t.Fatal("excluded/unknown amounts do not reconcile")
			}
		}
		if policyName == "confirmed-consolidated-transit-policy.json" {
			previousRaw, err := os.ReadFile(filepath.Join(dir, "confirmed-resource-with-source-exclusions-v2-policy.json"))
			if err != nil {
				t.Fatal(err)
			}
			var previousRules []financecur.AllocationRule
			if err := json.Unmarshal(previousRaw, &previousRules); err != nil {
				t.Fatal(err)
			}
			previous, err := financecur.Allocate(audit.CostEntries, previousRules)
			if err != nil {
				t.Fatal(err)
			}
			if previous.TotalNanoUSD != r.TotalNanoUSD || previous.UnallocatedNanoUSD != r.UnallocatedNanoUSD || r.ExcludedNanoUSD != 0 || r.ConflictNanoUSD != 0 || r.Publishable || len(previous.EntryAllocations) != len(r.EntryAllocations) {
				t.Fatal("business ownership revision changed source, unknown cost or closure")
			}
			if r.NexusAPINanoUSD-previous.NexusAPINanoUSD != previous.ExcludedNanoUSD {
				t.Fatal("previously excluded sub2api costs were not transferred exactly once")
			}
			for i, after := range r.EntryAllocations {
				before := previous.EntryAllocations[i]
				if before.Entry != after.Entry {
					t.Fatal("raw historical entry changed")
				}
				if before.ExcludedNanoUSD != 0 {
					if after.Status != "allocated" || after.NexusAPINanoUSD != before.ExcludedNanoUSD || after.ExcludedNanoUSD != 0 || after.UnallocatedNanoUSD != 0 {
						t.Fatal("consolidated transit cost is missing or counted twice")
					}
				} else if !reflect.DeepEqual(before, after) {
					t.Fatal("unrelated allocation changed, including unknown credits")
				}
			}
		}
		if policyName == "confirmed-transit-host-policy.json" {
			previousRaw, err := os.ReadFile(filepath.Join(dir, "confirmed-consolidated-transit-policy.json"))
			if err != nil {
				t.Fatal(err)
			}
			var previousRules []financecur.AllocationRule
			if err := json.Unmarshal(previousRaw, &previousRules); err != nil {
				t.Fatal(err)
			}
			previous, err := financecur.Allocate(audit.CostEntries, previousRules)
			if err != nil {
				t.Fatal(err)
			}
			wantIncluded, wantExcluded := int64(23_212_800_000), int64(23_220_000_000)
			if name == "2026-10.parquet" {
				wantIncluded, wantExcluded = 12_315_680_000, 12_319_500_000
			}
			if previous.TotalNanoUSD != r.TotalNanoUSD || r.NexusAPINanoUSD-previous.NexusAPINanoUSD != wantIncluded || r.ExcludedNanoUSD != wantExcluded || previous.UnallocatedNanoUSD-r.UnallocatedNanoUSD != wantIncluded+wantExcluded || r.Publishable || r.ConflictNanoUSD != 0 {
				t.Fatal("host attribution changed source, double counted costs or published incomplete statement")
			}
			if len(previous.EntryAllocations) != len(r.EntryAllocations) {
				t.Fatal("host attribution changed entry count")
			}
			for i, after := range r.EntryAllocations {
				before := previous.EntryAllocations[i]
				if reflect.DeepEqual(before, after) {
					continue
				}
				if before.Entry != after.Entry || before.Status != "unallocated" || after.Entry.ProductCode != "AmazonLightsail" || after.Status != "allocated" || after.UnallocatedNanoUSD != 0 {
					t.Fatal("unexpected host attribution transition")
				}
				switch after.Entry.ResourceID {
				case "arn:aws:lightsail:us-west-2:842806122225:Instance/6ec32c21-5ada-479d-93d6-03a6fa73588f":
					if after.ExcludedNanoUSD != after.Entry.NanoUSD || after.NexusAPINanoUSD != 0 {
						t.Fatal("video-demo leaked into transit costs")
					}
				case "arn:aws:lightsail:us-west-2:842806122225:Instance/74d1d201-cd97-41a0-9326-81593fa87cf2", "arn:aws:lightsail:us-west-2:842806122225:Instance/09345293-3dfa-48be-b010-e82adc68b0e0":
					if after.NexusAPINanoUSD != after.Entry.NanoUSD || after.ExcludedNanoUSD != 0 {
						t.Fatal("sub2api host cost missing")
					}
				default:
					t.Fatal("unrelated resource or shared charge reclassified")
				}
			}
		}
		if policyName == "confirmed-transit-linked-policy.json" {
			previousRaw, err := os.ReadFile(filepath.Join(dir, "confirmed-transit-host-policy.json"))
			if err != nil {
				t.Fatal(err)
			}
			var previousRules []financecur.AllocationRule
			if err := json.Unmarshal(previousRaw, &previousRules); err != nil {
				t.Fatal(err)
			}
			previous, err := financecur.Allocate(audit.CostEntries, previousRules)
			if err != nil {
				t.Fatal(err)
			}
			wantAdded := int64(6_267_651_570)
			if name == "2026-10.parquet" {
				wantAdded = 15_205_391_187
			}
			if previous.TotalNanoUSD != r.TotalNanoUSD || r.NexusAPINanoUSD-previous.NexusAPINanoUSD != wantAdded || previous.UnallocatedNanoUSD-r.UnallocatedNanoUSD != wantAdded || previous.ExcludedNanoUSD != r.ExcludedNanoUSD || r.Publishable || r.ConflictNanoUSD != 0 || len(previous.EntryAllocations) != len(r.EntryAllocations) {
				t.Fatal("linked resource allocation changed unrelated totals or closure")
			}
			allowed := map[string]string{
				"arn:aws:cloudfront::842806122225:distribution/E2NRPPOJNV9DL1":                                            "CloudFrontPlans",
				"arn:aws:route53:::hostedzone/Z06203713MY7RACSMMOLC":                                                      "AmazonRoute53",
				"arn:aws:secretsmanager:us-west-2:842806122225:secret:rds!db-ee970b24-06b4-448c-8831-a45b161d99c5-Sl7RM3": "AWSSecretsManager",
			}
			for i, after := range r.EntryAllocations {
				before := previous.EntryAllocations[i]
				if reflect.DeepEqual(before, after) {
					continue
				}
				if before.Entry != after.Entry || before.Status != "unallocated" || after.Status != "allocated" || allowed[after.Entry.ResourceID] != after.Entry.ProductCode || after.NexusAPINanoUSD != after.Entry.NanoUSD || after.ExcludedNanoUSD != 0 || after.UnallocatedNanoUSD != 0 {
					t.Fatal("linked allocation changed source, shared charge, credit or unrelated resource")
				}
			}
		}
		if err := addFinanceCURInt64(&originalExcluded, r.ExcludedNanoUSD); err != nil {
			t.Fatal(err)
		}
		s, tl, err := financecur.BuildStatement(audit, r, hex.EncodeToString(h.Sum(nil)), policyHash, "Asia/Shanghai", false)
		if err != nil {
			t.Fatal(err)
		}
		a, err := financecur.NewArtifact(s, tl)
		if err != nil {
			t.Fatal(err)
		}
		artifacts = append(artifacts, a)
		if err := addFinanceCURInt64(&originalKnown, s.NexusAPINanoUSD); err != nil {
			t.Fatal(err)
		}
	}
	expectedKnown := int64(871_709_437_272)
	if policyName == "confirmed-account-wide-policy.json" {
		expectedKnown = 1_063_603_128_367
	}
	if policyName == "confirmed-consolidated-transit-policy.json" {
		expectedKnown = 933_486_788_164
	}
	if policyName == "confirmed-transit-host-policy.json" {
		expectedKnown = 969_015_268_164
		if originalExcluded != 35_539_500_000 {
			t.Fatal("video-demo exclusions do not reconcile", originalExcluded)
		}
	}
	if policyName == "confirmed-transit-linked-policy.json" {
		expectedKnown = 990_488_310_921
		if originalExcluded != 35_539_500_000 {
			t.Fatal("linked resource policy changed video-demo exclusions", originalExcluded)
		}
	}
	if policyName != "confirmed-support-policy.json" && originalKnown != expectedKnown {
		t.Fatal("confirmed resource amounts differ from separately reconciled review", originalKnown)
	}
	if policyName == "confirmed-resource-with-source-exclusions-v2-policy.json" && originalExcluded != 61_777_350_892 {
		t.Fatal("excluded amounts differ from exact historical source review", originalExcluded)
	}
	path := filepath.Join(t.TempDir(), "source-set.json")
	financeCURWriteSet(t, path, artifacts...)
	m := &Monitor{cfg: Settings{FinanceCURArtifactEnabled: true, FinanceCURArtifactPath: path}}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	r := financeCURRangeReport(t, time.Date(2026, 9, 1, 0, 0, 0, 0, loc).Unix(), time.Date(2026, 11, 2, 0, 0, 0, 0, loc).Unix())
	m.applyFinanceCURArtifactToReport(r)
	if r.CURCost.Status != "draft" || r.CURCost.SourceCount != 2 || len(r.CURCost.MissingBillingPeriods) != 2 || r.Statement.OperatingProfit != nil || r.Statement.AWSInfrastructureCost != nil {
		t.Fatalf("unexpected closure: %+v", r.CURCost)
	}
	known, err := nanoUSDToRoundedMicroUSD(originalKnown)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := financeMoneyInt64(r.Statement.KnownAWSInfrastructureCost)
	if !ok || got != known {
		t.Fatal("source/report money drift", got, known)
	}
	var days, months, products int64
	for _, d := range r.Days {
		if d.Statement.AWSInfrastructureCost != nil {
			t.Fatal("draft day published")
		}
		if v, ok := financeMoneyInt64(d.Statement.KnownAWSInfrastructureCost); ok {
			if err := addFinanceCURInt64(&days, v); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, p := range r.Periods {
		if v, ok := financeMoneyInt64(p.Statement.KnownAWSInfrastructureCost); ok {
			if err := addFinanceCURInt64(&months, v); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, p := range r.CURProducts {
		if v, ok := financeMoneyInt64(p.KnownCost); ok {
			if err := addFinanceCURInt64(&products, v); err != nil {
				t.Fatal(err)
			}
		}
	}
	if days != got || months != got || products != got {
		t.Fatal("rounded totals do not reconcile", got, days, months, products)
	}
	if r.Statement.OperatingRevenue.MicroUSD != "5000000" || r.Statement.RawCorrectedUpstreamCost.MicroUSD != "2000000" {
		t.Fatal("unrelated report fields changed")
	}
	t.Logf("retained-source integration PASS: known_micro_usd=%d, months=%d, selected_days=%d, products=%d, source_gaps=%d, status=%s", got, len(r.Periods), len(r.Days), len(r.CURProducts), len(r.CURCost.MissingBillingPeriods), r.CURCost.Status)
	if policyName == "confirmed-account-wide-policy.json" {
		from := time.Date(2026, 5, 1, 0, 0, 0, 0, loc).Unix()
		before := financeCURRangeReport(t, from, r.To)
		m.applyFinanceCURArtifactToReport(before)
		if err := os.WriteFile(path, financeAWSLocalBundle(t, dir, artifacts), 0600); err != nil {
			t.Fatal(err)
		}
		after := financeCURRangeReport(t, from, r.To)
		m.applyFinanceCURArtifactToReport(after)
		want := []string{"9.9999999968", "4.9999999995", "79.7693883741", "197.1008700236"}
		if len(after.AWSMonthlyBills) != 4 {
			t.Fatalf("historical month missing: %+v", after.CURCost)
		}
		for i, bill := range after.AWSMonthlyBills {
			if bill.NetUSD != want[i] || bill.RangeRelation != "whole_utc_month" || bill.Status != "reconciled" {
				t.Fatalf("monthly money or status changed: %+v", bill)
			}
		}
		if !reflect.DeepEqual(before.Statement, after.Statement) || !reflect.DeepEqual(before.Days, after.Days) || !reflect.DeepEqual(before.Periods, after.Periods) || !reflect.DeepEqual(before.CURProducts, after.CURProducts) || !reflect.DeepEqual(before.CURCost, after.CURCost) {
			t.Fatal("retained CE supplement changed existing CUR, revenue, upstream or profit")
		}
		t.Log("retained CE+CUR bundle PASS: four historical monthly bills visible; current statement and daily/monthly CUR costs unchanged")
		if snapshot := os.Getenv("MONITOR_LOCAL_BILLING_REPORT"); snapshot != "" {
			raw, err := os.ReadFile(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			var saved struct {
				Report *financeOperatingReport `json:"after_rebuild"`
			}
			if err := json.Unmarshal(raw, &saved); err != nil || saved.Report == nil {
				t.Fatal("invalid retained snapshot", err)
			}
			if err := os.WriteFile(path, financeAWSLocalAccountingBundle(t, dir, artifacts), 0600); err != nil {
				t.Fatal(err)
			}
			business := saved.Report.Statement
			m.applyFinanceCURArtifactToReport(saved.Report)
			if saved.Report.BillingResult == nil || saved.Report.BillingResult.Profit != nil {
				t.Fatal("incomplete whole interval profit published")
			}
			found := false
			for _, p := range saved.Report.Periods {
				if p.Period != "2026-08" {
					continue
				}
				found = true
				b := p.BillingResult
				if b == nil || b.Status != "reconciled" || b.AWSCost.MicroUSD != "197100870" || b.Profit == nil || b.Profit.MicroUSD != "92714725462" || b.MarginPercent == nil || *b.MarginPercent != "59.11" {
					t.Fatalf("retained August monthly result mismatch: %+v", b)
				}
			}
			if !found || !reflect.DeepEqual(business.OperatingRevenue, saved.Report.Statement.OperatingRevenue) || !reflect.DeepEqual(business.RawCorrectedUpstreamCost, saved.Report.Statement.RawCorrectedUpstreamCost) {
				t.Fatal("monthly mode changed business money")
			}
			t.Log("retained snapshot August PASS: revenue=156848.163912, upstream=63936.337580, AWS=197.100870, monthly_profit=92714.725462, margin=59.11%; other incomplete months do not block August")
		}
	}
}

func financeCURLocalPolicyName(t *testing.T) string {
	t.Helper()
	name := os.Getenv("MONITOR_CUR_LOCAL_POLICY_FILE")
	if name == "" {
		return "confirmed-support-policy.json"
	}
	if name != "confirmed-support-policy.json" && name != "confirmed-resource-policy.json" && name != "confirmed-resource-with-source-exclusions-v2-policy.json" && name != "confirmed-consolidated-transit-policy.json" && name != "confirmed-transit-host-policy.json" && name != "confirmed-transit-linked-policy.json" && name != "confirmed-account-wide-policy.json" {
		t.Fatal("unsupported local policy fixture")
	}
	return name
}
