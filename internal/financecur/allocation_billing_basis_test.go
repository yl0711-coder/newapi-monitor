package financecur

import (
	"reflect"
	"testing"
)

func TestAllocateEqualAmountIssueBucketOrderIsDeterministic(t *testing.T) {
	base := CostEntry{FromUnix: 100, ToUnix: 200, ProductCode: "service", ResourceID: "resource", UsageType: "usage", LineItemType: "Usage", Rows: 1, NanoUSD: 10}
	entries := []CostEntry{base}
	for _, change := range []func(*CostEntry){
		func(e *CostEntry) { e.Operation = "op-b" },
		func(e *CostEntry) { e.Operation = "op-a" },
		func(e *CostEntry) { e.Description = "description-b" },
		func(e *CostEntry) { e.Description = "description-a" },
		func(e *CostEntry) { e.ServiceCode = "code" },
		func(e *CostEntry) { e.LineItemType = "Credit"; e.NanoUSD = -10 },
	} {
		e := base
		change(&e)
		entries = append(entries, e)
	}
	want, err := Allocate(entries, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(want.IssueBuckets) != len(entries) {
		t.Fatal("lost issue identities")
	}
	for i := 0; i < 64; i++ {
		got, err := Allocate(entries, nil)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("non-deterministic allocation replay", err)
		}
	}
}

// A credit is a signed source entry, not a second deduction from net cost.
// Unknown ownership must remain visible even when its signed sum is zero.
func TestAllocateBilledNetKeepsUnknownOffsettingEntries(t *testing.T) {
	entries := []CostEntry{
		{FromUnix: 100, ToUnix: 200, ProductCode: "AmazonLightsail", ResourceID: "support-host", LineItemType: "Usage", Rows: 1, NanoUSD: 100},
		{FromUnix: 100, ToUnix: 200, ProductCode: "AmazonLightsail", ResourceID: "support-host", LineItemType: "Credit", Rows: 1, NanoUSD: -20},
		{FromUnix: 100, ToUnix: 200, ProductCode: "AmazonLightsail", ResourceID: "unknown-host", LineItemType: "Usage", Rows: 1, NanoUSD: 30},
		{FromUnix: 100, ToUnix: 200, ProductCode: "AmazonLightsail", ResourceID: "unknown-host", LineItemType: "Credit", Rows: 1, NanoUSD: -30},
	}
	rules := []AllocationRule{{ID: "support", ProductCode: "AmazonLightsail", ResourceMatch: ResourceExact, ResourceID: "support-host", NexusAPIPPM: AllocationPPM, Reason: "explicit host ownership"}}
	r, err := Allocate(entries, rules)
	if err != nil {
		t.Fatal(err)
	}
	if r.TotalNanoUSD != 80 || r.NexusAPINanoUSD != 80 || r.UnallocatedNanoUSD != 0 || r.ExcludedNanoUSD != 0 || r.ConflictNanoUSD != 0 {
		t.Fatalf("signed amounts changed: %+v", r)
	}
	if r.Publishable || r.IssueCount != 2 || r.TotalAbsNanoUSD != 180 || r.AllocatedAbsNanoUSD != 120 {
		t.Fatalf("net-zero unknown costs must not imply complete coverage: %+v", r)
	}
}

func TestAllocateConfirmedSupportScopeDoesNotLeak(t *testing.T) {
	rule := AllocationRule{ID: "admin", EffectiveFromUnix: 100, EffectiveToUnix: 200, ProductCode: "AWSCostExplorer", ResourceMatch: ResourceEmpty, NexusAPIPPM: AllocationPPM, Reason: "confirmed query costs in reviewed period"}
	base := CostEntry{FromUnix: 100, ToUnix: 200, ProductCode: "AWSCostExplorer", LineItemType: "Usage", Rows: 1, NanoUSD: 100}
	tests := []struct {
		name   string
		change func(*CostEntry)
	}{
		{"other service", func(e *CostEntry) { e.ProductCode = "AmazonECS" }},
		{"nonempty resource", func(e *CostEntry) { e.ResourceID = "unconfirmed" }},
		{"older period", func(e *CostEntry) { e.FromUnix, e.ToUnix = 1, 100 }},
		{"later period", func(e *CostEntry) { e.FromUnix, e.ToUnix = 200, 300 }},
		{"crosses boundary", func(e *CostEntry) { e.FromUnix = 99 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := base
			tc.change(&e)
			r, err := Allocate([]CostEntry{base, e}, []AllocationRule{rule})
			if err != nil {
				t.Fatal(err)
			}
			if r.Publishable || r.NexusAPINanoUSD != 100 || r.UnallocatedNanoUSD != 100 || r.TotalNanoUSD != 200 || r.IssueCount != 1 {
				t.Fatalf("scoped rule matched unconfirmed entry: %+v", r)
			}
		})
	}
}

func TestAllocateOverlappingCreditRulesBlockDoubleDeduction(t *testing.T) {
	e := CostEntry{FromUnix: 100, ToUnix: 200, ProductCode: "AmazonECS", LineItemType: "Credit", Rows: 2, NanoUSD: -20}
	rules := []AllocationRule{
		{ID: "service", ProductCode: "AmazonECS", ResourceMatch: ResourceAny, NexusAPIPPM: AllocationPPM, Reason: "synthetic service ownership"},
		{ID: "credit", ProductCode: "AmazonECS", LineItemType: "Credit", ResourceMatch: ResourceEmpty, NexusAPIPPM: AllocationPPM, Reason: "synthetic credit ownership"},
	}
	r, err := Allocate([]CostEntry{e}, rules)
	if err != nil {
		t.Fatal(err)
	}
	if r.Publishable || r.TotalNanoUSD != -20 || r.NexusAPINanoUSD != 0 || r.ConflictNanoUSD != -20 || r.IssueCount != 1 {
		t.Fatalf("overlapping credit rules must block, not double deduct: %+v", r)
	}
}

func TestAllocateExactOtherBusinessFeeExclusionDoesNotLeak(t *testing.T) {
	rule := AllocationRule{ID: "domain-fee", EffectiveFromUnix: 100, EffectiveToUnix: 200, ProductCode: "AmazonRegistrar", LineItemType: "Fee", Description: "Registration of separate.example", ResourceMatch: ResourceEmpty, NexusAPIPPM: 0, Reason: "exact historical source identity"}
	base := CostEntry{FromUnix: 100, ToUnix: 200, ProductCode: "AmazonRegistrar", LineItemType: "Fee", Description: rule.Description, Rows: 1, NanoUSD: 100}
	for _, tc := range []struct {
		name   string
		change func(*CostEntry)
	}{
		{"other domain", func(e *CostEntry) { e.Description = "Registration of nexus.example" }},
		{"credit", func(e *CostEntry) { e.LineItemType = "Credit"; e.NanoUSD = -20 }},
		{"other service", func(e *CostEntry) { e.ProductCode = "AmazonRoute53" }},
		{"nonempty resource", func(e *CostEntry) { e.ResourceID = "unknown" }},
		{"older period", func(e *CostEntry) { e.FromUnix, e.ToUnix = 1, 100 }},
		{"future period", func(e *CostEntry) { e.FromUnix, e.ToUnix = 200, 300 }},
		{"partial overlap", func(e *CostEntry) { e.FromUnix = 99 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := base
			tc.change(&e)
			r, err := Allocate([]CostEntry{base, e}, []AllocationRule{rule})
			if err != nil {
				t.Fatal(err)
			}
			if r.ExcludedNanoUSD != base.NanoUSD || r.UnallocatedNanoUSD != e.NanoUSD || r.NexusAPINanoUSD != 0 || r.TotalNanoUSD != base.NanoUSD+e.NanoUSD || r.Publishable || r.IssueCount != 1 {
				t.Fatalf("exact exclusion leaked into unrelated bill entry: %+v", r)
			}
		})
	}
}
