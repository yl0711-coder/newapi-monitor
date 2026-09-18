package financecur

import "testing"

func TestAllocateRequiresCompleteUnambiguousCoverage(t *testing.T) {
	entries := []CostEntry{
		{FromUnix: 100, ToUnix: 200, LineItemType: "Usage", ProductCode: "AmazonECS", ServiceCode: "AmazonECS", UsageType: "Fargate-vCPU", Operation: "FargateTask", Description: "vCPU usage", ResourceID: "arn:task/nexusapi-one", Rows: 2, NanoUSD: 2_000_000_000},
		{FromUnix: 100, ToUnix: 200, LineItemType: "Credit", ProductCode: "AWSCloudFront", ResourceID: "", Rows: 1, NanoUSD: -500_000_000},
	}
	rules := []AllocationRule{
		{ID: "ecs", ProductCode: "AmazonECS", UsageType: "Fargate-vCPU", Operation: "FargateTask", Description: "vCPU usage", ResourceMatch: ResourcePrefix, ResourceID: "arn:task/nexusapi-", NexusAPIPPM: AllocationPPM, Reason: "dedicated NexusAPI service"},
		{ID: "credit", LineItemType: "Credit", ProductCode: "AWSCloudFront", ResourceMatch: ResourceEmpty, NexusAPIPPM: 250_000, Reason: "documented shared plan allocation"},
	}
	report, err := Allocate(entries, rules)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Publishable || report.IssueCount != 0 || len(report.Issues) != 0 {
		t.Fatalf("report must be publishable: %+v", report)
	}
	if report.TotalNanoUSD != 1_500_000_000 || report.NexusAPINanoUSD != 1_875_000_000 || report.ExcludedNanoUSD != -375_000_000 {
		t.Fatalf("allocation mismatch: %+v", report)
	}
	if report.AllocatedAbsNanoUSD != report.TotalAbsNanoUSD || len(report.Rules) != 2 {
		t.Fatalf("coverage mismatch: %+v", report)
	}
}

func TestAllocateDoesNotMutateCallerRules(t *testing.T) {
	rules := []AllocationRule{{ID: "  one  ", ProductCode: "  AmazonECS ", ResourceMatch: ResourceAny, Reason: "  dedicated "}}
	before := rules[0]
	entry := CostEntry{FromUnix: 100, ToUnix: 200, ProductCode: "AmazonECS", Rows: 1}
	if _, err := Allocate([]CostEntry{entry}, rules); err != nil {
		t.Fatal(err)
	}
	if rules[0] != before {
		t.Fatalf("caller rule mutated: before=%+v after=%+v", before, rules[0])
	}
}

func TestAllocateDoesNotBlockOnZeroCostEvidence(t *testing.T) {
	entry := CostEntry{FromUnix: 100, ToUnix: 200, LineItemType: "Tax", ProductCode: "AmazonECS", Rows: 3}
	report, err := Allocate([]CostEntry{entry}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Publishable || report.ZeroCostEntries != 1 || report.ZeroCostRows != 3 || report.IssueCount != 0 {
		t.Fatalf("zero-cost evidence mismatch: %+v", report)
	}
}

func TestAllocateBlocksMissingConflictingAndPartialRules(t *testing.T) {
	entry := CostEntry{FromUnix: 100, ToUnix: 200, LineItemType: "Usage", ProductCode: "AmazonECS", ResourceID: "arn:task/one", Rows: 1, NanoUSD: 100}
	report, err := Allocate([]CostEntry{entry}, nil)
	if err != nil || report.Publishable || len(report.Issues) != 1 || report.Issues[0].Kind != "unallocated" {
		t.Fatalf("missing rule result: report=%+v err=%v", report, err)
	}
	if len(report.IssueBuckets) != 1 || report.IssueBuckets[0].NanoUSD != 100 || report.IssueBuckets[0].AbsNanoUSD != 100 {
		t.Fatalf("issue bucket mismatch: %+v", report.IssueBuckets)
	}

	rules := []AllocationRule{
		{ID: "one", ProductCode: "AmazonECS", ResourceMatch: ResourceAny, NexusAPIPPM: AllocationPPM, Reason: "one"},
		{ID: "two", ResourceMatch: ResourceExact, ResourceID: "arn:task/one", NexusAPIPPM: AllocationPPM, Reason: "two"},
	}
	report, err = Allocate([]CostEntry{entry}, rules)
	if err != nil || report.Publishable || len(report.Issues) != 1 || report.Issues[0].Kind != "conflict" || report.ConflictNanoUSD != 100 {
		t.Fatalf("conflict result: report=%+v err=%v", report, err)
	}

	partial := []AllocationRule{{ID: "partial", EffectiveFromUnix: 150, EffectiveToUnix: 250, ProductCode: "AmazonECS", ResourceMatch: ResourceAny, NexusAPIPPM: AllocationPPM, Reason: "partial"}}
	report, err = Allocate([]CostEntry{entry}, partial)
	if err != nil || report.Publishable || len(report.Issues) != 1 || report.Issues[0].Kind != "partial_effective_period" {
		t.Fatalf("partial result: report=%+v err=%v", report, err)
	}
}

func TestAllocateRejectsUnsafePoliciesAndInvalidEntries(t *testing.T) {
	entry := CostEntry{FromUnix: 100, ToUnix: 200, Rows: 1}
	tests := [][]AllocationRule{
		{{ID: "", ResourceMatch: ResourceAny, Reason: "x"}},
		{{ID: "catch-all", ResourceMatch: ResourceAny, Reason: "x"}},
		{{ID: "bad-share", ProductCode: "x", ResourceMatch: ResourceAny, NexusAPIPPM: AllocationPPM + 1, Reason: "x"}},
		{{ID: "bad-resource", ProductCode: "x", ResourceMatch: ResourceExact, Reason: "x"}},
	}
	for _, rules := range tests {
		if _, err := Allocate([]CostEntry{entry}, rules); err == nil {
			t.Fatalf("policy must fail: %+v", rules)
		}
	}
	valid := []AllocationRule{{ID: "valid", ProductCode: "x", ResourceMatch: ResourceAny, Reason: "x"}}
	if _, err := Allocate([]CostEntry{{FromUnix: 100, ToUnix: 100, Rows: 1}}, valid); err == nil {
		t.Fatal("invalid entry must fail")
	}
}

func TestAllocationPolicyHashIsNormalizedAndOrderIndependent(t *testing.T) {
	one := AllocationRule{ID: " one ", ProductCode: " AmazonECS ", ResourceMatch: ResourceAny, NexusAPIPPM: AllocationPPM, Reason: " dedicated "}
	two := AllocationRule{ID: "two", ResourceMatch: ResourceExact, ResourceID: " arn:task/two ", Reason: "excluded"}
	first, err := AllocationPolicyHash([]AllocationRule{one, two})
	if err != nil {
		t.Fatal(err)
	}
	second, err := AllocationPolicyHash([]AllocationRule{two, one})
	if err != nil {
		t.Fatal(err)
	}
	if first == "" || first != second {
		t.Fatalf("normalized hashes differ: %q %q", first, second)
	}
	two.Reason = "different evidence"
	third, err := AllocationPolicyHash([]AllocationRule{one, two})
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("material policy change must change the hash")
	}
}
