package financecur

import (
	"testing"
	"time"
)

func TestPeriodizeSplitsUsageAtBeijingMidnightAndRecognizesFeeAtStart(t *testing.T) {
	usageStart := time.Date(2026, 9, 1, 15, 30, 0, 0, time.UTC)
	feeStart := time.Date(2026, 9, 2, 1, 0, 0, 0, time.UTC)
	entries := []CostEntry{
		{FromUnix: usageStart.Unix(), ToUnix: usageStart.Add(time.Hour).Unix(), LineItemType: "Usage", ProductCode: "AmazonECS", ResourceID: "nexus-usage", Rows: 1, NanoUSD: 101},
		{FromUnix: feeStart.Unix(), ToUnix: feeStart.AddDate(1, 0, 0).Unix(), LineItemType: "Fee", ProductCode: "AmazonRegistrar", ResourceID: "modelapi-fee", Rows: 1, NanoUSD: 50},
	}
	rules := []AllocationRule{
		{ID: "usage", ProductCode: "AmazonECS", ResourceMatch: ResourceExact, ResourceID: "nexus-usage", NexusAPIPPM: AllocationPPM, Reason: "dedicated"},
		{ID: "fee", ProductCode: "AmazonRegistrar", ResourceMatch: ResourceExact, ResourceID: "modelapi-fee", NexusAPIPPM: 0, Reason: "other project"},
	}
	report, err := Allocate(entries, rules)
	if err != nil {
		t.Fatal(err)
	}
	timeline, err := Periodize(report, "Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline.Days) != 2 || len(timeline.Months) != 1 {
		t.Fatalf("timeline=%+v", timeline)
	}
	if len(timeline.Products) != 2 || timeline.Products[0].ProductCode != "AmazonECS" || timeline.Products[1].ProductCode != "AmazonRegistrar" {
		t.Fatalf("product timelines=%+v", timeline.Products)
	}
	if got := timeline.Products[0].Months[0].NexusAPINanoUSD; got != 101 {
		t.Fatalf("AmazonECS nexus cost=%d", got)
	}
	if got := timeline.Products[1].Months[0].ExcludedNanoUSD; got != 50 {
		t.Fatalf("AmazonRegistrar excluded cost=%d", got)
	}
	if first := timeline.Days[0]; first.Key != "2026-09-01" || first.NexusAPINanoUSD != 50 || first.ExcludedNanoUSD != 0 || first.SourceNanoUSD != 50 {
		t.Fatalf("first day=%+v", first)
	}
	if second := timeline.Days[1]; second.Key != "2026-09-02" || second.NexusAPINanoUSD != 51 || second.ExcludedNanoUSD != 50 || second.SourceNanoUSD != 101 {
		t.Fatalf("second day=%+v", second)
	}
	month := timeline.Months[0]
	if month.Key != "2026-09" || month.SourceNanoUSD != 151 || month.NexusAPINanoUSD != 101 || month.ExcludedNanoUSD != 50 || !month.Complete {
		t.Fatalf("month=%+v", month)
	}
	firstHash, err := TimelineHash(timeline)
	if err != nil || firstHash == "" {
		t.Fatalf("hash=%q err=%v", firstHash, err)
	}
	secondHash, err := TimelineHash(timeline)
	if err != nil || firstHash != secondHash {
		t.Fatalf("timeline hash is not deterministic: %q %q err=%v", firstHash, secondHash, err)
	}
}

func TestTimelineHashRejectsProductBreakdownMismatch(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	entry := CostEntry{FromUnix: start.Unix(), ToUnix: start.Add(time.Hour).Unix(), LineItemType: "Usage", ProductCode: "AmazonECS", Rows: 1, NanoUSD: 10}
	rules := []AllocationRule{{ID: "rule", ProductCode: "AmazonECS", ResourceMatch: ResourceAny, NexusAPIPPM: AllocationPPM, Reason: "test"}}
	report, err := Allocate([]CostEntry{entry}, rules)
	if err != nil {
		t.Fatal(err)
	}
	timeline, err := Periodize(report, "Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	timeline.Products[0].Days[0].NexusAPINanoUSD--
	if _, err := TimelineHash(timeline); err == nil {
		t.Fatal("non-reconciling product breakdown accepted")
	}
}

func TestPeriodizeKeepsUnallocatedDayIncomplete(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	entry := CostEntry{FromUnix: start.Unix(), ToUnix: start.Add(time.Hour).Unix(), LineItemType: "Credit", ProductCode: "AmazonECS", Rows: 1, NanoUSD: -11}
	report, err := Allocate([]CostEntry{entry}, nil)
	if err != nil {
		t.Fatal(err)
	}
	timeline, err := Periodize(report, "Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline.Days) != 1 || timeline.Days[0].Complete || timeline.Days[0].UnallocatedNanoUSD != -11 || timeline.Days[0].UnallocatedAbsNanoUSD != 11 {
		t.Fatalf("timeline=%+v", timeline)
	}
}

func TestPeriodizeRejectsUnknownNonZeroBillingSemantics(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	entry := CostEntry{FromUnix: start.Unix(), ToUnix: start.Add(time.Hour).Unix(), LineItemType: "FutureAWSLineType", ProductCode: "AmazonECS", Rows: 1, NanoUSD: 1}
	rules := []AllocationRule{{ID: "rule", ProductCode: "AmazonECS", ResourceMatch: ResourceAny, NexusAPIPPM: AllocationPPM, Reason: "test"}}
	report, err := Allocate([]CostEntry{entry}, rules)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Periodize(report, "Asia/Shanghai"); err == nil {
		t.Fatal("unknown non-zero line-item type must fail closed")
	}
}

func TestPeriodizeRejectsTamperedEntryEvidence(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	entry := CostEntry{FromUnix: start.Unix(), ToUnix: start.Add(time.Hour).Unix(), LineItemType: "Usage", ProductCode: "AmazonECS", Rows: 1, NanoUSD: 10}
	rules := []AllocationRule{{ID: "rule", ProductCode: "AmazonECS", ResourceMatch: ResourceAny, NexusAPIPPM: AllocationPPM, Reason: "test"}}
	report, err := Allocate([]CostEntry{entry}, rules)
	if err != nil {
		t.Fatal(err)
	}
	report.EntryAllocations[0].NexusAPINanoUSD--
	if _, err := Periodize(report, "Asia/Shanghai"); err == nil {
		t.Fatal("tampered allocation evidence must fail")
	}
}
