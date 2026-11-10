package monitor

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/metering"
)

func TestTokenDetectionInputComparisonDoesNotRequirePriceTTLOrMapping(t *testing.T) {
	m, now, _ := detectionFixture(t)
	for _, id := range []int64{1, 2, 3, 10} {
		channel, total := int64(1+id%2), int64(10000)
		if id == 10 {
			channel, total = 3, 100000
		}
		f := detectionFact(t, m, id, now-300+id, channel, total, 0, 0)
		u := tokenMeteringDecode(t, f)
		for _, field := range []string{"is_model_mapped", "cache_creation_tokens_5m", "cache_creation_tokens_1h", "quota_per_unit"} {
			u.Fields[field] = metering.Field{State: "missing"}
		}
		u.UnassignedCacheWrite = nil
		f.SourceFacts = tokenDetectionJSON(u)
		if err := m.tokenStore().Save(&f).Error; err != nil {
			t.Fatal(err)
		}
	}
	input, err := tokenDetectionInputOf(tokenMeteringFactRead(t, m, 10))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := tokenReferenceFrom(m.tokenStore(), input, detectionPolicy(), 0, now)
	if err != nil {
		t.Fatal(err)
	}
	r := tokenDetectionEvaluate(input, detectionPolicy(), ref, 0, false)
	if !r.Rules[1].Hit || r.Rules[1].EvidenceLevel != "inferred" || !slices.Contains(ref.Gaps, "model_mapping_unverified") {
		t.Fatal("optional unknowns blocked a known total-input comparison", ref.Reason, r.Rules[1])
	}
	if r.Price.Supported || r.EstimatedLoss != nil || input.Usage.Fields["quota_per_unit"].State != "missing" || input.Usage.Fields["is_model_mapped"].State != "missing" {
		t.Fatal("input comparison manufactured monetary or mapping evidence")
	}
	// Invalid explicit mapping evidence still blocks comparison.
	for _, field := range []metering.Field{{State: "invalid"}, {State: "null"}, {State: "valid", Value: "true"}} {
		input.Usage.Fields["is_model_mapped"] = field
		if tokenComparable(input) {
			t.Fatal("invalid or unresolved explicit mapping treated as comparable", field.State)
		}
	}
}

func TestTokenDetectionCrossChannelControlsSurviveUnrelatedTraffic(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionSeed(t, m, now)
	f := detectionFact(t, m, 10, now-100, 3, 100000, 0, 0)
	noise := make([]TokenMeteringFact, 0, 220)
	for i := int64(0); i < 220; i++ {
		row := f
		row.LogID, row.CreatedAt = 1000+i, now-700+i
		noise = append(noise, row)
	}
	if err := m.tokenStore().CreateInBatches(noise, 128).Error; err != nil {
		t.Fatal(err)
	}
	in, err := tokenDetectionInputOf(f)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := tokenReferenceFrom(m.tokenStore(), in, detectionPolicy(), 0, now)
	if err != nil || ref.Truncated || ref.Upper == nil || *ref.Upper != 10000 {
		t.Fatal("same-channel burst crowded out valid controls", ref.Reason, err)
	}
	for _, sample := range ref.Samples {
		if sample.ChannelID == f.ChannelID {
			t.Fatal("target channel supplied its own reference")
		}
	}
}

func TestTokenDetectionComparisonRequiresSuccessfulEntryEvidence(t *testing.T) {
	m, now, _ := detectionFixture(t)
	f := detectionFact(t, m, 1, now-100, 1, 10000, 0, 0)
	input, err := tokenDetectionInputOf(f)
	if err != nil || !tokenComparable(input) {
		t.Fatal("valid fixture", err)
	}
	var entries []tokenMeteringCandidate
	if err := json.Unmarshal(input.Entry, &entries); err != nil {
		t.Fatal(err)
	}
	entries[0].Status = 500
	input.Entry = json.RawMessage(tokenDetectionJSON(entries))
	if tokenComparable(input) {
		t.Fatal("failed response used as a successful input control")
	}
	input.Entry = json.RawMessage("[]")
	if tokenComparable(input) {
		t.Fatal("missing status evidence became HTTP 200")
	}
}

func TestTokenDetectionFrozenReferenceSurvivesExpansionCapacity(t *testing.T) {
	for _, count := range []int{70, 201} {
		m, now, _ := detectionFixture(t)
		detectionSeed(t, m, now)
		target := detectionFact(t, m, 10, now-100, 3, 100000, 0, 0)
		detectionRun(t, m, now)
		_, before := detectionHead(t, m, 10)
		if !before.Rules[1].Hit {
			t.Fatal("initial reference missing")
		}
		control := tokenMeteringFactRead(t, m, 1)
		rows := make([]TokenMeteringFact, count)
		for i := range rows {
			rows[i] = control
			rows[i].LogID = int64(1000 + i)
			rows[i].CreatedAt = target.CreatedAt + 1
			rows[i].ChannelID = int64(1 + i%2)
		}
		if err := m.tokenStore().CreateInBatches(rows, 64).Error; err != nil {
			t.Fatal(err)
		}
		in, err := tokenDetectionInputOf(target)
		if err != nil {
			t.Fatal(err)
		}
		ref, err := tokenReferenceFrom(m.tokenStore(), in, detectionPolicy(), 1, now+301)
		result := tokenDetectionEvaluate(in, detectionPolicy(), ref, 0, false)
		if err != nil || !result.Rules[1].Hit || ref.Through != before.Reference.Through || len(ref.Samples) != 3 || ref.Reason != "anchor_frozen_reference_capacity" || !slices.Contains(ref.Gaps, "reference_expansion_capacity") {
			t.Fatal("new controls erased valid frozen evidence or fabricated expanded coverage", count, err, ref.Reason)
		}
		m.Close()
	}
}
