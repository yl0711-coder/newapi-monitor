package monitor

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestTokenReferenceLengthStagesAndConversionIsolation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		delta      int64
		conversion bool
		wantStage  int
		wantStatus string
	}{
		{"exact", 0, false, 1, "statistical"}, {"sixteen", 10, false, 2, "statistical"}, {"sixty_four", 40, false, 3, "statistical"},
		{"relative_guard", 40, false, 3, "insufficient"}, {"conversion_isolated", 0, true, 3, "insufficient"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, now, _ := detectionFixture(t)
			defer m.Close()
			p := detectionPolicy()
			p.SizeTolerance = 64
			if tc.name != "relative_guard" {
				p.SizeRelative = "0.1"
			}
			for i, ch := range []int64{2, 2, 3} {
				f := detectionFact(t, m, int64(i+1), now-20+int64(i), ch, 1000, 0, 0)
				length := int64(1000) + tc.delta
				f.RequestLength = &length
				var entry []tokenMeteringCandidate
				if err := json.Unmarshal([]byte(f.EvidenceJSON), &entry); err != nil {
					t.Fatal(err)
				}
				entry[0].Length = &length
				f.EvidenceJSON = tokenDetectionJSON(entry)
				if tc.conversion {
					u := tokenMeteringDecode(t, f)
					v := u.Fields["request_conversion"]
					v.State = "valid"
					v.Value = `["converted"]`
					u.Fields["request_conversion"] = v
					f.SourceFacts = tokenDetectionJSON(u)
				}
				if err := m.tokenStore().Save(&f).Error; err != nil {
					t.Fatal(err)
				}
			}
			target := detectionFact(t, m, 99, now, 1, 40000, 0, 0)
			in, err := tokenDetectionInputOf(target)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := tokenReferenceFrom(m.tokenStore(), in, p, 0, now)
			if err != nil || ref.Status != tc.wantStatus || len(ref.Search) != tc.wantStage {
				t.Fatalf("ref=%+v err=%v", ref, err)
			}
			if tc.conversion && ref.ExclusionReasons["incompatible:request_conversion"] != 3 {
				t.Fatal(ref.ExclusionReasons)
			}
			if ref.Status == "statistical" && (ref.Upper == nil || *ref.Upper != 1000) {
				t.Fatal(ref)
			}
		})
	}
}

func TestTokenReferenceExactCannotBeCrowdedOutByNearSizes(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	p := detectionPolicy()
	p.SizeTolerance = 64
	p.SizeRelative = "0.1"
	for i, ch := range []int64{2, 2, 3} {
		detectionFact(t, m, int64(i+1), now-5+int64(i), ch, 1000, 0, 0)
	}
	base := detectionFact(t, m, 10, now-100, 2, 1100, 0, 0)
	length := int64(1010)
	base.RequestLength = &length
	var rows []TokenMeteringFact
	for i := 0; i < 205; i++ {
		f := base
		f.LogID = int64(1000 + i)
		f.CreatedAt = now - 100
		f.RequestID = fmt.Sprint("near-", i)
		rows = append(rows, f)
	}
	if err := m.tokenStore().CreateInBatches(rows, 50).Error; err != nil {
		t.Fatal(err)
	}
	target := detectionFact(t, m, 9999, now, 1, 40000, 0, 0)
	in, _ := tokenDetectionInputOf(target)
	ref, err := tokenReferenceFrom(m.tokenStore(), in, p, 0, now)
	if err != nil || ref.Status != "statistical" || !ref.ExactSize || ref.Truncated || len(ref.Search) != 1 {
		t.Fatalf("ref=%+v err=%v", ref, err)
	}
}
