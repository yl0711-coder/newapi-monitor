package usagebill

import (
	"errors"
	"math"
	"testing"
)

func testPriceEvidence() PriceEvidence {
	// Synthetic, not a published model or approved production price.
	return PriceEvidence{ID: "test-short", Model: "synthetic-model", ServiceTier: "standard", SourceURL: "https://example.com/pricing", From: 100, Until: 200, VerifiedAt: 150, MaxInput: 272001, InputUSD: "2", OutputUSD: "10", CacheReadUSD: "0.1", CacheWriteUSD: "2.5"}
}

func TestPriceCatalogueRejectsInvalidOrAmbiguousEvidence(t *testing.T) {
	base := testPriceEvidence()
	for _, tc := range []struct {
		name string
		edit func(*PriceEvidence)
	}{
		{"missing ID", func(e *PriceEvidence) { e.ID = "" }},
		{"missing model", func(e *PriceEvidence) { e.Model = " " }},
		{"missing tier", func(e *PriceEvidence) { e.ServiceTier = "" }},
		{"zero start", func(e *PriceEvidence) { e.From = 0 }},
		{"empty interval", func(e *PriceEvidence) { e.Until = e.From }},
		{"unverified", func(e *PriceEvidence) { e.VerifiedAt = 0 }},
		{"current observation backdated", func(e *PriceEvidence) { e.CurrentObservationOnly = true }},
		{"negative input", func(e *PriceEvidence) { e.MinInput = -1 }},
		{"reversed band", func(e *PriceEvidence) { e.MinInput = 300000 }},
		{"invalid url", func(e *PriceEvidence) { e.SourceURL = "://" }},
		{"insecure source", func(e *PriceEvidence) { e.SourceURL = "http://example.com/pricing" }},
		{"source credential", func(e *PriceEvidence) { e.SourceURL = "https://secret@example.com/pricing" }},
		{"source query", func(e *PriceEvidence) { e.SourceURL += "?key=secret" }},
		{"missing input price", func(e *PriceEvidence) { e.InputUSD = "" }},
		{"negative price", func(e *PriceEvidence) { e.OutputUSD = "-1" }},
		{"malformed read price", func(e *PriceEvidence) { e.CacheReadUSD = "NaN" }},
		{"fraction price", func(e *PriceEvidence) { e.CacheWriteUSD = "1/2" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := base
			tc.edit(&e)
			if _, err := NewPriceCatalogue([]PriceEvidence{e}); !errors.Is(err, ErrInvalidPriceCatalogue) {
				t.Fatal("invalid evidence accepted")
			}
		})
	}
	duplicate := base
	duplicate.ID = "different-id-same-range"
	for _, entries := range [][]PriceEvidence{nil, make([]PriceEvidence, 513), {base, base}, {base, duplicate}} {
		if _, err := NewPriceCatalogue(entries); !errors.Is(err, ErrInvalidPriceCatalogue) {
			t.Fatal("ambiguous catalogue accepted")
		}
	}
	base.From, base.CurrentObservationOnly = base.VerifiedAt, true
	if _, err := NewPriceCatalogue([]PriceEvidence{base}); err != nil {
		t.Fatal(err)
	}
}

func TestPriceCatalogueExactBoundariesAndImmutableSnapshot(t *testing.T) {
	short := testPriceEvidence()
	long := short
	long.ID, long.MinInput, long.MaxInput, long.InputUSD = "test-long", 272001, 0, "4"
	next := short
	next.ID, next.From, next.Until = "test-next", 200, 300
	fast := short
	fast.ID, fast.ServiceTier = "test-fast", "fast"
	source := []PriceEvidence{short, long, next, fast}
	c, err := NewPriceCatalogue(source)
	if err != nil {
		t.Fatal(err)
	}
	source[0].InputUSD = "9999"
	for _, tc := range []struct {
		at, input int64
		want      string
	}{{99, 1, ""}, {100, 0, short.ID}, {199, 272000, short.ID}, {199, 272001, long.ID}, {199, math.MaxInt64, long.ID}, {200, 1, next.ID}, {300, 1, ""}, {150, -1, ""}} {
		e, ok := c.Match(short.Model, "standard", tc.at, tc.input)
		if ok != (tc.want != "") || e.ID != tc.want {
			t.Fatalf("boundary %v: %v %v", tc, e, ok)
		}
		if e.ID == short.ID && e.InputUSD != "2" {
			t.Fatal("caller changed snapshot")
		}
	}
	for _, keys := range [][2]string{{"synthetic-model-alias", "standard"}, {short.Model, ""}, {short.Model, "batch"}} {
		if _, ok := c.Match(keys[0], keys[1], 150, 1); ok {
			t.Fatal("unknown model/tier matched")
		}
	}
	got, _ := c.Match(short.Model, "standard", 150, 1)
	got.InputUSD = "9999"
	again, _ := c.Match(short.Model, "standard", 150, 1)
	if again.InputUSD != "2" {
		t.Fatal("result mutated shared catalogue")
	}
}

func TestPriceReferenceNormalizedUsageAndMissingCachePrice(t *testing.T) {
	price := testPriceEvidence()
	c, err := NewPriceCatalogue([]PriceEvidence{price})
	if err != nil {
		t.Fatal(err)
	}
	n := func(v int64) TokenCount { return TokenCount{State: CountValid, Value: v} }
	u := TokenUsage{Semantic: "openai", Complete: true, OrdinaryInput: n(1000), Output: n(200), CacheRead: n(500), CacheWrite: n(100)}
	before := u
	amount, matched, err := c.ReferenceOpenAIText(price.Model, "standard", 150, u)
	// (1000*2 + 200*10 + 500*0.1 + 100*2.5) / 1M * 7
	if err != nil || amount.Exact() != "301/10000" || matched.ID != price.ID || before != u {
		t.Fatalf("exact amount/identity: %s %v", amount.Exact(), err)
	}
	for _, edit := range []func(*TokenUsage){
		func(u *TokenUsage) { u.Complete = false },
		func(u *TokenUsage) { u.Semantic = "anthropic" },
		func(u *TokenUsage) { u.CacheWrite.State = CountMissing },
		func(u *TokenUsage) { u.Output.Value = -1 },
		func(u *TokenUsage) { u.OrdinaryInput.Value = math.MaxInt64 },
	} {
		bad := u
		edit(&bad)
		if _, _, err := c.ReferenceOpenAIText(price.Model, "standard", 150, bad); !errors.Is(err, ErrIncompletePrice) {
			t.Fatal("incomplete input accepted")
		}
	}
	if _, _, err := c.ReferenceOpenAIText(price.Model, "standard", 99, u); !errors.Is(err, ErrIncompletePrice) {
		t.Fatal("price fell back outside interval")
	}
	price.CacheWriteUSD = ""
	c, err = NewPriceCatalogue([]PriceEvidence{price})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.ReferenceOpenAIText(price.Model, "standard", 150, u); !errors.Is(err, ErrIncompletePrice) {
		t.Fatal("unknown cache rate became free")
	}
	u.CacheWrite = n(0)
	if _, _, err := c.ReferenceOpenAIText(price.Model, "standard", 150, u); err != nil {
		t.Fatal("known zero cache write rejected", err)
	}
}

func TestPriceReferenceContextIncludesCachedInput(t *testing.T) {
	short := testPriceEvidence()
	long := short
	long.ID, long.MinInput, long.MaxInput, long.InputUSD = "long", 272001, 0, "4"
	c, err := NewPriceCatalogue([]PriceEvidence{short, long})
	if err != nil {
		t.Fatal(err)
	}
	n := func(v int64) TokenCount { return TokenCount{State: CountValid, Value: v} }
	u := TokenUsage{Semantic: "openai", Complete: true, OrdinaryInput: n(1), Output: n(0), CacheRead: n(271999), CacheWrite: n(1)}
	_, e, err := c.ReferenceOpenAIText(short.Model, "standard", 150, u)
	if err != nil || e.ID != long.ID {
		t.Fatal("cached input excluded from context length", err)
	}
	u.CacheWrite = n(0)
	_, e, err = c.ReferenceOpenAIText(short.Model, "standard", 150, u)
	if err != nil || e.ID != short.ID {
		t.Fatal("272000-token boundary changed", err)
	}
}
