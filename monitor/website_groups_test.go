package monitor

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func TestCollectWebsiteGroupSourcesUsesUserVisibleAndSpecialGroups(t *testing.T) {
	sources, skipped := collectWebsiteGroupSources(
		[]string{" b ", "a", "a"},
		map[string]float64{"a": 1.2, "b": 2, "special": 3, "special2": 4, "legacy": 5, "hidden": .7, "zero": 0, "negative": -1},
		map[string]map[string]string{
			"default": {"-:b": "remove", "special": "", "+:special2": "description", "zero": "", "missing": ""},
			"vip":     {"negative": "", "-:a": "remove", "append_1": "legacy"},
		},
	)
	got := make([]string, 0, len(sources))
	for _, source := range sources {
		got = append(got, source.Name)
	}
	if want := []string{"a", "b", "legacy", "special", "special2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("groups = %v, want %v", got, want)
	}
	if skipped != 3 {
		t.Fatalf("skipped = %d, want 3", skipped)
	}
}

func TestCollectWebsiteGroupSourcesIncludesProductionSpecialSyntax(t *testing.T) {
	sources, skipped := collectWebsiteGroupSources(
		[]string{"codex-1.2x"},
		map[string]float64{"codex-0.7x": .7, "codex-1.2x": 1.2, "codex-1.4x": 1.4},
		map[string]map[string]string{
			"shangtang": {"codex-0.7x": "special user group", "-:codex-1.4x": "remove"},
		},
	)
	got := make([]string, 0, len(sources))
	for _, source := range sources {
		got = append(got, source.Name)
	}
	if want := []string{"codex-0.7x", "codex-1.2x"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("groups = %v, want %v", got, want)
	}
	if skipped != 0 {
		t.Fatalf("skipped = %d, want 0", skipped)
	}
}

func TestParseWebsiteGroupRatio(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
		want float64
	}{
		{name: "number", raw: `1.25`, want: 1.25},
		{name: "string", raw: `"2.5"`, want: 2.5},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseWebsiteGroupRatio([]byte(test.raw))
			if err != nil || got != test.want {
				t.Fatalf("parse = %v, %v; want %v", got, err, test.want)
			}
		})
	}
	if _, err := parseWebsiteGroupRatio([]byte(`"not-a-number"`)); err == nil {
		t.Fatal("malformed ratio should fail")
	}
	if _, err := parseWebsiteGroupRatio([]byte(`"1.2junk"`)); err == nil {
		t.Fatal("ratio with trailing garbage should fail")
	}
	if isPositiveFiniteWebsiteGroupRatio(math.NaN()) || isPositiveFiniteWebsiteGroupRatio(math.Inf(1)) || isPositiveFiniteWebsiteGroupRatio(0) {
		t.Fatal("non-positive or non-finite ratios should be rejected")
	}
}

func TestMergeWebsiteGroupRatiosIncludesSpecialOnlyAuthoritativeGroup(t *testing.T) {
	ratios, err := mergeWebsiteGroupRatios(
		map[string]json.RawMessage{
			"codex-1.2x": json.RawMessage(`1.2`),
			"shared":     json.RawMessage(`1`),
		},
		`{"codex-0.7x":0.7,"shared":1.1,"invalid":0}`,
	)
	if err != nil {
		t.Fatalf("merge ratios: %v", err)
	}
	if got := ratios["codex-0.7x"]; got != .7 {
		t.Fatalf("special-only ratio = %v, want 0.7", got)
	}
	if got := ratios["shared"]; got != 1.1 {
		t.Fatalf("authoritative ratio = %v, want 1.1", got)
	}
	if _, ok := ratios["invalid"]; ok {
		t.Fatal("non-positive authoritative ratio must be rejected")
	}
}

func TestMergeWebsiteGroupRatiosRejectsMalformedAuthoritativeOption(t *testing.T) {
	if _, err := mergeWebsiteGroupRatios(nil, `{`); err == nil {
		t.Fatal("malformed GroupRatio option must fail closed")
	}
}

func TestWebsiteGroupCatalogChanged(t *testing.T) {
	sources := []websiteGroupSource{{Name: "a", Multiplier: 1.2}}
	catalog := []WebsiteGroupCatalog{{Grp: "a", Source: "newapi", SourceMultiplier: 1.2, Active: true}}
	site := []ChannelSaleGroupRate{{Grp: "a", Multiplier: 1.2}}
	if websiteGroupCatalogChanged(catalog, site, sources) {
		t.Fatal("same active catalog and site rate should be unchanged")
	}
	site[0].Multiplier = 1.3
	if !websiteGroupCatalogChanged(catalog, site, sources) {
		t.Fatal("changed site rate should be detected")
	}
	site[0].Multiplier = 1.2
	site = append(site, ChannelSaleGroupRate{Grp: "stale", Multiplier: 9})
	if !websiteGroupCatalogChanged(catalog, site, sources) {
		t.Fatal("stale site group should be detected")
	}
}
