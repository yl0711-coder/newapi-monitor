package monitor

import (
	"math"
	"reflect"
	"testing"
)

func TestCollectWebsiteGroupSourcesUsesUserVisibleAndSpecialGroups(t *testing.T) {
	sources, skipped := collectWebsiteGroupSources(
		[]string{" b ", "a", "a"},
		map[string]float64{"a": 1.2, "b": 2, "special": 3, "special2": 4, "legacy": 5, "zero": 0, "negative": -1},
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
	if isPositiveFiniteWebsiteGroupRatio(math.NaN()) || isPositiveFiniteWebsiteGroupRatio(math.Inf(1)) || isPositiveFiniteWebsiteGroupRatio(0) {
		t.Fatal("non-positive or non-finite ratios should be rejected")
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
