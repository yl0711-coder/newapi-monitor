package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"reflect"
	"testing"

	_ "github.com/glebarez/go-sqlite"
)

func TestCollectWebsiteGroupSourcesUsesUserVisibleAndSpecialGroups(t *testing.T) {
	sources, skipped := collectWebsiteGroupSources(
		[]string{" b ", "a", "a"},
		map[string]float64{"a": 1.2, "b": 2, "special": 3, "special2": 4, "legacy": 5, "hidden": .7, "zero": 0, "negative": -1},
		map[string]map[string]string{
			"default": {"-:b": "remove", "special": "", "+:special2": "description", "zero": "", "missing": ""},
			"vip":     {"negative": "", "-:a": "remove", "append_1": "legacy"},
		},
		[]string{" hidden ", "zero", "effective-missing"},
	)
	got := make([]string, 0, len(sources))
	for _, source := range sources {
		got = append(got, source.Name)
	}
	if want := []string{"a", "b", "hidden", "legacy", "special", "special2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("groups = %v, want %v", got, want)
	}
	if skipped != 4 {
		t.Fatalf("skipped = %d, want 4", skipped)
	}
}

func TestCollectWebsiteGroupSourcesIncludesProductionSpecialSyntax(t *testing.T) {
	sources, skipped := collectWebsiteGroupSources(
		[]string{"codex-1.2x"},
		map[string]float64{"codex-0.7x": .7, "codex-1.2x": 1.2, "codex-1.4x": 1.4},
		map[string]map[string]string{
			"shangtang": {"codex-0.7x": "special user group", "-:codex-1.4x": "remove"},
		},
		nil,
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

func TestFetchEffectiveWebsiteGroupsUsesOnlyActiveUsersAndTokens(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/groups.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, statement := range []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, status INTEGER, deleted_at TIMESTAMP, `group` TEXT)",
		"CREATE TABLE tokens (id INTEGER PRIMARY KEY, user_id INTEGER, status INTEGER, deleted_at TIMESTAMP, `group` TEXT)",
		`INSERT INTO users(id,status,deleted_at,"group") VALUES
            (1,1,NULL,'codex 订阅'),
            (2,2,NULL,'disabled-user'),
            (3,1,'2026-01-01','deleted-user'),
            (4,1,NULL,' active-default ')`,
		`INSERT INTO tokens(id,user_id,status,deleted_at,"group") VALUES
            (10,1,1,NULL,''),
            (11,1,1,NULL,'explicit-token'),
            (12,1,2,NULL,'disabled-token'),
            (13,1,1,'2026-01-01','deleted-token'),
            (14,2,1,NULL,'inactive-owner-token'),
            (15,4,1,NULL,' explicit-token ')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("schema/fixture: %v", err)
		}
	}
	m := &Monitor{prodDB: db}
	groups, err := m.fetchEffectiveWebsiteGroups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"active-default", "codex 订阅", "explicit-token"}; !reflect.DeepEqual(groups, want) {
		t.Fatalf("effective groups = %v, want %v", groups, want)
	}
}

func TestFetchConfiguredWebsiteGroupsReadsAuthoritativeOption(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/configured-groups.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec("CREATE TABLE options (`key` TEXT PRIMARY KEY, `value` TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO options(`key`,`value`) VALUES (?,?)", websiteGroupUsableOption, `{" vip ":"VIP","default":"默认","":"忽略"}`); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{prodDB: db}
	groups, err := m.fetchConfiguredWebsiteGroups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"default", "vip"}; !reflect.DeepEqual(groups, want) {
		t.Fatalf("configured groups = %v, want %v", groups, want)
	}
}

func TestFetchWebsiteGroupSourcesDoesNotDependOnPricingHTTP(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/website-group-sources.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, statement := range []string{
		"CREATE TABLE options (`key` TEXT PRIMARY KEY, `value` TEXT)",
		"CREATE TABLE users (id INTEGER PRIMARY KEY, status INTEGER, deleted_at TIMESTAMP, `group` TEXT)",
		"CREATE TABLE tokens (id INTEGER PRIMARY KEY, user_id INTEGER, status INTEGER, deleted_at TIMESTAMP, `group` TEXT)",
		`INSERT INTO users(id,status,deleted_at,"group") VALUES (1,1,NULL,'hidden-live')`,
		`INSERT INTO tokens(id,user_id,status,deleted_at,"group") VALUES (1,1,1,NULL,'')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("schema/fixture: %v", err)
		}
	}
	options := map[string]string{
		websiteGroupUsableOption:  `{"default":"默认分组"}`,
		websiteGroupRatioOption:   `{"default":1,"hidden-live":0.7,"special":1.2}`,
		websiteGroupSpecialOption: `{"default":{"+:special":"特殊分组"}}`,
	}
	for key, value := range options {
		if _, err := db.Exec("INSERT INTO options(`key`,`value`) VALUES (?,?)", key, value); err != nil {
			t.Fatal(err)
		}
	}
	// BaseURL 故意不可用：RC26 可保护 /api/pricing，本功能仍应完全依靠只读数据库成功。
	m := &Monitor{prodDB: db, cfg: Settings{NewAPIBaseURL: "http://127.0.0.1:1"}}
	sources, skipped, err := m.fetchWebsiteGroupSources(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if skipped != 0 {
		t.Fatalf("skipped = %d, want 0", skipped)
	}
	got := make([]string, 0, len(sources))
	for _, source := range sources {
		got = append(got, source.Name)
	}
	if want := []string{"default", "hidden-live", "special"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sources = %v, want %v", got, want)
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

func TestWebsiteGroupBusinessScopeDefaultsIncludedAndPersistsExclusion(t *testing.T) {
	m := newStabilityTestMonitor(t)
	if err := m.storeDB.Create(&WebsiteGroupCatalog{Grp: "internal-test", Source: "newapi", SourceMultiplier: 1, Active: true, SyncedAt: 100}).Error; err != nil {
		t.Fatal(err)
	}
	finance := channelFinanceSnapshot{siteGroups: map[string]ChannelSaleGroupRate{"internal-test": {Grp: "internal-test", Multiplier: 1}}}
	views, _, err := m.loadWebsiteGroupRates(context.Background(), finance)
	if err != nil {
		t.Fatal(err)
	}
	if len(views) != 1 || !views[0].BusinessIncluded {
		t.Fatalf("无配置的老分组必须默认勾选: %+v", views)
	}
	if err := m.storeDB.Create(&ChannelBusinessGroupPolicy{Grp: "internal-test", Included: false, UpdatedAt: 101}).Error; err != nil {
		t.Fatal(err)
	}
	views, _, err = m.loadWebsiteGroupRates(context.Background(), finance)
	if err != nil {
		t.Fatal(err)
	}
	if len(views) != 1 || views[0].BusinessIncluded {
		t.Fatalf("显式取消勾选后必须返回 false: %+v", views)
	}
}
