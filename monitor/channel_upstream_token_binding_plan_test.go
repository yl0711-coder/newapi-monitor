package monitor

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

type tokenBindingPlanFixture struct {
	account  ChannelUpstreamAccount
	configs  []ChannelUpstreamTokenConfig
	channels []ChannelSnap
	key      []byte
}

func newTokenBindingPlanFixture() tokenBindingPlanFixture {
	a := ChannelUpstreamAccount{Domain: "upstream.example", Provider: upstreamProviderSub2API, BaseURL: "https://upstream.example", UserID: 31, Account: "test"}
	f := tokenBindingPlanFixture{account: a, key: []byte("0123456789abcdef0123456789abcdef")}
	for i, id := range []string{"575", "574"} {
		f.configs = append(f.configs, ChannelUpstreamTokenConfig{Domain: a.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(a), Provider: a.Provider, ChannelID: 185 + i, Revision: 1, TokenID: id, CreatedAt: 7000})
		f.channels = append(f.channels, ChannelSnap{ID: 185 + i, BaseDomain: a.Domain, Name: "same-name", Groups: "same-group", Status: 1})
	}
	return f
}

func (f tokenBindingPlanFixture) plan(t *testing.T, history []ChannelCostSourceBinding, now int64) upstreamTokenBindingPlan {
	t.Helper()
	p, err := buildUpstreamTokenBindingPlan(f.account, f.configs, f.channels, history, f.key, "v1", now)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// In-memory simulator only; deliberately not a production DB writer.
func simulateTokenBindingPlan(t *testing.T, history []ChannelCostSourceBinding, p upstreamTokenBindingPlan) []ChannelCostSourceBinding {
	t.Helper()
	out := append([]ChannelCostSourceBinding(nil), history...)
	for _, change := range p.Changes {
		if err := validateCostSourceBinding(change.After); err != nil {
			t.Fatal(err)
		}
		if change.Before != nil {
			found := false
			for i, b := range out {
				if channelCostBindingSignature(b) == channelCostBindingSignature(*change.Before) {
					found = true
					if change.ReplacePending {
						out = append(out[:i], out[i+1:]...)
					} else {
						out[i].ValidTo = p.EffectiveFrom
					}
					break
				}
			}
			if !found {
				t.Fatal("stale simulated plan")
			}
		}
		out = append(out, change.After)
	}
	return out
}

func TestUpstreamTokenBindingPlanSeparateSharedAndNoNameDependency(t *testing.T) {
	f := newTokenBindingPlanFixture()
	p := f.plan(t, nil, 7205)
	if p.EffectiveFrom != 10800 || len(p.Changes) != 2 || len(p.RequiredSources) != 2 {
		t.Fatalf("%+v", p)
	}
	seen := map[int]bool{}
	for _, c := range p.Changes {
		if c.Before != nil || c.ReplacePending || c.After.AllocationMode != "allocated" {
			t.Fatal("invalid initial plan", c)
		}
		seen[c.After.LocalChannelID] = true
	}
	if !seen[185] || !seen[186] {
		t.Fatal("same-group tokens not separated")
	}
	f.channels[0].Name = "renamed"
	f.channels[0].Groups = "new-group"
	f.channels[0].Status = 2
	if got := f.plan(t, nil, 7205); !reflect.DeepEqual(got, p) {
		t.Fatal("mutable name/group or disabled status changed ownership")
	}
	f.configs[1].TokenID = "575"
	shared := f.plan(t, nil, 7205)
	if len(shared.Changes) != 1 || shared.Changes[0].After.AllocationMode != "shared" || shared.Changes[0].After.LocalChannelID != 0 || len(shared.RequiredSources) != 1 {
		t.Fatal("shared token double allocated", shared)
	}
}

func TestUpstreamTokenBindingPlanReplacementClearAndHistory(t *testing.T) {
	f := newTokenBindingPlanFixture()
	p := f.plan(t, nil, 7205)
	history := simulateTokenBindingPlan(t, nil, p)
	original := append([]ChannelCostSourceBinding(nil), history...)
	if p := f.plan(t, history, 10900); len(p.Changes) != 0 || len(p.RequiredSources) != 0 {
		t.Fatal("same settings churn bindings")
	}
	// Change to an unobserved new token. This only proposes attribution and
	// explicitly requires proof; it cannot make the new token active itself.
	f.configs[0].TokenID = "999"
	f.configs[0].Revision++
	f.configs[0].CreatedAt = 10900
	p = f.plan(t, history, 10900)
	if p.EffectiveFrom != 14400 || len(p.Changes) != 2 || len(p.RequiredSources) != 1 {
		t.Fatal("wrong replacement", p)
	}
	if !reflect.DeepEqual(history, original) {
		t.Fatal("planning mutated historical input")
	}
	history = simulateTokenBindingPlan(t, history, p)
	for _, old := range original {
		found := false
		for _, b := range history {
			if b.SourceRef == old.SourceRef && b.ValidFrom == old.ValidFrom {
				want := old
				if b.LocalChannelID == 185 {
					want.ValidTo = 14400
				}
				if b != want {
					t.Fatal("old binding fields changed")
				}
				found = true
			}
		}
		if !found {
			t.Fatal("lost historical interval")
		}
	}
	f.configs[0].TokenID = ""
	f.configs[0].Revision++
	f.configs[0].CreatedAt = 14500
	p = f.plan(t, history, 14500)
	if len(p.Changes) != 1 || p.Changes[0].After.AllocationMode != "unallocated" || p.Changes[0].After.LocalChannelID != 0 || len(p.RequiredSources) != 0 {
		t.Fatal("clear guessed fallback ownership", p)
	}
}

func TestUpstreamTokenBindingPlanSameHourChangesAndSharedRemoval(t *testing.T) {
	f := newTokenBindingPlanFixture()
	history := simulateTokenBindingPlan(t, nil, f.plan(t, nil, 7205))
	f.configs[1].TokenID = "575"
	f.configs[1].Revision++
	f.configs[1].CreatedAt = 7300
	p := f.plan(t, history, 7300)
	if len(p.Changes) != 2 {
		t.Fatal("missing demotion of former token", p)
	}
	for _, c := range p.Changes {
		if !c.ReplacePending || c.After.ValidFrom != 10800 {
			t.Fatal("pending same-hour change backdated")
		}
	}
	history = simulateTokenBindingPlan(t, history, p)
	if len(history) != 2 {
		t.Fatal("same-hour plan added overlapping intervals")
	}
	f.configs[1].TokenID = ""
	f.configs[1].Revision++
	f.configs[1].CreatedAt = 10900
	p = f.plan(t, history, 10900)
	if len(p.Changes) != 1 || p.Changes[0].After.LocalChannelID != 185 || p.Changes[0].After.AllocationMode != "allocated" || len(p.RequiredSources) != 1 {
		t.Fatal("shared removal did not reverify remaining owner", p)
	}
	if p.Changes[0].Before.AllocationMode != "shared" || p.Changes[0].ReplacePending {
		t.Fatal("active shared interval overwritten")
	}
}

func TestUpstreamTokenBindingPlanDigestAndNewAPICompatibility(t *testing.T) {
	f := newTokenBindingPlanFixture()
	p := f.plan(t, nil, 7205)
	f.configs[0], f.configs[1] = f.configs[1], f.configs[0]
	f.channels[0], f.channels[1] = f.channels[1], f.channels[0]
	if got := f.plan(t, nil, 7205); !reflect.DeepEqual(got, p) {
		t.Fatal("input order changed plan")
	}
	f.configs[0].Revision++
	if got := f.plan(t, nil, 7205); got.Digest == p.Digest {
		t.Fatal("stale same-value config accepted")
	}
	f.account.Provider = upstreamProviderNewAPI
	for i := range f.configs {
		f.configs[i].Provider = f.account.Provider
		f.configs[i].AccountEpoch = newAPIUpstreamAccountEpoch(f.account)
	}
	n := f.plan(t, nil, 7205)
	for i, c := range n.Changes {
		if c.After.SourceRefKind != channelCostSourceKindNewAPIToken || c.After.SourceRef == p.Changes[i].After.SourceRef {
			t.Fatal("provider identities crossed")
		}
	}
	f.configs[0].TokenID = "9007199254740993"
	large := f.plan(t, nil, 7205)
	f.configs[0].TokenID = "9007199254740992"
	if f.plan(t, nil, 7205).Digest == large.Digest {
		t.Fatal("large IDs rounded")
	}
	encoded, err := json.Marshal(large)
	if err != nil || strings.Contains(string(encoded), "9007199254740993") || strings.Contains(string(encoded), string(f.key)) {
		t.Fatal("raw identity/key leaked into plan")
	}
}

func TestUpstreamTokenBindingPlanRejectsUnsafeSnapshots(t *testing.T) {
	for _, name := range []string{"missing-channel", "missing-config", "deleted-channel", "moved-channel", "duplicate-config", "wrong-account", "future-config", "unsupported", "manual-conflict", "future-binding", "overlap", "wrong-key", "too-many", "invalid-time"} {
		t.Run(name, func(t *testing.T) {
			f := newTokenBindingPlanFixture()
			history := simulateTokenBindingPlan(t, nil, f.plan(t, nil, 7205))
			now := int64(10900)
			switch name {
			case "missing-channel":
				f.channels = f.channels[1:]
			case "missing-config":
				f.configs = f.configs[1:]
			case "deleted-channel":
				f.channels[0].DeletedAt = 8000
			case "moved-channel":
				f.channels[0].BaseDomain = "another.example"
			case "duplicate-config":
				f.configs = append(f.configs, f.configs[0])
			case "wrong-account":
				f.configs[0].AccountEpoch = strings.Repeat("a", 64)
			case "future-config":
				f.configs[0].CreatedAt = now + 1
			case "unsupported":
				f.account.UsageAdapter = upstreamUsageAdapterSub2Stats
			case "manual-conflict":
				for i := range history {
					history[i].MappingSource = "manual"
					history[i].LocalChannelID = 999
				}
			case "future-binding":
				history[0].ValidFrom = 18000
			case "overlap":
				history = append(history, history[0])
			case "wrong-key":
				history[0].HMACKeyID = "another-key"
			case "too-many":
				f.channels = make([]ChannelSnap, upstreamTokenBindingMaxChannels+1)
			case "invalid-time":
				now = 0
			}
			before := append([]ChannelCostSourceBinding(nil), history...)
			p, err := buildUpstreamTokenBindingPlan(f.account, f.configs, f.channels, history, f.key, "v1", now)
			if err == nil || !reflect.DeepEqual(p, upstreamTokenBindingPlan{}) {
				t.Fatalf("accepted %s: %+v %v", name, p, err)
			}
			if !reflect.DeepEqual(before, history) {
				t.Fatal("rejected plan changed input")
			}
		})
	}
}

func TestUpstreamTokenBindingPreviewLatestAccountSnapshotIsReadOnly(t *testing.T) {
	m, a := tokenConfigFixture(t)
	m.cfg.ChannelCostHMACKey = strings.Repeat("unit-test-hmac-", 3)
	m.cfg.ChannelCostHMACKeyID = "v1"
	ctx := context.Background()
	for _, s := range []struct {
		ch    int
		token string
		rev   int64
	}{{185, "575", 0}, {186, "575", 0}, {186, "574", 1}} {
		if _, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(a, s.ch, s.token, s.rev), "fixture", 7000+s.rev); err != nil {
			t.Fatal(err)
		}
	}
	m.cfg.LocalSnapshotOnly = true
	p, err := m.previewUpstreamTokenBindingPlan(ctx, a.Domain, 185, 7205)
	if err != nil || len(p.Changes) != 2 {
		t.Fatalf("%+v %v", p, err)
	}
	for _, c := range p.Changes {
		if c.After.AllocationMode != "allocated" {
			t.Fatal("historical config treated as current")
		}
	}
	other, err := m.previewUpstreamTokenBindingPlan(ctx, a.Domain, 186, 7205)
	if err != nil || !reflect.DeepEqual(p, other) {
		t.Fatal("preview omitted another channel", err)
	}
	for _, model := range []any{&ChannelCostSourceBinding{}, &ChannelUpstreamCostHourEvidence{}, &ChannelEconomicsDirtyHour{}, &ChannelFinanceVersion{}, &ChannelEconomicsHourPublication{}} {
		var count int64
		if err := m.storeDB.Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("preview wrote %T: %d %v", model, count, err)
		}
	}
}

func TestUpstreamTokenBindingPlanBlankDoesNotTakeOverLegacyOwnership(t *testing.T) {
	f := newTokenBindingPlanFixture()
	history := simulateTokenBindingPlan(t, nil, f.plan(t, nil, 7205))
	for i := range history {
		history[i].MappingSource = "manual"
	}
	// Matching explicit IDs can coexist with manual ownership, without
	// replacing its provenance or rewriting its interval.
	if p := f.plan(t, history, 10900); len(p.Changes) != 0 {
		t.Fatal("took over matching manual mappings")
	}
	for i := range f.configs {
		f.configs[i].TokenID = ""
		f.configs[i].Revision++
	}
	if p := f.plan(t, history, 10900); len(p.Changes) != 0 || len(p.RequiredSources) != 0 {
		t.Fatal("blank optional field removed manual mappings")
	}
	f.configs = nil
	if p := f.plan(t, history, 10900); len(p.Changes) != 0 {
		t.Fatal("legacy account without settings changed")
	}
	if p := f.plan(t, nil, 10900); len(p.Changes) != 0 {
		t.Fatal("blank new configuration inferred ownership")
	}
}
