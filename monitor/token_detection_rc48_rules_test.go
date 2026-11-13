package monitor

import (
	"context"
	"database/sql"
	"encoding/base64"
	"math/big"
	"strconv"
	"testing"
)

func tokenRC48RuleFixture(t *testing.T, s Settings, id, at, input, read, quota int64, changes map[string]any) TokenMeteringFact {
	t.Helper()
	other := map[string]any{"billing_mode": "tiered_expr", "billing_unit": "token", "billing_source": "wallet", "expr_b64": base64.StdEncoding.EncodeToString([]byte(`tier("base",p*4+cr*0.4+cc*5+c*20)`)), "matched_tier": "base", "usage_semantic": "anthropic", "request_path": "/v1/messages", "cache_tokens": read, "cache_creation_tokens": 0, "cache_ratio": 0.1, "model_ratio": 1, "completion_ratio": 5, "group_ratio": 1, "model_price": 0}
	for k, v := range changes {
		other[k] = v
	}
	raw := tokenMeteringRawFixture(tokenDetectionJSON(other))
	raw.ID = id
	raw.CreatedAt = at
	raw.RequestID = "fixture-rc48-" + strconv.FormatInt(id, 10)
	raw.Input = sql.NullString{String: strconv.FormatInt(input, 10), Valid: true}
	raw.Output = sql.NullString{String: "0", Valid: true}
	raw.Quota = sql.NullString{String: strconv.FormatInt(quota, 10), Valid: true}
	f, _ := parseTokenMetering(s, raw, at+1)
	return f
}

func TestTokenRC48TierTTLGoldenBoundaries(t *testing.T) {
	s := tokenMeteringConfig()
	p := tokenRC48TestPolicy(s.TokenFactsSourceEpoch, 100)
	s.TokenDetectionPolicy = tokenDetectionJSON(p)
	cases := []struct {
		name, expr, semantic, want     string
		input, read, write, hour, long int64
	}{
		// Producer float64 TTL expansion makes 124.50000000000001 => 125.
		// Rational TTL expansion instead produces 124.49999999999999 => 124.
		{"one-hour fallback", "tier(\"base\",cc*5)", "anthropic", "125", 0, 0, 36, 23, 0},
		// cr is not priced separately here, so its 30 tokens remain in p.
		{"twelve-hour fallback", "tier(\"base\",p+cc)", "openai", "55", 100, 30, 20, 0, 10},
		{"twelve-hour separate", "tier(\"base\",p+cr*0.1+cc*2+cc12h*3)", "openai", "52", 100, 30, 20, 0, 10},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := tokenRC48RuleFixture(t, s, 1, 101, c.input, c.read, 0, map[string]any{"usage_semantic": c.semantic, "expr_b64": base64.StdEncoding.EncodeToString([]byte(c.expr)), "cache_creation_tokens": c.write, "cache_creation_tokens_1h": c.hour, "cache_creation_tokens_12h": c.long})
			got := tokenPrice(tokenMeteringDecode(t, f), p, big.NewRat(1, 1))
			if !got.Supported || got.Quota != c.want {
				t.Fatalf("producer golden quota %s: %+v", c.want, got)
			}
		})
	}
}

func TestTokenRC48RulesNormalCacheShiftAndFormulaDifference(t *testing.T) {
	s := tokenMeteringConfig()
	p := tokenRC48TestPolicy(s.TokenFactsSourceEpoch, 100)
	s.TokenDetectionPolicy = tokenDetectionJSON(p)
	normal := tokenRC48RuleFixture(t, s, 1, 101, 1000, 9000, 3800, nil)
	u := tokenMeteringDecode(t, normal)
	share, intensity := tokenCacheMetrics(u)
	if share != "9/10" || tokenRatio(intensity).Cmp(big.NewRat(19, 100)) != 0 {
		t.Fatal(share, intensity)
	}
	upper := int64(10000)
	ref := tokenReference{Status: "statistical", Upper: &upper, SampleCount: 3, Samples: []tokenReferenceSample{{ReadShare: share, Intensity: intensity}, {ReadShare: share, Intensity: intensity}, {ReadShare: share, Intensity: intensity}}}
	for _, c := range []struct {
		name                      string
		input, read, quota, prior int64
		cache, formula            bool
		diff                      string
	}{
		{"normal", 1000, 9000, 3800, 2, false, false, "0"},
		{"first cache shift waits for persistence", 10000, 0, 20000, 0, false, false, "0"},
		{"sustained cache shift is statistical", 10000, 0, 20000, 2, true, false, "0"},
		{"recomputable quota difference", 1000, 9000, 3807, 2, false, true, "7"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := tokenRC48RuleFixture(t, s, 2, 102, c.input, c.read, c.quota, nil)
			in, err := tokenInputForPolicy(f, p)
			if err != nil {
				t.Fatal(err)
			}
			r := tokenDetectionEvaluate(in, p, ref, c.prior, true)
			if !r.Rules[0].Eligible || r.Rules[0].Hit || !r.Rules[2].Eligible || r.Rules[2].Hit != c.cache || !r.Rules[3].Eligible || r.Rules[3].Hit != c.formula || r.FormulaDifference == nil || *r.FormulaDifference != c.diff {
				t.Fatalf("rules %+v", r)
			}
			a := tokenAssessFrozen(tokenDetectionFrozen{Input: in, Result: r, Policy: p})
			if a.UpstreamConfirmed {
				t.Fatal("statistics/formula became supplier overcharge")
			}
			if c.cache && (r.Rules[2].EvidenceLevel != "inferred" || r.EstimatedLoss != nil) {
				t.Fatal("cache shift fabricated exact loss", r)
			}
		})
	}
}

func TestTokenRC48PolicyBindingRequeuesAndRevocationIsIdempotent(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	m.cfg.TokenFactsUsageAdapter = "unknown"
	p := detectionPolicy()
	m.cfg.TokenDetectionPolicy = tokenDetectionJSON(p)
	f := tokenRC48RuleFixture(t, m.cfg, 100, now-30, 1000, 9000, 3800, nil)
	if err := m.tokenStore().Create(&f).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now)
	old, result := detectionHead(t, m, f.LogID)
	if result.Price.Supported {
		t.Fatal("unbound producer priced")
	}
	p.SourceContract = &tokenSourceContract{Version: tokenRC48Contract, Epoch: f.SourceEpoch, From: now - 100, Evidence: "fixture:verified release and settlement source"}
	m.cfg.TokenDetectionPolicy = tokenDetectionJSON(p)
	for i := int64(1); i <= 3; i++ {
		detectionRun(t, m, now+i)
	}
	bound, result := detectionHead(t, m, f.LogID)
	if bound.Revision <= old.Revision || !result.Price.Supported || result.Price.Quota != "3800" {
		t.Fatalf("policy change was not redetected: %+v %+v", bound, result)
	}
	if n, err := m.runTokenDetectionRound(context.Background(), now+4); err != nil || n != 0 {
		t.Fatal("unchanged decision rewritten", n, err)
	}
	// Retained facts produced with a binding must lose its live derived values
	// if that binding is removed; frozen older revisions remain unchanged.
	stored := tokenRC48RuleFixture(t, m.cfg, 101, now-20, 1000, 9000, 3800, nil)
	p.SourceContract = nil
	in, err := tokenInputForPolicy(stored, p)
	if err != nil {
		t.Fatal(err)
	}
	if in.Usage.Contract != nil || len(in.Usage.ContractOperands) != 0 || tokenPrice(in.Usage, p, big.NewRat(1, 1)).Supported {
		t.Fatal("revoked binding inherited", in.Usage)
	}
	m.cfg.TokenDetectionPolicy = tokenDetectionJSON(p)
	for i := int64(5); i <= 7; i++ {
		detectionRun(t, m, now+i)
	}
	revoked, result := detectionHead(t, m, f.LogID)
	if revoked.Revision <= bound.Revision || result.Price.Supported {
		t.Fatal("binding removal not requeued", revoked, result)
	}
}
