package monitor

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"os"
	"strconv"
	"testing"
)

func tokenRC48TestPolicy(epoch string, from int64) tokenDetectionPolicy {
	p := detectionPolicy()
	p.SourceContract = &tokenSourceContract{Version: tokenRC48Contract, Epoch: epoch, From: from, Evidence: "fixture:producer-release-and-settlement-source"}
	return p
}

func TestTokenRC48ContractPreservesRawMissingAndScope(t *testing.T) {
	s := tokenMeteringConfig()
	p := tokenRC48TestPolicy(s.TokenFactsSourceEpoch, 100)
	s.TokenDetectionPolicy = tokenDetectionJSON(p)
	expr := base64.StdEncoding.EncodeToString([]byte(`tier("base",p*4+cr*0.2+cc*5+cc1h*8+c*20)`))
	raw := tokenMeteringRawFixture(`{"billing_mode":"tiered_expr","billing_unit":"token","billing_source":"wallet","expr_b64":"` + expr + `","matched_tier":"base","usage_semantic":"anthropic","claude":true,"request_conversion":["Claude Messages"],"request_path":"/v1/messages","cache_tokens":100,"cache_creation_tokens":50,"cache_ratio":0.1,"model_ratio":1,"completion_ratio":5,"group_ratio":1.05,"user_group_ratio":-1,"model_price":0}`)
	raw.CreatedAt = 101
	raw.Input = sql.NullString{String: "20", Valid: true}
	raw.Output = sql.NullString{String: "10", Valid: true}
	f, _ := parseTokenMetering(s, raw, 102)
	u := tokenMeteringDecode(t, f)
	if u.Fields["cache_creation_tokens_1h"].State != "missing" || u.Fields["effective_group_ratio"].State != "missing" || u.Contract == nil || u.TotalInput == nil || *u.TotalInput != 170 {
		t.Fatalf("raw/source contract %+v", u)
	}
	price := tokenPrice(u, p, big.NewRat(1, 1))
	if !price.Supported || price.Quota != "289" {
		t.Fatalf("tier/TTL/group formula %+v", price)
	}
	raw.CreatedAt = 99
	old, _ := parseTokenMetering(s, raw, 102)
	older := tokenMeteringDecode(t, old)
	if older.Contract != nil || tokenPrice(older, p, big.NewRat(1, 1)).Supported {
		t.Fatal("current producer binding filled a historical request")
	}
	raw.CreatedAt = 101
	raw.Other = sql.NullString{String: `{"billing_mode":"tiered_expr","billing_source":"wallet","request_path":"/v1/messages","cache_tokens":null}`, Valid: true}
	bad, _ := parseTokenMetering(s, raw, 102)
	if tokenMeteringDecode(t, bad).Contract != nil {
		t.Fatal("incomplete emitter assigned a contract")
	}
}

func TestTokenRC48ProductionUsageReplay(t *testing.T) {
	path := os.Getenv("TOKEN_RC48_REPLAY_INPUT")
	if path == "" {
		t.Skip("retained read-only production sample not supplied")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var captured struct {
		From int64 `json:"from"`
		Rows []struct {
			ID     int64           `json:"log_id"`
			At     int64           `json:"created_at"`
			Input  int64           `json:"input_tokens"`
			Output int64           `json:"output_tokens"`
			Quota  int64           `json:"quota"`
			Stream int64           `json:"is_stream"`
			Other  json.RawMessage `json:"other"`
		} `json:"rows"`
	}
	if err = json.Unmarshal(body, &captured); err != nil {
		t.Fatal(err)
	}
	s := tokenMeteringConfig()
	p := tokenRC48TestPolicy(s.TokenFactsSourceEpoch, captured.From)
	s.TokenDetectionPolicy = tokenDetectionJSON(p)
	supported, mismatch := 0, 0
	gaps := map[string]int{}
	for _, r := range captured.Rows {
		raw := tokenMeteringRawFixture(string(r.Other))
		raw.ID = r.ID
		raw.CreatedAt = r.At
		raw.Input = sql.NullString{String: strconv.FormatInt(r.Input, 10), Valid: true}
		raw.Output = sql.NullString{String: strconv.FormatInt(r.Output, 10), Valid: true}
		raw.Quota = sql.NullString{String: strconv.FormatInt(r.Quota, 10), Valid: true}
		raw.Stream = sql.NullString{String: strconv.FormatInt(r.Stream, 10), Valid: true}
		f, _ := parseTokenMetering(s, raw, r.At+1)
		u := tokenMeteringDecode(t, f)
		price := tokenPrice(u, p, big.NewRat(1, 1))
		if !price.Supported {
			gaps[price.Reason]++
			continue
		}
		supported++
		if price.Quota != strconv.FormatInt(r.Quota, 10) {
			mismatch++
			t.Logf("id=%d logged=%d reconstructed=%s raw=%s semantic=%s", r.ID, r.Quota, price.Quota, price.RawQuota, u.Semantic)
		}
	}
	t.Logf("retained production rows=%d supported=%d mismatches=%d gaps=%v", len(captured.Rows), supported, mismatch, gaps)
	if supported < 90 || mismatch > 0 {
		t.Fatalf("production replay: supported=%d mismatches=%d", supported, mismatch)
	}
}

func TestTokenRC48TieredFloatRoundingAndTTL(t *testing.T) {
	s := tokenMeteringConfig()
	p := tokenRC48TestPolicy(s.TokenFactsSourceEpoch, 100)
	s.TokenDetectionPolicy = tokenDetectionJSON(p)
	// 249/1e6*500000 is 124.49999999999999 in the producer, not 124.5.
	expr := base64.StdEncoding.EncodeToString([]byte(`tier("base",p)`))
	raw := tokenMeteringRawFixture(`{"billing_mode":"tiered_expr","billing_unit":"token","billing_source":"wallet","expr_b64":"` + expr + `","matched_tier":"base","request_path":"/v1/responses","cache_tokens":0,"cache_ratio":0.1,"model_ratio":1,"completion_ratio":5,"group_ratio":1,"user_group_ratio":7,"model_price":0}`)
	raw.CreatedAt = 101
	raw.Input = sql.NullString{String: "249", Valid: true}
	raw.Output = sql.NullString{String: "0", Valid: true}
	f, _ := parseTokenMetering(s, raw, 102)
	u := tokenMeteringDecode(t, f)
	price := tokenPrice(u, p, big.NewRat(1, 1))
	if !price.Supported || price.Quota != "124" {
		t.Fatalf("producer rounding %+v", price)
	}
	if tokenMeteringOperand(u, "effective_group_ratio").Value != "1" {
		t.Fatal("user group ratio applied twice")
	}
	if u.Fields["cache_creation_tokens"].State != "missing" || tokenMeteringOperandInt(u, "cache_creation_tokens") == nil {
		t.Fatal("raw and derived presence conflated")
	}
}
