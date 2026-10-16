package usagebill

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func normalize(contract LogContract, prompt, output int64, other string) TokenUsage {
	return NormalizeTokens(SourceTokens{Contract: contract, Prompt: &prompt, Completion: &output, Other: other})
}

func TestTokenPinnedContractFourCategories(t *testing.T) {
	for _, contract := range []LogContract{ContractRC4, ContractRC26} {
		for _, tc := range []struct {
			name, raw                               string
			input, read, write, unsplit, five, hour int64
		}{
			{"zero omission", `{"cache_tokens":0}`, 100, 0, 0, 0, 0, 0},
			{"inclusive read", `{"cache_tokens":20}`, 80, 20, 0, 0, 0, 0},
			{"inclusive writes", `{"cache_tokens":20,"cache_creation_tokens":40}`, 40, 20, 40, 40, 0, 0},
			{"explicit zero flags", `{"cache_tokens":0,"cache_creation_tokens":0,"cache_creation_tokens_5m":0,"claude":false}`, 100, 0, 0, 0, 0, 0},
			{"exclusive anthropic", `{"usage_semantic":"anthropic","claude":true,"cache_tokens":20,"cache_creation_tokens":40}`, 100, 20, 40, 40, 0, 0},
			{"anthropic not claude format", `{"usage_semantic":"anthropic","claude":false,"cache_tokens":20,"cache_creation_tokens":40}`, 100, 20, 40, 40, 0, 0},
			{"split and remainder", `{"usage_semantic":"anthropic","cache_tokens":20,"cache_creation_tokens":40,"cache_creation_tokens_5m":10,"cache_creation_tokens_1h":20}`, 100, 20, 40, 10, 10, 20},
			{"redundant write total", `{"usage_semantic":"anthropic","cache_tokens":20,"cache_creation_tokens":40,"cache_write_tokens":40,"cache_creation_tokens_5m":10,"cache_creation_tokens_1h":30}`, 100, 20, 40, 0, 10, 30},
			{"explicit inclusive total", `{"usage_semantic":"openai","input_tokens_total":100,"cache_tokens":20,"cache_creation_tokens":40}`, 40, 20, 40, 40, 0, 0},
			{"explicit exclusive total", `{"usage_semantic":"anthropic","input_tokens_total":160,"cache_tokens":20,"cache_creation_tokens":40}`, 100, 20, 40, 40, 0, 0},
			{"integer exponent", `{"cache_tokens":2e1,"cache_creation_tokens":4e1}`, 40, 20, 40, 40, 0, 0},
		} {
			t.Run(string(contract)+"/"+tc.name, func(t *testing.T) {
				got := normalize(contract, 100, 10, tc.raw)
				if !got.Complete || got.Issue != "" || got.OrdinaryInput.Value != tc.input || got.Output.Value != 10 || got.CacheRead.Value != tc.read || got.CacheWrite.Value != tc.write || got.WriteUnsplit.Value != tc.unsplit || got.Write5m.Value != tc.five || got.Write1h.Value != tc.hour {
					t.Fatalf("wrong normalization: %+v", got)
				}
				for _, count := range []TokenCount{got.OrdinaryInput, got.Output, got.CacheRead, got.CacheWrite, got.WriteUnsplit, got.Write5m, got.Write1h} {
					if count.State != CountValid {
						t.Fatal("completed with unknown count")
					}
				}
			})
		}
	}
}

func TestTokenUnknownMissingAndConflictingEvidence(t *testing.T) {
	for _, tc := range []struct{ raw, issue string }{
		{`{}`, "token_count_incomplete"},
		{`{"cache_tokens":null}`, "token_count_incomplete"},
		{`{"cache_tokens":-1}`, "token_count_incomplete"},
		{`{"cache_tokens":1.5}`, "token_count_incomplete"},
		{`{"cache_tokens":"20"}`, "token_count_incomplete"},
		{`{"cache_tokens":true}`, "token_count_incomplete"},
		{`{"cache_tokens":20,"cache_creation_tokens":null}`, "token_count_incomplete"},
		{`{"cache_tokens":20,"cache_creation_tokens_5m":10}`, "write_total_missing"},
		{`{"usage_semantic":"anthropic","cache_tokens":20}`, "write_total_missing"},
		{`{"usage_semantic":"\u0061nthropic","cache_tokens":20}`, "write_total_missing"},
		{`{"cache_tokens":0,"cache_creation_tokens_5m":0}`, "write_total_missing"},
		{`{"cache_tokens":20,"cache_creation_tokens":4,"cache_creation_tokens_5m":5}`, "write_split_exceeds_total"},
		{`{"cache_tokens":20,"cache_creation_tokens":40,"cache_write_tokens":50}`, "write_total_conflict"},
		{`{"cache_tokens":0,"cache_write_tokens":null}`, "write_total_conflict"},
		{`{"cache_tokens":20,"claude":true}`, "semantic_missing_for_claude_format"},
		{`{"cache_tokens":20,"claude":null}`, "format_flag_invalid"},
		{`{"cache_tokens":20,"claude":"false"}`, "format_flag_invalid"},
		{`{"cache_tokens":20,"usage_semantic":null}`, "semantic_unknown"},
		{`{"cache_tokens":20,"usage_semantic":""}`, "semantic_unknown"},
		{`{"cache_tokens":20,"usage_semantic":"other"}`, "semantic_unknown"},
		{`{"cache_tokens":20,"usage_semantic":true}`, "semantic_unknown"},
		{`{"cache_tokens":20,"usage_semantic":"openai","claude":true}`, "semantic_conversion_ambiguous"},
		{`{"cache_tokens":20,"cache_creation_tokens":10,"cache_creation_tokens_5m":10}`, "semantic_conversion_ambiguous"},
		{`{"cache_tokens":90,"cache_creation_tokens":20}`, "cache_exceeds_input"},
		{`{"cache_tokens":101}`, "cache_exceeds_input"},
		{`{"cache_tokens":20,"input_tokens_total":101}`, "input_total_conflict"},
		{`{"cache_tokens":20,"input_tokens_total":null}`, "input_total_conflict"},
		{`{"cache_tokens":0,"cache_tokens":1}`, "metadata_invalid"},
		{`{"cache_tokens":0} {}`, "metadata_invalid"},
		{`{"cache_tokens":NaN}`, "metadata_invalid"},
		{`{`, "metadata_invalid"}, {`{"cache_tokens":}`, "metadata_invalid"},
		{`{"cache_tokens":0,0:1}`, "metadata_invalid"},
		{`[]`, "metadata_invalid"}, {`null`, "metadata_invalid"}, {``, "metadata_invalid"},
		{strings.Repeat(" ", 128<<10) + `{}`, "metadata_invalid"},
	} {
		got := normalize(ContractRC26, 100, 10, tc.raw)
		if got.Complete || got.Issue != tc.issue || got.OrdinaryInput.State == CountValid {
			t.Fatalf("%s: %+v; expected %s", tc.raw, got, tc.issue)
		}
	}
	unknown := normalize("", 100, 10, `{"cache_tokens":0}`)
	if unknown.Complete || unknown.Issue != "source_contract_unknown" || unknown.CacheWrite.State != CountMissing {
		t.Fatal("current-version guess filled a historical gap")
	}
	missing := NormalizeTokens(SourceTokens{Contract: ContractRC26, Other: `{"cache_tokens":0}`})
	if missing.Complete || missing.Output.State != CountMissing {
		t.Fatal("missing original counts became zero")
	}
}

func TestTokenExactCountsAndOverflow(t *testing.T) {
	for _, tc := range []struct {
		prompt int64
		raw    string
		want   int64
	}{
		{math.MaxInt64, `{"cache_tokens":0}`, math.MaxInt64},
		{9007199254740993, `{"cache_tokens":9007199254740992}`, 1},
		{100, `{"cache_tokens":100}`, 0},
		{100, `{"cache_tokens":1.0}`, 99},
	} {
		got := normalize(ContractRC4, tc.prompt, 0, tc.raw)
		if !got.Complete || got.OrdinaryInput.Value != tc.want {
			t.Fatalf("lost integer precision: %+v", got)
		}
	}
	for _, raw := range []string{
		`{"cache_tokens":9223372036854775808}`, `{"cache_tokens":1e1000000000}`,
		`{"cache_tokens":1e-1000000000}`, `{"cache_tokens":1e-1}`,
		`{"cache_tokens":0,"cache_creation_tokens":9223372036854775807,"cache_creation_tokens_5m":9223372036854775807,"cache_creation_tokens_1h":1}`,
		`{"usage_semantic":"anthropic","cache_tokens":1,"input_tokens_total":9223372036854775807}`,
	} {
		if got := normalize(ContractRC26, math.MaxInt64, 0, raw); got.Complete {
			t.Fatal("overflow/invalid count accepted")
		}
	}
	if got := normalize(ContractRC26, -1, 0, `{"cache_tokens":0}`); got.Complete {
		t.Fatal("negative prompt accepted")
	}
}

func TestTokenSourceAndActualChargeUnchanged(t *testing.T) {
	prompt, output, quota := int64(100), int64(10), int64(132)
	source := SourceTokens{Contract: ContractRC26, Prompt: &prompt, Completion: &output, Other: `{"cache_tokens":20,"cache_creation_tokens":40,"admin_info":{"secret":"not-for-output"},"api_key":"not-for-output"}`}
	before := source
	got := NormalizeTokens(source)
	actualBefore := charge(t, quota)
	if !got.Complete || !reflect.DeepEqual(source, before) || prompt != 100 || output != 10 || quota != 132 {
		t.Fatal("normalization changed source facts")
	}
	if strings.Contains(fmt.Sprintf("%+v", got), "not-for-output") {
		t.Fatal("retained raw/private metadata")
	}
	fixed(t, actualBefore, "0.000264")
	if charge(t, quota).Exact() != actualBefore.Exact() {
		t.Fatal("actual charge recalculated from token interpretation")
	}
	for _, raw := range []string{`{}`, `{"cache_tokens":null}`, `{"cache_tokens":101}`, `{"cache_tokens":20,"claude":true}`} {
		source.Other = raw
		if NormalizeTokens(source).Complete {
			t.Fatal("invalid sample classified as complete")
		}
		if charge(t, quota).Exact() != actualBefore.Exact() {
			t.Fatal("incomplete token evidence discarded an actual charge")
		}
	}
}

func TestTokenConservationAndWriteDisjointness(t *testing.T) {
	for prompt := int64(0); prompt < 24; prompt++ {
		for read := int64(0); read <= prompt; read++ {
			write := (prompt - read) / 2
			for _, semantic := range []string{"openai", "anthropic"} {
				raw := fmt.Sprintf(`{"usage_semantic":%q,"cache_tokens":%d,"cache_creation_tokens":%d}`, semantic, read, write)
				got := normalize(ContractRC26, prompt, 5, raw)
				if !got.Complete || got.CacheRead.Value != read || got.CacheWrite.Value != write || got.Output.Value != 5 {
					t.Fatalf("lost a disjoint category: %+v", got)
				}
				if semantic == "openai" && got.OrdinaryInput.Value+read+write != prompt {
					t.Fatal("inclusive input not conserved")
				}
				if semantic == "anthropic" && got.OrdinaryInput.Value != prompt {
					t.Fatal("exclusive input subtracted twice")
				}
			}
		}
	}
}

func FuzzTokenMetadataFailsClosed(f *testing.F) {
	for _, seed := range []string{`{"cache_tokens":0}`, `{"cache_tokens":null}`, `{"cache_tokens":0,"cache_tokens":2}`, `{"usage_semantic":"anthropic","cache_tokens":10,"cache_creation_tokens":20}`, `{"cache_tokens":1e999999999}`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		got := normalize(ContractRC26, 100, 10, raw)
		if got.Complete {
			for _, count := range []TokenCount{got.OrdinaryInput, got.Output, got.CacheRead, got.CacheWrite, got.WriteUnsplit, got.Write5m, got.Write1h} {
				if count.State != CountValid || count.Value < 0 {
					t.Fatal("published incomplete/negative counts")
				}
			}
			if got.CacheWrite.Value-got.Write5m.Value-got.Write1h.Value != got.WriteUnsplit.Value {
				t.Fatal("duplicate write total")
			}
		}
	})
}
