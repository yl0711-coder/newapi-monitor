package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func TestTokenProjectionAssemblySameMoneyAndOriginalProvenance(t *testing.T) {
	root, plan := observationAssemblyFixture(t)
	legacyRaw, _ := json.Marshal(plan)
	legacy, err := assembleObservations(context.Background(), root, "legacy-dry", plan, legacyRaw, false)
	if err != nil {
		t.Fatal(err)
	}
	for index, pair := range plan.Usage {
		var converted assemblyUsagePair
		for round, ref := range []assemblyCapture{pair.Older, pair.Newer} {
			raw, err := readAssemblyCapture(ref, "observe-usage")
			var usage monthlysource.UsageWindowObservation
			if err != nil || json.Unmarshal(raw, &usage) != nil {
				t.Fatal(err)
			}
			token := monthlysource.TokenUsageWindowObservation{Version: 1, Basis: "database_view_wallet_usage_and_token_groups_not_month_boundary",
				Request: usage.Request, StartedAtMS: usage.StartedAtMS, CompletedAtMS: usage.CompletedAtMS,
				Wallet: usage.Wallet, Totals: usage.Rows, Rows: []monthlysource.TokenUsageWindowRow{}}
			for _, row := range usage.Rows {
				token.Rows = append(token.Rows, monthlysource.TokenUsageWindowRow{UsageWindowRow: row, TokenID: 11, Name: "synthetic token"})
			}
			payload, _ := json.Marshal(token)
			request := observationRequest{Version: 1, Kind: "observe-token-usage", SourceIdentity: usage.Request.SourceIdentity,
				UserIDs: []int64{usage.Request.UserID}, From: usage.Request.From, To: usage.Request.To}
			capture := writeAssemblyFixtureCapture(t, root, fmt.Sprintf("token-projection-%d-%d", index, round), request, payload)
			if round == 0 {
				converted.Older = capture
			} else {
				converted.Newer = capture
			}
		}
		plan.TokenUsage = append(plan.TokenUsage, converted)
	}
	plan.Usage = nil
	raw, _ := json.Marshal(plan)
	got, err := assembleObservations(context.Background(), root, "projected", plan, raw, true)
	if err != nil || got.ReviewSHA256 != legacy.ReviewSHA256 || got.UsageCaptures != legacy.UsageCaptures {
		t.Fatal("projected user-view amounts or source observations changed", err)
	}
	selectionRaw, err := privateRead(filepath.Join(root, "projected", "selection.json"), 2<<20)
	var selected observationAssemblySelection
	if err != nil || json.Unmarshal(selectionRaw, &selected) != nil || len(selected.TokenProjections) != got.UsageCaptures {
		t.Fatal("parent source provenance missing", err)
	}
	parents := map[string]string{}
	for _, p := range selected.TokenProjections {
		if !validSHA(p.ParentSHA256) || !validSHA(p.UsageSHA256) || parents[p.ParentSHA256] != "" {
			t.Fatal("invalid or duplicated parent provenance")
		}
		parents[p.ParentSHA256] = p.UsageSHA256
	}
	for _, pair := range plan.TokenUsage {
		for _, ref := range []assemblyCapture{pair.Older, pair.Newer} {
			before, err := readAssemblyCapture(ref, "observe-token-usage")
			if err != nil || parents[hash(before)] == "" {
				t.Fatal("original missing or changed", err)
			}
		}
	}
	for _, scenario := range []string{"same_capture_twice", "foreign_manifest", "double_count_old_and_projected"} {
		t.Run(scenario, func(t *testing.T) {
			changed := plan
			changed.TokenUsage = append([]assemblyUsagePair{}, plan.TokenUsage...)
			switch scenario {
			case "same_capture_twice":
				changed.TokenUsage[0].Newer = changed.TokenUsage[0].Older
			case "foreign_manifest":
				changed.TokenUsage[0].Older.ManifestSHA256 = changed.TokenUsage[1].Older.ManifestSHA256
			case "double_count_old_and_projected":
				changed.Usage = []assemblyUsagePair{pairForProjectionUserView(t, root, changed.TokenUsage[0])}
			}
			raw, _ := json.Marshal(changed)
			if receipt, err := assembleObservations(context.Background(), root, "bad-"+scenario, changed, raw, false); err == nil || !reflect.DeepEqual(receipt, observationAssemblyReceipt{}) {
				t.Fatal("duplicate or unpinned source accepted")
			}
		})
	}
}

func pairForProjectionUserView(t *testing.T, root string, pair assemblyUsagePair) assemblyUsagePair {
	t.Helper()
	var out assemblyUsagePair
	for i, ref := range []assemblyCapture{pair.Older, pair.Newer} {
		raw, err := readAssemblyCapture(ref, "observe-token-usage")
		var token monthlysource.TokenUsageWindowObservation
		if err != nil || json.Unmarshal(raw, &token) != nil {
			t.Fatal(err)
		}
		usage, err := monthlysource.UsageObservationFromToken(token)
		if err != nil {
			t.Fatal(err)
		}
		payload, _ := json.Marshal(usage)
		r := usage.Request
		capture := writeAssemblyFixtureCapture(t, root, fmt.Sprintf("duplicate-projection-%d", i), observationRequest{Version: 1, Kind: "observe-usage",
			SourceIdentity: r.SourceIdentity, UserIDs: []int64{r.UserID}, From: r.From, To: r.To}, payload)
		if i == 0 {
			out.Older = capture
		} else {
			out.Newer = capture
		}
	}
	return out
}
