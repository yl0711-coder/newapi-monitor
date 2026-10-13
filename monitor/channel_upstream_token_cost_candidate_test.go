package monitor

import (
	"fmt"
	"reflect"
	"testing"
)

func tokenCostCandidates(t *testing.T, f tokenReconcileFixture, token string) []sub2TokenCostHourCandidate {
	t.Helper()
	rows, err := buildSub2TokenCostCandidates(f.account, token, f.day, f.day+86400, f.cursor, f.rows, f.states, f.controls, f.key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestSub2TokenCostCandidateConservesAllSourcesAndDoesNotWrite(t *testing.T) {
	f := newTokenReconcileFixture(t)
	beforeRows := append([]ChannelUpstreamPricingHourEvidence(nil), f.rows...)
	beforeStates := append([]ChannelUpstreamPricingHourState(nil), f.states...)
	beforeControls := append([]ChannelUpstreamUsageHour(nil), f.controls...)
	beforeCursor := f.cursor
	result := tokenCostCandidates(t, f, "575")
	if len(result) != 24 || !reflect.DeepEqual(result, tokenCostCandidates(t, f, "574")) || !reflect.DeepEqual(result, tokenCostCandidates(t, f, "575")) {
		t.Fatal("selected token filtered other sources or repeated preparation changed amounts")
	}
	sources := map[string]int64{}
	var total, requests int64
	for i, hour := range result {
		s := hour.State
		if s.HourTs != f.day+int64(i)*3600 || s.Status != "verified" || s.ReconcileStatus != "matched" ||
			s.VerifiedScans != 2 || s.ChargeUnitsPerUSD != "1000000" || s.ControlChargeUnits != s.EvidenceChargeUnits ||
			s.ReconcileDelta != 0 || s.ContentHash != channelCostEvidenceContentHash(hour.Rows) || s.EvidenceRows != int64(len(hour.Rows)) {
			t.Fatalf("invalid candidate control %+v", s)
		}
		for _, row := range hour.Rows {
			if row.ChargeUnit != sub2TokenChargeUnit || row.SourceRefKind != sub2TokenSourceKind || row.ChargeUnitsPerUSD != "1000000" {
				t.Fatal("Sub2 cost was relabeled as NewAPI quota")
			}
			amount, err := unitsToMicroUSDCanonical(row.ChargeUnits, row.ChargeUnitsPerUSD)
			if err != nil || amount != row.ChargeUnits {
				t.Fatal("conversion applied upstream group multiplier a second time", err)
			}
			sources[row.SourceRef] += amount
		}
		total += s.EvidenceChargeUnits
		requests += s.Requests
	}
	if total != 481479 || requests != 3 || len(sources) != 2 {
		t.Fatalf("account totals changed: %d / %d / %d", total, requests, len(sources))
	}
	source575, _ := channelCostSourceRef(f.key, f.account.Provider, newAPIUpstreamAccountEpoch(f.account), sub2TokenSourceKind, "575")
	if sources[source575] != 246912 {
		t.Fatal("same-group token costs merged")
	}
	if !reflect.DeepEqual(beforeRows, f.rows) || !reflect.DeepEqual(beforeStates, f.states) || !reflect.DeepEqual(beforeControls, f.controls) || beforeCursor != f.cursor {
		t.Fatal("candidate mutated source facts")
	}
	for _, model := range []any{&ChannelUpstreamCostHourEvidence{}, &ChannelUpstreamCostHourState{}, &ChannelCostSourceBinding{},
		&ChannelCostDirtyHour{}, &ChannelEconomicsDirtyHour{}, &ChannelEconomicsHourPublication{}, &ChannelFinanceVersion{}} {
		var count int64
		if err := f.m.storeDB.Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("candidate published to %T: %d %v", model, count, err)
		}
	}
}

func TestSub2TokenCostCandidateKeepsRechargeCorrectionSeparate(t *testing.T) {
	f := newTokenReconcileFixture(t)
	result := tokenCostCandidates(t, f, "575")
	for _, tc := range []struct {
		paid, credit float64
		want         int64
	}{
		{1, 1, 481479}, {1, 10, 48148}, {7, 1, 3370353}, {2.5, 3, 401233}, {1, 7.14, 67434},
	} {
		t.Run(fmt.Sprintf("%g:%g", tc.paid, tc.credit), func(t *testing.T) {
			var raw int64
			for _, h := range result {
				raw += h.State.EvidenceChargeUnits
			}
			got, err := correctedCostMicroUSD(raw, tc.paid, tc.credit)
			if err != nil || got != tc.want {
				t.Fatalf("correction got %d want %d: %v", got, tc.want, err)
			}
			if !reflect.DeepEqual(result, tokenCostCandidates(t, f, "575")) {
				t.Fatal("recharge policy changed raw evidence")
			}
		})
	}
}

func TestSub2TokenCostCandidateRoundingIsNotPublishedAsEquality(t *testing.T) {
	f := newTokenReconcileFixture(t)
	index := -1
	for i, s := range f.states {
		if s.EligibleRequests == 2 {
			index = i
			break
		}
	}
	if index < 0 {
		t.Fatal("missing fixture")
	}
	f.states[index].LegacyQuota--
	f.controls[index].CostUSD = float64(f.states[index].LegacyQuota) / 1e6
	result := tokenCostCandidates(t, f, "575")
	s := result[index].State
	if s.Status != "observed" || s.ReconcileStatus != "rounding_only" || s.ReconcileDelta != 1 || s.ReconcileTolerance != 2 ||
		s.ControlChargeUnits == s.EvidenceChargeUnits || len(result[index].Rows) != 2 {
		t.Fatalf("rounding hidden or fake balancing row added: %+v", s)
	}
}

func TestSub2TokenCostCandidateRejectsIncompleteAndWrongScope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*tokenReconcileFixture)
		token  string
	}{
		{"unseen-token", func(*tokenReconcileFixture) {}, "999"},
		{"blank-token", func(*tokenReconcileFixture) {}, ""},
		{"one-scan", func(f *tokenReconcileFixture) { f.cursor.StableScans = 1 }, "575"},
		{"revoked", func(f *tokenReconcileFixture) { f.cursor.SuccessRevision = 0 }, "575"},
		{"account-changed", func(f *tokenReconcileFixture) { f.account.Account = "different" }, "575"},
		{"wrong-cursor-domain", func(f *tokenReconcileFixture) { f.cursor.Domain = "other.example" }, "575"},
		{"wrong-cursor-day", func(f *tokenReconcileFixture) { f.cursor.DayTs += 86400 }, "575"},
		{"wrong-cursor-format", func(f *tokenReconcileFixture) { f.cursor.Format = "legacy" }, "575"},
		{"missing-completion", func(f *tokenReconcileFixture) { f.cursor.SuccessAt = 0 }, "575"},
		{"future-completion", func(f *tokenReconcileFixture) { f.cursor.SuccessAt++ }, "575"},
		{"stats-only", func(f *tokenReconcileFixture) { f.account.UsageAdapter = upstreamUsageAdapterSub2Stats }, "575"},
		{"missing-hour", func(f *tokenReconcileFixture) { f.controls = f.controls[1:] }, "575"},
		{"changed-price-evidence", func(f *tokenReconcileFixture) { f.rows[0].FinalQuota++ }, "575"},
		{"changed-control", func(f *tokenReconcileFixture) { f.controls[0].CostUSD++ }, "575"},
		{"changed-hmac-key", func(f *tokenReconcileFixture) { f.key = []byte("different-0123456789abcdef0123456789") }, "575"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTokenReconcileFixture(t)
			tc.mutate(&f)
			rows, err := buildSub2TokenCostCandidates(f.account, tc.token, f.day, f.day+86400, f.cursor, f.rows, f.states, f.controls, f.key, "v1")
			if err == nil || rows != nil {
				t.Fatalf("invalid evidence exposed partial candidate: %v", err)
			}
		})
	}
}
