package monthlysource

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func capturedPartitionFixture(t *testing.T, n int) []byte {
	t.Helper()
	planRaw, plan := tokenSegmentFixture(t, "2026-05", n)
	s := CapturedMonthSelection{Version: 1, Mode: CapturedMonthSelectionMode, Key: plan.Key, PlanSHA256: digest(planRaw), Scope: plan.Scope, Funding: []FundingMonthWindowVersion{}, WalletIDs: []string{digest([]byte("older wallet")), digest([]byte("newer wallet"))}, Tokens: []CapturedTokenSelection{}}
	for i := 0; i < 31; i++ {
		a, b := plan.Scans[i], plan.Scans[i+31]
		s.Funding = append(s.Funding, FundingMonthWindowVersion{From: a.From, To: a.To, OlderScanID: a.ID, NewerScanID: b.ID, OlderHeadID: digest([]byte(a.ID)), NewerHeadID: digest([]byte(b.ID))})
	}
	for _, id := range plan.Scope.Funding.UserIDs {
		token := CapturedTokenSelection{UserID: id, Parents: []string{}, Usage: []UsageWindowVersion{}}
		for day := 0; day < 31; day++ {
			identity := fmt.Sprintf("%d/%d", id, day)
			token.Parents = append(token.Parents, digest([]byte(identity+"/old token original")), digest([]byte(identity+"/new token original")))
			token.Usage = append(token.Usage, UsageWindowVersion{OlderID: digest([]byte(identity + "/old user projection")), NewerID: digest([]byte(identity + "/new user projection"))})
		}
		s.Tokens = append(s.Tokens, token)
	}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestCapturedUsagePartitionsLargeRosterRoundTripPreservesEveryOriginalID(t *testing.T) {
	for _, users := range []int{1, 10, 132, 133, 200} {
		t.Run(fmt.Sprint(users), func(t *testing.T) {
			raw := capturedPartitionFixture(t, users)
			before := append([]byte(nil), raw...)
			parts, err := PartitionCapturedUsageIndex(t.Context(), raw, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			reassembled, err := ReassembleCapturedUsageIndex(t.Context(), parts, time.Now())
			if err != nil || !bytes.Equal(reassembled, raw) || !bytes.Equal(before, raw) {
				t.Fatal("index round-trip changed original references", err)
			}
			var index capturedUsagePartIndex
			if err := json.Unmarshal(parts.Index, &index); err != nil {
				t.Fatal(err)
			}
			if users == 200 && len(index.Parts) != 2 {
				t.Fatal("6200 pairs were not partitioned", len(index.Parts))
			}
			count := 0
			for _, ref := range index.Parts {
				if ref.Pairs > 4096 || len(parts.Parts[ref.SHA256]) > 2<<20 {
					t.Fatal("part capacity bypass")
				}
				count += ref.Pairs
			}
			if count != users*31 {
				t.Fatal("lost/duplicated references", count)
			}
		})
	}
}

func TestCapturedUsagePartitionsRejectLostForeignOrModifiedParts(t *testing.T) {
	raw := capturedPartitionFixture(t, 200)
	for _, kind := range []string{"missing", "extra", "corrupt", "swapped", "scope", "pairs", "gap", "unknown_field", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			a, err := PartitionCapturedUsageIndex(t.Context(), raw, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			var index capturedUsagePartIndex
			if err := json.Unmarshal(a.Index, &index); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "missing":
				delete(a.Parts, index.Parts[0].SHA256)
			case "extra":
				a.Parts[digest([]byte("foreign"))] = []byte("foreign")
			case "corrupt":
				a.Parts[index.Parts[0].SHA256] = []byte(`{}`)
			case "swapped":
				index.Parts[0], index.Parts[1] = index.Parts[1], index.Parts[0]
			case "scope":
				index.Header.Scope.GroupID++
			case "pairs":
				index.Parts[0].Pairs--
			case "gap":
				index.Parts[1].FromMember++
			case "unknown_field":
				a.Index = append([]byte(`{"unexpected":true,`), a.Index[1:]...)
			case "oversize":
				a.Parts[index.Parts[0].SHA256] = make([]byte, (2<<20)+1)
			}
			if kind != "unknown_field" {
				a.Index, err = json.Marshal(index)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := ReassembleCapturedUsageIndex(t.Context(), a, time.Now()); err == nil {
				t.Fatal("bad partition index accepted")
			}
		})
	}
}

func TestCapturedUsagePartitionsStillRequireOriginalFinancialAudit(t *testing.T) {
	db, _, key, scope := capturedSelectionFixture(t, false)
	assembled, err := BuildCapturedMonthSelection(t.Context(), db, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	changes := adaptiveTokenCount(t, db, `SELECT total_changes()`)
	a, err := PartitionCapturedUsageIndex(t.Context(), assembled.Selection, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := ReassembleCapturedUsageIndex(t.Context(), a, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyCapturedObservationSelection(t.Context(), db, raw); err != nil {
		t.Fatal("unchanged original audit failed", err)
	}
	if err := VerifyCapturedObservationSelection(t.Context(), db, a.Index); err == nil {
		t.Fatal("old reader mistook partition manifest for approved source")
	}
	// A new digest does NOT make a false source true. Reindexing can preserve
	// arbitrary supplied bytes, but the unchanged DB audit must reject them.
	var forged CapturedMonthSelection
	if json.Unmarshal(raw, &forged) != nil {
		t.Fatal("bad fixture")
	}
	forged.WalletIDs[0] = digest([]byte("not an original wallet"))
	forgedRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	a, err = PartitionCapturedUsageIndex(t.Context(), forgedRaw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	raw, err = ReassembleCapturedUsageIndex(t.Context(), a, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyCapturedObservationSelection(t.Context(), db, raw); err == nil {
		t.Fatal("identity substituted for source approval")
	}
	if adaptiveTokenCount(t, db, `SELECT total_changes()`) != changes {
		t.Fatal("reindexing/audit wrote original DB")
	}
}

func TestCapturedUsagePartitionsCancellationAndDuplicateAccounts(t *testing.T) {
	raw := capturedPartitionFixture(t, 2)
	a, err := PartitionCapturedUsageIndex(t.Context(), raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := PartitionCapturedUsageIndex(ctx, raw, time.Now()); err == nil {
		t.Fatal("canceled producer accepted")
	}
	if _, err := ReassembleCapturedUsageIndex(ctx, a, time.Now()); err == nil {
		t.Fatal("canceled reader accepted")
	}
	if _, err := ReassembleCapturedUsageIndex(nil, a, time.Now()); err == nil { //nolint:staticcheck // SA1012: explicitly test nil-context rejection.
		t.Fatal("nil context accepted")
	}
	var s CapturedMonthSelection
	if json.Unmarshal(raw, &s) != nil {
		t.Fatal("bad fixture")
	}
	s.Tokens[1] = s.Tokens[0]
	raw, err = json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PartitionCapturedUsageIndex(t.Context(), raw, time.Now()); err == nil {
		t.Fatal("duplicate account accepted")
	}
}
