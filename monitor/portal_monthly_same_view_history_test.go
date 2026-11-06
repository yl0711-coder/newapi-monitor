package monitor

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// The no-op verifier is a calculator test double only. Production's draft
// adapter independently verifies both original wallet captures and their logs.
func sameViewHistoricalFixture() (MonthlyPreparationInput, MonthlyBundleContext) {
	i, c := historicalBalanceFixture()
	for n := range c.Accounts[0].Reconstructions {
		r := &c.Accounts[0].Reconstructions[n]
		r.SameView = &MonthlySameViewSnapshot{Version: 1, Boundary: preparationAt("2026-08-01"), CompletedAtMS: r.ObservedAt*1000 + 37,
			OriginalSHA256: [2]string{strings.Repeat("a", 64), strings.Repeat("b", 64)}, Funding: []MonthlySameViewFunding{}, TailConsumeQuota: preparationInt(0), TailRefundQuota: preparationInt(0)}
		r.SnapshotSource = "logs#wallet-tail:" + r.SameView.OriginalSHA256[1]
	}
	return i, c
}

func TestMonthlySameViewHistoryRecognizesOnceAndCarriesWithoutChangingLogs(t *testing.T) {
	for _, recognition := range []string{"2026-06", "2026-07"} {
		t.Run(recognition, func(t *testing.T) {
			i, c := sameViewHistoricalFixture()
			// Each reconstruction owns its own copy of the business treatment.
			for n := range c.Accounts[0].Reconstructions {
				h := *c.Accounts[0].Reconstructions[n].HistoricalBalance
				h.RecognitionMonth = recognition
				c.Accounts[0].Reconstructions[n].HistoricalBalance = &h
			}
			raw, meta := monthlyBundleRaw(t, i, c)
			beforeRaw, beforeMeta := bytes.Clone(raw), bytes.Clone(meta)
			now := time.Unix(preparationAt("2026-10-20"), 0)
			b, err := PrepareMonthlyCandidateBundleWithSameView(raw, meta, now, func([]byte, []byte) error { return nil })
			if err != nil || b.ReadyForReview != 2 || b.Blocked != 0 {
				t.Fatal("same-view reconstruction blocked", err, b.Entries)
			}
			first, next := b.Entries[0].Candidate.Users[0], b.Entries[1].Candidate.Users[0]
			if first.ConsumeQuota != 3000000 || first.RefundQuota != 0 || next.ConsumeQuota != 4000000 || next.RefundQuota != 250000 || first.ClosingQuota != next.OpeningQuota || next.ClosingQuota != -1749999 {
				t.Fatal("logs or month continuity changed", first, next)
			}
			if recognition == "2026-06" {
				if first.ClosingQuota != 2500001 || first.BalanceCorrection == nil || first.BalanceCorrection.Quota != 1 || next.BalanceCorrection != nil || first.HistoricalBalance == nil || next.HistoricalBalance == nil {
					t.Fatal("recognition repeated or not carried")
				}
			} else if first.ClosingQuota != 2500000 || first.BalanceCorrection != nil || first.HistoricalBalance != nil || next.BalanceCorrection == nil || next.BalanceCorrection.Quota != 1 || next.HistoricalBalance == nil {
				t.Fatal("future business credit leaked into earlier month")
			}
			for _, entry := range b.Entries {
				if _, err := PrepareAutomaticMonthlyBill(*entry.Candidate, now.Unix(), now); err != nil {
					t.Fatal("final money gate failed", err)
				}
			}
			if !bytes.Equal(raw, beforeRaw) || !bytes.Equal(meta, beforeMeta) {
				t.Fatal("changed raw money observations or treatment input")
			}
			if _, err := PrepareMonthlyCandidateBundle(raw, meta, now); err == nil {
				t.Fatal("unverified JSON admitted same-view treatment")
			}
		})
	}
}

func TestMonthlySameViewHistoryRejectsIncompleteOrChangedTail(t *testing.T) {
	for _, scenario := range []string{"missing_consume", "missing_refund", "negative", "changed_consume", "changed_refund", "without_treatment"} {
		t.Run(scenario, func(t *testing.T) {
			i, c := sameViewHistoricalFixture()
			r := &c.Accounts[0].Reconstructions[0]
			switch scenario {
			case "missing_consume":
				r.SameView.TailConsumeQuota = nil
			case "missing_refund":
				r.SameView.TailRefundQuota = nil
			case "negative":
				r.SameView.TailConsumeQuota = preparationInt(-1)
			case "changed_consume":
				r.SameView.TailConsumeQuota = preparationInt(1)
			case "changed_refund":
				r.SameView.TailRefundQuota = preparationInt(1)
			case "without_treatment":
				r.HistoricalBalance = nil
			}
			raw, meta := monthlyBundleRaw(t, i, c)
			b, err := PrepareMonthlyCandidateBundleWithSameView(raw, meta, time.Unix(preparationAt("2026-10-20"), 0), func([]byte, []byte) error { return nil })
			if err == nil && b.Entries[0].Candidate != nil {
				t.Fatal("changed or incomplete original tail admitted", scenario)
			}
		})
	}
}
