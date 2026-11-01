package monitor

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"
)

func monthlyForwardReconstructionFixture() (MonthlyPreparationInput, MonthlyBundleContext) {
	i := preparationFixture()
	a := &i.Accounts[0]
	start, end := preparationAt("2026-06-01"), preparationAt("2026-07-01")
	at := preparationAt("2026-06-15") + 37
	a.Months = []string{"2026-06"}
	a.Spans = []MonthlyPreparationSpan{
		{UserID: a.UserID, From: start, To: at, ConsumeQuota: 1000000, RefundQuota: 100000, Requests: preparationInt(10), Tokens: preparationInt(100), Source: "logs#original-prefix"},
		{UserID: a.UserID, From: at, To: end, ConsumeQuota: 2000000, RefundQuota: 200000, Requests: preparationInt(20), Tokens: preparationInt(200), Source: "logs#original-exact-tail"},
	}
	// Funding exactly at the observation belongs to the tail, not the snapshot.
	a.Movements = append(a.Movements, MonthlyPreparationMovement{UserID: a.UserID, ID: 3, At: at, DeltaQuota: -500000, Source: "logs#tail-debit"})
	a.Snapshots = []MonthlyPreparationBalance{{UserID: a.UserID, At: at, BalanceQuota: 4600000, UsedQuota: 900000, Source: "logs#original-mid-month", TimeBasis: monthlyCollectionObservation}}
	c := monthlyBundleFixture(i)
	c.Accounts[0].Reconstructions = []MonthlySnapshotReconstruction{{
		UserID: a.UserID, Month: "2026-06", ObservedAt: at, BridgeDirection: "forward",
		SnapshotSource: a.Snapshots[0].Source, ScopeSource: "logs#reviewed-independent-scope", MonthSource: "logs#original-full-month", BridgeSource: "logs#original-exact-tail", FundingSource: "logs#all-funding", OpeningBasisSource: "logs#registration-baseline",
		BalanceQuota: preparationInt(4600000), UsedQuota: preparationInt(900000),
		MonthConsumeQuota: preparationInt(3000000), MonthRefundQuota: preparationInt(300000), BridgeConsumeQuota: preparationInt(2000000), BridgeRefundQuota: preparationInt(200000), FundingEventIDs: []int64{1, 3},
	}}
	return i, c
}

func TestMonthlyForwardReconstructionWithoutDifference(t *testing.T) {
	i, c := monthlyForwardReconstructionFixture()
	before, _ := json.Marshal(i)
	b := monthlyBundleRun(t, i, c)
	if b.ReadyForReview != 1 || b.Blocked != 0 {
		t.Fatalf("exact independent forward reconstruction blocked: %+v", b.Entries)
	}
	u := b.Entries[0].Candidate.Users[0]
	if u.OpeningQuota != 500000 || u.ClosingQuota != 2300000 || u.ReverseClosingQuota != 2300000 || u.ConsumeQuota != 3000000 || u.RefundQuota != 300000 || u.Requests != 30 || u.Tokens != 300 || u.HistoricalBalance != nil || u.BalanceCorrection != nil || len(u.Events) != 2 {
		t.Fatalf("money, gross/refunds, tail funding or statistics changed: %+v", u)
	}
	after, _ := json.Marshal(i)
	if string(before) != string(after) {
		t.Fatal("original input changed")
	}
	if _, err := PrepareAutomaticMonthlyBill(*b.Entries[0].Candidate, preparationAt("2026-10-20"), time.Unix(preparationAt("2026-10-20"), 0)); err != nil {
		t.Fatal("candidate fails existing automatic financial policy", err)
	}
}

func TestMonthlyForwardReconstructionCannotInventTailOrSnapshot(t *testing.T) {
	for name, mutate := range map[string]func(*MonthlyPreparationInput, *MonthlyBundleContext){
		"unretained snapshot": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) { i.Accounts[0].Snapshots = nil },
		"snapshot different source": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) {
			i.Accounts[0].Snapshots[0].Source = "logs#different"
		},
		"snapshot different balance": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) { i.Accounts[0].Snapshots[0].BalanceQuota++ },
		"snapshot different counter": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) { i.Accounts[0].Snapshots[0].UsedQuota++ },
		"invented context balance": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			*c.Accounts[0].Reconstructions[0].BalanceQuota++
		},
		"invented context counter": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			*c.Accounts[0].Reconstructions[0].UsedQuota++
		},
		"net equal gross and refund tampered": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			r := &c.Accounts[0].Reconstructions[0]
			*r.BridgeConsumeQuota++
			*r.BridgeRefundQuota++
		},
		"no exact partial boundary": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) {
			a := &i.Accounts[0]
			a.Spans = []MonthlyPreparationSpan{{UserID: a.UserID, From: a.Spans[0].From, To: a.Spans[1].To, ConsumeQuota: 3000000, RefundQuota: 300000, Requests: preparationInt(30), Tokens: preparationInt(300), Source: "logs#full-month-only"}}
		},
		"funding coverage ends before tail": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) {
			i.Accounts[0].MovementCoverage[0].To = i.Accounts[0].Snapshots[0].At
		},
		"missing tail funding": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].FundingEventIDs = []int64{1}
		},
		"after month": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].ObservedAt = preparationAt("2026-07-01")
		},
	} {
		t.Run(name, func(t *testing.T) {
			i, c := monthlyForwardReconstructionFixture()
			mutate(&i, &c)
			raw, meta := monthlyBundleRaw(t, i, c)
			b, err := PrepareMonthlyCandidateBundle(raw, meta, time.Unix(preparationAt("2026-10-20"), 0))
			if err == nil && (b.ReadyForReview != 0 || (len(b.Entries) > 0 && b.Entries[0].Candidate != nil)) {
				t.Fatal("invalid forward source accepted")
			}
		})
	}
}

func TestMonthlyForwardReconstructionExactMoneySimulation(t *testing.T) {
	rng := rand.New(rand.NewSource(20261027))
	for sample := 0; sample < 256; sample++ {
		i, c := monthlyForwardReconstructionFixture()
		a, r := &i.Accounts[0], &c.Accounts[0].Reconstructions[0]
		opening := rng.Int63n(10000000000) - 2000000000
		prefixGross, tailGross := rng.Int63n(10000000000), rng.Int63n(10000000000)
		prefixRefund, tailRefund := rng.Int63n(prefixGross+1), rng.Int63n(tailGross+1)
		credit, debit := rng.Int63n(10000000000)+1, -rng.Int63n(10000000000)-1
		a.Baseline.BalanceQuota = opening
		a.Spans[0].ConsumeQuota, a.Spans[0].RefundQuota = prefixGross, prefixRefund
		a.Spans[1].ConsumeQuota, a.Spans[1].RefundQuota = tailGross, tailRefund
		a.Movements[0].DeltaQuota, a.Movements[2].DeltaQuota = credit, debit
		c.Accounts[0].Events[0].DeltaQuota, c.Accounts[0].Events[2].DeltaQuota = credit, debit
		observedBalance := opening + credit - prefixGross + prefixRefund
		observedUsed := prefixGross - prefixRefund
		a.Snapshots[0].BalanceQuota, a.Snapshots[0].UsedQuota = observedBalance, observedUsed
		r.BalanceQuota, r.UsedQuota = preparationInt(observedBalance), preparationInt(observedUsed)
		r.MonthConsumeQuota, r.MonthRefundQuota = preparationInt(prefixGross+tailGross), preparationInt(prefixRefund+tailRefund)
		r.BridgeConsumeQuota, r.BridgeRefundQuota = preparationInt(tailGross), preparationInt(tailRefund)
		b := monthlyBundleRun(t, i, c)
		if b.ReadyForReview != 1 || b.Blocked != 0 {
			t.Fatalf("sample %d blocked: %+v", sample, b.Entries)
		}
		u := b.Entries[0].Candidate.Users[0]
		want := opening + credit + debit - prefixGross - tailGross + prefixRefund + tailRefund
		if u.OpeningQuota != opening || u.ClosingQuota != want || u.ReverseClosingQuota != want || u.BalanceCorrection != nil || u.HistoricalBalance != nil {
			t.Fatalf("sample %d changed exact integer money: %+v", sample, u)
		}
	}
}
