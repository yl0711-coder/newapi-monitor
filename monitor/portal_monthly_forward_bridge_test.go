package monitor

import (
	"bytes"
	"testing"
	"time"
)

func monthlyForwardBridgeFixture() (MonthlyPreparationInput, MonthlyBundleContext) {
	i, c := historicalBalanceFixture()
	a := &i.Accounts[0]
	a.Months, c.Groups[0].Months = []string{"2026-06"}, []string{"2026-06"}
	c.Accounts[0].Reconstructions = c.Accounts[0].Reconstructions[:1]
	r := &c.Accounts[0].Reconstructions[0]
	r.ObservedAt, r.BridgeDirection = preparationAt("2026-06-15"), "forward"
	r.SnapshotSource = "logs#quiet-original-observation"
	r.BalanceQuota, r.UsedQuota = preparationInt(4500001), preparationInt(999999)
	r.BridgeConsumeQuota, r.BridgeRefundQuota = preparationInt(2000000), preparationInt(0)
	r.FundingEventIDs = []int64{1, 3}
	// A real, separately retained funding event AFTER the quiet observation
	// must be carried forward, not mistaken for an event visible at capture.
	a.Movements = append(a.Movements, MonthlyPreparationMovement{UserID: a.UserID, ID: 3, At: preparationAt("2026-06-20"), DeltaQuota: 250000, Source: "logs#later-funding"})
	c.Accounts[0].Events = append(c.Accounts[0].Events, MonthlyBundleEvent{ID: 3, At: preparationAt("2026-06-20"), DeltaQuota: 250000, Kind: "recharge", Method: "后台充值", Source: "logs#later-funding"})
	a.Snapshots = []MonthlyPreparationBalance{{UserID: a.UserID, At: r.ObservedAt, BalanceQuota: *r.BalanceQuota, UsedQuota: *r.UsedQuota, Source: r.SnapshotSource}}
	return i, c
}

func TestMonthlyForwardBridgePreservesMoneyStatisticsAndOriginals(t *testing.T) {
	i, c := monthlyForwardBridgeFixture()
	before := monthlyReviewJSON(t, i)
	b := monthlyBundleRun(t, i, c)
	if b.ReadyForReview != 1 || b.Blocked != 0 {
		t.Fatal(b.Entries)
	}
	u := b.Entries[0].Candidate.Users[0]
	if u.OpeningQuota != 500000 || u.ConsumeQuota != 3000000 || u.RefundQuota != 0 || u.ClosingQuota != 2750001 || u.ReverseClosingQuota != u.ClosingQuota || u.Requests != 20 || u.Tokens != 200 || u.ExpectedCreditEvents != 2 || u.BalanceCorrection == nil || u.BalanceCorrection.Quota != 1 {
		t.Fatal("forward bridge altered original month or lost later funding", u)
	}
	if !bytes.Equal(before, monthlyReviewJSON(t, i)) {
		t.Fatal("rewrote source intervals to fit a snapshot")
	}
	now := time.Unix(preparationAt("2026-10-20"), 0)
	if _, err := PrepareAutomaticMonthlyBill(*b.Entries[0].Candidate, now.Unix(), now); err != nil {
		t.Fatal("forward recognition cannot use normal immutable publication policy", err)
	}
}

func TestMonthlyForwardBridgeCannotBecomeImplicitFallbackOrHideMissingData(t *testing.T) {
	for name, change := range map[string]func(*MonthlyPreparationInput, *MonthlyBundleContext){
		"omitted direction": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].BridgeDirection = ""
		},
		"unknown direction": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].BridgeDirection = "reverse"
		},
		"missing accepted decision": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].HistoricalBalance = nil
		},
		"wrong tail": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].BridgeConsumeQuota = preparationInt(2000001)
		},
		"tail greater than month": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].BridgeConsumeQuota = preparationInt(3000001)
		},
		"counter conflict": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].UsedQuota = preparationInt(1000000)
		},
		"missing late funding": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].FundingEventIDs = []int64{1}
		},
		"future funding included": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].FundingEventIDs = []int64{1, 2, 3}
		},
		"missing coverage":   func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) { i.Accounts[0].MovementCoverage = nil },
		"missing month logs": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) { i.Accounts[0].Spans = nil },
		"capture before month": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].ObservedAt = preparationAt("2026-05-31")
		},
		"capture at month end": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].ObservedAt = preparationAt("2026-07-01")
		},
	} {
		t.Run(name, func(t *testing.T) {
			i, c := monthlyForwardBridgeFixture()
			change(&i, &c)
			raw, meta := monthlyBundleRaw(t, i, c)
			b, err := PrepareMonthlyCandidateBundle(raw, meta, time.Unix(preparationAt("2026-10-20"), 0))
			if err == nil && b.ReadyForReview != 0 {
				t.Fatal("bad forward selection silently accepted", b)
			}
		})
	}
	i, c := monthlyForwardBridgeFixture()
	raw, meta := monthlyBundleRaw(t, i, c)
	for _, bad := range [][]byte{
		bytes.Replace(meta, []byte(`"bridge_direction":"forward"`), []byte(`"Bridge_Direction":"forward"`), 1),
		bytes.Replace(meta, []byte(`"bridge_direction":"forward"`), []byte(`"bridge_direction":null`), 1),
		bytes.Replace(meta, []byte(`"bridge_direction":"forward"`), []byte(`"bridge_direction":""`), 1),
	} {
		if _, err := PrepareMonthlyCandidateBundle(raw, bad, time.Unix(preparationAt("2026-10-20"), 0)); err == nil {
			t.Fatal("ambiguous bridge direction accepted")
		}
	}
}

func TestMonthlyForwardBridgePreservesRefundsAndNegativeWallets(t *testing.T) {
	i, c := monthlyForwardBridgeFixture()
	i.Accounts[0].Spans[0].RefundQuota = 250000
	c.Accounts[0].Reconstructions[0].MonthRefundQuota = preparationInt(250000)
	c.Accounts[0].Reconstructions[0].BridgeRefundQuota = preparationInt(250000)
	b := monthlyBundleRun(t, i, c)
	if b.ReadyForReview != 1 || b.Blocked != 0 {
		t.Fatal(b.Entries)
	}
	u := b.Entries[0].Candidate.Users[0]
	if u.ConsumeQuota != 3000000 || u.RefundQuota != 250000 || u.ClosingQuota != 3000001 {
		t.Fatal("refund netted into gross charge or applied twice", u)
	}
	i, c = monthlyForwardBridgeFixture()
	i.Accounts[0].Baseline.BalanceQuota -= 5500000
	i.Accounts[0].Snapshots[0].BalanceQuota -= 5500000
	c.Accounts[0].Reconstructions[0].BalanceQuota = preparationInt(-999999)
	b = monthlyBundleRun(t, i, c)
	if b.ReadyForReview != 1 || b.Blocked != 0 || b.Entries[0].Candidate.Users[0].ClosingQuota != -2749999 {
		t.Fatal("negative actual balance became zero or an invented credit", b.Entries)
	}
}
