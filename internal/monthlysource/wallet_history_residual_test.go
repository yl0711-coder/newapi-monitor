package monthlysource

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
)

func TestWalletHistoryResidualReviewIsSeparateAndPreservesSignedOrigin(t *testing.T) {
	f := walletHistoryDefaultFixture(t)
	base, err := DeriveWalletHistory(t.Context(), f.closing, f.months)
	if err != nil {
		t.Fatal(err)
	}
	closing := f.closing
	closing.accounts = slices.Clone(closing.accounts)
	for i := range closing.accounts {
		closing.accounts[i].balance++
		closing.accounts[i].used--
	}
	if _, err := DeriveWalletHistory(t.Context(), closing, f.months); err == nil {
		t.Fatal("ordinary source path was loosened")
	}
	h, err := ReviewWalletHistoryOriginResidual(t.Context(), closing, f.months)
	if err != nil {
		t.Fatal(err)
	}
	for i, a := range h.Report().Accounts {
		if a.OriginBalanceQuota != 1 || a.OriginUsedQuota != -1 || !a.OriginRequiresResolution {
			t.Fatal("residual was clamped or hidden", a.UserID)
		}
		old := base.Report().Accounts[i]
		for j, m := range a.Months {
			b := old.Months[j]
			if m.ConsumeQuota != b.ConsumeQuota || m.RefundQuota != b.RefundQuota || !reflect.DeepEqual(m.Movements, b.Movements) || !reflect.DeepEqual(m.Tokens, b.Tokens) || m.OpeningBalanceQuota != b.OpeningBalanceQuota+1 || m.OpeningUsedQuota != b.OpeningUsedQuota-1 {
				t.Fatal("source money was changed", a.UserID, m.Month)
			}
		}
	}
	if _, err := json.Marshal(h); err == nil {
		t.Fatal("diagnostic became JSON source proof")
	}
}

func TestWalletHistoryResidualReviewRejectsWrongDirectionAndOriginalNegativeCounter(t *testing.T) {
	f := walletHistoryDefaultFixture(t)
	for _, change := range []func(*WalletHistoryBoundary){
		func(c *WalletHistoryBoundary) { c.accounts[0].balance += 2; c.accounts[0].used-- },
		func(c *WalletHistoryBoundary) { c.accounts[0].balance--; c.accounts[0].used-- },
		func(c *WalletHistoryBoundary) { c.accounts[0].used = -1 },
	} {
		closing := f.closing
		closing.accounts = slices.Clone(closing.accounts)
		change(&closing)
		if h, err := ReviewWalletHistoryOriginResidual(t.Context(), closing, f.months); err == nil || h.report != nil {
			t.Fatal("invalid counter was excused")
		}
	}
}

func TestWalletHistoryResidualReviewStillRejectsPreRegistrationZeroPriceActivity(t *testing.T) {
	registered := map[int64]int64{201: historyFixtureAt("2026-09", 15)}
	f := newWalletHistorySource(t, []string{"2026-09"}, registered, map[string]map[int64]historyFixtureFlow{"2026-09": {201: {zeroRequest: true, at: historyFixtureAt("2026-09", 1)}}}, map[int64]int64{201: 1}, map[int64]int64{201: 0})
	if h, err := ReviewWalletHistoryOriginResidual(t.Context(), f.closing, f.months); err == nil || h.report != nil {
		t.Fatal("zero-price pre-registration activity ignored")
	}
}
