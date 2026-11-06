package monthlysource

import (
	"context"
	"strings"
	"testing"
)

func TestFundingLegacyBindingClearExactReviewedProducer(t *testing.T) {
	for _, binding := range []string{"email", "github", "discord", "oidc", "wechat", "telegram", "linuxdo"} {
		for _, other := range []string{"", "{}", "null"} {
			row := fundingTestRow(3, "admin cleared "+binding+" binding for user 合成用户-test", other)
			got := NormalizeFundingLog(row, fundingTestRules())
			if got.Status != FundingNonWallet || got.Reason != "legacy_binding_clear_not_funding" || got.DeltaQuota != nil || got.TargetUserID != 0 || got.Kind != "" || got.Method != "" || !validDigest(got.SourceDigest) {
				t.Fatal("reviewed binding event became money or lost its original identity", got)
			}
		}
	}
	content := "admin cleared email binding for user synthetic"
	for _, row := range []FundingLog{
		fundingTestRow(3, strings.Replace(content, "email", "quota", 1), ""),
		fundingTestRow(3, content+strings.Repeat("x", 21), ""),
		fundingTestRow(3, "prefix "+content, ""),
		fundingTestRow(3, content+"\n", ""),
		fundingTestRow(3, "admin cleared email binding for user ", ""),
		fundingTestRow(3, content, `{"quota":"1 点额度"}`),
		fundingTestRow(3, content, `{"op":null}`),
		fundingTestRow(3, content, fundingTestAudit("user.quota_future", `{"quota":"1 点额度"}`)),
		fundingTestRow(1, content, ""),
		fundingTestRow(4, content, ""),
	} {
		if got := NormalizeFundingLog(row, fundingTestRules()); got.Status != FundingReview || got.DeltaQuota != nil {
			t.Fatal("unreviewed/ambiguous action silently exempted", got)
		}
	}
	// A recognized RC26 quota action remains authoritative even if its display
	// text resembles an old security message; do not hide a real credit/debit.
	row := fundingTestRow(3, content, fundingTestAudit("user.quota_add", `{"target_user_id":201,"quota":"1 点额度"}`))
	got := NormalizeFundingLog(row, fundingTestRules())
	if got.Status != FundingMovement || got.TargetUserID != 201 || got.DeltaQuota == nil || *got.DeltaQuota != 1 {
		t.Fatal("legacy text suppressed structured monetary evidence", got)
	}
}

func TestFundingLegacyBindingClearPreservesIndependentMonthMoney(t *testing.T) {
	security := fundingTestRow(3, "admin cleared email binding for user synthetic", "")
	security.ID, security.UserID, security.CreatedAt = 2, 999, 0
	db, request, windows := fundingMoneyMonth(t, []FundingLog{fundingMonthRow(1, 201, "3 点额度"), security})
	defer db.Close()
	got, err := ReviewFundingMoney(context.Background(), request, windows)
	if err != nil || len(got.Accounts) != 2 || got.Accounts[0].Status != "observed_month" || got.Accounts[1].Status != "observed_month" || got.Accounts[0].Totals == nil || got.Accounts[1].Totals == nil || got.Accounts[0].Totals.IncreaseQuota != 3 || got.Accounts[1].Totals.IncreaseQuota != 0 || len(got.Accounts[0].Movements) != 1 || len(got.Accounts[1].Movements) != 0 {
		t.Fatal("unrelated security action blocked accounts or changed money", err, got.Accounts)
	}
	// Unknown binding-related actions are still globally unresolved, never
	// silently treated as zero or assigned to the operator/current organization.
	security.Content = "admin cleared quota binding for user synthetic"
	from, to := got.Windows.From, got.Windows.To
	base := (to + 86400) * 1000
	windows[0] = fundingMonthObservation(t, db, request, from, from+3600, []FundingLog{security}, []FundingLog{security}, base, base+300000, 1000, 1000)
	got, err = ReviewFundingMoney(context.Background(), request, windows)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range got.Accounts {
		if a.Status != "incomplete_observation" || a.Totals != nil || len(a.Movements) != 0 {
			t.Fatal("unknown action was accepted as zero money", a)
		}
	}
}
