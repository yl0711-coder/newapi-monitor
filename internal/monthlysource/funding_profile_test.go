package monthlysource

import (
	"strings"
	"testing"
)

func TestFundingRC26ProfileEditIsNotMoney(t *testing.T) {
	rules := FundingRules{Producer: FundingProducerRC4RC26, QuotaPerUnit: 500000}
	for _, params := range []string{
		`{"id":102,"target_user_id":102,"username":"example"}`,
		`{"id":"102","target_user_id":"102","username":"example"}`,
		`{"id":5,"username":"self"}`,
	} {
		row := FundingLog{ID: 1, UserID: 5, CreatedAt: 100, Type: 3, Content: "Updated user", Other: `{"admin_info":{"admin_id":5},"op":{"action":"user.update","params":` + params + `}}`}
		got := NormalizeFundingLog(row, rules)
		if got.Status != FundingNonWallet || got.Reason != "profile_update_not_funding" || got.DeltaQuota != nil || got.TargetUserID <= 0 {
			t.Fatal("profile edit blocked generation or invented money", got)
		}
	}
}

func TestFundingProfileEditDoesNotHideAmbiguousOrMoneyFields(t *testing.T) {
	rules := FundingRules{Producer: FundingProducerRC4RC26, QuotaPerUnit: 500000}
	for _, params := range []string{
		`{"id":102,"target_user_id":102,"username":"example","quota":500}`,
		`{"id":102,"target_user_id":102,"username":"example","balance":0}`,
		`{"id":102,"target_user_id":102,"username":"example","role":100}`,
		`{"id":102,"target_user_id":103,"username":"example"}`,
		`{"id":102,"username":"example"}`,
		`{"id":102,"target_user_id":null,"username":"example"}`,
		`{"id":102,"target_user_id":102}`,
		`{"id":102,"target_user_id":102,"username":null}`,
		`{"id":102,"target_user_id":102,"username":""}`,
		`{"id":102,"id":103,"target_user_id":102,"username":"example"}`,
		`{"ID":102,"target_user_id":102,"username":"example"}`,
	} {
		row := FundingLog{ID: 1, UserID: 5, CreatedAt: 100, Type: 3, Content: "Updated user", Other: `{"admin_info":{"admin_id":5},"op":{"action":"user.update","params":` + params + `}}`}
		got := NormalizeFundingLog(row, rules)
		if got.Status != FundingReview || got.DeltaQuota != nil {
			t.Fatal("ambiguous event silently excluded", params, got)
		}
	}
	row := FundingLog{ID: 1, UserID: 5, CreatedAt: 100, Type: 3, Other: `{"op":{"action":"user.create","params":{"id":102,"target_user_id":102,"username":"example"}}}`}
	if got := NormalizeFundingLog(row, rules); got.Status != FundingReview {
		t.Fatal("unreviewed create treated as edit", got)
	}
	row.Other = strings.Replace(row.Other, "user.create", "user.update", 1)
	rules.Producer = "unknown-fork"
	if got := NormalizeFundingLog(row, rules); got.Status != FundingReview {
		t.Fatal("unknown producer accepted", got)
	}
}
