package monthlysource

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func fundingTestRules() FundingRules {
	return FundingRules{Producer: FundingProducerRC4RC26, QuotaPerUnit: 500000}
}

func fundingTestRow(kind int, content, other string) FundingLog {
	return FundingLog{ID: 11, UserID: 100, CreatedAt: 1790784000, Type: kind, Content: content, Other: other}
}

func fundingTestAudit(action, params string) string {
	return `{"op":{"action":"` + action + `","params":` + params + `},"admin_info":{"admin_id":100}}`
}

func TestFundingKnownRC4RC26Producers(t *testing.T) {
	for _, tc := range []struct {
		name           string
		typeID         int
		content, other string
		target, delta  int64
		kind, method   string
	}{
		{"register", 4, "新用户注册赠送 ＄100.000000 额度", "", 100, 50000000, "gift", "注册赠送"},
		{"invitee", 4, "使用邀请码赠送 $1.000002 额度", "null", 100, 500001, "gift", "邀请赠送"},
		{"checkin", 4, "用户签到，获得额度 1 点额度", "{}", 100, 1, "gift", "签到赠送"},
		{"redeem", 1, "通过兑换码充值 ＄800.000000 额度，兑换码ID 12", "", 100, 400000000, "recharge", "兑换码充值"},
		{"online epay", 1, "使用在线充值成功，充值金额: ＄7.000000 额度，支付金额：1.000000", "", 100, 3500000, "recharge", "在线充值"},
		{"online stripe", 1, "使用在线充值成功，充值金额: ＄7.000000，支付金额：1", "", 100, 3500000, "recharge", "在线充值"},
		{"manual topup", 1, "管理员补单成功，充值金额: ＄9.000000，支付金额：2.000000", `{"admin_info":{"admin_id":1}}`, 100, 4500000, "recharge", "后台充值"},
		{"creem raw points", 1, "使用Creem充值成功，充值额度: 3500001，支付金额：1.00", "", 100, 3500001, "recharge", "在线充值"},
		{"waffo", 1, "Waffo充值成功，充值额度: ＄7.000000，支付金额: 1.00", "", 100, 3500000, "recharge", "在线充值"},
		{"waffo pancake", 1, "Waffo Pancake充值成功，充值额度: 3500001，支付金额: 1.00", "", 100, 3500001, "recharge", "在线充值"},
		{"legacy add beneficiary", 3, "管理员增加用户额度 ＄800.000000 额度", `{"admin_info":{"admin_id":1}}`, 100, 400000000, "recharge", "后台充值"},
		{"legacy subtract", 3, "管理员减少用户额度 ＄800.000000 额度", "", 100, -400000000, "adjustment", "余额扣减"},
		{"legacy override negative", 3, "管理员覆盖用户额度从 -1 点额度 为 1 点额度", "", 100, 2, "recharge", "后台充值"},
		{"legacy override decrease", 3, "管理员覆盖用户额度从 $800.000000 额度 为 $0.000000 额度", "", 100, -400000000, "adjustment", "余额扣减"},
		{"rc26 add beneficiary", 3, "Increased user quota by ＄800.000000 额度", fundingTestAudit("user.quota_add", `{"target_user_id":201,"quota":"＄800.000000 额度"}`), 201, 400000000, "recharge", "后台充值"},
		{"rc26 subtract target string", 3, "text is not amount authority", fundingTestAudit("user.quota_subtract", `{"target_user_id":"202","quota":"800 点额度"}`), 202, -800, "adjustment", "余额扣减"},
		{"rc26 override", 3, "Overrode user quota", fundingTestAudit("user.quota_override", `{"target_user_id":201,"from":"-1 点额度","to":"1 点额度"}`), 201, 2, "recharge", "后台充值"},
		{"rc26 verified self change", 3, "self", fundingTestAudit("user.quota_add", `{"quota":"1 点额度"}`), 100, 1, "recharge", "后台充值"},
		{"exact integer beyond float64", 3, "管理员增加用户额度 9007199254740993 点额度", "", 100, 9007199254740993, "recharge", "后台充值"},
		{"max point quota", 3, "管理员增加用户额度 9223372036854775807 点额度", "", 100, math.MaxInt64, "recharge", "后台充值"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := fundingTestRow(tc.typeID, tc.content, tc.other)
			got := NormalizeFundingLog(row, fundingTestRules())
			if got.Status != FundingMovement || got.DeltaQuota == nil || *got.DeltaQuota != tc.delta || got.TargetUserID != tc.target || got.Kind != tc.kind || got.Method != tc.method || got.Reason != "" {
				t.Fatalf("unexpected normalized result: %+v", got)
			}
			if !validDigest(got.SourceDigest) || got.LogID != row.ID || got.At != row.CreatedAt {
				t.Fatal("source binding lost")
			}
		})
	}
}

func TestFundingAmbiguitiesDoNotProduceMoneyOrActorFallback(t *testing.T) {
	for name, row := range map[string]FundingLog{
		"CNY no historic FX":                  fundingTestRow(4, "新用户注册赠送 ¥100.000000 额度", ""),
		"custom no historic FX":               fundingTestRow(4, "新用户注册赠送 €100.000000 额度", ""),
		"no unit":                             fundingTestRow(3, "管理员增加用户额度 100 额度", ""),
		"negative add":                        fundingTestRow(3, "管理员增加用户额度 $-1.000000 额度", ""),
		"negative subtract":                   fundingTestRow(3, "管理员减少用户额度 -1 点额度", ""),
		"zero add":                            fundingTestRow(3, "管理员增加用户额度 0 点额度", ""),
		"half point":                          fundingTestRow(4, "新用户注册赠送 $0.000001 额度", ""),
		"float exponent":                      fundingTestRow(4, "新用户注册赠送 $1e6 额度", ""),
		"too many decimals":                   fundingTestRow(4, "新用户注册赠送 $0.0000020 额度", ""),
		"mixed symbols":                       fundingTestRow(4, "新用户注册赠送 $＄1 额度", ""),
		"huge rendered amount":                fundingTestRow(4, "新用户注册赠送 $2251799813.685248 额度", ""),
		"fractional points":                   fundingTestRow(4, "新用户注册赠送 1.1 点额度", ""),
		"integer overflow":                    fundingTestRow(4, "新用户注册赠送 9223372036854775808 点额度", ""),
		"override overflow":                   fundingTestRow(3, "管理员覆盖用户额度从 -1 点额度 为 9223372036854775807 点额度", ""),
		"unknown type":                        fundingTestRow(8, "新用户注册赠送 1 点额度", ""),
		"money text wrong type":               fundingTestRow(4, "管理员增加用户额度 1 点额度", ""),
		"unknown recharge":                    fundingTestRow(1, "某新支付方式充值100", ""),
		"only payment amount":                 fundingTestRow(1, "使用在线充值成功，支付金额：100.000000", ""),
		"truncated payment":                   fundingTestRow(1, "使用在线充值成功，充值金额: $1.000000，支付金额：", ""),
		"creem wrong unit":                    fundingTestRow(1, "使用Creem充值成功，充值额度: $1.000000，支付金额：1.00", ""),
		"bad redeem id":                       fundingTestRow(1, "通过兑换码充值 $1.000000 额度，兑换码ID 0", ""),
		"subscription wallet debit":           fundingTestRow(1, "使用余额购买订阅成功，套餐: 月卡，支付金额: 100.00，扣除额度: 50000000", ""),
		"video refund is not funding":         fundingTestRow(4, "Video async task failed task-1, refund ＄1.000000 额度", ""),
		"video adjustment":                    fundingTestRow(4, "视频任务成功补扣费，补扣费 ＄1.000000 额度", ""),
		"broken metadata no content fallback": fundingTestRow(3, "管理员增加用户额度 1 点额度", `{`),
		"null op no content fallback":         fundingTestRow(3, "管理员增加用户额度 1 点额度", `{"op":null}`),
		"unknown action no content fallback":  fundingTestRow(3, "管理员增加用户额度 1 点额度", fundingTestAudit("user.quota_future", `{"quota":"1 点额度"}`)),
		"no target or actor":                  fundingTestRow(3, "", `{"op":{"action":"user.quota_add","params":{"quota":"1 点额度"}}}`),
		"wrong self actor":                    fundingTestRow(3, "", `{"op":{"action":"user.quota_add","params":{"quota":"1 点额度"}},"admin_info":{"admin_id":99}}`),
		"unknown fields could hide target":    fundingTestRow(3, "", fundingTestAudit("user.quota_add", `{"quota":"1 点额度","target_User_id":201}`)),
		"param amount numeric":                fundingTestRow(3, "", fundingTestAudit("user.quota_add", `{"quota":100,"target_user_id":201}`)),
		"duplicate target":                    fundingTestRow(3, "", fundingTestAudit("user.quota_add", `{"quota":"1 点额度","target_user_id":201,"target_user_id":202}`)),
		"escaped duplicate target":            fundingTestRow(3, "", fundingTestAudit("user.quota_add", `{"quota":"1 点额度","target_user_id":201,"target_user_\u0069d":202}`)),
		"duplicate op":                        fundingTestRow(3, "", `{"op":{},"op":{"action":"user.quota_add","params":{"quota":"1 点额度","target_user_id":201}}}`),
		"trailing JSON":                       fundingTestRow(3, "管理员增加用户额度 1 点额度", `{} {}`),
		"array metadata":                      fundingTestRow(3, "管理员增加用户额度 1 点额度", `[]`),
		"invalid utf8":                        fundingTestRow(3, "管理员增加用户额度 1 点额度", "{\"unknown\":\"\xff\"}"),
		"too deep":                            fundingTestRow(3, "管理员增加用户额度 1 点额度", `{"extra":`+strings.Repeat("[", 18)+"0"+strings.Repeat("]", 18)+"}"),
		"too much content":                    fundingTestRow(3, strings.Repeat("a", 4097), ""),
		"too much metadata":                   fundingTestRow(3, "管理员增加用户额度 1 点额度", strings.Repeat(" ", 65537)),
	} {
		t.Run(name, func(t *testing.T) {
			got := NormalizeFundingLog(row, fundingTestRules())
			if got.Status != FundingReview || got.Reason == "" || got.TargetUserID != 0 || got.DeltaQuota != nil || got.Kind != "" || got.Method != "" {
				t.Fatalf("ambiguous record became customer money: %+v", got)
			}
		})
	}
	for _, invalid := range []string{"0", "-1", "null", "true", "1.0", "1e2", `"1.0"`, `"01"`, `"+1"`, `" 201 "`, `9223372036854775808`} {
		t.Run("target "+invalid, func(t *testing.T) {
			row := fundingTestRow(3, "管理员增加用户额度 1 点额度", fundingTestAudit("user.quota_add", `{"quota":"1 点额度","target_user_id":`+invalid+`}`))
			got := NormalizeFundingLog(row, fundingTestRules())
			if got.Status != FundingReview || got.TargetUserID != 0 || got.DeltaQuota != nil {
				t.Fatal("invalid explicit target fell back to actor")
			}
		})
	}
}

func TestFundingNonWalletAndRefundAreNeverDoubleCounted(t *testing.T) {
	for _, tc := range []struct {
		typeID                         int
		content, other, status, reason string
	}{
		{4, "邀请用户赠送 ＄10.000000 额度", "", FundingSeparate, "affiliate_reward_not_wallet"},
		{1, "订阅购买成功，套餐: 月卡，支付金额: 100.00，支付方式: stripe", "", FundingSeparate, "external_subscription_not_wallet_topup"},
		{2, "消费", "", FundingSeparate, "consumption_or_refund_not_funding"},
		{6, "退款", "", FundingSeparate, "consumption_or_refund_not_funding"},
		{5, "error", "", FundingNonWallet, "error_or_login_not_funding"},
		{7, "login", "", FundingNonWallet, "error_or_login_not_funding"},
		{4, "成功启用两步验证", "", FundingNonWallet, "security_event_not_funding"},
		{3, "", fundingTestAudit("channel.key_view", `{"id":12}`), FundingNonWallet, "security_action_not_funding"},
	} {
		got := NormalizeFundingLog(fundingTestRow(tc.typeID, tc.content, tc.other), fundingTestRules())
		if got.Status != tc.status || got.Reason != tc.reason || got.DeltaQuota != nil || got.Kind != "" || got.Method != "" {
			t.Fatalf("non funding event incorrectly classified: %+v", got)
		}
	}
	for _, row := range []FundingLog{
		fundingTestRow(3, "管理员覆盖用户额度从 $1.000000 额度 为 $1.000000 额度", ""),
		fundingTestRow(3, "", fundingTestAudit("user.quota_override", `{"target_user_id":100,"from":"1 点额度","to":"1 点额度"}`)),
	} {
		got := NormalizeFundingLog(row, fundingTestRules())
		if got.Status != FundingNonWallet || got.Reason != "unchanged_quota_override" || got.TargetUserID != 100 || got.DeltaQuota == nil || *got.DeltaQuota != 0 || got.Kind != "" || got.Method != "" {
			t.Fatal("unchanged override became a recharge")
		}
	}
}

func TestFundingIndependentAccountsAndPrivateEvidence(t *testing.T) {
	rows := []FundingLog{
		fundingTestRow(3, "private raw text", fundingTestAudit("user.quota_subtract", `{"target_user_id":201,"quota":"$800.000000 额度"}`)),
		fundingTestRow(3, "private raw text", fundingTestAudit("user.quota_add", `{"target_user_id":202,"quota":"$800.000000 额度"}`)),
	}
	rows[1].ID++
	var results []FundingResult
	for _, row := range rows {
		got := NormalizeFundingLog(row, fundingTestRules())
		if got.Status != FundingMovement {
			t.Fatal("transfer side missing")
		}
		results = append(results, got)
	}
	if results[0].TargetUserID == results[1].TargetUserID || *results[0].DeltaQuota != -400000000 || *results[1].DeltaQuota != 400000000 || results[0].SourceDigest == results[1].SourceDigest {
		t.Fatal("accounts conflated, transfer inferred away, or source binding lost")
	}
	for i, row := range rows {
		if !reflect.DeepEqual(results[i], NormalizeFundingLog(row, fundingTestRules())) {
			t.Fatal("normalization is not deterministic")
		}
		row.Other = strings.ReplaceAll(row.Other, "800.000000", "801.000000")
		if NormalizeFundingLog(row, fundingTestRules()).SourceDigest == results[i].SourceDigest {
			t.Fatal("source correction undetectable")
		}
	}
	raw, err := json.Marshal(results)
	if err != nil || strings.Contains(string(raw), "private raw text") || strings.Contains(string(raw), "admin_info") || strings.Contains(string(raw), "complete") {
		t.Fatal("private data or false completeness assertion exported", err)
	}
}

func TestFundingRejectsUnknownRulesAndInvalidSource(t *testing.T) {
	row := fundingTestRow(3, "管理员增加用户额度 1 点额度", "")
	for _, rules := range []FundingRules{{}, {Producer: FundingProducerRC4RC26, QuotaPerUnit: 1000000}, {Producer: "future", QuotaPerUnit: 500000}} {
		got := NormalizeFundingLog(row, rules)
		if got.Status != FundingReview || got.DeltaQuota != nil {
			t.Fatal("unsupported rules accepted")
		}
	}
	for i := 0; i < 3; i++ {
		bad := row
		switch i {
		case 0:
			bad.ID = 0
		case 1:
			bad.UserID = -1
		case 2:
			bad.CreatedAt = 0
		}
		if got := NormalizeFundingLog(bad, fundingTestRules()); got.Status != FundingReview || got.DeltaQuota != nil || got.SourceDigest != "" {
			t.Fatal("invalid source accepted")
		}
	}
}

func TestFundingMalformedStructuredFieldsCannotUseLegacyFallback(t *testing.T) {
	for _, other := range []string{
		`{"OP":{"action":"user.quota_add","params":{"target_user_id":201,"quota":"1 点额度"}}}`,
		`{"op":{"Action":"user.quota_add","params":{"target_user_id":201,"quota":"1 点额度"}}}`,
		`{"op":{"action":null,"params":{}}}`,
		`{"op":{"action":1,"params":{}}}`,
		`{"op":{"action":"user.quota_add","params":null}}`,
		`{"op":{"action":"user.quota_add","params":{"quota":null,"target_user_id":201}}}`,
		`{"op":{"action":"user.quota_add","params":{"quota":"-1 点额度","target_user_id":201}}}`,
		`{"op":{"action":"user.quota_override","params":{"from":null,"to":"1 点额度","target_user_id":201}}}`,
		`{"op":{"action":"user.quota_override","params":{"from":"¥1.000000 额度","to":"¥2.000000 额度","target_user_id":201}}}`,
	} {
		got := NormalizeFundingLog(fundingTestRow(3, "管理员增加用户额度 1 点额度", other), fundingTestRules())
		if got.Status != FundingReview || got.TargetUserID != 0 || got.DeltaQuota != nil {
			t.Fatal("malformed structured fields fell back to legacy amount/target")
		}
	}
}

func TestFundingStructuredOverrideDecreasesAndExactLimit(t *testing.T) {
	for _, tc := range []struct {
		from, to string
		delta    int64
	}{
		{"$800.000000 额度", "$0.000000 额度", -400000000},
		{"0 点额度", "-9223372036854775808 点额度", math.MinInt64},
	} {
		params, err := json.Marshal(map[string]any{"target_user_id": 201, "from": tc.from, "to": tc.to})
		if err != nil {
			t.Fatal(err)
		}
		got := NormalizeFundingLog(fundingTestRow(3, "", fundingTestAudit("user.quota_override", string(params))), fundingTestRules())
		if got.Status != FundingMovement || got.TargetUserID != 201 || got.DeltaQuota == nil || *got.DeltaQuota != tc.delta || got.Kind != "adjustment" {
			t.Fatal("exact override delta lost")
		}
	}
}

func TestFundingExactDollarRoundTripAtProducerPrecision(t *testing.T) {
	for _, n := range []int64{1, 2, 499999, 500000, 500001, 399999999, 400000000, 1000000000000, 1<<49 - 1, 1<<50 - 1} {
		for _, sign := range []int64{-1, 1} {
			n := n * sign
			text := fmt.Sprintf("＄%.6f 额度", float64(n)/500000)
			got, ok := fundingAmount(text, false)
			if !ok || got != n {
				t.Fatalf("producer round trip changed integer %d -> %s -> %d (ok=%v)", n, text, got, ok)
			}
		}
	}
}

func FuzzFundingNormalization(f *testing.F) {
	f.Add(3, "管理员增加用户额度 1 点额度", "")
	f.Add(3, "", fundingTestAudit("user.quota_add", `{"target_user_id":201,"quota":"$800.000000 额度"}`))
	f.Add(4, "邀请用户赠送 $100.000000 额度", "")
	f.Add(1, "使用Creem充值成功，充值额度: 1，支付金额：0.01", "")
	f.Fuzz(func(t *testing.T, kind int, content, other string) {
		result := NormalizeFundingLog(fundingTestRow(kind, content, other), fundingTestRules())
		if result.Status == FundingMovement {
			if result.DeltaQuota == nil || *result.DeltaQuota == 0 || result.TargetUserID <= 0 || result.Reason != "" || !validDigest(result.SourceDigest) {
				t.Fatal("invalid money result")
			}
			if (*result.DeltaQuota < 0) != (result.Kind == "adjustment") {
				t.Fatal("direction mismatch")
			}
		} else if result.DeltaQuota != nil && (result.Status != FundingNonWallet || *result.DeltaQuota != 0) {
			t.Fatal("unsupported result leaked money")
		}
		if result.Status == FundingReview && result.TargetUserID != 0 {
			t.Fatal("unknown target guessed")
		}
	})
}
