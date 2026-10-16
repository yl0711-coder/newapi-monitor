package monitor

import (
	"reflect"
	"testing"
)

func TestPortalBillMoneyLayout(t *testing.T) {
	want := []string{"官方原价（美元）", "折扣前价格（元）", "折后价格（元）", "折扣比例"}
	if !reflect.DeepEqual(portalBillHeader()[11:], want) {
		t.Fatal("money header order changed")
	}
	for _, tc := range []struct {
		name, metadata string
		quota          int64
		money          []string
	}{
		{"decimal ratio", `{"model_ratio":1,"completion_ratio":1,"request_path":"/v1/responses","model_price":1,"group_ratio":0.602}`, 301000, []string{"$1.000000", "¥7.000000", "¥0.602000", "0.086"}},
		{"round ratio up only", `{"model_ratio":1,"completion_ratio":1,"request_path":"/v1/responses","model_price":1,"group_ratio":0.6}`, 300000, []string{"$1.000000", "¥7.000000", "¥0.600000", "0.086"}},
		{"round ratio down only", `{"model_ratio":1,"completion_ratio":1,"request_path":"/v1/responses","model_price":1,"group_ratio":0.5978}`, 298900, []string{"$1.000000", "¥7.000000", "¥0.597800", "0.085"}},
		{"one fold", `{"model_ratio":1,"completion_ratio":1,"request_path":"/v1/responses","model_price":1,"group_ratio":0.7}`, 350000, []string{"$1.000000", "¥7.000000", "¥0.700000", "0.1"}},
		{"full price", `{"model_ratio":1,"completion_ratio":1,"request_path":"/v1/responses","model_price":1,"group_ratio":7}`, 3500000, []string{"$1.000000", "¥7.000000", "¥7.000000", "1"}},
		{"above reference", `{"model_ratio":1,"completion_ratio":1,"request_path":"/v1/responses","model_price":1,"group_ratio":14}`, 7000000, []string{"$1.000000", "¥7.000000", "¥14.000000", "2"}},
		{"free", `{"model_ratio":1,"completion_ratio":1,"request_path":"/v1/responses","model_price":1,"group_ratio":0}`, 0, []string{"$1.000000", "¥7.000000", "¥0.000000", "0"}},
		{"zero reference", `{"model_ratio":0,"completion_ratio":1,"request_path":"/v1/responses","model_price":0,"group_ratio":1}`, 0, []string{"$0.000000", "¥0.000000", "¥0.000000", ""}},
		{"missing evidence", `{}`, 123, []string{"", "", "¥0.000246", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := LogRow{Type: 2, PromptTokens: 1, billingFactsValid: true, billingQuota: tc.quota}
			populatePortalLogEvidence(&r, tc.metadata)
			row, err := portalBillRecord(r)
			if err != nil || len(row) != 15 || !reflect.DeepEqual(row[11:], tc.money) {
				t.Fatalf("money: %v err=%v", row, err)
			}
		})
	}
}
