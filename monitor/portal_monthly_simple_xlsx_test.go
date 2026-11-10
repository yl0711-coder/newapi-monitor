package monitor

import (
	"strings"
	"testing"
)

func TestSimpleChineseAmount(t *testing.T) {
	for cents, want := range map[int64]string{
		1000000: "壹万元整", 2000000: "贰万元整", 1000: "壹拾元整", 100500: "壹仟零伍元整", 1000500: "壹万零伍元整",
		10000000: "壹拾万元整", 101000: "壹仟零壹拾元整", 12345: "壹佰贰拾叁元肆角伍分", 105: "壹元零伍分", 50: "伍角整",
		0: "零元整", 1000000000000: "壹佰亿元整", 100000100: "壹佰万零壹元整", 999999999: "玖佰玖拾玖万玖仟玖佰玖拾玖元玖角玖分",
	} {
		if got := simpleChineseAmount(cents); got != want {
			t.Errorf("%d: got %s want %s", cents, got, want)
		}
	}
}

// The token sheet follows the summary: subscription usage net of its own
// refunds, 退款 = balance refund only, so each column sums to the summary line.
func TestSimpleTokenSheetSubscriptionRefundMatchesSummary(t *testing.T) {
	p := monthlyStatement{Members: []monthlyMemberStatement{{UserID: 1, Username: "u", TokenConsumption: []monthlyTokenConsumption{
		{TokenID: 7, Name: "k", Amount: "100.000000", Refund: "10.000000", Net: "90.000000"}}}}}
	ix := newSimpleSubIndex(&simpleSubscriptionBill{Users: []simpleSubscriptionUser{{UserID: 1, ConsumeCents: 5000, RefundCents: 1000,
		Tokens: []simpleSubscriptionToken{{TokenID: 7, Name: "k", ConsumeCents: 5000, RefundCents: 1000}}}}})
	sheet := simpleTokenSheet(p, ix)
	for _, want := range []string{"<v>40.00</v>", "<v>100.00</v>", "<v>10.00</v>", "<v>130.00</v>"} {
		if !strings.Contains(sheet, want) {
			t.Fatalf("token sheet missing %s", want)
		}
	}
	if strings.Contains(sheet, "<v>50.00</v>") || strings.Contains(sheet, "<v>20.00</v>") {
		t.Fatal("token sheet still shows gross subscription usage or mixed refunds")
	}
}
