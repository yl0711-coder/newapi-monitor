package monitor

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io"
	"math/big"
	"math/rand"
	"os"
	"strings"
	"testing"
)

// Independent oracle: decimal rational input, integer cents, half away from
// zero. Does not call the production display helpers or use binary floats.
func monthlyAuditCents(value string) string {
	r, ok := new(big.Rat).SetString(value)
	if !ok {
		panic("invalid acceptance decimal")
	}
	negative := r.Sign() < 0
	r.Abs(r).Mul(r, big.NewRat(100, 1)).Add(r, big.NewRat(1, 2))
	n := new(big.Int).Quo(r.Num(), r.Denom())
	digits := n.String()
	if len(digits) < 3 {
		digits = strings.Repeat("0", 3-len(digits)) + digits
	}
	if negative && n.Sign() != 0 {
		digits = "-" + digits
	}
	return digits[:len(digits)-2] + "." + digits[len(digits)-2:]
}

func monthlyAuditWalletClosing(m monthlyMemberStatement) string {
	sum := new(big.Rat)
	correction := m.BalanceCorrection
	if correction == "" {
		correction = "0.000000"
	}
	for i, value := range []string{m.Opening, m.CreditTotal, m.Adjustment, m.Consumption.Amount, m.Consumption.Refund, correction} {
		n, ok := new(big.Rat).SetString(monthlyAuditCents(value))
		if !ok {
			panic("invalid independent wallet operand")
		}
		if i == 3 {
			sum.Sub(sum, n)
		} else {
			sum.Add(sum, n)
		}
	}
	return sum.FloatString(2)
}

func TestMonthlyDisplayRoundingSharedCases(t *testing.T) {
	raw, err := os.ReadFile("testdata/monthly_display_rounding.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name, Total string
		Terms       [][]json.RawMessage
		Residual    *string
	}
	if err = json.Unmarshal(raw, &cases); err != nil || len(cases) < 10 {
		t.Fatal("missing shared coverage", err)
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			var terms []monthlyDisplayTerm
			for _, entry := range tc.Terms {
				var amount string
				var sign int64
				_ = json.Unmarshal(entry[0], &amount)
				_ = json.Unmarshal(entry[1], &sign)
				terms = append(terms, monthlyDisplayTerm{amount, sign})
			}
			want := ""
			if tc.Residual != nil {
				want = *tc.Residual
			}
			if got := monthlyDisplayResidual(tc.Total, terms); got != want {
				t.Fatal("display residual", got, want)
			}
		})
	}
}

func TestMonthlyDisplayRoundingIndependentWalletProperties(t *testing.T) {
	r := rand.New(rand.NewSource(20261027))
	for range 2000 {
		q := []int64{r.Int63n(1000000000) - 500000000, r.Int63n(1000000000), -r.Int63n(10000000), r.Int63n(1000000000), r.Int63n(10000), r.Int63n(10000)}
		closing := q[0] + q[1] + q[2] - q[3] + q[4] + q[5]
		values := make([]string, len(q))
		for i, v := range q {
			values[i] = monthlyAuditQuota(v)
		}
		consumption := monthlyConsumption{Amount: values[3], Refund: values[4]}
		tail := monthlyWalletDisplayResidual(values[0], values[1], values[2], consumption, values[5], monthlyAuditQuota(closing))
		if tail == "" {
			t.Fatal("valid wallet equation refused")
		}
		sum := new(big.Rat)
		for i, sign := range []int64{1, 1, 1, -1, 1, 1} {
			v, _ := new(big.Rat).SetString(monthlyAuditCents(values[i]))
			sum.Add(sum, v.Mul(v, big.NewRat(sign, 1)))
		}
		adjustment, _ := new(big.Rat).SetString(tail)
		sum.Add(sum, adjustment)
		want, _ := new(big.Rat).SetString(monthlyAuditCents(monthlyAuditQuota(closing)))
		if sum.Cmp(want) != 0 {
			t.Fatal("independent rounded wallet equation failed", values, tail)
		}
		derived := monthlyWalletDisplayClosing(values[0], values[1], values[2], consumption, values[5], monthlyAuditQuota(closing))
		display, ok := new(big.Rat).SetString(derived)
		if !ok || display.Cmp(new(big.Rat).Sub(sum, adjustment)) != 0 {
			t.Fatal("derived balance does not equal rounded operands", values, derived)
		}
		if got := monthlyWalletDisplayClosing(values[0], values[1], values[2], consumption, values[5], monthlyAuditQuota(closing+1)); got != "" {
			t.Fatal("real source discrepancy was concealed by derived balance", got)
		}
	}
}

func monthlyAuditWorkbookParts(t *testing.T, raw []byte) map[string]string {
	t.Helper()
	z, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	parts := map[string]string{}
	for _, f := range z.File {
		r, e := f.Open()
		if e != nil {
			t.Fatal(e)
		}
		b, e := io.ReadAll(r)
		r.Close()
		if e != nil {
			t.Fatal(e)
		}
		parts[f.Name] = string(b)
	}
	return parts
}

func TestMonthlyDisplayRoundingWorkbookKeepsOriginalPayloadAndCentsEquations(t *testing.T) {
	e, now := monthlyTokenFixture()
	e.Users = e.Users[:1]
	u := &e.Users[0]
	u.OpeningQuota = 1160872085
	u.ConsumeQuota = 1513097892
	u.RefundQuota = 0
	u.ClosingQuota = 2147774193
	u.ReverseClosingQuota = u.ClosingQuota
	u.Events = []MonthlyBillCreditEvidence{{ID: 1, At: 1788192001, DeltaQuota: 2500000000, Kind: "recharge", Method: "后台充值"}}
	u.ExpectedCreditEvents = 1
	u.Accounting = monthlyAccountingFixture(u.ConsumeQuota, 0)
	u.TokenUsage.Rows = []MonthlyBillTokenEvidence{{TokenID: 11, Name: "token", ConsumeQuota: u.ConsumeQuota}}
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(p)
	book, err := monthlyStatementXLSX(p)
	if err != nil {
		t.Fatal(err)
	}
	parts := monthlyAuditWorkbookParts(t, book)
	if strings.Contains(parts["xl/worksheets/sheet1.xml"], "四舍五入尾差（元）") || !strings.Contains(parts["xl/worksheets/sheet1.xml"], "<v>4295.54</v>") {
		t.Fatal("derived balance must close from visible cents operands")
	}
	if strings.Contains(parts["xl/worksheets/sheet2.xml"], "舍入尾差（元）") {
		t.Fatal("derived account balance must not require a wallet-tail column")
	}
	for name, part := range parts {
		if !strings.HasPrefix(name, "xl/worksheets/") {
			continue
		}
		var cells struct {
			Rows []struct {
				Cells []struct {
					Value string `xml:"v"`
					Style string `xml:"s,attr"`
				} `xml:"c"`
			} `xml:"sheetData>row"`
		}
		if xml.Unmarshal([]byte(part), &cells) != nil {
			t.Fatal("invalid worksheet")
		}
		for _, r := range cells.Rows {
			for _, c := range r.Cells {
				if c.Style == "4" || c.Style == "12" || c.Style == "13" || c.Style == "14" {
					if c.Value != "" && c.Value != monthlyAuditCents(c.Value) {
						t.Fatal("export money is not cents-valued", name, c.Value)
					}
				}
			}
		}
	}
	after, _ := json.Marshal(p)
	if !bytes.Equal(before, after) || p.Consumption.Amount != "3026.195784" || *p.MonthEndBalance != "4295.548386" {
		t.Fatal("display changed real money")
	}
}
