package monitor

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestMonthlySummaryPrecisionSharedCasesPreserveMoney(t *testing.T) {
	raw, err := os.ReadFile("testdata/monthly_summary_precision.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name   string
		Places int
		Micro  []int64
	}
	if json.Unmarshal(raw, &cases) != nil || len(cases) < 8 {
		t.Fatal("shared cases unavailable")
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			if len(tc.Micro) != 10 {
				t.Fatal("invalid shared case")
			}
			money := make([]string, len(tc.Micro))
			for i, n := range tc.Micro {
				sign := ""
				if n < 0 {
					sign = "-"
					n = -n
				}
				money[i] = fmt.Sprintf("%s%d.%06d", sign, n/1000000, n%1000000)
			}
			p := monthlyStatement{MonthOpeningBalance: &money[0], MonthEndBalance: &money[6],
				Credits:     monthlyCredits{KnownTotal: money[1], Adjustment: money[2], BalanceCorrection: money[5], RechargeTotal: &money[7], GiftTotal: &money[8]},
				Consumption: &monthlyConsumption{Amount: money[3], Refund: money[4], Net: money[9]}}
			before, _ := json.Marshal(p)
			if got := monthlySummaryPrecision(p); got != 2 {
				t.Fatal("bill display must always use cents", got)
			}
			after, _ := json.Marshal(p)
			if !bytes.Equal(before, after) {
				t.Fatal("display changed frozen money")
			}
			p.MonthOpeningBalance = nil
			if monthlySummaryPrecision(p) != 2 {
				t.Fatal("missing old opening inferred")
			}
		})
	}
}

func TestMonthlySummaryPrecisionListAndWorkbookUseSameDigits(t *testing.T) {
	e, now := monthlyTokenFixture()
	e.Users = e.Users[:1]
	u := &e.Users[0]
	u.OpeningQuota, u.ConsumeQuota, u.RefundQuota, u.ClosingQuota = 5002000, 1503000, 0, 3499000
	u.ReverseClosingQuota = u.ClosingQuota
	u.Accounting = monthlyAccountingFixture(u.ConsumeQuota, 0)
	u.TokenUsage.Rows = []MonthlyBillTokenEvidence{{TokenID: 11, Name: "synthetic token", ConsumeQuota: u.ConsumeQuota}}
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(p)
	r := monthlySummary(p)
	if r.MoneyDecimalPlaces != 0 {
		t.Fatal("default two-place list must omit the legacy precision override", r)
	}
	book, err := monthlyStatementXLSX(p)
	if err != nil {
		t.Fatal(err)
	}
	z, err := zip.NewReader(bytes.NewReader(book), int64(len(book)))
	if err != nil {
		t.Fatal(err)
	}
	parts := map[string]string{}
	for _, f := range z.File {
		in, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(in)
		in.Close()
		if err != nil {
			t.Fatal(err)
		}
		parts[f.Name] = string(b)
	}
	if !strings.Contains(parts["xl/styles.xml"], `numFmtId="168" formatCode="&quot;¥&quot;#,##0.00;`) {
		t.Fatal("workbook did not select two-place summary")
	}
	for _, v := range []string{"10.004000", "3.006000"} {
		if !strings.Contains(parts["xl/worksheets/sheet1.xml"], "<v>"+monthlyAuditCents(v)+"</v>") {
			t.Fatal("summary cents differ from source rounding", v)
		}
	}
	if !strings.Contains(parts["xl/worksheets/sheet1.xml"], "<v>6.99</v>") {
		t.Fatal("derived closing must be 10.00 - 3.01 = 6.99")
	}
	if strings.Contains(parts["xl/styles.xml"], "#,##0.00####") {
		t.Fatal("detail display must also use two places")
	}
	after, _ := json.Marshal(p)
	if !bytes.Equal(before, after) {
		t.Fatal("display/export changed saved object")
	}
	if got := monthlyMoneyCellAt("A1", 12, "1000000000.995002", 3); !strings.Contains(got, "<v>1000000001.00</v>") {
		t.Fatal("large text fallback must ignore legacy precision hints", got)
	}
	if got := monthlySummaryMoneyTextAt("-0.000002", 3); got != "0.000" {
		t.Fatal("rounded zero must not have a negative sign", got)
	}
}
