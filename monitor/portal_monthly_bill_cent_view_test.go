package monitor

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestMonthlyBillCentViewAndWorkbookShareRetainedMoney(t *testing.T) {
	p, plan, record := monthlyCentStorageFixture(t)
	retained, err := monthlyReadCentBill(record)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(retained)
	view, err := monthlyCentBillView(retained)
	if err != nil || view.MonthOpeningBalance == nil || *view.MonthEndBalance != plan.Organization.Closing ||
		view.Members[0].Closing != plan.Members[0].Wallet.Closing {
		t.Fatal("customer view differs from retained cents", err)
	}
	if p.Members[0].Closing == view.Members[0].Closing {
		t.Fatal("legacy conflict fixture was not exercised")
	}
	book, err := monthlyCentBillXLSX(retained)
	if err != nil {
		t.Fatal(err)
	}
	parts := monthlyAuditWorkbookParts(t, book)
	for name, part := range parts {
		if strings.Contains(part, "舍入尾差") || strings.Contains(part, "<f>") || strings.Contains(part, "<f ") {
			t.Fatal("customer workbook contains a technical tail or executable formula", name)
		}
	}
	if !strings.Contains(parts["xl/worksheets/sheet1.xml"], "<v>29.99</v>") ||
		!strings.Contains(parts["xl/worksheets/sheet2.xml"], "<v>9.99</v>") ||
		!strings.Contains(parts["xl/worksheets/sheet3.xml"], "<v>0.01</v>") {
		t.Fatal("workbook did not use the retained display values")
	}
	row, err := monthlyCentBillSummary(retained)
	if err != nil || !row.Downloadable || row.Consumption.Amount != view.Consumption.Amount || *row.CreditTotal != view.Credits.KnownTotal {
		t.Fatal("list, detail and workbook disagree", err)
	}
	after, _ := json.Marshal(retained)
	if !bytes.Equal(before, after) || !reflect.DeepEqual(plan, retained.Plan) {
		t.Fatal("customer rendering changed retained data")
	}
}

func TestMonthlyBillCentViewAllowsRoundedZeroMovementsWithoutFakingSource(t *testing.T) {
	p := monthlyCentBillSource(t, "2026-09", monthlyCentBillSpec{id: 1, recharge: 1, gift: 1, consume: 1})
	plan, err := monthlyPlanBillCentMonth(99, monthlyDigest([]byte("synthetic-origin")), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	retained := monthlyRetainedCentBill{Source: p, Plan: plan}
	view, err := monthlyCentBillView(retained)
	if err != nil || len(view.Credits.Rows) != len(p.Credits.Rows) {
		t.Fatal("rounded-zero movement was lost", err)
	}
	for _, row := range view.Credits.Rows {
		if row.Amount != "0.000000" {
			t.Fatal("micropayment display is not zero cents")
		}
	}
	// The display has zero-valued movement rows; it is NOT valid source evidence.
	if monthlyStatementAmountsComplete(monthlyStatement(view)) {
		t.Fatal("customer projection may be mistaken for source evidence")
	}
	if _, err := monthlyCentBillXLSX(retained); err != nil {
		t.Fatal("verified rounded-zero bill could not be downloaded", err)
	}
	if !monthlyStatementAmountsComplete(p) || p.Credits.Rows[0].Amount == "0.000000" {
		t.Fatal("source movement changed")
	}
}

func TestMonthlyBillCentViewRejectsDamagedRetainedPlan(t *testing.T) {
	p, plan, _ := monthlyCentStorageFixture(t)
	plan.Organization.Closing = "99.000000"
	plan.Digest = monthlyBillCentMonthDigest(plan)
	retained := monthlyRetainedCentBill{Source: p, Plan: plan}
	if _, err := monthlyCentBillView(retained); err == nil {
		t.Fatal("damaged plan reached the customer view")
	}
	if _, err := monthlyCentBillXLSX(retained); err == nil {
		t.Fatal("damaged plan was exported")
	}
	if _, err := monthlyCentBillSummary(retained); err == nil {
		t.Fatal("damaged plan reached the list")
	}
}
