package monitor

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func monthlyMembersFixture(t *testing.T) monthlyStatement {
	t.Helper()
	now := time.Now()
	start, end, _ := monthlyWindow("", now)
	e := MonthlyBillEvidence{Version: 2, GroupID: 1, Month: start.Format("2006-01"), ReviewedBy: "test", ReviewedAt: now.Unix(), SourceReference: "synthetic local member receipt"}
	for i, id := range []int64{122, 125, 126} {
		u := MonthlyBillUserEvidence{UserID: id, Complete: true, WalletOnly: true, Through: end.Unix(), Accounting: monthlyAccountingFixture(0, 0)}
		switch i {
		case 0:
			u.OpeningQuota, u.ConsumeQuota, u.ClosingQuota = 400000000, 1, -1
			u.Events = []MonthlyBillCreditEvidence{{ID: 1, At: start.Unix() + 1, DeltaQuota: -400000000, Kind: "adjustment", Method: "余额扣减"}}
		case 1:
			u.ConsumeQuota, u.ClosingQuota = 2, 400000000-2
			u.Events = []MonthlyBillCreditEvidence{{ID: 2, At: start.Unix() + 2, DeltaQuota: 400000000, Kind: "recharge", Method: "后台充值"}}
		}
		u.ExpectedCreditEvents, u.ReverseClosingQuota = len(u.Events), u.ClosingQuota
		u.Requests, u.Tokens = int64(i), int64(i)*10
		u.Accounting = monthlyAccountingFixture(u.ConsumeQuota, 0)
		e.Users = append(e.Users, u)
	}
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		t.Fatal(err)
	}
	p.Account = "逐账号验收（合成数据）"
	for i := range p.Members {
		p.Members[i].Username = map[int64]string{122: "转出账号", 125: "=WEBSERVICE(\"https://example.invalid\") <转入>", 126: "零活动账号"}[p.Members[i].UserID]
	}
	for i := range p.Credits.Rows {
		for _, m := range p.Members {
			if p.Credits.Rows[i].UserID == m.UserID {
				p.Credits.Rows[i].Username = m.Username
			}
		}
	}
	return p
}

func TestMonthlyMembersPreserveIndependentBalancesAndTotals(t *testing.T) {
	p := monthlyMembersFixture(t)
	if !monthlySummary(p).Downloadable || len(p.Members) != 3 {
		t.Fatal("valid member breakdown rejected")
	}
	a, b, zero := p.Members[0], p.Members[1], p.Members[2]
	if a.UserID != 122 || a.Adjustment != "-800.000000" || a.Closing != "-0.000002" || b.CreditTotal != "800.000000" || b.Closing != "799.999996" || zero.Closing != "0.000000" || *p.MonthEndBalance != "799.999994" {
		t.Fatalf("account amounts changed: %+v", p.Members)
	}
	if *p.Credits.RechargeTotal != "800.000000" || p.Credits.Adjustment != "-800.000000" {
		t.Fatal("transfer sides were netted away")
	}
}

func TestMonthlyMembersRejectDamageEvenWhenOrganizationStillBalances(t *testing.T) {
	for name, mutate := range map[string]func(*monthlyStatement){
		"missing detail":       func(p *monthlyStatement) { p.Members = nil },
		"unknown version":      func(p *monthlyStatement) { p.MemberDetailsVersion = 2 },
		"undeclared detail":    func(p *monthlyStatement) { p.MemberDetailsVersion = 0 },
		"duplicate account":    func(p *monthlyStatement) { p.Members[1].UserID = p.Members[0].UserID },
		"missing opening":      func(p *monthlyStatement) { p.Members[0].Opening = "" },
		"small balance erased": func(p *monthlyStatement) { p.Members[0].Closing = "0.000000" },
		"offsetting errors": func(p *monthlyStatement) {
			p.Members[0].Closing = "0.000000"
			p.Members[1].Closing = "799.999994"
		},
		"transfer wrong user": func(p *monthlyStatement) { p.Credits.Rows[0].UserID = 125 },
		"other user event":    func(p *monthlyStatement) { p.Credits.Rows[0].UserID = 999 },
		"name mismatch":       func(p *monthlyStatement) { p.Members[0].Username = "wrong" },
		"token mismatch":      func(p *monthlyStatement) { p.Members[0].Consumption.Tokens++ },
		"negative requests":   func(p *monthlyStatement) { p.Members[0].Consumption.Requests = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			p := monthlyMembersFixture(t)
			mutate(&p)
			if monthlySummary(p).Downloadable {
				t.Fatal("damaged per-account values passed")
			}
			if _, err := monthlyStatementXLSX(p); err == nil {
				t.Fatal("damaged per-account values exported")
			}
		})
	}
}

func TestMonthlyMembersLegacyCompatibilityAndScope(t *testing.T) {
	p := monthlyMembersFixture(t)
	read := func(p monthlyStatement) error {
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		_, err = readPublishedMonthlyBill(PortalMonthlyBill{Month: p.Month, Payload: string(b), PayloadHash: monthlyDigest(b), MemberFingerprint: portalMemberFingerprintFromIDs([]int64{122, 125, 126})})
		return err
	}
	if err := read(p); err != nil {
		t.Fatal(err)
	}
	p.Members = p.Members[:2] // no-money account: totals alone cannot detect omission
	if err := read(p); err == nil {
		t.Fatal("omitted zero-activity member was not detected")
	}
	p.MemberDetailsVersion, p.Members = 0, nil
	if err := read(p); err != nil {
		t.Fatal("legacy summary publication lost compatibility", err)
	}
	data, err := monthlyStatementXLSX(p)
	if err != nil {
		t.Fatal(err)
	}
	z, _ := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	for _, file := range z.File {
		if file.Name == "xl/worksheets/sheet2.xml" {
			t.Fatal("legacy member details fabricated")
		}
	}
}

func TestMonthlyMembersWorkbookExactCells(t *testing.T) {
	for _, amount := range []string{"0.000000", "-0.000002", "999999999.999998", "1000000000.000000"} {
		if cell := monthlyMoneyCell("A1", 4, amount); !strings.Contains(cell, "<v>"+monthlyAuditCents(amount)+"</v>") {
			t.Fatal("ordinary exact money must remain numeric", amount, cell)
		}
	}
	for _, amount := range []string{"10000000000000.010002", "-10000000000000.010002"} {
		if cell := monthlyMoneyCell("A1", 4, amount); !strings.Contains(cell, `t="inlineStr"`) || !strings.Contains(cell, "¥"+monthlySummaryMoneyText(amount)) {
			t.Fatal("large money must use exact cents text in Excel", amount, cell)
		}
	}
	if monthlyTextRowHeight("long_account_name_needing_three_lines\nID 125", 17, 40) < 70 || monthlyTextRowHeight("账号\nID 122", 17, 40) != 40 {
		t.Fatal("wrapped account text row height regression")
	}
	p := monthlyMembersFixture(t)
	data, err := monthlyStatementXLSX(p)
	if err != nil {
		t.Fatal(err)
	}
	z, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	parts := map[string]string{}
	for _, file := range z.File {
		r, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		parts[file.Name] = string(b)
		decoder := xml.NewDecoder(bytes.NewReader(b))
		for {
			if _, err := decoder.Token(); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatal(file.Name, err)
			}
		}
	}
	sheet := parts["xl/worksheets/sheet2.xml"]
	for _, value := range []string{`r="G4" s="4"><v>0.00</v>`, `r="G5" s="4"><v>800.00</v>`, `r="D4" s="4"><v>-800.00</v>`, `r="C5" s="4"><v>800.00</v>`, `r="G6" s="4"><v>0.00</v>`, "&lt;转入&gt;"} {
		if !strings.Contains(sheet, value) {
			t.Fatal("missing exact member value", value)
		}
	}
	if strings.Contains(sheet, "<f>") || strings.Contains(parts["xl/styles.xml"], "#,##0.00####") || !strings.Contains(parts["xl/workbook.xml"], `name="账号收支"`) {
		t.Fatal("unsafe formula or precision/detail sheet missing")
	}
	if dir := os.Getenv("MONTHLY_MEMBER_ARTIFACTS"); dir != "" {
		if !filepath.IsAbs(dir) {
			t.Fatal("artifact path must be absolute")
		}
		file, err := os.OpenFile(filepath.Join(dir, "member-precision.xlsx"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = file.Write(data)
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("write: %v close: %v", err, closeErr)
		}
	}
}
