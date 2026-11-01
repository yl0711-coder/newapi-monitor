package monitor

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Manual renderer artifact only; never enabled by application startup or CI.
func TestMonthlyTokenSyntheticPreviewFiles(t *testing.T) {
	dir := os.Getenv("MONTHLY_TOKEN_SYNTHETIC_PREVIEW")
	if dir == "" {
		t.Skip("explicit private synthetic preview directory required")
	}
	st, err := os.Lstat(dir)
	resolved, resolveErr := filepath.EvalSymlinks(dir)
	if err != nil || resolveErr != nil || !st.IsDir() || st.Mode().Perm() != 0700 || resolved != dir || !filepath.IsAbs(dir) {
		t.Fatal("private canonical preview directory required")
	}
	e, now := monthlyTokenFixture()
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		t.Fatal(err)
	}
	p.Account = "本机合成组织 · 非客户账单"
	for i := range p.Members {
		p.Members[i].Username = "合成用户" + strconv.Itoa(i+1)
	}
	p.Members[0].TokenConsumption[0].Name = `<测试名称> =1+1`
	book, err := monthlyStatementXLSX(p)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{"synthetic": true, "account": p.Account, "captured_at": now.Format(time.RFC3339), "months": map[string]monthlyStatement{p.Month: p}})
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{"dataset.json": raw, "bill.xlsx": book} {
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := f.Write(body)
		closeErr := f.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatal(writeErr, closeErr)
		}
	}
}

// Exercise the full allowed hierarchy, not a production load test. The XLSX
// is parsed again so a fast encoder cannot hide omitted or duplicated money.
func TestMonthlyTokenCapacityExactExportAndOrganizationLimit(t *testing.T) {
	e, now := monthlyTokenFixture()
	base := e.Users[0]
	e.Users = nil
	var consume, refund int64
	for account := range 10 {
		u := base
		u.UserID = int64(1000 + account)
		u.TokenUsage = &MonthlyBillTokenUsage{UserID: u.UserID, Month: e.Month, Source: "synthetic-capacity-only", Rows: []MonthlyBillTokenEvidence{}}
		u.ConsumeQuota, u.RefundQuota = 0, 0
		for token := range monthlyTokenPerUserLimit {
			q, r := int64(1000+token), int64(token%7)
			u.TokenUsage.Rows = append(u.TokenUsage.Rows, MonthlyBillTokenEvidence{TokenID: u.UserID*10000 + int64(token+1), Name: "同名令牌", ConsumeQuota: q, RefundQuota: r})
			u.ConsumeQuota += q
			u.RefundQuota += r
		}
		u.ClosingQuota = u.OpeningQuota - u.ConsumeQuota + u.RefundQuota
		u.ReverseClosingQuota = u.ClosingQuota
		u.Accounting = monthlyAccountingFixture(u.ConsumeQuota, u.RefundQuota)
		e.Users = append(e.Users, u)
		consume += u.ConsumeQuota
		refund += u.RefundQuota
	}
	started := time.Now()
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil || !monthlyStatementAmountsComplete(p) || len(p.Members) != 10 || p.Consumption.Amount != monthlyQuotaMoney(consume) || p.Consumption.Refund != monthlyQuotaMoney(refund) {
		t.Fatal("full-size hierarchy lost money", err)
	}
	buildTime := time.Since(started)
	raw, err := json.Marshal(p)
	if err != nil || len(raw) > 2<<20 {
		t.Fatal("ordinary names at count limit exceed public payload budget", len(raw), err)
	}
	var saved monthlyStatement
	if json.Unmarshal(raw, &saved) != nil || !monthlyStatementAmountsComplete(saved) {
		t.Fatal("full-size saved payload failed validation")
	}
	started = time.Now()
	book, err := monthlyStatementXLSX(saved)
	if err != nil {
		t.Fatal(err)
	}
	exportTime := time.Since(started)
	z, err := zip.NewReader(bytes.NewReader(book), int64(len(book)))
	if err != nil {
		t.Fatal(err)
	}
	var body []byte
	for _, f := range z.File {
		if f.Name != "xl/worksheets/sheet3.xml" {
			continue
		}
		r, openErr := f.Open()
		if openErr != nil {
			t.Fatal(openErr)
		}
		body, err = io.ReadAll(r)
		closeErr := r.Close()
		if err != nil || closeErr != nil {
			t.Fatal(err, closeErr)
		}
	}
	var sheet struct {
		Rows []struct {
			Index int `xml:"r,attr"`
			Cells []struct {
				Ref   string `xml:"r,attr"`
				Value string `xml:"v"`
				Text  string `xml:"is>t"`
			} `xml:"c"`
		} `xml:"sheetData>row"`
	}
	if xml.Unmarshal(body, &sheet) != nil || len(sheet.Rows) < monthlyTokenBillLimit+3 || len(sheet.Rows) > monthlyTokenBillLimit+len(p.Members)+4 || bytes.Contains(body, []byte("<f>")) {
		t.Fatal("wrong exported row count or unsafe formula", len(sheet.Rows))
	}
	var sums [3]int64
	seen := map[string]bool{}
	actualTokenRows := 0
	for _, row := range sheet.Rows {
		if row.Index < 4 {
			continue
		}
		isTail := false
		for _, cell := range row.Cells {
			if strings.HasPrefix(cell.Ref, "C") && (cell.Text == "分项舍入尾差" || cell.Text == "组织汇总舍入尾差") {
				isTail = true
			}
		}
		if !isTail {
			actualTokenRows++
		}
		for _, cell := range row.Cells {
			if !strings.ContainsAny(cell.Ref[:1], "EFG") {
				continue
			}
			if cell.Ref[1:] != strconv.Itoa(row.Index) || seen[cell.Ref] {
				t.Fatal("duplicated or misplaced money cell", cell.Ref)
			}
			seen[cell.Ref] = true
			q, ok := newMonthlyMoney(cell.Value + "0000")
			if !ok {
				t.Fatal("exported amount lost precision", cell.Ref, cell.Value)
			}
			sums[cell.Ref[0]-'E'] += q
		}
	}
	var expectedCents [3]int64
	for i, q := range []int64{consume, refund, consume - refund} {
		expectedCents[i], _ = newMonthlyMoney(monthlyAuditCents(monthlyQuotaMoney(q)) + "0000")
	}
	if actualTokenRows != monthlyTokenBillLimit || len(seen) != (len(sheet.Rows)-3)*3 || sums != expectedCents {
		t.Fatal("exported token rows plus explicit residuals do not equal displayed organization totals", sums, expectedCents)
	}
	t.Logf("10 users / 10000 tokens: build=%s export=%s JSON=%d XLSX=%d; every money cell reconciled", buildTime, exportTime, len(raw), len(book))
	// Every individual user remains valid, but the organization exceeds its
	// independent limit. Never truncate silently to make the bill downloadable.
	u := e.Users[0]
	u.UserID = 2000
	copyUsage := *u.TokenUsage
	copyUsage.UserID = u.UserID
	copyUsage.Rows = append([]MonthlyBillTokenEvidence{}, copyUsage.Rows...)
	for i := range copyUsage.Rows {
		copyUsage.Rows[i].TokenID = u.UserID*10000 + int64(i+1)
	}
	u.TokenUsage = &copyUsage
	e.Users = append(e.Users, u)
	if _, _, err := buildPublishedMonthlyBill(e, now); err == nil {
		t.Fatal("organization token limit bypassed")
	}
}
