package monitor

// Independent display checks: compare every funding row and the summary sheet
// with source evidence, not merely the generated workbook with itself.
import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func monthlyAuditQuota(q int64) string {
	n := new(big.Int).Mul(big.NewInt(q), big.NewInt(2)) // one quota = two micro-yuan
	negative := n.Sign() < 0
	digits := n.Abs(n).String()
	if len(digits) < 7 {
		digits = strings.Repeat("0", 7-len(digits)) + digits
	}
	value := digits[:len(digits)-6] + "." + digits[len(digits)-6:]
	if negative {
		return "-" + value
	}
	return value
}

type monthlyAuditCell struct {
	Ref     string `xml:"r,attr"`
	Value   string `xml:"v"`
	Text    string `xml:"is>t"`
	Formula string `xml:"f"`
}

func monthlyAuditSummaryCells(t *testing.T, raw []byte) map[string]monthlyAuditCell {
	t.Helper()
	z, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range z.File {
		if file.Name != "xl/worksheets/sheet1.xml" {
			continue
		}
		r, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		var sheet struct {
			Cells []monthlyAuditCell `xml:"sheetData>row>c"`
		}
		if err := xml.Unmarshal(body, &sheet); err != nil {
			t.Fatal(err)
		}
		cells := make(map[string]monthlyAuditCell, len(sheet.Cells))
		for _, cell := range sheet.Cells {
			if _, exists := cells[cell.Ref]; exists {
				t.Fatal("duplicate summary cell")
			}
			cells[cell.Ref] = cell
		}
		return cells
	}
	t.Fatal("missing summary sheet")
	return nil
}

func monthlyAuditProjection(t *testing.T, p monthlyStatement, e MonthlyBillEvidence, raw []byte) {
	t.Helper()
	names := map[int64]string{}
	for _, member := range p.Members {
		names[member.UserID] = member.Username
	}
	expected := []monthlyCredit{}
	var recharge, gifts, adjustment int64
	count := 0
	for _, user := range e.Users {
		canceled := map[int64]bool{}
		for _, pair := range user.Reversals {
			canceled[pair.CreditID], canceled[pair.DebitID] = true, true
		}
		for _, event := range user.Events {
			if canceled[event.ID] {
				continue
			}
			expected = append(expected, monthlyCredit{UserID: user.UserID, Username: names[user.UserID], At: event.At, Amount: monthlyAuditQuota(event.DeltaQuota), Method: event.Method, Kind: event.Kind})
			switch event.Kind {
			case "recharge":
				recharge += event.DeltaQuota
				count++
			case "gift":
				gifts += event.DeltaQuota
			case "adjustment":
				adjustment += event.DeltaQuota
			}
		}
	}
	sort.SliceStable(expected, func(i, j int) bool { return expected[i].At < expected[j].At })
	if !reflect.DeepEqual(p.Credits.Rows, expected) || *p.Credits.RechargeCount != count || *p.Credits.RechargeTotal != monthlyAuditQuota(recharge) || *p.Credits.GiftTotal != monthlyAuditQuota(gifts) || p.Credits.KnownTotal != monthlyAuditQuota(recharge+gifts) || p.Credits.Adjustment != monthlyAuditQuota(adjustment) {
		t.Fatal("source funding row/count/attribution differs from HTTP display")
	}
	cells := monthlyAuditSummaryCells(t, raw)
	value := func(ref string) string {
		c := cells[ref]
		return strings.TrimPrefix(c.Value+c.Text, "¥")
	}
	if value("C2") != p.Account || value("E3") != p.From+" 至 "+p.To {
		t.Fatal("workbook customer or period differs from HTTP")
	}
	firstFundingRow := 0
	for ref, cell := range cells {
		if strings.HasPrefix(ref, "A") && cell.Text == "序号" {
			n, err := strconv.Atoi(strings.TrimPrefix(ref, "A"))
			if err != nil || firstFundingRow != 0 {
				t.Fatal("invalid or duplicate funding header", ref, err)
			}
			firstFundingRow = n + 1
		}
	}
	if firstFundingRow == 0 {
		t.Fatal("funding header missing")
	}
	for i, event := range expected {
		row := strconv.Itoa(i + firstFundingRow)
		name := event.Username
		if name == "" {
			name = "—"
		}
		name += "\nID " + strconv.FormatInt(event.UserID, 10)
		want := []string{strconv.Itoa(i + 1), time.Unix(event.At, 0).In(usageCST).Format("2006-01-02 15:04:05"), name, event.Amount, event.Method, ""}
		want[3] = monthlyAuditCents(event.Amount)
		if event.Kind == "gift" {
			want[5] = "赠送额度"
		}
		for col, text := range want {
			if value(string(rune('A'+col))+row) != text {
				t.Fatalf("workbook funding row %d column %d differs from source", i, col)
			}
		}
	}
	checks := map[string]string{"充值合计": monthlyAuditQuota(recharge), "赠送合计": monthlyAuditQuota(gifts), "到账合计": monthlyAuditQuota(recharge + gifts), "月初余额（元）": *p.MonthOpeningBalance, "月末余额（元）": *p.MonthEndBalance}
	if adjustment != 0 {
		checks["余额扣减调整（元）"] = monthlyAuditQuota(adjustment)
	}
	if p.Consumption.Refund != "0.000000" {
		checks["退款金额（元）"], checks["净消费（元）"] = p.Consumption.Refund, p.Consumption.Net
	}
	metrics := false
	for ref, c := range cells {
		if !strings.HasPrefix(ref, "A") {
			continue
		}
		row := strings.TrimPrefix(ref, "A")
		if want, ok := checks[c.Text]; ok {
			col := "D"
			if c.Text == "月末余额（元）" {
				col = "C"
			}
			if value(col+row) != monthlyAuditCents(want) || (c.Text == "充值合计" && value("E"+row) != fmt.Sprintf("%d 次", count)) {
				t.Fatal("summary money or recharge count mismatch", c.Text)
			}
			delete(checks, c.Text)
		}
		if c.Text == "总消费（元）" {
			n, err := strconv.Atoi(row)
			if err != nil {
				t.Fatal(err)
			}
			next := strconv.Itoa(n + 1)
			requestWant := strconv.FormatInt(p.Consumption.Requests, 10)
			if p.Consumption.RequestsUnknown {
				requestWant = "—"
			}
			if value("A"+next) != monthlyAuditCents(p.Consumption.Amount) || value("C"+next) != requestWant {
				t.Fatal("consumption/request summary differs")
			}
			token := cells["E"+next]
			exact := token.Value
			if token.Formula != "" {
				exact = strings.Split(token.Formula, "/")[0]
			}
			tokenWant := strconv.FormatInt(p.Consumption.Tokens, 10)
			if p.Consumption.TokensUnknown {
				tokenWant, exact = "—", token.Text
				if token.Value != "" || token.Formula != "" {
					t.Fatal("unknown tokens have a hidden numeric value")
				}
			}
			if exact != tokenWant {
				t.Fatal("exact token count lost in workbook scaling")
			}
			metrics = true
		}
	}
	if len(checks) != 0 || !metrics {
		t.Fatal("summary fields missing", checks)
	}
}

func TestMonthlyProjectionAuditSynthetic(t *testing.T) {
	f := newMonthlyFunctionalFixture(t)
	var record PortalMonthlyBill
	if err := f.m.storeDB.First(&record, "group_id = ? AND month = ?", f.a.ID, f.latest).Error; err != nil {
		t.Fatal(err)
	}
	p, err := readPublishedMonthlyBill(record)
	if err != nil {
		t.Fatal(err)
	}
	xlsx, err := monthlyStatementXLSX(p)
	if err != nil {
		t.Fatal(err)
	}
	monthlyAuditProjection(t, p, f.evidence, xlsx)
}
