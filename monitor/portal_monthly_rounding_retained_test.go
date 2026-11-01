package monitor

// Opt-in independent acceptance of retained customer responses. No source
// connection, bill publication, filesystem writes or deployment configuration.
import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

type monthlyRoundingAuditRow struct {
	Index int                `xml:"r,attr"`
	Cells []monthlyAuditCell `xml:"c"`
}

func monthlyRoundingAuditRows(t *testing.T, part string) []monthlyRoundingAuditRow {
	t.Helper()
	var sheet struct {
		Rows []monthlyRoundingAuditRow `xml:"sheetData>row"`
	}
	if err := xml.Unmarshal([]byte(part), &sheet); err != nil {
		t.Fatal(err)
	}
	return sheet.Rows
}

func monthlyRoundingAuditValues(row monthlyRoundingAuditRow) map[string]string {
	values := map[string]string{}
	for _, c := range row.Cells {
		col := strings.TrimRight(c.Ref, "0123456789")
		values[col] = strings.TrimPrefix(c.Value+c.Text, "¥")
	}
	return values
}

func monthlyAuditIntegerCents(t *testing.T, value string) *big.Int {
	t.Helper()
	r, ok := new(big.Rat).SetString(value)
	if !ok {
		t.Fatal("invalid exported money", value)
	}
	r.Mul(r, big.NewRat(100, 1))
	if !r.IsInt() {
		t.Fatal("hidden sub-cent Excel amount", value)
	}
	return new(big.Int).Set(r.Num())
}

func monthlyAuditWalletEquation(t *testing.T, values map[string]string) {
	t.Helper()
	sum := new(big.Int)
	for key, sign := range map[string]int64{"月初余额（元）": 1, "到账（含赠送）（元）": 1, "余额扣减（元）": 1, "消费（元）": -1, "退款（元）": 1, "余额校正（元）": 1, "舍入尾差（元）": 1} {
		if v, ok := values[key]; ok {
			n := monthlyAuditIntegerCents(t, v)
			sum.Add(sum, n.Mul(n, big.NewInt(sign)))
		}
	}
	if sum.Cmp(monthlyAuditIntegerCents(t, values["月末余额（元）"])) != 0 {
		t.Fatal("exported cents wallet equation does not reconcile", values)
	}
}

func TestMonthlyRetainedDisplayRoundingWorkbook(t *testing.T) {
	root := os.Getenv("MONTHLY_ROUNDING_LOCAL_RESPONSES")
	if root == "" {
		t.Skip("explicit private retained-response directory required")
	}
	st, err := os.Stat(root)
	if err != nil || !filepath.IsAbs(root) || !st.IsDir() || st.Mode().Perm() != 0700 {
		t.Fatal("private absolute retained directory required")
	}
	files, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	pattern := regexp.MustCompile(`^group-(\d+)-customer-response-\d+\.json$`)
	latest := map[string]string{}
	for _, f := range files {
		m := pattern.FindStringSubmatch(f.Name())
		if len(m) > 0 && f.Name() > latest[m[1]] {
			latest[m[1]] = f.Name()
		}
	}
	if len(latest) < 6 {
		t.Fatal("six independently retained organizations required")
	}
	ids := make([]string, 0, len(latest))
	for id := range latest {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		t.Run("organization_"+id, func(t *testing.T) {
			raw, e := os.ReadFile(filepath.Join(root, latest[id]))
			if e != nil {
				t.Fatal(e)
			}
			var response struct {
				OK   bool             `json:"ok"`
				Data monthlyStatement `json:"data"`
			}
			if json.Unmarshal(raw, &response) != nil || !response.OK || !monthlyStatementAmountsComplete(response.Data) {
				t.Fatal("retained response not verified")
			}
			p := response.Data
			before, _ := json.Marshal(p)
			book, e := monthlyStatementXLSX(p)
			if e != nil {
				t.Fatal(e)
			}
			if download := os.Getenv("MONTHLY_ROUNDING_BROWSER_DOWNLOAD"); download != "" && id == "4" {
				if !filepath.IsAbs(download) {
					t.Fatal("explicit absolute browser download path required")
				}
				actual, err := os.ReadFile(download)
				if err != nil || !bytes.Equal(actual, book) {
					t.Fatal("actual browser download differs from independently audited workbook", err)
				}
			}
			parts := monthlyAuditWorkbookParts(t, book)
			labels := map[string]string{}
			metricsNext := false
			for _, row := range monthlyRoundingAuditRows(t, parts["xl/worksheets/sheet1.xml"]) {
				v := monthlyRoundingAuditValues(row)
				if metricsNext {
					labels["消费（元）"] = v["A"]
					metricsNext = false
				}
				if v["A"] == "总消费（元）" {
					metricsNext = true
				}
				if v["A"] == "月末余额（元）" {
					labels[v["A"]] = v["C"]
				} else if v["A"] != "" && v["D"] != "" {
					labels[v["A"]] = v["D"]
				}
			}
			if labels["消费（元）"] != monthlyAuditCents(p.Consumption.Amount) {
				t.Fatal("export consumption differs from source")
			}
			labels["到账（含赠送）（元）"] = labels["到账合计"]
			labels["余额扣减（元）"] = "0.00"
			if p.Credits.Adjustment != "0.000000" {
				labels["余额扣减（元）"] = labels["余额扣减调整（元）"]
			}
			labels["退款（元）"] = "0.00"
			if p.Consumption.Refund != "0.000000" {
				labels["退款（元）"] = labels["退款金额（元）"]
			}
			if labels["到账合计"] != monthlyAuditCents(p.Credits.KnownTotal) || labels["余额扣减（元）"] != monthlyAuditCents(p.Credits.Adjustment) || labels["退款（元）"] != monthlyAuditCents(p.Consumption.Refund) {
				t.Fatal("export funding or refund differs from source")
			}
			if v, ok := labels["四舍五入尾差（元）"]; ok {
				labels["舍入尾差（元）"] = v
			}
			org := monthlyMemberStatement{Opening: *p.MonthOpeningBalance, CreditTotal: p.Credits.KnownTotal, Adjustment: p.Credits.Adjustment, Consumption: *p.Consumption, BalanceCorrection: p.Credits.BalanceCorrection}
			if labels["月初余额（元）"] != monthlyAuditCents(*p.MonthOpeningBalance) || labels["月末余额（元）"] != monthlyAuditWalletClosing(org) {
				t.Fatal("derived summary balance does not match visible operands")
			}
			monthlyAuditWalletEquation(t, labels)
			memberRows := monthlyRoundingAuditRows(t, parts["xl/worksheets/sheet2.xml"])
			headers := map[string]string{}
			for _, row := range memberRows {
				if row.Index == 3 {
					for col, value := range monthlyRoundingAuditValues(row) {
						headers[col] = value
					}
				}
			}
			sums := map[string]*big.Int{}
			count := 0
			for _, row := range memberRows {
				if row.Index < 4 {
					continue
				}
				v := monthlyRoundingAuditValues(row)
				named := map[string]string{}
				for col, title := range headers {
					if col != "A" {
						named[title] = v[col]
					}
				}
				monthlyAuditWalletEquation(t, named)
				if v["A"] == "组织合计" {
					for title, n := range sums {
						if n.Cmp(monthlyAuditIntegerCents(t, named[title])) != 0 {
							t.Fatal("account column total and residual do not equal organization", title)
						}
					}
					continue
				}
				for title, value := range named {
					if sums[title] == nil {
						sums[title] = new(big.Int)
					}
					sums[title].Add(sums[title], monthlyAuditIntegerCents(t, value))
				}
				if v["A"] == "分项舍入尾差" {
					continue
				}
				count++
				matched := false
				for _, m := range p.Members {
					if strings.HasSuffix(v["A"], "\nID "+strconv.FormatInt(m.UserID, 10)) {
						matched = true
						if named["月末余额（元）"] != monthlyAuditWalletClosing(m) {
							t.Fatal("derived account balance does not match visible operands")
						}
						for title, source := range map[string]string{"月初余额（元）": m.Opening, "到账（含赠送）（元）": m.CreditTotal, "余额扣减（元）": m.Adjustment, "消费（元）": m.Consumption.Amount, "退款（元）": m.Consumption.Refund} {
							if named[title] != monthlyAuditCents(source) {
								t.Fatal("individual customer amount changed", title)
							}
						}
					}
				}
				if !matched {
					t.Fatal("unexpected account row")
				}
			}
			if count != len(p.Members) {
				t.Fatal("account omission")
			}
			var tokenTotals [3]big.Int
			perUser := map[string][]*big.Int{}
			actualTokenRows := 0
			for _, row := range monthlyRoundingAuditRows(t, parts["xl/worksheets/sheet3.xml"]) {
				if row.Index < 4 {
					continue
				}
				v := monthlyRoundingAuditValues(row)
				netCheck := new(big.Int).Sub(monthlyAuditIntegerCents(t, v["E"]), monthlyAuditIntegerCents(t, v["F"]))
				if h, ok := v["H"]; ok {
					netCheck.Add(netCheck, monthlyAuditIntegerCents(t, h))
				}
				if netCheck.Cmp(monthlyAuditIntegerCents(t, v["G"])) != 0 {
					t.Fatal("token row net cents do not reconcile")
				}
				for i, col := range []string{"E", "F", "G"} {
					n := monthlyAuditIntegerCents(t, v[col])
					tokenTotals[i].Add(&tokenTotals[i], n)
					if v["B"] != "—" {
						if perUser[v["B"]] == nil {
							perUser[v["B"]] = []*big.Int{new(big.Int), new(big.Int), new(big.Int)}
						}
						perUser[v["B"]][i].Add(perUser[v["B"]][i], n)
					}
				}
				if v["D"] != "—" || v["C"] == "历史未记录令牌" {
					actualTokenRows++
				}
			}
			wantTokenRows := 0
			for _, m := range p.Members {
				wantTokenRows += len(m.TokenConsumption)
				for i, source := range []string{m.Consumption.Amount, m.Consumption.Refund, m.Consumption.Net} {
					sum := new(big.Int)
					if perUser[strconv.FormatInt(m.UserID, 10)] != nil {
						sum = perUser[strconv.FormatInt(m.UserID, 10)][i]
					}
					if sum.Cmp(monthlyAuditIntegerCents(t, monthlyAuditCents(source))) != 0 {
						t.Fatal("token rows and explicit tails do not equal this user's amount")
					}
				}
			}
			if actualTokenRows != wantTokenRows {
				t.Fatal("actual tokens omitted or replaced")
			}
			for i, source := range []string{p.Consumption.Amount, p.Consumption.Refund, p.Consumption.Net} {
				if tokenTotals[i].Cmp(monthlyAuditIntegerCents(t, monthlyAuditCents(source))) != 0 {
					t.Fatal("flat Excel token SUM differs from summary")
				}
			}
			after, _ := json.Marshal(p)
			if string(before) != string(after) {
				t.Fatal("frozen bill mutated")
			}
			t.Logf("accounts=%d tokens=%d all cents equations and raw payload preserved", count, wantTokenRows)
		})
	}
}
