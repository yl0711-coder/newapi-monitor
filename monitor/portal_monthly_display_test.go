package monitor

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
)

func TestMonthlySummaryFormattingKeepsExactWorkbookValues(t *testing.T) {
	e, now := monthlyTokenFixture()
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		t.Fatal(err)
	}
	p.Account = "本机合成账单验收"
	before, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
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
		raw, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		parts[file.Name] = string(raw)
	}
	if !strings.Contains(parts["xl/styles.xml"], `numFmtId="168" formatCode="&quot;¥&quot;#,##0.00;`) || !strings.Contains(parts["xl/styles.xml"], `numFmtId="164" formatCode="&quot;¥&quot;#,##0.00;`) || strings.Contains(parts["xl/styles.xml"], "#,##0.00####") {
		t.Fatal("all money styles must display cents")
	}
	if !strings.Contains(parts["xl/worksheets/sheet1.xml"], `s="13"><v>4.00</v>`) || !strings.Contains(parts["xl/worksheets/sheet1.xml"], `s="14"><v>26.00</v>`) {
		t.Fatal("summary numeric values must equal the displayed cents")
	}
	if !strings.Contains(parts["xl/worksheets/sheet2.xml"], `s="4"><v>2.00</v>`) || !strings.Contains(parts["xl/worksheets/sheet3.xml"], `s="4"><v>0.00</v>`) {
		t.Fatal("account or token cells are not cents-valued")
	}
	if strings.Contains(parts["xl/worksheets/sheet1.xml"], "金额按实际精度显示") {
		t.Fatal("removed technical footer restored")
	}
	sheet := parts["xl/worksheets/sheet1.xml"]
	labels := []string{"组织名称", "本月收支汇总", "月初余额（元）", "充值合计", "消费统计", "月末余额（元）", "额度变动记录", "序号", "本月无额度变动"}
	for i := 1; i < len(labels); i++ {
		if !strings.Contains(sheet, labels[i-1]) || strings.Index(sheet, labels[i-1]) >= strings.Index(sheet, labels[i]) {
			t.Fatal("summary does not precede detailed movements", labels[i])
		}
	}
	after, err := json.Marshal(p)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("formatting mutated the frozen bill", err)
	}
	if path := os.Getenv("MONTHLY_DISPLAY_XLSX"); path != "" {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if path := os.Getenv("MONTHLY_DISPLAY_DATASET"); path != "" {
		dataset := struct {
			Account    string                      `json:"account"`
			CapturedAt string                      `json:"captured_at"`
			Synthetic  bool                        `json:"synthetic"`
			Months     map[string]monthlyStatement `json:"months"`
		}{p.Account, now.Format("2006-01-02 15:04:05"), true, map[string]monthlyStatement{p.Month: p}}
		raw, err := json.Marshal(dataset)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMonthlySummaryTextFallbackExactCentRounding(t *testing.T) {
	for _, tc := range [][2]string{{"35460.175262", "35460.18"}, {"0.004999", "0.00"}, {"0.005000", "0.01"}, {"9.995000", "10.00"}, {"-0.000002", "0.00"}, {"-2.005000", "-2.01"}, {"-0.000000", "0.00"}, {"1000000000.995002", "1000000001.00"}, {"-1000000000.995002", "-1000000001.00"}} {
		if got := monthlySummaryMoneyText(tc[0]); got != tc[1] {
			t.Fatal("incorrect exact rounding", tc, got)
		}
	}
	for _, style := range []int{12, 13, 14} {
		cell := monthlyMoneyCell("A1", style, "1000000000.995002")
		if !strings.Contains(cell, "<v>1000000001.00</v>") {
			t.Fatal("large numeric summary lost digits or wrong display", cell)
		}
	}
	if cell := monthlyMoneyCell("A1", 4, "1000000000.995002"); !strings.Contains(cell, "<v>1000000001.00</v>") {
		t.Fatal("large detail must also display cents", cell)
	}
}
