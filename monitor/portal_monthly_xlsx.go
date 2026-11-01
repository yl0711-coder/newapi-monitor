package monitor

// Small bounded OOXML workbook; no macro, external link, HTML masquerading as
// Excel, or executable formula derived from customer-controlled text.
import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const monthlyXLSXMIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

func monthlyXML(s string) string {
	s = strings.Map(func(r rune) rune {
		if r == 9 || r == 10 || r == 13 || r >= 32 && r <= 0xD7FF || r >= 0xE000 && r <= 0xFFFD || r >= 0x10000 && r <= 0x10FFFF {
			return r
		}
		return -1
	}, s)
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

// This customer workbook contains cents-valued numbers, not hidden sub-cent
// values. SUM therefore uses the same numbers customers see. The frozen DTO
// and source quotas remain unchanged. Very large cents use literal text.
func monthlyMoneyCell(ref string, style int, value string) string {
	return monthlyMoneyCellAt(ref, style, value, 2)
}

func monthlyMoneyCellAt(ref string, style int, value string, _ int) string {
	value = monthlySummaryMoneyTextAt(value, 2)
	digits := strings.ReplaceAll(strings.TrimPrefix(value, "-"), ".", "")
	digits = strings.Trim(digits, "0")
	if len(digits) > 15 {
		return fmt.Sprintf(`<c r="%s" s="%d" t="inlineStr"><is><t>¥%s</t></is></c>`, ref, style, monthlyXML(value))
	}
	return fmt.Sprintf(`<c r="%s" s="%d"><v>%s</v></c>`, ref, style, value)
}

// Integer arithmetic avoids float rounding at a half-cent or large magnitude.
// Only export/display values are rounded; the original DTO is never changed.
func monthlySummaryMoneyText(value string) string {
	return monthlySummaryMoneyTextAt(value, 2)
}

func monthlyStatementXLSX(p monthlyStatement) ([]byte, error) {
	if !p.Published || !monthlySummary(p).Downloadable || p.MonthOpeningBalance == nil || p.MonthEndBalance == nil || p.Credits.RechargeTotal == nil || p.Credits.GiftTotal == nil || len(p.Credits.Rows) > monthlyCreditLimit {
		return nil, errors.New("未发布或不完整的账单不可导出")
	}
	return monthlyRenderStatementXLSX(p)
}

// Only called after validation of the original publication. A new cent view
// can legitimately contain a rounded-zero movement; it is not source evidence.
func monthlyRenderStatementXLSX(p monthlyStatement) ([]byte, error) {
	cr, s := p.Credits, p.Consumption
	org, _ := monthlyOrganizationWallet(p)
	var frozenOrg *monthlyDisplayBalance
	if p.DisplayBalances != nil {
		frozenOrg = &p.DisplayBalances.Organization
	}
	displayOpening, displayClosing, displayOK := monthlyDisplayWallet(org, frozenOrg)
	if !displayOK {
		return nil, errors.New("invalid derived bill balance")
	}
	precision := monthlySummaryPrecision(p)
	summaryMoney := func(ref string, style int, amount string) string {
		return monthlyMoneyCellAt(ref, style, amount, precision)
	}
	var rows strings.Builder
	merges := []string{}
	n := 0
	text := func(ref string, style int, v string) string {
		return fmt.Sprintf(`<c r="%s" s="%d" t="inlineStr"><is><t xml:space="preserve">%s</t></is></c>`, ref, style, monthlyXML(v))
	}
	number := func(ref string, style int, v string) string {
		return fmt.Sprintf(`<c r="%s" s="%d"><v>%s</v></c>`, ref, style, v)
	}
	// Strict numeric serialization; never copy an arbitrary string into XML.
	validMoney := func(v string) bool {
		if len(v) > 30 {
			return false
		}
		_, ok := newMonthlyMoney(v)
		return ok
	}
	moneyValues := []string{*cr.RechargeTotal, *cr.GiftTotal, cr.KnownTotal, cr.Adjustment, s.Amount, s.Refund, s.Net, *p.MonthEndBalance}
	if p.MonthOpeningBalance != nil {
		moneyValues = append(moneyValues, *p.MonthOpeningBalance)
	}
	for _, r := range cr.Rows {
		moneyValues = append(moneyValues, r.Amount)
	}
	for _, v := range moneyValues {
		if !validMoney(v) {
			return nil, errors.New("invalid money")
		}
	}
	row := func(height int, build func(int) string) {
		n++
		fmt.Fprintf(&rows, `<row r="%d" ht="%d" customHeight="1">%s</row>`, n, height, build(n))
	}
	merge := func(a, b string) { merges = append(merges, a+":"+b) }
	full := func(value string, style, height int) {
		row(height, func(r int) string {
			merge(fmt.Sprintf("A%d", r), fmt.Sprintf("F%d", r))
			return text(fmt.Sprintf("A%d", r), style, value)
		})
	}
	full(p.Account+" · 月度消费账单", 1, 44)
	row(30, func(r int) string {
		merge("A2", "B2")
		merge("C2", "F2")
		return text("A2", 15, "组织名称") + text("C2", 0, p.Account)
	})
	row(32, func(r int) string {
		merge("A3", "B3")
		merge("E3", "F3")
		return text("A3", 15, "服务类型") + text("C3", 0, "MaaS 云计算服务") + text("D3", 15, "账单期间（北京时间）") + text("E3", 0, p.From+" 至 "+p.To)
	})
	funding := func() {
		full("额度变动记录", 2, 29)
		row(28, func(r int) string {
			return text(fmt.Sprintf("A%d", r), 3, "序号") + text(fmt.Sprintf("B%d", r), 3, "时间（北京时间）") + text(fmt.Sprintf("C%d", r), 3, "被操作账号") + text(fmt.Sprintf("D%d", r), 3, "金额（元）") + text(fmt.Sprintf("E%d", r), 3, "操作类型") + text(fmt.Sprintf("F%d", r), 3, "备注")
		})
		for i, entry := range cr.Rows {
			row(monthlyTextRowHeight(entry.Username+"\nID "+strconv.FormatInt(entry.UserID, 10), 17, 40), func(r int) string {
				note := ""
				if entry.Kind == "gift" {
					note = "赠送额度"
				}
				account := entry.Username
				if account == "" {
					account = "—"
				}
				if entry.UserID > 0 {
					// Keep the immutable ID on its own line; automatic wrapping can
					// otherwise split its digits after a long username.
					account += "\nID " + strconv.FormatInt(entry.UserID, 10)
				}
				return number(fmt.Sprintf("A%d", r), 7, strconv.Itoa(i+1)) + text(fmt.Sprintf("B%d", r), 0, time.Unix(entry.At, 0).In(usageCST).Format("2006-01-02 15:04:05")) + text(fmt.Sprintf("C%d", r), 0, account) + monthlyMoneyCell(fmt.Sprintf("D%d", r), 4, entry.Amount) + text(fmt.Sprintf("E%d", r), 0, entry.Method) + text(fmt.Sprintf("F%d", r), 0, note)
			})
		}
		if len(cr.Rows) == 0 {
			full("本月无额度变动", 0, 28)
		}
		for _, item := range []struct{ kind, amount, label string }{
			{"recharge", *cr.RechargeTotal, "充值分项舍入尾差"}, {"gift", *cr.GiftTotal, "赠送分项舍入尾差"}, {"adjustment", cr.Adjustment, "扣减分项舍入尾差"},
		} {
			var terms []monthlyDisplayTerm
			for _, entry := range cr.Rows {
				if entry.Kind == item.kind {
					terms = append(terms, monthlyDisplayTerm{entry.Amount, 1})
				}
			}
			residual := monthlyDisplayResidual(item.amount, terms)
			if !monthlyDisplayNonzero(residual) {
				continue
			}
			row(28, func(r int) string {
				merge(fmt.Sprintf("A%d", r), fmt.Sprintf("C%d", r))
				merge(fmt.Sprintf("E%d", r), fmt.Sprintf("F%d", r))
				return text(fmt.Sprintf("A%d", r), 0, item.label) + monthlyMoneyCell(fmt.Sprintf("D%d", r), 4, residual) + text(fmt.Sprintf("E%d", r), 0, "仅显示舍入，不是交易")
			})
		}
	}
	summary := func(label, amount, extra string) {
		row(28, func(r int) string {
			merge(fmt.Sprintf("A%d", r), fmt.Sprintf("C%d", r))
			merge(fmt.Sprintf("E%d", r), fmt.Sprintf("F%d", r))
			return text(fmt.Sprintf("A%d", r), 15, label) + summaryMoney(fmt.Sprintf("D%d", r), 12, amount) + text(fmt.Sprintf("E%d", r), 0, extra)
		})
	}
	full("本月收支汇总", 2, 29)
	if p.MonthOpeningBalance != nil {
		summary("月初余额（元）", displayOpening, "")
	}
	summary("充值合计", *cr.RechargeTotal, strconv.Itoa(*cr.RechargeCount)+" 次")
	summary("赠送合计", *cr.GiftTotal, "")
	if residual := monthlyDisplayResidual(cr.KnownTotal, []monthlyDisplayTerm{{*cr.RechargeTotal, 1}, {*cr.GiftTotal, 1}}); monthlyDisplayNonzero(residual) {
		summary("到账汇总舍入尾差（元）", residual, "仅显示舍入，不改变实际金额")
	}
	summary("到账合计", cr.KnownTotal, "")
	full("消费统计", 2, 29)
	row(28, func(r int) string {
		for _, a := range [][2]string{{"A", "B"}, {"C", "D"}, {"E", "F"}} {
			merge(fmt.Sprintf("%s%d", a[0], r), fmt.Sprintf("%s%d", a[1], r))
		}
		return text(fmt.Sprintf("A%d", r), 3, "总消费（元）") + text(fmt.Sprintf("C%d", r), 3, "请求次数") + text(fmt.Sprintf("E%d", r), 3, "总 Token 数")
	})
	row(42, func(r int) string {
		for _, a := range [][2]string{{"A", "B"}, {"C", "D"}, {"E", "F"}} {
			merge(fmt.Sprintf("%s%d", a[0], r), fmt.Sprintf("%s%d", a[1], r))
		}
		// Exact integer retained in the formula. Only the displayed unit is scaled.
		divisor, unit := int64(1), ""
		for _, x := range []struct {
			d int64
			u string
		}{{100000000, "亿"}, {10000, "万"}, {1000, "千"}} {
			if s.Tokens >= x.d {
				divisor, unit = x.d, x.u
				break
			}
		}
		tokenCell := number(fmt.Sprintf("E%d", r), 5, strconv.FormatInt(s.Tokens, 10))
		if s.TokensUnknown {
			tokenCell = text(fmt.Sprintf("E%d", r), 5, "—")
		} else if divisor > 1 {
			tokenCell = fmt.Sprintf(`<c r="E%d" s="%d"><f>%d/%d</f><v>%s</v></c>`, r, map[string]int{"亿": 9, "万": 10, "千": 11}[unit], s.Tokens, divisor, strconv.FormatFloat(float64(s.Tokens)/float64(divisor), 'f', 8, 64))
		}
		requestCell := number(fmt.Sprintf("C%d", r), 5, strconv.FormatInt(s.Requests, 10))
		if s.RequestsUnknown {
			requestCell = text(fmt.Sprintf("C%d", r), 5, "—")
		}
		return summaryMoney(fmt.Sprintf("A%d", r), 13, s.Amount) + requestCell + tokenCell
	})
	if s.Refund != "0.000000" {
		summary("退款金额（元）", s.Refund, "")
		if residual := monthlyDisplayResidual(s.Net, []monthlyDisplayTerm{{s.Amount, 1}, {s.Refund, -1}}); monthlyDisplayNonzero(residual) {
			summary("净消费舍入尾差（元）", residual, "仅显示舍入，不改变实际金额")
		}
		summary("净消费（元）", s.Net, "")
	}
	if cr.Adjustment != "0.000000" {
		summary("余额扣减调整（元）", cr.Adjustment, "")
	}
	if cr.BalanceCorrection != "" {
		summary("余额校正（元）", cr.BalanceCorrection, "独立余额核对调整，不计入充值或消费")
	}
	row(40, func(r int) string {
		merge(fmt.Sprintf("A%d", r), fmt.Sprintf("B%d", r))
		merge(fmt.Sprintf("C%d", r), fmt.Sprintf("F%d", r))
		return text(fmt.Sprintf("A%d", r), 16, "月末余额（元）") + summaryMoney(fmt.Sprintf("C%d", r), 14, displayClosing)
	})
	funding()
	var mxml strings.Builder
	for _, m := range merges {
		fmt.Fprintf(&mxml, `<mergeCell ref="%s"/>`, m)
	}
	sheet := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><dimension ref="A1:F%d"/><sheetViews><sheetView workbookViewId="0" showGridLines="0"/></sheetViews><cols><col min="1" max="1" width="9" customWidth="1"/><col min="2" max="2" width="23" customWidth="1"/><col min="3" max="3" width="20" customWidth="1"/><col min="4" max="4" width="25" customWidth="1"/><col min="5" max="6" width="20" customWidth="1"/></cols><sheetData>%s</sheetData><mergeCells count="%d">%s</mergeCells><printOptions horizontalCentered="1"/><pageMargins left="0.25" right="0.25" top="0.4" bottom="0.4" header="0.2" footer="0.2"/><pageSetup paperSize="9" orientation="landscape" fitToWidth="1" fitToHeight="0"/></worksheet>`, n, rows.String(), len(merges), mxml.String())
	files := [][2]string{
		{"[Content_Types].xml", `<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/></Types>`},
		{"_rels/.rels", `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`},
		{"xl/workbook.xml", `<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="月度账单" sheetId="1" r:id="rId1"/></sheets></workbook>`},
		{"xl/_rels/workbook.xml.rels", `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>`},
		{"xl/styles.xml", monthlyXLSXStyles}, {"xl/worksheets/sheet1.xml", sheet},
	}
	if p.MemberDetailsVersion == 1 || p.MemberDetailsVersion == 2 {
		files[0][1] = strings.Replace(files[0][1], "</Types>", `<Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>`, 1)
		files[2][1] = strings.Replace(files[2][1], "</sheets>", `<sheet name="账号收支" sheetId="2" r:id="rId3"/></sheets>`, 1)
		files[3][1] = strings.Replace(files[3][1], "</Relationships>", `<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/></Relationships>`, 1)
		files = append(files, [2]string{"xl/worksheets/sheet2.xml", monthlyMemberSheet(p)})
	}
	if p.TokenDetailsVersion == 1 {
		files[0][1] = strings.Replace(files[0][1], "</Types>", `<Override PartName="/xl/worksheets/sheet3.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>`, 1)
		files[2][1] = strings.Replace(files[2][1], "</sheets>", `<sheet name="令牌消费" sheetId="3" r:id="rId4"/></sheets>`, 1)
		files[3][1] = strings.Replace(files[3][1], "</Relationships>", `<Relationship Id="rId4" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet3.xml"/></Relationships>`, 1)
		files = append(files, [2]string{"xl/worksheets/sheet3.xml", monthlyTokenSheet(p)})
	}
	var buf bytes.Buffer
	z := zip.NewWriter(&buf)
	for _, f := range files {
		w, err := z.Create(f[0])
		if err != nil {
			return nil, err
		}
		if _, err = w.Write([]byte(f[1])); err != nil {
			return nil, err
		}
	}
	if err := z.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func monthlyTokenSheet(p monthlyStatement) string {
	var rows strings.Builder
	// Individually balanced users/tokens can still produce a nonzero net
	// rounding bridge after organization aggregation. The flat export must
	// reserve its H column for that organization residual row as well.
	hasNetTail := p.Consumption != nil && monthlyDisplayNonzero(monthlyNetDisplayResidual(*p.Consumption))
	for _, m := range p.Members {
		hasNetTail = hasNetTail || monthlyDisplayNonzero(monthlyNetDisplayResidual(m.Consumption))
		for _, r := range m.TokenConsumption {
			hasNetTail = hasNetTail || monthlyDisplayNonzero(monthlyNetDisplayResidual(monthlyConsumption{Amount: r.Amount, Refund: r.Refund, Net: r.Net}))
		}
	}
	lastCol := 'G'
	if hasNetTail {
		lastCol = 'H'
	}
	text := func(ref string, style int, value string) string {
		return fmt.Sprintf(`<c r="%s" s="%d" t="inlineStr"><is><t xml:space="preserve">%s</t></is></c>`, ref, style, monthlyXML(value))
	}
	fmt.Fprintf(&rows, `<row r="1" ht="36" customHeight="1">%s</row>`, text("A1", 1, p.Account+" · 令牌消费 · "+p.Month))
	fmt.Fprintf(&rows, `<row r="2" ht="42" customHeight="1">%s</row>`, text("A2", 0, "按用户 ID 和令牌 ID 独立汇总；同名不合并。历史未记录令牌的消费单独列示，计入用户合计。每位用户的总消费见“账号收支”，本表仅列令牌分项，避免加总时重复。"))
	rows.WriteString(`<row r="3" ht="30" customHeight="1">`)
	titles := []string{"用户", "用户 ID", "令牌名称", "令牌 ID", "消费（元）", "退款（元）", "净消费（元）"}
	if hasNetTail {
		titles = append(titles, "净额舍入尾差（元）")
	}
	for i, title := range titles {
		rows.WriteString(text(fmt.Sprintf("%c3", 'A'+i), 3, title))
	}
	rows.WriteString(`</row>`)
	n := 3
	tail := func(name, userID, label string, values []string) {
		nonzero := false
		for _, value := range values {
			nonzero = nonzero || monthlyDisplayNonzero(value)
		}
		if !nonzero {
			return
		}
		n++
		fmt.Fprintf(&rows, `<row r="%d" ht="30" customHeight="1">`, n)
		for i, value := range []string{name, userID, label, "—"} {
			rows.WriteString(text(fmt.Sprintf("%c%d", 'A'+i, n), 0, value))
		}
		for i, value := range values {
			rows.WriteString(monthlyMoneyCell(fmt.Sprintf("%c%d", 'E'+i, n), 4, value))
		}
		if hasNetTail {
			rows.WriteString(monthlyMoneyCell(fmt.Sprintf("H%d", n), 4, monthlyDisplayNetColumnResidual(values)))
		}
		rows.WriteString(`</row>`)
	}
	var accountParts []monthlyConsumption
	for _, m := range p.Members {
		name := m.Username
		if name == "" {
			name = "—"
		}
		// No subtotals in this flat sheet. Explicit display-only residual rows
		// make SUM(E:E) equal the same cents shown in the organization summary.
		var tokenParts []monthlyConsumption
		for _, token := range m.TokenConsumption {
			tokenParts = append(tokenParts, monthlyConsumption{Amount: token.Amount, Refund: token.Refund, Net: token.Net})
			n++
			id := strconv.FormatInt(token.TokenID, 10)
			if token.TokenID == 0 {
				id = "—"
			}
			height := max(monthlyTextRowHeight(name, 22, 30), monthlyTextRowHeight(token.Name, 24, 30))
			fmt.Fprintf(&rows, `<row r="%d" ht="%d" customHeight="1">`, n, height)
			for i, value := range []string{name, strconv.FormatInt(m.UserID, 10), token.Name, id} {
				rows.WriteString(text(fmt.Sprintf("%c%d", 'A'+i, n), 0, value))
			}
			for i, value := range []string{token.Amount, token.Refund, token.Net} {
				rows.WriteString(monthlyMoneyCell(fmt.Sprintf("%c%d", 'E'+i, n), 4, value))
			}
			if hasNetTail {
				rows.WriteString(monthlyMoneyCell(fmt.Sprintf("H%d", n), 4, monthlyNetDisplayResidual(monthlyConsumption{Amount: token.Amount, Refund: token.Refund, Net: token.Net})))
			}
			rows.WriteString(`</row>`)
		}
		tail(name, strconv.FormatInt(m.UserID, 10), "分项舍入尾差", monthlyConsumptionDisplayResiduals(m.Consumption, tokenParts))
		accountParts = append(accountParts, m.Consumption)
	}
	if p.Consumption != nil {
		tail("组织汇总", "—", "组织汇总舍入尾差", monthlyConsumptionDisplayResiduals(*p.Consumption, accountParts))
	}
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><dimension ref="A1:%c%d"/><sheetViews><sheetView workbookViewId="0" showGridLines="0"><pane ySplit="3" topLeftCell="A4" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews><cols><col min="1" max="1" width="26" customWidth="1"/><col min="2" max="2" width="18" customWidth="1"/><col min="3" max="3" width="30" customWidth="1"/><col min="4" max="%d" width="22" customWidth="1"/></cols><sheetData>%s</sheetData><mergeCells count="2"><mergeCell ref="A1:%c1"/><mergeCell ref="A2:%c2"/></mergeCells><pageMargins left="0.25" right="0.25" top="0.4" bottom="0.4" header="0.2" footer="0.2"/><pageSetup paperSize="9" orientation="landscape" fitToWidth="1" fitToHeight="0"/></worksheet>`, lastCol, n, len(titles), rows.String(), lastCol, lastCol)
}

// Shares the frozen DTO and numeric styles with the summary. No source query,
// wallet mutation, hidden adjustment or independently recomputed customer total.
func monthlyMemberSheet(p monthlyStatement) string {
	var rows strings.Builder
	hasCorrection, hasRounding, totals, residuals := monthlyMemberDisplayFooters(p)
	frozenMembers := monthlyDisplayMemberIndex(p)
	cell := func(ref string, style int, value string) string {
		return fmt.Sprintf(`<c r="%s" s="%d" t="inlineStr"><is><t xml:space="preserve">%s</t></is></c>`, ref, style, monthlyXML(value))
	}
	fmt.Fprintf(&rows, `<row r="1" ht="36" customHeight="1">%s</row>`, cell("A1", 1, p.Account+" · 账号收支 · "+p.Month))
	note := "各账号独立核算：月初余额 + 到账 + 余额扣减 − 消费 + 退款"
	if hasCorrection {
		note += " + 余额校正"
	}
	if hasRounding {
		note += " + 舍入尾差"
	}
	note += " = 月末余额。"
	fmt.Fprintf(&rows, `<row r="2" ht="38" customHeight="1">%s</row>`, cell("A2", 0, note))
	rows.WriteString(`<row r="3" ht="30" customHeight="1">`)
	titles := []string{"账号", "月初余额（元）", "到账（含赠送）（元）", "余额扣减（元）", "消费（元）", "退款（元）"}
	if hasCorrection {
		titles = append(titles, "余额校正（元）")
	}
	if hasRounding {
		titles = append(titles, "舍入尾差（元）")
	}
	titles = append(titles, "月末余额（元）")
	lastCol := rune('A' + len(titles) - 1)
	for i, title := range titles {
		rows.WriteString(cell(fmt.Sprintf("%c3", 'A'+i), 3, title))
	}
	rows.WriteString(`</row>`)
	for i, member := range p.Members {
		r := i + 4
		name := member.Username
		if name == "" {
			name = "—"
		}
		name += "\nID " + strconv.FormatInt(member.UserID, 10)
		fmt.Fprintf(&rows, `<row r="%d" ht="%d" customHeight="1">%s`, r, monthlyTextRowHeight(name, 25, 42), cell(fmt.Sprintf("A%d", r), 0, name))
		amounts := monthlyMemberDisplayAmounts(member, hasCorrection, hasRounding, frozenMembers[member.UserID])
		for j, amount := range amounts {
			rows.WriteString(monthlyMoneyCell(fmt.Sprintf("%c%d", 'B'+j, r), 4, amount))
		}
		rows.WriteString(`</row>`)
	}
	n := len(p.Members) + 3
	if len(p.Members) > 1 && len(totals) > 0 {
		footer := func(label string, values []string) {
			n++
			fmt.Fprintf(&rows, `<row r="%d" ht="30" customHeight="1">%s`, n, cell(fmt.Sprintf("A%d", n), 0, label))
			for i, value := range values {
				rows.WriteString(monthlyMoneyCell(fmt.Sprintf("%c%d", 'B'+i, n), 4, value))
			}
			rows.WriteString(`</row>`)
		}
		nonzero := false
		for _, value := range residuals {
			nonzero = nonzero || monthlyDisplayNonzero(value)
		}
		if nonzero {
			footer("分项舍入尾差", residuals)
		}
		footer("组织合计", totals)
	}
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetPr><pageSetUpPr fitToPage="1"/></sheetPr><dimension ref="A1:%c%d"/><sheetViews><sheetView workbookViewId="0" showGridLines="0"><pane ySplit="3" topLeftCell="A4" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews><cols><col min="1" max="1" width="28" customWidth="1"/><col min="2" max="%d" width="23" customWidth="1"/></cols><sheetData>%s</sheetData><mergeCells count="2"><mergeCell ref="A1:%c1"/><mergeCell ref="A2:%c2"/></mergeCells><pageMargins left="0.25" right="0.25" top="0.4" bottom="0.4" header="0.2" footer="0.2"/><pageSetup paperSize="9" orientation="landscape" fitToWidth="1" fitToHeight="0"/></worksheet>`, lastCol, n, len(titles), rows.String(), lastCol, lastCol)
}

// Keep the account name and immutable ID visible in non-merged cells. OOXML
// fixed heights otherwise clip wrapped names. Use a conservative character
// width estimate (wide characters occupy two units) and Excel's row-height cap.
func monthlyTextRowHeight(value string, width, minimum int) int {
	lines := 0
	for _, line := range strings.Split(value, "\n") {
		units := 0
		for _, r := range line {
			units++
			if r > 127 {
				units++
			}
		}
		lines += max(1, (units+width-1)/width)
	}
	return min(409, max(minimum, lines*15+10))
}

func newMonthlyMoney(v string) (int64, bool) {
	if strings.TrimSpace(v) != v || strings.Count(v, ".") != 1 {
		return 0, false
	}
	a := strings.Split(v, ".")
	if len(a[1]) != 6 {
		return 0, false
	}
	n := strings.TrimPrefix(a[0], "-")
	if n == "" {
		return 0, false
	}
	for _, r := range n + a[1] {
		if r > '9' || !unicode.IsDigit(r) {
			return 0, false
		}
	}
	q, ok := parseMonthlyQuota("$" + strings.TrimPrefix(v, "-"))
	if strings.HasPrefix(v, "-") {
		q = -q
	}
	return q, ok
}

const monthlyXLSXStyles = `<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<numFmts count="5"><numFmt numFmtId="164" formatCode="&quot;¥&quot;#,##0.00;&quot;¥-&quot;#,##0.00;&quot;¥&quot;0.00"/><numFmt numFmtId="165" formatCode="0.00&quot; 亿&quot;"/><numFmt numFmtId="166" formatCode="0.00&quot; 万&quot;"/><numFmt numFmtId="167" formatCode="0.00&quot; 千&quot;"/><numFmt numFmtId="168" formatCode="&quot;¥&quot;#,##0.00;&quot;¥-&quot;#,##0.00;&quot;¥&quot;0.00"/></numFmts>
<fonts count="4"><font><sz val="11"/><color rgb="FF213547"/><name val="Arial"/></font><font><b/><sz val="16"/><color rgb="FFFFFFFF"/><name val="Arial"/></font><font><b/><sz val="11"/><color rgb="FF204F78"/><name val="Arial"/></font><font><b/><sz val="14"/><color rgb="FF204F78"/><name val="Arial"/></font></fonts>
<fills count="5"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill><fill><patternFill patternType="solid"><fgColor rgb="FF204F78"/></patternFill></fill><fill><patternFill patternType="solid"><fgColor rgb="FFF1F5F9"/></patternFill></fill><fill><patternFill patternType="solid"><fgColor rgb="FFF1F5F9"/></patternFill></fill></fills>
<borders count="2"><border><left/><right/><top/><bottom/><diagonal/></border><border><left style="thin"><color rgb="FFDCE3EB"/></left><right style="thin"><color rgb="FFDCE3EB"/></right><top style="thin"><color rgb="FFDCE3EB"/></top><bottom style="thin"><color rgb="FFDCE3EB"/></bottom><diagonal/></border></borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="17">
<xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyAlignment="1"><alignment vertical="center" wrapText="1"/></xf>
<xf numFmtId="0" fontId="1" fillId="2" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="0" fontId="2" fillId="3" borderId="1" xfId="0" applyAlignment="1"><alignment vertical="center"/></xf>
<xf numFmtId="0" fontId="2" fillId="4" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="164" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="right" vertical="center"/></xf>
<xf numFmtId="3" fontId="3" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="164" fontId="3" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="1" fontId="0" fillId="0" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="164" fontId="1" fillId="2" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="right" vertical="center"/></xf>
<xf numFmtId="165" fontId="3" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="166" fontId="3" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="167" fontId="3" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="168" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="right" vertical="center"/></xf>
<xf numFmtId="168" fontId="3" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="168" fontId="3" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="right" vertical="center"/></xf>
<xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyAlignment="1"><alignment vertical="center" wrapText="1"/></xf>
<xf numFmtId="0" fontId="2" fillId="0" borderId="1" xfId="0" applyAlignment="1"><alignment vertical="center" wrapText="1"/></xf>
</cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles></styleSheet>`
