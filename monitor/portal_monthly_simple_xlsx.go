package monitor

// Customer workbook for bills issued by the simple monthly generator. The
// layout follows the statement the business team sends to customers (sheet
// order 账号收支 / 月度账单 / 令牌消费, fixed labels and cell positions), so
// a downloaded bill can be forwarded as-is.
//
// Every money cell is a cent value taken from the frozen publication; the few
// formulas only re-derive totals already present in the publication, and the
// renderer refuses a bill whose shown values do not satisfy them exactly.

import (
	"archive/zip"
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const simpleBillReviewer = "simple-monthly-generator"

type simpleXLSXSheet struct {
	name string
	xml  string
}

// simpleCents parses a publication money string ("123.450000") into cents.
func simpleBillCents(v string) (int64, error) {
	q, ok := newMonthlyMoney(v)
	if !ok || q%simpleCent != 0 {
		return 0, errors.New("invalid bill money")
	}
	return q / simpleCent, nil
}

func simpleBillCentText(c int64) string {
	sign := ""
	if c < 0 {
		sign, c = "-", -c
	}
	return fmt.Sprintf("%s%d.%02d", sign, c/100, c%100)
}

func simpleAccountLabel(name string, id int64) string {
	if name == "" {
		name = "—"
	}
	return name + "\nID " + strconv.FormatInt(id, 10)
}

// simpleCreditType maps a credit row to the 操作类型 / 备注 columns.
func simpleCreditType(c monthlyCredit) (string, string) {
	switch c.Kind {
	case "gift":
		return "赠送", c.Method
	case "adjustment":
		return "扣减", c.Method
	default:
		return "充值", c.Method
	}
}

type simpleMemberCents struct {
	opening, credit, adjustment, consume, refund, closing int64
	subConsume, subRefund                                 int64 // paid by a subscription; not in the balance
}

// simpleSubIndex looks up subscription-paid usage per account and token.
type simpleSubIndex struct {
	users  map[int64]simpleSubscriptionUser
	tokens map[[2]int64]simpleSubscriptionToken
	any    bool
}

func newSimpleSubIndex(sub *simpleSubscriptionBill) simpleSubIndex {
	ix := simpleSubIndex{users: map[int64]simpleSubscriptionUser{}, tokens: map[[2]int64]simpleSubscriptionToken{}}
	if sub == nil {
		return ix
	}
	for _, u := range sub.Users {
		ix.users[u.UserID] = u
		for _, t := range u.Tokens {
			ix.tokens[[2]int64{u.UserID, t.TokenID}] = t
		}
		ix.any = true
	}
	return ix
}

func simpleStatementXLSX(p monthlyStatement, sub *simpleSubscriptionBill) ([]byte, error) {
	if !p.Published || p.MonthOpeningBalance == nil || p.MonthEndBalance == nil || p.Consumption == nil ||
		p.Credits.RechargeTotal == nil || p.Credits.GiftTotal == nil || p.Credits.RechargeCount == nil || len(p.Members) == 0 {
		return nil, errors.New("未发布或不完整的账单不可导出")
	}
	ix := newSimpleSubIndex(sub)
	cr, cs := p.Credits, *p.Consumption
	var err error
	cent := func(v string) int64 {
		c, e := simpleBillCents(v)
		if e != nil && err == nil {
			err = e
		}
		return c
	}
	opening, closing := cent(*p.MonthOpeningBalance), cent(*p.MonthEndBalance)
	if p.DisplayBalances != nil {
		opening, closing = cent(p.DisplayBalances.Organization.Opening), cent(p.DisplayBalances.Organization.Closing)
	}
	recharge, gift, adjustment := cent(*cr.RechargeTotal), cent(*cr.GiftTotal), cent(cr.Adjustment)
	amount, refund := cent(cs.Amount), cent(cs.Refund)
	display := map[int64][2]int64{}
	if p.DisplayBalances != nil {
		for _, m := range p.DisplayBalances.Members {
			display[m.UserID] = [2]int64{cent(m.Opening), cent(m.Closing)}
		}
	}
	members := make([]simpleMemberCents, len(p.Members))
	var sum simpleMemberCents
	for i, m := range p.Members {
		mc := simpleMemberCents{opening: cent(m.Opening), credit: cent(m.CreditTotal), adjustment: cent(m.Adjustment),
			consume: cent(m.Consumption.Amount), refund: cent(m.Consumption.Refund), closing: cent(m.Closing)}
		if d, ok := display[m.UserID]; ok {
			mc.opening, mc.closing = d[0], d[1]
		}
		su := ix.users[m.UserID]
		mc.subConsume, mc.subRefund = su.ConsumeCents, su.RefundCents
		members[i] = mc
		sum.opening += mc.opening
		sum.credit += mc.credit
		sum.adjustment += mc.adjustment
		sum.consume += mc.consume
		sum.refund += mc.refund
		sum.closing += mc.closing
		sum.subConsume += mc.subConsume
		sum.subRefund += mc.subRefund
		if mc.opening+mc.credit+mc.adjustment-mc.consume+mc.refund != mc.closing {
			return nil, errors.New("account balance does not reconcile")
		}
	}
	var rowSum [3]int64 // recharge, gift, adjustment
	for _, r := range cr.Rows {
		c := cent(r.Amount)
		switch r.Kind {
		case "gift":
			rowSum[1] += c
		case "adjustment":
			rowSum[2] += c
		default:
			rowSum[0] += c
		}
	}
	if err != nil {
		return nil, err
	}
	// The workbook's formulas must reproduce the published values exactly.
	if opening+recharge+gift+adjustment-amount+refund != closing ||
		sum.opening != opening || sum.closing != closing || sum.consume != amount || sum.refund != refund ||
		sum.credit != recharge+gift || sum.adjustment != adjustment || rowSum != [3]int64{recharge, gift, adjustment} {
		return nil, errors.New("bill totals do not reconcile")
	}
	var sheets []simpleXLSXSheet
	if sub != nil && len(sub.Orders) > 0 {
		sheets = append(sheets, simpleXLSXSheet{"订阅服务账单", simpleSubscriptionSheet(p, sub)})
	}
	sheets = append(sheets,
		simpleXLSXSheet{"账号收支", simpleAccountsSheet(p, members, ix.any, len(sheets) == 0)},
		simpleXLSXSheet{"月度账单", simpleSummarySheet(p, opening, recharge, gift, adjustment, amount, refund, closing, sum.subConsume-sum.subRefund, ix)},
		simpleXLSXSheet{"令牌消费", simpleTokenSheet(p, ix)},
	)
	return simpleZipWorkbook(sheets)
}

// Cell styles of the customer-approved statement (index into the embedded
// styles.xml copied from that workbook). Every cell, including the blank
// cells inside a merged range, uses the style of the same cell there.
const (
	tplTitle       = 1  // dark title band
	tplSubtitle    = 2  // period line under a title
	tplHeader      = 3  // column header
	tplLabel       = 4  // summary label / plain text cell
	tplText        = 5  // centered text or integer
	tplMoney       = 6  // ¥ amount
	tplSection     = 7  // section band / total label
	tplTotalBlank  = 8  // blank cell inside a total label
	tplTotalMoney  = 9  // ¥ amount in a total row
	tplInfo        = 10 // organization line
	tplInfo2       = 11 // service / period line
	tplKnownLabel  = 12 // 到账合计 label
	tplKnownMoney  = 13 // 到账合计 amount
	tplMetricHead  = 14 // 总消费 / 请求次数 / 使用令牌数 header
	tplMetricMoney = 15 // 总消费 amount
	tplMetricInt   = 16 // request / token counts
	tplClosing     = 17 // 月末余额 amount
	tplFundingBand = 18 // 额度变动记录 band
	tplTime        = 19 // credit time
	tplAccount     = 20 // credit account (wrapped)
	tplHeaderSoft  = 21 // 账号收支 amount column headers
	tplAccountCell = 22 // 账号收支 account cell
)

//go:embed portal_monthly_simple_styles.xml
var simpleTemplateStyles string

type tplCell struct {
	col     rune
	style   int
	text    string
	num     string // literal number (already formatted)
	formula string
}

type tplSheet struct {
	rows   strings.Builder
	merges []string
	n      int
	width  rune
}

func (t *tplSheet) row(ht float64, cells ...tplCell) int {
	t.n++
	var b strings.Builder
	for _, c := range cells {
		ref := fmt.Sprintf("%c%d", c.col, t.n)
		switch {
		case c.formula != "":
			fmt.Fprintf(&b, `<c r="%s" s="%d"><f>%s</f><v>%s</v></c>`, ref, c.style, monthlyXML(c.formula), c.num)
		case c.num != "":
			fmt.Fprintf(&b, `<c r="%s" s="%d"><v>%s</v></c>`, ref, c.style, c.num)
		case c.text != "":
			fmt.Fprintf(&b, `<c r="%s" s="%d" t="inlineStr"><is><t xml:space="preserve">%s</t></is></c>`, ref, c.style, monthlyXML(c.text))
		default:
			fmt.Fprintf(&b, `<c r="%s" s="%d"/>`, ref, c.style)
		}
	}
	fmt.Fprintf(&t.rows, `<row r="%d" ht="%g" customHeight="1" spans="1:%d">%s</row>`, t.n, ht, t.width-'A'+1, b.String())
	return t.n
}

func (t *tplSheet) merge(a, b rune) {
	t.merges = append(t.merges, fmt.Sprintf("%c%d:%c%d", a, t.n, b, t.n))
}

// span returns the cells from..to: the first with content, the rest blank with
// the same style (as in the approved workbook).
func span(from, to rune, style int, first tplCell) []tplCell {
	first.col, first.style = from, style
	out := []tplCell{first}
	for c := from + 1; c <= to; c++ {
		out = append(out, tplCell{col: c, style: style})
	}
	return out
}

func cat(parts ...[]tplCell) []tplCell {
	var out []tplCell
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func tplMoneyNum(c int64) string { return simpleBillCentText(c) }

func (t *tplSheet) xml(widths []float64, frozen, selected bool) string {
	var cols strings.Builder
	for i, w := range widths {
		fmt.Fprintf(&cols, `<col min="%d" max="%d" width="%g" customWidth="1"/>`, i+1, i+1, w)
	}
	var m strings.Builder
	for _, x := range t.merges {
		fmt.Fprintf(&m, `<mergeCell ref="%s"/>`, x)
	}
	view := `<sheetView showGridLines="0" workbookViewId="0">`
	if selected {
		view = `<sheetView showGridLines="0" tabSelected="1" workbookViewId="0">`
	}
	if frozen {
		view += `<pane ySplit="3" topLeftCell="A4" activePane="bottomLeft" state="frozen"/><selection/><selection pane="bottomLeft" activeCell="A4" sqref="A4"/>`
	} else {
		view += `<selection activeCell="A1" sqref="A1"/>`
	}
	view += `</sheetView>`
	merges := ""
	if len(t.merges) > 0 {
		merges = fmt.Sprintf(`<mergeCells count="%d">%s</mergeCells>`, len(t.merges), m.String())
	}
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheetPr><tabColor rgb="FF1F4E78"/></sheetPr><dimension ref="A1:%c%d"/><sheetViews>%s</sheetViews><sheetFormatPr defaultColWidth="9" defaultRowHeight="13.8"/><cols>%s</cols><sheetData>%s</sheetData>%s<pageMargins left="0.7" right="0.7" top="0.75" bottom="0.75" header="0.3" footer="0.3"/><headerFooter/></worksheet>`,
		t.width, max(t.n, 1), view, cols.String(), t.rows.String(), merges)
}

func simpleAccountsSheet(p monthlyStatement, members []simpleMemberCents, withSub, selected bool) string {
	titles := []string{"账号", "月初余额（元）", "充值总数（元）", "消费总数（元）", "月末余额（元）", "本月支付账单"}
	widths := []float64{17, 19, 23, 19, 18.7, 20.6}
	if withSub {
		titles = []string{"账号", "月初余额（元）", "充值总数（元）", "消费总数（元）", "其中套餐内（元）", "其中余额支付（元）", "月末余额（元）", "本月支付账单"}
		widths = []float64{17, 19, 19, 19, 19, 19, 18.7, 20.6}
	}
	last := rune('A' + len(titles) - 1)
	t := &tplSheet{width: last}
	// The approved workbook merges the title and period over A..E (not F).
	t.row(44, span('A', last, tplTitle, tplCell{text: p.Account + " · 账号收支 · " + p.Month})...)
	t.merge('A', last-1)
	t.row(32, cat(span('A', last-1, tplSubtitle, tplCell{text: "账单期间：" + p.From + " 至 " + p.To + "（北京时间）"}))...)
	t.merge('A', last-1)
	var hdr []tplCell
	for i, title := range titles {
		st := tplHeaderSoft
		if i < 2 {
			st = tplHeader
		}
		hdr = append(hdr, tplCell{col: rune('A' + i), style: st, text: title})
	}
	t.row(38, hdr...)
	totals := make([]int64, len(titles)-1)
	for i, m := range p.Members {
		mc := members[i]
		wallet := mc.consume - mc.refund
		values := []int64{mc.opening, mc.credit + mc.adjustment, wallet, mc.closing, wallet}
		if withSub {
			subNet := mc.subConsume - mc.subRefund
			values = []int64{mc.opening, mc.credit + mc.adjustment, wallet + subNet, subNet, wallet, mc.closing, wallet}
		}
		cells := []tplCell{{col: 'A', style: tplAccountCell, text: simpleAccountLabel(m.Username, m.UserID)}}
		for j, v := range values {
			st := tplMoney
			if j == len(values)-1 {
				st = tplTotalMoney
			}
			cells = append(cells, tplCell{col: rune('B' + j), style: st, num: tplMoneyNum(v)})
			totals[j] += v
		}
		t.row(44, cells...)
	}
	if len(p.Members) > 1 {
		first, lastRow := 4, t.n
		cells := []tplCell{{col: 'A', style: tplSection, text: "合计"}}
		for j, v := range totals {
			col := rune('B' + j)
			cells = append(cells, tplCell{col: col, style: tplTotalMoney, num: tplMoneyNum(v), formula: fmt.Sprintf("ROUND(SUM(%c%d:%c%d),2)", col, first, col, lastRow)})
		}
		t.row(35, cells...)
	}
	return t.xml(widths, true, selected)
}

func simpleSummarySheet(p monthlyStatement, opening, recharge, gift, adjustment, amount, refund, closing, subNet int64, ix simpleSubIndex) string {
	withSub := ix.any
	cr, cs := p.Credits, *p.Consumption
	t := &tplSheet{width: 'F'}
	t.row(44, span('A', 'F', tplTitle, tplCell{text: p.Account + " · 月度消费账单"})...)
	t.merge('A', 'F')
	t.row(30, cat(span('A', 'B', tplInfo, tplCell{text: "组织名称"}), span('C', 'F', tplInfo, tplCell{text: p.Account}))...)
	t.merge('A', 'B')
	t.merge('C', 'F')
	t.row(36, cat(span('A', 'B', tplInfo2, tplCell{text: "服务类型"}), []tplCell{{col: 'C', style: tplInfo2, text: "MaaS 云计算服务"}, {col: 'D', style: tplInfo2, text: "账单期间（北京时间）"}},
		span('E', 'F', tplInfo2, tplCell{text: p.From + " 至 " + p.To}))...)
	t.merge('A', 'B')
	t.merge('E', 'F')
	section := func(title string, style int, ht float64) {
		t.row(ht, span('A', 'F', style, tplCell{text: title})...)
		t.merge('A', 'F')
	}
	line := func(label string, labelStyle, valueStyle int, value tplCell, ht float64) int {
		r := t.row(ht, cat(span('A', 'C', labelStyle, tplCell{text: label}), span('D', 'F', valueStyle, value))...)
		t.merge('A', 'C')
		t.merge('D', 'F')
		return r
	}
	money := func(c int64) tplCell { return tplCell{num: tplMoneyNum(c)} }
	section("本月收支汇总", tplSection, 32)
	rOpen := line("月初余额（元）", tplLabel, tplMoney, money(opening), 29)
	rRecharge := line("充值合计（"+strconv.Itoa(*cr.RechargeCount)+" 次）", tplLabel, tplMoney, money(recharge), 29)
	rGift := line("赠送合计", tplLabel, tplMoney, money(gift), 29)
	rKnown := line("到账合计", tplKnownLabel, tplKnownMoney, tplCell{num: tplMoneyNum(recharge + gift), formula: fmt.Sprintf("ROUND(SUM(D%d:D%d),2)", rRecharge, rGift)}, 29)
	section("消费统计", tplSection, 32)
	t.row(29, cat(span('A', 'B', tplMetricHead, tplCell{text: "总消费（元）"}), span('C', 'D', tplMetricHead, tplCell{text: "请求次数"}), span('E', 'F', tplMetricHead, tplCell{text: "使用令牌数"}))...)
	t.merge('A', 'B')
	t.merge('C', 'D')
	t.merge('E', 'F')
	var union []simpleTokenLine
	if withSub {
		union = simpleTokenUnion(p, &ix)
	} else {
		union = simpleTokenUnion(p, nil)
	}
	total := amount
	if withSub {
		total = amount + subNet
	}
	rAmount := t.row(44, cat(span('A', 'B', tplMetricMoney, money(total)), span('C', 'D', tplMetricInt, tplCell{num: strconv.FormatInt(cs.Requests, 10)}),
		span('E', 'F', tplMetricInt, tplCell{num: strconv.Itoa(len(union))}))...)
	t.merge('A', 'B')
	t.merge('C', 'D')
	t.merge('E', 'F')
	walletRef := fmt.Sprintf("A%d", rAmount)
	if withSub {
		line("其中 套餐内（元）", tplLabel, tplMoney, money(subNet), 29)
		walletRef = fmt.Sprintf("D%d", line("其中 余额支付（元）", tplLabel, tplMoney, money(amount), 29))
	}
	rRefund := line("消费退款（元）", tplLabel, tplMoney, money(refund), 29)
	rAdj := line("余额调整（元）", tplLabel, tplMoney, money(adjustment), 29)
	line("月末余额（元）", tplSection, tplClosing, tplCell{num: tplMoneyNum(closing), formula: fmt.Sprintf("ROUND(D%d+D%d+D%d-%s+D%d,2)", rOpen, rKnown, rAdj, walletRef, rRefund)}, 38)
	section("额度变动记录", tplFundingBand, 32)
	var hdr []tplCell
	for i, title := range []string{"序号", "时间（北京时间）", "被操作账号", "金额（元）", "操作类型", "备注"} {
		hdr = append(hdr, tplCell{col: rune('A' + i), style: tplHeader, text: title})
	}
	t.row(32, hdr...)
	for i, c := range cr.Rows {
		typ, note := simpleCreditType(c)
		cents, _ := simpleBillCents(c.Amount)
		t.row(38, tplCell{col: 'A', style: tplText, num: strconv.Itoa(i + 1)},
			tplCell{col: 'B', style: tplTime, text: time.Unix(c.At, 0).In(usageCST).Format("2006-01-02 15:04:05")},
			tplCell{col: 'C', style: tplAccount, text: simpleAccountLabel(c.Username, c.UserID)},
			tplCell{col: 'D', style: tplMoney, num: tplMoneyNum(cents)},
			tplCell{col: 'E', style: tplText, text: typ},
			tplCell{col: 'F', style: tplText, text: note})
	}
	if len(cr.Rows) == 0 {
		t.row(38, span('A', 'F', tplText, tplCell{text: "本月无额度变动"})...)
		t.merge('A', 'F')
	}
	return t.xml([]float64{7, 23, 19, 23, 18, 19}, false, false)
}

type simpleTokenLine struct {
	userID             int64
	user, name         string
	tokenID            int64
	wallet, subConsume int64
	refund, subRefund  int64
}

// simpleTokenUnion lists every token with wallet or subscription usage, in
// member order and then the publication's token order.
func simpleTokenUnion(p monthlyStatement, ix *simpleSubIndex) []simpleTokenLine {
	var out []simpleTokenLine
	for _, m := range p.Members {
		name := m.Username
		if name == "" {
			name = "—"
		}
		seen := map[int64]bool{}
		for _, t := range m.TokenConsumption {
			c, _ := simpleBillCents(t.Amount)
			f, _ := simpleBillCents(t.Refund)
			l := simpleTokenLine{userID: m.UserID, user: name, name: t.Name, tokenID: t.TokenID, wallet: c, refund: f}
			if ix != nil {
				st := ix.tokens[[2]int64{m.UserID, t.TokenID}]
				l.subConsume, l.subRefund = st.ConsumeCents, st.RefundCents
			}
			seen[t.TokenID] = true
			out = append(out, l)
		}
		if ix != nil {
			for _, st := range ix.users[m.UserID].Tokens {
				if !seen[st.TokenID] {
					out = append(out, simpleTokenLine{userID: m.UserID, user: name, name: st.Name, tokenID: st.TokenID, subConsume: st.ConsumeCents, subRefund: st.RefundCents})
				}
			}
		}
	}
	return out
}

func simpleTokenSheet(p monthlyStatement, ix simpleSubIndex) string {
	titles := []string{"用户", "用户 ID", "令牌名称", "令牌 ID", "消费（元）", "退款（元）", "净消费（元）"}
	widths := []float64{17, 12, 25, 12, 20, 18, 20}
	if ix.any {
		titles = []string{"用户", "用户 ID", "令牌名称", "令牌 ID", "套餐内消费（元）", "余额支付（元）", "退款（元）", "净消费（元）"}
		widths = []float64{17, 12, 25, 12, 19, 19, 16, 19}
	}
	last := rune('A' + len(titles) - 1)
	t := &tplSheet{width: last}
	t.row(44, span('A', last, tplTitle, tplCell{text: p.Account + " · 令牌消费 · " + p.Month})...)
	t.merge('A', last)
	t.row(32, span('A', last, tplSubtitle, tplCell{text: "账单期间：" + p.From + " 至 " + p.To + "（北京时间）"})...)
	t.merge('A', last)
	var hdr []tplCell
	for i, title := range titles {
		hdr = append(hdr, tplCell{col: rune('A' + i), style: tplHeader, text: title})
	}
	t.row(34, hdr...)
	var lines []simpleTokenLine
	if ix.any {
		lines = simpleTokenUnion(p, &ix)
	} else {
		lines = simpleTokenUnion(p, nil)
	}
	totals := make([]int64, len(titles)-4)
	for _, l := range lines {
		id := strconv.FormatInt(l.tokenID, 10)
		label := l.name
		idCell := tplCell{col: 'D', style: tplText, num: id}
		if l.tokenID == 0 {
			label = "历史未记录令牌"
			idCell = tplCell{col: 'D', style: tplText, text: "—"}
		}
		cells := []tplCell{{col: 'A', style: tplLabel, text: l.user}, {col: 'B', style: tplText, num: strconv.FormatInt(l.userID, 10)},
			{col: 'C', style: tplLabel, text: label}, idCell}
		r := t.n + 1
		var values []int64
		if ix.any {
			// Same rule as the summary: subscription usage net of its own
			// refunds, 退款 = balance refund only.
			subNet := l.subConsume - l.subRefund
			values = []int64{subNet, l.wallet, l.refund, subNet + l.wallet - l.refund}
			cells = append(cells, tplCell{col: 'E', style: tplMoney, num: tplMoneyNum(values[0])}, tplCell{col: 'F', style: tplMoney, num: tplMoneyNum(values[1])},
				tplCell{col: 'G', style: tplMoney, num: tplMoneyNum(values[2])}, tplCell{col: 'H', style: tplMoney, num: tplMoneyNum(values[3]), formula: fmt.Sprintf("ROUND(E%d+F%d-G%d,2)", r, r, r)})
		} else {
			values = []int64{l.wallet, l.refund, l.wallet - l.refund}
			cells = append(cells, tplCell{col: 'E', style: tplMoney, num: tplMoneyNum(values[0])}, tplCell{col: 'F', style: tplMoney, num: tplMoneyNum(values[1])},
				tplCell{col: 'G', style: tplMoney, num: tplMoneyNum(values[2]), formula: fmt.Sprintf("ROUND(E%d-F%d,2)", r, r)})
		}
		for j, v := range values {
			totals[j] += v
		}
		t.row(32, cells...)
	}
	first, lastRow := 4, t.n
	cells := span('A', 'D', tplTotalBlank, tplCell{})
	cells[0] = tplCell{col: 'A', style: tplSection, text: "合计"}
	for j, v := range totals {
		col := rune('E' + j)
		c := tplCell{col: col, style: tplTotalMoney, num: tplMoneyNum(v)}
		if lastRow >= first {
			c.formula = fmt.Sprintf("ROUND(SUM(%c%d:%c%d),2)", col, first, col, lastRow)
		}
		cells = append(cells, c)
	}
	t.row(35, cells...)
	t.merge('A', 'D')
	return t.xml(widths, true, false)
}

// simpleSubscriptionSheet follows the subscription statement sent to
// subscription customers: the subscriptions opened in this month.
func simpleSubscriptionSheet(p monthlyStatement, sub *simpleSubscriptionBill) string {
	names := map[int64]string{}
	for _, m := range p.Members {
		names[m.UserID] = m.Username
	}
	var accounts []string
	seen := map[int64]bool{}
	for _, o := range sub.Orders {
		if !seen[o.UserID] {
			seen[o.UserID] = true
			accounts = append(accounts, names[o.UserID])
		}
	}
	t := &tplSheet{width: 'G'}
	t.row(44, span('A', 'G', tplTitle, tplCell{text: "订阅服务账单"})...)
	t.merge('A', 'G')
	t.row(30, span('A', 'G', tplInfo, tplCell{text: "客户组织（平台登记）：" + p.Account + "    使用账号：" + strings.Join(accounts, "、")})...)
	t.merge('A', 'G')
	t.row(32, span('A', 'G', tplSection, tplCell{text: "订购明细    金额单位：人民币元；时间：北京时间"})...)
	t.merge('A', 'G')
	var hdr []tplCell
	for i, title := range []string{"订阅记录编号", "订阅服务名称", "数量（张）", "金额（元）", "服务开始时间", "服务到期时间", "服务状态"} {
		hdr = append(hdr, tplCell{col: rune('A' + i), style: tplHeader, text: title})
	}
	t.row(32, hdr...)
	first := t.n + 1
	var qty, total int64
	for _, o := range sub.Orders {
		status := "已到期"
		if o.Active {
			status = "服务中"
		}
		name := o.Name
		if o.Trial {
			name += "（试用）"
		}
		t.row(38, tplCell{col: 'A', style: tplText, text: fmt.Sprintf("SUB-%d-%06d", o.UserID, o.ID)}, tplCell{col: 'B', style: tplLabel, text: name},
			tplCell{col: 'C', style: tplText, num: strconv.FormatInt(o.Quantity, 10)}, tplCell{col: 'D', style: tplMoney, num: tplMoneyNum(o.PriceCents)},
			tplCell{col: 'E', style: tplTime, text: time.Unix(o.Start, 0).In(usageCST).Format("2006-01-02 15:04:05")},
			tplCell{col: 'F', style: tplTime, text: time.Unix(o.End, 0).In(usageCST).Format("2006-01-02 15:04:05")}, tplCell{col: 'G', style: tplText, text: status})
		qty += o.Quantity
		total += o.PriceCents
	}
	lastRow := t.n
	t.row(35, tplCell{col: 'A', style: tplSection, text: "合计"}, tplCell{col: 'B', style: tplTotalBlank},
		tplCell{col: 'C', style: tplTotalMoney, num: strconv.FormatInt(qty, 10), formula: fmt.Sprintf("SUM(C%d:C%d)", first, lastRow)},
		tplCell{col: 'D', style: tplTotalMoney, num: tplMoneyNum(total), formula: fmt.Sprintf("ROUND(SUM(D%d:D%d),2)", first, lastRow)},
		tplCell{col: 'E', style: tplTotalBlank}, tplCell{col: 'F', style: tplTotalBlank}, tplCell{col: 'G', style: tplTotalBlank})
	t.merge('A', 'B')
	t.row(32, span('A', 'G', tplSubtitle, tplCell{text: "合计金额大写：人民币" + simpleChineseAmount(total)})...)
	t.merge('A', 'G')
	return t.xml([]float64{20, 26, 12, 16, 21, 21, 12}, false, true)
}

// simpleChineseAmount writes a non-negative cent amount in Chinese capital
// numerals as used on invoices, e.g. 1000000 -> 壹万元整, 12345 -> 壹佰贰拾叁元肆角伍分.
func simpleChineseAmount(cents int64) string {
	if cents <= 0 {
		return "零元整"
	}
	digits := []string{"零", "壹", "贰", "叁", "肆", "伍", "陆", "柒", "捌", "玖"}
	units := []string{"", "拾", "佰", "仟"}
	groupUnits := []string{"", "万", "亿", "万亿"}
	yuan, jiao, fen := cents/100, cents/10%10, cents%10
	var b strings.Builder
	if yuan > 0 {
		var groups []int64
		for v := yuan; v > 0; v /= 10000 {
			groups = append(groups, v%10000)
		}
		pendingZero := false
		for gi := len(groups) - 1; gi >= 0; gi-- {
			g := groups[gi]
			for ui, div := 3, int64(1000); ui >= 0; ui, div = ui-1, div/10 {
				d := g / div % 10
				if d == 0 {
					pendingZero = pendingZero || b.Len() > 0
					continue
				}
				if pendingZero {
					b.WriteString("零")
					pendingZero = false
				}
				b.WriteString(digits[d] + units[ui])
			}
			if g > 0 {
				b.WriteString(groupUnits[gi])
			}
		}
		b.WriteString("元")
	}
	switch {
	case jiao == 0 && fen == 0:
		b.WriteString("整")
	case jiao == 0:
		if yuan > 0 {
			b.WriteString("零")
		}
		b.WriteString(digits[fen] + "分")
	case fen == 0:
		b.WriteString(digits[jiao] + "角整")
	default:
		b.WriteString(digits[jiao] + "角" + digits[fen] + "分")
	}
	return b.String()
}

func simpleZipWorkbook(sheets []simpleXLSXSheet) ([]byte, error) {
	var types, wbSheets, rels strings.Builder
	for i, s := range sheets {
		fmt.Fprintf(&types, `<Override PartName="/xl/worksheets/sheet%d.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`, i+1)
		fmt.Fprintf(&wbSheets, `<sheet name="%s" sheetId="%d" r:id="rId%d"/>`, monthlyXML(s.name), i+1, i+1)
		fmt.Fprintf(&rels, `<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet%d.xml"/>`, i+1, i+1)
	}
	files := [][2]string{
		{"[Content_Types].xml", `<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>` + types.String() + `</Types>`},
		{"_rels/.rels", `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`},
		{"xl/workbook.xml", `<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>` + wbSheets.String() + `</sheets><calcPr calcId="0" fullCalcOnLoad="1"/></workbook>`},
		{"xl/_rels/workbook.xml.rels", `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` + rels.String() + fmt.Sprintf(`<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>`, len(sheets)+1) + `</Relationships>`},
		{"xl/styles.xml", simpleTemplateStyles},
	}
	for i, s := range sheets {
		files = append(files, [2]string{fmt.Sprintf("xl/worksheets/sheet%d.xml", i+1), s.xml})
	}
	var buf bytes.Buffer
	z := zip.NewWriter(&buf)
	for _, f := range files {
		fw, err := z.Create(f[0])
		if err != nil {
			return nil, err
		}
		if _, err := fw.Write([]byte(f[1])); err != nil {
			return nil, err
		}
	}
	if err := z.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
