package monthlydraft

// Independent exact-rational audit of actual HTTP downloads. Expected figures
// come from retained source payloads, not the product's money/display helpers.
import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

type retainedSeriesConsumption struct{ Amount, Refund, Net string }
type retainedSeriesToken struct {
	TokenID int64  `json:"token_id"`
	Name    string `json:"name"`
	Amount  string `json:"amount"`
	Refund  string `json:"refund"`
	Net     string `json:"net"`
}
type retainedSeriesMember struct {
	UserID            int64                     `json:"user_id"`
	Username          string                    `json:"username"`
	Opening           string                    `json:"opening"`
	Closing           string                    `json:"closing"`
	CreditTotal       string                    `json:"credit_total"`
	Adjustment        string                    `json:"adjustment"`
	BalanceCorrection string                    `json:"balance_correction"`
	Consumption       retainedSeriesConsumption `json:"consumption"`
	Tokens            []retainedSeriesToken     `json:"token_consumption"`
}
type retainedSeriesView struct {
	Month   string `json:"month"`
	Opening string `json:"month_opening_balance"`
	Closing string `json:"month_end_balance"`
	Credits struct {
		Total      string `json:"known_total"`
		Adjustment string `json:"adjustment"`
		Correction string `json:"balance_correction"`
	} `json:"credits"`
	Consumption retainedSeriesConsumption `json:"consumption"`
	Members     []retainedSeriesMember    `json:"members"`
	Display     struct {
		Version      int                               `json:"version"`
		Organization struct{ Opening, Closing string } `json:"organization"`
		Members      []struct {
			UserID           int64 `json:"user_id"`
			Opening, Closing string
		} `json:"members"`
	} `json:"display_balances"`
}
type retainedSeriesXMLCell struct {
	Ref   string `xml:"r,attr"`
	Type  string `xml:"t,attr"`
	Value string `xml:"v"`
	Text  string `xml:"is>t"`
}
type retainedSeriesXMLRow struct {
	Number int                     `xml:"r,attr"`
	Cells  []retainedSeriesXMLCell `xml:"c"`
}

func retainedSeriesRows(t *testing.T, raw []byte) []retainedSeriesXMLRow {
	t.Helper()
	var sheet struct {
		Rows []retainedSeriesXMLRow `xml:"sheetData>row"`
	}
	if len(raw) == 0 || xml.Unmarshal(raw, &sheet) != nil {
		t.Fatal("actual workbook sheet missing or malformed")
	}
	return sheet.Rows
}
func retainedSeriesValues(row retainedSeriesXMLRow) map[string]string {
	v := map[string]string{}
	for _, c := range row.Cells {
		v[strings.TrimRight(c.Ref, "0123456789")] = c.Value + c.Text
	}
	return v
}
func retainedSeriesCents(t *testing.T, s string) string {
	t.Helper()
	if s == "" {
		s = "0"
	}
	return customerMoneyCents(t, s)
}
func retainedSeriesWallet(t *testing.T, m retainedSeriesMember) map[string]string {
	t.Helper()
	r := map[string]string{
		"月初余额（元）": retainedSeriesCents(t, m.Opening), "到账（含赠送）（元）": retainedSeriesCents(t, m.CreditTotal),
		"余额扣减（元）": retainedSeriesCents(t, m.Adjustment), "消费（元）": retainedSeriesCents(t, m.Consumption.Amount),
		"退款（元）": retainedSeriesCents(t, m.Consumption.Refund), "余额校正（元）": retainedSeriesCents(t, m.BalanceCorrection), "舍入尾差（元）": "0.00",
	}
	sum := new(big.Rat)
	for key, sign := range map[string]int{"月初余额（元）": 1, "到账（含赠送）（元）": 1, "余额扣减（元）": 1, "消费（元）": -1, "退款（元）": 1, "余额校正（元）": 1} {
		v, _ := new(big.Rat).SetString(r[key])
		sum.Add(sum, v.Mul(v, big.NewRat(int64(sign), 1)))
	}
	r["月末余额（元）"] = sum.FloatString(2)
	return r
}

// Chronological test-only oracle. All raw amounts come from independent,
// hash-pinned original references, never the actual customer's display DTO.
// Prefix carry must be computed before either forward or reverse requests.
type retainedIndependentDisplayMonth struct {
	Organization map[string]string
	Members      map[int64]map[string]string
}

var retainedIndependentRawMoney = regexp.MustCompile(`^-?(0|[1-9][0-9]*)\.[0-9]{6}$`)

func retainedIndependentCents(n *big.Rat) string {
	v := n.FloatString(2)
	if v == "-0.00" {
		return "0.00"
	}
	return v
}

func retainedIndependentWallet(m retainedSeriesMember, carry string) (map[string]string, error) {
	if m.BalanceCorrection == "" {
		m.BalanceCorrection = "0.000000"
	}
	values := []string{m.Opening, m.CreditTotal, m.Adjustment, m.Consumption.Amount, m.Consumption.Refund, m.BalanceCorrection, m.Closing, m.Consumption.Net}
	raw := make([]*big.Rat, len(values))
	for i, s := range values {
		if len(s) > 30 || !retainedIndependentRawMoney.MatchString(s) || s == "-0.000000" {
			return nil, errors.New("independent raw money missing or noncanonical")
		}
		raw[i], _ = new(big.Rat).SetString(s)
	}
	for _, i := range []int{1, 3, 4, 5} {
		if raw[i].Sign() < 0 {
			return nil, errors.New("invalid independent gross, credit or correction sign")
		}
	}
	if new(big.Rat).Sub(raw[3], raw[4]).Cmp(raw[7]) != 0 {
		return nil, errors.New("independent gross/refund/net does not close")
	}
	sum := new(big.Rat).Set(raw[0])
	for i, sign := range []int64{1, 1, 1, -1, 1, 1} {
		if i != 0 {
			sum.Add(sum, new(big.Rat).Mul(raw[i], big.NewRat(sign, 1)))
		}
	}
	if sum.Cmp(raw[6]) != 0 {
		return nil, errors.New("independent original wallet equation does not close")
	}
	opening := retainedIndependentCents(raw[0]) // exact rational half-away-from-zero
	if carry != "" {
		n, ok := new(big.Rat).SetString(carry)
		if !ok || retainedIndependentCents(n) != carry {
			return nil, errors.New("independent prefix is not exact cents")
		}
		opening = carry
	}
	labels := []string{"月初余额（元）", "到账（含赠送）（元）", "余额扣减（元）", "消费（元）", "退款（元）", "余额校正（元）"}
	out := map[string]string{"舍入尾差（元）": "0.00"}
	derived := new(big.Rat)
	for i, label := range labels {
		v := retainedIndependentCents(raw[i])
		if i == 0 {
			v = opening
		}
		out[label] = v
		cents, _ := new(big.Rat).SetString(v)
		sign := int64(1)
		if i == 3 {
			sign = -1
		}
		derived.Add(derived, new(big.Rat).Mul(cents, big.NewRat(sign, 1)))
	}
	out["月末余额（元）"] = retainedIndependentCents(derived)
	return out, nil
}

func retainedIndependentDisplaySeries(months []string, references map[string][]byte) (map[string]retainedIndependentDisplayMonth, error) {
	if len(months) == 0 || len(months) != len(references) {
		return nil, errors.New("complete independent chronological sources required")
	}
	out := map[string]retainedIndependentDisplayMonth{}
	var previous retainedSeriesView
	var previousDisplay retainedIndependentDisplayMonth
	for index, month := range months {
		at, err := time.Parse("2006-01", month)
		if err != nil || at.Format("2006-01") != month || (index > 0 && at.AddDate(0, -1, 0).Format("2006-01") != months[index-1]) {
			return nil, errors.New("independent month sequence has a gap or duplicate")
		}
		var envelope struct {
			OK   bool               `json:"ok"`
			Data retainedSeriesView `json:"data"`
		}
		if json.Unmarshal(references[month], &envelope) != nil || !envelope.OK || envelope.Data.Month != month || len(envelope.Data.Members) == 0 {
			return nil, errors.New("independent monthly source missing or wrong period")
		}
		p := envelope.Data
		if index > 0 && (previous.Closing != p.Opening || len(previous.Members) != len(p.Members)) {
			return nil, errors.New("independent raw organization boundaries or roster differ")
		}
		carry := ""
		if index > 0 {
			carry = previousDisplay.Organization["月末余额（元）"]
		}
		org, err := retainedIndependentWallet(retainedSeriesMember{Opening: p.Opening, Closing: p.Closing, CreditTotal: p.Credits.Total, Adjustment: p.Credits.Adjustment, BalanceCorrection: p.Credits.Correction, Consumption: p.Consumption}, carry)
		if err != nil {
			return nil, err
		}
		members, rawTotals := map[int64]map[string]string{}, make([]*big.Rat, 8)
		for i := range rawTotals {
			rawTotals[i] = new(big.Rat)
		}
		for _, m := range p.Members {
			if m.UserID <= 0 || members[m.UserID] != nil {
				return nil, errors.New("independent account identity missing or duplicated")
			}
			memberCarry := ""
			if index > 0 {
				found := false
				for _, old := range previous.Members {
					if old.UserID == m.UserID && old.Closing == m.Opening {
						found = true
					}
				}
				if !found {
					return nil, errors.New("independent account boundary or roster changed")
				}
				memberCarry = previousDisplay.Members[m.UserID]["月末余额（元）"]
			}
			wallet, err := retainedIndependentWallet(m, memberCarry)
			if err != nil {
				return nil, err
			}
			members[m.UserID] = wallet
			correction := m.BalanceCorrection
			if correction == "" {
				correction = "0.000000"
			}
			for i, value := range []string{m.Opening, m.CreditTotal, m.Adjustment, m.Consumption.Amount, m.Consumption.Refund, correction, m.Closing, m.Consumption.Net} {
				n, _ := new(big.Rat).SetString(value)
				rawTotals[i].Add(rawTotals[i], n)
			}
		}
		correction := p.Credits.Correction
		if correction == "" {
			correction = "0.000000"
		}
		for i, value := range []string{p.Opening, p.Credits.Total, p.Credits.Adjustment, p.Consumption.Amount, p.Consumption.Refund, correction, p.Closing, p.Consumption.Net} {
			n, _ := new(big.Rat).SetString(value)
			if rawTotals[i].Cmp(n) != 0 {
				return nil, errors.New("independent account sums differ from organization")
			}
		}
		out[month] = retainedIndependentDisplayMonth{Organization: org, Members: members}
		previous, previousDisplay = p, out[month]
	}
	return out, nil
}

func retainedIndependentReferenceFixture(t *testing.T, month string, members ...retainedSeriesMember) []byte {
	t.Helper()
	p := retainedSeriesView{Month: month, Members: members}
	sums := make([]*big.Rat, 8)
	for i := range sums {
		sums[i] = new(big.Rat)
	}
	for _, m := range members {
		correction := m.BalanceCorrection
		if correction == "" {
			correction = "0.000000"
		}
		for i, s := range []string{m.Opening, m.CreditTotal, m.Adjustment, m.Consumption.Amount, m.Consumption.Refund, correction, m.Closing, m.Consumption.Net} {
			n, ok := new(big.Rat).SetString(s)
			if !ok {
				t.Fatal("invalid synthetic original money")
			}
			sums[i].Add(sums[i], n)
		}
	}
	p.Opening, p.Closing = sums[0].FloatString(6), sums[6].FloatString(6)
	p.Credits.Total, p.Credits.Adjustment, p.Credits.Correction = sums[1].FloatString(6), sums[2].FloatString(6), sums[5].FloatString(6)
	p.Consumption = retainedSeriesConsumption{Amount: sums[3].FloatString(6), Refund: sums[4].FloatString(6), Net: sums[7].FloatString(6)}
	// These intentionally wrong old display values must NOT become the oracle.
	p.Display.Version = 1
	p.Display.Organization.Opening, p.Display.Organization.Closing = "999.000000", "999.000000"
	raw, err := json.Marshal(map[string]any{"ok": true, "data": p})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestRetainedIndependentDisplaySeriesUsesOriginalChronologicalPrefix(t *testing.T) {
	base := retainedSeriesMember{UserID: 20, Username: "synthetic", Opening: "0.000000", CreditTotal: "100.000000", Adjustment: "0.000000", Closing: "2.576780", Consumption: retainedSeriesConsumption{Amount: "97.423220", Refund: "0.000000", Net: "97.423220"}}
	july := base
	july.Opening, july.Closing, july.CreditTotal = base.Closing, "0.542992", "0.000000"
	july.Consumption = retainedSeriesConsumption{Amount: "2.033788", Refund: "0.000000", Net: "2.033788"}
	august := july
	august.Opening = july.Closing
	august.Consumption = retainedSeriesConsumption{Amount: "0.000000", Refund: "0.000000", Net: "0.000000"}
	months := []string{"2026-06", "2026-07", "2026-08"}
	references := map[string][]byte{months[0]: retainedIndependentReferenceFixture(t, months[0], base), months[1]: retainedIndependentReferenceFixture(t, months[1], july), months[2]: retainedIndependentReferenceFixture(t, months[2], august)}
	before := map[string][]byte{}
	for month, raw := range references {
		before[month] = bytes.Clone(raw)
	}
	got, err := retainedIndependentDisplaySeries(months, references)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range [][2]string{{"0.00", "2.58"}, {"2.58", "0.55"}, {"0.55", "0.55"}} {
		for _, wallet := range []map[string]string{got[months[i]].Organization, got[months[i]].Members[20]} {
			if wallet["月初余额（元）"] != want[0] || wallet["月末余额（元）"] != want[1] {
				t.Fatal("oracle reset the prefix to independently rounded raw money", months[i])
			}
		}
		if !bytes.Equal(before[months[i]], references[months[i]]) {
			t.Fatal("independent oracle rewrote original money or saved display values")
		}
	}
	// A raw, individually rounded August opening would be 0.54, not 0.55.
	single, err := retainedIndependentDisplaySeries(months[2:], map[string][]byte{months[2]: references[months[2]]})
	if err != nil || single[months[2]].Organization["月初余额（元）"] != "0.54" {
		t.Fatal("synthetic counterexample no longer exercises a real prefix carry")
	}
}

func TestRetainedIndependentDisplaySeriesKeepsAccountsIndependentAndExactCents(t *testing.T) {
	a := retainedSeriesMember{UserID: 20, Opening: "1.005000", Closing: "1.005000", CreditTotal: "0.000000", Adjustment: "0.000000", Consumption: retainedSeriesConsumption{Amount: "0.000000", Refund: "0.000000", Net: "0.000000"}}
	b := a
	b.UserID = 21
	months := []string{"2026-06", "2026-07"}
	got, err := retainedIndependentDisplaySeries(months, map[string][]byte{months[0]: retainedIndependentReferenceFixture(t, months[0], a, b), months[1]: retainedIndependentReferenceFixture(t, months[1], a, b)})
	if err != nil {
		t.Fatal(err)
	}
	for _, month := range months {
		if got[month].Organization["月末余额（元）"] != "2.01" || got[month].Members[20]["月末余额（元）"] != "1.01" || got[month].Members[21]["月末余额（元）"] != "1.01" {
			t.Fatal("an account was redistributed to absorb the organization's rounding")
		}
	}
	for _, amount := range []string{"1.005000", "0.000002"} {
		negative := retainedSeriesMember{UserID: 20, Opening: "0.000000", CreditTotal: "0.000000", Adjustment: "0.000000", Closing: "-" + amount, Consumption: retainedSeriesConsumption{Amount: amount, Refund: "0.000000", Net: amount}}
		wallet, err := retainedIndependentWallet(negative, "")
		want := "-1.01"
		if amount == "0.000002" {
			want = "0.00"
		}
		if err != nil || wallet["月末余额（元）"] != want {
			t.Fatal("exact rational tie or negative zero handled incorrectly", amount, err)
		}
	}
}

func TestRetainedIndependentDisplaySeriesRejectsUnknownAndInconsistentSources(t *testing.T) {
	member := retainedSeriesMember{UserID: 20, Opening: "0.000000", Closing: "0.000000", CreditTotal: "0.000000", Adjustment: "0.000000", Consumption: retainedSeriesConsumption{Amount: "0.000000", Refund: "0.000000", Net: "0.000000"}}
	months := []string{"2026-06", "2026-07"}
	for _, fault := range []string{"gap", "duplicate-month", "wrong-period", "missing-month", "duplicate-account", "switched-account", "changed-boundary", "unclosed-equation", "unknown-net", "short-precision"} {
		t.Run(fault, func(t *testing.T) {
			order := append([]string(nil), months...)
			refs := map[string][]byte{months[0]: retainedIndependentReferenceFixture(t, months[0], member), months[1]: retainedIndependentReferenceFixture(t, months[1], member)}
			changed := member
			switch fault {
			case "gap":
				order[1] = "2026-08"
				refs[order[1]] = retainedIndependentReferenceFixture(t, order[1], member)
				delete(refs, months[1])
			case "duplicate-month":
				order[1] = order[0]
			case "wrong-period":
				refs[months[1]] = retainedIndependentReferenceFixture(t, "2026-08", member)
			case "missing-month":
				delete(refs, months[1])
			case "duplicate-account":
				refs[months[1]] = retainedIndependentReferenceFixture(t, months[1], member, member)
			case "switched-account":
				changed.UserID++
				refs[months[1]] = retainedIndependentReferenceFixture(t, months[1], changed)
			case "changed-boundary":
				changed.Opening, changed.Closing = "0.000002", "0.000002"
				refs[months[1]] = retainedIndependentReferenceFixture(t, months[1], changed)
			case "unclosed-equation":
				changed.Closing = "0.000002"
				refs[months[1]] = retainedIndependentReferenceFixture(t, months[1], changed)
			case "unknown-net":
				refs[months[1]] = bytes.Replace(refs[months[1]], []byte(`"Net":"0.000000"`), []byte(`"Net":""`), 1)
			case "short-precision":
				refs[months[1]] = bytes.Replace(refs[months[1]], []byte(`"opening":"0.000000"`), []byte(`"opening":"0.00"`), 1)
			}
			if _, err := retainedIndependentDisplaySeries(order, refs); err == nil {
				t.Fatal("independent oracle accepted missing evidence or inconsistent original money")
			}
		})
	}
}

func retainedSeriesWorkbookMatchesSource(t *testing.T, source, current, book []byte, prefix ...retainedIndependentDisplayMonth) {
	t.Helper()
	retainedSeriesWorkbookMatchesSourceFormat(t, source, current, book, false, prefix...)
}

func retainedSeriesCentWorkbookMatchesSource(t *testing.T, source, current, book []byte) {
	t.Helper()
	retainedSeriesWorkbookMatchesSourceFormat(t, source, current, book, true)
}

func retainedSeriesWorkbookMatchesSourceFormat(t *testing.T, source, current, book []byte, modern bool, prefix ...retainedIndependentDisplayMonth) {
	t.Helper()
	var old, now struct {
		OK   bool               `json:"ok"`
		Data retainedSeriesView `json:"data"`
	}
	if json.Unmarshal(source, &old) != nil || json.Unmarshal(current, &now) != nil || !old.OK || !now.OK || len(old.Data.Members) == 0 {
		t.Fatal("retained financial source unavailable")
	}
	p := old.Data
	org := retainedSeriesWallet(t, retainedSeriesMember{Opening: p.Opening, Closing: p.Closing, CreditTotal: p.Credits.Total, Adjustment: p.Credits.Adjustment, BalanceCorrection: p.Credits.Correction, Consumption: p.Consumption})
	members := map[int64]retainedSeriesMember{}
	wallets := map[int64]map[string]string{}
	for _, m := range p.Members {
		if _, dup := members[m.UserID]; dup {
			t.Fatal("duplicate retained account")
		}
		members[m.UserID] = m
		wallets[m.UserID] = retainedSeriesWallet(t, m)
	}
	if len(prefix) > 1 {
		t.Fatal("one independently computed chronological display reference required")
	}
	if len(prefix) == 1 {
		if len(prefix[0].Members) != len(members) || len(prefix[0].Organization) != len(org) {
			t.Fatal("independent display prefix scope differs")
		}
		org, wallets = prefix[0].Organization, prefix[0].Members
		for id := range members {
			if len(wallets[id]) != len(org) {
				t.Fatal("independent display prefix omitted an account")
			}
		}
	}
	seen := map[int64]bool{}
	if modern {
		// New-format HTTP money is already the frozen cent view. No legacy
		// display_balances object is expected. Check actual amounts against
		// the independent original-source oracle, never inject a fake object.
		if now.Data.Display.Version != 0 || now.Data.Opening != org["月初余额（元）"]+"0000" || now.Data.Closing != org["月末余额（元）"]+"0000" || len(now.Data.Members) != len(members) {
			t.Fatal("new-format customer summary/scope differs from independent cents")
		}
		if now.Data.Consumption != p.Consumption || retainedSeriesCents(t, now.Data.Credits.Total) != org["到账（含赠送）（元）"] || retainedSeriesCents(t, now.Data.Credits.Adjustment) != org["余额扣减（元）"] || retainedSeriesCents(t, now.Data.Credits.Correction) != org["余额校正（元）"] {
			t.Fatal("exact-cent fixture changed consumption or funding")
		}
		for _, m := range now.Data.Members {
			want, ok := wallets[m.UserID]
			if !ok || seen[m.UserID] || m.Opening != want["月初余额（元）"]+"0000" || m.Closing != want["月末余额（元）"]+"0000" {
				t.Fatal("new-format customer account cents differ from independent source", m.UserID)
			}
			if original := members[m.UserID]; m.Username != original.Username || m.Consumption != original.Consumption || len(m.Tokens) != len(original.Tokens) {
				t.Fatal("new-format account consumption or token scope changed")
			}
			if retainedSeriesCents(t, m.CreditTotal) != want["到账（含赠送）（元）"] || retainedSeriesCents(t, m.Adjustment) != want["余额扣减（元）"] || retainedSeriesCents(t, m.BalanceCorrection) != want["余额校正（元）"] {
				t.Fatal("new-format account funding differs from independent source")
			}
			originalTokens := map[int64]retainedSeriesToken{}
			for _, token := range members[m.UserID].Tokens {
				originalTokens[token.TokenID] = token
			}
			seenTokens := map[int64]bool{}
			for _, token := range m.Tokens {
				original, ok := originalTokens[token.TokenID]
				if !ok || seenTokens[token.TokenID] || original.Name != token.Name {
					t.Fatal("new-format customer token ownership/name changed")
				}
				for i, amount := range []string{token.Amount, token.Refund, token.Net} {
					if amount != retainedSeriesCents(t, []string{original.Amount, original.Refund, original.Net}[i])+"0000" {
						t.Fatal("new-format customer token money differs from independent source")
					}
				}
				seenTokens[token.TokenID] = true
			}
			seen[m.UserID] = true
		}
	} else {
		if now.Data.Display.Version != 1 || now.Data.Display.Organization.Opening != org["月初余额（元）"]+"0000" || now.Data.Display.Organization.Closing != org["月末余额（元）"]+"0000" {
			t.Fatal("legacy customer summary differs from independent cents arithmetic")
		}
		if len(now.Data.Display.Members) != len(members) {
			t.Fatal("actual frozen account scope differs")
		}
		for _, m := range now.Data.Display.Members {
			want, ok := wallets[m.UserID]
			if !ok || seen[m.UserID] || m.Opening != want["月初余额（元）"]+"0000" || m.Closing != want["月末余额（元）"]+"0000" {
				t.Fatal("frozen account cents differ from independent source", m.UserID)
			}
			seen[m.UserID] = true
		}
	}
	parts := retainedWorkbookParts(t, book)
	headers := map[string]string{}
	totals := map[string]*big.Rat{}
	seen = map[int64]bool{}
	footer := false
	for _, row := range retainedSeriesRows(t, parts["xl/worksheets/sheet2.xml"]) {
		v := retainedSeriesValues(row)
		if row.Number == 3 {
			headers = v
			continue
		}
		if row.Number < 4 {
			continue
		}
		want := org
		isFooter := v["A"] == "组织合计"
		isResidual := v["A"] == "分项舍入尾差"
		if !isFooter && !isResidual {
			_, id, ok := strings.Cut(v["A"], "\nID ")
			uid, err := strconv.ParseInt(id, 10, 64)
			m, found := members[uid]
			if !ok || err != nil || !found || seen[uid] {
				t.Fatal("wrong or duplicate actual exported account")
			}
			seen[uid] = true
			want = wallets[uid]
			name := m.Username
			if name == "" {
				name = "—"
			}
			if v["A"] != name+"\nID "+id {
				t.Fatal("account name/identity changed")
			}
		}
		for col, title := range headers {
			if col == "A" {
				continue
			}
			if _, ok := org[title]; !ok {
				t.Fatal("unknown account money column", title)
			}
			actual := v[col]
			if actual != retainedSeriesCents(t, actual) {
				t.Fatal("hidden sub-cent actual exported money")
			}
			n, ok := new(big.Rat).SetString(actual)
			if !ok {
				t.Fatal("actual money is not numeric")
			}
			if totals[title] == nil {
				totals[title] = new(big.Rat)
			}
			if isResidual {
				expected, _ := new(big.Rat).SetString(org[title])
				for _, mw := range wallets {
					part, _ := new(big.Rat).SetString(mw[title])
					expected.Sub(expected, part)
				}
				if n.Cmp(expected) != 0 {
					t.Fatal("account rounding residual differs from source", title)
				}
			} else if actual != want[title] {
				t.Fatal("exported account/footer money differs from source", title)
			}
			if isFooter {
				if totals[title].Cmp(n) != 0 {
					t.Fatal("actual account SUM differs from organization", title)
				}
			} else {
				totals[title].Add(totals[title], n)
			}
		}
		if isFooter {
			if footer {
				t.Fatal("duplicate organization footer")
			}
			footer = true
		}
	}
	if len(seen) != len(members) || footer != (len(members) > 1) {
		t.Fatal("actual workbook omitted an account or organization total")
	}
	for col, title := range headers {
		if col == "A" {
			continue
		}
		expected, _ := new(big.Rat).SetString(org[title])
		if totals[title] == nil || totals[title].Cmp(expected) != 0 {
			t.Fatal("actual account column sum differs from source organization", title)
		}
	}
	retainedSeriesTokens(t, p, parts["xl/worksheets/sheet3.xml"])
	// Audit the customer-facing main summary too, not only detail totals.
	labels := map[string]string{}
	metric := false
	for _, row := range retainedSeriesRows(t, parts["xl/worksheets/sheet1.xml"]) {
		v := retainedSeriesValues(row)
		if metric {
			labels["消费（元）"] = v["A"]
			metric = false
		}
		if v["A"] == "总消费（元）" {
			metric = true
		}
		if v["A"] == "月末余额（元）" {
			labels[v["A"]] = v["C"]
		} else if v["A"] != "" && v["D"] != "" {
			labels[v["A"]] = v["D"]
		}
	}
	for title, want := range map[string]string{"月初余额（元）": org["月初余额（元）"], "到账合计": org["到账（含赠送）（元）"], "消费（元）": org["消费（元）"], "月末余额（元）": org["月末余额（元）"]} {
		if strings.TrimPrefix(labels[title], "¥") != want {
			t.Fatal("main summary differs from independent actual source", title)
		}
	}
}

func retainedSeriesTokens(t *testing.T, p retainedSeriesView, raw []byte) {
	t.Helper()
	type key struct{ user, token int64 }
	expected := map[key]retainedSeriesToken{}
	byUser := map[int64][3]*big.Rat{}
	seen := map[key]bool{}
	sum := [3]*big.Rat{new(big.Rat), new(big.Rat), new(big.Rat)}
	for _, m := range p.Members {
		byUser[m.UserID] = [3]*big.Rat{new(big.Rat), new(big.Rat), new(big.Rat)}
		for _, r := range m.Tokens {
			if _, duplicate := expected[key{m.UserID, r.TokenID}]; duplicate {
				t.Fatal("duplicate retained token identity")
			}
			expected[key{m.UserID, r.TokenID}] = r
		}
	}
	for _, row := range retainedSeriesRows(t, raw) {
		if row.Number < 4 {
			continue
		}
		v := retainedSeriesValues(row)
		uid, e1 := strconv.ParseInt(v["B"], 10, 64)
		tid, e2 := strconv.ParseInt(v["D"], 10, 64)
		residual := v["D"] == "—" && (v["C"] == "分项舍入尾差" || v["C"] == "组织汇总舍入尾差")
		if !residual {
			if v["D"] == "—" && v["C"] == "历史未记录令牌" {
				tid, e2 = 0, nil
			}
			want, ok := expected[key{uid, tid}]
			if e1 != nil || e2 != nil || !ok || seen[key{uid, tid}] || want.Name != v["C"] {
				t.Fatal("actual exported token scope/name differs")
			}
			seen[key{uid, tid}] = true
			for i, s := range []string{want.Amount, want.Refund, want.Net} {
				if v[[]string{"E", "F", "G"}[i]] != retainedSeriesCents(t, s) {
					t.Fatal("actual exported token amount differs from source")
				}
			}
		}
		values := [3]*big.Rat{}
		for i, col := range []string{"E", "F", "G"} {
			s := v[col]
			if s != retainedSeriesCents(t, s) {
				t.Fatal("hidden sub-cent token amount")
			}
			n, ok := new(big.Rat).SetString(s)
			if !ok {
				t.Fatal("non-numeric token money")
			}
			values[i] = n
			sum[i].Add(sum[i], n)
			if e1 == nil {
				parts, ok := byUser[uid]
				if !ok {
					t.Fatal("token attributed to a foreign account")
				}
				parts[i].Add(parts[i], n)
			}
		}
		net := new(big.Rat).Sub(values[0], values[1])
		if v["H"] != "" {
			tail, ok := new(big.Rat).SetString(v["H"])
			if !ok || v["H"] != retainedSeriesCents(t, v["H"]) {
				t.Fatal("invalid net rounding bridge")
			}
			net.Add(net, tail)
		}
		if net.Cmp(values[2]) != 0 {
			t.Fatal("actual token gross/refund/net does not close")
		}
	}
	if len(seen) != len(expected) {
		t.Fatal("actual export omitted a retained token")
	}
	for _, m := range p.Members {
		for i, s := range []string{m.Consumption.Amount, m.Consumption.Refund, m.Consumption.Net} {
			if byUser[m.UserID][i].FloatString(2) != retainedSeriesCents(t, s) {
				t.Fatal("token SUM differs from its source account", m.UserID)
			}
		}
	}
	for i, s := range []string{p.Consumption.Amount, p.Consumption.Refund, p.Consumption.Net} {
		if sum[i].FloatString(2) != retainedSeriesCents(t, s) {
			t.Fatal("actual token SUM differs from source organization")
		}
	}
}
