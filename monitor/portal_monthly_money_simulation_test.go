package monitor

// Synthetic money simulations only. They do not establish real source proof,
// connect to NewAPI, alter an existing backup, or charge an account.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

type monthlySimulationCase struct {
	Label        string              `json:"label"`
	Data         monthlyStatement    `json:"data"`
	Organization []string            `json:"organization"`
	Members      map[string][]string `json:"members"`
}

func monthlySimulationSeed(t *testing.T) int64 {
	t.Helper()
	if raw := os.Getenv("MONTHLY_MONEY_SIMULATION_SEED"); raw != "" {
		seed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || seed < 0 || seed > 1000000000 {
			t.Fatal("invalid explicit simulation seed")
		}
		return seed
	}
	return 20261027
}

// Raw quota expectations come from the independent fixture ledger, not the
// calculator output. Equal organization sums cannot conceal swapped users.
func monthlySimulationRaw(t *testing.T, p monthlyStatement, expected map[int64]monthlyUniversalExpected) {
	t.Helper()
	if len(p.Members) != len(expected) || !monthlyStatementAmountsComplete(p) {
		t.Fatal("incomplete synthetic statement")
	}
	var total [7]big.Int
	for _, member := range p.Members {
		want, found := expected[member.UserID]
		if !found || len(member.TokenConsumption) != len(want.tokens) {
			t.Fatal("account/token identity omitted or merged", member.UserID)
		}
		for i, raw := range []string{member.Opening, member.CreditTotal, member.Adjustment, member.Consumption.Amount, member.Consumption.Refund, member.Consumption.Net, member.Closing} {
			if raw != monthlyUniversalMoney(want.money[i]) {
				t.Fatal("source money misattributed", member.UserID, i)
			}
			total[i].Add(&total[i], big.NewInt(want.money[i]))
		}
		for _, token := range member.TokenConsumption {
			q, found := want.tokens[token.TokenID]
			if !found || token.Amount != monthlyUniversalMoney(q[0]) || token.Refund != monthlyUniversalMoney(q[1]) || token.Net != monthlyUniversalMoney(q[0]-q[1]) {
				t.Fatal("token money or immutable ownership changed", member.UserID, token.TokenID)
			}
		}
	}
	for i, raw := range []string{*p.MonthOpeningBalance, p.Credits.KnownTotal, p.Credits.Adjustment, p.Consumption.Amount, p.Consumption.Refund, p.Consumption.Net, *p.MonthEndBalance} {
		if !total[i].IsInt64() || raw != monthlyUniversalMoney(total[i].Int64()) {
			t.Fatal("organization source amount differs from independent accounts", i)
		}
	}
}

// Exact rational cents oracle, intentionally separate from the production
// planner/formatter. Previous customer-visible endpoints are model state.
func monthlySimulationWallet(t *testing.T, m monthlyMemberStatement, previous []string) []string {
	t.Helper()
	values := []string{monthlyAuditCents(m.Opening), monthlyAuditCents(m.CreditTotal), monthlyAuditCents(m.Adjustment), monthlyAuditCents(m.Consumption.Amount), monthlyAuditCents(m.Consumption.Refund)}
	if previous != nil {
		values[0] = previous[len(previous)-1]
	}
	n := new(big.Rat)
	for i, value := range values {
		r, ok := new(big.Rat).SetString(value)
		if !ok {
			t.Fatal("invalid oracle amount")
		}
		if i == 3 {
			n.Sub(n, r)
		} else {
			n.Add(n, r)
		}
	}
	return append(values, n.FloatString(2))
}

func monthlySimulationFreeze(t *testing.T, groupID int64, p *monthlyStatement, previous *monthlyDisplayChainLink, model *monthlySimulationCase) (monthlyDisplayChainLink, monthlySimulationCase) {
	t.Helper()
	before, _ := json.Marshal(p)
	var neighbors []monthlyDisplayChainLink
	if previous != nil {
		neighbors = append(neighbors, *previous)
	}
	link, err := monthlyPlanDisplayChain(groupID, *p, neighbors)
	if err != nil {
		t.Fatal("synthetic chain rejected", p.Month, err)
	}
	after, _ := json.Marshal(p)
	if !bytes.Equal(before, after) {
		t.Fatal("planning mutated financial facts")
	}
	org := monthlyMemberStatement{Opening: *p.MonthOpeningBalance, CreditTotal: p.Credits.KnownTotal, Adjustment: p.Credits.Adjustment, Consumption: *p.Consumption}
	want := monthlySimulationCase{Members: map[string][]string{}}
	var prior []string
	if model != nil {
		prior = model.Organization
	}
	want.Organization = monthlySimulationWallet(t, org, prior)
	if link.Organization.Opening != want.Organization[0]+"0000" || link.Organization.Closing != want.Organization[5]+"0000" {
		t.Fatal("organization cents chain differs from independent model", p.Month)
	}
	frozen := &monthlyDisplayBalances{Version: 1, Organization: monthlyDisplayBalance{Opening: link.Organization.Opening, Closing: link.Organization.Closing}, Members: []monthlyDisplayBalanceMember{}}
	for i, member := range p.Members {
		key := strconv.FormatInt(member.UserID, 10)
		prior = nil
		if model != nil {
			prior = model.Members[key]
		}
		values := monthlySimulationWallet(t, member, prior)
		if link.Members[i].UserID != member.UserID || link.Members[i].Wallet.Opening != values[0]+"0000" || link.Members[i].Wallet.Closing != values[5]+"0000" {
			t.Fatal("individual cents chain differs from independent model", p.Month, member.UserID)
		}
		want.Members[key] = values
		frozen.Members = append(frozen.Members, monthlyDisplayBalanceMember{UserID: member.UserID, Opening: link.Members[i].Wallet.Opening, Closing: link.Members[i].Wallet.Closing})
	}
	p.DisplayBalances = frozen
	if !monthlyStatementAmountsComplete(*p) {
		t.Fatal("verified simulation display metadata rejected")
	}
	want.Data = *p
	return link, want
}

func monthlySimulationWorkbook(t *testing.T, c monthlySimulationCase) {
	t.Helper()
	p := c.Data
	before, _ := json.Marshal(p)
	book, err := monthlyStatementXLSX(p)
	if err != nil {
		t.Fatal("simulation download rejected", c.Label, err)
	}
	parts := monthlyAuditWorkbookParts(t, book)
	labels := map[string]string{}
	metric := false
	for _, row := range monthlyRoundingAuditRows(t, parts["xl/worksheets/sheet1.xml"]) {
		v := monthlyRoundingAuditValues(row)
		if metric {
			labels["消费（元）"], metric = v["A"], false
		}
		if v["A"] == "总消费（元）" {
			metric = true
		} else if v["A"] == "月末余额（元）" {
			labels[v["A"]] = v["C"]
		} else if v["A"] != "" && v["D"] != "" {
			labels[v["A"]] = v["D"]
		}
	}
	for label, want := range map[string]string{"月初余额（元）": c.Organization[0], "到账合计": c.Organization[1], "消费（元）": c.Organization[3], "月末余额（元）": c.Organization[5], "充值合计": monthlyAuditCents(*p.Credits.RechargeTotal), "赠送合计": monthlyAuditCents(*p.Credits.GiftTotal)} {
		if labels[label] != want {
			t.Fatal("summary workbook money differs from model", c.Label, label, labels[label], want)
		}
	}
	seen := map[string]bool{}
	sums := make([]big.Int, 6)
	for _, row := range monthlyRoundingAuditRows(t, parts["xl/worksheets/sheet2.xml"]) {
		if row.Index < 4 {
			continue
		}
		v := monthlyRoundingAuditValues(row)
		want := c.Organization
		if v["A"] != "组织合计" && v["A"] != "分项舍入尾差" {
			_, key, ok := strings.Cut(v["A"], "\nID ")
			want = c.Members[key]
			if !ok || want == nil || seen[key] {
				t.Fatal("wrong or repeated exported member", c.Label, key)
			}
			seen[key] = true
		}
		for i, column := range []string{"B", "C", "D", "E", "F", "G"} {
			n := monthlyAuditIntegerCents(t, v[column])
			if v["A"] == "组织合计" {
				if n.Cmp(&sums[i]) != 0 || v[column] != c.Organization[i] {
					t.Fatal("account sum/footer differs from organization", c.Label, i)
				}
				continue
			}
			if v["A"] != "分项舍入尾差" && v[column] != want[i] {
				t.Fatal("individual workbook money differs from model", c.Label, v["A"], i)
			}
			sums[i].Add(&sums[i], n)
		}
		rowEquation := new(big.Int)
		for i, col := range []string{"B", "C", "D", "E", "F"} {
			n := monthlyAuditIntegerCents(t, v[col])
			if i == 3 {
				rowEquation.Sub(rowEquation, n)
			} else {
				rowEquation.Add(rowEquation, n)
			}
		}
		if rowEquation.Cmp(monthlyAuditIntegerCents(t, v["G"])) != 0 {
			t.Fatal("visible account wallet does not close in cents", c.Label)
		}
	}
	if len(seen) != len(c.Members) {
		t.Fatal("member omitted from workbook", c.Label)
	}
	tokenExpected := map[string]monthlyTokenConsumption{}
	for _, member := range p.Members {
		for _, token := range member.TokenConsumption {
			tokenExpected[fmt.Sprintf("%d/%d", member.UserID, token.TokenID)] = token
		}
	}
	perUser := map[string]*[3]big.Int{}
	var tokenTotal [3]big.Int
	tokenSeen := map[string]bool{}
	for _, row := range monthlyRoundingAuditRows(t, parts["xl/worksheets/sheet3.xml"]) {
		if row.Index < 4 {
			continue
		}
		v := monthlyRoundingAuditValues(row)
		if v["D"] != "—" || v["C"] == "历史未记录令牌" {
			id := v["D"]
			if id == "—" {
				id = "0"
			}
			key := v["B"] + "/" + id
			want, exists := tokenExpected[key]
			if !exists || tokenSeen[key] || want.Name != v["C"] || v["E"] != monthlyAuditCents(want.Amount) || v["F"] != monthlyAuditCents(want.Refund) || v["G"] != monthlyAuditCents(want.Net) {
				t.Fatal("exported token money/owner differs", c.Label, key)
			}
			tokenSeen[key] = true
		}
		for i, column := range []string{"E", "F", "G"} {
			n := monthlyAuditIntegerCents(t, v[column])
			tokenTotal[i].Add(&tokenTotal[i], n)
			if v["B"] != "—" {
				if perUser[v["B"]] == nil {
					perUser[v["B"]] = new([3]big.Int)
				}
				perUser[v["B"]][i].Add(&perUser[v["B"]][i], n)
			}
		}
		net := new(big.Int).Sub(monthlyAuditIntegerCents(t, v["E"]), monthlyAuditIntegerCents(t, v["F"]))
		if v["H"] != "" {
			net.Add(net, monthlyAuditIntegerCents(t, v["H"]))
		}
		if net.Cmp(monthlyAuditIntegerCents(t, v["G"])) != 0 {
			t.Fatal("token/refund net equation does not close", c.Label, v)
		}
	}
	if len(tokenSeen) != len(tokenExpected) {
		t.Fatal("token disappeared", c.Label)
	}
	for _, member := range p.Members {
		total := perUser[strconv.FormatInt(member.UserID, 10)]
		if total == nil {
			total = new([3]big.Int)
		}
		for i, raw := range []string{member.Consumption.Amount, member.Consumption.Refund, member.Consumption.Net} {
			if total[i].Cmp(monthlyAuditIntegerCents(t, monthlyAuditCents(raw))) != 0 {
				t.Fatal("token sum does not equal its user's bill", c.Label, member.UserID)
			}
		}
	}
	for i, raw := range []string{p.Consumption.Amount, p.Consumption.Refund, p.Consumption.Net} {
		if tokenTotal[i].Cmp(monthlyAuditIntegerCents(t, monthlyAuditCents(raw))) != 0 {
			t.Fatal("flat token SUM differs from organization", c.Label)
		}
	}
	after, _ := json.Marshal(p)
	if !bytes.Equal(before, after) {
		t.Fatal("download mutated financial facts", c.Label)
	}
}

func TestMonthlyMoneySimulationOrganizationsAndWorkbooks(t *testing.T) {
	seed := monthlySimulationSeed(t)
	cases := make([]monthlySimulationCase, 0, 600)
	accounts, tokens := 0, 0
	for iteration := range 600 {
		e, expected, now := monthlyUniversalEvidence(t, int(seed%10000)+100+iteration)
		if iteration%3 == 0 {
			start, _, _ := monthlyWindow(e.Month, now)
			for i := range e.Users {
				u := &e.Users[i]
				q := int64(2500 + iteration)
				u.Events = append(u.Events, MonthlyBillCreditEvidence{ID: u.UserID*10 + 5, At: start.Unix() + 110, DeltaQuota: q, Kind: "recharge", Method: "后台充值"}, MonthlyBillCreditEvidence{ID: u.UserID*10 + 6, At: start.Unix() + 111, DeltaQuota: -q, Kind: "adjustment", Method: "余额扣减"})
				u.Reversals = append(u.Reversals, MonthlyBillReversal{CreditID: u.UserID*10 + 5, DebitID: u.UserID*10 + 6, Reason: "synthetic confirmed same-month reversal"})
				u.ExpectedCreditEvents = len(u.Events)
			}
		}
		p, _, err := buildPublishedMonthlyBill(e, now)
		if err != nil {
			t.Fatal("simulation build", seed, iteration, err)
		}
		monthlySimulationRaw(t, p, expected)
		p.Account = fmt.Sprintf("合成组织-%d", iteration)
		_, c := monthlySimulationFreeze(t, e.GroupID, &p, nil, nil)
		c.Label = fmt.Sprintf("seed=%d organization=%d", seed, iteration)
		monthlySimulationWorkbook(t, c)
		cases = append(cases, c)
		accounts += len(p.Members)
		for _, member := range p.Members {
			tokens += len(member.TokenConsumption)
		}
	}
	if output := os.Getenv("MONTHLY_MONEY_SIMULATION_OUTPUT"); output != "" {
		stat, err := os.Stat(output)
		if err != nil || !filepath.IsAbs(output) || !stat.IsDir() || stat.Mode().Perm() != 0700 {
			t.Fatal("existing private absolute simulation output directory required")
		}
		payload, err := json.Marshal(struct {
			Mode       string                  `json:"mode"`
			Seed       int64                   `json:"seed"`
			Accounts   int                     `json:"accounts"`
			Tokens     int                     `json:"tokens"`
			Statements []monthlySimulationCase `json:"statements"`
		}{"synthetic_money_simulation_no_production_access", seed, accounts, tokens, cases})
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(filepath.Join(output, "browser-cases.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := file.Write(payload)
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatal(writeErr, closeErr)
		}
	}
	t.Logf("seed=%d organizations=600 accounts=%d token_rows=%d raw money, individual owners, cents wallets and workbook sums verified", seed, accounts, tokens)
}

func TestMonthlyMoneySimulationOrganizationOnlyNetTail(t *testing.T) {
	e, now := monthlyTokenFixture()
	e.Users = e.Users[:2]
	for i := range e.Users {
		u := &e.Users[i]
		u.ConsumeQuota, u.RefundQuota = 0, 0
		if i == 0 {
			u.ConsumeQuota = 2500
		} else {
			u.RefundQuota = 2499
		}
		u.ClosingQuota = u.OpeningQuota - u.ConsumeQuota + u.RefundQuota
		u.ReverseClosingQuota = u.ClosingQuota
		u.Accounting = monthlyAccountingFixture(u.ConsumeQuota, u.RefundQuota)
		u.TokenUsage.Rows = []MonthlyBillTokenEvidence{{TokenID: u.UserID * 10, Name: "同名令牌", ConsumeQuota: u.ConsumeQuota, RefundQuota: u.RefundQuota}}
	}
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range p.Members {
		if monthlyNetDisplayResidual(member.Consumption) != "0.000000" {
			t.Fatal("regression fixture must balance every individual user")
		}
	}
	if monthlyNetDisplayResidual(*p.Consumption) != "-0.010000" {
		t.Fatal("fixture lost organization-only net tail")
	}
	_, c := monthlySimulationFreeze(t, e.GroupID, &p, nil, nil)
	c.Label = "organization-only one-cent net bridge"
	monthlySimulationWorkbook(t, c)
	if sheet := monthlyTokenSheet(p); !strings.Contains(sheet, "净额舍入尾差（元）") || !strings.Contains(sheet, `<v>-0.01</v>`) {
		t.Fatal("organization-only net bridge column omitted")
	}
}

func TestMonthlyMoneySimulationTwentyFourMonthCarry(t *testing.T) {
	seed := monthlySimulationSeed(t)
	for organization := range 32 {
		var previous *monthlyDisplayChainLink
		var model *monthlySimulationCase
		closing := map[int64]int64{}
		for month := range 24 {
			e, expected, now := monthlyUniversalEvidence(t, int(seed%10000)+organization+2000)
			period := time.Date(2024, time.Month(1+month), 1, 0, 0, 0, 0, usageCST)
			oldStart, _, _ := monthlyWindow(e.Month, now)
			e.Month = period.Format("2006-01")
			_, end, _ := monthlyWindow(e.Month, now)
			factor := int64(1 + month%5)
			for i := range e.Users {
				u := &e.Users[i]
				want := expected[u.UserID]
				if month > 0 {
					u.OpeningQuota = closing[u.UserID]
				}
				u.Through, u.TokenUsage.Month = end.Unix(), e.Month
				u.ConsumeQuota *= factor
				u.RefundQuota *= factor
				u.Accounting = monthlyAccountingFixture(u.ConsumeQuota, u.RefundQuota)
				for j := range u.TokenUsage.Rows {
					u.TokenUsage.Rows[j].ConsumeQuota *= factor
					u.TokenUsage.Rows[j].RefundQuota *= factor
				}
				for id, q := range want.tokens {
					want.tokens[id] = [2]int64{q[0] * factor, q[1] * factor}
				}
				for j := range u.Events {
					u.Events[j].At += period.Unix() - oldStart.Unix()
				}
				u.ClosingQuota = u.OpeningQuota + want.money[1] + want.money[2] - u.ConsumeQuota + u.RefundQuota
				u.ReverseClosingQuota = u.ClosingQuota
				want.money[0], want.money[3], want.money[4], want.money[5], want.money[6] = u.OpeningQuota, u.ConsumeQuota, u.RefundQuota, u.ConsumeQuota-u.RefundQuota, u.ClosingQuota
				expected[u.UserID], closing[u.UserID] = want, u.ClosingQuota
			}
			p, _, err := buildPublishedMonthlyBill(e, now)
			if err != nil {
				t.Fatal(organization, month, err)
			}
			monthlySimulationRaw(t, p, expected)
			link, c := monthlySimulationFreeze(t, e.GroupID, &p, previous, model)
			c.Label = fmt.Sprintf("chain seed=%d org=%d month=%s", seed, organization, e.Month)
			if month%6 == 5 {
				monthlySimulationWorkbook(t, c)
			}
			previous, model = &link, &c
		}
	}
	t.Logf("seed=%d organizations=32 months=768 independent original balances and customer-cent continuity verified; 128 cross-month workbooks audited", seed)
}

func TestMonthlyMoneySimulationFaultsNeverBecomePublishedMoney(t *testing.T) {
	for iteration := range 500 {
		e, _, now := monthlyUniversalEvidence(t, 4001+iteration)
		p, _, err := buildPublishedMonthlyBill(e, now)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = monthlySimulationFreeze(t, e.GroupID, &p, nil, nil)
		p.DisplayBalances.Organization.Closing = monthlySimulationShiftCent(t, p.DisplayBalances.Organization.Closing)
		if monthlyStatementAmountsComplete(p) {
			t.Fatal("bad frozen cent delta accepted", iteration)
		}
		if _, err := monthlyStatementXLSX(p); err == nil {
			t.Fatal("bad frozen cent delta downloaded", iteration)
		}
		// A single quota (0.000002 yuan) is a real source mismatch, not a
		// rounding tail. It must be rejected even when visible cents agree.
		e.Users[0].ClosingQuota++
		e.Users[0].ReverseClosingQuota++
		if _, _, err := buildPublishedMonthlyBill(e, now); err == nil {
			t.Fatal("one-quota source mismatch concealed", iteration)
		}
	}
	t.Log("500 corrupted frozen endpoints and 500 one-quota source mismatches rejected")
}

func monthlySimulationShiftCent(t *testing.T, raw string) string {
	t.Helper()
	n, ok := new(big.Rat).SetString(raw)
	if !ok {
		t.Fatal("invalid simulation cent")
	}
	return n.Add(n, big.NewRat(1, 100)).FloatString(6)
}

func TestMonthlyMoneySimulationRandomMonthOrder(t *testing.T) {
	seed := monthlySimulationSeed(t)
	random := rand.New(rand.NewSource(seed))
	accepted, conflictingBridges := 0, 0
	for scenario := range 100 {
		statements := make([]monthlyStatement, 6)
		opening := random.Int63n(10000000) - 5000000
		for month := range statements {
			credit, consume, refund := random.Int63n(10000), random.Int63n(10000), random.Int63n(10000)
			statements[month] = monthlyChainFixture(t, fmt.Sprintf("2026-%02d", month+4), opening, credit, consume, refund)
			opening += credit - consume + refund
		}
		links := map[int]monthlyDisplayChainLink{}
		model := map[int][2]string{}
		for _, month := range random.Perm(6) {
			p := statements[month]
			var neighbors []monthlyDisplayChainLink
			for _, index := range []int{month - 1, month + 1} {
				if link, found := links[index]; found {
					neighbors = append(neighbors, link)
				}
			}
			frozenBefore, _ := json.Marshal(links)
			sourceBefore, _ := json.Marshal(p)
			member := p.Members[0]
			delta, _ := new(big.Rat).SetString(monthlyAuditWalletClosing(member))
			roundedOpening, _ := new(big.Rat).SetString(monthlyAuditCents(member.Opening))
			delta.Sub(delta, roundedOpening)
			expectedOpening := new(big.Rat).Set(roundedOpening)
			if previous, found := model[month-1]; found {
				expectedOpening, _ = new(big.Rat).SetString(previous[1])
			} else if next, found := model[month+1]; found {
				expectedOpening, _ = new(big.Rat).SetString(next[0])
				expectedOpening.Sub(expectedOpening, delta)
			}
			expectedClosing := new(big.Rat).Add(expectedOpening, delta)
			conflict := false
			if next, found := model[month+1]; found {
				conflict = next[0] != expectedClosing.FloatString(6)
			}
			link, err := monthlyPlanDisplayChain(4, p, neighbors)
			frozenAfter, _ := json.Marshal(links)
			sourceAfter, _ := json.Marshal(p)
			if !bytes.Equal(frozenBefore, frozenAfter) || !bytes.Equal(sourceBefore, sourceAfter) {
				t.Fatal("random-order planning changed an issued balance or source", scenario, month)
			}
			if conflict {
				if !errors.Is(err, errMonthlyDisplayChainConflict) {
					t.Fatal("contradictory frozen endpoints accepted", scenario, month, err)
				}
				conflictingBridges++
				continue
			}
			if err != nil || link.Organization.Opening != expectedOpening.FloatString(6) || link.Organization.Closing != expectedClosing.FloatString(6) || link.Members[0].Wallet.Opening != link.Organization.Opening || link.Members[0].Wallet.Closing != link.Organization.Closing {
				t.Fatal("random-order cents differ from independent model", scenario, month, err)
			}
			links[month], model[month] = link, [2]string{expectedOpening.FloatString(6), expectedClosing.FloatString(6)}
			again, err := monthlyPlanDisplayChain(4, p, append(neighbors, link))
			if err != nil || !reflect.DeepEqual(again, link) {
				t.Fatal("random-order repeat changed frozen amounts", scenario, month, err)
			}
			accepted++
		}
	}
	t.Logf("seed=%d random month creations=600 accepted=%d incompatible frozen bridges safely rejected=%d; no source or issued balance rewritten", seed, accepted, conflictingBridges)
	// Characterize an unresolved product risk, NOT a success claim: the
	// source ledger is identical, but which month was frozen first selects
	// a different display-cent anchor. Keep the numerical reproduction until
	// a generation-order/anchor policy is explicitly resolved.
	a := monthlyChainFixture(t, "2026-08", 1160872085, 2500000000, 1513097892, 0)
	b := monthlyChainFixture(t, "2026-09", 2147774193, 0, 2500, 0)
	forwardA, err := monthlyPlanDisplayChain(4, a, nil)
	if err != nil {
		t.Fatal(err)
	}
	forwardB, err := monthlyPlanDisplayChain(4, b, []monthlyDisplayChainLink{forwardA})
	if err != nil {
		t.Fatal(err)
	}
	reverseB, err := monthlyPlanDisplayChain(4, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	reverseA, err := monthlyPlanDisplayChain(4, a, []monthlyDisplayChainLink{reverseB})
	if err != nil {
		t.Fatal(err)
	}
	if forwardA.Organization.Opening != "2321.740000" || reverseA.Organization.Opening != "2321.750000" || forwardB.Organization.Closing != "4295.530000" || reverseB.Organization.Closing != "4295.540000" {
		t.Fatal("known anchor-order characterization changed; reassess the unresolved risk")
	}
	t.Logf("KNOWN_UNRESOLVED_ANCHOR_ORDER_RISK forward August opening=%s September closing=%s; reverse August opening=%s September closing=%s; original six-decimal financial facts identical", forwardA.Organization.Opening, forwardB.Organization.Closing, reverseA.Organization.Opening, reverseB.Organization.Closing)
}

func TestMonthlyMoneySimulationConcurrentReplayAndRollback(t *testing.T) {
	m, _, group, _ := monthlyFrozenFixture(t)
	t.Cleanup(m.Close)
	now := time.Now()
	last, _, _ := monthlyWindow("", now)
	e := monthlyFrozenEvidence(group.ID, last.AddDate(0, -1, 0).Format("2006-01"), 1160872085, 2500000000, 1513097892, now)
	raw := monthlyTestRaw(t, e)
	start, failures := make(chan struct{}), make(chan error, 8)
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 10 {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				var err error
				for attempt := 0; attempt < 20; attempt++ {
					_, err = ImportMonthlyBill(ctx, m.storeDB, raw, true, now)
					if err == nil || (!strings.Contains(err.Error(), "locked") && !strings.Contains(err.Error(), "SQLITE_BUSY")) {
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
				cancel()
				if err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	close(start)
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Fatal("concurrent synthetic replays failed", err)
	}
	var count int64
	if err := m.storeDB.Model(&PortalMonthlyBill{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("replay duplicated a bill", err, count)
	}
	frozen := monthlyFrozenRecord(t, m.storeDB, group.ID, e.Month)
	e.Users[0].ConsumeQuota++
	e.Users[0].ClosingQuota--
	e.Users[0].ReverseClosingQuota--
	e.Users[0].Accounting = monthlyAccountingFixture(e.Users[0].ConsumeQuota, e.Users[0].RefundQuota)
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, e), true, now); err == nil || !reflect.DeepEqual(frozen, monthlyFrozenRecord(t, m.storeDB, group.ID, e.Month)) {
		t.Fatal("changed input overwrote issued bill", err)
	}
	next := monthlyFrozenEvidence(group.ID, last.Format("2006-01"), 2147774193, 0, 2500, now)
	injected := errors.New("synthetic storage failure only")
	const callback = "synthetic_money_simulation_rollback"
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "portal_monthly_bills" {
			if err := tx.AddError(injected); !errors.Is(err, injected) {
				t.Error("synthetic failure injection failed", err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	_, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, next), true, now)
	if removeErr := m.storeDB.Callback().Create().Remove(callback); removeErr != nil {
		t.Fatal(removeErr)
	}
	if !errors.Is(err, injected) || !reflect.DeepEqual(frozen, monthlyFrozenRecord(t, m.storeDB, group.ID, frozen.Month)) {
		t.Fatal("storage failure changed issued money", err)
	}
	if err := m.storeDB.Model(&PortalMonthlyBill{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("failed publication left a partial bill", err, count)
	}
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, next), true, now); err != nil {
		t.Fatal("recovery failed", err)
	}
	t.Log("80 concurrent duplicate imports kept one immutable bill; changed input rejected; injected write failure rolled back; retry recovered")

	// Confirm the order risk through the real SQLite publication transaction,
	// not only a pure planner. Separate synthetic organizations keep the two
	// ledger sequences isolated; neither source balance/charge is changed.
	makeGroup := func(name string, uid int64) CustomerGroup {
		g := CustomerGroup{Name: name}
		if err := m.storeDB.Create(&g).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&TrackedUser{UserID: uid, GroupID: g.ID, Username: "synthetic-order-risk"}).Error; err != nil {
			t.Fatal(err)
		}
		return g
	}
	makeEvidence := func(g CustomerGroup, uid int64, month string, opening, credit, consume int64) MonthlyBillEvidence {
		value := monthlyFrozenEvidence(g.ID, month, opening, credit, consume, now)
		value.Users[0].UserID = uid
		return value
	}
	reverse := makeGroup("synthetic reverse order", 44)
	reverseB := makeEvidence(reverse, 44, next.Month, 2147774193, 0, 2500)
	reverseA := makeEvidence(reverse, 44, frozen.Month, 1160872085, 2500000000, 1513097892)
	for _, value := range []MonthlyBillEvidence{reverseB, reverseA} {
		if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, value), true, now); err != nil {
			t.Fatal("reverse transaction reproduction failed", err)
		}
	}
	forwardFirst, err := readPublishedMonthlyBill(frozen)
	if err != nil {
		t.Fatal(err)
	}
	backwardFirst, err := readPublishedMonthlyBill(monthlyFrozenRecord(t, m.storeDB, reverse.ID, reverseA.Month))
	if err != nil {
		t.Fatal(err)
	}
	if forwardFirst.DisplayBalances.Organization.Opening != "2321.740000" || backwardFirst.DisplayBalances.Organization.Opening != "2321.750000" {
		t.Fatal("SQLite order-risk characterization changed; reassess before release")
	}
	gapGroup := makeGroup("synthetic missing middle", 45)
	gapA := makeEvidence(gapGroup, 45, last.AddDate(0, -2, 0).Format("2006-01"), 1160872085, 2500000000, 1513097892)
	gapB := makeEvidence(gapGroup, 45, last.AddDate(0, -1, 0).Format("2006-01"), 2147774193, 0, 2500)
	gapC := makeEvidence(gapGroup, 45, last.Format("2006-01"), 2147771693, 0, 0)
	for _, value := range []MonthlyBillEvidence{gapA, gapC} {
		if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, value), true, now); err != nil {
			t.Fatal(err)
		}
	}
	firstBefore := monthlyFrozenRecord(t, m.storeDB, gapGroup.ID, gapA.Month)
	lastBefore := monthlyFrozenRecord(t, m.storeDB, gapGroup.ID, gapC.Month)
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, gapB), true, now); !errors.Is(err, errMonthlyDisplayChainConflict) {
		t.Fatal("middle-month conflict did not fail closed", err)
	}
	if err := m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id=?", gapGroup.ID).Count(&count).Error; err != nil || count != 2 || !reflect.DeepEqual(firstBefore, monthlyFrozenRecord(t, m.storeDB, gapGroup.ID, gapA.Month)) || !reflect.DeepEqual(lastBefore, monthlyFrozenRecord(t, m.storeDB, gapGroup.ID, gapC.Month)) {
		t.Fatal("rejected middle-month publication modified issued bills", err)
	}
	t.Log("KNOWN_UNRESOLVED_ANCHOR_ORDER_RISK confirmed in SQLite: forward opening 2321.74 vs reverse 2321.75; complete raw middle month blocked between incompatible frozen endpoints; issued rows untouched")
}
