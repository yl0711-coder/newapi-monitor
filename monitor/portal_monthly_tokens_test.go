package monitor

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func monthlyTokenFixture() (MonthlyBillEvidence, time.Time) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, usageCST)
	_, end, _ := monthlyWindow("2026-09", now)
	e := MonthlyBillEvidence{Version: 5, GroupID: 5, Month: "2026-09", ReviewedBy: "synthetic", ReviewedAt: now.Unix(), SourceReference: "synthetic local source only"}
	for _, id := range []int64{201, 202, 203} {
		u := MonthlyBillUserEvidence{UserID: id, Complete: true, WalletOnly: true, Through: end.Unix(), OpeningQuota: 5000000, ClosingQuota: 5000000, ReverseClosingQuota: 5000000,
			Accounting: monthlyAccountingFixture(0, 0), Events: []MonthlyBillCreditEvidence{}, Reversals: []MonthlyBillReversal{},
			TokenUsage: &MonthlyBillTokenUsage{UserID: id, Month: e.Month, Source: "synthetic#tokens", Rows: []MonthlyBillTokenEvidence{}}}
		if id != 203 {
			u.ConsumeQuota, u.RefundQuota = 1000003, 2
			u.ClosingQuota -= u.ConsumeQuota - u.RefundQuota
			u.ReverseClosingQuota = u.ClosingQuota
			u.Accounting = monthlyAccountingFixture(u.ConsumeQuota, u.RefundQuota)
			base := id * 10
			u.TokenUsage.Rows = []MonthlyBillTokenEvidence{{TokenID: base + 2, Name: "同名令牌", ConsumeQuota: 500001}, {TokenID: base + 1, Name: "同名令牌", ConsumeQuota: 500001, RefundQuota: 2}, {TokenID: 0, ConsumeQuota: 1}}
		}
		e.Users = append(e.Users, u)
	}
	return e, now
}

func TestMonthlyTokenConsumptionHierarchyExactAndLegacy(t *testing.T) {
	e, now := monthlyTokenFixture()
	raw, _ := json.Marshal(e)
	decoded, err := decodeMonthlyBillEvidence(raw)
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := buildPublishedMonthlyBill(decoded, now)
	if err != nil || !monthlyStatementAmountsComplete(p) {
		t.Fatal("new token bill rejected", err)
	}
	if p.TokenDetailsVersion != 1 || len(p.Members) != 3 || p.Consumption.Amount != "4.000012" || p.Consumption.Refund != "0.000008" {
		t.Fatal("organization money changed", p)
	}
	for i := range 2 {
		m := p.Members[i]
		if m.TokenConsumption[0].TokenID != m.UserID*10+1 || m.TokenConsumption[0].Name != m.TokenConsumption[1].Name || m.TokenConsumption[2].TokenID != 0 || m.TokenConsumption[2].Amount != "0.000002" {
			t.Fatal("same-name IDs merged, wrong owner or tiny unknown bucket lost", m)
		}
	}
	newRaw, _ := json.Marshal(p)
	if bytes.Contains(newRaw, []byte("synthetic#tokens")) || bytes.Contains(newRaw, []byte("consume_quota")) {
		t.Fatal("private token evidence leaked")
	}
	var roundTrip monthlyStatement
	if json.Unmarshal(newRaw, &roundTrip) != nil || !monthlyStatementAmountsComplete(roundTrip) {
		t.Fatal("saved token bill cannot be read")
	}
	// The optional breakdown must not change a single existing monetary field.
	for i := range e.Users {
		e.Users[i].TokenUsage = nil
	}
	e.Version = monthlyEvidenceVersion(e.Users)
	legacy, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		t.Fatal(err)
	}
	p.TokenDetailsVersion = 0
	for i := range p.Members {
		p.Members[i].TokenConsumption = nil
	}
	if !reflect.DeepEqual(p, legacy) {
		t.Fatal("adding token breakdown changed existing statement fields")
	}
	oldRaw, _ := json.Marshal(legacy)
	if bytes.Contains(oldRaw, []byte("token_consumption")) || bytes.Contains(oldRaw, []byte("token_details_version")) {
		t.Fatal("legacy frozen format changed")
	}
}

func TestMonthlyTokenConsumptionRejectsWrongMoneyScopeAndDuplicates(t *testing.T) {
	for name, mutate := range map[string]func(*MonthlyBillEvidence){
		"missing user":   func(e *MonthlyBillEvidence) { e.Users[1].TokenUsage = nil },
		"foreign user":   func(e *MonthlyBillEvidence) { e.Users[0].TokenUsage.UserID++ },
		"wrong month":    func(e *MonthlyBillEvidence) { e.Users[0].TokenUsage.Month = "2026-08" },
		"missing source": func(e *MonthlyBillEvidence) { e.Users[0].TokenUsage.Source = "" },
		"duplicate token": func(e *MonthlyBillEvidence) {
			e.Users[0].TokenUsage.Rows[1].TokenID = e.Users[0].TokenUsage.Rows[0].TokenID
		},
		"cross user token": func(e *MonthlyBillEvidence) {
			e.Users[1].TokenUsage.Rows[0].TokenID = e.Users[0].TokenUsage.Rows[0].TokenID
		},
		"negative ID": func(e *MonthlyBillEvidence) { e.Users[0].TokenUsage.Rows[0].TokenID = -1 },
		"unsafe ID":   func(e *MonthlyBillEvidence) { e.Users[0].TokenUsage.Rows[0].TokenID = 9007199254740992 },
		"net matches but gross wrong": func(e *MonthlyBillEvidence) {
			e.Users[0].TokenUsage.Rows[0].ConsumeQuota++
			e.Users[0].TokenUsage.Rows[0].RefundQuota++
		},
		"negative amount":     func(e *MonthlyBillEvidence) { e.Users[0].TokenUsage.Rows[0].RefundQuota = -1 },
		"overflow":            func(e *MonthlyBillEvidence) { e.Users[0].TokenUsage.Rows[0].ConsumeQuota = math.MaxInt64 },
		"lost unknown bucket": func(e *MonthlyBillEvidence) { e.Users[0].TokenUsage.Rows = e.Users[0].TokenUsage.Rows[:2] },
		"too many tokens": func(e *MonthlyBillEvidence) {
			e.Users[0].TokenUsage.Rows = make([]MonthlyBillTokenEvidence, monthlyTokenPerUserLimit+1)
		},
		"different wallet cost": func(e *MonthlyBillEvidence) { e.Users[0].Accounting.LogConsumeQuota++ },
		"no version":            func(e *MonthlyBillEvidence) { e.Version = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			e, now := monthlyTokenFixture()
			mutate(&e)
			if _, _, err := buildPublishedMonthlyBill(e, now); err == nil {
				t.Fatal("incorrect token bill accepted")
			}
		})
	}
}

func TestMonthlyTokenConsumptionSavedDamageAndWorkbook(t *testing.T) {
	e, now := monthlyTokenFixture()
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		t.Fatal(err)
	}
	p.Account = "合成组织"
	p.Members[0].Username = "用户甲"
	p.Members[0].TokenConsumption[0].Name = `=WEBSERVICE("https://example.invalid") <令牌>`
	book, err := monthlyStatementXLSX(p)
	if err != nil {
		t.Fatal(err)
	}
	z, err := zip.NewReader(bytes.NewReader(book), int64(len(book)))
	if err != nil {
		t.Fatal(err)
	}
	parts := map[string]string{}
	for _, file := range z.File {
		r, openErr := file.Open()
		if openErr != nil {
			t.Fatal(openErr)
		}
		body, readErr := io.ReadAll(r)
		closeErr := r.Close()
		if readErr != nil || closeErr != nil {
			t.Fatal(readErr, closeErr)
		}
		parts[file.Name] = string(body)
	}
	xml := parts["xl/worksheets/sheet3.xml"]
	for _, want := range []string{`r="E4" s="4"><v>1.00</v>`, `r="F4" s="4"><v>0.00</v>`, `r="G4" s="4"><v>1.00</v>`, "历史未记录令牌", "&lt;令牌&gt;"} {
		if !strings.Contains(xml, want) {
			t.Fatal("workbook cell missing", want, xml)
		}
	}
	if strings.Contains(xml, "<f>") || !strings.Contains(parts["xl/workbook.xml"], `name="令牌消费"`) {
		t.Fatal("token name interpreted as formula or sheet absent")
	}
	for _, mutate := range []func(*monthlyStatement){
		func(p *monthlyStatement) { p.Members[0].TokenConsumption[0].Amount = "1.000004" },
		func(p *monthlyStatement) { p.Members[0].TokenConsumption = nil },
		func(p *monthlyStatement) { p.TokenDetailsVersion = 0 },
		func(p *monthlyStatement) { p.TokenDetailsVersion = 99 },
	} {
		raw, _ := json.Marshal(p)
		var copy monthlyStatement
		if err := json.Unmarshal(raw, &copy); err != nil {
			t.Fatal(err)
		}
		mutate(&copy)
		if monthlyStatementAmountsComplete(copy) {
			t.Fatal("damaged saved token bill accepted")
		}
		if _, err := monthlyStatementXLSX(copy); err == nil {
			t.Fatal("damaged token bill exported")
		}
	}
}

func TestMonthlyTokenConsumptionInputShapeAndCandidate(t *testing.T) {
	input := preparationFixture()
	a := &input.Accounts[0]
	a.TokenMonths = []MonthlyBillTokenUsage{{UserID: a.UserID, Month: "2026-06", Source: "logs#token-usage", Rows: []MonthlyBillTokenEvidence{{TokenID: 123, Name: "测试令牌", ConsumeQuota: 3000000}}}}
	context := monthlyBundleFixture(input)
	bundle := monthlyBundleRun(t, input, context)
	if bundle.Entries[0].Candidate == nil || bundle.Entries[0].Candidate.Users[0].TokenUsage == nil {
		t.Fatal("token month not carried into real candidate", bundle.Entries[0])
	}
	for _, replacement := range []string{`"consume_quota":null`, `"not_consume_quota":3000000`} {
		raw, _ := json.Marshal(input)
		// Target only the unique token block, not the already-verified spans.
		raw = bytes.Replace(raw, []byte(`"name":"测试令牌","consume_quota":3000000`), []byte(`"name":"测试令牌",`+replacement), 1)
		if _, err := DecodeMonthlyPreparationInput(raw); err == nil {
			t.Fatal("missing token amount accepted")
		}
	}
	a.TokenMonths[0].Rows[0].ConsumeQuota++
	if got := monthlyBundleRun(t, input, context); got.Entries[0].Candidate != nil {
		t.Fatal("token total mismatch did not block candidate")
	}
}

func TestMonthlyTokenConsumptionLogNormalization(t *testing.T) {
	e, now := monthlyTokenFixture()
	start, end, _ := monthlyWindow(e.Month, now)
	logs := []MonthlyTokenUsageLog{
		{ID: 1, UserID: 201, At: start.Unix(), Type: 2, BillingSource: "wallet", TokenID: 11, Name: "旧名字", Quota: 500001},
		{ID: 2, UserID: 201, At: start.Unix() + 1, Type: 6, BillingSource: "wallet", TokenID: 11, Name: "改名后的令牌", Quota: 2},
		{ID: 3, UserID: 201, At: start.Unix() + 1, Type: 2, BillingSource: "wallet", TokenID: 12, Name: "改名后的令牌", Quota: 500001},
		{ID: 4, UserID: 201, At: end.Unix() - 1, Type: 2, BillingSource: "wallet", TokenID: 0, Name: "不可凭名字定位", Quota: 1},
	}
	got, err := PrepareMonthlyTokenUsage(201, e.Month, "retained#logs", logs, now)
	if err != nil || len(got.Rows) != 3 || got.Rows[1].TokenID != 11 || got.Rows[1].Name != "改名后的令牌" || got.Rows[1].ConsumeQuota != 500001 || got.Rows[1].RefundQuota != 2 || got.Rows[0].Name != "历史未记录令牌" {
		t.Fatal("raw token logs grouped incorrectly", got, err)
	}
	for i, j := 0, len(logs)-1; i < j; i, j = i+1, j-1 {
		logs[i], logs[j] = logs[j], logs[i]
	}
	again, err := PrepareMonthlyTokenUsage(201, e.Month, "retained#logs", logs, now)
	if err != nil || !reflect.DeepEqual(got, again) {
		t.Fatal("input order changed token totals/name")
	}
	for name, mutate := range map[string]func([]MonthlyTokenUsageLog){
		"duplicate log":               func(rows []MonthlyTokenUsageLog) { rows[1].ID = rows[0].ID },
		"foreign account":             func(rows []MonthlyTokenUsageLog) { rows[0].UserID = 202 },
		"month end excluded":          func(rows []MonthlyTokenUsageLog) { rows[0].At = end.Unix() },
		"month beginning excluded":    func(rows []MonthlyTokenUsageLog) { rows[0].At = start.Unix() - 1 },
		"subscription":                func(rows []MonthlyTokenUsageLog) { rows[0].BillingSource = "subscription" },
		"unknown source":              func(rows []MonthlyTokenUsageLog) { rows[0].BillingSource = "" },
		"recharge is not consumption": func(rows []MonthlyTokenUsageLog) { rows[0].Type = 1 },
		"negative quota":              func(rows []MonthlyTokenUsageLog) { rows[0].Quota = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			copy := append([]MonthlyTokenUsageLog{}, logs...)
			mutate(copy)
			if _, err := PrepareMonthlyTokenUsage(201, e.Month, "retained#logs", copy, now); err == nil {
				t.Fatal("invalid token source accepted")
			}
		})
	}
}

func TestMonthlyTokenConsumptionCustomerReadAndSavedReuse(t *testing.T) {
	m, _, _ := newPortalTestMonitor(t)
	m.cfg.PortalMonthlyStatementEnabled = true
	router := gin.New()
	m.RegisterPortalRoutes(router)
	e, now := monthlyTokenFixture()
	group := CustomerGroup{ID: e.GroupID, Name: "合成多用户组织", PortalEmail: "synthetic-token-bill@example.invalid"}
	if err := m.storeDB.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	for _, u := range e.Users {
		if err := m.storeDB.Create(&TrackedUser{UserID: u.UserID, GroupID: group.ID, Username: "合成用户"}).Error; err != nil {
			t.Fatal(err)
		}
	}
	raw, _ := json.Marshal(e)
	ctx := context.Background()
	firstVersion, err := ImportMonthlyBill(ctx, m.storeDB, raw, true, now)
	if err != nil {
		t.Fatal(err)
	}
	secondVersion, err := ImportMonthlyBill(ctx, m.storeDB, raw, true, now)
	if err != nil || firstVersion != secondVersion {
		t.Fatal("repeat import changed saved token bill", err)
	}
	var saved PortalMonthlyBill
	if err := m.storeDB.First(&saved, "group_id = ? AND month = ?", group.ID, e.Month).Error; err != nil {
		t.Fatal(err)
	}
	if stored, err := readStoredMonthlyEvidence(saved); err != nil || stored.Version != 5 {
		t.Fatal("new token evidence cannot be reused for continuity", err)
	}
	cookie := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(group.ID, group.PortalAuthVer, time.Now().Unix())}
	path := "/api/monthly-statement?month=" + e.Month
	detail := portalDo(router, http.MethodGet, path, "", cookie)
	if detail.Code != 200 || !strings.Contains(detail.Body.String(), `"token_details_version":1`) || strings.Contains(detail.Body.String(), "synthetic#tokens") {
		t.Fatal("customer token detail missing or private source leaked", detail.Code, detail.Body.String())
	}
	var p struct {
		Data monthlyStatement `json:"data"`
	}
	if json.Unmarshal(detail.Body.Bytes(), &p) != nil || len(p.Data.Members) != 3 {
		t.Fatal("wrong member scope")
	}
	book := portalDo(router, http.MethodGet, "/api/monthly-statement/download?month="+e.Month+"&publication="+p.Data.Publication, "", cookie)
	expected, err := monthlyStatementXLSX(p.Data)
	if err != nil || book.Code != 200 || !bytes.Equal(expected, book.Body.Bytes()) {
		t.Fatal("customer download differs from saved page", err, book.Code)
	}
	for range 3 {
		again := portalDo(router, http.MethodGet, path, "", cookie)
		if again.Code != 200 || !bytes.Equal(again.Body.Bytes(), detail.Body.Bytes()) {
			t.Fatal("saved token bill changed during repeated reads")
		}
	}
	other := CustomerGroup{Name: "other", PortalEmail: "other-token@example.invalid"}
	if err := m.storeDB.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	foreign := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(other.ID, other.PortalAuthVer, time.Now().Unix())}
	if w := portalDo(router, http.MethodGet, path, "", foreign); w.Code != 404 {
		t.Fatal("other organization accessed token bill", w.Code)
	}
	if w := portalDo(router, http.MethodGet, "/api/monthly-statement/download?month="+e.Month+"&publication="+strings.Repeat("a", 64), "", cookie); w.Code != 409 {
		t.Fatal("stale version downloaded token bill", w.Code)
	}
}

func TestMonthlyTokenConsumptionKeepsUnknownStatisticsAndBalanceCorrectionSeparate(t *testing.T) {
	e, now := monthlyTokenFixture()
	u := &e.Users[0]
	u.Statistics = &MonthlyBillStatisticsCoverage{}
	u.ClosingQuota++
	u.ReverseClosingQuota++
	u.BalanceCorrection = &MonthlyBillBalanceCorrection{Version: 1, Quota: 1}
	scope := MonthlyCorrectionScopeSHA256(*u, e.Month)
	for i, hash := range []string{strings.Repeat("a", 64), strings.Repeat("b", 64)} {
		u.BalanceCorrection.Observations = append(u.BalanceCorrection.Observations, MonthlyBillCorrectionObservation{
			UserID: u.UserID, Month: e.Month, CapturedAt: now.Unix() - 600 + int64(i)*300,
			SourceSHA256: hash, ScopeSHA256: scope, ClosingQuota: u.ClosingQuota, NetUsedQuota: u.ConsumeQuota - u.RefundQuota - 1,
		})
	}
	raw, _ := json.Marshal(e)
	decoded, err := decodeMonthlyBillEvidence(raw)
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := buildPublishedMonthlyBill(decoded, now)
	if err != nil || !monthlyStatementAmountsComplete(p) {
		t.Fatal("token/statistics/correction combination rejected", err)
	}
	if p.MemberDetailsVersion != 2 || !p.Consumption.RequestsUnknown || !p.Consumption.TokensUnknown || p.Credits.BalanceCorrection != "0.000002" || p.Members[1].BalanceCorrection != "0.000000" || p.Members[0].TokenConsumption[0].Amount != "1.000002" {
		t.Fatal("token feature changed auxiliary statistics or shifted correction into consumption", p)
	}
}

func TestMonthlyTokenCandidateWalletDifferenceIsNotAnIssuanceBypass(t *testing.T) {
	input := monthlyWalletFixture()
	a := &input.Accounts[0]
	a.TokenMonths = []MonthlyBillTokenUsage{{UserID: a.UserID, Month: "2026-06", Source: "logs#original-token-logs", Rows: []MonthlyBillTokenEvidence{{TokenID: 7, Name: "retained token", ConsumeQuota: 200009000}}}}
	before := monthlyReviewJSON(t, input)
	bundle := monthlyBundleRun(t, input, monthlyBundleFixture(input))
	if bundle.ReadyForReview != 1 || bundle.Entries[0].Candidate == nil {
		t.Fatal("existing wallet difference blocked private token candidate", bundle.Entries)
	}
	candidate := bundle.Entries[0].Candidate
	u := candidate.Users[0]
	if u.ConsumeQuota != 200000000 || u.TokenUsage == nil || u.TokenUsage.Rows[0].ConsumeQuota != 200009000 || u.Accounting.LogConsumeQuota != 200009000 {
		t.Fatal("candidate changed user debit or token log amount")
	}
	if !bytes.Equal(before, monthlyReviewJSON(t, input)) {
		t.Fatal("candidate validation mutated original input")
	}
	now := time.Unix(preparationAt("2026-10-20"), 0)
	e := MonthlyBillEvidence{Version: 5, GroupID: candidate.GroupID, Month: candidate.Month, ReviewedBy: "synthetic", ReviewedAt: now.Unix(), SourceReference: "synthetic original source", Users: candidate.Users}
	if _, _, err := buildPublishedMonthlyBill(e, now); err == nil {
		t.Fatal("private candidate validation bypassed final token/money reconciliation")
	}
	if _, err := PrepareAutomaticMonthlyBill(*candidate, now.Unix(), now); err == nil {
		t.Fatal("wallet difference automatically hidden without independent correction evidence")
	}
}
