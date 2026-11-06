package monitor

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func automaticMonthlyFixture(now time.Time, uid int64, correction int64) MonthlyBillCandidate {
	start, end, _ := monthlyWindow("", now)
	if now.Unix() < end.Unix()+310 {
		start, end, _ = monthlyWindow(start.AddDate(0, -1, 0).Format("2006-01"), now)
	}
	u := MonthlyBillUserEvidence{UserID: uid, Complete: true, WalletOnly: true, Through: end.Unix(),
		ExpectedCreditEvents: 1, OpeningQuota: 200000 * 500000, ConsumeQuota: 100000 * 500000, RefundQuota: 3 * 500000,
		Accounting: monthlyAccountingFixture(100000*500000, 3*500000), Requests: 40, Tokens: 10000,
		Events: []MonthlyBillCreditEvidence{{ID: uid, At: start.Unix() + 5, DeltaQuota: 20000 * 500000, Kind: "recharge", Method: "后台充值"}}, Reversals: []MonthlyBillReversal{}}
	u.ClosingQuota = u.OpeningQuota + u.Events[0].DeltaQuota - u.ConsumeQuota + u.RefundQuota + correction
	u.ReverseClosingQuota = u.ClosingQuota
	if correction != 0 {
		scope := MonthlyCorrectionScopeSHA256(u, start.Format("2006-01"))
		u.BalanceCorrection = &MonthlyBillBalanceCorrection{Version: 1, Quota: correction, Observations: []MonthlyBillCorrectionObservation{
			{UserID: uid, Month: start.Format("2006-01"), CapturedAt: end.Unix() + 10, SourceSHA256: monthlyDigest([]byte("first capture")), ScopeSHA256: scope, ClosingQuota: u.ClosingQuota, NetUsedQuota: u.ConsumeQuota - u.RefundQuota - correction},
			{UserID: uid, Month: start.Format("2006-01"), CapturedAt: end.Unix() + 310, SourceSHA256: monthlyDigest([]byte("second capture")), ScopeSHA256: scope, ClosingQuota: u.ClosingQuota, NetUsedQuota: u.ConsumeQuota - u.RefundQuota - correction},
		}}
	}
	return MonthlyBillCandidate{GroupID: 1, Month: start.Format("2006-01"), SourceReference: "synthetic-retained-audit", Users: []MonthlyBillUserEvidence{u}}
}

func TestMonthlyAutomaticMoneyAndPolicy(t *testing.T) {
	now := time.Now()
	for _, correction := range []int64{0, 1, 78768, 909785, monthlyCorrectionQuotaLimit} {
		c := automaticMonthlyFixture(now, 43, correction)
		raw, err := PrepareAutomaticMonthlyBill(c, now.Unix(), now)
		if err != nil {
			t.Fatal(correction, err)
		}
		e, err := decodeMonthlyBillEvidence(raw)
		if err != nil || e.ReviewedBy != "policy:"+monthlyAutomaticPolicy {
			t.Fatal("machine policy missing", err)
		}
		p, _, err := buildPublishedMonthlyBill(e, now)
		if err != nil || !monthlyStatementAmountsComplete(p) || p.Consumption.Amount != "100000.000000" || *p.Credits.RechargeTotal != "20000.000000" || p.Consumption.Refund != "3.000000" {
			t.Fatal("raw charges/funding changed", err)
		}
		if correction == 0 {
			if e.Version != 2 || p.Credits.BalanceCorrection != "" {
				t.Fatal("legacy format changed")
			}
		} else if e.Version != 4 || p.Credits.BalanceCorrection != monthlyQuotaMoney(correction) || p.Members[0].BalanceCorrection != p.Credits.BalanceCorrection {
			t.Fatal("correction not separately disclosed")
		}
		second, err := PrepareAutomaticMonthlyBill(c, e.ReviewedAt, now.Add(time.Second))
		if err != nil || !bytes.Equal(second, raw) {
			t.Fatal("same source was not deterministic", err)
		}
	}
	base := automaticMonthlyFixture(now, 43, 909785)
	for name, change := range map[string]func(*MonthlyBillCandidate){
		"unfavorable":     func(c *MonthlyBillCandidate) { c.Users[0].BalanceCorrection.Quota = -1 },
		"zero correction": func(c *MonthlyBillCandidate) { c.Users[0].BalanceCorrection.Quota = 0 },
		"absolute cap":    func(c *MonthlyBillCandidate) { c.Users[0].BalanceCorrection.Quota = monthlyCorrectionQuotaLimit + 1 },
		"relative cap": func(c *MonthlyBillCandidate) {
			c.Users[0].ConsumeQuota = 500000
			c.Users[0].Accounting.LogConsumeQuota = 500000
		},
		"one observation": func(c *MonthlyBillCandidate) {
			c.Users[0].BalanceCorrection.Observations = c.Users[0].BalanceCorrection.Observations[:1]
		},
		"duplicate source": func(c *MonthlyBillCandidate) {
			c.Users[0].BalanceCorrection.Observations[1].SourceSHA256 = c.Users[0].BalanceCorrection.Observations[0].SourceSHA256
		},
		"scope changed":     func(c *MonthlyBillCandidate) { c.Users[0].Events[0].DeltaQuota++ },
		"different account": func(c *MonthlyBillCandidate) { c.Users[0].BalanceCorrection.Observations[0].UserID++ },
		"different month":   func(c *MonthlyBillCandidate) { c.Users[0].BalanceCorrection.Observations[0].Month = "2020-01" },
		"future capture": func(c *MonthlyBillCandidate) {
			c.Users[0].BalanceCorrection.Observations[1].CapturedAt = now.Unix() + 1
		},
		"overwritten consumption": func(c *MonthlyBillCandidate) { c.Users[0].ConsumeQuota-- },
		"overwritten refund":      func(c *MonthlyBillCandidate) { c.Users[0].RefundQuota++ },
		"incomplete sources":      func(c *MonthlyBillCandidate) { c.Users[0].Complete = false },
		"subscription":            func(c *MonthlyBillCandidate) { c.Users[0].WalletOnly = false },
		"one unit mismatch":       func(c *MonthlyBillCandidate) { c.Users[0].ClosingQuota++ },
		"missing actual reverse":  func(c *MonthlyBillCandidate) { c.Users[0].ReverseClosingQuota++ },
		"overflow":                func(c *MonthlyBillCandidate) { c.Users[0].OpeningQuota = math.MaxInt64 },
		"implicit reversal":       func(c *MonthlyBillCandidate) { c.Users[0].Reversals = []MonthlyBillReversal{{CreditID: 1, DebitID: 2}} },
	} {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(base)
			var c MonthlyBillCandidate
			_ = json.Unmarshal(raw, &c)
			change(&c)
			if _, err := PrepareAutomaticMonthlyBill(c, now.Unix(), now); err == nil {
				t.Fatal("invalid money or proof accepted")
			}
		})
	}
	for _, mutate := range []func(*MonthlyBillCandidate){
		func(c *MonthlyBillCandidate) { c.Users[0].BalanceCorrection.Observations[0].ClosingQuota-- },
		func(c *MonthlyBillCandidate) { c.Users[0].BalanceCorrection.Observations[1].NetUsedQuota++ },
		func(c *MonthlyBillCandidate) { c.Users[0].BalanceCorrection.Observations[1].CapturedAt-- },
	} {
		raw, _ := json.Marshal(base)
		var c MonthlyBillCandidate
		_ = json.Unmarshal(raw, &c)
		mutate(&c)
		if _, err := PrepareAutomaticMonthlyBill(c, now.Unix(), now); !errors.Is(err, ErrMonthlyObservationUnstable) {
			t.Fatal("transient capture mismatch not classified for retry", err)
		}
	}
}

func TestMonthlyAutomaticPublicationCustomerAndWorkbook(t *testing.T) {
	m, _, _ := newPortalTestMonitor(t)
	t.Cleanup(m.Close)
	m.cfg.PortalMonthlyStatementEnabled = true
	r := gin.New()
	m.RegisterPortalRoutes(r)
	g := CustomerGroup{Name: "synthetic-org", PortalEmail: "auto@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 43, GroupID: g.ID, Username: "account-one"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 44, GroupID: g.ID, Username: "account-two"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	c := automaticMonthlyFixture(now, 43, 909785)
	c.GroupID = g.ID
	c.Users = append(c.Users, automaticMonthlyFixture(now, 44, 0).Users[0])
	var first PortalMonthlyBill
	for i := range 2 {
		if _, err := IssueMonthlyBillAutomatically(context.Background(), m.storeDB, c, now.Unix(), now); err != nil {
			t.Fatal(err)
		}
		var current PortalMonthlyBill
		if err := m.storeDB.First(&current, "group_id = ? AND month = ?", g.ID, c.Month).Error; err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = current
		} else if first != current {
			t.Fatal("idempotent issuance changed frozen bill")
		}
	}
	p, err := readPublishedMonthlyBill(first)
	if err != nil || !monthlyStatementAmountsComplete(p) || p.Members[1].BalanceCorrection != "0.000000" {
		t.Fatal("account isolation", err)
	}
	if _, err := readStoredMonthlyEvidence(first); err != nil {
		t.Fatal(err)
	}
	// Changing both member corrections can preserve org sums; individual account
	// equations still reject it. A common organization total is insufficient.
	p.Members[0].BalanceCorrection, p.Members[1].BalanceCorrection = "0.000000", "1.819570"
	if monthlyStatementAmountsComplete(p) {
		t.Fatal("cross-account correction accepted")
	}
	p, _ = readPublishedMonthlyBill(first)
	book, err := monthlyStatementXLSX(p)
	if err != nil {
		t.Fatal(err)
	}
	z, err := zip.NewReader(bytes.NewReader(book), int64(len(book)))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range z.File {
		if file.Name != "xl/worksheets/sheet1.xml" && file.Name != "xl/worksheets/sheet2.xml" {
			continue
		}
		f, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(data, []byte("余额校正（元）")) || !bytes.Contains(data, []byte("<v>1.82</v>")) {
			t.Fatal("workbook missing correction", file.Name)
		}
	}
	cookie := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, now.Unix())}
	path := "/api/monthly-statement?month=" + c.Month
	paths := []string{"/api/monthly-statements?month=" + c.Month, path, "/api/monthly-statement/download?month=" + c.Month + "&publication=" + p.Publication}
	for _, target := range paths {
		if w := portalDo(r, "GET", target, "", cookie); w.Code != 409 {
			t.Fatal("old client hides correction", target, w.Code)
		}
	}
	req := func(path string) *http.Request {
		req, err := http.NewRequest("GET", path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.AddCookie(cookie)
		req.Header.Set("X-Monthly-Accounting", "balance-correction-v1")
		req.Header.Set("X-Monthly-Display", "cents-chain-v1")
		return req
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req(path))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"balance_correction":"1.819570"`) || strings.Contains(w.Body.String(), "source_sha256") || strings.Contains(w.Body.String(), "policy:") {
		t.Fatal("customer response", w.Code, w.Body.String())
	}
	for _, target := range paths {
		response := httptest.NewRecorder()
		r.ServeHTTP(response, req(target))
		if response.Code != 200 {
			t.Fatal("current client could not read bill", target, response.Code)
		}
	}
	// A repeated generation with changed money cannot silently replace a bill.
	c.Users[0].Requests++
	if _, err := IssueMonthlyBillAutomatically(context.Background(), m.storeDB, c, now.Unix(), now); err == nil {
		t.Fatal("changed evidence overwrote bill")
	}
	var after PortalMonthlyBill
	_ = m.storeDB.First(&after, "group_id = ? AND month = ?", g.ID, c.Month).Error
	if after != first {
		t.Fatal("failed regeneration changed issued bill")
	}
}

func TestMonthlyAutomaticCorrectionStrictJSON(t *testing.T) {
	now := time.Now()
	raw, err := PrepareAutomaticMonthlyBill(automaticMonthlyFixture(now, 43, 909785), now.Unix(), now)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"version", "quota", "observations", "user_id", "month", "captured_at", "source_sha256", "scope_sha256", "closing_quota", "net_used_quota"} {
		// Mutate the nested correction only, not similarly named root fields.
		for _, mode := range []string{"missing", "null", "case-alias"} {
			copyRoot := map[string]any{}
			_ = json.Unmarshal(raw, &copyRoot)
			copyUser := copyRoot["users"].([]any)[0].(map[string]any)
			copyObject := copyUser["balance_correction"].(map[string]any)
			if field != "version" && field != "quota" && field != "observations" {
				copyObject = copyObject["observations"].([]any)[0].(map[string]any)
			}
			value := copyObject[field]
			delete(copyObject, field)
			if mode == "null" {
				copyObject[field] = nil
			}
			if mode == "case-alias" {
				copyObject[strings.ToUpper(field)] = value
			}
			bad, _ := json.Marshal(copyRoot)
			if _, err := decodeMonthlyBillEvidence(bad); err == nil {
				t.Fatal("ambiguous correction accepted", field, mode)
			}
		}
	}
	legacy := bytes.Replace(raw, []byte(`"version":4`), []byte(`"version":2`), 1)
	if _, err := decodeMonthlyBillEvidence(legacy); err == nil {
		t.Fatal("old evidence version accepted correction")
	}
}

func TestMonthlyAutomaticCorrectionCrossMonth(t *testing.T) {
	m, _, _ := newPortalTestMonitor(t)
	t.Cleanup(m.Close)
	g := CustomerGroup{Name: "carry-month", PortalEmail: "carry@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 43, GroupID: g.ID, Username: "single-wallet"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	later := automaticMonthlyFixture(now, 43, 0)
	start, _, _ := monthlyWindow(later.Month, now)
	earlierMonth := start.AddDate(0, -1, 0).Format("2006-01")
	earlier := automaticMonthlyFixture(now, 43, 909785)
	earlier.Month, earlier.GroupID = earlierMonth, g.ID
	previousStart, previousEnd, _ := monthlyWindow(earlier.Month, now)
	u := &earlier.Users[0]
	u.Through = previousEnd.Unix()
	u.Events[0].At = previousStart.Unix() + 5
	for i := range u.BalanceCorrection.Observations {
		o := &u.BalanceCorrection.Observations[i]
		o.Month = earlier.Month
		o.CapturedAt = previousEnd.Unix() + 10 + int64(i)*300
		o.ScopeSHA256 = MonthlyCorrectionScopeSHA256(*u, earlier.Month)
	}
	ctx := context.Background()
	if _, err := IssueMonthlyBillAutomatically(ctx, m.storeDB, earlier, now.Unix(), now); err != nil {
		t.Fatal(err)
	}
	// The prior correction is part of the next opening, not another new credit.
	later.GroupID = g.ID
	later.Users[0].OpeningQuota = u.ClosingQuota
	v := &later.Users[0]
	v.ClosingQuota = v.OpeningQuota + v.Events[0].DeltaQuota - v.ConsumeQuota + v.RefundQuota
	v.ReverseClosingQuota = v.ClosingQuota
	if _, err := IssueMonthlyBillAutomatically(ctx, m.storeDB, later, now.Unix(), now); err != nil {
		t.Fatal(err)
	}
	var records []PortalMonthlyBill
	if err := m.storeDB.Order("month").Find(&records).Error; err != nil || len(records) != 2 {
		t.Fatal(err)
	}
	a, _ := readPublishedMonthlyBill(records[0])
	b, _ := readPublishedMonthlyBill(records[1])
	if *a.MonthEndBalance != *b.MonthOpeningBalance || b.Credits.BalanceCorrection != "" || b.Consumption.Amount != a.Consumption.Amount || *b.Credits.RechargeTotal != *a.Credits.RechargeTotal {
		t.Fatal("correction was repeated or logs changed")
	}
	// A subsequent revision cannot replace even a correctly reconciling month.
	later.Users[0].OpeningQuota--
	later.Users[0].ClosingQuota--
	later.Users[0].ReverseClosingQuota--
	if _, err := IssueMonthlyBillAutomatically(ctx, m.storeDB, later, now.Unix(), now); err == nil {
		t.Fatal("different month boundary replaced issued bill")
	}
}

func TestMonthlyAutomaticPublicationAuthorizationAndUnknownStatistics(t *testing.T) {
	m, _, _ := newPortalTestMonitor(t)
	t.Cleanup(m.Close)
	g := CustomerGroup{Name: "versioned-authorization", PortalEmail: "scope@example.invalid", PortalAuthVer: 1}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 43, GroupID: g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	c := automaticMonthlyFixture(now, 43, 909785)
	c.GroupID = g.ID
	c.Users[0].Statistics = &MonthlyBillStatisticsCoverage{}
	c.Users[0].Requests, c.Users[0].Tokens = 0, 0
	raw, err := PrepareAutomaticMonthlyBill(c, now.Unix(), now)
	if err != nil {
		t.Fatal(err)
	}
	e, err := decodeMonthlyBillEvidence(raw)
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil || !monthlyStatementAmountsComplete(p) || p.MemberDetailsVersion != 2 || !p.Consumption.RequestsUnknown || !p.Consumption.TokensUnknown {
		t.Fatal("correction lost unknown statistics", err)
	}
	scope, err := loadMonthlyBillAuthorization(context.Background(), m.storeDB, g.ID)
	if err != nil {
		t.Fatal(err)
	}
	expected := MonthlyBillExpectedAuthorization{GroupID: g.ID, AuthVersion: 1, MemberFingerprint: scope.Fingerprint, MemberManifest: scope.Manifest}
	if err := m.storeDB.Model(&CustomerGroup{}).Where("id=?", g.ID).Update("portal_auth_ver", 2).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := ImportMonthlyBillForAuthorization(context.Background(), m.storeDB, raw, expected, now); err == nil {
		t.Fatal("stale owner published after revocation")
	}
	var count int64
	if err := m.storeDB.Model(&PortalMonthlyBill{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("unauthorized publication changed database", count, err)
	}
	expected.AuthVersion = 2
	if _, err := ImportMonthlyBillForAuthorization(context.Background(), m.storeDB, raw, expected, now); err != nil {
		t.Fatal(err)
	}
}
