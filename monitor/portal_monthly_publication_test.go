package monitor

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func monthlyEvidenceFixture(gid int64, month string, now time.Time) MonthlyBillEvidence {
	start, end, _ := monthlyWindow(month, now)
	return MonthlyBillEvidence{Version: 2, GroupID: gid, Month: month, ReviewedBy: "test-reviewer", ReviewedAt: now.Unix(), SourceReference: "synthetic local test; not customer data", Users: []MonthlyBillUserEvidence{{UserID: 43, Complete: true, WalletOnly: true, Through: end.Unix(), ExpectedCreditEvents: 5, OpeningQuota: 0, ClosingQuota: 850000000, ReverseClosingQuota: 850000000, ConsumeQuota: 250000000, Accounting: monthlyAccountingFixture(250000000, 0), Requests: 2, Tokens: 17788983329, Events: []MonthlyBillCreditEvidence{
		{ID: 1, At: start.Unix() + 1, DeltaQuota: 500000, Kind: "gift", Method: "注册赠送"},
		{ID: 2, At: start.Unix() + 2, DeltaQuota: 99500000, Kind: "recharge", Method: "后台充值"},
		{ID: 3, At: start.Unix() + 3, DeltaQuota: 1000000000, Kind: "recharge", Method: "后台充值"},
		{ID: 4, At: start.Unix() + 4, DeltaQuota: -1000000000, Kind: "adjustment", Method: "余额扣减"},
		{ID: 5, At: start.Unix() + 5, DeltaQuota: 1000000000, Kind: "recharge", Method: "后台充值"},
	}, Reversals: []MonthlyBillReversal{{CreditID: 3, DebitID: 4, Reason: "confirmed operator correction"}}}}}
}

func monthlyAccountingFixture(consume, refund int64) *MonthlyBillAccountingEvidence {
	return &MonthlyBillAccountingEvidence{Basis: "independent_wallet_reconciliation", OpeningSource: "synthetic opening", ClosingSource: "synthetic closing", MovementSource: "synthetic movements", RefundSource: "synthetic refunds", ReconciliationSource: "synthetic independent wallet reconciliation", LogConsumeQuota: consume, LogRefundQuota: refund}
}

func TestMonthlyPublicationEvidence(t *testing.T) {
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	base := monthlyEvidenceFixture(1, start.Format("2006-01"), now)
	p, ids, err := buildPublishedMonthlyBill(base, now)
	if err != nil || len(ids) != 1 || len(p.Credits.Rows) != 3 || *p.Credits.RechargeCount != 2 || *p.Credits.RechargeTotal != "2199.000000" || *p.Credits.GiftTotal != "1.000000" || p.Credits.KnownTotal != "2200.000000" || p.Credits.Adjustment != "0.000000" || *p.MonthEndBalance != "1700.000000" {
		t.Fatalf("unexpected bill: %+v %v", p, err)
	}
	raw, _ := json.Marshal(base)
	for _, bad := range []string{
		strings.Replace(string(raw), `"opening_quota":0,`, "", 1),
		strings.Replace(string(raw), `"opening_quota":0`, `"opening_quota":null`, 1),
		strings.Replace(string(raw), `"opening_quota":0`, `"opening_quota":0,"opening_quota":0`, 1),
	} {
		if _, err := decodeMonthlyBillEvidence([]byte(bad)); err == nil {
			t.Fatal("missing, null or duplicate field accepted")
		}
	}
	for name, change := range map[string]func(*MonthlyBillEvidence){
		"future review":     func(e *MonthlyBillEvidence) { e.ReviewedAt = now.Unix() + 1 },
		"current month":     func(e *MonthlyBillEvidence) { e.Month = now.In(usageCST).Format("2006-01") },
		"missing coverage":  func(e *MonthlyBillEvidence) { e.Users[0].Complete = false },
		"subscription":      func(e *MonthlyBillEvidence) { e.Users[0].WalletOnly = false },
		"missing event":     func(e *MonthlyBillEvidence) { e.Users[0].ExpectedCreditEvents++ },
		"duplicate user":    func(e *MonthlyBillEvidence) { e.Users = append(e.Users, e.Users[0]) },
		"duplicate event":   func(e *MonthlyBillEvidence) { e.Users[0].Events[1].ID = 1 },
		"out of month":      func(e *MonthlyBillEvidence) { e.Users[0].Events[0].At = start.AddDate(0, 1, 0).Unix() },
		"bad method":        func(e *MonthlyBillEvidence) { e.Users[0].Events[0].Method = "=WEBSERVICE(\"x\")" },
		"different closing": func(e *MonthlyBillEvidence) { e.Users[0].ClosingQuota++ },
		"reverse mismatch":  func(e *MonthlyBillEvidence) { e.Users[0].ReverseClosingQuota++ },
		"bad reversal":      func(e *MonthlyBillEvidence) { e.Users[0].Reversals[0].CreditID = 2 },
		"repeat reversal": func(e *MonthlyBillEvidence) {
			e.Users[0].Reversals = append(e.Users[0].Reversals, e.Users[0].Reversals[0])
		},
		"unsafe JS count": func(e *MonthlyBillEvidence) { e.Users[0].Tokens = 9007199254740992 },
	} {
		t.Run(name, func(t *testing.T) {
			var e MonthlyBillEvidence
			_ = json.Unmarshal(raw, &e)
			change(&e)
			if _, _, err := buildPublishedMonthlyBill(e, now); err == nil {
				t.Fatal("invalid evidence accepted")
			}
		})
	}
	// Without explicit review, all three events remain; equal amounts are not
	// sufficient to infer a correction. The debit is shown separately.
	base.Users[0].Reversals = nil
	p, _, err = buildPublishedMonthlyBill(base, now)
	if err != nil || len(p.Credits.Rows) != 5 || *p.Credits.RechargeCount != 3 || p.Credits.Adjustment != "-2000.000000" {
		t.Fatal("automatic cancellation", err)
	}
	base.Users[0].RefundQuota = 500000
	base.Users[0].ClosingQuota += 500000
	base.Users[0].ReverseClosingQuota += 500000
	p, _, err = buildPublishedMonthlyBill(base, now)
	if err != nil || p.Consumption.Net != "499.000000" {
		t.Fatal("refund", err)
	}
	for _, v := range [][]byte{append(append([]byte{}, raw...), []byte(" {}")...), []byte(`{"unknown":1}`), bytes.Repeat([]byte(" "), (2<<20)+1)} {
		if _, err := decodeMonthlyBillEvidence(v); err == nil {
			t.Fatal("bad input accepted")
		}
	}
}

func TestPortalMonthlyPublishedLocalOnly(t *testing.T) {
	m, _, off := newPortalTestMonitor(t)
	if w := portalDo(off, "GET", "/api/monthly-statements", ""); w.Code != 404 {
		t.Fatal("flag defaults on")
	}
	m.cfg.PortalMonthlyStatementEnabled = true
	r := gin.New()
	m.RegisterPortalRoutes(r)
	g := CustomerGroup{Name: "customer", PortalEmail: "customer@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 43, GroupID: g.ID, Username: "account"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	month := start.Format("2006-01")
	cookie := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, now.Unix())}
	ctx := context.Background()
	e := monthlyEvidenceFixture(g.ID, month, now)
	raw, _ := json.Marshal(e)
	if _, err := ImportMonthlyBill(ctx, m.storeDB, raw, false, now); err != nil {
		t.Fatal(err)
	}
	var count int64
	m.storeDB.Model(&PortalMonthlyBill{}).Count(&count)
	if count != 0 {
		t.Fatal("dry-run wrote")
	}
	w := portalDo(r, "GET", "/api/monthly-statements", "", cookie)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"rows":[]`) {
		t.Fatalf("empty: %d %s", w.Code, w.Body.String())
	}
	for i := 0; i < 2; i++ {
		if _, err := ImportMonthlyBill(ctx, m.storeDB, raw, true, now); err != nil {
			t.Fatal(err)
		}
	}
	m.storeDB.Model(&PortalMonthlyBill{}).Count(&count)
	if count != 1 {
		t.Fatal("non-idempotent import")
	}
	e.ReviewedBy = "different"
	changed, _ := json.Marshal(e)
	if _, err := ImportMonthlyBill(ctx, m.storeDB, changed, true, now); err == nil {
		t.Fatal("changed evidence overwrote bill")
	}
	for _, path := range []string{"/api/monthly-statements", "/api/monthly-statement", "/api/monthly-statement/download"} {
		if got := portalDo(r, "GET", path, ""); got.Code != 401 {
			t.Fatal("anonymous", path, got.Code)
		}
		if got := portalDo(r, "GET", path+"?month="+now.In(usageCST).Format("2006-01"), "", cookie); got.Code != 400 {
			t.Fatal("current month", path, got.Code)
		}
	}
	w = portalDo(r, "GET", "/api/monthly-statement?month="+month+"&uid=99&group_id=999", "", cookie)
	var reply struct {
		Data monthlyStatement `json:"data"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &reply)
	if w.Code != 200 || reply.Data.Account != "customer" || !reply.Data.Published || reply.Data.MonthEndBalance == nil {
		t.Fatalf("detail %d %s", w.Code, w.Body.String())
	}
	for _, credit := range reply.Data.Credits.Rows {
		if credit.UserID != 43 || credit.Username != "account" {
			t.Fatalf("missing recharge account: %+v", credit)
		}
	}
	for _, s := range []string{"source_reference", "reviewed_by", "evidence_hash", "confirmed operator", "current_balance\":\""} {
		if strings.Contains(w.Body.String(), s) {
			t.Fatal("internal leak", s)
		}
	}
	download := portalDo(r, "GET", "/api/monthly-statement/download?month="+month+"&publication="+reply.Data.Publication, "", cookie)
	if download.Code != 200 || download.Header().Get("Content-Type") != monthlyXLSXMIME || download.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("download %d %s", download.Code, download.Body.String())
	}
	if !bytes.HasPrefix(download.Body.Bytes(), []byte("PK")) {
		t.Fatal("not XLSX")
	}
	// Only actual published months; no synthetic zero months back to 2020.
	for i := 1; i <= 3; i++ {
		other := monthlyEvidenceFixture(g.ID, start.AddDate(0, -i, 0).Format("2006-01"), now)
		// Earlier synthetic months must join the next month's opening exactly.
		other.Users[0].OpeningQuota = -int64(i) * 850000000
		other.Users[0].ClosingQuota = -int64(i-1) * 850000000
		other.Users[0].ReverseClosingQuota = other.Users[0].ClosingQuota
		b, _ := json.Marshal(other)
		if _, err := ImportMonthlyBill(ctx, m.storeDB, b, true, now); err != nil {
			t.Fatal(err)
		}
	}
	w = portalDo(r, "GET", "/api/monthly-statements", "", cookie)
	var listing struct {
		Data struct {
			Rows []monthlyStatementRow `json:"rows"`
			Next string                `json:"next"`
		} `json:"data"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &listing)
	if len(listing.Data.Rows) != 3 || listing.Data.Next != start.AddDate(0, -3, 0).Format("2006-01") {
		t.Fatalf("page: %s", w.Body.String())
	}
	w = portalDo(r, "GET", "/api/monthly-statements?before="+listing.Data.Next, "", cookie)
	_ = json.Unmarshal(w.Body.Bytes(), &listing)
	if len(listing.Data.Rows) != 1 || listing.Data.Next != "" {
		t.Fatal("last page")
	}
	if w := portalDo(r, "GET", "/api/monthly-statements?month="+month+"&before="+month, "", cookie); w.Code != 400 {
		t.Fatal("conflicting filters")
	}
	// Saved bills retain their original scope; roster changes do not hide them.
	if err := m.storeDB.Create(&TrackedUser{UserID: 44, GroupID: g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	if w := portalDo(r, "GET", "/api/monthly-statement?month="+month, "", cookie); w.Code != 200 {
		t.Fatal("historical bill hidden by membership change", w.Code)
	}
	if _, err := ImportMonthlyBill(ctx, m.storeDB, raw, false, now); err == nil {
		t.Fatal("wrong members accepted")
	}
	m.storeDB.Model(&g).Update("portal_auth_ver", 1)
	if w := portalDo(r, "GET", "/api/monthly-statements", "", cookie); w.Code != 401 {
		t.Fatal("revocation", w.Code)
	}
}

func TestMonthlyWorkbookSafetyAndParity(t *testing.T) {
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	p, _, err := buildPublishedMonthlyBill(monthlyEvidenceFixture(1, start.Format("2006-01"), now), now)
	if err != nil {
		t.Fatal(err)
	}
	p.Account = "=WEBSERVICE(\"https://invalid\") <客户>\x00"
	payload, err := monthlyStatementXLSX(p)
	if err != nil {
		t.Fatal(err)
	}
	z, err := zip.NewReader(bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	var sheet string
	for _, f := range z.File {
		r, _ := f.Open()
		raw, _ := io.ReadAll(r)
		r.Close()
		dec := xml.NewDecoder(bytes.NewReader(raw))
		for {
			_, err = dec.Token()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(f.Name, err)
			}
		}
		if f.Name == "xl/worksheets/sheet1.xml" {
			sheet = string(raw)
		}
	}
	for _, v := range []string{"2199.00", "2200.00", "1700.00", "500.00", "17788983329/100000000", "注册赠送", "后台充值", "&lt;客户&gt;"} {
		if !strings.Contains(sheet, v) {
			t.Fatal("missing value", v)
		}
	}
	if strings.Contains(sheet, "<f>WEBSERVICE") || strings.Contains(sheet, "\x00") {
		t.Fatal("unsafe cell")
	}
	if path := os.Getenv("MONTHLY_XLSX_TEST_OUTPUT"); path != "" {
		if err := os.WriteFile(path, payload, 0600); err != nil {
			t.Fatal(err)
		}
	}
	recordRaw, _ := json.Marshal(p)
	record := PortalMonthlyBill{Month: p.Month, Payload: string(recordRaw), PayloadHash: monthlyDigest(recordRaw)}
	if _, err := readPublishedMonthlyBill(record); err != nil {
		t.Fatal(err)
	}
	record.Payload += " "
	if _, err := readPublishedMonthlyBill(record); err == nil {
		t.Fatal("checksum bypass")
	}
	p.Published = false
	if _, err := monthlyStatementXLSX(p); err == nil {
		t.Fatal("unpublished exported")
	}
}
