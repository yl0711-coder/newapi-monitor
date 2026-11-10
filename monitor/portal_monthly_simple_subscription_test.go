package monitor

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"gorm.io/gorm"
)

// Subscription-paid usage is split out of the wallet: balances follow the
// wallet only, the bill's subscription part lists the orders opened in the
// month (configured price, trial at zero) and the subscription-paid usage per
// token, and the customer workbook shows both.
func TestSimpleMonthlyGenerationSubscription(t *testing.T) {
	m, svc, r, cookie, g, src := simpleTestService(t)
	ctx := context.Background()
	facts := svc.facts.(*simpleFakeFacts)
	octStart, novStart := simpleCST(2026, 10, 1, 0), simpleCST(2026, 11, 1, 0)
	src.snap.Subscriptions = []simpleSubscription{
		{ID: 16, UserID: 202, PlanID: 2, Start: simpleCST(2026, 9, 12, 10), End: simpleCST(2026, 9, 26, 10), Status: "expired", Source: "admin"},
		{ID: 18, UserID: 202, PlanID: 2, Start: simpleCST(2026, 10, 8, 9), End: simpleCST(2026, 11, 7, 9), Status: "active", Source: "admin"},
	}
	var wantSub int64 // October subscription-paid quota of token 2022
	for _, d := range facts.days {
		if d.UserID == 202 && d.TokenID == 2022 && d.DateTS >= octStart && d.DateTS < novStart && d.Consume > 0 {
			part := d.Consume / 2
			src.snap.SubDays = append(src.snap.SubDays, simpleSubDay{UserID: 202, TokenID: 2022, DateTS: d.DateTS, Consume: part})
			src.snap.Quota[202] += part // the wallet did not pay it
			wantSub += part
		}
	}
	run := func(month, state string) {
		t.Helper()
		if _, err := svc.SubmitMonthly(ctx, g.ID, g.PortalAuthVer, month); err != nil {
			t.Fatal(err)
		}
		if !svc.runOne(ctx) {
			t.Fatalf("no job ran for %s", month)
		}
		var job PortalMonthlySimpleJob
		m.storeDB.First(&job, "group_id = ? AND month = ?", g.ID, month)
		if job.State != state {
			t.Fatalf("job %s ended %s (want %s): %s", month, job.State, state, job.LastError)
		}
	}
	// Without a configured plan the month that opens a subscription is held.
	run("2026-10", simpleJobReview)
	svc.SetSubscriptionPlans(map[int64]simplePlan{2: {Name: "GPT模型调用服务月卡", PriceCents: 1000000}}, map[int64]simplePlan{16: {Trial: true}})
	m.storeDB.Where("group_id = ?", g.ID).Delete(&PortalMonthlySimpleJob{})
	run("2026-10", simpleJobDone)
	run("2026-09", simpleJobDone)

	var audits []PortalMonthlySimpleAudit
	m.storeDB.Where("user_id = ?", 202).Order("month").Find(&audits)
	for _, a := range audits {
		if a.WalletResidual != 0 {
			t.Fatalf("subscription usage leaked into the wallet check: %+v", a)
		}
	}
	var factsOct int64
	for _, d := range facts.days {
		if d.UserID == 202 && d.DateTS >= octStart && d.DateTS < novStart {
			factsOct += d.Consume
		}
	}
	if audits[1].LogConsume != factsOct-wantSub {
		t.Fatalf("wallet consumption %d, want %d", audits[1].LogConsume, factsOct-wantSub)
	}

	read := func(month string) simpleSubscriptionBill {
		t.Helper()
		var row PortalMonthlySimpleSubscription
		if err := m.storeDB.First(&row, "group_id = ? AND month = ?", g.ID, month).Error; err != nil {
			t.Fatalf("subscription part %s: %v", month, err)
		}
		sum := sha256.Sum256([]byte(row.Data))
		var bill PortalMonthlyBill
		m.storeDB.First(&bill, "group_id = ? AND month = ?", g.ID, month)
		if row.Digest != hex.EncodeToString(sum[:]) || !strings.Contains(bill.Evidence, simpleSubscriptionRefPrefix+row.Digest) {
			t.Fatalf("subscription digest not bound to the bill %s", month)
		}
		var sb simpleSubscriptionBill
		if err := json.Unmarshal([]byte(row.Data), &sb); err != nil {
			t.Fatal(err)
		}
		return sb
	}
	oct := read("2026-10")
	if len(oct.Orders) != 1 || oct.Orders[0].ID != 18 || oct.Orders[0].PriceCents != 1000000 || oct.Orders[0].Trial || !oct.Orders[0].Active {
		t.Fatalf("october orders: %+v", oct.Orders)
	}
	if len(oct.Users) != 1 || oct.Users[0].UserID != 202 || len(oct.Users[0].Tokens) != 1 || oct.Users[0].Tokens[0].TokenID != 2022 ||
		oct.Users[0].ConsumeCents != simpleHalfUpCents(wantSub) {
		t.Fatalf("october subscription usage: %+v (want about %d quota)", oct.Users, wantSub)
	}
	sep := read("2026-09")
	if len(sep.Orders) != 1 || sep.Orders[0].ID != 16 || sep.Orders[0].PriceCents != 0 || !sep.Orders[0].Trial || sep.Orders[0].Active {
		t.Fatalf("september trial: %+v", sep.Orders)
	}

	// The detail carries the subscription part; the workbook shows it.
	w := portalDo(r, "GET", "/api/monthly-statement?month=2026-10", "", cookie)
	var detail struct {
		Data struct {
			Publication string `json:"publication"`
		} `json:"data"`
		Subscription *simpleSubscriptionBill `json:"subscription"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &detail); err != nil || detail.Subscription == nil || detail.Data.Publication == "" {
		t.Fatalf("detail: %d %s", w.Code, w.Body.String())
	}
	req := httptest.NewRequest("GET", "http://127.0.0.1/api/monthly-statement/download?month=2026-10&publication="+detail.Data.Publication, nil)
	req.AddCookie(cookie)
	req.Header.Set("X-Monthly-Statistics", "nullable-v1")
	req.Header.Set("X-Monthly-Accounting", "balance-correction-v1")
	req.Header.Set("X-Monthly-Display", "cents-chain-v1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("download: %d %s", rec.Code, rec.Body.String())
	}
	zr, err := zip.NewReader(bytes.NewReader(rec.Body.Bytes()), int64(rec.Body.Len()))
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, f := range zr.File {
		rc, _ := f.Open()
		b, _ := io.ReadAll(rc)
		rc.Close()
		files[f.Name] = string(b)
	}
	wb := files["xl/workbook.xml"]
	if !(strings.Index(wb, "订阅服务账单") < strings.Index(wb, "账号收支") && strings.Index(wb, "账号收支") < strings.Index(wb, "月度账单") && strings.Contains(wb, "令牌消费")) {
		t.Fatalf("sheet order: %s", wb)
	}
	if !strings.Contains(files["xl/worksheets/sheet1.xml"], "SUB-202-000018") || !strings.Contains(files["xl/worksheets/sheet1.xml"], "人民币壹万元整") {
		t.Fatal("subscription sheet content")
	}
	if !strings.Contains(files["xl/worksheets/sheet2.xml"], "其中套餐内（元）") || !strings.Contains(files["xl/worksheets/sheet3.xml"], "其中 余额支付（元）") ||
		!strings.Contains(files["xl/worksheets/sheet4.xml"], "套餐内消费（元）") {
		t.Fatal("subscription columns missing")
	}
	// A bill without subscription activity keeps the plain template.
	if _, err := simpleStatementXLSX(monthlyStatement{}, nil); err == nil {
		t.Fatal("incomplete statement exported")
	}

	// Once issued, the subscription part is never rewritten: not when the
	// issued-bill lookup fails, and not when the lookup misses a legacy row,
	// even though the inputs have changed since (the plan price was edited).
	var issuedSub PortalMonthlySimpleSubscription
	m.storeDB.First(&issuedSub, "group_id = ? AND month = ?", g.ID, "2026-10")
	svc.SetSubscriptionPlans(map[int64]simplePlan{2: {Name: "GPT模型调用服务月卡", PriceCents: 900000}}, map[int64]simplePlan{16: {Trial: true}})
	failLookup := true
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register("test:fail-saved-bill-lookup", func(db *gorm.DB) {
		if failLookup && db.Statement.Table == "portal_monthly_bills" && len(db.Statement.Selects) > 0 {
			_ = db.AddError(errors.New("disk I/O error"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SubmitMonthly(ctx, g.ID, g.PortalAuthVer, "2026-10"); err != nil {
		t.Fatal(err)
	}
	svc.runOne(ctx)
	failLookup = false
	var fp string
	m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id = ? AND month = ?", g.ID, "2026-10").Pluck("member_fingerprint", &fp)
	m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id = ? AND month = ?", g.ID, "2026-10").Update("member_fingerprint", "")
	m.storeDB.Model(&PortalMonthlySimpleJob{}).Where("group_id = ? AND month = ?", g.ID, "2026-10").Updates(map[string]any{"state": simpleJobQueued, "lease_until": 0, "next_at": 0})
	svc.runOne(ctx)
	m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id = ? AND month = ?", g.ID, "2026-10").Update("member_fingerprint", fp)
	var afterSub PortalMonthlySimpleSubscription
	m.storeDB.First(&afterSub, "group_id = ? AND month = ?", g.ID, "2026-10")
	if afterSub.Digest != issuedSub.Digest || afterSub.Data != issuedSub.Data {
		t.Fatal("subscription part of an issued bill was rewritten")
	}
	if w := portalDo(r, "GET", "/api/monthly-statement?month=2026-10", "", cookie); w.Code != 200 {
		t.Fatalf("issued bill unreadable after regeneration: %d %s", w.Code, w.Body.String())
	}
}

// The status endpoint carries a coarse progress hint while a request waits or
// runs: requests ahead in the queue, elapsed time and a typical duration.
func TestSimpleMonthlyGenerationProgressHint(t *testing.T) {
	_, svc, r, cookie, _, _ := simpleTestService(t)
	for _, month := range []string{"2026-09", "2026-10"} {
		if w := generationHTTPDo(r, "POST", "", `{"month":"`+month+`"}`, cookie, nil); w.Code != 202 {
			t.Fatalf("submit %s: %d %s", month, w.Code, w.Body.String())
		}
	}
	var view struct {
		State    string                     `json:"state"`
		Progress *MonthlyGenerationProgress `json:"progress"`
	}
	w := generationHTTPDo(r, "GET", "?month=2026-10", "", cookie, nil)
	if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil || view.State != "queued" || view.Progress == nil ||
		view.Progress.QueueAhead != 1 || view.Progress.EstimatedSeconds != 2*simpleTypicalSeconds {
		t.Fatalf("queued progress: %d %s", w.Code, w.Body.String())
	}
	for i := 0; i < 2; i++ {
		if !svc.runOne(context.Background()) {
			t.Fatalf("job %d did not run", i)
		}
	}
	w = generationHTTPDo(r, "GET", "?month=2026-10", "", cookie, nil)
	if strings.Contains(w.Body.String(), `"progress"`) || !strings.Contains(w.Body.String(), `"state":"available"`) {
		t.Fatalf("finished bill must not carry progress: %s", w.Body.String())
	}
}
