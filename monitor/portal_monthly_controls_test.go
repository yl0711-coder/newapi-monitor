package monitor

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func monthlyFlatEvidence(gid int64, month string, balance int64, now time.Time) MonthlyBillEvidence {
	_, end, _ := monthlyWindow(month, now)
	return MonthlyBillEvidence{Version: 2, GroupID: gid, Month: month, ReviewedBy: "synthetic reviewer", ReviewedAt: now.Unix(), SourceReference: "synthetic balance anchors", Users: []MonthlyBillUserEvidence{{
		UserID: 43, Complete: true, WalletOnly: true, Through: end.Unix(), OpeningQuota: balance, ClosingQuota: balance, ReverseClosingQuota: balance,
		Accounting: monthlyAccountingFixture(0, 0), Events: []MonthlyBillCreditEvidence{}, Reversals: []MonthlyBillReversal{},
	}}}
}

func monthlyTestRaw(t *testing.T, e MonthlyBillEvidence) []byte {
	t.Helper()
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestMonthlyBothQuotaChangeRowsRetainAffectedUser(t *testing.T) {
	m, _, _ := newPortalTestMonitor(t)
	g := CustomerGroup{Name: "synthetic quota changes", PortalEmail: "bill@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	for _, u := range []TrackedUser{{UserID: 122, GroupID: g.ID, Username: "<debit-user>"}, {UserID: 125, GroupID: g.ID}} {
		if err := m.storeDB.Create(&u).Error; err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	e := monthlyFlatEvidence(g.ID, start.Format("2006-01"), 0, now)
	u := &e.Users[0]
	u.UserID, u.OpeningQuota, u.ExpectedCreditEvents = 122, 800*500000, 1
	u.Events = []MonthlyBillCreditEvidence{{ID: 1, At: start.Unix() + 10, DeltaQuota: -800 * 500000, Kind: "adjustment", Method: "余额扣减"}}
	v := *u
	v.UserID, v.OpeningQuota, v.ClosingQuota, v.ReverseClosingQuota = 125, 0, 800*500000, 800*500000
	v.Events = []MonthlyBillCreditEvidence{{ID: 2, At: start.Unix() + 29, DeltaQuota: 800 * 500000, Kind: "recharge", Method: "后台充值"}}
	e.Users = append(e.Users, v)
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, e), true, now); err != nil {
		t.Fatal(err)
	}
	var r PortalMonthlyBill
	if err := m.storeDB.First(&r, "group_id = ? AND month = ?", g.ID, e.Month).Error; err != nil {
		t.Fatal(err)
	}
	p, err := readPublishedMonthlyBill(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Credits.Rows) != 2 || p.Credits.Rows[0].UserID != 122 || p.Credits.Rows[0].Username != "<debit-user>" || p.Credits.Rows[0].Amount != "-800.000000" || p.Credits.Rows[1].UserID != 125 || p.Credits.Rows[1].Amount != "800.000000" {
		t.Fatalf("source changes hidden or attribution lost: %+v", p.Credits.Rows)
	}
	if *p.Credits.RechargeTotal != "800.000000" || *p.Credits.RechargeCount != 1 || p.Credits.Adjustment != "-800.000000" || *p.MonthOpeningBalance != *p.MonthEndBalance {
		t.Fatal("quota totals changed")
	}
	data, err := monthlyStatementXLSX(p)
	if err != nil {
		t.Fatal(err)
	}
	if path := os.Getenv("MONTHLY_CHANGED_USERS_XLSX"); path != "" {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if path := os.Getenv("MONTHLY_CHANGED_USERS_DATASET"); path != "" {
		dataset := struct {
			Account    string                      `json:"account"`
			CapturedAt string                      `json:"captured_at"`
			Synthetic  bool                        `json:"synthetic"`
			Months     map[string]monthlyStatement `json:"months"`
		}{p.Account, now.In(usageCST).Format("2006-01-02 15:04:05"), true, map[string]monthlyStatement{p.Month: p}}
		raw, err := json.Marshal(dataset)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	z, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	var sheet string
	for _, f := range z.File {
		if f.Name == "xl/worksheets/sheet1.xml" {
			r, err := f.Open()
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(r)
			r.Close()
			if err != nil {
				t.Fatal(err)
			}
			sheet = string(raw)
		}
	}
	for _, want := range []string{monthlyXML("<debit-user>\nID 122"), monthlyXML("—\nID 125"), "-800.00", "800.00", "被操作账号"} {
		if !strings.Contains(sheet, want) {
			t.Fatal("workbook missing", want)
		}
	}
}

func TestMonthlyActualDebitNotReplacedByLogAmount(t *testing.T) {
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	e := monthlyEvidenceFixture(1, start.Format("2006-01"), now)
	e.Users[0].Accounting.LogConsumeQuota += 909785
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		t.Fatal(err)
	}
	if p.Consumption.Amount != "500.000000" || p.Consumption.Net != "500.000000" || *p.MonthEndBalance != "1700.000000" {
		t.Fatal("log discrepancy changed customer debit")
	}
	public, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"log_consume_quota", "reconciliation_source", "synthetic opening", "909785"} {
		if strings.Contains(string(public), private) {
			t.Fatal("private reconciliation leaked", private)
		}
	}
	// A changed actual debit must reconcile to independent closing evidence;
	// merely changing the amount and retaining old boundaries remains blocked.
	e.Users[0].ConsumeQuota++
	if _, _, err := buildPublishedMonthlyBill(e, now); err == nil {
		t.Fatal("unreconciled actual debit accepted")
	}
}

func TestMonthlyActualDebitProofRequired(t *testing.T) {
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	base := monthlyEvidenceFixture(1, start.Format("2006-01"), now)
	for name, change := range map[string]func(*MonthlyBillEvidence){
		"legacy is not actual proof": func(e *MonthlyBillEvidence) { e.Version = 1; e.Users[0].Accounting = nil },
		"missing proof":              func(e *MonthlyBillEvidence) { e.Users[0].Accounting = nil },
		"log-only basis":             func(e *MonthlyBillEvidence) { e.Users[0].Accounting.Basis = "logs" },
		"opening absent":             func(e *MonthlyBillEvidence) { e.Users[0].Accounting.OpeningSource = " " },
		"closing absent":             func(e *MonthlyBillEvidence) { e.Users[0].Accounting.ClosingSource = "" },
		"movements absent":           func(e *MonthlyBillEvidence) { e.Users[0].Accounting.MovementSource = "" },
		"refund evidence absent":     func(e *MonthlyBillEvidence) { e.Users[0].Accounting.RefundSource = "" },
		"review absent":              func(e *MonthlyBillEvidence) { e.Users[0].Accounting.ReconciliationSource = "" },
		"invalid logged amount":      func(e *MonthlyBillEvidence) { e.Users[0].Accounting.LogConsumeQuota = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			var e MonthlyBillEvidence
			if err := json.Unmarshal(monthlyTestRaw(t, base), &e); err != nil {
				t.Fatal(err)
			}
			change(&e)
			if _, _, err := buildPublishedMonthlyBill(e, now); err == nil {
				t.Fatal("unproven actual wallet debit accepted")
			}
		})
	}
	raw := string(monthlyTestRaw(t, base))
	for _, key := range []string{"log_consume_quota", "log_refund_quota"} {
		var shape map[string]any
		if err := json.Unmarshal([]byte(raw), &shape); err != nil {
			t.Fatal(err)
		}
		a := shape["users"].([]any)[0].(map[string]any)["accounting"].(map[string]any)
		for _, null := range []bool{false, true} {
			delete(a, key)
			if null {
				a[key] = nil
			}
			b, err := json.Marshal(shape)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeMonthlyBillEvidence(b); err == nil {
				t.Fatal("missing/null log amount became zero", key)
			}
		}
	}
}

func TestMonthlyAdjacentBalancesBothImportOrders(t *testing.T) {
	for _, newestFirst := range []bool{false, true} {
		for _, mismatch := range []bool{false, true} {
			t.Run(stringBool(newestFirst)+"-"+stringBool(mismatch), func(t *testing.T) {
				m, _, _ := newPortalTestMonitor(t)
				g := CustomerGroup{Name: "continuity"}
				if err := m.storeDB.Create(&g).Error; err != nil {
					t.Fatal(err)
				}
				if err := m.storeDB.Create(&TrackedUser{UserID: 43, GroupID: g.ID}).Error; err != nil {
					t.Fatal(err)
				}
				now := time.Now()
				last, _, _ := monthlyWindow("", now)
				a := monthlyFlatEvidence(g.ID, last.AddDate(0, -1, 0).Format("2006-01"), 100*500000, now)
				b := monthlyFlatEvidence(g.ID, last.Format("2006-01"), 100*500000, now)
				if mismatch {
					b.Users[0].OpeningQuota += 500000
					b.Users[0].ClosingQuota += 500000
					b.Users[0].ReverseClosingQuota += 500000
				}
				if newestFirst {
					a, b = b, a
				}
				if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, a), true, now); err != nil {
					t.Fatal(err)
				}
				for _, apply := range []bool{false, true} {
					_, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, b), apply, now)
					if mismatch && (err == nil || !strings.Contains(err.Error(), "月界余额")) {
						t.Fatal("unexplained cross-month balance accepted", err)
					}
					if !mismatch && err != nil {
						t.Fatal(err)
					}
				}
				var count int64
				if err := m.storeDB.Model(&PortalMonthlyBill{}).Count(&count).Error; err != nil {
					t.Fatal(err)
				}
				want := int64(2)
				if mismatch {
					want = 1
				}
				if count != want {
					t.Fatal("unexpected writes", count, want)
				}
			})
		}
	}
}

func TestMonthlyStoredEvidenceCorruptionRejected(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	if err := f.m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id = ? AND month = ?", f.g.ID, f.e.Month).Update("evidence", "corrupt synthetic evidence").Error; err != nil {
		t.Fatal(err)
	}
	for _, apply := range []bool{false, true} {
		if _, err := ImportMonthlyBill(context.Background(), f.m.storeDB, f.raw, apply, f.now); err == nil {
			t.Fatal("stored evidence body was trusted without hashing")
		}
	}
	var current PortalMonthlyBill
	if err := f.m.storeDB.First(&current, "group_id = ? AND month = ?", f.g.ID, f.e.Month).Error; err != nil {
		t.Fatal(err)
	}
	if current.Evidence != "corrupt synthetic evidence" {
		t.Fatal("import silently repaired original evidence")
	}
	// Corruption must not prevent explicit archival/withdrawal for recovery.
	if err := f.withdraw(true); err != nil {
		t.Fatal(err)
	}
	monthlyRecoveryCounts(t, f.m, 0, 1)
}

func TestMonthlyFrozenBillDuringFactsMaintenance(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	f.m.cfg.UsageFactsClassificationMigrationEnabled = true
	f.m.cfg.UsageFactsFullHistoryEnabled = true
	if !f.m.usageFactsReadRequested() || f.m.usageFactsReadEnabled() {
		t.Fatal("invalid synthetic maintenance state")
	}
	for _, path := range []string{"/api/monthly-statements", "/api/monthly-statement?month=" + f.e.Month, "/api/monthly-statement/download?month=" + f.e.Month + "&publication=" + f.version} {
		w := portalDo(f.r, http.MethodGet, path, "", f.cookie)
		if w.Code != 200 {
			t.Fatalf("frozen bill affected by unrelated facts: %d %s", w.Code, w.Body.String())
		}
	}
	if w := portalDo(f.r, http.MethodGet, "/api/overview", "", f.cookie); w.Code != 503 {
		t.Fatal("existing live usage fail-close was changed", w.Code)
	}
}

func TestMonthlyCorruptMonthIsolatedFromList(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	last, _, _ := monthlyWindow(f.e.Month, f.now)
	e := monthlyFlatEvidence(f.g.ID, last.AddDate(0, -1, 0).Format("2006-01"), 0, f.now)
	if _, err := ImportMonthlyBill(context.Background(), f.m.storeDB, monthlyTestRaw(t, e), true, f.now); err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id = ? AND month = ?", f.g.ID, e.Month).Update("payload", "corrupt synthetic payload").Error; err != nil {
		t.Fatal(err)
	}
	if w := portalDo(f.r, "GET", "/api/monthly-statement?month="+e.Month, "", f.cookie); w.Code != 503 {
		t.Fatal("damaged bill served", w.Code)
	}
	w := portalDo(f.r, "GET", "/api/monthly-statements", "", f.cookie)
	var result struct {
		Data struct {
			Rows        []monthlyStatementRow `json:"rows"`
			Unavailable []string              `json:"unavailable_months"`
		}
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || len(result.Data.Rows) != 1 || result.Data.Rows[0].Month != f.e.Month || len(result.Data.Unavailable) != 1 || result.Data.Unavailable[0] != e.Month {
		t.Fatalf("valid bill hidden: %d %s", w.Code, w.Body.String())
	}
	w = portalDo(f.r, "GET", "/api/monthly-statements?month="+e.Month, "", f.cookie)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"rows":[]`) {
		t.Fatal("all-invalid page must remain recoverable, not fake zero bills", w.Body.String())
	}
}

func TestMonthlyAuthorizationRevocationAndRejoin(t *testing.T) {
	t.Run("missing control", func(t *testing.T) {
		f := newMonthlyRecoveryFixture(t)
		if err := f.m.storeDB.Delete(&UsageMemberControl{}, "user_id = ?", 43).Error; err != nil {
			t.Fatal(err)
		}
		if w := portalDo(f.r, "GET", "/api/monthly-statement?month="+f.e.Month, "", f.cookie); w.Code != 503 {
			t.Fatal("broken membership accepted", w.Code)
		}
	})
	t.Run("rejoin revision", func(t *testing.T) {
		f := newMonthlyRecoveryFixture(t)
		if _, err := f.m.removeUsageMember(context.Background(), 43, usageMemberMutationMeta{RequestID: "monthly-remove"}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.m.addUsageMember(context.Background(), TrackedUser{UserID: 43}, f.g.ID, usageMemberMutationMeta{RequestID: "monthly-rejoin"}); err != nil {
			t.Fatal(err)
		}
		w := portalDo(f.r, "GET", "/api/monthly-statement?month="+f.e.Month, "", f.cookie)
		if w.Code != 200 || !strings.Contains(w.Body.String(), f.version) {
			t.Fatal("historical publication lost after member rejoin", w.Code, w.Body.String())
		}
		if _, err := ImportMonthlyBill(context.Background(), f.m.storeDB, f.raw, true, f.now); err == nil {
			t.Fatal("idempotent replay changed old authorization scope")
		}
	})
	t.Run("revoke during response", func(t *testing.T) {
		f := newMonthlyRecoveryFixture(t)
		r := gin.New()
		r.GET("/probe", func(c *gin.Context) { c.Set("portalGID", f.g.ID); c.Set("portalAuthVer", f.g.PortalAuthVer) }, f.m.portalMonthlyAuthorizationGuard(func(c *gin.Context) {
			c.JSON(200, gin.H{"secret": "must-not-leak"})
			if err := f.m.storeDB.Model(&CustomerGroup{}).Where("id = ?", f.g.ID).Update("portal_auth_ver", f.g.PortalAuthVer+1).Error; err != nil {
				t.Error(err)
			}
		}))
		w := portalDo(r, "GET", "/probe", "")
		if w.Code != 401 || strings.Contains(w.Body.String(), "must-not-leak") {
			t.Fatal("stale authorized response committed", w.Code, w.Body.String())
		}
	})
}

func stringBool(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func TestMonthlyVisibleEventLimitDoesNotTruncateDebits(t *testing.T) {
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	e := monthlyFlatEvidence(1, start.Format("2006-01"), 0, now)
	for i := 0; i < monthlyCreditLimit; i++ {
		delta, kind, method := int64(500000), "recharge", "后台充值"
		if i%2 == 1 {
			delta, kind, method = -500000, "adjustment", "余额扣减"
		}
		e.Users[0].Events = append(e.Users[0].Events, MonthlyBillCreditEvidence{ID: int64(i + 1), At: start.Unix() + int64(i), DeltaQuota: delta, Kind: kind, Method: method})
	}
	e.Users[0].ExpectedCreditEvents = len(e.Users[0].Events)
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Credits.Rows) != monthlyCreditLimit || p.Credits.Adjustment != "-1000.000000" || *p.Credits.RechargeTotal != "1000.000000" {
		t.Fatal("visible event rows truncated or miscounted")
	}
	started := time.Now()
	file, err := monthlyStatementXLSX(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("2000 signed rows: workbook bytes=%d elapsed=%s", len(file), time.Since(started))
	e.Users[0].Events = append(e.Users[0].Events, MonthlyBillCreditEvidence{ID: 2001, At: start.Unix() + 2001, DeltaQuota: 500000, Kind: "recharge", Method: "后台充值"})
	e.Users[0].ExpectedCreditEvents++
	e.Users[0].ClosingQuota = 500000
	e.Users[0].ReverseClosingQuota = 500000
	if _, _, err := buildPublishedMonthlyBill(e, now); err == nil {
		t.Fatal("over-limit evidence silently accepted")
	}
}

func TestMonthlyAdjacentUserDifferencesCannotCancel(t *testing.T) {
	m, _, _ := newPortalTestMonitor(t)
	g := CustomerGroup{Name: "synthetic member boundaries"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{43, 44} {
		if err := m.storeDB.Create(&TrackedUser{UserID: id, GroupID: g.ID}).Error; err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	prior := monthlyFlatEvidence(g.ID, start.AddDate(0, -1, 0).Format("2006-01"), 100*500000, now)
	u := prior.Users[0]
	u.UserID = 44
	prior.Users = append(prior.Users, u)
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, prior), true, now); err != nil {
		t.Fatal(err)
	}
	next := monthlyFlatEvidence(g.ID, start.Format("2006-01"), 101*500000, now)
	u = next.Users[0]
	u.UserID = 44
	u.OpeningQuota = 99 * 500000
	u.ClosingQuota = u.OpeningQuota
	u.ReverseClosingQuota = u.OpeningQuota
	next.Users = append(next.Users, u)
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, next), true, now); err == nil {
		t.Fatal("equal organization total hid opposing per-member differences")
	}
}

func TestMonthlyMembershipChangedDuringResponseRechecksOrganization(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	r := gin.New()
	calls := 0
	r.GET("/api/monthly-statement", func(c *gin.Context) {
		c.Set("portalGID", f.g.ID)
		c.Set("portalAuthVer", f.g.PortalAuthVer)
	}, f.m.portalMonthlyAuthorizationGuard(func(c *gin.Context) {
		calls++
		f.m.portalPublishedMonthlyStatement(c)
		if calls == 1 {
			if _, err := f.m.removeUsageMember(context.Background(), 43, usageMemberMutationMeta{RequestID: "monthly-response-removal"}); err != nil {
				t.Error(err)
			}
		}
	}))
	w := portalDo(r, "GET", "/api/monthly-statement?month="+f.e.Month, "")
	if calls != 2 || w.Code != 200 || !strings.Contains(w.Body.String(), "500.000000") || !strings.Contains(w.Body.String(), f.version) {
		t.Fatal("historical bill was not reauthorized for its original organization", calls, w.Code, w.Body.String())
	}
}

func TestMonthlyLegacyManifestMigrationPreservesEvidenceAndWithdrawal(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	legacy := f.old
	legacy.MemberManifest = ""
	e := f.e
	e.Version = 1
	e.Users[0].Accounting = nil
	legacy.Evidence = string(monthlyTestRaw(t, e))
	legacy.EvidenceHash = monthlyDigest([]byte(legacy.Evidence))
	if err := f.m.storeDB.Save(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	// Model a pre-repair prototype database: add the column without rewriting
	// immutable payload/evidence or granting the old row new authorization.
	if err := f.m.storeDB.Migrator().DropColumn(&PortalMonthlyBill{}, "MemberManifest"); err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.AutoMigrate(&PortalMonthlyBill{}); err != nil {
		t.Fatal(err)
	}
	var got PortalMonthlyBill
	if err := f.m.storeDB.First(&got, "group_id = ? AND month = ?", f.g.ID, f.e.Month).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, legacy) {
		t.Fatal("additive migration rewrote legacy publication")
	}
	if _, err := readStoredMonthlyEvidence(got); err != nil {
		t.Fatal("legacy evidence no longer auditable", err)
	}
	oldVersionBody, err := json.Marshal(struct {
		GroupID                                              int64
		Month, MemberFingerprint, PayloadHash, PublicationID string
		PublishedAt                                          int64
	}{legacy.GroupID, legacy.Month, legacy.MemberFingerprint, legacy.PayloadHash, legacy.PublicationID, legacy.PublishedAt})
	if err != nil {
		t.Fatal(err)
	}
	if monthlyBillVersion(got) != monthlyDigest(oldVersionBody) {
		t.Fatal("legacy withdrawal token format changed")
	}
	if w := portalDo(f.r, "GET", "/api/monthly-statement?month="+f.e.Month, "", f.cookie); w.Code != 404 {
		t.Fatal("old prototype acquired authorization without review", w.Code)
	}
	if _, err := ImportMonthlyBill(context.Background(), f.m.storeDB, []byte(legacy.Evidence), true, f.now); err == nil {
		t.Fatal("legacy log evidence upgraded to actual wallet proof")
	}
	if err := WithdrawMonthlyBill(context.Background(), f.m.storeDB, f.g.ID, f.e.Month, monthlyBillVersion(got), "synthetic reviewer", "review before reissue", true, f.now); err != nil {
		t.Fatal("legacy publication cannot be recovered", err)
	}
	var audit PortalMonthlyBillWithdrawal
	if err := f.m.storeDB.First(&audit).Error; err != nil {
		t.Fatal(err)
	}
	var archived PortalMonthlyBill
	if err := json.Unmarshal([]byte(audit.Record), &archived); err != nil || !reflect.DeepEqual(archived, legacy) {
		t.Fatal("legacy evidence lost during archival", err)
	}
}
