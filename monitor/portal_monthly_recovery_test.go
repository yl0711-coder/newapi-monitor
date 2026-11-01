package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type monthlyRecoveryFixture struct {
	m       *Monitor
	r       *gin.Engine
	g       CustomerGroup
	e       MonthlyBillEvidence
	raw     []byte
	cookie  *http.Cookie
	now     time.Time
	version string
	old     PortalMonthlyBill
}

func newMonthlyRecoveryFixture(t *testing.T) monthlyRecoveryFixture {
	t.Helper()
	m, _, _ := newPortalTestMonitor(t)
	m.cfg.PortalMonthlyStatementEnabled = true
	r := gin.New()
	m.RegisterPortalRoutes(r)
	g := CustomerGroup{Name: "synthetic-bill", PortalEmail: "review@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 43, GroupID: g.ID, Username: "bill-user"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	e := monthlyEvidenceFixture(g.ID, start.Format("2006-01"), now)
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, raw, true, now); err != nil {
		t.Fatal(err)
	}
	var old PortalMonthlyBill
	if err := m.storeDB.First(&old, "group_id = ? AND month = ?", g.ID, e.Month).Error; err != nil {
		t.Fatal(err)
	}
	version, err := MonthlyBillPublicationVersion(context.Background(), m.storeDB, g.ID, e.Month, now)
	if err != nil {
		t.Fatal(err)
	}
	return monthlyRecoveryFixture{m: m, r: r, g: g, e: e, raw: raw, now: now, version: version, old: old,
		cookie: &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, now.Unix())}}
}

func (f monthlyRecoveryFixture) withdraw(apply bool) error {
	return WithdrawMonthlyBill(context.Background(), f.m.storeDB, f.g.ID, f.e.Month, f.version, "local-reviewer", "核验成员范围后重新出账", apply, f.now)
}

func monthlyRecoveryCounts(t *testing.T, m *Monitor, active, archived int64) {
	t.Helper()
	var a, b int64
	if err := m.storeDB.Model(&PortalMonthlyBill{}).Count(&a).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&PortalMonthlyBillWithdrawal{}).Count(&b).Error; err != nil {
		t.Fatal(err)
	}
	if a != active || b != archived {
		t.Fatalf("active=%d archived=%d; want %d %d", a, b, active, archived)
	}
}

func TestMonthlyWithdrawalAndMembershipReissue(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	if err := f.withdraw(false); err != nil {
		t.Fatal(err)
	}
	monthlyRecoveryCounts(t, f.m, 1, 0)
	if err := f.m.storeDB.Create(&TrackedUser{UserID: 44, GroupID: f.g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	path := "/api/monthly-statement?month=" + f.e.Month
	if w := portalDo(f.r, "GET", path, "", f.cookie); w.Code != 200 {
		t.Fatal("historical bill hidden before explicit withdrawal", w.Code)
	}
	if err := f.withdraw(true); err != nil {
		t.Fatal(err)
	}
	if err := f.withdraw(true); err != nil {
		t.Fatal("idempotent withdrawal failed", err)
	}
	monthlyRecoveryCounts(t, f.m, 0, 1)
	var audit PortalMonthlyBillWithdrawal
	if err := f.m.storeDB.First(&audit).Error; err != nil {
		t.Fatal(err)
	}
	var old PortalMonthlyBill
	if err := json.Unmarshal([]byte(audit.Record), &old); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(old, f.old) || monthlyDigest([]byte(audit.Record)) != audit.RecordHash || audit.Actor != "local-reviewer" || audit.Reason == "" {
		t.Fatal("original evidence/audit lost")
	}
	// The legacy public projection is gone too; an older reader cannot show a withdrawn bill.
	var legacyCount int64
	if err := f.m.storeDB.Table("portal_monthly_bills").Where("group_id = ? AND month = ?", f.g.ID, f.e.Month).Count(&legacyCount).Error; err != nil || legacyCount != 0 {
		t.Fatal("legacy reader saw withdrawn bill", err)
	}
	if _, err := ImportMonthlyBill(context.Background(), f.m.storeDB, f.raw, true, f.now); err == nil {
		t.Fatal("stale evidence bypassed member check")
	}
	_, end, _ := monthlyWindow(f.e.Month, f.now)
	f.e.Users = append(f.e.Users, MonthlyBillUserEvidence{UserID: 44, Complete: true, WalletOnly: true, Through: end.Unix(), Accounting: monthlyAccountingFixture(0, 0), Events: []MonthlyBillCreditEvidence{}, Reversals: []MonthlyBillReversal{}})
	raw, err := json.Marshal(f.e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ImportMonthlyBill(context.Background(), f.m.storeDB, raw, true, f.now); err != nil {
		t.Fatal(err)
	}
	monthlyRecoveryCounts(t, f.m, 1, 1)
	if err := f.withdraw(true); !errors.Is(err, errMonthlyPublicationChanged) {
		t.Fatal("old withdrawal must not delete reissue", err)
	}
	w := portalDo(f.r, "GET", path, "", f.cookie)
	var reply struct {
		Data monthlyStatement `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || reply.Data.Consumption.Amount != "500.000000" || reply.Data.Publication == f.version {
		t.Fatal("reissue not readable", w.Body.String())
	}
	for _, forbidden := range []string{"local-reviewer", "source_reference", "reversals", "核验成员范围"} {
		if strings.Contains(w.Body.String(), forbidden) {
			t.Fatal("audit leaked", forbidden)
		}
	}
	w = portalDo(f.r, "GET", "/api/monthly-statement/download?month="+f.e.Month+"&publication="+f.version, "", f.cookie)
	if w.Code != 409 {
		t.Fatal("old viewed version downloaded new bill", w.Code)
	}
	w = portalDo(f.r, "GET", "/api/monthly-statement/download?month="+f.e.Month+"&publication="+reply.Data.Publication, "", f.cookie)
	if w.Code != 200 || !strings.HasPrefix(w.Body.String(), "PK") {
		t.Fatal("reissue workbook unavailable", w.Code)
	}
}

func TestMonthlySameEvidenceReissueGetsNewVersion(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	if err := f.withdraw(true); err != nil {
		t.Fatal(err)
	}
	// Same bytes and same second: nonce must still distinguish two issuances.
	if _, err := ImportMonthlyBill(context.Background(), f.m.storeDB, f.raw, true, f.now); err != nil {
		t.Fatal(err)
	}
	version, err := MonthlyBillPublicationVersion(context.Background(), f.m.storeDB, f.g.ID, f.e.Month, f.now)
	if err != nil || version == f.version {
		t.Fatal("publication ABA", version, err)
	}
	if err := checkMonthlyPublicationCurrent(context.Background(), f.m.storeDB, f.old); !errors.Is(err, errMonthlyPublicationChanged) {
		t.Fatal("stale version accepted", err)
	}
	if err := f.withdraw(true); !errors.Is(err, errMonthlyPublicationChanged) {
		t.Fatal("stale command accepted", err)
	}
	monthlyRecoveryCounts(t, f.m, 1, 1)
}

func TestMonthlyWithdrawalAtomicRollback(t *testing.T) {
	for _, table := range []string{"portal_monthly_bills", "portal_monthly_bill_withdrawals"} {
		t.Run(table, func(t *testing.T) {
			f := newMonthlyRecoveryFixture(t)
			sql := "CREATE TRIGGER monthly_test_failure BEFORE DELETE ON portal_monthly_bills BEGIN SELECT RAISE(ABORT, 'test write failure'); END"
			if table == "portal_monthly_bill_withdrawals" {
				sql = "CREATE TRIGGER monthly_test_failure BEFORE INSERT ON portal_monthly_bill_withdrawals BEGIN SELECT RAISE(ABORT, 'test archive failure'); END"
			}
			if err := f.m.storeDB.Exec(sql).Error; err != nil {
				t.Fatal(err)
			}
			if err := f.withdraw(true); err == nil {
				t.Fatal("injected failure ignored")
			}
			monthlyRecoveryCounts(t, f.m, 1, 0)
			var current PortalMonthlyBill
			if err := f.m.storeDB.First(&current, "group_id = ? AND month = ?", f.g.ID, f.e.Month).Error; err != nil || !reflect.DeepEqual(current, f.old) {
				t.Fatal("failed withdrawal mutated source", err)
			}
		})
	}
}

func TestMonthlyWithdrawalRejectsUnsafeTargets(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	for _, args := range []struct {
		gid                           int64
		month, version, actor, reason string
	}{
		{0, f.e.Month, f.version, "actor", "reason"},
		{f.g.ID, "", f.version, "actor", "reason"},
		{f.g.ID, f.now.In(usageCST).Format("2006-01"), f.version, "actor", "reason"},
		{f.g.ID, f.e.Month, strings.Repeat("0", 64), "actor", "reason"},
		{f.g.ID, f.e.Month, f.version, "", "reason"},
		{f.g.ID, f.e.Month, f.version, "actor", ""},
		{f.g.ID + 100, f.e.Month, f.version, "actor", "reason"},
	} {
		if err := WithdrawMonthlyBill(context.Background(), f.m.storeDB, args.gid, args.month, args.version, args.actor, args.reason, true, f.now); err == nil {
			t.Fatal("unsafe target accepted", args)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WithdrawMonthlyBill(ctx, f.m.storeDB, f.g.ID, f.e.Month, f.version, "actor", "reason", true, f.now); err == nil {
		t.Fatal("canceled operation succeeded")
	}
	monthlyRecoveryCounts(t, f.m, 1, 0)
}

func TestMonthlyWithdrawalLegacyPublication(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	// Existing local bills from the initial implementation have a NULL new column.
	if err := f.m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id = ? AND month = ?", f.g.ID, f.e.Month).Update("publication_id", nil).Error; err != nil {
		t.Fatal(err)
	}
	var err error
	f.version, err = MonthlyBillPublicationVersion(context.Background(), f.m.storeDB, f.g.ID, f.e.Month, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if w := portalDo(f.r, "GET", "/api/monthly-statement?month="+f.e.Month, "", f.cookie); w.Code != 200 {
		t.Fatal("legacy bill unreadable", w.Code)
	}
	if err := f.withdraw(true); err != nil {
		t.Fatal(err)
	}
	monthlyRecoveryCounts(t, f.m, 0, 1)
}

func TestMonthlyResponsesRejectInFlightWithdrawal(t *testing.T) {
	for _, path := range []string{"/api/monthly-statement", "/api/monthly-statements", "/api/monthly-statement/download"} {
		t.Run(path, func(t *testing.T) {
			f := newMonthlyRecoveryFixture(t)
			var triggered atomic.Bool
			var callbackErr error
			if err := f.m.storeDB.Callback().Query().After("gorm:query").Register("monthly_withdraw_after_read", func(tx *gorm.DB) {
				if tx.Statement.Table == "portal_monthly_bills" && triggered.CompareAndSwap(false, true) {
					callbackErr = f.withdraw(true)
				}
			}); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := f.m.storeDB.Callback().Query().Remove("monthly_withdraw_after_read"); err != nil {
					t.Error(err)
				}
			}()
			w := portalDo(f.r, "GET", path+"?month="+f.e.Month+"&publication="+f.version, "", f.cookie)
			if callbackErr != nil {
				t.Fatal(callbackErr)
			}
			if !triggered.Load() || w.Code != 409 || strings.HasPrefix(w.Body.String(), "PK") || strings.Contains(w.Body.String(), "500.000000") {
				t.Fatalf("withdrawn payload served: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestMonthlyDownloadVersionAndCapacity(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	path := "/api/monthly-statement/download?month=" + f.e.Month
	if w := portalDo(f.r, "GET", path, "", f.cookie); w.Code != 400 {
		t.Fatal("unbound download accepted", w.Code)
	}
	if w := portalDo(f.r, "GET", path+"&publication="+strings.Repeat("0", 64), "", f.cookie); w.Code != 409 {
		t.Fatal("wrong version accepted", w.Code)
	}
	monthlyWorkbookSlots <- struct{}{}
	monthlyWorkbookSlots <- struct{}{}
	w := portalDo(f.r, "GET", path+"&publication="+f.version, "", f.cookie)
	<-monthlyWorkbookSlots
	<-monthlyWorkbookSlots
	if w.Code != 429 || w.Header().Get("Retry-After") != "2" {
		t.Fatal("download capacity unbounded", w.Code)
	}
	if w := portalDo(f.r, "GET", path+"&publication="+f.version, "", f.cookie); w.Code != 200 {
		t.Fatal("capacity failed to recover", w.Code)
	}
}

func TestMonthlyWithdrawalIsolationAndAuditGuards(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	g := CustomerGroup{Name: "other-customer", PortalEmail: "other@example.invalid"}
	if err := f.m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.Create(&TrackedUser{UserID: 44, GroupID: g.ID, Username: "other-account"}).Error; err != nil {
		t.Fatal(err)
	}
	e := monthlyEvidenceFixture(g.ID, f.e.Month, f.now)
	e.Users[0].UserID = 44
	e.Users[0].ConsumeQuota += 500000
	e.Users[0].ClosingQuota -= 500000
	e.Users[0].ReverseClosingQuota -= 500000
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ImportMonthlyBill(context.Background(), f.m.storeDB, raw, true, f.now); err != nil {
		t.Fatal(err)
	}
	otherCookie := &http.Cookie{Name: portalSessionCookie, Value: f.m.signPortalSession(g.ID, g.PortalAuthVer, f.now.Unix())}
	for _, cookie := range []*http.Cookie{f.cookie, otherCookie} {
		w := portalDo(f.r, "GET", "/api/monthly-statement?month="+f.e.Month+"&group_id=999&uid=999", "", cookie)
		var result struct {
			Data monthlyStatement `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		want := "500.000000"
		if cookie == otherCookie {
			want = "501.000000"
		}
		if w.Code != 200 || result.Data.Consumption.Amount != want {
			t.Fatal("customer scope leak", w.Code, w.Body.String())
		}
	}
	if err := WithdrawMonthlyBill(context.Background(), f.m.storeDB, g.ID, f.e.Month, f.version, "actor", "wrong customer", true, f.now); !errors.Is(err, errMonthlyPublicationChanged) {
		t.Fatal("version not bound to customer", err)
	}
	if err := f.withdraw(true); err != nil {
		t.Fatal(err)
	}
	monthlyRecoveryCounts(t, f.m, 1, 1)
	w := portalDo(f.r, "GET", "/api/monthly-statement?month="+f.e.Month, "", otherCookie)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "501.000000") {
		t.Fatal("other customer affected", w.Code)
	}
	var audit PortalMonthlyBillWithdrawal
	if err := f.m.storeDB.First(&audit).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.Model(&audit).Update("reason", "overwrite").Error; err == nil {
		t.Fatal("audit was editable")
	}
	if err := f.m.storeDB.Delete(&audit).Error; err == nil {
		t.Fatal("audit was removable")
	}
	monthlyRecoveryCounts(t, f.m, 1, 1)
}

func TestMonthlyConcurrentWithdrawalIsRecoverable(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = f.withdraw(true) }()
	}
	wg.Wait()
	// SQLite may reject a competing read-to-write upgrade as busy; a retry is
	// safe and converges to exactly one archive, never a partial deletion.
	if err := f.withdraw(true); err != nil {
		t.Fatal(err)
	}
	monthlyRecoveryCounts(t, f.m, 0, 1)
}
