package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func monthlyFrozenFixture(t *testing.T) (*Monitor, *gin.Engine, CustomerGroup, *http.Cookie) {
	t.Helper()
	m, _, _ := newPortalTestMonitor(t)
	m.cfg.PortalMonthlyStatementEnabled = true
	r := gin.New()
	m.RegisterPortalRoutes(r)
	g := CustomerGroup{Name: "synthetic frozen cents", PortalEmail: "frozen@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 43, GroupID: g.ID, Username: "synthetic"}).Error; err != nil {
		t.Fatal(err)
	}
	cookie := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, time.Now().Unix())}
	return m, r, g, cookie
}

func monthlyFrozenEvidence(gid int64, month string, opening, credit, consume int64, now time.Time) MonthlyBillEvidence {
	e := monthlyFlatEvidence(gid, month, opening, now)
	u := &e.Users[0]
	u.ConsumeQuota = consume
	u.ClosingQuota, u.ReverseClosingQuota = opening+credit-consume, opening+credit-consume
	u.Accounting = monthlyAccountingFixture(consume, 0)
	if credit > 0 {
		start, _, _ := monthlyWindow(month, now)
		u.Events = []MonthlyBillCreditEvidence{{ID: 1, At: start.Unix() + 1, DeltaQuota: credit, Kind: "recharge", Method: "后台充值"}}
		u.ExpectedCreditEvents = 1
	}
	return e
}

func monthlyFrozenRecord(t *testing.T, db *gorm.DB, gid int64, month string) PortalMonthlyBill {
	t.Helper()
	var record PortalMonthlyBill
	if err := db.First(&record, "group_id=? AND month=?", gid, month).Error; err != nil {
		t.Fatal(err)
	}
	return record
}

func TestMonthlyFrozenDisplayPublicationReadExportAndRestart(t *testing.T) {
	m, r, group, cookie := monthlyFrozenFixture(t)
	now := time.Now()
	last, _, _ := monthlyWindow("", now)
	a := monthlyFrozenEvidence(group.ID, last.AddDate(0, -1, 0).Format("2006-01"), 1160872085, 2500000000, 1513097892, now)
	b := monthlyFrozenEvidence(group.ID, last.Format("2006-01"), 2147774193, 0, 2500, now)
	for _, e := range []MonthlyBillEvidence{a, b} {
		if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, e), true, now); err != nil {
			t.Fatal(err)
		}
	}
	first := monthlyFrozenRecord(t, m.storeDB, group.ID, a.Month)
	second := monthlyFrozenRecord(t, m.storeDB, group.ID, b.Month)
	p, err := readPublishedMonthlyBill(second)
	if err != nil || p.DisplayBalances == nil || p.DisplayBalances.Organization.Opening != "4295.540000" || p.DisplayBalances.Organization.Closing != "4295.530000" || *p.MonthOpeningBalance != "4295.548386" || *p.MonthEndBalance != "4295.543386" || p.Consumption.Amount != "0.005000" {
		t.Fatal("frozen cents or original money changed", err)
	}
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, b), true, now.Add(time.Second)); err != nil {
		t.Fatal("idempotent import failed", err)
	}
	w := portalDo(r, "GET", "/api/monthly-statement?month="+b.Month, "", cookie)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"display_balances"`) || strings.Contains(w.Body.String(), "source_sha256") || strings.Contains(w.Body.String(), "source_opening") {
		t.Fatal("customer response wrong or private chain leaked", w.Code)
	}
	book, err := monthlyStatementXLSX(p)
	if err != nil {
		t.Fatal(err)
	}
	parts := monthlyAuditWorkbookParts(t, book)
	for _, sheet := range []string{"xl/worksheets/sheet1.xml", "xl/worksheets/sheet2.xml"} {
		if !strings.Contains(parts[sheet], "<v>4295.54</v>") || !strings.Contains(parts[sheet], "<v>4295.53</v>") || strings.Contains(parts[sheet], "<v>4295.55</v>") {
			t.Fatal("customer workbook ignored frozen cents", sheet)
		}
	}
	w = portalDo(r, "GET", "/api/monthly-statement/download?month="+b.Month+"&publication="+p.Publication, "", cookie)
	if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), book) {
		t.Fatal("HTTP workbook differs from viewed bill", w.Code)
	}
	req := httptest.NewRequest("GET", "/api/monthly-statement?month="+b.Month, nil)
	req.AddCookie(cookie)
	old := httptest.NewRecorder()
	r.ServeHTTP(old, req)
	if old.Code != 409 || !strings.Contains(old.Body.String(), "刷新") {
		t.Fatal("old page could silently use a different cents policy", old.Code)
	}
	if monthlyFrozenRecord(t, m.storeDB, group.ID, a.Month) != first || monthlyFrozenRecord(t, m.storeDB, group.ID, b.Month) != second {
		t.Fatal("reads or retries rewrote frozen publications")
	}
	// Reopen a private copy through the real store migration path. This is not
	// the live preview or an online database; no existing connection is stopped.
	copyPath := filepath.Join(t.TempDir(), "restart.db")
	if err := m.storeDB.Exec("VACUUM INTO ?", copyPath).Error; err != nil {
		t.Fatal(err)
	}
	restarted := &Monitor{cfg: Settings{SessionSecret: "synthetic-frozen"}}
	if err := restarted.openStore(copyPath); err != nil {
		t.Fatal(err)
	}
	conn, _ := restarted.storeDB.DB()
	defer conn.Close()
	after, err := readPublishedMonthlyBill(monthlyFrozenRecord(t, restarted.storeDB, group.ID, b.Month))
	afterBook, exportErr := monthlyStatementXLSX(after)
	if err != nil || exportErr != nil || !bytes.Equal(afterBook, book) {
		t.Fatal("restart changed the frozen bill", err, exportErr)
	}
	// Even a recomputed outer checksum must not permit an inconsistent cents
	// override. Source money must never be replaced by malformed display data.
	p.DisplayBalances.Organization.Closing = "4295.540000"
	raw, _ := json.Marshal(p)
	corrupt := second
	corrupt.Payload, corrupt.PayloadHash = string(raw), monthlyDigest(raw)
	if _, err := readPublishedMonthlyBill(corrupt); err == nil {
		t.Fatal("inconsistent display override survived validation")
	}
	p.DisplayBalances = nil
	raw, _ = json.Marshal(p)
	corrupt.Payload, corrupt.PayloadHash = string(raw), monthlyDigest(raw)
	if _, err := readPublishedMonthlyBill(corrupt); err == nil {
		t.Fatal("new-format bill silently fell back after frozen fields were lost")
	}
}

func TestMonthlyFrozenDisplayLegacyNeighborRemainsByteIdentical(t *testing.T) {
	m, _, group, _ := monthlyFrozenFixture(t)
	now := time.Now()
	last, _, _ := monthlyWindow("", now)
	a := monthlyFrozenEvidence(group.ID, last.AddDate(0, -1, 0).Format("2006-01"), 1160872085, 2500000000, 1513097892, now)
	b := monthlyFrozenEvidence(group.ID, last.Format("2006-01"), 2147774193, 0, 2500, now)
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, a), true, now); err != nil {
		t.Fatal(err)
	}
	old := monthlyFrozenRecord(t, m.storeDB, group.ID, a.Month)
	p, err := readPublishedMonthlyBill(old)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate an actual older-format publication, not removal from a marked
	// new-format bill. Only this temporary test database is changed.
	p.Publication, p.DisplayBalances = "", nil
	raw, _ := json.Marshal(p)
	if err := m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id=? AND month=?", group.ID, a.Month).Updates(map[string]any{"payload": string(raw), "payload_hash": monthlyDigest(raw), "publication_id": "synthetic-legacy-issuance"}).Error; err != nil {
		t.Fatal(err)
	}
	before := monthlyFrozenRecord(t, m.storeDB, group.ID, a.Month)
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, b), true, now); err != nil {
		t.Fatal("new-format bill failed to link a valid legacy neighbor", err)
	}
	after := monthlyFrozenRecord(t, m.storeDB, group.ID, a.Month)
	if before != after {
		t.Fatal("new publication rewrote an old-format neighbor")
	}
	current, err := readPublishedMonthlyBill(monthlyFrozenRecord(t, m.storeDB, group.ID, b.Month))
	if err != nil || current.DisplayBalances.Organization.Opening != "4295.540000" {
		t.Fatal("legacy display closing was not preserved as next opening", err)
	}
}

func TestMonthlyFrozenDisplayPublicationFailureRollsBackWholeRecord(t *testing.T) {
	m, _, group, _ := monthlyFrozenFixture(t)
	now := time.Now()
	last, _, _ := monthlyWindow("", now)
	a := monthlyFrozenEvidence(group.ID, last.AddDate(0, -1, 0).Format("2006-01"), 0, 1000000, 1, now)
	b := monthlyFrozenEvidence(group.ID, last.Format("2006-01"), 999999, 0, 2, now)
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, a), true, now); err != nil {
		t.Fatal(err)
	}
	prior := monthlyFrozenRecord(t, m.storeDB, group.ID, a.Month)
	const callback = "synthetic_frozen_publication_failure"
	injected := errors.New("synthetic save failure")
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "portal_monthly_bills" {
			if err := tx.AddError(injected); !errors.Is(err, injected) {
				t.Error("save failure injection did not persist", err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	_, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, b), true, now)
	if removeErr := m.storeDB.Callback().Create().Remove(callback); removeErr != nil {
		t.Fatal("remove synthetic failure callback", removeErr)
	}
	if !errors.Is(err, injected) {
		t.Fatal("injected save failure ignored")
	}
	var count int64
	if err := m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id=?", group.ID).Count(&count).Error; err != nil || count != 1 || monthlyFrozenRecord(t, m.storeDB, group.ID, a.Month) != prior {
		t.Fatal("partial bill saved or predecessor mutated", err, count)
	}
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, b), true, now); err != nil {
		t.Fatal("retry after rollback failed", err)
	}
}

func TestMonthlyFrozenDisplayConcurrentAdjacentPublication(t *testing.T) {
	m, _, group, _ := monthlyFrozenFixture(t)
	now := time.Now()
	last, _, _ := monthlyWindow("", now)
	a := monthlyFrozenEvidence(group.ID, last.AddDate(0, -1, 0).Format("2006-01"), 1160872085, 2500000000, 1513097892, now)
	b := monthlyFrozenEvidence(group.ID, last.Format("2006-01"), 2147774193, 0, 2500, now)
	inputs := [][]byte{monthlyTestRaw(t, a), monthlyTestRaw(t, b)}
	start, errs := make(chan struct{}), make(chan error, 2)
	var wg sync.WaitGroup
	for _, raw := range inputs {
		wg.Add(1)
		go func(raw []byte) {
			defer wg.Done()
			<-start
			var err error
			for attempt := 0; attempt < 5; attempt++ {
				_, err = ImportMonthlyBill(context.Background(), m.storeDB, raw, true, now)
				if err == nil || (!strings.Contains(err.Error(), "locked") && !strings.Contains(err.Error(), "SQLITE_BUSY")) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			errs <- err
		}(raw)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal("concurrent publication failed", err)
		}
	}
	pa, err := readPublishedMonthlyBill(monthlyFrozenRecord(t, m.storeDB, group.ID, a.Month))
	if err != nil {
		t.Fatal(err)
	}
	pb, err := readPublishedMonthlyBill(monthlyFrozenRecord(t, m.storeDB, group.ID, b.Month))
	if err != nil || pa.DisplayBalances.Organization.Closing != pb.DisplayBalances.Organization.Opening || pa.DisplayBalances.Members[0].Closing != pb.DisplayBalances.Members[0].Opening {
		t.Fatal("concurrent writers froze inconsistent boundaries", err)
	}
}

// Explicit opt-in manual UI acceptance only. Uses production routes and normal
// password login, but synthetic evidence, a temporary local DB and no source
// loader/credentials/collectors. Not included in the application binary.
func TestMonthlyFrozenDisplayLocalBrowserServer(t *testing.T) {
	if os.Getenv("MONTHLY_FROZEN_DISPLAY_BROWSER") != "1" {
		t.Skip("explicit loopback-only synthetic browser acceptance")
	}
	m, _, group, _ := monthlyFrozenFixture(t)
	m.cfg.LocalSnapshotOnly, m.cfg.AlertsDisabled = true, true
	pw, err := hashPassword("BillLocal2026!")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&CustomerGroup{}).Where("id=?", group.ID).Updates(map[string]any{"name": "跨月账单验收（合成数据）", "portal_pw_admin": pw}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	last, _, _ := monthlyWindow("", now)
	for _, e := range []MonthlyBillEvidence{
		monthlyFrozenEvidence(group.ID, last.AddDate(0, -1, 0).Format("2006-01"), 1160872085, 2500000000, 1513097892, now),
		monthlyFrozenEvidence(group.ID, last.Format("2006-01"), 2147774193, 0, 2500, now),
	} {
		if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, e), true, now); err != nil {
			t.Fatal(err)
		}
	}
	r := gin.New()
	r.Use(gin.Recovery())
	_ = r.SetTrustedProxies(nil)
	r.Use(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/api/") && !strings.HasPrefix(c.Request.URL.Path, "/api/monthly-statement") {
			c.AbortWithStatusJSON(503, gin.H{"error": "本机仅验收合成月度账单，不连接线上"})
			return
		}
		if c.Request.Method == "GET" && c.Request.URL.Path == "/" {
			original := c.Writer
			buffer := newPortalBufferedResponseWriter(original)
			c.Writer = buffer
			c.Next()
			c.Writer = original
			if buffer.Status() == http.StatusOK {
				page := strings.Replace(buffer.body.String(), "\nload();\n", "\nshowView('monthly');\n", 1)
				page = strings.Replace(page, "<body>", "<body><p style=\"padding:12px;color:#92400e\">本机跨月账单验收 · 全部为合成数据，不是客户账单</p>", 1)
				buffer.body.Reset()
				buffer.body.WriteString(page)
			}
			buffer.commitTo(original)
		}
	})
	m.RegisterPortalRoutes(r)
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: r, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	defer srv.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); _ = srv.Close() }()
	// localhost has a separate cookie host from the user's 127.0.0.1 previews.
	// A synthetic login must not replace the real-data preview's session cookie.
	t.Logf("LOCAL_FROZEN_BROWSER http://localhost:%d/login", ln.Addr().(*net.TCPAddr).Port)
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
}
