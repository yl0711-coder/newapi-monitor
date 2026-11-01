package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func monthlyCentPublicationFixture(t *testing.T) (*Monitor, *gin.Engine, CustomerGroup, *http.Cookie, []MonthlyBillEvidence, []MonthlyDisplayPublicationSource, MonthlyDisplaySeriesScope, MonthlyBillExpectedAuthorization) {
	t.Helper()
	m, router, group, cookie := monthlyFrozenFixture(t)
	t.Cleanup(m.Close)
	for _, id := range []int64{44, 45} {
		if err := m.storeDB.Create(&TrackedUser{UserID: id, GroupID: group.ID, Username: "synthetic"}).Error; err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	evidence := []MonthlyBillEvidence{}
	sources := []MonthlyDisplayPublicationSource{}
	prior := map[int64]int64{}
	for _, month := range []string{"2026-09", "2026-10"} {
		e, _ := monthlyTokenFixture()
		e.GroupID, e.Month, e.ReviewedAt = group.ID, month, now.Unix()
		_, end, err := monthlyWindow(month, now)
		if err != nil {
			t.Fatal(err)
		}
		names := map[int64]string{}
		for i := range e.Users {
			u := &e.Users[i]
			u.UserID, u.Through = int64(43+i), end.Unix()
			if opening, exists := prior[u.UserID]; exists {
				u.OpeningQuota = opening
			}
			u.ConsumeQuota, u.RefundQuota = 0, 0
			if i < 2 {
				u.ConsumeQuota = 2000
			}
			u.ClosingQuota = u.OpeningQuota - u.ConsumeQuota
			u.ReverseClosingQuota = u.ClosingQuota
			u.Accounting = monthlyAccountingFixture(u.ConsumeQuota, 0)
			u.TokenUsage.UserID, u.TokenUsage.Month = u.UserID, month
			u.TokenUsage.Rows = []MonthlyBillTokenEvidence{{TokenID: u.UserID * 10, Name: "合成令牌", ConsumeQuota: u.ConsumeQuota}}
			prior[u.UserID], names[u.UserID] = u.ClosingQuota, "synthetic"
		}
		evidence = append(evidence, e)
		sources = append(sources, MonthlyDisplayPublicationSource{Evidence: monthlyTestRaw(t, e), Account: group.Name, Names: names})
	}
	owner, err := loadMonthlyBillAuthorization(t.Context(), m.storeDB, group.ID)
	if err != nil {
		t.Fatal(err)
	}
	scope := MonthlyDisplaySeriesScope{GroupID: group.ID, SourceIdentity: monthlyDigest([]byte("synthetic-cent-origin")), From: "2026-09", Through: "2026-10"}
	expected := MonthlyBillExpectedAuthorization{GroupID: group.ID, AuthVersion: group.PortalAuthVer, MemberFingerprint: owner.Fingerprint, MemberManifest: owner.Manifest}
	return m, router, group, cookie, evidence, sources, scope, expected
}

func TestMonthlyBillCentPublicationHTTPReadDownloadRestartAndBinding(t *testing.T) {
	m, router, group, cookie, evidence, sources, scope, expected := monthlyCentPublicationFixture(t)
	now := time.Now()
	prepared, err := PrepareMonthlyRegisteredDisplayPublication(t.Context(), m.storeDB, scope, sources, now)
	if err != nil || prepared.value.cent == nil {
		t.Fatal("new token-complete series did not select new format", err)
	}
	// The requested later month is the only record written. Its predecessor
	// determines the opening but does not acquire a fake customer task or bill.
	if _, err := ImportMonthlyBillWithDisplaySeries(t.Context(), m.storeDB, monthlyTestRaw(t, evidence[1]), expected, prepared, now); err != nil {
		t.Fatal(err)
	}
	monthlySeriesRecordCount(t, m.storeDB, 1)
	later := monthlyFrozenRecord(t, m.storeDB, group.ID, "2026-10")
	if !strings.HasPrefix(later.PublicationID, monthlyCentBillPublicationPrefix) {
		t.Fatal("new task saved a legacy payload")
	}
	if err := ValidateMonthlyBillPublicationRecord(later, now); err != nil {
		t.Fatal("source/evidence audit rejected new format", err)
	}
	bound, err := MonthlyBillSavedGenerationBinding(later, scope.SourceIdentity, now)
	want, bindErr := monthlyPreparedSavedBinding(prepared, later.Month)
	if err != nil || bindErr != nil || bound != want {
		t.Fatal("saved record cannot recover its original task binding", err, bindErr)
	}
	for range 2 {
		if _, err := ImportMonthlyBillWithDisplaySeries(t.Context(), m.storeDB, monthlyTestRaw(t, evidence[0]), expected, prepared, now); err != nil {
			t.Fatal("earlier request or retry failed", err)
		}
	}
	monthlySeriesRecordCount(t, m.storeDB, 2)
	if after := monthlyFrozenRecord(t, m.storeDB, group.ID, later.Month); after != later {
		t.Fatal("earlier request changed frozen later publication")
	}
	get := func(url string, cookie *http.Cookie) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req.AddCookie(cookie)
		req.Header.Set("X-Monthly-Statistics", "nullable-v1")
		req.Header.Set("X-Monthly-Accounting", "balance-correction-v1")
		req.Header.Set("X-Monthly-Display", "cents-chain-v1")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}
	detail := get("/api/monthly-statement?month=2026-10", cookie)
	var response struct {
		Data monthlyStatement `json:"data"`
	}
	if detail.Code != 200 || json.Unmarshal(detail.Body.Bytes(), &response) != nil ||
		response.Data.MonthEndBalance == nil || *response.Data.MonthEndBalance != "29.980000" || response.Data.Members[0].Closing != "9.980000" {
		t.Fatal("customer detail did not use retained cents", detail.Code, detail.Body.String())
	}
	rawSource, err := readPublishedMonthlyBill(later)
	if err != nil || rawSource.Members[0].Closing != "9.992000" {
		t.Fatal("backend source reader returned rounded customer money", err)
	}
	list := get("/api/monthly-statements?month=2026-10", cookie)
	if list.Code != 200 || !strings.Contains(list.Body.String(), `"amount":"0.010000"`) {
		t.Fatal("list not using the same cent totals", list.Code, list.Body.String())
	}
	download := get("/api/monthly-statement/download?month=2026-10&publication="+response.Data.Publication, cookie)
	if download.Code != 200 {
		t.Fatal("new format could not download", download.Code, download.Body.String())
	}
	for _, part := range monthlyAuditWorkbookParts(t, download.Body.Bytes()) {
		if strings.Contains(part, "舍入尾差") {
			t.Fatal("technical rounding row exposed")
		}
	}
	if wrong := get("/api/monthly-statement/download?month=2026-10&publication="+strings.Repeat("a", 64), cookie); wrong.Code != 409 {
		t.Fatal("download version binding was weakened", wrong.Code)
	}
	other := CustomerGroup{Name: "other synthetic owner", PortalEmail: "other@example.invalid"}
	if err := m.storeDB.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	otherCookie := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(other.ID, other.PortalAuthVer, time.Now().Unix())}
	if leaked := get("/api/monthly-statement?month=2026-10", otherCookie); leaked.Code != 404 {
		t.Fatal("other organization reached saved bill", leaked.Code)
	}
	copyPath := filepath.Join(t.TempDir(), "restart.db")
	if err := m.storeDB.Exec("VACUUM INTO ?", copyPath).Error; err != nil {
		t.Fatal(err)
	}
	restarted := &Monitor{cfg: Settings{SessionSecret: "synthetic-cent-restart"}}
	if err := restarted.openStore(copyPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restarted.Close)
	reloaded := monthlyFrozenRecord(t, restarted.storeDB, group.ID, later.Month)
	bill, err := monthlyReadCustomerBill(reloaded)
	if err != nil {
		t.Fatal(err)
	}
	book, err := bill.workbook()
	if err != nil || !bytes.Equal(book, download.Body.Bytes()) || reloaded != later {
		t.Fatal("restart changed retained publication or download", err)
	}
}

func TestMonthlyBillCentPublicationPreservesBothBoundTaskFormats(t *testing.T) {
	m, _, _, _, _, sources, scope, _ := monthlyCentPublicationFixture(t)
	now := time.Now()
	old, err := PrepareMonthlyDisplayPublication(t.Context(), scope, sources, now)
	if err != nil {
		t.Fatal(err)
	}
	modern, err := PrepareMonthlyRegisteredDisplayPublication(t.Context(), m.storeDB, scope, sources, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []*PreparedMonthlyDisplayPublication{old, modern} {
		binding, err := monthlyPreparedSavedBinding(expected, "2026-10")
		if err != nil {
			t.Fatal(err)
		}
		got, err := PrepareMonthlyRegisteredDisplayPublicationForBinding(t.Context(), m.storeDB, scope, sources, now, "2026-10", binding)
		if err != nil || (got.value.cent == nil) != (expected.value.cent == nil) {
			t.Fatal("upgrade changed a previously frozen task policy", err)
		}
		actual, err := monthlyPreparedSavedBinding(got, "2026-10")
		if err != nil || actual != binding {
			t.Fatal("bound task hash changed", err)
		}
	}
	if _, err := PrepareMonthlyRegisteredDisplayPublicationForBinding(t.Context(), m.storeDB, scope, sources, now, "2026-10", monthlyDigest([]byte("wrong binding"))); err == nil {
		t.Fatal("unknown binding was replaced")
	}
	monthlySeriesRecordCount(t, m.storeDB, 0)
}

func TestMonthlyBillCentPublicationLegacySeriesAndPolicyRaceStayImmutable(t *testing.T) {
	m, _, group, _, evidence, sources, scope, expected := monthlyCentPublicationFixture(t)
	now := time.Now()
	modern, err := PrepareMonthlyRegisteredDisplayPublication(t.Context(), m.storeDB, scope, sources, now)
	if err != nil {
		t.Fatal(err)
	}
	old, err := PrepareMonthlyDisplayPublication(t.Context(), scope, sources, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ImportMonthlyBillWithDisplaySeries(t.Context(), m.storeDB, monthlyTestRaw(t, evidence[0]), expected, old, now); err != nil {
		t.Fatal(err)
	}
	first := monthlyFrozenRecord(t, m.storeDB, group.ID, evidence[0].Month)
	if _, err := ImportMonthlyBillWithDisplaySeries(t.Context(), m.storeDB, monthlyTestRaw(t, evidence[1]), expected, modern, now); err == nil {
		t.Fatal("stale format selection mixed new cents into a legacy series")
	}
	monthlySeriesRecordCount(t, m.storeDB, 1)
	selected, err := PrepareMonthlyRegisteredDisplayPublication(t.Context(), m.storeDB, scope, sources, now)
	if err != nil || selected.value.cent != nil {
		t.Fatal("existing legacy series policy changed", err)
	}
	if _, err := ImportMonthlyBillWithDisplaySeries(t.Context(), m.storeDB, monthlyTestRaw(t, evidence[1]), expected, selected, now); err != nil {
		t.Fatal("compatible legacy continuation was broken", err)
	}
	if monthlyFrozenRecord(t, m.storeDB, group.ID, evidence[0].Month) != first {
		t.Fatal("legacy record was migrated in place")
	}
	badAuth := expected
	badAuth.AuthVersion++
	if _, err := ImportMonthlyBillWithDisplaySeries(context.Background(), m.storeDB, monthlyTestRaw(t, evidence[1]), badAuth, selected, now); err == nil {
		t.Fatal("stale authorization permitted publication")
	}
}

func TestMonthlyBillCentPublicationStorageFailureAndLegacyWriterFence(t *testing.T) {
	m, _, group, _, evidence, sources, scope, expected := monthlyCentPublicationFixture(t)
	now := time.Now()
	modern, err := PrepareMonthlyRegisteredDisplayPublication(t.Context(), m.storeDB, scope, sources, now)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := PrepareMonthlyDisplayPublication(t.Context(), scope, sources, now)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("synthetic cent publication storage failure")
	const callback = "monthly_cent_storage_failure"
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "portal_monthly_bills" {
			_ = tx.AddError(injected)
		}
	}); err != nil {
		t.Fatal(err)
	}
	_, err = ImportMonthlyBillWithDisplaySeries(t.Context(), m.storeDB, monthlyTestRaw(t, evidence[0]), expected, modern, now)
	if removeErr := m.storeDB.Callback().Create().Remove(callback); removeErr != nil {
		t.Fatal(removeErr)
	}
	if !errors.Is(err, injected) {
		t.Fatal("storage failure was not propagated", err)
	}
	monthlySeriesRecordCount(t, m.storeDB, 0)
	if _, err := ImportMonthlyBillWithDisplaySeries(t.Context(), m.storeDB, monthlyTestRaw(t, evidence[0]), expected, modern, now); err != nil {
		t.Fatal("retry after rollback failed", err)
	}
	first := monthlyFrozenRecord(t, m.storeDB, group.ID, evidence[0].Month)
	if _, err := readLegacyMonthlyBill(first); err == nil {
		t.Fatal("legacy reader silently accepted a new-format bill")
	}
	if _, err := ImportMonthlyBillWithDisplaySeries(t.Context(), m.storeDB, monthlyTestRaw(t, evidence[1]), expected, legacy, now); err == nil {
		t.Fatal("stale legacy prepared writer extended a modern series")
	}
	if _, err := ImportMonthlyBillForAuthorization(t.Context(), m.storeDB, monthlyTestRaw(t, evidence[1]), expected, now); err == nil {
		t.Fatal("non-series legacy writer extended a modern series")
	}
	monthlySeriesRecordCount(t, m.storeDB, 1)
	if monthlyFrozenRecord(t, m.storeDB, group.ID, evidence[0].Month) != first {
		t.Fatal("blocked legacy write changed the saved bill")
	}
	if _, err := ImportMonthlyBillWithDisplaySeries(t.Context(), m.storeDB, monthlyTestRaw(t, evidence[1]), expected, modern, now); err != nil {
		t.Fatal("current writer could not resume after rejected legacy writes", err)
	}
	monthlySeriesRecordCount(t, m.storeDB, 2)
	if monthlyFrozenRecord(t, m.storeDB, group.ID, evidence[0].Month) != first {
		t.Fatal("resumed publication rewrote its predecessor")
	}
}
