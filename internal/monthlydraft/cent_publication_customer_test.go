package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"golang.org/x/crypto/bcrypt"
)

// Real registered runtime, calendar/prefix gates, retained token source audit,
// publication journal and customer routes. Only source/registration metadata
// are synthetic; no ready state, financial evidence gate or task is bypassed.
func TestRegisteredCentPublicationCustomerClickWorkerAndDownload(t *testing.T) {
	auth, ro, path := usageAuthorityFixture(t)
	m, err := monitor.New(monitor.Settings{StorePath: path, LocalSnapshotOnly: true, AlertsDisabled: true,
		PortalMonthlyStatementEnabled: true, SessionSecret: "synthetic-registered-cent"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	const password = "SyntheticCentOnly!2026"
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Model(&monitor.CustomerGroup{}).Where("id = 1").Update("portal_pw_admin", string(hash)).Error; err != nil {
		t.Fatal(err)
	}
	f := selectionFixture(t)
	owner, _, err := LoadUsageGenerationOwner(t.Context(), ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	addTokenArchives(t, &f)
	applyTokenInput(t, &f)
	scope := automaticScopeFixture(t, f)
	scopeRaw, err := json.Marshal(RetainedAutomaticScope{Version: 1, Owner: owner, Month: f.plan.Funding.Month,
		SourceIdentity: f.plan.Funding.SourceIdentity, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	scopePath := filepath.Join(f.root, "cent-scope.json")
	if err := os.WriteFile(scopePath, scopeRaw, 0600); err != nil {
		t.Fatal(err)
	}
	f.input.Sources = append(f.input.Sources, monitor.MonthlyPreparationSource{ID: "scope", Path: scopePath, SHA256: generationHash(scopeRaw)})
	f.plan.AutomaticScopeSourceID = "scope"
	usageInputUpdate(t, &f)
	stageFixture(t, f, "")
	selector, err := GenerationSelector(f.receiver)
	if err != nil {
		t.Fatal(err)
	}
	f.g.verifyCurrent, err = NewUsageGenerationGuard(ro, selector)
	if err != nil {
		t.Fatal(err)
	}
	load, err := NewRetainedAutomaticScopeLoader(f.g, "scope")
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := NewObservedAutomaticContext(f.g, load)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := newAutomaticDisplaySeriesRuntime(f.g, ro, GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}, metadata, nil, displaySeriesPublisher(auth))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.EnableOnDemandPreparation(t.Context(), ro,
		func(context.Context, SourcePreparationRequest) error { return nil },
		func(context.Context, SourcePreparationRequest) (bool, error) {
			t.Error("already-retained source was recaptured")
			return false, ErrGenerationEvidence
		}); err != nil {
		t.Fatal(err)
	}
	d := &Deployment{runtime: runtime, g: f.g, receiver: f.receiver}
	_, _, _, wallet, _ := walletPreparationFixture(t)
	source, err := NewFundingSourcePreparation(wallet.funding.source, wallet.funding.capture, f.g, ro, f.plan.Funding.SourceIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.EnableRegistrationCalendar(source); err != nil {
		t.Fatal(err)
	}
	runtime.service.(*registrationCalendarService).read = func(_ context.Context, ids []int64) ([]monthlysource.RegistrationTime, error) {
		rows := make([]monthlysource.RegistrationTime, len(ids))
		for i, id := range ids {
			rows[i] = monthlysource.RegistrationTime{UserID: id, At: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()}
		}
		return rows, nil
	}
	if err := d.EnableRegisteredSeriesPreparation(); err != nil {
		t.Fatal(err)
	}
	runtime.worker.pollMin, runtime.worker.pollMax = 5*time.Millisecond, 20*time.Millisecond
	router := gin.New()
	if err := m.RegisterPortalRoutesWithGeneration(router, d); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopRuntime(t, runtime) })
	request := func(method, url, body string, cookies []*http.Cookie) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "http://127.0.0.1"+url, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "http://127.0.0.1")
		req.Header.Set("X-Usage-Bill-Request", "1")
		req.Header.Set("X-Monthly-Statistics", "nullable-v1")
		req.Header.Set("X-Monthly-Display", "cents-chain-v1")
		for _, cookie := range cookies {
			req.AddCookie(cookie)
		}
		out := httptest.NewRecorder()
		router.ServeHTTP(out, req)
		return out
	}
	login := request("POST", "/login", `{"email":"owner@example.invalid","password":"`+password+`"}`, nil)
	if login.Code != 200 || len(login.Result().Cookies()) == 0 {
		t.Fatal("synthetic login failed", login.Code)
	}
	cookies := login.Result().Cookies()
	if submit := request("POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`, cookies); submit.Code != 202 {
		t.Fatal("customer generation rejected", submit.Code, submit.Body.String())
	}
	var saved monitor.PortalMonthlyBill
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if auth.First(&saved, "group_id = 1 AND month = ?", "2026-09").Error == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.HasPrefix(saved.PublicationID, "monthly-cent-bill-v1:") {
		t.Fatal("registered worker did not save new format", runtime.worker.Status())
	}
	stopRuntime(t, runtime)
	if err := monitor.ValidateMonthlyBillPublicationRecord(saved, time.Now()); err != nil {
		t.Fatal(err)
	}
	var taskID, binding string
	if err := f.receiver.QueryRow(`SELECT task_id,evidence_hash FROM portal_monthly_publication_jobs`).Scan(&taskID, &binding); err != nil {
		t.Fatal(err)
	}
	want, err := monitor.MonthlyBillSavedGenerationBinding(saved, f.plan.Funding.SourceIdentity, time.Now())
	if err != nil || want != binding || taskID == "" {
		t.Fatal("new saved record cannot recover its bound task", err)
	}
	page := request("GET", "/api/monthly-statement?month=2026-09", "", cookies)
	var wire struct {
		Data struct {
			Publication string `json:"publication"`
			Closing     string `json:"month_end_balance"`
			Members     []struct {
				UserID int64 `json:"user_id"`
			} `json:"members"`
		} `json:"data"`
	}
	if page.Code != 200 || json.Unmarshal(page.Body.Bytes(), &wire) != nil || wire.Data.Closing != "2006.000000" || len(wire.Data.Members) != 2 {
		t.Fatal("customer saved page mismatch", page.Code, page.Body.String())
	}
	file := request("GET", "/api/monthly-statement/download?month=2026-09&publication="+wire.Data.Publication, "", cookies)
	if file.Code != 200 || file.Body.Len() < 100 {
		t.Fatal("saved download unavailable", file.Code)
	}
	// Generation has already been drained. Reusing a saved result is read-only;
	// no stopped service may admit another month or rewrite the frozen bill.
	if reuse := request("POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`, cookies); reuse.Code != 200 || !strings.Contains(reuse.Body.String(), `"state":"available"`) {
		t.Fatal("stopping generation hid an issued bill", reuse.Code)
	}
	if fresh := request("POST", "/api/monthly-statement/generation", `{"month":"2026-10"}`, cookies); fresh.Code != 503 {
		t.Fatal("stopped generation admitted a new month", fresh.Code, fresh.Body.String())
	}
	// Feature-disable rollback retains the current compatible reader only.
	// Old-binary compatibility is tested separately, not claimed by this step.
	router = gin.New()
	m.RegisterPortalRoutes(router)
	if absent := request("GET", "/api/monthly-statement/generation?month=2026-09", "", cookies); absent.Code != 404 {
		t.Fatal("generation wiring survived reader-only rollback", absent.Code)
	}
	if after := request("GET", "/api/monthly-statement?month=2026-09", "", cookies); after.Code != 200 || !bytes.Equal(after.Body.Bytes(), page.Body.Bytes()) {
		t.Fatal("reader-only rollback changed customer money", after.Code)
	}
	if after := request("GET", "/api/monthly-statement/download?month=2026-09&publication="+wire.Data.Publication, "", cookies); after.Code != 200 || !bytes.Equal(after.Body.Bytes(), file.Body.Bytes()) {
		t.Fatal("reader-only rollback changed downloaded bill", after.Code)
	}
	var unchanged monitor.PortalMonthlyBill
	if err := auth.First(&unchanged, "group_id = 1 AND month = ?", "2026-09").Error; err != nil || unchanged != saved {
		t.Fatal("stopping or disabling generation rewrote saved data", err)
	}
}
