package monthlydraft

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// Uses only fresh synthetic files. The production resource opener and router
// are composed here; approval remains an explicit test-only operation.
func TestDeploymentProductionHTTPJourneyAndDisable(t *testing.T) {
	a := newLocalGenerationAcceptance(t)
	if err := a.runtime.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	var databases []struct{ Name, File string }
	if err := a.auth.Raw("PRAGMA database_list").Scan(&databases).Error; err != nil {
		t.Fatal(err)
	}
	var authorityPath string
	for _, db := range databases {
		if db.Name == "main" {
			authorityPath = db.File
		}
	}
	if err := os.Chmod(a.f.receiverPath, 0600); err != nil {
		t.Fatal(err)
	}
	d, err := OpenDeployment(context.Background(), DeploymentConfig{
		Rollout:      GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: "2026-09"}}},
		ReceiverPath: a.f.receiverPath, FundingPath: a.f.path, AuthorityPath: authorityPath, EvidenceRoot: a.f.root,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, d) })
	r := gin.New()
	if err := a.monitor.RegisterPortalRoutesWithGeneration(r, d); err != nil {
		t.Fatal(err)
	}
	if err := d.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	request := func(method, path, body string, cookies []*http.Cookie) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "http://127.0.0.1"+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "http://127.0.0.1")
		req.Header.Set("X-Usage-Bill-Request", "1")
		req.Header.Set("X-Monthly-Statistics", "nullable-v1")
		req.Header.Set("X-Monthly-Display", "cents-chain-v1")
		for _, cookie := range cookies {
			req.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	login := func(email string) []*http.Cookie {
		w := request("POST", "/login", `{"email":"`+email+`","password":"`+a.password+`"}`, nil)
		if w.Code != 200 || len(w.Result().Cookies()) == 0 {
			t.Fatal("production route login failed", w.Code)
		}
		return w.Result().Cookies()
	}
	own, foreign := login("owner@example.invalid"), login("other@example.invalid")
	// selectionFixture has one selected input, but no admitted task yet.
	generationAdmissionCounts(t, a.f.receiver, 0, 1)
	if w := request("GET", "/", "", own); w.Code != 200 || !strings.Contains(w.Body.String(), "生成月度账单") {
		t.Fatal("production page missing generation", w.Code)
	}
	if w := request("POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`, foreign); w.Code != 409 || !strings.Contains(w.Body.String(), `"code":"not_prepared"`) {
		t.Fatal("outside target must remain not prepared, without admission", w.Code, w.Body.String())
	}
	generationAdmissionCounts(t, a.f.receiver, 0, 1)
	if w := request("POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`, own); w.Code != 202 {
		t.Fatal("production composition admission failed", w.Code)
	}
	job, err := d.g.tasks.Find(context.Background(), generationRequest(a.f.plan, mustGenerationPlan(t, a.f)))
	if err != nil {
		t.Fatal(err)
	}
	waitLocalDraft(t, d.g, job)
	if w := request("GET", "/api/monthly-statement?month=2026-09", "", own); w.Code != 404 {
		t.Fatal("draft automatically published", w.Code)
	}
	// Persisted ready is visible before the worker has returned and released
	// its bounded calculation slot. Drain before this independent test review;
	// do not turn a transient busy result into approval or weaken the slot.
	if err := d.runtime.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Independently review the synthetic fixture; assert per-account money,
	// organization conservation, idempotency and XLSX using the existing oracle.
	testGenerationReviewedPublication(t, d.g, a.f, job, a.auth, request, own, foreign)
	before := request("GET", "/api/monthly-statement?month=2026-09", "", own)
	closeDeploymentTest(t, d)
	// Stopping generation must leave an already frozen bill reusable without
	// opening the closed task DB. It must NOT accept new work after shutdown.
	for _, method := range []string{"GET", "POST"} {
		path, body := "/api/monthly-statement/generation?month=2026-09", ""
		if method == "POST" {
			path, body = "/api/monthly-statement/generation", `{"month":"2026-09"}`
		}
		if w := request(method, path, body, own); w.Code != 200 || !strings.Contains(w.Body.String(), `"state":"available"`) {
			t.Fatal("closed deployment failed to reuse saved bill", method, w.Code, w.Body.String())
		}
	}
	if w := request("POST", "/api/monthly-statement/generation", `{"month":"2026-08"}`, own); w.Code != 503 {
		t.Fatal("closed deployment admitted new work", w.Code)
	}
	generationAdmissionCounts(t, a.f.receiver, 1, 1)
	r = gin.New()
	a.monitor.RegisterPortalRoutes(r)
	after := request("GET", "/api/monthly-statement?month=2026-09", "", own)
	if before.Code != 200 || after.Code != 200 || before.Body.String() != after.Body.String() {
		t.Fatal("disable changed published bill")
	}
}
