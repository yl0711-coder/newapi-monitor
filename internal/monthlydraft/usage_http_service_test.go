package monthlydraft

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"golang.org/x/crypto/bcrypt"
)

// Real HTTP handlers, login cookies, authority records, selected inputs, task
// receiver and calculator. All databases/evidence are synthetic temporary files.
// Run is invoked explicitly, not via a hidden goroutine in a customer request.
func TestUsageGenerationHTTPRealLoginSubmissionAndDraftJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, false, false, false)
}

func TestUsageGenerationHTTPAutomaticWorkerJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, false, false)
}

func TestUsageGenerationHTTPOnDemandAutomaticPublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, false)
}

func TestUsageGenerationHTTPObservedFundingPublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, true)
}

func TestUsageGenerationHTTPObservedUsagePublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, true, true)
}

func TestUsageGenerationHTTPMachineUsagePublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, true, true, true)
}

func TestUsageGenerationHTTPMachineWalletPublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, true, true, true, true)
}

func TestUsageGenerationHTTPCombinedPreparationPublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, true, true, true, true, true)
}

func TestUsageGenerationHTTPAutomaticDeploymentPublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, true, true, true, true, true, true)
}

func TestUsageGenerationHTTPRetainedScopeDeploymentPublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, true, true, true, true, true, true, true)
}

func TestUsageGenerationHTTPRunningPreparationPublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, true, true, true, true, true, true, true, true)
}

func TestUsageGenerationHTTPPreparationInboxPublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, true, true, true, true, true, true, true, true, true)
}

func TestUsageGenerationHTTPObservationStoreInboxPublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, true, true, true, true, true, true, true, true, true, true)
}

func TestUsageGenerationHTTPObservationMaterialInboxPublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, true, true, true, true, true, true, true, true, true, true, true)
}

func TestUsageGenerationHTTPOrganizationHistoryInboxPublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, true, true, true, true, true, true, true, true, true, true, true, true)
}

func TestUsageGenerationHTTPRetainedMonthHistoryInboxPublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true)
}

func TestUsageGenerationHTTPWalletReuseInboxPublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourney(t, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true)
}

func TestUsageGenerationHTTPRetainedNormalizedInboxPublicationJourney(t *testing.T) {
	testUsageGenerationHTTPJourneyWithRetained(t, true, true, true, false)
}

func testUsageGenerationHTTPJourney(t *testing.T, automatic, issue, observed bool, usage ...bool) {
	testUsageGenerationHTTPJourneyWithRetained(t, false, automatic, issue, observed, usage...)
}

func testUsageGenerationHTTPJourneyWithRetained(t *testing.T, retainedPrepared, automatic, issue, observed bool, usage ...bool) {
	gin.SetMode(gin.TestMode)
	auth, authRO, authPath := usageAuthorityFixture(t)
	m, err := monitor.New(monitor.Settings{StorePath: authPath, LocalSnapshotOnly: true,
		AlertsDisabled: true, PortalMonthlyStatementEnabled: true, SessionSecret: "synthetic-http-generation-secret"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	const password = "SyntheticLocalOnly!2026"
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Model(&monitor.CustomerGroup{}).Where("id IN ?", []int64{1, 2}).Update("portal_pw_admin", string(hash)).Error; err != nil {
		t.Fatal(err)
	}
	f := selectionFixture(t)
	if len(usage) != 0 && usage[0] {
		addUsageInputFixture(t, &f, "")
	}
	ctx := context.Background()
	owner, _, err := LoadUsageGenerationOwner(ctx, authRO, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	combined := len(usage) > 3 && usage[3]
	machine := len(usage) > 1 && usage[1]
	retainedScope := retainedPrepared || len(usage) > 5 && usage[5]
	runningPreparation := len(usage) > 6 && usage[6]
	inboxPreparation := retainedPrepared || len(usage) > 7 && usage[7]
	materialPreparation := len(usage) > 9 && usage[9]
	if combined {
		sealUsagePreparationFixture(t, &f)
		addWalletInputFixture(t, &f, "")
		clearObservedFundingFixture(t, &f)
		if !retainedScope {
			result, err := f.g.PrepareObservedInput(ctx, f.plan, f.raw, "observed_wallet")
			if err != nil {
				t.Fatal("combined source preparation failed", err)
			}
			useWalletPreparation(t, &f, WalletPreparation{Plan: result.Plan, Preparation: result.Preparation})
		}
	} else if machine {
		sealUsagePreparationFixture(t, &f)
		result, err := f.g.PrepareUsageSpans(ctx, f.plan, f.raw, "observed")
		if err != nil {
			t.Fatal("machine usage preparation failed", err)
		}
		if err := json.Unmarshal(result.Plan, &f.plan); err != nil {
			t.Fatal(err)
		}
		f.input, err = monitor.DecodeMonthlyPreparationInput(result.Preparation)
		if err != nil {
			t.Fatal(err)
		}
		f.raw = result.Preparation
	}
	if !combined && len(usage) > 2 && usage[2] {
		addWalletInputFixture(t, &f, "")
		result, err := f.g.PrepareWalletObservations(ctx, f.plan, f.raw)
		if err != nil {
			t.Fatal("machine wallet preparation failed", err)
		}
		useWalletPreparation(t, &f, result)
	}
	if materialPreparation {
		addTokenArchivesFromObserved(t, &f)
	}
	fundingPath := f.path
	if len(usage) > 8 && usage[8] {
		f.plan.ObservationStoreSourceID = "observed_wallet"
		fundingPath = unrelatedObservationDefault(t, f.g)
		f.funding = f.g.funding
	}
	if retainedScope {
		output := filepath.Join(f.root, "prepared-files")
		if err := os.Mkdir(output, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(f.receiverPath, 0600); err != nil {
			t.Fatal(err)
		}
		doc := RetainedAutomaticScope{Version: 1, Owner: owner, Month: f.plan.Funding.Month,
			SourceIdentity: f.plan.Funding.SourceIdentity, Scope: automaticScopeFixture(t, f)}
		scopeRaw, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		cfg := DeploymentConfig{ReceiverPath: f.receiverPath, FundingPath: fundingPath, AuthorityPath: authPath, EvidenceRoot: f.root,
			Rollout: GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: owner.GroupID, Month: f.plan.Funding.Month}}}}
		var receipt AutomaticPreparationReceipt
		if retainedPrepared {
			addRetainedScopeFixture(t, &f, nil)
			receipt, err = PrepareRetainedGenerationFiles(ctx, cfg, RetainedPreparationRequest{
				Plan: mustGenerationPlan(t, f), Preparation: f.raw, OutputRoot: output, Write: true})
		} else if materialPreparation {
			request, _ := writeObservationMaterialPreparationFixture(t, f)
			request.Scope, request.ScopeSourceID, request.OutputRoot, request.Write = scopeRaw, syntheticScopeID, output, true
			if len(usage) > 10 && usage[10] {
				useOrganizationHistoryFixture(t, f, cfg, &request)
				if len(usage) > 11 && usage[11] {
					fillRetainedHistoryMonth(t, f, &request, true)
				}
			}
			if len(usage) > 12 && usage[12] {
				useWalletReusePreparationFixture(t, f, &request)
			}
			receipt, err = PrepareObservationMaterialFiles(ctx, cfg, request)
		} else {
			receipt, err = PrepareAutomaticGenerationFiles(ctx, cfg, AutomaticPreparationRequest{Plan: mustGenerationPlan(t, f),
				Template: f.raw, Scope: scopeRaw, ObservationSourceID: "observed_wallet", ScopeSourceID: syntheticScopeID, OutputRoot: output, Write: true})
		}
		if err != nil || !receipt.Complete {
			t.Fatal("formal file preparation failed", err)
		}
		if inboxPreparation {
			writePreparationInboxTest(t, f.root, "inbox.json", PreparedInputInboxEntry{GroupID: owner.GroupID,
				Month: f.plan.Funding.Month, Revision: 1, Directory: "prepared-files", ManifestSHA256: receipt.ManifestSHA256})
		}
		plan, err := os.ReadFile(filepath.Join(output, "plan.json"))
		if err != nil {
			t.Fatal(err)
		}
		input, err := os.ReadFile(filepath.Join(output, "input.json"))
		if err != nil {
			t.Fatal(err)
		}
		useWalletPreparation(t, &f, WalletPreparation{Plan: plan, Preparation: input})
	}
	selector, err := GenerationSelector(f.receiver)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := NewUsageGenerationGuard(authRO, selector)
	if err != nil {
		t.Fatal(err)
	}
	g, err := NewGeneration(f.receiver, f.funding, f.root, guard)
	if err != nil {
		t.Fatal(err)
	}
	originalGeneration := g
	t.Cleanup(func() { _ = originalGeneration.Close() })
	service, err := NewUsageHTTPService(g, authRO)
	if err != nil {
		t.Fatal(err)
	}
	var worker *LocalWorker
	var runtime *LocalRuntime
	var deployment *Deployment
	var verifying chan struct{}
	var resume func()
	if issue {
		verifying = make(chan struct{})
		proceed := make(chan struct{})
		var once sync.Once
		resume = func() { once.Do(func() { close(proceed) }) }
		var started sync.Once
		wait := func(ctx context.Context) error {
			started.Do(func() { close(verifying) })
			select {
			case <-proceed:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		var metadata AutomaticContext = func(ctx context.Context, _ monthlytasks.Request) (AutomaticSourceContext, error) {
			if err := wait(ctx); err != nil {
				return AutomaticSourceContext{}, err
			}
			return AutomaticSourceContext{BundleContext: handoffContext(t, f)}, nil
		}
		var load AutomaticScopeLoader
		if observed {
			scope := automaticScopeFixture(t, f)
			load = func(ctx context.Context, _ monthlytasks.Request) (AutomaticScope, error) {
				if err := wait(ctx); err != nil {
					return AutomaticScope{}, err
				}
				return scope, nil
			}
			metadata, err = NewObservedAutomaticContext(g, load)
			if err != nil {
				t.Fatal(err)
			}
		}
		publish := func(ctx context.Context, req monthlytasks.Request, raw []byte) error {
			_, err := m.PublishMonthlyBillForAuthorization(ctx, raw, monitor.MonthlyBillExpectedAuthorization{
				GroupID: req.Owner.GroupID, AuthVersion: req.Owner.AuthVersion,
				MemberFingerprint: req.Owner.MemberFingerprint, MemberManifest: req.Owner.MemberManifest})
			return err
		}
		rollout := GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: "2026-09"}}}
		if retainedPrepared || len(usage) > 4 && usage[4] {
			if err := os.Chmod(f.receiverPath, 0600); err != nil {
				t.Fatal(err)
			}
			cfg := DeploymentConfig{Rollout: rollout, ReceiverPath: f.receiverPath, FundingPath: fundingPath, AuthorityPath: authPath, EvidenceRoot: f.root}
			if inboxPreparation {
				cfg.PreparationInbox = "inbox.json"
			}
			if retainedScope {
				deployment, err = OpenRetainedAutomaticDeployment(ctx, cfg, syntheticScopeID, publish)
			} else {
				deployment, err = OpenAutomaticDeployment(ctx, cfg, load, publish)
			}
			if err == nil {
				runtime, g = deployment.runtime, deployment.g
				t.Cleanup(func() { resume(); closeDeploymentTest(t, deployment) })
			}
		} else {
			runtime, err = NewAutomaticLocalRuntime(g, authRO, rollout, metadata, publish)
		}
		if err != nil {
			t.Fatal(err)
		}
		service, worker = runtime, runtime.worker
		if deployment != nil {
			service = deployment
		}
		worker.pollMin = time.Millisecond
		worker.pollMax = 10 * time.Millisecond
		if runtime.Phase() != "disabled" || worker.Status().Running {
			t.Fatal("automatic constructor started processing")
		}
		start := runtime.Start
		if deployment != nil {
			start = deployment.Start
		}
		if err := start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resume(); stopRuntime(t, runtime) })
	} else if automatic {
		worker = testLocalWorker(t, g)
		service, err = NewUsageHTTPServiceWithWorker(g, authRO, worker)
		if err != nil {
			t.Fatal(err)
		}
	}
	r := gin.New()
	r.Use(gin.Recovery())
	if err := r.SetTrustedProxies(nil); err != nil {
		t.Fatal(err)
	}
	if err := m.RegisterPortalRoutesWithGeneration(r, service); err != nil {
		t.Fatal(err)
	}
	request := func(method, path, body string, cookies []*http.Cookie) *httptest.ResponseRecorder {
		t.Helper()
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
		t.Helper()
		w := request("POST", "/login", `{"email":"`+email+`","password":"`+password+`"}`, nil)
		if w.Code != 200 || len(w.Result().Cookies()) == 0 {
			t.Fatal("real local login failed", w.Code, w.Body.String())
		}
		return w.Result().Cookies()
	}
	a, b := login("owner@example.invalid"), login("other@example.invalid")
	const path = "/api/monthly-statement/generation"
	check := func(method, want string, cookies []*http.Cookie) {
		t.Helper()
		p, body, code := path+"?month=2026-09", "", 200
		if method == "POST" {
			p, body, code = path, `{"month":"2026-09"}`, 202
		}
		w := request(method, p, body, cookies)
		var out map[string]any
		decodeErr := json.Unmarshal(w.Body.Bytes(), &out)
		stateOK, codeOK := out["state"] == want, w.Code == code
		if issue && want == "queued" {
			stateOK = out["state"] == "queued" || out["state"] == "processing" || out["state"] == "finalizing" || out["state"] == "available"
			codeOK = w.Code == code || (w.Code == 200 && out["state"] == "available")
		}
		if !codeOK || decodeErr != nil || len(out) != 3 || out["month"] != "2026-09" || !stateOK {
			t.Fatal("unexpected real journey response", w.Code, w.Body.String(), want)
		}
		for _, private := range []string{"source_identity", "task_id", "input_sha", "user_id", "published", "downloadable", "208.000000"} {
			if strings.Contains(w.Body.String(), private) {
				t.Fatal("private draft exposed", private)
			}
		}
	}
	if !inboxPreparation {
		check("GET", "not_prepared", a)
	}
	var selected GenerationSelection
	if inboxPreparation {
		deadline := time.Now().Add(20 * time.Second)
		for {
			selected, err = ReadGenerationSelection(ctx, f.receiver, 1, "2026-09")
			if err != nil {
				t.Fatal(err)
			}
			if selected.Request != nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("running inbox did not prepare the customer's allowed month")
			}
			time.Sleep(10 * time.Millisecond)
		}
		preparationCounts(t, f, 1, 1) // intake did not submit a task for the customer
	} else if runningPreparation {
		if deployment == nil {
			t.Fatal("running preparation requires the actual resource owner")
		}
		plan := mustGenerationPlan(t, f)
		preview, err := deployment.PrepareMonthlyInput(ctx, plan, f.raw, "", false)
		if err != nil || preview.Applied {
			t.Fatal("running preparation preview failed", preview, err)
		}
		preparationCounts(t, f, 0, 0)
		prepared, err := deployment.PrepareMonthlyInput(ctx, plan, f.raw, "", true)
		if err != nil || !prepared.Applied || !prepared.Changed {
			t.Fatal("running preparation could not select qualified inputs", prepared, err)
		}
		preparationCounts(t, f, 1, 1)
		selected, err = ReadGenerationSelection(ctx, f.receiver, 1, "2026-09")
		if err != nil || selected.Request == nil || selected.Head != prepared.SelectionHead || runtime.Phase() != "running" {
			t.Fatal("running preparation lost selection or restarted service", selected, err)
		}
	} else {
		selected = stageFixture(t, f, "")
	}
	check("GET", "not_requested", a)
	check("POST", "queued", a)
	check("POST", "queued", a)
	generationAdmissionCounts(t, f.receiver, 1, 1)
	job, err := g.tasks.Find(ctx, *selected.Request)
	if err != nil {
		t.Fatal(err)
	}
	if automatic {
		if !issue && (len(worker.wake) != 1 || worker.Status().Running) {
			t.Fatal("HTTP did not coalesce wakeups or started an implicit worker")
		}
		if issue {
			if !retainedScope {
				select {
				case <-verifying:
				case <-time.After(5 * time.Second):
					t.Fatal("automatic verification did not begin", worker.Status())
				}
				check("GET", "finalizing", a)
				if w := request("GET", "/api/monthly-statement?month=2026-09", "", a); w.Code != 404 {
					t.Fatal("unfinished verification became visible", w.Code)
				}
				resume()
			}
			deadline := time.Now().Add(5 * time.Second)
			for worker.Status().Published != 1 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			stopRuntime(t, runtime)
			if worker.Status().Published != 1 {
				t.Fatal("on-demand publication did not complete", worker.Status())
			}
		} else {
			workerCtx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- worker.Serve(workerCtx) }()
			var once sync.Once
			stop := func() { once.Do(func() { cancel(); <-done }) }
			t.Cleanup(stop)
			waitLocalDraft(t, g, job)
			stop()
		}
	}
	artifact, err := g.Run(ctx, owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if materialPreparation && artifact.TokenSHA256 == "" {
		t.Fatal("material publication omitted token audit")
	}
	if machine {
		assertGenerationAmounts(t, artifact, 1)
	} else {
		assertGenerationAmounts(t, artifact)
	}
	if deployment != nil {
		// Frozen statements must remain readable through real routes even after
		// generation connections and its exclusive process lock are released.
		closeDeploymentTest(t, deployment)
	}
	if issue {
		check("GET", "available", a)
		if w := request("POST", path, `{"month":"2026-09"}`, a); w.Code != 200 || !strings.Contains(w.Body.String(), `"state":"available"`) {
			t.Fatal("saved publication regenerated", w.Code, w.Body.String())
		}
	} else {
		check("GET", "pending_verification", a)
		check("POST", "pending_verification", a)
		check("GET", "not_prepared", b)
	}
	if w := request("POST", path, `{"month":"2026-09"}`, b); w.Code < 400 {
		t.Fatal("other organization submitted existing task", w.Code)
	}
	// Draft creation must not turn an unreviewed month into a formal statement.
	if w := request("GET", "/api/monthly-statement?month=2026-09", "", a); !issue && w.Code != 404 {
		t.Fatal("internal draft became formal bill", w.Code, w.Body.String())
	}
	if automatic && !issue {
		testGenerationReviewedPublication(t, g, f, job, auth, request, a, b)
	}
	if issue {
		w := request("GET", "/api/monthly-statement?month=2026-09", "", a)
		var detail struct {
			Data struct {
				Publication string `json:"publication"`
				Opening     string `json:"month_opening_balance"`
				Closing     string `json:"month_end_balance"`
				Members     []struct {
					UserID      int64  `json:"user_id"`
					Opening     string `json:"opening"`
					Credit      string `json:"credit_total"`
					Adjustment  string `json:"adjustment"`
					Closing     string `json:"closing"`
					Consumption struct {
						Amount string `json:"amount"`
						Refund string `json:"refund"`
					} `json:"consumption"`
				} `json:"members"`
			} `json:"data"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &detail) != nil || detail.Data.Opening != "2000.000000" || detail.Data.Closing != "2006.000000" || len(detail.Data.Publication) != 64 {
			t.Fatal("automatic saved statement money", w.Code, w.Body.String())
		}
		download := request("GET", "/api/monthly-statement/download?month=2026-09&publication="+detail.Data.Publication, "", a)
		if download.Code != 200 || !strings.HasPrefix(download.Body.String(), "PK") {
			t.Fatal("automatic saved statement download", download.Code)
		}
		if retainedPrepared {
			// Group totals alone cannot detect swapping one user's money with
			// another's. Compare each immutable user in BOTH HTTP and XLSX.
			expected := map[int64]monitor.MonthlyPreparationAmounts{}
			for _, member := range artifact.Reconciliation.Accounts {
				expected[member.UserID] = *member.Months[0].Amounts
			}
			if len(detail.Data.Members) != len(expected) {
				t.Fatal("customer page omitted organization members")
			}
			seen := map[int64]bool{}
			for _, member := range detail.Data.Members {
				want, ok := expected[member.UserID]
				got := monitor.MonthlyPreparationAmounts{Opening: member.Opening, Increase: member.Credit, Decrease: member.Adjustment,
					Consume: member.Consumption.Amount, Refund: member.Consumption.Refund, Closing: member.Closing}
				if !ok || seen[member.UserID] || want != got {
					t.Fatal("customer page mixed up per-user money")
				}
				seen[member.UserID] = true
			}
			parts := retainedWorkbookParts(t, download.Body.Bytes())
			var sheet struct {
				Rows []struct {
					Cells []struct {
						Ref   string `xml:"r,attr"`
						Value string `xml:"v"`
						Text  string `xml:"is>t"`
					} `xml:"c"`
				} `xml:"sheetData>row"`
			}
			if err := xml.Unmarshal(parts["xl/worksheets/sheet2.xml"], &sheet); err != nil {
				t.Fatal("account worksheet unavailable", err)
			}
			exported := map[int64]bool{}
			for _, row := range sheet.Rows {
				if len(row.Cells) != 7 || !strings.Contains(row.Cells[0].Text, "\nID ") {
					continue
				}
				var id int64
				_, idText, _ := strings.Cut(row.Cells[0].Text, "\nID ")
				if json.Unmarshal([]byte(idText), &id) != nil {
					t.Fatal("export lost immutable user ID")
				}
				want, ok := expected[id]
				if ok {
					want = customerMoneyAmounts(t, want)
				}
				got := monitor.MonthlyPreparationAmounts{Opening: row.Cells[1].Value, Increase: row.Cells[2].Value, Decrease: row.Cells[3].Value,
					Consume: row.Cells[4].Value, Refund: row.Cells[5].Value, Closing: row.Cells[6].Value}
				if !ok || exported[id] || got != want {
					t.Fatal("download mixed up per-user money")
				}
				exported[id] = true
			}
			if len(exported) != len(expected) {
				t.Fatal("download omitted organization members")
			}
		}
		generationAdmissionCounts(t, f.receiver, 1, 1)
	}
	if _, err := WithdrawGenerationSelection(ctx, f.receiver, 1, "2026-09", selected.Head); err != nil {
		t.Fatal(err)
	}
	if automatic {
		check("GET", "available", a)
		if w := request("POST", path, `{"month":"2026-09"}`, a); w.Code != 200 || !strings.Contains(w.Body.String(), `"state":"available"`) {
			t.Fatal("saved bill depended on retired preparation", w.Code, w.Body.String())
		}
	} else {
		check("GET", "needs_preparation", a)
		if w := request("POST", path, `{"month":"2026-09"}`, a); w.Code != 409 {
			t.Fatal("withdrawn input admitted", w.Code)
		}
	}
	if err := auth.Model(&monitor.CustomerGroup{}).Where("id = 1").Update("portal_auth_ver", 2).Error; err != nil {
		t.Fatal(err)
	}
	if w := request("GET", path+"?month=2026-09", "", a); w.Code != 401 {
		t.Fatal("old login remained authorized", w.Code)
	}
	if automatic {
		if w := request("GET", "/api/monthly-statement?month=2026-09", "", a); w.Code != 401 {
			t.Fatal("revoked login could still read published bill", w.Code)
		}
	}
	generationAdmissionCounts(t, f.receiver, 1, 1)
	source, err := os.ReadFile(f.path)
	if err != nil || generationHash(source) != f.sourceHash {
		t.Fatal("source data changed", err)
	}
}

func TestUsageGenerationHTTPServiceRejectsIncompleteSetup(t *testing.T) {
	_, authRO, _ := usageAuthorityFixture(t)
	for _, g := range []*Generation{nil, {}} {
		if service, err := NewUsageHTTPService(g, authRO); err == nil || service != nil {
			t.Fatal("incomplete engine accepted")
		}
	}
}
