package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

func TestExistingRetainedScopeMatchesOriginalPackagingWithoutWriting(t *testing.T) {
	f := normalizedScopePreparationFixture(t)
	path := filepath.Join(f.root, "existing-scope.json")
	want, err := f.g.PrepareRetainedScopeInput(t.Context(), f.plan, f.raw, syntheticScopeID, path, automaticScopeFixture(t, f))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, want.Scope, 0600); err != nil {
		t.Fatal(err)
	}
	ref := monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(want.Scope)}
	got, err := f.g.PrepareExistingRetainedScopeInput(t.Context(), f.plan, f.raw, syntheticScopeID, ref)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("existing file changed calculation or input identity", err)
	}
	body, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(body, want.Scope) {
		t.Fatal("scope file rewritten", err)
	}
	preparationCounts(t, f, 0, 0)
	// The original scope reader permits surrounding whitespace. Keep the
	// ACTUAL retained bytes/hash instead of silently canonicalizing that file.
	padded := append(append([]byte("\n"), want.Scope...), '\n')
	if err := os.WriteFile(path, padded, 0600); err != nil {
		t.Fatal(err)
	}
	ref.SHA256 = generationHash(padded)
	got, err = f.g.PrepareExistingRetainedScopeInput(t.Context(), f.plan, f.raw, syntheticScopeID, ref)
	if err != nil || !bytes.Equal(got.Scope, padded) {
		t.Fatal("legacy scope bytes not preserved", err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(got.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	last := input.Sources[len(input.Sources)-1]
	if last.Path != path || last.SHA256 != ref.SHA256 {
		t.Fatal("pinned a synthetic rather than original digest")
	}
}

type capturedHistoryFixture struct {
	f       generationFixture
	auth    *gorm.DB
	wallet  *WalletSourcePreparation
	spool   *CapturedSourceSpool
	req     SourcePreparationRequest
	cfg     DeploymentConfig
	sources CapturedHistorySources
	index   string
}

func newCapturedHistoryFixture(t *testing.T, tokenEdits ...func(*monthlysource.TokenUsageWindowObservation)) capturedHistoryFixture {
	t.Helper()
	f, auth, p, req, _ := capturedFilesFixture(t, tokenEdits...)
	root, err := filepath.EvalSymlinks(f.root)
	if err != nil {
		t.Fatal(err)
	}
	// New independent SYNTHETIC historical cutoffs: both accounts start at
	// 1000, have no funding events, consume 3/refund 1, close at 998. They are
	// not inferred from the intentionally different current wallet captures.
	history := draftCopy(t, f.input)
	for i := range history.Accounts {
		a := &history.Accounts[i]
		a.Movements = []monitor.MonthlyPreparationMovement{}
		a.Spans = []monitor.MonthlyPreparationSpan{}
		a.TokenMonths = nil
		a.Snapshots = a.Snapshots[:1:1]
		a.Snapshots[0].BalanceQuota = 499000000
		a.Snapshots[0].UsedQuota = 1001000000
	}
	basisPath := filepath.Join(root, "independent-history-basis.json")
	basis := materialPreparationJSON(t, history.Accounts)
	if err := os.WriteFile(basisPath, basis, 0600); err != nil {
		t.Fatal(err)
	}
	history.Sources = []monitor.MonthlyPreparationSource{{ID: "fixture", Path: basisPath, SHA256: generationHash(basis)}}
	historyPath := filepath.Join(root, "independent-history-catalogue.json")
	raw := draftJSON(t, history)
	if err := os.WriteFile(historyPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	f.input = history
	f.raw = raw
	scope := RetainedAutomaticScope{Version: 1, Owner: req.Owner, Month: req.Month, SourceIdentity: p.funding.sourceIdentity, Scope: automaticScopeFixture(t, f)}
	scopeRaw := materialPreparationJSON(t, scope)
	scopePath := filepath.Join(root, "independent-history-scope.json")
	if err := os.WriteFile(scopePath, scopeRaw, 0600); err != nil {
		t.Fatal(err)
	}
	spoolPath := filepath.Join(root, "captured-sources")
	if err := os.Mkdir(spoolPath, 0700); err != nil {
		t.Fatal(err)
	}
	spool, err := NewCapturedSourceSpool(p, spoolPath)
	if err != nil {
		t.Fatal(err)
	}
	authDB, err := auth.DB()
	if err != nil {
		t.Fatal(err)
	}
	var seq int
	var name, authorityPath string
	if err := authDB.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &authorityPath); err != nil {
		t.Fatal(err)
	}
	if err := auth.AutoMigrate(&monitor.PortalMonthlyBill{}); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(f.receiverPath, 0600); err != nil {
		t.Fatal(err)
	}
	index := "captured-history-series.json"
	indexRaw := materialPreparationJSON(t, AutomaticDisplaySeriesIndex{Version: 1, Ranges: []AutomaticDisplaySeriesRange{{GroupID: req.Owner.GroupID, SourceIdentity: p.funding.sourceIdentity, From: req.Month, Through: req.Month}}})
	if err := os.WriteFile(filepath.Join(root, index), indexRaw, 0600); err != nil {
		t.Fatal(err)
	}
	return capturedHistoryFixture{f: f, auth: auth, wallet: p, spool: spool, req: req, index: index,
		cfg:     DeploymentConfig{Rollout: GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: req.Owner.GroupID, Month: req.Month}}}, ReceiverPath: f.receiverPath, FundingPath: f.path, AuthorityPath: authorityPath, EvidenceRoot: root},
		sources: CapturedHistorySources{History: monthlysource.MonthlyObservationFile{Path: historyPath, SHA256: generationHash(raw)}, Scope: monthlysource.MonthlyObservationFile{Path: scopePath, SHA256: generationHash(scopeRaw)}}}
}

func (f capturedHistoryFixture) open(t *testing.T) *Deployment {
	t.Helper()
	d, err := OpenRetainedAutomaticDisplaySeriesDeployment(t.Context(), f.cfg, "captured-scope", f.index, displaySeriesPublisher(f.auth))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, d) })
	return d
}

func TestCapturedHistoryNormalizeUsesOriginalMoneyAndPinsExistingScope(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d := f.open(t)
	files, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := d.g.prepareCapturedHistoryInput(t.Context(), f.req, files, f.sources, "captured-scope")
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(prepared.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Accounts) != 2 {
		t.Fatal("missing member")
	}
	for _, a := range input.Accounts {
		if a.Baseline.BalanceQuota != 500000000 || a.Snapshots[0].BalanceQuota != 499000000 || len(a.TokenMonths) != 1 || len(a.TokenMonths[0].Rows) != 3 || len(a.Movements) != 0 {
			t.Fatal("history or log attribution changed", a.UserID)
		}
		var consumed, refunded int64
		for _, r := range a.TokenMonths[0].Rows {
			consumed += r.ConsumeQuota
			refunded += r.RefundQuota
		}
		if consumed != 1500000 || refunded != 500000 {
			t.Fatal("wrong per-account raw amounts", a.UserID, consumed, refunded)
		}
	}
	var plan GenerationPlan
	if json.Unmarshal(prepared.Plan, &plan) != nil {
		t.Fatal("invalid plan")
	}
	found := false
	for _, s := range input.Sources {
		if s.ID == "captured-scope" {
			found = s.Path == f.sources.Scope.Path && s.SHA256 == f.sources.Scope.SHA256
		}
	}
	if !found || plan.AutomaticScopeSourceID != "captured-scope" {
		t.Fatal("lost pinned original scope")
	}
	if original, err := os.ReadFile(f.sources.Scope.Path); err != nil || !bytes.Equal(original, prepared.Scope) {
		t.Fatal("rewrote original scope", err)
	}
	preparationCounts(t, f.f, 0, 0)
}

func TestCapturedHistoryOnDemandSeriesActuallyPublishesOnce(t *testing.T) {
	for _, mode := range []string{"exact_wallet", "historical_reconstruction"} {
		t.Run(mode, func(t *testing.T) {
			f := newCapturedHistoryFixture(t)
			if mode == "historical_reconstruction" {
				addCapturedHistoryReconstruction(t, &f)
			}
			testCapturedHistoryPublication(t, f, false, false)
		})
	}
}

func addCapturedHistoryReconstruction(t *testing.T, f *capturedHistoryFixture) {
	t.Helper()
	body, err := os.ReadFile(f.sources.History.Path)
	if err != nil {
		t.Fatal(err)
	}
	history, err := monitor.DecodeMonthlyPreparationInput(body)
	if err != nil {
		t.Fatal(err)
	}
	a := &history.Accounts[0]
	end := a.Snapshots[0].At
	a.Baseline.TimeBasis, a.Baseline.CutoffSource = "collection_observation", ""
	a.Snapshots[0].TimeBasis, a.Snapshots[0].CutoffSource = "collection_observation", ""
	a.WalletMonths = nil
	zero := int64(0)
	a.Spans = append(a.Spans, monitor.MonthlyPreparationSpan{UserID: a.UserID, From: end, To: end + 3600, Requests: &zero, Tokens: &zero, Source: "captured-reconstruction#independent-bridge"})
	a.MovementCoverage[0].To = end + 3600
	q, used, consume, refund := int64(499000000), int64(1001000000), int64(1500000), int64(500000)
	r := monitor.MonthlySnapshotReconstruction{UserID: a.UserID, Month: f.req.Month, ObservedAt: end + 3600, SnapshotSource: "captured-reconstruction#snapshot", ScopeSource: "captured-reconstruction#scope", MonthSource: "captured-reconstruction#month", BridgeSource: "captured-reconstruction#bridge", FundingSource: "captured-reconstruction#funding", OpeningBasisSource: "fixture#exact-opening", BalanceQuota: &q, UsedQuota: &used, MonthConsumeQuota: &consume, MonthRefundQuota: &refund, BridgeConsumeQuota: &zero, BridgeRefundQuota: &zero, FundingEventIDs: []int64{}}
	proof := materialPreparationJSON(t, r)
	path := filepath.Join(f.cfg.EvidenceRoot, "independent-reconstruction.json")
	if err := os.WriteFile(path, proof, 0600); err != nil {
		t.Fatal(err)
	}
	history.Sources = append(history.Sources, monitor.MonthlyPreparationSource{ID: "captured-reconstruction", Path: path, SHA256: generationHash(proof)})
	basis := materialPreparationJSON(t, history.Accounts)
	if err := os.WriteFile(history.Sources[0].Path, basis, 0600); err != nil {
		t.Fatal(err)
	}
	history.Sources[0].SHA256 = generationHash(basis)
	body = draftJSON(t, history)
	if err := os.WriteFile(f.sources.History.Path, body, 0600); err != nil {
		t.Fatal(err)
	}
	f.sources.History.SHA256 = generationHash(body)
	scopeRaw, err := os.ReadFile(f.sources.Scope.Path)
	if err != nil {
		t.Fatal(err)
	}
	var doc RetainedAutomaticScope
	if json.Unmarshal(scopeRaw, &doc) != nil {
		t.Fatal("invalid fixture")
	}
	doc.Scope.Accounts[0].Reconstructions = []monitor.MonthlySnapshotReconstruction{r}
	scopeRaw = materialPreparationJSON(t, doc)
	if err := os.WriteFile(f.sources.Scope.Path, scopeRaw, 0600); err != nil {
		t.Fatal(err)
	}
	f.sources.Scope.SHA256 = generationHash(scopeRaw)
}

func testCapturedHistoryPublication(t *testing.T, f capturedHistoryFixture, clickThroughScript, registeredCalendar bool) {
	t.Helper()
	testCapturedHistoryPublicationAdapter(t, f, clickThroughScript, registeredCalendar, false, false)
}

func testCapturedHistoryPublicationAdapter(t *testing.T, f capturedHistoryFixture, clickThroughScript, registeredCalendar, catalogueSources, startupInstaller bool, beforeStart ...func(*Deployment)) {
	t.Helper()
	d := f.open(t)
	loads := 0
	removableSource := f.sources.Scope.Path
	index := CapturedHistoryIndex{Version: 1, Entries: []CapturedHistoryIndexEntry{{GroupID: f.req.Owner.GroupID, Month: f.req.Month, History: f.sources.History, Scope: f.sources.Scope}}}
	indexRaw := materialPreparationJSON(t, index)
	indexPath := filepath.Join(f.cfg.EvidenceRoot, "captured-history-index.json")
	if err := os.WriteFile(indexPath, indexRaw, 0600); err != nil {
		t.Fatal(err)
	}
	load, err := NewCapturedHistoryFileLoader(d.g, monthlysource.MonthlyObservationFile{Path: indexPath, SHA256: generationHash(indexRaw)})
	if err != nil {
		t.Fatal(err)
	}
	next, err := NewCapturedHistoryPreparation(d, "captured-scope", func(ctx context.Context, req SourcePreparationRequest) (CapturedHistorySources, error) {
		loads++
		if req != f.req {
			return CapturedHistorySources{}, ErrGenerationChanged
		}
		return load(ctx, req)
	})
	if err != nil {
		t.Fatal(err)
	}
	if catalogueSources {
		_, ref := capturedHistoryCatalogueFixture(t, f)
		removableSource = ref.Path
		catalogueNext, err := NewCatalogueCapturedHistoryPreparation(d, f.spool, "captured-scope", f.sources.History, ref)
		if err != nil {
			t.Fatal(err)
		}
		next = func(ctx context.Context, req SourcePreparationRequest, files CapturedSourceFiles) (bool, error) {
			loads++
			return catalogueNext(ctx, req, files)
		}
	}
	if startupInstaller {
		_, config := capturedPreparationConfigFixture(t, f)
		if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", config, capturedPreparationTestOpener(f)); err != nil {
			t.Fatal(err)
		}
		removableSource = filepath.Join(f.cfg.EvidenceRoot, "shared-scope-catalogue.json")
	} else {
		step, err := ComposeCapturedSourcePreparation(f.wallet, f.spool, next)
		if err != nil {
			t.Fatal(err)
		}
		authority := d.runtime.service.(*usageHTTPService).authorization
		if err := d.runtime.EnableOnDemandPreparation(t.Context(), authority, func(_ context.Context, req SourcePreparationRequest) error {
			if req != f.req {
				return ErrGenerationChanged
			}
			return nil
		}, step); err != nil {
			t.Fatal(err)
		}
		if registeredCalendar {
			if err := d.EnableRegistrationCalendar(f.wallet.funding); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, setup := range beforeStart {
		setup(d)
	}
	request, own, foreign := capturedHistoryHTTP(t, f, d)
	if f.wallet.funding.tokenRecovery {
		original := d.runtime.preparation.step
		d.runtime.preparation.step = func(ctx context.Context, req SourcePreparationRequest) (bool, error) {
			ready, err := original(ctx, req)
			if err != nil {
				t.Log("synthetic recovery preparation stage:", err)
			}
			return ready, err
		}
	}
	d.runtime.worker.pollMin = 5 * time.Millisecond
	d.runtime.worker.pollMax = 20 * time.Millisecond
	if loads != 0 {
		t.Fatal("construction loaded history")
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	var calendarRaw []byte
	if registeredCalendar {
		out := request("GET", "/api/monthly-statement/calendar", "", own)
		var calendar monitor.MonthlyGenerationCalendar
		if out.Code != 200 || json.Unmarshal(out.Body.Bytes(), &calendar) != nil || !reflect.DeepEqual(calendar.Months, []string{f.req.Month}) || calendar.FirstMonth != "2026-09" {
			t.Fatal("actual registration range unavailable", out.Code, out.Body.String())
		}
		calendarRaw = bytes.Clone(out.Body.Bytes())
		for _, path := range []string{"/api/monthly-statement/calendar?group_id=2", "/api/monthly-statement/calendar?month=2026-09"} {
			if out := request("GET", path, "", own); out.Code != 400 {
				t.Fatal("calendar accepted client scope", out.Code)
			}
		}
		if out := request("GET", "/api/monthly-statement/calendar", "", foreign); out.Code == 200 {
			t.Fatal("unsupported organization received calendar")
		}
		for _, month := range []string{"2026-08", time.Now().In(time.FixedZone("CST", 28800)).Format("2006-01")} {
			if out := request("POST", "/api/monthly-statement/generation", `{"month":"`+month+`"}`, own); out.Code < 400 {
				t.Fatal("invalid month entered generation", month)
			}
		}
		page := request("GET", "/", "", own)
		if page.Code != 200 || !bytes.Contains(page.Body.Bytes(), []byte("/*MONTHLY_CALENDAR_ENABLED*/ true")) {
			t.Fatal("registered page omitted range integration")
		}
	}
	if out := request("POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`, foreign); out.Code != 409 {
		t.Fatal("foreign request admitted", out.Code)
	}
	// Use the ACTUAL first customer GET before POST. A backend test that
	// submits directly misses the UI contract: only not_requested enables
	// the first button; not_prepared intentionally keeps it disabled.
	initial := request("GET", "/api/monthly-statement/generation?month=2026-09", "", own)
	var first struct {
		Month string `json:"month"`
		State string `json:"state"`
	}
	if initial.Code != 200 || json.Unmarshal(initial.Body.Bytes(), &first) != nil || first.Month != f.req.Month || first.State != "not_requested" {
		t.Fatal("first customer request cannot enable generation", initial.Code, initial.Body.String())
	}
	method, path, body := "POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`
	if clickThroughScript {
		method, path, body = capturedHistoryFirstButtonRequest(t, initial.Body.Bytes(), calendarRaw)
	}
	if out := request(method, path, body, own); out.Code != 202 {
		t.Fatal("customer generation HTTP failed", out.Code, out.Body.String())
	}
	deadline := time.Now().Add(20 * time.Second)
	var record monitor.PortalMonthlyBill
	for {
		err := f.auth.First(&record, "group_id=? AND month=?", f.req.Owner.GroupID, f.req.Month).Error
		if err == nil {
			break
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			state, steps, failures := preparationState(t, f.f.receiver)
			t.Fatal("no saved bill", state, steps, failures, d.runtime.worker.Status())
		}
		time.Sleep(20 * time.Millisecond)
	}
	closeDeploymentTest(t, d)
	pageBefore, bookBefore := assertCapturedHistoryCustomerDownload(t, request, own, foreign)
	if (!startupInstaller && loads != 1) || (startupInstaller && loads != 0) {
		t.Fatal("history repeatedly loaded", loads)
	}
	var e monitor.MonthlyBillEvidence
	if json.Unmarshal([]byte(record.Evidence), &e) != nil || len(e.Users) != 2 {
		t.Fatal("invalid saved evidence")
	}
	for _, u := range e.Users {
		if u.OpeningQuota != 500000000 || u.ClosingQuota != 499000000 || u.ConsumeQuota != 1500000 || u.RefundQuota != 500000 || u.TokenUsage == nil || len(u.TokenUsage.Rows) != 3 {
			t.Fatal("wrong customer money or missing token detail", u.UserID)
		}
	}
	var payload struct {
		MonthOpeningBalance *string           `json:"month_opening_balance"`
		MonthEndBalance     *string           `json:"month_end_balance"`
		Members             []json.RawMessage `json:"members"`
		Display             json.RawMessage   `json:"display_balances"`
	}
	if json.Unmarshal([]byte(record.Payload), &payload) != nil || payload.MonthOpeningBalance == nil || *payload.MonthOpeningBalance != "2000.000000" || payload.MonthEndBalance == nil || *payload.MonthEndBalance != "1996.000000" || len(payload.Members) != 2 || len(payload.Display) == 0 {
		t.Fatal("wrong customer saved payload", record.Payload)
	}
	reopened := f.open(t)
	if worked, err := reopened.runtime.worker.step(t.Context()); err != nil || worked {
		t.Fatal("restart issued duplicate", worked, err)
	}
	var after monitor.PortalMonthlyBill
	if err := f.auth.First(&after, "group_id=? AND month=?", f.req.Owner.GroupID, f.req.Month).Error; err != nil || after != record {
		t.Fatal("restart changed frozen bill", err)
	}
	// Remove ONLY this test's newly manufactured proof, never retained user
	// evidence. A saved bill/download must not depend on generation being live.
	if err := os.Remove(removableSource); err != nil {
		t.Fatal(err)
	}
	pageAfter, bookAfter := assertCapturedHistoryCustomerDownload(t, request, own, foreign)
	if !bytes.Equal(pageBefore, pageAfter) || !bytes.Equal(bookBefore, bookAfter) {
		t.Fatal("saved page/download recomputed money from missing generation sources")
	}
}

func TestCapturedHistoryRejectsIncompleteConflictingOrWrongScope(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d := f.open(t)
	files, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	oldHistory, err := os.ReadFile(f.sources.History.Path)
	if err != nil {
		t.Fatal(err)
	}
	oldScope, err := os.ReadFile(f.sources.Scope.Path)
	if err != nil {
		t.Fatal(err)
	}
	oldSelection, err := os.ReadFile(files.Selection.Path)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"missing_member", "wrong_group", "missing_month", "usage_one_quota", "not_wallet_only", "missing_scope_member", "wrong_owner", "wrong_month", "wrong_source", "self_scope", "changed_original", "wrong_capture_key", "wrong_mode", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			sources, selected := f.sources, files
			history, err := monitor.DecodeMonthlyPreparationInput(oldHistory)
			if err != nil {
				t.Fatal(err)
			}
			var scope RetainedAutomaticScope
			if json.Unmarshal(oldScope, &scope) != nil {
				t.Fatal("invalid fixture")
			}
			switch scenario {
			case "missing_member":
				history.Accounts = history.Accounts[:1]
			case "wrong_group":
				history.Accounts[0].GroupID++
			case "missing_month":
				history.Accounts[0].Months = []string{"2026-08"}
			case "usage_one_quota":
				history.Accounts[0].Spans = []monitor.MonthlyPreparationSpan{{UserID: 201, From: history.Accounts[0].Baseline.At, To: history.Accounts[0].Snapshots[0].At, ConsumeQuota: 1500001, RefundQuota: 500000, Source: "fixture#usage"}}
			case "not_wallet_only":
				scope.Scope.Accounts[0].WalletOnly = false
			case "missing_scope_member":
				scope.Scope.Group.Members = scope.Scope.Group.Members[:1]
			case "wrong_owner":
				scope.Owner.AuthVersion++
			case "wrong_month":
				scope.Month = "2026-08"
			case "wrong_source":
				scope.SourceIdentity = generationHash([]byte("another synthetic source"))
			case "self_scope":
				scope.Scope.Accounts[0].WalletSource = "captured-scope#self"
			case "wrong_capture_key":
				var s monthlysource.CapturedMonthSelection
				if json.Unmarshal(oldSelection, &s) != nil {
					t.Fatal("invalid fixture")
				}
				s.Key = generationHash([]byte("another request"))
				raw := materialPreparationJSON(t, s)
				path := filepath.Join(f.cfg.EvidenceRoot, "wrong-request-selection.json")
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
				selected.Selection = monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(raw)}
			case "wrong_mode":
				selected.Mode = "not_a_sealed_capture"
			}
			historyRaw, scopeRaw := draftJSON(t, history), materialPreparationJSON(t, scope)
			for _, entry := range []struct {
				name string
				raw  []byte
				out  *monthlysource.MonthlyObservationFile
			}{{"negative-history-" + scenario + ".json", historyRaw, &sources.History}, {"negative-scope-" + scenario + ".json", scopeRaw, &sources.Scope}} {
				path := filepath.Join(f.cfg.EvidenceRoot, entry.name)
				if err := os.WriteFile(path, entry.raw, 0600); err != nil {
					t.Fatal(err)
				}
				*entry.out = monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(entry.raw)}
			}
			if scenario == "changed_original" {
				sources.Scope.SHA256 = generationHash([]byte("stale retained hash"))
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if scenario == "cancel" {
				cancel()
			}
			got, err := d.g.prepareCapturedHistoryInput(ctx, f.req, selected, sources, "captured-scope")
			if err == nil || len(got.Plan) != 0 || len(got.Preparation) != 0 {
				t.Fatal("invalid evidence produced usable money", err)
			}
			if scenario == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			preparationCounts(t, f.f, 0, 0)
		})
	}
}

func TestCapturedHistorySelectionLifecycleAndNoOverwrite(t *testing.T) {
	for _, scenario := range []string{"not_running", "revoke", "duplicate", "other_version", "stop"} {
		t.Run(scenario, func(t *testing.T) {
			f := newCapturedHistoryFixture(t)
			d := f.open(t)
			files, _, err := f.spool.Advance(t.Context(), f.req)
			if err != nil {
				t.Fatal(err)
			}
			loads := 0
			started := make(chan struct{})
			next, err := NewCapturedHistoryPreparation(d, "captured-scope", func(ctx context.Context, _ SourcePreparationRequest) (CapturedHistorySources, error) {
				loads++
				if scenario == "revoke" {
					if err := f.auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
						return CapturedHistorySources{}, err
					}
				}
				if scenario == "stop" {
					close(started)
					<-ctx.Done()
					return CapturedHistorySources{}, ctx.Err()
				}
				return f.sources, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "not_running" {
				if applied, err := next(t.Context(), f.req, files); !errors.Is(err, ErrGenerationBusy) || applied || loads != 0 {
					t.Fatal("disabled runtime did work", err)
				}
				preparationCounts(t, f.f, 0, 0)
				return
			}
			if err := d.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if scenario == "stop" {
				result := make(chan error, 1)
				go func() { _, err := next(t.Context(), f.req, files); result <- err }()
				select {
				case <-started:
				case <-time.After(2 * time.Second):
					t.Fatal("history loader did not start")
				}
				closeDeploymentTest(t, d)
				select {
				case err := <-result:
					if !errors.Is(err, context.Canceled) {
						t.Fatal("stop lost cancellation", err)
					}
				case <-time.After(time.Second):
					t.Fatal("stop did not join preparation")
				}
				preparationCounts(t, f.f, 0, 0)
				return
			}
			var oldHead string
			if scenario == "other_version" {
				prepared, err := d.g.prepareCapturedHistoryInput(t.Context(), f.req, files, f.sources, "another-scope-id")
				if err != nil {
					t.Fatal(err)
				}
				result, err := d.PrepareMonthlyInput(t.Context(), prepared.Plan, prepared.Preparation, "", true)
				if err != nil || !result.Applied {
					t.Fatal(err)
				}
				oldHead = result.SelectionHead
			}
			applied, err := next(t.Context(), f.req, files)
			switch scenario {
			case "revoke":
				if !errors.Is(err, ErrGenerationChanged) || applied {
					t.Fatal("revoked scope was selected", err)
				}
				preparationCounts(t, f.f, 0, 0)
			case "other_version":
				if !errors.Is(err, ErrGenerationSelectionConflict) || applied {
					t.Fatal("existing version overwritten", err)
				}
				selected, err := ReadGenerationSelection(t.Context(), d.receiver, f.req.Owner.GroupID, f.req.Month)
				if err != nil || selected.Head != oldHead {
					t.Fatal("selection changed", err)
				}
				preparationCounts(t, f.f, 1, 1)
			case "duplicate":
				if err != nil || !applied {
					t.Fatal(err)
				}
				first, err := ReadGenerationSelection(t.Context(), d.receiver, f.req.Owner.GroupID, f.req.Month)
				if err != nil {
					t.Fatal(err)
				}
				if applied, err := next(t.Context(), f.req, files); err != nil || !applied {
					t.Fatal(err)
				}
				again, err := ReadGenerationSelection(t.Context(), d.receiver, f.req.Owner.GroupID, f.req.Month)
				if err != nil || !reflect.DeepEqual(first, again) {
					t.Fatal("duplicate selection changed identity", err)
				}
				preparationCounts(t, f.f, 1, 1)
			}
			closeDeploymentTest(t, d)
			var bills int64
			if err := f.auth.Model(&monitor.PortalMonthlyBill{}).Count(&bills).Error; err != nil || bills != 0 {
				t.Fatal("source selection itself published a bill", bills, err)
			}
		})
	}
}

func TestCapturedHistoryCannotSkipMissingFixedOriginPredecessor(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	// Supported origin is independently fixed BEFORE the customer's request.
	// Only September sources exist: never silently change the origin to Sep.
	index := AutomaticDisplaySeriesIndex{Version: 1, Ranges: []AutomaticDisplaySeriesRange{{GroupID: f.req.Owner.GroupID, SourceIdentity: f.wallet.funding.sourceIdentity, From: "2026-08", Through: f.req.Month}}}
	if err := os.WriteFile(filepath.Join(f.cfg.EvidenceRoot, f.index), materialPreparationJSON(t, index), 0600); err != nil {
		t.Fatal(err)
	}
	d := f.open(t)
	next, err := NewCapturedHistoryPreparation(d, "captured-scope", func(context.Context, SourcePreparationRequest) (CapturedHistorySources, error) { return f.sources, nil })
	if err != nil {
		t.Fatal(err)
	}
	step, err := ComposeCapturedSourcePreparation(f.wallet, f.spool, next)
	if err != nil {
		t.Fatal(err)
	}
	authority := d.runtime.service.(*usageHTTPService).authorization
	if err := d.runtime.EnableOnDemandPreparation(t.Context(), authority, func(context.Context, SourcePreparationRequest) error { return nil }, step); err != nil {
		t.Fatal(err)
	}
	d.runtime.worker.pollMin = 5 * time.Millisecond
	d.runtime.worker.pollMax = 20 * time.Millisecond
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SubmitMonthly(t.Context(), f.req.Owner.GroupID, f.req.Owner.AuthVersion, f.req.Month); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for d.runtime.worker.Status().LastCode != "not_prepared" {
		if time.Now().After(deadline) {
			t.Fatal("missing predecessor not detected", d.runtime.worker.Status())
		}
		time.Sleep(20 * time.Millisecond)
	}
	closeDeploymentTest(t, d)
	var bills int64
	if err := f.auth.Model(&monitor.PortalMonthlyBill{}).Count(&bills).Error; err != nil || bills != 0 {
		t.Fatal("missing history was replaced by invented money", bills, err)
	}
	var leases int
	if err := f.f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_publication_jobs`).Scan(&leases); err != nil || leases != 0 {
		t.Fatal("missing predecessor consumed publication retries", leases, err)
	}
	selection, err := ReadGenerationSelection(t.Context(), f.f.receiver, f.req.Owner.GroupID, "2026-08")
	if err != nil || selection.Head != "" {
		t.Fatal("fabricated an earlier month", err)
	}
}
