package monthlydraft

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// This fixture tests ONLY range selection plus the existing financial chain.
// Its independently prepared original monthly inputs remain explicit; it does
// not stand in for the next step, automatic history material construction.
func dynamicSeriesFixture(t *testing.T) registeredPrefixFixture {
	t.Helper()
	f := newRegisteredPrefixFixture(t)
	f.cfg.Rollout = GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}
	writeDynamicSeriesIndex(t, f, []AutomaticDisplaySeriesRange{})
	return f
}

func writeDynamicSeriesIndex(t *testing.T, f registeredPrefixFixture, ranges []AutomaticDisplaySeriesRange) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(f.cfg.EvidenceRoot, f.index), materialPreparationJSON(t, AutomaticDisplaySeriesIndex{Version: 1, Ranges: ranges}), 0600); err != nil {
		t.Fatal(err)
	}
}

func dynamicSeriesClock(t *testing.T, d *Deployment, now time.Time) *atomic.Int64 {
	t.Helper()
	if d.runtime.Phase() != "disabled" {
		t.Fatal("cannot change an active test clock")
	}
	clock := &atomic.Int64{}
	clock.Store(now.UnixNano())
	read := func() time.Time { return time.Unix(0, clock.Load()) }
	d.runtime.scope.now = read
	d.runtime.service.(*registrationCalendarService).now = read
	// Keep queue leases/backoff on a progressing wall clock. Freezing them
	// would stop a resumable preparation forever after its first step.
	return clock
}

func TestRegisteredDynamicRangeEmptyIndexAndNewMonthWithoutRestart(t *testing.T) {
	f := dynamicSeriesFixture(t)
	d, _, _ := f.open(t, nil)
	clock := dynamicSeriesClock(t, d, time.Date(2026, 9, 30, 15, 59, 59, 0, time.UTC))
	p := d.runtime.preparation.dependencies
	owner, _, err := LoadUsageGenerationOwner(t.Context(), f.f.auth, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !d.runtime.worker.dynamicSeries || len(d.runtime.worker.seriesScopes) != 0 || d.runtime.worker.seriesIdentity != f.f.ranges[0].SourceIdentity {
		t.Fatal("dynamic worker still requires a per-group range")
	}
	if _, err := p.plan(t.Context(), SourcePreparationRequest{Owner: owner, Month: "2026-09"}); err == nil {
		t.Fatal("current month became a financial dependency")
	}
	scope, err := p.plan(t.Context(), SourcePreparationRequest{Owner: owner, Month: "2026-08"})
	if err != nil || scope.From != "2026-08" || scope.Through != "2026-08" {
		t.Fatal("unrequested later month was included", scope, err)
	}
	clock.Add(int64(time.Second))
	scope, err = p.plan(t.Context(), SourcePreparationRequest{Owner: owner, Month: "2026-09"})
	if err != nil || scope.From != "2026-08" || scope.Through != "2026-09" || scope.SourceIdentity != f.f.ranges[0].SourceIdentity {
		t.Fatal("new month required new range config", scope, err)
	}
	for _, month := range []string{"2026-07", "2026-10", "2027-01", "2026-9", ""} {
		if _, err := p.plan(t.Context(), SourcePreparationRequest{Owner: owner, Month: month}); err == nil {
			t.Fatal("invalid month passed plan", month)
		}
	}
	generationAdmissionCounts(t, d.receiver, 0, 0)
}

func TestRegisteredDynamicRangeNewOrganizationUsesOwnOwnerAndServerIdentity(t *testing.T) {
	f := dynamicSeriesFixture(t)
	d, _, _ := f.open(t, nil)
	dynamicSeriesClock(t, d, time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC))
	if err := f.f.auth.Create(&monitor.CustomerGroup{ID: 81733, Name: "synthetic new organization", PortalEmail: "new-range@example.invalid", PortalAuthVer: 9}).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.f.auth.Create(&monitor.TrackedUser{UserID: 871601, GroupID: 81733}).Error; err != nil {
		t.Fatal(err)
	}
	owner, _, err := LoadUsageGenerationOwner(t.Context(), f.f.auth, 81733, 9)
	if err != nil {
		t.Fatal(err)
	}
	p := d.runtime.preparation.dependencies
	scope, missing, err := p.missing(t.Context(), SourcePreparationRequest{Owner: owner, Month: "2026-09"})
	if err != nil || missing != "2026-08" || scope.GroupID != owner.GroupID || scope.From != "2026-08" || scope.Through != "2026-09" || scope.SourceIdentity != f.f.ranges[0].SourceIdentity {
		t.Fatal("new organization did not get its own bounded dependency range", scope, missing, err)
	}
	request := monthlytasks.Request{Owner: owner, Month: "2026-09", SourceIdentity: generationHash([]byte("forged-source"))}
	if _, err := d.runtime.worker.automaticSeriesScope(t.Context(), request); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal("customer-selected source accepted", err)
	}
	request.SourceIdentity = f.f.ranges[0].SourceIdentity
	request.Owner.MemberFingerprint = generationHash([]byte("another-roster"))
	if _, err := d.runtime.worker.automaticSeriesScope(t.Context(), request); err == nil {
		t.Fatal("wrong owner accepted")
	}
	if err := f.f.auth.Model(&monitor.CustomerGroup{}).Where("id=?", owner.GroupID).Update("portal_auth_ver", 10).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := p.plan(t.Context(), SourcePreparationRequest{Owner: owner, Month: "2026-09"}); err == nil {
		t.Fatal("revoked owner kept its cached range")
	}
	generationAdmissionCounts(t, d.receiver, 0, 0)
}

func TestRegisteredDynamicRangeCustomerGenerationRetainsMoneyAndRestartDownload(t *testing.T) {
	f := dynamicSeriesFixture(t)
	d, calls, mu := f.open(t, nil)
	dynamicSeriesClock(t, d, time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC))
	request, own, _ := capturedHistoryHTTP(t, capturedHistoryFixture{cfg: f.cfg, auth: f.f.auth}, d)
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	calendar := request("GET", "/api/monthly-statement/calendar", "", own)
	first := request("GET", "/api/monthly-statement/generation?month=2026-09", "", own)
	if calendar.Code != 200 || first.Code != 200 {
		t.Fatal("first state unavailable", calendar.Code, first.Code)
	}
	method, path, body := capturedHistoryFirstButtonRequest(t, first.Body.Bytes(), calendar.Body.Bytes())
	if out := request(method, path, body, own); out.Code != 202 {
		t.Fatal("first customer click failed", out.Code, out.Body.String())
	}
	record := waitRegisteredPrefixBill(t, f, d)
	stopRuntime(t, d.runtime)
	assertRegisteredPrefixOnlyRequestedPublished(t, f, d, record)
	mu.Lock()
	got := append([]string(nil), (*calls)...)
	mu.Unlock()
	if !reflect.DeepEqual(got, []string{"2026-08", "2026-09"}) {
		t.Fatal("wrong prefix work", got)
	}
	page := request("GET", "/api/monthly-statement?month=2026-09", "", own)
	if page.Code != 200 {
		t.Fatal("saved page unavailable", page.Code)
	}
	var view struct {
		Data struct {
			Publication string `json:"publication"`
		} `json:"data"`
	}
	if json.Unmarshal(page.Body.Bytes(), &view) != nil || view.Data.Publication == "" {
		t.Fatal("missing saved publication")
	}
	url := "/api/monthly-statement/download?month=2026-09&publication=" + view.Data.Publication
	book := request("GET", url, "", own)
	if book.Code != 200 || len(book.Body.Bytes()) < 100 {
		t.Fatal("download unavailable", book.Code)
	}
	closeDeploymentTest(t, d)
	reopened, resumed, resumedMu := f.open(t, nil)
	dynamicSeriesClock(t, reopened, time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC))
	read, session, _ := capturedHistoryHTTP(t, capturedHistoryFixture{cfg: f.cfg, auth: f.f.auth}, reopened)
	if err := reopened.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.SubmitMonthly(t.Context(), 1, 1, "2026-09"); err != nil {
		t.Fatal(err)
	}
	closeDeploymentTest(t, reopened)
	if after := read("GET", "/api/monthly-statement?month=2026-09", "", session); after.Code != 200 || after.Body.String() != page.Body.String() {
		t.Fatal("restart changed customer bill", after.Code)
	}
	if after := read("GET", url, "", session); after.Code != 200 {
		t.Fatal("restart download unavailable", after.Code)
	}
	resumedMu.Lock()
	n := len(*resumed)
	resumedMu.Unlock()
	if n != 0 {
		t.Fatal("restart recaptured committed original sources")
	}
}

func TestRegisteredDynamicRangeIgnoresStaleEndButLegacyKeepsExactBoundary(t *testing.T) {
	f := dynamicSeriesFixture(t)
	stale := f.f.ranges[0]
	stale.From, stale.Through = "2026-05", "2026-08"
	writeDynamicSeriesIndex(t, f, []AutomaticDisplaySeriesRange{stale})
	d, _, _ := f.open(t, nil)
	dynamicSeriesClock(t, d, time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC))
	owner, _, err := LoadUsageGenerationOwner(t.Context(), f.f.auth, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := d.runtime.preparation.dependencies.plan(t.Context(), SourcePreparationRequest{Owner: owner, Month: "2026-09"})
	if err != nil || scope.From != "2026-08" || scope.Through != "2026-09" {
		t.Fatal("stale file changed registered bounds", scope, err)
	}
	closeDeploymentTest(t, d)
	f.cfg.Rollout = GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: "2026-09"}}}
	if old, err := OpenRetainedAutomaticDisplaySeriesDeployment(t.Context(), f.cfg, "scope", f.index, displaySeriesPublisher(f.f.auth)); err == nil || old != nil {
		t.Fatal("legacy exact range silently broadened", err)
	}
}

func TestRegisteredDynamicRangeStillRequiresSourceAndCalendarComposition(t *testing.T) {
	f := dynamicSeriesFixture(t)
	d, _, _ := f.open(t, nil)
	dynamicSeriesClock(t, d, time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC))
	w := d.runtime.worker
	identity, resolver := w.seriesIdentity, w.seriesResolve
	for _, scenario := range []string{"missing_source", "missing_resolver"} {
		t.Run(scenario, func(t *testing.T) {
			if scenario == "missing_source" {
				w.seriesIdentity = ""
			} else {
				w.seriesResolve = nil
			}
			if err := d.Start(t.Context()); !errors.Is(err, ErrGenerationInput) || d.runtime.Phase() != "disabled" {
				t.Fatal("incomplete dynamic worker started", err)
			}
			if _, err := w.automaticSeriesIdentity(1); err == nil {
				t.Fatal("incomplete identity lookup succeeded")
			}
			w.seriesIdentity, w.seriesResolve = identity, resolver
		})
	}
	if _, err := NewSelectedAutomaticDisplaySeries(d.g, nil); err == nil {
		t.Fatal("legacy preparer accepted empty ranges")
	}
	if _, err := NewAutomaticDisplaySeriesLocalWorker(d.g, f.f.metadata, nil, displaySeriesPublisher(f.f.auth)); err == nil {
		t.Fatal("legacy worker accepted empty ranges")
	}
}

func TestRegisteredDynamicRangeActualCaptureInstallationWithEmptyIndexIsLazy(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	f.cfg.Rollout = GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}
	if err := os.WriteFile(filepath.Join(f.cfg.EvidenceRoot, f.index), materialPreparationJSON(t, AutomaticDisplaySeriesIndex{Version: 1, Ranges: []AutomaticDisplaySeriesRange{}}), 0600); err != nil {
		t.Fatal(err)
	}
	d := f.open(t)
	d.runtime.scope.now = func() time.Time { return time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC) }
	_, config := capturedPreparationConfigFixture(t, f)
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", config, capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	if d.capturedPreparation.source.Stats().OpenConnections != 0 || d.runtime.worker.seriesIdentity != f.wallet.funding.sourceIdentity {
		t.Fatal("installation queried source or borrowed the wrong identity")
	}
	p := d.runtime.preparation.dependencies
	scope, err := p.plan(t.Context(), f.req)
	if err != nil || scope.From != "2026-09" || scope.Through != "2026-09" {
		t.Fatal(scope, err)
	}
	preparationCounts(t, f.f, 0, 0)
	closeDeploymentTest(t, d)
}

func TestRegisteredDynamicRangeRegistrationFailureIsNotAZeroOrigin(t *testing.T) {
	f := dynamicSeriesFixture(t)
	d, _, _ := f.open(t, nil)
	dynamicSeriesClock(t, d, time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC))
	calendar := d.runtime.service.(*registrationCalendarService)
	read := calendar.read
	calendar.read = func(context.Context, []int64) ([]monthlysource.RegistrationTime, error) {
		return nil, errors.New("synthetic metadata unavailable")
	}
	owner, _, err := LoadUsageGenerationOwner(t.Context(), f.f.auth, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	req := SourcePreparationRequest{Owner: owner, Month: "2026-09"}
	if _, err := d.runtime.preparation.dependencies.plan(t.Context(), req); err == nil {
		t.Fatal("failed source became a guessed origin")
	}
	if len(calendar.cache) != 0 {
		t.Fatal("failed metadata cached")
	}
	calendar.read = read
	if scope, err := d.runtime.preparation.dependencies.plan(t.Context(), req); err != nil || scope.From != "2026-08" {
		t.Fatal("metadata recovery failed", scope, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := d.runtime.preparation.dependencies.plan(ctx, req); err == nil {
		t.Fatal("cancelled range accepted")
	}
	generationAdmissionCounts(t, d.receiver, 0, 0)
}

func TestRegisteredDynamicRangeLaterPublicationStillConstrainsEarlierBill(t *testing.T) {
	f := dynamicSeriesFixture(t)
	d, _, _ := f.open(t, nil)
	clock := dynamicSeriesClock(t, d, time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC))
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SubmitMonthly(t.Context(), 1, 1, "2026-09"); err != nil {
		t.Fatal(err)
	}
	later := waitRegisteredPrefixBill(t, f, d)
	owner, _, err := LoadUsageGenerationOwner(t.Context(), f.f.auth, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Calendar eligibility can advance, but neither the new unrequested month
	// nor an earlier click may remove the already published later constraint.
	clock.Store(time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC).UnixNano())
	scope, err := d.runtime.preparation.dependencies.plan(t.Context(), SourcePreparationRequest{Owner: owner, Month: "2026-08"})
	if err != nil || scope.From != "2026-08" || scope.Through != "2026-09" {
		t.Fatal("published later month no longer constrains earlier request", scope, err)
	}
	if _, err := d.SubmitMonthly(t.Context(), 1, 1, "2026-08"); err != nil {
		t.Fatal(err)
	}
	earlier := waitRegisteredPrefixBillMonth(t, f, d, "2026-08")
	stopRuntime(t, d.runtime)
	var after monitor.PortalMonthlyBill
	if err := f.f.auth.First(&after, "group_id=1 AND month=?", later.Month).Error; err != nil || after != later {
		t.Fatal("earlier request changed a saved later bill", err)
	}
	var a, b struct {
		Display struct {
			Organization struct{ Opening, Closing string } `json:"organization"`
		} `json:"display_balances"`
	}
	if json.Unmarshal([]byte(earlier.Payload), &a) != nil || json.Unmarshal([]byte(later.Payload), &b) != nil || a.Display.Organization.Closing != b.Display.Organization.Opening {
		t.Fatal("adjacent displayed balances do not join")
	}
	if s, err := ReadGenerationSelection(t.Context(), d.receiver, 1, "2026-10"); err != nil || s.Head != "" {
		t.Fatal("calendar rollover collected an unrequested month", err)
	}
}

func TestRegisteredDynamicRangeFingerprintRefencesCurrentOriginAndRollover(t *testing.T) {
	f := dynamicSeriesFixture(t)
	d, _, _ := f.open(t, nil)
	clock := dynamicSeriesClock(t, d, time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC))
	for i, plan := range f.f.plans {
		if _, err := StageGenerationVersion(t.Context(), d.receiver, plan, f.f.inputs[i], ""); err != nil {
			t.Fatal(err)
		}
	}
	selection, err := ReadGenerationSelection(t.Context(), d.receiver, 1, "2026-09")
	if err != nil || selection.Request == nil {
		t.Fatal(err)
	}
	w := d.runtime.worker
	fingerprint, err := w.automaticSeriesFingerprint(t.Context(), *selection.Request)
	if err != nil || fingerprint == "" {
		t.Fatal(err)
	}
	clock.Store(time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC).UnixNano())
	if after, err := w.automaticSeriesFingerprint(t.Context(), *selection.Request); err != nil || after != fingerprint {
		t.Fatal("unrequested new month changed required prefix fingerprint", err)
	}
	calendar := d.runtime.service.(*registrationCalendarService)
	calendar.mu.Lock()
	calendar.cache = map[monthlytasks.Owner]registrationCalendarEntry{}
	calendar.mu.Unlock()
	calendar.read = func(_ context.Context, ids []int64) ([]monthlysource.RegistrationTime, error) {
		var rows []monthlysource.RegistrationTime
		for _, id := range ids {
			rows = append(rows, monthlysource.RegistrationTime{UserID: id, At: time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC).Unix()})
		}
		return rows, nil
	}
	if _, err := w.automaticSeriesFingerprint(t.Context(), *selection.Request); !errors.Is(err, ErrGenerationSelectionMissing) {
		t.Fatal("changed origin reused the old range fingerprint", err)
	}
	displaySeriesCount(t, f.f, 0)
}
