package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func originalRuntimeFixture(t *testing.T, first ...string) (capturedHistoryFixture, *Deployment, CapturedPreparationConfig) {
	t.Helper()
	f, _, _ := automaticHistoryOriginalFixtureWithTail(t, func(o *monthlysource.WalletTailObservation, phase int) {
		sameSecondHistoryTail(o, phase)
		*o.User.BalanceQuota -= 1000000 // Synthetic zero origin; logged money is untouched.
	}, first...)
	return configureOriginalRuntimeFixture(t, f)
}

func configureOriginalRuntimeFixture(t *testing.T, f capturedHistoryFixture) (capturedHistoryFixture, *Deployment, CapturedPreparationConfig) {
	t.Helper()
	contextDirectoryLabels(t, f)
	d, cfg := registeredPipelineConfig(t, f)
	cfg.DeriveHistoryFromOriginals, cfg.AssembleRetainedHistory = true, false
	cfg.History, cfg.ScopeCatalogue = monthlysource.MonthlyObservationFile{}, monthlysource.MonthlyObservationFile{}
	cfg.CaptureWalletTails, cfg.PartitionUsage, cfg.RetainCompletedSeals, cfg.AccountTokenWork = true, true, true, true
	// These retained/manual inputs are intentionally unavailable. No test stages
	// a generation plan, prepares a financial scope or preselects an input.
	for _, path := range []string{f.sources.History.Path, f.sources.Scope.Path, filepath.Join(f.f.root, "shared-scope-catalogue.json")} {
		if err := os.Rename(path, path+".not-used"); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	d.runtime.worker.pollMin, d.runtime.worker.pollMax = 5*time.Millisecond, 20*time.Millisecond
	return f, d, cfg
}

func TestAutomaticHistoryRuntimeStartupClickSaveDownloadWithoutManualHistory(t *testing.T) {
	f, d, cfg := originalRuntimeFixture(t)
	ref := writeHistoryAssemblyConfig(t, f, cfg)
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	if d.capturedPreparation.source.Stats().OpenConnections != 0 || d.runtime.preparation.historySourceWork == nil || !d.MonthlyCalendarEnabled() {
		t.Fatal("startup was not lazy or omitted original-source stages")
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
	request, own, foreign := capturedHistoryHTTP(t, f, d)
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	calendar := request("GET", "/api/monthly-statement/calendar", "", own)
	first := request("GET", "/api/monthly-statement/generation?month=2026-09", "", own)
	if calendar.Code != 200 || first.Code != 200 {
		t.Fatal(calendar.Code, first.Code, first.Body.String())
	}
	method, path, body := capturedHistoryFirstButtonRequest(t, first.Body.Bytes(), calendar.Body.Bytes())
	if out := request(method, path, body, own); out.Code != 202 {
		t.Fatal("actual first customer click failed", out.Code, out.Body.String())
	}
	record := waitOriginalRuntimeBill(t, f, d)
	var evidence monitor.MonthlyBillEvidence
	if json.Unmarshal([]byte(record.Evidence), &evidence) != nil || len(evidence.Users) != 2 {
		t.Fatal("saved bill missing")
	}
	for _, u := range evidence.Users {
		if u.OpeningQuota != 0 || u.ClosingQuota != -1000000 || u.ConsumeQuota != 1500000 || u.RefundQuota != 500000 || u.TokenUsage == nil || len(u.TokenUsage.Rows) != 3 {
			t.Fatal("automatic customer money differs from original ledger", u.UserID)
		}
	}
	var taskCount int
	if err := d.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_draft_tasks`).Scan(&taskCount); err != nil || taskCount != 1 {
		t.Fatal("unexpected customer tasks", taskCount, err)
	}
	if d.capturedPreparation.funding.source.Driver().(*walletTailPreparationDriver).base.writes.Load() != 0 {
		t.Fatal("upstream was mutated")
	}
	closeDeploymentTest(t, d)
	page, book := historySavedDownload(t, request, own, foreign)
	reopenedFixture := f
	reopenedFixture.cfg.Rollout = GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}
	reopened := reopenedFixture.open(t)
	if err := reopened.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	read, session, other := capturedHistoryHTTP(t, f, reopened)
	if worked, err := reopened.runtime.worker.step(t.Context()); err != nil || worked {
		t.Fatal("restart regenerated immutable bill", worked, err)
	}
	afterPage, afterBook := historySavedDownload(t, read, session, other)
	if !bytes.Equal(page, afterPage) || !bytes.Equal(book, afterBook) || reopened.capturedPreparation.source.Stats().OpenConnections != 0 {
		t.Fatal("restart/view/download changed bill or queried upstream")
	}
	var after monitor.PortalMonthlyBill
	if err := f.auth.First(&after, "group_id=? AND month=?", record.GroupID, record.Month).Error; err != nil || after != record {
		t.Fatal("saved bill changed", err)
	}
}

func TestAutomaticHistoryRuntimeRejectsAmbiguousOrUnsafeOptInBeforeSource(t *testing.T) {
	f, d, original := originalRuntimeFixture(t)
	for _, scenario := range []string{"manual_history", "catalogue", "mixed_mode", "no_registration", "no_tail", "no_partition", "no_retention", "no_work_accounting", "wrong_policy"} {
		t.Run(scenario, func(t *testing.T) {
			cfg := original
			switch scenario {
			case "manual_history":
				cfg.History = f.sources.History
			case "catalogue":
				cfg.ScopeCatalogue = f.sources.Scope
			case "mixed_mode":
				cfg.AssembleRetainedHistory = true
			case "no_registration":
				cfg.CaptureRegisteredOrigins = false
			case "no_tail":
				cfg.CaptureWalletTails = false
			case "no_partition":
				cfg.PartitionUsage = false
			case "no_retention":
				cfg.RetainCompletedSeals = false
			case "no_work_accounting":
				cfg.AccountTokenWork = false
			case "wrong_policy":
				d.runtime.scope.registered = false
				defer func() { d.runtime.scope.registered = true }()
			}
			opened := 0
			err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", writeHistoryAssemblyConfig(t, f, cfg), func(*mysql.Config) (*sql.DB, error) {
				opened++
				return nil, errors.New("synthetic source must not open")
			})
			if err == nil || opened != 0 || d.capturedPreparation != nil || d.runtime.preparation != nil {
				t.Fatal("unsafe original-source mode installed", err, opened)
			}
		})
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistoryRuntimeLocatorIsNotSerializableOrCrossRuntime(t *testing.T) {
	f, d, cfg := originalRuntimeFixture(t)
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", writeHistoryAssemblyConfig(t, f, cfg), capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	p := d.runtime.preparation.historySourceWork
	step, err := newAutomaticHistorySelectedPreparation(d, p, "captured-scope")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := step(t.Context(), f.req); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("missing in-process raw coordinator accepted", err)
	}
	l := automaticHistoryLocator{producer: p, parent: f.req}
	if _, err := json.Marshal(l); err == nil {
		t.Fatal("locator became JSON authority")
	}
	if json.Unmarshal([]byte(`{}`), &l) == nil || l.producer != nil {
		t.Fatal("decoded locator retained executable binding")
	}
	other := *p
	ctx := context.WithValue(t.Context(), automaticHistoryLocatorKey{}, automaticHistoryLocator{producer: &other, parent: f.req})
	if _, err := step(ctx, f.req); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("locator crossed coordinator", err)
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistoryRuntimeLegacyConfigBytesUnchanged(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	cfg, _ := capturedPreparationConfigFixture(t, f)
	raw := materialPreparationJSON(t, cfg)
	if strings.Contains(string(raw), "derive_history_from_originals") {
		t.Fatal("legacy opt-out format changed")
	}
	var decoded CapturedPreparationConfig
	if json.Unmarshal(raw, &decoded) != nil || !reflect.DeepEqual(cfg, decoded) {
		t.Fatal("legacy config changed")
	}
}

func TestAutomaticHistoryRuntimeMissingRawPrefixUsesSameParentAndOnlyRequestedBill(t *testing.T) {
	f, d, cfg := originalRuntimeFixture(t, "2026-08")
	older := SourcePreparationRequest{Owner: f.req.Owner, Month: "2026-08"}
	seedHistoryEmptyMonth(t, f, older, true)
	state := f.wallet.funding.source.Driver().(*walletTailPreparationDriver).base
	// Only a protocol transport double. The real collectors must finish the
	// missing funding/token/wallet captures through their bounded RO queries.
	source := &historyPreparationDriver{base: state}
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", writeHistoryAssemblyConfig(t, f, cfg), tailPreparationOpener(source)); err != nil {
		t.Fatal(err)
	}
	d.receiver.SetMaxOpenConns(1)
	state.onQuery = func() error {
		// An unrelated worker may briefly inspect the same SQLite pool. With
		// one connection, successful bounded acquisition proves THIS source
		// operation has not held it across the read; a raw Stats().InUse check
		// would incorrectly fail when another goroutine happens to be reading.
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		for _, db := range []*sql.DB{d.receiver, d.capturedPreparation.capture} {
			if err := db.PingContext(ctx); err != nil {
				return errors.New("SQLite connection held across source read")
			}
		}
		return nil
	}
	originalStep := d.runtime.preparation.step
	d.runtime.preparation.step = func(ctx context.Context, req SourcePreparationRequest) (bool, error) {
		ready, err := originalStep(ctx, req)
		if err != nil {
			t.Log("original runtime preparation:", err)
		}
		return ready, err
	}
	request, own, foreign := capturedHistoryHTTP(t, f, d)
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, month := range []string{"2026-07", time.Now().In(time.FixedZone("CST", 28800)).Format("2006-01")} {
		if out := request("POST", "/api/monthly-statement/generation", `{"month":"`+month+`"}`, own); out.Code < 400 {
			t.Fatal("invalid month admitted", month)
		}
	}
	before := state.pings.Load()
	if out := request("POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`, own); out.Code != 202 {
		t.Fatal(out.Code, out.Body.String())
	}
	record := waitOriginalRuntimeBill(t, f, d)
	if state.pings.Load() <= before || state.writes.Load() != 0 {
		t.Fatal("missing raw data was not collected read-only")
	}
	for _, month := range []string{"2026-08", "2026-09"} {
		selected, err := ReadGenerationSelection(t.Context(), d.receiver, f.req.Owner.GroupID, month)
		if err != nil || selected.Request == nil || selected.Request.Owner != f.req.Owner {
			t.Fatal("missing financial prefix input", month, err)
		}
	}
	var tasks int
	var bills int64
	if err := d.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_draft_tasks`).Scan(&tasks); err != nil || tasks != 1 {
		t.Fatal("source dependency became customer task", tasks, err)
	}
	if err := f.auth.Model(&monitor.PortalMonthlyBill{}).Count(&bills).Error; err != nil || bills != 1 {
		t.Fatal("source dependency was published", bills, err)
	}
	closeDeploymentTest(t, d)
	historySavedDownload(t, request, own, foreign)
	if record.Month != "2026-09" {
		t.Fatal("wrong requested bill")
	}
}

func waitOriginalRuntimeBill(t *testing.T, f capturedHistoryFixture, d *Deployment) monitor.PortalMonthlyBill {
	t.Helper()
	// End-to-end test deadline only. Production source/SQL/dispatch deadlines,
	// read gaps, work accounting and retry limits are deliberately unchanged.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var record monitor.PortalMonthlyBill
		if f.auth.First(&record, "group_id=? AND month=?", f.req.Owner.GroupID, f.req.Month).Error == nil {
			return record
		}
		time.Sleep(20 * time.Millisecond)
	}
	state, steps, failures := preparationState(t, f.f.receiver)
	t.Fatal("original-source generation incomplete", state, steps, failures, d.runtime.worker.Status())
	return monitor.PortalMonthlyBill{}
}

func TestAutomaticHistoryRuntimeWithdrawnPrefixStopsBeforeRawRead(t *testing.T) {
	f, d, cfg := originalRuntimeFixture(t)
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", writeHistoryAssemblyConfig(t, f, cfg), capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	// A withdrawn control descriptor is deliberately NOT financial evidence.
	// No retained input is needed: this path must stop before trying to load it.
	prior := generationRequest(f.f.plan, materialPreparationJSON(t, f.f.plan))
	prior.Owner, prior.Month, prior.SourceIdentity = f.req.Owner, f.req.Month, f.wallet.funding.sourceIdentity
	selected, err := changeGenerationSelection(t.Context(), d.receiver, f.req.Owner.GroupID, f.req.Month, &prior, "")
	if err != nil {
		t.Fatal(err)
	}
	withdrawn, err := WithdrawGenerationSelection(t.Context(), d.receiver, f.req.Owner.GroupID, f.req.Month, selected.Head)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.runtime.preparation.dependencies.plan(t.Context(), f.req); err != nil {
		t.Fatal(err)
	}
	state := f.wallet.funding.source.Driver().(*walletTailPreparationDriver).base
	before := state.pings.Load()
	if _, err := d.runtime.preparation.step(t.Context(), f.req); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal("withdrawn selection reactivated", err)
	}
	after, err := ReadGenerationSelection(t.Context(), d.receiver, f.req.Owner.GroupID, f.req.Month)
	if err != nil || after.Head != withdrawn.Head || after.Request != nil || state.pings.Load() != before {
		t.Fatal("withdrawal triggered raw reads or new selection", err)
	}
}
