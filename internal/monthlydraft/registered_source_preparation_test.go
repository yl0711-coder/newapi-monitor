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
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func registeredPipelineConfig(t *testing.T, f capturedHistoryFixture) (*Deployment, CapturedPreparationConfig) {
	t.Helper()
	f.cfg.Rollout = GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}
	if err := os.WriteFile(filepath.Join(f.cfg.EvidenceRoot, f.index), materialPreparationJSON(t, AutomaticDisplaySeriesIndex{Version: 1, Ranges: []AutomaticDisplaySeriesRange{}}), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, _ := capturedPreparationConfigFixture(t, f)
	cfg.AssembleRetainedHistory, cfg.CaptureRegisteredOrigins = true, true
	cfg.ScopeCatalogue = monthlysource.MonthlyObservationFile{}
	return f.open(t), cfg
}

func TestRegisteredPipelineStartupClickCaptureSealBillDownloadAndRestart(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	if err := monthlysource.InitRegisteredWalletMonthCaptureStore(t.Context(), f.wallet.funding.capture); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(f.sources.History.Path)
	if err != nil {
		t.Fatal(err)
	}
	d, cfg := registeredPipelineConfig(t, f)
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", writeHistoryAssemblyConfig(t, f, cfg), capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	if d.capturedPreparation.source.Stats().OpenConnections != 0 {
		t.Fatal("startup contacted source")
	}
	d.runtime.worker.pollMin, d.runtime.worker.pollMax = 5*time.Millisecond, 20*time.Millisecond
	request, own, foreign := capturedHistoryHTTP(t, f, d)
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	calendar := request("GET", "/api/monthly-statement/calendar", "", own)
	first := request("GET", "/api/monthly-statement/generation?month=2026-09", "", own)
	if calendar.Code != 200 || first.Code != 200 {
		t.Fatal(calendar.Code, first.Code)
	}
	method, path, body := capturedHistoryFirstButtonRequest(t, first.Body.Bytes(), calendar.Body.Bytes())
	if out := request(method, path, body, own); out.Code != 202 {
		t.Fatal(out.Code, out.Body.String())
	}
	record := waitHistoryAssemblyBill(t, f, d)
	assertHistoryAssemblyMoney(t, record) // original historical money, NOT the new current-zero wallet
	scope, err := d.capturedPreparation.funding.scope(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	progress, err := monthlysource.InspectRegisteredWalletMonthCapture(t.Context(), d.capturedPreparation.capture, f.req.Key(), scope)
	if err != nil || progress.Completed != 1 || progress.ID == "" {
		t.Fatal("actual worker omitted original/checkpoint", progress, err)
	}
	files, completed, err := d.capturedPreparation.spool.Advance(t.Context(), f.req)
	if err != nil || completed {
		t.Fatal("completed seal was not reused", err)
	}
	var material monthlysource.MonthlyObservationMaterial
	raw, err := os.ReadFile(files.Material.Path)
	if err != nil || json.Unmarshal(raw, &material) != nil {
		t.Fatal("sealed material missing", err)
	}
	selection, err := os.ReadFile(files.Selection.Path)
	if err != nil {
		t.Fatal(err)
	}
	ro, err := sql.Open("sqlite", "file:"+material.Database.Path+"?mode=ro&immutable=1&_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	id, captured, err := monthlysource.ReadSelectedRegisteredWallet(t.Context(), ro, selection)
	if closeErr := ro.Close(); err != nil || closeErr != nil || id != progress.ID || len(captured.Users) != 2 || *captured.Users[0].BalanceQuota != 0 {
		t.Fatal("sealed current observation changed or was omitted", err, closeErr)
	}
	closeDeploymentTest(t, d)
	page, book := assertCapturedHistoryCustomerDownload(t, request, own, foreign)
	reopened := f
	reopened.cfg.Rollout = GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}
	d2 := reopened.open(t)
	if err := d2.enableCapturedSourcePreparation(t.Context(), "captured-scope", writeHistoryAssemblyConfig(t, f, cfg), capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	read, session, other := capturedHistoryHTTP(t, f, d2)
	if err := d2.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	closeDeploymentTest(t, d2)
	after, afterBook := assertCapturedHistoryCustomerDownload(t, read, session, other)
	if !bytes.Equal(page, after) || !bytes.Equal(book, afterBook) {
		t.Fatal("restart changed saved customer money/download")
	}
	if current, err := os.ReadFile(f.sources.History.Path); err != nil || !bytes.Equal(current, original) {
		t.Fatal("original financial history changed", err)
	}
}

func TestRegisteredPipelineOptInMissingSchemaAndStartupRecovery(t *testing.T) {
	for _, fault := range []string{"schema", "requires_assembly"} {
		t.Run(fault, func(t *testing.T) {
			f := newCapturedHistoryFixture(t)
			d, cfg := registeredPipelineConfig(t, f)
			if fault == "requires_assembly" {
				cfg.AssembleRetainedHistory = false
			}
			if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", writeHistoryAssemblyConfig(t, f, cfg), capturedPreparationTestOpener(f)); err == nil {
				t.Fatal("unprepared/unapproved origin mode installed")
			}
			var n int
			if err := f.wallet.funding.capture.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='portal_monthly_registered_wallet_observations'`).Scan(&n); err != nil || n != 0 {
				t.Fatal("startup implicitly initialized source schema", err)
			}
			cfg.AssembleRetainedHistory, cfg.CaptureRegisteredOrigins = true, false
			if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", writeHistoryAssemblyConfig(t, f, cfg), capturedPreparationTestOpener(f)); err != nil {
				t.Fatal("failed opt-in leaked resources or damaged legacy installation", err)
			}
			if d.capturedPreparation.funding.registeredOrigins || d.capturedPreparation.source.Stats().OpenConnections != 0 {
				t.Fatal("disabled mode captured or enabled new source path")
			}
		})
	}
}

func TestRegisteredPipelineSeparateSealsAndLegacyReadCompatibility(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	legacyName := f.spool.jobName(f.req)
	legacy, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(legacy.Material.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := monthlysource.InitRegisteredWalletMonthCaptureStore(t.Context(), f.wallet.funding.capture); err != nil {
		t.Fatal(err)
	}
	f.wallet.funding.registeredOrigins = true
	calls := 0
	stage, err := registeredSourceBefore(f.wallet.funding, func(context.Context, SourcePreparationRequest) (bool, error) { calls++; return false, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stage(t.Context(), f.req); err != nil || calls != 0 {
		t.Fatal("capture did another stage in same step", err)
	}
	if f.spool.jobName(f.req) == legacyName {
		t.Fatal("new format can overwrite legacy seal path")
	}
	richer, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil || richer.Material.Path == legacy.Material.Path {
		t.Fatal("new seal overwrote legacy path", err)
	}
	after, err := os.ReadFile(legacy.Material.Path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("new format edited old seal", err)
	}
	f.wallet.funding.registeredOrigins = false
	read, err := f.wallet.SealCapturedSources(t.Context(), f.req, filepath.Dir(richer.Material.Path))
	if err != nil || !reflect.DeepEqual(read, richer) {
		t.Fatal("legacy reader changed or rejected richer sealed core", err)
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestRegisteredPipelineRevocationBeforeOrDuringSourceStopsNextStage(t *testing.T) {
	for _, when := range []string{"before", "during"} {
		t.Run(when, func(t *testing.T) {
			f := newCapturedHistoryFixture(t)
			if err := monthlysource.InitRegisteredWalletMonthCaptureStore(t.Context(), f.wallet.funding.capture); err != nil {
				t.Fatal(err)
			}
			f.wallet.funding.registeredOrigins = true
			state := f.wallet.funding.source.Driver().(*walletPreparationDriver).base
			calls := 0
			stage, err := registeredSourceBefore(f.wallet.funding, func(context.Context, SourcePreparationRequest) (bool, error) { calls++; return true, nil })
			if err != nil {
				t.Fatal(err)
			}
			revoke := func() error {
				return f.auth.Model(&monitor.CustomerGroup{}).Where("id=?", f.req.Owner.GroupID).Update("portal_auth_ver", 2).Error
			}
			if when == "before" {
				if err := revoke(); err != nil {
					t.Fatal(err)
				}
			} else {
				state.onQuery = revoke
			}
			if ready, err := stage(t.Context(), f.req); err == nil || ready || calls != 0 {
				t.Fatal("revoked owner proceeded to next stage")
			}
			if when == "before" && state.queries.Load() != 0 {
				t.Fatal("revoked owner contacted source")
			}
			if _, err := f.wallet.AssembleCapturedSources(t.Context(), f.req); !errors.Is(err, ErrGenerationChanged) {
				t.Fatal("revoked owner could seal data", err)
			}
			generationAdmissionCounts(t, f.f.receiver, 0, 0)
		})
	}
}
