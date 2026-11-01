package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func assemblyHistoryUpdate(t *testing.T, f *capturedHistoryFixture, edit func(*monitor.MonthlyPreparationInput)) {
	t.Helper()
	raw, err := os.ReadFile(f.sources.History.Path)
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil {
		t.Fatal(err)
	}
	edit(&input)
	raw = materialPreparationJSON(t, input)
	if err := os.WriteFile(f.sources.History.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	f.sources.History.SHA256 = generationHash(raw)
}

func openHistoryAssembly(t *testing.T, f capturedHistoryFixture, withCatalogue bool, edit func(*CapturedPreparationConfig)) (*Deployment, CapturedPreparationConfig) {
	t.Helper()
	f.cfg.Rollout = GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}
	if err := os.WriteFile(filepath.Join(f.cfg.EvidenceRoot, f.index), materialPreparationJSON(t, AutomaticDisplaySeriesIndex{Version: 1, Ranges: []AutomaticDisplaySeriesRange{}}), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, _ := capturedPreparationConfigFixture(t, f)
	cfg.AssembleRetainedHistory = true
	if !withCatalogue {
		cfg.ScopeCatalogue = monthlysource.MonthlyObservationFile{}
	}
	if edit != nil {
		edit(&cfg)
	}
	ref := writeHistoryAssemblyConfig(t, f, cfg)
	d := f.open(t)
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	if d.capturedPreparation.source.Stats().OpenConnections != 0 {
		t.Fatal("startup contacted a source")
	}
	d.runtime.worker.pollMin, d.runtime.worker.pollMax = 5*time.Millisecond, 20*time.Millisecond
	return d, cfg
}

func writeHistoryAssemblyConfig(t *testing.T, f capturedHistoryFixture, cfg CapturedPreparationConfig) monthlysource.MonthlyObservationFile {
	t.Helper()
	raw := materialPreparationJSON(t, cfg)
	path := filepath.Join(f.cfg.EvidenceRoot, "history-assembly-config.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(raw)}
}

func waitHistoryAssemblyBill(t *testing.T, f capturedHistoryFixture, d *Deployment) monitor.PortalMonthlyBill {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var record monitor.PortalMonthlyBill
		if err := f.auth.First(&record, "group_id=? AND month=?", f.req.Owner.GroupID, f.req.Month).Error; err == nil {
			return record
		}
		time.Sleep(20 * time.Millisecond)
	}
	state, steps, failures := preparationState(t, f.f.receiver)
	t.Fatal("assembly did not publish requested bill", state, steps, failures, d.runtime.worker.Status())
	return monitor.PortalMonthlyBill{}
}

func assertHistoryAssemblyMoney(t *testing.T, record monitor.PortalMonthlyBill) {
	t.Helper()
	var evidence monitor.MonthlyBillEvidence
	if json.Unmarshal([]byte(record.Evidence), &evidence) != nil || len(evidence.Users) != 2 {
		t.Fatal("invalid saved evidence")
	}
	for _, user := range evidence.Users {
		if user.OpeningQuota != 500000000 || user.ClosingQuota != 499000000 || user.ConsumeQuota != 1500000 || user.RefundQuota != 500000 || user.TokenUsage == nil || len(user.TokenUsage.Rows) != 3 {
			t.Fatal("money or token attribution changed", user.UserID)
		}
	}
}

func TestRetainedHistoryAssemblyNoMonthlyScopeAndUnlistedMonthSavesDownloadsRestarts(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	assemblyHistoryUpdate(t, &f, func(input *monitor.MonthlyPreparationInput) {
		for i := range input.Accounts {
			input.Accounts[i].Months = []string{"2026-08"}
		}
	})
	original, err := os.ReadFile(f.sources.History.Path)
	if err != nil {
		t.Fatal(err)
	}
	d, cfg := openHistoryAssembly(t, f, false, nil)
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
	assertHistoryAssemblyMoney(t, record)
	generationAdmissionCounts(t, d.receiver, 1, 1)
	closeDeploymentTest(t, d)
	page, book := assertCapturedHistoryCustomerDownload(t, request, own, foreign)
	if current, err := os.ReadFile(f.sources.History.Path); err != nil || !bytes.Equal(current, original) {
		t.Fatal("original history changed", err)
	}
	// Restart uses the same captured originals/config, not an operator-created
	// month package, and leaves the already saved customer version unchanged.
	reopened := f
	reopened.cfg.Rollout = GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}
	d2 := reopened.open(t)
	ref := writeHistoryAssemblyConfig(t, f, cfg)
	if err := d2.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	read, session, other := capturedHistoryHTTP(t, f, d2)
	if err := d2.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := d2.SubmitMonthly(t.Context(), f.req.Owner.GroupID, f.req.Owner.AuthVersion, f.req.Month); err != nil {
		t.Fatal(err)
	}
	closeDeploymentTest(t, d2)
	after, afterBook := assertCapturedHistoryCustomerDownload(t, read, session, other)
	if !bytes.Equal(page, after) || !bytes.Equal(book, afterBook) {
		t.Fatal("restart changed published page or workbook")
	}
}

func TestRetainedHistoryAssemblyBindingsAndNormalizationKeepOriginalMoney(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d, _ := openHistoryAssembly(t, f, false, nil)
	files, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	document, proof, err := d.assembleRetainedHistoryScope(t.Context(), f.spool, f.req, files, f.sources.History)
	if err != nil || proof == nil {
		t.Fatal(err)
	}
	body := materialPreparationJSON(t, document)
	if !proof.matches(f.req.Owner, f.req.Month, document.SourceIdentity, f.sources.History, files.Material, files.Selection, body) {
		t.Fatal("producer binding does not match its exact bytes")
	}
	ref, err := f.spool.retainHistoryScope(t.Context(), f.req, files, body)
	if err != nil {
		t.Fatal(err)
	}
	sources := CapturedHistorySources{History: f.sources.History, Scope: ref, assembly: proof}
	prepared, err := d.g.prepareCapturedHistoryInput(t.Context(), f.req, files, sources, "captured-scope")
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(prepared.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range input.Accounts {
		if a.Baseline.BalanceQuota != 500000000 || a.Snapshots[0].BalanceQuota != 499000000 {
			t.Fatal("derived a historical balance", a.UserID)
		}
	}
	for _, scenario := range []string{"owner", "month", "identity", "history", "material", "selection", "scope"} {
		t.Run(scenario, func(t *testing.T) {
			owner, month, identity, history, material, selection, raw := f.req.Owner, f.req.Month, document.SourceIdentity, f.sources.History, files.Material, files.Selection, bytes.Clone(body)
			switch scenario {
			case "owner":
				owner.GroupID++
			case "month":
				month = "2026-08"
			case "identity":
				identity = generationHash([]byte("another-source"))
			case "history":
				history.SHA256 = generationHash([]byte("other-history"))
			case "material":
				material.Path += ".other"
			case "selection":
				selection.SHA256 = generationHash([]byte("other-selection"))
			case "scope":
				raw = append(raw, ' ')
			}
			if proof.matches(owner, month, identity, history, material, selection, raw) {
				t.Fatal("producer capability was portable")
			}
		})
	}
	bad := *proof
	bad.scopeSHA256 = generationHash([]byte("wrong"))
	sources.assembly = &bad
	if _, err := d.g.prepareCapturedHistoryInput(t.Context(), f.req, files, sources, "captured-scope"); err == nil {
		t.Fatal("forged capability bypassed source normalization")
	}
}

func TestRetainedHistoryAssemblyExplicitHistoricalScopeKeepsPriority(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	addCapturedHistoryReconstruction(t, &f)
	d, _ := openHistoryAssembly(t, f, true, nil)
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SubmitMonthly(t.Context(), f.req.Owner.GroupID, f.req.Owner.AuthVersion, f.req.Month); err != nil {
		t.Fatal(err)
	}
	record := waitHistoryAssemblyBill(t, f, d)
	assertHistoryAssemblyMoney(t, record)
	selection, err := ReadGenerationSelection(t.Context(), d.receiver, f.req.Owner.GroupID, f.req.Month)
	if err != nil || selection.Request == nil {
		t.Fatal(err)
	}
	saved, err := d.g.loadRetainedGeneration(t.Context(), *selection.Request, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	scope, err := d.g.selectedRetainedScope(t.Context(), saved.plan, saved.input)
	if err != nil || len(scope.Accounts[0].Reconstructions) != 1 {
		t.Fatal("automatic assembly dropped selected historical reconstruction", err)
	}
	closeDeploymentTest(t, d)
}

func TestRetainedHistoryAssemblyMissingCatalogueTargetUsesOrdinaryOriginalHistory(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d, _ := openHistoryAssembly(t, f, true, nil)
	// A separate valid catalogue has only a different month. Missing target is
	// recoverable under the explicit policy; malformed/conflicting target is not.
	catalogue, ref := capturedHistoryCatalogueFixture(t, f)
	catalogue.Scopes[0].Group.Months = []string{"2026-08"}
	raw := materialPreparationJSON(t, catalogue)
	if err := os.WriteFile(ref.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ref.SHA256 = generationHash(raw)
	next, err := newCatalogueCapturedHistoryPreparation(d, f.spool, "captured-scope", f.sources.History, ref, true)
	if err != nil {
		t.Fatal(err)
	}
	files, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if ready, err := next(t.Context(), f.req, files); err != nil || !ready {
		t.Fatal("valid missing catalogue target did not assemble", ready, err)
	}
	closeDeploymentTest(t, d)
}

func TestRetainedHistoryAssemblyMissingOrUnusableAccountDoesNotBecomeZero(t *testing.T) {
	for _, scenario := range []string{"missing_member", "wrong_group", "later_baseline", "counter_basis"} {
		t.Run(scenario, func(t *testing.T) {
			f := newCapturedHistoryFixture(t)
			assemblyHistoryUpdate(t, &f, func(input *monitor.MonthlyPreparationInput) {
				switch scenario {
				case "missing_member":
					input.Accounts = input.Accounts[:1]
				case "wrong_group":
					input.Accounts[0].GroupID++
				case "later_baseline":
					input.Accounts[0].Baseline.At++
				case "counter_basis":
					input.Accounts[0].CounterBasis = "unknown"
				}
			})
			d, _ := openHistoryAssembly(t, f, false, nil)
			files, _, err := f.spool.Advance(t.Context(), f.req)
			if err != nil {
				t.Fatal(err)
			}
			if _, proof, err := d.assembleRetainedHistoryScope(t.Context(), f.spool, f.req, files, f.sources.History); !errors.Is(err, ErrGenerationEvidence) || proof != nil {
				t.Fatal("bad history accepted", err)
			}
			generationAdmissionCounts(t, d.receiver, 0, 0)
		})
	}
}

func TestRetainedHistoryAssemblyMoneyMismatchCannotPublish(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	assemblyHistoryUpdate(t, &f, func(input *monitor.MonthlyPreparationInput) { input.Accounts[0].Snapshots[0].BalanceQuota++ })
	d, _ := openHistoryAssembly(t, f, false, nil)
	files, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	next, err := newCatalogueCapturedHistoryPreparation(d, f.spool, "captured-scope", f.sources.History, monthlysource.MonthlyObservationFile{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if ready, err := next(t.Context(), f.req, files); err == nil || ready {
		t.Fatal("amount mismatch became an approved selection", ready, err)
	}
	var count int64
	if err := f.auth.Model(&monitor.PortalMonthlyBill{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("mismatched history published", count, err)
	}
	closeDeploymentTest(t, d)
}

func TestRetainedHistoryAssemblyReservedReferencesCannotBecomeFinancialEvidence(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	assemblyHistoryUpdate(t, &f, func(input *monitor.MonthlyPreparationInput) {
		input.Accounts[0].Baseline.Source = organizationRosterSourceID + "#not-a-balance"
	})
	d, _ := openHistoryAssembly(t, f, false, nil)
	files, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	next, err := newCatalogueCapturedHistoryPreparation(d, f.spool, "captured-scope", f.sources.History, monthlysource.MonthlyObservationFile{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if ready, err := next(t.Context(), f.req, files); err == nil || ready {
		t.Fatal("a new directory reference became an old balance source", ready, err)
	}
	generationAdmissionCounts(t, d.receiver, 0, 0)
	closeDeploymentTest(t, d)
}

func TestRetainedHistoryAssemblyReauditsWalletClassificationInsteadOfAssumingIt(t *testing.T) {
	// Create original synthetic subscription rows BEFORE append-only capture.
	// Never remove an immutability trigger to manufacture this test condition.
	f := newCapturedHistoryFixture(t, func(o *monthlysource.TokenUsageWindowObservation) {
		for i := range o.Rows {
			o.Rows[i].BillingSource = "subscription"
		}
		for i := range o.Totals {
			o.Totals[i].BillingSource = "subscription"
		}
	})
	d, _ := openHistoryAssembly(t, f, false, nil)
	// No valid seal can exist for this source. The producer independently
	// rechecks classification before accepting even an alleged sealed descriptor.
	files := CapturedSourceFiles{Mode: capturedSourceFilesMode}
	if _, proof, err := d.assembleRetainedHistoryScope(t.Context(), f.spool, f.req, files, f.sources.History); err == nil || proof != nil || !strings.Contains(err.Error(), "subscription or unclassified") {
		t.Fatal("subscription classification was not rejected by the original auditor", err)
	}
	generationAdmissionCounts(t, d.receiver, 0, 0)
}

func TestRetainedHistoryAssemblyInvalidExplicitCatalogueDoesNotFallBack(t *testing.T) {
	for _, scenario := range []string{"wrong_pin", "wrong_member", "duplicate_scope"} {
		t.Run(scenario, func(t *testing.T) {
			f := newCapturedHistoryFixture(t)
			d, _ := openHistoryAssembly(t, f, false, nil)
			catalogue, ref := capturedHistoryCatalogueFixture(t, f)
			switch scenario {
			case "wrong_member":
				catalogue.Scopes[0].Accounts = catalogue.Scopes[0].Accounts[:1]
			case "duplicate_scope":
				catalogue.Scopes = append(catalogue.Scopes, catalogue.Scopes[0])
			}
			raw := materialPreparationJSON(t, catalogue)
			if err := os.WriteFile(ref.Path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if scenario != "wrong_pin" {
				ref.SHA256 = generationHash(raw)
			} else {
				ref.SHA256 = generationHash([]byte("wrong pin"))
			}
			next, err := newCatalogueCapturedHistoryPreparation(d, f.spool, "captured-scope", f.sources.History, ref, true)
			if err != nil {
				t.Fatal(err)
			}
			files, _, err := f.spool.Advance(t.Context(), f.req)
			if err != nil {
				t.Fatal(err)
			}
			if err := d.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if ready, err := next(t.Context(), f.req, files); err == nil || ready {
				t.Fatal("invalid selected catalogue silently fell back", ready, err)
			}
			generationAdmissionCounts(t, d.receiver, 0, 0)
			closeDeploymentTest(t, d)
		})
	}
}

func TestRetainedHistoryAssemblyOldModeAndSourceChangesStayClosed(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d := f.open(t)
	cfg, _ := capturedPreparationConfigFixture(t, f)
	cfg.AssembleRetainedHistory = true
	ref := writeHistoryAssemblyConfig(t, f, cfg)
	opens := 0
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, func(*mysql.Config) (*sql.DB, error) { opens++; return nil, errors.New("must not open") }); err == nil || opens != 0 {
		t.Fatal("legacy policy accepted automatic history assembly", err)
	}
	if _, err := newCatalogueCapturedHistoryPreparation(d, f.spool, "captured-scope", f.sources.History, cfg.ScopeCatalogue, true); err == nil {
		t.Fatal("direct constructor bypassed registered policy")
	}
	closeDeploymentTest(t, d)
	d2, _ := openHistoryAssembly(t, f, false, nil)
	files, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.auth.Model(&monitor.CustomerGroup{}).Where("id=?", f.req.Owner.GroupID).Update("portal_auth_ver", 2).Error; err != nil {
		t.Fatal(err)
	}
	if _, proof, err := d2.assembleRetainedHistoryScope(t.Context(), f.spool, f.req, files, f.sources.History); err == nil || proof != nil {
		t.Fatal("revoked organization kept its assembly capability")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, proof, err := d2.assembleRetainedHistoryScope(ctx, f.spool, f.req, files, f.sources.History); err == nil || proof != nil {
		t.Fatal("cancelled operation produced a capability")
	}
	closeDeploymentTest(t, d2)
}
