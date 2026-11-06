package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func capturedHistoryCatalogueFixture(t *testing.T, f capturedHistoryFixture) (CapturedHistoryCatalogue, monthlysource.MonthlyObservationFile) {
	t.Helper()
	raw, err := os.ReadFile(f.sources.Scope.Path)
	if err != nil {
		t.Fatal(err)
	}
	var doc RetainedAutomaticScope
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	catalogue := CapturedHistoryCatalogue{Version: 1, SourceIdentity: doc.SourceIdentity, Scopes: []AutomaticScope{doc.Scope}}
	raw = materialPreparationJSON(t, catalogue)
	path := filepath.Join(f.cfg.EvidenceRoot, "shared-scope-catalogue.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return catalogue, monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(raw)}
}

func TestCapturedHistoryCatalogueExactSelectionAndNoMonetaryChanges(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	catalogue, _ := capturedHistoryCatalogueFixture(t, f)
	current, err := f.wallet.funding.scope(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	// Unrelated metadata is NOT interpreted as this group's sources. It can
	// reference absent original files without causing unrelated I/O.
	other := catalogue.Scopes[0]
	other.Group.GroupID++
	other.Group.Source = "absent-other-organization"
	catalogue.Scopes = append(catalogue.Scopes, other)
	raw := materialPreparationJSON(t, catalogue)
	got, err := selectCapturedHistoryScope(raw, catalogue.SourceIdentity, f.req.Owner, f.req.Month, current.Funding.UserIDs)
	if err != nil || got.Owner != f.req.Owner || got.Month != f.req.Month || !reflect.DeepEqual(got.Scope, catalogue.Scopes[0]) {
		t.Fatal("source metadata changed or selected another group", err)
	}
	if !bytes.Equal(raw, materialPreparationJSON(t, catalogue)) {
		t.Fatal("mutated original catalogue")
	}
	for _, scenario := range []string{"wrong_source", "wrong_group", "wrong_month", "missing_member", "extra_member", "duplicate_member", "duplicate_scope", "multiple_months", "unknown_field", "bad_version"} {
		t.Run(scenario, func(t *testing.T) {
			var c CapturedHistoryCatalogue
			if err := json.Unmarshal(raw, &c); err != nil {
				t.Fatal(err)
			}
			owner, month, ids := f.req.Owner, f.req.Month, slices.Clone(current.Funding.UserIDs)
			switch scenario {
			case "wrong_source":
				c.SourceIdentity = generationHash([]byte("another source"))
			case "wrong_group":
				owner.GroupID += 7
			case "wrong_month":
				month = "2026-08"
			case "missing_member":
				c.Scopes[0].Accounts = c.Scopes[0].Accounts[:1]
			case "extra_member":
				ids = append(ids, 900001)
			case "duplicate_member":
				c.Scopes[0].Group.Members[1] = c.Scopes[0].Group.Members[0]
			case "duplicate_scope":
				c.Scopes = append(c.Scopes, c.Scopes[0])
			case "multiple_months":
				c.Scopes[0].Group.Months = append(c.Scopes[0].Group.Months, "2026-08")
			case "bad_version":
				c.Version++
			}
			bad := materialPreparationJSON(t, c)
			if scenario == "unknown_field" {
				bad = append([]byte(`{"approval":true,`), bad[1:]...)
			}
			if _, err := selectCapturedHistoryScope(bad, catalogue.SourceIdentity, owner, month, ids); err == nil {
				t.Fatal("ambiguous, missing or different source accepted")
			}
		})
	}
}

func TestCapturedHistoryCatalogueRetainsOriginalProvenanceAndReusesSeal(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d := f.open(t)
	catalogue, ref := capturedHistoryCatalogueFixture(t, f)
	files, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	next, err := NewCatalogueCapturedHistoryPreparation(d, f.spool, "captured-scope", f.sources.History, ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if ok, err := next(t.Context(), f.req, files); err != nil || !ok {
		t.Fatal("catalogue path did not select an audited input", ok, err)
	}
	prepared, err := ReadGenerationSelection(t.Context(), d.receiver, f.req.Owner.GroupID, f.req.Month)
	if err != nil || prepared.Request == nil {
		t.Fatal("selection missing", err)
	}
	// No per-org/month scope file supplied to the constructor. The derived
	// envelope is exact original scope + current authority, never new money.
	body := materialPreparationJSON(t, RetainedAutomaticScope{Version: 1, Owner: f.req.Owner, Month: f.req.Month, SourceIdentity: catalogue.SourceIdentity, Scope: catalogue.Scopes[0]})
	scopePath := filepath.Join(filepath.Dir(files.Material.Path), "history-scope-"+generationHash(body)+".json")
	if got, err := os.ReadFile(scopePath); err != nil || !bytes.Equal(got, body) {
		t.Fatal("derived envelope differs", err)
	}
	sources := CapturedHistorySources{History: f.sources.History, Scope: monthlysource.MonthlyObservationFile{Path: scopePath, SHA256: generationHash(body)}, catalogue: &ref}
	input, err := d.g.prepareCapturedHistoryInput(t.Context(), f.req, files, sources, "captured-scope")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := monitor.DecodeMonthlyPreparationInput(input.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range decoded.Sources {
		if s.ID == organizationScopeCatalogueID {
			found = s.Path == ref.Path && s.SHA256 == ref.SHA256
		}
	}
	if !found {
		t.Fatal("derived envelope lost original metadata provenance")
	}
	for _, a := range decoded.Accounts {
		if a.Baseline.BalanceQuota != 500000000 || a.Snapshots[0].BalanceQuota != 499000000 || len(a.TokenMonths) != 1 || len(a.TokenMonths[0].Rows) != 3 {
			t.Fatal("changed original money or attribution", a.UserID)
		}
	}
	if got, fresh, err := f.spool.Advance(t.Context(), f.req); err != nil || fresh || got != files {
		t.Fatal("valid derived scope forced raw reseal", err)
	}
	if ok, err := next(t.Context(), f.req, files); err != nil || !ok {
		t.Fatal("repeat preparation failed", err)
	}
	after, err := ReadGenerationSelection(t.Context(), d.receiver, f.req.Owner.GroupID, f.req.Month)
	if err != nil || !reflect.DeepEqual(after, prepared) {
		t.Fatal("repeat selected a different version", err)
	}
	// Pinned originals cannot be changed after envelope creation. Neither the
	// existing selection nor original raw files may be replaced on failure.
	if err := os.WriteFile(ref.Path, append(materialPreparationJSON(t, catalogue), '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if ok, err := next(t.Context(), f.req, files); err == nil || ok {
		t.Fatal("stale original digest accepted")
	}
	final, err := ReadGenerationSelection(t.Context(), d.receiver, f.req.Owner.GroupID, f.req.Month)
	if err != nil || !reflect.DeepEqual(final, prepared) {
		t.Fatal("failed revalidation changed selected money", err)
	}
}

func TestCapturedHistoryCatalogueCustomerFirstClickSavesAndDownloads(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	addCapturedHistoryReconstruction(t, &f)
	testCapturedHistoryPublicationAdapter(t, f, true, true, true, false)
}

func TestCapturedHistoryCatalogueCannotApproveMissingOrIncorrectHistory(t *testing.T) {
	for _, scenario := range []string{"missing_month", "incomplete_roster", "unproven_wallet", "incorrect_balance", "missing_original"} {
		t.Run(scenario, func(t *testing.T) {
			f := newCapturedHistoryFixture(t)
			d := f.open(t)
			catalogue, ref := capturedHistoryCatalogueFixture(t, f)
			history := f.sources.History
			switch scenario {
			case "missing_month":
				catalogue.Scopes[0].Group.Months = []string{"2026-08"}
			case "incomplete_roster":
				catalogue.Scopes[0].Accounts = catalogue.Scopes[0].Accounts[:1]
			case "unproven_wallet":
				catalogue.Scopes[0].Accounts[0].WalletOnly = false
			case "incorrect_balance":
				input := draftCopy(t, f.f.input)
				input.Accounts[0].Snapshots[0].BalanceQuota++
				raw := draftJSON(t, input)
				if err := os.WriteFile(history.Path, raw, 0600); err != nil {
					t.Fatal(err)
				}
				history.SHA256 = generationHash(raw)
			case "missing_original":
				history.Path = filepath.Join(f.cfg.EvidenceRoot, "missing-history.json")
			}
			raw := materialPreparationJSON(t, catalogue)
			if err := os.WriteFile(ref.Path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			ref.SHA256 = generationHash(raw)
			next, err := NewCatalogueCapturedHistoryPreparation(d, f.spool, "captured-scope", history, ref)
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
			if ok, err := next(t.Context(), f.req, files); err == nil || ok {
				t.Fatal("missing or inconsistent history became approved input")
			}
			preparationCounts(t, f.f, 0, 0)
			var bills int64
			if err := f.auth.Model(&monitor.PortalMonthlyBill{}).Count(&bills).Error; err != nil || bills != 0 {
				t.Fatal("bad source created a customer bill", err)
			}
		})
	}
}

func TestCapturedHistoryCatalogueDerivedScopeCannotReplaceOriginalMetadata(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d := f.open(t)
	catalogue, ref := capturedHistoryCatalogueFixture(t, f)
	files, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	// Rehashing a forged envelope cannot turn it into independent proof.
	catalogue.Scopes[0].Group.Name = "changed derived name"
	forged := materialPreparationJSON(t, RetainedAutomaticScope{Version: 1, Owner: f.req.Owner, Month: f.req.Month, SourceIdentity: catalogue.SourceIdentity, Scope: catalogue.Scopes[0]})
	path := filepath.Join(f.cfg.EvidenceRoot, "forged-derived-scope.json")
	if err := os.WriteFile(path, forged, 0600); err != nil {
		t.Fatal(err)
	}
	sources := CapturedHistorySources{History: f.sources.History, Scope: monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(forged)}, catalogue: &ref}
	if _, err := d.g.prepareCapturedHistoryInput(t.Context(), f.req, files, sources, "captured-scope"); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("derived scope attested itself", err)
	}
	preparationCounts(t, f.f, 0, 0)
}

func TestCapturedHistoryScopeRetentionLimitsAndNoOverwrite(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	files, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"synthetic":"scope-envelope"}`)
	root, lock, err := openCapturedOutput(f.spool.directory)
	if err != nil {
		t.Fatal(err)
	}
	_, retainErr := f.spool.retainHistoryScope(t.Context(), f.req, files, body)
	closeErr := errors.Join(lock.Close(), root.Close())
	if !errors.Is(retainErr, ErrGenerationBusy) || closeErr != nil {
		t.Fatal("ignored spool writer ownership", retainErr, closeErr)
	}
	originalLimit := f.spool.limit
	f.spool.limit = 1
	if _, err := f.spool.retainHistoryScope(t.Context(), f.req, files, body); !errors.Is(err, ErrGenerationCapacity) {
		t.Fatal("ignored allocated-byte budget", err)
	}
	f.spool.limit = originalLimit
	wrong := f.req
	wrong.Month = "2026-08"
	if _, err := f.spool.retainHistoryScope(t.Context(), wrong, files, body); err == nil {
		t.Fatal("wrote another request's seal")
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.spool.retainHistoryScope(cancelled, f.req, files, body); !errors.Is(err, context.Canceled) {
		t.Fatal("ignored cancellation", err)
	}
	ref, err := f.spool.retainHistoryScope(t.Context(), f.req, files, body)
	if err != nil {
		t.Fatal(err)
	}
	// Reusing the same content does not allocate a new version, even when
	// budget is now exhausted; original completed sources remain readable.
	f.spool.limit = 1
	if got, err := f.spool.retainHistoryScope(t.Context(), f.req, files, body); err != nil || got != ref {
		t.Fatal("repeat consumed capacity", err)
	}
	f.spool.limit = originalLimit
	for n := 1; n < capturedHistoryScopeVersions; n++ {
		if _, err := f.spool.retainHistoryScope(t.Context(), f.req, files, []byte{byte(n)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.spool.retainHistoryScope(t.Context(), f.req, files, []byte("ninth version")); !errors.Is(err, ErrGenerationCapacity) {
		t.Fatal("unbounded scope revisions", err)
	}
	if err := os.WriteFile(ref.Path, []byte("corrupted partial file"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.spool.retainHistoryScope(t.Context(), f.req, files, body); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("overwrote corrupted output", err)
	}
	if _, fresh, err := f.spool.Advance(t.Context(), f.req); err == nil || fresh {
		t.Fatal("corrupt scope was replaced by a fresh raw seal", err)
	}
	if got, _ := os.ReadFile(ref.Path); string(got) != "corrupted partial file" {
		t.Fatal("failure deleted or overwrote diagnostic evidence")
	}
}
