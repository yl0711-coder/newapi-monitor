package monthlydraft

// Explicit local original-file replay. Tests the new history normalizer with
// real already-populated token months; does not imitate a fresh SQL capture,
// grant production authority or publish a customer bill.
import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

type retainedCapturedHistoryRequest struct {
	Version  int                                  `json:"version"`
	Plan     monthlysource.MonthlyObservationFile `json:"plan"`
	Input    monthlysource.MonthlyObservationFile `json:"input"`
	Material monthlysource.MonthlyObservationFile `json:"material"`
}

func TestRetainedCapturedHistoryPreparationCompatibility(t *testing.T) {
	path, pin, output := os.Getenv("MONTHLY_REAL_CAPTURED_HISTORY_REQUEST"), os.Getenv("MONTHLY_REAL_CAPTURED_HISTORY_SHA256"), os.Getenv("MONTHLY_REAL_CAPTURED_HISTORY_OUTPUT")
	if path == "" {
		t.Skip("explicit pinned retained history/materials and private new output required")
	}
	raw := acceptancePrivateRead(t, path, 64<<10)
	var r retainedCapturedHistoryRequest
	if !generationHex(pin, 32) || generationHash(raw) != pin || json.Unmarshal(raw, &r) != nil || r.Version != 1 {
		t.Fatal("invalid pinned real history request")
	}
	retainedFixedPrivateDirectory(t, output)
	if entries, err := os.ReadDir(output); err != nil || len(entries) != 0 {
		t.Fatal("new empty private output required")
	}
	originals := map[string]string{path: pin}
	read := func(ref monthlysource.MonthlyObservationFile, limit int64) []byte {
		t.Helper()
		body := acceptancePrivateRead(t, ref.Path, limit)
		if !generationHex(ref.SHA256, 32) || generationHash(body) != ref.SHA256 {
			t.Fatal("original digest differs")
		}
		if old, ok := originals[ref.Path]; ok && old != ref.SHA256 {
			t.Fatal("conflicting original references")
		}
		originals[ref.Path] = ref.SHA256
		return body
	}
	defer func() {
		for file, digest := range originals {
			if generationHash(acceptancePrivateRead(t, file, generationEvidenceLimit)) != digest {
				t.Error("retained original changed")
			}
		}
	}()
	var p GenerationPlan
	if json.Unmarshal(read(r.Plan, generationPlanLimit), &p) != nil {
		t.Fatal("original plan missing")
	}
	input, err := monitor.DecodeMonthlyPreparationInput(read(r.Input, monitor.MonthlyPreparationInputLimit))
	if err != nil {
		t.Fatal(err)
	}
	before := materialPreparationJSON(t, input.Accounts)
	var scope RetainedAutomaticScope
	var scopeRef monthlysource.MonthlyObservationFile
	for i, s := range input.Sources {
		body := read(monthlysource.MonthlyObservationFile{Path: s.Path, SHA256: s.SHA256}, generationEvidenceLimit)
		if !validScopeSourceID(s.ID) {
			t.Fatal("unsafe retained source ID")
		}
		file := filepath.Join(output, "original-"+s.ID+".source")
		acceptanceWriteNew(t, file, body)
		input.Sources[i].Path = file
		if s.ID == p.AutomaticScopeSourceID {
			if json.Unmarshal(body, &scope) != nil || scope.Owner != p.Owner || scope.Month != p.Funding.Month || scope.SourceIdentity != p.Funding.SourceIdentity {
				t.Fatal("independent historical scope differs")
			}
			scopeRef = monthlysource.MonthlyObservationFile{Path: file, SHA256: s.SHA256}
		}
	}
	if !bytes.Equal(before, materialPreparationJSON(t, input.Accounts)) || scopeRef.Path == "" {
		t.Fatal("relocation changed account money or lost scope")
	}
	historyRaw := draftJSON(t, input)
	history := monthlysource.MonthlyObservationFile{Path: filepath.Join(output, "history.json"), SHA256: generationHash(historyRaw)}
	acceptanceWriteNew(t, history.Path, historyRaw)
	catalogueRaw := materialPreparationJSON(t, CapturedHistoryCatalogue{Version: 1, SourceIdentity: scope.SourceIdentity, Scopes: []AutomaticScope{scope.Scope}})
	catalogue := monthlysource.MonthlyObservationFile{Path: filepath.Join(output, "scope-catalogue.json"), SHA256: generationHash(catalogueRaw)}
	acceptanceWriteNew(t, catalogue.Path, catalogueRaw)
	var material monthlysource.MonthlyObservationMaterial
	if json.Unmarshal(read(r.Material, 2<<20), &material) != nil || material.GroupID != p.Owner.GroupID || material.Funding.Month != p.Funding.Month || !reflect.DeepEqual(material.Funding, p.Funding) || material.WalletReuse != nil || len(material.WalletIDs) != 2 {
		t.Fatal("real original captured material differs")
	}
	database := read(material.Database, generationEvidenceLimit)
	material.Database.Path = filepath.Join(output, "captured-observations.sqlite")
	acceptanceWriteNew(t, material.Database.Path, database)
	for i, token := range material.Tokens {
		body := read(token.File, generationEvidenceLimit)
		material.Tokens[i].File.Path = filepath.Join(output, capturedTokenName(token.UserID))
		acceptanceWriteNew(t, material.Tokens[i].File.Path, body)
	}
	materialRaw := materialPreparationJSON(t, material)
	materialRef := monthlysource.MonthlyObservationFile{Path: filepath.Join(output, "materials.json"), SHA256: generationHash(materialRaw)}
	acceptanceWriteNew(t, materialRef.Path, materialRaw)
	req := SourcePreparationRequest{Owner: p.Owner, Month: p.Funding.Month}
	// Current-scope envelope only, from the retained full owner. This is NOT a
	// manufactured source observation, balance or publication authorization.
	selection := monthlysource.CapturedMonthSelection{Version: 1, Mode: monthlysource.CapturedMonthSelectionMode,
		Key: req.Key(), PlanSHA256: r.Plan.SHA256, Scope: monthlysource.OrganizationMonthScope{Version: 1, Mode: monthlysource.OrganizationMonthScopeMode,
			GroupID: p.Owner.GroupID, AuthVersion: p.Owner.AuthVersion, MemberFingerprint: p.Owner.MemberFingerprint, MemberManifest: p.Owner.MemberManifest, Funding: p.Funding}}
	selectionRaw := materialPreparationJSON(t, selection)
	selectionRef := monthlysource.MonthlyObservationFile{Path: filepath.Join(output, "current-scope.json"), SHA256: generationHash(selectionRaw)}
	acceptanceWriteNew(t, selectionRef.Path, selectionRaw)
	receiver := generationDB(t, filepath.Join(output, "test-receiver.sqlite"), false)
	if InitGenerationReceiver(t.Context(), receiver) != nil || InitGenerationSelections(t.Context(), receiver) != nil {
		t.Fatal("local receiver initialization failed")
	}
	funding := generationDB(t, material.Database.Path, true)
	g, err := NewGeneration(receiver, funding, output, func(context.Context, monthlytasks.Request) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	prepared, err := g.prepareCapturedHistoryInput(t.Context(), req, CapturedSourceFiles{Mode: capturedSourceFilesMode, Material: materialRef, Selection: selectionRef},
		CapturedHistorySources{History: history, Scope: scopeRef, catalogue: &catalogue}, p.AutomaticScopeSourceID)
	if err != nil {
		t.Fatal("real retained history rejected by captured preparation", err)
	}
	got, err := monitor.DecodeMonthlyPreparationInput(prepared.Preparation)
	if err != nil || len(got.Accounts) != len(input.Accounts) {
		t.Fatal("prepared member scope differs", err)
	}
	for i, old := range input.Accounts {
		actual := got.Accounts[i]
		if actual.UserID != old.UserID || actual.Baseline != old.Baseline || !reflect.DeepEqual(actual.TokenMonths[0].Rows, old.TokenMonths[0].Rows) {
			t.Fatal("real account or token amount changed", old.UserID)
		}
		if len(actual.Snapshots) < len(old.Snapshots) || !reflect.DeepEqual(actual.Snapshots[:len(old.Snapshots)], old.Snapshots) {
			t.Fatal("original historical snapshots changed")
		}
	}
	acceptanceWriteNew(t, filepath.Join(output, "prepared-input.json"), prepared.Preparation)
	acceptanceWriteNew(t, filepath.Join(output, "prepared-plan.json"), prepared.Plan)
	acceptanceWriteNew(t, filepath.Join(output, "acceptance.json"), materialPreparationJSON(t, map[string]any{
		"status": "retained_captured_history_preparation_passed", "group_id": p.Owner.GroupID, "month": p.Funding.Month,
		"accounts": len(input.Accounts), "originals_checked": len(originals), "original_money_changed": false,
		"online_access": false, "production_writes": false, "published": false,
		"qualification": "original captured material plus retained history; not a live source, startup, authority or customer-publication test",
	}))
}
