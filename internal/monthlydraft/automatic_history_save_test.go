package monthlydraft

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func retainedHistoryCandidateFixture(t *testing.T) (capturedHistoryFixture, automaticHistoryCandidate, RetainedScopePreparation) {
	t.Helper()
	f, input, directory := historyCandidateFixture(t, false)
	candidate, err := f.f.g.prepareAutomaticHistoryCandidate(t.Context(), input, directory)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.f.root, "derived-retained-scope.json")
	prepared, err := f.f.g.PrepareRetainedScopeInput(t.Context(), candidate.plan, candidate.input, "captured-scope", path, candidate.scope)
	if err != nil {
		t.Fatal("source-derived candidate could not enter retained scope", err)
	}
	if err := os.WriteFile(path, prepared.Scope, 0600); err != nil {
		t.Fatal(err)
	}
	reused, err := f.f.g.PrepareExistingRetainedScopeInput(t.Context(), candidate.plan, candidate.input, "captured-scope", monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(prepared.Scope)})
	if err != nil || !reflect.DeepEqual(prepared, reused) {
		t.Fatal("retained repackaging changed financial input", err)
	}
	return f, candidate, prepared
}

func TestAutomaticHistorySaveActualPortalAndRestart(t *testing.T) {
	f, _, prepared := retainedHistoryCandidateFixture(t)
	// Full formal admission under actual local Usage authorization/selection,
	// not a permissive fixture callback. All files are temporary synthetic data.
	if result, err := PrepareGenerationInput(t.Context(), f.cfg, prepared.Plan, prepared.Preparation, "", false); err != nil || result.Applied {
		t.Fatal("formal read-only admission", err)
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
	if result, err := PrepareGenerationInput(t.Context(), f.cfg, prepared.Plan, prepared.Preparation, "", true); err != nil || !result.Applied {
		t.Fatal("formal local selection", err)
	}
	d := f.open(t)
	request, own, foreign := capturedHistoryHTTP(t, f, d)
	d.runtime.worker.pollMin = 5 * time.Millisecond
	d.runtime.worker.pollMax = 20 * time.Millisecond
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if out := request("POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`, own); out.Code != 202 {
		t.Fatal("customer button admission", out.Code, out.Body.String())
	}
	deadline := time.Now().Add(20 * time.Second)
	var record monitor.PortalMonthlyBill
	for f.auth.First(&record, "group_id=? AND month=?", f.req.Owner.GroupID, f.req.Month).Error != nil {
		if time.Now().After(deadline) {
			t.Fatal("source-derived bill not saved", d.runtime.worker.Status())
		}
		time.Sleep(20 * time.Millisecond)
	}
	closeDeploymentTest(t, d)
	var evidence monitor.MonthlyBillEvidence
	if json.Unmarshal([]byte(record.Evidence), &evidence) != nil || len(evidence.Users) != 2 {
		t.Fatal("invalid saved evidence")
	}
	for _, u := range evidence.Users {
		if u.OpeningQuota != 0 || u.ClosingQuota != -1000000 || u.ConsumeQuota != 1500000 || u.RefundQuota != 500000 || len(u.Events) != 0 || u.TokenUsage == nil || len(u.TokenUsage.Rows) != 3 {
			t.Fatal("saved customer money/token allocation differs from originals", u.UserID)
		}
	}
	page, book := historySavedDownload(t, request, own, foreign)
	reopened := f.open(t)
	if worked, err := reopened.runtime.worker.step(t.Context()); err != nil || worked {
		t.Fatal("restart regenerated published bill", worked, err)
	}
	var after monitor.PortalMonthlyBill
	if err := f.auth.First(&after, "group_id=? AND month=?", f.req.Owner.GroupID, f.req.Month).Error; err != nil || after != record {
		t.Fatal("immutable bill changed", err)
	}
	// Saved viewing/download must not need mutable generation evidence.
	var p GenerationPlan
	if json.Unmarshal(prepared.Plan, &p) != nil {
		t.Fatal("plan")
	}
	input, err := monitor.DecodeMonthlyPreparationInput(prepared.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range input.Sources {
		if source.ID == automaticHistoryDirectorySourceID {
			if err := os.Remove(source.Path); err != nil {
				t.Fatal(err)
			}
		}
	}
	pageAfter, bookAfter := historySavedDownload(t, request, own, foreign)
	if !bytes.Equal(page, pageAfter) || !bytes.Equal(book, bookAfter) {
		t.Fatal("saved view/download recalculated amounts")
	}
}

func historySavedDownload(t *testing.T, request func(string, string, string, []*http.Cookie) *httptest.ResponseRecorder, own, foreign []*http.Cookie) ([]byte, []byte) {
	t.Helper()
	page := request("GET", "/api/monthly-statement?month=2026-09", "", own)
	var view struct {
		Data struct {
			Publication string `json:"publication"`
			Opening     string `json:"month_opening_balance"`
			Closing     string `json:"month_end_balance"`
		} `json:"data"`
	}
	if page.Code != 200 || json.Unmarshal(page.Body.Bytes(), &view) != nil || view.Data.Publication == "" || view.Data.Opening != "0.000000" || view.Data.Closing != "-4.000000" {
		t.Fatal("customer saved projection differs", page.Code, page.Body.String())
	}
	if out := request("GET", "/api/monthly-statement?month=2026-09", "", foreign); out.Code == 200 {
		t.Fatal("another organization read saved bill")
	}
	book := request("GET", "/api/monthly-statement/download?month=2026-09&publication="+view.Data.Publication, "", own)
	if book.Code != 200 {
		t.Fatal("download unavailable", book.Code)
	}
	r, err := zip.NewReader(bytes.NewReader(book.Body.Bytes()), int64(book.Body.Len()))
	if err != nil {
		t.Fatal(err)
	}
	sheets := 0
	for _, part := range r.File {
		if !strings.HasPrefix(part.Name, "xl/worksheets/sheet") {
			continue
		}
		file, err := part.Open()
		if err != nil {
			t.Fatal(err)
		}
		raw, readErr := io.ReadAll(io.LimitReader(file, (2<<20)+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil || len(raw) > 2<<20 {
			t.Fatal("invalid workbook", readErr, closeErr)
		}
		if part.Name == "xl/worksheets/sheet1.xml" && (!bytes.Contains(raw, []byte(">0.00<")) || !bytes.Contains(raw, []byte(">-4.00<"))) {
			t.Fatal("workbook summary differs from saved bill")
		}
		for _, private := range []string{"same_view", "automatic-history", "candidate_sha256", "snapshot_source"} {
			if bytes.Contains(raw, []byte(private)) || bytes.Contains(page.Body.Bytes(), []byte(private)) {
				t.Fatal("private source proof leaked", private)
			}
		}
		sheets++
	}
	if sheets != 3 {
		t.Fatal("summary/member/token sheets missing", sheets)
	}
	return bytes.Clone(page.Body.Bytes()), bytes.Clone(book.Body.Bytes())
}

func TestAutomaticHistorySaveRejectsForgedScopeAndMetadata(t *testing.T) {
	f, c, _ := retainedHistoryCandidateFixture(t)
	input, err := monitor.DecodeMonthlyPreparationInput(c.input)
	if err != nil {
		t.Fatal(err)
	}
	bound, release, err := f.f.g.bindObservationStore(t.Context(), c.plan, input)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	windows, err := bound.auditWindows(t.Context(), c.plan)
	if err != nil {
		t.Fatal(err)
	}
	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := bound.observedAutomaticContext(expired, c.plan, c.input, windows, c.scope); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("transient timeout became permanent evidence failure", err)
	}
	meta, err := bound.observedAutomaticContext(t.Context(), c.plan, c.input, windows, c.scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepareRetainedContextBundle(t.Context(), c.plan, c.input, meta, time.Now()); err != nil {
		t.Fatal(err)
	}
	changed := meta
	changed.BundleContext = bytes.Replace(meta.BundleContext, []byte("shared-label"), []byte("changed-label"), 1)
	if bytes.Equal(changed.BundleContext, meta.BundleContext) {
		t.Fatal("mutation did not change test context")
	}
	if _, err := prepareRetainedContextBundle(t.Context(), c.plan, c.input, changed, time.Now()); err == nil {
		t.Fatal("native proof allowed changed metadata")
	}
	changedPlan := c.plan
	changedPlan.Owner.AuthVersion++
	if _, err := prepareRetainedContextBundle(t.Context(), changedPlan, c.input, meta, time.Now()); err == nil {
		t.Fatal("native proof crossed authorization")
	}
	public, err := json.Marshal(meta)
	if err != nil || json.Unmarshal(public, &meta) != nil || meta.historyWitness != nil {
		t.Fatal("JSON preserved native audit authority", err)
	}
	if _, err := prepareRetainedContextBundle(t.Context(), c.plan, c.input, meta, time.Now()); err == nil {
		t.Fatal("serialized metadata became source proof")
	}
	for _, mutate := range []func(*AutomaticScope){
		func(s *AutomaticScope) { *s.Accounts[0].Reconstructions[0].BalanceQuota++ },
		func(s *AutomaticScope) { s.Accounts[0].Reconstructions[0].SameView.Funding[0].DeltaQuota++ },
		func(s *AutomaticScope) { s.Group.Members[0].Username = "another-user" },
		func(s *AutomaticScope) { s.Accounts[0].Reconstructions[0].SameView.CompletedAtMS++ },
	} {
		var altered AutomaticScope
		if json.Unmarshal(materialPreparationJSON(t, c.scope), &altered) != nil {
			t.Fatal("scope")
		}
		mutate(&altered)
		if _, err := bound.observedAutomaticContext(t.Context(), c.plan, c.input, windows, altered); err == nil {
			t.Fatal("altered financial scope accepted")
		}
	}
}

func TestAutomaticHistorySaveReadyReuseStillAuditsOriginals(t *testing.T) {
	f, _, prepared := retainedHistoryCandidateFixture(t)
	if _, err := PrepareGenerationInput(t.Context(), f.cfg, prepared.Plan, prepared.Preparation, "", true); err != nil {
		t.Fatal(err)
	}
	d := f.open(t)
	job, err := d.g.SubmitUsageMonth(t.Context(), f.auth, f.req.Owner.GroupID, f.req.Owner.AuthVersion, f.req.Month)
	if err != nil {
		t.Fatal(err)
	}
	first, err := d.g.Run(t.Context(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	again, err := d.g.Run(t.Context(), job.Request.Owner, job.ID)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatal("ready reuse changed calculation", err)
	}
	load, err := NewRetainedAutomaticScopeLoader(d.g, "captured-scope")
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := NewObservedAutomaticContext(d.g, load)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := metadata(t.Context(), job.Request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.g.PrepareReviewHandoff(t.Context(), job.Request.Owner, job.ID, meta.BundleContext); err != nil {
		t.Fatal("fresh handoff rejected original context", err)
	}
	changed := bytes.Replace(meta.BundleContext, []byte("shared-label"), []byte("changed-label"), 1)
	if bytes.Equal(changed, meta.BundleContext) {
		t.Fatal("handoff mutation did not change context")
	}
	if _, err := d.g.PrepareReviewHandoff(t.Context(), job.Request.Owner, job.ID, changed); err == nil {
		t.Fatal("caller supplied altered handoff metadata")
	}
	if err := os.WriteFile(filepath.Join(f.f.root, "retained-directory.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := d.g.Run(t.Context(), job.Request.Owner, job.ID); err == nil {
		t.Fatal("ready artifact skipped fresh original audit")
	}
	var count int64
	if err := f.auth.Model(&monitor.PortalMonthlyBill{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("draft/corruption published a bill", count, err)
	}
}

func TestAutomaticHistorySaveRetainedScopeCannotAttestItself(t *testing.T) {
	f, _, prepared := retainedHistoryCandidateFixture(t)
	var p GenerationPlan
	if json.Unmarshal(prepared.Plan, &p) != nil {
		t.Fatal("plan")
	}
	input, err := monitor.DecodeMonthlyPreparationInput(prepared.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	var doc RetainedAutomaticScope
	if json.Unmarshal(prepared.Scope, &doc) != nil {
		t.Fatal("scope")
	}
	for _, source := range input.Sources {
		if source.ID != automaticHistoryDirectorySourceID {
			continue
		}
		ambiguous := input
		ambiguous.Sources = append(append([]monitor.MonthlyPreparationSource(nil), input.Sources...), source)
		bound, release, err := f.f.g.bindObservationStore(t.Context(), p, input)
		if err != nil {
			t.Fatal(err)
		}
		err = bound.auditAutomaticHistoryBinding(t.Context(), p, ambiguous)
		release()
		if err == nil {
			t.Fatal("duplicate metadata silently stripped before validation")
		}
	}
	*doc.Scope.Accounts[0].Reconstructions[0].BalanceQuota++
	body := materialPreparationJSON(t, doc)
	for i := range input.Sources {
		s := &input.Sources[i]
		if s.ID == p.AutomaticScopeSourceID {
			if err := os.WriteFile(s.Path, body, 0600); err != nil {
				t.Fatal(err)
			}
			s.SHA256 = generationHash(body)
		}
	}
	raw := materialPreparationJSON(t, input)
	p.PreparationSHA256 = generationHash(raw)
	bound, release, err := f.f.g.bindObservationStore(t.Context(), p, input)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := bound.auditAutomaticHistoryBinding(t.Context(), p, input); err == nil {
		t.Fatal("rehashed edited scope attested its own financial claim")
	}
	if generationArtifactReconciled(p, monitor.MonthlyPreparationReport{}, nil) {
		t.Fatal("selected scope omitted fresh financial proof")
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}
