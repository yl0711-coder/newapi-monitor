package monthlydraft

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// Real loopback login -> customer generation POST -> worker -> explicit TEST
// review -> saved bill -> public JSON/XLSX. Every account/source is synthetic;
// no .env, online source, genuine customer authority or production publication.
func TestReconstructedCustomerStatementSavedPageAndDownloadParity(t *testing.T) {
	a := newLocalGenerationAcceptance(t, true)
	own, foreign := a.login(t, "owner@example.invalid"), a.login(t, "other@example.invalid")
	request := func(method, path, body string, cookies []*http.Cookie) *httptest.ResponseRecorder {
		res := a.request(t, method, path, body, cookies)
		defer res.Body.Close()
		w := httptest.NewRecorder()
		for key, values := range res.Header {
			w.Header()[key] = values
		}
		w.WriteHeader(res.StatusCode)
		if _, err := io.Copy(w, io.LimitReader(res.Body, 8<<20)); err != nil {
			t.Fatal(err)
		}
		return w
	}
	if w := request(http.MethodGet, "/monthly-generation-local", "", own); w.Code != 200 || !strings.Contains(w.Body.String(), `id="monthlyView"`) {
		t.Fatal("actual monthly page missing", w.Code)
	}
	if w := request(http.MethodPost, "/api/monthly-statement/generation", `{"month":"2026-09"}`, own); w.Code != 202 {
		t.Fatal("customer generation request rejected", w.Code, w.Body.String())
	}
	job, err := a.g.tasks.Find(context.Background(), generationRequest(a.f.plan, mustGenerationPlan(t, a.f)))
	if err != nil {
		t.Fatal(err)
	}
	waitLocalDraft(t, a.g, job)
	// Drain the worker before this independent TEST review. Its successful
	// draft must never by itself expose an unapproved customer bill.
	stopRuntime(t, a.runtime)
	if w := request(http.MethodGet, "/api/monthly-statement?month=2026-09", "", own); w.Code != 404 {
		t.Fatal("unreviewed historical draft exposed to customer", w.Code)
	}
	load, err := NewRetainedAutomaticScopeLoader(a.g, syntheticScopeID)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := NewObservedAutomaticContext(a.g, load)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := metadata(context.Background(), job.Request)
	if err != nil {
		t.Fatal(err)
	}
	// Shared exact oracle compares source money, public summary, every account
	// and workbook cells; also checks wrong-org isolation and stale versions.
	testGenerationReviewedPublication(t, a.g, a.f, job, a.auth, request, own, foreign, meta.BundleContext)
	detailPath := "/api/monthly-statement?month=2026-09"
	before := request(http.MethodGet, detailPath, "", own)
	var bill struct {
		Data struct {
			Publication string `json:"publication"`
		} `json:"data"`
	}
	if json.Unmarshal(before.Body.Bytes(), &bill) != nil || bill.Data.Publication == "" {
		t.Fatal("frozen bill version missing")
	}
	downloadPath := "/api/monthly-statement/download?month=2026-09&publication=" + bill.Data.Publication
	beforeDownload := request(http.MethodGet, downloadPath, "", own)
	if beforeDownload.Code != 200 || !strings.HasPrefix(beforeDownload.Body.String(), "PK") {
		t.Fatal("frozen workbook unavailable", beforeDownload.Code)
	}
	privateMarkers := []string{"selected_scope_review", "candidate_sha256", "reconstruction_fixture", "snapshot_source", "reconstructions"}
	assertPublic := func(raw []byte) {
		t.Helper()
		for _, marker := range privateMarkers {
			if bytes.Contains(raw, []byte(marker)) {
				t.Fatal("private historical proof leaked to customer", marker)
			}
		}
	}
	assertPublic(before.Body.Bytes())
	book, err := zip.NewReader(bytes.NewReader(beforeDownload.Body.Bytes()), int64(beforeDownload.Body.Len()))
	if err != nil || len(book.File) > 32 {
		t.Fatal("invalid workbook parts", err)
	}
	for _, file := range book.File {
		reader, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		raw, readErr := io.ReadAll(io.LimitReader(reader, (2<<20)+1))
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || len(raw) > 2<<20 {
			t.Fatal("invalid workbook content", readErr, closeErr)
		}
		assertPublic(raw) // Inspect decompressed XML, not compressed ZIP bytes.
	}
	// Delete ONLY this test's freshly-created synthetic private evidence file.
	// A saved bill must survive stopped generation and unavailable private
	// input without re-reading sources or silently recalculating old amounts.
	if err := os.Remove(a.f.input.Sources[0].Path); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		after := request(http.MethodGet, detailPath, "", own)
		download := request(http.MethodGet, downloadPath, "", own)
		if after.Code != 200 || download.Code != 200 || !bytes.Equal(before.Body.Bytes(), after.Body.Bytes()) || !bytes.Equal(beforeDownload.Body.Bytes(), download.Body.Bytes()) {
			t.Fatal("published page/download depends on private generation evidence", after.Code, download.Code)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		path, body := "/api/monthly-statement/generation?month=2026-09", ""
		if method == http.MethodPost {
			path, body = "/api/monthly-statement/generation", `{"month":"2026-09"}`
		}
		if w := request(method, path, body, own); w.Code != 200 || !strings.Contains(w.Body.String(), `"state":"available"`) {
			t.Fatal("saved bill cannot be reused with generation stopped", method, w.Code, w.Body.String())
		}
	}
	generationAdmissionCounts(t, a.f.receiver, 1, 1)
}
