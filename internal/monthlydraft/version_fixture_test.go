package monthlydraft

// Explicit synthetic fixture export for an offline binary-version drill. This
// file is excluded from production images; it never accepts a source database.
import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeploymentExportSyntheticVersionFixture(t *testing.T) {
	root := os.Getenv("MONTHLY_VERSION_SYNTHETIC_FIXTURE")
	if root == "" {
		t.Skip("explicit empty synthetic export directory required")
	}
	info, err := os.Lstat(root)
	if err != nil || !filepath.IsAbs(root) || filepath.Clean(root) != root || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		t.Fatal("private absolute directory required")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatal("export refuses a nonempty directory", err)
	}
	a := newLocalGenerationAcceptance(t)
	ctx := context.Background()
	if err := a.runtime.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	job, err := a.g.Submit(ctx, a.f.plan, a.f.raw)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := a.g.Run(ctx, job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact)
	request := func(method, path, body string, cookies []*http.Cookie) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, a.server.URL+path, strings.NewReader(body))
		req.Header.Set("X-Monthly-Statistics", "nullable-v1")
		req.Header.Set("X-Monthly-Display", "cents-chain-v1")
		for _, cookie := range cookies {
			req.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		a.router.ServeHTTP(w, req)
		return w
	}
	own, foreign := a.login(t, "owner@example.invalid"), a.login(t, "other@example.invalid")
	testGenerationReviewedPublication(t, a.g, a.f, job, a.auth, request, own, foreign)
	expected := request("GET", "/api/monthly-statement?month=2026-09", "", own)
	if expected.Code != 200 {
		t.Fatal("synthetic bill unavailable", expected.Code)
	}
	for _, dir := range []string{"data", "sources", "jobs", "evidence"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	write := func(name string, body []byte) {
		t.Helper()
		f, err := os.OpenFile(filepath.Join(root, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.Write(body)
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			t.Fatal(err, closeErr)
		}
	}
	write("expected-monthly.json", expected.Body.Bytes())
	source, err := os.ReadFile(a.f.input.Sources[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	write("evidence/independent-evidence.json", source)
	authDB, err := a.auth.DB()
	if err != nil {
		t.Fatal(err)
	}
	for _, copy := range []struct {
		name   string
		vacuum func(string) error
	}{
		{"data/monitor.db", func(path string) error { _, err := authDB.ExecContext(ctx, "VACUUM INTO ?", path); return err }},
		{"sources/funding.sqlite", func(path string) error { _, err := a.f.funding.ExecContext(ctx, "VACUUM INTO ?", path); return err }},
	} {
		path := filepath.Join(root, copy.name)
		if err := copy.vacuum(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Prepare a fresh selected input without submitting a task. The real main
	// process must receive the authenticated request and perform the calculation.
	f := a.f
	f.root = filepath.Join(root, "evidence")
	f.input.Sources[0].Path = filepath.Join(f.root, "independent-evidence.json")
	f.raw = draftJSON(t, f.input)
	f.plan.PreparationSHA256 = generationHash(f.raw)
	f.receiverPath = filepath.Join(root, "jobs/receiver.sqlite")
	f.receiver = generationDB(t, f.receiverPath, false)
	if err := InitGenerationReceiver(ctx, f.receiver); err != nil {
		t.Fatal(err)
	}
	if err := InitGenerationSelections(ctx, f.receiver); err != nil {
		t.Fatal(err)
	}
	stageFixture(t, f, "")
	if err := f.receiver.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(f.receiverPath, 0600); err != nil {
		t.Fatal(err)
	}
	manifest, err := json.Marshal(map[string]any{"synthetic_only": true, "month": "2026-09", "group_id": 1,
		"expected_sha256": generationHash(expected.Body.Bytes()), "source_sha256": generationHash(source)})
	if err != nil {
		t.Fatal(err)
	}
	write("fixture-manifest.json", manifest)
	t.Log("exported only fresh synthetic data; no task submitted; no real source accepted")
}
