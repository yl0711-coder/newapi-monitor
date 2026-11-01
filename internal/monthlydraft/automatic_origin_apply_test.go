package monthlydraft

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func originApplyFixture(t *testing.T, late bool, residual ...int64) (capturedHistoryFixture, CapturedSourceFiles, monthlysource.MonthlyObservationFile) {
	t.Helper()
	quota := int64(5000)
	if len(residual) > 0 {
		quota = residual[0]
	}
	zone := time.FixedZone("CST", 28800)
	registrations := map[int64]int64{}
	first := "2026-09"
	if late {
		first = "2026-08"
		registrations[202] = time.Date(2026, 9, 1, 0, 0, 0, 0, zone).Unix()
	}
	scale := func(o *monthlysource.TokenUsageWindowObservation) {
		for i := range o.Totals {
			o.Totals[i].Quota *= 1000
			o.Totals[i].MinQuota *= 1000
			o.Totals[i].MaxQuota *= 1000
		}
		for i := range o.Rows {
			o.Rows[i].Quota *= 1000
			o.Rows[i].MinQuota *= 1000
			o.Rows[i].MaxQuota *= 1000
		}
	}
	f, files, ref := automaticHistoryOriginalFixtureOptions(t, func(o *monthlysource.WalletTailObservation, phase int) {
		sameSecondHistoryTail(o, phase)
		*o.User.BalanceQuota += -1000000000 + quota
		*o.User.UsedQuota += 999000000 - quota
	}, registrations, []func(*monthlysource.TokenUsageWindowObservation){scale}, first)
	contextDirectoryLabels(t, f)
	return f, files, ref
}

func originApplyCandidate(t *testing.T, f capturedHistoryFixture, ref monthlysource.MonthlyObservationFile) (automaticHistoryCandidate, monthlysource.MonthlyObservationFile) {
	t.Helper()
	policyPath := filepath.Join(f.f.root, "standing-origin-policy.json")
	if err := os.WriteFile(policyPath, automaticOriginPolicyBytes(), 0600); err != nil {
		t.Fatal(err)
	}
	policy := monthlysource.MonthlyObservationFile{Path: policyPath, SHA256: generationHash(automaticOriginPolicyBytes())}
	prepared, err := f.f.g.prepareAutomaticHistoryContextInputWithPolicy(t.Context(), f.auth, ref, f.req, &policy)
	if err != nil {
		t.Fatal("policy normalization", err)
	}
	dirPath := filepath.Join(f.f.root, "policy-directory.json")
	if err := os.WriteFile(dirPath, prepared.directory, 0600); err != nil {
		t.Fatal(err)
	}
	c, err := f.f.g.prepareAutomaticHistoryCandidate(t.Context(), prepared, monthlysource.MonthlyObservationFile{Path: dirPath, SHA256: generationHash(prepared.directory)})
	if err != nil {
		t.Fatal("policy candidate", err)
	}
	return c, policy
}

func TestAutomaticOriginApplyCandidatePreservesRawChargesAndSourceBalance(t *testing.T) {
	f, _, ref := originApplyFixture(t, false)
	c, _ := originApplyCandidate(t, f, ref)
	if c.plan.AutomaticHistoryPolicySourceID != automaticOriginPolicySourceID {
		t.Fatal("policy omitted from immutable plan")
	}
	for _, u := range c.bundle.Entries[0].Candidate.Users {
		if u.OpeningQuota != 0 || u.ClosingQuota != -999995000 || u.ConsumeQuota != 1500000000 || u.RefundQuota != 500000000 || u.HistoricalBalance == nil || u.BalanceCorrection == nil || u.BalanceCorrection.Quota != 5000 || u.HistoricalBalance.Recognition.RecognitionMonth != "2026-09" {
			t.Fatal("changed charges or failed one-time recognition", u.UserID)
		}
	}
	if _, err := monitor.PrepareAutomaticMonthlyBill(*c.bundle.Entries[0].Candidate, time.Now().Unix(), time.Now()); err != nil {
		t.Fatal("existing monetary policy rejected result", err)
	}
	if _, err := monitor.PrepareMonthlyCandidateBundle(c.input, c.metadata, time.Now()); err == nil {
		t.Fatal("plain JSON bypassed original-source verifier")
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticOriginApplyActualCustomerMonthOrderLateMemberAndRestart(t *testing.T) {
	f, _, _ := originApplyFixture(t, true)
	older := SourcePreparationRequest{Owner: f.req.Owner, Month: "2026-08"}
	seedHistoryEmptyMonth(t, f, older, false)
	f, d, cfg := configureOriginalRuntimeFixture(t, f)
	config := writeHistoryAssemblyConfig(t, f, cfg)
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", config, capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	request, own, foreign := capturedHistoryHTTP(t, f, d)
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if out := request("POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`, own); out.Code != 202 {
		t.Fatal(out.Code, out.Body.String())
	}
	sep := waitOriginalRuntimeBill(t, f, d)
	var e monitor.MonthlyBillEvidence
	if json.Unmarshal([]byte(sep.Evidence), &e) != nil || len(e.Users) != 2 {
		t.Fatal("invalid saved September")
	}
	for _, u := range e.Users {
		if u.OpeningQuota != 0 || u.ClosingQuota != -999995000 || u.ConsumeQuota != 1500000000 || u.RefundQuota != 500000000 || u.BalanceCorrection == nil || u.BalanceCorrection.Quota != 5000 || u.HistoricalBalance == nil {
			t.Fatal("September money differs from originals", u.UserID)
		}
	}
	if out := request("POST", "/api/monthly-statement/generation", `{"month":"2026-08"}`, own); out.Code != 202 {
		t.Fatal(out.Code, out.Body.String())
	}
	augF := f
	augF.req = older
	aug := waitOriginalRuntimeBill(t, augF, d)
	// Decode independent bills independently: omitted optional fields must not
	// retain pointers from the September evidence's reused slice elements.
	e = monitor.MonthlyBillEvidence{}
	if json.Unmarshal([]byte(aug.Evidence), &e) != nil || len(e.Users) != 2 {
		t.Fatal("invalid saved August")
	}
	for _, u := range e.Users {
		if u.OpeningQuota != 0 || u.ClosingQuota != 0 || u.ConsumeQuota != 0 || u.RefundQuota != 0 || u.BalanceCorrection != nil || u.HistoricalBalance != nil {
			t.Fatal("early month or pre-registration member inherited a correction", u.UserID)
		}
	}
	page := request("GET", "/api/monthly-statement?month=2026-09", "", own)
	var view struct {
		Data struct {
			Opening     string `json:"month_opening_balance"`
			Closing     string `json:"month_end_balance"`
			Publication string `json:"publication"`
		} `json:"data"`
	}
	if page.Code != 200 || json.Unmarshal(page.Body.Bytes(), &view) != nil || view.Data.Opening != "0.000000" || view.Data.Closing != "-3999.980000" {
		t.Fatal("customer projection mismatch", page.Code, page.Body.String())
	}
	book := request("GET", "/api/monthly-statement/download?month=2026-09&publication="+view.Data.Publication, "", own)
	if book.Code != 200 || book.Body.Len() == 0 {
		t.Fatal("download failed", book.Code)
	}
	z, err := zip.NewReader(bytes.NewReader(book.Body.Bytes()), int64(book.Body.Len()))
	if err != nil {
		t.Fatal(err)
	}
	verifiedSheets := 0
	for _, part := range z.File {
		if part.Name != "xl/worksheets/sheet1.xml" && part.Name != "xl/worksheets/sheet2.xml" {
			continue
		}
		stream, err := part.Open()
		if err != nil {
			t.Fatal(err)
		}
		xml, readErr := io.ReadAll(stream)
		closeErr := stream.Close()
		if readErr != nil || closeErr != nil {
			t.Fatal(readErr, closeErr)
		}
		values := []string{"6000.00", "-3999.98"}
		if strings.HasSuffix(part.Name, "sheet2.xml") {
			values = []string{"3000.00", "-1999.99"}
		}
		for _, value := range values {
			if !bytes.Contains(xml, []byte("<v>"+value+"</v>")) {
				t.Fatal("workbook money differs from original logs or customer page", part.Name, value)
			}
		}
		if bytes.Contains(xml, []byte(automaticOriginPolicySourceID)) || bytes.Contains(page.Body.Bytes(), []byte(automaticOriginPolicySourceID)) {
			t.Fatal("internal policy source exposed to customer")
		}
		verifiedSheets++
	}
	if verifiedSheets != 2 {
		t.Fatal("missing financial worksheets")
	}
	if other := request("GET", "/api/monthly-statement?month=2026-09", "", foreign); other.Code == 200 {
		t.Fatal("foreign bill exposed")
	}
	if d.capturedPreparation.funding.source.Driver().(*walletTailPreparationDriver).base.writes.Load() != 0 {
		t.Fatal("original-history generation attempted an upstream write")
	}
	closeDeploymentTest(t, d)
	// Frozen viewing/downloading survives restart without recapturing sources.
	reopenedF := f
	reopenedF.cfg.Rollout = GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}
	reopened := reopenedF.open(t)
	if err := reopened.enableCapturedSourcePreparation(t.Context(), "captured-scope", config, capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	read, session, _ := capturedHistoryHTTP(t, f, reopened)
	if worked, err := reopened.runtime.worker.step(t.Context()); err != nil || worked {
		t.Fatal("restart repeated recognition", worked, err)
	}
	after := read("GET", "/api/monthly-statement?month=2026-09", "", session)
	afterBook := read("GET", "/api/monthly-statement/download?month=2026-09&publication="+view.Data.Publication, "", session)
	if !bytes.Equal(page.Body.Bytes(), after.Body.Bytes()) || !bytes.Equal(book.Body.Bytes(), afterBook.Body.Bytes()) || reopened.capturedPreparation.source.Stats().OpenConnections != 0 {
		t.Fatal("restart changed bill or read upstream")
	}
}

func TestAutomaticOriginApplyRejectsChangedPolicyAndStrippedBinding(t *testing.T) {
	f, _, ref := originApplyFixture(t, false)
	c, policy := originApplyCandidate(t, f, ref)
	input, err := monitor.DecodeMonthlyPreparationInput(c.input)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"baseline", "consumption"} {
		var changed monitor.MonthlyPreparationInput
		if json.Unmarshal(c.input, &changed) != nil {
			t.Fatal("invalid test input")
		}
		if field == "baseline" {
			changed.Accounts[0].Baseline.BalanceQuota++
		} else {
			changed.Accounts[0].Spans[0].ConsumeQuota++
		}
		raw := materialPreparationJSON(t, changed)
		p := c.plan
		p.PreparationSHA256 = generationHash(raw)
		if _, err := f.f.g.auditAutomaticHistoryScope(t.Context(), p, changed); err == nil {
			t.Fatal("rehashed financial edit bypassed source audit", field)
		}
	}
	for _, badID := range []string{"", "other-policy"} {
		p := c.plan
		p.AutomaticHistoryPolicySourceID = badID
		if _, _, _, err := canonicalGeneration(p, c.input, time.Now()); err == nil {
			t.Fatal("policy binding stripped/renamed")
		}
	}
	if err := os.WriteFile(policy.Path, append(automaticOriginPolicyBytes(), ' '), 0600); err != nil {
		t.Fatal(err)
	}
	policy.SHA256 = generationHash(append(automaticOriginPolicyBytes(), ' '))
	if _, err := f.f.g.auditAutomaticHistoryForPlan(t.Context(), ref, f.req, &policy); err == nil {
		t.Fatal("rehashed policy edit was accepted")
	}
	if len(input.Accounts) != 2 {
		t.Fatal("partial organization")
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticOriginApplyCannotUsePolicyForUnsupportedResidual(t *testing.T) {
	for name, amount := range map[string]int64{"opposite_direction": -5000, "relative_limit": 100001, "absolute_limit": 2500001, "zero_has_no_treatment": 0} {
		t.Run(name, func(t *testing.T) {
			f, _, ref := originApplyFixture(t, false, amount)
			path := filepath.Join(f.f.root, "standing-policy-unsupported.json")
			if err := os.WriteFile(path, automaticOriginPolicyBytes(), 0600); err != nil {
				t.Fatal(err)
			}
			policy := monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(automaticOriginPolicyBytes())}
			if _, err := f.f.g.auditAutomaticHistoryForPlan(t.Context(), ref, f.req, &policy); err == nil {
				t.Fatal("standing policy admitted an unsupported adjustment", name)
			}
			generationAdmissionCounts(t, f.f.receiver, 0, 0)
		})
	}
}
