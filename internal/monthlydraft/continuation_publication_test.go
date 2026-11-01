package monthlydraft

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Transport replacement only. Real collectors, original slots, scope checks,
// normalization, queue, financial checks and portal publication are unchanged.
type continuationSourceDriver struct{ base *sourcePreparationDriver }
type continuationSourceConn struct{ *sourcePreparationConn }

func (d *continuationSourceDriver) Open(string) (driver.Conn, error) {
	return &continuationSourceConn{&sourcePreparationConn{d: d.base}}, nil
}
func (c *continuationSourceConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(q, "FROM users WHERE id IN") {
		return (&walletPreparationConn{c.sourcePreparationConn}).QueryContext(ctx, q, args)
	}
	return (&tokenPreparationConn{c.sourcePreparationConn}).QueryContext(ctx, q, args)
}

func TestContinuationRealCollectorToSavedCustomerBillAndDownloadKeepsGoldenMoney(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	addCapturedHistoryReconstruction(t, &f)
	base := f.wallet.funding.source.Driver().(*walletPreparationDriver).base
	source := sql.OpenDB(capturedPreparationTestConnector{&continuationSourceDriver{base}})
	source.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = source.Close() })
	f.wallet.funding.source = source
	// Manufacture a synthetic INTERRUPTED capture by omitting the last empty
	// window. No user or retained real proof is modified. Reinstate the guard
	// before calling any product code.
	if _, err := f.wallet.funding.capture.Exec(`DROP TRIGGER monthly_token_capture_no_delete; DELETE FROM portal_monthly_token_capture_slots WHERE ordinal=(SELECT max(ordinal) FROM portal_monthly_token_capture_slots)`); err != nil {
		t.Fatal(err)
	}
	if err := monthlysource.InitTokenMonthCaptureStore(t.Context(), f.wallet.funding.capture); err != nil {
		t.Fatal(err)
	}
	d := f.open(t)
	cfg, _ := capturedPreparationConfigFixture(t, f)
	cfg.AccountTokenWork = true
	raw := materialPreparationJSON(t, cfg)
	ref := monthlysource.MonthlyObservationFile{Path: f.cfg.EvidenceRoot + "/continuation-preparation.json", SHA256: generationHash(raw)}
	if err := os.WriteFile(ref.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	p := d.runtime.preparation
	if _, err := p.SubmitMonthly(t.Context(), 1, 1, f.req.Month); err != nil {
		t.Fatal(err)
	}
	if _, err := f.f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET steps=4096`); err != nil {
		t.Fatal(err)
	}
	if err := p.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := d.EnableSourcePreparationContinuations(); err != nil {
		t.Fatal(err)
	}
	plan, err := d.PreviewSourcePreparationContinuation(t.Context(), 1, 1, f.req.Month)
	if err != nil || plan.Observed != 119 {
		t.Fatal("original checkpoint missing", plan.Observed, err)
	}
	if _, err := d.ContinueSourcePreparation(t.Context(), plan); err != nil {
		t.Fatal(err)
	}
	// Complete the missing original, then actually close/reopen the resource
	// owner BEFORE normalization/publication. Reopening may not renew the grant
	// or recapture the now-complete observation.
	if err := p.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	beforeRestart, err := readPreparationContinuation(t.Context(), f.f.receiver, f.req.Key())
	if err != nil || beforeRestart.claims != 1 || beforeRestart.exempt != 1 {
		t.Fatal("missing work did not checkpoint", beforeRestart, err)
	}
	closeDeploymentTest(t, d)
	d = f.open(t)
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	if err := d.EnableSourcePreparationContinuations(); err != nil {
		t.Fatal(err)
	}
	afterRestart, err := readPreparationContinuation(t.Context(), f.f.receiver, f.req.Key())
	if err != nil || afterRestart != beforeRestart {
		t.Fatal("restart renewed continuation", err)
	}
	request, own, foreign := capturedHistoryHTTP(t, f, d)
	d.runtime.worker.pollMin = 5 * time.Millisecond
	d.runtime.worker.pollMax = 20 * time.Millisecond
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if out := request("POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`, own); out.Code != 202 {
		t.Fatal(out.Code, out.Body.String())
	}
	deadline := time.Now().Add(25 * time.Second)
	var saved monitor.PortalMonthlyBill
	for {
		err = f.auth.First(&saved, "group_id=1 AND month=?", f.req.Month).Error
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			state, steps, failures := preparationState(t, f.f.receiver)
			t.Fatal("continuation did not publish", state, steps, failures, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	closeDeploymentTest(t, d)
	var evidence monitor.MonthlyBillEvidence
	if json.Unmarshal([]byte(saved.Evidence), &evidence) != nil || len(evidence.Users) != 2 {
		t.Fatal("invalid saved evidence")
	}
	for _, u := range evidence.Users {
		if u.OpeningQuota != 500000000 || u.ClosingQuota != 499000000 || u.ConsumeQuota != 1500000 || u.RefundQuota != 500000 || u.TokenUsage == nil || len(u.TokenUsage.Rows) != 3 {
			t.Fatal("continuation altered money", u.UserID)
		}
	}
	observed, credited := workCounts(t, f.f)
	if observed != 120 || credited != 1 {
		t.Fatal("missing work recaptured", observed, credited)
	}
	state, steps, failures := preparationState(t, f.f.receiver)
	if state != "submitted" || steps != 4096 || failures != 0 {
		t.Fatal("original budget changed", state, steps, failures)
	}
	beforePage, beforeBook := assertCapturedHistoryCustomerDownload(t, request, own, foreign)
	if len(beforePage) == 0 || len(beforeBook) == 0 {
		t.Fatal("empty output")
	}
	reopened := f.open(t)
	if worked, err := reopened.runtime.worker.step(t.Context()); err != nil || worked {
		t.Fatal("reissued saved bill", worked, err)
	}
	var again monitor.PortalMonthlyBill
	if err := f.auth.First(&again, "group_id=1 AND month=?", f.req.Month).Error; err != nil || again != saved {
		t.Fatal("restart changed customer money", err)
	}
	if _, err := reopened.ContinueSourcePreparation(t.Context(), plan); err == nil {
		t.Fatal("default reopened owner activated grant", err)
	}
}
