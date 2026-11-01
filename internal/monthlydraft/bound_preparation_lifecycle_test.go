package monthlydraft

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestBoundResumeLostIntentWithReleaseReceiptIsQuarantined(t *testing.T) {
	x := newBoundResumeFixture(t)
	p := previewBound(t, x)
	x.now = time.UnixMilli(p.EarliestMS)
	if r, err := x.d.ResumeBoundSourcePreparation(t.Context(), p); err != nil || !r.Applied {
		t.Fatal(r, err)
	}
	base := x.f.wallet.funding.source.Driver().(*walletPreparationDriver).base
	queries := base.queries.Load()
	// SETUP ONLY: model mismatched backup restoration. The real maintenance
	// path never drops immutability guards or deletes an intent.
	if _, err := x.f.f.receiver.Exec(`DROP TRIGGER bound_resume_no_delete; DELETE FROM portal_monthly_bound_resume_intents; CREATE TRIGGER bound_resume_no_delete BEFORE DELETE ON portal_monthly_bound_resume_intents BEGIN SELECT RAISE(ABORT,'bound recovery intents are immutable'); END`); err != nil {
		t.Fatal(err)
	}
	if err := x.d.runtime.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, steps, _ := preparationState(t, x.f.f.receiver)
	if state != "failed" || steps != 3 || base.queries.Load() != queries {
		t.Fatal("orphan release ran source", state, steps)
	}
	var code string
	if err := x.f.f.receiver.QueryRow(`SELECT failure_code FROM portal_monthly_source_preparation`).Scan(&code); err != nil || code != "invalid_descriptor" {
		t.Fatal(code, err)
	}
	if resumeReceiptCount(t, x.f.f.receiver) != 1 || rootResumeCount(t, x) != 1 {
		t.Fatal("audit lost or reset")
	}
	// Quarantining this one restored job does not starve another authorized
	// organization at the queue boundary. No financial producer runs here.
	other, _, err := LoadUsageGenerationOwner(t.Context(), x.f.auth, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	req := SourcePreparationRequest{Owner: other, Month: p.Queue.Request.Month}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := x.f.f.receiver.Exec(`INSERT INTO portal_monthly_source_preparation(key,group_id,payload,state,created_at,next_at) VALUES(?,2,?,'queued',?,?)`, req.Key(), raw, x.now.UnixMilli(), x.now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	job, err := x.d.runtime.preparation.claim(t.Context())
	if err != nil || job.key != req.Key() || job.request != req || base.queries.Load() != queries {
		t.Fatal("quarantine blocked unrelated queue work", job.key, err)
	}
}

func TestBoundResumeStopDrainsSourceAndRestartKeepsBudgetsAndSavedBill(t *testing.T) {
	x := newBoundResumeFixture(t)
	p := previewBound(t, x)
	x.now = time.UnixMilli(p.EarliestMS)
	if r, err := x.d.ResumeBoundSourcePreparation(t.Context(), p); err != nil || !r.Applied {
		t.Fatal(r, err)
	}
	base := x.f.wallet.funding.source.Driver().(*walletPreparationDriver).base
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	base.onQuery = func() error { enteredOnce.Do(func() { close(entered) }); <-release; return context.Canceled }
	x.d.runtime.preparation.now = time.Now
	before, err := failedPreparationSnapshot(t.Context(), x.f.f.receiver, p.Queue.Request)
	if err != nil {
		t.Fatal(err)
	}
	if err := x.d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("source was never entered")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	err = x.d.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || x.d.closed {
		t.Fatal("close succeeded with active source", err)
	}
	if err := x.d.receiver.PingContext(t.Context()); err != nil {
		t.Fatal("active receiver closed", err)
	}
	if x.d.capturedPreparation.source.Stats().InUse != 1 {
		t.Fatal("active source not retained")
	}
	if _, err := OpenDeployment(t.Context(), x.f.cfg); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("close timeout released owner lock", err)
	}
	if _, err := x.d.SubmitMonthly(t.Context(), 1, 1, p.Queue.Request.Month); err == nil {
		t.Fatal("stopping owner admitted work")
	}
	unblock()
	closeDeploymentTest(t, x.d)
	base.onQuery = nil // prior producer drained; no concurrent field mutation
	var count int64
	if err := x.f.auth.Model(&monitor.PortalMonthlyBill{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("stopped source published a partial bill", count, err)
	}
	after, err := failedPreparationSnapshot(t.Context(), x.f.f.receiver, p.Queue.Request)
	if err != nil || after.CreatedMS != before.CreatedMS || after.Steps != before.Steps+1 || after.Failures != 0 || after.State != "running" || after.Lease == "" {
		t.Fatal("stopped claim lost lease/budget", after, err)
	}
	x.d = openBoundFixture(t, x.f)
	// Only the fixture clock advances past the original 60s lease. This does
	// not reset the lease/fault row or relax production cooldowns.
	x.d.runtime.preparation.now = func() time.Time { return time.Now().Add(61 * time.Second) }
	request, own, foreign := capturedHistoryHTTP(t, x.f, x.d)
	if err := x.d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(25 * time.Second)
	var record monitor.PortalMonthlyBill
	for {
		err := x.f.auth.First(&record, "group_id=? AND month=?", 1, p.Queue.Request.Month).Error
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			state, steps, failures := preparationState(t, x.f.f.receiver)
			t.Fatal("restarted source failed", state, steps, failures, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := x.d.runtime.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertCapturedHistoryCustomerDownload(t, request, own, foreign)
	var evidence monitor.MonthlyBillEvidence
	if json.Unmarshal([]byte(record.Evidence), &evidence) != nil || len(evidence.Users) != 2 {
		t.Fatal("invalid final evidence")
	}
	for _, u := range evidence.Users {
		if u.OpeningQuota != 500000000 || u.ClosingQuota != 499000000 || u.ConsumeQuota != 1500000 || u.RefundQuota != 500000 || u.TokenUsage == nil || len(u.TokenUsage.Rows) != 3 {
			t.Fatal("stop/restart changed money", u.UserID)
		}
	}
	after, err = failedPreparationSnapshot(t.Context(), x.f.f.receiver, p.Queue.Request)
	if err != nil || after.CreatedMS != before.CreatedMS || after.Steps <= before.Steps || after.Failures != 1 || after.State != "submitted" {
		t.Fatal("restart reset original work/error budgets", after, err)
	}
	if rootResumeCount(t, x) != 1 || resumeReceiptCount(t, x.f.f.receiver) != 1 {
		t.Fatal("restart granted new epochs")
	}
	closeDeploymentTest(t, x.d)
	assertCapturedHistoryCustomerDownload(t, request, own, foreign)
	var reread monitor.PortalMonthlyBill
	if err := x.f.auth.First(&reread, "group_id=? AND month=?", 1, p.Queue.Request.Month).Error; err != nil || reread != record {
		t.Fatal("stopping changed frozen bill", err)
	}
}
