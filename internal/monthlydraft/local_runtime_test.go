package monthlydraft

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func stopRuntime(t *testing.T, r *LocalRuntime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Stop(ctx); err != nil || r.Phase() != "stopped" {
		t.Fatal("runtime did not drain", err, r.Phase())
	}
}

func TestLocalRuntimeDisabledAndSingleUse(t *testing.T) {
	f := generationFixtureNew(t)
	_, ro, _ := usageAuthorityFixture(t)
	r, err := NewLocalRuntime(f.g, ro)
	if err != nil {
		t.Fatal(err)
	}
	if r.Phase() != "disabled" || r.worker.Status().Running {
		t.Fatal("constructor started execution")
	}
	for _, submit := range []bool{false, true} {
		if _, err := r.call(context.Background(), 1, 1, "2026-09", submit); err == nil {
			t.Fatal("disabled runtime admitted work")
		}
	}
	generationAdmissionCounts(t, f.receiver, 0, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Start(ctx); !errors.Is(err, context.Canceled) || r.Phase() != "disabled" {
		t.Fatal("cancelled start mutated runtime", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopRuntime(t, r) })
	if err := r.Start(context.Background()); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("duplicate start allowed", err)
	}
	stopRuntime(t, r)
	if err := r.Start(context.Background()); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("stopped runtime reopened")
	}
	other, err := NewLocalRuntime(f.g, ro)
	if err != nil {
		t.Fatal(err)
	}
	stopRuntime(t, other) // stopping before first start is also terminal
	if err := other.Start(context.Background()); err == nil {
		t.Fatal("pre-start stop ignored")
	}
}

func TestLocalRuntimeConcurrentStartStop(t *testing.T) {
	f := generationFixtureNew(t)
	_, ro, _ := usageAuthorityFixture(t)
	if _, err := NewLocalRuntime(nil, ro); err == nil {
		t.Fatal("nil generation accepted")
	}
	if _, err := NewLocalRuntime(f.g, nil); err == nil {
		t.Fatal("nil authority accepted")
	}
	for range 20 {
		r, err := NewLocalRuntime(f.g, ro)
		if err != nil {
			t.Fatal(err)
		}
		ready := make(chan struct{})
		started, stopped := make(chan error, 1), make(chan error, 1)
		go func() { <-ready; started <- r.Start(context.Background()) }()
		go func() { <-ready; stopped <- r.Stop(context.Background()) }()
		close(ready)
		if err := <-started; err != nil && !errors.Is(err, ErrGenerationBusy) {
			t.Fatal(err)
		}
		if err := <-stopped; err != nil {
			t.Fatal(err)
		}
		stopRuntime(t, r)
		if r.worker.Status().Running {
			t.Fatal("worker survived completed stop")
		}
	}
	generationAdmissionCounts(t, f.receiver, 0, 0)
}

// Test service intentionally delays cancellation to prove that Stop timeout
// does not pretend database users have drained. Never used by production.
type runtimeBlockedService struct {
	entered, release chan struct{}
	delegate         monitor.MonthlyGenerationService
	persistFirst     bool
}

func (s *runtimeBlockedService) SubmitMonthly(ctx context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	var v monitor.MonthlyGenerationView
	var err error
	if s.persistFirst {
		v, err = s.delegate.SubmitMonthly(ctx, gid, version, month)
	}
	close(s.entered)
	<-s.release
	return v, err
}

func (s *runtimeBlockedService) ReadMonthly(ctx context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	return s.SubmitMonthly(ctx, gid, version, month)
}

func TestLocalRuntimeStopWaitsForCallsAndCancelsOnParentExit(t *testing.T) {
	for _, submit := range []bool{false, true} {
		t.Run(map[bool]string{false: "read", true: "submit"}[submit], func(t *testing.T) {
			f := generationFixtureNew(t)
			_, ro, _ := usageAuthorityFixture(t)
			r, err := NewLocalRuntime(f.g, ro)
			if err != nil {
				t.Fatal(err)
			}
			blocked := &runtimeBlockedService{entered: make(chan struct{}), release: make(chan struct{})}
			r.service = blocked
			parent, cancel := context.WithCancel(context.Background())
			if err := r.Start(parent); err != nil {
				t.Fatal(err)
			}
			var once sync.Once
			release := func() { once.Do(func() { close(blocked.release) }) }
			t.Cleanup(func() { release(); cancel(); stopRuntime(t, r) })
			result := make(chan error, 1)
			go func() { _, err := r.call(context.Background(), 1, 1, "2026-09", submit); result <- err }()
			select {
			case <-blocked.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("request did not enter")
			}
			cancel()
			if r.Phase() != "stopping" {
				t.Fatal("cancelled parent still reported running", r.Phase())
			}
			ctx, end := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer end()
			if err := r.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) || r.Phase() != "stopping" {
				t.Fatal("timeout falsely claimed drained", err, r.Phase())
			}
			if _, err := r.ReadMonthly(context.Background(), 1, 1, "2026-09"); err == nil {
				t.Fatal("new request admitted after stop")
			}
			release()
			if err := <-result; err == nil {
				t.Fatal("cancelled response reported success")
			}
			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := r.Stop(context.Background()); err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()
			stopRuntime(t, r)
		})
	}
}

func TestLocalRuntimeStopDuringCalculationRetainsRetryState(t *testing.T) {
	f := generationFixtureNew(t)
	_, ro, _ := usageAuthorityFixture(t)
	job := generationSubmit(t, f)
	calculate := f.g.calculate
	entered := make(chan struct{})
	f.g.calculate = func(ctx context.Context, _ monthlysource.FundingMonthRequest, _ []monthlysource.FundingWindowObservation, _ []byte, _ time.Time) (Review, error) {
		close(entered)
		<-ctx.Done()
		return Review{}, ctx.Err()
	}
	r, err := NewLocalRuntime(f.g, ro)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopRuntime(t, r) })
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("calculation did not start")
	}
	stopRuntime(t, r)
	got, err := f.g.tasks.Get(context.Background(), job.Request.Owner, job.ID)
	if err != nil || got.State != monthlytasks.Failed || got.Attempts != 1 || got.RetryAtMS == 0 {
		t.Fatal("interrupted task lost bounded retry", got, err)
	}
	if candidates, err := f.g.tasks.WorkCandidates(context.Background()); err != nil || len(candidates) != 0 {
		t.Fatal("stop bypassed cooldown", err)
	}
	f.g.calculate = calculate // prior worker and requests have fully exited
	ageSyntheticFailure(t, f)
	resumed, err := NewLocalRuntime(f.g, ro)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopRuntime(t, resumed) })
	deadline := time.Now().Add(15 * time.Second)
	for {
		got, err := f.g.tasks.Get(context.Background(), job.Request.Owner, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.State == monthlytasks.DraftReady {
			break
		}
		if got.State == monthlytasks.Invalidated || (got.State == monthlytasks.Failed && got.Attempts > 1) || time.Now().After(deadline) {
			t.Fatal("resumed task did not complete", got.State, got.Attempts, got.FailureCode)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopRuntime(t, resumed)
	artifact, err := f.g.Run(context.Background(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact)
	got, err = f.g.tasks.Get(context.Background(), job.Request.Owner, job.ID)
	if err != nil || got.Attempts != 2 {
		t.Fatal("retry count reset", got, err)
	}
	generationAdmissionCounts(t, f.receiver, 1, 1)
}

func TestLocalRuntimeRollbackLeavesPublishedBillsUsable(t *testing.T) {
	a := newLocalGenerationAcceptance(t)
	owner, foreign := a.login(t, "owner@example.invalid"), a.login(t, "other@example.invalid")
	res := a.request(t, "POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`, owner)
	_ = res.Body.Close()
	if res.StatusCode != 202 {
		t.Fatal(res.StatusCode)
	}
	job, err := a.g.tasks.Find(context.Background(), generationRequest(a.f.plan, mustGenerationPlan(t, a.f)))
	if err != nil {
		t.Fatal(err)
	}
	waitLocalDraft(t, a.g, job)
	request := func(method, path, body string, cookies []*http.Cookie) *httptest.ResponseRecorder {
		res := a.request(t, method, path, body, cookies)
		defer res.Body.Close()
		w := httptest.NewRecorder()
		for k, v := range res.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(res.StatusCode)
		if _, err := io.Copy(w, res.Body); err != nil {
			t.Fatal(err)
		}
		return w
	}
	testGenerationReviewedPublication(t, a.g, a.f, job, a.auth, request, owner, foreign)
	readyJob, err := a.g.tasks.Get(context.Background(), job.Request.Owner, job.ID)
	if err != nil || readyJob.State != monthlytasks.DraftReady {
		t.Fatal("missing ready draft", err)
	}
	path := "/api/monthly-statement?month=2026-09"
	before := request("GET", path, "", owner)
	var bill struct {
		Data struct {
			Publication string `json:"publication"`
		} `json:"data"`
	}
	if err := json.Unmarshal(before.Body.Bytes(), &bill); err != nil || bill.Data.Publication == "" {
		t.Fatal("missing formal version", err)
	}
	downloadPath := "/api/monthly-statement/download?month=2026-09&publication=" + bill.Data.Publication
	beforeDownload := request("GET", downloadPath, "", owner)
	if beforeDownload.Code != 200 {
		t.Fatal("formal download failed", beforeDownload.Code)
	}
	stopRuntime(t, a.runtime)
	for _, method := range []string{"GET", "POST"} {
		p, body := "/api/monthly-statement/generation?month=2026-09", ""
		if method == "POST" {
			p, body = "/api/monthly-statement/generation", `{"month":"2026-09"}`
		}
		if w := request(method, p, body, owner); w.Code != 200 || !strings.Contains(w.Body.String(), `"state":"available"`) {
			t.Fatal("saved bill depended on running generation worker", w.Code)
		}
	}
	// Reuse is a read-only path, not reopening admission after shutdown.
	if w := request("POST", "/api/monthly-statement/generation", `{"month":"2026-08"}`, owner); w.Code != 503 {
		t.Fatal("stopped endpoint admitted new work", w.Code)
	}
	generationAdmissionCounts(t, a.f.receiver, 1, 1)
	if after := request("GET", path, "", owner); after.Code != 200 || after.Body.String() != before.Body.String() {
		t.Fatal("stop changed published money")
	}
	if after := request("GET", downloadPath, "", owner); after.Code != 200 || after.Body.String() != beforeDownload.Body.String() {
		t.Fatal("stop changed formal download", after.Code)
	}
	// Remove generation wiring, not the monthly reader or its tables. This
	// models feature-disable rollback, NOT an untested old binary/schema rollback.
	plain := gin.New()
	a.monitor.RegisterPortalRoutes(plain)
	plainRequest := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("X-Monthly-Statistics", "nullable-v1")
		req.Header.Set("X-Monthly-Display", "cents-chain-v1")
		for _, c := range owner {
			req.AddCookie(c)
		}
		w := httptest.NewRecorder()
		plain.ServeHTTP(w, req)
		return w
	}
	if w := plainRequest("/api/monthly-statement/generation?month=2026-09"); w.Code != 404 {
		t.Fatal("rollback retained generation route", w.Code)
	}
	if w := plainRequest(path); w.Code != 200 || w.Body.String() != before.Body.String() {
		t.Fatal("reader-only rollback changed bill", w.Code)
	}
	if w := plainRequest(downloadPath); w.Code != 200 || w.Body.String() != beforeDownload.Body.String() {
		t.Fatal("reader-only rollback changed download", w.Code)
	}
	if w := plainRequest("/monthly-generation-local"); w.Code != 404 {
		t.Fatal("rollback retained preview")
	}
	resumed, err := NewLocalRuntime(a.g, a.auth)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopRuntime(t, resumed) })
	v, err := resumed.SubmitMonthly(context.Background(), 1, 1, "2026-09")
	if err != nil || v.State != "pending_verification" {
		t.Fatal("resume lost ready task", v, err)
	}
	stopRuntime(t, resumed)
	generationAdmissionCounts(t, a.f.receiver, 1, 1)
	afterJob, err := a.g.tasks.Get(context.Background(), job.Request.Owner, job.ID)
	if err != nil || afterJob.Attempts != 1 || afterJob.ResultSHA256 != readyJob.ResultSHA256 {
		t.Fatal("resume recalculated ready task", afterJob, err)
	}
	source, err := os.ReadFile(a.f.path)
	if err != nil || generationHash(source) != a.f.sourceHash {
		t.Fatal("source changed", err)
	}
}

func TestLocalRuntimeStopAfterDurableAdmissionDoesNotDuplicate(t *testing.T) {
	a := newLocalGenerationAcceptance(t)
	blocked := &runtimeBlockedService{entered: make(chan struct{}), release: make(chan struct{}), delegate: a.runtime.service, persistFirst: true}
	a.runtime.service = blocked // no requests have started; worker does not read it
	var once sync.Once
	release := func() { once.Do(func() { close(blocked.release) }) }
	t.Cleanup(release)
	result := make(chan error, 1)
	go func() { _, err := a.runtime.SubmitMonthly(context.Background(), 1, 1, "2026-09"); result <- err }()
	select {
	case <-blocked.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("durable submission did not reach handoff")
	}
	job, err := a.g.tasks.Find(context.Background(), generationRequest(a.f.plan, mustGenerationPlan(t, a.f)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := a.runtime.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("in-flight admission not drained", err)
	}
	release()
	if err := <-result; err == nil {
		t.Fatal("stopping response was reported as successful")
	}
	stopRuntime(t, a.runtime)
	resumed, err := NewLocalRuntime(a.g, a.auth)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopRuntime(t, resumed) })
	if _, err := resumed.SubmitMonthly(context.Background(), 1, 1, "2026-09"); err != nil {
		t.Fatal(err)
	}
	stopRuntime(t, resumed)
	after, err := a.g.tasks.Find(context.Background(), job.Request)
	if err != nil || after.ID != job.ID {
		t.Fatal("admission response loss duplicated task", err)
	}
	generationAdmissionCounts(t, a.f.receiver, 1, 1)
}
