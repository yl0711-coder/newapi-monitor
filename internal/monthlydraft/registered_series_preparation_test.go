package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

type registeredPrefixFixture struct {
	f     displaySeriesFixture
	cfg   DeploymentConfig
	index string
}

func newRegisteredPrefixFixture(t *testing.T) registeredPrefixFixture {
	t.Helper()
	f := newDisplaySeriesFixtureFrom(t, time.August)
	root := f.g.root.Name()
	receiverPath := filepath.Join(root, "requested-only.db")
	db := generationDB(t, receiverPath, false)
	if err := InitGenerationReceiver(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if err := InitGenerationSelections(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(receiverPath, 0600); err != nil {
		t.Fatal(err)
	}
	// Only Aug/Sep ORIGINAL synthetic sources exist. A stale site-May
	// envelope must NOT force the customer to provide May-July records.
	index := "registered-prefix.json"
	scope := f.ranges[0]
	scope.From = "2026-05"
	if err := os.WriteFile(filepath.Join(root, index), materialPreparationJSON(t, AutomaticDisplaySeriesIndex{Version: 1, Ranges: []AutomaticDisplaySeriesRange{scope}}), 0600); err != nil {
		t.Fatal(err)
	}
	return registeredPrefixFixture{f: f, index: index, cfg: DeploymentConfig{Rollout: GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: "2026-09"}}}, ReceiverPath: receiverPath, FundingPath: f.fundingPath, AuthorityPath: f.authorityPath, EvidenceRoot: root}}
}

func (f registeredPrefixFixture) open(t *testing.T, before func(context.Context, SourcePreparationRequest) error) (*Deployment, *[]string, *sync.Mutex) {
	t.Helper()
	d, err := OpenRetainedAutomaticDisplaySeriesDeployment(t.Context(), f.cfg, "scope", f.index, displaySeriesPublisher(f.f.auth))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, d) })
	calls := []string{}
	mu := &sync.Mutex{}
	step := func(ctx context.Context, req SourcePreparationRequest) (bool, error) {
		mu.Lock()
		calls = append(calls, req.Month)
		mu.Unlock()
		if before != nil {
			if err := before(ctx, req); err != nil {
				return false, err
			}
		}
		for i, plan := range f.f.plans {
			if plan.Funding.Month == req.Month {
				// Existing full financial/source/CAS preparation, not a hand-set
				// "ready" flag or a synthetic customer task for the predecessor.
				result, err := d.PrepareMonthlyInput(ctx, materialPreparationJSON(t, plan), f.f.inputs[i], "", true)
				return result.Applied, err
			}
		}
		return false, ErrGenerationSelectionMissing
	}
	auth := d.runtime.service.(*usageHTTPService).authorization
	if err := d.runtime.EnableOnDemandPreparation(t.Context(), auth, func(context.Context, SourcePreparationRequest) error { return nil }, step); err != nil {
		t.Fatal(err)
	}
	// Use a normally validated source object. The original registration
	// timestamps here are explicit SYNTHETIC metadata, not live DB reads.
	_, _, _, wallet, _ := walletPreparationFixture(t)
	source, err := NewFundingSourcePreparation(wallet.funding.source, wallet.funding.capture, d.g, auth, f.f.ranges[0].SourceIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.EnableRegistrationCalendar(source); err != nil {
		t.Fatal(err)
	}
	d.runtime.service.(*registrationCalendarService).read = func(_ context.Context, ids []int64) ([]monthlysource.RegistrationTime, error) {
		rows := []monthlysource.RegistrationTime{}
		for _, id := range ids {
			rows = append(rows, monthlysource.RegistrationTime{UserID: id, At: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC).Unix()})
		}
		return rows, nil
	}
	if err := d.EnableRegisteredSeriesPreparation(); err != nil {
		t.Fatal(err)
	}
	d.runtime.worker.pollMin = 5 * time.Millisecond
	d.runtime.worker.pollMax = 20 * time.Millisecond
	return d, &calls, mu
}

func waitRegisteredPrefixBill(t *testing.T, f registeredPrefixFixture, d *Deployment) monitor.PortalMonthlyBill {
	return waitRegisteredPrefixBillMonth(t, f, d, "2026-09")
}

func waitRegisteredPrefixBillMonth(t *testing.T, f registeredPrefixFixture, d *Deployment, month string) monitor.PortalMonthlyBill {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var record monitor.PortalMonthlyBill
		if err := f.f.auth.First(&record, "group_id=1 AND month=?", month).Error; err == nil {
			return record
		}
		time.Sleep(20 * time.Millisecond)
	}
	var state, code string
	_ = d.receiver.QueryRow(`SELECT state,failure_code FROM portal_monthly_source_preparation LIMIT 1`).Scan(&state, &code)
	t.Fatal("requested bill did not save", state, code, d.runtime.worker.Status())
	return monitor.PortalMonthlyBill{}
}

func assertRegisteredPrefixOnlyRequestedPublished(t *testing.T, f registeredPrefixFixture, d *Deployment, record monitor.PortalMonthlyBill) {
	t.Helper()
	displaySeriesCount(t, f.f, 1)
	generationAdmissionCounts(t, d.receiver, 1, 2)
	var preparations, publications int
	if err := d.receiver.QueryRow(`SELECT (SELECT count(*) FROM portal_monthly_source_preparation),(SELECT count(*) FROM portal_monthly_publication_jobs)`).Scan(&preparations, &publications); err != nil || preparations != 1 || publications != 1 {
		t.Fatal("predecessor became a customer job", preparations, publications, err)
	}
	for _, month := range []string{"2026-05", "2026-06", "2026-07"} {
		s, err := ReadGenerationSelection(t.Context(), d.receiver, 1, month)
		if err != nil || s.Head != "" {
			t.Fatal("pre-registration history requested", month, err)
		}
	}
	var payload struct {
		Display struct {
			Organization struct{ Opening, Closing string } `json:"organization"`
		} `json:"display_balances"`
	}
	if json.Unmarshal([]byte(record.Payload), &payload) != nil || payload.Display.Organization.Opening != "2325.710000" || payload.Display.Organization.Closing != "2325.680000" {
		t.Fatal("wrong fixed registered-origin cents", payload)
	}
	var evidence monitor.MonthlyBillEvidence
	if json.Unmarshal([]byte(record.Evidence), &evidence) != nil || len(evidence.Users) != 2 {
		t.Fatal("missing independent user evidence")
	}
	if evidence.Users[0].ConsumeQuota != 2500 || evidence.Users[1].ConsumeQuota != 10000 {
		t.Fatal("raw consumption was changed")
	}
}

func TestRegisteredPrefixLatestMonthFirstPreparesOnlyNeededSourcesAndSurvivesRestart(t *testing.T) {
	f := newRegisteredPrefixFixture(t)
	d, calls, mu := f.open(t, nil)
	request, own, foreign := capturedHistoryHTTP(t, capturedHistoryFixture{cfg: f.cfg, auth: f.f.auth}, d)
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	calendar := request("GET", "/api/monthly-statement/calendar", "", own)
	first := request("GET", "/api/monthly-statement/generation?month=2026-09", "", own)
	if calendar.Code != 200 || first.Code != 200 {
		t.Fatal("registered customer could not read first state", calendar.Code, first.Code)
	}
	method, path, body := capturedHistoryFirstButtonRequest(t, first.Body.Bytes(), calendar.Body.Bytes())
	if out := request(method, path, body, own); out.Code != 202 {
		t.Fatal("actual first click did not admit target", out.Code)
	}
	record := waitRegisteredPrefixBill(t, f, d)
	stopRuntime(t, d.runtime)
	mu.Lock()
	got := append([]string(nil), (*calls)...)
	mu.Unlock()
	if !reflect.DeepEqual(got, []string{"2026-08", "2026-09"}) {
		t.Fatal("wrong missing-source order", got)
	}
	assertRegisteredPrefixOnlyRequestedPublished(t, f, d, record)
	page := request("GET", "/api/monthly-statement?month=2026-09", "", own)
	if page.Code != 200 {
		t.Fatal("saved requested bill unavailable", page.Code)
	}
	var view struct {
		Data struct {
			Publication string `json:"publication"`
		} `json:"data"`
	}
	if json.Unmarshal(page.Body.Bytes(), &view) != nil || view.Data.Publication == "" {
		t.Fatal("missing saved bill version")
	}
	book := request("GET", "/api/monthly-statement/download?month=2026-09&publication="+view.Data.Publication, "", own)
	if book.Code != 200 || !bytes.HasPrefix(book.Body.Bytes(), []byte("PK")) {
		t.Fatal("saved requested bill download unavailable", book.Code)
	}
	if out := request("GET", "/api/monthly-statement?month=2026-09", "", foreign); out.Code == 200 {
		t.Fatal("foreign organization accessed bill")
	}
	closeDeploymentTest(t, d)
	reopened, newCalls, newMu := f.open(t, nil)
	if err := reopened.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.SubmitMonthly(t.Context(), 1, 1, "2026-09"); err != nil {
		t.Fatal(err)
	}
	closeDeploymentTest(t, reopened)
	newMu.Lock()
	n := len(*newCalls)
	newMu.Unlock()
	if n != 0 {
		t.Fatal("restart prepared already committed sources")
	}
	var after monitor.PortalMonthlyBill
	if err := f.f.auth.First(&after, "group_id=1 AND month='2026-09'").Error; err != nil || after != record {
		t.Fatal("restart changed saved statement", err)
	}
	pageAfter := request("GET", "/api/monthly-statement?month=2026-09", "", own)
	bookAfter := request("GET", "/api/monthly-statement/download?month=2026-09&publication="+view.Data.Publication, "", own)
	if pageAfter.Code != 200 || bookAfter.Code != 200 || !bytes.Equal(page.Body.Bytes(), pageAfter.Body.Bytes()) || !bytes.Equal(book.Body.Bytes(), bookAfter.Body.Bytes()) {
		t.Fatal("saved customer page/download changed after restart")
	}
}

func TestRegisteredPrefixSelectedTargetStillQueuesMissingPredecessor(t *testing.T) {
	f := newRegisteredPrefixFixture(t)
	d, calls, mu := f.open(t, nil)
	if _, err := StageGenerationVersion(t.Context(), d.receiver, f.f.plans[1], f.f.inputs[1], ""); err != nil {
		t.Fatal(err)
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SubmitMonthly(t.Context(), 1, 1, "2026-09"); err != nil {
		t.Fatal(err)
	}
	record := waitRegisteredPrefixBill(t, f, d)
	stopRuntime(t, d.runtime)
	mu.Lock()
	got := append([]string(nil), (*calls)...)
	mu.Unlock()
	if !reflect.DeepEqual(got, []string{"2026-08"}) {
		t.Fatal("selected target bypassed dependency preparation", got)
	}
	assertRegisteredPrefixOnlyRequestedPublished(t, f, d, record)
	closeDeploymentTest(t, d)
}

func TestRegisteredPrefixWithdrawnDependencyIsNeverReactivated(t *testing.T) {
	f := newRegisteredPrefixFixture(t)
	d, calls, mu := f.open(t, nil)
	s, err := StageGenerationVersion(t.Context(), d.receiver, f.f.plans[0], f.f.inputs[0], "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WithdrawGenerationSelection(t.Context(), d.receiver, 1, "2026-08", s.Head); err != nil {
		t.Fatal(err)
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, err = d.SubmitMonthly(t.Context(), 1, 1, "2026-09")
	if err != nil && !errors.Is(err, monitor.MonthlyGenerationFailure("state_changed")) {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var state string
		_ = d.receiver.QueryRow(`SELECT state FROM portal_monthly_source_preparation LIMIT 1`).Scan(&state)
		if state == "failed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	closeDeploymentTest(t, d)
	mu.Lock()
	n := len(*calls)
	mu.Unlock()
	if n != 0 {
		t.Fatal("withdrawn prefix invoked source work")
	}
	displaySeriesCount(t, f.f, 0)
}

func TestRegisteredPrefixStopResumesFromCommittedDependencyWithoutResettingFailureBudget(t *testing.T) {
	f := newRegisteredPrefixFixture(t)
	entered := make(chan struct{})
	d, _, _ := f.open(t, func(ctx context.Context, req SourcePreparationRequest) error {
		if req.Month == "2026-09" {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SubmitMonthly(t.Context(), 1, 1, "2026-09"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(15 * time.Second):
		t.Fatal("did not reach second dependency")
	}
	closeDeploymentTest(t, d)
	displaySeriesCount(t, f.f, 0)
	reopened, calls, mu := f.open(t, nil)
	// Virtual clock only for the ORIGINAL receiver's expired-lease policy;
	// no timestamps or money in retained evidence are changed or fabricated.
	reopened.runtime.preparation.now = func() time.Time { return time.Now().Add(65 * time.Second) }
	if err := reopened.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	record := waitRegisteredPrefixBill(t, f, reopened)
	stopRuntime(t, reopened.runtime)
	mu.Lock()
	got := append([]string(nil), (*calls)...)
	mu.Unlock()
	if !reflect.DeepEqual(got, []string{"2026-09"}) {
		t.Fatal("restart repeated completed dependency", got)
	}
	var failures int
	if err := reopened.receiver.QueryRow(`SELECT failures FROM portal_monthly_source_preparation LIMIT 1`).Scan(&failures); err != nil || failures != 1 {
		t.Fatal("restart reset original failure budget", failures, err)
	}
	assertRegisteredPrefixOnlyRequestedPublished(t, f, reopened, record)
}

func TestRegisteredPrefixInternalGrantCannotBroadenCustomerOrOtherRuntimeScope(t *testing.T) {
	f := newRegisteredPrefixFixture(t)
	d, _, _ := f.open(t, nil)
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SubmitMonthly(t.Context(), 1, 1, "2026-08"); err == nil {
		t.Fatal("internal preparation expanded customer rollout")
	}
	plan := f.f.plans[0]
	if _, err := d.PrepareMonthlyInput(t.Context(), materialPreparationJSON(t, plan), f.f.inputs[0], "", true); err == nil {
		t.Fatal("dependency allowed without private capability")
	}
	for _, scenario := range []string{"runtime", "owner", "month", "target", "from"} {
		grant := dependencyPreparationGrant{runtime: d.runtime, owner: plan.Owner, target: "2026-09", from: "2026-08", month: "2026-08"}
		switch scenario {
		case "runtime":
			grant.runtime = &LocalRuntime{}
		case "owner":
			grant.owner.AuthVersion++
		case "month":
			grant.month = "2026-07"
		case "target":
			grant.target = "2026-10"
		case "from":
			grant.from = "2026-09"
		}
		ctx := context.WithValue(t.Context(), dependencyPreparationContextKey{}, grant)
		if _, err := d.PrepareMonthlyInput(ctx, materialPreparationJSON(t, plan), f.f.inputs[0], "", true); err == nil {
			t.Fatal("invalid dependency capability accepted", scenario)
		}
	}
	stopRuntime(t, d.runtime)
	generationAdmissionCounts(t, d.receiver, 0, 0)
}

func TestRegisteredPrefixPreviouslyDraftedTargetOffersPreparationWithoutResettingPublication(t *testing.T) {
	f := newRegisteredPrefixFixture(t)
	d, _, _ := f.open(t, nil)
	if _, err := StageGenerationVersion(t.Context(), d.receiver, f.f.plans[1], f.f.inputs[1], ""); err != nil {
		t.Fatal(err)
	}
	task, err := d.g.SubmitUsageMonth(t.Context(), d.runtime.preparation.auth, 1, 1, "2026-09")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.g.Run(t.Context(), task.Request.Owner, task.ID); err != nil {
		t.Fatal(err)
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	view, err := d.ReadMonthly(t.Context(), 1, 1, "2026-09")
	if err != nil || view.State != "not_requested" {
		t.Fatal("drafted target hid missing-source first click", view, err)
	}
	if _, err := d.SubmitMonthly(t.Context(), 1, 1, "2026-09"); err != nil {
		t.Fatal(err)
	}
	record := waitRegisteredPrefixBill(t, f, d)
	stopRuntime(t, d.runtime)
	assertRegisteredPrefixOnlyRequestedPublished(t, f, d, record)
}

func TestRegisteredPrefixPausedPublicationCannotBeReactivatedByCustomer(t *testing.T) {
	f := newRegisteredPrefixFixture(t)
	d, calls, mu := f.open(t, nil)
	if _, err := StageGenerationVersion(t.Context(), d.receiver, f.f.plans[1], f.f.inputs[1], ""); err != nil {
		t.Fatal(err)
	}
	task, err := d.g.SubmitUsageMonth(t.Context(), d.runtime.preparation.auth, 1, 1, "2026-09")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.g.Run(t.Context(), task.Request.Owner, task.ID); err != nil {
		t.Fatal(err)
	}
	task, err = d.g.tasks.Find(t.Context(), task.Request)
	if err != nil {
		t.Fatal(err)
	}
	lease, claimed, err := d.runtime.worker.publications.claim(t.Context(), task)
	if err != nil || !claimed {
		t.Fatal(claimed, err)
	}
	if err := d.runtime.worker.publications.finish(t.Context(), lease, ErrGenerationEvidence); err != nil {
		t.Fatal(err)
	}
	before, err := d.runtime.worker.publications.read(t.Context(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		view, err := d.SubmitMonthly(t.Context(), 1, 1, "2026-09")
		if err != nil || view.State != "needs_review" {
			t.Fatal("paused publication was offered fresh generation", view, err)
		}
	}
	stopRuntime(t, d.runtime)
	after, err := d.runtime.worker.publications.read(t.Context(), task.ID)
	if err != nil || after != before {
		t.Fatal("customer reset publication state", err)
	}
	var preparations int
	if err := d.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_source_preparation`).Scan(&preparations); err != nil || preparations != 0 {
		t.Fatal("paused publication queued new sources", preparations, err)
	}
	mu.Lock()
	n := len(*calls)
	mu.Unlock()
	if n != 0 {
		t.Fatal("paused publication resumed source work")
	}
	displaySeriesCount(t, f.f, 0)
}

func TestRegisteredPrefixMissingSourcesAndFalseReadyNeverPublishOrSpin(t *testing.T) {
	for _, mode := range []string{"missing_source", "false_ready"} {
		t.Run(mode, func(t *testing.T) {
			f := newRegisteredPrefixFixture(t)
			d, _, _ := f.open(t, nil)
			var mu sync.Mutex
			calls := 0
			d.runtime.preparation.dependencies.step = func(context.Context, SourcePreparationRequest) (bool, error) {
				mu.Lock()
				calls++
				mu.Unlock()
				if mode == "false_ready" {
					return true, nil
				}
				return false, ErrGenerationSelectionMissing
			}
			if err := d.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := d.SubmitMonthly(t.Context(), 1, 1, "2026-09"); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			var state string
			for time.Now().Before(deadline) {
				_ = d.receiver.QueryRow(`SELECT state FROM portal_monthly_source_preparation LIMIT 1`).Scan(&state)
				if state == "failed" {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			stopRuntime(t, d.runtime)
			mu.Lock()
			n := calls
			mu.Unlock()
			if state != "failed" || n != 1 {
				t.Fatal("invalid source readiness retried or succeeded", state, n)
			}
			generationAdmissionCounts(t, d.receiver, 0, 0)
			displaySeriesCount(t, f.f, 0)
		})
	}
}

func TestRegisteredPrefixChangedRegistrationCannotRebasePublishedMoney(t *testing.T) {
	f := newRegisteredPrefixFixture(t)
	f.cfg.Rollout.Targets = append(f.cfg.Rollout.Targets, GenerationTarget{GroupID: 1, Month: "2026-08"})
	d, _, _ := f.open(t, nil)
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SubmitMonthly(t.Context(), 1, 1, "2026-08"); err != nil {
		t.Fatal(err)
	}
	issued := waitRegisteredPrefixBillMonth(t, f, d, "2026-08")
	closeDeploymentTest(t, d)
	reopened, calls, mu := f.open(t, nil)
	// Simulate corrected/mismatched source metadata on a new process, never
	// mutate a live service/cache or alter the retained original ledger.
	reopened.runtime.service.(*registrationCalendarService).read = func(_ context.Context, ids []int64) ([]monthlysource.RegistrationTime, error) {
		rows := []monthlysource.RegistrationTime{}
		for _, id := range ids {
			rows = append(rows, monthlysource.RegistrationTime{UserID: id, At: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()})
		}
		return rows, nil
	}
	if err := reopened.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.SubmitMonthly(t.Context(), 1, 1, "2026-09"); err != nil {
		t.Fatal(err)
	}
	var state string
	key := (SourcePreparationRequest{Owner: f.f.plans[1].Owner, Month: "2026-09"}).Key()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_ = reopened.receiver.QueryRow(`SELECT state FROM portal_monthly_source_preparation WHERE key=?`, key).Scan(&state)
		if state == "failed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopRuntime(t, reopened.runtime)
	if state != "failed" {
		t.Fatal("origin conflict did not stop requested preparation", state)
	}
	mu.Lock()
	n := len(*calls)
	mu.Unlock()
	if n != 0 {
		t.Fatal("origin conflict continued source preparation")
	}
	var after monitor.PortalMonthlyBill
	if err := f.f.auth.First(&after, "group_id=1 AND month='2026-08'").Error; err != nil || after != issued {
		t.Fatal("changed registration rewrote published money", err)
	}
	displaySeriesCount(t, f.f, 1)
}
