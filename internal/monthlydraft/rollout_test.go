package monthlydraft

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestGenerationRolloutValidationAndDisabledDefaults(t *testing.T) {
	if r, err := NewControlledLocalRuntime(nil, nil, GenerationRollout{}); err != nil || r != nil {
		t.Fatal("disabled config accessed dependencies", err)
	}
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.FixedZone("CST", 8*3600))
	valid := GenerationTarget{GroupID: 1, Month: "2026-09"}
	for _, cfg := range []GenerationRollout{
		{Targets: []GenerationTarget{valid}}, {Enabled: true},
		{Enabled: true, Targets: []GenerationTarget{valid, valid}},
		{Enabled: true, Targets: []GenerationTarget{{GroupID: 0, Month: "2026-09"}}},
		{Enabled: true, Targets: []GenerationTarget{{GroupID: -1, Month: "2026-09"}}},
	} {
		if _, err := rolloutScope(cfg, now); err == nil {
			t.Fatal("invalid rollout accepted", cfg)
		}
	}
	for _, month := range []string{"", "*", "2026-9", "2026-00", "2026-13", "2026-09 ", "2019-12", "2026-10", "2027-01"} {
		if _, err := rolloutScope(GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: month}}}, now); err == nil {
			t.Fatal("invalid month accepted", month)
		}
	}
	cfg := GenerationRollout{Enabled: true, Targets: []GenerationTarget{valid}}
	if _, err := rolloutScope(cfg, now.Add(-time.Nanosecond)); err == nil {
		t.Fatal("unclosed Beijing month accepted")
	}
	scope, err := rolloutScope(cfg, now)
	if err != nil || !scope.allows(1, "2026-09") || scope.allows(1, "2026-08") || scope.allows(2, "2026-09") {
		t.Fatal("scope mismatch", err)
	}
	cfg.Targets[0] = GenerationTarget{GroupID: 2, Month: "2026-08"}
	if !scope.allows(1, "2026-09") || scope.allows(2, "2026-08") {
		t.Fatal("caller mutation widened scope")
	}
	var targets []GenerationTarget
	for i := int64(1); i <= 33; i++ {
		targets = append(targets, GenerationTarget{GroupID: i, Month: "2026-09"})
	}
	if _, err := rolloutScope(GenerationRollout{Enabled: true, Targets: targets[:32]}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := rolloutScope(GenerationRollout{Enabled: true, Targets: targets}, now); err == nil {
		t.Fatal("unbounded rollout accepted")
	}
	if r, err := NewControlledLocalRuntime(nil, nil, GenerationRollout{Enabled: true, Targets: []GenerationTarget{valid}}); err == nil || r != nil {
		t.Fatal("enabled runtime accepted missing dependencies")
	}
}

type rolloutCountingService struct{ calls atomic.Int32 }

func (s *rolloutCountingService) SubmitMonthly(_ context.Context, _, _ int64, month string) (monitor.MonthlyGenerationView, error) {
	s.calls.Add(1)
	return monitor.MonthlyGenerationView{Month: month, State: "not_prepared"}, nil
}
func (s *rolloutCountingService) ReadMonthly(ctx context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	return s.SubmitMonthly(ctx, gid, version, month)
}

func TestGenerationRolloutAdmissionAndStatusShareExactScope(t *testing.T) {
	f := generationFixtureNew(t)
	_, ro, _ := usageAuthorityFixture(t)
	r, err := NewControlledLocalRuntime(f.g, ro, GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: "2026-09"}}})
	if err != nil {
		t.Fatal(err)
	}
	s := &rolloutCountingService{}
	r.service = s
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopRuntime(t, r) })
	for _, submit := range []bool{false, true} {
		for _, target := range []GenerationTarget{{GroupID: 2, Month: "2026-09"}, {GroupID: 1, Month: "2026-08"}, {GroupID: 0, Month: "2026-09"}} {
			v, err := r.call(context.Background(), target.GroupID, 1, target.Month, submit)
			if target.GroupID > 0 && !submit {
				if err != nil || v != (monitor.MonthlyGenerationView{Month: target.Month, State: "not_prepared"}) {
					t.Fatal("unprepared scope was reported as worker failure", target, v, err)
				}
			} else if err == nil || v != (monitor.MonthlyGenerationView{}) {
				t.Fatal("outside target exposed/admitted", target, err)
			}
		}
	}
	if s.calls.Load() != 0 {
		t.Fatal("blocked request reached source/authorization service")
	}
	for _, submit := range []bool{false, true} {
		if _, err := r.call(context.Background(), 1, 1, "2026-09", submit); err != nil {
			t.Fatal(err)
		}
	}
	if s.calls.Load() != 2 {
		t.Fatal("allowed request did not delegate")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.ReadMonthly(cancelled, 2, 1, "2026-09"); err == nil {
		t.Fatal("cancelled outside-scope lookup returned a healthy status")
	}
	stopRuntime(t, r)
	if _, err := r.ReadMonthly(context.Background(), 2, 1, "2026-09"); !errors.Is(err, monitor.MonthlyGenerationFailure("temporarily_unavailable")) {
		t.Fatal("stopped worker was confused with unprepared scope", err)
	}
	generationAdmissionCounts(t, f.receiver, 0, 0)
}

func TestGenerationRolloutHTTPEnforcesTarget(t *testing.T) {
	a := newLocalGenerationAcceptance(t)
	owner, other := a.login(t, "owner@example.invalid"), a.login(t, "other@example.invalid")
	for _, target := range []struct {
		cookies []*http.Cookie
		month   string
	}{{owner, "2026-08"}, {other, "2026-09"}} {
		for _, method := range []string{"GET", "POST"} {
			path, body := "/api/monthly-statement/generation?month="+target.month, ""
			if method == "POST" {
				path, body = "/api/monthly-statement/generation", `{"month":"`+target.month+`"}`
			}
			res := a.request(t, method, path, body, target.cookies)
			bodyBytes, err := io.ReadAll(res.Body)
			_ = res.Body.Close()
			wantStatus, wantBody := http.StatusOK, `"state":"not_prepared"`
			if method == "POST" {
				wantStatus, wantBody = http.StatusConflict, `"code":"not_prepared"`
			}
			if err != nil || res.StatusCode != wantStatus || !strings.Contains(string(bodyBytes), wantBody) || strings.Contains(string(bodyBytes), "task_id") {
				t.Fatal("outside rollout response", res.StatusCode, err)
			}
		}
	}
	res := a.request(t, "GET", "/api/monthly-statement/generation?month=2026-09", "", owner)
	body, err := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if err != nil || res.StatusCode != 200 || !strings.Contains(string(body), `"state":"not_requested"`) {
		t.Fatal("allowed scope was disturbed", res.StatusCode, err)
	}
	generationAdmissionCounts(t, a.f.receiver, 0, 1)
}

func TestGenerationRolloutWorkerLeavesOutsideQueueUntouched(t *testing.T) {
	f := generationFixtureNew(t)
	_, ro, _ := usageAuthorityFixture(t)
	ctx := context.Background()
	first := generationSubmit(t, f)
	f.plan.Owner.GroupID = 2
	for i := range f.input.Accounts {
		f.input.Accounts[i].GroupID = 2
	}
	f.raw = draftJSON(t, f.input)
	f.plan.PreparationSHA256 = generationHash(f.raw)
	second := generationSubmit(t, f)
	var checked []int64
	guard := f.g.verifyCurrent
	f.g.verifyCurrent = func(ctx context.Context, req monthlytasks.Request) error {
		checked = append(checked, req.Owner.GroupID)
		return guard(ctx, req)
	}
	out, err := NewControlledLocalRuntime(f.g, ro, GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 2, Month: "2026-08"}}})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if worked, err := out.worker.step(ctx); err != nil || worked {
			t.Fatal("outside queue dispatched", err)
		}
	}
	if len(checked) != 0 {
		t.Fatal("outside task reached guard and risked invalidation")
	}
	for _, before := range []monthlytasks.Task{first, second} {
		after, err := f.g.tasks.Get(ctx, before.Request.Owner, before.ID)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatal("outside task mutated", err)
		}
	}
	stopRuntime(t, out)
	targets := []GenerationTarget{{GroupID: 2, Month: "2026-09"}}
	allowed, err := NewControlledLocalRuntime(f.g, ro, GenerationRollout{Enabled: true, Targets: targets})
	if err != nil {
		t.Fatal(err)
	}
	targets[0].GroupID = 1 // mutation must not change worker or admission scope
	if worked, err := allowed.worker.step(ctx); err != nil || !worked {
		t.Fatal("allowed task did not run", err)
	}
	for _, gid := range checked {
		if gid != 2 {
			t.Fatal("worker widened scope", gid)
		}
	}
	afterFirst, err := f.g.tasks.Get(ctx, first.Request.Owner, first.ID)
	if err != nil || !reflect.DeepEqual(first, afterFirst) {
		t.Fatal("older unselected task changed/starved selected task", err)
	}
	ready, err := f.g.tasks.Get(ctx, second.Request.Owner, second.ID)
	if err != nil || ready.State != monthlytasks.DraftReady || ready.Attempts != 1 {
		t.Fatal("selected task incomplete", ready, err)
	}
	stopRuntime(t, allowed)
	// Scope expansion is explicit stop/rebuild, not implicit retry or mutation.
	resumed, err := NewControlledLocalRuntime(f.g, ro, GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: "2026-09"}}})
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := resumed.worker.step(ctx); err != nil || !worked {
		t.Fatal("previously excluded task could not resume", err)
	}
	stopRuntime(t, resumed)
	stillReady, err := f.g.tasks.Get(ctx, second.Request.Owner, second.ID)
	if err != nil || !reflect.DeepEqual(ready, stillReady) {
		t.Fatal("scope change recomputed completed task", err)
	}
	for _, job := range []monthlytasks.Task{first, second} {
		artifact, err := f.g.Run(ctx, job.Request.Owner, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		assertGenerationAmounts(t, artifact)
	}
	generationAdmissionCounts(t, f.receiver, 2, 2)
}
