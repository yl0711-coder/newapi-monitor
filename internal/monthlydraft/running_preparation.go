package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// PrepareMonthlyInput is an INTERNAL trusted-source entry, never a customer DTO
// or HTTP endpoint. It admits only the already configured rollout targets while
// this resource owner is running. It reuses its process lock/connections rather
// than opening a second writer. No source collection, task submission, schema
// initialization, approval or customer publication takes place here.
//
// apply=false checks without writing. apply=true selects complete retained
// inputs with the existing expected-head CAS; this is not a financial approval.
func (d *Deployment) PrepareMonthlyInput(parent context.Context, planRaw, preparation []byte, expectedHead string, apply bool) (PreparationResult, error) {
	if d == nil || d.runtime == nil || d.readOnly || parent == nil || len(planRaw) == 0 || len(planRaw) > generationPlanLimit ||
		len(preparation) == 0 || len(preparation) > monitor.MonthlyPreparationInputLimit || (expectedHead != "" && !generationHex(expectedHead, 32)) {
		return PreparationResult{}, ErrGenerationInput
	}
	if err := parent.Err(); err != nil {
		return PreparationResult{}, err
	}
	// Bound cloning/decoding as well as SQL/file checks. Trusted callers must
	// receive busy rather than allocate many complete 8 MiB inputs in parallel.
	select {
	case generationSlots <- struct{}{}:
		defer func() { <-generationSlots }()
	default:
		return PreparationResult{}, ErrGenerationBusy
	}
	planRaw, preparation = bytes.Clone(planRaw), bytes.Clone(preparation)
	var plan GenerationPlan
	if json.Unmarshal(planRaw, &plan) != nil {
		return PreparationResult{}, ErrGenerationInput
	}
	p, canonical, input, err := canonicalGeneration(plan, preparation, time.Now())
	if err != nil || !bytes.Equal(bytes.TrimSpace(planRaw), canonical) {
		if err != nil {
			return PreparationResult{}, err
		}
		return PreparationResult{}, ErrGenerationInput
	}
	if grant, ok := parent.Value(dependencyPreparationContextKey{}).(dependencyPreparationGrant); ok && (grant.runtime != d.runtime || grant.owner != p.Owner || grant.month != p.Funding.Month) {
		return PreparationResult{}, ErrGenerationChanged
	}
	ctx, done, err := d.runtime.beginPreparation(parent, p.Owner.GroupID, p.Funding.Month)
	if err != nil {
		return PreparationResult{}, err
	}
	defer done()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	// Preserve a committed receipt even if cancellation races the return. A
	// caller must inspect/reload the selection after an ambiguous result, never
	// infer rollback or blindly repeat a stale expected-head operation.
	return prepareGenerationInputOnDeployment(ctx, d, p, canonical, input, preparation, expectedHead, apply)
}

// Register work BEFORE touching owned handles. Stop cancels its context and
// drains this operation along with customer metadata requests and the worker;
// a deadline cannot close SQLite or the evidence root under a running check.
func (r *LocalRuntime) beginPreparation(parent context.Context, gid int64, month string) (context.Context, func(), error) {
	if parent == nil || parent.Err() != nil {
		if parent != nil {
			return nil, nil, parent.Err()
		}
		return nil, nil, ErrGenerationInput
	}
	r.mu.Lock()
	if r.phase != "running" || r.ctx.Err() != nil {
		r.mu.Unlock()
		return nil, nil, ErrGenerationBusy
	}
	if r.scope == nil || !r.scope.allows(gid, month) && !r.permitsDependencyPreparation(parent, gid, month) {
		r.mu.Unlock()
		return nil, nil, ErrGenerationInput
	}
	r.active++
	lifetime := r.ctx
	r.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(lifetime, cancel)
	if lifetime.Err() != nil {
		cancel()
	}
	return ctx, func() {
		stop()
		cancel()
		r.mu.Lock()
		r.active--
		r.finishLocked()
		r.mu.Unlock()
	}, nil
}
