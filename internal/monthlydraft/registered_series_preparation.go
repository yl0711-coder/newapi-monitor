package monthlydraft

import (
	"context"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

// Private capability for preparing one internal dependency of an admitted
// customer request. It is never serialized, accepted from HTTP or evidence of
// money. Submission/publication retain the configured server eligibility gate.
type dependencyPreparationContextKey struct{}
type dependencyPreparationGrant struct {
	runtime             *LocalRuntime
	owner               monthlytasks.Owner
	target, from, month string
}

func (r *LocalRuntime) permitsDependencyPreparation(ctx context.Context, gid int64, month string) bool {
	grant, ok := ctx.Value(dependencyPreparationContextKey{}).(dependencyPreparationGrant)
	return ok && grant.runtime == r && grant.owner.GroupID == gid && grant.month == month &&
		validSelectionScope(gid, month) && grant.from <= month && month <= grant.target && r.scope.allows(gid, grant.target)
}

type registeredSeriesPreparation struct {
	d        *Deployment
	calendar *registrationCalendarService
	step     SourcePreparationStep
}

// EnableRegisteredSeriesPreparation composes existing bounded source steps
// with the SAME source-prefix policy used by the final publisher. Call after
// EnableRegistrationCalendar and before Start. It creates no new jobs, files,
// schema, customer months or schedules. Existing modes remain unchanged.
func (d *Deployment) EnableRegisteredSeriesPreparation() error {
	if d == nil || d.runtime == nil || d.readOnly {
		return ErrGenerationInput
	}
	r := d.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	calendar, ok := r.service.(*registrationCalendarService)
	if r.phase != "disabled" {
		return ErrGenerationBusy
	}
	if !ok || r.preparation == nil || r.preparation.dependencies != nil || r.worker == nil || r.preparation.step == nil {
		return ErrGenerationInput
	}
	if r.worker.dynamicSeries {
		if r.scope == nil || !r.scope.registered || !generationHex(r.worker.seriesIdentity, 32) {
			return ErrGenerationInput
		}
	} else if len(r.worker.seriesScopes) == 0 {
		return ErrGenerationInput
	}
	p := &registeredSeriesPreparation{d: d, calendar: calendar, step: r.preparation.step}
	r.preparation.dependencies = p
	r.preparation.step = p.advance
	r.worker.seriesResolve = p.resolve
	return nil
}

// Registered origin is independent of which month the customer clicks first.
// The original publisher's authoritative transaction still protects frozen
// origins/bills: a different established origin is a conflict, not permission
// to recalculate or overwrite an already issued statement.
func (p *registeredSeriesPreparation) resolve(ctx context.Context, scope AutomaticDisplaySeriesRange, request monthlytasks.Request) (AutomaticDisplaySeriesRange, error) {
	var empty AutomaticDisplaySeriesRange
	registered := p.d.runtime.worker.dynamicSeries
	if registered && (p.d.runtime.scope == nil || !p.d.runtime.scope.registered || scope.SourceIdentity != p.d.runtime.worker.seriesIdentity) {
		return empty, ErrGenerationChanged
	}
	if scope.GroupID != request.Owner.GroupID || scope.SourceIdentity != request.SourceIdentity || (!registered && request.Month > scope.Through) || !p.d.runtime.scope.allows(request.Owner.GroupID, request.Month) {
		return empty, ErrGenerationChanged
	}
	calendar, err := p.calendar.calendar(ctx, request.Owner.GroupID, request.Owner.AuthVersion)
	if err != nil {
		return empty, err
	}
	owner, _, err := LoadUsageGenerationOwner(ctx, p.calendar.auth, request.Owner.GroupID, request.Owner.AuthVersion)
	if err != nil {
		return empty, err
	}
	if owner != request.Owner || calendar.FirstMonth > request.Month || request.Month > calendar.LastMonth {
		return empty, ErrGenerationChanged
	}
	if registered {
		// Only the real registration metadata establishes origin. End eligibility
		// advances with the calendar; unrequested months are trimmed by the
		// existing required-prefix scan, including uncertain publication attempts.
		scope.Through = calendar.LastMonth
	}
	from, e1 := time.Parse("2006-01", calendar.FirstMonth)
	through, e2 := time.Parse("2006-01", scope.Through)
	months := (through.Year()-from.Year())*12 + int(through.Month()-from.Month()) + 1
	if e1 != nil || e2 != nil || months < 1 || months > 120 {
		return empty, ErrGenerationCapacity
	}
	scope.From = calendar.FirstMonth
	return scope, ctx.Err()
}

func (p *registeredSeriesPreparation) plan(ctx context.Context, req SourcePreparationRequest) (AutomaticDisplaySeriesRange, error) {
	identity, err := p.d.runtime.worker.automaticSeriesIdentity(req.Owner.GroupID)
	if err != nil {
		return AutomaticDisplaySeriesRange{}, err
	}
	// Planning descriptor only: never persisted/admitted as a customer task.
	// RequiredScope reads existing authoritative publication attempt records
	// so a previous post-write crash/later bill remains a constraint.
	request := monthlytasks.Request{Owner: req.Owner, Month: req.Month, SourceIdentity: identity}
	scope, err := p.d.runtime.worker.automaticSeriesScope(ctx, request)
	if err != nil {
		return AutomaticDisplaySeriesRange{}, err
	}
	return p.d.g.automaticDisplayRequiredScope(ctx, scope, request)
}

// Inspect only small selection metadata. Financial files/amounts are still
// independently re-audited by the existing preparation and final publisher.
// Scan the whole prefix BEFORE doing work: a withdrawal/conflict in a later
// dependency cannot cause earlier sources to be silently regenerated.
func (p *registeredSeriesPreparation) missing(ctx context.Context, req SourcePreparationRequest) (AutomaticDisplaySeriesRange, string, error) {
	scope, err := p.plan(ctx, req)
	if err != nil {
		return scope, "", err
	}
	from, _ := time.Parse("2006-01", scope.From)
	missing := ""
	for at := from; at.Format("2006-01") <= scope.Through; at = at.AddDate(0, 1, 0) {
		month := at.Format("2006-01")
		selection, err := ReadGenerationSelection(ctx, p.d.receiver, req.Owner.GroupID, month)
		if err != nil {
			return scope, "", err
		}
		if selection.Head == "" {
			if missing == "" {
				missing = month
			}
			continue
		}
		if selection.Request == nil {
			return scope, "", ErrGenerationChanged
		}
		source := *selection.Request
		if source.Owner.GroupID != req.Owner.GroupID || source.Month != month || source.SourceIdentity != scope.SourceIdentity || !currentGenerationRules(source) || month == req.Month && source.Owner != req.Owner {
			return scope, "", ErrGenerationChanged
		}
	}
	return scope, missing, ctx.Err()
}

func (p *registeredSeriesPreparation) ready(ctx context.Context, req SourcePreparationRequest) (bool, error) {
	_, missing, err := p.missing(ctx, req)
	return missing == "" && err == nil, err
}

func (p *registeredSeriesPreparation) hasPublicationAttempt(ctx context.Context, req SourcePreparationRequest) (bool, error) {
	selected, err := ReadGenerationSelection(ctx, p.d.receiver, req.Owner.GroupID, req.Month)
	if err != nil {
		return false, err
	}
	if selected.Request == nil {
		return false, nil
	}
	if selected.Request.Owner != req.Owner {
		return false, ErrGenerationChanged
	}
	task, err := p.d.g.tasks.Find(ctx, *selected.Request)
	if errors.Is(err, monthlytasks.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var attempted bool
	err = p.d.receiver.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM portal_monthly_publication_jobs WHERE task_id=?)`, task.ID).Scan(&attempted)
	return attempted, err
}

func (p *registeredSeriesPreparation) advance(ctx context.Context, req SourcePreparationRequest) (bool, error) {
	scope, month, err := p.missing(ctx, req)
	if err != nil {
		return false, err
	}
	if month == "" {
		return true, nil
	}
	grant := dependencyPreparationGrant{runtime: p.d.runtime, owner: req.Owner, target: req.Month, from: scope.From, month: month}
	local := context.WithValue(ctx, dependencyPreparationContextKey{}, grant)
	dependency := SourcePreparationRequest{Owner: req.Owner, Month: month}
	ready, err := p.step(local, dependency)
	if err != nil {
		return false, err
	}
	if ready {
		selected, err := ReadGenerationSelection(ctx, p.d.receiver, req.Owner.GroupID, month)
		if err != nil {
			return false, err
		}
		if selected.Request == nil || selected.Request.Owner != req.Owner || selected.Request.SourceIdentity != scope.SourceIdentity || selected.Request.Month != month || !currentGenerationRules(*selected.Request) {
			return false, ErrGenerationEvidence
		}
	}
	// A committed dependency is durable progress; the next bounded queue step
	// checks all heads again before submitting ONLY the requested customer month.
	return false, nil
}
