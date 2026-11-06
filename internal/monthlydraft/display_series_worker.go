package monthlydraft

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

type AutomaticDisplaySeriesPublisher func(context.Context, monthlytasks.Request, []byte, *monitor.PreparedMonthlyDisplayPublication) error

// Explicit opt-in; the existing automatic/draft constructors keep their old
// behavior. Preparation precedes the publication lease, so waiting on a missing
// predecessor never exhausts the immutable publication retry journal.
func NewAutomaticDisplaySeriesLocalWorker(g *Generation, metadata AutomaticContext, ranges []AutomaticDisplaySeriesRange, publish AutomaticDisplaySeriesPublisher) (*LocalWorker, error) {
	return newAutomaticDisplaySeriesLocalWorker(g, metadata, ranges, publish, false, nil)
}

func newAutomaticDisplaySeriesLocalWorker(g *Generation, metadata AutomaticContext, ranges []AutomaticDisplaySeriesRange, publish AutomaticDisplaySeriesPublisher, registered bool, authority *gorm.DB) (*LocalWorker, error) {
	if metadata == nil || publish == nil {
		return nil, ErrGenerationInput
	}
	w, err := NewLocalWorker(g)
	if err != nil {
		return nil, err
	}
	owned, err := automaticDisplaySeriesRanges(g, ranges, registered)
	if err != nil {
		return nil, err
	}
	w.seriesScopes = owned
	w.dynamicSeries = registered
	factory := automaticDisplayPlanFactory(monitor.PrepareMonthlyDisplayPublication)
	if registered {
		if authority == nil {
			return nil, ErrGenerationInput
		}
		factory = func(ctx context.Context, scope monitor.MonthlyDisplaySeriesScope, sources []monitor.MonthlyDisplayPublicationSource, now time.Time) (*monitor.PreparedMonthlyDisplayPublication, error) {
			return monitor.PrepareMonthlyRegisteredDisplayPublication(ctx, authority, scope, sources, now)
		}
	}
	prepare, err := newAutomaticDisplaySeriesPreparerWithPlan(g, w.automaticSeriesScope, factory)
	if err != nil {
		return nil, err
	}
	w.publications, err = newPublicationStore(g.receiver)
	if err != nil {
		return nil, err
	}
	// Fail closed if the preparation hook is ever accidentally bypassed.
	w.finalize = func(context.Context, monthlytasks.Task, publicationLease) error { return ErrGenerationEvidence }
	w.beforeFinalize = func(parent context.Context, task monthlytasks.Task) (func(context.Context, monthlytasks.Task, publicationLease) error, error) {
		ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
		defer cancel()
		fingerprint, err := w.automaticSeriesFingerprint(ctx, task.Request)
		if err != nil {
			return nil, err
		}
		state := w.getDeferral(task.ID)
		if state.preparation != fingerprint {
			state.preparation, state.preparationFailures = fingerprint, 0
			w.putDeferral(task.ID, state)
		}
		if state.preparationFailures >= 3 {
			return nil, ErrGenerationEvidence // cheap heads-only checks until a source version changes
		}
		original, err := automaticBillEvidence(ctx, g, metadata, task)
		var result automaticDisplayPublication
		if err == nil {
			selected := prepare
			if registered {
				journal, readErr := w.publications.read(ctx, task.ID)
				if readErr != nil && !errors.Is(readErr, sql.ErrNoRows) {
					err = readErr
				} else if readErr == nil && journal.evidence != "" {
					selected, err = newAutomaticDisplaySeriesPreparerWithPlan(g, w.automaticSeriesScope,
						func(ctx context.Context, scope monitor.MonthlyDisplaySeriesScope, sources []monitor.MonthlyDisplayPublicationSource, now time.Time) (*monitor.PreparedMonthlyDisplayPublication, error) {
							return monitor.PrepareMonthlyRegisteredDisplayPublicationForBinding(ctx, authority, scope, sources, now, task.Request.Month, journal.evidence)
						})
				}
			}
			if err == nil {
				result, err = selected(ctx, task.Request, original)
			}
		}
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrGenerationBusy) {
				state.preparationFailures++
			}
			w.putDeferral(task.ID, state)
			return nil, err
		}
		if err := g.currentTask(ctx, task); err != nil {
			return nil, err
		}
		// Recheck dependency heads immediately before claiming a publication.
		current, err := w.automaticSeriesFingerprint(ctx, task.Request)
		if err != nil || current != fingerprint {
			return nil, errors.Join(ErrGenerationChanged, err)
		}
		return func(parent context.Context, ready monthlytasks.Task, lease publicationLease) error {
			ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
			defer cancel()
			if ready.Request != task.Request || ready.ResultSHA256 != task.ResultSHA256 {
				return ErrGenerationChanged
			}
			if err := g.currentTask(ctx, ready); err != nil {
				return err
			}
			current, err := w.automaticSeriesFingerprint(ctx, ready.Request)
			if err != nil || current != fingerprint {
				return errors.Join(ErrGenerationChanged, err)
			}
			if err := w.publications.bind(ctx, lease, result.binding); err != nil {
				return err
			}
			return publish(ctx, ready.Request, result.evidence, result.series)
		}, nil
	}
	return w, nil
}

// Cheap, bounded recovery activation. No hashing source databases or computing
// money on a paused dependency snapshot. A new selected version activates a
// fresh bounded preparation budget without modifying any published bill.
func automaticDisplayDependencyFingerprint(ctx context.Context, g *Generation, scope AutomaticDisplaySeriesRange, request monthlytasks.Request) (string, error) {
	if request.Owner.GroupID != scope.GroupID || request.SourceIdentity != scope.SourceIdentity || request.Month < scope.From || request.Month > scope.Through {
		return "", ErrGenerationInput
	}
	var err error
	scope, err = g.automaticDisplayRequiredScope(ctx, scope, request)
	if err != nil {
		return "", err
	}
	from, _ := time.Parse("2006-01", scope.From)
	var selections []GenerationSelection
	for at := from; at.Format("2006-01") <= scope.Through; at = at.AddDate(0, 1, 0) {
		selection, err := ReadGenerationSelection(ctx, g.receiver, scope.GroupID, at.Format("2006-01"))
		if err != nil {
			return "", err
		}
		if selection.Request == nil {
			return "", ErrGenerationSelectionMissing
		}
		if selection.Request.Owner.GroupID != request.Owner.GroupID || selection.Request.SourceIdentity != scope.SourceIdentity || (selection.Request.Month == request.Month && *selection.Request != request) {
			return "", ErrGenerationChanged
		}
		selections = append(selections, selection)
	}
	raw, err := json.Marshal(struct {
		Scope      AutomaticDisplaySeriesRange
		Selections []GenerationSelection
	}{scope, selections})
	if err != nil {
		return "", err
	}
	return generationHash(raw), ctx.Err()
}
