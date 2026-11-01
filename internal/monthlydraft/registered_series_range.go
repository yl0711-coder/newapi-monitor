package monthlydraft

import (
	"context"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

// The registered worker borrows its identity from the validated SELECT source,
// not a range file or the request. The real calendar/owner resolver is mandatory.
// Legacy workers continue using their immutable per-group configured ranges.
func (w *LocalWorker) automaticSeriesIdentity(gid int64) (string, error) {
	if w == nil || gid <= 0 {
		return "", ErrGenerationInput
	}
	if w.dynamicSeries {
		if w.scope == nil || !w.scope.registered || !generationHex(w.seriesIdentity, 32) || w.seriesResolve == nil {
			return "", ErrGenerationInput
		}
		return w.seriesIdentity, nil
	}
	scope, ok := w.seriesScopes[gid]
	if !ok {
		return "", ErrGenerationInput
	}
	return scope.SourceIdentity, nil
}

func (w *LocalWorker) automaticSeriesScope(ctx context.Context, req monthlytasks.Request) (AutomaticDisplaySeriesRange, error) {
	var empty AutomaticDisplaySeriesRange
	if ctx == nil {
		return empty, ErrGenerationInput
	}
	identity, err := w.automaticSeriesIdentity(req.Owner.GroupID)
	if err != nil {
		return empty, err
	}
	if req.SourceIdentity != identity {
		if !w.dynamicSeries {
			return empty, ErrGenerationInput
		}
		return empty, ErrGenerationChanged
	}
	scope := w.seriesScopes[req.Owner.GroupID]
	if w.dynamicSeries {
		scope = AutomaticDisplaySeriesRange{GroupID: req.Owner.GroupID, SourceIdentity: identity}
	} else if req.Month < scope.From || req.Month > scope.Through {
		return empty, ErrGenerationInput
	}
	if w.seriesResolve != nil {
		return w.seriesResolve(ctx, scope, req)
	}
	return scope, ctx.Err()
}

// Re-resolve registration and complete authority at EVERY fingerprint fence.
// Rechecking only old range heads could miss an origin change while preparing.
// The fingerprint includes the trimmed required range, so a calendar rollover
// alone does not invalidate an unchanged target and its publication constraints.
func (w *LocalWorker) automaticSeriesFingerprint(ctx context.Context, req monthlytasks.Request) (string, error) {
	scope, err := w.automaticSeriesScope(ctx, req)
	if err != nil {
		return "", err
	}
	return automaticDisplayDependencyFingerprint(ctx, w.g, scope, req)
}
