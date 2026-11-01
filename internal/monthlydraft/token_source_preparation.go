package monthlydraft

import (
	"context"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// TokenSourcePreparation shares the already-isolated funding collector's
// source/capture lifetime and pinned current organization roster. It creates
// no new financial rules, worker, connection or application configuration.
type TokenSourcePreparation struct{ funding *FundingSourcePreparation }

// NewTokenSourcePreparation starts nothing and initializes no schemas. Only
// the owner explicitly initializes InitTokenMonthCaptureStore on its private
// unsealed capture database. Production composition remains disabled.
func NewTokenSourcePreparation(funding *FundingSourcePreparation) (*TokenSourcePreparation, error) {
	if funding == nil || funding.source == nil || funding.capture == nil || funding.authorization == nil {
		return nil, ErrGenerationInput
	}
	return &TokenSourcePreparation{funding: funding}, nil
}

func (p *TokenSourcePreparation) Advance(parent context.Context, req SourcePreparationRequest) (monthlysource.TokenMonthCaptureProgress, error) {
	if p == nil || p.funding == nil || p.funding.source == nil || p.funding.capture == nil || p.funding.authorization == nil || parent == nil {
		return monthlysource.TokenMonthCaptureProgress{}, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	scope, err := p.funding.scope(ctx, req)
	if err != nil {
		return monthlysource.TokenMonthCaptureProgress{}, err
	}
	if _, err := monthlysource.EnsureFundingMonthCapture(ctx, p.funding.capture, req.Key(), scope); err != nil {
		return monthlysource.TokenMonthCaptureProgress{}, fundingPreparationError(err)
	}
	if _, err := p.funding.scope(ctx, req); err != nil {
		return monthlysource.TokenMonthCaptureProgress{}, err
	}
	var before monthlysource.TokenWorkCheckpoint
	if p.funding.accountTokenWork {
		d, ok := ctx.Value(tokenDispatchKey{}).(*tokenWorkDispatch)
		if !ok || d == nil || !d.p.accountTokenWork || d.job.request.Owner != req.Owner || d.receipt != nil {
			return monthlysource.TokenMonthCaptureProgress{}, ErrGenerationInput
		}
		before, err = monthlysource.ReadTokenWorkCheckpoint(ctx, p.funding.capture, req.Key(), scope)
		if err != nil {
			return monthlysource.TokenMonthCaptureProgress{}, err
		}
		if err := checkTokenCheckpoint(ctx, req, before); err != nil {
			return monthlysource.TokenMonthCaptureProgress{}, err
		}
	}
	var progress monthlysource.TokenMonthCaptureProgress
	if p.funding.tokenRecovery {
		progress, err = monthlysource.CaptureNextRecoveringTokenMonthSlot(ctx, p.funding.source, p.funding.capture, req.Key(), scope)
	} else {
		progress, err = monthlysource.CaptureNextTokenMonthSlot(ctx, p.funding.source, p.funding.capture, req.Key(), scope)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return monthlysource.TokenMonthCaptureProgress{}, err
		}
		if errors.Is(err, monthlysource.ErrTokenMonthCaptureBudget) || errors.Is(err, monthlysource.ErrAdaptiveTokenBudget) || errors.Is(err, monthlysource.ErrAdaptiveTokenStopped) {
			err = errors.Join(ErrGenerationEvidence, err)
		}
		if errors.Is(err, monthlysource.ErrTokenMonthCaptureIntegrity) || errors.Is(err, monthlysource.ErrAdaptiveTokenIntegrity) {
			err = errors.Join(ErrGenerationIntegrity, err)
		}
		if errors.Is(err, monthlysource.ErrWalletCaptureBusy) {
			err = errors.Join(ErrGenerationBusy, err)
		}
		return monthlysource.TokenMonthCaptureProgress{}, err
	}
	if _, err := p.funding.scope(ctx, req); err != nil {
		return monthlysource.TokenMonthCaptureProgress{}, err
	}
	if p.funding.accountTokenWork && progress.Captured {
		after, e := monthlysource.ReadTokenWorkCheckpoint(ctx, p.funding.capture, req.Key(), scope)
		if e != nil {
			return monthlysource.TokenMonthCaptureProgress{}, e
		}
		if e := recordTokenWork(ctx, req, before, after); e != nil {
			return monthlysource.TokenMonthCaptureProgress{}, e
		}
	}
	return progress, nil
}

// Before cannot approve a bill: the next trusted stage must still assemble,
// audit and select complete immutable inputs through the existing pipeline.
func (p *TokenSourcePreparation) Before(next SourcePreparationStep) (SourcePreparationStep, error) {
	if p == nil || next == nil {
		return nil, ErrGenerationInput
	}
	return func(ctx context.Context, req SourcePreparationRequest) (bool, error) {
		progress, err := p.Advance(ctx, req)
		if err != nil {
			return false, err
		}
		if progress.Captured {
			return false, nil
		}
		if progress.NextEligibleMS > time.Now().UnixMilli() {
			return false, &sourcePreparationPause{until: time.UnixMilli(progress.NextEligibleMS)}
		}
		if !progress.TraversalsFinished {
			return false, nil
		}
		return next(ctx, req)
	}, nil
}
