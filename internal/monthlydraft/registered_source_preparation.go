package monthlydraft

import (
	"context"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// Borrow the existing private source/capture handles and queue lifetime. No
// second scheduler, implicit schema, customer-configurable amount or authority.
func registeredSourceBefore(p *FundingSourcePreparation, next SourcePreparationStep) (SourcePreparationStep, error) {
	if p == nil || !p.registeredOrigins || next == nil {
		return nil, ErrGenerationInput
	}
	if err := monthlysource.ValidateRegisteredWalletMonthCaptureStore(context.Background(), p.capture); err != nil {
		return nil, err
	}
	return func(parent context.Context, req SourcePreparationRequest) (bool, error) {
		if parent == nil {
			return false, ErrGenerationInput
		}
		ctx, cancel := context.WithTimeout(parent, 20*time.Second)
		defer cancel()
		scope, err := p.scope(ctx, req)
		if err != nil {
			return false, err
		}
		if _, err := monthlysource.EnsureFundingMonthCapture(ctx, p.capture, req.Key(), scope); err != nil {
			return false, fundingPreparationError(err)
		}
		if _, err := p.scope(ctx, req); err != nil {
			return false, err
		}
		progress, err := monthlysource.CaptureNextRegisteredWalletMonthView(ctx, p.source, p.capture, req.Key(), scope)
		if err != nil {
			if errors.Is(err, monthlysource.ErrWalletCaptureBusy) {
				err = errors.Join(ErrGenerationBusy, err)
			}
			if errors.Is(err, monthlysource.ErrWalletMonthCaptureIntegrity) {
				err = errors.Join(ErrGenerationEvidence, err)
			}
			return false, err
		}
		if _, err := p.scope(ctx, req); err != nil {
			return false, err
		}
		if progress.Captured {
			return false, nil // one new operation only, next stage on a later step
		}
		if progress.Completed != 1 {
			return false, ErrGenerationEvidence
		}
		return next(ctx, req)
	}, nil
}
