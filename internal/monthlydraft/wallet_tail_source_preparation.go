package monthlydraft

import (
	"context"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// Reuse the SAME on-demand queue, authority, private handles and shutdown
// lifetime. Warm mode collects first originals before useful funding/token
// work; final mode requires independently re-audited pairs before any seal.
func walletTailSourceBefore(p *FundingSourcePreparation, next SourcePreparationStep, warm bool) (SourcePreparationStep, error) {
	if p == nil || !p.walletTails || !p.registeredOrigins || next == nil {
		return nil, ErrGenerationInput
	}
	if err := monthlysource.ValidateWalletTailCaptureStore(context.Background(), p.capture); err != nil {
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
		progress, err := monthlysource.CaptureNextWalletTailMonthView(ctx, p.source, p.capture, req.Key(), scope, warm)
		if err != nil {
			switch {
			case errors.Is(err, monthlysource.ErrWalletCaptureBusy):
				err = errors.Join(ErrGenerationBusy, err)
			case errors.Is(err, monthlysource.ErrWalletTailCaptureCapacity):
				err = errors.Join(ErrGenerationCapacity, err)
			case errors.Is(err, monthlysource.ErrWalletMonthCaptureIntegrity):
				err = errors.Join(ErrGenerationEvidence, err)
			}
			return false, err
		}
		if _, err := p.scope(ctx, req); err != nil {
			return false, err
		}
		if progress.Captured {
			return false, nil
		}
		if progress.TotalViews > 0 && (progress.Completed == progress.TotalViews || warm && progress.FirstViews*2 == progress.TotalViews) {
			return next(ctx, req)
		}
		if progress.NextEligibleMS > time.Now().UnixMilli() {
			return false, &sourcePreparationPause{until: time.UnixMilli(progress.NextEligibleMS)}
		}
		return false, nil
	}, nil
}
