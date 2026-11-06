package monthlydraft

import (
	"context"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// WalletSourcePreparation records current wallet observations only. It does
// not derive historical month boundaries, reconcile money, or approve bills.
type WalletSourcePreparation struct{ funding *FundingSourcePreparation }

func NewWalletSourcePreparation(funding *FundingSourcePreparation) (*WalletSourcePreparation, error) {
	if funding == nil || funding.source == nil || funding.capture == nil || funding.authorization == nil {
		return nil, ErrGenerationInput
	}
	return &WalletSourcePreparation{funding: funding}, nil
}

func (p *WalletSourcePreparation) Advance(parent context.Context, req SourcePreparationRequest) (monthlysource.WalletMonthCaptureProgress, error) {
	if p == nil || p.funding == nil || p.funding.source == nil || p.funding.capture == nil || p.funding.authorization == nil || parent == nil {
		return monthlysource.WalletMonthCaptureProgress{}, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	scope, err := p.funding.scope(ctx, req)
	if err != nil {
		return monthlysource.WalletMonthCaptureProgress{}, err
	}
	if _, err := monthlysource.EnsureFundingMonthCapture(ctx, p.funding.capture, req.Key(), scope); err != nil {
		return monthlysource.WalletMonthCaptureProgress{}, fundingPreparationError(err)
	}
	if _, err := p.funding.scope(ctx, req); err != nil {
		return monthlysource.WalletMonthCaptureProgress{}, err
	}
	progress, err := monthlysource.CaptureNextWalletMonthView(ctx, p.funding.source, p.funding.capture, req.Key(), scope)
	if err != nil {
		if errors.Is(err, monthlysource.ErrWalletMonthCaptureIntegrity) {
			err = errors.Join(ErrGenerationEvidence, err)
		}
		if errors.Is(err, monthlysource.ErrWalletCaptureBusy) {
			err = errors.Join(ErrGenerationBusy, err)
		}
		return monthlysource.WalletMonthCaptureProgress{}, err
	}
	if _, err := p.funding.scope(ctx, req); err != nil {
		return monthlysource.WalletMonthCaptureProgress{}, err
	}
	return progress, nil
}

func (p *WalletSourcePreparation) Before(next SourcePreparationStep) (SourcePreparationStep, error) {
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
		if progress.Completed != 2 {
			return false, nil
		}
		return next(ctx, req)
	}, nil
}

// WarmFirst records the first wallet view before the other source stages.
// Useful funding/token work can then run during the independent-read gap.
// Compose as wallet.WarmFirst(funding.Before(token.Before(wallet.Before(final))));
// the final wallet.Before still requires the actual separated second read.
func (p *WalletSourcePreparation) WarmFirst(next SourcePreparationStep) (SourcePreparationStep, error) {
	if p == nil || p.funding == nil || next == nil {
		return nil, ErrGenerationInput
	}
	return func(ctx context.Context, req SourcePreparationRequest) (bool, error) {
		scope, err := p.funding.scope(ctx, req)
		if err != nil {
			return false, err
		}
		if _, err := monthlysource.EnsureFundingMonthCapture(ctx, p.funding.capture, req.Key(), scope); err != nil {
			return false, fundingPreparationError(err)
		}
		progress, err := monthlysource.InspectWalletMonthCapture(ctx, p.funding.capture, req.Key(), scope)
		if err != nil {
			if errors.Is(err, monthlysource.ErrWalletMonthCaptureIntegrity) {
				return false, errors.Join(ErrGenerationEvidence, err)
			}
			return false, fundingPreparationError(err)
		}
		if progress.Completed == 0 {
			_, err := p.Advance(ctx, req)
			return false, err // One source operation only, even at a stage boundary.
		}
		if _, err := p.funding.scope(ctx, req); err != nil {
			return false, err
		}
		return next(ctx, req)
	}, nil
}

// AssembleCapturedSources keeps server-derived authority around local source
// assembly; callers never transcribe member IDs or supply financial values.
// The result still requires sealed files and history, not publication approval.
func (p *WalletSourcePreparation) AssembleCapturedSources(parent context.Context, req SourcePreparationRequest) (monthlysource.CapturedMonthAssembly, error) {
	if p == nil || p.funding == nil || p.funding.capture == nil || p.funding.authorization == nil || parent == nil {
		return monthlysource.CapturedMonthAssembly{}, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	scope, err := p.funding.scope(ctx, req)
	if err != nil {
		return monthlysource.CapturedMonthAssembly{}, err
	}
	if p.funding.registeredOrigins {
		registered, err := monthlysource.InspectRegisteredWalletMonthCapture(ctx, p.funding.capture, req.Key(), scope)
		if err != nil || registered.Completed != 1 {
			return monthlysource.CapturedMonthAssembly{}, errors.Join(ErrGenerationEvidence, err)
		}
	}
	if p.funding.walletTails {
		tails, err := monthlysource.InspectWalletTailMonthCapture(ctx, p.funding.capture, req.Key(), scope)
		if err != nil || tails.TotalViews == 0 || tails.Completed != tails.TotalViews {
			return monthlysource.CapturedMonthAssembly{}, errors.Join(ErrGenerationEvidence, err)
		}
	}
	var out monthlysource.CapturedMonthAssembly
	if p.funding.partitionUsage {
		out, err = monthlysource.BuildPartitionedCapturedMonthSelection(ctx, p.funding.capture, req.Key(), scope, p.funding.tokenRecovery)
	} else if p.funding.tokenRecovery {
		out, err = monthlysource.BuildRecoveredCapturedMonthSelection(ctx, p.funding.capture, req.Key(), scope)
	} else {
		out, err = monthlysource.BuildCapturedMonthSelection(ctx, p.funding.capture, req.Key(), scope)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return monthlysource.CapturedMonthAssembly{}, err
		}
		switch {
		case errors.Is(err, monthlysource.ErrWalletMonthCaptureIntegrity), errors.Is(err, monthlysource.ErrFundingMonthCaptureIntegrity), errors.Is(err, monthlysource.ErrTokenMonthCaptureIntegrity), errors.Is(err, monthlysource.ErrTokenMonthCaptureBudget), errors.Is(err, monthlysource.ErrAdaptiveTokenIntegrity), errors.Is(err, monthlysource.ErrAdaptiveTokenBudget), errors.Is(err, monthlysource.ErrAdaptiveTokenStopped):
			err = errors.Join(ErrGenerationEvidence, err)
		case errors.Is(err, monthlysource.ErrFundingAuditBusy), errors.Is(err, monthlysource.ErrFundingMoneyBusy), errors.Is(err, monthlysource.ErrUsageMonthBusy):
			err = errors.Join(ErrGenerationBusy, err)
		}
		return monthlysource.CapturedMonthAssembly{}, err
	}
	if _, err := p.funding.scope(ctx, req); err != nil {
		return monthlysource.CapturedMonthAssembly{}, err
	}
	return out, nil
}
