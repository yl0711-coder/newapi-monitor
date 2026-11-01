package monthlydraft

import "context"

// Data-only dependency work. In particular a month later than the requested
// bill may be needed to walk back from an actual current wallet view. Such a
// month must NOT be submitted/selected/published as another customer bill.
// Keep the scheduler's "ready means validated selection" contract unchanged:
// the callback always returns false, and readiness here means only a seal.
func collectCapturedSources(ctx context.Context, wallet *WalletSourcePreparation, spool *CapturedSourceSpool, req SourcePreparationRequest) (files CapturedSourceFiles, sealed bool, err error) {
	if ctx == nil {
		return files, false, ErrGenerationInput
	}
	step, err := ComposeCapturedSourcePreparation(wallet, spool, func(_ context.Context, _ SourcePreparationRequest, original CapturedSourceFiles) (bool, error) {
		files, sealed = original, true
		return false, nil
	})
	if err != nil {
		return files, false, err
	}
	if ready, err := step(ctx, req); err != nil {
		return CapturedSourceFiles{}, false, err
	} else if ready {
		return CapturedSourceFiles{}, false, ErrGenerationIntegrity
	}
	return files, sealed, ctx.Err()
}
