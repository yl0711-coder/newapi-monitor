package monthlydraft

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// Borrow the same handles, spool budget, locks and queue. No second worker,
// new schema, customer task, source selection or publication is created here.
// Installation in the customer-generation path remains explicit; this private
// data dependency layer alone is NOT an automatic historical admission path.
type automaticHistoryPreparation struct {
	wallet *WalletSourcePreparation
	spool  *CapturedSourceSpool
	raw    *WalletSourcePreparation
	core   *CapturedSourceSpool
}

func newAutomaticHistoryPreparation(wallet *WalletSourcePreparation, spool *CapturedSourceSpool) (*automaticHistoryPreparation, error) {
	if wallet == nil || wallet.funding == nil || spool == nil || spool.wallet != wallet || spool.identity == nil ||
		!wallet.funding.registeredOrigins || !wallet.funding.walletTails || !wallet.funding.partitionUsage || spool.historyDependency {
		return nil, ErrGenerationInput
	}
	funding := *wallet.funding
	funding.registeredOrigins, funding.walletTails = false, false
	raw, err := NewWalletSourcePreparation(&funding)
	if err != nil {
		return nil, err
	}
	core := *spool
	core.wallet, core.historyDependency = raw, true
	return &automaticHistoryPreparation{wallet: wallet, spool: spool, raw: raw, core: &core}, nil
}

// Before is composed OUTSIDE registered normalized-prefix preparation so the
// real requested month keeps the boundary originals. Later raw months are only
// dependencies; they never broaden the inner selection/publication permission.
// next must still use existing normalization, current authority and CAS gates.
func (p *automaticHistoryPreparation) Before(next func(context.Context, SourcePreparationRequest, monthlysource.MonthlyObservationFile) (bool, error)) (SourcePreparationStep, error) {
	if p == nil || next == nil {
		return nil, ErrGenerationInput
	}
	return func(parent context.Context, req SourcePreparationRequest) (bool, error) {
		if parent == nil {
			return false, ErrGenerationInput
		}
		ctx, cancel := context.WithTimeout(parent, 20*time.Second)
		defer cancel()
		if err := p.workDispatch(ctx, req); err != nil {
			return false, err
		}
		if err := p.checkRetainedWorkSource(ctx, req); err != nil {
			return false, err
		}
		// First registered/tail views come before useful closed-month work.
		// Both wrappers perform one new source operation at most per step.
		entered := false
		warm, err := walletTailSourceBefore(p.wallet.funding, func(context.Context, SourcePreparationRequest) (bool, error) { entered = true; return false, nil }, true)
		if err != nil {
			return false, err
		}
		warm, err = registeredSourceBefore(p.wallet.funding, warm)
		if err != nil {
			return false, err
		}
		if _, err := warm(ctx, req); err != nil || !entered {
			return false, err
		}
		requests, err := p.months(ctx, req)
		if err != nil {
			return false, err
		}
		ctx, err = p.workContext(ctx, req, requests)
		if err != nil {
			return false, err
		}
		// Warm every month's first ordinary collection view before waiting for
		// any second one. This amortizes the read gap across useful month work.
		for _, month := range requests {
			scope, err := p.raw.funding.scope(ctx, month)
			if err != nil {
				return false, err
			}
			if _, err := monthlysource.EnsureFundingMonthCapture(ctx, p.raw.funding.capture, month.Key(), scope); err != nil {
				return false, err
			}
			progress, err := monthlysource.InspectWalletMonthCapture(ctx, p.raw.funding.capture, month.Key(), scope)
			if err != nil {
				return false, err
			}
			if progress.Completed == 0 {
				_, err := p.raw.Advance(ctx, month)
				return false, err
			}
		}
		manifest := automaticHistorySources{Version: 1, Mode: automaticHistorySourcesMode, Owner: req.Owner, SourceIdentity: p.wallet.funding.sourceIdentity}
		var waiting *sourcePreparationPause
		for _, month := range requests {
			wallet, spool := p.raw, p.core
			if month == req {
				wallet, spool = p.wallet, p.spool
			}
			files, found, err := spool.completedSourceReferences(ctx, month)
			if err != nil {
				return false, err
			}
			if !found {
				_, _, err := collectCapturedSources(ctx, wallet, spool, month)
				var pause *sourcePreparationPause
				if errors.As(err, &pause) {
					if waiting == nil || pause.until.Before(waiting.until) {
						waiting = pause
					}
					continue // other months may do useful work while this waits
				}
				return false, err // includes one new operation/seal; next tick rechecks
			}
			manifest.Months = append(manifest.Months, files)
			if month == req {
				manifest.Boundary = files
			}
		}
		if waiting != nil {
			return false, waiting
		}
		if len(manifest.Months) != len(requests) || manifest.Boundary.Mode == "" {
			return false, ErrGenerationIntegrity
		}
		if _, err := p.wallet.funding.scope(ctx, req); err != nil {
			return false, err
		}
		body, err := json.Marshal(manifest)
		if err != nil {
			return false, err
		}
		// Content-addressed preparation envelope, NOT a financial approval.
		// It shares the existing bounded envelope inventory; no new file class.
		ref, err := p.spool.retainHistoryScope(ctx, req, manifest.Boundary, body)
		if err != nil {
			return false, err
		}
		if _, err := p.wallet.funding.scope(ctx, req); err != nil {
			return false, err
		}
		return next(ctx, req, ref)
	}, nil
}

// The actual complete view's month boundary fixes the source range, NOT the
// month the customer clicked, and not a later wall-clock month after restart.
// Complete old paired rounds remain usable without retiming their originals.
func (p *automaticHistoryPreparation) months(ctx context.Context, req SourcePreparationRequest) ([]SourcePreparationRequest, error) {
	scope, err := p.wallet.funding.scope(ctx, req)
	if err != nil {
		return nil, err
	}
	registered, err := monthlysource.InspectRegisteredWalletMonthCapture(ctx, p.wallet.funding.capture, req.Key(), scope)
	if err != nil || registered.Completed != 1 {
		return nil, errors.Join(ErrGenerationEvidence, err)
	}
	original, err := monthlysource.LoadRegisteredWalletObservation(ctx, p.wallet.funding.capture, registered.ID, scope.Funding.SourceIdentity, scope.Funding.UserIDs)
	if err != nil {
		return nil, err
	}
	tail, err := monthlysource.InspectWalletTailMonthCapture(ctx, p.wallet.funding.capture, req.Key(), scope)
	if err != nil || tail.FirstViews*2 != tail.TotalViews || tail.TotalViews == 0 {
		return nil, errors.Join(ErrGenerationEvidence, err)
	}
	zone := time.FixedZone("Usage-UTC+8", 8*3600)
	first, through := "", time.Unix(tail.Boundary, 0).In(zone).AddDate(0, -1, 0).Format("2006-01")
	for _, account := range original.Users {
		month := time.Unix(account.RegisteredAt, 0).In(zone).Format("2006-01")
		if first == "" || month < first {
			first = month
		}
	}
	if first == "" || req.Month < first || req.Month > through {
		return nil, ErrGenerationEvidence
	}
	at, err := time.ParseInLocation("2006-01", first, zone)
	if err != nil {
		return nil, err
	}
	months := []SourcePreparationRequest{}
	for ; at.Format("2006-01") <= through; at = at.AddDate(0, 1, 0) {
		if len(months) == 120 {
			return nil, ErrGenerationCapacity
		}
		months = append(months, SourcePreparationRequest{Owner: req.Owner, Month: at.Format("2006-01")})
	}
	return months, ctx.Err()
}

// Cheap reference discovery only, not a complete-seal or financial audit. It
// avoids rereading every finished raw DB on every source queue tick. Consumers
// MUST pass these pinned references through auditAutomaticHistorySources and
// the existing admission audits before selecting/using financial input.
func (s *CapturedSourceSpool) completedSourceReferences(ctx context.Context, req SourcePreparationRequest) (CapturedSourceFiles, bool, error) {
	var empty CapturedSourceFiles
	root, lock, err := openCapturedOutput(s.directory)
	if err != nil {
		return empty, false, err
	}
	defer root.Close()
	defer lock.Close()
	info, err := root.Stat(".")
	if err != nil || !os.SameFile(s.identity, info) {
		return empty, false, ErrGenerationIntegrity
	}
	job := s.jobName(req)
	if _, err := root.Lstat(job); os.IsNotExist(err) {
		return empty, false, nil
	} else if err != nil {
		return empty, false, err
	}
	jobRoot, err := root.OpenRoot(job)
	if err != nil {
		return empty, false, err
	}
	defer jobRoot.Close()
	_, complete, err := inspectCapturedJob(jobRoot)
	if err != nil || complete == "" {
		return empty, false, err
	}
	seal, err := jobRoot.OpenRoot(complete)
	if err != nil {
		return empty, false, err
	}
	defer seal.Close()
	directory := filepath.Join(s.directory, job, complete)
	refs := []monthlysource.MonthlyObservationFile{}
	for _, name := range []string{"materials.json", "selection.json"} {
		hash, _, err := capturedFileDigest(ctx, seal, name, generationPlanLimit)
		if err != nil {
			return empty, false, err
		}
		refs = append(refs, monthlysource.MonthlyObservationFile{Path: filepath.Join(directory, name), SHA256: hash})
	}
	if err := errors.Join(s.current(root, jobRoot, job), automaticPreparationRootCurrent(seal, directory)); err != nil {
		return empty, false, err
	}
	return CapturedSourceFiles{Mode: capturedSourceFilesMode, Material: refs[0], Selection: refs[1]}, true, ctx.Err()
}
