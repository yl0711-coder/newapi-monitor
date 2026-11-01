package monthlydraft

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"gorm.io/gorm"
)

// FundingSourcePreparation owns no connections and starts no work. Capture is
// a separate PRIVATE SQLite database, not the task receiver, authority, running
// Monitor database or a previously sealed evidence database. Source must be a
// dedicated SELECT-only MySQL handle. Initialize only the capture store with
// monthlysource.InitFundingMonthCaptureStore before constructing this adapter.
type FundingSourcePreparation struct {
	source, capture   *sql.DB
	authorization     *gorm.DB
	sourceIdentity    string
	tokenRecovery     bool // immutable after trusted startup composition; default off
	accountTokenWork  bool // source-work ledger; no source/finance budget relaxation
	partitionUsage    bool // explicit captured index reader; legacy handoffs unchanged
	registeredOrigins bool // explicit original registration/wallet checkpoint; default off
	walletTails       bool // same-view tail originals only; no automatic historical balance claim
}

func NewFundingSourcePreparation(source, capture *sql.DB, generation *Generation, authorization *gorm.DB, sourceIdentity string) (*FundingSourcePreparation, error) {
	if source == nil || capture == nil || generation == nil || generation.receiver == nil || generation.funding == nil || authorization == nil || source == capture || source == generation.receiver || capture == generation.receiver || !generationHex(sourceIdentity, 32) {
		return nil, ErrGenerationInput
	}
	// Reject a SQLite source/capture alias even when passed through distinct
	// handles. A MySQL source and SQLite capture cannot share this driver kind.
	if reflect.TypeOf(source.Driver()) == reflect.TypeOf(capture.Driver()) {
		return nil, ErrGenerationInput
	}
	authDB, err := authorization.DB()
	if err != nil {
		return nil, err
	}
	if source == authDB || capture == authDB {
		return nil, ErrGenerationInput
	}
	file, err := generationDatabaseFile(capture)
	if err != nil || file.Mode().Perm()&0077 != 0 {
		return nil, ErrGenerationInput
	}
	for _, db := range []*sql.DB{authDB, generation.receiver, generation.funding} {
		other, err := generationDatabaseFile(db)
		if err != nil {
			return nil, err
		}
		if os.SameFile(file, other) {
			return nil, ErrGenerationInput
		}
	}
	return &FundingSourcePreparation{source: source, capture: capture, authorization: authorization, sourceIdentity: sourceIdentity}, nil
}

func (p *FundingSourcePreparation) scope(ctx context.Context, req SourcePreparationRequest) (monthlysource.OrganizationMonthScope, error) {
	owner, ids, err := LoadUsageGenerationOwner(ctx, p.authorization, req.Owner.GroupID, req.Owner.AuthVersion)
	if err != nil {
		return monthlysource.OrganizationMonthScope{}, err
	}
	if owner != req.Owner {
		return monthlysource.OrganizationMonthScope{}, ErrGenerationChanged
	}
	scope := monthlysource.OrganizationMonthScope{Version: 1, Mode: monthlysource.OrganizationMonthScopeMode,
		GroupID: owner.GroupID, AuthVersion: owner.AuthVersion, MemberFingerprint: owner.MemberFingerprint, MemberManifest: owner.MemberManifest,
		Funding: monthlysource.FundingMonthRequest{Month: req.Month, SourceIdentity: p.sourceIdentity,
			Rules: monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}, UserIDs: ids}}
	return scope, nil
}

// Advance is called by a background queue owner, never an HTTP handler. One
// invocation advances at most one page; current authority is checked before
// source I/O and again afterward. All source SQL/transaction/row limits are the
// existing collector's limits. No request text or customer bill is read/written.
func (p *FundingSourcePreparation) Advance(parent context.Context, req SourcePreparationRequest) (monthlysource.FundingMonthCaptureProgress, error) {
	if p == nil || parent == nil {
		return monthlysource.FundingMonthCaptureProgress{}, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	scope, err := p.scope(ctx, req)
	if err != nil {
		return monthlysource.FundingMonthCaptureProgress{}, err
	}
	_, err = monthlysource.EnsureFundingMonthCapture(ctx, p.capture, req.Key(), scope)
	if err != nil {
		return monthlysource.FundingMonthCaptureProgress{}, fundingPreparationError(err)
	}
	if _, err := p.scope(ctx, req); err != nil {
		return monthlysource.FundingMonthCaptureProgress{}, err
	}
	progress, err := monthlysource.CaptureNextFundingMonthPage(ctx, p.source, p.capture, req.Key(), scope)
	if err != nil {
		return monthlysource.FundingMonthCaptureProgress{}, fundingPreparationError(err)
	}
	if _, err := p.scope(ctx, req); err != nil {
		return monthlysource.FundingMonthCaptureProgress{}, err
	}
	return progress, nil
}

func fundingPreparationError(err error) error {
	switch {
	case errors.Is(err, monthlysource.ErrFundingMonthCaptureIntegrity):
		return errors.Join(ErrGenerationIntegrity, err)
	case errors.Is(err, monthlysource.ErrFundingMonthCaptureCapacity):
		return errors.Join(ErrGenerationCapacity, err)
	case errors.Is(err, monthlysource.ErrFundingRunBusy), errors.Is(err, monthlysource.ErrFundingCaptureBusy):
		return errors.Join(ErrGenerationBusy, err)
	default:
		return err
	}
}

// Before requires a further trusted preparation stage. Finishing both funding
// traversals alone NEVER reports whole-bill readiness: full source audit,
// consumption/token/wallet evidence and existing selection checks still apply.
func (p *FundingSourcePreparation) Before(next SourcePreparationStep) (SourcePreparationStep, error) {
	if p == nil || next == nil {
		return nil, ErrGenerationInput
	}
	// Scheduling hint only, scoped to this runtime and immutable request key.
	// Do not reread every funding page for every following token/day step.
	// Restart discards the hint; final evidence audits remain mandatory.
	var mu sync.Mutex
	done := make(map[string]bool)
	return func(ctx context.Context, req SourcePreparationRequest) (bool, error) {
		mu.Lock()
		finished := done[req.Key()]
		mu.Unlock()
		if finished {
			if _, err := p.scope(ctx, req); err != nil {
				return false, err
			}
			return next(ctx, req)
		}
		progress, err := p.Advance(ctx, req)
		if err != nil || !progress.TraversalsFinished {
			return false, err
		}
		mu.Lock()
		if len(done) < 256 {
			done[req.Key()] = true
		}
		mu.Unlock()
		if progress.Captured {
			return false, nil
		}
		return next(ctx, req)
	}, nil
}
