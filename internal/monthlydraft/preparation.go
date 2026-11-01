package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// PreparationResult is operator-only metadata. No customer money, source paths,
// approval or task state is inferred from a successful preparation check.
type PreparationResult struct {
	Mode              string `json:"mode"`
	GroupID           int64  `json:"group_id"`
	Month             string `json:"month"`
	PlanSHA256        string `json:"plan_sha256"`
	PreparationSHA256 string `json:"preparation_sha256"`
	PreviousHead      string `json:"previous_head"`
	SelectionHead     string `json:"selection_head"`
	Applied           bool   `json:"applied"`
	Changed           bool   `json:"changed"`
}

// PrepareGenerationInput checks one exact operator-selected group/month. The
// plan must be canonical machine-produced JSON; ambiguous/unknown fields fail
// closed. Default inspection opens every DB read-only and starts no worker.
// Apply retains inputs and selects them atomically in an EXISTING receiver;
// it neither initializes schemas nor submits, approves or publishes a bill.
// Financial evidence semantics still require the separate review procedure.
func PrepareGenerationInput(parent context.Context, cfg DeploymentConfig, planRaw, preparation []byte, expectedHead string, apply bool) (result PreparationResult, err error) {
	if parent == nil || !cfg.Rollout.Enabled || len(cfg.Rollout.Targets) != 1 ||
		len(planRaw) == 0 || len(planRaw) > generationPlanLimit || len(preparation) > monitor.MonthlyPreparationInputLimit ||
		(expectedHead != "" && !generationHex(expectedHead, 32)) {
		return result, ErrGenerationInput
	}
	preparation = bytes.Clone(preparation)
	var plan GenerationPlan
	if json.Unmarshal(planRaw, &plan) != nil {
		return result, ErrGenerationInput
	}
	p, canonical, input, err := canonicalGeneration(plan, preparation, time.Now())
	if err != nil {
		return result, err
	}
	if !bytes.Equal(bytes.TrimSpace(planRaw), canonical) || cfg.Rollout.Targets[0] != (GenerationTarget{GroupID: p.Owner.GroupID, Month: p.Funding.Month}) {
		return result, ErrGenerationInput
	}
	select {
	case generationSlots <- struct{}{}:
		defer func() { <-generationSlots }()
	default:
		return result, ErrGenerationBusy
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	d, err := openDeployment(ctx, cfg, !apply)
	if err != nil {
		return result, err
	}
	defer func() {
		// No worker was started, so direct resource closure is safe. Preserve an
		// applied receipt if closure fails; never imply that a committed write
		// was rolled back merely because cleanup/reporting failed.
		err = errors.Join(err, d.closeResources())
	}()
	return prepareGenerationInputOnDeployment(ctx, d, p, canonical, input, preparation, expectedHead, apply)
}

// Both offline preparation and the running resource owner use the same financial
// and compare-and-swap checks. This helper never opens/closes a deployment and
// must run under either the offline process lock or a registered runtime call.
func prepareGenerationInputOnDeployment(ctx context.Context, d *Deployment, p GenerationPlan, canonical []byte,
	input monitor.MonthlyPreparationInput, preparation []byte, expectedHead string, apply bool) (PreparationResult, error) {
	var result PreparationResult
	auth, err := gorm.Open(&sqlite.Dialector{Conn: d.authority}, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent), DisableAutomaticPing: true})
	if err != nil {
		return result, err
	}
	checkOwner := func(ctx context.Context) error {
		owner, _, err := LoadUsageGenerationOwner(ctx, auth, p.Owner.GroupID, p.Owner.AuthVersion)
		if err != nil {
			return err
		}
		if owner != p.Owner {
			return ErrGenerationChanged
		}
		return ctx.Err()
	}
	if err := checkOwner(ctx); err != nil {
		return result, err
	}
	request := generationRequest(p, canonical)
	checkSelection := func() (GenerationSelection, error) {
		current, seen, err := readGenerationSelection(ctx, d.receiver, p.Owner.GroupID, p.Funding.Month)
		if err != nil {
			return current, err
		}
		if current.Head != expectedHead || (seen[request] && (current.Request == nil || *current.Request != request)) {
			return current, ErrGenerationSelectionConflict
		}
		if current.Request != nil && *current.Request == request {
			if _, err := loadGeneration(ctx, d.receiver, request, time.Now()); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					err = ErrGenerationIntegrity
				}
				return current, err
			}
		}
		return current, nil
	}
	if _, err := checkSelection(); err != nil {
		return result, err
	}
	g, releaseSource, err := d.g.bindObservationStore(ctx, p, input)
	if err != nil {
		return result, err
	}
	defer releaseSource()
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return result, err
	}
	windows, err := g.auditWindows(ctx, p)
	if err != nil {
		return result, err
	}
	if _, err := g.auditUsageInput(ctx, p, input); err != nil {
		return result, err
	}
	if _, err := g.auditWalletInput(ctx, p, input); err != nil {
		return result, err
	}
	if _, err := g.auditTokenInput(ctx, p, input); err != nil {
		return result, err
	}
	review, err := ReconcileObservedMonth(ctx, p.Funding, windows, preparation, time.Now())
	if err != nil {
		return result, err
	}
	proof, err := g.selectedGenerationReview(ctx, p, preparation, windows, review.Reconciliation)
	if err != nil {
		return result, err
	}
	if !generationArtifactReconciled(p, review.Reconciliation, proof) {
		return result, ErrGenerationEvidence
	}
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return result, err
	}
	if err := checkOwner(ctx); err != nil {
		return result, err
	}
	current, err := checkSelection()
	if err != nil {
		return result, err
	}
	result = PreparationResult{Mode: "checked_snapshot_not_approval", GroupID: p.Owner.GroupID, Month: p.Funding.Month,
		PlanSHA256: generationHash(canonical), PreparationSHA256: p.PreparationSHA256,
		PreviousHead: current.Head, SelectionHead: current.Head}
	if !apply {
		return result, nil
	}
	selected, err := stageGenerationChecked(ctx, d.receiver, p, preparation, expectedHead, checkOwner)
	if err != nil {
		return PreparationResult{}, err
	}
	result.Mode = "selected_input_not_publication"
	result.Applied = true
	result.SelectionHead = selected.Head
	result.Changed = selected.Head != current.Head
	return result, nil
}
