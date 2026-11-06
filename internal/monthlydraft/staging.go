package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"gorm.io/gorm"
)

// StageGenerationVersion atomically retains PRIVATE immutable inputs and selects
// their descriptor. Only trusted source preparation may call it, never customer
// payloads. Registration validates binding, not historical financial truth.
// It writes exclusively to the explicit local receiver; no task is started.
func StageGenerationVersion(ctx context.Context, db *sql.DB, plan GenerationPlan, preparation []byte, expectedHead string) (GenerationSelection, error) {
	return stageGenerationChecked(ctx, db, plan, preparation, expectedHead, nil)
}

func stageGenerationChecked(ctx context.Context, db *sql.DB, plan GenerationPlan, preparation []byte, expectedHead string, check func(context.Context) error) (GenerationSelection, error) {
	preparation = bytes.Clone(preparation)
	p, raw, _, err := canonicalGeneration(plan, preparation, time.Now())
	if err != nil {
		return GenerationSelection{}, err
	}
	request := generationRequest(p, raw)
	return changeGenerationSelectionPrepared(ctx, db, p.Owner.GroupID, p.Funding.Month, &request, expectedHead,
		func(ctx context.Context, tx *sql.Tx, current GenerationSelection) error {
			// A selected version or established task must already retain its input.
			// Never turn lost/corrupt state into an unnoticed reconstruction.
			established := current.Request != nil && *current.Request == request
			tasks, err := monthlytasks.InspectReceiverTasks(ctx, tx)
			if err != nil {
				return err
			}
			for _, observation := range tasks {
				task := observation.Task
				if task.Request.InputSHA256 != request.InputSHA256 {
					continue
				}
				if task.Request != request {
					return ErrGenerationIntegrity
				}
				if task.State == monthlytasks.Invalidated {
					return ErrGenerationChanged
				}
				established = true
			}
			if err := retainGenerationPlan(ctx, tx, raw, preparation, established); err != nil {
				return err
			}
			if check != nil {
				return check(ctx)
			}
			return nil
		})
}

// SubmitUsageMonth is an INTERNAL service entry, not an HTTP handler or customer
// DTO. gid/version MUST come from an authenticated session, not request fields.
// The only customer selection is month: amounts, users, source files and input
// digests are loaded from the server receiver. It does not publish a bill.
func (g *Generation) SubmitUsageMonth(ctx context.Context, authorization *gorm.DB, gid, version int64, month string) (monthlytasks.Task, error) {
	if !validSelectionScope(gid, month) {
		return monthlytasks.Task{}, ErrGenerationInput
	}
	owner, _, err := LoadUsageGenerationOwner(ctx, authorization, gid, version)
	if err != nil {
		return monthlytasks.Task{}, err
	}
	selector, err := GenerationSelector(g.receiver)
	if err != nil {
		return monthlytasks.Task{}, err
	}
	guard, err := NewUsageGenerationGuard(authorization, selector)
	if err != nil {
		return monthlytasks.Task{}, err
	}
	request, err := selector(ctx, gid, month)
	if err != nil {
		return monthlytasks.Task{}, err
	}
	if request.Owner != owner {
		return monthlytasks.Task{}, ErrGenerationChanged
	}
	if err := guard(ctx, request); err != nil {
		return monthlytasks.Task{}, err
	}
	if !currentGenerationRules(request) {
		return monthlytasks.Task{}, ErrGenerationRulesChanged
	}
	saved, err := loadGeneration(ctx, g.receiver, request, time.Now())
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrGenerationIntegrity
	}
	if err != nil {
		return monthlytasks.Task{}, err
	}
	task, err := g.Submit(ctx, saved.plan, saved.inputRaw)
	if err != nil {
		return monthlytasks.Task{}, err
	}
	// The selection/auth may change during admission. Never return success for
	// an obsolete task; transient failure leaves a retryable existing task intact.
	if err := guard(ctx, task.Request); err != nil {
		if errors.Is(err, ErrGenerationChanged) {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			defer cancel()
			err = errors.Join(err, g.tasks.Invalidate(cleanup, owner, task.ID))
		}
		return monthlytasks.Task{}, err
	}
	return task, nil
}
