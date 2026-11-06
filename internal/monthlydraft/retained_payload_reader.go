package monthlydraft

import (
	"context"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

// Read-only compatibility for consumers of already completed inputs. Missing
// hot data is NOT a reason to restore, recreate a task, retry or change money.
// Only an existing verified archive receipt may select cold bytes. Admission
// paths deliberately retain their separate no-reconstruction rules.
func (g *Generation) loadRetainedGeneration(parent context.Context, request monthlytasks.Request, now time.Time) (generationRetained, error) {
	var empty generationRetained
	if parent == nil || g == nil || g.receiver == nil || g.tasks == nil || g.verifyCurrent == nil {
		return empty, ErrGenerationInput
	}
	saved, err := loadGeneration(parent, g.receiver, request, now)
	if !errors.Is(err, ErrGenerationArchived) {
		if err != nil {
			return empty, err
		}
		return saved, nil // Never mask a corrupt present payload with an archive.
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	if err := g.current(ctx, request); err != nil {
		return empty, err
	}
	before, err := g.retainedReadyTask(ctx, request)
	if err != nil {
		return empty, err
	}
	saved, err = g.loadArchivedReady(ctx, before)
	if err != nil {
		return empty, err
	}
	if err := g.current(ctx, request); err != nil {
		return empty, err
	}
	after, err := g.retainedReadyTask(ctx, request)
	if err != nil {
		return empty, err
	}
	if after != before {
		return empty, ErrGenerationChanged
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return saved, nil
}

// Only bounded receiver metadata is read here, BEFORE filesystem work and
// without retaining a SQLite transaction. Store.Get validates the original
// descriptor, owner, lease/state invariants and task key; SQL matching is only
// a lookup hint, never permission to borrow another organization's archive.
func (g *Generation) retainedReadyTask(ctx context.Context, request monthlytasks.Request) (monthlytasks.Task, error) {
	var empty monthlytasks.Task
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := g.receiver.QueryContext(readCtx, `SELECT id FROM portal_monthly_draft_tasks WHERE group_id=? AND json_extract(CAST(descriptor AS TEXT),'$.input_sha256')=? LIMIT 2`, request.Owner.GroupID, request.InputSHA256)
	if err != nil {
		return empty, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return empty, err
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return empty, err
	}
	if len(ids) != 1 {
		return empty, ErrGenerationIntegrity
	}
	task, err := g.tasks.Get(readCtx, request.Owner, ids[0])
	if err != nil {
		return empty, err
	}
	if task.Request != request || task.State != monthlytasks.DraftReady || !generationHex(task.ResultSHA256, 32) {
		return empty, ErrGenerationIntegrity
	}
	return task, nil
}
