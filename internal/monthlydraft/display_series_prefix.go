package monthlydraft

import (
	"context"
	"encoding/json"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

// Reduce the configured permission range to the required source prefix. Later
// months with no publication attempt are unrelated. Keep any later publication
// attempt, including an uncertain post-write crash, so the EXISTING main-store
// transaction can still recheck every issued source/link. This never treats a
// journal as proof of money or skips the authoritative publication transaction.
func (g *Generation) automaticDisplayRequiredScope(ctx context.Context, scope AutomaticDisplaySeriesRange, request monthlytasks.Request) (AutomaticDisplaySeriesRange, error) {
	var empty AutomaticDisplaySeriesRange
	if ctx == nil || g == nil || g.receiver == nil || g.tasks == nil || request.Owner.GroupID != scope.GroupID || request.SourceIdentity != scope.SourceIdentity || request.Month < scope.From || request.Month > scope.Through {
		return empty, ErrGenerationInput
	}
	type retainedAttempt struct {
		id, key, result string
		request         monthlytasks.Request
	}
	rows, err := g.receiver.QueryContext(ctx, `SELECT p.task_id,p.task_key,p.result_hash,
CASE WHEN length(t.descriptor)<=2048 THEN t.descriptor END
FROM portal_monthly_publication_jobs p JOIN portal_monthly_draft_tasks t ON t.id=p.task_id
WHERE t.group_id=? ORDER BY p.task_id LIMIT ?`, scope.GroupID, generationRecordLimit+1)
	if err != nil {
		return empty, err
	}
	defer rows.Close()
	var attempts []retainedAttempt
	for rows.Next() {
		var attempt retainedAttempt
		var raw []byte
		if err := rows.Scan(&attempt.id, &attempt.key, &attempt.result, &raw); err != nil {
			return empty, err
		}
		if json.Unmarshal(raw, &attempt.request) != nil || generationHash(raw) != attempt.key {
			return empty, ErrGenerationIntegrity
		}
		attempts = append(attempts, attempt)
		if len(attempts) > generationRecordLimit {
			return empty, ErrGenerationCapacity
		}
	}
	if err := rows.Err(); err != nil {
		return empty, err
	}
	if err := rows.Close(); err != nil {
		return empty, err
	}
	through := request.Month
	for _, attempt := range attempts {
		// Inspect the original task under its own retained owner, not today's
		// auth revision. Live authorization is separately checked on the target.
		task, err := g.tasks.Get(ctx, attempt.request.Owner, attempt.id)
		if err != nil || task.Key != attempt.key || task.ResultSHA256 != attempt.result || task.Request != attempt.request || task.State != monthlytasks.DraftReady {
			return empty, ErrGenerationIntegrity
		}
		old := task.Request
		if old.Owner.GroupID != scope.GroupID || old.SourceIdentity != scope.SourceIdentity || !currentGenerationRules(old) || old.Month < scope.From || old.Month > scope.Through {
			return empty, ErrGenerationChanged
		}
		if old.Month > through {
			through = old.Month
		}
	}
	scope.Through = through
	return scope, ctx.Err()
}
