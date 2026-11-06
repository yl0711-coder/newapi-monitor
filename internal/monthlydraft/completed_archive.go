package monthlydraft

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

// This guard still requires an ephemeral operation permit. A completed input
// also needs a newer ready task in the same organization/month. The maintenance
// transaction additionally validates the newer payload's integrity (SQL alone
// cannot certify that a newer draft is intact).
const completedGenerationDeleteGuard = `CREATE TRIGGER monthly_generation_no_delete BEFORE DELETE ON portal_monthly_generation_inputs
WHEN NOT EXISTS(SELECT 1 FROM portal_monthly_generation_archives WHERE id=OLD.id)
OR NOT EXISTS(SELECT 1 FROM portal_monthly_generation_release_permits WHERE id=OLD.id)
OR NOT EXISTS(SELECT 1 FROM portal_monthly_draft_tasks WHERE json_extract(CAST(descriptor AS TEXT),'$.input_sha256')=OLD.id)
OR EXISTS(SELECT 1 FROM portal_monthly_draft_tasks WHERE json_extract(CAST(descriptor AS TEXT),'$.input_sha256')=OLD.id AND NOT(state='invalidated' OR (state='failed' AND attempts=3) OR state='draft_ready'))
OR EXISTS(SELECT 1 FROM portal_monthly_draft_tasks AS oldtask WHERE json_extract(CAST(oldtask.descriptor AS TEXT),'$.input_sha256')=OLD.id AND oldtask.state='draft_ready'
AND NOT EXISTS(SELECT 1 FROM portal_monthly_draft_tasks AS newer WHERE newer.state='draft_ready' AND newer.group_id=oldtask.group_id
AND json_extract(CAST(newer.descriptor AS TEXT),'$.month')=json_extract(CAST(oldtask.descriptor AS TEXT),'$.month')
AND (newer.updated_at>oldtask.updated_at OR (newer.updated_at=oldtask.updated_at AND newer.id>oldtask.id))))
BEGIN SELECT RAISE(ABORT,'verified archive and protected latest ready draft required'); END`

// InitCompletedGenerationRetention is a second explicit LOCAL opt-in, never a
// startup migration. The v1 entrypoint continues to reject completed eviction.
// The archive reader must be deployed before this mode is ever enabled.
func InitCompletedGenerationRetention(ctx context.Context, db *sql.DB) error {
	return initGenerationRetention(ctx, db, true)
}

// ChangeCompletedGenerationPayload only extends explicit maintenance; it is
// NOT a public customer endpoint or an automatic retention scheduler. It never
// publishes a bill, recalculates money or deletes task metadata. restore=true
// restores exact bytes without altering any task state.
func ChangeCompletedGenerationPayload(ctx context.Context, db *sql.DB, receipt ArchiveReceipt, id string, asOf time.Time, restore bool) error {
	return changeArchivedGenerationPayload(ctx, db, receipt, id, asOf, restore, true)
}

// loadArchivedReady loads a private immutable draft into memory only. Run must
// still perform its current authorization checks, independent evidence checks,
// funding review and final task-state fence before returning any amounts.
func (g *Generation) loadArchivedReady(parent context.Context, task monthlytasks.Task) (generationRetained, error) {
	if task.State != monthlytasks.DraftReady || !generationHex(task.ResultSHA256, 32) {
		return generationRetained{}, ErrGenerationIntegrity
	}
	select {
	case archiveSlots <- struct{}{}:
		defer func() { <-archiveSlots }()
	default:
		return generationRetained{}, ErrGenerationBusy
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	raw, err := archiveReference(ctx, g.receiver, task.Request.InputSHA256)
	if err != nil {
		return generationRetained{}, err
	}
	var receipt ArchiveReceipt
	if len(raw) == 0 || json.Unmarshal(raw, &receipt) != nil {
		return generationRetained{}, ErrGenerationIntegrity
	}
	saved, entry, err := readEvictionArchive(ctx, receipt, task.Request.InputSHA256)
	if err != nil {
		return generationRetained{}, err
	}
	matched := false
	for _, archived := range entry.Tasks {
		if archived.Task == task {
			matched = true
		}
	}
	p, canonical, input, err := canonicalGeneration(saved.plan, saved.inputRaw, time.Now())
	if err != nil || !matched || generationRequest(p, canonical) != task.Request || saved.digest != task.ResultSHA256 || len(saved.artifact) == 0 {
		return generationRetained{}, ErrGenerationIntegrity
	}
	saved.input = input
	return saved, nil
}
