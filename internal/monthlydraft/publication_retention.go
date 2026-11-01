package monthlydraft

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

// Read in the SAME retention snapshot. Journal changes invalidate an older
// archive plan. Legacy draft-only receivers with neither table remain readable.
func inspectPublicationRetention(ctx context.Context, tx *sql.Tx, tasks []monthlytasks.TaskObservation, encoder *json.Encoder) (map[string]publicationRecord, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('portal_monthly_publication_control','portal_monthly_publication_jobs')`).Scan(&count); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	if count != 2 {
		return nil, ErrGenerationIntegrity
	}
	if err := publicationVersion(ctx, tx); err != nil {
		return nil, err
	}
	if err := encoder.Encode("publication-recovery-schema-v1"); err != nil {
		return nil, err
	}
	byID := make(map[string]monthlytasks.Task, len(tasks))
	for _, t := range tasks {
		byID[t.Task.ID] = t.Task
	}
	rows, err := tx.QueryContext(ctx, `SELECT task_id,task_key,result_hash,state,attempts,updated_at,lease_until,lease_token,retry_at,evidence_hash FROM portal_monthly_publication_jobs ORDER BY task_id LIMIT 257`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	protected := map[string]publicationRecord{}
	seen := 0
	for rows.Next() {
		var r publicationRecord
		if err := rows.Scan(&r.id, &r.key, &r.result, &r.state, &r.attempts, &r.updated, &r.until, &r.token, &r.retry, &r.evidence); err != nil {
			return nil, err
		}
		seen++
		if seen > 256 {
			return nil, ErrGenerationCapacity
		}
		t, exists := byID[r.id]
		if validatePublicationRecord(r) != nil || !exists || t.Key != r.key ||
			(t.State != monthlytasks.DraftReady && t.State != monthlytasks.Invalidated) ||
			(t.State == monthlytasks.DraftReady && t.ResultSHA256 != r.result) {
			return nil, ErrGenerationIntegrity
		}
		// Hash recovery metadata, not customer money or an exposed lease token.
		if err := encoder.Encode([]any{"publication-v1", r.id, r.key, r.result, r.state, r.attempts, r.updated, r.until, generationHash([]byte(r.token)), r.retry, r.evidence}); err != nil {
			return nil, err
		}
		if t.State == monthlytasks.DraftReady && r.state != publicationSaved {
			protected[r.id] = r
		}
	}
	return protected, rows.Err()
}

func protectPublicationInput(item *RetentionItem, publications map[string]publicationRecord, asOf time.Time) {
	for _, id := range item.TaskIDs {
		r, exists := publications[id]
		if !exists {
			continue
		}
		if r.updated > item.LastActivityMS {
			item.LastActivityMS = r.updated
		}
		if item.Disposition == "manual_review" {
			continue
		}
		switch {
		case r.updated > asOf.UnixMilli():
			item.Disposition, item.Reason = "manual_review", "publication_activity_is_in_the_future"
		case r.state == publicationPaused:
			item.Disposition, item.Reason = "manual_review", "publication_requires_review"
		case r.state == publicationRunning:
			item.Disposition, item.Reason = "keep_active", "publication_running_even_if_lease_expired"
		case r.state == publicationRetry && item.Disposition != "keep_active":
			item.Disposition, item.Reason = "keep_retryable", "publication_attempt_budget_remaining"
		}
	}
}
