package monitor

import (
	"errors"

	"gorm.io/gorm"
)

// A legacy NULL and a stored Go zero are different to the queue's SQL
// predicates. Preserve the former Save behavior on nullable legacy heads;
// ordinary fully populated heads can use the narrow update below.
type tokenDetectionStoredCurrent struct {
	TokenDetectionCurrent
	HasNullFields bool
}

const tokenDetectionCurrentNullCheck = `reference_bucket IS NULL OR reference_generation IS NULL OR
	created_at IS NULL OR user_id IS NULL OR channel_id IS NULL OR model IS NULL OR user_group IS NULL OR
	fact_revision IS NULL OR link_revision IS NULL OR policy_hash IS NULL OR revision IS NULL OR decision_hash IS NULL OR
	source_complete IS NULL OR bucket IS NULL OR cache_signal IS NULL OR candidate IS NULL OR quarantined IS NULL OR
	detection_state IS NULL OR stage IS NULL OR next_at IS NULL OR first_evaluated_at IS NULL OR evaluated_at IS NULL OR expires_at IS NULL OR result_json IS NULL`

// The caller holds the fenced write transaction. Create a first result directly;
// subsequent evaluations update only values that changed. Rewriting every head
// column also rewrites unrelated identity, time, status and scheduling indexes.
func saveTokenDetectionCurrent(tx *gorm.DB, before *tokenDetectionStoredCurrent, after TokenDetectionCurrent) error {
	if before == nil {
		return tx.Create(&after).Error
	}
	if before.SourceEpoch != after.SourceEpoch || before.LogID != after.LogID {
		return errors.New("token detection identity changed")
	}
	fields := map[string]any{}
	for _, field := range []struct {
		name          string
		before, after any
	}{
		{"reference_bucket", before.ReferenceBucket, after.ReferenceBucket},
		{"reference_generation", before.ReferenceGeneration, after.ReferenceGeneration},
		{"created_at", before.CreatedAt, after.CreatedAt},
		{"user_id", before.UserID, after.UserID},
		{"channel_id", before.ChannelID, after.ChannelID},
		{"model", before.Model, after.Model},
		{"user_group", before.UserGroup, after.UserGroup},
		{"fact_revision", before.FactRevision, after.FactRevision},
		{"link_revision", before.LinkRevision, after.LinkRevision},
		{"policy_hash", before.PolicyHash, after.PolicyHash},
		{"revision", before.Revision, after.Revision},
		{"decision_hash", before.DecisionHash, after.DecisionHash},
		{"source_complete", before.SourceComplete, after.SourceComplete},
		{"bucket", before.Bucket, after.Bucket},
		{"cache_signal", before.CacheSignal, after.CacheSignal},
		{"candidate", before.Candidate, after.Candidate},
		{"quarantined", before.Quarantined, after.Quarantined},
		{"detection_state", before.DetectionState, after.DetectionState},
		{"stage", before.Stage, after.Stage},
		{"next_at", before.NextAt, after.NextAt},
		{"first_evaluated_at", before.FirstEvaluatedAt, after.FirstEvaluatedAt},
		{"evaluated_at", before.EvaluatedAt, after.EvaluatedAt},
		{"expires_at", before.ExpiresAt, after.ExpiresAt},
		{"result_json", before.ResultJSON, after.ResultJSON},
	} {
		if before.HasNullFields || field.before != field.after {
			fields[field.name] = field.after
		}
	}
	if len(fields) == 0 {
		return nil
	}
	saved := tx.Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND log_id=?", after.SourceEpoch, after.LogID).Updates(fields)
	if saved.Error != nil {
		return saved.Error
	}
	if saved.RowsAffected != 1 {
		return errors.New("token detection result disappeared")
	}
	return nil
}
