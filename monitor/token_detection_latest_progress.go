package monitor

import "gorm.io/gorm"

// This bounded sample describes recently collected requests independently of
// outcome filters. The maximum observed event time is not a continuity proof.
type tokenLatestProgress struct {
	CollectedThrough    int64          `json:"collected_through"`
	DetectedThrough     int64          `json:"detected_through"`
	DecisionCommittedAt int64          `json:"decision_committed_at"`
	SampleCount         int            `json:"sample_count"`
	SampleLimit         int            `json:"sample_limit"`
	States              map[string]int `json:"states"`
}

func tokenDetectionLatestProgress(tx *gorm.DB, q tokenDetectionQuery, hash string) (tokenLatestProgress, error) {
	out := tokenLatestProgress{SampleLimit: 100, States: map[string]int{}}
	var facts []TokenMeteringFact
	err := tokenReportPendingScope(tx.Model(&TokenMeteringFact{}), q, "").
		Select("source_epoch,log_id,created_at,revision,link_revision,link_state").Where("created_at>=? AND created_at<?", q.From, q.To).
		Order("created_at DESC,log_id DESC").Limit(out.SampleLimit).Find(&facts).Error
	if err != nil || len(facts) == 0 {
		return out, err
	}
	ids := make([]int64, 0, len(facts))
	for _, f := range facts {
		ids = append(ids, f.LogID)
	}
	stale, err := tokenDetectionStaleIdentities(tx, q.SourceEpoch, hash, ids)
	if err != nil {
		return out, err
	}
	var heads []TokenDetectionCurrent
	if err = tx.Select("log_id,created_at,fact_revision,link_revision,policy_hash,detection_state,evaluated_at").Where("source_epoch=? AND log_id IN ?", q.SourceEpoch, ids).Find(&heads).Error; err != nil {
		return out, err
	}
	byID := map[int64]TokenDetectionCurrent{}
	for _, h := range heads {
		byID[h.LogID] = h
	}
	for _, f := range facts {
		out.SampleCount++
		out.CollectedThrough = max(out.CollectedThrough, f.CreatedAt)
		h := byID[f.LogID]
		state := tokenDetectionPendingStateForFact(f)
		if !stale[f.LogID] && h.FactRevision == f.Revision && h.LinkRevision == f.LinkRevision && h.PolicyHash == hash && h.DetectionState != "" {
			state = h.DetectionState
			out.DetectedThrough = max(out.DetectedThrough, f.CreatedAt)
			out.DecisionCommittedAt = max(out.DecisionCommittedAt, h.EvaluatedAt)
		}
		out.States[state]++
	}
	return out, nil
}
