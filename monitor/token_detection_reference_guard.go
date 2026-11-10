package monitor

import "gorm.io/gorm"

// Only small identity/version tuples are reread under the writer. Changes to
// unrelated bucket generations do not invalidate an otherwise identical input.
// The query limits match reference construction, including its overflow row.
type tokenReferenceDependency struct {
	LogID, Revision, LinkRevision int64
	Quarantined                   bool
}

type tokenReferenceGuard struct {
	Bucket                         string
	From, Through                  int64
	Anchor                         TokenDetectionReference
	Candidates, Additions, Samples []tokenReferenceDependency
}

func tokenReferenceDependencies(tx *gorm.DB, in tokenDetectionInput, p tokenDetectionPolicy, ref tokenReference) (tokenReferenceGuard, error) {
	g := tokenReferenceGuard{Bucket: ref.Bucket, From: ref.From, Through: ref.Through}
	if !tokenComparable(in) {
		return g, nil
	}
	f := in.Fact
	if len(ref.Search) > 0 {
		p.SizeTolerance = ref.Search[len(ref.Search)-1].Tolerance
	}
	if err := tx.Select("id,bucket,through,recorded_at,expires_at,invalidated_at,invalid_reason,stored_bytes").Where("bucket=? AND through<=?", ref.Bucket, f.CreatedAt).Order("through,id").Limit(1).Find(&g.Anchor).Error; err != nil {
		return g, err
	}
	query := func() *gorm.DB {
		return tokenReferenceCandidateQuery(tx, f, p, ref.From, ref.Through).
			Select("f.log_id,f.revision,f.link_revision,COALESCE(c.quarantined,0) AS quarantined").
			Joins("LEFT JOIN token_detection_currents c ON c.source_epoch=f.source_epoch AND c.log_id=f.log_id")
	}
	if err := query().Scan(&g.Candidates).Error; err != nil {
		return g, err
	}
	if g.Anchor.ID != "" {
		if err := query().Where("f.created_at>?", g.Anchor.Through).Limit(65).Scan(&g.Additions).Error; err != nil {
			return g, err
		}
	}
	ids := make([]int64, 0, len(ref.Samples))
	for _, s := range ref.Samples {
		ids = append(ids, s.LogID)
	}
	if len(ids) > 0 {
		if err := tokenReferenceFactIdentities(tx, f.SourceEpoch, ids).Select("f.log_id,f.revision,f.link_revision,COALESCE(c.quarantined,0) AS quarantined").
			Joins("LEFT JOIN token_detection_currents c ON c.source_epoch=f.source_epoch AND c.log_id=f.log_id").
			Order("f.log_id").Scan(&g.Samples).Error; err != nil {
			return g, err
		}
	}
	return g, nil
}

// Sustained-cache rules only consume a threshold, never an unbounded count.
func tokenCachePriorSignals(tx *gorm.DB, f TokenMeteringFact, bucket string, p tokenDetectionPolicy) (int64, error) {
	var ids []struct{ LogID int64 }
	err := tx.Model(&TokenDetectionCurrent{}).Select("log_id").Where("source_epoch=? AND channel_id=? AND bucket=? AND created_at>=? AND created_at<? AND cache_signal=1", f.SourceEpoch, f.ChannelID, bucket, f.CreatedAt-p.LookbackSeconds, f.CreatedAt).Limit(p.CacheMinSustained).Find(&ids).Error
	return int64(len(ids)), err
}
