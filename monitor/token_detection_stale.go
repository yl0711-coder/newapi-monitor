package monitor

import "gorm.io/gorm"

// Page and summary validity use the same version tuple as the worker. Each
// query probes an explicit bounded identity set; it never walks retained facts.
func tokenDetectionStaleIdentities(tx *gorm.DB, epoch, policy string, ids []int64) (map[int64]bool, error) {
	stale := make(map[int64]bool)
	for start := 0; start < len(ids); start += 256 {
		var rows []struct {
			LogID int64
			Stale bool
		}
		err := tx.Raw(`SELECT c.log_id,(COALESCE(c.detection_state,'')='' OR
		 (f.log_id IS NOT NULL AND (c.fact_revision IS NOT f.revision OR c.link_revision IS NOT f.link_revision
		 OR (?<>'' AND COALESCE(c.policy_hash,'')<>?) OR COALESCE(c.reference_generation,-1)<>COALESCE(b.generation,0)
		 OR COALESCE(c.source_complete,-1)<>CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END))) AS stale
		 FROM token_detection_currents c LEFT JOIN token_metering_facts f ON f.source_epoch=c.source_epoch AND f.log_id=c.log_id
		 LEFT JOIN token_detection_buckets b ON b.id=c.reference_bucket
		 LEFT JOIN token_metering_windows w ON w.source_epoch=c.source_epoch AND w.start_at=(c.created_at/300)*300
		 WHERE c.source_epoch=? AND c.log_id IN ?`, policy, policy, epoch, ids[start:min(start+256, len(ids))]).Scan(&rows).Error
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			stale[row.LogID] = row.Stale
		}
	}
	return stale, nil
}

func (m *Monitor) tokenDetectionConfiguredPolicyHash() string {
	p, err := parseTokenDetectionPolicy(m.cfg.TokenDetectionPolicy)
	if err != nil {
		return ""
	}
	return tokenDetectionPolicyHash(p)
}
