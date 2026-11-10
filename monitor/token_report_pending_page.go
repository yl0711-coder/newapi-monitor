package monitor

import "gorm.io/gorm"

func tokenReportIncludePending(q tokenDetectionQuery) bool {
	return q.Status == "" && q.Rule == "" && q.Evidence == "" && q.Method == "" && q.Subject == "" && q.Unit == "" && q.QuotaPerUnit == "" && !q.Unavailable
}

func tokenReportPendingPage(tx *gorm.DB, q tokenDetectionQuery) ([]TokenDetectionCurrent, error) {
	var rows []TokenDetectionCurrent
	query := tx.Table(tokenReportPendingMembers+" INDEXED BY idx_token_report_pending_no_head_v1").
		Select("source_epoch,log_id,created_at,user_id,channel_id,model,user_group,0 AS fact_revision,0 AS link_revision,'' AS detection_state,'{}' AS result_json").
		Where("source_epoch=? AND head_present=0 AND created_at>=? AND created_at<?", q.SourceEpoch, q.From, q.To)
	if q.AfterAt > 0 {
		comparison := ">"
		if q.NewestFirst {
			comparison = "<"
		}
		query = query.Where("(created_at,log_id)"+comparison+"(?,?)", q.AfterAt, q.AfterID)
	}
	if q.UserSet || q.UserID > 0 {
		query = query.Where("user_id=?", q.UserID)
	}
	if q.ChannelSet || q.ChannelID > 0 {
		query = query.Where("channel_id=?", q.ChannelID)
	}
	if q.ModelSet || q.Model != "" {
		query = query.Where("model=?", q.Model)
	}
	if q.GroupSet || q.Group != "" {
		query = query.Where("user_group=?", q.Group)
	}
	order := "created_at,log_id"
	if q.NewestFirst {
		order = "created_at DESC,log_id DESC"
	}
	err := query.Order(order).Limit(q.Limit + 1).Find(&rows).Error
	return rows, err
}
