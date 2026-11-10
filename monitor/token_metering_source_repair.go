package monitor

import "gorm.io/gorm"

// Invalid expressions were deliberately not retained by older parsers. Renaming
// that parser cannot recover the source operands: schedule the existing bounded
// window replay and let its normal revision/link/queue triggers publish changes.
func tokenMeteringNeedsSourceReplay(f TokenMeteringFact, p tokenMeteringParsed) bool {
	return f.ParserVersion != tokenMeteringParserVersion &&
		(p.Fields["expr_b64"].State == "invalid" || p.Fields["request_rules"].State == "invalid" || p.Fields["response_model"].State == "invalid")
}

func tokenMeteringRequestSourceReplay(tx *gorm.DB, f TokenMeteringFact, now int64) error {
	return tx.Exec(`INSERT OR IGNORE INTO token_metering_source_repairs_v1(source_epoch,start_at,requested_at) VALUES(?,?,?)`, f.SourceEpoch, f.CreatedAt/300*300, now).Error
}

func tokenMeteringRecentSourceRepair(tx *gorm.DB, state TokenMeteringState, now int64) (TokenMeteringWindow, bool, error) {
	var start int64
	err := tx.Raw(`SELECT r.start_at FROM token_metering_source_repairs_v1 r LEFT JOIN token_metering_windows w
	 ON w.source_epoch=r.source_epoch AND w.start_at=r.start_at
	 WHERE r.source_epoch=? AND r.start_at>=? AND r.start_at<=? AND ((w.done=1 AND w.last_reason='') OR COALESCE(w.next_at,0)<=?)
	 ORDER BY r.start_at DESC LIMIT 1`, state.SourceEpoch, now-3*3600, now/300*300, now).Scan(&start).Error
	if err != nil || start == 0 {
		return TokenMeteringWindow{}, false, err
	}
	var w TokenMeteringWindow
	found := tx.Where("source_epoch=? AND start_at=?", state.SourceEpoch, start).Limit(1).Find(&w)
	if found.Error != nil {
		return w, false, found.Error
	}
	if found.RowsAffected == 0 {
		w = TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: start, EndAt: start + 300, CursorAt: start}
	}
	if !w.Replay {
		w.CursorAt, w.CursorID = w.StartAt, 0
		w.NextAt = 0
	}
	w.Replay, w.Done, w.SourceComplete = true, false, false
	return w, true, nil
}
