package monitor

import (
	"context"
	"time"

	"gorm.io/gorm"
)

type tokenPipelinePressure struct {
	Level             string `json:"level"`
	Reason            string `json:"reason"`
	FactsRows         int64  `json:"facts_rows"`
	FactsBytes        int64  `json:"facts_payload_bytes"`
	MaxFactsRows      int64  `json:"max_facts_rows"`
	MaxFactsBytes     int64  `json:"max_facts_payload_bytes"`
	DetectionRows     int64  `json:"detection_rows"`
	DetectionBytes    int64  `json:"detection_payload_bytes"`
	MaxDetectionRows  int64  `json:"max_detection_rows"`
	MaxDetectionBytes int64  `json:"max_detection_payload_bytes"`
	SoftPercent       int    `json:"soft_watermark_percent"`
	HardPercent       int    `json:"hard_watermark_percent"`
}

func (m *Monitor) tokenPipelinePressure(ctx context.Context) (tokenPipelinePressure, error) {
	p := tokenPipelinePressure{Level: "normal", SoftPercent: 80, HardPercent: 100}
	p.MaxFactsRows, p.MaxFactsBytes = m.cfg.tokenMeteringLimits()
	p.MaxDetectionRows, p.MaxDetectionBytes = m.cfg.tokenDetectionLimits()
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	db := m.tokenReadStore().WithContext(ctx)
	var state TokenMeteringState
	if err := db.Select("fact_count,fact_bytes").Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).Limit(1).Find(&state).Error; err != nil {
		return p, err
	}
	p.FactsRows, p.FactsBytes = state.FactCount, state.FactBytes
	if db.Migrator().HasTable(&tokenDetectionCapacity{}) {
		var capacity tokenDetectionCapacity
		if err := db.Where("id=1").Limit(1).Find(&capacity).Error; err != nil {
			return p, err
		}
		p.DetectionRows, p.DetectionBytes = capacity.CurrentRows, capacity.HeadBytes+capacity.ReferenceBytes+capacity.RevisionBytes
	}
	for _, v := range []struct {
		value, maximum int64
		name           string
	}{{p.FactsRows, p.MaxFactsRows, "facts_rows"}, {p.FactsBytes, p.MaxFactsBytes, "facts_bytes"}, {p.DetectionRows, p.MaxDetectionRows, "detection_rows"}, {p.DetectionBytes, p.MaxDetectionBytes, "detection_bytes"}} {
		if v.maximum > 0 && v.value >= v.maximum {
			p.Level, p.Reason = "hard", v.name
			break
		}
		if v.maximum > 0 && v.value >= v.maximum*4/5 {
			p.Level, p.Reason = "soft", v.name
		}
	}
	return p, nil
}

func tokenMeteringDeferredLane(tx *gorm.DB, epoch, lane, reason string, now int64) error {
	return tx.Exec(`INSERT INTO token_metering_lane_states(source_epoch,lane,last_reason,deferred,updated_at) VALUES(?,?,?,1,?)
	 ON CONFLICT(source_epoch,lane) DO UPDATE SET last_reason=excluded.last_reason,deferred=COALESCE(deferred,0)+1,updated_at=excluded.updated_at`, epoch, lane, reason, now).Error
}
