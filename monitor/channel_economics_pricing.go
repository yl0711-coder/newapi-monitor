package monitor

import (
	"encoding/json"
	"errors"
	"math"

	"gorm.io/gorm"
)

// Cost evidence is hourly, not per request. A ratio effective at the start
// cannot price the entire hour if a different ratio takes effect inside it.
// Keep the point-in-time selector for its other callers; publications use the
// same whole-bucket proof as account bills and internal-test deductions.
func economicsFinanceForHour(tx *gorm.DB, domain string, hour int64) (int64, float64, float64, bool, error) {
	if hour < 0 || hour%3600 != 0 || hour > math.MaxInt64-3600 {
		return 0, 0, 0, false, errors.New("invalid economics pricing hour")
	}
	version, paid, credit, known, err := economicsFinanceAt(tx, domain, hour)
	if err != nil || !known {
		return version, paid, credit, known, err
	}
	var changes []ChannelFinanceVersion
	if err := tx.Where("domain=? AND effective_at>? AND effective_at<?", domain, hour, hour+3600).
		Order("effective_at,version").Limit(financeRechargeCorrectionVersionLimit + 1).Find(&changes).Error; err != nil {
		return 0, 0, 0, false, err
	}
	if len(changes) > financeRechargeCorrectionVersionLimit {
		return 0, 0, 0, false, errors.New("hourly recharge history exceeds safety limit")
	}
	terms := []channelRechargeVersion{{Version: version, EffectiveAt: hour, Paid: paid, Credit: credit, Valid: true}}
	for _, row := range changes {
		var snapshot channelFinanceVersionSnapshot
		err := json.Unmarshal([]byte(row.SnapshotJSON), &snapshot)
		terms = append(terms, channelRechargeVersion{Version: row.Version, EffectiveAt: row.EffectiveAt,
			Paid: snapshot.UpstreamRechargePaid, Credit: snapshot.UpstreamRechargeCredit,
			Valid: err == nil && validChannelFinanceNumber(snapshot.UpstreamRechargePaid) && validChannelFinanceNumber(snapshot.UpstreamRechargeCredit)})
	}
	p, c, status := rechargeTermsForBucket(terms, hour, hour+3600)
	return version, p, c, status == upstreamAdjustedCostComplete, nil
}
