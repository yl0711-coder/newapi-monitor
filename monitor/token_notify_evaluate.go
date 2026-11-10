package monitor

import (
	"encoding/json"
	"errors"
	"math/big"
	"sort"

	"gorm.io/gorm"
)

type tokenNotifyPayload struct {
	Version       string                `json:"version"`
	PolicyVersion string                `json:"policy_version"`
	SourceEpoch   string                `json:"source_epoch"`
	ChannelID     int64                 `json:"channel_id"`
	Model         string                `json:"model"`
	Group         string                `json:"group"`
	Rule          string                `json:"rule"`
	From          int64                 `json:"from"`
	To            int64                 `json:"to"`
	Count         int64                 `json:"candidate_requests"`
	Customers     int                   `json:"customers"`
	Severity      string                `json:"severity"`
	Evidence      string                `json:"evidence_level"`
	Summary       tokenDetectionSummary `json:"summary"`
	Gaps          []string              `json:"gaps"`
}
type tokenNotifyGroup struct {
	payload      tokenNotifyPayload
	rows         []TokenDetectionCurrent
	fingerprints []string
	amount       *big.Rat
}

func tokenNotifyHit(r tokenDetectionResult, rule tokenNotifyRule) (tokenDetectionRule, bool) {
	for _, v := range r.Rules {
		if v.Name == rule.Name && v.Hit && tokenEvidenceRank(v.EvidenceLevel) >= tokenEvidenceRank(rule.Evidence) {
			return v, true
		}
	}
	return tokenDetectionRule{}, false
}
func tokenNotifyMoney(r tokenDetectionResult, method string) *string {
	switch method {
	case "observed_consumption_not_loss":
		return r.ActualQuota
	case "historical_formula_v1":
		return r.FormulaDifference
	case "input_upper_reference_v1":
		return r.EstimatedLoss
	}
	return nil
}
func tokenNotifyGroups(rows []TokenDetectionCurrent, p tokenNotifyPolicy, now int64) (map[string]tokenNotifyGroup, error) {
	result := map[string]tokenNotifyGroup{}
	for _, rule := range p.Rules {
		groups := map[string]tokenNotifyGroup{}
		for _, h := range rows {
			var r tokenDetectionResult
			if json.Unmarshal([]byte(h.ResultJSON), &r) != nil {
				return nil, errors.New("notification result invalid")
			}
			hit, ok := tokenNotifyHit(r, rule)
			if !ok || !r.Candidate {
				continue
			}
			amount := new(big.Rat)
			if rule.MinQuota != "" {
				v := tokenNotifyMoney(r, rule.AmountMethod)
				if v == nil || r.Price.Subject != rule.Subject || r.Price.Unit != rule.Unit || r.Price.QuotaPerUnit != rule.QuotaPerUnit {
					continue
				}
				n, valid := amount.SetString(*v)
				if !valid {
					continue
				}
				amount = n
			}
			key := tokenDetectionHash([]any{h.SourceEpoch, h.ChannelID, h.Model, h.UserGroup, rule.Name})
			g, ok := groups[key]
			if !ok {
				g = tokenNotifyGroup{amount: new(big.Rat), payload: tokenNotifyPayload{Version: tokenDeliveryVersion, PolicyVersion: p.Version, SourceEpoch: h.SourceEpoch, ChannelID: h.ChannelID, Model: h.Model, Group: h.UserGroup, Rule: rule.Name, From: now - p.WindowSeconds, To: now + 1, Severity: rule.Severity, Evidence: hit.EvidenceLevel, Gaps: []string{"candidate_not_confirmed_overcharge", "supplier_wallet_unavailable"}}}
			}
			if tokenEvidenceRank(hit.EvidenceLevel) < tokenEvidenceRank(g.payload.Evidence) {
				g.payload.Evidence = hit.EvidenceLevel
			}
			g.rows = append(g.rows, h)
			g.fingerprints = append(g.fingerprints, tokenDetectionHash([]any{h.LogID, h.Revision, h.DecisionHash}))
			g.amount.Add(g.amount, amount)
			groups[key] = g
		}
		for key, g := range groups {
			if len(g.rows) < rule.MinRequests || rule.MinQuota != "" && g.amount.Cmp(tokenRatio(rule.MinQuota)) < 0 {
				continue
			}
			s, err := tokenDetectionSummarize(g.rows)
			if err != nil {
				return nil, err
			}
			g.payload.Count = int64(len(g.rows))
			g.payload.Customers = s.Dimensions["customers"]
			g.payload.Summary = s
			for _, row := range g.rows {
				if !row.SourceComplete {
					g.payload.Gaps = append(g.payload.Gaps, "source_window_incomplete")
					break
				}
			}
			if old, ok := result[key]; !ok || g.payload.Severity < old.payload.Severity {
				result[key] = g
			}
		}
	}
	return result, nil
}
func tokenNotifyOutboxActive(state string) bool {
	switch state {
	case "pending", "paused", "retry", "uncertain", "sending":
		return true
	default:
		return false
	}
}

func tokenNotifyOutboxTerminal(state string) bool {
	switch state {
	case "sent", "failed", "expired", "superseded":
		return true
	default:
		return false
	}
}

func tokenNotifyEnqueue(tx *gorm.DB, key string, g tokenNotifyGroup, p tokenNotifyPolicy, now int64) error {
	sort.Strings(g.fingerprints)
	fingerprint := tokenDetectionHash([]any{p, g.payload.Severity, g.payload.Evidence, g.fingerprints})
	policyHash := tokenDetectionHash(p)
	var target tokenNotifyTarget
	if err := tx.Where("id = ?", key).Limit(1).Find(&target).Error; err != nil {
		return err
	}
	var latest TokenNotifyOutbox
	if err := tx.Where("target_key = ? AND fingerprint = ? AND policy_hash = ?", key, fingerprint, policyHash).
		Order("created_at DESC, id DESC").Limit(1).Find(&latest).Error; err != nil {
		return err
	}
	upgraded := target.Severity != "" && (g.payload.Severity < target.Severity || tokenEvidenceRank(g.payload.Evidence) > tokenEvidenceRank(target.Evidence) || target.SentCount > 0 && g.payload.Count >= target.SentCount*int64(p.EscalationFactor))
	lastSentAt := target.LastSentAt
	if latest.SentAt > lastSentAt {
		lastSentAt = latest.SentAt
	}
	if tokenNotifyOutboxActive(latest.State) {
		return nil
	}
	if lastSentAt > 0 && now < lastSentAt+p.CooldownSeconds && !upgraded {
		return nil
	}
	if latest.State == "failed" || latest.State == "expired" {
		nextAt := latest.NextAt
		if nextAt == 0 {
			nextAt = latest.CreatedAt + 1800
		}
		if now < nextAt {
			return nil
		}
	}
	reuse := latest.ID != "" && tokenNotifyOutboxTerminal(latest.State)
	body := tokenDetectionJSON(g.payload)
	if len(body) > 128<<10 {
		return errTokenDeliveryCapacity
	}
	var total, active int64
	if err := tx.Model(&TokenNotifyOutbox{}).Count(&total).Error; err != nil {
		return err
	}
	if err := tx.Model(&TokenNotifyOutbox{}).Where("state IN ?", []string{"pending", "paused", "retry", "uncertain", "sending"}).Count(&active).Error; err != nil {
		return err
	}
	if (!reuse && total >= 5000) || active >= 1000 {
		return errTokenDeliveryCapacity
	}
	if target.ID == "" {
		var n int64
		if err := tx.Model(&tokenNotifyTarget{}).Count(&n).Error; err != nil {
			return err
		}
		if n >= 10000 {
			return errTokenDeliveryCapacity
		}
	}
	var size int64
	if err := tx.Model(&TokenNotifyOutbox{}).Select("COALESCE(SUM(length(payload)),0)").Scan(&size).Error; err != nil {
		return err
	}
	if reuse {
		size -= int64(len(latest.Payload))
		if size < 0 {
			size = 0
		}
	}
	if size+int64(len(body)) > 16<<20 {
		return errTokenDeliveryCapacity
	}
	// Only never-sent work is superseded. A sending lease is not disturbed.
	if err := tx.Model(&TokenNotifyOutbox{}).Where("target_key = ? AND state IN ?", key, []string{"pending", "paused", "retry", "uncertain"}).Updates(map[string]any{"state": "superseded", "reason": "new_frozen_summary"}).Error; err != nil {
		return err
	}
	var row TokenNotifyOutbox
	if reuse {
		row = latest
		updated := tx.Model(&TokenNotifyOutbox{}).Where("id = ? AND state = ?", latest.ID, latest.State).Updates(map[string]any{
			"target_key": key, "policy_hash": policyHash, "fingerprint": fingerprint,
			"severity": g.payload.Severity, "evidence": g.payload.Evidence, "count": g.payload.Count,
			"state": "pending", "reason": "terminal_notification_requeued", "created_at": now,
			"expires_at": now + p.MaxAgeSeconds, "next_at": now, "attempts": 0,
			"lease_token": "", "lease_until": 0, "sent_at": 0, "payload": body,
		})
		if updated.Error != nil {
			return updated.Error
		}
		if updated.RowsAffected != 1 {
			return errors.New("notification terminal row changed during requeue")
		}
		row.TargetKey, row.PolicyHash, row.Fingerprint = key, policyHash, fingerprint
		row.Severity, row.Evidence, row.Count = g.payload.Severity, g.payload.Evidence, g.payload.Count
		row.State, row.Reason, row.CreatedAt = "pending", "terminal_notification_requeued", now
		row.ExpiresAt, row.NextAt, row.Attempts, row.LeaseToken, row.LeaseUntil, row.SentAt, row.Payload = now+p.MaxAgeSeconds, now, 0, "", 0, 0, body
	} else {
		id := tokenDetectionHash([]string{key, fingerprint, policyHash})
		// The deterministic ID is unique for a first enqueue. A conflict is
		// returned to the caller instead of being silently swallowed.
		row = TokenNotifyOutbox{ID: id, TargetKey: key, PolicyHash: policyHash, Fingerprint: fingerprint, Severity: g.payload.Severity, Evidence: g.payload.Evidence, Count: g.payload.Count, State: "pending", CreatedAt: now, ExpiresAt: now + p.MaxAgeSeconds, NextAt: now, Payload: body}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
	}
	target.ID, target.Fingerprint, target.UpdatedAt = key, fingerprint, now
	if err := tx.Save(&target).Error; err != nil {
		return err
	}
	return tokenDeliveryAudit(tx, "worker", "notification_queued", row.ID, "frozen_local_candidate_summary", now)
}
func (m *Monitor) evaluateTokenNotifications(tx *gorm.DB, p tokenNotifyPolicy, now int64) error {
	policy, err := parseTokenDetectionPolicy(m.cfg.TokenDetectionPolicy)
	if err != nil {
		return errors.New("detection policy unavailable")
	}
	var rows []TokenDetectionCurrent
	query := tx.Table("token_detection_currents c").Select("c.*").
		Joins("JOIN token_metering_facts f ON f.source_epoch=c.source_epoch AND f.log_id=c.log_id").
		Joins("LEFT JOIN token_detection_buckets b ON b.id=c.reference_bucket").
		Joins("LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300").
		Where("c.source_epoch = ? AND c.candidate = 1 AND c.created_at >= ? AND c.created_at <= ? AND c.policy_hash = ? AND c.fact_revision=f.revision AND c.link_revision=f.link_revision AND c.reference_generation=COALESCE(b.generation,0) AND c.source_complete=CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END", m.cfg.TokenFactsSourceEpoch, now-p.WindowSeconds, now, tokenDetectionPolicyHash(policy)).
		Order("c.created_at,c.log_id").Limit(5001)
	if err := query.Find(&rows).Error; err != nil {
		return err
	}
	if len(rows) > 5000 {
		return errTokenDeliveryCapacity
	}
	groups, err := tokenNotifyGroups(rows, p, now)
	if err != nil {
		return err
	}
	if p.TelemetryLagSeconds > 0 && m.cfg.TokenFactsEnabled {
		var state TokenMeteringState
		if err := tx.Where("source_epoch = ?", m.cfg.TokenFactsSourceEpoch).Limit(1).Find(&state).Error; err != nil {
			return err
		}
		if state.SourceEpoch != "" && state.LastSuccessAt > 0 && now-state.LastSuccessAt >= p.TelemetryLagSeconds {
			key := tokenDetectionHash([]string{"telemetry_gap", state.SourceEpoch})
			groups[key] = tokenNotifyGroup{fingerprints: []string{tokenDetectionHash([]any{state.LastSuccessAt, state.LastReason})}, payload: tokenNotifyPayload{Version: tokenDeliveryVersion, PolicyVersion: p.Version, SourceEpoch: state.SourceEpoch, Rule: "telemetry_gap", From: state.LastSuccessAt, To: now, Severity: "SEV3", Evidence: "unavailable", Gaps: []string{"collection_delayed_no_health_conclusion"}}}
		}
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	// The transaction is all-or-nothing: no partial evaluation is called complete.
	if len(keys) > 100 {
		return errTokenDeliveryCapacity
	}
	for _, key := range keys {
		if err := tokenNotifyEnqueue(tx, key, groups[key], p, now); err != nil {
			return err
		}
	}
	return nil
}
