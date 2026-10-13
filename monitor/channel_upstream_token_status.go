package monitor

// Read-only projection for the configuration panel. Binding activation is not
// evidence completeness or cost publication. Never run workers or inspect log
// payloads in this request path; saves continue to return configuration only.

import (
	"context"
	"crypto/hmac"
	"errors"
	"math"

	"gorm.io/gorm"
)

type upstreamTokenProcessingView struct {
	Status      string                     `json:"status"`
	CheckStatus string                     `json:"check_status,omitempty"`
	EffectiveAt int64                      `json:"effective_at,omitempty"`
	RetryAt     int64                      `json:"retry_at,omitempty"`
	UpdatedAt   int64                      `json:"updated_at,omitempty"`
	Recovery    *upstreamTokenRecoveryView `json:"recovery,omitempty"`
}

// Account-level scan metadata only, not per-channel financial coverage.
type upstreamTokenRecoveryView struct {
	Status    string `json:"status"`
	DayTs     int64  `json:"day_ts,omitempty"`
	RetryAt   int64  `json:"retry_at,omitempty"`
	UpdatedAt int64  `json:"updated_at,omitempty"`
}

func (m *Monitor) loadUpstreamTokenConfigWithStatus(ctx context.Context, domain string, channelID int, now int64) (upstreamTokenConfigView, error) {
	domain, err := normalizeUpstreamTokenScope(domain, channelID)
	if err != nil {
		return upstreamTokenConfigView{}, err
	}
	var view upstreamTokenConfigView
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		account, err := readUpstreamTokenScope(tx, domain, channelID)
		if err != nil {
			return err
		}
		view, err = readUpstreamTokenConfig(tx, account, channelID)
		if err != nil {
			return err
		}
		status, statusErr := m.readUpstreamTokenProcessing(tx, account, view, now)
		if statusErr != nil {
			// Missing/inconsistent status metadata must not hide saved intent or
			// expose SQL. Cancellation still fails the request, not a false success.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			status = upstreamTokenProcessingView{Status: "unavailable"}
		}
		view.Processing = &status
		view.AttributionVerified = status.Status == "scheduled" || status.Status == "active" || status.Status == "shared"
		if view.TokenID != "" && view.State != "unsupported" && account.Provider == upstreamProviderSub2API {
			recovery := upstreamTokenRecoveryView{Status: "unavailable"}
			var recoveryErr error
			if status.Status != "unavailable" {
				recovery, recoveryErr = m.readUpstreamTokenRecoveryStatus(tx, account, now)
			}
			if recoveryErr != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				recovery = upstreamTokenRecoveryView{Status: "unavailable"}
			}
			status.Recovery = &recovery
		}
		return nil
	})
	return view, err
}

func (m *Monitor) readUpstreamTokenRecoveryStatus(tx *gorm.DB, scope ChannelUpstreamAccount, now int64) (upstreamTokenRecoveryView, error) {
	p := upstreamTokenRecoveryView{Status: "pending"}
	if now <= 86400 || now > math.MaxInt64-3600 {
		return p, errors.New("invalid recovery status clock")
	}
	// Scope loading deliberately excludes credentials. Load only the fields
	// needed here; credentials are used locally for the opaque revision check,
	// never returned, refreshed, logged or sent to an upstream from this GET.
	var account ChannelUpstreamAccount
	if err := tx.Select("domain", "provider", "base_url", "user_id", "account", "usage_adapter", "enabled", "usage_sync_enabled", "credential", "credential_version").Where("domain=?", scope.Domain).Take(&account).Error; err != nil {
		return p, err
	}
	if !m.sub2TokenRecoveryEnabled(account) {
		return upstreamTokenRecoveryView{Status: "disabled"}, nil
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	if epoch != newAPIUpstreamAccountEpoch(scope) {
		return p, errUpstreamTokenScope
	}
	revision := m.sub2TokenRecoveryCredentialRevision(account)
	var row ChannelUpstreamTokenWorkState
	// Return at most one representative day. Active authentication pause takes
	// precedence over old successful scans; stale credentials cannot keep the
	// display stuck on "reconnect" after reauthorization. No proof/report reads.
	q := tx.Raw(`SELECT * FROM channel_upstream_token_work_states
	 WHERE domain=? AND account_epoch=? AND kind='scan_day' AND channel_id=0 AND day_ts>0 AND day_ts<=?
	 ORDER BY CASE WHEN status='reconnect' AND input_revision=? AND next_attempt_at>? THEN 0
	 WHEN lease_until>? THEN 1 WHEN status<>'observed' THEN 2 ELSE 3 END,
	 updated_at DESC, day_ts DESC LIMIT 1`, account.Domain, epoch, cstDayStart(now)-86400, revision, now, now).Scan(&row)
	if q.Error != nil {
		return p, q.Error
	}
	if q.RowsAffected == 0 {
		return p, nil
	}
	p.DayTs, p.UpdatedAt = row.DayTs, row.UpdatedAt
	if row.LeaseUntil > now {
		p.Status = "running"
		return p, nil
	}
	switch row.Status {
	case "reconnect":
		if row.InputRevision == revision && row.NextAttemptAt > now {
			p.Status = "reconnect"
		}
	case "retry", "paging", "waiting_second_scan", "waiting_legacy", "observed":
		p.Status = row.Status
	}
	if p.Status != "reconnect" && !(row.Status == "reconnect" && row.InputRevision != revision) && row.NextAttemptAt > now && row.NextAttemptAt < upstreamAccountIsolatedUntil {
		p.RetryAt = row.NextAttemptAt
	}
	return p, nil
}

func (m *Monitor) readUpstreamTokenProcessing(tx *gorm.DB, account ChannelUpstreamAccount, view upstreamTokenConfigView, now int64) (upstreamTokenProcessingView, error) {
	p := upstreamTokenProcessingView{Status: "pending", CheckStatus: "pending"}
	if view.TokenID == "" {
		return upstreamTokenProcessingView{Status: "unconfigured"}, nil
	}
	if view.State == "unsupported" {
		return upstreamTokenProcessingView{Status: "unsupported"}, nil
	}
	if !m.cfg.ChannelCostClosureEnabled || !channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, view.Domain) {
		return upstreamTokenProcessingView{Status: "disabled"}, nil
	}
	if now <= 0 || now > math.MaxInt64-3600 {
		return p, errors.New("invalid status clock")
	}
	key, keyID := []byte(m.cfg.ChannelCostHMACKey), m.cfg.ChannelCostHMACKeyID
	check, err := channelCostKeyCheck(key)
	if err != nil {
		return p, err
	}
	var registry []ChannelCostKeyRegistry
	if err := tx.Limit(2).Find(&registry).Error; err != nil {
		return p, err
	}
	if len(registry) != 1 || registry[0].KeyID != keyID || !hmac.Equal([]byte(registry[0].KeyCheck), []byte(check)) {
		return p, errors.New("status key mismatch")
	}
	snapshot, err := readUpstreamTokenBindingSnapshot(tx, view.Domain, view.ChannelID, key, keyID, now)
	if err != nil {
		return p, err
	}
	var revision int64
	if err := tx.Raw(`SELECT COALESCE(MAX(rowid),0) FROM channel_upstream_token_configs WHERE domain=? AND account_epoch=?`, view.Domain, view.AccountEpoch).Scan(&revision).Error; err != nil {
		return p, err
	}
	var work ChannelUpstreamTokenWorkState
	q := upstreamTokenWorkScope(tx, ChannelUpstreamTokenWorkState{Domain: view.Domain, AccountEpoch: view.AccountEpoch, Kind: "binding"}).Limit(1).Find(&work)
	if q.Error != nil {
		return p, q.Error
	}
	if q.RowsAffected == 1 && work.InputRevision == revision {
		p.UpdatedAt = work.UpdatedAt
		if work.LeaseUntil > now {
			p.CheckStatus = "running"
		} else {
			switch work.Status {
			case "retry":
				p.CheckStatus = "retry"
				if work.NextAttemptAt > now {
					p.RetryAt = work.NextAttemptAt
				}
			case "waiting_evidence":
				p.CheckStatus = "waiting_evidence"
			case "scheduled", "unchanged":
				p.CheckStatus = "checked"
			}
		}
	}
	kind, err := upstreamTokenBindingKind(account)
	if err != nil {
		return p, err
	}
	source, err := channelCostSourceRef(key, account.Provider, view.AccountEpoch, kind, view.TokenID)
	if err != nil {
		return p, err
	}
	for _, change := range snapshot.Plan.Changes {
		if change.After.SourceRef == source {
			return p, nil // old binding does not validate the latest intent
		}
	}
	var binding ChannelCostSourceBinding
	q = tx.Where("domain=? AND account_epoch=? AND source_ref=? AND status='confirmed' AND valid_to=0", view.Domain, view.AccountEpoch, source).Limit(1).Find(&binding)
	if q.Error != nil {
		return p, q.Error
	}
	if q.RowsAffected != 1 || binding.HMACKeyID != keyID || binding.SourceRefKind != kind ||
		!(binding.AllocationMode == "allocated" && binding.LocalChannelID == view.ChannelID || binding.AllocationMode == "shared" && binding.LocalChannelID == 0) {
		return p, nil
	}
	// The planner has validated all intervals and current owner fingerprints.
	// An older compatible manual binding cannot backdate new token intent.
	p.EffectiveAt = max(binding.ValidFrom, (view.UpdatedAt/3600+1)*3600)
	p.Status = "scheduled"
	if p.EffectiveAt <= now {
		p.Status = "active"
		if binding.AllocationMode == "shared" {
			p.Status = "shared"
		}
	}
	return p, nil
}
