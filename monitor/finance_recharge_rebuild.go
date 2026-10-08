package monitor

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

// This is deliberately a finite offline operation, not a collector or a new
// background job. A caller must supply the isolated rehearsal transaction.
const financeRechargeRebuildHourLimit = 512

type FinanceRechargeEconomicsRebuild struct {
	Domain               string                  `json:"domain"`
	Status               string                  `json:"status"`
	WindowHours          int64                   `json:"window_hours"`
	WithoutVerifiedHours int64                   `json:"without_verified_cost_hours"`
	RebuiltHours         int64                   `json:"rebuilt_hours"`
	UnchangedHours       int64                   `json:"unchanged_hours"`
	CompleteHours        int64                   `json:"complete_hours"`
	IncompleteHours      int64                   `json:"incomplete_hours"`
	HourDiagnostics      *financePairingHourView `json:"hour_diagnostics,omitempty"`
}

func rebuildFinanceRechargeEconomics(ctx context.Context, tx *gorm.DB, corrections []FinanceRechargeCorrection, now int64) ([]FinanceRechargeEconomicsRebuild, error) {
	var result []FinanceRechargeEconomicsRebuild
	var totalHours int64
	for _, correction := range corrections {
		if correction.FromTs < 0 || correction.ToTs <= correction.FromTs || correction.ToTs > now-now%3600 {
			return nil, errors.New("economics rebuild requires a closed correction interval")
		}
		first := correction.FromTs - correction.FromTs%3600
		hours := (correction.ToTs-1-first)/3600 + 1
		totalHours += hours
		if totalHours > financeRechargeRebuildHourLimit {
			return nil, errors.New("economics rebuild exceeds 512 hours; split the local proposal")
		}
		out := FinanceRechargeEconomicsRebuild{Domain: correction.Domain, WindowHours: hours, WithoutVerifiedHours: hours}
		var account ChannelUpstreamAccount
		// Account identity is needed for epoch isolation; credentials are not.
		if err := tx.WithContext(ctx).Select("domain", "provider", "base_url", "user_id", "account").
			Where("domain=?", correction.Domain).First(&account).Error; err != nil {
			return nil, err
		}
		if err := validateRechargeRebuildSources(ctx, tx, account, first, correction.ToTs); err != nil {
			return nil, err
		}
		if account.Provider != upstreamProviderNewAPI {
			out.Status = "no_supported_cost_evidence"
			result = append(result, out)
			continue
		}
		var states []ChannelUpstreamCostHourState
		if err := tx.WithContext(ctx).Where("domain=? AND account_epoch=? AND hour_ts>=? AND hour_ts<? AND semantics_version=? AND status='verified' AND reconcile_status='matched'",
			account.Domain, newAPIUpstreamAccountEpoch(account), first, correction.ToTs, channelCostEvidenceSemanticsVersion).
			Order("hour_ts").Limit(financeRechargeRebuildHourLimit + 1).Find(&states).Error; err != nil {
			return nil, err
		}
		if int64(len(states)) > hours {
			return nil, errors.New("invalid or duplicate hourly cost evidence")
		}
		// Only reuse already verified local evidence. No source clients are
		// constructed and this Monitor never starts any workers.
		m := &Monitor{storeDB: tx, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{account.Domain}}}
		out.WithoutVerifiedHours -= int64(len(states))
		out.Status = "local_evidence_rebuilt"
		if len(states) == 0 {
			out.Status = "no_verified_cost_evidence"
		}
		for _, state := range states {
			before, _, err := rechargeEconomicsManifest(ctx, tx, account.Domain, state.HourTs)
			if err != nil {
				return nil, err
			}
			if err := m.publishChannelEconomicsHour(ctx, account, state.HourTs, "recharge_local_rehearsal", now); err != nil {
				return nil, fmt.Errorf("rebuild %s hour %d: %w", account.Domain, state.HourTs, err)
			}
			after, complete, err := rechargeEconomicsManifest(ctx, tx, account.Domain, state.HourTs)
			if err != nil {
				return nil, fmt.Errorf("read rebuilt economics manifest: %w", err)
			}
			if after == "" {
				return nil, errors.New("economics rebuild did not publish an authoritative manifest")
			}
			if before == after {
				out.UnchangedHours++
			} else {
				out.RebuiltHours++
			}
			if complete {
				out.CompleteHours++
			} else {
				out.IncompleteHours++ // Mapping/local facts/pricing gaps remain gaps.
			}
		}
		diagnostics, err := loadFinancePairingHours(ctx, tx, stabilityScope{FromTs: first, ToTs: correction.ToTs})
		if err != nil {
			return nil, err
		}
		for _, diagnostic := range diagnostics {
			if diagnostic.Domain == account.Domain {
				out.HourDiagnostics = &diagnostic
				break
			}
		}
		result = append(result, out)
	}
	return result, nil
}

// Missing evidence is a reportable gap only when no old authoritative ledger
// can survive with the superseded price. Otherwise reject the transaction:
// silently keeping it would mix new account bills with stale ledger fallback.
func validateRechargeRebuildSources(ctx context.Context, tx *gorm.DB, account ChannelUpstreamAccount, from, to int64) error {
	var blocked int64
	err := tx.WithContext(ctx).Raw(`SELECT COUNT(*) FROM channel_economics_hour_manifest_current mc
		JOIN channel_economics_hour_manifest_publications mp ON mp.manifest_id=mc.manifest_id
		WHERE mc.domain=? AND mc.hour_ts>=? AND mc.hour_ts<? AND mc.semantics_version=?
		AND (? != ? OR mp.authoritative_epoch != ? OR NOT EXISTS (
			SELECT 1 FROM channel_upstream_cost_hour_states s WHERE s.domain=mp.domain
			AND s.account_epoch=mp.authoritative_epoch AND s.hour_ts=mp.hour_ts AND s.semantics_version=?
			AND s.status='verified' AND s.reconcile_status='matched'))`,
		account.Domain, from, to, channelEconomicsSemanticsVersion, account.Provider, upstreamProviderNewAPI,
		newAPIUpstreamAccountEpoch(account), channelCostEvidenceSemanticsVersion).Scan(&blocked).Error
	if err != nil {
		return err
	}
	if blocked > 0 {
		return errors.New("existing economics cannot be rebuilt from this account's verified evidence; refusing mixed pricing")
	}
	return nil
}

func rechargeEconomicsManifest(ctx context.Context, tx *gorm.DB, domain string, hour int64) (string, bool, error) {
	var row ChannelEconomicsHourManifestPublication
	err := tx.WithContext(ctx).Table("channel_economics_hour_manifest_current mc").Select("mp.*").
		Joins("JOIN channel_economics_hour_manifest_publications mp ON mp.manifest_id=mc.manifest_id").
		Where("mc.domain=? AND mc.hour_ts=? AND mc.semantics_version=?", domain, hour, channelEconomicsSemanticsVersion).
		Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", false, nil
	}
	return row.ManifestID, row.CoverageStatus == "verified_complete" && row.ProfitKnown, err
}
