package monitor

// This read-only projection compares current Monitor configuration with already
// collected request-time pricing evidence. It does not fetch logs, infer a
// recharge ratio, change prices, or publish historical cost/profit.

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"math/big"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const (
	pricingObservationWindowHours = 24
	pricingObservationRecentAge   = 3 * time.Hour
	pricingObservationRowLimit    = 2000
	pricingObservationChangeLimit = 20
	pricingObservationTimeout     = 2 * time.Second
)

type channelPricingConfiguration struct {
	ChannelID           int    `json:"channel_id"`
	ChannelName         string `json:"channel_name"`
	LocalGroup          string `json:"local_group"`
	BaseMultiplier      string `json:"base_multiplier"`
	EffectiveMultiplier string `json:"effective_multiplier"`
	DiscountFactor      string `json:"discount_factor"`
}

type channelPricingObservation struct {
	SourceGroup    string   `json:"source_group"`
	ModelName      string   `json:"model_name"`
	HourTs         int64    `json:"hour_ts"`
	Requests       int64    `json:"requests"`
	Rates          []string `json:"rates"`
	Discounts      []string `json:"discounts"`
	Comparison     string   `json:"comparison"`
	EvidenceStatus string   `json:"evidence_status"`
}

type channelPricingChangeView struct {
	SourceGroup string `json:"source_group"`
	ModelName   string `json:"model_name"`
	Previous    string `json:"previous"`
	Current     string `json:"current"`
	HourTs      int64  `json:"hour_ts"`
}

type channelPricingObservationView struct {
	Domain           string                                   `json:"domain"`
	Provider         string                                   `json:"provider"`
	CollectionActive bool                                     `json:"collection_active"`
	AsOfHour         int64                                    `json:"as_of_hour"`
	WindowFrom       int64                                    `json:"window_from"`
	Rows             []channelPricingObservation              `json:"rows"`
	Changes          []channelPricingChangeView               `json:"changes"`
	Truncated        bool                                     `json:"truncated"`
	ChangesTruncated bool                                     `json:"changes_truncated"`
	Configurations   map[string][]channelPricingConfiguration `json:"configurations"`
}

type pricingConfigurationRow struct {
	ChannelFinanceChannelCost
	ChannelName   string
	ChannelGroups string
}

func pricingRatioDisplay(value *big.Rat) string {
	text := strings.TrimRight(strings.TrimRight(value.FloatString(30), "0"), ".")
	if text == "" {
		return "0"
	}
	return text
}

func pricingConfigurations(rows []pricingConfigurationRow) map[string][]channelPricingConfiguration {
	out := make(map[string][]channelPricingConfiguration)
	for _, row := range rows {
		// Obsolete local groups are retained for history, not current comparison.
		if !containsString(splitList(row.ChannelGroups), row.Grp) || strings.TrimSpace(row.UpstreamGroupName) == "" {
			continue
		}
		item := channelPricingConfiguration{ChannelID: row.ChannelID, ChannelName: row.ChannelName, LocalGroup: row.Grp}
		discount := normalizedUpstreamDiscountFactor(row.DiscountFactor)
		if validChannelFinanceNumber(row.Multiplier) && validChannelFinanceNumber(discount) {
			base, _ := new(big.Rat).SetString(strconv.FormatFloat(row.Multiplier, 'g', -1, 64))
			factor, _ := new(big.Rat).SetString(strconv.FormatFloat(discount, 'g', -1, 64))
			item.EffectiveMultiplier = pricingRatioDisplay(new(big.Rat).Mul(base, factor))
			item.BaseMultiplier = pricingRatioDisplay(base)
			item.DiscountFactor = pricingRatioDisplay(factor)
		}
		group := strings.TrimSpace(row.UpstreamGroupName)
		out[group] = append(out[group], item)
	}
	return out
}

func sortedPricingValues(values map[string]string) []string {
	out := make([]string, 0, len(values))
	for _, display := range values {
		out = append(out, display)
	}
	sort.Strings(out)
	return out
}

func comparePricingObservation(rows []ChannelUpstreamPricingHourEvidence, state ChannelUpstreamPricingHourState, configs []channelPricingConfiguration, now int64, active bool) (channelPricingObservation, error) {
	if len(rows) == 0 {
		return channelPricingObservation{}, errors.New("计价证据为空")
	}
	first := rows[0]
	out := channelPricingObservation{
		SourceGroup: first.SourceGroup, ModelName: first.ModelName, HourTs: first.HourTs,
		Rates: []string{}, Discounts: []string{},
		Comparison: "unverified", EvidenceStatus: "unverified",
	}
	rates, discounts := map[string]string{}, map[string]string{}
	usable, fullRate := true, true
	for _, row := range rows {
		if row.EligibleRequests < 0 || out.Requests > math.MaxInt64-row.EligibleRequests {
			return out, errors.New("计价证据请求数无效")
		}
		out.Requests += row.EligibleRequests
		if !row.OtherValid {
			usable = false
		}
		var display string
		switch row.EvidenceCapability {
		case "full_rate", "effective_rate":
			if row.EffectiveRatioSource == "" || row.EffectiveRatioSource == "unknown" {
				usable = false
			}
			display = row.EffectiveRatio
		case "discount_only":
			fullRate = false
			if row.DiscountRatioState != pricingRatioValid {
				usable = false
			}
			display = row.DiscountRatio
		default:
			fullRate, usable = false, false
		}
		// Persisted evidence uses normalized decimals, not unbounded exponents.
		if len(display) == 0 || len(display) > 80 || strings.ContainsAny(display, "eE") {
			usable = false
			continue
		}
		value, ok := new(big.Rat).SetString(display)
		if !ok || value.Sign() < 0 {
			usable = false
			continue
		}
		if row.EvidenceCapability == "discount_only" {
			discounts[value.RatString()] = pricingRatioDisplay(value)
		} else {
			rates[value.RatString()] = pricingRatioDisplay(value)
		}
	}
	out.Rates, out.Discounts = sortedPricingValues(rates), sortedPricingValues(discounts)
	if state.Status != "verified" || !pricingReconcileAccepted(first.Provider, state.ReconcileStatus) || !usable {
		return out, nil
	}
	out.EvidenceStatus = "historical"
	if active && now >= out.HourTs+3600 && now-(out.HourTs+3600) <= int64(pricingObservationRecentAge/time.Second) {
		out.EvidenceStatus = "recent"
	}
	if len(rates)+len(discounts) != 1 {
		out.Comparison = "mixed_rates"
		return out, nil
	}
	if !fullRate {
		out.Comparison = "discount_only"
		return out, nil
	}
	if len(configs) == 0 {
		out.Comparison = "not_configured"
		return out, nil
	}
	configured := map[string]bool{}
	for _, config := range configs {
		value, ok := new(big.Rat).SetString(config.EffectiveMultiplier)
		if !ok || value.Sign() <= 0 {
			out.Comparison = "configuration_conflict"
			return out, nil
		}
		configured[value.RatString()] = true
	}
	if len(configured) != 1 {
		out.Comparison = "configuration_conflict"
		return out, nil
	}
	out.Comparison = "different"
	for canonical := range rates {
		if configured[canonical] {
			out.Comparison = "same"
		}
	}
	return out, nil
}

// All reads share a short, deferred read transaction so an account/configuration
// change or an atomic evidence publication cannot produce a mixed-generation view.
func (m *Monitor) loadChannelPricingObservations(ctx context.Context, domain string, now int64) (channelPricingObservationView, error) {
	out := channelPricingObservationView{Domain: domain, Rows: []channelPricingObservation{}, Changes: []channelPricingChangeView{}, Configurations: map[string][]channelPricingConfiguration{}}
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var account ChannelUpstreamAccount
		if err := tx.Select("domain", "provider", "base_url", "user_id", "account", "usage_adapter", "enabled", "usage_sync_enabled").First(&account, "domain = ?", domain).Error; err != nil {
			return err
		}
		out.Provider = account.Provider
		out.CollectionActive = !m.cfg.LocalSnapshotOnly && m.cfg.UpstreamPricingLedgerEnabled && pricingLedgerDomainAllowed(m.cfg.UpstreamPricingLedgerDomains, domain) &&
			pricingLedgerAccountSupported(account) && account.Enabled && account.UsageSyncEnabled
		epoch := newAPIUpstreamAccountEpoch(account)
		var latest ChannelUpstreamPricingHourState
		query := tx.Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND hour_ts < ?", domain, epoch, upstreamPricingSemanticsVersion, now-now%3600)
		if err := query.Order("hour_ts DESC").First(&latest).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		out.AsOfHour = latest.HourTs
		out.WindowFrom = latest.HourTs - (pricingObservationWindowHours-1)*3600
		var states []ChannelUpstreamPricingHourState
		if err := tx.Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND hour_ts BETWEEN ? AND ?", domain, epoch, upstreamPricingSemanticsVersion, out.WindowFrom, out.AsOfHour).Find(&states).Error; err != nil {
			return err
		}
		stateAt := make(map[int64]ChannelUpstreamPricingHourState, len(states))
		for _, state := range states {
			stateAt[state.HourTs] = state
		}
		var evidence []ChannelUpstreamPricingHourEvidence
		// Restrict the grouped scan to 24 collected hours, never all history.
		if err := tx.Raw(`WITH latest_dimension AS (
			SELECT source_group, model_name, MAX(hour_ts) AS hour_ts
			FROM channel_upstream_pricing_hour_evidence
			WHERE domain = ? AND account_epoch = ? AND semantics_version = ? AND hour_ts BETWEEN ? AND ? AND eligible_requests > 0
			GROUP BY source_group, model_name)
			SELECT e.* FROM channel_upstream_pricing_hour_evidence e JOIN latest_dimension d
			ON e.source_group = d.source_group AND e.model_name = d.model_name AND e.hour_ts = d.hour_ts
			WHERE e.domain = ? AND e.account_epoch = ? AND e.semantics_version = ? AND e.eligible_requests > 0
			ORDER BY e.source_group, e.model_name, e.dimension_hash LIMIT ?`, domain, epoch, upstreamPricingSemanticsVersion, out.WindowFrom, out.AsOfHour,
			domain, epoch, upstreamPricingSemanticsVersion, pricingObservationRowLimit+1).Scan(&evidence).Error; err != nil {
			return err
		}
		if len(evidence) > pricingObservationRowLimit {
			// Never compare a truncated subset: it could hide another multiplier.
			out.Truncated = true
		} else {
			var configuration []pricingConfigurationRow
			if err := tx.Table("channel_finance_channel_costs f").Select("f.*, c.name AS channel_name, c.groups AS channel_groups").
				Joins("JOIN channel_snaps c ON c.id = f.channel_id").Where("c.base_domain = ? AND c.deleted_at = 0", domain).
				Order("f.channel_id, f.grp").Limit(pricingObservationRowLimit + 1).Scan(&configuration).Error; err != nil {
				return err
			}
			if len(configuration) > pricingObservationRowLimit {
				out.Truncated = true
			} else {
				configs := pricingConfigurations(configuration)
				out.Configurations = configs
				for start := 0; start < len(evidence); {
					end := start + 1
					for end < len(evidence) && evidence[end].SourceGroup == evidence[start].SourceGroup && evidence[end].ModelName == evidence[start].ModelName {
						end++
					}
					for _, source := range evidence[start:end] {
						if source.Provider != account.Provider {
							return errors.New("计价证据类型与账号不符")
						}
					}
					row, err := comparePricingObservation(evidence[start:end], stateAt[evidence[start].HourTs], configs[evidence[start].SourceGroup], now, out.CollectionActive)
					if err != nil {
						return err
					}
					out.Rows = append(out.Rows, row)
					start = end
				}
			}
		}
		var changes []ChannelUpstreamPricingChangeEvent
		if err := tx.Select("source_group", "model_name", "previous_ratio", "current_ratio", "first_observed_hour").
			Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND first_observed_hour <= ?", domain, epoch, upstreamPricingSemanticsVersion, out.AsOfHour).
			Order("first_observed_hour DESC, id DESC").Limit(pricingObservationChangeLimit + 1).Find(&changes).Error; err != nil {
			return err
		}
		out.ChangesTruncated = len(changes) > pricingObservationChangeLimit
		if out.ChangesTruncated {
			changes = changes[:pricingObservationChangeLimit]
		}
		for _, change := range changes {
			out.Changes = append(out.Changes, channelPricingChangeView{change.SourceGroup, change.ModelName, change.PreviousRatio, change.CurrentRatio, change.FirstObservedHour})
		}
		return nil
	}, &sql.TxOptions{ReadOnly: true})
	return out, err
}

func (m *Monitor) getChannelPricingObservationsHandler(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	domain := strings.ToLower(strings.TrimSpace(c.Query("domain")))
	if domain == "" || len(domain) > 253 || normalizeChannelBaseDomain(domain) != domain {
		c.JSON(http.StatusBadRequest, gin.H{"error": "上游主域名无效"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), pricingObservationTimeout)
	defer cancel()
	view, err := m.loadChannelPricingObservations(ctx, domain, time.Now().Unix())
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "该上游尚未配置账号"})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "本地计价证据暂不可读取，请稍后重试"})
		return
	}
	c.JSON(http.StatusOK, view)
}
