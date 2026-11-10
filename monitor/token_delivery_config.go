package monitor

import (
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
)

const tokenDeliveryVersion = "token-delivery-v1"
const tokenExportMaxRequests = 100
const tokenExportMaxRawBytes = 8 << 20
const tokenExportMaxBytes = 4 << 20
const tokenExportMaxStoredBytes = 32 << 20
const tokenExportTTL int64 = 3600
const tokenDeliveryRetention int64 = 90 * 86400

type tokenExportTarget struct {
	ID                   string  `json:"id"`
	SourceEpoch          string  `json:"source_epoch"`
	ChannelIDs           []int64 `json:"channel_ids"`
	From                 int64   `json:"from"`
	To                   int64   `json:"to"`
	AllowCustomerPricing bool    `json:"allow_customer_pricing"`
}
type tokenExportPolicy struct {
	Version string              `json:"version"`
	Targets []tokenExportTarget `json:"targets"`
}
type tokenNotifyRule struct {
	Name         string `json:"name"`
	MinRequests  int    `json:"min_requests"`
	Evidence     string `json:"evidence_level"`
	Severity     string `json:"severity"`
	AmountMethod string `json:"amount_method"`
	MinQuota     string `json:"min_quota"`
	Subject      string `json:"subject"`
	Unit         string `json:"unit"`
	QuotaPerUnit string `json:"quota_per_unit"`
}
type tokenNotifyPolicy struct {
	Version             string            `json:"version"`
	WindowSeconds       int64             `json:"window_seconds"`
	CooldownSeconds     int64             `json:"cooldown_seconds"`
	MaxAgeSeconds       int64             `json:"max_age_seconds"`
	DailyBudget         int               `json:"daily_budget"`
	EscalationFactor    int               `json:"escalation_factor"`
	TelemetryLagSeconds int64             `json:"telemetry_lag_seconds"`
	Rules               []tokenNotifyRule `json:"rules"`
}

func decodeTokenDeliveryJSON(raw string, out any) error {
	if len(raw) == 0 || len(raw) > 16384 {
		return errors.New("configuration size invalid")
	}
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return errors.New("configuration invalid")
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing configuration data")
	}
	return nil
}
func parseTokenExportPolicy(raw string) (tokenExportPolicy, error) {
	var p tokenExportPolicy
	if err := decodeTokenDeliveryJSON(raw, &p); err != nil {
		return p, err
	}
	if !tokenMeteringName.MatchString(p.Version) || len(p.Targets) == 0 || len(p.Targets) > 50 {
		return p, errors.New("export policy bounds")
	}
	seen := map[string]bool{}
	for _, t := range p.Targets {
		if !tokenMeteringName.MatchString(t.ID) || !tokenMeteringName.MatchString(t.SourceEpoch) || seen[t.ID] || len(t.ChannelIDs) == 0 || len(t.ChannelIDs) > 50 || t.From <= 0 || t.To <= t.From {
			return p, errors.New("export target bounds")
		}
		seen[t.ID] = true
		ids := map[int64]bool{}
		for _, id := range t.ChannelIDs {
			if id <= 0 || ids[id] {
				return p, errors.New("export channel invalid")
			}
			ids[id] = true
		}
	}
	return p, nil
}
func tokenRuleName(v string) bool {
	return v == "input_inflation" || v == "cache_structure" || v == "usage_format" || v == "historical_price_difference"
}
func tokenMoneyMethod(v string) bool {
	return v == "observed_consumption_not_loss" || v == "historical_formula_v1" || v == "input_upper_reference_v1"
}
func tokenEvidenceRank(v string) int {
	switch v {
	case "exact":
		return 3
	case "correlated":
		return 2
	case "inferred":
		return 1
	}
	return 0
}
func parseTokenNotifyPolicy(raw string) (tokenNotifyPolicy, error) {
	var p tokenNotifyPolicy
	if err := decodeTokenDeliveryJSON(raw, &p); err != nil {
		return p, err
	}
	if !tokenMeteringName.MatchString(p.Version) || p.WindowSeconds < 60 || p.WindowSeconds > 3600 || p.CooldownSeconds < 60 || p.CooldownSeconds > 86400 || p.MaxAgeSeconds < 60 || p.MaxAgeSeconds > 86400 || p.DailyBudget < 1 || p.DailyBudget > 100 || p.EscalationFactor < 2 || p.EscalationFactor > 100 || len(p.Rules) > 16 || p.TelemetryLagSeconds < 0 || p.TelemetryLagSeconds > 86400 || len(p.Rules) == 0 && p.TelemetryLagSeconds == 0 {
		return p, errors.New("notification policy bounds")
	}
	for _, r := range p.Rules {
		if !tokenRuleName(r.Name) || r.MinRequests < 1 || r.MinRequests > 5000 || tokenEvidenceRank(r.Evidence) == 0 || (r.Severity != "SEV2" && r.Severity != "SEV3") {
			return p, errors.New("notification rule invalid")
		}
		if r.MinQuota != "" {
			if !tokenMoneyMethod(r.AmountMethod) || r.Subject != "customer_wallet" || !tokenMeteringName.MatchString(r.Unit) || !tokenPolicyRatio(r.MinQuota, "0", "1000000000000000000000000000000", false) || !tokenPolicyRatio(r.QuotaPerUnit, "0", "1000000000000000000000000000000", false) || tokenRatio(r.QuotaPerUnit).Sign() <= 0 {
				return p, errors.New("notification amount identity invalid")
			}
		} else if r.AmountMethod != "" || r.Subject != "" || r.Unit != "" || r.QuotaPerUnit != "" {
			return p, errors.New("notification amount incomplete")
		}
	}
	return p, nil
}
func validateTokenDeliverySettings(s Settings) error {
	if s.TokenExportsEnabled {
		if _, err := parseTokenExportPolicy(s.TokenExportPolicy); err != nil {
			return err
		}
	}
	if s.TokenNotificationsEnabled && !s.TokenNotificationEvaluationEnabled {
		return errors.New("notification delivery requires independent evaluation")
	}
	if s.TokenNotificationEvaluationEnabled {
		if _, err := parseTokenNotifyPolicy(s.TokenNotificationPolicy); err != nil {
			return err
		}
	}
	if s.TokenNotificationsEnabled {
		u, err := url.Parse(s.TokenNotificationBaseURL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
			return errors.New("notification requires internal monitor base URL")
		}
		if s.LocalSnapshotOnly || s.LocalAuthBypass || s.AlertsDisabled || len(recipientList(s.TokenNotificationRecipients)) == 0 {
			return errors.New("notification delivery requires authenticated non-snapshot mode and recipients")
		}
	}
	return nil
}
