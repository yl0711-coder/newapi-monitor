package monitor

import (
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
)

// An explicit interval is required. A current configuration or fund-log
// observation is never implicitly converted into a historical correction.
type FinanceRechargeCorrection struct {
	Domain string  `json:"domain"`
	FromTs int64   `json:"from_ts"`
	ToTs   int64   `json:"to_ts"`
	Paid   float64 `json:"paid"`
	Credit float64 `json:"credit"`
	Reason string  `json:"reason"`
}

// buildFinanceRechargeCorrection only plans append-only rows. It preserves
// every old publication's referenced version, all unrelated snapshot fields,
// and the effective configuration outside the explicit interval. The offline
// rehearsal is its only writer; no automatic repair or HTTP route uses it.
func buildFinanceRechargeCorrection(history []ChannelFinanceVersion, in FinanceRechargeCorrection, now int64) ([]ChannelFinanceVersion, error) {
	if in.Domain == "" || len(in.Domain) > 253 || normalizeChannelBaseDomain(in.Domain) != in.Domain ||
		in.FromTs < 0 || in.ToTs <= in.FromTs || in.ToTs > now-now%3600 ||
		in.ToTs-in.FromTs > financeReportMaxDays*86400 ||
		!validChannelFinanceNumber(in.Paid) || !validChannelFinanceNumber(in.Credit) ||
		strings.TrimSpace(in.Reason) == "" || len(in.Reason) > 512 {
		return nil, errors.New("historical recharge correction requires a domain, closed finite interval, valid paid:credit and reason")
	}
	if len(history) == 0 || len(history) > financeRechargeCorrectionVersionLimit {
		return nil, errors.New("historical recharge correction requires bounded existing version history")
	}
	rows := append([]ChannelFinanceVersion(nil), history...)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].EffectiveAt != rows[j].EffectiveAt {
			return rows[i].EffectiveAt < rows[j].EffectiveAt
		}
		return rows[i].Version < rows[j].Version
	})
	var maxVersion int64
	var latestSaved ChannelFinanceVersion
	seen := make(map[int64]bool)
	for _, row := range rows {
		if row.Domain != in.Domain || row.Version <= 0 || seen[row.Version] || row.EffectiveAt < 0 || !json.Valid([]byte(row.SnapshotJSON)) {
			return nil, errors.New("invalid or mixed historical recharge versions")
		}
		seen[row.Version] = true
		if row.Version > maxVersion {
			maxVersion, latestSaved = row.Version, row
		}
	}
	last := rows[len(rows)-1]
	// Keep this repair historical: do not rewrite the current/future settings.
	// MAX(version) readers must still select the same current snapshot, hence
	// a final unchanged anchor is appended after the historical correction.
	if in.ToTs > last.EffectiveAt || latestSaved.EffectiveAt != last.EffectiveAt || latestSaved.Version != last.Version {
		return nil, errors.New("correction must end before an unambiguous current configuration")
	}
	if maxVersion > math.MaxInt64-int64(len(rows))-3 {
		return nil, errors.New("historical version number overflow")
	}
	activeAt := func(at int64) string {
		snapshot := `{"domain":` + string(mustRechargeJSON(in.Domain)) + `}`
		for _, row := range rows {
			if row.EffectiveAt > at {
				break
			}
			snapshot = row.SnapshotJSON
		}
		return snapshot
	}
	appendRow := func(out *[]ChannelFinanceVersion, at int64, snapshot string) {
		maxVersion++
		*out = append(*out, ChannelFinanceVersion{Domain: in.Domain, Version: maxVersion,
			EffectiveAt: at, SnapshotJSON: snapshot, CreatedAt: now, UpdatedBy: "local-recharge-rehearsal"})
	}
	points := []int64{in.FromTs}
	for i, row := range rows {
		if row.EffectiveAt <= in.FromTs || row.EffectiveAt >= in.ToTs ||
			(i+1 < len(rows) && rows[i+1].EffectiveAt == row.EffectiveAt) {
			continue
		}
		points = append(points, row.EffectiveAt)
	}
	var out []ChannelFinanceVersion
	for _, at := range points {
		snapshot, changed, err := correctedRechargeSnapshot(activeAt(at), in)
		if err != nil {
			return nil, err
		}
		if changed {
			appendRow(&out, at, snapshot)
		}
	}
	if len(out) == 0 {
		return nil, nil // Already repaired: do not grow the ledger on retry.
	}
	appendRow(&out, in.ToTs, activeAt(in.ToTs))
	if in.ToTs != last.EffectiveAt {
		appendRow(&out, last.EffectiveAt, last.SnapshotJSON)
	}
	return out, nil
}

const financeRechargeCorrectionVersionLimit = 5000

func correctedRechargeSnapshot(raw string, in FinanceRechargeCorrection) (string, bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil || fields == nil {
		return "", false, errors.New("historical finance snapshot is not an object")
	}
	var domain string
	if value, exists := fields["domain"]; exists {
		if json.Unmarshal(value, &domain) != nil || domain != in.Domain {
			return "", false, errors.New("historical snapshot domain differs from correction")
		}
	}
	var previous channelFinanceVersionSnapshot
	if err := json.Unmarshal([]byte(raw), &previous); err != nil {
		return "", false, err
	}
	oldRatio, valid := financeRechargeFraction(previous.UpstreamRechargePaid, previous.UpstreamRechargeCredit)
	nextRatio, _ := financeRechargeFraction(in.Paid, in.Credit)
	if valid && oldRatio.Cmp(nextRatio) == 0 {
		return raw, false, nil
	}
	fields["upstream_recharge_paid"] = mustRechargeJSON(in.Paid)
	fields["upstream_recharge_credit"] = mustRechargeJSON(in.Credit)
	encoded, err := json.Marshal(fields)
	return string(encoded), true, err
}

// Only validated finite numbers and canonical domain strings reach here.
func mustRechargeJSON(value any) json.RawMessage {
	raw, _ := json.Marshal(value)
	return raw
}
