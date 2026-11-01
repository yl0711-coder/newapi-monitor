package monthlysource

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"time"
)

type TokenUsageWindowPair struct {
	Older TokenUsageWindowObservation `json:"older"`
	Newer TokenUsageWindowObservation `json:"newer"`
}

type TokenUsageMonthRow struct {
	TokenID      int64  `json:"token_id"`
	Name         string `json:"name"`
	ConsumeQuota int64  `json:"consume_quota"`
	RefundQuota  int64  `json:"refund_quota"`
}

type TokenUsageMonthReport struct {
	Basis          string               `json:"basis"`
	SourceIdentity string               `json:"source_identity"`
	UserID         int64                `json:"user_id"`
	Month          string               `json:"month"`
	From           int64                `json:"from"`
	To             int64                `json:"to"`
	ConsumeQuota   int64                `json:"consume_quota"`
	RefundQuota    int64                `json:"refund_quota"`
	Sources        []string             `json:"sources"`
	Rows           []TokenUsageMonthRow `json:"rows"`
}

// SummarizeTokenUsageMonth is a pure offline normalization of explicitly
// retained pairs. It cannot connect, collect, write, approve historical scope,
// change an input template, queue a task or publish a bill. Empty days must be
// explicitly captured twice; missing intervals are never inferred as zero.
func SummarizeTokenUsageMonth(parent context.Context, request FundingMonthRequest, pairs []TokenUsageWindowPair, now time.Time) (TokenUsageMonthReport, error) {
	var empty TokenUsageMonthReport
	start, err := time.ParseInLocation("2006-01", request.Month, fundingMonthZone)
	current := time.Date(now.In(fundingMonthZone).Year(), now.In(fundingMonthZone).Month(), 1, 0, 0, 0, 0, fundingMonthZone)
	if parent == nil || err != nil || start.Format("2006-01") != request.Month || start.Year() < 2020 || !start.Before(current) || !validDigest(request.SourceIdentity) || request.Rules.Producer != FundingProducerRC4RC26 || request.Rules.QuotaPerUnit != 500000 || len(request.UserIDs) != 1 || request.UserIDs[0] <= 0 || len(pairs) == 0 || len(pairs) > 744 {
		return empty, errors.New("invalid token month scope or observation budget")
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	ordered := append([]TokenUsageWindowPair{}, pairs...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Older.Request.From < ordered[j].Older.Request.From })
	type tokenTotal struct {
		row       TokenUsageMonthRow
		nameLogID int64
	}
	byID := map[int64]*tokenTotal{}
	endpointWindows := map[int64]int64{}
	report := TokenUsageMonthReport{Basis: "retained_wallet_token_log_groups_not_balance_proof", SourceIdentity: request.SourceIdentity, UserID: request.UserIDs[0], Month: request.Month, From: start.Unix(), To: start.AddDate(0, 1, 0).Unix(), Sources: []string{}, Rows: []TokenUsageMonthRow{}}
	cursor := report.From
	usedBytes := 0
	add := func(dst *int64, q int64) error {
		if q < 0 || q > math.MaxInt64-*dst {
			return errors.New("token month amount overflow")
		}
		*dst += q
		return nil
	}
	for _, pair := range ordered {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		a, b := pair.Older, pair.Newer
		if a.Request.UserID != report.UserID || a.Request.SourceIdentity != request.SourceIdentity || a.Request.Rules != request.Rules || a.Request.From != cursor || a.Request.To > report.To || b.CompletedAtMS > now.UnixMilli() || !TokenUsageWindowsMatch(a, b) {
			return empty, errors.New("token month has gaps, overlaps, changed observations or wrong scope")
		}
		for _, o := range []TokenUsageWindowObservation{a, b} {
			raw, err := json.Marshal(o)
			if err != nil {
				return empty, err
			}
			usedBytes += len(raw)
			if usedBytes > 64<<20 {
				return empty, errors.New("token month exceeds retained payload budget")
			}
			report.Sources = append(report.Sources, digest(raw))
		}
		for _, r := range b.Rows {
			if r.BillingSource != "wallet" {
				return empty, errors.New("token month contains subscription or unclassified billing sources; not assumed wallet")
			}
			for _, id := range []int64{r.MinID, r.MaxID} {
				if previous, ok := endpointWindows[id]; ok && previous != b.Request.From {
					return empty, errors.New("same retained log ID appears in different calendar windows")
				}
				endpointWindows[id] = b.Request.From
			}
			t := byID[r.TokenID]
			if t == nil {
				if len(byID) >= 1000 {
					return empty, errors.New("token month exceeds per-account token limit")
				}
				t = &tokenTotal{row: TokenUsageMonthRow{TokenID: r.TokenID}}
				byID[r.TokenID] = t
			}
			if r.MaxID > t.nameLogID {
				t.row.Name, t.nameLogID = r.Name, r.MaxID
			}
			dst := &t.row.ConsumeQuota
			if r.Type == 6 {
				dst = &t.row.RefundQuota
			}
			if err := add(dst, r.Quota); err != nil {
				return empty, err
			}
		}
		for _, r := range b.Totals {
			if r.BillingSource != "wallet" {
				return empty, errors.New("token month user totals are not wallet-only")
			}
			dst := &report.ConsumeQuota
			if r.Type == 6 {
				dst = &report.RefundQuota
			}
			if err := add(dst, r.Quota); err != nil {
				return empty, err
			}
		}
		cursor = a.Request.To
	}
	if cursor != report.To {
		return empty, errors.New("token month does not reach month end")
	}
	var consume, refund int64
	for _, t := range byID {
		if err := add(&consume, t.row.ConsumeQuota); err != nil {
			return empty, err
		}
		if err := add(&refund, t.row.RefundQuota); err != nil {
			return empty, err
		}
		report.Rows = append(report.Rows, t.row)
	}
	if consume != report.ConsumeQuota || refund != report.RefundQuota {
		return empty, errors.New("token month does not conserve user amounts")
	}
	sort.Slice(report.Rows, func(i, j int) bool { return report.Rows[i].TokenID < report.Rows[j].TokenID })
	return report, nil
}
