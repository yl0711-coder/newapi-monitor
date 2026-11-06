package monthlydraft

import (
	"context"
	"math/big"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Source diagnostics, not an AutomaticScope, normalized input or decision.
// Even a bounded candidate still needs recognition-period and snapshot proof
// integration before it may enter the unchanged financial publication gates.
type automaticOriginReview struct {
	originals auditedAutomaticHistory
	accounts  []automaticOriginAccount
}

type automaticOriginAccount struct {
	UserID                              int64
	OriginBalanceQuota, OriginUsedQuota int64
	RegistrationMonth                   string
	RegistrationMonthLogNet             int64
	EligibleRecognitionMonths           []string
	TargetBeforeRegistration            bool
	Status                              string
}

func (automaticOriginReview) MarshalJSON() ([]byte, error) { return nil, ErrGenerationEvidence }
func (r *automaticOriginReview) UnmarshalJSON([]byte) error {
	if r != nil {
		*r = automaticOriginReview{}
	}
	return ErrGenerationEvidence
}

func (g *Generation) reviewAutomaticOrigins(ctx context.Context, ref monthlysource.MonthlyObservationFile, req SourcePreparationRequest) (automaticOriginReview, error) {
	var empty automaticOriginReview
	audited, err := g.auditAutomaticHistoryResidualSources(ctx, ref, req)
	if err != nil {
		return empty, err
	}
	return classifyAutomaticOrigins(ctx, audited, req)
}

func classifyAutomaticOrigins(ctx context.Context, audited auditedAutomaticHistory, req SourcePreparationRequest) (automaticOriginReview, error) {
	var empty automaticOriginReview
	zone := time.FixedZone("Usage-UTC+8", 8*3600)
	result := automaticOriginReview{originals: audited}
	history := audited.history.Report()
	for _, a := range history.Accounts {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		month := time.Unix(a.RegisteredAt, 0).In(zone).Format("2006-01")
		row := automaticOriginAccount{UserID: a.UserID, OriginBalanceQuota: a.OriginBalanceQuota, OriginUsedQuota: a.OriginUsedQuota,
			RegistrationMonth: month, TargetBeforeRegistration: req.Month < month, Status: "not_opposite_residual"}
		found := false
		for _, m := range a.Months {
			net := new(big.Int).Sub(big.NewInt(m.ConsumeQuota), big.NewInt(m.RefundQuota))
			if !net.IsInt64() {
				return empty, ErrGenerationCapacity
			}
			if m.Month == month {
				row.RegistrationMonthLogNet, found = net.Int64(), true
			}
			if m.Month >= month && monitor.MonthlyHistoricalBalanceWithinLimits(a.OriginBalanceQuota, net.Int64()) {
				row.EligibleRecognitionMonths = append(row.EligibleRecognitionMonths, m.Month)
			}
		}
		if !found && month <= history.ThroughMonth {
			return empty, ErrGenerationEvidence
		}
		switch {
		case a.OriginBalanceQuota == 0 && a.OriginUsedQuota == 0:
			row.Status = "no_difference"
		case a.OriginBalanceQuota > 0 && a.OriginUsedQuota < 0 && new(big.Int).Add(big.NewInt(a.OriginBalanceQuota), big.NewInt(a.OriginUsedQuota)).Sign() == 0:
			row.Status = "outside_existing_amount_limits"
			if !found {
				row.Status = "registration_month_not_closed"
			}
			if len(row.EligibleRecognitionMonths) > 0 {
				row.Status = "bounded_policy_candidate_not_approval"
			}
		}
		if row.Status != "bounded_policy_candidate_not_approval" {
			row.EligibleRecognitionMonths = nil
		}
		result.accounts = append(result.accounts, row)
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return result, nil
}
