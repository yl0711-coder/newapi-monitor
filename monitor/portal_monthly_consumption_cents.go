package monitor

// Preparation only: a versioned display projection of verified consumption.
// It must not replace source money, rewrite an issued bill, or be applied to
// frozen wallets without a compatible, separately versioned wallet plan.
import (
	"encoding/json"
	"reflect"
)

type monthlyConsumptionCentMember struct {
	UserID      int64            `json:"user_id"`
	Consumption monthlyCentRow   `json:"consumption"`
	Tokens      []monthlyCentRow `json:"tokens"`
}

type monthlyConsumptionCentPlan struct {
	Version      int                            `json:"version"`
	SourceSHA256 string                         `json:"source_sha256"`
	Organization monthlyCentRow                 `json:"organization"`
	Members      []monthlyConsumptionCentMember `json:"members"`
}

func monthlyConsumptionCentSourceDigest(p monthlyStatement) (string, error) {
	// These are issuance/presentation metadata, not the exact underlying facts.
	// The caller must still separately bind/recheck the actual publication.
	p.Publication = ""
	p.DisplayBalances = nil
	raw, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return monthlyDigest(raw), nil
}

func monthlyPlanConsumptionCents(p monthlyStatement) (monthlyConsumptionCentPlan, error) {
	var empty monthlyConsumptionCentPlan
	if _, ok := monthlyDisplayChainMonth(p.Month); !ok || !p.Published || p.CurrentMonth ||
		p.TokenDetailsVersion != 1 || len(p.Members) == 0 || !monthlyStatementAmountsComplete(p) {
		return empty, errMonthlyCentAllocation
	}
	digest, err := monthlyConsumptionCentSourceDigest(p)
	if err != nil {
		return empty, err
	}
	rows := make([]monthlyCentRow, len(p.Members))
	byID := make(map[int64]monthlyMemberStatement, len(p.Members))
	for i, member := range p.Members {
		rows[i] = monthlyCentRow{ID: member.UserID, Amount: member.Consumption.Amount,
			Refund: member.Consumption.Refund, Net: member.Consumption.Net}
		byID[member.UserID] = member
	}
	org, err := monthlyCentDefault(*p.Consumption, rows)
	if err != nil {
		return empty, err
	}
	result := monthlyConsumptionCentPlan{Version: 1, SourceSHA256: digest,
		Organization: monthlyCentRow{Amount: org.Amount, Refund: org.Refund, Net: org.Net},
		Members:      make([]monthlyConsumptionCentMember, 0, len(rows))}
	for _, shown := range org.Rows {
		member := byID[shown.ID]
		tokens := make([]monthlyCentRow, len(member.TokenConsumption))
		for i, row := range member.TokenConsumption {
			tokens[i] = monthlyCentRow{ID: row.TokenID, Amount: row.Amount, Refund: row.Refund, Net: row.Net}
		}
		child, err := monthlyCentAllocate(member.Consumption, shown.Amount, shown.Refund, tokens)
		if err != nil {
			return empty, err
		}
		result.Members = append(result.Members, monthlyConsumptionCentMember{
			UserID: shown.ID, Consumption: shown, Tokens: child.Rows})
	}
	return result, nil
}

// Integrity check for a retained plan. This is not source attestation or
// authorization: it only binds the deterministic projection to verified DTOs.
func monthlyConsumptionCentPlanValid(p monthlyStatement, plan monthlyConsumptionCentPlan) bool {
	if plan.Version != 1 || len(plan.Members) > maxTrackedUsers {
		return false
	}
	want, err := monthlyPlanConsumptionCents(p)
	return err == nil && reflect.DeepEqual(want, plan)
}

// Do not mix the new per-account allocation with existing frozen balances.
// A changed displayed net changes that account's cents wallet equation even
// when organization totals still match. Refuse it instead of moving balances.
func monthlyConsumptionCentsFitsFrozenWallets(p monthlyStatement, plan monthlyConsumptionCentPlan) bool {
	if !monthlyConsumptionCentPlanValid(p, plan) {
		return false
	}
	byID := make(map[int64]monthlyConsumption, len(p.Members))
	for _, member := range p.Members {
		byID[member.UserID] = member.Consumption
	}
	for _, member := range plan.Members {
		raw := byID[member.UserID]
		gross, gok := monthlyRoundedDisplayUnits(raw.Amount, 2)
		refund, rok := monthlyRoundedDisplayUnits(raw.Refund, 2)
		if !gok || !rok || monthlyDisplayCentsMoney(gross.Sub(gross, refund)) != member.Consumption.Net {
			return false
		}
	}
	return true
}
