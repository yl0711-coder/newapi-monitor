package monitor

// Pure preparation of a new presentation version, not an update to a source
// wallet or an issued bill. Existing v1 balance publications keep their reader.
import (
	"encoding/json"
	"math/big"
)

type monthlyBillCentWallet struct {
	SourceOpening     string `json:"source_opening"`
	SourceClosing     string `json:"source_closing"`
	Opening           string `json:"opening"`
	Recharge          string `json:"recharge"`
	Gift              string `json:"gift"`
	CreditTotal       string `json:"credit_total"`
	Adjustment        string `json:"adjustment"`
	BalanceCorrection string `json:"balance_correction"`
	Closing           string `json:"closing"`
}

type monthlyBillCentMember struct {
	UserID int64                 `json:"user_id"`
	Wallet monthlyBillCentWallet `json:"wallet"`
}

type monthlyBillCentMonth struct {
	Version        int                        `json:"version"`
	GroupID        int64                      `json:"group_id"`
	SourceIdentity string                     `json:"source_identity"`
	Month          string                     `json:"month"`
	SourceSHA256   string                     `json:"source_sha256"`
	Consumption    monthlyConsumptionCentPlan `json:"consumption"`
	Organization   monthlyBillCentWallet      `json:"organization"`
	Members        []monthlyBillCentMember    `json:"members"`
	CreditRows     []monthlyCentScalar        `json:"credit_rows"` // ID is the retained source-row index
	Digest         string                     `json:"digest"`
}

func monthlyBillCentMonthDigest(plan monthlyBillCentMonth) string {
	plan.Digest = ""
	raw, _ := json.Marshal(plan)
	return monthlyDigest(raw)
}

func monthlyCentOptional(value string) string {
	if value == "" {
		return "0.000000"
	}
	return value
}

func monthlyCentSum(terms ...monthlyDisplayTerm) (string, error) {
	sum := new(big.Int)
	for _, term := range terms {
		if term.sign != 1 && term.sign != -1 {
			return "", errMonthlyCentAllocation
		}
		value, ok := monthlyDisplayChainCents(term.amount)
		if !ok {
			return "", errMonthlyCentAllocation
		}
		sum.Add(sum, value.Mul(value, big.NewInt(term.sign)))
	}
	return monthlyDisplayCentsMoney(sum), nil
}

func monthlyBillCentClose(wallet *monthlyBillCentWallet, consumption monthlyCentRow) error {
	var err error
	wallet.CreditTotal, err = monthlyCentSum(monthlyDisplayTerm{wallet.Recharge, 1}, monthlyDisplayTerm{wallet.Gift, 1})
	if err != nil {
		return err
	}
	wallet.Closing, err = monthlyCentSum(monthlyDisplayTerm{wallet.Opening, 1}, monthlyDisplayTerm{wallet.CreditTotal, 1},
		monthlyDisplayTerm{wallet.Adjustment, 1}, monthlyDisplayTerm{wallet.BalanceCorrection, 1},
		monthlyDisplayTerm{consumption.Amount, -1}, monthlyDisplayTerm{consumption.Refund, 1})
	return err
}

// previous must come from this same preparation run, not an unchecked customer
// payload. The series adapter rechecks retained plans against all source months.
// No reverse solving, legacy-wallet overwrite or source-gap repair is allowed.
func monthlyPlanBillCentMonth(groupID int64, identity string, p monthlyStatement, previous *monthlyBillCentMonth) (monthlyBillCentMonth, error) {
	var empty monthlyBillCentMonth
	if groupID <= 0 || !validMonthlyPublication(identity) {
		return empty, errMonthlyCentAllocation
	}
	consumption, err := monthlyPlanConsumptionCents(p)
	if err != nil {
		return empty, err
	}
	result := monthlyBillCentMonth{Version: 1, GroupID: groupID, SourceIdentity: identity, Month: p.Month,
		SourceSHA256: consumption.SourceSHA256, Consumption: consumption,
		Members: make([]monthlyBillCentMember, len(consumption.Members)), CreditRows: make([]monthlyCentScalar, len(p.Credits.Rows))}
	byID := make(map[int64]monthlyMemberStatement, len(p.Members))
	index := make(map[int64]int, len(p.Members))
	for _, member := range p.Members {
		byID[member.UserID] = member
	}
	openings := make([]monthlyCentScalar, len(result.Members))
	for i, m := range consumption.Members {
		source := byID[m.UserID]
		index[m.UserID] = i
		result.Members[i] = monthlyBillCentMember{UserID: m.UserID, Wallet: monthlyBillCentWallet{
			SourceOpening: source.Opening, SourceClosing: source.Closing}}
		openings[i] = monthlyCentScalar{ID: m.UserID, Amount: source.Opening}
	}
	result.Organization.SourceOpening, result.Organization.SourceClosing = *p.MonthOpeningBalance, *p.MonthEndBalance
	if err := monthlyBillCentOpenings(&result, openings, previous); err != nil {
		return empty, err
	}
	if err := monthlyBillCentFunding(&result, p, byID, index); err != nil {
		return empty, err
	}
	for i := range result.Members {
		if err := monthlyBillCentClose(&result.Members[i].Wallet, consumption.Members[i].Consumption); err != nil {
			return empty, err
		}
	}
	if err := monthlyBillCentClose(&result.Organization, consumption.Organization); err != nil {
		return empty, err
	}
	// Every visible account column must add to the visible organization column.
	columns := func(w monthlyBillCentWallet) []string {
		return []string{w.Opening, w.Recharge, w.Gift, w.CreditTotal, w.Adjustment, w.BalanceCorrection, w.Closing}
	}
	for column, total := range columns(result.Organization) {
		terms := make([]monthlyDisplayTerm, len(result.Members))
		for i, member := range result.Members {
			terms[i] = monthlyDisplayTerm{columns(member.Wallet)[column], 1}
		}
		sum, err := monthlyCentSum(terms...)
		if err != nil || sum != total {
			return empty, errMonthlyCentAllocation
		}
	}
	result.Digest = monthlyBillCentMonthDigest(result)
	return result, nil
}

func monthlyBillCentOpenings(plan *monthlyBillCentMonth, raw []monthlyCentScalar, previous *monthlyBillCentMonth) error {
	if previous == nil {
		rows, err := monthlyCentSplitDefault(plan.Organization.SourceOpening, raw)
		if err != nil {
			return err
		}
		n, _ := monthlyRoundedDisplayUnits(plan.Organization.SourceOpening, 2)
		plan.Organization.Opening = monthlyDisplayCentsMoney(n)
		for i, row := range rows {
			plan.Members[i].Wallet.Opening = row.Amount
		}
		return nil
	}
	date, _ := monthlyDisplayChainMonth(plan.Month)
	if previous.Version != 1 || previous.GroupID != plan.GroupID || previous.SourceIdentity != plan.SourceIdentity ||
		len(previous.Members) == 0 || len(previous.Members) > maxTrackedUsers ||
		previous.Month != date.AddDate(0, -1, 0).Format("2006-01") || previous.Digest != monthlyBillCentMonthDigest(*previous) ||
		!monthlyDisplayChainSourceEqual(previous.Organization.SourceClosing, plan.Organization.SourceOpening) {
		return errMonthlyDisplayChainConflict
	}
	plan.Organization.Opening = previous.Organization.Closing
	prior := make(map[int64]monthlyBillCentWallet, len(previous.Members))
	for _, m := range previous.Members {
		if _, duplicate := prior[m.UserID]; m.UserID <= 0 || duplicate {
			return errMonthlyDisplayChainConflict
		}
		prior[m.UserID] = m.Wallet
	}
	for i := range plan.Members {
		m := &plan.Members[i]
		p, exists := prior[m.UserID]
		if exists {
			if !monthlyDisplayChainSourceEqual(p.SourceClosing, m.Wallet.SourceOpening) {
				return errMonthlyDisplayChainConflict
			}
			m.Wallet.Opening = p.Closing
			delete(prior, m.UserID)
		} else {
			if m.Wallet.SourceOpening != "0.000000" {
				return errMonthlyDisplayChainConflict
			}
			m.Wallet.Opening = "0.000000"
		}
	}
	for _, m := range prior {
		if m.SourceClosing != "0.000000" || m.Closing != "0.000000" {
			return errMonthlyDisplayChainConflict
		}
	}
	return nil
}

func monthlyBillCentFunding(plan *monthlyBillCentMonth, p monthlyStatement, byID map[int64]monthlyMemberStatement, index map[int64]int) error {
	kinds := []string{"recharge", "gift", "adjustment", "balance_correction"}
	totals := []string{*p.Credits.RechargeTotal, *p.Credits.GiftTotal, p.Credits.Adjustment, monthlyCentOptional(p.Credits.BalanceCorrection)}
	for category, kind := range kinds {
		sums := make([]*big.Int, len(plan.Members))
		children := make([][]monthlyCentScalar, len(plan.Members))
		for i := range sums {
			sums[i] = new(big.Int)
		}
		for rowIndex, row := range p.Credits.Rows {
			if row.Kind != kind {
				continue
			}
			i, exists := index[row.UserID]
			if !exists {
				return errMonthlyCentAllocation
			}
			n, err := monthlyCentParse(row.Amount)
			if err != nil {
				return err
			}
			sums[i].Add(sums[i], n)
			children[i] = append(children[i], monthlyCentScalar{ID: int64(rowIndex), Amount: row.Amount})
		}
		raw := make([]monthlyCentScalar, len(plan.Members))
		for i, member := range plan.Members {
			if kind == "balance_correction" {
				n, err := monthlyCentParse(monthlyCentOptional(byID[member.UserID].BalanceCorrection))
				if err != nil {
					return err
				}
				sums[i] = n
			}
			raw[i] = monthlyCentScalar{ID: member.UserID, Amount: monthlyCentMicroText(sums[i])}
		}
		shown, err := monthlyCentSplitDefault(totals[category], raw)
		if err != nil {
			return err
		}
		n, _ := monthlyRoundedDisplayUnits(totals[category], 2)
		monthlyBillCentSetFunding(&plan.Organization, category, monthlyDisplayCentsMoney(n))
		for i, row := range shown {
			monthlyBillCentSetFunding(&plan.Members[i].Wallet, category, row.Amount)
			if kind == "balance_correction" {
				continue
			}
			entries, err := monthlyCentSplit(raw[i].Amount, row.Amount, children[i])
			if err != nil {
				return err
			}
			for _, entry := range entries {
				plan.CreditRows[int(entry.ID)] = entry
			}
		}
	}
	return nil
}

func monthlyBillCentSetFunding(wallet *monthlyBillCentWallet, category int, value string) {
	switch category {
	case 0:
		wallet.Recharge = value
	case 1:
		wallet.Gift = value
	case 2:
		wallet.Adjustment = value
	case 3:
		wallet.BalanceCorrection = value
	}
}
