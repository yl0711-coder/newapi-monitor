package monitor

import "gorm.io/gorm"

const monthlyFrozenPublicationPrefix = "cents-chain-v1:"

// Optional, immutable presentation data inside the existing bill payload.
// Source balances and actual charges are retained separately, at full precision.
// No schema migration or additional database/ledger is needed.
type monthlyDisplayBalance struct {
	Opening string `json:"opening"`
	Closing string `json:"closing"`
}

type monthlyDisplayBalanceMember struct {
	UserID  int64  `json:"user_id"`
	Opening string `json:"opening"`
	Closing string `json:"closing"`
}

type monthlyDisplayBalances struct {
	Version      int                           `json:"version"`
	Organization monthlyDisplayBalance         `json:"organization"`
	Members      []monthlyDisplayBalanceMember `json:"members"`
}

func monthlyDisplayWallet(m monthlyMemberStatement, frozen *monthlyDisplayBalance) (string, string, bool) {
	closing := monthlyWalletDisplayClosing(m.Opening, m.CreditTotal, m.Adjustment, m.Consumption, m.BalanceCorrection, m.Closing)
	if closing == "" {
		return "", "", false
	}
	opening, _ := monthlyRoundedDisplayUnits(m.Opening, 2)
	if frozen == nil {
		return monthlyDisplayCentsMoney(opening), closing, true
	}
	fo, ook := monthlyDisplayChainCents(frozen.Opening)
	fc, cok := monthlyDisplayChainCents(frozen.Closing)
	if !ook || !cok {
		return "", "", false
	}
	delta, _ := monthlyRoundedDisplayUnits(closing, 2)
	delta.Sub(delta, opening)
	if fc.Sub(fc, fo).Cmp(delta) != 0 {
		return "", "", false
	}
	return frozen.Opening, frozen.Closing, true
}

func monthlyOrganizationWallet(p monthlyStatement) (monthlyMemberStatement, bool) {
	if p.MonthOpeningBalance == nil || p.MonthEndBalance == nil || p.Consumption == nil {
		return monthlyMemberStatement{}, false
	}
	return monthlyMemberStatement{Opening: *p.MonthOpeningBalance, Closing: *p.MonthEndBalance,
		CreditTotal: p.Credits.KnownTotal, Adjustment: p.Credits.Adjustment,
		Consumption: *p.Consumption, BalanceCorrection: p.Credits.BalanceCorrection}, true
}

func monthlyDisplayBalancesValid(p monthlyStatement) bool {
	d := p.DisplayBalances
	if d == nil {
		return true
	}
	org, ok := monthlyOrganizationWallet(p)
	if !ok || d.Version != 1 || len(d.Members) != len(p.Members) || len(d.Members) > maxTrackedUsers {
		return false
	}
	if _, _, ok := monthlyDisplayWallet(org, &d.Organization); !ok {
		return false
	}
	byID := make(map[int64]monthlyMemberStatement, len(p.Members))
	for _, m := range p.Members {
		byID[m.UserID] = m
	}
	var last int64
	for _, f := range d.Members {
		m, found := byID[f.UserID]
		if !found || f.UserID <= last {
			return false
		}
		if _, _, ok := monthlyDisplayWallet(m, &monthlyDisplayBalance{Opening: f.Opening, Closing: f.Closing}); !ok {
			return false
		}
		last = f.UserID
	}
	return true
}

func monthlyDisplayMemberIndex(p monthlyStatement) map[int64]*monthlyDisplayBalance {
	index := make(map[int64]*monthlyDisplayBalance)
	if p.DisplayBalances != nil {
		for _, member := range p.DisplayBalances.Members {
			index[member.UserID] = &monthlyDisplayBalance{Opening: member.Opening, Closing: member.Closing}
		}
	}
	return index
}

// Read an immutable neighboring publication. Legacy bills keep exactly their
// existing standalone cents presentation; do not rewrite them during a GET.
func monthlyDisplayLinkFromBill(groupID int64, p monthlyStatement) (monthlyDisplayChainLink, error) {
	if !monthlyStatementAmountsComplete(p) {
		return monthlyDisplayChainLink{}, errMonthlyDisplayChainInvalid
	}
	source := p
	source.DisplayBalances = nil
	link, err := monthlyPlanDisplayChain(groupID, source, nil)
	if err != nil || p.DisplayBalances == nil {
		return link, err
	}
	link.Organization.Opening = p.DisplayBalances.Organization.Opening
	link.Organization.Closing = p.DisplayBalances.Organization.Closing
	for i, member := range p.DisplayBalances.Members {
		link.Members[i].Wallet.Opening, link.Members[i].Wallet.Closing = member.Opening, member.Closing
	}
	link.Digest = monthlyDisplayChainDigest(link)
	if !monthlyDisplayChainValidLink(link) {
		return monthlyDisplayChainLink{}, errMonthlyDisplayChainInvalid
	}
	return link, nil
}

func monthlyFreezeDisplayBalances(tx *gorm.DB, groupID int64, p *monthlyStatement) error {
	// The legacy entrypoint in THIS binary must not extend a series using its
	// request-order-dependent next-month back-solving rule. A rollback to an
	// older binary still requires automatic generation to be explicitly off.
	var series int64
	if err := tx.Model(&PortalMonthlyBill{}).Where("group_id = ? AND (publication_id LIKE ? OR publication_id LIKE ?)", groupID, monthlySeriesPublicationPrefix+"%", monthlyCentBillPublicationPrefix+"%").Limit(1).Count(&series).Error; err != nil {
		return err
	}
	if series != 0 {
		return errMonthlyDisplaySeriesIncomplete
	}
	month, ok := monthlyDisplayChainMonth(p.Month)
	if !ok {
		return errMonthlyDisplayChainInvalid
	}
	var records []PortalMonthlyBill
	columns := "group_id, month, payload, payload_hash, member_fingerprint, member_manifest, published_at, publication_id"
	if err := tx.Select(columns).Where("group_id = ? AND month IN ?", groupID,
		[]string{month.AddDate(0, -1, 0).Format("2006-01"), month.AddDate(0, 1, 0).Format("2006-01")}).Find(&records).Error; err != nil {
		return err
	}
	neighbors := make([]monthlyDisplayChainLink, 0, len(records))
	for _, record := range records {
		peer, err := readPublishedMonthlyBill(record)
		if err != nil {
			return err
		}
		link, err := monthlyDisplayLinkFromBill(groupID, peer)
		if err != nil {
			return err
		}
		neighbors = append(neighbors, link)
	}
	plan, err := monthlyPlanDisplayChain(groupID, *p, neighbors)
	if err != nil {
		return err
	}
	frozen := &monthlyDisplayBalances{Version: 1, Organization: monthlyDisplayBalance{Opening: plan.Organization.Opening, Closing: plan.Organization.Closing}, Members: []monthlyDisplayBalanceMember{}}
	for _, m := range plan.Members {
		frozen.Members = append(frozen.Members, monthlyDisplayBalanceMember{UserID: m.UserID, Opening: m.Wallet.Opening, Closing: m.Wallet.Closing})
	}
	p.DisplayBalances = frozen
	if !monthlyDisplayBalancesValid(*p) {
		return errMonthlyDisplayChainInvalid
	}
	return nil
}
