package monitor

// Pure, local planning for frozen display-cent balances. No database, source
// queries, publication, wallet mutation or customer response is changed here.
// Persistence/HTTP integration must separately bind and atomically recheck all
// referenced bill versions before exposing a plan to customers.
import (
	"encoding/json"
	"errors"
	"math/big"
	"sort"
	"time"
)

var (
	errMonthlyDisplayChainInvalid  = errors.New("invalid monthly display chain input")
	errMonthlyDisplayChainConflict = errors.New("monthly display chain requires reconciliation")
)

type monthlyDisplayChainWallet struct {
	SourceOpening string `json:"source_opening"`
	SourceClosing string `json:"source_closing"`
	Opening       string `json:"opening"`
	Closing       string `json:"closing"`
	Delta         string `json:"delta"`
}

type monthlyDisplayChainMember struct {
	UserID int64                     `json:"user_id"`
	Wallet monthlyDisplayChainWallet `json:"wallet"`
}

// A presentation record, not a replacement for the source bill. Frozen records
// are never rewritten to fit a later-created neighboring month.
type monthlyDisplayChainLink struct {
	Version              int                         `json:"version"`
	GroupID              int64                       `json:"group_id"`
	Month                string                      `json:"month"`
	SourceSHA256         string                      `json:"source_sha256"`
	MemberDetailsVersion int                         `json:"member_details_version"`
	Organization         monthlyDisplayChainWallet   `json:"organization"`
	Members              []monthlyDisplayChainMember `json:"members"`
	Digest               string                      `json:"digest"`
}

func monthlyDisplayChainDigest(link monthlyDisplayChainLink) string {
	link.Digest = ""
	raw, _ := json.Marshal(link)
	return monthlyDigest(raw)
}

func monthlyDisplayChainMonth(value string) (time.Time, bool) {
	m, err := time.Parse("2006-01", value)
	return m, err == nil && m.Year() >= 2020 && m.Format("2006-01") == value
}

func monthlyDisplayChainCents(value string) (*big.Int, bool) {
	n, ok := monthlyRoundedDisplayUnits(value, 2)
	return n, ok && monthlyDisplayCentsMoney(n) == value
}

func monthlyDisplayChainSourceEqual(a, b string) bool {
	x, xok := monthlyRoundedDisplayUnits(a, 6)
	y, yok := monthlyRoundedDisplayUnits(b, 6)
	return xok && yok && x.Cmp(y) == 0
}

func monthlyDisplayChainValidWallet(w monthlyDisplayChainWallet) bool {
	_, sourceOK := monthlyRoundedDisplayUnits(w.SourceOpening, 6)
	_, closingOK := monthlyRoundedDisplayUnits(w.SourceClosing, 6)
	o, ook := monthlyDisplayChainCents(w.Opening)
	c, cok := monthlyDisplayChainCents(w.Closing)
	d, dok := monthlyDisplayChainCents(w.Delta)
	return sourceOK && closingOK && ook && cok && dok && c.Sub(c, o).Cmp(d) == 0
}

func monthlyDisplayChainValidLink(link monthlyDisplayChainLink) bool {
	_, monthOK := monthlyDisplayChainMonth(link.Month)
	if link.Version != 1 || link.GroupID <= 0 || !monthOK || !validMonthlyPublication(link.SourceSHA256) || link.Digest != monthlyDisplayChainDigest(link) || !monthlyDisplayChainValidWallet(link.Organization) || len(link.Members) > maxTrackedUsers {
		return false
	}
	if link.MemberDetailsVersion == 0 {
		return len(link.Members) == 0
	}
	if (link.MemberDetailsVersion != 1 && link.MemberDetailsVersion != 2) || len(link.Members) == 0 {
		return false
	}
	var last int64
	for _, member := range link.Members {
		if member.UserID <= last || !monthlyDisplayChainValidWallet(member.Wallet) {
			return false
		}
		last = member.UserID
	}
	return true
}

func monthlyDisplayChainPlanWallet(m monthlyMemberStatement, previous, next *monthlyDisplayChainWallet) (monthlyDisplayChainWallet, error) {
	w := monthlyDisplayChainWallet{SourceOpening: m.Opening, SourceClosing: m.Closing}
	derived := monthlyWalletDisplayClosing(m.Opening, m.CreditTotal, m.Adjustment, m.Consumption, m.BalanceCorrection, m.Closing)
	if derived == "" {
		return w, errMonthlyDisplayChainInvalid
	}
	opening, _ := monthlyRoundedDisplayUnits(m.Opening, 2)
	closing, _ := monthlyRoundedDisplayUnits(derived, 2)
	delta := new(big.Int).Sub(closing, opening)
	if previous != nil {
		if !monthlyDisplayChainSourceEqual(previous.SourceClosing, m.Opening) {
			return w, errMonthlyDisplayChainConflict
		}
		opening, _ = monthlyDisplayChainCents(previous.Closing)
	} else if next != nil {
		end, _ := monthlyDisplayChainCents(next.Opening)
		opening.Sub(end, delta)
	}
	closing.Add(opening, delta)
	if next != nil && (!monthlyDisplayChainSourceEqual(m.Closing, next.SourceOpening) || monthlyDisplayCentsMoney(closing) != next.Opening) {
		return w, errMonthlyDisplayChainConflict
	}
	w.Opening, w.Closing, w.Delta = monthlyDisplayCentsMoney(opening), monthlyDisplayCentsMoney(closing), monthlyDisplayCentsMoney(delta)
	return w, nil
}

// Compatibility checks include raw source boundaries, not just rounded cents.
// Nonzero balances cannot disappear when an organization's roster changes.
func monthlyDisplayChainAdjacent(previous, next monthlyDisplayChainLink) bool {
	if !monthlyDisplayChainSourceEqual(previous.Organization.SourceClosing, next.Organization.SourceOpening) || previous.Organization.Closing != next.Organization.Opening {
		return false
	}
	if previous.MemberDetailsVersion == 0 || next.MemberDetailsVersion == 0 {
		return true // Legacy bills have no customer-visible per-account boundaries.
	}
	prior := make(map[int64]monthlyDisplayChainWallet, len(previous.Members))
	for _, member := range previous.Members {
		prior[member.UserID] = member.Wallet
	}
	for _, member := range next.Members {
		p, found := prior[member.UserID]
		if found {
			if !monthlyDisplayChainSourceEqual(p.SourceClosing, member.Wallet.SourceOpening) || p.Closing != member.Wallet.Opening {
				return false
			}
			delete(prior, member.UserID)
		} else if !monthlyDisplayChainSourceEqual(member.Wallet.SourceOpening, "0.000000") || member.Wallet.Opening != "0.000000" {
			return false
		}
	}
	for _, p := range prior {
		if !monthlyDisplayChainSourceEqual(p.SourceClosing, "0.000000") || p.Closing != "0.000000" {
			return false
		}
	}
	return true
}

// neighbors contains at most previous/current/next month from the same owner.
// Reverse-order requests work too: an already frozen next opening pins the
// preceding closing. A contradictory bridge fails without changing either end.
func monthlyPlanDisplayChain(groupID int64, p monthlyStatement, neighbors []monthlyDisplayChainLink) (monthlyDisplayChainLink, error) {
	var empty monthlyDisplayChainLink
	month, monthOK := monthlyDisplayChainMonth(p.Month)
	if groupID <= 0 || !monthOK || !p.Published || p.CurrentMonth || !monthlyStatementAmountsComplete(p) || len(neighbors) > 3 {
		return empty, errMonthlyDisplayChainInvalid
	}
	// Publication is derived HTTP metadata, not a frozen source-payload field.
	// The caller must separately authorize/recheck the issuance version. Reading
	// the same saved payload must not change its cent plan solely by adding it.
	source := p
	source.Publication = ""
	source.DisplayBalances = nil
	raw, _ := json.Marshal(source)
	result := monthlyDisplayChainLink{Version: 1, GroupID: groupID, Month: p.Month, SourceSHA256: monthlyDigest(raw), MemberDetailsVersion: p.MemberDetailsVersion}
	var previous, existing, next *monthlyDisplayChainLink
	for i := range neighbors {
		link := &neighbors[i]
		if link.GroupID != groupID || !monthlyDisplayChainValidLink(*link) {
			return empty, errMonthlyDisplayChainInvalid
		}
		var slot **monthlyDisplayChainLink
		switch link.Month {
		case month.AddDate(0, -1, 0).Format("2006-01"):
			slot = &previous
		case p.Month:
			slot = &existing
		case month.AddDate(0, 1, 0).Format("2006-01"):
			slot = &next
		default:
			return empty, errMonthlyDisplayChainInvalid
		}
		if *slot != nil {
			return empty, errMonthlyDisplayChainInvalid
		}
		*slot = link
	}
	if existing != nil {
		if existing.SourceSHA256 != result.SourceSHA256 {
			return empty, errMonthlyDisplayChainConflict
		}
		result = *existing
		if existing.Members != nil {
			result.Members = make([]monthlyDisplayChainMember, len(existing.Members))
			copy(result.Members, existing.Members)
		}
	} else {
		org := monthlyMemberStatement{Opening: *p.MonthOpeningBalance, Closing: *p.MonthEndBalance, CreditTotal: p.Credits.KnownTotal, Adjustment: p.Credits.Adjustment, BalanceCorrection: p.Credits.BalanceCorrection, Consumption: *p.Consumption}
		var priorWallet, nextWallet *monthlyDisplayChainWallet
		if previous != nil {
			priorWallet = &previous.Organization
		}
		if next != nil {
			nextWallet = &next.Organization
		}
		var err error
		result.Organization, err = monthlyDisplayChainPlanWallet(org, priorWallet, nextWallet)
		if err != nil {
			return empty, err
		}
		priorMembers, nextMembers := map[int64]monthlyDisplayChainWallet{}, map[int64]monthlyDisplayChainWallet{}
		if previous != nil {
			for _, member := range previous.Members {
				priorMembers[member.UserID] = member.Wallet
			}
		}
		if next != nil {
			for _, member := range next.Members {
				nextMembers[member.UserID] = member.Wallet
			}
		}
		for _, member := range p.Members {
			priorWallet, nextWallet = nil, nil
			if wallet, ok := priorMembers[member.UserID]; ok {
				priorWallet = &wallet
			}
			if wallet, ok := nextMembers[member.UserID]; ok {
				nextWallet = &wallet
			}
			wallet, err := monthlyDisplayChainPlanWallet(member, priorWallet, nextWallet)
			if err != nil {
				return empty, err
			}
			result.Members = append(result.Members, monthlyDisplayChainMember{UserID: member.UserID, Wallet: wallet})
		}
		sort.Slice(result.Members, func(i, j int) bool { return result.Members[i].UserID < result.Members[j].UserID })
		result.Digest = monthlyDisplayChainDigest(result)
	}
	if previous != nil && !monthlyDisplayChainAdjacent(*previous, result) || next != nil && !monthlyDisplayChainAdjacent(result, *next) {
		return empty, errMonthlyDisplayChainConflict
	}
	if !monthlyDisplayChainValidLink(result) {
		return empty, errMonthlyDisplayChainInvalid
	}
	return result, nil
}
