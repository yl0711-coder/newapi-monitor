package monitor

// Fixed-range display-cent planning. This is a pure preparation step, NOT a
// publication/read endpoint. A trusted producer must supply an immutable,
// organization-owned supported range and independently verified source months.
// The range MUST NOT be inferred from the customer's selected month or from
// whichever month happened to finish first.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

const monthlyDisplaySeriesPolicy = "fixed-start-cents-v1"
const monthlyDisplaySeriesMonthLimit = 120

var (
	errMonthlyDisplaySeriesIncomplete = errors.New("monthly display series requires complete verified months")
	errMonthlyDisplaySeriesFrozen     = errors.New("monthly display series differs from an issued publication")
	errMonthlyDisplaySeriesCapacity   = errors.New("monthly display series exceeds preparation budget")
)

type monthlyDisplaySeriesScope struct {
	GroupID        int64
	SourceIdentity string // already audited canonical source origin; not a customer field
	From           string
	Through        string
}

type monthlyDisplaySeriesSource struct {
	GroupID        int64
	SourceIdentity string
	Statement      monthlyStatement
}

type monthlyDisplaySeriesFrozen struct {
	// Empty is a legacy publication, NOT a zero-money/empty-source assertion.
	// It must still match the canonical source and all amounts exactly.
	AnchorSHA256 string
	Link         monthlyDisplayChainLink
}

type monthlyDisplaySeriesPlan struct {
	Version        int                       `json:"version"`
	Policy         string                    `json:"policy"`
	GroupID        int64                     `json:"group_id"`
	SourceIdentity string                    `json:"source_identity"`
	From           string                    `json:"from"`
	Through        string                    `json:"through"`
	AnchorSHA256   string                    `json:"anchor_sha256"`
	PlanSHA256     string                    `json:"plan_sha256"`
	Months         []monthlyDisplayChainLink `json:"months"`
}

func monthlyDisplaySeriesAnchor(plan monthlyDisplaySeriesPlan) (string, error) {
	if len(plan.Months) == 0 {
		return "", errMonthlyDisplayChainInvalid
	}
	raw, err := json.Marshal(struct {
		Policy         string                  `json:"policy"`
		SourceIdentity string                  `json:"source_identity"`
		GroupID        int64                   `json:"group_id"`
		From           string                  `json:"from"`
		First          monthlyDisplayChainLink `json:"first"`
	}{plan.Policy, plan.SourceIdentity, plan.GroupID, plan.From, plan.Months[0]})
	if err != nil {
		return "", err
	}
	return monthlyDigest(raw), nil
}

func monthlyDisplaySeriesDigest(plan monthlyDisplaySeriesPlan) (string, error) {
	plan.PlanSHA256 = ""
	raw, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	if len(raw) > MonthlyPreparationInputLimit {
		return "", errMonthlyDisplaySeriesCapacity
	}
	return monthlyDigest(raw), nil
}

// Both links have already passed their individual digest checks. Compare
// semantic identity and every wallet, not only a sum or an opening/closing.
// Nil and empty legacy member collections are the same empty scope.
func monthlyDisplaySeriesSameLink(a, b monthlyDisplayChainLink) bool {
	if a.Version != b.Version || a.GroupID != b.GroupID || a.Month != b.Month || a.SourceSHA256 != b.SourceSHA256 || a.MemberDetailsVersion != b.MemberDetailsVersion || a.Organization != b.Organization || len(a.Members) != len(b.Members) {
		return false
	}
	for i := range a.Members {
		if a.Members[i] != b.Members[i] {
			return false
		}
	}
	return true
}

// Compute every supported month in chronological order BEFORE choosing which
// one a customer will publish. Existing bills are comparison-only constraints;
// never run the old next-month back-solving rule to move the starting balance.
// There is no fallback to zero, an incomplete month or a later source anchor.
// The caller must separately bind/recheck source hashes, current authorization,
// the complete published-record snapshot and the returned plan at publication.
func monthlyPlanDisplaySeries(ctx context.Context, scope monthlyDisplaySeriesScope, sources []monthlyDisplaySeriesSource, frozen []monthlyDisplaySeriesFrozen) (monthlyDisplaySeriesPlan, error) {
	var empty monthlyDisplaySeriesPlan
	if ctx == nil {
		return empty, errMonthlyDisplayChainInvalid
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	from, fromOK := monthlyDisplayChainMonth(scope.From)
	through, throughOK := monthlyDisplayChainMonth(scope.Through)
	if scope.GroupID <= 0 || !validMonthlyPublication(scope.SourceIdentity) || !fromOK || !throughOK || through.Before(from) {
		return empty, errMonthlyDisplayChainInvalid
	}
	count := (through.Year()-from.Year())*12 + int(through.Month()-from.Month()) + 1
	if count > monthlyDisplaySeriesMonthLimit || len(sources) > monthlyDisplaySeriesMonthLimit || len(frozen) > monthlyDisplaySeriesMonthLimit {
		return empty, errMonthlyDisplaySeriesCapacity
	}
	if len(sources) != count {
		return empty, errMonthlyDisplaySeriesIncomplete
	}
	byMonth := make(map[string]monthlyStatement, count)
	size := 0
	for _, source := range sources {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		p := source.Statement
		date, valid := monthlyDisplayChainMonth(p.Month)
		if source.GroupID != scope.GroupID || source.SourceIdentity != scope.SourceIdentity || !valid || date.Before(from) || date.After(through) || !p.Published || p.CurrentMonth || p.DisplayBalances != nil || !monthlyStatementAmountsComplete(p) {
			return empty, errMonthlyDisplayChainInvalid
		}
		if _, duplicate := byMonth[p.Month]; duplicate {
			return empty, errMonthlyDisplayChainInvalid
		}
		// A transport publication version is not a change to the raw facts.
		p.Publication = ""
		raw, err := json.Marshal(p)
		if err != nil {
			return empty, err
		}
		size += len(raw)
		if len(raw) > 2<<20 || size > MonthlyPreparationInputLimit {
			return empty, errMonthlyDisplaySeriesCapacity
		}
		byMonth[p.Month] = p
	}

	plan := monthlyDisplaySeriesPlan{Version: 1, Policy: monthlyDisplaySeriesPolicy, GroupID: scope.GroupID, SourceIdentity: scope.SourceIdentity, From: scope.From, Through: scope.Through, Months: make([]monthlyDisplayChainLink, 0, count)}
	for month := from; !month.After(through); month = month.AddDate(0, 1, 0) {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		key := month.Format("2006-01")
		p, present := byMonth[key]
		if !present {
			return empty, fmt.Errorf("%w: %s", errMonthlyDisplaySeriesIncomplete, key)
		}
		var previous []monthlyDisplayChainLink
		if len(plan.Months) > 0 {
			previous = []monthlyDisplayChainLink{plan.Months[len(plan.Months)-1]}
		}
		link, err := monthlyPlanDisplayChain(scope.GroupID, p, previous)
		if err != nil {
			return empty, fmt.Errorf("%s: %w", key, err)
		}
		plan.Months = append(plan.Months, link)
	}
	anchor, err := monthlyDisplaySeriesAnchor(plan)
	if err != nil {
		return empty, err
	}
	// Through is intentionally absent: extending the SAME fixed start must
	// not change the anchor or previously prepared customer-cent balances.
	plan.AnchorSHA256 = anchor
	indexed := make(map[string]monthlyDisplayChainLink, len(plan.Months))
	for _, link := range plan.Months {
		indexed[link.Month] = link
	}
	seen := make(map[string]bool, len(frozen))
	for _, issued := range frozen {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		link, exists := indexed[issued.Link.Month]
		if !exists || seen[issued.Link.Month] || !monthlyDisplayChainValidLink(issued.Link) || (issued.AnchorSHA256 != "" && issued.AnchorSHA256 != plan.AnchorSHA256) || !monthlyDisplaySeriesSameLink(link, issued.Link) {
			return empty, errMonthlyDisplaySeriesFrozen
		}
		seen[issued.Link.Month] = true
	}
	digest, err := monthlyDisplaySeriesDigest(plan)
	if err != nil {
		return empty, err
	}
	plan.PlanSHA256 = digest
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return plan, nil
}

// Read an already prepared common plan into the existing display DTO. This
// does not publish or add a customer route. Hash validation detects corruption;
// it is NOT source attestation or authorization. The eventual publication
// adapter must bind this exact plan to trusted retained preparation and recheck
// its complete source/frozen-record snapshot inside the write transaction.
func monthlySelectDisplaySeries(ctx context.Context, plan monthlyDisplaySeriesPlan, source monthlyDisplaySeriesSource) (*monthlyDisplayBalances, error) {
	if ctx == nil {
		return nil, errMonthlyDisplayChainInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	from, fromOK := monthlyDisplayChainMonth(plan.From)
	through, throughOK := monthlyDisplayChainMonth(plan.Through)
	if plan.Version != 1 || plan.Policy != monthlyDisplaySeriesPolicy || plan.GroupID <= 0 || plan.GroupID != source.GroupID || !validMonthlyPublication(plan.SourceIdentity) || source.SourceIdentity != plan.SourceIdentity || !fromOK || !throughOK || through.Before(from) || len(plan.Months) == 0 || len(plan.Months) > monthlyDisplaySeriesMonthLimit {
		return nil, errMonthlyDisplayChainInvalid
	}
	count := (through.Year()-from.Year())*12 + int(through.Month()-from.Month()) + 1
	if count != len(plan.Months) || source.Statement.DisplayBalances != nil || !source.Statement.Published || source.Statement.CurrentMonth || !monthlyStatementAmountsComplete(source.Statement) {
		return nil, errMonthlyDisplayChainInvalid
	}
	digest, err := monthlyDisplaySeriesDigest(plan)
	if err != nil || digest != plan.PlanSHA256 {
		return nil, errMonthlyDisplayChainInvalid
	}
	anchor, err := monthlyDisplaySeriesAnchor(plan)
	if err != nil || anchor != plan.AnchorSHA256 {
		return nil, errMonthlyDisplayChainInvalid
	}
	var selected *monthlyDisplayChainLink
	for i := range plan.Months {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		link := &plan.Months[i]
		if link.GroupID != plan.GroupID || link.Month != from.AddDate(0, i, 0).Format("2006-01") || !monthlyDisplayChainValidLink(*link) {
			return nil, errMonthlyDisplayChainInvalid
		}
		if i == 0 {
			opening, _ := monthlyRoundedDisplayUnits(link.Organization.SourceOpening, 2)
			if monthlyDisplayCentsMoney(opening) != link.Organization.Opening {
				return nil, errMonthlyDisplayChainInvalid
			}
			for _, member := range link.Members {
				opening, _ := monthlyRoundedDisplayUnits(member.Wallet.SourceOpening, 2)
				if monthlyDisplayCentsMoney(opening) != member.Wallet.Opening {
					return nil, errMonthlyDisplayChainInvalid
				}
			}
		} else if !monthlyDisplayChainAdjacent(plan.Months[i-1], *link) {
			return nil, errMonthlyDisplayChainConflict
		}
		if link.Month == source.Statement.Month {
			selected = link
		}
	}
	if selected == nil {
		return nil, errMonthlyDisplaySeriesIncomplete
	}
	solo, err := monthlyPlanDisplayChain(source.GroupID, source.Statement, nil)
	if err != nil || solo.SourceSHA256 != selected.SourceSHA256 || solo.MemberDetailsVersion != selected.MemberDetailsVersion || solo.Organization.SourceOpening != selected.Organization.SourceOpening || solo.Organization.SourceClosing != selected.Organization.SourceClosing || solo.Organization.Delta != selected.Organization.Delta || len(solo.Members) != len(selected.Members) {
		return nil, errMonthlyDisplayChainInvalid
	}
	for i, member := range solo.Members {
		fixed := selected.Members[i]
		if member.UserID != fixed.UserID || member.Wallet.SourceOpening != fixed.Wallet.SourceOpening || member.Wallet.SourceClosing != fixed.Wallet.SourceClosing || member.Wallet.Delta != fixed.Wallet.Delta {
			return nil, errMonthlyDisplayChainInvalid
		}
	}
	display := &monthlyDisplayBalances{Version: 1, Organization: monthlyDisplayBalance{Opening: selected.Organization.Opening, Closing: selected.Organization.Closing}, Members: []monthlyDisplayBalanceMember{}}
	for _, member := range selected.Members {
		display.Members = append(display.Members, monthlyDisplayBalanceMember{UserID: member.UserID, Opening: member.Wallet.Opening, Closing: member.Wallet.Closing})
	}
	view := source.Statement
	view.DisplayBalances = display
	if !monthlyStatementAmountsComplete(view) {
		return nil, errMonthlyDisplayChainInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return display, nil
}
