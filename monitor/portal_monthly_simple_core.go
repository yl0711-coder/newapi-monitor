package monitor

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Pure computation of one organization's monthly bill (no IO).
//
// Balances: NewAPI keeps no balance history. Every month is computed from the
// logs alone, carried forward from 0 at registration:
//
//	closing = opening + credits - consumption + refund
//
// so each month's consumption, refunds and credits equal the logs exactly and
// the result does not depend on which month is generated first. Once a
// boundary is used it is frozen (raw quota and displayed cents) and reused by
// every later generation, which keeps adjacent bills continuous.
//
// The live wallet is used only as an internal check: the difference between it
// and the log-implied balance is recorded in the audit table (never shown to
// the customer); a difference above the limit holds the bill for review.

type errSimpleRetry string

func (e errSimpleRetry) Error() string { return "retry: " + string(e) }

type errSimpleReview string

// errSimplePublication is a failed publication: retried, but held for review
// after simplePublishAttempts failures (e.g. a conflicting saved bill).
type errSimplePublication string

func (e errSimplePublication) Error() string { return "retry: " + string(e) }

const simplePublishAttempts = 3

func (e errSimpleReview) Error() string { return "review: " + string(e) }

type simpleBoundaryKey struct {
	UserID int64
	Month  string // balance at 00:00 CST on the 1st of Month
}

type simpleFrozen struct {
	Raw   int64
	Cents int64
}

type simpleOrgInput struct {
	GroupID       int64
	Month         string
	Members       []int64
	Snapshot      simpleSnapshot
	Days          []simpleFactDay // date_ts < Snapshot.TailFrom
	SourceFloor   map[int64]int64 // first logged hour per member (0 = unknown)
	Frozen        map[simpleBoundaryKey]simpleFrozen
	ResidualLimit int64 // quota
	Now           time.Time
	SourceRef     string
	Plans         map[int64]simplePlan // subscription plan -> customer name and price
	SubOverrides  map[int64]simplePlan // per subscription ID (for example a free trial)
}

// simplePlan is the customer-facing description of a NewAPI subscription plan.
type simplePlan struct {
	Name       string `json:"name"`
	PriceCents int64  `json:"price_cents"`
	Trial      bool   `json:"trial,omitempty"`
}

// simpleSubscriptionBill is the subscription part of one organization month.
// It is stored next to the bill (write-once, digest in the bill's source
// reference) and merged into the customer view and workbook.
type simpleSubscriptionBill struct {
	Version int                       `json:"version"`
	Orders  []simpleSubscriptionOrder `json:"orders"` // opened in this month
	Users   []simpleSubscriptionUser  `json:"users"`  // subscription-paid usage in this month
}

type simpleSubscriptionOrder struct {
	ID         int64  `json:"id"`
	UserID     int64  `json:"user_id"`
	Name       string `json:"name"`
	Quantity   int64  `json:"quantity"`
	PriceCents int64  `json:"price_cents"`
	Trial      bool   `json:"trial,omitempty"`
	Start      int64  `json:"start"`
	End        int64  `json:"end"`
	Active     bool   `json:"active"` // still in service at the end of the month
}

type simpleSubscriptionUser struct {
	UserID       int64                     `json:"user_id"`
	ConsumeCents int64                     `json:"consume_cents"`
	RefundCents  int64                     `json:"refund_cents"`
	Tokens       []simpleSubscriptionToken `json:"tokens"`
}

type simpleSubscriptionToken struct {
	TokenID      int64  `json:"token_id"`
	Name         string `json:"name"`
	ConsumeCents int64  `json:"consume_cents"`
	RefundCents  int64  `json:"refund_cents"`
}

type simpleAuditRow struct {
	UserID                                         int64
	OpeningRaw, ClosingRaw, CreditRaw              int64
	LogConsume, LogRefund, Consume, WalletResidual int64
	OpeningCents, ClosingCents, ConsumeCents       int64
	RefundCents, CreditCents                       int64
}

type simpleOrgOutput struct {
	Evidence MonthlyBillEvidence
	Freeze   map[simpleBoundaryKey]simpleFrozen // newly decided boundaries
	Audit    []simpleAuditRow
	Sub      *simpleSubscriptionBill // nil when the month has no subscription activity
}

func simpleMonthStart(month string) (time.Time, error) {
	t, err := time.ParseInLocation("2006-01", month, usageCST)
	if err != nil || t.Format("2006-01") != month {
		return time.Time{}, errors.New("invalid month")
	}
	return t, nil
}

func simpleMonthOf(ts int64) string { return time.Unix(ts, 0).In(usageCST).Format("2006-01") }

type simpleUserMonth struct {
	logConsume, logRefund int64
	requests, tokens      int64
	tokenConsume          map[int64]int64
	tokenRefund           map[int64]int64
	tokenName             map[int64]string
	events                []simpleEvent
	tokenSubConsume       map[int64]int64 // subscription-paid (not wallet) usage per token
	tokenSubRefund        map[int64]int64
}

func simpleBuildOrgMonth(in simpleOrgInput) (simpleOrgOutput, error) {
	var out simpleOrgOutput
	target, err := simpleMonthStart(in.Month)
	if err != nil || len(in.Members) == 0 {
		return out, errSimpleReview("invalid request")
	}
	end := target.AddDate(0, 1, 0)
	if end.Unix() > in.Snapshot.TailFrom {
		return out, errSimpleRetry("usage facts do not cover the month yet")
	}
	if len(in.Snapshot.Review) > 0 {
		return out, errSimpleReview(fmt.Sprintf("unrecognized funding rows %v", in.Snapshot.Review))
	}
	members := append([]int64(nil), in.Members...)
	sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })

	// Organization chain: from the earliest member registration month.
	first := ""
	for _, uid := range members {
		reg, ok := in.Snapshot.Registered[uid]
		if !ok || reg <= 0 {
			return out, errSimpleReview("registration time missing")
		}
		if floor := in.SourceFloor[uid]; floor > 0 && reg < floor-86400 {
			return out, errSimpleReview("account registered before the retained log history")
		}
		if m := simpleMonthOf(reg); first == "" || m < first {
			first = m
		}
	}
	if in.Month < first {
		return out, errSimpleReview("month before registration")
	}
	firstStart, _ := simpleMonthStart(first)
	var months []time.Time
	for t := firstStart; !t.After(target); t = t.AddDate(0, 1, 0) {
		months = append(months, t)
	}
	if len(months) > 120 {
		return out, errSimpleReview("too many months")
	}
	boundaries := append(append([]time.Time(nil), months...), end) // len(months)+1

	// Per user per month inputs.
	data := map[int64][]simpleUserMonth{}
	monthIndex := map[string]int{}
	for i, t := range months {
		monthIndex[t.Format("2006-01")] = i
	}
	for _, uid := range members {
		ums := make([]simpleUserMonth, len(months))
		for i := range ums {
			ums[i] = simpleUserMonth{tokenConsume: map[int64]int64{}, tokenRefund: map[int64]int64{}, tokenName: map[int64]string{},
				tokenSubConsume: map[int64]int64{}, tokenSubRefund: map[int64]int64{}}
		}
		data[uid] = ums
	}
	for _, d := range in.Days {
		ums, ok := data[d.UserID]
		if !ok || d.DateTS >= in.Snapshot.TailFrom {
			continue
		}
		i, ok := monthIndex[simpleMonthOf(d.DateTS)]
		if !ok {
			continue
		}
		um := &ums[i]
		um.logConsume += d.Consume
		um.logRefund += d.Refund
		um.requests += d.Requests
		um.tokens += d.Tokens
		um.tokenConsume[d.TokenID] += d.Consume
		um.tokenRefund[d.TokenID] += d.Refund
		if d.TokenName != "" {
			um.tokenName[d.TokenID] = d.TokenName
		}
	}
	// Split subscription-paid usage out of the daily facts: the wallet (and so
	// the balance chain) only carries what the wallet paid.
	type subKey struct{ uid, token, day int64 }
	subByDay := map[subKey][2]int64{}
	for _, d := range in.Snapshot.SubDays {
		k := subKey{d.UserID, d.TokenID, d.DateTS}
		v := subByDay[k]
		subByDay[k] = [2]int64{v[0] + d.Consume, v[1] + d.Refund}
	}
	factByDay := map[subKey][2]int64{}
	for _, d := range in.Days {
		if d.DateTS < in.Snapshot.TailFrom {
			k := subKey{d.UserID, d.TokenID, d.DateTS}
			v := factByDay[k]
			factByDay[k] = [2]int64{v[0] + d.Consume, v[1] + d.Refund}
		}
	}
	for k, v := range subByDay {
		ums, ok := data[k.uid]
		if !ok {
			continue
		}
		f := factByDay[k]
		if v[0] > f[0] || v[1] > f[1] {
			return out, errSimpleRetry(fmt.Sprintf("account %d subscription usage not in the daily facts yet", k.uid))
		}
		i, ok := monthIndex[simpleMonthOf(k.day)]
		if !ok {
			continue
		}
		um := &ums[i]
		um.logConsume -= v[0]
		um.logRefund -= v[1]
		um.tokenConsume[k.token] -= v[0]
		um.tokenRefund[k.token] -= v[1]
		um.tokenSubConsume[k.token] += v[0]
		um.tokenSubRefund[k.token] += v[1]
		if um.tokenConsume[k.token] == 0 && um.tokenRefund[k.token] == 0 {
			delete(um.tokenConsume, k.token)
			delete(um.tokenRefund, k.token)
		}
	}
	for _, e := range in.Snapshot.Events {
		if ums, ok := data[e.UserID]; ok {
			if i, ok := monthIndex[simpleMonthOf(e.At)]; ok {
				ums[i].events = append(ums[i].events, e)
			}
		}
	}

	// Reconstructed (log-implied) balance at each boundary.
	reconstruct := func(uid int64, at int64) int64 {
		b := in.Snapshot.BalanceAtTail(uid)
		for _, e := range in.Snapshot.Events {
			if e.UserID == uid && e.At >= at && e.At < in.Snapshot.TailFrom {
				b -= e.Delta
			}
		}
		for _, d := range in.Days {
			if d.UserID == uid && d.DateTS >= at && d.DateTS < in.Snapshot.TailFrom {
				b += d.Consume - d.Refund
			}
		}
		for _, d := range in.Snapshot.SubDays {
			if d.UserID == uid && d.DateTS >= at && d.DateTS < in.Snapshot.TailFrom {
				b -= d.Consume - d.Refund // paid by the subscription, not the wallet
			}
		}
		return b
	}

	// Raw boundaries, carried forward from the logs only:
	//   closing = opening + credits - consumption + refund
	// starting at 0 before registration, or at the last frozen boundary. Every
	// month's consumption is exactly its logged consumption.
	raw := map[int64][]int64{}
	settled := map[int64][]int64{}  // consumption per month (= logged)
	residualAt := map[int64]int64{} // internal: wallet balance minus log-implied balance at the chain end
	for _, uid := range members {
		reg := in.Snapshot.Registered[uid]
		rb := make([]int64, len(boundaries))
		known := make([]bool, len(boundaries))
		anchor := -1
		for bi, t := range boundaries {
			if t.Unix() <= reg {
				rb[bi], known[bi] = 0, true // before registration
				anchor = bi
				continue
			}
			if f, ok := in.Frozen[simpleBoundaryKey{uid, t.Format("2006-01")}]; ok {
				rb[bi], known[bi] = f.Raw, true
				anchor = bi
			}
		}
		if anchor < 0 {
			return out, errSimpleReview("no balance anchor")
		}
		for bi := anchor + 1; bi < len(boundaries); bi++ {
			if known[bi] {
				return out, errSimpleReview("frozen balances are not a prefix")
			}
		}
		st := make([]int64, len(months))
		for mi := range months {
			um := data[uid][mi]
			credits := simpleSumEvents(um.events)
			if mi >= anchor {
				rb[mi+1] = rb[mi] + credits - um.logConsume + um.logRefund
			}
			st[mi] = rb[mi] + credits + um.logRefund - rb[mi+1]
			// A frozen month must still match its logs exactly.
			if st[mi] != um.logConsume {
				return out, errSimpleReview(fmt.Sprintf("account %d month %s does not reconcile with its logs", uid, months[mi].Format("2006-01")))
			}
		}
		// Internal reconciliation only (never shown to the customer): NewAPI's
		// wallet against the log-implied balance at the end of the chain. A
		// large difference means money moved outside the logs (for example an
		// unlogged balance edit) and the bill is held for review.
		residual := reconstruct(uid, end.Unix()) - rb[len(boundaries)-1]
		if residual < 0 {
			// The wallet holds less than the logs imply: most likely a request
			// charged but not logged yet. Retry; a persistent case is held.
			return out, errSimpleRetry(fmt.Sprintf("account %d balance below its logs", uid))
		}
		if residual > in.ResidualLimit {
			return out, errSimpleReview(fmt.Sprintf("account %d balance/log difference over limit", uid))
		}
		raw[uid], settled[uid], residualAt[uid] = rb, st, residual
	}

	// Controlled rounding over the whole chain.
	// Credits are shown per event and as totals per account and kind. Within
	// each (account, month, kind) the events are allocated by largest
	// remainder so the kind total is the rounded true total and every event
	// stays within one cent; the remaining totals are checked below.
	creditCache := map[[2]int64]map[int64]int64{}
	creditCents := func(uid int64, mi int) (int64, map[int64]int64) {
		key := [2]int64{uid, int64(mi)}
		per, ok := creditCache[key]
		if !ok {
			per = map[int64]int64{}
			byKind := map[string][]simpleEvent{}
			var kinds []string
			for _, e := range data[uid][mi].events {
				if _, seen := byKind[e.Kind]; !seen {
					kinds = append(kinds, e.Kind)
				}
				byKind[e.Kind] = append(byKind[e.Kind], e)
			}
			for _, k := range kinds {
				es := byKind[k]
				parts := make([]int64, len(es))
				for i, e := range es {
					parts[i] = e.Delta
				}
				cents, ok := simpleAllocate(simpleHalfUpCents(simpleSumEvents(es)), parts)
				if !ok {
					cents = make([]int64, len(es))
					for i, e := range es {
						cents[i] = simpleHalfUpCents(e.Delta)
					}
				}
				for i, e := range es {
					per[e.ID] = cents[i]
				}
			}
			creditCache[key] = per
		}
		var sum int64
		for _, c := range per {
			sum += c
		}
		return sum, per
	}
	tokenIDs := func(uid int64, mi int) []int64 {
		var ids []int64
		for id := range data[uid][mi].tokenConsume {
			ids = append(ids, id)
		}
		for id := range data[uid][mi].tokenRefund {
			if _, ok := data[uid][mi].tokenConsume[id]; !ok {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		return ids
	}
	// True (logged) token consumption.
	tokenTrue := func(uid int64, mi int, ids []int64) []simpleRoundToken {
		um := data[uid][mi]
		toks := make([]simpleRoundToken, len(ids))
		for i, id := range ids {
			toks[i] = simpleRoundToken{Consume: um.tokenConsume[id], Refund: um.tokenRefund[id]}
		}
		return toks
	}
	openings := make([]int64, len(members))
	for i, uid := range members {
		if f, ok := in.Frozen[simpleBoundaryKey{uid, boundaries[0].Format("2006-01")}]; ok {
			openings[i] = f.Cents
		} else {
			openings[i] = simpleHalfUpCents(raw[uid][0])
		}
	}
	build := func(mi int, open []int64) []simpleRoundUser {
		users := make([]simpleRoundUser, len(members))
		for i, uid := range members {
			c, _ := creditCents(uid, mi)
			users[i] = simpleRoundUser{OpeningCents: open[i], CreditCents: c, Closing: raw[uid][mi+1], Consume: settled[uid][mi], Refund: data[uid][mi].logRefund,
				Tokens: tokenTrue(uid, mi, tokenIDs(uid, mi))}
			if f, ok := in.Frozen[simpleBoundaryKey{uid, boundaries[mi+1].Format("2006-01")}]; ok {
				v := f.Cents
				users[i].FixedClosing = &v
			} else if boundaries[mi+1].Unix() <= in.Snapshot.Registered[uid] {
				v := int64(0)
				users[i].FixedClosing = &v
			}
		}
		return users
	}
	chain, err := simpleSolveChain(openings, len(months), build)
	if err != nil {
		return out, errSimpleReview("no balanced cent rounding")
	}

	// Boundaries to freeze (all chain boundaries not frozen yet).
	out.Freeze = map[simpleBoundaryKey]simpleFrozen{}
	for i, uid := range members {
		k0 := simpleBoundaryKey{uid, boundaries[0].Format("2006-01")}
		if _, ok := in.Frozen[k0]; !ok {
			out.Freeze[k0] = simpleFrozen{Raw: raw[uid][0], Cents: openings[i]}
		}
		for mi := range months {
			k := simpleBoundaryKey{uid, boundaries[mi+1].Format("2006-01")}
			if _, ok := in.Frozen[k]; !ok {
				out.Freeze[k] = simpleFrozen{Raw: raw[uid][mi+1], Cents: chain[mi][i].ClosingCents}
			}
		}
	}

	// Every credit amount the bill shows must be within one cent of its true
	// value: each event (by allocation), and the totals per account, per kind
	// and for the organization.
	for mi := range months {
		var orgShown, orgActual int64
		kindShown, kindActual := map[string]int64{}, map[string]int64{}
		for _, uid := range members {
			_, per := creditCents(uid, mi)
			var memShown, memActual, adjShown, adjActual int64
			for _, e := range data[uid][mi].events {
				c := per[e.ID]
				if !simpleWithinCent(c, e.Delta) {
					return out, errSimpleReview("credit rounding over one cent")
				}
				kindShown[e.Kind] += c
				kindActual[e.Kind] += e.Delta
				orgShown += c
				orgActual += e.Delta
				if e.Kind == "adjustment" {
					adjShown += c
					adjActual += e.Delta
				} else {
					memShown += c
					memActual += e.Delta
				}
			}
			if !simpleWithinCent(memShown, memActual) || !simpleWithinCent(adjShown, adjActual) {
				return out, errSimpleReview("credit rounding over one cent")
			}
		}
		for k := range kindShown {
			if !simpleWithinCent(kindShown[k], kindActual[k]) {
				return out, errSimpleReview("credit rounding over one cent")
			}
		}
		if !simpleWithinCent(orgShown, orgActual) ||
			!simpleWithinCent(orgShown-kindShown["adjustment"], orgActual-kindActual["adjustment"]) { // known credits: recharge + gift
			return out, errSimpleReview("credit rounding over one cent")
		}
	}

	// The requested month's organization opening is shown on the bill. It is
	// the sum of frozen closings, which a membership change can move; it must
	// still be within one cent of the true opening.
	{
		mi := len(months) - 1
		var shown, actual int64
		for i, uid := range members {
			if mi == 0 {
				shown += openings[i]
			} else {
				shown += chain[mi-1][i].ClosingCents
			}
			actual += raw[uid][mi]
		}
		if !simpleWithinCent(shown, actual) {
			return out, errSimpleReview("organization opening over one cent after membership change")
		}
	}

	// Evidence for the requested (last) month, in exact cents.
	mi := len(months) - 1
	open := openings
	if mi > 0 {
		open = make([]int64, len(members))
		for i := range members {
			open[i] = chain[mi-1][i].ClosingCents
		}
	}
	e := MonthlyBillEvidence{Version: 5, GroupID: in.GroupID, Month: in.Month, ReviewedBy: simpleBillReviewer,
		ReviewedAt: in.Now.Unix(), SourceReference: in.SourceRef}
	for i, uid := range members {
		r := chain[mi][i]
		um := data[uid][mi]
		ccents, per := creditCents(uid, mi)
		events := make([]MonthlyBillCreditEvidence, 0, len(um.events))
		for _, ev := range um.events {
			if per[ev.ID] == 0 {
				continue // a sub-half-cent movement displays as nothing; credits already exclude it
			}
			events = append(events, MonthlyBillCreditEvidence{ID: ev.ID, At: ev.At, DeltaQuota: per[ev.ID] * simpleCent, Kind: ev.Kind, Method: ev.Method})
		}
		sort.Slice(events, func(a, b int) bool {
			return events[a].At < events[b].At || (events[a].At == events[b].At && events[a].ID < events[b].ID)
		})
		ids := tokenIDs(uid, mi)
		rows := make([]MonthlyBillTokenEvidence, 0, len(ids))
		for k, id := range ids {
			if r.TokenConsume[k] == 0 && r.TokenRefund[k] == 0 {
				continue
			}
			rows = append(rows, MonthlyBillTokenEvidence{TokenID: id, Name: um.tokenName[id], ConsumeQuota: r.TokenConsume[k] * simpleCent, RefundQuota: r.TokenRefund[k] * simpleCent})
		}
		consume, refund := r.ConsumeCents*simpleCent, r.RefundCents*simpleCent
		e.Users = append(e.Users, MonthlyBillUserEvidence{
			UserID: uid, Complete: true, WalletOnly: true, Through: end.Unix(), ExpectedCreditEvents: len(events),
			OpeningQuota: open[i] * simpleCent, ClosingQuota: r.ClosingCents * simpleCent, ReverseClosingQuota: r.ClosingCents * simpleCent,
			ConsumeQuota: consume, RefundQuota: refund,
			// Amounts are the customer's cent-exact bill; the raw reconciliation
			// (exact balances, logged totals, wallet difference) is kept in the
			// internal audit table, never in the customer payload.
			Accounting: &MonthlyBillAccountingEvidence{Basis: "independent_wallet_reconciliation",
				OpeningSource: "simple-boundary:" + boundaries[mi].Format("2006-01"), ClosingSource: "simple-boundary:" + end.Format("2006-01"),
				MovementSource: "newapi-funding-logs", RefundSource: "usage-daily-facts", ReconciliationSource: "simple-controlled-cents-v1",
				LogConsumeQuota: consume, LogRefundQuota: refund},
			Requests: um.requests, Tokens: um.tokens,
			TokenUsage: &MonthlyBillTokenUsage{UserID: uid, Month: in.Month, Source: "usage-daily-facts", Rows: rows},
			Events:     events, Reversals: []MonthlyBillReversal{},
		})
		out.Audit = append(out.Audit, simpleAuditRow{UserID: uid, OpeningRaw: raw[uid][mi], ClosingRaw: raw[uid][mi+1], CreditRaw: simpleSumEvents(um.events),
			LogConsume: um.logConsume, LogRefund: um.logRefund, Consume: settled[uid][mi], WalletResidual: residualAt[uid],
			OpeningCents: open[i], ClosingCents: r.ClosingCents, ConsumeCents: r.ConsumeCents, RefundCents: r.RefundCents, CreditCents: ccents})
	}
	out.Evidence = e
	sub, err := simpleBuildSubscriptionBill(in, members, func(uid int64) simpleUserMonth { return data[uid][mi] }, target, end)
	if err != nil {
		return out, err
	}
	out.Sub = sub
	return out, nil
}

// simpleBuildSubscriptionBill collects the subscriptions opened in the month
// (priced from the configured plans) and the subscription-paid usage per
// account and token, in cents (half-up per token; account totals are the sum
// of their tokens).
func simpleBuildSubscriptionBill(in simpleOrgInput, members []int64, month func(int64) simpleUserMonth, start, end time.Time) (*simpleSubscriptionBill, error) {
	b := &simpleSubscriptionBill{Version: 1, Orders: []simpleSubscriptionOrder{}, Users: []simpleSubscriptionUser{}}
	member := map[int64]bool{}
	for _, uid := range members {
		member[uid] = true
	}
	for _, sub := range in.Snapshot.Subscriptions {
		if !member[sub.UserID] || sub.Start < start.Unix() || sub.Start >= end.Unix() {
			continue
		}
		// Plan defaults, then a per-subscription override (e.g. a free trial).
		plan, ok := in.Plans[sub.PlanID]
		if o, has := in.SubOverrides[sub.ID]; has {
			if strings.TrimSpace(o.Name) != "" {
				plan.Name = o.Name
			}
			if o.PriceCents > 0 {
				plan.PriceCents = o.PriceCents
			}
			plan.Trial = plan.Trial || o.Trial
			ok = ok || strings.TrimSpace(plan.Name) != ""
		}
		if !ok || strings.TrimSpace(plan.Name) == "" || plan.PriceCents < 0 {
			return nil, errSimpleReview(fmt.Sprintf("subscription %d plan %d has no configured name/price", sub.ID, sub.PlanID))
		}
		price := plan.PriceCents
		if plan.Trial {
			price = 0
		}
		b.Orders = append(b.Orders, simpleSubscriptionOrder{ID: sub.ID, UserID: sub.UserID, Name: plan.Name, Quantity: 1, PriceCents: price, Trial: plan.Trial,
			Start: sub.Start, End: sub.End, Active: sub.End > end.Unix()})
	}
	for _, uid := range members {
		um := month(uid)
		ids := map[int64]bool{}
		for id := range um.tokenSubConsume {
			ids[id] = true
		}
		for id := range um.tokenSubRefund {
			ids[id] = true
		}
		var sorted []int64
		for id := range ids {
			sorted = append(sorted, id)
		}
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		u := simpleSubscriptionUser{UserID: uid, Tokens: []simpleSubscriptionToken{}}
		// Account total = rounded true total; tokens allocated by largest
		// remainder (each within one cent of its true value).
		consume, refund := make([]int64, len(sorted)), make([]int64, len(sorted))
		for i, id := range sorted {
			consume[i], refund[i] = um.tokenSubConsume[id], um.tokenSubRefund[id]
		}
		alloc := func(parts []int64) []int64 {
			var sum int64
			for _, p := range parts {
				sum += p
			}
			if cents, ok := simpleAllocate(simpleHalfUpCents(sum), parts); ok {
				return cents
			}
			out := make([]int64, len(parts))
			for i, p := range parts {
				out[i] = simpleHalfUpCents(p)
			}
			return out
		}
		cc, rc := alloc(consume), alloc(refund)
		for i, id := range sorted {
			t := simpleSubscriptionToken{TokenID: id, Name: um.tokenName[id], ConsumeCents: cc[i], RefundCents: rc[i]}
			if t.ConsumeCents == 0 && t.RefundCents == 0 {
				continue
			}
			u.ConsumeCents += t.ConsumeCents
			u.RefundCents += t.RefundCents
			u.Tokens = append(u.Tokens, t)
		}
		if len(u.Tokens) > 0 {
			b.Users = append(b.Users, u)
		}
	}
	if len(b.Orders) == 0 && len(b.Users) == 0 {
		return nil, nil
	}
	return b, nil
}

func simpleSumEvents(es []simpleEvent) int64 {
	var s int64
	for _, e := range es {
		s += e.Delta
	}
	return s
}
