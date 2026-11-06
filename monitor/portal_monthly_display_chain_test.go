package monitor

import (
	"encoding/json"
	"errors"
	"math/big"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func monthlyChainFixture(t *testing.T, month string, opening, credit, consume, refund int64) monthlyStatement {
	t.Helper()
	e, now := monthlyTokenFixture()
	e.Month = month
	e.Users = e.Users[:1]
	start, end, err := monthlyWindow(month, now)
	if err != nil {
		t.Fatal(err)
	}
	u := &e.Users[0]
	u.Through, u.OpeningQuota, u.ConsumeQuota, u.RefundQuota = end.Unix(), opening, consume, refund
	u.ClosingQuota = opening + credit - consume + refund
	u.ReverseClosingQuota = u.ClosingQuota
	u.Accounting = monthlyAccountingFixture(consume, refund)
	u.Events = []MonthlyBillCreditEvidence{}
	u.ExpectedCreditEvents = 0
	if credit > 0 {
		u.Events = []MonthlyBillCreditEvidence{{ID: 1, At: start.Unix() + 1, DeltaQuota: credit, Kind: "recharge", Method: "后台充值"}}
		u.ExpectedCreditEvents = 1
	}
	u.TokenUsage.Month = month
	u.TokenUsage.Rows = []MonthlyBillTokenEvidence{{TokenID: 11, Name: "synthetic", ConsumeQuota: consume, RefundQuota: refund}}
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func monthlyChainAssertEquation(t *testing.T, w monthlyDisplayChainWallet) {
	t.Helper()
	opening, _ := new(big.Rat).SetString(w.Opening)
	closing, _ := new(big.Rat).SetString(w.Closing)
	delta, _ := new(big.Rat).SetString(w.Delta)
	if !monthlyDisplayChainValidWallet(w) || new(big.Rat).Add(opening, delta).Cmp(closing) != 0 {
		t.Fatal("independent display wallet equation failed", w)
	}
}

func TestMonthlyDisplayChainForwardCarriesCentsWithoutMutatingSource(t *testing.T) {
	a := monthlyChainFixture(t, "2026-08", 1160872085, 2500000000, 1513097892, 0)
	b := monthlyChainFixture(t, "2026-09", 2147774193, 0, 2500, 0)
	beforeA, _ := json.Marshal(a)
	beforeB, _ := json.Marshal(b)
	first, err := monthlyPlanDisplayChain(4, a, nil)
	if err != nil || first.Organization.Opening != "2321.740000" || first.Organization.Closing != "4295.540000" {
		t.Fatal("initial cents anchor invalid", first, err)
	}
	frozen, _ := json.Marshal(first)
	second, err := monthlyPlanDisplayChain(4, b, []monthlyDisplayChainLink{first})
	if err != nil || second.Organization.Opening != first.Organization.Closing || second.Organization.Closing != "4295.530000" {
		t.Fatal("next month did not carry prior cents", second, err)
	}
	if second.Members[0].Wallet.Opening != first.Members[0].Wallet.Closing {
		t.Fatal("member cents did not carry")
	}
	for _, w := range []monthlyDisplayChainWallet{first.Organization, second.Organization, first.Members[0].Wallet, second.Members[0].Wallet} {
		monthlyChainAssertEquation(t, w)
	}
	afterA, _ := json.Marshal(a)
	afterB, _ := json.Marshal(b)
	afterFrozen, _ := json.Marshal(first)
	if string(beforeA) != string(afterA) || string(beforeB) != string(afterB) || string(frozen) != string(afterFrozen) || *b.MonthOpeningBalance != "4295.548386" {
		t.Fatal("planning changed frozen money or a preceding record")
	}
	repeated, err := monthlyPlanDisplayChain(4, b, []monthlyDisplayChainLink{first, second})
	if err != nil || !reflect.DeepEqual(repeated, second) {
		t.Fatal("repeat planning changed a saved record", err)
	}
	readVersion := b
	readVersion.Publication = strings.Repeat("b", 64)
	fromRead, err := monthlyPlanDisplayChain(4, readVersion, []monthlyDisplayChainLink{first, second})
	if err != nil || !reflect.DeepEqual(fromRead, second) {
		t.Fatal("computed transport version changed the same frozen payload", err)
	}
	repeated.Members[0].Wallet.Opening = "0.000000"
	if second.Members[0].Wallet.Opening == "0.000000" {
		t.Fatal("returned record aliases saved member data")
	}
}

func TestMonthlyDisplayChainReverseOrderAndBridgeNeverRewriteFrozenEnds(t *testing.T) {
	a := monthlyChainFixture(t, "2026-07", 1160872085, 2500000000, 1513097892, 0)
	b := monthlyChainFixture(t, "2026-08", 2147774193, 0, 2500, 0)
	c := monthlyChainFixture(t, "2026-09", 2147771693, 0, 0, 0)
	last, err := monthlyPlanDisplayChain(4, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	middle, err := monthlyPlanDisplayChain(4, b, []monthlyDisplayChainLink{last})
	if err != nil || middle.Organization.Closing != last.Organization.Opening {
		t.Fatal("reverse-order month did not pin frozen next opening", err)
	}
	first, err := monthlyPlanDisplayChain(4, a, []monthlyDisplayChainLink{middle})
	if err != nil || first.Organization.Closing != middle.Organization.Opening {
		t.Fatal("reverse-order chain failed", err)
	}
	_, err = monthlyPlanDisplayChain(4, b, []monthlyDisplayChainLink{first, middle, last})
	if err != nil {
		t.Fatal("matching saved bridge refused", err)
	}
	// Independently frozen disconnected ends can conflict. Never move either
	// customer-visible endpoint or alter consumption just to fill the gap.
	standaloneFirst, err := monthlyPlanDisplayChain(4, a, nil)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal([]monthlyDisplayChainLink{standaloneFirst, last})
	if _, err = monthlyPlanDisplayChain(4, b, []monthlyDisplayChainLink{standaloneFirst, last}); !errors.Is(err, errMonthlyDisplayChainConflict) {
		t.Fatal("contradictory bridge must be flagged", err)
	}
	after, _ := json.Marshal([]monthlyDisplayChainLink{standaloneFirst, last})
	if string(before) != string(after) {
		t.Fatal("failed bridge modified frozen endpoints")
	}
}

func TestMonthlyDisplayChainRejectsUnsafeInputsAndVersionChanges(t *testing.T) {
	a := monthlyChainFixture(t, "2026-08", 0, 1000000, 1, 0)
	b := monthlyChainFixture(t, "2026-09", 999999, 0, 2, 0)
	prior, err := monthlyPlanDisplayChain(4, a, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*monthlyDisplayChainLink){
		"wrong owner":        func(l *monthlyDisplayChainLink) { l.GroupID = 5; l.Digest = monthlyDisplayChainDigest(*l) },
		"corrupt checksum":   func(l *monthlyDisplayChainLink) { l.Organization.Closing = "1.000000" },
		"non adjacent month": func(l *monthlyDisplayChainLink) { l.Month = "2026-06"; l.Digest = monthlyDisplayChainDigest(*l) },
		"non cent amount": func(l *monthlyDisplayChainLink) {
			l.Organization.Delta = "1.000002"
			l.Digest = monthlyDisplayChainDigest(*l)
		},
		"invalid equation": func(l *monthlyDisplayChainLink) {
			l.Organization.Closing = "1.000000"
			l.Digest = monthlyDisplayChainDigest(*l)
		},
		"duplicate member": func(l *monthlyDisplayChainLink) {
			l.Members = append(l.Members, l.Members[0])
			l.Digest = monthlyDisplayChainDigest(*l)
		},
	} {
		t.Run(name, func(t *testing.T) {
			copy := prior
			copy.Members = append([]monthlyDisplayChainMember(nil), prior.Members...)
			mutate(&copy)
			if _, err := monthlyPlanDisplayChain(4, b, []monthlyDisplayChainLink{copy}); !errors.Is(err, errMonthlyDisplayChainInvalid) {
				t.Fatal("unsafe neighbor accepted", err)
			}
		})
	}
	if _, err = monthlyPlanDisplayChain(4, b, []monthlyDisplayChainLink{prior, prior}); !errors.Is(err, errMonthlyDisplayChainInvalid) {
		t.Fatal("duplicate neighbor accepted", err)
	}
	changed := a
	changed.Account = "changed source version"
	if _, err = monthlyPlanDisplayChain(4, changed, []monthlyDisplayChainLink{prior}); !errors.Is(err, errMonthlyDisplayChainConflict) {
		t.Fatal("new version overwrote frozen cents", err)
	}
	badSource := b
	wrong := "1.999992"
	badSource.MonthEndBalance = &wrong
	if _, err = monthlyPlanDisplayChain(4, badSource, nil); !errors.Is(err, errMonthlyDisplayChainInvalid) {
		t.Fatal("real source discrepancy hidden", err)
	}
}

func TestMonthlyDisplayChainYearBoundaryAndIndependentWalletProperties(t *testing.T) {
	rng := rand.New(rand.NewSource(20261027))
	for n := range 150 {
		opening, credit, consume, refund := rng.Int63n(10000000)-5000000, rng.Int63n(1000000), rng.Int63n(1000000), rng.Int63n(10000)
		a := monthlyChainFixture(t, "2025-12", opening, credit, consume, refund)
		b := monthlyChainFixture(t, "2026-01", opening+credit-consume+refund, 0, rng.Int63n(10000), 0)
		prior, err := monthlyPlanDisplayChain(4, a, nil)
		if err != nil {
			t.Fatal(n, err)
		}
		next, err := monthlyPlanDisplayChain(4, b, []monthlyDisplayChainLink{prior})
		if err != nil || !monthlyDisplayChainAdjacent(prior, next) {
			t.Fatal("year rollover or continuity failed", n, err)
		}
		for _, pair := range []struct {
			m monthlyMemberStatement
			w monthlyDisplayChainWallet
		}{{a.Members[0], prior.Members[0].Wallet}, {b.Members[0], next.Members[0].Wallet}} {
			monthlyChainAssertEquation(t, pair.w)
			independent, _ := new(big.Rat).SetString(monthlyAuditWalletClosing(pair.m))
			roundedOpening, _ := new(big.Rat).SetString(monthlyAuditCents(pair.m.Opening))
			wantDelta := independent.Sub(independent, roundedOpening)
			actual, _ := new(big.Rat).SetString(pair.w.Delta)
			if actual.Cmp(wantDelta) != 0 {
				t.Fatal("planner changed cents movement", n)
			}
		}
	}
}

func TestMonthlyDisplayChainRosterAndRawBoundaryIsolation(t *testing.T) {
	a := monthlyChainFixture(t, "2026-08", 0, 1000000, 1, 0)
	b := monthlyChainFixture(t, "2026-09", 999999, 0, 2, 0)
	prior, err := monthlyPlanDisplayChain(4, a, nil)
	if err != nil {
		t.Fatal(err)
	}
	zero := monthlyChainFixture(t, "2026-09", 0, 0, 0, 0).Members[0]
	zero.UserID = 999
	zero.TokenConsumption = nil
	withZero := b
	withZero.Members = append(append([]monthlyMemberStatement(nil), b.Members...), zero)
	if _, err = monthlyPlanDisplayChain(4, withZero, []monthlyDisplayChainLink{prior}); err != nil {
		t.Fatal("new zero-balance account should not break an existing chain", err)
	}
	// The same organization totals do not authorize moving one user's balance
	// into another user's account when the membership changes.
	replaced := b
	replaced.Members = append([]monthlyMemberStatement(nil), b.Members...)
	replaced.Members[0].UserID = 999
	if !monthlyStatementAmountsComplete(replaced) {
		t.Fatal("synthetic replacement source invalid")
	}
	if _, err = monthlyPlanDisplayChain(4, replaced, []monthlyDisplayChainLink{prior}); !errors.Is(err, errMonthlyDisplayChainConflict) {
		t.Fatal("nonzero account balance crossed immutable user IDs", err)
	}
	for _, memberOnly := range []bool{false, true} {
		broken := prior
		broken.Members = append([]monthlyDisplayChainMember(nil), prior.Members...)
		if memberOnly {
			broken.Members[0].Wallet.SourceClosing = "1.999996"
		} else {
			broken.Organization.SourceClosing = "1.999996"
		}
		broken.Digest = monthlyDisplayChainDigest(broken)
		if _, err = monthlyPlanDisplayChain(4, b, []monthlyDisplayChainLink{broken}); !errors.Is(err, errMonthlyDisplayChainConflict) {
			t.Fatal("sub-cent source discrepancy was hidden by cents equality", memberOnly, err)
		}
	}
}

func TestMonthlyDisplayChainLegacyRepeatPreservesEmptyCollectionIdentity(t *testing.T) {
	p := monthlyChainFixture(t, "2026-09", 0, 1000000, 1, 0)
	p.MemberDetailsVersion, p.TokenDetailsVersion, p.Members = 0, 0, nil
	link, err := monthlyPlanDisplayChain(4, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, members := range [][]monthlyDisplayChainMember{nil, {}} {
		link.Members = members
		link.Digest = monthlyDisplayChainDigest(link)
		got, err := monthlyPlanDisplayChain(4, p, []monthlyDisplayChainLink{link})
		if err != nil || !reflect.DeepEqual(got, link) || !monthlyDisplayChainValidLink(got) {
			t.Fatal("legacy nil/empty collection changed its frozen identity", err)
		}
	}
}
