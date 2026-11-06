package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var monthlySeriesTestOrigin = monthlyDigest([]byte("synthetic fixed source origin only"))

// Deterministically cancel a real context at a work boundary, rather than
// racing a timer against the test runner. Done/Err keep normal semantics.
type monthlySeriesCancelAt struct {
	context.Context
	calls  atomic.Int64
	at     int64
	cancel context.CancelFunc
}

func (c *monthlySeriesCancelAt) Err() error {
	if c.calls.Add(1) == c.at {
		c.cancel()
	}
	return c.Context.Err()
}

func monthlySeriesFixture(t *testing.T) (monthlyDisplaySeriesScope, []monthlyDisplaySeriesSource) {
	t.Helper()
	return monthlyDisplaySeriesScope{GroupID: 4, SourceIdentity: monthlySeriesTestOrigin, From: "2026-07", Through: "2026-09"}, []monthlyDisplaySeriesSource{
		{GroupID: 4, SourceIdentity: monthlySeriesTestOrigin, Statement: monthlyChainFixture(t, "2026-07", 1160872085, 2500000000, 1513097892, 0)},
		{GroupID: 4, SourceIdentity: monthlySeriesTestOrigin, Statement: monthlyChainFixture(t, "2026-08", 2147774193, 0, 2500, 0)},
		{GroupID: 4, SourceIdentity: monthlySeriesTestOrigin, Statement: monthlyChainFixture(t, "2026-09", 2147771693, 0, 0, 0)},
	}
}

func TestMonthlyDisplaySeriesAllPublicationOrdersHaveIdenticalMoney(t *testing.T) {
	scope, sources := monthlySeriesFixture(t)
	before, _ := json.Marshal(sources)
	want, err := monthlyPlanDisplaySeries(context.Background(), scope, sources, nil)
	if err != nil || len(want.Months) != 3 || want.Months[0].Organization.Opening != "2321.740000" || want.Months[2].Organization.Closing != "4295.530000" {
		t.Fatal("fixed-origin fixture incorrect", err)
	}
	for _, order := range [][]int{{0, 1, 2}, {2, 1, 0}, {0, 2, 1}, {1, 2, 0}, {1, 0, 2}, {2, 0, 1}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			var issued []monthlyDisplaySeriesFrozen
			for _, selected := range order {
				// Input file order and customer publication order are independent.
				reordered := []monthlyDisplaySeriesSource{sources[2], sources[0], sources[1]}
				issuedBefore, _ := json.Marshal(issued)
				plan, err := monthlyPlanDisplaySeries(context.Background(), scope, reordered, issued)
				if err != nil || !reflect.DeepEqual(plan, want) {
					t.Fatal("publication order changed any planned amount or identity", selected, err)
				}
				issuedAfter, _ := json.Marshal(issued)
				if !bytes.Equal(issuedBefore, issuedAfter) {
					t.Fatal("planning rewrote a frozen publication")
				}
				issued = append(issued, monthlyDisplaySeriesFrozen{AnchorSHA256: plan.AnchorSHA256, Link: plan.Months[selected]})
			}
			if _, err := monthlyPlanDisplaySeries(context.Background(), scope, sources, issued); err != nil {
				t.Fatal("same complete series stopped matching issued bills", err)
			}
		})
	}
	after, _ := json.Marshal(sources)
	if !bytes.Equal(before, after) {
		t.Fatal("source consumption, funding or exact balances changed")
	}
	// No target-month parameter is available to shift the fixed origin. The
	// caller selects from this common plan only AFTER all months are validated.
	t.Log("all six publication orders, including first/last before middle: origin=2321.74 final=4295.53; identical plan and unchanged source/issued data")
}

func TestMonthlyDisplaySeriesMissingSourceNeverReturnsPartialPlan(t *testing.T) {
	scope, sources := monthlySeriesFixture(t)
	for _, subset := range [][]monthlyDisplaySeriesSource{nil, {sources[2]}, {sources[0], sources[2]}, {sources[0], sources[1]}} {
		plan, err := monthlyPlanDisplaySeries(context.Background(), scope, subset, nil)
		if !errors.Is(err, errMonthlyDisplaySeriesIncomplete) || !reflect.DeepEqual(plan, monthlyDisplaySeriesPlan{}) {
			t.Fatal("missing source became zero, later anchor or a partial publishable plan", err)
		}
	}
	duplicate := []monthlyDisplaySeriesSource{sources[0], sources[0], sources[2]}
	if plan, err := monthlyPlanDisplaySeries(context.Background(), scope, duplicate, nil); !errors.Is(err, errMonthlyDisplayChainInvalid) || !reflect.DeepEqual(plan, monthlyDisplaySeriesPlan{}) {
		t.Fatal("duplicate source accepted", err)
	}
	// A complete-looking source can balance in isolation while disagreeing by
	// one quota with last month's source boundary. Never hide it in cents.
	sources[1].Statement = monthlyChainFixture(t, "2026-08", 2147774194, 0, 2500, 0)
	if plan, err := monthlyPlanDisplaySeries(context.Background(), scope, sources, nil); !errors.Is(err, errMonthlyDisplayChainConflict) || !reflect.DeepEqual(plan, monthlyDisplaySeriesPlan{}) {
		t.Fatal("sub-cent source discontinuity accepted", err)
	}
}

func TestMonthlyDisplaySeriesPreservesLegacyAndRefusesWrongFrozenAnchors(t *testing.T) {
	scope, sources := monthlySeriesFixture(t)
	canonical, err := monthlyPlanDisplaySeries(context.Background(), scope, sources, nil)
	if err != nil {
		t.Fatal(err)
	}
	legacy := []monthlyDisplaySeriesFrozen{{Link: canonical.Months[0]}}
	before, _ := json.Marshal(legacy)
	if plan, err := monthlyPlanDisplaySeries(context.Background(), scope, sources, legacy); err != nil || !reflect.DeepEqual(plan, canonical) {
		t.Fatal("compatible legacy snapshot changed the common plan", err)
	}
	after, _ := json.Marshal(legacy)
	if !bytes.Equal(before, after) || legacy[0].AnchorSHA256 != "" {
		t.Fatal("legacy metadata was retroactively edited")
	}
	standaloneLast, err := monthlyPlanDisplayChain(scope.GroupID, sources[2].Statement, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, records := range map[string][]monthlyDisplaySeriesFrozen{
		"old incompatible isolated last month": {{Link: standaloneLast}},
		"wrong new-series anchor":              {{AnchorSHA256: strings.Repeat("f", 64), Link: canonical.Months[0]}},
		"duplicate issued record":              {{Link: canonical.Months[0]}, {Link: canonical.Months[0]}},
	} {
		t.Run(name, func(t *testing.T) {
			before, _ := json.Marshal(records)
			plan, err := monthlyPlanDisplaySeries(context.Background(), scope, sources, records)
			if !errors.Is(err, errMonthlyDisplaySeriesFrozen) || !reflect.DeepEqual(plan, monthlyDisplaySeriesPlan{}) {
				t.Fatal("incompatible old invoice silently re-anchored", err)
			}
			after, _ := json.Marshal(records)
			if !bytes.Equal(before, after) {
				t.Fatal("frozen financial data changed on rejection")
			}
		})
	}
	foreign := canonical.Months[0]
	foreign.GroupID++
	foreign.Digest = monthlyDisplayChainDigest(foreign)
	if _, err := monthlyPlanDisplaySeries(context.Background(), scope, sources, []monthlyDisplaySeriesFrozen{{Link: foreign}}); !errors.Is(err, errMonthlyDisplaySeriesFrozen) {
		t.Fatal("different organization's frozen invoice accepted", err)
	}
}

func TestMonthlyDisplaySeriesExtensionAndOwnedMemory(t *testing.T) {
	scope, sources := monthlySeriesFixture(t)
	short := scope
	short.Through = "2026-08"
	first, err := monthlyPlanDisplaySeries(context.Background(), short, sources[:2], nil)
	if err != nil {
		t.Fatal(err)
	}
	issued := []monthlyDisplaySeriesFrozen{{AnchorSHA256: first.AnchorSHA256, Link: first.Months[1]}}
	full, err := monthlyPlanDisplaySeries(context.Background(), scope, sources, issued)
	if err != nil || first.AnchorSHA256 != full.AnchorSHA256 || first.PlanSHA256 == full.PlanSHA256 || !reflect.DeepEqual(first.Months, full.Months[:2]) {
		t.Fatal("extending fixed-origin coverage changed old money/anchor", err)
	}
	// Transport metadata never becomes a different source-money version.
	sources[0].Statement.Publication = strings.Repeat("a", 64)
	again, err := monthlyPlanDisplaySeries(context.Background(), scope, sources, issued)
	if err != nil || !reflect.DeepEqual(again, full) {
		t.Fatal("transport ID changed the common plan", err)
	}
	full.Months[0].Members[0].Wallet.Opening = "0.000000"
	full.Months[1].Members[0].Wallet.Opening = "0.000000"
	if first.Months[0].Members[0].Wallet.Opening == "0.000000" || issued[0].Link.Members[0].Wallet.Opening == "0.000000" || again.Months[0].Members[0].Wallet.Opening == "0.000000" {
		t.Fatal("output aliases a prior plan or frozen input")
	}
	otherScope := short
	otherScope.SourceIdentity = monthlyDigest([]byte("synthetic independent second origin"))
	otherSources := append([]monthlyDisplaySeriesSource(nil), sources[:2]...)
	for i := range otherSources {
		otherSources[i].SourceIdentity = otherScope.SourceIdentity
	}
	other, err := monthlyPlanDisplaySeries(context.Background(), otherScope, otherSources, nil)
	if err != nil || other.AnchorSHA256 == first.AnchorSHA256 || other.PlanSHA256 == first.PlanSHA256 {
		t.Fatal("identical monetary values from another origin reused an anchor", err)
	}
	if _, err := monthlyPlanDisplaySeries(context.Background(), otherScope, otherSources, issued); !errors.Is(err, errMonthlyDisplaySeriesFrozen) {
		t.Fatal("another origin adopted a previously issued anchor", err)
	}
}

func TestMonthlyDisplaySeriesScopeCapacityAndCancellation(t *testing.T) {
	scope, sources := monthlySeriesFixture(t)
	for _, wrong := range []monthlyDisplaySeriesScope{{}, {GroupID: 4, From: "2026-09", Through: "2026-07"}, {GroupID: 4, From: "2026-7", Through: "2026-09"}} {
		if _, err := monthlyPlanDisplaySeries(context.Background(), wrong, sources, nil); !errors.Is(err, errMonthlyDisplayChainInvalid) {
			t.Fatal("invalid fixed source range accepted", err)
		}
	}
	large := monthlyDisplaySeriesScope{GroupID: 4, SourceIdentity: monthlySeriesTestOrigin, From: "2020-01", Through: "2030-01"}
	if _, err := monthlyPlanDisplaySeries(context.Background(), large, nil, nil); !errors.Is(err, errMonthlyDisplaySeriesCapacity) {
		t.Fatal("unbounded month range accepted", err)
	}
	if _, err := monthlyPlanDisplaySeries(nil, scope, sources, nil); !errors.Is(err, errMonthlyDisplayChainInvalid) { //nolint:staticcheck // Intentionally tests rejection of an invalid nil context.
		t.Fatal("nil cancellation context accepted", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if plan, err := monthlyPlanDisplaySeries(ctx, scope, sources, nil); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(plan, monthlyDisplaySeriesPlan{}) {
		t.Fatal("cancellation exposed a partial financial plan", err)
	}
	inner, stop := context.WithCancel(context.Background())
	defer stop()
	during := &monthlySeriesCancelAt{Context: inner, at: 6, cancel: stop}
	if plan, err := monthlyPlanDisplaySeries(during, scope, sources, nil); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(plan, monthlyDisplaySeriesPlan{}) {
		t.Fatal("mid-calculation cancellation returned a partial money plan", err)
	}
	wrongOwner := append([]monthlyDisplaySeriesSource(nil), sources...)
	wrongOwner[1].GroupID++
	if _, err := monthlyPlanDisplaySeries(context.Background(), scope, wrongOwner, nil); !errors.Is(err, errMonthlyDisplayChainInvalid) {
		t.Fatal("foreign raw source accepted", err)
	}
	wrongOrigin := append([]monthlyDisplaySeriesSource(nil), sources...)
	wrongOrigin[1].SourceIdentity = monthlyDigest([]byte("synthetic different origin"))
	if _, err := monthlyPlanDisplaySeries(context.Background(), scope, wrongOrigin, nil); !errors.Is(err, errMonthlyDisplayChainInvalid) {
		t.Fatal("matching IDs from a different source origin were mixed into the ledger", err)
	}
	// Frozen DTOs must be explicitly separated into verified source facts and
	// issued constraints by the later adapter. Never silently strip bad cents.
	frozenSource := append([]monthlyDisplaySeriesSource(nil), sources...)
	frozenSource[0].Statement.DisplayBalances = &monthlyDisplayBalances{Version: 1}
	if _, err := monthlyPlanDisplaySeries(context.Background(), scope, frozenSource, nil); !errors.Is(err, errMonthlyDisplayChainInvalid) {
		t.Fatal("frozen customer DTO mistaken for verified raw preparation", err)
	}
	oversized := append([]monthlyDisplaySeriesSource(nil), sources...)
	oversized[0].Statement.Account = strings.Repeat("x", 2<<20)
	if plan, err := monthlyPlanDisplaySeries(context.Background(), scope, oversized, nil); !errors.Is(err, errMonthlyDisplaySeriesCapacity) || !reflect.DeepEqual(plan, monthlyDisplaySeriesPlan{}) {
		t.Fatal("oversized source did not fail within the existing budget", err)
	}
	var many []monthlyDisplaySeriesSource
	label := strings.Repeat("x", 1500<<10)
	for i := range 6 {
		p := monthlyChainFixture(t, fmt.Sprintf("2026-%02d", i+1), 0, 0, 0, 0)
		p.Account = label
		many = append(many, monthlyDisplaySeriesSource{GroupID: 4, SourceIdentity: monthlySeriesTestOrigin, Statement: p})
	}
	if plan, err := monthlyPlanDisplaySeries(context.Background(), monthlyDisplaySeriesScope{GroupID: 4, SourceIdentity: monthlySeriesTestOrigin, From: "2026-01", Through: "2026-06"}, many, nil); !errors.Is(err, errMonthlyDisplaySeriesCapacity) || !reflect.DeepEqual(plan, monthlyDisplaySeriesPlan{}) {
		t.Fatal("combined source budget overflow returned financial output", err)
	}
}

func TestMonthlyDisplaySeriesCorrectionsAndMemberIndependence(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, usageCST)
	for _, quota := range []int64{1, 2500, 502500, 909785} {
		candidate := automaticMonthlyFixture(now, 43, quota)
		raw, err := PrepareAutomaticMonthlyBill(candidate, now.Unix(), now)
		if err != nil {
			t.Fatal(err)
		}
		evidence, err := decodeMonthlyBillEvidence(raw)
		if err != nil {
			t.Fatal(err)
		}
		p, _, err := buildPublishedMonthlyBill(evidence, now)
		if err != nil {
			t.Fatal(err)
		}
		source := monthlyDisplaySeriesSource{GroupID: evidence.GroupID, SourceIdentity: monthlySeriesTestOrigin, Statement: p}
		scope := monthlyDisplaySeriesScope{GroupID: evidence.GroupID, SourceIdentity: monthlySeriesTestOrigin, From: evidence.Month, Through: evidence.Month}
		before, _ := json.Marshal(p)
		plan, err := monthlyPlanDisplaySeries(context.Background(), scope, []monthlyDisplaySeriesSource{source}, nil)
		if err != nil {
			t.Fatal(err)
		}
		display, err := monthlySelectDisplaySeries(context.Background(), plan, source)
		if err != nil {
			t.Fatal(err)
		}
		member := p.Members[0]
		if monthlyAuditCents(display.Organization.Closing) != monthlyAuditWalletClosing(member) || display.Members[0].Closing != display.Organization.Closing {
			t.Fatal("recognized correction changed a user/organization cent movement")
		}
		p.DisplayBalances = display
		book, err := monthlyStatementXLSX(p)
		if err != nil || !strings.Contains(monthlyAuditWorkbookParts(t, book)["xl/worksheets/sheet1.xml"], "余额校正（元）") {
			t.Fatal("existing explicit correction disappeared from export", err)
		}
		p.DisplayBalances = nil
		after, _ := json.Marshal(p)
		if !bytes.Equal(before, after) || p.Consumption.Amount != "100000.000000" || p.Consumption.Refund != "3.000000" {
			t.Fatal("series preparation changed original consumption/refund/correction")
		}
	}
	scope, sources := monthlySeriesFixture(t)
	replaced := append([]monthlyDisplaySeriesSource(nil), sources...)
	p := monthlyChainFixture(t, "2026-08", 2147774193, 0, 2500, 0)
	p.Members[0].UserID = 999
	for i := range p.Credits.Rows {
		p.Credits.Rows[i].UserID = 999
	}
	if !monthlyStatementAmountsComplete(p) {
		t.Fatal("synthetic replacement must balance in isolation")
	}
	replaced[1].Statement = p
	if _, err := monthlyPlanDisplaySeries(context.Background(), scope, replaced, nil); !errors.Is(err, errMonthlyDisplayChainConflict) {
		t.Fatal("matching organization totals moved a nonzero balance to another user", err)
	}
	zeroA := monthlyChainFixture(t, "2026-08", 0, 0, 0, 0)
	zeroB := monthlyChainFixture(t, "2026-09", 0, 0, 0, 0)
	// A pre-attested zero-balance entrant is allowed. An absent source is NOT
	// converted to this entry: the trusted producer must establish that zero.
	newUser := zeroB.Members[0]
	newUser.UserID, newUser.TokenConsumption = 999, nil
	zeroB.Members = append(zeroB.Members, newUser)
	if _, err := monthlyPlanDisplaySeries(context.Background(), monthlyDisplaySeriesScope{GroupID: 4, SourceIdentity: monthlySeriesTestOrigin, From: "2026-08", Through: "2026-09"}, []monthlyDisplaySeriesSource{{GroupID: 4, SourceIdentity: monthlySeriesTestOrigin, Statement: zeroA}, {GroupID: 4, SourceIdentity: monthlySeriesTestOrigin, Statement: zeroB}}, nil); err != nil {
		t.Fatal("explicitly proven zero entrant incorrectly broke the common plan", err)
	}
}

func TestMonthlyDisplaySeriesRandomOrderSimulation(t *testing.T) {
	random := rand.New(rand.NewSource(20261027))
	accepted := 0
	var views []monthlySimulationCase
	for scenario := range 100 {
		start := time.Date(2025, time.Month(1+scenario%6), 1, 0, 0, 0, 0, usageCST)
		scope := monthlyDisplaySeriesScope{GroupID: int64(scenario + 1), SourceIdentity: monthlySeriesTestOrigin, From: start.Format("2006-01"), Through: start.AddDate(0, 5, 0).Format("2006-01")}
		opening := random.Int63n(10000000) - 5000000
		var sources []monthlyDisplaySeriesSource
		for month := range 6 {
			credit, consume, refund := random.Int63n(10000), random.Int63n(10000), random.Int63n(10000)
			p := monthlyChainFixture(t, start.AddDate(0, month, 0).Format("2006-01"), opening, credit, consume, refund)
			sources = append(sources, monthlyDisplaySeriesSource{GroupID: scope.GroupID, SourceIdentity: monthlySeriesTestOrigin, Statement: p})
			opening += credit - consume + refund
		}
		want, err := monthlyPlanDisplaySeries(context.Background(), scope, sources, nil)
		if err != nil {
			t.Fatal(scenario, err)
		}
		var previous *monthlyDisplayChainLink
		var oracle *monthlySimulationCase
		expected := make([]monthlySimulationCase, 6)
		for i := range sources {
			p := sources[i].Statement
			link, c := monthlySimulationFreeze(t, scope.GroupID, &p, previous, oracle)
			if !monthlyDisplaySeriesSameLink(link, want.Months[i]) {
				t.Fatal("fixed series differs from independent cent-wallet model", scenario, i)
			}
			previous, oracle = &link, &c
			expected[i] = c
		}
		var issued []monthlyDisplaySeriesFrozen
		for _, selected := range random.Perm(6) {
			reordered := make([]monthlyDisplaySeriesSource, 6)
			for i, index := range random.Perm(6) {
				reordered[i] = sources[index]
			}
			plan, err := monthlyPlanDisplaySeries(context.Background(), scope, reordered, issued)
			if err != nil || !reflect.DeepEqual(plan, want) {
				t.Fatal("complete-source random request changed cents or refused middle month", scenario, selected, err)
			}
			display, err := monthlySelectDisplaySeries(context.Background(), plan, sources[selected])
			if err != nil || !reflect.DeepEqual(display, expected[selected].Data.DisplayBalances) {
				t.Fatal("prepared selection differs from independent cent model", scenario, selected, err)
			}
			view := expected[selected]
			view.Label = fmt.Sprintf("fixed-series scenario=%d month=%d", scenario, selected)
			view.Data = sources[selected].Statement
			view.Data.DisplayBalances = display
			monthlySimulationWorkbook(t, view)
			views = append(views, view)
			issued = append(issued, monthlyDisplaySeriesFrozen{AnchorSHA256: plan.AnchorSHA256, Link: plan.Months[selected]})
			accepted++
		}
	}
	if accepted != 600 {
		t.Fatal("not all 600 prepared-source publications were checked")
	}
	if output := os.Getenv("MONTHLY_DISPLAY_SERIES_OUTPUT"); output != "" {
		st, err := os.Stat(output)
		if err != nil || !filepath.IsAbs(output) || !st.IsDir() || st.Mode().Perm() != 0700 {
			t.Fatal("private absolute series acceptance output required")
		}
		payload, err := json.Marshal(struct {
			Mode       string                  `json:"mode"`
			Seed       int64                   `json:"seed"`
			Accounts   int                     `json:"accounts"`
			Tokens     int                     `json:"tokens"`
			Statements []monthlySimulationCase `json:"statements"`
		}{"synthetic_money_simulation_no_production_access", 20261027, len(views), len(views), views})
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(filepath.Join(output, "browser-cases.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := file.Write(payload)
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatal(writeErr, closeErr)
		}
	}
	t.Log("100 complete six-month source series; 600 randomly ordered selections: identical financial plan, no middle-month conflicts; 600 selected views/workbooks match independent cents; source scope verified separately")
}

func TestMonthlyDisplaySeriesSelectionRejectsChangedOrIncompletePlan(t *testing.T) {
	scope, sources := monthlySeriesFixture(t)
	plan, err := monthlyPlanDisplaySeries(context.Background(), scope, sources, nil)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(plan)
	for index := range sources {
		value, err := monthlySelectDisplaySeries(context.Background(), plan, sources[index])
		if err != nil || value.Organization.Opening != plan.Months[index].Organization.Opening || value.Organization.Closing != plan.Months[index].Organization.Closing {
			t.Fatal("selection lost canonical amounts", err)
		}
		value.Members[0].Opening = "0.000000"
	}
	after, _ := json.Marshal(plan)
	if !bytes.Equal(before, after) {
		t.Fatal("selected display aliases or mutates the common plan")
	}
	for name, change := range map[string]func(*monthlyDisplaySeriesPlan){
		"wrong policy":     func(p *monthlyDisplaySeriesPlan) { p.Policy = "unknown" },
		"wrong owner":      func(p *monthlyDisplaySeriesPlan) { p.GroupID++ },
		"missing month":    func(p *monthlyDisplaySeriesPlan) { p.Months = p.Months[:2] },
		"changed closing":  func(p *monthlyDisplaySeriesPlan) { p.Months[1].Organization.Closing = "0.000000" },
		"changed checksum": func(p *monthlyDisplaySeriesPlan) { p.PlanSHA256 = strings.Repeat("a", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			var broken monthlyDisplaySeriesPlan
			if err := json.Unmarshal(before, &broken); err != nil {
				t.Fatal(err)
			}
			change(&broken)
			if selected, err := monthlySelectDisplaySeries(context.Background(), broken, sources[1]); err == nil || selected != nil {
				t.Fatal("corrupt plan exposed amounts", err)
			}
		})
	}
	changed := sources[1]
	changed.Statement = monthlyChainFixture(t, "2026-08", 2147774193, 0, 2501, 0)
	if selected, err := monthlySelectDisplaySeries(context.Background(), plan, changed); err == nil || selected != nil {
		t.Fatal("new source amount used an old prepared plan")
	}
	foreign := sources[1]
	foreign.GroupID++
	if selected, err := monthlySelectDisplaySeries(context.Background(), plan, foreign); err == nil || selected != nil {
		t.Fatal("foreign source selected the organization's amounts")
	}
	wrongOrigin := sources[1]
	wrongOrigin.SourceIdentity = monthlyDigest([]byte("another synthetic origin"))
	if selected, err := monthlySelectDisplaySeries(context.Background(), plan, wrongOrigin); err == nil || selected != nil {
		t.Fatal("same-ID foreign source reused the cached common plan")
	}
	outside := monthlyDisplaySeriesSource{GroupID: scope.GroupID, SourceIdentity: monthlySeriesTestOrigin, Statement: monthlyChainFixture(t, "2026-06", 0, 0, 0, 0)}
	if selected, err := monthlySelectDisplaySeries(context.Background(), plan, outside); !errors.Is(err, errMonthlyDisplaySeriesIncomplete) || selected != nil {
		t.Fatal("unprepared month reused a nearest month's amount", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if selected, err := monthlySelectDisplaySeries(ctx, plan, sources[1]); !errors.Is(err, context.Canceled) || selected != nil {
		t.Fatal("canceled selection exposed data", err)
	}
	inner, stop := context.WithCancel(context.Background())
	defer stop()
	during := &monthlySeriesCancelAt{Context: inner, at: 5, cancel: stop}
	if selected, err := monthlySelectDisplaySeries(during, plan, sources[1]); !errors.Is(err, context.Canceled) || selected != nil {
		t.Fatal("late cancellation exposed selected balances", err)
	}
}
