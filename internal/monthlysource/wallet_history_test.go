package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

type historyFixtureFlow struct {
	credit, consume, refund, at int64
	zeroRequest                 bool
}

type walletHistoryFixture struct {
	db         *sql.DB
	file       string
	assemblies []CapturedMonthAssembly
	closing    WalletHistoryBoundary
	months     []WalletHistoryMonth
}

func historyFixtureAt(month string, day int) int64 {
	at, _ := time.ParseInLocation("2006-01", month, fundingMonthZone)
	return at.AddDate(0, 0, day-1).Add(12 * time.Hour).Unix()
}

// Synthetic ORIGINAL observations, not a generated report used as source.
// Cash/counter closing values are independently specified by each test.
func newWalletHistorySource(t *testing.T, names []string, registrations map[int64]int64, flows map[string]map[int64]historyFixtureFlow, balances, used map[int64]int64) walletHistoryFixture {
	t.Helper()
	db, file := fundingTestStore(t)
	if err := InitFundingMonthCaptureStore(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if err := InitTokenMonthCaptureStore(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if err := InitWalletMonthCaptureStore(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	ids := []int64{}
	for id := range registrations {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var fingerprint strings.Builder
	for _, id := range ids {
		_, _ = fmt.Fprintf(&fingerprint, "%d,", id)
	}
	last, _ := time.ParseInLocation("2006-01", names[len(names)-1], fundingMonthZone)
	boundary := last.AddDate(0, 1, 0).Unix()
	base := time.Unix(boundary+86400, 0)
	f := walletHistoryFixture{db: db, file: file}
	for _, name := range names {
		scope := OrganizationMonthScope{Version: 1, Mode: OrganizationMonthScopeMode, GroupID: 1, AuthVersion: 1, MemberFingerprint: digest([]byte(fingerprint.String()))[:32], MemberManifest: strings.Repeat("b", 64), Funding: FundingMonthRequest{Month: name, SourceIdentity: digest([]byte("synthetic-history-source")), Rules: fundingTestRules(), UserIDs: slices.Clone(ids)}}
		key := digest([]byte("synthetic-history/" + name))
		plan, err := EnsureFundingMonthCapture(t.Context(), db, key, scope)
		if err != nil {
			t.Fatal(err)
		}
		days := len(plan.Scans) / 2
		for phase := 0; phase < 2; phase++ {
			for day := 0; day < days; day++ {
				scan := plan.Scans[phase*days+day]
				at := base.Add(time.Duration(phase)*6*time.Minute + time.Duration(day)*time.Millisecond).UnixMilli()
				page := FundingPage{Version: 1, Scan: scan, FenceID: math.MaxInt64, EndReached: true, StartedAtMS: at, CompletedAtMS: at + 1, Rows: []FundingLog{}}
				for member, id := range ids {
					flow := flows[name][id]
					if flow.credit != 0 && flow.at >= scan.From && flow.at < scan.To {
						direction, amount := "user.quota_add", flow.credit
						if amount < 0 {
							direction, amount = "user.quota_subtract", -amount
						}
						row := FundingLog{ID: plan.Scans[0].From + 900000 + int64(member), UserID: 100, CreatedAt: flow.at, Type: 3, Content: "synthetic target audit", Other: fundingTestAudit(direction, fmt.Sprintf(`{"target_user_id":%d,"quota":"%d 点额度"}`, id, amount))}
						page.Rows = append(page.Rows, row)
					}
				}
				sort.Slice(page.Rows, func(i, j int) bool {
					return fundingCursorBefore(FundingCursor{At: page.Rows[i].CreatedAt, ID: page.Rows[i].ID}, FundingCursor{At: page.Rows[j].CreatedAt, ID: page.Rows[j].ID})
				})
				if len(page.Rows) > 0 {
					row := page.Rows[len(page.Rows)-1]
					page.Next = FundingCursor{At: row.CreatedAt, ID: row.ID}
				}
				if _, err := SaveFundingPage(t.Context(), db, page); err != nil {
					t.Fatal(err)
				}
				for member, id := range ids {
					flow := flows[name][id]
					request := UsageWindowRequest{SourceIdentity: scope.Funding.SourceIdentity, Rules: scope.Funding.Rules, UserID: id, From: scan.From, To: scan.To}
					w := WalletUser{UserID: id, Exists: true, BalanceQuota: walletPointer(balances[id]), UsedQuota: walletPointer(used[id])}
					o := TokenUsageWindowObservation{Version: 1, Basis: tokenUsageBasis, Request: request, StartedAtMS: at, CompletedAtMS: at + 1, Wallet: w, Totals: []UsageWindowRow{}, Rows: []TokenUsageWindowRow{}}
					if flow.at >= scan.From && flow.at < scan.To {
						for _, v := range []struct{ kind, quota int64 }{{2, flow.consume}, {6, flow.refund}} {
							if v.quota == 0 && !(v.kind == 2 && flow.zeroRequest) {
								continue
							}
							logID := plan.Scans[0].From + int64(day)*20 + int64(member)*3 + v.kind
							row := UsageWindowRow{UserID: id, Type: int(v.kind), BillingSource: "wallet", Records: 1, Quota: v.quota, MinQuota: v.quota, MaxQuota: v.quota, MinID: logID, MaxID: logID, FirstAt: flow.at + 10 + v.kind, LastAt: flow.at + 10 + v.kind}
							o.Totals = append(o.Totals, row)
							o.Rows = append(o.Rows, TokenUsageWindowRow{UsageWindowRow: row, TokenID: 11, Name: "same-display-name-" + name})
						}
					}
					ordinal := phase*days*len(ids) + day*len(ids) + member
					if err := saveTokenCaptureSlot(t.Context(), db, plan, ordinal, o); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		for phase := 0; phase < 2; phase++ {
			at := base.Add(time.Duration(phase) * 6 * time.Minute).UnixMilli()
			o := WalletObservation{Version: 1, Basis: walletObservationBasis, SourceIdentity: scope.Funding.SourceIdentity, StartedAtMS: at, CompletedAtMS: at + 1, Users: []WalletUser{}}
			for _, id := range ids {
				o.Users = append(o.Users, WalletUser{UserID: id, Exists: true, BalanceQuota: walletPointer(balances[id]), UsedQuota: walletPointer(used[id])})
			}
			if err := saveWalletCaptureSlot(t.Context(), db, plan, phase, o); err != nil {
				t.Fatal(err)
			}
		}
		assembly, err := BuildPartitionedCapturedMonthSelection(t.Context(), db, key, scope, false)
		if err != nil {
			t.Fatal(err)
		}
		f.assemblies = append(f.assemblies, assembly)
		observed, err := ReviewCapturedWalletHistoryMonth(t.Context(), db, assembly.Selection, assembly.Tokens)
		if err != nil {
			t.Fatal(err)
		}
		f.months = append(f.months, observed)
		if name == names[len(names)-1] {
			if err := InitWalletTailCaptureStore(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				for phase := 0; phase < 2; phase++ {
					at := base.Add(time.Duration(phase) * 6 * time.Minute).UnixMilli()
					o := WalletTailObservation{Version: 1, Basis: walletTailBasis, Request: WalletTailRequest{SourceIdentity: scope.Funding.SourceIdentity, Rules: scope.Funding.Rules, UserID: id, Boundary: boundary}, StartedAtMS: at, CompletedAtMS: at + 1, User: RegisteredWalletUser{UserID: id, RegisteredAt: registrations[id], BalanceQuota: walletPointer(balances[id]), UsedQuota: walletPointer(used[id])}, Usage: []UsageWindowRow{}, Funding: []FundingLog{}}
					if err := saveWalletTailCapture(t.Context(), db, key, registeredCapturePlanSHA(plan), scope, phase, o, time.Now()); err != nil {
						t.Fatal(err)
					}
				}
			}
			f.closing, err = ReviewCapturedWalletHistoryBoundary(t.Context(), db, assembly.Selection)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	return f
}

func walletHistoryDefaultFixture(t *testing.T) walletHistoryFixture {
	t.Helper()
	months := []string{"2026-06", "2026-07", "2026-08", "2026-09"}
	registered := map[int64]int64{201: historyFixtureAt("2026-06", 15), 202: historyFixtureAt("2026-08", 12)}
	flows := map[string]map[int64]historyFixtureFlow{
		"2026-06": {201: {credit: 1000, consume: 200, refund: 10, at: registered[201]}},
		"2026-07": {201: {credit: 500, consume: 300, at: historyFixtureAt("2026-07", 2)}},
		"2026-08": {201: {consume: 100, refund: 5, at: historyFixtureAt("2026-08", 2)}, 202: {credit: 2000, consume: 50, at: registered[202]}},
		"2026-09": {202: {consume: 100, refund: 10, at: historyFixtureAt("2026-09", 2)}},
	}
	return newWalletHistorySource(t, months, registered, flows, map[int64]int64{201: 915, 202: 1860}, map[int64]int64{201: 585, 202: 140})
}

func TestWalletHistoryCompleteOriginalChainAndIndependentAccounts(t *testing.T) {
	f := walletHistoryDefaultFixture(t)
	before, err := os.ReadFile(f.file)
	if err != nil {
		t.Fatal(err)
	}
	reversed := slices.Clone(f.months)
	slices.Reverse(reversed)
	history, err := DeriveWalletHistory(t.Context(), f.closing, reversed)
	if err != nil {
		t.Fatal(err)
	}
	r := history.Report()
	if r.FromMonth != "2026-06" || r.ThroughMonth != "2026-09" || r.GroupID != 1 || len(r.Accounts) != 2 {
		t.Fatal(r)
	}
	wantBalance := [][]int64{{0, 810, 1010, 915, 915}, {0, 0, 0, 1950, 1860}}
	wantUsed := [][]int64{{0, 190, 490, 585, 585}, {0, 0, 0, 50, 140}}
	for i, a := range r.Accounts {
		if a.UserID != int64(201+i) || a.OriginBalanceQuota != 0 || a.OriginUsedQuota != 0 || a.OriginRequiresResolution || !validDigest(a.ClosingOriginalSHA256[0]) || a.ClosingOriginalSHA256[0] == a.ClosingOriginalSHA256[1] {
			t.Fatal("origin/capture identity or account changed", a)
		}
		for j, m := range a.Months {
			if m.OpeningBalanceQuota != wantBalance[i][j] || m.ClosingBalanceQuota != wantBalance[i][j+1] || m.OpeningUsedQuota != wantUsed[i][j] || m.ClosingUsedQuota != wantUsed[i][j+1] || !validDigest(m.SelectionSHA256) || !validDigest(m.TokenSHA256) {
				t.Fatal("month did not close exactly", i, j, m)
			}
			var consume, refund int64
			for _, token := range m.Tokens {
				consume += token.ConsumeQuota
				refund += token.RefundQuota
			}
			if consume != m.ConsumeQuota || refund != m.RefundQuota {
				t.Fatal("token/user money mismatch", i, j)
			}
		}
	}
	after, err := os.ReadFile(f.file)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("local derivation wrote original sources", err)
	}
	// These reports are not round-trippable proof or user-editable balances.
	for _, proof := range []any{history, f.months[0], f.closing} {
		if _, err := json.Marshal(proof); err == nil {
			t.Fatal("proof serialized as an editable financial assertion")
		}
	}
}

func TestWalletHistoryReopenAndReportMutationDoNotAlterSourceMoney(t *testing.T) {
	f := walletHistoryDefaultFixture(t)
	want, err := DeriveWalletHistory(t.Context(), f.closing, f.months)
	if err != nil {
		t.Fatal(err)
	}
	original := want.Report()
	mutated := want.Report()
	mutated.Accounts[0].Months[0].Movements[0].DeltaQuota++
	mutated.Accounts[0].Months[0].Tokens[0].ConsumeQuota++
	if !reflect.DeepEqual(want.Report(), original) {
		t.Fatal("caller changed opaque source arithmetic")
	}
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := sql.Open("sqlite", "file:"+f.file+"?mode=ro&_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	ro.SetMaxOpenConns(1)
	months := []WalletHistoryMonth{}
	for _, a := range f.assemblies {
		m, err := ReviewCapturedWalletHistoryMonth(t.Context(), ro, a.Selection, a.Tokens)
		if err != nil {
			t.Fatal(err)
		}
		months = append(months, m)
	}
	closing, err := ReviewCapturedWalletHistoryBoundary(t.Context(), ro, f.assemblies[len(f.assemblies)-1].Selection)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DeriveWalletHistory(t.Context(), closing, months)
	if err != nil || !reflect.DeepEqual(got.Report(), original) {
		t.Fatal("reopen changed money or provenance", err)
	}
}

func TestWalletHistoryRejectsGapsDuplicatesAndCrossScope(t *testing.T) {
	f := walletHistoryDefaultFixture(t)
	for _, scenario := range []string{"gap", "duplicate", "missing_prefix", "missing_tail", "zero_proof", "group", "auth", "manifest", "source", "roster", "endpoint_reuse", "pre_registration", "cancel", "nil_context"} {
		t.Run(scenario, func(t *testing.T) {
			months := slices.Clone(f.months)
			closing := f.closing
			ctx := t.Context()
			switch scenario {
			case "gap":
				months = append(slices.Clone(months[:1]), months[2:]...)
			case "duplicate":
				months = append(months, months[0])
			case "missing_prefix":
				months = months[1:]
			case "missing_tail":
				months = months[:len(months)-1]
			case "zero_proof":
				months[0] = WalletHistoryMonth{}
			case "group", "auth", "manifest", "source", "roster":
				d := *months[1].data
				months[1] = WalletHistoryMonth{data: &d}
				switch scenario {
				case "group":
					d.scope.GroupID++
				case "auth":
					d.scope.AuthVersion++
				case "manifest":
					d.scope.MemberManifest = strings.Repeat("a", 64)
				case "source":
					d.scope.Funding.SourceIdentity = strings.Repeat("a", 64)
				case "roster":
					d.scope.Funding.UserIDs = []int64{201}
				}
			case "endpoint_reuse":
				d := *months[1].data
				d.endpoints = slices.Clone(d.endpoints)
				d.endpoints[0].id = months[0].data.endpoints[0].id
				months[1] = WalletHistoryMonth{data: &d}
			case "pre_registration":
				closing.accounts = slices.Clone(closing.accounts)
				closing.accounts[0].registeredAt = historyFixtureAt("2026-06", 16)
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "nil_context":
				ctx = nil
			}
			if h, err := DeriveWalletHistory(ctx, closing, months); err == nil || h.report != nil {
				t.Fatal("incomplete/mixed source became a history", scenario, err)
			}
		})
	}
}

func TestWalletHistoryRawMonthRejectsMissingAndChangedSources(t *testing.T) {
	f := walletHistoryDefaultFixture(t)
	base := f.assemblies[0]
	for _, scenario := range []string{"no_archive", "reordered_archives", "wrong_archive_selection", "changed_parent", "unknown_billing", "wallet_ids", "funding_gap", "nil_context"} {
		t.Run(scenario, func(t *testing.T) {
			var s CapturedMonthSelection
			if err := json.Unmarshal(base.Selection, &s); err != nil {
				t.Fatal(err)
			}
			raw := slices.Clone(base.Selection)
			tokens := slices.Clone(base.Tokens)
			ctx := t.Context()
			switch scenario {
			case "no_archive":
				tokens = tokens[:1]
			case "reordered_archives":
				slices.Reverse(tokens)
			case "wallet_ids":
				s.WalletIDs = nil
				raw, _ = json.Marshal(s)
			case "funding_gap":
				s.Funding = s.Funding[1:]
				raw, _ = json.Marshal(s)
			case "wrong_archive_selection", "changed_parent", "unknown_billing":
				var archive TokenUsageMonthArchive
				if err := json.Unmarshal(tokens[0].Raw, &archive); err != nil {
					t.Fatal(err)
				}
				switch scenario {
				case "wrong_archive_selection":
					archive.SelectionSHA256 = strings.Repeat("a", 64)
				case "changed_parent":
					archive.Pairs[0].Older.Wallet.BalanceQuota = walletPointer(123)
				case "unknown_billing":
					for i := range archive.Pairs {
						for _, o := range []*TokenUsageWindowObservation{&archive.Pairs[i].Older, &archive.Pairs[i].Newer} {
							for j := range o.Rows {
								o.Rows[j].BillingSource = "unspecified"
							}
							for j := range o.Totals {
								o.Totals[j].BillingSource = "unspecified"
							}
						}
					}
				}
				tokens[0].Raw, _ = json.Marshal(archive)
			case "nil_context":
				ctx = nil
			}
			if m, err := ReviewCapturedWalletHistoryMonth(ctx, f.db, raw, tokens); err == nil || m.data != nil {
				t.Fatal("changed/missing originals accepted", scenario, err)
			}
		})
	}
}

func TestWalletHistoryOriginResidualDoesNotChangeConsumption(t *testing.T) {
	f := walletHistoryDefaultFixture(t)
	closing := f.closing
	closing.accounts = slices.Clone(closing.accounts)
	closing.accounts[0].balance++
	h, err := DeriveWalletHistory(t.Context(), closing, f.months)
	if err != nil {
		t.Fatal(err)
	}
	a := h.Report().Accounts[0]
	if !a.OriginRequiresResolution || a.OriginBalanceQuota != 1 || a.OriginUsedQuota != 0 || a.Months[0].ConsumeQuota != 200 || a.Months[0].RefundQuota != 10 || a.Months[3].ClosingBalanceQuota != 916 {
		t.Fatal("unexplained balance was absorbed into charges", a)
	}
	closing.accounts[0].used++
	h, err = DeriveWalletHistory(t.Context(), closing, f.months)
	if err != nil || h.Report().Accounts[0].OriginUsedQuota != 1 || !h.Report().Accounts[0].OriginRequiresResolution {
		t.Fatal("counter residual hidden", err)
	}
	closing.accounts[0].used = 584
	if h, err := DeriveWalletHistory(t.Context(), closing, f.months); err == nil || h.report != nil {
		t.Fatal("negative initial used quota invented")
	}
	closing = f.closing
	closing.accounts = slices.Clone(closing.accounts)
	closing.accounts[0].balance++
	closing.accounts[1].balance--
	if h, err := DeriveWalletHistory(t.Context(), closing, f.months); err == nil || h.report != nil {
		t.Fatal("opposite account differences netted or late member given an earlier balance")
	}
}

func TestWalletHistoryZeroPriceActivityBeforeRegistrationIsNotAnEmptyMonth(t *testing.T) {
	registered := map[int64]int64{201: historyFixtureAt("2026-09", 15)}
	flows := map[string]map[int64]historyFixtureFlow{"2026-09": {201: {at: historyFixtureAt("2026-09", 14), zeroRequest: true}}}
	f := newWalletHistorySource(t, []string{"2026-09"}, registered, flows, map[int64]int64{201: 0}, map[int64]int64{201: 0})
	if h, err := DeriveWalletHistory(t.Context(), f.closing, f.months); err == nil || h.report != nil {
		t.Fatal("observed zero-price request treated as absence")
	}
}

func TestWalletHistoryExactLargeQuotasAndOverflow(t *testing.T) {
	registered := map[int64]int64{201: historyFixtureAt("2026-09", 15)}
	const gross = int64(9007199254740993)
	flows := map[string]map[int64]historyFixtureFlow{"2026-09": {201: {credit: gross, consume: 7, refund: 3, at: registered[201]}}}
	f := newWalletHistorySource(t, []string{"2026-09"}, registered, flows, map[int64]int64{201: gross - 4}, map[int64]int64{201: 4})
	h, err := DeriveWalletHistory(t.Context(), f.closing, f.months)
	if err != nil || h.Report().Accounts[0].OriginBalanceQuota != 0 || h.Report().Accounts[0].Months[0].ClosingBalanceQuota != gross-4 {
		t.Fatal("quota passed through a float or lost an integer point", err)
	}
	row := walletHistoryMonthAccount{ConsumeQuota: 1}
	if _, _, err := walletHistoryReverse(math.MaxInt64, 1, row); !errors.Is(err, ErrWalletMonthCaptureIntegrity) {
		t.Fatal("derived balance overflow accepted", err)
	}
	row = walletHistoryMonthAccount{Movements: []FundingObservedMovement{{DeltaQuota: math.MaxInt64}, {DeltaQuota: math.MaxInt64}, {DeltaQuota: -math.MaxInt64}, {DeltaQuota: -math.MaxInt64}}}
	b, u, err := walletHistoryReverse(42, 0, row)
	if err != nil || b != 42 || u != 0 {
		t.Fatal("intermediate signed sum overflow changed exact final value", b, u, err)
	}
}

func TestWalletHistoryScopeBindingAndReportCannotBecomeProof(t *testing.T) {
	f := walletHistoryDefaultFixture(t)
	h, err := DeriveWalletHistory(t.Context(), f.closing, f.months)
	if err != nil {
		t.Fatal(err)
	}
	for _, month := range f.months {
		if !h.MatchesCollectionScope(month.data.scope) {
			t.Fatal("valid original month scope not bound")
		}
	}
	for _, kind := range []string{"month", "bad_month", "auth", "roster", "manifest", "group", "source"} {
		s := f.closing.scope
		switch kind {
		case "month":
			s.Funding.Month = "2026-05"
		case "bad_month":
			s.Funding.Month = "2026-06bad"
		case "auth":
			s.AuthVersion++
		case "roster":
			s.Funding.UserIDs = []int64{201}
		case "manifest":
			s.MemberManifest = strings.Repeat("c", 64)
		case "group":
			s.GroupID++
		case "source":
			s.Funding.SourceIdentity = strings.Repeat("c", 64)
		}
		if h.MatchesCollectionScope(s) {
			t.Fatal("history rebound to another scope", kind)
		}
	}
	raw, err := json.Marshal(h.Report())
	if err != nil {
		t.Fatal(err)
	}
	copy := h
	if err := json.Unmarshal(raw, &copy); err == nil || copy.report != nil || copy.MatchesCollectionScope(f.closing.scope) {
		t.Fatal("editable report became a source history")
	}
	m := f.months[0]
	if err := json.Unmarshal([]byte(`{"complete":true}`), &m); err == nil || m.data != nil {
		t.Fatal("report flag became a source month")
	}
	b := f.closing
	if err := json.Unmarshal([]byte(`{"balance":0}`), &b); err == nil || len(b.accounts) != 0 {
		t.Fatal("report became an original boundary")
	}
}

func TestWalletHistoryMisconfiguredSourceRejectedBeforeBusinessRead(t *testing.T) {
	db, state, _ := walletTestSource(t, nil)
	if m, err := ReviewCapturedWalletHistoryMonth(t.Context(), db, []byte(`{}`), nil); err == nil || m.data != nil {
		t.Fatal("non-receiver source accepted")
	}
	if len(state.queries) != 1 || state.queries[0] != "SELECT sqlite_version()" {
		t.Fatal("misconfigured source reached business queries", state.queries)
	}
}

// Numeric simulation only. The original source readers and real retained
// shapes are exercised above; these internal mutations are NOT source proof.
func TestWalletHistorySimulatedSignedMoneyNeverChangesCharges(t *testing.T) {
	registered := map[int64]int64{201: historyFixtureAt("2026-09", 15)}
	flows := map[string]map[int64]historyFixtureFlow{"2026-09": {201: {credit: 1000, consume: 200, refund: 10, at: registered[201]}}}
	f := newWalletHistorySource(t, []string{"2026-09"}, registered, flows, map[int64]int64{201: 810}, map[int64]int64{201: 190})
	rng := rand.New(rand.NewSource(20261030))
	for i := 0; i < 5000; i++ {
		origin, originUsed := rng.Int63n(2001)-1000, rng.Int63n(1000)
		credit, debit, consume := rng.Int63n(1<<42), rng.Int63n(1<<42), rng.Int63n(1<<42)
		refund := rng.Int63n(consume + 1)
		closing := f.closing
		closing.accounts = slices.Clone(closing.accounts)
		closing.accounts[0].balance = origin + credit - debit - consume + refund
		closing.accounts[0].used = originUsed + consume - refund
		d := *f.months[0].data
		d.accounts = slices.Clone(d.accounts)
		row := &d.accounts[0]
		row.ConsumeQuota, row.RefundQuota = consume, refund
		row.Movements = []FundingObservedMovement{{LogID: 11, DeltaQuota: credit}, {LogID: 12, DeltaQuota: -debit}}
		row.Tokens = []TokenUsageMonthRow{{TokenID: 11, ConsumeQuota: consume, RefundQuota: refund}}
		h, err := DeriveWalletHistory(t.Context(), closing, []WalletHistoryMonth{{data: &d}})
		if err != nil {
			t.Fatal("integer simulation rejected bounded inputs", i, err)
		}
		a := h.Report().Accounts[0]
		m := a.Months[0]
		if a.OriginBalanceQuota != origin || a.OriginUsedQuota != originUsed || a.OriginRequiresResolution != (origin != 0 || originUsed != 0) || m.ConsumeQuota != consume || m.RefundQuota != refund || m.ClosingBalanceQuota != closing.accounts[0].balance || m.ClosingUsedQuota != closing.accounts[0].used {
			t.Fatal("simulation changed source charges, residual or account balance", i, a)
		}
	}
}
