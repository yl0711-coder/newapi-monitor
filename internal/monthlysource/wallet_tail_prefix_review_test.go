package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

type tailPrefixFixture struct {
	source, receiver *sql.DB
	d                *pageTailDriver
	key              string
	scope            OrganizationMonthScope
	plan             FundingMonthCapturePlan
	now              time.Time
}

func newTailPrefixFixture(t *testing.T, kind string, count int) *tailPrefixFixture {
	t.Helper()
	source, d, receiver, _, key, scope, plan, now := tailPageJournalFixture(t, kind, count)
	return &tailPrefixFixture{source, receiver, d, key, scope, plan, now}
}

func (f *tailPrefixFixture) series(pass int) walletTailPageSeries {
	r := f.d.r
	r.After = WalletTailPageCursor{}
	return walletTailPageSeries{JobKey: f.key, PlanSHA: registeredCapturePlanSHA(f.plan), ScopeSHA: registeredCaptureScopeSHA(f.scope), Request: r, Pass: pass}
}

// Real capture/commit APIs against the synthetic SQL protocol. No fabricated
// observation bytes or manually filled checkpoint flags in the happy path.
func (f *tailPrefixFixture) capturePass(t *testing.T, pass int) {
	t.Helper()
	s := f.series(pass)
	f.d.r.After = WalletTailPageCursor{}
	before := WalletTailPageJournalProgress{}
	for i := 0; i < 8; i++ {
		o, err := captureWalletTailPage(t.Context(), f.source, f.d.r, func() time.Time { return f.now })
		if err != nil {
			t.Fatal(err)
		}
		if err := saveTailPageJournal(t.Context(), f.receiver, s, before, o, f.now); err != nil {
			t.Fatal(err)
		}
		before, err = loadTailPageJournal(t.Context(), f.receiver, s, f.now)
		if err != nil {
			t.Fatal(err)
		}
		f.now = f.now.Add(time.Second)
		if before.Traversed {
			f.now = f.now.Add(5 * time.Minute)
			return
		}
		f.d.r.After = before.After
	}
	t.Fatal("fixture traversal did not finish")
}

func (f *tailPrefixFixture) review(ctx context.Context) (WalletTailPrefixReview, error) {
	return reviewWalletTailPagePrefix(ctx, f.receiver, f.key, f.scope, 201, f.d.r.Kind, f.d.r.Wallet.Boundary, func() time.Time { return f.now })
}

func TestWalletTailPrefixReviewExactMoneyOnlyOnceAndMovingSuffixExcluded(t *testing.T) {
	f := newTailPrefixFixture(t, "user_usage", 2*walletTailPageRows+3)
	for i := range f.d.items {
		f.d.items[i].quota = int64(i%11 + 1)
		if i%7 == 0 {
			f.d.items[i].typ = 6
		}
	}
	f.d.items[1].quota = 9007199254740993
	f.capturePass(t, 0)
	// A changing live wallet and a new last-time event are NOT a stable prefix
	// change. Do not require a quiet production account to reuse closed pages.
	f.d.s.data[0][2] = int64(1)
	f.d.s.data[0][3] = int64(9007199254740995)
	f.d.items = append(f.d.items, pageTailItem{id: 999999, at: f.d.r.Wallet.Boundary + 20, typ: 2, user: 201, quota: 19, billing: "wallet"})
	f.capturePass(t, 1)
	queries := len(f.d.s.queries)
	if err := f.source.Close(); err != nil {
		t.Fatal(err)
	}
	// A finished retained audit may be reviewed next month without relabeling
	// the original source month or reopening production/source connections.
	f.now = f.now.AddDate(0, 1, 0)
	p, err := f.review(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	r := p.Report()
	if !p.MatchesCaptureScope(f.key, f.scope) || p.MatchesCaptureScope(strings.Repeat("f", 64), f.scope) {
		t.Fatal("lost pinned capture scope")
	}
	for _, changed := range []string{"auth", "group", "roster"} {
		scope := f.scope
		switch changed {
		case "auth":
			scope.AuthVersion++
		case "group":
			scope.GroupID++
		case "roster":
			scope.Funding.UserIDs = []int64{201}
		}
		if p.MatchesCaptureScope(f.key, scope) {
			t.Fatal("review reused under changed capture authority", changed)
		}
	}
	if r.Status != "stable_prefix" || r.StablePages != 2 || r.After.ID == 999999 || len(f.d.s.queries) != queries || r.Boundary != f.d.r.Wallet.Boundary {
		t.Fatal("moving suffix or source reread was mistaken for a cached prefix", r)
	}
	var consume, refund, cn, rn int64
	for _, item := range f.d.items {
		if item.at > r.After.At || item.at == r.After.At && (item.typ > r.After.Type || item.typ == r.After.Type && item.id > r.After.ID) {
			continue
		}
		if item.typ == 2 {
			consume += item.quota
			cn++
		} else {
			refund += item.quota
			rn++
		}
	}
	if r.ConsumeQuota != consume || r.RefundQuota != refund || r.ConsumeRecords != cn || r.RefundRecords != rn || cn+rn != 2*walletTailPageRows || r.FundingDeltaQuota != 0 {
		t.Fatal("prefix rounded, doubled, netted or included the live suffix", r, consume, refund, cn, rn)
	}
	r.ConsumeQuota = -1
	if p.Report().ConsumeQuota != consume {
		t.Fatal("diagnostic report aliases retained amounts")
	}
	if _, err := json.Marshal(p); !errors.Is(err, ErrWalletTailPrefixReview) {
		t.Fatal("review could be serialized as approval")
	}
	raw, _ := json.Marshal(p.Report())
	var decoded WalletTailPrefixReview
	if json.Unmarshal(raw, &decoded) == nil || decoded.Report() != (WalletTailPrefixReport{}) {
		t.Fatal("editable report acquired review capability")
	}
	var full WalletTailObservation
	if json.Unmarshal(raw, &full) != nil || ValidateWalletTailObservation(full) == nil {
		t.Fatal("prefix report became a complete tail")
	}
	for _, name := range []string{"BalanceQuota", "UsedQuota", "Complete", "Approved"} {
		if _, ok := reflect.TypeOf(r).FieldByName(name); ok {
			t.Fatal("prefix exposed an unproved balance or publication flag")
		}
	}
}

func TestWalletTailPrefixReviewBackfillOrChangedQuotaInvalidatesCachedSegment(t *testing.T) {
	for _, change := range []string{"backfill", "quota", "refund", "billing_source", "delete"} {
		t.Run(change, func(t *testing.T) {
			f := newTailPrefixFixture(t, "user_usage", walletTailPageRows+3)
			f.capturePass(t, 0)
			switch change {
			case "backfill":
				f.d.items = append(f.d.items, pageTailItem{id: 90001, at: f.d.r.Wallet.Boundary + 1, typ: 2, user: 201, quota: 99, billing: "wallet"})
			case "quota":
				f.d.items[0].quota++
			case "refund":
				f.d.items[0].typ = 6
			case "billing_source":
				f.d.items[0].billing = "subscription"
			case "delete":
				f.d.items = f.d.items[1:]
			}
			f.capturePass(t, 1)
			p, err := f.review(t.Context())
			if err != nil || p.Report().Status != "changed_prefix" || p.Report().StablePages != 0 || p.Report().After != (WalletTailPageCursor{}) || p.Report().ConsumeQuota != 0 {
				t.Fatal("changed data was reused as stable money", p.Report(), err)
			}
		})
	}
}

func TestWalletTailPrefixReviewFundingKeepsActorsAndSignedAmountsNotForeignTopups(t *testing.T) {
	f := newTailPrefixFixture(t, "global_funding", 401)
	var want int64
	var wantN int64
	for i := range f.d.items {
		item := &f.d.items[i]
		item.content = "使用Creem充值成功，充值额度: 7，支付金额：0.01"
		if i%4 == 0 {
			item.typ, item.user = 3, 999
			item.other = fundingTestAudit("user.quota_add", `{"target_user_id":201,"quota":"123 点额度"}`)
		} else if i%7 == 0 {
			item.typ, item.user = 3, 999
			item.other = fundingTestAudit("user.quota_subtract", `{"target_user_id":201,"quota":"50 点额度"}`)
		} else if i%11 == 0 {
			item.typ, item.user = 3, 999
			item.other = fundingTestAudit("user.quota_add", `{"target_user_id":202,"quota":"999 点额度"}`)
		} else if i%5 == 0 {
			item.user, item.content = 202, "foreign-private-topup"
		}
	}
	// All candidates share a timestamp: index type order is not the ID order.
	// Derive the independent expected set by its final reviewed cursor below.
	f.capturePass(t, 0)
	f.capturePass(t, 1)
	p, err := f.review(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	r := p.Report()
	want, wantN = 0, 0
	for i, item := range f.d.items {
		if item.typ > r.After.Type || item.typ == r.After.Type && item.id > r.After.ID {
			continue
		}
		switch {
		case i%4 == 0:
			want += 123
			wantN++
		case i%7 == 0:
			want -= 50
			wantN++
		case i%11 == 0:
		case i%5 == 0:
		default:
			want += 7
			wantN++
		}
	}
	if r.Status != "stable_prefix" || r.StablePages != 2 || r.FundingDeltaQuota != want || r.FundingRecords != wantN || r.ConsumeQuota != 0 || r.RefundQuota != 0 {
		t.Fatal("actor, signed funds, foreign topup or pass accounting differs", r, want, wantN)
	}
	f = newTailPrefixFixture(t, "global_funding", 201)
	for i := range f.d.items {
		f.d.items[i].content = "使用Creem充值成功，充值额度: 7，支付金额：0.01"
	}
	f.capturePass(t, 0)
	f.d.items[0].content = "使用Creem充值成功，充值额度: 8，支付金额：0.01"
	f.capturePass(t, 1)
	p, err = f.review(t.Context())
	if err != nil || p.Report().Status != "changed_prefix" || p.Report().StablePages != 0 {
		t.Fatal("changed funding reused under unchanged index IDs", p.Report(), err)
	}
}

// Counterfactual LOCAL synthetic corruption. Recompute row digests/child
// metadata so tests also prove a hash alone is not a monetary/coverage audit.
func rewriteTailPrefixPage(t *testing.T, f *tailPrefixFixture, pass, seq int, change func(*WalletTailPageObservation)) {
	t.Helper()
	s := f.series(pass)
	var raw []byte
	if err := f.receiver.QueryRow(`SELECT payload FROM portal_monthly_tail_page_journal WHERE series=? AND seq=?`, s.id(), seq).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var p WalletTailPageObservation
	if json.Unmarshal(raw, &p) != nil {
		t.Fatal("invalid fixture original")
	}
	change(&p)
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.receiver.Exec(`DROP TRIGGER monthly_tail_page_no_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.receiver.Exec(`UPDATE portal_monthly_tail_page_journal SET id=?,payload=?,started_at_ms=?,completed_at_ms=? WHERE series=? AND seq=?`, digest(raw), raw, p.StartedAtMS, p.CompletedAtMS, s.id(), seq); err != nil {
		t.Fatal(err)
	}
	if _, err := f.receiver.Exec(`UPDATE portal_monthly_tail_page_journal SET previous_id=? WHERE series=? AND seq=?`, digest(raw), s.id(), seq+1); err != nil {
		t.Fatal(err)
	}
	if err := InitWalletTailPageJournal(t.Context(), f.receiver); err != nil {
		t.Fatal(err)
	}
}

func TestWalletTailPrefixReviewAuditsMiddleRowsAndDoesNotTrustRehashedCorruption(t *testing.T) {
	for _, fault := range []string{"cursor", "registration", "time", "early_end", "money"} {
		t.Run(fault, func(t *testing.T) {
			f := newTailPrefixFixture(t, "user_usage", 2*walletTailPageRows+3)
			f.capturePass(t, 0)
			f.capturePass(t, 1)
			rewriteTailPrefixPage(t, f, 0, 1, func(p *WalletTailPageObservation) {
				switch fault {
				case "cursor":
					p.Request.After.ID++
				case "registration":
					p.User.RegisteredAt--
				case "time":
					p.StartedAtMS -= 1001
				case "early_end":
					p.ViewEndReached = true
				case "money":
					p.Usage[0].Quota++
				}
			})
			p, err := f.review(t.Context())
			if fault == "money" {
				if err != nil || p.Report().Status != "changed_prefix" || p.Report().StablePages != 1 {
					t.Fatal("rehashed changed money reused", p.Report(), err)
				}
			} else if err == nil || p.Report() != (WalletTailPrefixReport{}) {
				t.Fatal("middle-chain damage passed full audit", p.Report(), err)
			}
		})
	}
}

func TestWalletTailPrefixReviewUnresolvedOverflowIncompleteAndWrongScopeHaveNoApproval(t *testing.T) {
	for _, fault := range []string{"unknown_source", "overflow", "incomplete", "scope", "member", "kind", "boundary", "cancel", "too_close"} {
		t.Run(fault, func(t *testing.T) {
			f := newTailPrefixFixture(t, "user_usage", 2*walletTailPageRows+3)
			if fault == "unknown_source" {
				for i := range f.d.items {
					f.d.items[i].billing = "unspecified"
				}
			}
			if fault == "overflow" {
				for i := range f.d.items {
					f.d.items[i].quota = 1000000000000000
				}
			}
			f.capturePass(t, 0)
			if fault != "incomplete" {
				f.capturePass(t, 1)
			}
			ctx := t.Context()
			switch fault {
			case "scope":
				f.scope.AuthVersion++
			case "member":
				f.scope.Funding.UserIDs = []int64{999}
			case "kind":
				f.d.r.Kind = "other"
			case "boundary":
				f.d.r.Wallet.Boundary--
			case "cancel":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			case "too_close":
				for seq := 0; seq < 3; seq++ {
					rewriteTailPrefixPage(t, f, 1, seq, func(p *WalletTailPageObservation) { p.StartedAtMS -= 300000; p.CompletedAtMS -= 300000 })
				}
			}
			p, err := f.review(ctx)
			if err == nil || p.Report() != (WalletTailPrefixReport{}) {
				t.Fatal("unresolved or incomplete input returned a review", p.Report(), err)
			}
			if (fault == "unknown_source" || fault == "overflow") && !errors.Is(err, ErrWalletTailPrefixReview) {
				t.Fatal("lost unresolved-amount classification", err)
			}
		})
	}
}

func TestWalletTailPrefixReviewOnePageAlwaysRemainsLiveAndBigSignedFundingIsExact(t *testing.T) {
	f := newTailPrefixFixture(t, "user_usage", 3)
	f.capturePass(t, 0)
	f.capturePass(t, 1)
	p, err := f.review(t.Context())
	if err != nil || p.Report().Status != "no_closed_prefix" || p.Report().StablePages != 0 || p.Report().ConsumeQuota != 0 {
		t.Fatal("last page cached as closed month", p.Report(), err)
	}
	f = newTailPrefixFixture(t, "global_funding", 201)
	for i := range f.d.items {
		f.d.items[i].typ, f.d.items[i].user = 3, 999
		action := "user.quota_add"
		if i%4 >= 2 {
			action = "user.quota_subtract"
		}
		f.d.items[i].other = fundingTestAudit(action, fmt.Sprintf(`{"target_user_id":201,"quota":"%d 点额度"}`, int64(9223372036854775807)))
	}
	f.capturePass(t, 0)
	f.capturePass(t, 1)
	p, err = f.review(t.Context())
	if err != nil || p.Report().StablePages != 1 || p.Report().FundingRecords != 200 || p.Report().FundingDeltaQuota != 0 {
		t.Fatal("signed funding rounded or overflowed", p.Report(), err)
	}
	raw, _ := json.Marshal(p.Report())
	if strings.Contains(string(raw), "admin-actor") {
		t.Fatal("raw funding body exposed by review report")
	}
	f = newTailPrefixFixture(t, "global_funding", 201)
	f.capturePass(t, 0)
	f.capturePass(t, 1)
	if p, err := f.review(t.Context()); !errors.Is(err, ErrWalletTailPrefixReview) || p.Report() != (WalletTailPrefixReport{}) {
		t.Fatal("unresolved funding was treated as zero", p.Report(), err)
	}
}
