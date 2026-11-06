package monthlysource

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func tokenWalletReuseFixture(t *testing.T) (TokenUsageMonthArchive, time.Time) {
	t.Helper()
	r, pairs, now := tokenMonthFixture()
	for i := range pairs {
		for _, o := range []*TokenUsageWindowObservation{&pairs[i].Older, &pairs[i].Newer} {
			balance, used := int64(-1), int64(9007199254740993)
			o.Wallet = WalletUser{UserID: r.UserIDs[0], Exists: true, BalanceQuota: &balance, UsedQuota: &used}
		}
	}
	return TokenUsageMonthArchive{Version: 1, Basis: TokenUsageArchiveBasis, SelectionSHA256: strings.Repeat("c", 64),
		SourceIdentity: r.SourceIdentity, Rules: r.Rules, UserID: r.UserIDs[0], Month: r.Month, Pairs: pairs}, now
}

func tokenWalletReuseJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestTokenWalletProjectionPreservesOriginalTimeAndExactDetachedValues(t *testing.T) {
	a, _ := tokenWalletReuseFixture(t)
	parent := a.Pairs[0].Older
	raw := tokenWalletReuseJSON(t, parent)
	got, err := WalletObservationFromToken(parent, digest(raw))
	if err != nil || ValidateWalletObservation(got) != nil || got.SourceIdentity != parent.Request.SourceIdentity || got.StartedAtMS != parent.StartedAtMS || got.CompletedAtMS != parent.CompletedAtMS || got.Basis != walletObservationBasis || len(got.Users) != 1 || *got.Users[0].BalanceQuota != -1 || *got.Users[0].UsedQuota != 9007199254740993 {
		t.Fatal("wallet projection altered source", got, err)
	}
	if !bytes.Equal(raw, tokenWalletReuseJSON(t, parent)) {
		t.Fatal("caller source mutated")
	}
	*got.Users[0].BalanceQuota = 99
	if *parent.Wallet.BalanceQuota != -1 {
		t.Fatal("output aliases parent wallet")
	}
	*parent.Wallet.UsedQuota = 7
	if *got.Users[0].UsedQuota != 9007199254740993 {
		t.Fatal("parent mutation alters projection")
	}
	// An unchanged exact three-second interval is eligible; 3,001ms is not.
	parent.CompletedAtMS = parent.StartedAtMS + 3000
	if _, err := WalletObservationFromToken(parent, digest(tokenWalletReuseJSON(t, parent))); err != nil {
		t.Fatal("exact old wallet budget rejected", err)
	}
	parent.CompletedAtMS++
	if _, err := WalletObservationFromToken(parent, digest(tokenWalletReuseJSON(t, parent))); !errors.Is(err, ErrTokenWalletReuseIneligible) {
		t.Fatal("wallet timeout was expanded", err)
	}
}

func TestTokenWalletReuseFullAuditAndStableIndependentSelection(t *testing.T) {
	a, now := tokenWalletReuseFixture(t)
	raw := tokenWalletReuseJSON(t, a)
	report, err := ReviewTokenWalletReuse(t.Context(), raw, digest(raw), now)
	if err != nil || report.Mode != TokenWalletReuseMode || report.Status != "two_original_views_available_not_boundary_proof" || report.Observations != 60 || report.Eligible != 60 || len(report.Rejected) != 0 || len(report.Selected) != 2 {
		t.Fatal("valid source reuse failed", report, err)
	}
	x, y := report.Selected[0], report.Selected[1]
	if y.StartedAtMS-x.CompletedAtMS < 300000 || x.ParentSHA256 == y.ParentSHA256 || x.WalletSHA256 == y.WalletSHA256 {
		t.Fatal("views are not independent")
	}
	parents := map[string]TokenUsageWindowObservation{}
	for _, pair := range a.Pairs {
		for _, p := range []TokenUsageWindowObservation{pair.Older, pair.Newer} {
			parents[digest(tokenWalletReuseJSON(t, p))] = p
		}
	}
	for _, selected := range report.Selected {
		p, ok := parents[selected.ParentSHA256]
		if !ok {
			t.Fatal("selected parent absent")
		}
		wallet, err := WalletObservationFromToken(p, selected.ParentSHA256)
		if err != nil || digest(tokenWalletReuseJSON(t, wallet)) != selected.WalletSHA256 || wallet.StartedAtMS != selected.StartedAtMS || wallet.CompletedAtMS != selected.CompletedAtMS {
			t.Fatal("selected digest does not reconstruct original wallet", err)
		}
	}
	again, err := ReviewTokenWalletReuse(t.Context(), raw, digest(raw), now)
	if err != nil || !reflect.DeepEqual(again, report) || !bytes.Equal(raw, tokenWalletReuseJSON(t, a)) {
		t.Fatal("inspection mutated source or output", err)
	}
	slices.Reverse(a.Pairs)
	reordered := tokenWalletReuseJSON(t, a)
	other, err := ReviewTokenWalletReuse(t.Context(), reordered, digest(reordered), now)
	report.ArchiveSHA256, other.ArchiveSHA256 = "", ""
	if err != nil || !reflect.DeepEqual(other, report) {
		t.Fatal("source pair order changed selected views", err)
	}
	body := tokenWalletReuseJSON(t, other)
	for _, secret := range []string{"9007199254740993", "balance_quota", "used_quota", "same name", "legacy name", "renamed", "password", "approved\""} {
		if bytes.Contains(body, []byte(secret)) {
			t.Fatal("private values or authorization leaked into eligibility report", secret)
		}
	}
}

func TestTokenWalletReuseIneligibleViewsAreExplicitNotFakeZeroOrNewCaptures(t *testing.T) {
	for _, kind := range []string{"all_missing", "all_slow", "one_eligible", "only_close_older_views", "duplicate_wallet_views", "some_slow"} {
		t.Run(kind, func(t *testing.T) {
			a, now := tokenWalletReuseFixture(t)
			start := a.Pairs[0].Older.StartedAtMS
			for i := range a.Pairs {
				pair := &a.Pairs[i]
				if kind == "duplicate_wallet_views" {
					pair.Older.StartedAtMS, pair.Older.CompletedAtMS = start, start+1
					pair.Newer.StartedAtMS, pair.Newer.CompletedAtMS = start+300001, start+300002
				}
				if kind == "all_missing" {
					pair.Older.Wallet = WalletUser{UserID: a.UserID}
					pair.Newer.Wallet = WalletUser{UserID: a.UserID}
				}
				if kind == "all_slow" || kind == "one_eligible" {
					pair.Older.CompletedAtMS = pair.Older.StartedAtMS + 4000
					if kind == "one_eligible" && i == 0 {
						pair.Older.CompletedAtMS = pair.Older.StartedAtMS + 1
					}
					pair.Newer.StartedAtMS = pair.Older.CompletedAtMS + 300000
					pair.Newer.CompletedAtMS = pair.Newer.StartedAtMS + 4000
				}
				if kind == "only_close_older_views" {
					pair.Newer.CompletedAtMS = pair.Newer.StartedAtMS + 4000
				}
				if kind == "some_slow" && i < 3 {
					pair.Newer.CompletedAtMS = pair.Newer.StartedAtMS + 4000
				}
			}
			raw := tokenWalletReuseJSON(t, a)
			r, err := ReviewTokenWalletReuse(t.Context(), raw, digest(raw), now)
			if err != nil || r.Eligible+len(r.Rejected) != r.Observations {
				t.Fatal("valid source rejected or observations silently skipped", r, err)
			}
			switch kind {
			case "all_missing", "all_slow":
				if r.Eligible != 0 || len(r.Selected) != 0 || r.Status != "insufficient_eligible_views" {
					t.Fatal("missing/slow views treated as usable", r)
				}
			case "one_eligible":
				if r.Eligible != 1 || len(r.Selected) != 0 {
					t.Fatal("one capture counted twice", r)
				}
			case "only_close_older_views":
				if r.Eligible != 30 || len(r.Selected) != 0 || r.Status != "insufficient_separation" {
					t.Fatal("close timestamps counted as sufficiently separated", r)
				}
			case "duplicate_wallet_views":
				if r.Eligible != 2 || len(r.Rejected) != 58 || len(r.Selected) != 2 {
					t.Fatal("different window names counted as independent wallet reads", r)
				}
			case "some_slow":
				if r.Eligible != 57 || len(r.Rejected) != 3 || len(r.Selected) != 2 {
					t.Fatal("slow token views not explicitly excluded", r)
				}
			}
			if kind == "duplicate_wallet_views" {
				slices.Reverse(a.Pairs)
				b := tokenWalletReuseJSON(t, a)
				x, err := ReviewTokenWalletReuse(t.Context(), b, digest(b), now)
				r.ArchiveSHA256, x.ArchiveSHA256 = "", ""
				if err != nil || !reflect.DeepEqual(x, r) {
					t.Fatal("duplicate-view source attribution depends on input order", err)
				}
			}
		})
	}
}

func TestTokenWalletReuseCannotBypassArchiveIntegrityOrCancellation(t *testing.T) {
	for _, kind := range []string{"digest", "unknown", "duplicate_key", "missing_day", "wrong_user", "wrong_source", "changed_tokens", "future", "nil_context", "canceled", "invalid_projection_digest"} {
		t.Run(kind, func(t *testing.T) {
			a, now := tokenWalletReuseFixture(t)
			switch kind {
			case "missing_day":
				a.Pairs = a.Pairs[:29]
			case "wrong_user":
				a.UserID++
			case "wrong_source":
				a.SourceIdentity = strings.Repeat("b", 64)
			case "changed_tokens":
				a.Pairs[0].Newer.Rows[0].Name = "changed"
			case "future":
				a.Pairs[0].Newer.StartedAtMS = now.Add(time.Hour).UnixMilli()
				a.Pairs[0].Newer.CompletedAtMS = a.Pairs[0].Newer.StartedAtMS + 1
			}
			raw := tokenWalletReuseJSON(t, a)
			if kind == "unknown" {
				raw = append(bytes.TrimSuffix(raw, []byte("}")), []byte(`,"ready":true}`)...)
			}
			if kind == "duplicate_key" {
				raw = bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
			}
			expected := digest(raw)
			if kind == "digest" {
				expected = strings.Repeat("f", 64)
			}
			ctx := context.Background()
			if kind == "nil_context" {
				ctx = nil
			}
			if kind == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if kind == "invalid_projection_digest" {
				if _, err := WalletObservationFromToken(a.Pairs[0].Older, strings.Repeat("f", 64)); !errors.Is(err, ErrTokenWalletReuseIneligible) {
					t.Fatal("wrong parent digest accepted", err)
				}
				return
			}
			r, err := ReviewTokenWalletReuse(ctx, raw, expected, now)
			if err == nil || !reflect.DeepEqual(r, TokenWalletReuseReport{}) {
				t.Fatal("invalid source returned a reusable partial report", kind, r, err)
			}
		})
	}
}
