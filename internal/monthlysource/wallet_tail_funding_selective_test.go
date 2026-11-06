package monthlysource

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestWalletTailSelectiveFundingQueryLimitsAndDisjointRanges(t *testing.T) {
	source, d, _ := pageTailSource(t, "user_usage", nil)
	_ = source
	r := d.r.Wallet
	for _, after := range []FundingCursor{{}, {At: r.Boundary + 1, ID: 99}} {
		q, args, err := selectiveTailFundingQuery(r, after)
		branches := 3
		want := []any{r.Boundary, r.UserID, r.Boundary, r.UserID, r.Boundary}
		if after.ID != 0 {
			branches = 6
			want = []any{after.At, after.ID, after.At, r.UserID, after.At, after.ID, r.UserID, after.At, r.UserID, after.At, after.ID, r.UserID, after.At}
		}
		if err != nil || strings.Count(q, "UNION ALL") != branches-1 || strings.Count(q, "LIMIT 201") != branches+1 || strings.Count(q, "ORDER BY created_at,type,id") != branches || strings.Contains(q, " OR ") || strings.Contains(q, "OFFSET") || !strings.Contains(q, "STRAIGHT_JOIN logs AS l FORCE INDEX(PRIMARY)") || !reflect.DeepEqual(args, want) {
			t.Fatal("unbounded/overlapping funding range or changed argument scope", q, args, err)
		}
		if !strings.Contains(q, "type=3") || !strings.Contains(q, "user_id=? AND type=1") || !strings.Contains(q, "user_id=? AND type=4") || !strings.Contains(q, "MAX_EXECUTION_TIME(2000)") {
			t.Fatal("lost actor/type scope or source statement bound")
		}
	}
	for _, bad := range []FundingCursor{{At: r.Boundary - 1, ID: 1}, {At: r.Boundary, ID: 0}, {ID: 1}} {
		if _, _, err := selectiveTailFundingQuery(r, bad); err == nil {
			t.Fatal("invalid cursor accepted")
		}
	}
}

func TestWalletTailSelectiveFundingDenseTiesExcludeForeignNoise(t *testing.T) {
	source, d, now := pageTailSource(t, "user_usage", nil)
	for i := int64(1); i <= 601; i++ {
		row := pageTailItem{id: i, at: d.r.Wallet.Boundary + 1 + i/400, typ: 1, user: 201, content: "own"}
		if i%3 == 0 {
			row.typ, row.user = 3, 999
		} else if i%3 == 1 {
			row.typ = 4
		}
		d.items = append(d.items, row)
	}
	for i := int64(10000); i < 15000; i++ {
		typ := int64(1)
		if i%2 == 0 {
			typ = 4
		}
		d.items = append(d.items, pageTailItem{id: i, at: d.r.Wallet.Boundary + 1, typ: typ, user: 202, content: "foreign-private"})
	}
	o, err := captureWalletTailPaged(t.Context(), source, d.r.Wallet, func() time.Time { return now }, walletTailJournalPages)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]pageTailItem(nil), d.items[:601]...)
	sort.Slice(want, func(i, j int) bool {
		return want[i].at < want[j].at || want[i].at == want[j].at && want[i].id < want[j].id
	})
	if len(o.Funding) != len(want) {
		t.Fatal("funding lost or foreign events added", len(o.Funding))
	}
	for i, row := range o.Funding {
		if row.ID != want[i].id || row.Type != int(want[i].typ) || row.UserID != want[i].user {
			t.Fatal("tie boundary repeats/skips or actor rewritten", i, row)
		}
	}
	n := 0
	for _, q := range d.s.queries {
		if strings.Contains(q, "AS funding_candidates") {
			n++
		}
	}
	if n != 4 {
		t.Fatal("foreign topups expanded target funding pages", n)
	}
}
